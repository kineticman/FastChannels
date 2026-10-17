"""
Amazon browser-based auto-login for FastChannels.

Runs as a daemon thread spawned by the Flask route.  All inter-process
communication goes through Redis (keyed by source_id) so the flow works
without an app context in the thread.

Redis keys (all scoped to source_id):
  amazon:auth:status:<id>  — JSON status blob (TTL 10 min)
  amazon:auth:otp:<id>     — OTP value written by the Flask route (TTL 5 min)
  amazon:auth:result:<id>  — cookie_header + storage_state on success (TTL 2 min)

The Flask status-polling endpoint consumes amazon:auth:result when it sees
status=success and writes the credentials into source.config inside a proper
DB transaction.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

_STATUS_TTL = 600   # 10 min
# The result blob (cookies + PE) must outlive a full capture + a lazy poll. Capture alone
# takes ~90s, so a 120s TTL could expire before amazon-auth-status polled, silently dropping
# a freshly captured PE. Match the status TTL so the consumer always has time.
_RESULT_TTL = 600   # 10 min for the caller to consume
_OTP_TIMEOUT = 300  # seconds to wait for user to enter OTP

_STEALTH_ARGS = [
    '--disable-blink-features=AutomationControlled',
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
]

_STEALTH_SCRIPT = """
(function () {
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    try {
        if (!navigator.plugins || !navigator.plugins.length) {
            Object.defineProperty(navigator, 'plugins', {
                get: () => { const a=[1,2,3,4,5]; a.__proto__=navigator.plugins.__proto__; return a; }
            });
        }
    } catch(e) {}
    try { Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']}); } catch(e) {}
    if (!window.chrome) {
        window.chrome = {runtime:{}, loadTimes:function(){}, csi:function(){}, app:{}};
    }
    try {
        const orig = window.navigator.permissions.query.bind(navigator.permissions);
        window.navigator.permissions.query = (p) =>
            p.name === 'notifications'
                ? Promise.resolve({state: Notification.permission})
                : orig(p);
    } catch(e) {}
})();
"""

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

_SIGN_IN_URL = (
    "https://www.amazon.com/ap/signin"
    "?openid.pape.max_auth_age=0"
    "&openid.return_to=https%3A%2F%2Fwww.amazon.com%2F"
    "&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select"
    "&openid.assoc_handle=usflex"
    "&openid.mode=checkid_setup"
    "&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select"
    "&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0"
)


# Fill an input the way a typing user would: native value setter plus
# input/change events.  A bare ``el.value = v`` is invisible to Amazon's
# script-driven /ax/claim pages, which then never submit the password.
_FILL_JS = """
([selector, value]) => {
    const els = Array.from(document.querySelectorAll(selector));
    const el = els.find(e => e.offsetParent !== null) || els[0];
    if (!el) return false;
    el.focus();
    const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
    setter.call(el, value);
    el.dispatchEvent(new Event('input', {bubbles: true}));
    el.dispatchEvent(new Event('change', {bubbles: true}));
    return true;
}
"""

# Submit the form that owns ``field`` through requestSubmit() so submit
# handlers run (Amazon fills metadata1 there).  Falls back to clicking the
# first matching button when the field has no form.
_SUBMIT_JS = """
([field, buttons]) => {
    const els = Array.from(document.querySelectorAll(field));
    const el = els.find(e => e.offsetParent !== null) || els[0];
    const form = el && el.form;
    if (form) {
        const btn = form.querySelector(buttons);
        const submitter = btn && btn.type === 'submit' ? btn : null;
        if (form.requestSubmit) {
            form.requestSubmit(submitter);
            return 'requestSubmit';
        }
        if (btn) { btn.click(); return 'click'; }
        form.submit();
        return 'submit';
    }
    const btn = document.querySelector(buttons);
    if (btn) { btn.click(); return 'click'; }
    return 'none';
}
"""

# Ask Amazon to text a sign-in code instead of using a password.  Passwordless
# accounts get form#signin-with-otp-form ("Sign in with a code"); other
# variants show a "Send code to +1…" control next to the password field.
_CODE_SIGNIN_JS = """
() => {
    const form = document.querySelector('form#signin-with-otp-form');
    if (form) {
        const btn = form.querySelector('input[type="submit"], button[type="submit"]');
        if (form.requestSubmit) form.requestSubmit(btn || null); else form.submit();
        return 'signin-with-otp-form';
    }
    const label = (e) => {
        const ref = e.getAttribute('aria-labelledby');
        const refEl = ref && document.getElementById(ref);
        return (e.innerText || e.value || (refEl && refEl.innerText) || '').trim();
    };
    // \\b keeps "Resend code" (already on the code page) from matching.
    const re = /\\bsend (a )?code|sign in with a code|text me a code/i;
    const ctl = Array.from(document.querySelectorAll(
        'input[type="submit"], button, a, [role="button"]'
    )).find(e => e.offsetParent !== null && re.test(label(e)));
    if (ctl) { ctl.click(); return 'control: ' + label(ctl).slice(0, 40); }
    return 'none';
}
"""

_OTP_INPUT_SELECTOR = (
    'input[name="otpCode"], input#auth-mfa-otpcode, input#cvf-input-code, '
    'input[name="otc"], input[name="code"], input[autocomplete="one-time-code"]'
)

_EMAIL_SELECTOR = 'input#ap_email, input[name="email"]'
_PASSWORD_SELECTOR = 'input#ap_password, input[name="password"], input[type="password"]'


# ── Redis helpers ──────────────────────────────────────────────────────────────

def _status_key(source_id: int) -> str:
    return f"amazon:auth:status:{source_id}"

def _otp_key(source_id: int) -> str:
    return f"amazon:auth:otp:{source_id}"

def _result_key(source_id: int) -> str:
    return f"amazon:auth:result:{source_id}"


def _write_status(r, source_id: int, status: str, detail: str | None = None) -> None:
    payload = {
        'status': status,
        'detail': detail,
        'updated_ms': int(time.time() * 1000),
    }
    r.set(_status_key(source_id), json.dumps(payload), ex=_STATUS_TTL)
    logger.debug('[amazon-auth] status=%s detail=%s', status, detail)


def _wait_for_otp(r, source_id: int) -> str | None:
    key = _otp_key(source_id)
    r.delete(key)
    deadline = time.time() + _OTP_TIMEOUT
    while time.time() < deadline:
        val = r.get(key)
        if val:
            r.delete(key)
            return val.decode() if isinstance(val, bytes) else str(val)
        time.sleep(2)
    return None


# ── Page detection helpers ─────────────────────────────────────────────────────

def _is_captcha_page(page) -> bool:
    url = page.url
    if '/errors/validateCaptcha' in url:
        return True
    try:
        return bool(
            page.query_selector('input#captchacharacters') or
            page.query_selector('img[src*="captcha"]')
        )
    except Exception:
        return False


def _is_otp_page(page) -> bool:
    url = page.url
    if '/ap/mfa' in url or '/ap/cvf' in url:
        return True
    try:
        return bool(
            page.query_selector(_OTP_INPUT_SELECTOR) or
            page.query_selector('form#auth-mfa-form')
        )
    except Exception:
        return False


def _is_signed_in(page) -> bool:
    url = page.url
    for blocked in ('/ap/signin', '/ap/mfa', '/ap/cvf', '/ax/claim', '/errors/validateCaptcha'):
        if blocked in url:
            return False
    if 'amazon.com' not in url:
        return False
    try:
        # Primary: the account greeting ("Hello, <name>" when signed in,
        # "Hello, sign in" when signed out). Check the whole account block so
        # we catch it whether the greeting sits on line-1 or the container.
        acct = page.query_selector('#nav-link-accountList-nav-line-1, #nav-link-accountList')
        if acct:
            text = (acct.inner_text() or '').lower().strip()
            return 'sign in' not in text
        # Secondary: a sign-out link (only present once the account flyout DOM
        # is rendered) or a personalized "Deliver to <name>" glow address.
        if page.query_selector('#nav-item-signout, a[href*="/gp/sign-out"]'):
            return True
        glow = page.query_selector('#glow-ingress-line2, #nav-global-location-popover-link')
        if glow:
            loc = (glow.inner_text() or '').lower().strip()
            # Logged-out default is "select your address" / empty; a name or ZIP
            # means Amazon recognized the session.
            return bool(loc) and 'select your address' not in loc and 'update location' not in loc
        return False
    except Exception:
        return True  # URL check already passed, assume ok


def _auth_error_text(page) -> str | None:
    """Return Amazon's visible sign-in error text (e.g. wrong password), if any."""
    try:
        for el in page.query_selector_all('#auth-error-message-box, .a-alert-error'):
            if el.is_visible():
                text = ' '.join((el.inner_text() or '').split())
                if text:
                    return text[:200]
    except Exception:
        pass
    return None


def _wait_for_login_outcome(page, timeout: float = 12.0) -> str:
    """Wait for either a completed login or an MFA challenge.

    Amazon can navigate to its CVF page a few seconds after the password form
    signals DOMContentLoaded.  Checking for MFA only once immediately after
    that form therefore misses the challenge and reports it as a failed login.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if _is_otp_page(page):
                return 'otp'
            if _is_signed_in(page):
                return 'signed_in'
            if _is_captcha_page(page):
                return 'captcha'
            if _auth_error_text(page):
                return 'auth_error'
        except Exception:
            pass
        time.sleep(0.5)
    if _is_otp_page(page):
        return 'otp'
    return 'signed_in' if _is_signed_in(page) else 'pending'


def _wait_for_otp_submission(page, timeout: float = 45.0) -> str:
    """Wait for an OTP/app-approval submission without requiring navigation.

    Amazon's transaction-approval page can update in place after an app approval,
    so ``expect_navigation`` incorrectly turns a successful submission into a
    timeout.  Keep polling the page instead.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if _is_signed_in(page):
                return 'signed_in'
            if _is_captcha_page(page):
                return 'captcha'
        except Exception:
            pass
        time.sleep(0.5)
    return 'pending'


# ── Debug helpers ─────────────────────────────────────────────────────────────

def _log_step(page, label: str) -> None:
    """One-line progress note for the normal sign-in path."""
    try:
        logger.info('[amazon-auth] %s — %s', label, page.url.split('?')[0])
    except Exception:
        pass


def _dump_page_debug(page, debug: bool, label: str) -> None:
    """Record what the page showed when sign-in failed or stalled.

    URL (query-stripped), title and input fields always — none carry account
    details. Page text and a screenshot only with debug logging on (FC_DEBUG
    or the Settings toggle, see app/debug_flag.py): the page shows the
    account email, and this log is mirrored into the UI activity feed.
    Best-effort; never raises.
    """
    try:
        logger.info('[amazon-auth] %s — url: %s', label, page.url.split('?')[0])
        logger.info('[amazon-auth] %s — title: %s', label, page.title())
        # Amazon repeats the same hidden fields in every form on the page, so
        # list non-hidden inputs in full and hidden ones once by name.
        visible, hidden = [], []
        for el in page.query_selector_all('input'):
            name, type_ = el.get_attribute('name'), el.get_attribute('type')
            if type_ == 'hidden':
                if name and name not in hidden:
                    hidden.append(name)
            else:
                visible.append((name, el.get_attribute('id'), type_))
        logger.info('[amazon-auth] %s — inputs: %s; hidden: %s', label, visible, hidden)
        if not debug:
            return
        body = (page.inner_text('body') or '')[:800].replace('\n', ' ')
        logger.info('[amazon-auth] DEBUG %s — body: %s', label, body)
        path = f'/tmp/amazon_auth_{int(time.time())}_{label.replace(" ", "_")}.png'
        page.screenshot(path=path)
        logger.info('[amazon-auth] DEBUG screenshot saved to %s', path)
    except Exception as exc:
        logger.debug('[amazon-auth] debug dump failed: %s', exc)


# ── Result extraction ──────────────────────────────────────────────────────────

def _save_result(r, source_id: int, context) -> None:
    try:
        cookies = context.cookies()
        pairs = [
            f'{c["name"]}={c["value"]}'
            for c in cookies
            if 'amazon.com' in (c.get('domain') or '')
        ]
        cookie_header = '; '.join(pairs)
        storage_state = context.storage_state()
        result = {
            'cookie_header': cookie_header,
            'storage_state': json.dumps(storage_state),
            'captured_at': time.time(),
        }
        r.set(_result_key(source_id), json.dumps(result), ex=_RESULT_TTL)
        logger.info('[amazon-auth] saved %d cookies to Redis result for source_id=%s',
                    len(pairs), source_id)
    except Exception as exc:
        logger.error('[amazon-auth] failed to save result to Redis: %s', exc)


# ── Main entry point (runs in daemon thread) ───────────────────────────────────

def run_amazon_auth(
    redis_url: str,
    source_id: int,
    email: str,
    password: str,
    storage_state_json: str | None = None,
    debug: bool = False,
) -> None:
    """
    Drive a Playwright browser through Amazon's sign-in flow.
    Status updates land in Redis; the Flask status endpoint reads them.
    On success, writes cookie_header + browser_storage_state to Redis for
    the Flask endpoint to consume and persist into source.config.
    ``debug`` (the debug-logging setting, read by the caller while it still
    has an app context) adds page text and screenshots to failure dumps.
    """
    import redis as _redis
    r = _redis.from_url(redis_url)

    _write_status(r, source_id, 'starting', 'Launching browser…')
    logger.info('[amazon-auth] starting for source_id=%s', source_id)

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as _PWTimeout
    except ImportError as exc:
        _write_status(r, source_id, 'failed', f'Playwright unavailable: {exc}')
        return

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=_STEALTH_ARGS)
            try:
                ctx_kwargs: dict[str, Any] = {
                    'viewport': {'width': 1920, 'height': 1080},
                    'user_agent': _UA,
                    'locale': 'en-US',
                    'timezone_id': 'America/New_York',
                }
                if storage_state_json:
                    try:
                        ctx_kwargs['storage_state'] = json.loads(storage_state_json)
                        logger.info('[amazon-auth] loaded saved browser storage state')
                    except Exception:
                        logger.warning('[amazon-auth] ignoring unparseable storage_state')

                context = browser.new_context(**ctx_kwargs)
                context.add_init_script(_STEALTH_SCRIPT)
                page = context.new_page()

                # ── Try silent session resume ──────────────────────────────
                if storage_state_json:
                    _write_status(r, source_id, 'running', 'Checking existing session…')
                    try:
                        page.goto('https://www.amazon.com/', wait_until='domcontentloaded', timeout=20000)
                        if _is_signed_in(page):
                            logger.info('[amazon-auth] existing session still valid')
                            _save_result(r, source_id, context)
                            _write_status(r, source_id, 'success', 'Session resumed — cookies refreshed.')
                            return
                        logger.info('[amazon-auth] existing session expired, logging in fresh')
                    except _PWTimeout:
                        logger.warning('[amazon-auth] timeout checking existing session, proceeding to login')

                # ── Navigate to sign-in ────────────────────────────────────
                _write_status(r, source_id, 'running', 'Loading sign-in page…')
                try:
                    page.goto(_SIGN_IN_URL, wait_until='domcontentloaded', timeout=20000)
                except _PWTimeout:
                    _write_status(r, source_id, 'failed', 'Timed out loading Amazon sign-in page.')
                    return

                if _is_captcha_page(page):
                    _write_status(r, source_id, 'captcha',
                                  'Amazon served a CAPTCHA. Please paste cookies manually.')
                    return

                # ── Bot-check interstitial ("Continue shopping") ───────────
                if 'continue shopping' in (page.inner_text('body') or '').lower():
                    logger.info('[amazon-auth] bot-check interstitial detected, clicking through')
                    _write_status(r, source_id, 'running', 'Passing Amazon bot check…')
                    try:
                        with page.expect_navigation(wait_until='domcontentloaded', timeout=15000):
                            page.click('button:has-text("Continue shopping"), input[type="submit"]')
                    except _PWTimeout:
                        _dump_page_debug(page, debug, 'after_botcheck')
                        _write_status(r, source_id, 'failed',
                                      'Could not click through Amazon bot check.')
                        return
                    if _is_captcha_page(page):
                        _write_status(r, source_id, 'captcha',
                                      'Amazon served a CAPTCHA after bot check.')
                        return

                # ── Email ──────────────────────────────────────────────────
                # Amazon has two auth flows: old /ap/signin (two-page) and new
                # /ax/claim (SPA — password revealed on same page after email submit).
                # We use JS clicks to bypass Playwright's actionability checks, which
                # can hang when running inside a gevent-monkey-patched thread.
                _write_status(r, source_id, 'running', 'Entering email address…')
                try:
                    page.wait_for_selector(_EMAIL_SELECTOR, timeout=15000)
                    page.evaluate(_FILL_JS, [_EMAIL_SELECTOR, email])
                    page.evaluate(_SUBMIT_JS, [
                        _EMAIL_SELECTOR,
                        'input#continue, input[type="submit"], button[type="submit"]',
                    ])
                    # Wait for password field to appear (SPA reveal or page nav)
                    try:
                        page.wait_for_load_state('domcontentloaded', timeout=8000)
                    except _PWTimeout:
                        pass
                except _PWTimeout:
                    _dump_page_debug(page, debug, 'email_timeout')
                    _write_status(r, source_id, 'failed',
                                  'Email field not found on sign-in page.')
                    return

                if _is_captcha_page(page):
                    _write_status(r, source_id, 'captcha',
                                  'Amazon served a CAPTCHA after email entry.')
                    return

                # ── Password, or a texted code when no password is saved ───
                if password:
                    _write_status(r, source_id, 'running', 'Entering password…')
                    try:
                        page.wait_for_selector(
                            'input[name="password"]:visible, input[type="password"]:visible',
                            timeout=15000,
                        )
                        page.evaluate(_FILL_JS, [_PASSWORD_SELECTOR, password])
                        how = page.evaluate(_SUBMIT_JS, [
                            _PASSWORD_SELECTOR,
                            'input#signInSubmit, input[type="submit"], button[type="submit"]',
                        ])
                        logger.info('[amazon-auth] password submitted via %s', how)
                        try:
                            page.wait_for_load_state('domcontentloaded', timeout=10000)
                        except _PWTimeout:
                            pass
                    except _PWTimeout:
                        _dump_page_debug(page, debug, 'password_timeout')
                        if page.query_selector('form#signin-with-otp-form'):
                            _write_status(r, source_id, 'failed',
                                          'This Amazon account signs in without a password. '
                                          'Clear the saved password to sign in with a texted code.')
                        else:
                            _write_status(r, source_id, 'failed',
                                          'Password field not found.')
                        return

                    if _is_captcha_page(page):
                        _write_status(r, source_id, 'captcha',
                                      'Amazon served a CAPTCHA after password entry.')
                        return
                else:
                    _write_status(r, source_id, 'running', 'Asking Amazon to text a sign-in code…')
                    try:
                        page.wait_for_selector(
                            'form#signin-with-otp-form, input[type="password"]:visible, '
                            + _OTP_INPUT_SELECTOR,
                            timeout=15000,
                        )
                    except _PWTimeout:
                        pass
                    # Amazon sometimes texts the code as soon as the email is
                    # entered and goes straight to the code page.
                    how = 'already sent' if _is_otp_page(page) else page.evaluate(_CODE_SIGNIN_JS)
                    if how == 'none':
                        _dump_page_debug(page, debug, 'no_code_signin')
                        _write_status(r, source_id, 'failed',
                                      'Amazon did not offer sign-in with a code for this account. '
                                      'Save your Amazon password, or turn on passwordless sign-in '
                                      'in your Amazon account.')
                        return
                    logger.info('[amazon-auth] requested a sign-in code via %s', how)
                    try:
                        page.wait_for_load_state('domcontentloaded', timeout=10000)
                    except _PWTimeout:
                        pass

                    if _is_captcha_page(page):
                        _write_status(r, source_id, 'captcha',
                                      'Amazon served a CAPTCHA after requesting a code.')
                        return

                # ── OTP / 2FA ──────────────────────────────────────────────
                # Amazon's CVF redirect may arrive after the password form's
                # navigation event, so wait for either a signed-in page or a
                # challenge instead of checking the page only once.
                login_outcome = _wait_for_login_outcome(page)
                try:
                    stuck = bool(password) and login_outcome == 'pending' and bool(
                        page.query_selector('input[type="password"]:visible'))
                except Exception:
                    stuck = False
                if stuck:
                    # Still on the password form with no error: the submit
                    # never registered.  Retry once with a real Enter keypress.
                    logger.info('[amazon-auth] password form still showing, retrying with Enter')
                    try:
                        page.focus('input[type="password"]:visible', timeout=3000)
                        page.keyboard.press('Enter')
                    except Exception:
                        pass
                    login_outcome = _wait_for_login_outcome(page)
                if login_outcome == 'otp':
                    logger.info('[amazon-auth] OTP challenge detected for source_id=%s', source_id)
                    _log_step(page, 'otp_challenge')
                    _write_status(r, source_id, 'waiting_otp',
                                  'Enter the one-time code Amazon sent to your phone or email.')
                    otp = _wait_for_otp(r, source_id)
                    if not otp:
                        _write_status(r, source_id, 'failed',
                                      'OTP not submitted within 5 minutes — login cancelled.')
                        return

                    _write_status(r, source_id, 'running', 'Submitting OTP…')
                    try:
                        page.wait_for_selector(_OTP_INPUT_SELECTOR, timeout=8000)
                        page.fill(_OTP_INPUT_SELECTOR, otp.strip())
                        _log_step(page, 'otp_ready_to_submit')
                        submit_selector = (
                            'input#auth-signin-button, '
                            'input#cvf-submit-otp-button, '
                            'input[type="submit"]'
                        )
                        page.click(submit_selector, timeout=8000)
                        _log_step(page, 'otp_submitted')
                    except _PWTimeout:
                        _dump_page_debug(page, debug, 'otp_submit_timeout')
                        _write_status(r, source_id, 'failed', 'OTP submission timed out.')
                        return

                    _write_status(r, source_id, 'running',
                                  'Waiting for Amazon to confirm the code or app approval…')
                    login_outcome = _wait_for_otp_submission(page)
                    if login_outcome != 'signed_in':
                        _dump_page_debug(page, debug, 'otp_confirmation_pending')

                # ── Verify success ─────────────────────────────────────────
                if login_outcome != 'signed_in':
                    final_url = page.url
                    _dump_page_debug(page, debug, 'verify_failed')
                    auth_error = _auth_error_text(page)
                    if _is_captcha_page(page):
                        _write_status(r, source_id, 'captcha',
                                      'Amazon served a CAPTCHA during login. Please paste cookies manually.')
                    elif auth_error:
                        logger.warning('[amazon-auth] Amazon rejected sign-in: %s', auth_error)
                        _write_status(r, source_id, 'failed', f'Amazon said: {auth_error}')
                    else:
                        _write_status(r, source_id, 'failed',
                                      f'Login did not succeed (landed on: {final_url[:80]})')
                    logger.warning('[amazon-auth] login failed, final URL: %s', page.url)
                    return

                logger.info('[amazon-auth] login successful for source_id=%s', source_id)
                _save_result(r, source_id, context)
                _write_status(r, source_id, 'success', 'Logged in — cookies saved.')

            finally:
                browser.close()

    except Exception as exc:
        logger.exception('[amazon-auth] unexpected error for source_id=%s: %s', source_id, exc)
        _write_status(r, source_id, 'failed', f'Unexpected error: {type(exc).__name__}: {exc}')

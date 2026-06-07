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
_RESULT_TTL = 120   # 2 min for the caller to consume
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
            page.query_selector('input[name="otpCode"]') or
            page.query_selector('input#auth-mfa-otpcode') or
            page.query_selector('form#auth-mfa-form') or
            page.query_selector('input#cvf-input-code')
        )
    except Exception:
        return False


def _is_signed_in(page) -> bool:
    url = page.url
    for blocked in ('/ap/signin', '/ap/mfa', '/ap/cvf', '/errors/validateCaptcha'):
        if blocked in url:
            return False
    if 'amazon.com' not in url:
        return False
    try:
        el = page.query_selector('#nav-link-accountList-nav-line-1')
        if el:
            text = (el.inner_text() or '').lower().strip()
            return 'sign in' not in text
        # Fallback: presence of sign-out link
        return bool(page.query_selector('#nav-item-signout, a[href*="/gp/sign-out"]'))
    except Exception:
        return True  # URL check already passed, assume ok


# ── Debug helpers ─────────────────────────────────────────────────────────────

def _dump_page_debug(page, label: str) -> None:
    """Log URL, title, visible inputs, and save a screenshot — all best-effort."""
    try:
        logger.info('[amazon-auth] DEBUG %s — url: %s', label, page.url)
        logger.info('[amazon-auth] DEBUG %s — title: %s', label, page.title())
        inputs = page.query_selector_all('input')
        input_info = [(el.get_attribute('name'), el.get_attribute('id'), el.get_attribute('type'))
                      for el in inputs]
        logger.info('[amazon-auth] DEBUG %s — inputs: %s', label, input_info)
        # First 800 chars of body text to see what Amazon returned
        body = (page.inner_text('body') or '')[:800].replace('\n', ' ')
        logger.info('[amazon-auth] DEBUG %s — body: %s', label, body)
        path = f'/tmp/amazon_auth_{label.replace(" ", "_")}.png'
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
) -> None:
    """
    Drive a Playwright browser through Amazon's sign-in flow.
    Status updates land in Redis; the Flask status endpoint reads them.
    On success, writes cookie_header + browser_storage_state to Redis for
    the Flask endpoint to consume and persist into source.config.
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
                        _dump_page_debug(page, 'after_botcheck')
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
                    page.wait_for_selector('input[name="email"]', timeout=15000)
                    page.evaluate(
                        "(v) => { const el = document.querySelector('input[name=\"email\"]'); "
                        "if (el) el.value = v; }",
                        email,
                    )
                    # JS-click the submit/continue button — bypasses actionability timeouts
                    page.evaluate(
                        "() => { const b = document.querySelector("
                        "'input#continue, input[type=\"submit\"], button[type=\"submit\"]'); "
                        "if (b) b.click(); }"
                    )
                    # Wait for password field to appear (SPA reveal or page nav)
                    try:
                        page.wait_for_load_state('domcontentloaded', timeout=8000)
                    except _PWTimeout:
                        pass
                except _PWTimeout:
                    _dump_page_debug(page, 'email_timeout')
                    _write_status(r, source_id, 'failed',
                                  'Email field not found on sign-in page.')
                    return

                if _is_captcha_page(page):
                    _write_status(r, source_id, 'captcha',
                                  'Amazon served a CAPTCHA after email entry.')
                    return

                # ── Password ───────────────────────────────────────────────
                _write_status(r, source_id, 'running', 'Entering password…')
                try:
                    page.wait_for_selector(
                        'input[name="password"]:visible, input[type="password"]:visible',
                        timeout=15000,
                    )
                    page.evaluate(
                        "(v) => { const el = document.querySelector("
                        "'input[name=\"password\"], input[type=\"password\"]'); "
                        "if (el) el.value = v; }",
                        password,
                    )
                    page.evaluate(
                        "() => { const b = document.querySelector("
                        "'input#signInSubmit, input[type=\"submit\"], button[type=\"submit\"]'); "
                        "if (b) b.click(); }"
                    )
                    try:
                        page.wait_for_load_state('domcontentloaded', timeout=10000)
                    except _PWTimeout:
                        pass
                except _PWTimeout:
                    _dump_page_debug(page, 'password_timeout')
                    _write_status(r, source_id, 'failed',
                                  'Password field not found.')
                    return

                if _is_captcha_page(page):
                    _write_status(r, source_id, 'captcha',
                                  'Amazon served a CAPTCHA after password entry.')
                    return

                # ── OTP / 2FA ──────────────────────────────────────────────
                if _is_otp_page(page):
                    logger.info('[amazon-auth] OTP challenge detected for source_id=%s', source_id)
                    _write_status(r, source_id, 'waiting_otp',
                                  'Enter the one-time code Amazon sent to your phone or email.')
                    otp = _wait_for_otp(r, source_id)
                    if not otp:
                        _write_status(r, source_id, 'failed',
                                      'OTP not submitted within 5 minutes — login cancelled.')
                        return

                    _write_status(r, source_id, 'running', 'Submitting OTP…')
                    try:
                        otp_selector = (
                            'input[name="otpCode"], '
                            'input#auth-mfa-otpcode, '
                            'input#cvf-input-code'
                        )
                        page.wait_for_selector(otp_selector, timeout=8000)
                        page.fill(otp_selector, otp.strip())
                        submit_selector = (
                            'input#auth-signin-button, '
                            'input#cvf-submit-otp-button, '
                            'input[type="submit"]'
                        )
                        with page.expect_navigation(wait_until='domcontentloaded', timeout=15000):
                            page.click(submit_selector)
                    except _PWTimeout:
                        _write_status(r, source_id, 'failed', 'OTP submission timed out.')
                        return

                # ── Verify success ─────────────────────────────────────────
                if not _is_signed_in(page):
                    final_url = page.url
                    if _is_captcha_page(page):
                        _write_status(r, source_id, 'captcha',
                                      'Amazon served a CAPTCHA during login. Please paste cookies manually.')
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

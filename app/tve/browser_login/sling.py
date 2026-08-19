"""Sling TV browser-assisted login."""
import logging
import time
from datetime import datetime, timezone
from rq import get_current_job
import redis

from app.worker import flask_app
from app.models import Source
from app.scrapers import registry
from app.config_store import persist_source_config_updates
from app.tve.browser_login.common import (
    _safe_page_url,
    _apply_sling_browser_login_input,
)

logger = logging.getLogger(__name__)


SLING_BROWSER_LOGIN_STATUS_KEY = 'sling:browser-login:status'
SLING_BROWSER_LOGIN_SHOT_KEY = 'sling:browser-login:screenshot'
SLING_BROWSER_LOGIN_INPUT_KEY = 'sling:browser-login:input'
SLING_BROWSER_LOGIN_STOP_KEY = 'sling:browser-login:stop'
_SLING_BROWSER_LOGIN_TIMEOUT_SECONDS = 1800


def _autofill_sling_credentials(page, username: str, password: str) -> None:
    """Fill and submit the saved username/password so the human only has to
    solve the captcha, not retype credentials under a live challenge. Not
    security-relevant - hCaptcha challenges the session either way, before
    or after this runs, so this doesn't change what a human still has to do."""
    for _ in range(30):
        page.wait_for_timeout(1000)
        if page.locator('input').count() >= 2:
            break
    page.wait_for_timeout(1000)

    inputs = page.locator('input')
    email_idx = None
    for i in range(min(inputs.count(), 6)):
        typ = (inputs.nth(i).get_attribute('type') or '').lower()
        name = (inputs.nth(i).get_attribute('name') or '').lower()
        placeholder = (inputs.nth(i).get_attribute('placeholder') or '').lower()
        if typ in ('email', 'text') and ('email' in name or 'email' in placeholder or 'user' in name):
            email_idx = i
            break
    if email_idx is None and inputs.count() >= 2:
        email_idx = 0
    if email_idx is None:
        raise RuntimeError('could not find the email input')

    inputs.nth(email_idx).click()
    page.keyboard.type(username, delay=50)
    pw_idx = email_idx + 1
    if pw_idx >= inputs.count():
        raise RuntimeError('could not find the password input')
    inputs.nth(pw_idx).click()
    page.keyboard.type(password, delay=55)
    page.wait_for_timeout(400)
    inputs.nth(pw_idx).press('Enter')


def _extract_sling_oauth_tokens(page) -> tuple[str | None, str | None]:
    """Sling's SPA persists OAuth1 credentials somewhere under localStorage
    (documented at DevTools -> Application -> Local Storage -> 'persist:root'
    -> user -> userData -> oauth_token/oauth_token_secret). Search recursively
    rather than assuming that exact nesting, since it's redux-persist-shaped
    JSON-strings-within-JSON and the exact structure isn't guaranteed stable
    across app versions."""
    found = page.evaluate("""() => {
        function tryParse(v) { try { return JSON.parse(v); } catch (e) { return v; } }
        function deepFind(obj, keys, found, seen) {
            seen = seen || new Set();
            if (obj == null || typeof obj !== 'object' || seen.has(obj)) return;
            seen.add(obj);
            for (const k in obj) {
                if (keys.includes(k) && typeof obj[k] === 'string' && obj[k]) {
                    found[k] = obj[k];
                } else if (typeof obj[k] === 'object') {
                    deepFind(obj[k], keys, found, seen);
                } else if (typeof obj[k] === 'string') {
                    const parsed = tryParse(obj[k]);
                    if (parsed && typeof parsed === 'object') deepFind(parsed, keys, found, seen);
                }
            }
        }
        const found = {};
        const wanted = ['oauth_token', 'oauth_token_secret'];
        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            const raw = localStorage.getItem(key);
            const parsed = tryParse(raw);
            if (parsed && typeof parsed === 'object') deepFind(parsed, wanted, found);
            else if (wanted.includes(key)) found[key] = raw;
        }
        return found;
    }""")
    return found.get('oauth_token'), found.get('oauth_token_secret')


def run_sling_browser_login():
    """Drive a real, human-operated sign-in to sling.com.

    A Camoufox (anti-detect Firefox) tab auto-fills the saved credentials and
    loads the real sign-in page. The admin UI streams periodic screenshots of
    it and forwards the admin's own clicks/keystrokes back to the page (via
    Redis, since screenshots/input may be served by a different gunicorn
    worker than the one running this job) — so the human sees the real page
    and solves the real hCaptcha challenge themselves. The OAuth token is
    captured either straight from the auth-callback URL (primary path) or
    from localStorage as a fallback, and saved to the source config.
    """
    with flask_app.app_context():
        import json as _json_login

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[sling-login] Redis unavailable, aborting: %s', exc)
            return

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    SLING_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': url}),
                )
            except Exception:  # noqa: BLE001
                pass

        r.delete(SLING_BROWSER_LOGIN_STOP_KEY)
        r.delete(SLING_BROWSER_LOGIN_INPUT_KEY)
        set_status('starting', 'Launching browser…')

        source = Source.query.filter_by(name='sling').first()
        if not source:
            set_status('error', 'Sling source not found')
            return

        scraper_cls = registry.get('sling')
        if not scraper_cls:
            set_status('error', 'Sling scraper not registered')
            return
        scraper = scraper_cls(config=dict(source.config or {}))

        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            set_status('error', 'Camoufox is not installed on this container')
            return

        profile_dir = '/data/browser_profiles/sling'
        try:
            import os as _os_login
            _os_login.makedirs(profile_dir, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[sling-login] could not create profile dir %s: %s', profile_dir, exc)

        try:
            # Camoufox (anti-detect Firefox) instead of stock Playwright Chromium:
            # fingerprint spoofing (WebGL, navigator, etc.) happens in the compiled
            # browser itself rather than via JS patches layered on top, and it's
            # Firefox-based so it isn't subject to CDP's Runtime.enable leak at all.
            # headless='virtual' runs a real (non-headless) Firefox against an
            # internal Xvfb display - plain headless=True disables WebGL entirely.
            # A persistent profile (not a fresh context every run) so cookies and
            # device-trust state accumulate across attempts instead of presenting
            # as a never-before-seen browser each time.
            with Camoufox(
                headless='virtual',
                os='windows',
                persistent_context=True,
                user_data_dir=profile_dir,
                window=(1280, 800),
            ) as context:
                page = context.pages[0] if context.pages else context.new_page()
                page.on('crash', lambda p: logger.warning('[sling-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
                page.on('close', lambda p: logger.warning('[sling-login] page CLOSE event fired'))
                page.on('pageerror', lambda exc: logger.warning('[sling-login] page JS error: %s', str(exc)[:500]))
                try:
                    page.goto('https://www.sling.com/sign-in', wait_until='domcontentloaded', timeout=30000)
                except Exception as exc:  # noqa: BLE001
                    set_status('error', f'Failed to load sign-in page: {exc}')
                    return

                _sling_username = (source.config or {}).get('username', '').strip()
                _sling_password = (source.config or {}).get('password', '').strip()
                if _sling_username and _sling_password:
                    try:
                        _autofill_sling_credentials(page, _sling_username, _sling_password)
                        set_status('running', 'Solve the captcha below if shown.', page.url)
                    except Exception as exc:  # noqa: BLE001
                        # Not fatal - the human can still fill the form manually from here.
                        logger.info('[sling-login] credential autofill failed, falling back to manual: %s', exc)
                        set_status('running', 'Sign in below, including the captcha if shown.', page.url)
                else:
                    set_status('running', 'Sign in below, including the captcha if shown.', page.url)

                deadline = time.monotonic() + _SLING_BROWSER_LOGIN_TIMEOUT_SECONDS
                last_shot = 0.0
                last_heartbeat = 0.0
                consecutive_failures = 0
                # RQ only calls job.heartbeat() once at start and once at completion -
                # never during execution (confirmed against the installed rq==1.16.2
                # source). This job can legitimately run up to
                # _SLING_BROWSER_LOGIN_TIMEOUT_SECONDS, but _job_already_active()'s
                # staleness check (tasks.py) treats ANY job with no heartbeat for 300s
                # as a dead zombie and deletes it - which would let a second click
                # launch a duplicate Camoufox instance against the same locked
                # persistent profile dir while this one is still genuinely running.
                # Self-heartbeat periodically so RQ's own bookkeeping doesn't disagree
                # with reality.
                current_job = get_current_job()
                # A dead/crashed page makes every call below fail. Each one is caught
                # individually (a single transient hiccup on one call shouldn't kill
                # the session) - but if a screenshot attempt fails this many times in
                # a row, the page is gone, not just having a bad moment. Bail out with
                # a real error instead of silently looping against a dead browser for
                # the full deadline, which is what happened before this fix: nothing
                # ever raised past the loop, so it just spun uselessly for 30 minutes.
                _MAX_CONSECUTIVE_FAILURES = 15
                while time.monotonic() < deadline:
                    if r.exists(SLING_BROWSER_LOGIN_STOP_KEY):
                        set_status('stopped', 'Cancelled')
                        return

                    if page.is_closed():
                        set_status('error', 'Browser page closed unexpectedly.')
                        return

                    for _ in range(20):
                        raw = r.lpop(SLING_BROWSER_LOGIN_INPUT_KEY)
                        if raw is None:
                            break
                        try:
                            _apply_sling_browser_login_input(page, _json_login.loads(raw))
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[sling-login] input apply failed: %s', exc)

                    now = time.monotonic()
                    if current_job and now - last_heartbeat > 60:
                        try:
                            current_job.heartbeat(datetime.now(timezone.utc), 180)
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[sling-login] self-heartbeat failed: %s', exc)
                        last_heartbeat = now

                    if now - last_shot > 0.25:
                        try:
                            shot = page.screenshot(type='jpeg', quality=60)
                            r.setex(SLING_BROWSER_LOGIN_SHOT_KEY, 30, shot)
                            set_status('running', 'Sign in below, including the captcha if shown.', page.url)
                            consecutive_failures = 0
                        except Exception as exc:  # noqa: BLE001
                            consecutive_failures += 1
                            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                                logger.warning('[sling-login] page unresponsive after %d failed screenshots: %s', consecutive_failures, exc)
                                set_status('error', f'Browser session died: {exc}')
                                return
                        last_shot = now

                    # The SPA lands here on success (.../sign-in/auth-callback?transient_token=...)
                    # then does its OWN in-page exchange and closes itself shortly after - which
                    # we've seen fail with "NetworkError when attempting to fetch resource" before
                    # closing, seemingly unrelated to the sign-in itself. Grab the token straight
                    # from the URL the moment it appears and exchange it ourselves via plain HTTP
                    # (the same code path the manual "paste this URL" fallback already uses) rather
                    # than depend on the in-page fetch succeeding or the page surviving long enough
                    # for localStorage to populate.
                    if 'transient_token=' in page.url:
                        raw_token = page.url.split('transient_token=', 1)[1].split('&', 1)[0].strip()
                        if raw_token:
                            try:
                                scraper._exchange_browser_auth_token(raw_token)
                                persist_source_config_updates(source.id, scraper._pending_config_updates)
                                set_status('success', 'Signed in — OAuth token saved.')
                                logger.info('[sling-login] exchanged transient_token captured from URL')
                                return
                            except Exception as exc:  # noqa: BLE001
                                logger.warning('[sling-login] transient_token exchange failed: %s', exc)
                                set_status('error', f'Sign-in succeeded but token exchange failed: {exc}')
                                return

                    try:
                        token, secret = _extract_sling_oauth_tokens(page)
                    except Exception:  # noqa: BLE001
                        token, secret = None, None
                    if token and secret:
                        persist_source_config_updates(source.id, {
                            'oauth_token': token,
                            'oauth_token_secret': secret,
                            'oauth_token_time': int(time.time()),
                            'browser_auth_token': '',
                        })
                        set_status('success', 'Signed in — OAuth token saved.')
                        logger.info('[sling-login] captured and saved OAuth token from localStorage')
                        return

                    page.wait_for_timeout(80)

                set_status('error', 'Timed out waiting for sign-in to complete.')
                return
        except BaseException as exc:  # noqa: BLE001
            # BaseException, not Exception: two prior runs vanished silently
            # (RQ 'finished', not 'failed'; nothing logged) after the browser
            # process died mid-session - whatever that was didn't surface as a
            # plain Exception here, so widen the net to actually see it next time.
            logger.exception('[sling-login] browser session failed')
            try:
                set_status('error', f'Browser session failed: {exc}')
            except Exception:  # noqa: BLE001
                pass
            return

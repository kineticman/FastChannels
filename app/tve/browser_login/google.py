"""Standalone "Sign in with Google" browser-assisted flow."""
import logging
import time
import redis

from app.worker import flask_app
from app.models import TVEAccount
from app.tve.browser_login.common import (
    _safe_page_url,
    _GOOGLE_SETUP_URL,
    _exchange_and_save_google_master_token,
    _relay_input_and_screenshot,
    _BrowserSessionDied,
)

logger = logging.getLogger(__name__)


GOOGLE_SIGNIN_STATUS_KEY = 'google-signin:browser-login:status'
GOOGLE_SIGNIN_SHOT_KEY = 'google-signin:browser-login:screenshot'
GOOGLE_SIGNIN_INPUT_KEY = 'google-signin:browser-login:input'
GOOGLE_SIGNIN_STOP_KEY = 'google-signin:browser-login:stop'
GOOGLE_SIGNIN_HINT_KEY = 'google-signin:browser-login:hint'
_GOOGLE_SIGNIN_TIMEOUT_SECONDS = 600


def run_google_signin():
    """Standalone "Sign in with Google" — captures a Google master_token
    directly against Google's own embedded Android device-setup page
    (_GOOGLE_SETUP_URL), completely independent of Adobe Pass/any specific
    TVE network's SAML flow.

    Why this exists as its own dedicated entry point rather than relying
    solely on _maybe_capture_google_master_token's opportunistic piggyback
    on a successful per-network login (2026-08-19): that's a chicken-and-egg
    problem when Adobe/YouTubeTV's own SAML bounce chain
    (.../saml/module.php/authbypass/firstbookend.php and friends) is
    unhealthy — confirmed live the same day via a rigorous multi-signal
    bisection (network/console event logging, not just screenshots) that
    Adobe's side can simply stop completing that chain entirely, with zero
    automation involvement, so no per-network login ever succeeds long
    enough to piggyback a capture on. This flow never touches Adobe at all,
    so it's unaffected by whatever's wrong over there — once a master_token
    is on file this way, _prime_google_session (already wired into all six
    MVPD browser-login flows) injects a fresh Google session into every one
    of them before they navigate, and each stops needing this dance
    entirely.

    Reuses the SAME shared /data/browser_profiles/mvpd_tve profile as every
    other MVPD browser-login (same profile-busy gating in
    app.routes.tasks — only one Camoufox instance can hold it at a time),
    but its own dedicated GOOGLE_SIGNIN_* redis keys/modal namespace, so a
    human can run this independent of any specific network's login attempt.
    """
    _ctx = flask_app.app_context()
    _ctx.push()
    _ctx_popped = {'v': False}
    try:
        import json as _json_login

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[google-signin] Redis unavailable, aborting: %s', exc)
            return

        _terminal_status_set = {'v': False}

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    GOOGLE_SIGNIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': url}),
                )
                if state in ('success', 'error', 'stopped'):
                    _terminal_status_set['v'] = True
            except Exception:  # noqa: BLE001
                pass

        r.delete(GOOGLE_SIGNIN_STOP_KEY)
        r.delete(GOOGLE_SIGNIN_INPUT_KEY)
        set_status('starting', 'Launching browser…')

        from app.tve import adobe_pass
        account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if account_row is None:
            set_status('error', 'TVE account not found — save TVE settings first.')
            return
        already = adobe_pass.load_google_master_token(account_row)
        if already:
            set_status('success', f"Already signed in as {already.get('email', '?')}. Sign in again only if this stops working.")
            return

        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            set_status('error', 'Camoufox is not installed on this container')
            return

        profile_dir = '/data/browser_profiles/mvpd_tve'
        try:
            import os as _os_login
            _os_login.makedirs(profile_dir, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[google-signin] could not create profile dir %s: %s', profile_dir, exc)

        _ctx.pop()
        _ctx_popped['v'] = True
        deadline = time.monotonic() + _GOOGLE_SIGNIN_TIMEOUT_SECONDS
        oauth_token = None
        try:
            with Camoufox(
                headless='virtual', os='windows', persistent_context=True,
                user_data_dir=profile_dir, window=(1280, 800),
            ) as context:
                page = context.pages[0] if context.pages else context.new_page()
                page.on('crash', lambda p: logger.warning('[google-signin] page CRASH event fired (url was %s)', _safe_page_url(p)))
                page.on('close', lambda p: logger.warning('[google-signin] page CLOSE event fired'))
                page.on('pageerror', lambda exc: logger.warning('[google-signin] page JS error: %s', str(exc)[:500]))

                page.goto(_GOOGLE_SETUP_URL, wait_until='domcontentloaded', timeout=30000)
                set_status('running', 'Sign in below, including any 2FA/captcha if shown.', page.url)

                wait_started = time.monotonic()
                last_shot = 0.0
                while time.monotonic() < deadline:
                    if r.exists(GOOGLE_SIGNIN_STOP_KEY):
                        set_status('stopped', 'Cancelled')
                        return
                    if page.is_closed():
                        raise _BrowserSessionDied('browser page closed before sign-in completed')
                    now = time.monotonic()
                    if now - last_shot > 0.25:
                        last_shot = now
                        if _relay_input_and_screenshot(
                            page, r, waiting_since=wait_started,
                            stop_key=GOOGLE_SIGNIN_STOP_KEY, input_key=GOOGLE_SIGNIN_INPUT_KEY,
                            shot_key=GOOGLE_SIGNIN_SHOT_KEY, hint_key=GOOGLE_SIGNIN_HINT_KEY,
                        ):
                            set_status('stopped', 'Cancelled')
                            return
                    try:
                        cookies = context.cookies()
                        oauth_token = next((c['value'] for c in cookies if c['name'] == 'oauth_token'), None)
                    except Exception:  # noqa: BLE001
                        oauth_token = None
                    if oauth_token:
                        break
                    page.wait_for_timeout(200)
        except BaseException as exc:  # noqa: BLE001
            if _terminal_status_set['v']:
                logger.info('[google-signin] ignoring cleanup-time exception after terminal status was already set: %s', exc)
                return
            if r.exists(GOOGLE_SIGNIN_STOP_KEY):
                set_status('stopped', 'Cancelled')
                return
            logger.exception('[google-signin] browser session failed')
            set_status('error', f'Browser session failed: {exc}')
            return

        if not oauth_token:
            set_status('error', 'Timed out waiting for sign-in to complete.')
            return

        set_status('running', 'Verifying with Google…')
        data = _exchange_and_save_google_master_token(oauth_token)
        if not data:
            set_status('error', 'Google accepted the sign-in but the token exchange failed — see server logs.')
            return
        set_status('success', f"Signed in as {data.get('email', '?')}. Every YouTubeTV network can now sign in silently.")
    finally:
        if not _ctx_popped['v']:
            _ctx.pop()

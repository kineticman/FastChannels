"""Standalone Spectrum sign-in — validates login against Spectrum's reCAPTCHA
Enterprise + ThreatMetrix gate and harvests the OAuth token pair needed to
drive the Spectrum scraper.

A human drives the actual credential entry via the relayed screenshot exactly
like every other MVPD flow in this package — credentials are typed directly
into the live browser view rendered in the admin UI, never passed through any
other layer of this app.

Reuses the SAME shared /data/browser_profiles/mvpd_tve Camoufox profile as
every other MVPD browser-login (same profile-busy gating in app.routes.tasks)
rather than a fresh one — deliberately: Spectrum's login blocked a genuinely
fresh/incognito device outright (403 AUTH_REJECT_BY_RECAPTCHA_PASS_THMX_REJECT_
STATUS) while a device with real prior history passed, so reusing this
already-established, long-lived profile is closer to the case that worked than
starting clean would be.
"""
from __future__ import annotations

import json
import logging
import time

import redis

from app.worker import flask_app
from app.tve.browser_login.common import (
    _safe_page_url,
    _relay_input_and_screenshot,
    _try_autofill_credentials,
    _BrowserSessionDied,
    install_browser_login_activity_log,
    uninstall_browser_login_activity_log,
)

logger = logging.getLogger(__name__)

SPECTRUM_SIGNIN_STATUS_KEY = 'spectrum:browser-login:status'
SPECTRUM_SIGNIN_SHOT_KEY = 'spectrum:browser-login:screenshot'
SPECTRUM_SIGNIN_INPUT_KEY = 'spectrum:browser-login:input'
SPECTRUM_SIGNIN_STOP_KEY = 'spectrum:browser-login:stop'
SPECTRUM_SIGNIN_HINT_KEY = 'spectrum:browser-login:hint'
_SPECTRUM_SIGNIN_TIMEOUT_SECONDS = 600

_START_URL = 'https://watch.spectrum.net/guide'


def run_spectrum_signin():
    _ctx = flask_app.app_context()
    _ctx.push()
    _ctx_popped = {'v': False}
    _activity_handler = None
    try:
        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[spectrum-signin] Redis unavailable, aborting: %s', exc)
            return
        _activity_handler = install_browser_login_activity_log(r)

        _terminal_status_set = {'v': False}

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    SPECTRUM_SIGNIN_STATUS_KEY, 120,
                    json.dumps({'state': state, 'message': message, 'url': url}),
                )
                if state in ('success', 'error', 'stopped'):
                    _terminal_status_set['v'] = True
            except Exception:  # noqa: BLE001
                pass

        r.delete(SPECTRUM_SIGNIN_STOP_KEY)
        r.delete(SPECTRUM_SIGNIN_INPUT_KEY)
        set_status('starting', 'Launching browser…')

        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            set_status('error', 'Camoufox is not installed on this container')
            return

        from app.models import Source
        source = Source.query.filter_by(name='spectrum').first()
        saved_cfg = source.config or {} if source else {}
        username = saved_cfg.get('username')
        password = saved_cfg.get('password')

        profile_dir = '/data/browser_profiles/mvpd_tve'
        try:
            import os as _os_login
            _os_login.makedirs(profile_dir, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[spectrum-signin] could not create profile dir %s: %s', profile_dir, exc)

        _ctx.pop()
        _ctx_popped['v'] = True
        deadline = time.monotonic() + _SPECTRUM_SIGNIN_TIMEOUT_SECONDS
        captured: dict = {}
        _rejected: set = set()  # access_token values already proven dead — never re-try one
        _last_mismatch: dict = {}  # {'account': str} — last valid-but-wrong-account
        # identity seen, so a final timeout can say WHY instead of a generic
        # "timed out" that looks identical to every other stuck sign-in.

        # Response interception is primary (proven reliable across multiple live
        # runs); localStorage is a fallback for the case SSO carries over via the
        # persistent profile's cookies with no token network call at all.
        # CRITICAL: confirmed live 2026-09-17 that either source can hand back a
        # token that LOOKS right (correct JWE shape, plausible length) but is
        # already dead server-side (isValidSession:false) — so nothing here is
        # trusted until verified with a real validateSession call from inside
        # the page itself. Only a verified token is ever saved or reported as
        # success; an unverified one is discarded and the wait continues.
        #
        # A VALID token isn't enough either — forum reports (2026-09-22) of users
        # getting "signed in successfully" with no login form ever shown, and of
        # the login form flashing then vanishing before they could pick an
        # account, point at Spectrum silently authenticating the browser as some
        # OTHER account: either stale cookie SSO carried over in this shared
        # persistent profile, or (for anyone actually browsing from a Spectrum
        # residential IP) Charter's own network-based auto-auth, neither of which
        # has anything to do with the username/password saved in config. When a
        # username IS configured, the captured identity (localStorage's
        # xoauth_username, which the page itself sets on any successful auth —
        # cookie-carried-over, network-auto-authed, or freshly typed) must match
        # it before a candidate is accepted, or we silently save someone else's
        # session under this install's config.
        def _on_response(response):
            try:
                if (
                    response.request.method == 'POST'
                    and response.ok
                    and 'json' in (response.headers.get('content-type') or '')
                ):
                    data = response.json()
                    if isinstance(data, dict) and data.get('access_token'):
                        captured['candidate'] = {
                            'oauth_token': data['access_token'],
                            'xoauth_refresh_token': data.get('refresh_token'),
                            'device_id': (
                                response.request.headers.get('x-client-device-id')
                                or response.request.headers.get('device_id')
                            ),
                            'xoauth_device_verifier': data.get('deviceVerifier'),
                        }
                        # Use the same absolute millisecond expiry as localStorage.
                        # The intercepted response wins over that fallback, so its
                        # expires_in must survive through save_login_result().
                        try:
                            ttl = int(data.get('expires_in') or 0)
                            if ttl > 0:
                                captured['candidate']['xoauth_token_expiration'] = int(
                                    (time.time() + ttl) * 1000)
                        except (TypeError, ValueError, OverflowError):
                            pass
            except Exception as exc:  # noqa: BLE001
                logger.debug('[spectrum-signin] response capture failed for %s: %s', response.url, exc)

        _LS_JS = (
            "() => ({"
            "oauth_token: localStorage.getItem('oauth_token'),"
            "xoauth_refresh_token: localStorage.getItem('xoauth_refresh_token'),"
            "device_id: localStorage.getItem('device_id'),"
            "xoauth_device_verifier: localStorage.getItem('xoauth_device_verifier'),"
            "xoauth_username: localStorage.getItem('xoauth_username'),"
            "xoauth_token_expiration: localStorage.getItem('xoauth_token_expiration')"
            "})"
        )

        _VALIDATE_JS = (
            "async (t) => {"
            "  const resp = await fetch('https://apis.spectrum.net/auth/oauth/v2/validateSession?getLocation=false', {"
            "    headers: {"
            "      'x-access-token': t.oauth_token,"
            "      'x-refresh-token': t.xoauth_refresh_token || '',"
            "      'x-client-device-id': t.device_id || '',"
            "      'x-client-id': 'stva-ovp',"
            "    }"
            "  });"
            "  if (!resp.ok) return false;"
            "  const body = await resp.json();"
            "  return body.isValidSession === true;"
            "}"
        )

        def _try_capture(page) -> bool:
            if 'verified' in captured:
                return True
            candidate = captured.get('candidate')
            if not candidate:
                try:
                    ls = page.evaluate(_LS_JS)
                except Exception as exc:  # noqa: BLE001
                    logger.debug('[spectrum-signin] localStorage read failed: %s', exc)
                    ls = None
                if ls and ls.get('oauth_token'):
                    candidate = ls
            if not candidate or not candidate.get('oauth_token'):
                return False
            if candidate['oauth_token'] in _rejected:
                return False
            try:
                is_valid = page.evaluate(_VALIDATE_JS, candidate)
            except Exception as exc:  # noqa: BLE001
                logger.debug('[spectrum-signin] validateSession check failed: %s', exc)
                return False
            if not is_valid:
                logger.info('[spectrum-signin] captured token failed validateSession — discarding, still waiting')
                _rejected.add(candidate['oauth_token'])
                captured.pop('candidate', None)
                return False
            if username:
                try:
                    captured_username = page.evaluate("() => localStorage.getItem('xoauth_username')") or ''
                except Exception as exc:  # noqa: BLE001
                    logger.debug('[spectrum-signin] xoauth_username read failed: %s', exc)
                    captured_username = ''
                captured_username = captured_username.strip()
                # Confirmed live 2026-09-22: the page stores this value
                # JSON-stringified (literal surrounding quotes included), not as
                # a bare string — compare against the *unwrapped* value or a
                # correctly-configured username falsely mismatches its own
                # quoted echo every time.
                if len(captured_username) >= 2 and captured_username[0] == '"' and captured_username[-1] == '"':
                    try:
                        captured_username = json.loads(captured_username)
                    except (ValueError, TypeError):
                        captured_username = captured_username[1:-1]
                # Only reject on a CONFIRMED mismatch — an empty read means we
                # can't tell (e.g. this build of the page doesn't set it) and
                # shouldn't block an otherwise-valid session over missing data.
                if captured_username.strip() and captured_username.strip().lower() != username.strip().lower():
                    logger.warning(
                        '[spectrum-signin] captured a VALID session for account %r, '
                        'but %r is configured — this is SSO/network-auto-auth carryover '
                        'for a different account, not this install\'s login; discarding '
                        'and forcing a fresh sign-in', captured_username, username)
                    _last_mismatch['account'] = captured_username
                    _rejected.add(candidate['oauth_token'])
                    captured.pop('candidate', None)
                    set_status(
                        'running',
                        'Signed-in account doesn\'t match the configured username — '
                        'forcing a fresh sign-in…', page.url)
                    return False
            captured['verified'] = candidate
            logger.info('[spectrum-signin] captured and verified a working token')
            return True

        cox_cookies: list = []
        try:
            with Camoufox(
                headless='virtual', os='windows', persistent_context=True,
                user_data_dir=profile_dir, window=(1280, 800), block_images=True,
            ) as context:
                page = context.pages[0] if context.pages else context.new_page()
                page.on('crash', lambda p: logger.warning('[spectrum-signin] page CRASH event fired (url was %s)', _safe_page_url(p)))
                page.on('close', lambda p: logger.warning('[spectrum-signin] page CLOSE event fired'))
                page.on('pageerror', lambda exc: logger.warning('[spectrum-signin] page JS error: %s', str(exc)[:500]))
                page.on('response', _on_response)

                page.goto(_START_URL, wait_until='domcontentloaded', timeout=30000)
                set_status('running', 'Sign in below, including any captcha if shown.', page.url)

                # SSO can carry over instantly via this profile's existing cookies —
                # the page looks fully signed in (real Guide data, no login form to
                # autofill) while the underlying token is actually already dead
                # (confirmed live 2026-09-17: even the page's OWN fetch() with its
                # OWN localStorage token failed validateSession). Give that a brief
                # chance to verify; if it's there but invalid, force a real fresh
                # login instead of sitting forever waiting for a login form that
                # SSO carryover means will never appear. Scoped to ONLY Spectrum/
                # Cox cookies — never the whole shared profile: clearing the whole
                # context once wiped every other MVPD's cookies.
                page.wait_for_timeout(1500)
                if not _try_capture(page) and _rejected:
                    set_status('running', 'Saved session looks stale — starting a fresh sign-in…', page.url)
                    logger.info('[spectrum-signin] SSO-carried-over token failed verification — '
                                'clearing Spectrum/Cox cookies only to force a fresh login')
                    try:
                        for domain in ('.spectrum.net', 'id.spectrum.net', 'watch.spectrum.net',
                                       'apis.spectrum.net', '.cox.com', 'login.cox.com'):
                            context.clear_cookies(domain=domain)
                        page.evaluate("() => { try { localStorage.clear(); } catch(e) {} "
                                      "try { sessionStorage.clear(); } catch(e) {} }")
                    except Exception as exc:  # noqa: BLE001
                        logger.warning('[spectrum-signin] recovery cookie clear failed: %s', exc)
                    page.goto(_START_URL, wait_until='domcontentloaded', timeout=30000)
                    set_status('running', 'Sign in below, including any captcha if shown.', page.url)

                if username and password:
                    set_status('running', 'Auto-filling saved credentials…', page.url)
                    _try_autofill_credentials(
                        page, username, password, wait_seconds=12.0, r=r,
                        stop_key=SPECTRUM_SIGNIN_STOP_KEY, input_key=SPECTRUM_SIGNIN_INPUT_KEY,
                        shot_key=SPECTRUM_SIGNIN_SHOT_KEY, hint_key=SPECTRUM_SIGNIN_HINT_KEY,
                    )
                    set_status('running', 'Sign in below, including any captcha if shown.', page.url)

                wait_started = time.monotonic()
                last_shot = 0.0
                while time.monotonic() < deadline:
                    if r.exists(SPECTRUM_SIGNIN_STOP_KEY):
                        set_status('stopped', 'Cancelled')
                        return
                    if page.is_closed():
                        raise _BrowserSessionDied('browser page closed before sign-in completed')
                    now = time.monotonic()
                    if now - last_shot > 0.25:
                        last_shot = now
                        if _relay_input_and_screenshot(
                            page, r, waiting_since=wait_started,
                            stop_key=SPECTRUM_SIGNIN_STOP_KEY, input_key=SPECTRUM_SIGNIN_INPUT_KEY,
                            shot_key=SPECTRUM_SIGNIN_SHOT_KEY, hint_key=SPECTRUM_SIGNIN_HINT_KEY,
                        ):
                            set_status('stopped', 'Cancelled')
                            return
                    if _try_capture(page):
                        break
                    page.wait_for_timeout(200)

                if 'verified' not in captured:
                    if _last_mismatch.get('account'):
                        set_status(
                            'error',
                            f"Timed out — kept detecting a signed-in session for "
                            f"account {_last_mismatch['account']!r}, not the "
                            f"configured {username!r}. This means this device/network "
                            f"already has an existing Spectrum session for a "
                            f"different account (shared browser profile, or "
                            f"network-based auto-auth) — check that the configured "
                            f"username is the account you want, or try from a "
                            f"different network.")
                    else:
                        set_status('error', 'Timed out waiting for sign-in to complete.')
                    return

                # Best-effort — purely so a future Cox TVE integration can reuse
                # this same session without a second login. Never blocks success.
                try:
                    cox_cookies = [c for c in context.cookies() if 'cox.com' in (c.get('domain') or '')]
                except Exception as exc:  # noqa: BLE001
                    logger.warning('[spectrum-signin] cox cookie harvest failed: %s', exc)
        except BaseException as exc:  # noqa: BLE001
            if _terminal_status_set['v']:
                logger.info('[spectrum-signin] ignoring cleanup-time exception after terminal status was already set: %s', exc)
                return
            if r.exists(SPECTRUM_SIGNIN_STOP_KEY):
                set_status('stopped', 'Cancelled')
                return
            logger.exception('[spectrum-signin] browser session failed')
            set_status('error', f'Browser session failed: {exc}')
            return

        set_status('running', 'Saving tokens…')
        try:
            with flask_app.app_context():
                from app.scrapers.spectrum import save_login_result
                save_login_result(captured['verified'], cox_cookies)
        except Exception as exc:  # noqa: BLE001
            logger.exception('[spectrum-signin] failed to save tokens')
            set_status('error', f'Signed in but failed to save tokens: {exc}')
            return
        set_status('success', 'Signed in to Spectrum.')
    finally:
        uninstall_browser_login_activity_log(_activity_handler)
        if not _ctx_popped['v']:
            _ctx.pop()

"""FOX One browser-assisted login."""
import logging
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit as _urlsplit
import redis

from app.worker import flask_app
from app.extensions import db
from app.models import Source, TVEAccount
from app.config_store import persist_source_config_updates
from app.tve.browser_login.common import (
    MVPD_BROWSER_LOGIN_STATUS_KEY,
    MVPD_BROWSER_LOGIN_INPUT_KEY,
    MVPD_BROWSER_LOGIN_STOP_KEY,
    _safe_page_url,
    _settle_after_mvpd_navigation,
    _record_tve_login_error,
    _prime_google_session,
    _maybe_capture_google_master_token,
    _relay_input_and_screenshot,
    _is_browser_death,
    _url_for_log,
    _gateway_url_for_log,
    _youtube_tv_gateway_failure_message,
    _maybe_retry_youtubetv_from_scratch,
    _log_youtubetv_gateway_diagnostics,
    _YOUTUBETV_ISOLATED_PROFILE_DIR,
    _YOUTUBETV_CAMOUFOX_FIREFOX_PREFS,
)

logger = logging.getLogger(__name__)


def _run_foxone_browser_assisted_login(r, set_status, source, account, scraper, mso_id: str) -> None:
    """Browser-assisted counterpart to FOX One's scripted Cox fast path
    (api.foxone_signin / _authenticate_via_mvpd's Cox branch), for any MSO
    whose login page blocks scripted clients outright (YouTubeTV/Google,
    Sling, etc.) — same "second screen" idea as _run_amcn_browser_assisted_login
    and _run_discovery_browser_assisted_login, adapted to FOX One's own
    adobeauthn/regcode API (_foxone_mvpd_register/_foxone_mvpd_finish).

    Completion has no independent poll signal the way NBC/AMCN's
    /profiles/code/{code} does, and no browser-landed-URL signal the way
    Discovery's does either — FOX One's own frontend just POSTs
    requests/complete claiming "status: authenticated" and then asks
    checkauthn for a token, trusting Adobe's own server-side binding to
    reject that claim if the human hasn't actually finished yet. So this
    polls _foxone_mvpd_finish() itself (repeating the POST+GET) rather than
    a separate read-only check — unconfirmed until tested live whether FOX's
    backend tolerates repeated "authenticated" claims before the real login
    completes; logged verbosely so a live run makes that obvious immediately
    if not.

    Reuses the shared legacy 'mvpd:browser-login:*' redis keys (via
    set_status and _relay_input_and_screenshot's defaults), same as AMCN/
    Discovery.
    """
    try:
        from camoufox.sync_api import Camoufox
    except ImportError:
        set_status('error', 'Camoufox is not installed on this container')
        return

    import os as _os_login
    # Isolated, cookie-permissive profile for YouTubeTV — shared with the
    # other browser-login flows (see _YOUTUBETV_ISOLATED_PROFILE_DIR's
    # docstring in common.py).
    profile_dir = (
        _YOUTUBETV_ISOLATED_PROFILE_DIR
        if mso_id == 'YouTubeTV'
        else '/data/browser_profiles/mvpd_tve'
    )
    try:
        _os_login.makedirs(profile_dir, exist_ok=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning('[foxone-mvpd-login] could not create profile dir %s: %s', profile_dir, exc)

    try:
        session, request_id, device_id, mso_login_url, page_response = scraper._foxone_mvpd_register(mso_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception('[foxone-mvpd-login] unexpected failure registering session')
        _record_tve_login_error('foxone', str(exc))
        set_status('error', f'FOX One: {exc}')
        return

    nav_url = mso_login_url or str(page_response.url)

    _PER_LOGIN_TIMEOUT_SECONDS = 150
    _POLL_SECONDS = 3.0
    access_token = ''
    expires_at = 0.0

    camoufox_options = {
        'headless': 'virtual', 'os': 'windows', 'persistent_context': True,
        'user_data_dir': profile_dir, 'window': (1280, 800),
    }
    if mso_id == 'YouTubeTV':
        # See _YOUTUBETV_CAMOUFOX_FIREFOX_PREFS's docstring in common.py.
        camoufox_options['firefox_user_prefs'] = _YOUTUBETV_CAMOUFOX_FIREFOX_PREFS
        logger.info('[foxone-mvpd-login] using isolated YouTubeTV profile with cross-site cookies enabled')
    try:
        with Camoufox(**camoufox_options) as context:
            page = context.pages[0] if context.pages else context.new_page()
            _prime_google_session(context, mso_id)
            page.on('crash', lambda p: logger.warning('[foxone-mvpd-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
            page.on('close', lambda p: logger.warning('[foxone-mvpd-login] page CLOSE event fired'))
            page.on('pageerror', lambda exc: logger.warning('[foxone-mvpd-login] page JS error: %s', str(exc)[:500]))

            youtube_gateway_responses = []

            def _log_navigation_response(response):
                try:
                    if not response.request.is_navigation_request():
                        return
                    response_url = response.url
                    response_parts = _urlsplit(response_url)
                    is_gateway_bookend = (
                        mso_id == 'YouTubeTV'
                        and response_parts.netloc == 'youtube.auth-gateway.net'
                        and response_parts.path.endswith('/authbypass/firstbookend.php')
                    )
                    if is_gateway_bookend:
                        youtube_gateway_responses.append(response)
                    logger.info(
                        '[foxone-mvpd-login] navigation response HTTP %s %s',
                        response.status,
                        _gateway_url_for_log(response_url) if is_gateway_bookend else _url_for_log(response_url),
                    )
                except Exception:  # noqa: BLE001
                    pass

            page.on('response', _log_navigation_response)

            set_status('running', 'Signing in to FOX One…')
            try:
                page.goto(nav_url, wait_until='domcontentloaded', timeout=30000)
            except Exception as exc:  # noqa: BLE001
                if _is_browser_death(exc):
                    raise
                set_status('error', f'FOX One: failed to load sign-in page ({exc})')
                return
            if mso_id == 'YouTubeTV':
                try:
                    _maybe_retry_youtubetv_from_scratch(page, youtube_gateway_responses, nav_url)
                except Exception as exc:  # noqa: BLE001
                    if _is_browser_death(exc):
                        raise
                    logger.warning(
                        '[foxone-mvpd-login] YouTubeTV retry-from-scratch recovery failed; '
                        'continuing to normal stall detection: %s', exc,
                    )
            # See _settle_after_mvpd_navigation's docstring: a
            # page.screenshot() call during Adobe/YouTubeTV's still-in-
            # flight SAML bounce chain silently cancels it. FOX One never
            # calls _try_autofill_credentials (polls
            # scraper._foxone_mvpd_finish() instead), so this is the only
            # place that can protect its first screenshot.
            settled = _settle_after_mvpd_navigation(
                page, set_status=set_status,
                respect_youtubetv_soft_block=mso_id != 'YouTubeTV',
            )
            landing_url = _safe_page_url(page)
            if not settled:
                if mso_id == 'YouTubeTV':
                    _log_youtubetv_gateway_diagnostics(context, youtube_gateway_responses)
                    message = _youtube_tv_gateway_failure_message()
                else:
                    message = (
                        'The provider sign-in redirect did not finish within 15 seconds. '
                        'FOX One stopped before displaying an incomplete or blank login page; try again later.'
                    )
                logger.warning(
                    '[foxone-mvpd-login] aborting stalled provider redirect before polling for completion '
                    '(mso_id=%s landing=%s gateway_responses=%d)',
                    mso_id, _url_for_log(landing_url), len(youtube_gateway_responses),
                )
                set_status('error', message)
                return
            if (
                mso_id == 'YouTubeTV'
                and _urlsplit(landing_url).netloc == 'support.google.com'
                and _urlsplit(landing_url).path.startswith('/accounts/answer/32050')
            ):
                logger.warning(
                    '[foxone-mvpd-login] Google rejected the primed browser session and redirected to its '
                    'cookie-recovery page (%s)', _url_for_log(landing_url),
                )
                set_status(
                    'error',
                    'Google rejected the saved browser session. Use “Sign in with Google” again, then retry.',
                )
                return
            set_status('running', 'Signing in to FOX One…', landing_url)

            wait_started = time.monotonic()
            deadline = wait_started + _PER_LOGIN_TIMEOUT_SECONDS
            last_shot = 0.0
            last_poll = 0.0
            cancelled = False
            while time.monotonic() < deadline:
                if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
                    cancelled = True
                    break
                now = time.monotonic()
                if now - last_shot > 0.25:
                    last_shot = now
                    if _relay_input_and_screenshot(page, r, waiting_since=wait_started):
                        cancelled = True
                        break
                if now - last_poll > _POLL_SECONDS:
                    last_poll = now
                    try:
                        access_token, expires_at = scraper._foxone_mvpd_finish(session, request_id, device_id, mso_id)
                    except Exception as exc:  # noqa: BLE001
                        logger.info('[foxone-mvpd-login] poll not-yet/error: %s', exc)
                        continue
                    break
                page.wait_for_timeout(80)

            if cancelled:
                set_status('stopped', 'Cancelled')
                return
            if not access_token:
                set_status('error', 'FOX One: timed out waiting for sign-in to complete.')
                return
            if mso_id == 'YouTubeTV':
                _maybe_capture_google_master_token(context, mso_id)
    except BaseException as exc:  # noqa: BLE001
        if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
            set_status('stopped', 'Cancelled')
            return
        logger.exception('[foxone-mvpd-login] browser-assisted session failed')
        set_status('error', f'Browser session failed: {exc}')
        return

    scraper._update_config('access_token', access_token)
    scraper._update_config('access_expires_at', expires_at)
    scraper._update_config('access_token_captured_at', int(time.time()))
    # Fresh, self-contained app_context — see _prime_google_session's
    # docstring: the caller (run_foxone_browser_login) pops its own before
    # calling this function, since everything above runs the actual browser
    # session, so nothing can be assumed pushed by the time execution
    # reaches here.
    with flask_app.app_context():
        persist_source_config_updates(source.id, scraper._pending_config_updates)
        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if account:
            account.last_auth_status = 'ok'
            account.last_auth_message = f'FOX One access token obtained through {mso_id} MVPD (browser-assisted).'
            account.last_auth_at = datetime.now(timezone.utc)
            db.session.commit()
    set_status('success', f'Signed in — FOX One authorized via {mso_id}.')
    logger.info('[foxone-mvpd-login] paired mso_id=%s (browser-assisted)', mso_id)


def run_foxone_browser_login(mso_id: str):
    """Standalone "Sign in" for FOX One.

    Mirrors run_amcn_browser_login/run_discovery_browser_login: Cox keeps
    its existing fast scripted path (mirroring api.foxone_signin exactly,
    since that route is what "Sign in" called before this function existed
    for FOX One at all — this is the FIRST browser-assisted entry point FOX
    One has ever had); any other MSO goes through the real browser-assisted
    flow below instead of erroring out with "no scripted sign-in wired up".
    """
    # Manual push/pop instead of `with flask_app.app_context():` — see
    # _prime_google_session's docstring: Camoufox's own rendering breaks
    # intermittently while a Flask app_context is active on this thread.
    # Popped right before handing off to _run_foxone_browser_assisted_login
    # (which launches Camoufox and pushes its own fresh, short-lived context
    # for the one DB write it still needs afterward) and never re-pushed —
    # the Cox scripted path below is the only other branch, and it never
    # reaches the pop.
    _ctx = flask_app.app_context()
    _ctx.push()
    _ctx_popped = {'v': False}
    try:
        import json as _json_login

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[foxone-mvpd-login] Redis unavailable, aborting: %s', exc)
            return

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    MVPD_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': url, 'requestor_id': 'FOX One', 'steps': []}),
                )
            except Exception:  # noqa: BLE001
                pass

        r.delete(MVPD_BROWSER_LOGIN_STOP_KEY)
        r.delete(MVPD_BROWSER_LOGIN_INPUT_KEY)
        set_status('running', 'Signing in to FOX One…')

        from app.scrapers.fox_one import FoxOneScraper

        source = Source.query.filter_by(name='fox_one').first()
        if not source:
            set_status('error', 'FOX One source not found.')
            return
        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if not account or not account.is_enabled or not account.has_credentials():
            set_status('error', 'TVE credentials are not configured in Settings.')
            return
        scraper = FoxOneScraper(config=dict(source.config or {}))

        if mso_id != 'Cox':
            _ctx.pop()
            _ctx_popped['v'] = True
            _run_foxone_browser_assisted_login(r, set_status, source, account, scraper, mso_id)
            return

        try:
            access_token, expires_at = scraper._authenticate_via_mvpd(
                mso_id, account.username or '', account.password or '', (account.config or {}).get('xfinity_cookie_jar'),
            )
        except Exception as exc:  # noqa: BLE001
            account.last_auth_status = 'error'
            account.last_auth_message = f'FOX One {mso_id} MVPD auth failed: {exc}'[:500]
            account.last_auth_at = datetime.now(timezone.utc)
            _record_tve_login_error('foxone', str(exc))
            db.session.commit()
            persist_source_config_updates(source.id, scraper._pending_config_updates)
            set_status('error', f'FOX One: {exc}')
            return
        scraper._update_config('access_token', access_token)
        scraper._update_config('access_expires_at', expires_at)
        scraper._update_config('access_token_captured_at', int(time.time()))
        persist_source_config_updates(source.id, scraper._pending_config_updates)
        account.last_auth_status = 'ok'
        account.last_auth_message = f'FOX One access token obtained through {mso_id} MVPD.'
        account.last_auth_at = datetime.now(timezone.utc)
        db.session.commit()
        set_status('success', 'Signed in — FOX One authorized.')
        logger.info('[foxone-mvpd-login] paired mso_id=%s (scripted, no browser)', mso_id)
    finally:
        if not _ctx_popped['v']:
            _ctx.pop()

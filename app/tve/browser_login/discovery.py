"""Discovery TVE browser-assisted login."""
import logging
import time
import redis

from app.worker import flask_app
from app.models import Source, TVEAccount
from app.config_store import persist_source_cache_updates
from app.tve.adobe_pass import TVEAuthError, TVENotAuthorizedError
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
)

logger = logging.getLogger(__name__)


def _run_discovery_browser_assisted_login(r, set_status, source, account, scraper, mso_id: str, mso_name: str) -> None:
    """Browser-assisted counterpart to run_discovery_browser_login's scripted
    Cox fast path, for any MSO whose login page blocks scripted clients
    outright (YouTubeTV/Google, Sling, etc.) — same "second screen" idea as
    _run_amcn_browser_assisted_login, adapted to Discovery's single shared
    session (all 14 channels ride ONE gauth session/cookie jar, unlike
    AMCN's 4 independent per-channel logins — see SESSION_CACHE_KEY), and to
    Discovery's own completion shape: rather than an independent
    /profiles/code/{code} poll API (NBC/AMCN), a Discovery login completes
    when the BROWSER's own redirect chain lands on a URL carrying a `code`
    query param (see DiscoveryTVEScraper._authenticate()'s Cox/Xfinity
    branches — the code is extracted from wherever redirect_url's own chain
    lands, not fetched independently), so this watches page.url directly
    instead of polling a separate endpoint.

    Reuses the shared legacy 'mvpd:browser-login:*' redis keys (via
    set_status and _relay_input_and_screenshot's defaults), same as AMCN.
    """
    import uuid as _uuid_login
    from urllib.parse import parse_qs as _parse_qs_login, urlsplit as _urlsplit_login
    from app.scrapers.discovery_tve import AUTH_HOST, CALLBACK_BASE

    try:
        from camoufox.sync_api import Camoufox
    except ImportError:
        set_status('error', 'Camoufox is not installed on this container')
        return

    import os as _os_login
    profile_dir = '/data/browser_profiles/mvpd_tve'
    try:
        _os_login.makedirs(profile_dir, exist_ok=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning('[discovery-mvpd-login] could not create profile dir %s: %s', profile_dir, exc)

    session = scraper._session()
    device_id = scraper.config.get('device_id') or str(_uuid_login.uuid4())
    if not scraper.config.get('device_id'):
        scraper._update_config('device_id', device_id)

    try:
        mso_login_url, page_response = scraper._discovery_session_redirect(session, device_id, mso_id, mso_name)
    except TVENotAuthorizedError as exc:
        _record_tve_login_error('discovery', f'not entitled — {exc}')
        set_status('error', f'Discovery TVE: not entitled — {exc}')
        return
    except TVEAuthError as exc:
        _record_tve_login_error('discovery', str(exc))
        set_status('error', f'Discovery TVE: {exc}')
        return
    except Exception as exc:  # noqa: BLE001
        logger.exception('[discovery-mvpd-login] unexpected failure registering session')
        _record_tve_login_error('discovery', str(exc))
        set_status('error', f'Discovery TVE: {exc}')
        return

    nav_url = mso_login_url or str(page_response.url)

    def _extract_code(url: str) -> str:
        if not (url.startswith(AUTH_HOST) or url.startswith(CALLBACK_BASE)):
            return ''
        return (_parse_qs_login(_urlsplit_login(url).query).get('code') or [''])[0]

    _PER_LOGIN_TIMEOUT_SECONDS = 150
    _POLL_SECONDS = 1.0

    try:
        with Camoufox(
            headless='virtual', os='windows', persistent_context=True,
            user_data_dir=profile_dir, window=(1280, 800),
        ) as context:
            page = context.pages[0] if context.pages else context.new_page()
            _prime_google_session(context, mso_id)
            page.on('crash', lambda p: logger.warning('[discovery-mvpd-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
            page.on('close', lambda p: logger.warning('[discovery-mvpd-login] page CLOSE event fired'))
            page.on('pageerror', lambda exc: logger.warning('[discovery-mvpd-login] page JS error: %s', str(exc)[:500]))

            set_status('running', 'Signing in to Discovery TVE…')
            try:
                page.goto(nav_url, wait_until='domcontentloaded', timeout=30000)
            except Exception as exc:  # noqa: BLE001
                if _is_browser_death(exc):
                    raise
                set_status('error', f'Discovery TVE: failed to load sign-in page ({exc})')
                return
            # See _settle_after_mvpd_navigation's docstring: a
            # page.screenshot() call during Adobe/YouTubeTV's still-in-
            # flight SAML bounce chain silently cancels it. Discovery never
            # calls _try_autofill_credentials (watches page.url for a code
            # param instead), so this is the only place that can protect
            # its first screenshot.
            _settle_after_mvpd_navigation(page, set_status=set_status)
            set_status('running', 'Signing in to Discovery TVE…', _safe_page_url(page))

            wait_started = time.monotonic()
            deadline = wait_started + _PER_LOGIN_TIMEOUT_SECONDS
            last_shot = 0.0
            last_poll = 0.0
            code = ''
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
                    code = _extract_code(_safe_page_url(page))
                    if code:
                        break
                page.wait_for_timeout(80)

            if cancelled:
                set_status('stopped', 'Cancelled')
                return
            if not code:
                set_status('error', 'Discovery TVE: timed out waiting for sign-in to complete.')
                return
            if mso_id == 'YouTubeTV':
                _maybe_capture_google_master_token(context, mso_id)
    except BaseException as exc:  # noqa: BLE001
        if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
            set_status('stopped', 'Cancelled')
            return
        logger.exception('[discovery-mvpd-login] browser-assisted session failed')
        set_status('error', f'Browser session failed: {exc}')
        return

    try:
        scraper._discovery_finish_login(session, device_id, code)
    except TVENotAuthorizedError as exc:
        _record_tve_login_error('discovery', f'not entitled — {exc}')
        set_status('error', f'Discovery TVE: not entitled — {exc}')
        return
    except TVEAuthError as exc:
        _record_tve_login_error('discovery', str(exc))
        set_status('error', f'Discovery TVE: {exc}')
        return
    except Exception as exc:  # noqa: BLE001
        logger.exception('[discovery-mvpd-login] unexpected failure finishing login')
        _record_tve_login_error('discovery', str(exc))
        set_status('error', f'Discovery TVE: {exc}')
        return

    # Fresh, self-contained app_context — see _prime_google_session's
    # docstring: run_discovery_browser_login pops its own before calling
    # this function.
    with flask_app.app_context():
        persist_source_cache_updates(source.id, scraper._pending_cache_updates)
    set_status('success', f'Signed in — Discovery TVE authorized via {mso_id}.')
    logger.info('[discovery-mvpd-login] paired mso_id=%s (browser-assisted)', mso_id)


def run_discovery_browser_login(mso_id: str):
    """Standalone "Sign in" for Discovery TVE.

    Unlike AMC/NBC/FOX, Discovery's Cox login never actually needs a
    browser: DiscoveryTVEScraper._authenticate() already does the whole
    thing scripted (register, gauth authorize, _cox_saml_login's direct
    POST to login.cox.com/api/v1/authn, code exchange, entitlement check)
    on every session refresh during normal scraping. Confirmed live
    2026-08-11: a full authenticate+entitlement round trip in ~3s with the
    real Cox account, zero Camoufox involved. The old page.goto()+autofill
    version routed this same login through a full Firefox launch for no
    reason, which is almost certainly why Discovery was one of the networks
    reported stuck/timing out in the community thread — Camoufox
    render/timeout budgets, not anything about Discovery's actual auth
    requirements. This only supports Cox (the only
    MSO _authenticate() has wired up); non-Cox reports back as an error
    same as before.

    No stop-key check here (unlike run_amcn_browser_login's per-channel
    loop, code review 2026-08-11) — _authenticate() is one ~3s scripted call
    with no natural interruption point partway through, so there's nothing
    meaningful to cancel into; worst case is bounded by its own per-request
    timeouts (30s each) rather than the old ~30min browser session.
    """
    # Manual push/pop instead of `with flask_app.app_context():` — see
    # _prime_google_session's docstring: Camoufox's own rendering breaks
    # intermittently while a Flask app_context is active on this thread.
    # Popped right before handing off to _run_discovery_browser_assisted_login
    # (which launches Camoufox and pushes its own fresh, short-lived
    # contexts for the DB writes it still needs) and never re-pushed — the
    # Cox scripted path below is the only other branch, and it never
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
            logger.warning('[discovery-mvpd-login] Redis unavailable, aborting: %s', exc)
            return

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    MVPD_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': url, 'requestor_id': 'Discovery TVE', 'steps': []}),
                )
            except Exception:  # noqa: BLE001
                pass

        r.delete(MVPD_BROWSER_LOGIN_STOP_KEY)
        r.delete(MVPD_BROWSER_LOGIN_INPUT_KEY)
        set_status('running', 'Signing in to Discovery TVE…')

        from app.scrapers.discovery_tve import DiscoveryTVEScraper

        source = Source.query.filter_by(name='discovery_tve').first()
        if not source:
            set_status('error', 'Discovery TVE source not found.')
            return
        scraper = DiscoveryTVEScraper(config=dict(source.config or {}))

        if mso_id != 'Cox':
            account = TVEAccount.query.filter_by(provider_id='mvpd').first()
            if not account or not account.is_enabled or not account.has_credentials():
                set_status('error', 'TVE credentials are not configured in Settings.')
                return
            mso_name = ((account.config or {}).get('selected_mso_name') or mso_id).strip()
            _ctx.pop()
            _ctx_popped['v'] = True
            _run_discovery_browser_assisted_login(r, set_status, source, account, scraper, mso_id, mso_name)
            return

        try:
            scraper._authenticate()
        except TVENotAuthorizedError as exc:
            _record_tve_login_error('discovery', f'not entitled — {exc}')
            set_status('error', f'Discovery TVE: not entitled — {exc}')
            return
        except TVEAuthError as exc:
            _record_tve_login_error('discovery', str(exc))
            set_status('error', f'Discovery TVE: {exc}')
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception('[discovery-mvpd-login] unexpected failure')
            _record_tve_login_error('discovery', str(exc))
            set_status('error', f'Discovery TVE: {exc}')
            return
        persist_source_cache_updates(source.id, scraper._pending_cache_updates)
        set_status('success', 'Signed in — Discovery TVE authorized.')
        logger.info('[discovery-mvpd-login] paired mso_id=%s (scripted, no browser)', mso_id)
    finally:
        if not _ctx_popped['v']:
            _ctx.pop()

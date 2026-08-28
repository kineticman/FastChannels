"""Discovery TVE browser-assisted login."""
import logging
import time
import redis

from app.worker import flask_app
from app.models import Source, TVEAccount
from app.config_store import persist_source_cache_updates
from app.tve.adobe_pass import TVEAuthError, TVENotAuthorizedError
from urllib.parse import urlsplit as _urlsplit
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
    _autofill_xfinity_credentials,
    _harvest_and_save_xfinity_cookies,
    _is_browser_death,
    _url_for_log,
    _gateway_url_for_log,
    _youtube_tv_gateway_failure_message,
    _maybe_retry_youtubetv_from_scratch,
    _log_youtubetv_gateway_diagnostics,
    _YOUTUBETV_ISOLATED_PROFILE_DIR,
    _YOUTUBETV_CAMOUFOX_FIREFOX_PREFS,
    install_browser_login_activity_log,
    uninstall_browser_login_activity_log,
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
        parts = _urlsplit_login(url)
        # gauth-sync's own JS throws deterministically ("can't access
        # property 'type', o is undefined") before it can act on the
        # landing URL — confirmed live 2026-08-28: reloading the exact same
        # URL twice reproduces the identical crash both times, so it isn't
        # the same kind of timing race the confirmed_login_url retry in
        # common.py works around. That script likely exists to read `code`
        # out of the URL fragment (a `#code=...` implicit-flow-style
        # delivery, never sent to the server, so client JS has to be the
        # one to read it) and hand it off via postMessage/redirect — which
        # would explain why polling page.url's query string alone never
        # saw it even though the page really did land with a valid code.
        # Checking the fragment too costs nothing if it's actually in the
        # query string like every other MSO.
        code = (_parse_qs_login(parts.query).get('code') or [''])[0]
        if code:
            return code
        return (_parse_qs_login(parts.fragment).get('code') or [''])[0]

    _PER_LOGIN_TIMEOUT_SECONDS = 150
    _POLL_SECONDS = 1.0

    try:
        camoufox_options = {
            'headless': 'virtual', 'os': 'windows', 'persistent_context': True,
            'user_data_dir': profile_dir, 'window': (1280, 800),
        }
        if mso_id == 'YouTubeTV':
            # See _YOUTUBETV_CAMOUFOX_FIREFOX_PREFS's docstring in common.py.
            camoufox_options['firefox_user_prefs'] = _YOUTUBETV_CAMOUFOX_FIREFOX_PREFS
            logger.info('[discovery-mvpd-login] using isolated YouTubeTV profile with cross-site cookies enabled')
        with Camoufox(**camoufox_options) as context:
            page = context.pages[0] if context.pages else context.new_page()
            _prime_google_session(context, mso_id)
            page.on('crash', lambda p: logger.warning('[discovery-mvpd-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
            page.on('close', lambda p: logger.warning('[discovery-mvpd-login] page CLOSE event fired'))
            page.on('pageerror', lambda exc: logger.warning('[discovery-mvpd-login] page JS error: %s', str(exc)[:500]))

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
                        '[discovery-mvpd-login] navigation response HTTP %s %s',
                        response.status,
                        _gateway_url_for_log(response_url) if is_gateway_bookend else _url_for_log(response_url),
                    )
                except Exception:  # noqa: BLE001
                    pass

            page.on('response', _log_navigation_response)

            set_status('running', 'Signing in to Discovery TVE…')
            try:
                if mso_id == 'Comcast_SSO':
                    # Xfinity's WAF (Akamai) flatly denies a cold top-level
                    # navigation to the Adobe Pass authenticate/saml URL —
                    # same wall fox.py/mvpd.py hit and work around; see
                    # run_mvpd_browser_login's comment on this exact pattern
                    # for the full explanation. Landing on a real Discovery
                    # page first and redirecting via in-page JS (real
                    # Referer/Sec-Fetch-Site chain) sails through instead.
                    page.goto('https://www.discovery.com', wait_until='domcontentloaded', timeout=30000)
                    _settle_deadline = time.monotonic() + 3.0
                    while time.monotonic() < _settle_deadline:
                        _relay_input_and_screenshot(page, r)
                        page.wait_for_timeout(500)
                    page.evaluate('(u) => { window.location.href = u; }', nav_url)
                    _load_deadline = time.monotonic() + 30.0
                    while time.monotonic() < _load_deadline:
                        try:
                            page.wait_for_load_state('domcontentloaded', timeout=1000)
                            break
                        except Exception:  # noqa: BLE001
                            pass
                        _relay_input_and_screenshot(page, r)
                else:
                    page.goto(nav_url, wait_until='domcontentloaded', timeout=30000)
            except Exception as exc:  # noqa: BLE001
                if _is_browser_death(exc):
                    raise
                set_status('error', f'Discovery TVE: failed to load sign-in page ({exc})')
                return
            if mso_id == 'YouTubeTV':
                try:
                    _maybe_retry_youtubetv_from_scratch(page, youtube_gateway_responses, nav_url)
                except Exception as exc:  # noqa: BLE001
                    if _is_browser_death(exc):
                        raise
                    logger.warning(
                        '[discovery-mvpd-login] YouTubeTV retry-from-scratch recovery failed; '
                        'continuing to normal stall detection: %s', exc,
                    )
            # See _settle_after_mvpd_navigation's docstring: a
            # page.screenshot() call during Adobe/YouTubeTV's still-in-
            # flight SAML bounce chain silently cancels it. This is the
            # only place that can protect its first screenshot.
            settled = _settle_after_mvpd_navigation(
                page, set_status=set_status,
                respect_youtubetv_soft_block=mso_id != 'YouTubeTV',
            )
            landing_url = _safe_page_url(page)
            if not settled:
                if mso_id == 'YouTubeTV':
                    _log_youtubetv_gateway_diagnostics(context, youtube_gateway_responses)
                    message = f'Discovery TVE: {_youtube_tv_gateway_failure_message()}'
                else:
                    message = (
                        'Discovery TVE: the provider sign-in redirect did not finish within 15 seconds. '
                        'Stopped before displaying an incomplete or blank login page; try again later.'
                    )
                logger.warning(
                    '[discovery-mvpd-login] aborting stalled provider redirect before polling '
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
                    '[discovery-mvpd-login] Google rejected the primed browser session and redirected to its '
                    'cookie-recovery page (%s)', _url_for_log(landing_url),
                )
                set_status(
                    'error',
                    'Google rejected the saved browser session. Use “Sign in with Google” again, then retry.',
                )
                return
            if account.username and account.password and mso_id == 'Comcast_SSO':
                _autofill_xfinity_credentials(
                    page, account.username, account.password, r=r,
                    stop_key=MVPD_BROWSER_LOGIN_STOP_KEY, input_key=MVPD_BROWSER_LOGIN_INPUT_KEY,
                )
            set_status('running', 'Signing in to Discovery TVE…', landing_url)

            # gauth-sync itself has a real client-side bug (confirmed live
            # 2026-08-28: a JS error — "can't access property 'type', o is
            # undefined" — fires right as it loads). Confirmed live the same
            # day that reloading the exact same URL reproduces the identical
            # crash every time — it's deterministic, not a timing race like
            # _autofill_xfinity_credentials's confirmed_login_url retry in
            # common.py — so this reload is really just a cheap safety net
            # for whatever fraction of loads AREN'T hitting it, while
            # _extract_code's fragment check (see its docstring) is the
            # actual fix for the deterministic case.
            _GAUTH_SYNC_STALL_SECONDS = 10.0
            _GAUTH_SYNC_MAX_RELOADS = 2
            wait_started = time.monotonic()
            deadline = wait_started + _PER_LOGIN_TIMEOUT_SECONDS
            last_shot = 0.0
            last_poll = 0.0
            code = ''
            cancelled = False
            gauth_sync_stalled_since = None
            gauth_sync_reloads = 0
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
                    current_url = _safe_page_url(page)
                    code = _extract_code(current_url)
                    if code:
                        break
                    if current_url.startswith(f'{AUTH_HOST}/gauth-sync'):
                        if gauth_sync_stalled_since is None:
                            gauth_sync_stalled_since = now
                            # Param NAMES only (never values — these can
                            # carry auth tokens) so if the fragment theory
                            # above is wrong, the next live attempt still
                            # tells us what's actually on this URL instead
                            # of just timing out silently again.
                            _url_parts = _urlsplit_login(current_url)
                            logger.warning(
                                '[discovery-mvpd-login] landed on gauth-sync without a code; '
                                'query keys=%s fragment keys=%s',
                                sorted(_parse_qs_login(_url_parts.query).keys()) or '-',
                                sorted(_parse_qs_login(_url_parts.fragment).keys()) or '-',
                            )
                        elif (
                            gauth_sync_reloads < _GAUTH_SYNC_MAX_RELOADS
                            and now - gauth_sync_stalled_since > _GAUTH_SYNC_STALL_SECONDS
                        ):
                            gauth_sync_reloads += 1
                            logger.warning(
                                '[discovery-mvpd-login] gauth-sync stalled without a code for '
                                '%.0fs, reloading (attempt %d/%d)',
                                now - gauth_sync_stalled_since, gauth_sync_reloads, _GAUTH_SYNC_MAX_RELOADS,
                            )
                            try:
                                page.reload(wait_until='domcontentloaded', timeout=15000)
                            except Exception as exc:  # noqa: BLE001
                                if _is_browser_death(exc):
                                    raise
                                logger.warning('[discovery-mvpd-login] gauth-sync reload failed: %s', exc)
                            gauth_sync_stalled_since = None
                    else:
                        gauth_sync_stalled_since = None
                        gauth_sync_reloads = 0
                page.wait_for_timeout(80)

            if cancelled:
                set_status('stopped', 'Cancelled')
                return
            if not code:
                set_status('error', 'Discovery TVE: timed out waiting for sign-in to complete.')
                return
            if mso_id == 'YouTubeTV':
                _maybe_capture_google_master_token(context, mso_id)
            elif mso_id == 'Comcast_SSO':
                # Same idea as the YouTubeTV branch above, for the Xfinity
                # cookie jar instead of a Google master_token — see
                # _harvest_and_save_xfinity_cookies's docstring. Was missing
                # here entirely (unlike mvpd.py/nbc.py/fox.py), so a fully
                # successful Discovery TVE browser login never saved
                # anything for other TVE families' cookie-jar fast path.
                _harvest_and_save_xfinity_cookies(context)
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
    _activity_handler = None
    try:
        import json as _json_login

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[discovery-mvpd-login] Redis unavailable, aborting: %s', exc)
            return
        _activity_handler = install_browser_login_activity_log(r)

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

            if mso_id == 'Comcast_SSO':
                # Try a saved cookie jar (harvested from a previous
                # successful Comcast_SSO browser pairing for ANY TVE family
                # — see _harvest_and_save_xfinity_cookies) BEFORE ever
                # opening a browser, same as mvpd.py/nbc.py/fox.py already
                # do. Confirmed live 2026-08-28: scraper._authenticate()
                # (already used by the Cox branch below, and by every
                # scheduled session refresh) works unmodified for
                # Comcast_SSO too once a jar exists — the interactive
                # browser flow was what was actually tripping Comcast's own
                # fraud/step-up check on a password-hydration retry, not
                # anything about Discovery itself (see gauth-sync stall
                # investigation in _run_discovery_browser_assisted_login).
                cookie_jar = (account.config or {}).get('xfinity_cookie_jar')
                if cookie_jar:
                    set_status('running', 'Trying saved sign-in (no browser needed)…')
                    try:
                        scraper._authenticate()
                    except TVENotAuthorizedError as exc:
                        _record_tve_login_error('discovery', f'not entitled — {exc}')
                        set_status('error', f'Discovery TVE: not entitled — {exc}')
                        return
                    except Exception as exc:  # noqa: BLE001
                        logger.info(
                            '[discovery-mvpd-login] saved xfinity cookie jar did not work, falling back to browser: %s',
                            exc,
                        )
                    else:
                        persist_source_cache_updates(source.id, scraper._pending_cache_updates)
                        set_status('success', 'Signed in — Discovery TVE authorized (no browser needed).')
                        logger.info('[discovery-mvpd-login] paired mso_id=Comcast_SSO via saved cookie jar (no browser)')
                        return
                set_status('running', 'No usable saved sign-in — opening a browser…')

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
        uninstall_browser_login_activity_log(_activity_handler)
        if not _ctx_popped['v']:
            _ctx.pop()

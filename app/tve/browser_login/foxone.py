"""FOX One browser-assisted login."""
import logging
import time
from dataclasses import replace
from urllib.parse import urlsplit as _urlsplit
import redis

from app.worker import flask_app
from app.models import Source
from app.config_store import persist_source_config_updates
from app.tve.browser_login.common import (
    _watch_spectrum_auth_results,
    MVPD_BROWSER_LOGIN_STATUS_KEY,
    MVPD_BROWSER_LOGIN_INPUT_KEY,
    MVPD_BROWSER_LOGIN_STOP_KEY,
    _safe_page_url,
    _settle_after_mvpd_navigation,
    _prime_google_session,
    _maybe_capture_google_master_token,
    _relay_input_and_screenshot,
    _log_signin_timeout_snapshot,
    SpectrumWantsCoxProvider,
    _spectrum_retry_as_cox,
    _spectrum_signin_error_message,
    _autofill_xfinity_credentials,
    _try_autofill_credentials,
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


def _record_foxone_result(source_id: int, scraper, login, error: str | None, *, how: str = '') -> None:
    """Persist a sign-in outcome (FoxOneScraper.record_signin_result) plus any
    config the scraper queued. Pushes its own app context — the browser flow
    runs without one (see run_foxone_browser_login)."""
    with flask_app.app_context():
        scraper.record_signin_result(login, error, how=how)
        persist_source_config_updates(source_id, scraper._pending_config_updates)


def _is_fox_callback(parts) -> bool:
    """FOX's post-login callback pages — see _POLL_SECONDS below."""
    return (
        (parts.netloc == 'auth.fox.com' and parts.path.startswith('/foxone/mvpd/callback'))
        or (parts.netloc == 'www.fox.com' and parts.path.startswith('/callback'))
    )


def _run_foxone_browser_assisted_login(r, set_status, source, login, scraper) -> None:
    """Browser-assisted counterpart to FOX One's scripted MVPD sign-in
    (api.foxone_signin / _authenticate_via_mvpd), for any MSO whose login
    page blocks scripted clients outright (Cox/Spectrum, YouTubeTV/Google,
    Sling, etc.) — same "second screen" idea as _run_amcn_browser_assisted_login
    and _run_discovery_browser_assisted_login, adapted to FOX One's own
    adobeauthn/regcode API (_foxone_mvpd_register/_foxone_mvpd_finish).

    Completion has no independent poll signal the way NBC/AMCN's
    /profiles/code/{code} does, and no browser-landed-URL signal the way
    Discovery's does either — FOX One's own frontend just POSTs
    requests/complete claiming "status: authenticated" and then asks
    checkauthn for a token, trusting Adobe's own server-side binding to
    reject that claim if the human hasn't actually finished yet. So this
    calls _foxone_mvpd_finish() itself, like FOX's site does, once the
    browser reaches FOX's callback page (retrying every few seconds there),
    with a slow fallback poll before that. Early calls just get a 404
    ("no completed request yet") — confirmed harmless live 2026-09-25.

    Reuses the shared legacy 'mvpd:browser-login:*' redis keys (via
    set_status and _relay_input_and_screenshot's defaults), same as AMCN/
    Discovery. `login` is the FoxOneMvpdLogin to use; a separate FOX One
    login (login.shared False) gets its own browser profile and never
    reads or writes the shared TV-provider account.
    """
    from app.scrapers.fox_one import OWN_PROFILE_DIR, SHARED_PROFILE_DIR

    mso_id = login.mso_id
    try:
        from camoufox.sync_api import Camoufox
    except ImportError:
        set_status('error', 'Camoufox is not installed on this container')
        return

    import os as _os_login
    # A separate FOX One login gets its own profile (see OWN_PROFILE_DIR).
    # Otherwise: the isolated, cookie-permissive profile for YouTubeTV —
    # shared with the other browser-login flows (see
    # _YOUTUBETV_ISOLATED_PROFILE_DIR's docstring in common.py).
    if not login.shared:
        profile_dir = OWN_PROFILE_DIR
    elif mso_id == 'YouTubeTV':
        profile_dir = _YOUTUBETV_ISOLATED_PROFILE_DIR
    else:
        profile_dir = SHARED_PROFILE_DIR
    try:
        _os_login.makedirs(profile_dir, exist_ok=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning('[foxone-mvpd-login] could not create profile dir %s: %s', profile_dir, exc)

    try:
        session, request_id, device_id, mso_login_url, page_response = scraper._foxone_mvpd_register(mso_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception('[foxone-mvpd-login] unexpected failure registering session')
        _record_foxone_result(source.id, scraper, login, str(exc))
        set_status('error', f'FOX One: {exc}')
        return

    logger.info(
        '[foxone-mvpd-login] starting sign-in: provider=%s, %s login, profile=%s',
        mso_id, 'shared TV-provider' if login.shared else 'separate FOX One', profile_dir,
    )
    nav_url = mso_login_url or str(page_response.url)

    _PER_LOGIN_TIMEOUT_SECONDS = 150
    # FOX's own site calls requests/complete once, after the browser lands
    # on its callback page (auth.fox.com/foxone/mvpd/callback, then
    # www.fox.com/callback). Until then FOX answers 404 ("no completed
    # request yet"), so poll every few seconds only once that page has been
    # reached; before that, a slow fallback poll covers a missed callback.
    _POLL_SECONDS = 3.0
    _FALLBACK_POLL_SECONDS = 15.0
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
            _watch_spectrum_auth_results(page, 'foxone-mvpd-login')
            if login.shared:
                # The saved Google session belongs to the shared account.
                _prime_google_session(context, mso_id)
            page.on('crash', lambda p: logger.warning('[foxone-mvpd-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
            page.on('close', lambda p: logger.warning('[foxone-mvpd-login] page CLOSE event fired'))
            page.on('pageerror', lambda exc: logger.warning('[foxone-mvpd-login] page JS error: %s', str(exc)[:500]))

            youtube_gateway_responses = []
            reached_fox_callback = {'v': False}

            def _log_navigation_response(response):
                try:
                    if not response.request.is_navigation_request():
                        return
                    response_url = response.url
                    response_parts = _urlsplit(response_url)
                    if _is_fox_callback(response_parts):
                        reached_fox_callback['v'] = True
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
                if mso_id == 'Comcast_SSO':
                    # Xfinity's WAF (Akamai) flatly denies a cold top-level
                    # navigation to the Adobe Pass authenticate/saml URL —
                    # same wall fox.py/mvpd.py hit and work around; see
                    # run_mvpd_browser_login's comment on this exact pattern
                    # for the full explanation. Landing on a real page first
                    # (same FOX-brand URL fox.py already uses) and
                    # redirecting via in-page JS (real Referer/Sec-Fetch-Site
                    # chain) sails through instead.
                    page.goto('https://www.foxsports.com/live/fs1', wait_until='domcontentloaded', timeout=30000)
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
            if login.username and login.password and mso_id == 'Comcast_SSO':
                _autofill_xfinity_credentials(
                    page, login.username, login.password, r=r,
                    stop_key=MVPD_BROWSER_LOGIN_STOP_KEY, input_key=MVPD_BROWSER_LOGIN_INPUT_KEY,
                )
            elif login.username and login.password and mso_id != 'YouTubeTV':
                # Every other generic MSO needs the same credential-form
                # autofill Discovery/AMCN got 2026-09-18 — confirmed live
                # 2026-09-24 this copy never had it: a Cox account routed to
                # Spectrum's login page sat on the empty form until timeout.
                _try_autofill_credentials(
                    page, login.username, login.password, r=r,
                    stop_key=MVPD_BROWSER_LOGIN_STOP_KEY, input_key=MVPD_BROWSER_LOGIN_INPUT_KEY,
                    navigation_already_settled=True, log_tag='foxone-mvpd-login',
                )
            set_status('running', 'Signing in to FOX One…', landing_url)

            wait_started = time.monotonic()
            deadline = wait_started + _PER_LOGIN_TIMEOUT_SECONDS
            last_shot = 0.0
            last_poll = 0.0
            cancelled = False
            idid_message = None
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
                idid_message = _spectrum_signin_error_message(page, 'FOX One', mso_id)
                if idid_message:
                    break
                at_callback = reached_fox_callback['v'] or _is_fox_callback(_urlsplit(_safe_page_url(page)))
                if now - last_poll > (_POLL_SECONDS if at_callback else _FALLBACK_POLL_SECONDS):
                    last_poll = now
                    try:
                        access_token, expires_at = scraper._foxone_mvpd_finish(session, request_id, device_id, mso_id)
                    except Exception as exc:  # noqa: BLE001
                        if at_callback:
                            logger.info('[foxone-mvpd-login] completion check after FOX callback failed: %s', exc)
                        else:
                            logger.debug('[foxone-mvpd-login] completion not ready yet: %s', exc)
                        continue
                    break
                page.wait_for_timeout(80)

            if cancelled:
                set_status('stopped', 'Cancelled')
                return
            if idid_message:
                _record_foxone_result(source.id, scraper, login, idid_message)
                set_status('error', idid_message)
                return
            if not access_token:
                _log_signin_timeout_snapshot(page, 'foxone-mvpd-login')
                set_status('error', 'FOX One: timed out waiting for sign-in to complete.')
                return
            if not login.shared:
                # The Google token / Xfinity cookie jar below are saved onto
                # the shared account, so a separate login keeps them out.
                pass
            elif mso_id == 'YouTubeTV':
                _maybe_capture_google_master_token(context, mso_id)
            elif mso_id == 'Comcast_SSO':
                # Same idea as the YouTubeTV branch above, for the Xfinity
                # cookie jar instead of a Google master_token — see
                # _harvest_and_save_xfinity_cookies's docstring. Was missing
                # here entirely (unlike mvpd.py/nbc.py/fox.py), so a fully
                # successful FOX One browser login never saved anything for
                # other TVE families' cookie-jar fast path.
                _harvest_and_save_xfinity_cookies(context)
    except BaseException as exc:  # noqa: BLE001
        if isinstance(exc, SpectrumWantsCoxProvider):
            if login.shared:
                if _spectrum_retry_as_cox(exc, mso_id, 'FOX One', set_status):
                    return _run_foxone_browser_assisted_login(r, set_status, source, replace(login, mso_id='Cox'), scraper)
                return
            if mso_id != 'Spectrum':
                set_status('error', f'FOX One: Spectrum returned IDLI-4213 ("select Cox Spectrum") even '
                                    f'though the TV provider is already set to {mso_id}.')
                return
            # Same switch as _spectrum_retry_as_cox, but saved on FOX One's
            # own login instead of the shared account.
            scraper._update_config('mvpd_provider_id', 'Cox')
            with flask_app.app_context():
                persist_source_config_updates(source.id, scraper._pending_config_updates)
            set_status('starting', 'Spectrum says this account signs in as "Cox Spectrum" — switched '
                                   'FOX One\'s TV provider and retrying…')
            return _run_foxone_browser_assisted_login(r, set_status, source, replace(login, mso_id='Cox'), scraper)
        if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
            set_status('stopped', 'Cancelled')
            return
        logger.exception('[foxone-mvpd-login] browser-assisted session failed')
        set_status('error', f'Browser session failed: {exc}')
        return

    scraper._update_config('access_token', access_token)
    scraper._update_config('access_expires_at', expires_at)
    scraper._update_config('access_token_captured_at', int(time.time()))
    _record_foxone_result(source.id, scraper, login, None, how=' (browser-assisted)')
    set_status('success', f'Signed in — FOX One authorized via {mso_id}.')
    logger.info('[foxone-mvpd-login] paired mso_id=%s (browser-assisted)', mso_id)


def run_foxone_browser_login(*_legacy_args):
    """Standalone "Sign in" for FOX One (from its card on the Sources page).

    Signs in with whichever TV-provider login FOX One is set up for (see
    FoxOneScraper._mvpd_login). Comcast_SSO on the shared account tries a
    saved cookie jar first; everything else (including Cox, which signs in
    on Spectrum's page now) goes through the browser-assisted flow.

    _legacy_args swallows the mso_id older queued jobs were enqueued with.
    """
    # Manual push/pop instead of `with flask_app.app_context():` — see
    # _prime_google_session's docstring: Camoufox's own rendering breaks
    # intermittently while a Flask app_context is active on this thread.
    # Popped right before handing off to _run_foxone_browser_assisted_login
    # (which launches Camoufox and pushes its own fresh, short-lived context
    # for the DB writes it still needs afterward) and never re-pushed —
    # the cookie-jar path below returns before the pop on success.
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
            logger.warning('[foxone-mvpd-login] Redis unavailable, aborting: %s', exc)
            return
        _activity_handler = install_browser_login_activity_log(r)

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

        from app.scrapers.fox_one import OWN_PROFILE_DIR, FoxOneScraper
        from app.tve.providers import unsupported_network_reason

        source = Source.query.filter_by(name='fox_one').first()
        if not source:
            set_status('error', 'FOX One source not found.')
            return
        scraper = FoxOneScraper(config=dict(source.config or {}))
        login = scraper._mvpd_login()
        if not login:
            set_status('error', (
                'FOX One: choose a TV provider for its separate login first.'
                if scraper._signin_method() == 'own'
                else 'FOX One: set up your TV provider under Settings > TV Everywhere first.'
            ))
            return
        reason = unsupported_network_reason('foxone', login.mso_id)
        if reason:
            set_status('error', reason)
            return

        if not login.shared and scraper.config.get('reset_browser_profile'):
            # The separate login's credentials/provider changed since the
            # last sign-in — start from an empty profile so the old account's
            # remembered session can't carry over.
            import shutil
            shutil.rmtree(OWN_PROFILE_DIR, ignore_errors=True)
            scraper._update_config('reset_browser_profile', False)
            persist_source_config_updates(source.id, scraper._pending_config_updates)
            logger.info('[foxone-mvpd-login] cleared FOX One browser profile after a login change')

        if login.mso_id == 'Comcast_SSO' and login.cookie_jar:
            # Try a saved cookie jar (harvested from a previous successful
            # Comcast_SSO browser pairing for ANY TVE family — see
            # _harvest_and_save_xfinity_cookies) BEFORE ever opening a
            # browser, same as mvpd.py/nbc.py/fox.py already do. Confirmed
            # live 2026-08-28: _authenticate_via_mvpd works unmodified for
            # Comcast_SSO once a jar exists — it's the same login_to_mvpd()
            # dispatcher.
            set_status('running', 'Trying saved sign-in (no browser needed)…')
            try:
                access_token, expires_at = scraper._authenticate_via_mvpd(
                    login.mso_id, login.username, login.password, login.cookie_jar,
                )
            except Exception as exc:  # noqa: BLE001
                logger.info(
                    '[foxone-mvpd-login] saved xfinity cookie jar did not work, falling back to browser: %s', exc,
                )
            else:
                scraper._update_config('access_token', access_token)
                scraper._update_config('access_expires_at', expires_at)
                scraper._update_config('access_token_captured_at', int(time.time()))
                scraper.record_signin_result(login, None, how=' (no browser needed)')
                persist_source_config_updates(source.id, scraper._pending_config_updates)
                set_status('success', 'Signed in — FOX One authorized (no browser needed).')
                logger.info('[foxone-mvpd-login] paired mso_id=%s via saved cookie jar (no browser)', login.mso_id)
                return
            set_status('running', 'No usable saved sign-in — opening a browser…')

        # persist_source_config_updates commits, expiring loaded ORM rows;
        # touch what the browser flow reads before popping the context.
        _ = (source.id,)
        _ctx.pop()
        _ctx_popped['v'] = True
        _run_foxone_browser_assisted_login(r, set_status, source, login, scraper)
    finally:
        uninstall_browser_login_activity_log(_activity_handler)
        if not _ctx_popped['v']:
            _ctx.pop()

"""NBC (Adobe Pass v2) TVE browser-assisted login."""
import logging
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit as _urlsplit
from rq import get_current_job
import redis

from app.worker import flask_app
from app.extensions import db
from app.models import Source, TVEAccount
from app.config_store import persist_source_cache_updates, persist_source_config_updates
from app.tve.adobe_pass import (
    TVEAuthError,
    TVENotAuthorizedError,
)
from app.tve.browser_login.common import (
    _safe_page_url,
    _same_page_url,
    _settle_after_mvpd_navigation,
    _try_autofill_credentials,
    _autofill_xfinity_credentials,
    _apply_sling_browser_login_input,
    _harvest_and_save_xfinity_cookies,
    _record_tve_login_error,
    _prime_google_session,
    _maybe_capture_google_master_token,
    _relay_input_and_screenshot,
    _sling_f5_recover,
    _url_for_log,
    _gateway_url_for_log,
    _youtube_tv_gateway_failure_message,
    _maybe_retry_youtubetv_from_scratch,
    _log_youtubetv_gateway_diagnostics,
    _YOUTUBETV_ISOLATED_PROFILE_DIR,
    _YOUTUBETV_CAMOUFOX_FIREFOX_PREFS,
    _BROWSER_LOGIN_MAX_ATTEMPTS,
    _BrowserSessionDied,
    _is_browser_death,
    install_browser_login_activity_log,
    uninstall_browser_login_activity_log,
)

logger = logging.getLogger(__name__)


NBC_BROWSER_LOGIN_STATUS_KEY = 'nbc-mvpd:browser-login:status'
NBC_BROWSER_LOGIN_SHOT_KEY = 'nbc-mvpd:browser-login:screenshot'
NBC_BROWSER_LOGIN_INPUT_KEY = 'nbc-mvpd:browser-login:input'
NBC_BROWSER_LOGIN_STOP_KEY = 'nbc-mvpd:browser-login:stop'
NBC_BROWSER_LOGIN_HINT_KEY = 'nbc-mvpd:browser-login:hint'
_NBC_BROWSER_LOGIN_TIMEOUT_SECONDS = 1800
# A flat 2s poll for up to 30 minutes is up to ~900 requests to
# /api/v2/<requestor_id>/profiles/<mso_id> in one human-operated attempt.
# NBC grows the interval while nothing changes and resets it after a real page
# navigation, keeping completion responsive without unnecessary Adobe traffic.
# Live-validated 2026-08-19 (a full YouTubeTV sign-in completed cleanly);
# mvpd.py now has the same pattern — see its own constants' docstring.
_NBC_SESSION_POLL_SECONDS = 2.0
_NBC_SESSION_POLL_MAX_SECONDS = 20.0
_NBC_SESSION_POLL_BACKOFF = 1.5


def _save_nbc_mvpd_auth(mso_id: str, access_token: str, device_fingerprint: str) -> None:
    """Pushes its own app_context — see _prime_google_session's docstring.
    Called mid-browser-session, after run_nbc_browser_login has already
    popped its outer one before launching Camoufox."""
    with flask_app.app_context():
        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if not account:
            return
        cfg = dict(account.config or {})
        cfg['nbc_mvpd_auth'] = {
            'mso_id': mso_id,
            'access_token': access_token,
            'device_fingerprint': device_fingerprint,
            'captured_at': int(time.time()),
        }
        account.config = cfg
        db.session.commit()


def run_nbc_browser_login(mso_id: str, _attempt: int = 1, _deadline: float | None = None):
    """Drive a real, human-operated sign-in for NBC TVE's Adobe Pass v2 flow.

    Same "second screen" idea as run_mvpd_browser_login, adapted to nbc.com's
    JSON REST Adobe Pass v2 API (app/scrapers/nbc_tve.py's AdobePassV2Client)
    instead of the legacy XML protocol — different endpoints (POST /sessions
    instead of regcode, GET /profiles/<mvpd> instead of /adobe-services/session),
    but the same underlying design: client registration + session creation are
    scripted (never blocked), a real browser completes the MVPD's own login
    page, and this process polls /profiles/<mvpd> independently using the same
    access_token + device fingerprint — confirmed live (2026-08-05) that this
    poll works from an entirely separate HTTP session as long as those two
    values match, so it doesn't matter that the browser and this process are
    different clients.

    Unlike the legacy protocol's authn_token, it's untested how long NBC's v2
    "authenticated" state survives reuse of the same access_token/device
    fingerprint across resolve() calls — cached in TVEAccount.config either
    way; if it stops working, resolve() will surface a clear error and this
    flow needs to be re-run.
    """
    # Manual push/pop instead of `with flask_app.app_context():` — see
    # _prime_google_session's docstring: Camoufox's own rendering breaks
    # intermittently while a Flask app_context is active on this thread
    # (confirmed live 2026-08-19 via single-variable bisection). This context
    # gets popped right before Camoufox launches and never re-pushed — every
    # exit path from that point on is a `return` anyway (nothing after needs
    # DB access), and every DB-touching helper called during the browser
    # session (see _save_nbc_mvpd_auth, _harvest_and_save_xfinity_cookies,
    # _maybe_capture_google_master_token, _prime_google_session) pushes its
    # own short-lived context instead of relying on this one. _ctx_popped
    # tracks whether that already happened so the final `finally` below
    # doesn't double-pop.
    _ctx = flask_app.app_context()
    _ctx.push()
    _ctx_popped = {'v': False}
    _activity_handler = None
    try:
        import json as _json_login
        from app.scrapers.nbc_tve import NbcTveScraper, AdobePassV2Client, REQUESTOR_ID, DEFAULT_REDIRECT_URL, ADOBE_BASE as ADOBE_BASE_NBC

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[nbc-mvpd-login] Redis unavailable, aborting: %s', exc)
            return
        _activity_handler = install_browser_login_activity_log(r)

        # Same teardown-clobber guard as run_mvpd_browser_login: a dead
        # browser's `with` teardown can raise while unwinding a successful
        # return and must not replace the real terminal status.
        _terminal_status_set = {'v': False}

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    NBC_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': url}),
                )
                if state in ('success', 'error', 'stopped'):
                    _terminal_status_set['v'] = True
            except Exception:  # noqa: BLE001
                pass

        r.delete(
            NBC_BROWSER_LOGIN_STATUS_KEY,
            NBC_BROWSER_LOGIN_SHOT_KEY,
            NBC_BROWSER_LOGIN_INPUT_KEY,
            NBC_BROWSER_LOGIN_STOP_KEY,
            NBC_BROWSER_LOGIN_HINT_KEY,
        )
        logger.info('[nbc-mvpd-login] starting attempt=%d mso_id=%s; cleared prior modal state', _attempt, mso_id)

        if mso_id == 'Cox':
            # Cox's login step is already fully scripted: NbcTveScraper.
            # _ensure_entitled() calls AdobePassV2Client.authorize(), which
            # does the direct login.cox.com/api/v1/authn POST (_cox_saml_login,
            # shared with fox_tve.py) on every entitlement refresh — same
            # pattern as resolve()'s own normal playback path. No browser
            # needed; confirmed live 2026-08-11 (full authorize+preauthorize
            # round trip with the real Cox account, zero Camoufox). Only
            # non-Cox MSOs fall through to the browser-assisted flow below.
            set_status('running', 'Signing in to NBC TVE…')
            source = Source.query.filter_by(name='nbc_tve').first()
            if not source:
                set_status('error', 'NBC TVE source not found.')
                return
            scraper = NbcTveScraper(config=dict(source.config or {}))
            try:
                guide = scraper._fetch_guide()
                if not guide:
                    set_status('error', 'NBC TVE: could not load channel guide.')
                    return
                resource_id = next(iter(guide.values())).resource_id
                # Force a fresh entitlement check — _ensure_entitled() short-
                # circuits on a still-fresh cached decision, which would make
                # a deliberate "Sign in" click silently no-op.
                scraper._update_cache('nbc_entitlements', {})
                scraper._ensure_entitled(resource_id)
            # Deliberately NOT using _cox_login_error_detail() here (unlike
            # the legacy/FOX Cox branches, code review 2026-08-11) —
            # _ensure_entitled()'s own exceptions already carry full context
            # ("NBC TVE: <mso_id> is not authorized: <reason>"), so running
            # them through that classifier too would double up the framing
            # instead of clarifying it. See that function's docstring.
            except (TVENotAuthorizedError, TVEAuthError) as exc:
                persist_source_config_updates(source.id, scraper._pending_config_updates)
                persist_source_cache_updates(source.id, scraper._pending_cache_updates)
                _record_tve_login_error('nbc', str(exc))
                set_status('error', f'NBC TVE: {exc}')
                return
            except Exception as exc:  # noqa: BLE001
                logger.exception('[nbc-mvpd-login] unexpected failure')
                _record_tve_login_error('nbc', str(exc))
                set_status('error', f'NBC TVE: {exc}')
                return
            persist_source_config_updates(source.id, scraper._pending_config_updates)
            persist_source_cache_updates(source.id, scraper._pending_cache_updates)
            set_status('success', 'Signed in — NBC TVE authorized.')
            logger.info('[nbc-mvpd-login] paired mso_id=Cox (scripted, no browser)')
            return

        if mso_id == 'Comcast_SSO':
            # Same idea as the Cox branch above, but via a saved cookie jar
            # (harvested from a previous successful Comcast_SSO browser
            # pairing — see _harvest_and_save_xfinity_cookies and
            # app/tve/adobe_pass.py's xfinity_cookie_jar_login()) instead of
            # a scripted credential POST — Xfinity's own login page blocks
            # scripted credential submission outright (Akamai Bot Manager),
            # confirmed live 2026-08-14, but a jar matured by a real browser
            # gets straight through over plain HTTP. Falls through to the
            # browser-assisted flow below only when there's no jar yet, or
            # the saved one has gone stale (TVEAuthError) — a definitive
            # TVENotAuthorizedError is NOT retried via browser, same as
            # every other MSO fast-path in this file, since a browser login
            # can't change Adobe's actual entitlement decision.
            cookie_jar_account = TVEAccount.query.filter_by(provider_id='mvpd').first()
            cookie_jar = (cookie_jar_account.config or {}).get('xfinity_cookie_jar') if cookie_jar_account else None
            if cookie_jar:
                set_status('running', 'Trying saved sign-in (no browser needed)…')
                source = Source.query.filter_by(name='nbc_tve').first()
                if source:
                    scraper = NbcTveScraper(config=dict(source.config or {}))
                    try:
                        guide = scraper._fetch_guide()
                        if not guide:
                            raise TVEAuthError('could not load channel guide')
                        resource_id = next(iter(guide.values())).resource_id
                        scraper._update_cache('nbc_entitlements', {})
                        scraper._ensure_entitled(resource_id)
                    except TVENotAuthorizedError as exc:
                        persist_source_config_updates(source.id, scraper._pending_config_updates)
                        persist_source_cache_updates(source.id, scraper._pending_cache_updates)
                        _record_tve_login_error('nbc', str(exc))
                        set_status('error', f'NBC TVE: {exc}')
                        return
                    except TVEAuthError as exc:
                        persist_source_config_updates(source.id, scraper._pending_config_updates)
                        persist_source_cache_updates(source.id, scraper._pending_cache_updates)
                        logger.info('[nbc-mvpd-login] saved xfinity cookie jar did not work, falling back to browser: %s', exc)
                    else:
                        persist_source_config_updates(source.id, scraper._pending_config_updates)
                        persist_source_cache_updates(source.id, scraper._pending_cache_updates)
                        set_status('success', 'Signed in — NBC TVE authorized (no browser needed).')
                        logger.info('[nbc-mvpd-login] paired mso_id=Comcast_SSO via saved cookie jar (no browser)')
                        return
            set_status('running', 'No usable saved sign-in — opening a browser…')

        set_status('starting', 'Registering with Adobe Pass…')

        # One deadline shared across browser-crash retries (RQ job_timeout is
        # only ~30s above _NBC_BROWSER_LOGIN_TIMEOUT_SECONDS, so a retry must
        # never reset the clock).
        deadline = _deadline if _deadline is not None else time.monotonic() + _NBC_BROWSER_LOGIN_TIMEOUT_SECONDS

        account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
        mvpd_username = (account_row.username if account_row else '') or ''
        mvpd_password = (account_row.password if account_row else '') or ''

        source = Source.query.filter_by(name='nbc_tve').first()
        scraper = NbcTveScraper(config=dict((source.config if source else {}) or {}))

        try:
            page_config = scraper._discover_page_config()
            device_fingerprint = scraper._ensure_device_fingerprint()
            if source:
                persist_source_config_updates(source.id, scraper._pending_config_updates)
        except Exception as exc:  # noqa: BLE001
            set_status('error', f'Could not discover NBC page config: {exc}')
            return

        from app.tve.adobe_pass import load_cached_adobe_client_creds, save_adobe_client_creds
        nbc_client_creds = load_cached_adobe_client_creds(account_row, REQUESTOR_ID) if account_row else None
        client = AdobePassV2Client(
            REQUESTOR_ID, page_config['software_statement'], DEFAULT_REDIRECT_URL, device_fingerprint,
            client_creds=nbc_client_creds,
        )
        try:
            client._register_client()
            if not nbc_client_creds and account_row:
                save_adobe_client_creds(account_row, REQUESTOR_ID, client.client_id, client.client_secret, client.access_token)
            r_sessions = client._post(
                f'{ADOBE_BASE_NBC}/api/v2/{client.requestor_id}/sessions',
                data={'mvpd': mso_id, 'redirectUrl': client.redirect_url, 'domainName': 'nbc.com'},
                headers={**client._bearer_headers(), 'Content-Type': 'application/x-www-form-urlencoded'},
            )
            if not r_sessions.ok:
                try:
                    detail = r_sessions.json().get('message') or r_sessions.text[:300]
                except ValueError:
                    detail = r_sessions.text[:300]
                set_status('error', f'Adobe session request failed for MVPD {mso_id}: {detail}')
                return
            auth_path = r_sessions.json().get('url')
            if not auth_path:
                set_status('error', 'Adobe Pass v2: sessions call did not return an authenticate url.')
                return
            # Not every MVPD's authenticate endpoint answers with an HTTP
            # redirect here — confirmed live 2026-08-17 for YouTubeTV: it
            # returns a 200 HTML auto-submit SAMLRequest form instead (same
            # shape as DIRECTV's idp.dtvce.com, see
            # app/tve/mvpd/directv.py's directv_login() docstring). A real
            # browser handles either shape identically via a normal
            # navigation (follows a 3xx itself, executes the form's onload
            # auto-submit JS if there isn't one) — so just hand the browser
            # this URL directly rather than pre-resolving it scripted here
            # and erroring out whenever there's no Location header to find.
            # Confirmed live: this exact URL, fetched scripted, comes back
            # as the auto-submit form (not an error page), so it's a genuine
            # landing page, not a dead end.
            mso_login_url = f'{ADOBE_BASE_NBC}{auth_path}'
            logger.info(
                '[nbc-mvpd-login] Adobe session ready requestor_id=%s HTTP %s cached_client=%s authenticate=%s',
                client.requestor_id, r_sessions.status_code, bool(nbc_client_creds), _url_for_log(mso_login_url),
            )
        except TVEAuthError as exc:
            set_status('error', f'Adobe Pass registration failed: {exc}')
            return

        def _grace_poll_pairing(reason: str) -> bool:
            """The pairing completes SERVER-side, and the MSO completion page
            (Cox's Okta widget, observed 3-for-3 on 2026-08-06) calls
            window.close() on itself ~1.5s after posting credentials — so the
            browser usually dies at the exact moment the flow is FINISHING.
            Before treating a dead page as a failed attempt (which would
            relaunch and re-submit a real MSO login — a login-storm risk),
            poll for completion browser-free. Returns True if it completed
            (terminal status already set)."""
            logger.info('[nbc-mvpd-login] page gone (%s) — polling for server-side completion', reason)
            grace_deadline = min(time.monotonic() + 30, deadline)
            while time.monotonic() < grace_deadline:
                if r.exists(NBC_BROWSER_LOGIN_STOP_KEY):
                    return False
                try:
                    r_profile = client._get(
                        f'{ADOBE_BASE_NBC}/api/v2/{client.requestor_id}/profiles/{mso_id}',
                        headers=client._bearer_headers(),
                    )
                    profile = ((r_profile.json() or {}).get('profiles') or {}).get(mso_id)
                except Exception:  # noqa: BLE001
                    profile = None
                if profile:
                    _save_nbc_mvpd_auth(mso_id, client.access_token, device_fingerprint)
                    if mso_id == 'Comcast_SSO':
                        _harvest_and_save_xfinity_cookies(context)
                    elif mso_id == 'YouTubeTV':
                        _maybe_capture_google_master_token(context, mso_id)
                    set_status('success', f'Signed in — NBC TVE authorized via {mso_id}.')
                    logger.info('[nbc-mvpd-login] paired mso_id=%s (completed after the page closed itself)', mso_id)
                    return True
                time.sleep(2)
            return False

        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            set_status('error', 'Camoufox is not installed on this container')
            return

        # Keep the YouTube TV sign-in isolated from the browser profile used
        # by every other TVE scraper (shared with run_mvpd_browser_login's
        # own YouTubeTV branch — see _YOUTUBETV_ISOLATED_PROFILE_DIR's
        # docstring in common.py). Google is still primed from the saved
        # master_token below, so NBC does not depend on cross-scraper
        # cookies in the shared profile.
        profile_dir = (
            _YOUTUBETV_ISOLATED_PROFILE_DIR
            if mso_id == 'YouTubeTV'
            else '/data/browser_profiles/mvpd_tve'
        )
        try:
            import os as _os_login
            _os_login.makedirs(profile_dir, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[nbc-mvpd-login] could not create profile dir %s: %s', profile_dir, exc)

        _ctx.pop()
        _ctx_popped['v'] = True
        camoufox_options = {
            'headless': 'virtual',
            'os': 'windows',
            'persistent_context': True,
            'user_data_dir': profile_dir,
            'window': (1280, 800),
        }
        if mso_id == 'YouTubeTV':
            # See _YOUTUBETV_CAMOUFOX_FIREFOX_PREFS's docstring in common.py.
            camoufox_options['firefox_user_prefs'] = _YOUTUBETV_CAMOUFOX_FIREFOX_PREFS
            logger.info(
                '[nbc-mvpd-login] using isolated YouTubeTV profile with cross-site cookies enabled'
            )
        try:
            with Camoufox(**camoufox_options) as context:
                page = context.pages[0] if context.pages else context.new_page()
                google_session_primed = _prime_google_session(context, mso_id)
                if mso_id == 'YouTubeTV':
                    logger.info('[nbc-mvpd-login] Google session priming result=%s', 'primed' if google_session_primed else 'not-available')
                page.on('crash', lambda p: logger.warning('[nbc-mvpd-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
                page.on('close', lambda p: logger.warning('[nbc-mvpd-login] page CLOSE event fired'))
                page.on('pageerror', lambda exc: logger.warning('[nbc-mvpd-login] page JS error: %s', str(exc)[:500]))

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
                            # Retain the object only. Full headers/body are read
                            # after a stall so diagnostics cannot delay the relay.
                            youtube_gateway_responses.append(response)
                        logger.info(
                            '[nbc-mvpd-login] navigation response HTTP %s %s',
                            response.status,
                            _gateway_url_for_log(response_url)
                            if is_gateway_bookend
                            else _url_for_log(response_url),
                        )
                    except Exception:  # noqa: BLE001
                        pass

                page.on('response', _log_navigation_response)
                try:
                    if mso_id == 'Comcast_SSO':
                        # Same Akamai cold-navigation wall as the legacy
                        # family's Comcast_SSO path — see
                        # run_mvpd_browser_login's comment on this exact
                        # pattern for the full explanation.
                        origin = f'{_urlsplit(client.redirect_url).scheme}://{_urlsplit(client.redirect_url).netloc}'
                        page.goto(origin, wait_until='domcontentloaded', timeout=30000)
                        _settle_deadline = time.monotonic() + 3.0
                        while time.monotonic() < _settle_deadline:
                            _relay_input_and_screenshot(
                                page, r, stop_key=NBC_BROWSER_LOGIN_STOP_KEY, input_key=NBC_BROWSER_LOGIN_INPUT_KEY,
                                shot_key=NBC_BROWSER_LOGIN_SHOT_KEY, hint_key=NBC_BROWSER_LOGIN_HINT_KEY,
                            )
                            page.wait_for_timeout(500)
                        page.evaluate('(u) => { window.location.href = u; }', mso_login_url)
                        _load_deadline = time.monotonic() + 30.0
                        while time.monotonic() < _load_deadline:
                            try:
                                page.wait_for_load_state('domcontentloaded', timeout=1000)
                                break
                            except Exception:  # noqa: BLE001
                                pass
                            _relay_input_and_screenshot(
                                page, r, stop_key=NBC_BROWSER_LOGIN_STOP_KEY, input_key=NBC_BROWSER_LOGIN_INPUT_KEY,
                                shot_key=NBC_BROWSER_LOGIN_SHOT_KEY, hint_key=NBC_BROWSER_LOGIN_HINT_KEY,
                            )
                    else:
                        page.goto(mso_login_url, wait_until='domcontentloaded', timeout=30000)
                except Exception as exc:  # noqa: BLE001
                    if _is_browser_death(exc):
                        raise
                    set_status('error', f'Failed to load provider sign-in page: {exc}')
                    return
                if mso_id == 'YouTubeTV':
                    try:
                        _maybe_retry_youtubetv_from_scratch(page, youtube_gateway_responses, mso_login_url)
                    except Exception as exc:  # noqa: BLE001
                        if _is_browser_death(exc):
                            raise
                        logger.warning(
                            '[nbc-mvpd-login] YouTubeTV retry-from-scratch recovery failed; '
                            'continuing to normal stall detection: %s',
                            exc,
                        )
                # See _settle_after_mvpd_navigation's docstring: a
                # page.screenshot() call during Adobe/YouTubeTV's still-in-
                # flight SAML bounce chain silently cancels it. Unconditional
                # (not just inside _try_autofill_credentials below) so the
                # no-saved-credentials path doesn't leave the main poll
                # loop's first screenshot unprotected.
                settled = _settle_after_mvpd_navigation(
                    page, set_status=set_status,
                    respect_youtubetv_soft_block=mso_id != 'YouTubeTV',
                )
                landing_url = _safe_page_url(page)
                logger.info(
                    '[nbc-mvpd-login] provider redirect settle result=%s landing=%s',
                    'settled' if settled else 'stalled', _url_for_log(landing_url),
                )
                if not settled:
                    if mso_id == 'YouTubeTV':
                        _log_youtubetv_gateway_diagnostics(context, youtube_gateway_responses)
                        message = _youtube_tv_gateway_failure_message()
                    else:
                        message = (
                            'The provider sign-in redirect did not finish within 15 seconds. '
                            'NBC stopped before displaying an incomplete or blank login page; try again later.'
                        )
                    logger.warning(
                        '[nbc-mvpd-login] aborting stalled provider redirect before autofill/screenshot polling '
                        '(mso_id=%s landing=%s gateway_responses=%d)',
                        mso_id, _url_for_log(landing_url), len(youtube_gateway_responses),
                    )
                    set_status('error', message)
                    return

                landing_parts = _urlsplit(landing_url)
                if (
                    mso_id == 'YouTubeTV'
                    and landing_parts.netloc == 'support.google.com'
                    and landing_parts.path.startswith('/accounts/answer/32050')
                ):
                    logger.warning(
                        '[nbc-mvpd-login] Google rejected the primed browser session and redirected to its '
                        'cookie-recovery page (%s)', _url_for_log(landing_url),
                    )
                    set_status(
                        'error',
                        'Google rejected the saved browser session. Use “Sign in with Google” again, then retry NBC.',
                    )
                    return
                if _same_page_url(_safe_page_url(page), client.redirect_url):
                    set_status('error', f'{mso_id} does not appear to be a participating provider for NBC TVE.')
                    return
                if mvpd_username and mvpd_password:
                    if mso_id == 'Comcast_SSO':
                        _autofill_xfinity_credentials(
                            page, mvpd_username, mvpd_password, r=r,
                            stop_key=NBC_BROWSER_LOGIN_STOP_KEY, input_key=NBC_BROWSER_LOGIN_INPUT_KEY,
                            shot_key=NBC_BROWSER_LOGIN_SHOT_KEY, hint_key=NBC_BROWSER_LOGIN_HINT_KEY,
                        )
                    else:
                        _try_autofill_credentials(
                            page, mvpd_username, mvpd_password, r=r,
                            stop_key=NBC_BROWSER_LOGIN_STOP_KEY, input_key=NBC_BROWSER_LOGIN_INPUT_KEY,
                            shot_key=NBC_BROWSER_LOGIN_SHOT_KEY, hint_key=NBC_BROWSER_LOGIN_HINT_KEY,
                            navigation_already_settled=True,
                        )
                set_status('running', 'Sign in below, including any captcha if shown.', page.url)

                last_shot = 0.0
                last_heartbeat = 0.0
                last_poll = 0.0
                last_progress_log = 0.0
                consecutive_failures = 0
                f5_retried = False
                session_poll_interval = _NBC_SESSION_POLL_SECONDS
                last_seen_page_url = _safe_page_url(page)
                current_job = get_current_job()
                _MAX_CONSECUTIVE_FAILURES = 15
                while time.monotonic() < deadline:
                    if r.exists(NBC_BROWSER_LOGIN_STOP_KEY):
                        set_status('stopped', 'Cancelled')
                        return
                    if page.is_closed():
                        if _grace_poll_pairing('page closed'):
                            return
                        if r.exists(NBC_BROWSER_LOGIN_STOP_KEY):
                            set_status('stopped', 'Cancelled')
                            return
                        raise _BrowserSessionDied('browser page closed and pairing did not complete')

                    for _ in range(20):
                        raw = r.lpop(NBC_BROWSER_LOGIN_INPUT_KEY)
                        if raw is None:
                            break
                        try:
                            _apply_sling_browser_login_input(page, _json_login.loads(raw))
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[nbc-mvpd-login] input apply failed: %s', exc)

                    now = time.monotonic()
                    if current_job and now - last_heartbeat > 60:
                        try:
                            current_job.heartbeat(datetime.now(timezone.utc), 180)
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[nbc-mvpd-login] self-heartbeat failed: %s', exc)
                        last_heartbeat = now

                    if now - last_shot > 0.25:
                        try:
                            shot = page.screenshot(type='jpeg', quality=60)
                            r.setex(NBC_BROWSER_LOGIN_SHOT_KEY, 30, shot)
                            set_status('running', 'Sign in below, including any captcha if shown.', _safe_page_url(page))
                            consecutive_failures = 0
                        except Exception as exc:  # noqa: BLE001
                            consecutive_failures += 1
                            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                                logger.warning('[nbc-mvpd-login] page unresponsive after %d failed screenshots: %s', consecutive_failures, exc)
                                raise _BrowserSessionDied(f'page stopped answering screenshots: {exc}')
                        last_shot = now

                    current_page_url = _safe_page_url(page)
                    if current_page_url != last_seen_page_url:
                        last_seen_page_url = current_page_url
                        session_poll_interval = _NBC_SESSION_POLL_SECONDS

                    if now - last_poll > session_poll_interval:
                        last_poll = now
                        if not f5_retried and _sling_f5_recover(
                            page, mso_login_url, mvpd_username, mvpd_password, r=r,
                            stop_key=NBC_BROWSER_LOGIN_STOP_KEY, input_key=NBC_BROWSER_LOGIN_INPUT_KEY,
                            shot_key=NBC_BROWSER_LOGIN_SHOT_KEY, hint_key=NBC_BROWSER_LOGIN_HINT_KEY,
                        ):
                            f5_retried = True
                            continue
                        try:
                            r_profile = client._get(
                                f'{ADOBE_BASE_NBC}/api/v2/{client.requestor_id}/profiles/{mso_id}',
                                headers={
                                    **client._bearer_headers(),
                                    'Sec-Fetch-Site': 'cross-site',
                                    'Sec-Fetch-Mode': 'cors',
                                    'Sec-Fetch-Dest': 'empty',
                                },
                            )
                            profile = ((r_profile.json() or {}).get('profiles') or {}).get(mso_id)
                        except TVEAuthError as exc:
                            set_status('error', f'Adobe Pass error: {exc}')
                            return
                        if not profile:
                            session_poll_interval = min(
                                session_poll_interval * _NBC_SESSION_POLL_BACKOFF,
                                _NBC_SESSION_POLL_MAX_SECONDS,
                            )
                            if now - last_progress_log >= 60:
                                logger.info(
                                    '[nbc-mvpd-login] authorization still pending '
                                    '(profile_http=%s landing=%s next_poll=%.1fs)',
                                    r_profile.status_code, _url_for_log(current_page_url), session_poll_interval,
                                )
                                last_progress_log = now
                            continue  # human hasn't finished the MSO login yet
                        _save_nbc_mvpd_auth(mso_id, client.access_token, device_fingerprint)
                        if mso_id == 'Comcast_SSO':
                            _harvest_and_save_xfinity_cookies(context)
                        elif mso_id == 'YouTubeTV':
                            _maybe_capture_google_master_token(context, mso_id)
                        set_status('success', f'Signed in — NBC TVE authorized via {mso_id}.')
                        logger.info('[nbc-mvpd-login] paired mso_id=%s', mso_id)
                        return

                    page.wait_for_timeout(80)

                set_status('error', 'Timed out waiting for sign-in to complete.')
                return
        except BaseException as exc:  # noqa: BLE001
            if _terminal_status_set['v']:
                logger.info('[nbc-mvpd-login] ignoring cleanup-time exception after terminal status was already set: %s', exc)
                return
            if _is_browser_death(exc) and _grace_poll_pairing(str(exc)[:80]):
                return
            if (_is_browser_death(exc) and _attempt < _BROWSER_LOGIN_MAX_ATTEMPTS
                    and time.monotonic() < deadline - 30
                    and not r.exists(NBC_BROWSER_LOGIN_STOP_KEY)):
                logger.warning('[nbc-mvpd-login] browser died (attempt %d/%d), relaunching: %s', _attempt, _BROWSER_LOGIN_MAX_ATTEMPTS, exc)
                set_status('starting', 'Browser hiccuped — relaunching…')
                return run_nbc_browser_login(mso_id, _attempt=_attempt + 1, _deadline=deadline)
            if r.exists(NBC_BROWSER_LOGIN_STOP_KEY):
                logger.info('[nbc-mvpd-login] stopped by request (browser force-killed to interrupt a stuck wait)')
                try:
                    set_status('stopped', 'Cancelled.')
                except Exception:  # noqa: BLE001
                    pass
                return
            logger.exception('[nbc-mvpd-login] browser session failed')
            try:
                set_status('error', f'Browser session failed: {exc}')
            except Exception:  # noqa: BLE001
                pass
            return
    finally:
        uninstall_browser_login_activity_log(_activity_handler)
        if not _ctx_popped['v']:
            _ctx.pop()

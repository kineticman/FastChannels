"""FOX (api3.fox.com) TVE browser-assisted login."""
import logging
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit as _urlsplit
from rq import get_current_job
import redis

from app.worker import flask_app
from app.extensions import db
from app.models import TVEAccount
from app.tve.browser_login.common import (
    _safe_page_url,
    _same_page_url,
    _settle_after_mvpd_navigation,
    _try_autofill_credentials,
    _autofill_xfinity_credentials,
    _apply_sling_browser_login_input,
    _harvest_and_save_xfinity_cookies,
    _record_tve_login_error,
    _cox_login_error_detail,
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


FOX_BROWSER_LOGIN_STATUS_KEY = 'fox-mvpd:browser-login:status'
FOX_BROWSER_LOGIN_SHOT_KEY = 'fox-mvpd:browser-login:screenshot'
FOX_BROWSER_LOGIN_INPUT_KEY = 'fox-mvpd:browser-login:input'
FOX_BROWSER_LOGIN_STOP_KEY = 'fox-mvpd:browser-login:stop'
FOX_BROWSER_LOGIN_HINT_KEY = 'fox-mvpd:browser-login:hint'
_FOX_BROWSER_LOGIN_TIMEOUT_SECONDS = 1800
# See mvpd.py's identical constants' docstring — same growing-backoff poll,
# ported here 2026-08-20 after FOX hit the identical youtube.auth-gateway.net
# empty-bookend stall mvpd.py/nbc.py already had fixes for.
_FOX_SESSION_POLL_SECONDS = 2.0
_FOX_SESSION_POLL_MAX_SECONDS = 20.0
_FOX_SESSION_POLL_BACKOFF = 1.5


def run_fox_browser_login(mso_id: str, _attempt: int = 1, _deadline: float | None = None):
    """Drive a real, human-operated sign-in for FOX Sports TVE's Adobe Pass flow.

    Same "second screen" idea as run_mvpd_browser_login/run_nbc_browser_login,
    adapted to fox.com's own api3.fox.com REST flow (app/scrapers/fox_tve.py's
    _fox_sports_mvpd_token) — POST /accountregcode/v2 + /mvpdlogin (scripted,
    never blocked) instead of the legacy protocol's regcode, GET
    /checkadobeauthn/v2 instead of /adobe-services/session or /profiles/<mvpd>.
    Confirmed live (2026-08-05) that /checkadobeauthn/v2 returns 404 "Token Not
    Found" pre-completion and works identically from a separate HTTP session as
    long as the anon access_token + device_id match, so cross-client polling
    works the same way here too. On success, saves directly into the SAME
    account config keys _fox_sports_access_token() already checks
    (fox_sports_access_token/_exp/_mso), so no extra wiring is needed there.
    """
    # Manual push/pop instead of `with flask_app.app_context():` — see
    # _prime_google_session's docstring: Camoufox's own rendering breaks
    # intermittently while a Flask app_context is active on this thread.
    # Popped right before Camoufox launches and never re-pushed (every exit
    # path from there on is a `return`); DB-touching code during the browser
    # session (this function inlines its own account.config saves rather
    # than going through a shared helper — see the two `with
    # flask_app.app_context():` blocks below) pushes its own short-lived
    # context instead.
    _ctx = flask_app.app_context()
    _ctx.push()
    _ctx_popped = {'v': False}
    _activity_handler = None
    try:
        import json as _json_login
        import requests
        from app.scrapers.fox_tve import _fox_json_headers, _jwt_exp

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[fox-mvpd-login] Redis unavailable, aborting: %s', exc)
            return
        _activity_handler = install_browser_login_activity_log(r)

        # Same teardown-clobber guard as run_mvpd_browser_login: a dead
        # browser's `with` teardown can raise while unwinding a successful
        # return and must not replace the real terminal status.
        _terminal_status_set = {'v': False}

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    FOX_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': url}),
                )
                if state in ('success', 'error', 'stopped'):
                    _terminal_status_set['v'] = True
            except Exception:  # noqa: BLE001
                pass

        r.delete(FOX_BROWSER_LOGIN_STOP_KEY)
        r.delete(FOX_BROWSER_LOGIN_INPUT_KEY)

        if mso_id == 'Cox':
            # Cox's login step is already fully scripted via
            # fox_tve._fox_sports_mvpd_token() (the direct login.cox.com/
            # api/v1/authn POST, same _cox_saml_login used elsewhere). No
            # browser needed; confirmed live 2026-08-11. Only non-Cox MSOs
            # fall through to the browser-assisted flow below.
            #
            # Calls _fox_sports_mvpd_token() directly rather than going
            # through _fox_sports_access_token()'s cache-first path — same
            # reasoning as foxone_signin()'s own docstring: a "Sign in"
            # click should always exercise a live Cox login, not silently
            # return a still-valid cached token untested. That cache-first
            # wrapper also swallows its own exceptions and falls back to an
            # anonymous preview token instead of raising (the right call at
            # play time, wrong for this button — code review, 2026-08-11:
            # this button was reading the account-wide last_auth_status
            # afterward instead of the actual outcome, which a DIFFERENT
            # network's more recent attempt could have overwritten, and the
            # cache-hit path never touched that field at all).
            set_status('running', 'Signing in to FOX TVE…')
            import uuid as _uuid
            from app.scrapers.fox_tve import _fox_sports_mvpd_token
            account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
            if not account_row or not account_row.is_enabled or not account_row.has_credentials():
                set_status('error', 'TVE credentials are not configured in Settings.')
                return
            fox_session = requests.Session()
            try:
                token = _fox_sports_mvpd_token(fox_session, str(_uuid.uuid4()), mso_id, account_row.username or '', account_row.password or '')
            except Exception as exc:  # noqa: BLE001
                detail = _cox_login_error_detail(exc, 'FOX TVE')
                message = f'FOX Sports {mso_id} auth failed: {detail}'
                account_row.last_auth_status = 'error'
                account_row.last_auth_message = message[:500]
                account_row.last_auth_at = datetime.now(timezone.utc)
                db.session.commit()
                _record_tve_login_error('fox', detail)
                set_status('error', f'FOX TVE: {detail}')
                return
            now = int(time.time())
            cfg = dict(account_row.config or {})
            cfg['fox_sports_access_token'] = token
            cfg['fox_sports_access_token_exp'] = _jwt_exp(token) or (now + 3600)
            cfg['fox_sports_access_token_mso'] = mso_id
            cfg['fox_sports_access_token_captured_at'] = now
            account_row.config = cfg
            account_row.last_auth_status = 'ok'
            account_row.last_auth_message = f'FOX Sports MVPD token obtained through {mso_id}.'
            account_row.last_auth_at = datetime.now(timezone.utc)
            db.session.commit()
            set_status('success', 'Signed in — FOX TVE authorized.')
            logger.info('[fox-mvpd-login] paired mso_id=Cox (scripted, no browser)')
            return

        if mso_id == 'Comcast_SSO':
            # Same idea as the Cox branch above, but via a saved cookie jar
            # instead of a scripted credential POST — see
            # run_nbc_browser_login's identical block for the full
            # reasoning. Falls through to the browser-assisted flow below
            # only when there's no jar yet or the saved one has gone stale.
            cookie_jar_account = TVEAccount.query.filter_by(provider_id='mvpd').first()
            cookie_jar = (cookie_jar_account.config or {}).get('xfinity_cookie_jar') if cookie_jar_account else None
            if cookie_jar and cookie_jar_account and cookie_jar_account.is_enabled and cookie_jar_account.has_credentials():
                set_status('running', 'Trying saved sign-in (no browser needed)…')
                import uuid as _uuid
                from app.scrapers.fox_tve import _fox_sports_mvpd_token
                fox_session = requests.Session()
                try:
                    token = _fox_sports_mvpd_token(
                        fox_session, str(_uuid.uuid4()), mso_id,
                        cookie_jar_account.username or '', cookie_jar_account.password or '',
                        cookie_jar=cookie_jar,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.info('[fox-mvpd-login] saved xfinity cookie jar did not work, falling back to browser: %s', exc)
                else:
                    now = int(time.time())
                    cfg = dict(cookie_jar_account.config or {})
                    cfg['fox_sports_access_token'] = token
                    cfg['fox_sports_access_token_exp'] = _jwt_exp(token) or (now + 3600)
                    cfg['fox_sports_access_token_mso'] = mso_id
                    cfg['fox_sports_access_token_captured_at'] = now
                    cookie_jar_account.config = cfg
                    cookie_jar_account.last_auth_status = 'ok'
                    cookie_jar_account.last_auth_message = f'FOX Sports MVPD token obtained through {mso_id} (no browser needed).'
                    cookie_jar_account.last_auth_at = datetime.now(timezone.utc)
                    db.session.commit()
                    set_status('success', 'Signed in — FOX TVE authorized (no browser needed).')
                    logger.info('[fox-mvpd-login] paired mso_id=Comcast_SSO via saved cookie jar (no browser)')
                    return
            set_status('running', 'No usable saved sign-in — opening a browser…')

        set_status('starting', 'Registering with FOX…')

        # One deadline shared across browser-crash retries (RQ job_timeout is
        # only ~30s above _FOX_BROWSER_LOGIN_TIMEOUT_SECONDS, so a retry must
        # never reset the clock).
        deadline = _deadline if _deadline is not None else time.monotonic() + _FOX_BROWSER_LOGIN_TIMEOUT_SECONDS

        account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
        mvpd_username = (account_row.username if account_row else '') or ''
        mvpd_password = (account_row.password if account_row else '') or ''

        fox_redirect_url = 'https://www.foxsports.com/live/fs1'
        import uuid as _uuid
        session = requests.Session()
        device_id = str(_uuid.uuid4())
        try:
            anon = session.post('https://api3.fox.com/v2.0/login', headers=_fox_json_headers(), json={'deviceId': device_id}, timeout=30)
            anon.raise_for_status()
            anon_token = anon.json()['accessToken']
            headers = _fox_json_headers(anon_token)

            reg = session.post(
                'https://api3.fox.com/v2.0/accountregcode/v2', headers=headers,
                json={'deviceId': device_id, 'isRegister': False, 'isMvpd': True, 'selectedMvpdId': mso_id},
                timeout=30,
            )
            reg.raise_for_status()
            code = reg.json()['code']

            mvpd = session.post(
                f'https://api3.fox.com/v2.0/accountregcode/{code}/mvpdlogin', headers=headers,
                json={'mvpdId': mso_id, 'redirectUrl': fox_redirect_url},
                timeout=30,
            )
            mvpd.raise_for_status()
            auth_url = mvpd.json()['authenticateUrl']

            # See run_nbc_browser_login's identical fallback for the full
            # explanation — not every MVPD's authenticate endpoint answers
            # with an HTTP redirect (confirmed live 2026-08-17 for
            # YouTubeTV via NBC's own v2 REST family; api3.fox.com's own
            # flow is the same shape of Adobe Pass v2 API, so treat it the
            # same way rather than assuming a Location header always
            # exists). A real browser handles a bare 200 auto-submit
            # SAMLRequest form (DIRECTV/YouTubeTV shape) exactly like a 3xx
            # redirect (Cox/Xfinity shape) via a normal navigation.
            r_redirect = session.get(auth_url, headers={'Accept': 'text/html,application/json'}, allow_redirects=False, timeout=30)
            mso_login_url = r_redirect.headers.get('location') or auth_url
        except requests.RequestException as exc:
            set_status('error', f'FOX registration failed: {exc}')
            return
        except (KeyError, ValueError) as exc:
            set_status('error', f'FOX registration returned an unexpected response: {exc}')
            return

        def _grace_poll_pairing(reason: str) -> bool:
            """Same as run_nbc_browser_login's helper: the MSO completion page
            (Cox's Okta widget) closes itself right after the credentials POST
            while the pairing completes server-side — poll browser-free before
            treating a dead page as a failed attempt. Returns True if it
            completed (terminal status already set)."""
            logger.info('[fox-mvpd-login] page gone (%s) — polling for server-side completion', reason)
            grace_deadline = min(time.monotonic() + 30, deadline)
            while time.monotonic() < grace_deadline:
                if r.exists(FOX_BROWSER_LOGIN_STOP_KEY):
                    return False
                token = None
                try:
                    check = session.get(
                        'https://api3.fox.com/v2.0/checkadobeauthn/v2', headers=headers,
                        params={'device_id': device_id, 'requestor': 'fbc-fox'},
                        timeout=30,
                    )
                    if check.ok:
                        token = (check.json() or {}).get('accessToken')
                except requests.RequestException:
                    token = None
                if token:
                    exp = _jwt_exp(token) or int(time.time()) + 3600
                    with flask_app.app_context():
                        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
                        if account:
                            acct_cfg = dict(account.config or {})
                            acct_cfg['fox_sports_access_token'] = token
                            acct_cfg['fox_sports_access_token_exp'] = exp
                            acct_cfg['fox_sports_access_token_mso'] = mso_id
                            acct_cfg['fox_sports_access_token_captured_at'] = int(time.time())
                            account.config = acct_cfg
                            account.last_auth_status = 'ok'
                            account.last_auth_message = f'FOX Sports MVPD token obtained through {mso_id} (browser-assisted).'
                            account.last_auth_at = datetime.now(timezone.utc)
                            db.session.commit()
                    if mso_id == 'Comcast_SSO':
                        _harvest_and_save_xfinity_cookies(context)
                    elif mso_id == 'YouTubeTV':
                        _maybe_capture_google_master_token(context, mso_id)
                    set_status('success', f'Signed in — FOX Sports authorized via {mso_id}.')
                    logger.info('[fox-mvpd-login] paired mso_id=%s (completed after the page closed itself)', mso_id)
                    return True
                time.sleep(2)
            return False

        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            set_status('error', 'Camoufox is not installed on this container')
            return

        # Isolated, cookie-permissive profile for YouTubeTV — shared with
        # run_nbc_browser_login/run_mvpd_browser_login (see
        # _YOUTUBETV_ISOLATED_PROFILE_DIR's docstring in common.py).
        profile_dir = (
            _YOUTUBETV_ISOLATED_PROFILE_DIR
            if mso_id == 'YouTubeTV'
            else '/data/browser_profiles/mvpd_tve'
        )
        try:
            import os as _os_login
            _os_login.makedirs(profile_dir, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[fox-mvpd-login] could not create profile dir %s: %s', profile_dir, exc)

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
            logger.info('[fox-mvpd-login] using isolated YouTubeTV profile with cross-site cookies enabled')
        try:
            with Camoufox(**camoufox_options) as context:
                page = context.pages[0] if context.pages else context.new_page()
                _prime_google_session(context, mso_id)
                page.on('crash', lambda p: logger.warning('[fox-mvpd-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
                page.on('close', lambda p: logger.warning('[fox-mvpd-login] page CLOSE event fired'))
                page.on('pageerror', lambda exc: logger.warning('[fox-mvpd-login] page JS error: %s', str(exc)[:500]))

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
                            '[fox-mvpd-login] navigation response HTTP %s %s',
                            response.status,
                            _gateway_url_for_log(response_url) if is_gateway_bookend else _url_for_log(response_url),
                        )
                    except Exception:  # noqa: BLE001
                        pass

                page.on('response', _log_navigation_response)
                try:
                    if mso_id == 'Comcast_SSO':
                        # Same Akamai cold-navigation wall as the legacy/NBC
                        # Comcast_SSO paths — see run_mvpd_browser_login's
                        # comment on this exact pattern for the full
                        # explanation.
                        origin = f'{_urlsplit(fox_redirect_url).scheme}://{_urlsplit(fox_redirect_url).netloc}'
                        page.goto(origin, wait_until='domcontentloaded', timeout=30000)
                        _settle_deadline = time.monotonic() + 3.0
                        while time.monotonic() < _settle_deadline:
                            _relay_input_and_screenshot(
                                page, r, stop_key=FOX_BROWSER_LOGIN_STOP_KEY, input_key=FOX_BROWSER_LOGIN_INPUT_KEY,
                                shot_key=FOX_BROWSER_LOGIN_SHOT_KEY, hint_key=FOX_BROWSER_LOGIN_HINT_KEY,
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
                                page, r, stop_key=FOX_BROWSER_LOGIN_STOP_KEY, input_key=FOX_BROWSER_LOGIN_INPUT_KEY,
                                shot_key=FOX_BROWSER_LOGIN_SHOT_KEY, hint_key=FOX_BROWSER_LOGIN_HINT_KEY,
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
                            '[fox-mvpd-login] YouTubeTV retry-from-scratch recovery failed; '
                            'continuing to normal stall detection: %s', exc,
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
                if not settled:
                    if mso_id == 'YouTubeTV':
                        _log_youtubetv_gateway_diagnostics(context, youtube_gateway_responses)
                        message = _youtube_tv_gateway_failure_message()
                    else:
                        message = (
                            'The provider sign-in redirect did not finish within 15 seconds. '
                            'Sign-in stopped before displaying an incomplete or blank login page; try again later.'
                        )
                    logger.warning(
                        '[fox-mvpd-login] aborting stalled provider redirect before autofill/screenshot polling '
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
                        '[fox-mvpd-login] Google rejected the primed browser session and redirected to its '
                        'cookie-recovery page (%s)', _url_for_log(landing_url),
                    )
                    set_status(
                        'error',
                        'Google rejected the saved browser session. Use “Sign in with Google” again, then retry.',
                    )
                    return
                if _same_page_url(landing_url, fox_redirect_url):
                    set_status('error', f'{mso_id} does not appear to be a participating provider for FOX TVE.')
                    return
                if mvpd_username and mvpd_password:
                    if mso_id == 'Comcast_SSO':
                        _autofill_xfinity_credentials(
                            page, mvpd_username, mvpd_password, r=r,
                            stop_key=FOX_BROWSER_LOGIN_STOP_KEY, input_key=FOX_BROWSER_LOGIN_INPUT_KEY,
                            shot_key=FOX_BROWSER_LOGIN_SHOT_KEY, hint_key=FOX_BROWSER_LOGIN_HINT_KEY,
                        )
                    else:
                        _try_autofill_credentials(
                            page, mvpd_username, mvpd_password, r=r,
                            stop_key=FOX_BROWSER_LOGIN_STOP_KEY, input_key=FOX_BROWSER_LOGIN_INPUT_KEY,
                            shot_key=FOX_BROWSER_LOGIN_SHOT_KEY, hint_key=FOX_BROWSER_LOGIN_HINT_KEY,
                            navigation_already_settled=True,
                        )
                set_status('running', 'Sign in below, including any captcha if shown.', page.url)

                last_shot = 0.0
                last_heartbeat = 0.0
                last_poll = 0.0
                last_progress_log = 0.0
                consecutive_failures = 0
                f5_retried = False
                session_poll_interval = _FOX_SESSION_POLL_SECONDS
                last_seen_page_url = _safe_page_url(page)
                current_job = get_current_job()
                _MAX_CONSECUTIVE_FAILURES = 15
                while time.monotonic() < deadline:
                    if r.exists(FOX_BROWSER_LOGIN_STOP_KEY):
                        set_status('stopped', 'Cancelled')
                        return
                    if page.is_closed():
                        if _grace_poll_pairing('page closed'):
                            return
                        if r.exists(FOX_BROWSER_LOGIN_STOP_KEY):
                            set_status('stopped', 'Cancelled')
                            return
                        raise _BrowserSessionDied('browser page closed and pairing did not complete')

                    for _ in range(20):
                        raw = r.lpop(FOX_BROWSER_LOGIN_INPUT_KEY)
                        if raw is None:
                            break
                        try:
                            _apply_sling_browser_login_input(page, _json_login.loads(raw))
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[fox-mvpd-login] input apply failed: %s', exc)

                    now = time.monotonic()
                    if current_job and now - last_heartbeat > 60:
                        try:
                            current_job.heartbeat(datetime.now(timezone.utc), 180)
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[fox-mvpd-login] self-heartbeat failed: %s', exc)
                        last_heartbeat = now

                    if now - last_shot > 0.25:
                        try:
                            shot = page.screenshot(type='jpeg', quality=60)
                            r.setex(FOX_BROWSER_LOGIN_SHOT_KEY, 30, shot)
                            set_status('running', 'Sign in below, including any captcha if shown.', _safe_page_url(page))
                            consecutive_failures = 0
                        except Exception as exc:  # noqa: BLE001
                            consecutive_failures += 1
                            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                                logger.warning('[fox-mvpd-login] page unresponsive after %d failed screenshots: %s', consecutive_failures, exc)
                                raise _BrowserSessionDied(f'page stopped answering screenshots: {exc}')
                        last_shot = now

                    current_page_url = _safe_page_url(page)
                    if current_page_url != last_seen_page_url:
                        last_seen_page_url = current_page_url
                        session_poll_interval = _FOX_SESSION_POLL_SECONDS

                    if now - last_poll > session_poll_interval:
                        last_poll = now
                        if not f5_retried and _sling_f5_recover(
                            page, mso_login_url, mvpd_username, mvpd_password, r=r,
                            stop_key=FOX_BROWSER_LOGIN_STOP_KEY, input_key=FOX_BROWSER_LOGIN_INPUT_KEY,
                            shot_key=FOX_BROWSER_LOGIN_SHOT_KEY, hint_key=FOX_BROWSER_LOGIN_HINT_KEY,
                        ):
                            f5_retried = True
                            continue
                        try:
                            check = session.get(
                                'https://api3.fox.com/v2.0/checkadobeauthn/v2', headers=headers,
                                params={'device_id': device_id, 'requestor': 'fbc-fox'},
                                timeout=30,
                            )
                        except requests.RequestException as exc:
                            set_status('error', f'FOX checkadobeauthn request failed: {exc}')
                            return

                        def _back_off_and_log(http_status):
                            nonlocal session_poll_interval, last_progress_log
                            session_poll_interval = min(
                                session_poll_interval * _FOX_SESSION_POLL_BACKOFF,
                                _FOX_SESSION_POLL_MAX_SECONDS,
                            )
                            if now - last_progress_log >= 60:
                                logger.info(
                                    '[fox-mvpd-login] authorization still pending (checkadobeauthn_http=%s next_poll=%.1fs)',
                                    http_status, session_poll_interval,
                                )
                                last_progress_log = now

                        if check.status_code == 404:
                            _back_off_and_log(404)
                            continue  # human hasn't finished the MSO login yet
                        if not check.ok:
                            set_status('error', f'FOX checkadobeauthn returned HTTP {check.status_code}: {check.text[:300]}')
                            return
                        token = (check.json() or {}).get('accessToken')
                        if not token:
                            _back_off_and_log(check.status_code)
                            continue
                        exp = _jwt_exp(token) or int(time.time()) + 3600
                        with flask_app.app_context():
                            account = TVEAccount.query.filter_by(provider_id='mvpd').first()
                            if account:
                                acct_cfg = dict(account.config or {})
                                acct_cfg['fox_sports_access_token'] = token
                                acct_cfg['fox_sports_access_token_exp'] = exp
                                acct_cfg['fox_sports_access_token_mso'] = mso_id
                                acct_cfg['fox_sports_access_token_captured_at'] = int(time.time())
                                account.config = acct_cfg
                                account.last_auth_status = 'ok'
                                account.last_auth_message = f'FOX Sports MVPD token obtained through {mso_id} (browser-assisted).'
                                account.last_auth_at = datetime.now(timezone.utc)
                                db.session.commit()
                        if mso_id == 'Comcast_SSO':
                            _harvest_and_save_xfinity_cookies(context)
                        elif mso_id == 'YouTubeTV':
                            _maybe_capture_google_master_token(context, mso_id)
                        set_status('success', f'Signed in — FOX Sports authorized via {mso_id}.')
                        logger.info('[fox-mvpd-login] paired mso_id=%s', mso_id)
                        return

                    page.wait_for_timeout(80)

                set_status('error', 'Timed out waiting for sign-in to complete.')
                return
        except BaseException as exc:  # noqa: BLE001
            if _terminal_status_set['v']:
                logger.info('[fox-mvpd-login] ignoring cleanup-time exception after terminal status was already set: %s', exc)
                return
            if _is_browser_death(exc) and _grace_poll_pairing(str(exc)[:80]):
                return
            if (_is_browser_death(exc) and _attempt < _BROWSER_LOGIN_MAX_ATTEMPTS
                    and time.monotonic() < deadline - 30
                    and not r.exists(FOX_BROWSER_LOGIN_STOP_KEY)):
                logger.warning('[fox-mvpd-login] browser died (attempt %d/%d), relaunching: %s', _attempt, _BROWSER_LOGIN_MAX_ATTEMPTS, exc)
                set_status('starting', 'Browser hiccuped — relaunching…')
                return run_fox_browser_login(mso_id, _attempt=_attempt + 1, _deadline=deadline)
            if r.exists(FOX_BROWSER_LOGIN_STOP_KEY):
                logger.info('[fox-mvpd-login] stopped by request (browser force-killed to interrupt a stuck wait)')
                try:
                    set_status('stopped', 'Cancelled.')
                except Exception:  # noqa: BLE001
                    pass
                return
            logger.exception('[fox-mvpd-login] browser session failed')
            try:
                set_status('error', f'Browser session failed: {exc}')
            except Exception:  # noqa: BLE001
                pass
            return
    finally:
        uninstall_browser_login_activity_log(_activity_handler)
        if not _ctx_popped['v']:
            _ctx.pop()

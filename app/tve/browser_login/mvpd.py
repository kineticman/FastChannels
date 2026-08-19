"""Generic Adobe Pass MVPD browser-assisted login (Cox/Xfinity/DirecTV/yt-dlp-legacy flow)."""
import logging
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit as _urlsplit
from rq import get_current_job
import redis

from app.worker import flask_app
from app.extensions import db
from app.models import TVEAccount
from app.tve.adobe_pass import AdobePassCoxClient, TVEAuthError, TVENotAuthorizedError, TVEPendingAuthError
from app.tve.browser_login.common import (
    MVPD_BROWSER_LOGIN_STATUS_KEY,
    MVPD_BROWSER_LOGIN_SHOT_KEY,
    MVPD_BROWSER_LOGIN_INPUT_KEY,
    MVPD_BROWSER_LOGIN_STOP_KEY,
    _safe_page_url,
    _same_page_url,
    _settle_after_mvpd_navigation,
    _try_autofill_credentials,
    _autofill_xfinity_credentials,
    _apply_sling_browser_login_input,
    _harvest_and_save_xfinity_cookies,
    _record_tve_login_error,
    _cox_login_error_detail,
    _autofill_google_account_chooser,
    _prime_google_session,
    _maybe_capture_google_master_token,
    _relay_input_and_screenshot,
    _sling_f5_recover,
    _BROWSER_LOGIN_MAX_ATTEMPTS,
    _BrowserSessionDied,
    _is_browser_death,
)

logger = logging.getLogger(__name__)


_MVPD_BROWSER_LOGIN_TIMEOUT_SECONDS = 1800
# Adobe's own session polling — short and frequent, since this is a plain
# GET/POST exchange, not something that needs to be rate-limited.
_MVPD_SESSION_POLL_SECONDS = 2.0


def _save_mvpd_authn_token(requestor_id: str, authn_token: str) -> None:
    """Pushes its own app_context — see _prime_google_session's docstring.
    May be called mid-browser-session, after the caller has already popped
    its outer one before launching Camoufox."""
    with flask_app.app_context():
        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if not account:
            return
        cfg = dict(account.config or {})
        mvpd_authn = dict(cfg.get('mvpd_authn') or {})
        mvpd_authn[requestor_id] = {'authn_token': authn_token, 'captured_at': int(time.time())}
        cfg['mvpd_authn'] = mvpd_authn
        account.config = cfg
        db.session.commit()


def _summarize_pairing_results(results: dict[str, tuple[bool, str]]) -> str:
    authorized = [k for k, (ok, _) in results.items() if ok]
    other = [(k, msg) for k, (ok, msg) in results.items() if not ok]
    parts = []
    if authorized:
        parts.append(f'Signed in — authorized: {", ".join(authorized)}.')
    else:
        parts.append('Signed in.')
    if other:
        parts.append('Not available: ' + '; '.join(f'{k} ({msg})' for k, msg in other) + '.')
    return ' '.join(parts)


def run_mvpd_browser_login(requestor_id: str, resource: str, software_statement: str, redirect_url: str, mso_id: str, _attempt: int = 1, _deadline: float | None = None):
    """Drive a real, human-operated sign-in to an MVPD's Adobe Pass login page
    for just requestor_id — each network is signed into independently (the
    admin UI's "Sign in to all" batches these calls client-side, one per
    network; there is no shared-browser-session sweep anymore, see below).

    For MSOs whose login page blocks scripted clients outright (Sling's
    identity.sling.com returns HTTP 417 to yt-dlp even with browser TLS
    impersonation — see app/tve/ytdlp_mvpd.py), the only thing that reliably
    gets through is an actual browser. Adobe Pass's legacy protocol is built
    for exactly this "second screen" case: setup_client/register_device/
    create_regcode happen here, scripted, same as always (that part was never
    blocked — only the MSO's own login form is). We then hand the resulting
    authenticate/saml URL to a real Camoufox tab for a human to complete
    Sling's actual login in, while polling Adobe's /adobe-services/session
    endpoint with our own reg_code from this process. Adobe's backend binds
    the browser's completed SAML round-trip to our reg_code server-side, so
    polling picks it up regardless of the browser and this process being
    entirely separate HTTP sessions — no token-scraping from the page needed.
    The resulting authn_token is long-lived and cached, so this browser
    session is a one-time cost per requestor_id (see authorize_mvpd()).
    """
    # Manual push/pop instead of `with flask_app.app_context():` — see
    # _prime_google_session's docstring: Camoufox's own rendering breaks
    # intermittently while a Flask app_context is active on this thread.
    # Popped right before Camoufox launches and never re-pushed (every exit
    # path from there on is a `return`); DB-touching helpers called during
    # the browser session push their own short-lived context instead.
    _ctx = flask_app.app_context()
    _ctx.push()
    _ctx_popped = {'v': False}
    try:
        import json as _json_login

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[mvpd-login] Redis unavailable, aborting: %s', exc)
            return

        # Live-updated checklist (single entry — one network per call) so the
        # modal shows real progress instead of one static message.
        steps: list[dict] = [{'label': requestor_id, 'state': 'running'}]

        def _step(label: str, state: str, message: str = ''):
            for entry in steps:
                if entry['label'] == label:
                    entry['state'] = state
                    entry['message'] = message
                    break
            else:
                steps.append({'label': label, 'state': state, 'message': message})

        # Camoufox's underlying Firefox process has been observed to die on its
        # own mid-session (confirmed live 2026-08-05). By that point the job has
        # often already finished its real work and called set_status('success'/
        # 'stopped', ...). The `with Camoufox(...)` block's own teardown then
        # tries to close the already-dead browser, raises, and — since that
        # happens while Python is unwinding this function's `return` — REPLACES
        # the good terminal status with a misleading crash message unless we
        # explicitly remember a terminal status was already recorded.
        _terminal_status_set = {'v': False}

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    MVPD_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': url, 'requestor_id': requestor_id, 'steps': steps}),
                )
                if state in ('success', 'error', 'stopped'):
                    _terminal_status_set['v'] = True
            except Exception:  # noqa: BLE001
                pass

        r.delete(MVPD_BROWSER_LOGIN_STOP_KEY)
        r.delete(MVPD_BROWSER_LOGIN_INPUT_KEY)
        set_status('starting', 'Registering with Adobe Pass…')

        # One deadline shared across browser-crash retries (the RQ job_timeout
        # is only ~30s above _MVPD_BROWSER_LOGIN_TIMEOUT_SECONDS, so a retry
        # must never reset the clock).
        deadline = _deadline if _deadline is not None else time.monotonic() + _MVPD_BROWSER_LOGIN_TIMEOUT_SECONDS

        account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
        mvpd_username = (account_row.username if account_row else '') or ''
        mvpd_password = (account_row.password if account_row else '') or ''

        from app.tve.adobe_pass import _ensure_cox_device_fingerprint, load_cached_adobe_client_creds, save_adobe_client_creds
        client_creds = load_cached_adobe_client_creds(account_row, requestor_id) if account_row else None
        client = AdobePassCoxClient(
            requestor_id=requestor_id,
            resource=resource,
            software_statement=software_statement,
            redirect_url=redirect_url,
            device_fingerprint=_ensure_cox_device_fingerprint(account_row) if account_row else None,
            client_creds=client_creds,
        )

        if mso_id == 'Cox':
            # Cox's login step is already fully scripted:
            # AdobePassCoxClient.authorize_with_cox() does the whole thing —
            # register, regcode, direct login.cox.com/api/v1/authn POST
            # (authenticate_with_cox), fetch_session_token, authorize — exactly
            # what authorize_mvpd() already does automatically at play time for
            # this same "legacy" family (History/A&E/Warner). No browser
            # needed; confirmed live 2026-08-11 (History TVE authorized in a
            # few seconds, zero Camoufox). Only non-Cox MSOs (e.g. Sling,
            # whose login page blocks scripted clients outright) fall through
            # to the browser-assisted flow below.
            set_status('running', f'Signing in to {requestor_id}…')
            try:
                client.authorize_with_cox(mvpd_username, mvpd_password)
            except Exception as exc:  # noqa: BLE001
                detail = _cox_login_error_detail(exc, requestor_id)
                _step(requestor_id, 'failed', detail[:120])
                _record_tve_login_error(requestor_id, detail)
                set_status('error', f'{requestor_id}: {detail}')
                return
            if not client_creds and account_row:
                save_adobe_client_creds(account_row, requestor_id, client.ctx.client_id, client.ctx.client_secret, client.ctx.access_token)
            _save_mvpd_authn_token(requestor_id, client.ctx.authn_token)
            _step(requestor_id, 'done', 'authorized')
            set_status('success', f'Signed in — {requestor_id} authorized.')
            logger.info('[mvpd-login] paired requestor_id=%s mso_id=Cox (scripted, no browser)', requestor_id)
            return

        if mso_id == 'Comcast_SSO':
            # Try a saved cookie jar (harvested from a previous successful
            # Comcast_SSO browser pairing — see _harvest_and_save_xfinity_
            # cookies below, and app/tve/adobe_pass.py's authorize_mvpd())
            # BEFORE ever opening a browser. Without this check, every
            # single network in a "Sign in to all" batch opened its own
            # fresh Camoufox window even though the FIRST one's success
            # already saved a jar good for every other network too —
            # confirmed live 2026-08-14 (HISTORY/AETV/LIFETIME each did a
            # full browser login back to back despite each one saving a
            # cookie jar right after). Only falls through to the browser
            # below when there's no jar yet, or the saved one has gone
            # stale (Akamai cookies expire in ~2-4h — see
            # dev/comcast/XFINITY_ADOBE_PASS_DIRECT_HTTP_RESEARCH.md).
            cookie_jar = (account_row.config or {}).get('xfinity_cookie_jar') if account_row else None
            if cookie_jar:
                set_status('running', 'Trying saved sign-in (no browser needed)…')
                try:
                    token = client.authorize_with_xfinity_cookies(mvpd_username, mvpd_password, cookie_jar)
                except TVENotAuthorizedError as exc:
                    _step(requestor_id, 'failed', 'not entitled')
                    _record_tve_login_error(requestor_id, str(exc))
                    set_status('error', f'{requestor_id}: not entitled ({exc})')
                    return
                except TVEAuthError as exc:
                    logger.info(
                        '[mvpd-login] saved xfinity cookie jar did not work for %s, falling back to browser: %s',
                        requestor_id, exc,
                    )
                else:
                    if not client_creds and account_row:
                        save_adobe_client_creds(account_row, requestor_id, client.ctx.client_id, client.ctx.client_secret, client.ctx.access_token)
                    _save_mvpd_authn_token(requestor_id, client.ctx.authn_token)
                    _step(requestor_id, 'done', 'authorized')
                    set_status('success', f'Signed in — {requestor_id} authorized (no browser needed).')
                    logger.info(
                        '[mvpd-login] paired requestor_id=%s mso_id=Comcast_SSO via saved cookie jar (no browser)',
                        requestor_id,
                    )
                    return
            set_status('running', 'No usable saved sign-in — opening a browser…')

        try:
            client.setup_client()
            if not client_creds and account_row:
                save_adobe_client_creds(account_row, requestor_id, client.ctx.client_id, client.ctx.client_secret, client.ctx.access_token)
            client.register_device()
            client.create_regcode()
        except TVEAuthError as exc:
            _step(requestor_id, 'failed', str(exc)[:120])
            set_status('error', f'Adobe Pass registration failed: {exc}')
            return
        auth_url = client.authenticate_redirect_url(mso_id)

        def _grace_poll_pairing(reason: str) -> bool:
            """See run_nbc_browser_login's helper: MSO completion pages (Cox's
            Okta widget) close themselves right after the credentials POST
            while Adobe binds the session server-side — poll browser-free
            before treating a dead page as a failed attempt. Returns True if
            the sign-in reached a terminal answer (status already set)."""
            logger.info('[mvpd-login] page gone (%s) — polling for server-side completion', reason)
            grace_deadline = min(time.monotonic() + 30, deadline)
            while time.monotonic() < grace_deadline:
                if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
                    return False
                try:
                    client.fetch_session_token()
                except TVEPendingAuthError:
                    time.sleep(2)
                    continue
                except TVEAuthError as exc:
                    _step(requestor_id, 'failed', str(exc)[:120])
                    set_status('error', f'Adobe Pass error: {exc}')
                    return True  # definitive answer — nothing to relaunch for
                _save_mvpd_authn_token(requestor_id, client.ctx.authn_token)
                if mso_id == 'Comcast_SSO':
                    _harvest_and_save_xfinity_cookies(context)
                elif mso_id == 'YouTubeTV':
                    _maybe_capture_google_master_token(context, mso_id)
                results: dict[str, tuple[bool, str]] = {}
                try:
                    client.authorize()
                    results[requestor_id] = (True, 'authorized')
                    _step(requestor_id, 'done', 'authorized')
                    logger.info('[mvpd-login] paired requestor_id=%s (completed after the page closed itself)', requestor_id)
                except TVENotAuthorizedError as exc:
                    results[requestor_id] = (False, 'not entitled')
                    _step(requestor_id, 'failed', 'not entitled')
                    logger.info('[mvpd-login] %s: not entitled (completed after the page closed itself) — %s', requestor_id, exc)
                except TVEAuthError as exc:
                    results[requestor_id] = (False, str(exc)[:120])
                    _step(requestor_id, 'failed', str(exc)[:120])
                    logger.warning('[mvpd-login] %s authorize failed (completed after the page closed itself): %s', requestor_id, exc)
                set_status('success', _summarize_pairing_results(results))
                return True
            return False

        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            _step(requestor_id, 'failed', 'Camoufox not installed')
            set_status('error', 'Camoufox is not installed on this container')
            return

        profile_dir = '/data/browser_profiles/mvpd_tve'
        try:
            import os as _os_login
            _os_login.makedirs(profile_dir, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[mvpd-login] could not create profile dir %s: %s', profile_dir, exc)

        _ctx.pop()
        _ctx_popped['v'] = True
        try:
            # Same persistent-profile Camoufox setup as run_sling_browser_login
            # (see its comments for why: real, non-headless Firefox behind a
            # virtual display so WebGL/fingerprinting stays intact, and a
            # persistent profile so the MSO's session cookies survive across
            # runs — which is what lets sibling requestor_ids pair silently
            # afterward without a human involved again).
            with Camoufox(
                headless='virtual',
                os='windows',
                persistent_context=True,
                user_data_dir=profile_dir,
                window=(1280, 800),
            ) as context:
                page = context.pages[0] if context.pages else context.new_page()
                _prime_google_session(context, mso_id)
                page.on('crash', lambda p: logger.warning('[mvpd-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
                page.on('close', lambda p: logger.warning('[mvpd-login] page CLOSE event fired'))
                page.on('pageerror', lambda exc: logger.warning('[mvpd-login] page JS error: %s', str(exc)[:500]))
                try:
                    if mso_id == 'Comcast_SSO':
                        # Xfinity's WAF (Akamai) flatly denies a cold top-level
                        # navigation to the Adobe Pass authenticate/saml URL —
                        # no Referer, sec-fetch-site: none — with an HTTP 403
                        # "Access Denied" page (server: AkamaiGHost). Confirmed
                        # live 2026-08-14 via both a scripted curl_cffi request
                        # and a real headful browser pasting the URL directly;
                        # neither is a bot-fingerprint issue (the plain-paste
                        # block hit even with a fully genuine browser). Landing
                        # on any real page first and redirecting via in-page JS
                        # (so the request carries a real Referer/Sec-Fetch-Site
                        # chain) sails through to the actual login form instead
                        # — verified end-to-end: real login + Adobe authorize +
                        # shortAuthorize + a live /play redirect to a real CDN
                        # URL. redirect_url's own origin is used as the landing
                        # page since it's guaranteed reachable for any
                        # requestor_id without needing extra per-network config.
                        origin = f'{_urlsplit(redirect_url).scheme}://{_urlsplit(redirect_url).netloc}'
                        page.goto(origin, wait_until='domcontentloaded', timeout=30000)
                        set_status('running', 'Loading sign-in page…', page.url)
                        # This whole navigation (landing page + fixed settle +
                        # in-page redirect + load-state wait) used to be one
                        # long blocking stretch with zero relay calls in it —
                        # the modal sat completely blank for up to ~30s
                        # (reported live 2026-08-14: "I don't see any
                        # screenshots"). Relay every ~1s throughout instead.
                        _settle_deadline = time.monotonic() + 3.0
                        while time.monotonic() < _settle_deadline:
                            _relay_input_and_screenshot(page, r)
                            page.wait_for_timeout(500)
                        page.evaluate('(u) => { window.location.href = u; }', auth_url)
                        _load_deadline = time.monotonic() + 30.0
                        while time.monotonic() < _load_deadline:
                            try:
                                page.wait_for_load_state('domcontentloaded', timeout=1000)
                                break
                            except Exception:  # noqa: BLE001
                                pass
                            _relay_input_and_screenshot(page, r)
                    elif mso_id == 'DTV':
                        # Mirrors app/scrapers/directv.py's capture_directv_auth_cffi(),
                        # which primes both stream.directv.com and
                        # identity.directv.com before ever touching an auth
                        # endpoint — that scripted (non-browser) client has no
                        # prior browsing history to lean on, unlike a real
                        # user's browser. This Camoufox profile is in the same
                        # position on its first-ever visit to either origin
                        # (fresh persistent profile, arriving cold via Adobe's
                        # authenticate/saml redirect chain rather than a normal
                        # user journey that would have already touched these
                        # pages), so give it the same head start rather than
                        # assuming real-browser JS execution alone is
                        # sufficient — unconfirmed either way, but cheap to do
                        # and directly mirrors the one DIRECTV flow already
                        # proven reliable in this codebase.
                        set_status('running', 'Priming DIRECTV session…', page.url)
                        # Best-effort — priming is a head start, not the real
                        # navigation, so a hiccup here (e.g. Firefox's
                        # NS_BINDING_ABORTED when a page redirects fast enough
                        # to race Playwright's own navigation tracking,
                        # confirmed live 2026-08-17 on the bare
                        # identity.directv.com/ root) shouldn't abort the
                        # whole pairing attempt the way a failure in the real
                        # auth_url navigation below should.
                        for _prime_url in ('https://stream.directv.com/guide', 'https://identity.directv.com/'):
                            try:
                                page.goto(_prime_url, wait_until='domcontentloaded', timeout=30000)
                            except Exception as _prime_exc:  # noqa: BLE001
                                if _is_browser_death(_prime_exc):
                                    raise
                                logger.info('[mvpd-login] DIRECTV priming nav to %s did not settle cleanly: %s', _prime_url, _prime_exc)
                            _relay_input_and_screenshot(page, r)
                        _settle_deadline = time.monotonic() + 2.0
                        while time.monotonic() < _settle_deadline:
                            _relay_input_and_screenshot(page, r)
                            page.wait_for_timeout(500)
                        set_status('running', 'Loading sign-in page…', page.url)
                        page.goto(auth_url, wait_until='domcontentloaded', timeout=30000)
                    else:
                        page.goto(auth_url, wait_until='domcontentloaded', timeout=30000)
                except Exception as exc:  # noqa: BLE001
                    if _is_browser_death(exc):
                        raise
                    _step(requestor_id, 'failed', 'failed to load sign-in page')
                    set_status('error', f'Failed to load provider sign-in page: {exc}')
                    return
                # See _settle_after_mvpd_navigation's docstring: a
                # page.screenshot() call during Adobe/YouTubeTV's still-in-
                # flight SAML bounce chain silently cancels it. Unconditional
                # (not just inside _try_autofill_credentials below) so the
                # no-saved-credentials path — which skips that call entirely
                # — doesn't leave the main poll loop's first screenshot
                # unprotected.
                _settle_after_mvpd_navigation(page, set_status=set_status)
                # If Adobe doesn't have this MVPD registered for this content
                # owner at all (confirmed live for Turner/TNT+Sling: a single
                # 302 straight back, never even touching Sling's login), the
                # very FIRST landing page is redirect_url itself — no MSO
                # domain was ever visited. Left undetected, the human just
                # stares at a dead, unexplained page for up to 30 minutes
                # (confirmed live 2026-08-05).
                #
                # But that landing is NOT unambiguous — this browser profile
                # is reused across every pairing run (persistent_context=True,
                # same user_data_dir below), so a warm Adobe SSO cookie left
                # over from an earlier successful pairing can bind the
                # regcode server-side before the browser ever touches the
                # MSO's domain, landing here on real success too. Give
                # fetch_session_token() (via _grace_poll_pairing, same helper
                # used when the MSO completion page closes itself) a real
                # chance to find a bound session before concluding this
                # network truly isn't participating — confirmed live
                # 2026-08-06: truTV reported "not a participating provider"
                # via this exact bounce right after TNT/TBS had just warmed
                # the same profile's Adobe session, even though Cox is
                # genuinely a listed truTV MVPD (a cold, cookie-free
                # `requests` session redirects cleanly to Cox's real login).
                if _same_page_url(_safe_page_url(page), redirect_url):
                    if _grace_poll_pairing('landed directly on redirect_url'):
                        return
                    _step(requestor_id, 'failed', 'not a participating provider')
                    set_status('error', f'{requestor_id}: {mso_id} does not appear to be a participating provider for this network.')
                    return
                if mvpd_username and mvpd_password:
                    if mso_id == 'Comcast_SSO':
                        _autofill_xfinity_credentials(page, mvpd_username, mvpd_password, r=r)
                    else:
                        _try_autofill_credentials(page, mvpd_username, mvpd_password, r=r)
                set_status('running', 'Sign in below, including any captcha if shown.', page.url)

                last_shot = 0.0
                last_heartbeat = 0.0
                last_poll = 0.0
                consecutive_failures = 0
                f5_retried = False
                current_job = get_current_job()
                _MAX_CONSECUTIVE_FAILURES = 15
                while time.monotonic() < deadline:
                    if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
                        _step(requestor_id, 'failed', 'cancelled')
                        set_status('stopped', 'Cancelled')
                        return

                    if page.is_closed():
                        if _grace_poll_pairing('page closed'):
                            return
                        if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
                            _step(requestor_id, 'failed', 'cancelled')
                            set_status('stopped', 'Cancelled')
                            return
                        raise _BrowserSessionDied('browser page closed and sign-in did not complete')

                    # Unlike AMCN/Discovery/FOX One/NBC/FOX (all routed
                    # through _relay_input_and_screenshot, which already does
                    # this), this loop has its own separate inline
                    # implementation and never got the same auto-click —
                    # this family (History/A&E/Warner) is exactly the one
                    # that hits Google's account-chooser most often tonight.
                    _autofill_google_account_chooser(page)

                    for _ in range(20):
                        raw = r.lpop(MVPD_BROWSER_LOGIN_INPUT_KEY)
                        if raw is None:
                            break
                        try:
                            _apply_sling_browser_login_input(page, _json_login.loads(raw))
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[mvpd-login] input apply failed: %s', exc)

                    now = time.monotonic()
                    if current_job and now - last_heartbeat > 60:
                        try:
                            current_job.heartbeat(datetime.now(timezone.utc), 180)
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[mvpd-login] self-heartbeat failed: %s', exc)
                        # This loop otherwise goes completely log-silent while
                        # waiting on a human (e.g. a Google account-chooser
                        # click) — the browser is alive and the screenshot is
                        # updating the whole time, but that's invisible to
                        # anyone tailing logs instead of the admin UI modal,
                        # which reads exactly like a hang (confirmed live
                        # 2026-08-17: TNT sat on a Google "Choose an account"
                        # screen for minutes with zero log output after the
                        # autofill warning). One line per heartbeat interval
                        # makes "alive and waiting" distinguishable from
                        # "actually stuck" without opening the modal.
                        logger.info(
                            '[mvpd-login] %s: still waiting on sign-in — current page: %s',
                            requestor_id, _safe_page_url(page),
                        )
                        last_heartbeat = now

                    if now - last_shot > 0.25:
                        try:
                            shot = page.screenshot(type='jpeg', quality=60)
                            r.setex(MVPD_BROWSER_LOGIN_SHOT_KEY, 30, shot)
                            set_status('running', 'Sign in below, including any captcha if shown.', _safe_page_url(page))
                            consecutive_failures = 0
                        except Exception as exc:  # noqa: BLE001
                            consecutive_failures += 1
                            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                                logger.warning('[mvpd-login] page unresponsive after %d failed screenshots: %s', consecutive_failures, exc)
                                raise _BrowserSessionDied(f'page stopped answering screenshots: {exc}')
                        last_shot = now

                    if now - last_poll > _MVPD_SESSION_POLL_SECONDS:
                        last_poll = now
                        if not f5_retried and _sling_f5_recover(page, auth_url, mvpd_username, mvpd_password, r=r):
                            f5_retried = True
                            continue
                        try:
                            client.fetch_session_token()
                            # authn_token proves the MSO login itself succeeded — save it
                            # now, independent of whether authorize() below finds THIS
                            # requestor_id entitled. It's still reusable for any other
                            # requestor_id under the same MSO account (see authorize_mvpd()
                            # in app/tve/adobe_pass.py).
                            _save_mvpd_authn_token(requestor_id, client.ctx.authn_token)
                            if mso_id == 'Comcast_SSO':
                                _harvest_and_save_xfinity_cookies(context)
                            elif mso_id == 'YouTubeTV':
                                _maybe_capture_google_master_token(context, mso_id)
                        except TVEPendingAuthError:
                            continue  # human hasn't finished the MSO login yet
                        except TVEAuthError as exc:
                            _step(requestor_id, 'failed', str(exc)[:120])
                            set_status('error', f'Adobe Pass error: {exc}')
                            return

                        # Login itself succeeded (we have a real authn_token) the
                        # moment fetch_session_token() stops raising.
                        results: dict[str, tuple[bool, str]] = {}
                        try:
                            token = client.authorize()
                            results[requestor_id] = (True, 'authorized')
                            _step(requestor_id, 'done', 'authorized')
                            logger.info('[mvpd-login] paired requestor_id=%s (token len=%d)', requestor_id, len(token or ''))
                        except TVENotAuthorizedError as exc:
                            results[requestor_id] = (False, 'not entitled')
                            _step(requestor_id, 'failed', 'not entitled')
                            logger.info('[mvpd-login] %s: not entitled — %s', requestor_id, exc)
                        except TVEAuthError as exc:
                            results[requestor_id] = (False, str(exc)[:120])
                            _step(requestor_id, 'failed', str(exc)[:120])
                            logger.warning('[mvpd-login] %s authorize failed: %s', requestor_id, exc)

                        set_status('success', _summarize_pairing_results(results))
                        return

                    page.wait_for_timeout(80)

                _step(requestor_id, 'failed', 'timed out')
                set_status('error', 'Timed out waiting for sign-in to complete.')
                return
        except BaseException as exc:  # noqa: BLE001
            if _terminal_status_set['v']:
                # A real success/error/stopped status was already recorded
                # before this — almost certainly the Camoufox `with` block's
                # own teardown failing to close an already-dead browser, not
                # an actual job failure. Don't clobber the real result.
                logger.info('[mvpd-login] ignoring cleanup-time exception after terminal status was already set: %s', exc)
                return
            if _is_browser_death(exc) and _grace_poll_pairing(str(exc)[:80]):
                return
            if (_is_browser_death(exc) and _attempt < _BROWSER_LOGIN_MAX_ATTEMPTS
                    and time.monotonic() < deadline - 30
                    and not r.exists(MVPD_BROWSER_LOGIN_STOP_KEY)):
                logger.warning('[mvpd-login] browser died (attempt %d/%d), relaunching: %s', _attempt, _BROWSER_LOGIN_MAX_ATTEMPTS, exc)
                set_status('starting', 'Browser hiccuped — relaunching…')
                return run_mvpd_browser_login(requestor_id, resource, software_statement, redirect_url, mso_id, _attempt=_attempt + 1, _deadline=deadline)
            if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
                logger.info('[mvpd-login] stopped by request (browser force-killed to interrupt a stuck wait)')
                try:
                    _step(requestor_id, 'failed', 'cancelled')
                    set_status('stopped', 'Cancelled.')
                except Exception:  # noqa: BLE001
                    pass
                return
            logger.exception('[mvpd-login] browser session failed')
            try:
                _step(requestor_id, 'failed', str(exc)[:120])
                set_status('error', f'Browser session failed: {exc}')
            except Exception:  # noqa: BLE001
                pass
            return
    finally:
        if not _ctx_popped['v']:
            _ctx.pop()

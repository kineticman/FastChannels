"""AMC Networks (AMCN) TVE browser-assisted login."""
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit as _urlsplit
import redis

from app.worker import flask_app
from app.models import Source, TVEAccount
from app.config_store import persist_source_cache_updates, persist_source_config_updates
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


def _run_amcn_browser_assisted_login(r, set_status, source, account, scraper, device_id: str, mso_id: str, channels: dict) -> None:
    """Browser-assisted counterpart to run_amcn_browser_login's scripted Cox
    fast path, for any MSO whose login page blocks scripted clients outright
    (YouTubeTV/Google, Sling, etc.) — same "second screen" idea as
    run_nbc_browser_login, adapted to AMCNetworksTVEScraper's own v2 REST
    API (_adobe_session_redirect/_adobe_decision_finish, already MSO-generic
    since today's DIRECTV wiring routed non-Cox logins through the shared
    login_to_mvpd() dispatcher — this function is what was still missing: a
    real browser to drive that dispatcher's browser-only MSOs through).

    Unlike the Cox path, the 4 channels are done ONE AT A TIME through a
    single shared page/profile — each needs its own live MSO login
    (independent requestor_id/session), so there's no equivalent of Cox's
    "parallelize since nothing shares state" shortcut. In practice this is
    fast after the first channel: the persistent profile
    (/data/browser_profiles/mvpd_tve, same one NBC/FOX/legacy use) keeps
    Google/Adobe's SSO session warm, so channels 2-4 usually just need an
    account-picker/consent click apiece — confirmed live 2026-08-17 for
    Warner's TNT/TBS/truTV under the exact same profile.

    Reuses the shared legacy 'mvpd:browser-login:*' redis keys (via
    set_status and _relay_input_and_screenshot's defaults) since AMCN's
    "Sign in" button already rides the same modal/polling infra as
    History/Warner — see _relay_input_and_screenshot's docstring.
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
        logger.warning('[amcn-mvpd-login] could not create profile dir %s: %s', profile_dir, exc)

    _PER_CHANNEL_TIMEOUT_SECONDS = 120
    _POLL_SECONDS = 2.0
    _POLL_MAX_SECONDS = 20.0
    _POLL_BACKOFF = 1.5
    authorized: list[str] = []
    failed: list[str] = []

    camoufox_options = {
        'headless': 'virtual', 'os': 'windows', 'persistent_context': True,
        'user_data_dir': profile_dir, 'window': (1280, 800),
    }
    if mso_id == 'YouTubeTV':
        # See _YOUTUBETV_CAMOUFOX_FIREFOX_PREFS's docstring in common.py.
        camoufox_options['firefox_user_prefs'] = _YOUTUBETV_CAMOUFOX_FIREFOX_PREFS
        logger.info('[amcn-mvpd-login] using isolated YouTubeTV profile with cross-site cookies enabled')
    try:
        with Camoufox(**camoufox_options) as context:
            page = context.pages[0] if context.pages else context.new_page()
            _prime_google_session(context, mso_id)
            page.on('crash', lambda p: logger.warning('[amcn-mvpd-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
            page.on('close', lambda p: logger.warning('[amcn-mvpd-login] page CLOSE event fired'))
            page.on('pageerror', lambda exc: logger.warning('[amcn-mvpd-login] page JS error: %s', str(exc)[:500]))

            # Single listener for the whole (multi-channel) browser session —
            # reset gateway_state['responses'] at the start of each channel's
            # navigation below rather than re-attaching a new listener per
            # channel, which would stack listeners on the same long-lived page.
            gateway_state = {'responses': []}

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
                        gateway_state['responses'].append(response)
                    logger.info(
                        '[amcn-mvpd-login] navigation response HTTP %s %s',
                        response.status,
                        _gateway_url_for_log(response_url) if is_gateway_bookend else _url_for_log(response_url),
                    )
                except Exception:  # noqa: BLE001
                    pass

            page.on('response', _log_navigation_response)

            for channel in channels.values():
                if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
                    failed.append(f'{channel.name}: cancelled')
                    break
                set_status('running', f'Signing in to {channel.name}…')
                try:
                    statement = scraper._amcn_software_statement(channel, account)
                    client, code, mso_login_url, auth_headers, _resp = scraper._adobe_session_redirect(
                        channel, statement, device_id, mso_id,
                    )
                except TVENotAuthorizedError:
                    failed.append(f'{channel.name}: not a participating provider')
                    continue
                except TVEAuthError as exc:
                    failed.append(f'{channel.name}: {str(exc)[:120]}')
                    continue
                except Exception as exc:  # noqa: BLE001
                    logger.exception('[amcn-mvpd-login] unexpected failure registering %s', channel.name)
                    failed.append(f'{channel.name}: {str(exc)[:120]}')
                    continue

                gateway_state['responses'] = []
                try:
                    if mso_id == 'Comcast_SSO':
                        # Xfinity's WAF (Akamai) flatly denies a cold top-level
                        # navigation to the Adobe Pass authenticate/saml URL —
                        # same wall fox.py/mvpd.py hit and work around; see
                        # run_mvpd_browser_login's comment on this exact
                        # pattern for the full explanation. Landing on the
                        # channel's own real page first and redirecting via
                        # in-page JS (real Referer/Sec-Fetch-Site chain)
                        # sails through to the actual login form instead.
                        page.goto(channel.page_origin, wait_until='domcontentloaded', timeout=30000)
                        _settle_deadline = time.monotonic() + 3.0
                        while time.monotonic() < _settle_deadline:
                            _relay_input_and_screenshot(page, r)
                            page.wait_for_timeout(500)
                        page.evaluate('(u) => { window.location.href = u; }', mso_login_url)
                        _load_deadline = time.monotonic() + 30.0
                        while time.monotonic() < _load_deadline:
                            try:
                                page.wait_for_load_state('domcontentloaded', timeout=1000)
                                break
                            except Exception:  # noqa: BLE001
                                pass
                            _relay_input_and_screenshot(page, r)
                    else:
                        page.goto(mso_login_url, wait_until='domcontentloaded', timeout=30000)
                except Exception as exc:  # noqa: BLE001
                    if _is_browser_death(exc):
                        raise
                    failed.append(f'{channel.name}: failed to load sign-in page ({exc})')
                    continue
                if mso_id == 'YouTubeTV':
                    try:
                        _maybe_retry_youtubetv_from_scratch(page, gateway_state['responses'], mso_login_url)
                    except Exception as exc:  # noqa: BLE001
                        if _is_browser_death(exc):
                            raise
                        logger.warning(
                            '[amcn-mvpd-login] YouTubeTV retry-from-scratch recovery failed for %s; '
                            'continuing to normal stall detection: %s', channel.name, exc,
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
                        _log_youtubetv_gateway_diagnostics(context, gateway_state['responses'])
                        failed.append(f'{channel.name}: {_youtube_tv_gateway_failure_message()}')
                    else:
                        failed.append(f'{channel.name}: sign-in redirect did not finish within 15 seconds')
                    logger.warning(
                        '[amcn-mvpd-login] aborting stalled provider redirect for %s before polling '
                        '(mso_id=%s landing=%s gateway_responses=%d)',
                        channel.name, mso_id, _url_for_log(landing_url), len(gateway_state['responses']),
                    )
                    continue
                if (
                    mso_id == 'YouTubeTV'
                    and _urlsplit(landing_url).netloc == 'support.google.com'
                    and _urlsplit(landing_url).path.startswith('/accounts/answer/32050')
                ):
                    logger.warning(
                        '[amcn-mvpd-login] Google rejected the primed browser session for %s and redirected to '
                        'its cookie-recovery page (%s)', channel.name, _url_for_log(landing_url),
                    )
                    failed.append(f'{channel.name}: Google rejected the saved browser session')
                    continue
                if account.username and account.password and mso_id == 'Comcast_SSO':
                    _autofill_xfinity_credentials(
                        page, account.username, account.password, r=r,
                        stop_key=MVPD_BROWSER_LOGIN_STOP_KEY, input_key=MVPD_BROWSER_LOGIN_INPUT_KEY,
                    )
                set_status('running', f'Signing in to {channel.name}…', landing_url)

                wait_started = time.monotonic()
                deadline = wait_started + _PER_CHANNEL_TIMEOUT_SECONDS
                last_shot = 0.0
                last_poll = 0.0
                last_progress_log = 0.0
                paired = False
                cancelled = False
                denied_message: str | None = None
                session_poll_interval = _POLL_SECONDS
                last_seen_page_url = landing_url
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

                    current_page_url = _safe_page_url(page)
                    if current_page_url != last_seen_page_url:
                        last_seen_page_url = current_page_url
                        session_poll_interval = _POLL_SECONDS

                    if now - last_poll > session_poll_interval:
                        last_poll = now
                        try:
                            adobe_token, adobe_id, notafter_ms = scraper._adobe_decision_finish(
                                client.session, channel, code, mso_id, auth_headers,
                            )
                        except TVENotAuthorizedError as exc:
                            # A DEFINITIVE answer from Adobe (the MSO login
                            # itself succeeded — the profile has a real
                            # userID — but the entitlement decision was a
                            # clean "Deny"), as opposed to the merely-pending
                            # case below (profile still empty because the
                            # human hasn't finished the MSO login yet). No
                            # point burning the rest of this channel's
                            # timeout re-polling the same denial.
                            #
                            # This branch previously had no log line at all —
                            # a clean denial looked identical to a channel
                            # that just silently moved on, with no evidence
                            # in the logs of what actually happened (confirmed
                            # live 2026-08-20: IFC/WE tv's Adobe session cache
                            # stayed untouched after full SAML+Google
                            # completion, with nothing explaining why until
                            # this was traced to this exact silent branch).
                            logger.info('[amcn-mvpd-login] %s denied: %s', channel.name, exc)
                            denied_message = str(exc)
                            break
                        except Exception as exc:  # noqa: BLE001
                            # Ambiguous pending state — AMCN's
                            # /profiles/code/{code} answers the same way
                            # (200, empty profile) whether the human just
                            # hasn't finished yet or something else is wrong,
                            # so this keeps polling until the deadline.
                            session_poll_interval = min(session_poll_interval * _POLL_BACKOFF, _POLL_MAX_SECONDS)
                            if now - last_progress_log >= 60:
                                logger.info(
                                    '[amcn-mvpd-login] %s poll not-yet/error: %s (next_poll=%.1fs)',
                                    channel.name, exc, session_poll_interval,
                                )
                                last_progress_log = now
                            continue
                        scraper._save_adobe_session_cache(channel, mso_id, code, client.ctx.access_token)
                        scraper._save_adobe_auth_cache(channel, mso_id, adobe_token, adobe_id, notafter_ms)
                        authorized.append(channel.name)
                        paired = True
                        break
                    page.wait_for_timeout(80)
                if mso_id == 'YouTubeTV' and (paired or denied_message is not None):
                    # Either outcome proves the MSO login itself went through
                    # (a real userID came back from Adobe) even if THIS
                    # channel wasn't entitled — see denied_message's own
                    # comment above. Cheap to call per-channel: it no-ops
                    # instantly once a master_token is already on file.
                    _maybe_capture_google_master_token(context, mso_id)
                elif mso_id == 'Comcast_SSO' and paired:
                    # Same idea as the YouTubeTV branch above, for the
                    # Xfinity cookie jar instead of a Google master_token —
                    # was previously missing here entirely (unlike mvpd.py/
                    # nbc.py/fox.py), so a fully successful AMCN browser
                    # login never saved anything for OTHER TVE families'
                    # cookie-jar fast path to reuse. See
                    # _harvest_and_save_xfinity_cookies's docstring.
                    _harvest_and_save_xfinity_cookies(context)
                if cancelled:
                    failed.append(f'{channel.name}: cancelled')
                    break
                if denied_message is not None:
                    failed.append(denied_message)
                elif not paired:
                    failed.append(f'{channel.name}: not entitled or timed out')
    except BaseException as exc:  # noqa: BLE001
        # Fresh, self-contained app_context — see _prime_google_session's
        # docstring: run_amcn_browser_login pops its own before calling this
        # function, so nothing can be assumed pushed by the time execution
        # reaches here (the browser session above runs with none active).
        with flask_app.app_context():
            persist_source_config_updates(source.id, scraper._pending_config_updates)
            persist_source_cache_updates(source.id, scraper._pending_cache_updates)
        if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
            set_status('stopped', f'Cancelled — authorized: {", ".join(authorized)}.' if authorized else 'Cancelled.')
            return
        logger.exception('[amcn-mvpd-login] browser-assisted session failed')
        set_status('error', f'Browser session failed: {exc}')
        return

    with flask_app.app_context():
        persist_source_config_updates(source.id, scraper._pending_config_updates)
        persist_source_cache_updates(source.id, scraper._pending_cache_updates)
    if authorized:
        message = f'Signed in — authorized: {", ".join(authorized)}.'
        if failed:
            message += ' Not authorized: ' + '; '.join(failed) + '.'
        set_status('success', message)
        logger.info('[amcn-mvpd-login] paired mso_id=%s authorized=%s failed=%s (browser-assisted)', mso_id, authorized, failed)
    else:
        message = '; '.join(failed) or 'No AMC Networks channels authorized.'
        _record_tve_login_error('amcn', message)
        set_status('error', message)


def run_amcn_browser_login(mso_id: str):
    """Standalone "Sign in" for AMC Networks TVE.

    Same reasoning as run_discovery_browser_login: resolve() already does a
    fully scripted Cox login per channel family via
    AMCNetworksTVEScraper._adobe_decision_token() (register session, follow
    the Adobe redirect, then _cox_saml_login()'s direct POST to
    login.cox.com/api/v1/authn) — no browser needed. Unlike Discovery, AMCN
    caches auth separately per requestor_id (AMC/BBCA/IFC/WETV each has its
    own adobe_auth:<requestor_id> cache entry, see _adobe_auth_cache_key),
    so "Sign in" here warms all four instead of just one. Passes force=True
    to _adobe_decision_token so a click always exercises a live Cox login
    instead of silently returning a still-valid cached token untested — same
    "Sign in should be real" reasoning as FOX's and FOX One's own buttons
    (see run_fox_browser_login's docstring); without it, re-clicking within
    a token's ~24h lifetime logged a "paired"/"authorized" success in
    milliseconds with no request actually sent and no admin/settings
    timestamp movement to show for it (reported live 2026-08-12).
    """
    # Manual push/pop instead of `with flask_app.app_context():` — see
    # _prime_google_session's docstring: Camoufox's own rendering breaks
    # intermittently while a Flask app_context is active on this thread.
    # Popped right before handing off to _run_amcn_browser_assisted_login
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
        from app.scrapers.amcn_tve import AMCNetworksTVEScraper, CHANNELS

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[amcn-mvpd-login] Redis unavailable, aborting: %s', exc)
            return
        _activity_handler = install_browser_login_activity_log(r)

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    MVPD_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': url, 'requestor_id': 'AMC Networks TVE', 'steps': []}),
                )
            except Exception:  # noqa: BLE001
                pass

        r.delete(MVPD_BROWSER_LOGIN_STOP_KEY)
        r.delete(MVPD_BROWSER_LOGIN_INPUT_KEY)
        set_status('running', 'Signing in to AMC Networks TVE…')

        source = Source.query.filter_by(name='amcn_tve').first()
        if not source:
            set_status('error', 'AMC Networks TVE source not found.')
            return
        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if not account or not account.is_enabled or not account.has_credentials():
            set_status('error', 'TVE credentials are not configured in Settings.')
            return
        scraper = AMCNetworksTVEScraper(config=dict(source.config or {}))
        device_id = scraper._device_id()
        # Force the lazy cache load here, in this thread, before any worker
        # threads touch scraper.cache below — otherwise two threads racing
        # the "is it loaded yet" check could both trigger it concurrently.
        scraper.cache  # noqa: B018

        cfg = account.config or {}
        mso_id = (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or cfg.get('adobe_mso_id') or 'Cox').strip()

        # The 4 channels' logins are fully independent (each its own
        # requestor_id, own adobe_auth:<requestor_id> cache entry, own
        # AdobePassCoxClient/requests.Session — nothing shared but this one
        # scraper instance, and each writes to its own distinct dict key,
        # safe under the GIL) — running them one at a time was ~4x slower
        # than necessary. throttle_cox_login() still serializes the actual
        # Cox POSTs to the same safe spacing either way, so this doesn't
        # change how fast real credential attempts hit Cox/Okta — it only
        # overlaps the OTHER three Adobe API calls each channel makes,
        # which don't touch Cox at all (code review, 2026-08-11; measured
        # live: 4 sequential cold-cache logins took ~30s, ~16s of which was
        # pure serial throttle waiting). Shared by the Cox branch below and
        # the Comcast_SSO cookie-jar-first attempt above it — both just need
        # "run _adobe_decision_token for every channel, bucket the results";
        # _adobe_decision_token's own login_to_mvpd() call already knows how
        # to use mso_id/cookie_jar correctly either way.
        def _scripted_channel_pass():
            stopped = r.exists(MVPD_BROWSER_LOGIN_STOP_KEY)
            authorized, failed = [], []
            if not stopped:
                def _sign_in_one(channel):
                    with flask_app.app_context():
                        try:
                            scraper._adobe_decision_token(channel, account, device_id, force=True)
                            return channel.name, None
                        except TVENotAuthorizedError as exc:
                            return channel.name, f'not entitled ({exc})'
                        except TVEAuthError as exc:
                            return channel.name, str(exc)
                        except Exception as exc:  # noqa: BLE001
                            logger.exception('[amcn-mvpd-login] unexpected failure for %s', channel.name)
                            return channel.name, str(exc)

                with ThreadPoolExecutor(max_workers=4, thread_name_prefix='amcn-mvpd-login') as pool:
                    # Submitted (not as_completed) order so the final message
                    # lists networks in CHANNELS' own order regardless of
                    # which thread happens to finish first.
                    futures = [pool.submit(_sign_in_one, channel) for channel in CHANNELS.values()]
                    for future in futures:
                        name, error = future.result()
                        if error is None:
                            authorized.append(name)
                        else:
                            failed.append(f'{name}: {error}')
            return stopped, authorized, failed

        if mso_id == 'Comcast_SSO':
            # Try a saved cookie jar (harvested from a previous successful
            # Comcast_SSO browser pairing for ANY TVE family — see
            # _harvest_and_save_xfinity_cookies) BEFORE ever opening a
            # browser, same as mvpd.py/nbc.py/fox.py already do. Confirmed
            # live 2026-08-28 this scripted path works unmodified for AMCN
            # too once a jar exists — all 4 channels share one Xfinity
            # account, so "zero channels authorized" cleanly means the jar
            # is missing/stale (falls through to the browser below) while
            # ANY channel authorizing is treated as this attempt's result,
            # same partial-success tolerance the browser flow already has.
            cookie_jar = cfg.get('xfinity_cookie_jar')
            if cookie_jar:
                set_status('running', 'Trying saved sign-in (no browser needed)…')
                stopped, authorized, failed = _scripted_channel_pass()
                persist_source_config_updates(source.id, scraper._pending_config_updates)
                persist_source_cache_updates(source.id, scraper._pending_cache_updates)
                if stopped:
                    set_status('stopped', f'Cancelled — authorized: {", ".join(authorized)}.' if authorized else 'Cancelled.')
                    return
                if authorized:
                    message = f'Signed in — authorized: {", ".join(authorized)} (no browser needed).'
                    if failed:
                        message += ' Not authorized: ' + '; '.join(failed) + '.'
                    set_status('success', message)
                    logger.info(
                        '[amcn-mvpd-login] paired mso_id=Comcast_SSO authorized=%s failed=%s via saved cookie jar (no browser)',
                        authorized, failed,
                    )
                    return
                logger.info(
                    '[amcn-mvpd-login] saved xfinity cookie jar did not authorize any AMCN channel, falling back to browser: %s',
                    failed,
                )
            set_status('running', 'No usable saved sign-in — opening a browser…')

        if mso_id != 'Cox':
            _ctx.pop()
            _ctx_popped['v'] = True
            _run_amcn_browser_assisted_login(r, set_status, source, account, scraper, device_id, mso_id, CHANNELS)
            return

        # Force-stop is checked once up front rather than between channels
        # now — once dispatched, an in-flight login couldn't be interrupted
        # either way (same limitation the old sequential loop had for
        # whichever channel was actively running), and the whole batch is
        # now short enough (~5-10s) that mid-flight cancellation matters
        # much less than it did at ~30s.
        stopped, authorized, failed = _scripted_channel_pass()

        persist_source_config_updates(source.id, scraper._pending_config_updates)
        persist_source_cache_updates(source.id, scraper._pending_cache_updates)

        if stopped:
            set_status('stopped', f'Cancelled — authorized: {", ".join(authorized)}.' if authorized else 'Cancelled.')
        elif authorized:
            message = f'Signed in — authorized: {", ".join(authorized)}.'
            if failed:
                message += ' Not authorized: ' + '; '.join(failed) + '.'
            set_status('success', message)
            logger.info('[amcn-mvpd-login] paired mso_id=%s authorized=%s failed=%s (scripted, no browser)', mso_id, authorized, failed)
        else:
            message = '; '.join(failed) or 'No AMC Networks channels authorized.'
            _record_tve_login_error('amcn', message)
            set_status('error', message)
    finally:
        uninstall_browser_login_activity_log(_activity_handler)
        if not _ctx_popped['v']:
            _ctx.pop()

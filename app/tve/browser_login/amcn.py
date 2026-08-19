"""AMC Networks (AMCN) TVE browser-assisted login."""
import logging
import time
from concurrent.futures import ThreadPoolExecutor
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
    _is_browser_death,
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
    profile_dir = '/data/browser_profiles/mvpd_tve'
    try:
        _os_login.makedirs(profile_dir, exist_ok=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning('[amcn-mvpd-login] could not create profile dir %s: %s', profile_dir, exc)

    _PER_CHANNEL_TIMEOUT_SECONDS = 120
    _POLL_SECONDS = 2.0
    authorized: list[str] = []
    failed: list[str] = []

    try:
        with Camoufox(
            headless='virtual', os='windows', persistent_context=True,
            user_data_dir=profile_dir, window=(1280, 800),
        ) as context:
            page = context.pages[0] if context.pages else context.new_page()
            _prime_google_session(context, mso_id)
            page.on('crash', lambda p: logger.warning('[amcn-mvpd-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
            page.on('close', lambda p: logger.warning('[amcn-mvpd-login] page CLOSE event fired'))
            page.on('pageerror', lambda exc: logger.warning('[amcn-mvpd-login] page JS error: %s', str(exc)[:500]))

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

                try:
                    page.goto(mso_login_url, wait_until='domcontentloaded', timeout=30000)
                except Exception as exc:  # noqa: BLE001
                    if _is_browser_death(exc):
                        raise
                    failed.append(f'{channel.name}: failed to load sign-in page ({exc})')
                    continue
                # See _settle_after_mvpd_navigation's docstring: a
                # page.screenshot() call during Adobe/YouTubeTV's still-in-
                # flight SAML bounce chain silently cancels it. AMCN never
                # calls _try_autofill_credentials (no autofill here at
                # all — human/Google-session driven), so this is the only
                # place that can protect its first screenshot.
                _settle_after_mvpd_navigation(page, set_status=set_status)
                set_status('running', f'Signing in to {channel.name}…', _safe_page_url(page))

                wait_started = time.monotonic()
                deadline = wait_started + _PER_CHANNEL_TIMEOUT_SECONDS
                last_shot = 0.0
                last_poll = 0.0
                paired = False
                cancelled = False
                denied_message: str | None = None
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
                            denied_message = str(exc)
                            break
                        except Exception as exc:  # noqa: BLE001
                            # Ambiguous pending state — AMCN's
                            # /profiles/code/{code} answers the same way
                            # (200, empty profile) whether the human just
                            # hasn't finished yet or something else is wrong,
                            # so this keeps polling until the deadline.
                            logger.info('[amcn-mvpd-login] %s poll not-yet/error: %s', channel.name, exc)
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
    try:
        import json as _json_login
        from app.scrapers.amcn_tve import AMCNetworksTVEScraper, CHANNELS

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[amcn-mvpd-login] Redis unavailable, aborting: %s', exc)
            return

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

        if mso_id != 'Cox':
            _ctx.pop()
            _ctx_popped['v'] = True
            _run_amcn_browser_assisted_login(r, set_status, source, account, scraper, device_id, mso_id, CHANNELS)
            return

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
        # pure serial throttle waiting).
        #
        # Force-stop is checked once up front rather than between channels
        # now — once dispatched, an in-flight login couldn't be interrupted
        # either way (same limitation the old sequential loop had for
        # whichever channel was actively running), and the whole batch is
        # now short enough (~5-10s) that mid-flight cancellation matters
        # much less than it did at ~30s.
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
                # lists networks in CHANNELS' own order regardless of which
                # thread happens to finish first.
                futures = [pool.submit(_sign_in_one, channel) for channel in CHANNELS.values()]
                for future in futures:
                    name, error = future.result()
                    if error is None:
                        authorized.append(name)
                    else:
                        failed.append(f'{name}: {error}')

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
        if not _ctx_popped['v']:
            _ctx.pop()

"""Standalone Spectrum sign-in — validates login against Spectrum's reCAPTCHA
Enterprise + ThreatMetrix gate and harvests the OAuth token pair needed to
drive the Spectrum scraper.

A human drives the actual credential entry via the relayed screenshot exactly
like every other MVPD flow in this package — credentials are typed directly
into the live browser view rendered in the admin UI, never passed through any
other layer of this app.

Uses its OWN persistent Camoufox profile (/data/browser_profiles/spectrum),
isolated from the mvpd_tve profile every Adobe-Pass TVE flow shares — added
2026-09-22 after the account-mismatch guard below revealed just how much
cross-flow cookie carryover the shared profile invited. Isolating it removes
Spectrum from that blast radius entirely (a Cox/Xfinity/NBC session dying or
getting corrupted can no longer touch Spectrum's, and vice versa) with no
downside the other TVE flows would have: Spectrum doesn't share its account
with any sibling network the way e.g. TNT/TBS/truTV all ride one Cox login,
so there's nothing to lose by not sharing its browser state either.

CRITICAL constraint this migration had to respect: a genuinely fresh/incognito
device is NOT safe here — Spectrum's login blocked one outright (403
AUTH_REJECT_BY_RECAPTCHA_PASS_THMX_REJECT_STATUS) while a device with real
prior history passed (confirmed live 2026-09-17, original mvpd_tve-sharing
decision). This profile was therefore seeded as a copy of the already-warmed
mvpd_tve profile at migration time, never created empty — still gated by the
SAME profile-busy check in app.routes.tasks as every other MVPD flow (harmless
extra caution now that it's not literally the same directory, just no longer
load-bearing for correctness the way it is for the shared ones). Long-term
trust-score stability of a profile that only ever visits spectrum.net/cox.com
going forward (vs. the broader cross-site history it inherited from having
been part of the shared pool) is unconfirmed — watch for a THMX rejection
resurfacing over time, same signature as above.

UPDATE 2026-09-23, in tension with the above: a genuinely fresh Camoufox
profile (no mvpd_tve to migrate from at all, real credentials, same trusted
home network as every other test) completed a full real sign-in on its very
first attempt — no THMX rejection whatsoever. A SECOND fresh-profile
registration against the same account minutes later hit a completely
different failure instead: Spectrum's own "Feature Unavailable... please try
again from home" error (an IDID-XXXX code — 4000 seen live here, 4003 in a
real public forum report). A bare retry ~60-90s later, same account/device/
profile, succeeded outright — too fast to be a lasting account-level flag.
Current best read: this looks more like a short-lived rate-limit or a plain
transient backend condition tied to repeated new-device registrations in a
short window, not a fixed "fresh device = rejected" rule. See
_detect_spectrum_feature_unavailable's docstring for the retry logic this
prompted, and set FC_SPECTRUM_DEBUG=1 for a fuller diagnostic trail if this
resurfaces.
"""
from __future__ import annotations

import json
import logging
import re
import time

import redis

from app.worker import flask_app
from app.tve.browser_login.common import (
    _detect_spectrum_feature_unavailable,
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

# Suffix match (not a fixed list of subdomains) so any current or future
# spectrum.net-owned host (watch./id./apis./bare) counts as "ours" without
# needing a code update — everything else found in the profile is foreign
# and gets cleared by the one-time scrub below (see foreign_cookies_scrubbed).
_SPECTRUM_OWN_COOKIE_SUFFIXES = ('spectrum.net',)


def _is_spectrum_own_cookie_domain(domain: str) -> bool:
    bare = (domain or '').lstrip('.')
    return any(bare == suf or bare.endswith('.' + suf) for suf in _SPECTRUM_OWN_COOKIE_SUFFIXES)


_debug_state = {'settings_checked': False}  # populated once per run, while
# the app context is still live — see _spectrum_debug_enabled's docstring.


def _spectrum_debug_enabled() -> bool:
    """See app/debug_flag.py for the general mechanism (root logger is
    hard-capped at INFO, so this is an opt-in-at-INFO pattern, not true
    DEBUG level). Checks FC_SPECTRUM_DEBUG (safe from anywhere, including
    inside the Camoufox browser session) OR AppSettings.debug_logging_enabled
    — the latter via _debug_state, cached once near the top of
    run_spectrum_signin while the app context is still live, since a DB
    query isn't safe to make from inside the browser session (its caller
    has already popped that context by then, same pattern
    _prime_google_session's docstring describes elsewhere in this package)."""
    from app.debug_flag import debug_logging_enabled
    return debug_logging_enabled('FC_SPECTRUM_DEBUG', settings_checked=_debug_state['settings_checked'])


def _debug_log(msg: str, *args) -> None:
    if _spectrum_debug_enabled():
        logger.info('[spectrum-signin][debug] ' + msg, *args)


def _dismiss_spectrum_tos_welcome(page) -> bool:
    """Click through Spectrum's one-time "Welcome to Spectrum TV" consent
    gate (agree to Terms and Conditions + Privacy Policy) automatically,
    same shape as common.py's _autofill_spectrum_sso_confirm but for a
    DIFFERENT screen entirely — this one has no relation to an existing SSO
    session, it's a plain consent screen shown to any account that hasn't
    accepted these terms yet, with a "Continue" button and no password field
    anywhere on it.

    Found live via a real forum report (2026-09-22, community thread post
    #3180): a legacy Cox-migrated account hit this on watch.spectrum.net
    with no handling for it at all — _try_autofill_credentials' own wait
    loop only looks for a visible password field, which this screen never
    has, so it just sits there until its own 12s timeout without ever
    clicking Continue, regardless of whether autofill or a human is
    driving. Never observed once in this session's own extensive testing
    (the account used for that has evidently already accepted these terms
    long ago) — a one-time per-account gate, not something every login
    re-shows, which is exactly why it went unhandled: the only account this
    flow was ever tested against had already cleared it.

    Scoped by CONTENT rather than domain (unlike _autofill_spectrum_sso_confirm,
    built for the generic multi-site MVPD flow) — this flow only ever shows
    watch.spectrum.net pages, so a bare "Continue" button text match alone
    risks catching some other unrelated Continue button somewhere else in
    the app; requiring the "Terms and Conditions" text alongside it keeps
    this specific to the actual consent screen.
    """
    try:
        already = page.evaluate("() => !!window.__fcSpectrumTosClicked")
        if already:
            return False
        if page.get_by_text('Terms and Conditions').count() == 0:
            return False
        btn = None
        for locator in (
            page.get_by_role('button', name='Continue', exact=True),
            page.locator('button:has-text("Continue")'),
        ):
            try:
                if locator.count() > 0:
                    btn = locator.first
                    break
            except Exception:  # noqa: BLE001
                continue
        if btn is None:
            return False
        btn.click(timeout=2000)
        page.evaluate("() => { window.__fcSpectrumTosClicked = true; }")
        logger.info(
            '[spectrum-signin] clicked through the "Welcome to Spectrum TV" '
            'Terms and Conditions/Privacy Policy consent screen url=%s', _safe_page_url(page))
        deadline = time.monotonic() + 5
        start_url = page.url
        while time.monotonic() < deadline and page.url == start_url:
            page.wait_for_timeout(150)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.debug('[spectrum-signin] ToS-welcome click failed: %s', exc)
        return False


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
        # Set by save_source_config when the saved username/password actually
        # change, and by the "Clear sign-in" button (clear_spectrum_auth) —
        # both in app/routes/api_sources.py. Consumed once below to force a
        # real fresh sign-in (clearing Spectrum/Cox cookies before ever
        # trusting a carried-over session) instead of the passive mismatch
        # guard's slower reject-and-wait-for-timeout path. See module
        # docstring's forum-reports paragraph for why a proactive path matters
        # here: someone who just changed credentials wants a real login form
        # NOW, not a multi-minute reject loop.
        force_fresh = bool(saved_cfg.get('force_fresh_signin'))
        # Deliberately NOT the same thing as just_migrated below — installs
        # that already ran the migration BEFORE this scrub existed (every
        # public install between b0b19fe and this commit) already have a
        # populated profile_dir, so "only scrub right after a fresh
        # migration" would never fire for them at all. Keyed to its own
        # persisted flag instead, so it retroactively covers an
        # already-migrated profile the next time this flow runs, not just a
        # brand-new one.
        #
        # Broader than just Cox: inspecting a real migrated profile live
        # (2026-09-22) found cookies AND localStorage for 20+ foreign
        # domains inherited wholesale from the shared mvpd_tve profile —
        # Xfinity (login.xfinity.com/oauth.xfinity.com), Adobe Pass's own
        # domains (api.auth.adobe.com/sp.auth.adobe.com), Google
        # (accounts.google.com), and full TVE network sessions (aetv.com,
        # foxsports.com, history.com, mylifetime.com, nbc.com) — not just
        # cox.com. An earlier version of this scrub only targeted cox.com
        # specifically (the one case that happened to get noticed first);
        # fixed to an allowlist instead — keep ONLY Spectrum's own domains,
        # clear every other cookie found in the profile, so it isn't a
        # denylist that has to be manually extended every time a new foreign
        # domain turns up.
        foreign_cookies_scrubbed = bool(saved_cfg.get('foreign_cookies_scrubbed'))
        # Cached now, while the app context is still live (this function
        # pops it below before launching Camoufox) — see
        # _spectrum_debug_enabled's docstring and app/debug_flag.py's
        # settings_flag_enabled for why a DB read isn't safe once inside
        # the browser session.
        from app.debug_flag import settings_flag_enabled as _settings_flag_enabled
        _debug_state['settings_checked'] = _settings_flag_enabled()
        _debug_log(
            'starting run: username_set=%s password_set=%s force_fresh=%s '
            'foreign_cookies_scrubbed=%s',
            bool(username), bool(password), force_fresh, foreign_cookies_scrubbed)

        # Isolated from the mvpd_tve profile every Adobe-Pass TVE flow shares
        # — see module docstring for why, and the migration constraint (must
        # be seeded as a copy of an already-warmed profile, never created
        # empty).
        #
        # This matters for every existing public install, not just this one:
        # anyone who already signed into Spectrum successfully has real,
        # trusted history sitting in the OLD shared mvpd_tve profile. Without
        # migrating it forward, upgrading to this code would hand them a
        # brand-new EMPTY profile at this path — precisely the
        # genuinely-fresh-device case Spectrum's gate rejects outright,
        # silently regressing every previously-working install the next time
        # its saved session needs a real re-login (which could be weeks
        # later, via the unattended watchdog, with nobody watching). One-time,
        # self-healing, idempotent — same shape as schema.py's boot-time
        # backfills — so no separate migration script or manual step is
        # needed; it just runs itself the first time this flow does after the
        # upgrade. A box that has never signed into ANY MVPD before has no
        # old profile to migrate and starts cold either way, same as it
        # always has.
        profile_dir = '/data/browser_profiles/spectrum'
        _old_shared_profile_dir = '/data/browser_profiles/mvpd_tve'
        just_migrated = False
        try:
            import os as _os_login
            import shutil as _shutil_login
            if not _os_login.path.exists(profile_dir) and _os_login.path.isdir(_old_shared_profile_dir):
                logger.info(
                    '[spectrum-signin] first run on the isolated profile — seeding it from '
                    'the existing %s (preserves the device trust that already passed '
                    'Spectrum\'s recaptcha/ThreatMetrix gate, rather than starting fresh)',
                    _old_shared_profile_dir)
                try:
                    # symlinks=True: a real (not freshly-created) Firefox
                    # profile has a `lock` symlink pointing at "host:pid" —
                    # not a real path, so copytree's default dereferencing
                    # behavior fails on it outright (confirmed live 2026-09-22
                    # against a real prod profile's stale lock left over from
                    # an unclean Camoufox exit; silently degrading to an empty
                    # profile is exactly the failure this migration exists to
                    # prevent). Recreate symlinks as symlinks instead, same as
                    # `cp -a` — the migration must never silently produce an
                    # empty profile just because a stale lock exists.
                    _shutil_login.copytree(_old_shared_profile_dir, profile_dir, symlinks=True)
                    just_migrated = True
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        '[spectrum-signin] could not seed isolated profile from %s (%s) — '
                        'falling back to an empty profile, which may hit a fresh-device '
                        'rejection on first use', _old_shared_profile_dir, exc)
            _os_login.makedirs(profile_dir, exist_ok=True)

            # Filesystem-level counterpart to the in-browser cookie scrub
            # below: cookies aren't the only thing inherited wholesale from
            # mvpd_tve. Firefox stores each origin's localStorage/IndexedDB/
            # Cache API data in its own directory under storage/default/,
            # named "<scheme>+++<host>" — e.g. "https+++login.xfinity.com".
            # The in-browser clear can only reach the CURRENT page's own
            # origin (same-origin policy — page.evaluate()'s
            # localStorage.clear() can't touch a different origin's storage
            # no matter what page it runs from), so a foreign origin's
            # localStorage would otherwise survive indefinitely even after
            # every one of its cookies is gone. Done here, before Camoufox
            # ever opens the profile, rather than from inside it — no need
            # to navigate to 20 different origins one at a time, and no live
            # file-lock contention with Firefox's own process. Same
            # one-time gate as the cookie scrub (foreign_cookies_scrubbed,
            # checked again below); skips moz-extension+++... directories
            # (Camoufox's own bundled extensions, not a real site).
            if not foreign_cookies_scrubbed:
                storage_default_dir = _os_login.path.join(profile_dir, 'storage', 'default')
                if _os_login.path.isdir(storage_default_dir):
                    pruned_origins = []
                    for entry in _os_login.listdir(storage_default_dir):
                        if not (entry.startswith('https+++') or entry.startswith('http+++')):
                            continue
                        parts = entry.split('+++')
                        host = parts[1] if len(parts) > 1 else ''
                        if host and not _is_spectrum_own_cookie_domain(host):
                            target = _os_login.path.join(storage_default_dir, entry)
                            try:
                                _shutil_login.rmtree(target)
                                pruned_origins.append(host)
                            except Exception as exc:  # noqa: BLE001
                                logger.warning(
                                    '[spectrum-signin] could not prune storage dir for %s: %s',
                                    host, exc)
                    if pruned_origins:
                        logger.info(
                            '[spectrum-signin] pruned localStorage/IndexedDB/cache for %d '
                            'foreign origin(s): %s', len(pruned_origins), ', '.join(sorted(pruned_origins)))
        except Exception as exc:  # noqa: BLE001
            logger.warning('[spectrum-signin] could not create profile dir %s: %s', profile_dir, exc)
        _debug_log('profile_dir=%s just_migrated=%s', profile_dir, just_migrated)

        _ctx.pop()
        _ctx_popped['v'] = True
        deadline = time.monotonic() + _SPECTRUM_SIGNIN_TIMEOUT_SECONDS
        captured: dict = {}
        _rejected: set = set()  # access_token values already proven dead — never re-try one
        _last_mismatch: dict = {}  # {'account': str} — last valid-but-wrong-account
        # identity seen, so a final timeout can say WHY instead of a generic
        # "timed out" that looks identical to every other stuck sign-in.
        _observed_username: dict = {}  # {'value': str} — whatever account actually signed
        # in, captured regardless of whether a username was configured, so a
        # blank config field can be auto-populated on success (see
        # save_login_result's observed_username param).

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
        _AUTH_HOST_MARKERS = ('spectrum.net', 'cox.com')

        def _on_response(response):
            try:
                if (
                    _spectrum_debug_enabled()
                    and not response.ok
                    and response.status != 304  # cache revalidation, not an error
                    and response.request.resource_type in ('xhr', 'fetch')
                    and any(m in response.url for m in _AUTH_HOST_MARKERS)
                ):
                    # A wrong password, a WAF/ThreatMetrix block, or a rate
                    # limit all currently look identical from the outside —
                    # the poll loop just keeps waiting until the generic
                    # "Timed out" message. This is the only place that can
                    # tell them apart after the fact, from a user's report
                    # alone, without needing a screenshot enabled.
                    try:
                        body = response.text()[:300]
                    except Exception:  # noqa: BLE001
                        body = '<unreadable>'
                    _debug_log(
                        'non-OK response: %s %s -> %d — body=%r',
                        response.request.method, response.url, response.status, body)
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
            _debug_log('candidate token found (source=%s, len=%d) — validating',
                       'response-interception' if captured.get('candidate') is candidate else 'localStorage',
                       len(candidate['oauth_token']))
            try:
                is_valid = page.evaluate(_VALIDATE_JS, candidate)
            except Exception as exc:  # noqa: BLE001
                logger.debug('[spectrum-signin] validateSession check failed: %s', exc)
                return False
            _debug_log('validateSession result: %s', is_valid)
            if not is_valid:
                logger.info('[spectrum-signin] captured token failed validateSession — discarding, still waiting')
                _rejected.add(candidate['oauth_token'])
                captured.pop('candidate', None)
                return False
            # Read regardless of whether a username is configured — needed
            # both for the mismatch check below AND to auto-populate a blank
            # username field on success (see _observed_username's docstring).
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
            captured_username = captured_username.strip()
            if captured_username:
                _observed_username['value'] = captured_username
            _debug_log('captured_username=%r configured_username=%r', captured_username, username)
            if username:
                # Only reject on a CONFIRMED mismatch — an empty read means we
                # can't tell (e.g. this build of the page doesn't set it) and
                # shouldn't block an otherwise-valid session over missing data.
                if captured_username and captured_username.lower() != username.strip().lower():
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

        def _clear_spectrum_cox_cookies(context, page, reason: str) -> None:
            """Scoped to ONLY Spectrum/Cox cookies — never the whole profile:
            clearing the whole context once wiped every other MVPD's cookies
            back when this profile was still shared (pre-b0b19fe). No longer
            load-bearing for that specific reason now that Spectrum has its
            own isolated profile, but kept scoped anyway — no reason to wipe
            more than the two domains that actually matter here."""
            logger.info('[spectrum-signin] %s — clearing Spectrum/Cox cookies only to force a fresh login', reason)
            try:
                for domain in ('.spectrum.net', 'id.spectrum.net', 'watch.spectrum.net',
                               'apis.spectrum.net', '.cox.com', 'login.cox.com'):
                    context.clear_cookies(domain=domain)
                page.evaluate("() => { try { localStorage.clear(); } catch(e) {} "
                              "try { sessionStorage.clear(); } catch(e) {} }")
            except Exception as exc:  # noqa: BLE001
                logger.warning('[spectrum-signin] cookie clear failed: %s', exc)

        def _clear_force_fresh_flag() -> None:
            """Consumed exactly once per force_fresh_signin request (set by
            save_source_config on a credential change, or the "Clear sign-in"
            button) — pushes its own app_context since the outer one was
            already popped before Camoufox launched (see
            _prime_google_session's docstring in common.py for why)."""
            try:
                with flask_app.app_context():
                    src = Source.query.filter_by(name='spectrum').first()
                    if src and (src.config or {}).get('force_fresh_signin'):
                        cfg = dict(src.config)
                        cfg.pop('force_fresh_signin', None)
                        src.config = cfg
                        from app.extensions import db
                        db.session.commit()
            except Exception as exc:  # noqa: BLE001
                logger.warning('[spectrum-signin] could not clear force_fresh_signin flag: %s', exc)

        def _mark_foreign_cookies_scrubbed() -> None:
            """Persists the one-time foreign_cookies_scrubbed flag so the
            inherited-cookie cleanup above never repeats after its first
            real run — same app_context-pushing pattern as
            _clear_force_fresh_flag, for the same reason (outer context
            already popped before Camoufox launched)."""
            try:
                with flask_app.app_context():
                    src = Source.query.filter_by(name='spectrum').first()
                    if src and not (src.config or {}).get('foreign_cookies_scrubbed'):
                        cfg = dict(src.config or {})
                        cfg['foreign_cookies_scrubbed'] = True
                        src.config = cfg
                        from app.extensions import db
                        db.session.commit()
            except Exception as exc:  # noqa: BLE001
                logger.warning('[spectrum-signin] could not persist foreign_cookies_scrubbed: %s', exc)

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

                def _dismiss_tos_and_autofill() -> None:
                    """Shared between the initial attempt below and the
                    IDID-error retry path inside the main loop, so they
                    can't drift apart. Some accounts (confirmed live: a
                    legacy Cox-migrated one, forum post #3180) land on a
                    one-time "Welcome to Spectrum TV" Terms and Conditions
                    consent screen instead of the login form — no password
                    field on it at all, so _try_autofill_credentials' own
                    wait would just time out without this."""
                    _dismiss_spectrum_tos_welcome(page)
                    if username and password:
                        set_status('running', 'Auto-filling saved credentials…', page.url)
                        _try_autofill_credentials(
                            page, username, password, wait_seconds=12.0, r=r,
                            stop_key=SPECTRUM_SIGNIN_STOP_KEY, input_key=SPECTRUM_SIGNIN_INPUT_KEY,
                            shot_key=SPECTRUM_SIGNIN_SHOT_KEY, hint_key=SPECTRUM_SIGNIN_HINT_KEY,
                            log_tag='spectrum-signin',
                        )
                        set_status('running', 'Sign in below, including any captcha if shown.', page.url)

                if not foreign_cookies_scrubbed:
                    # This profile's non-spectrum.net cookies are
                    # unattributable — some could be Spectrum's own past Okta
                    # round-trip (ex-Cox accounts authenticate through Cox's
                    # Okta org, see module docstring), but most are from
                    # COMPLETELY UNRELATED products: this same shared
                    # mvpd_tve profile is also what every other MVPD/TVE
                    # browser-login uses (Cox/Xfinity/NBC/FOX/AMCN/
                    # Discovery/Google), each writing its own session
                    # cookies. A stale foreign-context session riding along
                    # on this install's Spectrum sign-in could plausibly
                    # confuse or short-circuit Spectrum's own Cox-Okta-backed
                    # device-linking flow the same way a stale Cox one could
                    # — confirmed live 2026-09-22 that a real migrated
                    # profile carried real session cookies for cox.com,
                    # xfinity.com, Adobe Pass's own domains, Google, and
                    # several TVE networks (aetv.com/foxsports.com/
                    # history.com/mylifetime.com/nbc.com), not just cox.com.
                    # An earlier version of this scrub targeted cox.com
                    # specifically (the one case noticed first by inspecting
                    # the cookie DB) — fixed to an ALLOWLIST instead: keep
                    # only Spectrum's own domains, clear every other cookie
                    # actually found in the profile, so a denylist never has
                    # to be manually extended again for the next foreign
                    # domain that turns up. Never clears spectrum.net's own
                    # cookies — that accumulated device trust is the entire
                    # reason the migration exists.
                    #
                    # Deliberately keyed to foreign_cookies_scrubbed, NOT
                    # just_migrated: every public install that already ran
                    # the migration before this scrub existed (b0b19fe
                    # through 0bbec49) has a profile_dir that already exists,
                    # so gating this on "did a migration just happen" would
                    # never fire for them — this needs to retroactively catch
                    # an already-migrated profile too, exactly once, not just
                    # a brand-new one.
                    #
                    # NOTE (confirmed live 2026-09-22 against a real 20-foreign-
                    # domain profile): this reliably clears every actual
                    # session/auth cookie — api.auth.adobe.com, sp.auth.adobe.
                    # com, login.cox.com, oauth.xfinity.com, and the TVE
                    # "play." domains all end up with ZERO cookies left. What
                    # survives on some domains is exclusively analytics/bot-
                    # detection residue (Tealium, Adobe Analytics/Target,
                    # Datadog, Akamai Bot Manager, Cloudflare, Incapsula —
                    # e.g. cox.com's `_cidt`, httpOnly) that resists even an
                    # exact name+domain+path clear — likely Firefox's cookie
                    # partitioning (Total Cookie Protection) holding a
                    # third-party-context copy Playwright can't reach without
                    # a fully unfiltered clear_cookies(), which would also
                    # destroy the spectrum.net trust this migration exists to
                    # preserve. Not a session/identity leak — bot-detection/
                    # analytics continuity tokens, lower risk than what this
                    # guards against.
                    try:
                        foreign_domains = {
                            c['domain'] for c in context.cookies()
                            if not _is_spectrum_own_cookie_domain(c.get('domain', ''))
                        }
                        logger.info(
                            '[spectrum-signin] clearing cookies for %d foreign domain(s) found '
                            'in this profile (%s this run): %s',
                            len(foreign_domains),
                            'just migrated' if just_migrated else 'already migrated previously',
                            ', '.join(sorted(foreign_domains)) or '(none found)')
                        for domain in foreign_domains:
                            context.clear_cookies(domain=domain)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning('[spectrum-signin] could not clear foreign cookies: %s', exc)
                    # Mark it done regardless of whether the clear above fully
                    # succeeded — this is a one-time cleanup, not something to
                    # retry every single sign-in attempt forever (that would
                    # also defeat legitimate same-account SSO carryover after
                    # the first successful post-scrub login).
                    _mark_foreign_cookies_scrubbed()

                page.goto(_START_URL, wait_until='domcontentloaded', timeout=30000)
                set_status('running', 'Sign in below, including any captcha if shown.', page.url)

                if force_fresh:
                    # A human or an automatic credential change explicitly
                    # asked for a real fresh sign-in — never give the SSO-
                    # carryover fast path below a chance to silently reuse
                    # whatever session is still sitting in this profile.
                    set_status('running', 'Forcing a fresh sign-in…', page.url)
                    _clear_spectrum_cox_cookies(
                        context, page,
                        'force_fresh_signin requested (credentials changed, or "Clear sign-in" was used)')
                    page.goto(_START_URL, wait_until='domcontentloaded', timeout=30000)
                    set_status('running', 'Sign in below, including any captcha if shown.', page.url)
                    _clear_force_fresh_flag()
                else:
                    # SSO can carry over instantly via this profile's existing cookies —
                    # the page looks fully signed in (real Guide data, no login form to
                    # autofill) while the underlying token is actually already dead
                    # (confirmed live 2026-09-17: even the page's OWN fetch() with its
                    # OWN localStorage token failed validateSession). Give that a brief
                    # chance to verify; if it's there but invalid, force a real fresh
                    # login instead of sitting forever waiting for a login form that
                    # SSO carryover means will never appear.
                    page.wait_for_timeout(1500)
                    if not _try_capture(page) and _rejected:
                        set_status('running', 'Saved session looks stale — starting a fresh sign-in…', page.url)
                        _clear_spectrum_cox_cookies(context, page, 'SSO-carried-over token failed verification')
                        page.goto(_START_URL, wait_until='domcontentloaded', timeout=30000)
                        set_status('running', 'Sign in below, including any captcha if shown.', page.url)

                _dismiss_tos_and_autofill()

                wait_started = time.monotonic()
                last_shot = 0.0
                last_debug_url_log = 0.0
                last_seen_url = _safe_page_url(page)
                _idid_retries_remaining = 2  # up to 3 total attempts at this error specifically
                while time.monotonic() < deadline:
                    if r.exists(SPECTRUM_SIGNIN_STOP_KEY):
                        set_status('stopped', 'Cancelled')
                        return
                    if page.is_closed():
                        raise _BrowserSessionDied('browser page closed before sign-in completed')
                    # Same consent screen as above — checked here too since
                    # it's not fully confirmed whether it can show up AFTER
                    # credential submission instead of only before (the
                    # forum report's own description reads as if it appeared
                    # right after entering a password), and this is cheap
                    # to check on every poll tick regardless.
                    _dismiss_spectrum_tos_welcome(page)
                    now = time.monotonic()
                    if _spectrum_debug_enabled():
                        current_url = _safe_page_url(page)
                        if current_url != last_seen_url or now - last_debug_url_log > 5.0:
                            if current_url != last_seen_url:
                                _debug_log('page navigated: %s -> %s', last_seen_url, current_url)
                                last_seen_url = current_url
                            last_debug_url_log = now
                    idid_code = _detect_spectrum_feature_unavailable(page)
                    if idid_code:
                        # Confirmed live 2026-09-23: Spectrum's own "Feature
                        # Unavailable... try again from home" error — in the
                        # one case watched end-to-end, a bare retry ~60-90s
                        # later (same account/device/profile) succeeded
                        # outright, too fast to be a lasting account-level
                        # block. See module docstring's 2026-09-23 update and
                        # _detect_spectrum_feature_unavailable's docstring.
                        if _idid_retries_remaining > 0:
                            _idid_retries_remaining -= 1
                            logger.info(
                                '[spectrum-signin] hit Spectrum\'s "%s" error — retrying '
                                '(%d attempt(s) left after this one)', idid_code, _idid_retries_remaining)
                            set_status('running', f'Spectrum returned "{idid_code}" — retrying…', page.url)
                            # A real pause, not an instant hammer — if this
                            # is any kind of rate limit, retrying instantly
                            # would be exactly the wrong move.
                            page.wait_for_timeout(5000)
                            page.goto(_START_URL, wait_until='domcontentloaded', timeout=30000)
                            _dismiss_tos_and_autofill()
                            wait_started = time.monotonic()
                            last_seen_url = _safe_page_url(page)
                            continue
                        else:
                            logger.warning(
                                '[spectrum-signin] hit Spectrum\'s "%s" error again — retries '
                                'exhausted, giving up', idid_code)
                            set_status(
                                'error',
                                f'Spectrum returned "{idid_code}" ("Feature Unavailable... try '
                                f'again from home") and retrying didn\'t help this time — this '
                                f'looked transient in the one case seen live, so try again in a '
                                f'minute or two.')
                            return
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
                    if _spectrum_debug_enabled():
                        # Neither the ToS-consent gate nor the IDID error
                        # detector recognize this page — a snapshot is the
                        # only way to tell a genuinely novel blocker (a
                        # reCAPTCHA/ThreatMetrix challenge, a redesigned
                        # screen, etc.) apart from a plain slow network,
                        # from a user's report alone.
                        try:
                            title = page.title()
                            text = re.sub(r'\s+', ' ', page.inner_text('body')).strip()[:300]
                        except Exception as exc:  # noqa: BLE001
                            title, text = '<unreadable>', str(exc)
                        _debug_log('timed out — final page url=%s title=%r text=%r', page.url, title, text)
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
                save_login_result(captured['verified'], cox_cookies, observed_username=_observed_username.get('value'))
        except Exception as exc:  # noqa: BLE001
            logger.exception('[spectrum-signin] failed to save tokens')
            set_status('error', f'Signed in but failed to save tokens: {exc}')
            return
        set_status('success', 'Signed in to Spectrum.')
    finally:
        uninstall_browser_login_activity_log(_activity_handler)
        if not _ctx_popped['v']:
            _ctx.pop()

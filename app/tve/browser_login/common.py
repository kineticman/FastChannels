"""Shared browser-automation helpers for TVE MVPD login flows (settle/autofill/relay/google-priming/error-recording). Used by 2+ providers in this package."""
import hashlib
import logging
import re as _re
import time
from urllib.parse import parse_qs as _parse_qs, urlsplit as _urlsplit

from app.worker import flask_app
from app.extensions import db
from app.models import TVEAccount
from app.tve.adobe_pass import TVEAuthError, TVENotAuthorizedError, save_xfinity_cookie_jar

logger = logging.getLogger(__name__)


MVPD_BROWSER_LOGIN_STATUS_KEY = 'mvpd:browser-login:status'
MVPD_BROWSER_LOGIN_SHOT_KEY = 'mvpd:browser-login:screenshot'
MVPD_BROWSER_LOGIN_INPUT_KEY = 'mvpd:browser-login:input'
MVPD_BROWSER_LOGIN_STOP_KEY = 'mvpd:browser-login:stop'
MVPD_BROWSER_LOGIN_HINT_KEY = 'mvpd:browser-login:hint'
# How long a single silent/automated wait (autofill detection, or the
# post-submit poll-for-completion loop) can run before we admit we don't know
# whether it's actually stuck or just slow, and suggest the human glance at
# the screenshot. Most silent waits resolve well under this even when no
# password field ever shows (SSO carries over via background API polling,
# not anything visible on the page) — this is intentionally a "we're not
# sure, take a look" nudge, not a "this IS broken" claim.
_MVPD_STUCK_HINT_SECONDS = 10.0

# Single shared key across every MVPD/network family (legacy, NBC, FOX,
# FOX One, Google, AMCN, Discovery) — only one browser-login job can hold
# the shared Camoufox profile at a time (see MVPD_LOGIN_FAMILIES's comment
# in settings.js), so there's never more than one run's worth of activity to
# show and no need to namespace this per family. Feeds the "Sign in to all"
# modal's activity feed: the per-network status message only ever shows the
# latest coarse state ("Signing in…"), which during a long silent stretch
# (e.g. polling Adobe for server-side completion after the page navigates
# away) reads as stuck even though real work is happening — reported live
# 2026-08-22. This mirrors the existing logger.info/.warning calls already
# scattered through every run_*_browser_login() instead of duplicating their
# messages into set_status() calls by hand.
TVE_BROWSER_LOGIN_LOG_KEY = 'tve:browser-login:log'
_TVE_BROWSER_LOGIN_LOG_MAX = 60
_TVE_BROWSER_LOGIN_LOG_TTL_SECONDS = 900


class _BrowserLoginActivityLogHandler(logging.Handler):
    """Mirrors INFO+ records from the app.tve.browser_login logger tree into
    a capped Redis list. Installed/removed per-run by install/uninstall_
    browser_login_activity_log() below — never left attached between runs."""

    def __init__(self, redis_conn):
        super().__init__(level=logging.INFO)
        self._redis = redis_conn
        self.setFormatter(logging.Formatter('%(asctime)s  %(message)s', datefmt='%H:%M:%S'))

    def emit(self, record):
        try:
            line = self.format(record)
            pipe = self._redis.pipeline()
            pipe.rpush(TVE_BROWSER_LOGIN_LOG_KEY, line)
            pipe.ltrim(TVE_BROWSER_LOGIN_LOG_KEY, -_TVE_BROWSER_LOGIN_LOG_MAX, -1)
            pipe.expire(TVE_BROWSER_LOGIN_LOG_KEY, _TVE_BROWSER_LOGIN_LOG_TTL_SECONDS)
            pipe.execute()
        except Exception:  # noqa: BLE001
            pass


def install_browser_login_activity_log(redis_conn):
    """Call once near the top of each run_*_browser_login(), right after its
    own Redis connection is confirmed live. Clears out whatever the previous
    run left behind (so re-opening the modal later never shows a stale
    network's log lines) and returns the handler to pass to
    uninstall_browser_login_activity_log() in that function's outer
    `finally`."""
    try:
        redis_conn.delete(TVE_BROWSER_LOGIN_LOG_KEY)
    except Exception:  # noqa: BLE001
        pass
    handler = _BrowserLoginActivityLogHandler(redis_conn)
    logging.getLogger('app.tve.browser_login').addHandler(handler)
    return handler


def uninstall_browser_login_activity_log(handler):
    if handler is None:
        return
    try:
        logging.getLogger('app.tve.browser_login').removeHandler(handler)
    except Exception:  # noqa: BLE001
        pass


def _safe_page_url(page) -> str:
    try:
        return page.url
    except Exception:  # noqa: BLE001
        return '<unknown - page unresponsive>'


def _same_page_url(actual: str, expected: str) -> bool:
    """Compares scheme+host+path only, ignoring query/fragment — Adobe or the
    destination site sometimes appends tracking params on the bounce-back, so
    an exact string match would miss real matches."""
    try:
        a = _urlsplit(actual)
        e = _urlsplit(expected)
        return (a.scheme, a.netloc, a.path.rstrip('/')) == (e.scheme, e.netloc, e.path.rstrip('/'))
    except Exception:  # noqa: BLE001
        return False


# Path fragments seen live (2026-08-19) on Adobe/YouTubeTV's intermediate
# SAML relay hops — never treated as a "settled" destination regardless of
# how long the URL holds steady there, since each is just waiting on its
# own delayed JS redirect timer. /saml/module.php/ covers both the blank
# "bookend"/authbypass interstitial (originally identified against Sling)
# and the authgoogle/linkback hop; /api/v2/authenticate/ covers Adobe's own
# landing page before it hands off to Google (sp.auth.adobe.com for the
# legacy/NBC family, api.auth.adobe.com for FOX's).


_MVPD_TRANSITIONAL_URL_MARKERS = ('/saml/module.php/', '/api/v2/authenticate/')


def _settle_after_mvpd_navigation(
    page, max_seconds: float = 15.0, set_status=None,
    respect_youtubetv_soft_block: bool = True,
) -> bool:
    """Pure wait — zero page.screenshot() calls, zero locator queries,
    nothing but page.wait_for_timeout() and reading page.url (a cheap local
    property, not an IPC round-trip — safe to poll freely) — right after
    navigating to an MSO's real login URL, until the URL stops changing or
    max_seconds elapses.

    Confirmed live 2026-08-19 via a clean single-variable bisection:
    Adobe/YouTubeTV's SAML bounce chain (a JS-driven window.location relay
    through 2-3 intermediate hosts, e.g. .../authbypass/firstbookend.php ->
    youtube.auth-gateway.net -> accounts.google.com) silently gets cancelled
    if page.screenshot() is called at ANY point while it's still mid-
    transition — the page then sits on a permanently blank intermediate hop
    forever. page.wait_for_timeout() alone does not have this effect, and
    once the chain has actually landed on a stable page, screenshotting it
    repeatedly afterward is completely harmless.

    Adaptive rather than a fixed delay — the chain's actual duration varies
    live-observed anywhere from ~1s to 12+s. A plain "URL hasn't changed in
    1.5s" stability check isn't enough on its own, either — confirmed live
    2026-08-19 that the bookend/authbypass hop itself can sit at the SAME
    URL for well over a second before ITS OWN delayed JS timer finally fires
    the next redirect, so a naive stability check exits early while still
    on it. Known-transitional URL markers (below) are therefore never
    treated as "stable" regardless of how long they've held, no matter how
    long that takes — only max_seconds forces a give-up. Once genuinely off
    all known markers, a real 1.5s-stable check still applies (fast path
    for MSOs that never bounce through any of this at all).

    Call this unconditionally right after every page.goto()/page.evaluate()
    navigation to an MSO's real login URL, BEFORE the first screenshot ever
    fires — not just inside _try_autofill_credentials, since that's skipped
    entirely when no username/password is saved (confirmed live: this bug
    reproduced even after credentials were cleared), which would otherwise
    leave the very next screenshot (the main poll loop's first one)
    unprotected.

    set_status, if given, gets ONE call up front — reported live 2026-08-19:
    with no status update at all during this window, a human watching the
    modal sees the exact same "nothing happening" as an actually-stuck
    session, no way to tell them apart. Text-only (the whole point of this
    function is to avoid touching the page beyond wait_for_timeout()/reading
    .url — a screenshot call here would defeat it), so it can't show live
    progress, just that this phase is expected and still in progress.

    Returns True if it settled normally (moved off every known-transitional
    marker, or was never on one to begin with). Returns False if it gave up
    at max_seconds while STILL on a known-transitional URL — every clean
    success observed live took 1-3s, so still being there at the cap is a
    real signal something's wrong (very likely upstream rate-limiting from
    Adobe/Google, not this code — confirmed live 2026-08-19: reproduced with
    a completely fresh, never-used browser profile and zero automation
    touching the page at all). Callers should surface that distinctly rather
    than silently falling back to their normal "signing in" messaging,
    which otherwise looks identical to a session that's actually progressing
    normally, just slow to render.

    Checks adobe_pass.load_adobe_youtubetv_soft_block() FIRST and skips the
    wait entirely (returns False immediately) if one is active — no point
    burning another max_seconds on a hop already known to be dead, and every
    extra attempt plausibly just extends whatever's rate-limiting it. If it
    DOES still time out while stuck, also checks page.content() for the
    known "empty <body></body>" signature (see
    ADOBE_YOUTUBETV_SOFT_BLOCK_SECONDS's docstring) and records a fresh
    block if it matches, so every OTHER network's next attempt gets the fast
    skip too instead of independently re-discovering the same dead end.
    """
    try:
        from app.tve import adobe_pass
        with flask_app.app_context():
            account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
            cfg = (account_row.config or {}) if account_row else {}
            current_mso_id = (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or cfg.get('adobe_mso_id') or '').strip()
            # This block only ever means "YouTubeTV's SAML bounce chain looked
            # rate-limited" — gate on the account's MSO actually being
            # YouTubeTV right now, or a login against some other MVPD (Cox,
            # Xfinity, DirecTV) would wrongly show this message and skip its
            # wait too, even though that MVPD's login page is unrelated and
            # was never touched.
            existing_block = (
                adobe_pass.load_adobe_youtubetv_soft_block(account_row)
                if account_row and current_mso_id == 'YouTubeTV' else None
            )
    except Exception:  # noqa: BLE001
        existing_block = None
    if existing_block and respect_youtubetv_soft_block:
        retry_at = time.strftime('%H:%M', time.localtime(existing_block['retry_after']))
        if set_status is not None:
            try:
                set_status(
                    'running',
                    f"Adobe/YouTubeTV sign-in looked rate-limited as of {time.strftime('%H:%M', time.localtime(existing_block['detected_at']))} "
                    f"— skipping the wait this time rather than repeating the same dead end. Should clear on its own by {retry_at}.",
                )
            except Exception:  # noqa: BLE001
                pass
        return False
    if set_status is not None:
        try:
            set_status('running', 'Waiting for sign-in redirect to complete…')
        except Exception:  # noqa: BLE001
            pass
    deadline = time.monotonic() + max_seconds
    last_url = None
    stable_since = None
    while time.monotonic() < deadline:
        try:
            current_url = page.url
        except Exception:  # noqa: BLE001
            current_url = None
        now = time.monotonic()
        if current_url != last_url:
            last_url = current_url
            stable_since = now
        is_known_transitional = bool(current_url) and any(
            marker in current_url for marker in _MVPD_TRANSITIONAL_URL_MARKERS
        )
        if not is_known_transitional and stable_since is not None and now - stable_since >= 1.5:
            return True
        page.wait_for_timeout(200)
    still_stuck = bool(last_url) and any(marker in last_url for marker in _MVPD_TRANSITIONAL_URL_MARKERS)
    if still_stuck:
        # Confirm the known soft-block signature (an empty <body></body> —
        # the page never actually rendered its real interstitial at all,
        # as opposed to genuinely being slow) before recording a block that
        # gates every OTHER network's next attempt too — a false positive
        # here would wrongly skip the wait on a hop that just needed more
        # time. len() < 200 is generous; the real signature observed live
        # is 39 bytes exactly ('<html><head></head><body></body></html>').
        try:
            html = page.content()
        except Exception:  # noqa: BLE001
            html = ''
        if len(html) < 200:
            # Non-secret response fingerprint for a later controlled retest
            # to compare against — see save_adobe_youtubetv_soft_block()'s
            # `details` docstring. ppp increments per hit and is the leading
            # "is this an attempt counter" signal so far; bbp being absent
            # means the server never even started the expected handshake.
            try:
                cookies = page.context.cookies()
            except Exception:  # noqa: BLE001
                cookies = []
            cookie_names = sorted({c.get('name', '') for c in cookies if c.get('name')})
            ppp = next((c.get('value') for c in cookies if c.get('name') == 'ppp'), None)
            bbp_present = any(c.get('name') == 'bbp' for c in cookies)
            details = {
                'timestamp': int(time.time()),
                'url': last_url[:200],
                'body_length': len(html),
                'cookie_names': cookie_names,
                'ppp': ppp,
                'bbp_present': bbp_present,
            }
            try:
                from app.tve import adobe_pass
                with flask_app.app_context():
                    account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
                    if account_row is not None:
                        adobe_pass.save_adobe_youtubetv_soft_block(
                            account_row, reason=f'empty body ({len(html)} chars) on {last_url[:150]}',
                            details=details,
                        )
                logger.warning(
                    '[mvpd-login] recorded an Adobe/YouTubeTV soft-block — empty body on %s (ppp=%s bbp_present=%s)',
                    last_url[:150], ppp, bbp_present,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning('[mvpd-login] failed to record Adobe soft-block (non-fatal): %s', exc)
        if set_status is not None:
            try:
                set_status(
                    'running',
                    "Sign-in redirect is taking much longer than usual — likely a temporary block on the provider's "
                    "side rather than anything wrong here. Worth cancelling and trying again later rather than waiting.",
                )
            except Exception:  # noqa: BLE001
                pass
    return not still_stuck


# ── Shared YouTubeTV-as-MVPD gateway helpers ────────────────────────────────
# Everything below was originally written for run_nbc_browser_login, then
# extracted here (2026-08-19/20) once run_mvpd_browser_login needed the same
# recovery: TNT/Warner hit the identical stall — an empty second SAML bookend
# on youtube.auth-gateway.net — via the completely separate mvpd.py code
# path, confirming this is a property of the shared YouTubeTV/Adobe gateway,
# not anything NBC-specific. Pure functions, no coupling to either caller's
# Redis keys or local state.

# Give the YouTubeTV sign-in its own persistent Camoufox profile, isolated
# from the shared /data/browser_profiles/mvpd_tve profile every other MSO
# (Cox/Xfinity/DirecTV/Sling) in this package uses. Both run_nbc_browser_login
# and run_mvpd_browser_login use this same profile for mso_id == 'YouTubeTV'
# — the two callers are mutually exclusive at any given time (see
# _MVPD_TVE_PROFILE_JOB_IDS in app/routes/tasks.py, one shared busy-lock
# across every browser-login flow in this package), so sharing it is safe
# and lets a Google session warmed by one caller's successful pairing carry
# over to the other's next attempt instead of every caller cold-priming its
# own copy.
_YOUTUBETV_ISOLATED_PROFILE_DIR = '/data/browser_profiles/youtubetv'
# Firefox enables Total Cookie Protection by default, which was blocking
# Synacor's authbypass bookend during its short-lived ppp cross-site cookie
# probe (confirmed live 2026-08-19/20) — allow cross-site cookies for this
# controlled, isolated profile rather than disabling protection everywhere.
_YOUTUBETV_CAMOUFOX_FIREFOX_PREFS = {
    'network.cookie.cookieBehavior': 0,
    'network.cookie.cookieBehavior.pbmode': 0,
    'privacy.trackingprotection.enabled': False,
    'privacy.trackingprotection.pbmode.enabled': False,
}


def _url_for_log(url: str) -> str:
    """Return a useful URL location without auth/query tokens."""
    try:
        parts = _urlsplit(url)
        return f'{parts.scheme}://{parts.netloc}{parts.path}'
    except Exception:  # noqa: BLE001
        return '<unavailable>'


def _youtube_tv_gateway_failure_message() -> str:
    return (
        'YouTube TV’s sign-in gateway did not hand off to Google and returned a blank relay page. '
        'The attempt stopped before displaying that unusable page; the server log now contains sanitized diagnostics.'
    )


def _cookie_names(raw_cookie_header: str) -> str:
    """Return cookie names only; auth cookie values must never reach logs."""
    names = []
    for item in (raw_cookie_header or '').split(';'):
        name = item.strip().split('=', 1)[0]
        if name:
            names.append(name)
    return ','.join(sorted(set(names))) or '-'


def _set_cookie_names(raw_set_cookie: str) -> str:
    """Extract Set-Cookie names without logging values or attributes."""
    names = _re.findall(r'(?:^|[,\n])\s*([^=;,\s]+)=', raw_set_cookie or '')
    return ','.join(sorted(set(names))) or '-'


def _gateway_url_for_log(url: str) -> str:
    """Keep only non-secret relay counters from a YouTube TV gateway URL."""
    safe = _url_for_log(url)
    try:
        query = _parse_qs(_urlsplit(url).query)
        counters = []
        for key in ('history', 'coeff'):
            value = (query.get(key) or [''])[0]
            if value.lstrip('-').isdigit():
                counters.append(f'{key}={value[:12]}')
        return f"{safe} ({' '.join(counters)})" if counters else safe
    except Exception:  # noqa: BLE001
        return safe


def _maybe_retry_youtubetv_from_scratch(page, responses: list, original_url: str) -> bool:
    """Recover from a zero-byte second SAML bookend by re-navigating to the
    original SSO entry point, NOT the gateway's own ppp/restart.php hop.

    First version of this function (2026-08-19) tried resuming via
    ppp/restart.php, on the theory that a healthy trace's two bookends are
    followed by a request there. Confirmed live 2026-08-19/20 across FOX, FOX
    One, and 3 separate AMC Networks channels — 5/5 failures, always the
    identical `HTTP 400 "Bad request received"` (a Synacor SimpleSAMLphp
    error page). The `ppp` cookie present at that point always has Max-Age=0
    (already dead by design) — restart.php has no live state left to resume
    from, so trying it is a guaranteed-failed extra round trip, not a
    recovery.

    This version just starts over: a completely fresh navigation to the same
    mso_login_url the caller originally used, which mints a brand-new `ppp`
    cookie instead of trying to reuse a dead one. Justified by the same live
    evidence: IFC's second bookend came back healthy with zero intervention
    on the very next fresh attempt in the same browser session as 3 failures
    — the stall looks like it's per-transaction gateway flakiness, not
    anything sticky about the session/account, so retrying from scratch is a
    fresh roll of the same dice that already lands clean a good fraction of
    the time.

    Returns True when a retry navigation was started.
    """
    page.wait_for_timeout(500)
    current_url = _safe_page_url(page)
    current = _urlsplit(current_url)
    if (
        current.netloc != 'youtube.auth-gateway.net'
        or not current.path.endswith('/authbypass/firstbookend.php')
    ):
        return False

    bookends = []
    for response in responses:
        try:
            parts = _urlsplit(response.url)
            if (
                parts.netloc == 'youtube.auth-gateway.net'
                and parts.path.endswith('/authbypass/firstbookend.php')
            ):
                bookends.append(response)
        except Exception:  # noqa: BLE001
            continue
    if len(bookends) < 2:
        return False

    last_headers = getattr(bookends[-1], 'headers', {}) or {}
    if last_headers.get('content-length') != '0' or last_headers.get('location'):
        return False

    logger.warning(
        '[mvpd-login] second YouTubeTV bookend was zero-byte with no redirect; '
        'retrying from the original sign-in URL instead of the gateway restart endpoint'
    )
    page.evaluate('(url) => { window.location.replace(url); }', original_url)
    return True


def _log_youtubetv_gateway_diagnostics(context, responses: list) -> None:
    """Inspect retained relay responses only after navigation has stalled.

    Reading full headers or bodies during the live relay can affect timing. The
    response callback retains objects only; heavier reads happen after the
    15-second settle has failed. SAML/OAuth and cookie values are reduced to
    URL paths, cookie names, lengths, and hashes — EXCEPT on a 4xx/5xx
    response, where a short body snippet is logged verbatim too (added
    2026-08-20 after ppp/restart.php came back HTTP 400 with no visible
    reason). An MVPD gateway's own error page is expected to be a generic
    "bad request" message, not a page that echoes back session secrets —
    unlike the 200 SAML/OAuth hops this function also inspects, which do
    carry real assertions/tokens and stay hash-only.
    """
    for sequence, response in enumerate(responses, start=1):
        request = getattr(response, 'request', None)
        try:
            request_headers = request.all_headers() if request else {}
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                '[mvpd-login] gateway diagnostic #%d request headers unavailable: %s',
                sequence, exc,
            )
            request_headers = {}
        try:
            response_headers = response.all_headers()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                '[mvpd-login] gateway diagnostic #%d response headers unavailable: %s',
                sequence, exc,
            )
            response_headers = getattr(response, 'headers', {}) or {}
        try:
            body = response.body()
            lowered = body.lower()
            body_summary = (
                'bytes=%d sha256=%s form=%s meta_refresh=%s js_location=%s'
                % (
                    len(body), hashlib.sha256(body).hexdigest()[:12],
                    b'<form' in lowered,
                    b'http-equiv="refresh"' in lowered or b"http-equiv='refresh'" in lowered,
                    b'location' in lowered,
                )
            )
            status = getattr(response, 'status', 0)
            if isinstance(status, int) and status >= 400:
                snippet = body[:300].decode('utf-8', errors='replace').replace('\n', ' ').replace('\r', ' ')
                body_summary += f' error_text={snippet!r}'
        except Exception as exc:  # noqa: BLE001
            body_summary = f'unavailable={type(exc).__name__}'

        try:
            redirected_from = request.redirected_from if request else None
        except Exception:  # noqa: BLE001
            redirected_from = None
        location = response_headers.get('location', '')
        refresh = response_headers.get('refresh', '')
        if location:
            location = _url_for_log(location)
        if refresh:
            refresh = refresh.split(';', 1)[0][:40]
        logger.warning(
            '[mvpd-login] YouTubeTV gateway diagnostic #%d: HTTP %s method=%s url=%s '
            'redirected_from=%s request_cookie_names=%s sec_fetch=(site=%s mode=%s dest=%s) '
            'response=(type=%s length=%s location=%s refresh=%s set_cookie_names=%s) body=(%s)',
            sequence, getattr(response, 'status', '?'), getattr(request, 'method', '?'),
            _gateway_url_for_log(getattr(response, 'url', '')),
            _url_for_log(redirected_from.url) if redirected_from else '-',
            _cookie_names(request_headers.get('cookie', '')),
            request_headers.get('sec-fetch-site', '-'), request_headers.get('sec-fetch-mode', '-'),
            request_headers.get('sec-fetch-dest', '-'), response_headers.get('content-type', '-'),
            response_headers.get('content-length', '-'), location or '-', refresh or '-',
            _set_cookie_names(response_headers.get('set-cookie', '')), body_summary,
        )

    try:
        gateway_cookies = context.cookies('https://youtube.auth-gateway.net/')
    except Exception as exc:  # noqa: BLE001
        logger.warning('[mvpd-login] YouTubeTV gateway cookie diagnostic unavailable: %s', exc)
        return
    cookie_summary = []
    now = time.time()
    for cookie in sorted(gateway_cookies, key=lambda item: item.get('name', '')):
        expires = cookie.get('expires', -1)
        ttl = (
            'session'
            if not isinstance(expires, (int, float)) or expires <= 0
            else f'{int(expires - now)}s'
        )
        cookie_summary.append(
            f"{cookie.get('name', '?')}[domain={cookie.get('domain', '?')},"
            f"path={cookie.get('path', '?')},secure={cookie.get('secure', False)},"
            f"sameSite={cookie.get('sameSite', '-')},ttl={ttl}]"
        )
    logger.warning(
        '[mvpd-login] YouTubeTV gateway cookies after stall: %s',
        '; '.join(cookie_summary) if cookie_summary else '<none>',
    )


def _try_autofill_credentials(
    page, username: str, password: str, wait_seconds: float = 12.0, r=None,
    stop_key: str | None = None, input_key: str | None = None,
    shot_key: str | None = None, hint_key: str | None = None,
    navigation_already_settled: bool = False,
) -> bool:
    """Best-effort, short-timeout sibling of _autofill_sling_credentials for
    run_mvpd_browser_login's single-network browser-assisted flow.

    If r (a redis client) is given, this also relays screenshots/input and
    surfaces a "may need your input" hint the same way the poll loops after
    it do (see _relay_input_and_screenshot) — previously this whole up-to-12s
    wait had NO screenshot/input activity at all, so the modal looked frozen
    for the first 12s of every single network, on top of whatever it froze
    for afterward. Optional and defaults to off so callers that don't have a
    redis client handy (e.g. the F5-recovery retry) still work unchanged.

    Empirically (2026-08-05), Sling's login session does NOT reliably carry
    over via cookies between separate Camoufox launches/navigations even
    within the same persistent profile — every additional network genuinely
    needed a fresh interactive login, not just a silent SSO redirect. Rather
    than depend on that, this actively fills and submits the stored
    credentials on whatever login form the shared page lands on, using the
    SAME saved account the human already used for the first network. Never
    raises — returns False (and the caller falls through to its normal
    poll-and-give-up path) if no matching form shows up in time, e.g.
    because SSO actually DID carry over this time, or a captcha blocks it.

    Mechanics proven by a live dry-run against Sling (2026-08-05): the Adobe
    authenticate URL first lands on a BLANK F5 "bookend" interstitial
    (firstbookend.php) whose only inputs are hidden SAML relay fields; the
    real authSynacor login form replaces it ~4-5s later. So the wait keys on
    a VISIBLE password field, never a raw input count — the old count>=2
    check matched the hidden bookend fields and "filled" an invisible form,
    which is why autofill appeared to do nothing. The real form also animates
    in (clicks need a settle + generous timeout, with force/JS-focus
    fallbacks) and can re-render mid-fill, so both values are verified to
    have actually stuck before submitting, with one retry.
    """
    deadline = time.monotonic() + wait_seconds
    wait_started = time.monotonic()
    # This function is also called on its own after an F5-recovery reload, so
    # settling remains the safe default. NBC explicitly passes True after its
    # immediately preceding settle succeeds; repeating the settle there adds
    # delay and, on a known-dead YouTubeTV relay, used to start the 12-second
    # autofill wait that ultimately published a white screenshot.
    if not navigation_already_settled:
        _settle_after_mvpd_navigation(page, max_seconds=min(8.0, wait_seconds))
    last_relay = time.monotonic()
    while time.monotonic() < deadline:
        try:
            if page.locator('input[type="password"]:visible').count() > 0:
                break
        except Exception as exc:  # noqa: BLE001
            logger.info('[mvpd-login] autofill: locator query failed: %s', exc)
            return False
        if r is not None:
            now = time.monotonic()
            if now - last_relay >= 1.0:
                last_relay = now
                if _relay_input_and_screenshot(
                    page, r, waiting_since=wait_started,
                    stop_key=stop_key, input_key=input_key, shot_key=shot_key, hint_key=hint_key,
                ):
                    logger.info('[mvpd-login] autofill: cancelled while waiting for a password field')
                    return False
        page.wait_for_timeout(300)
    else:
        logger.info('[mvpd-login] autofill: no visible password field after %.1fs (SSO already past login, a captcha-first page, or an unrecognized form) url=%s', wait_seconds, _safe_page_url(page))
        return False

    page.wait_for_timeout(1200)  # the form animates in — let it become clickable/stable

    def _focus_and_type(field, value):
        try:
            field.click(timeout=8000)
        except Exception:  # noqa: BLE001
            try:
                field.click(timeout=2000, force=True)
            except Exception:  # noqa: BLE001
                field.evaluate('el => el.focus()')
        # Clear any prefilled/remembered value first — Cox's Okta widget
        # remembers the username in the persistent profile, and typing appends
        # (observed live 2026-08-06: 9 remembered + 17 typed = 26-char user
        # field, compounding on the verify-retry). Also cleans up our own
        # attempt-1 leftovers on a retry.
        try:
            if field.input_value(timeout=1500):
                field.fill('', timeout=3000)
        except Exception:  # noqa: BLE001
            pass
        field.press_sequentially(value, delay=30, timeout=15000)

    try:
        for fill_attempt in (1, 2):
            pw_loc = page.locator('input[type="password"]:visible')
            if pw_loc.count() == 0:
                logger.info('[mvpd-login] autofill: password field disappeared before fill (page likely mid-redirect) url=%s', _safe_page_url(page))
                return False
            pw_field = pw_loc.first

            user_field = None
            for selector in (
                'input[type="email"]:visible',
                'input[type="text"][name*="email" i]:visible, input[type="text"][name*="user" i]:visible',
                'input[type="text"]:visible',
            ):
                loc = page.locator(selector)
                if loc.count() > 0:
                    user_field = loc.first
                    break
            if user_field is None:
                logger.info('[mvpd-login] autofill: password field present but no visible email/text input url=%s', _safe_page_url(page))
                return False

            _focus_and_type(user_field, username)
            _focus_and_type(pw_field, password)
            page.wait_for_timeout(300)

            got_user = user_field.input_value(timeout=2000)
            got_pw = pw_field.input_value(timeout=2000)
            if got_user != username or got_pw != password:
                logger.info('[mvpd-login] autofill: values did not stick (attempt %d): user %d/%d chars, password %d/%d chars — page likely re-rendered mid-fill',
                            fill_attempt, len(got_user), len(username), len(got_pw), len(password))
                page.wait_for_timeout(700)
                continue

            pw_field.press('Enter')
            logger.info('[mvpd-login] autofill: filled and submitted credentials for %s (attempt %d)', username, fill_attempt)
            return True

        logger.info('[mvpd-login] autofill: gave up — could not get a stable filled form url=%s', _safe_page_url(page))
        return False
    except Exception as exc:  # noqa: BLE001
        logger.info('[mvpd-login] autofill: exception mid-fill: %s', exc)
        return False


_XFINITY_USER_SELECTOR = "prism-input-text[name='user'] input:visible, input[autocomplete='username']:visible"
_XFINITY_PASSWORD_SELECTOR = "prism-input-text[name='passwd'] input:visible, input[type='password']:visible"
_XFINITY_SUBMIT_SELECTOR = "prism-button[prism-id='sign_in'], button[type='submit']"


def _autofill_xfinity_credentials(
    page, username: str, password: str, wait_seconds: float = 20.0, r=None,
    stop_key: str | None = None, input_key: str | None = None,
    shot_key: str | None = None, hint_key: str | None = None,
) -> bool:
    """Xfinity-specific sibling of _try_autofill_credentials for
    run_mvpd_browser_login's Comcast_SSO path.

    The generic single-step autofill above waits for a password field to be
    visible before touching anything, then fills both fields at once. That
    never fires here: Xfinity's login is identifier-first (submit username
    alone; a password step only renders afterward) and built on Comcast's
    own Prism UI web components (<prism-input-text>, <prism-button> — not
    plain <input>/<button>). Confirmed live 2026-08-14 that Playwright's
    shadow-DOM piercing reaches the real underlying <input> fine via
    `prism-input-text[name='user'] input` / `[name='passwd'] input`, and
    both steps submit via `prism-button[prism-id='sign_in']`.

    Also best-effort dismisses the post-login "Add your email address"
    account-hygiene screen via its "Ask me later" skip — confirmed live
    2026-08-14 this is a skippable nag, not a real second factor, at least
    for the account tested. A genuine OTP/2FA prompt still falls through to
    the caller's normal poll-and-give-up path untouched.

    Never raises — returns False if no matching form shows up in time.
    """
    def _wait_for_any(selector: str, deadline: float) -> bool:
        while time.monotonic() < deadline:
            try:
                if page.locator(selector).count() > 0:
                    return True
            except Exception as exc:  # noqa: BLE001
                logger.info('[mvpd-login] xfinity autofill: locator query failed: %s', exc)
                return False
            if r is not None:
                nonlocal last_relay
                now = time.monotonic()
                if now - last_relay >= 1.0:
                    last_relay = now
                    if _relay_input_and_screenshot(
                        page, r, waiting_since=wait_started,
                        stop_key=stop_key, input_key=input_key, shot_key=shot_key, hint_key=hint_key,
                    ):
                        logger.info('[mvpd-login] xfinity autofill: cancelled mid-wait')
                        raise _XfinityAutofillCancelled()
            page.wait_for_timeout(300)
        return False

    class _XfinityAutofillCancelled(Exception):
        pass

    def _fill_and_submit_username(tag: str = '') -> None:
        user_field = page.locator(_XFINITY_USER_SELECTOR).first
        user_field.click(force=True, timeout=8000)
        try:
            if user_field.input_value(timeout=1000):
                user_field.fill('', timeout=2000)
        except Exception:  # noqa: BLE001
            pass
        user_field.fill(username)
        submit = page.locator(_XFINITY_SUBMIT_SELECTOR).first
        try:
            submit.click(force=True, timeout=5000)
        except Exception:  # noqa: BLE001
            page.keyboard.press('Enter')
        logger.info('[mvpd-login] xfinity autofill: submitted username for %s%s', username, tag)

    wait_started = time.monotonic()
    last_relay = wait_started

    try:
        if not _wait_for_any(_XFINITY_USER_SELECTOR, time.monotonic() + wait_seconds):
            # The page can look fully rendered (real login form visible in a
            # screenshot) while the underlying React app failed to hydrate —
            # confirmed live 2026-08-14 via a "Minified React error #418"
            # (hydration mismatch) in the page's console right before this
            # exact timeout, most likely from stale cookies/localStorage in
            # the persistent browser profile (shared across every network's
            # different client_id) conflicting with THIS page's server-
            # rendered assumptions. A reload forces a fresh hydration attempt
            # instead of staring at a permanently-dead shell for the rest of
            # the job's budget.
            logger.info('[mvpd-login] xfinity autofill: no visible username field after %.1fs — reloading once and retrying url=%s', wait_seconds, _safe_page_url(page))
            try:
                page.reload(wait_until='domcontentloaded', timeout=30000)
            except Exception as exc:  # noqa: BLE001
                logger.info('[mvpd-login] xfinity autofill: reload failed: %s', exc)
                return False
            if not _wait_for_any(_XFINITY_USER_SELECTOR, time.monotonic() + wait_seconds):
                logger.info('[mvpd-login] xfinity autofill: still no visible username field after reload (SSO already past login, or an unrecognized form) url=%s', _safe_page_url(page))
                return False

        # Real, settled login.xfinity.com URL — captured HERE, the moment the
        # username field is confirmed visible, not passed in by the caller.
        # A caller-supplied `landing_url` looked equivalent but isn't: it's
        # captured right after the caller's own JS-triggered navigation
        # (window.location.href = mso_login_url), and Playwright's
        # wait_for_load_state('domcontentloaded') there can return instantly
        # on the PREVIOUS page's already-satisfied load state before the new
        # navigation has even started — confirmed live 2026-08-28 via a real
        # retry landing back on the pre-redirect page (e.g. www.amc.com/)
        # instead of the actual login form URL. Self-capturing here sidesteps
        # that race entirely.
        confirmed_login_url = _safe_page_url(page)
        _fill_and_submit_username()

        if not _wait_for_any(_XFINITY_PASSWORD_SELECTOR, time.monotonic() + wait_seconds):
            # Confirmed live 2026-08-14 that a bare page.reload() here is NOT
            # safe: it reloads whatever URL the page is CURRENTLY on, and a
            # real HAR capture (2026-08-28, dev/amc/amc.har) shows the browser
            # is already sitting on the bare https://login.xfinity.com/login
            # (no query string) by the time the username POST lands — the
            # client_id/acr_values/reqId context only ever existed in the
            # query string of the FIRST GET. A plain reload() replays that
            # bare, context-less URL and lands on a generic unbranded login
            # page, which is why that fix made things worse and got reverted.
            #
            # `confirmed_login_url` (captured above, the moment the username
            # field was confirmed visible — still carrying the full query
            # string) is a different, untried target: re-navigate there and
            # retry the whole username+password sequence once. A caller-
            # supplied landing_url was tried here first and found unreliable
            # (see confirmed_login_url's own comment above) — don't reuse that
            # approach. Whether reqId/the Akamai c_ds_* challenge params in
            # this URL tolerate a second GET is unconfirmed — if they're
            # single-use nonces this recovery attempt will itself just fail
            # cleanly and fall through to the caller's normal poll-and-give-up
            # path, same as today.
            current_url = _safe_page_url(page)
            logger.info('[mvpd-login] xfinity autofill: no visible password field after username submit url=%s', current_url)
            if not confirmed_login_url or confirmed_login_url == current_url:
                return False
            logger.info('[mvpd-login] xfinity autofill: retrying once via confirmed_login_url=%s', confirmed_login_url)
            try:
                page.goto(confirmed_login_url, wait_until='domcontentloaded', timeout=30000)
            except Exception as exc:  # noqa: BLE001
                logger.info('[mvpd-login] xfinity autofill: confirmed_login_url retry goto failed: %s', exc)
                return False
            if not _wait_for_any(_XFINITY_USER_SELECTOR, time.monotonic() + wait_seconds):
                logger.info('[mvpd-login] xfinity autofill: no visible username field after confirmed_login_url retry url=%s', _safe_page_url(page))
                return False
            _fill_and_submit_username(tag=' (confirmed_login_url retry)')
            if not _wait_for_any(_XFINITY_PASSWORD_SELECTOR, time.monotonic() + wait_seconds):
                logger.info('[mvpd-login] xfinity autofill: still no visible password field after confirmed_login_url retry url=%s', _safe_page_url(page))
                return False

        pw_field = page.locator(_XFINITY_PASSWORD_SELECTOR).first
        pw_field.click(force=True, timeout=8000)
        pw_field.fill(password)

        submit = page.locator(_XFINITY_SUBMIT_SELECTOR).first
        try:
            submit.click(force=True, timeout=5000)
        except Exception:  # noqa: BLE001
            page.keyboard.press('Enter')
        logger.info('[mvpd-login] xfinity autofill: filled and submitted password for %s', username)
    except _XfinityAutofillCancelled:
        return False
    except Exception as exc:  # noqa: BLE001
        logger.info('[mvpd-login] xfinity autofill: exception mid-fill: %s', exc)
        return False

    for _ in range(8):
        page.wait_for_timeout(1500)
        url = _safe_page_url(page) or ''
        if 'login.xfinity.com' not in url and 'idm.xfinity.com' not in url:
            break
        for sel in ("text=/ask me later/i", "button:has-text('Skip')"):
            loc = page.locator(sel).first
            try:
                loc.wait_for(state='visible', timeout=500)
                loc.click(force=True, timeout=3000)
                logger.info('[mvpd-login] xfinity autofill: dismissed post-login nag via %s', sel)
            except Exception:  # noqa: BLE001
                continue
    return True


def _apply_sling_browser_login_input(page, cmd: dict) -> None:
    kind = cmd.get('type')
    if kind == 'click':
        page.mouse.click(float(cmd['x']), float(cmd['y']))
    elif kind == 'mousemove':
        page.mouse.move(float(cmd['x']), float(cmd['y']))
    elif kind == 'mousedown':
        # move-then-down (not click()) so a drag can start here: subsequent
        # mousemove commands are dispatched with the button already held,
        # which the page sees as a drag rather than independent hovers.
        page.mouse.move(float(cmd['x']), float(cmd['y']))
        page.mouse.down()
    elif kind == 'mouseup':
        page.mouse.move(float(cmd['x']), float(cmd['y']))
        page.mouse.up()
    elif kind == 'key':
        page.keyboard.press(str(cmd['key']))


def _harvest_and_save_xfinity_cookies(context) -> None:
    """Grabs the real, JS-matured Akamai Bot Manager + Xfinity SESSION
    cookies out of a Camoufox context right after a successful Comcast_SSO
    pairing, and persists them so authorize_mvpd() can do every SUBSEQUENT
    Comcast_SSO sign-in (any network, not just this one — the cookies are
    account/browser-session-scoped, not per-client_id) over plain HTTP
    without a browser at all. Confirmed live 2026-08-14: login.xfinity.com's
    credential POST is blocked by Akamai Bot Manager for any bare HTTP
    client's own freshly-issued cookies — only cookies matured by a real
    browser session pass. See app/tve/adobe_pass.py's
    authenticate_with_xfinity_cookies() and
    dev/comcast/XFINITY_ADOBE_PASS_DIRECT_HTTP_RESEARCH.md. Best-effort,
    never raises — a failed harvest just means the next Comcast_SSO login
    falls back to needing another browser-assisted pairing, same as today.

    Pushes its own app_context rather than assuming the caller already has
    one active — see _prime_google_session's docstring for why callers now
    deliberately pop theirs before running Camoufox. Flask app contexts nest
    safely, so this is harmless on the (still-common) path where a caller's
    own context IS already active.
    """
    try:
        raw_cookies = context.cookies()
    except Exception as exc:  # noqa: BLE001
        logger.info('[mvpd-login] xfinity cookie harvest failed: %s', exc)
        return
    jar = {}
    for c in raw_cookies:
        domain = (c.get('domain') or '').lstrip('.')
        if 'xfinity.com' not in domain:
            continue
        jar[c['name']] = {'value': c['value'], 'domain': domain, 'path': c.get('path') or '/'}
    if not jar:
        logger.info('[mvpd-login] xfinity cookie harvest found no xfinity.com cookies')
        return
    with flask_app.app_context():
        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if not account:
            return
        save_xfinity_cookie_jar(account, jar)
    logger.info('[mvpd-login] harvested and saved %d xfinity.com cookies for future pure-HTTP sign-ins', len(jar))


def _record_tve_login_error(key: str, message: str) -> None:
    """Records the last failed sign-in attempt for one TVE network/requestor,
    keyed the same way app/tve/status.py's tve_network_status() keys its
    per-network entries (requestor_id for the legacy family, 'nbc'/'fox'/
    'amcn'/'discovery'/'foxone' for the rest). A network that has never
    signed in successfully otherwise just shows "Never" on the admin
    settings page with no indication why — e.g. bad entitlement for that
    specific network on an otherwise-working Cox account (confirmed live
    2026-08-11: FYI came back "not entitled" while its A+E siblings
    succeeded). No need to explicitly clear this on a later success —
    tve_network_status() only surfaces an error note when it's newer than
    the last successful sign-in, so a subsequent success naturally
    supersedes it.
    """
    with flask_app.app_context():
        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if not account:
            return
        cfg = dict(account.config or {})
        errors = dict(cfg.get('tve_last_error') or {})
        errors[key] = {'message': str(message)[:300], 'at': int(time.time())}
        cfg['tve_last_error'] = errors
        account.config = cfg
        db.session.commit()


def _cox_login_error_detail(exc: Exception, label: str) -> str:
    """Classifies an exception from a Cox-branch scripted sign-in attempt
    into a short, human-readable detail (never includes the network's own
    label/name — callers prepend that themselves only where it's not
    already implied by context, e.g. the admin modal's status line but not
    the "last attempt failed" note, which sits directly under that
    network's own row label already).

    Shared by run_mvpd_browser_login's and run_fox_browser_login's Cox
    branches, which had this exact three-way classification hand-copied
    and already drifted slightly inconsistent between them (code review,
    2026-08-11). run_nbc_browser_login deliberately does NOT use this —
    NbcTveScraper._ensure_entitled() raises exceptions that already have
    their own full context baked in one layer down (e.g. "NBC TVE:
    <mso_id> is not authorized: <reason>"), so wrapping them again here
    would double up the framing instead of clarifying it. Legacy's
    AdobePassCoxClient and FOX's _fox_sports_mvpd_token both raise bare,
    terse exceptions that actually need this context added.
    """
    if isinstance(exc, TVENotAuthorizedError):
        return f'not entitled — {exc}'
    if isinstance(exc, TVEAuthError):
        return str(exc)
    logger.exception('[mvpd-login] unexpected failure for %s', label)
    return str(exc)


def _autofill_google_account_chooser(page) -> bool:
    """Click through Google's "Choose an account" step automatically instead
    of waiting on a human.

    Confirmed live 2026-08-17: this screen is forced on every single fresh
    Adobe/YouTubeTV authorization request (prompt=select_account in the
    auth URL) regardless of how warm the persistent profile's Google
    session already is — even signing into the SAME network twice in a row,
    minutes apart, showed it again. But it's not actually a real
    credentials/2FA step; it's just a fixed tile for whichever Google
    account this profile is already logged into, exposed via the standard
    `[data-identifier]` attribute Google's account-chooser always renders
    (matched exactly 1 element live; its value was the real signed-in
    email). Clicking that element — not a hardcoded pixel coordinate — is
    what makes this generic across accounts: it reads whichever email the
    profile actually holds rather than assuming any specific user's name.
    Verified this actually advances the flow (landed on
    youtube.auth-gateway.net's linkback, not just "something happened").

    Cheap and safe to call on every poll iteration regardless of which page
    is currently showing — the URL check makes it a no-op everywhere else.

    Confirmed live 2026-08-17 (AMCN's WE tv): the first version of this
    returned immediately after click(), so the caller's very next poll
    iteration (~0.25-1s later, before the page had actually navigated away)
    still saw an `accountchooser` URL and clicked the SAME tile a second
    time — a duplicate submission of Google's one-shot auth step, which
    Google/Adobe's SAML bridge answered with its own "Error: Infinite
    Browser Redirects" loop-breaker page instead of a normal denial. Fixed
    with two layers: wait here for the URL to actually leave the chooser
    before returning (closes the race for the common case), plus a
    window-level marker so even a call that lands mid-transition (the wait
    below timed out, or two calls raced each other) can't click twice for
    the same chooser instance.
    """
    try:
        if 'accountchooser' not in page.url:
            return False
        already = page.evaluate("() => { const v = window.__fcAcctChooserClicked; window.__fcAcctChooserClicked = true; return v; }")
        if already:
            return False
        tile = page.locator('[data-identifier]').first
        if tile.count() == 0:
            return False
        tile.click(timeout=2000)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and 'accountchooser' in page.url:
            page.wait_for_timeout(150)
        return True
    except Exception:  # noqa: BLE001
        return False


_GOOGLE_SETUP_URL = 'https://accounts.google.com/embedded/setup/v2/android?ipt=&ipr=&flowName=EmbeddedSetupAndroid'


def _prime_google_session(context, mso_id: str) -> bool:
    """Best-effort: if a Google master_token is already on file for the mvpd
    TVE account (see app.tve.google_master_token's module docstring —
    captured once from a real interactive login, renewable forever with zero
    browser involvement), mint a fresh set of real Google session cookies
    from it and seed this Camoufox context with them BEFORE it ever
    navigates.

    Confirmed live 2026-08-19: the shared /data/browser_profiles/mvpd_tve
    profile these browser-logins all use has never actually persisted a real
    Google login cookie across runs (only generic NID/OTZ/__Host-GAPS ones),
    so without this every single YouTubeTV-MSO'd authorization fell back to
    the full interactive human-relay flow regardless of how "warm" the
    profile looked. This doesn't replace that fallback — it just gives
    Google's own login page a chance to recognize the session and skip
    straight past it, the same way a real returning user's browser would.
    No-ops (returns False) for any non-YouTubeTV MSO, or when nothing's
    captured yet.

    Does its own TVEAccount lookup rather than taking one from the caller —
    these six browser-login functions don't consistently keep one in scope
    under the same name (run_fox_browser_login in particular only fetches
    one deep inside a nested success branch), and a fresh indexed lookup
    here is cheap enough not to matter.

    Also pushes its own app_context — confirmed live 2026-08-19: running
    Camoufox while a Flask app_context is active on the same thread
    intermittently breaks the browser's OWN rendering (pages loaded real
    content — page.content() had a full Google sign-in form — but never
    painted it; page.screenshot() kept returning a blank frame indefinitely).
    Isolated via a single-variable bisection against a direct reproduction of
    run_nbc_browser_login: identical code inside vs. outside an active
    app_context flipped the bug on and off consistently. The six
    browser-login functions now pop their outer context before launching
    Camoufox, so every DB-touching call made during the browser session
    (this one included) needs to push its own — safe to nest even on the
    (now rare) path where a caller's context is still active.
    """
    if mso_id != 'YouTubeTV':
        return False
    try:
        from app.tve import adobe_pass, google_master_token
        with flask_app.app_context():
            account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
            if account_row is None:
                return False
            saved = adobe_pass.load_google_master_token(account_row)
        if not saved:
            return False
        cookies = google_master_token.mint_browser_cookies(saved)
        if not cookies:
            return False
        context.add_cookies(cookies)
        logger.info('[mvpd-login] primed Camoufox context with a Google session minted from the saved master_token')
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning('[mvpd-login] _prime_google_session failed (falling back to interactive login): %s', exc)
        return False


def _exchange_and_save_google_master_token(oauth_token: str) -> dict | None:
    """Shared tail end of the capture flow — exchange an oauth_token
    (harvested from Google's embedded Android setup page) for a
    master_token and persist it. Used by both _maybe_capture_google_master_
    token's opportunistic piggyback and run_google_signin's dedicated flow.
    Returns the saved {'email', 'master_token', 'android_id', 'captured_at'}
    dict on success, None on any failure (never raises).
    """
    try:
        from app.tve import adobe_pass, google_master_token
        data = google_master_token.exchange_oauth_token(oauth_token)
        if not data:
            return None
        with flask_app.app_context():
            account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
            if account_row is None:
                return None
            adobe_pass.save_google_master_token(account_row, data)
        logger.info('[mvpd-login] captured a Google master_token for %s — future YouTubeTV authorizations can skip interactive login', data.get('email', '?'))
        return data
    except Exception as exc:  # noqa: BLE001
        logger.warning('[mvpd-login] _exchange_and_save_google_master_token failed (non-fatal): %s', exc)
        return None


def _maybe_capture_google_master_token(context, mso_id: str) -> None:
    """Best-effort opportunistic capture of a Google master_token — see
    app.tve.google_master_token's module docstring. Called right after a
    YouTubeTV-MSO'd browser-login has already confirmed a real Google-backed
    Adobe Pass authorization succeeded in `context`, the same pattern
    run_mvpd_browser_login already uses for _harvest_and_save_xfinity_cookies
    on a successful Comcast_SSO login. See _prime_google_session's docstring
    for why this does its own TVEAccount lookup instead of taking one from
    the caller.

    No-ops entirely (no extra navigation at all) once a master_token is
    already on file — it doesn't expire on any schedule the way authn/authz
    tokens do, so there's nothing to refresh here; only revocation (password
    change, security event) invalidates it, at which point _prime_google_
    session's mint just fails and the ordinary interactive fallback re-runs
    this capture naturally.

    Pushes a SHORT-lived app_context for each of the two DB touches (the
    initial check, the final save) rather than holding one open for the
    whole call — see _prime_google_session's docstring: the ~25s of browser
    interaction in between must run with no app_context active at all, the
    same reason the outer browser-login functions pop theirs before
    Camoufox. Re-queries account_row fresh for the save rather than reusing
    the one from the initial check — it would otherwise be a detached
    SQLAlchemy instance from an already-popped context's session, which
    db.session.commit() can't correctly persist.
    """
    if mso_id != 'YouTubeTV':
        return
    try:
        from app.tve import adobe_pass
        with flask_app.app_context():
            account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
            if account_row is None:
                return
            if adobe_pass.load_google_master_token(account_row):
                return
        capture_page = context.new_page()
        try:
            capture_page.goto(_GOOGLE_SETUP_URL, wait_until='domcontentloaded', timeout=15000)
            # 10s wasn't enough live (2026-08-19): the embedded setup page can
            # take a while to resolve right after a real interactive login
            # (its own extra verification step, slow redirect chain, etc.).
            # 25s costs nothing extra on the success path (the loop breaks
            # the instant the cookie shows up) and this whole thing is
            # already best-effort/non-blocking to the real pairing result.
            deadline = time.monotonic() + 25
            oauth_token = None
            while time.monotonic() < deadline:
                _autofill_google_account_chooser(capture_page)
                cookies = context.cookies()
                oauth_token = next((c['value'] for c in cookies if c['name'] == 'oauth_token'), None)
                if oauth_token:
                    break
                capture_page.wait_for_timeout(500)
            if not oauth_token:
                logger.info(
                    '[mvpd-login] master_token capture: no oauth_token cookie appeared after 25s, skipping (final url: %s)',
                    _safe_page_url(capture_page),
                )
                return
            _exchange_and_save_google_master_token(oauth_token)
        finally:
            capture_page.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning('[mvpd-login] _maybe_capture_google_master_token failed (non-fatal): %s', exc)


def _relay_input_and_screenshot(
    page, r, waiting_since: float | None = None,
    stop_key: str | None = None, input_key: str | None = None,
    shot_key: str | None = None, hint_key: str | None = None,
) -> bool:
    """Keep the streamed browser modal alive and interactive during a silent
    wait phase (e.g. _try_autofill_credentials's wait for a password field
    to render), and report whether the human clicked Stop/Cancel.

    Without the relay half, human input queued via the modal is never
    drained and no new screenshots are ever taken once the primary loop
    hands off to this wait — so the modal freezes on whatever page was
    showing the instant the wait started, while the real browser has
    already moved on. A human watching that frozen frame and clicking on it
    looks exactly like "stuck, can't click" (confirmed live 2026-08-05) even
    though their clicks WERE being queued server-side the whole time, just
    never applied.

    Without the stop-check half, clicking Stop/Cancel during this wait does
    nothing at all — only the primary loop ever checked that key, so a
    human canceling mid-wait would see repeated Stop/Start clicks fail
    silently while the job kept polling on its own budget (confirmed live
    2026-08-05). Returns True if the human asked to stop — callers should
    break out of their own loop when this happens.

    waiting_since, if given, is the monotonic time this particular silent
    wait (autofill detection, or the post-submit poll-for-completion loop)
    started. Once it's run longer than _MVPD_STUCK_HINT_SECONDS, a short-TTL
    hint is published alongside the screenshot suggesting the human glance at
    it — landing on a generic "Sign In" gate page and sitting there is USUALLY
    fine (the real auth resolves via background API polling, independent of
    what's on screen — confirmed live 2026-08-10 against AMC Networks TVE's
    BBC America/IFC/WE tv gate pages), but occasionally a page genuinely does
    need a manual click and there's no reliable way to tell those apart
    server-side. The hint has a 5s TTL and this function is called ~1/s while
    waiting, so it stays lit for the duration of a genuinely long wait and
    disappears within 5s of the wait actually ending — no explicit clear call
    needed.

    Best-effort; never raises. Call this every ~1s from within a silent
    wait's own poll loop, same cadence as the primary loop.

    stop_key/input_key/shot_key/hint_key default to the shared legacy
    'mvpd:browser-login:*' keys — correct for legacy/AMC/Discovery (which
    genuinely share one modal/redis-namespace by design). NBC's and FOX's
    own STANDALONE primary loops have their
    own separate 'nbc-mvpd:*'/'fox-mvpd:*' namespace and must pass their own
    keys explicitly — passing the defaults there would write screenshots
    nobody's modal ever reads (confirmed live via code review, 2026-08-10:
    the NBC/FOX autofill-wait screenshot fix silently didn't work because of
    exactly this default). Params are keyword-only in spirit; resolved
    inside the function body (not as literal defaults) since NBC_/FOX_
    BROWSER_LOGIN_*_KEY aren't defined yet at this point in the file.
    """
    import json as _json
    stop_key = stop_key or MVPD_BROWSER_LOGIN_STOP_KEY
    input_key = input_key or MVPD_BROWSER_LOGIN_INPUT_KEY
    shot_key = shot_key or MVPD_BROWSER_LOGIN_SHOT_KEY
    hint_key = hint_key or MVPD_BROWSER_LOGIN_HINT_KEY
    _autofill_google_account_chooser(page)
    stopped = False
    try:
        stopped = bool(r.exists(stop_key))
    except Exception:  # noqa: BLE001
        pass
    for _ in range(20):
        try:
            raw = r.lpop(input_key)
        except Exception:  # noqa: BLE001
            break
        if raw is None:
            break
        try:
            _apply_sling_browser_login_input(page, _json.loads(raw))
        except Exception:  # noqa: BLE001
            pass
    try:
        shot = page.screenshot(type='jpeg', quality=60)
        r.setex(shot_key, 30, shot)
    except Exception:  # noqa: BLE001
        pass
    if waiting_since is not None and time.monotonic() - waiting_since > _MVPD_STUCK_HINT_SECONDS:
        try:
            r.setex(
                hint_key, 5,
                "Taking a while — if the screen below shows a Sign In or Continue button, "
                "clicking it can help. It may also just be working in the background.",
            )
        except Exception:  # noqa: BLE001
            pass
    return stopped


_F5_REJECTION_MARKER = 'The requested URL was rejected'


def _sling_f5_recover(
    page, login_url: str, username: str, password: str, r=None,
    stop_key: str | None = None, input_key: str | None = None,
    shot_key: str | None = None, hint_key: str | None = None,
) -> bool:
    """Detect Sling's F5 bot-defense block page ("The requested URL was
    rejected. Please consult with your administrator.") that replaces the
    login form in-place when a submitted POST gets flagged. Observed live
    2026-08-05: AETV's autofilled submit was rejected while LIFETIME/FYI's
    identical submits under a minute later passed — the block is transient
    per-attempt scoring, so one backoff + reload + refill retry is usually
    enough. Returns True if the block page was detected and a retry was
    attempted (caller should extend its poll window), False otherwise."""
    try:
        if page.get_by_text(_F5_REJECTION_MARKER).count() == 0:
            return False
    except Exception:  # noqa: BLE001
        return False
    logger.info('[mvpd-login] Sling bot-defense rejected the submitted login — backing off 8s and retrying once')
    try:
        page.wait_for_timeout(8000)
        page.goto(login_url, wait_until='domcontentloaded', timeout=30000)
        if username and password:
            _try_autofill_credentials(
                page, username, password, r=r,
                stop_key=stop_key, input_key=input_key, shot_key=shot_key, hint_key=hint_key,
            )
    except Exception as exc:  # noqa: BLE001
        logger.info('[mvpd-login] F5-recovery reload failed: %s', exc)
    return True


_BROWSER_LOGIN_MAX_ATTEMPTS = 3


class _BrowserSessionDied(Exception):
    """The Camoufox page/browser died mid-login (page CLOSE fired, target
    crashed, or the page stopped answering screenshots). Observed
    intermittently on first attempts across NBC/FOX/legacy flows (2026-08-05,
    cause unknown — a plain retry has worked every time), so the login jobs
    treat it as retryable and relaunch the browser instead of failing out to
    the user."""


def _is_browser_death(exc: BaseException) -> bool:
    """True if exc means the browser/page itself died (retryable), as opposed
    to a real auth/protocol failure."""
    if isinstance(exc, _BrowserSessionDied):
        return True
    try:
        from playwright.sync_api import Error as _PlaywrightError
    except ImportError:
        return False
    if not isinstance(exc, _PlaywrightError):
        return False
    msg = str(exc)
    return 'has been closed' in msg or 'Target crashed' in msg or 'Connection closed' in msg

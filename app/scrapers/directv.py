"""
directv.py — DirecTV Stream, resolved natively (no PrismCast dependency).

DirecTV Stream channels are Widevine-DRM-protected, but — unlike the earlier
assumption behind this file — the manifest and license flow are both plain
HTTP APIs on api.cld.dtvce.com, reachable directly from the server. This
mirrors the existing Roku DRM bridge (app/scrapers/roku.py's resolve_dash())
almost exactly, reusing the same generic infrastructure:
  - app/routes/play.py's license_proxy() route (works for any scraper that
    implements get_license_url/prepare_license_request/process_license_response)
  - app/routes/api.py's _get_playback_info() (auto-wires the license URL and
    picks the right Shaka playback mode)
  - app/static/js/fc_player.js (already generically handles "hls mode + a
    license URL" — no DASH-specific code required)
  - app/worker.py's audit-driven requires_drm_bridge flagging + app/generators/
    m3u.py's generate_prismcast_m3u() (for viewers on non-DRM-capable clients)
No new routes or player-side code were needed — see the plan notes in
/home/brad/.claude/plans/eager-swinging-crab.md for the full investigation.

Three API calls, all on api.cld.dtvce.com (confirmed via live testing + two
real HAR captures of a successful Chrome playback session):

  1. Channel resolve (per channel, cached ~55min):
     GET /right/authorization/channel/v1?ccid=<ccid>&clientContext=<...>
         &proximity=O&timeShiftEnabled=true&daiEnabled=true
         &reserveCTicket=true&dualManifest=false&abrEnabled=true
     -> {"playbackData": {"fallbackStreamUrl": "<direct Fastly CDN, CORS-open,
         no ad-stitching>"}, "dRights": {"playToken": "..."}}
     The required query params beyond ccid/clientContext were the actual
     reason this endpoint 403'd in earlier attempts — it isn't a dead
     endpoint, it just needs the full parameter set.

  2. Widevine license (per key rotation):
     POST /rights/management/mdrm/vgemultidrm/v1/widevine/license
     Body: {"contentID": ccid, "contentType": "2", "identityCookie": ...,
            "authorizationToken": <playToken from #1>,
            "licenseChallenge": base64(raw EME challenge)}
     Response: {"licenseData": [base64(raw Widevine license)]}
     Same JSON-wrap-base64-challenge / JSON-unwrap-base64-response shape as
     the existing Amazon license proxy (amazon_prime_free.py) — just
     different field names.

  3. identityCookie (session-level, long-lived — has its own expiry) comes
     from POST /rights/management/mdrm/vgemultidrm/v1/widevine/activate,
     which needs the playback client's Widevine/Shaka activation challenge.
     It is minted lazily by license_proxy() and cached per playback device.

Channel/EPG metadata (name, number, logo, schedule) uses a separate,
lightweight path: plain `requests` calls with the bearer token captured by the
curl-cffi ForgeRock/PKCE auth flow below. Bare `requests` is not enough for
sign-in, but curl-cffi with Chrome impersonation gets the tokens and the
captured bearer works over plain HTTP for metadata, manifests, and license
calls.

source_channel_id is the ccid (not DirecTV's resourceId UUID) — the
channel-resolve and license calls both key off ccid, so it has to be the
primary FastChannels channel ID for this source. resourceId is only needed
for the schedule/EPG API, so it's embedded in the opaque stream_url instead:
`directv://<ccid>/<url-encoded resourceId>`.
"""
from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import logging
import os
import random
import re
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib.parse import quote, unquote, urlparse, parse_qs

import requests
try:
    from curl_cffi import requests as _cffi_requests
    _CFFI_IMPERSONATE = 'chrome'
except ImportError:
    _cffi_requests = None
    _CFFI_IMPERSONATE = None

from .base import (
    BaseScraper, ChannelData, ConfigField, ProgramData, ScrapeSkipError,
    infer_language_from_metadata,
)
from .category_utils import category_for_channel, infer_category_from_name
from ..gracenote_map import resolve_gracenote

logger = logging.getLogger(__name__)


class DirectvAuthExpiredError(RuntimeError):
    """Raised when DirecTV accepts the request but reports an expired token."""

_ALLCHANNELS_URL = "https://api.cld.dtvce.com/discovery/metadata/channel/v5/service/allchannels"
_SCHEDULE_URL = "https://api.cld.dtvce.com/discovery/edge/schedule/v1/service/schedule"
_CHANNEL_AUTH_URL = "https://api.cld.dtvce.com/right/authorization/channel/v1"
_ACTIVATE_URL = "https://api.cld.dtvce.com/rights/management/mdrm/vgemultidrm/v1/widevine/activate"
_LICENSE_URL = "https://api.cld.dtvce.com/rights/management/mdrm/vgemultidrm/v1/widevine/license"
_IDENTITY_AUTH_URL = "https://identity.directv.com/am/IdPwdAuth"
_IDENTITY_AUTHORIZE_URL = "https://identity.directv.com/authorize"
_AUTHN_TOKEN_URL = "https://api.cld.dtvce.com/authn-tokengo/v3/tokens"
_LOGIN_REDIRECT_URL = "https://api.cld.dtvce.com/authn-tokengo/v3/loginRedirect"

_FORGEROCK_CLIENT_ID = "fr_web_02"
_WEB_CLIENT_ID = "UNIFIED_DTV_WEB"
_AUTH_RETURN_URL = "https://stream.directv.com/auth-return"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
)

# Refresh well before DirecTV's own session naturally expires. Observed
# channel/v1 playback auth reject as early as ~11h40m, and scrape_interval
# is also 12h, so a 12h TTL left no real margin for the proactive check to
# beat a real expiry — dropped to 10h to give it headroom.
_TOKEN_TTL = 10 * 3600
_REAUTH_LOCK_TTL = 20 * 60  # avoid spawning overlapping background logins

# Live-captured channel/v1 responses reported duration ~3874s — cache with
# margin so resolve() refreshes a bit before DirecTV's own entitlement would
# expire, mirroring roku.py's _DASH_TTL pattern.
_PLAYBACK_CACHE_TTL = 3300

_SCHEDULE_DAYS = 2
_SCHEDULE_WINDOW_HOURS = 6
# Ask the schedule endpoint to hydrate content.images. Without this selector,
# the same schedule rows include IDs and metadata but no artwork.
_SCHEDULE_FIS_PROPERTIES = (
    "ISF:2.0"
    "#poster,640,360"
    "#series-poster,640,360"
    "#bg-fplayer,1024,576"
    "#iconic,64,36"
)
# The schedule API silently drops every channel but the first when channelIds
# is sent the normal `requests` way (repeated query keys) — it must be ONE
# comma-joined value (see fetch_epg). Comma-joined also has a hard batch cap:
# empirically confirmed 16 succeeds, 17+ returns 400 "invalid input
# parameter(s): channelIds". Kept one under that confirmed boundary.
_SCHEDULE_BATCH = 15

# Gracenote station IDs moved to the community gracenote_map CSV under
# provider 'directv'. DirecTV's channel payload doesn't expose station IDs,
# so most rows there are keyed by name:<_directv_gracenote_key(name)> rather
# than ccid — see resolve_gracenote() call in fetch_channels() below.

def _pick(d: dict, *keys: str) -> str:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, (int, float)):
            return str(v)
    return ""



def _directv_gracenote_key(value: str | None) -> str:
    return ''.join(ch for ch in (value or '').lower() if ch.isalnum())


def _progress(scraper: Any, phase: str, done: int = 0, total: int = 0) -> None:
    cb = getattr(scraper, '_progress_cb', None)
    if cb:
        cb(phase, done, total)

def _looks_like_channel_obj(d: dict) -> bool:
    keys = {k.lower() for k in d.keys()}
    return bool(keys & {'ccid', 'callsign', 'channelnumber', 'resourceid', 'logourl'})


def _find_channel_list(root: Any, max_depth: int = 6) -> list[dict]:
    """DirecTV's AllChannels payload shape is only known from a third-party
    capture, not documentation — walk the JSON defensively for the largest
    list of channel-shaped dicts rather than assuming one exact path, so a
    minor response-shape change doesn't silently return zero channels."""
    best: list[dict] = []

    def walk(node: Any, depth: int) -> None:
        nonlocal best
        if depth > max_depth:
            return
        if isinstance(node, list):
            if node and all(isinstance(x, dict) for x in node):
                score = sum(1 for x in node if _looks_like_channel_obj(x))
                if score > 0 and (score, len(node)) > (
                    sum(1 for x in best if _looks_like_channel_obj(x)), len(best)
                ):
                    best = node
            for x in node:
                walk(x, depth + 1)
        elif isinstance(node, dict):
            for v in node.values():
                walk(v, depth + 1)

    walk(root, 0)
    return best


def _parse_iso(s: str) -> datetime | None:
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except ValueError:
        return None


def _license_content_id_from_stream_url(stream_url: str, fallback: str) -> str:
    match = re.search(r'/channel\([^)]*-(\d+)(?:\.|[)])', stream_url or '')
    if match:
        return match.group(1)
    return fallback


def _fetch_channel_playback(
    bearer_token: str, cookies: list[dict], client_context: str | None, ccid: str,
) -> dict | None:
    """GET /right/authorization/channel/v1 for one channel — the manifest +
    playToken resolve. Used both by resolve() (via the scraper's live
    session) and prepare_license_request() (a classmethod with no live
    session — builds its own throwaway one)."""
    session = requests.Session()
    session.headers.update({
        'Accept': '*/*',
        'Origin': 'https://stream.directv.com',
        'Referer': 'https://stream.directv.com/',
        'Authorization': f'Bearer {bearer_token}',
        'User-Agent': _UA,
    })
    for c in cookies or []:
        try:
            session.cookies.set(
                c['name'], c['value'],
                domain=c.get('domain') or None, path=c.get('path') or '/',
            )
        except Exception:
            continue

    params = {
        'ccid': ccid,
        'proximity': 'O',
        'timeShiftEnabled': 'true',
        'daiEnabled': 'true',
        'reserveCTicket': 'true',
        'dualManifest': 'false',
        'abrEnabled': 'true',
    }
    if client_context:
        params['clientContext'] = client_context

    try:
        r = session.get(_CHANNEL_AUTH_URL, params=params, timeout=15)
    except requests.RequestException as exc:
        logger.warning('[directv] channel/v1 request failed for ccid=%s: %s', ccid, exc)
        return None
    if r.status_code != 200:
        logger.warning('[directv] channel/v1 HTTP %s for ccid=%s', r.status_code, ccid)
        return None
    try:
        data = r.json()
    except ValueError:
        return None
    if not data.get('authorized'):
        status = data.get('responseStatus') or {}
        error_code = str(status.get('errorCode') or '').strip()
        error_text = str(status.get('errorText') or status.get('message') or '').strip()
        logger.warning(
            '[directv] channel/v1 not authorized for ccid=%s errorCode=%s errorText=%s',
            ccid, error_code or '?', error_text or '?',
        )
        if error_code == '0015' or 'access token has expired' in error_text.lower():
            raise DirectvAuthExpiredError(error_text or 'access token has expired')
        return None

    pb = data.get('playbackData') or {}
    fallback_url = pb.get('fallbackStreamUrl') or pb.get('streamURL')
    play_token = (data.get('dRights') or {}).get('playToken')
    if not fallback_url or not play_token:
        return None
    return {
        'fallback_url': fallback_url,
        'play_token': play_token,
        'license_content_id': _license_content_id_from_stream_url(fallback_url, ccid),
        'cached_at': time.time(),
    }


# ── Playwright auth capture ────────────────────────────────────────────────

_STATUS_TTL = 600  # 10 min
_RESULT_TTL = 600  # 10 min for the caller to consume

_STEALTH_ARGS = [
    '--disable-blink-features=AutomationControlled',
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
    # Confirmed via deobfuscating a sibling project's DirecTV-specific stealth
    # launcher (dev/dvrtuner/chrome-matched-stealth.js — decoded with a
    # sandboxed vm harness, not guessed) — its full launch-arg list.
    '--disable-infobars',
    '--no-first-run',
    '--no-default-browser-check',
    '--disable-backgrounding-occluded-windows',
    '--disable-renderer-backgrounding',
]
# Chrome adds these two flags itself for any CDP-driven session; the sibling
# project explicitly drops them via ignoreDefaultArgs, removing a signal
# sensor JS can read straight off the command line.
_IGNORE_DEFAULT_ARGS = ['--enable-automation', '--enable-blink-features=IdleDetection']

_STEALTH_SCRIPT = """
(function () {
    // Hide the overrides below from Function.prototype.toString introspection
    // — some bot-detection scripts toString() a property's getter to check
    // whether it looks like native code vs. a JS-defined override. Applied
    // via context.add_init_script(), so (unlike the sibling project's
    // one-shot page.waitForFunction() version) this also covers iframes —
    // DirecTV's login form can render inside one (see _find_frame_with_selector).
    const _nativeToString = Function.prototype.toString;
    const _patchedFns = new Set();
    Function.prototype.toString = function () {
        if (_patchedFns.has(this)) return 'function () { [native code] }';
        return _nativeToString.call(this);
    };
    _patchedFns.add(Function.prototype.toString);
    function definePatched(obj, prop, getter) {
        Object.defineProperty(obj, prop, {get: getter, configurable: true});
        const desc = Object.getOwnPropertyDescriptor(obj, prop);
        if (desc && desc.get) _patchedFns.add(desc.get);
    }

    definePatched(navigator, 'webdriver', () => false);

    // Remove CDP automation globals stock Playwright's driver leaves behind
    // (patchright strips these natively; plain playwright does not).
    const cdcKeys = Object.keys(window).filter(k => k.startsWith('cdc_') || k.startsWith('$cdc'));
    cdcKeys.forEach(key => { try { delete window[key]; } catch (e) {} });

    if (!window.chrome) window.chrome = {};
    window.chrome.runtime = {
        connect: function () {},
        sendMessage: function () {},
        onMessage: {addListener: function () {}},
        id: undefined,
    };
    window.chrome.loadTimes = function () {};
    window.chrome.csi = function () {};

    try {
        if (!navigator.plugins || !navigator.plugins.length) {
            definePatched(navigator, 'plugins', () => {
                const a = [1, 2, 3, 4, 5];
                a.__proto__ = navigator.plugins.__proto__;
                return a;
            });
        }
    } catch (e) {}
    try { definePatched(navigator, 'languages', () => ['en-US', 'en']); } catch (e) {}

    try {
        const originalQuery = navigator.permissions && navigator.permissions.query;
        if (originalQuery) {
            navigator.permissions.query = (params) => {
                if (params && params.name === 'notifications') {
                    return Promise.resolve({state: 'default', onchange: null});
                }
                return originalQuery.call(navigator.permissions, params);
            };
            _patchedFns.add(navigator.permissions.query);
        }
    } catch (e) {}

    try {
        if (typeof Notification !== 'undefined') {
            definePatched(Notification, 'permission', () => 'default');
        }
    } catch (e) {}

    try {
        const style = document.createElement('style');
        style.textContent = '.automation-indicator { display: none !important; }';
        document.head && document.head.appendChild(style);
    } catch (e) {}
})();
"""
# NOT ported from the sibling project, deliberately: it also overrides
# navigator.platform/hardwareConcurrency/deviceMemory and the WebGL
# vendor/renderer strings to match one specific real Mac (its whole browser
# runs on that Mac). This container's real Chrome runs on Linux — spoofing a
# Mac platform/GPU in JS while Chrome's own Client-Hints headers
# (Sec-CH-UA-Platform) and actual TLS/HTTP2 fingerprint still say Linux would
# be a mismatch a sophisticated check could flag, which is worse than no
# override (same reasoning as the "no user_agent override" comment below).
# If this test run doesn't clear Akamai, that's the next thing to try —
# either match Linux's own real values, or accept the OS-mismatch risk.

_GUIDE_URL = "https://stream.directv.com/guide"
# Persistent so DirecTV/Akamai's device-trust cookies accumulate across runs
# instead of every login looking like a brand-new, never-before-seen browser.
_DIRECTV_PROFILE_DIR = "/data/browser_profiles/directv"
_ALLCHANNELS_MARKER = "/discovery/metadata/channel/v5/service/allchannels"
_ACTIVATE_MARKER = "/rights/management/mdrm/vgemultidrm/v1/widevine/activate"
_EMAIL_SELECTOR = (
    'input[type="email"], input[name="email"], input[name="username"], '
    'input[id="userId"], input[autocomplete="username"]'
)
_PASSWORD_SELECTOR = 'input[type="password"]'
# The guide's logo column is not virtualized — every channel logo carries
# aria-label="view {channelName}" (same technique PrismCast's own directv.ts
# tuner uses). Any channel works here; clicking one is just what triggers the
# browser's Widevine CDM to fire its activation exchange.
_CHANNEL_LOGO_SELECTOR = '[aria-label^="view " i]'

# Generous — Akamai challenges + the SPA login round-trip can be slow, and we'd
# rather wait than falsely report a failed login.
_LOGIN_TIMEOUT = 90.0
# DRM activation is best-effort (see capture_directv_auth) — shorter budget
# since a bearer-token-only capture is still useful without it.
_ACTIVATE_TIMEOUT = 20.0


@contextlib.contextmanager
def _virtual_display():
    """Spawn a throwaway Xvfb display so Chrome can launch non-headless.

    DirecTV's Akamai bot protection resets the connection outright for
    headless Chrome — confirmed true even for the real Chrome binary (not
    just open-source Chromium), Aug 2026 live testing. Headed Chrome under a
    virtual display clears it, so headed is not optional here.
    """
    proc: subprocess.Popen | None = None
    display_num: int | None = None
    for _ in range(5):
        candidate = random.randint(90, 989)
        if os.path.exists(f'/tmp/.X{candidate}-lock'):
            continue
        candidate_proc = subprocess.Popen(
            ['Xvfb', f':{candidate}', '-screen', '0', '1920x1080x24', '-nolisten', 'tcp'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(1)
        if candidate_proc.poll() is None:
            proc, display_num = candidate_proc, candidate
            break
        candidate_proc.wait()
    if proc is None or display_num is None:
        raise DirectvAuthError('Could not start a virtual X display for headed Chrome')

    old_display = os.environ.get('DISPLAY')
    os.environ['DISPLAY'] = f':{display_num}'
    try:
        yield
    finally:
        if old_display is not None:
            os.environ['DISPLAY'] = old_display
        else:
            os.environ.pop('DISPLAY', None)
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def _find_json_value(node: Any, key: str) -> str:
    if isinstance(node, dict):
        for k, v in node.items():
            if k == key and isinstance(v, str) and v.strip():
                return v.strip()
            found = _find_json_value(v, key)
            if found:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _find_json_value(item, key)
            if found:
                return found
    return ''


def _capture_activation_token_from_storage(page) -> str:
    try:
        rows = page.evaluate("""
        () => {
          const out = [];
          for (const store of [window.localStorage, window.sessionStorage]) {
            for (let i = 0; i < store.length; i++) {
              const key = store.key(i);
              out.push([key, store.getItem(key)]);
            }
          }
          return out;
        }
        """)
    except Exception:
        return ''
    for key, value in rows or []:
        if key == 'activationToken' and value:
            return str(value).strip()
        text = str(value or '')
        if 'activationToken' not in text:
            continue
        try:
            found = _find_json_value(json.loads(text), 'activationToken')
            if found:
                return found
        except Exception:
            continue
    return ''


class DirectvAuthError(Exception):
    """Raised by capture_directv_auth() on any unrecoverable login failure."""


# ── Redis helpers (manual/admin-UI path only) ──────────────────────────────

def _status_key(source_id: int) -> str:
    return f'directv:auth:status:{source_id}'


def _result_key(source_id: int) -> str:
    return f'directv:auth:result:{source_id}'


def _write_status(r, source_id: int, status: str, detail: str | None = None) -> None:
    payload = {'status': status, 'detail': detail, 'updated_ms': int(time.time() * 1000)}
    r.set(_status_key(source_id), json.dumps(payload), ex=_STATUS_TTL)
    logger.debug('[directv-auth] status=%s detail=%s', status, detail)


# ── Page state detection ────────────────────────────────────────────────────

def _is_captcha_or_botcheck(page) -> bool:
    try:
        body = (page.inner_text('body') or '').lower()
    except Exception:
        return False
    return any(m in body for m in (
        'captcha', 'are you a robot', 'verify you are human', 'access denied',
    ))


def _is_akamai_error_page(page) -> bool:
    """DirecTV's own generic app error ("(8003)") — a downstream symptom of
    Akamai 403'ing a request the page depends on (confirmed live, Aug 2026:
    /am/IdPwdAuth returning 403 with no user action taken yet). Checking for
    this explicitly lets a blocked run fail in seconds instead of silently
    waiting out the full frame-search + bearer-token timeouts with a vague
    "login may have failed" error."""
    try:
        body = (page.inner_text('body') or '').lower()
    except Exception:
        return False
    return 'ran into an issue' in body or '(8003)' in body


def _find_frame_with_selector(page, selector: str, timeout_ms: int):
    """Poll every frame on the page for one containing `selector`.

    The login form doesn't always arrive via a top-level redirect to
    identity.directv.com — confirmed live (Aug 2026) that it can instead
    render inside an iframe embedded in stream.directv.com/guide, with the
    top-level URL never changing. Checking page.url alone (the old
    _is_identity_page-gated approach) silently misses that case entirely.
    """
    deadline = time.time() + timeout_ms / 1000
    while time.time() < deadline:
        for f in page.frames:
            try:
                if f.query_selector(selector):
                    return f
            except Exception:
                continue
        page.wait_for_timeout(250)
    return None


# Live testing (Aug 2026) found Akamai's _abck sensor cookie can stay at its
# unvalidated "~-1~" state indefinitely across purely-synthetic .click()/.fill()
# automation (confirmed by decrypting the local cookie store) — none of that
# interaction carries the mouse-movement or per-keystroke timing entropy a
# real user's session naturally produces. These helpers replace instant
# synthetic actions with actual moved-mouse-then-clicked and typed-not-filled
# interaction. Whether this alone flips sensor validation is unconfirmed
# (a successful login was observed with _abck still unvalidated), but it's
# a strict improvement in realism at negligible cost, so it stays on by
# default rather than gated behind a flag.
_mouse_pos = [random.randint(200, 600), random.randint(200, 400)]


def _human_move(page, x: float, y: float) -> None:
    start_x, start_y = _mouse_pos
    waypoints = []
    for _ in range(random.randint(1, 2)):
        wx = start_x + (x - start_x) * random.uniform(0.3, 0.7) + random.randint(-40, 40)
        wy = start_y + (y - start_y) * random.uniform(0.3, 0.7) + random.randint(-40, 40)
        waypoints.append((wx, wy))
    waypoints.append((x, y))
    for wx, wy in waypoints:
        page.mouse.move(wx, wy, steps=random.randint(8, 18))
        page.wait_for_timeout(random.randint(20, 90))
    _mouse_pos[0], _mouse_pos[1] = x, y


def _human_idle_wander(page, moves: int = 3) -> None:
    for _ in range(moves):
        _human_move(page, random.randint(150, 1400), random.randint(150, 800))
        page.wait_for_timeout(random.randint(150, 500))


def _human_click_locator(page, locator) -> bool:
    try:
        box = locator.bounding_box()
    except Exception:
        box = None
    if not box:
        return False
    tx = box['x'] + box['width'] * random.uniform(0.3, 0.7)
    ty = box['y'] + box['height'] * random.uniform(0.3, 0.7)
    _human_move(page, tx, ty)
    page.wait_for_timeout(random.randint(80, 220))
    page.mouse.down()
    page.wait_for_timeout(random.randint(40, 130))
    page.mouse.up()
    return True


def _human_fill(page, frame, selector: str, text: str) -> None:
    loc = frame.locator(selector).first
    _human_click_locator(page, loc)
    page.wait_for_timeout(random.randint(100, 250))
    loc.press_sequentially(text, delay=random.randint(60, 160))


def _click_primary_button(page, frame, candidates=('Next', 'Continue', 'Sign In', 'Log In', 'Submit')) -> bool:
    """Click the primary action button on a DirecTV login step.

    These are plain <button> elements with no id and no type="submit" —
    unusable as a CSS/JS selector target — and the password step renders
    THREE buttons (Back, a password-visibility toggle, Sign In), so blindly
    clicking "the first button" clicks Back. Match on the known label text
    instead, trying each step's known label before ever falling back to "any
    visible button".

    `candidates` should be ordered for the step actually being submitted —
    confirmed live that a stale "Next" button can still exist (and match)
    in the DOM after the SPA has already transitioned to the password step,
    intercepting the click meant for "Sign In". Passing the password step's
    own labels first avoids ever matching that leftover element.

    `page` is the top-level page (mouse coordinates are page-relative even
    when the button lives in an iframe); `frame` is where the button is
    searched for.
    """
    for text in candidates:
        try:
            btn = frame.get_by_role('button', name=text, exact=False)
            if btn.count() > 0 and _human_click_locator(page, btn.first):
                return True
        except Exception:
            continue
    try:
        return _human_click_locator(page, frame.locator('button:visible').first)
    except Exception:
        return False


# ── Core capture ─────────────────────────────────────────────────────────────


def _pkce_verifier() -> str:
    alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~'
    raw = os.urandom(128)
    return ''.join(alphabet[b % len(alphabet)] for b in raw)


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode('ascii').rstrip('=')


def _pkce_challenge(verifier: str) -> str:
    return _b64url(hashlib.sha256(verifier.encode('ascii')).digest())


def _fill_auth_callback(payload: dict, value: str) -> dict:
    data = json.loads(json.dumps(payload))
    for callback in data.get('callbacks') or []:
        if callback.get('type') not in ('NameCallback', 'PasswordCallback', 'ChoiceCallback'):
            continue
        inputs = callback.get('input') or []
        if inputs:
            inputs[0]['value'] = value
            break
    return data


def _json_or_error(response, label: str) -> dict:
    try:
        data = response.json()
    except Exception as exc:
        raise DirectvAuthError(
            f'{label} returned non-JSON HTTP {response.status_code}: {response.text[:160]}'
        ) from exc
    if response.status_code < 200 or response.status_code >= 300:
        msg = data.get('message') or data.get('errorDescription') or data.get('reason') or data
        raise DirectvAuthError(f'{label} failed HTTP {response.status_code}: {msg}')
    return data


def _normalize_activation_token(raw_token: str) -> str:
    token = (raw_token or '').strip()
    if not token:
        return ''
    if len(token) % 2 == 0 and all(c in '0123456789abcdefABCDEF' for c in token):
        try:
            return base64.b64encode(bytes.fromhex(token)).decode('ascii')
        except Exception:
            pass
    return token


def capture_directv_auth_cffi(
    username: str,
    password: str,
    *,
    on_status: Callable[[str, str], None] | None = None,
) -> dict:
    """Sign in to DirecTV with curl-cffi instead of browser automation.

    The web app uses ForgeRock callbacks plus a PKCE auth-code exchange. AuthN
    returns activationToken as hex; the DRM activate endpoint expects the same
    bytes base64-encoded, so normalize it before persisting.

    As of Aug 2026, identity.directv.com started rejecting a POST straight to
    /am/IdPwdAuth with a WAF "Access Denied" 403 (custom `x-dtvtokn: bad`
    header) — confirmed via a real-browser HAR capture that the missing piece
    is the OAUTH_REQUEST_ATTRIBUTES cookie, which only gets set by actually
    walking the same loginRedirect -> weblogin/authorize -> /authorize ->
    weblogin/authenticate chain stream.directv.com's own SPA runs before ever
    calling /am/IdPwdAuth. The PKCE verifier/challenge generated here is the
    same one threaded through that entire chain (loginRedirect's codeChallenge
    param through to the final /authorize call) — matching the real flow,
    where it is not regenerated after login.
    """
    if _cffi_requests is None:
        raise DirectvAuthError('curl_cffi unavailable')

    def _status(state: str, detail: str = '') -> None:
        level = logger.info if state != 'running' else logger.debug
        level('[directv-auth] %s %s', state, detail)
        if on_status:
            try:
                on_status(state, detail)
            except Exception:
                pass

    _status('running', 'Signing in to DirecTV…')
    session = _cffi_requests.Session(impersonate=_CFFI_IMPERSONATE)
    session.headers.update({
        'User-Agent': _UA,
        'Accept-Language': 'en-US,en;q=0.9',
    })

    verifier = _pkce_verifier()
    challenge = _pkce_challenge(verifier)
    login_session_id = uuid.uuid4().hex[:16]
    state_param = json.dumps({'loginSessionId': login_session_id, 'nextUrl': 'guide'})

    # Walk the same OAuth kickoff chain the SPA runs on page load. This is
    # what mints the OAUTH_REQUEST_ATTRIBUTES cookie /am/IdPwdAuth now
    # requires — skipping straight to /am/IdPwdAuth is what started 403'ing.
    try:
        session.get(_GUIDE_URL, timeout=20)
        redirect_resp = session.post(
            _LOGIN_REDIRECT_URL,
            params={'clientID': _WEB_CLIENT_ID, 'state': state_param},
            data={
                'clientID': _WEB_CLIENT_ID,
                'returnURL': _AUTH_RETURN_URL,
                'codeChallenge': challenge,
                'codeChallengeMethod': 'S256',
            },
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://stream.directv.com',
                'Referer': 'https://stream.directv.com/',
            },
            timeout=20, allow_redirects=True,
        )
        authenticate_resp = session.get(
            _IDENTITY_AUTHORIZE_URL,
            params={
                'scope': 'read',
                'response_type': 'code',
                'client_id': _FORGEROCK_CLIENT_ID,
                'redirect_uri': _AUTH_RETURN_URL,
                'state': state_param,
                'code_challenge': challenge,
                'code_challenge_method': 'S256',
            },
            headers={'Referer': redirect_resp.url},
            timeout=20, allow_redirects=True,
        )
    except Exception as exc:
        raise DirectvAuthError(f'DirecTV auth session priming failed: {exc}') from exc

    auth_headers = {
        'Content-Type': 'application/json',
        'Accept-API-Version': 'resource=2.0, protocol=1.0',
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'client_id': _FORGEROCK_CLIENT_ID,
        'Origin': 'https://identity.directv.com',
        'Referer': authenticate_resp.url,
        'Accept': 'application/json, text/plain, */*',
    }

    data = _json_or_error(
        session.post(_IDENTITY_AUTH_URL, headers=auth_headers, data='{}', timeout=30),
        'DirecTV username challenge',
    )
    if not any(c.get('type') == 'NameCallback' for c in data.get('callbacks') or []):
        raise DirectvAuthError('DirecTV username challenge did not return NameCallback')

    _status('running', 'Submitting email…')
    data = _json_or_error(
        session.post(_IDENTITY_AUTH_URL, headers=auth_headers,
                     json=_fill_auth_callback(data, username), timeout=30),
        'DirecTV password challenge',
    )
    if not any(c.get('type') == 'PasswordCallback' for c in data.get('callbacks') or []):
        raise DirectvAuthError('DirecTV password challenge did not return PasswordCallback')

    _status('running', 'Submitting password…')
    data = _json_or_error(
        session.post(_IDENTITY_AUTH_URL, headers=auth_headers,
                     json=_fill_auth_callback(data, password), timeout=30),
        'DirecTV password submit',
    )
    if not data.get('tokenId'):
        msg = data.get('message') or data.get('reason') or 'missing tokenId'
        raise DirectvAuthError(f'DirecTV login failed: {msg}')

    _status('running', 'Exchanging auth code…')
    authz = session.get(
        _IDENTITY_AUTHORIZE_URL,
        params={
            'scope': 'read',
            'response_type': 'code',
            'client_id': _FORGEROCK_CLIENT_ID,
            'redirect_uri': _AUTH_RETURN_URL,
            'state': state_param,
            'code_challenge': challenge,
            'code_challenge_method': 'S256',
        },
        headers={'Referer': authenticate_resp.url},
        timeout=30,
        allow_redirects=False,
    )
    location = authz.headers.get('location') or authz.url
    code = (parse_qs(urlparse(location).query).get('code') or [''])[0]
    if not code:
        raise DirectvAuthError(f'DirecTV authorize did not return an auth code (HTTP {authz.status_code})')

    token_response = session.post(
        _AUTHN_TOKEN_URL,
        data=[
            ('clientID', _WEB_CLIENT_ID),
            ('deviceClassID', str(uuid.uuid4())),
            ('clientMake', 'Google'),
            ('clientModel', 'Chrome'),
            ('authCode', code),
            ('codeVerifier', verifier),
            ('returnURL', _AUTH_RETURN_URL),
            ('reqParams', 'DEVICEID'),
            ('reqParams', 'AUTHGROUPS'),
        ],
        headers={
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json, text/plain, */*',
            'Origin': 'https://stream.directv.com',
            'Referer': 'https://stream.directv.com/',
        },
        timeout=30,
    )
    token_data = _json_or_error(token_response, 'DirecTV token exchange')
    bearer = (token_data.get('access_token') or '').strip()
    refresh = (token_data.get('refresh_token') or '').strip()
    activation = _normalize_activation_token(
        ((token_data.get('valuePairs') or {}).get('activationToken') or '').strip()
    )
    if not bearer or not refresh or not activation:
        raise DirectvAuthError(
            'DirecTV token exchange did not return bearer, refresh, and activation tokens'
        )

    # Cheap sanity check: the token should authorize metadata without browser
    # cookies or clientContext.
    verify = session.get(
        _ALLCHANNELS_URL,
        params={'sort': 'OrdCh=ASC'},
        headers={
            'Authorization': f'Bearer {bearer}',
            'Accept': 'application/json, text/plain, */*',
            'Origin': 'https://stream.directv.com',
            'Referer': 'https://stream.directv.com/',
        },
        timeout=30,
    )
    if verify.status_code < 200 or verify.status_code >= 300:
        raise DirectvAuthError(f'DirecTV token verify failed HTTP {verify.status_code}')

    _status('success', 'Captured DirecTV session.')
    return {
        'bearer_token': bearer,
        'refresh_token': refresh,
        'activation_token': activation,
        'cookies': [],
        'captured_at': time.time(),
        'auth_method': 'curl_cffi',
    }


def capture_directv_auth_fast(
    username: str,
    password: str,
    *,
    on_status: Callable[[str, str], None] | None = None,
) -> dict:
    try:
        return capture_directv_auth_cffi(username, password, on_status=on_status)
    except Exception as exc:
        logger.warning('[directv-auth] curl-cffi auth failed, falling back to browser: %s', exc)
        if on_status:
            try:
                on_status('running', 'Lightweight auth failed; falling back to browser…')
            except Exception:
                pass
        return capture_directv_auth(username, password, on_status=on_status)


def capture_directv_auth(
    username: str,
    password: str,
    *,
    headless: bool = False,
    on_status: Callable[[str, str], None] | None = None,
) -> dict:
    """
    Drive a real Chrome browser (via Playwright) through DirecTV's sign-in flow and capture the
    Authorization bearer token + clientContext + session cookies that plain
    `requests` calls need against api.cld.dtvce.com, plus (best-effort) the
    identityCookie DirecTV's Widevine license flow needs — see this module's
    module docstring for the full DRM picture.

    Returns {'bearer_token': str, 'client_context': str | None,
             'cookies': list[dict], 'captured_at': float,
             'identity_cookie': str | None (absent if DRM activation didn't fire),
             'identity_cookie_expires_at': str | None}.
    Raises DirectvAuthError on any failure (bad creds, captcha/bot-block,
    unrecognized page, timeout) — but NOT on a missing identityCookie, which
    is best-effort (metadata/EPG scraping doesn't need it, only playback does).
    """
    def _status(state: str, detail: str = '') -> None:
        level = logger.info if state != 'running' else logger.debug
        level('[directv-auth] %s %s', state, detail)
        if on_status:
            try:
                on_status(state, detail)
            except Exception:
                pass

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as _PWTimeout
    except ImportError as exc:
        raise DirectvAuthError(f'Playwright unavailable: {exc}') from exc

    captured: dict[str, Any] = {}

    try:
        os.makedirs(_DIRECTV_PROFILE_DIR, exist_ok=True)
    except Exception as exc:
        raise DirectvAuthError(f'Could not create browser profile dir: {exc}') from exc

    with contextlib.ExitStack() as stack:
        if not headless:
            stack.enter_context(_virtual_display())
        p = stack.enter_context(sync_playwright())
        try:
            # EXPERIMENT (see dev/dvrtuner/EXTRACTED-chrome-matched-stealth.js):
            # previously this used patchright specifically because stock
            # Playwright Chromium gets its HTTP/2 connection reset by
            # DirecTV's Akamai bot protection, and even genuine Chrome driven
            # over bare CDP was assumed to fail the same way (Akamai's sensor
            # JS detecting Playwright's CDP automation artifacts regardless
            # of the browser binary underneath). A sibling project
            # (dev/dvrtuner) does drive DirecTV successfully with stock
            # `playwright` + channel="chrome" + a manual JS fingerprint patch
            # instead of a patched automation fork — this swaps to that
            # combination to test whether the manual patch (_STEALTH_SCRIPT)
            # is sufficient on its own, given our current patchright-based
            # flow has been unreliable in practice. If this regresses,
            # reverting to `from patchright.sync_api import sync_playwright`
            # is the fix.
            #
            # channel="chrome" launches an actual Google Chrome build
            # (installed via `playwright install chrome`); a persistent
            # profile (not a fresh context every run) lets DirecTV/Akamai's
            # device-trust cookies accumulate across runs instead of
            # presenting as a never-before-seen browser each time —
            # confirmed live: a from-scratch profile needs the full
            # email/password flow, but a profile with a prior session often
            # skips the login form entirely on the next run.
            #
            # No user_agent override — real Chrome's own UA must stay
            # internally consistent with its genuine TLS/JS fingerprint,
            # which is the whole point of using it over impersonation. (The
            # sibling project does override UA/platform, but it's matching
            # its own real Mac; see the note above _STEALTH_SCRIPT.)
            context = p.chromium.launch_persistent_context(
                user_data_dir=_DIRECTV_PROFILE_DIR,
                headless=headless,
                channel='chrome',
                args=_STEALTH_ARGS,
                ignore_default_args=_IGNORE_DEFAULT_ARGS,
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York',
            )
        except Exception as exc:
            raise DirectvAuthError(f'Chrome unavailable: {exc}') from exc
        try:
            context.add_init_script(_STEALTH_SCRIPT)
            page = context.pages[0] if context.pages else context.new_page()

            def _on_request(request):
                if _ACTIVATE_MARKER in request.url:
                    try:
                        data = json.loads(request.post_data or '{}')
                        token = data.get('activationToken')
                    except Exception:
                        token = None
                    if token:
                        captured['activation_token'] = token
                        logger.info('[directv-auth] captured activationToken from activation request')
                    return
                if 'bearer_token' in captured or _ALLCHANNELS_MARKER not in request.url:
                    return
                auth = request.headers.get('authorization', '')
                if not auth.lower().startswith('bearer '):
                    return
                captured['bearer_token'] = auth.split(' ', 1)[1].strip()
                q = parse_qs(urlparse(request.url).query)
                cc = q.get('clientContext') or q.get('clientcontext')
                if cc:
                    captured['client_context'] = cc[0]
                logger.info('[directv-auth] captured bearer token + clientContext from network')

            def _on_response(response):
                url_l = response.url.lower()
                wants_activation = 'activation_token' not in captured and (
                    'auth' in url_l or 'token' in url_l or 'identity' in url_l
                )
                wants_identity = 'identity_cookie' not in captured and _ACTIVATE_MARKER in response.url
                if not wants_activation and not wants_identity:
                    return
                try:
                    data = response.json()
                except Exception:
                    return
                if wants_activation:
                    token = _find_json_value(data, 'activationToken')
                    if token:
                        captured['activation_token'] = token
                        logger.info('[directv-auth] captured activationToken from network')
                if wants_identity:
                    cookie = data.get('identityCookie')
                    if not cookie:
                        return
                    captured['identity_cookie'] = cookie
                    if data.get('idCookieExpiration'):
                        captured['identity_cookie_expires_at'] = data['idCookieExpiration']
                    logger.info('[directv-auth] captured identityCookie from network')

            page.on('request', _on_request)
            page.on('response', _on_response)

            _status('running', 'Loading DirecTV guide…')
            try:
                page.goto(_GUIDE_URL, wait_until='domcontentloaded', timeout=30000)
            except _PWTimeout:
                raise DirectvAuthError('Timed out loading stream.directv.com')

            if _is_captcha_or_botcheck(page):
                raise DirectvAuthError('DirecTV served a bot-check/CAPTCHA page')

            # /guide is a SPA — an unauthenticated session is NOT redirected to
            # identity.directv.com by the server; it hydrates client-side and
            # issues its own multi-hop redirect (/guide -> /authenticate ->
            # identity.directv.com's OAuth chain) that takes several seconds.
            # Checking page.url immediately after domcontentloaded is too
            # early and misreads "hasn't redirected yet" as "already
            # authenticated", skipping login entirely.
            page.wait_for_timeout(2000)
            _human_idle_wander(page, moves=4)
            page.wait_for_timeout(1500)

            if _is_captcha_or_botcheck(page):
                raise DirectvAuthError('DirecTV served a bot-check/CAPTCHA page')
            if _is_akamai_error_page(page):
                raise DirectvAuthError(
                    'DirecTV/Akamai returned a transient error page (8003) before login even '
                    'started — this is a known intermittent block, not a credentials problem; '
                    'will retry on the next scheduled attempt'
                )

            # The login form isn't reliably a top-level navigation to
            # identity.directv.com — it can render inside an iframe embedded
            # in stream.directv.com/guide with the top-level URL never
            # changing, so search every frame rather than gating on page.url.
            # A persistent profile with a still-valid session skips this
            # entirely (no email frame appears) — confirmed live.
            email_frame = _find_frame_with_selector(page, _EMAIL_SELECTOR, timeout_ms=15000)
            if email_frame:
                _status('running', 'Entering email…')
                _human_fill(page, email_frame, _EMAIL_SELECTOR, username)
                page.wait_for_timeout(random.randint(300, 700))
                if not _click_primary_button(page, email_frame, candidates=('Next', 'Continue')):
                    raise DirectvAuthError('Could not find a submit button on the email step')
                page.wait_for_timeout(2000)
                if _is_akamai_error_page(page):
                    raise DirectvAuthError(
                        'DirecTV/Akamai returned a transient error page (8003) after email '
                        'submit — known intermittent block, not a credentials problem'
                    )

                _status('running', 'Entering password…')
                password_frame = _find_frame_with_selector(page, _PASSWORD_SELECTOR, timeout_ms=15000)
                if not password_frame:
                    raise DirectvAuthError('Password field not found on DirecTV sign-in page')
                _human_fill(page, password_frame, _PASSWORD_SELECTOR, password)
                page.wait_for_timeout(random.randint(300, 700))
                if not _click_primary_button(page, password_frame, candidates=('Sign In', 'Log In', 'Submit', 'Next', 'Continue')):
                    raise DirectvAuthError('Could not find a submit button on the password step')

                page.wait_for_timeout(4000)
                if _is_captcha_or_botcheck(page):
                    raise DirectvAuthError('DirecTV served a bot-check/CAPTCHA page during login')

                # Post-login redirect doesn't always land back on /guide.
                try:
                    page.goto(_GUIDE_URL, wait_until='domcontentloaded', timeout=20000)
                except _PWTimeout:
                    pass

            # The AllChannels request fires during guide load — wait for the
            # network listener above to catch it.
            _status('running', 'Waiting for session token…')
            deadline = time.time() + _LOGIN_TIMEOUT
            while 'bearer_token' not in captured and time.time() < deadline:
                page.wait_for_timeout(500)

            if 'bearer_token' not in captured:
                raise DirectvAuthError(
                    'Did not observe an authenticated AllChannels request — login may have failed'
                )

            # Best-effort: click any channel to trigger a real DRM playback
            # attempt, which is what causes the browser's Widevine CDM to fire
            # the activation exchange that yields identityCookie (see
            # this module's module docstring for why the license flow needs
            # it). Not fatal on failure — metadata/EPG scraping only needs the
            # bearer token above; only the license proxy needs this.
            _status('running', 'Triggering DRM activation…')
            try:
                page.locator(_CHANNEL_LOGO_SELECTOR).first.click(timeout=5000)
                deadline = time.time() + _ACTIVATE_TIMEOUT
                while 'identity_cookie' not in captured and time.time() < deadline:
                    page.wait_for_timeout(500)
                if 'identity_cookie' not in captured:
                    logger.warning('[directv-auth] no widevine/activate response observed '
                                    '(DRM playback may not have started) — continuing without identityCookie')
            except Exception as exc:
                # DirecTV often fires the Widevine activation XHR even while
                # Playwright is still waiting for the SPA click action to become
                # fully actionable. Give the response handlers a short grace
                # window before deciding the activation attempt really failed.
                deadline = time.time() + 3.0
                while 'identity_cookie' not in captured and time.time() < deadline:
                    page.wait_for_timeout(250)
                if 'identity_cookie' in captured or 'activation_token' in captured:
                    logger.debug('[directv-auth] DRM activation click timed out after activation capture: %s', exc)
                else:
                    logger.warning('[directv-auth] channel click for DRM activation failed: %s', exc)

            if 'activation_token' not in captured:
                token = _capture_activation_token_from_storage(page)
                if token:
                    captured['activation_token'] = token
                    logger.info('[directv-auth] captured activationToken from browser storage')

            captured['cookies'] = [
                {'name': c['name'], 'value': c['value'], 'domain': c.get('domain', ''),
                 'path': c.get('path', '/')}
                for c in context.cookies()
            ]
            captured['captured_at'] = time.time()
            _status('success', 'Captured DirecTV session.')
            return captured
        finally:
            context.close()


# ── Manual admin-UI entry point ─────────────────────────────────────────────

def run_directv_auth(
    redis_url: str,
    source_id: int,
    username: str,
    password: str,
    app=None,
    lock_key: str | None = None,
) -> None:
    """Redis-status-driven wrapper for DirecTV authentication.

    Used by the admin-UI "Authenticate" button and by queued background
    refreshes. Queued refreshes pass no Flask app object, so this function
    creates one when needed and persists the captured session directly.
    """
    import redis as _redis
    r = _redis.from_url(redis_url)

    _write_status(r, source_id, 'starting', 'Signing in…')
    logger.info('[directv-auth] starting for source_id=%s', source_id)

    def _on_status(state: str, detail: str) -> None:
        if state == 'running':
            _write_status(r, source_id, 'running', detail)

    try:
        try:
            result = capture_directv_auth_fast(username, password, on_status=_on_status)
        except Exception as exc:
            logger.warning('[directv-auth] capture failed for source_id=%s: %s', source_id, exc)
            _write_status(r, source_id, 'failed', str(exc))
            return

        if app is None:
            try:
                from flask import current_app
                app = current_app._get_current_object()
            except Exception:
                from app import create_app
                app = create_app()

        try:
            with app.app_context():
                from ..extensions import db
                from ..models import Source

                source = Source.query.get(source_id)
                if source is not None:
                    cfg = dict(source.config or {})
                    cfg['bearer_token'] = result['bearer_token']
                    if result.get('refresh_token'):
                        cfg['refresh_token'] = result['refresh_token']
                    if result.get('client_context'):
                        cfg['client_context'] = result['client_context']
                    else:
                        cfg.pop('client_context', None)
                    if result.get('activation_token'):
                        cfg['activation_token'] = result['activation_token']
                    cfg['cookies'] = result.get('cookies') or []
                    cfg['token_captured_at'] = result['captured_at']
                    if result.get('identity_cookie'):
                        cfg['identity_cookie'] = result['identity_cookie']
                    else:
                        cfg.pop('identity_cookie', None)
                    if result.get('identity_cookie_expires_at'):
                        cfg['identity_cookie_expires_at'] = result['identity_cookie_expires_at']
                    else:
                        cfg.pop('identity_cookie_expires_at', None)
                    if result.get('auth_method'):
                        cfg['auth_method'] = result['auth_method']
                    source.config = cfg
                    db.session.commit()
                    logger.info('[directv-auth] persisted session to source config source_id=%s', source_id)

                    # A prior scrape may have hit the stale token, skipped itself,
                    # and left DirecTV waiting for the next scheduled run (up to
                    # scrape_interval away). Queue a fresh scrape now so the new
                    # session gets used within seconds instead of hours.
                    try:
                        from rq import Queue
                        from app.worker import _scrape_job_already_active
                        q = Queue('scraper', connection=r)
                        if not _scrape_job_already_active(q, source.name):
                            q.enqueue(
                                'app.worker.run_scraper', source.name,
                                job_timeout=3600, job_id=f'scrape-{source.name}',
                            )
                            logger.info(
                                '[directv-auth] queued follow-up scrape after session refresh source_id=%s',
                                source_id,
                            )
                    except Exception:
                        logger.warning(
                            '[directv-auth] failed to queue follow-up scrape after refresh',
                            exc_info=True,
                        )
        except Exception as exc:
            logger.error('[directv-auth] failed to persist result directly: %s', exc)

        r.set(_result_key(source_id), json.dumps(result), ex=_RESULT_TTL)
        _write_status(r, source_id, 'success', 'Logged in — session captured.')
    finally:
        if lock_key:
            try:
                r.delete(lock_key)
            except Exception:
                logger.debug('[directv-auth] failed to release refresh lock %s', lock_key, exc_info=True)


class DirectvScraper(BaseScraper):
    """DirecTV Stream — subscription live TV, resolved natively via DirecTV's own API."""

    source_name      = 'directv'
    display_name     = 'DirecTV Stream'
    # EPG/channel metadata doesn't need to refresh often; auth-token freshness
    # is checked independently every run via pre_run_setup(), decoupled from
    # this interval.
    scrape_interval     = 720
    min_scrape_interval = 60
    config_required      = True
    is_premium            = True
    source_category       = 'premium'
    activation_url         = _ACTIVATE_URL
    license_url            = _LICENSE_URL
    # Every DirecTV Stream channel is DRM-protected. Keep them out of standard
    # IPTV outputs and route them through the PrismCast bridge immediately after
    # scraping instead of waiting for Stream Audit to discover that fact.
    all_channels_require_drm_bridge = True
    stream_audit_enabled  = True
    audit_requires_config = ['username', 'password']

    config_schema = [
        ConfigField('username', 'Email', required=True,
                    placeholder='you@example.com',
                    help_text='Your DirecTV Stream login email.'),
        ConfigField('password', 'Password', field_type='password', required=True,
                    secret=True,
                    help_text='Your DirecTV Stream password.'),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Origin': 'https://stream.directv.com',
            'Referer': 'https://stream.directv.com/',
        })
        self._apply_auth_headers()

    def _apply_auth_headers(self) -> None:
        token = self.config.get('bearer_token')
        if token:
            self.session.headers['Authorization'] = f'Bearer {token}'
        for c in self.config.get('cookies') or []:
            try:
                self.session.cookies.set(
                    c['name'], c['value'],
                    domain=c.get('domain') or None, path=c.get('path') or '/',
                )
            except Exception:
                continue

    # ── Auth ─────────────────────────────────────────────────────────────────

    def _token_stale(self) -> bool:
        captured_at = self.config.get('token_captured_at') or 0
        return (not self.config.get('bearer_token')) or (time.time() - float(captured_at)) > _TOKEN_TTL

    def pre_run_setup(self) -> None:
        if not self._token_stale():
            return
        username = (self.config.get('username') or '').strip()
        password = (self.config.get('password') or '').strip()
        if not username or not password:
            raise ScrapeSkipError('DirecTV Stream: username and password are required')
        self._start_background_reauth(username, password)
        raise ScrapeSkipError(
            'DirecTV Stream: session token expired — refreshing in the background, '
            'will pick up on the next scheduled run'
        )

    def _mark_auth_stale_and_reauth(self, reason: str) -> None:
        # Do not queue token-field config updates here: play-route finally blocks
        # persist scraper updates after resolve() returns, and a fast background
        # login can otherwise be overwritten by those stale updates.
        self._update_cache('directv_playback', {})

        username = (self.config.get('username') or '').strip()
        password = (self.config.get('password') or '').strip()
        if not username or not password:
            logger.warning('[directv] cannot auto-reauth after %s: username/password missing', reason)
            return
        self._start_background_reauth(username, password)

    def _start_background_reauth(self, username: str, password: str) -> None:
        """Queue a login outside the scraper work-horse process.

        Scrape jobs run under RQ's forking Worker. A daemon thread started from
        pre_run_setup() can be killed as soon as the skipped scrape job exits, so
        the refresh has to run as its own job on the non-forking fast worker.
        """
        try:
            import redis as _redis
            from rq import Queue
            from flask import current_app
        except Exception:
            logger.warning('[directv] cannot start background re-auth (redis/flask unavailable)')
            return

        lock_key = f'directv:auth:refreshing:{self.source_name}'
        try:
            r = _redis.from_url(current_app.config['REDIS_URL'])
            if not r.set(lock_key, '1', nx=True, ex=_REAUTH_LOCK_TTL):
                logger.info('[directv] background re-auth already in progress')
                return
        except Exception:
            logger.warning('[directv] cannot queue background re-auth (redis unavailable)', exc_info=True)
            return

        try:
            from ..models import Source
            source = Source.query.filter_by(name=self.source_name).first()
            if source is None:
                logger.warning('[directv] cannot start background re-auth: source not found')
                try:
                    r.delete(lock_key)
                except Exception:
                    pass
                return

            q = Queue('fast', connection=r)
            q.enqueue(
                'app.scrapers.directv.run_directv_auth',
                current_app.config['REDIS_URL'],
                source.id,
                username,
                password,
                None,
                lock_key,
                job_timeout=1800,
                job_id=f'directv-auth-refresh-{source.id}-{int(time.time())}',
            )
            logger.info('[directv] queued background re-auth job source_id=%s', source.id)
        except Exception:
            try:
                r.delete(lock_key)
            except Exception:
                pass
            logger.exception('[directv] failed to queue background re-auth job')

    # ── Channels ─────────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        self._apply_auth_headers()
        if not self.session.headers.get('Authorization'):
            raise ScrapeSkipError(
                'DirecTV Stream: not authenticated yet — use "Authenticate" in source settings'
            )

        _progress(self, 'channels', 0, 1)
        params = {'sort': 'OrdCh=ASC'}
        client_context = self.config.get('client_context')
        if client_context:
            params['clientContext'] = client_context

        try:
            r = self.session.get(_ALLCHANNELS_URL, params=params, timeout=20)
        except requests.RequestException as exc:
            raise ScrapeSkipError(f'DirecTV Stream: AllChannels request failed ({exc})') from exc

        if r.status_code == 401:
            # Token rejected — clear it so pre_run_setup re-authenticates next
            # run instead of failing loudly on the same stale token forever.
            self._update_config('bearer_token', '')
            self._update_config('token_captured_at', 0)
            raise ScrapeSkipError('DirecTV Stream: session expired (401) — will re-authenticate next run')
        if r.status_code != 200:
            raise ScrapeSkipError(f'DirecTV Stream: AllChannels returned HTTP {r.status_code}')

        try:
            payload = r.json()
        except ValueError as exc:
            raise ScrapeSkipError(f'DirecTV Stream: AllChannels response was not JSON ({exc})') from exc

        rows = _find_channel_list(payload)
        if not rows:
            raise ScrapeSkipError('DirecTV Stream: could not find a channel list in the AllChannels response')

        channels: list[ChannelData] = []

        for row in rows:
            ccid = _pick(row, 'ccid', 'ccId', 'channelId', 'channel_id', 'id')
            resource_id = _pick(row, 'resourceId', 'resourceID', 'resource_id', 'guid')
            name = _pick(row, 'channelName', 'name', 'displayName', 'title')
            if not ccid or not resource_id or not name:
                continue

            number_raw = _pick(row, 'channelNumber', 'channel_number', 'number')
            number = int(number_raw) if number_raw.isdigit() else None
            logo = _pick(row, 'logoUrl', 'logoURL', 'logo_url') or (
                f"https://dfwfis.prod.dtvcdn.com/catalog/image/imageserver/v1/"
                f"service/channel/{resource_id}/chlogo-clb-guide/120/90"
            )

            category = category_for_channel(name, None) or infer_category_from_name(name) or 'Entertainment'
            language = infer_language_from_metadata(name)

            channels.append(ChannelData(
                source_channel_id=ccid,
                name=name,
                stream_url=f'directv://{ccid}/{quote(resource_id, safe="")}',
                logo_url=logo,
                category=category,
                language=language,
                stream_type='hls',
                number=number,
                gracenote_id=(resolve_gracenote('directv', lookup_key=ccid)
                              or resolve_gracenote('directv', lookup_key=f'name:{_directv_gracenote_key(name)}')),
            ))

        _progress(self, 'channels', 1, 1)
        return channels

    # ── EPG ──────────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        self._apply_auth_headers()
        if not self.session.headers.get('Authorization'):
            return []

        # The schedule API keys by resourceId, but our channel IDs are ccid
        # (see module docstring) — build the reverse map so parsed programs
        # land on the right Channel row.
        resource_to_ccid: dict[str, str] = {}
        resource_ids: list[str] = []
        for c in channels:
            if not c.stream_url or not c.stream_url.startswith('directv://'):
                continue
            _, _, rest = c.stream_url.partition('directv://')
            ccid, _, encoded_resource_id = rest.partition('/')
            resource_id = unquote(encoded_resource_id)
            if ccid and resource_id:
                resource_to_ccid[resource_id] = ccid
                resource_ids.append(resource_id)

        if not resource_ids:
            return []

        now = datetime.now(timezone.utc)
        end = now + timedelta(days=_SCHEDULE_DAYS)
        window = timedelta(hours=_SCHEDULE_WINDOW_HOURS)
        client_context = self.config.get('client_context')

        programs: list[ProgramData] = []
        seen_schedule_ids: set[str] = set()

        batch_count = (len(resource_ids) + _SCHEDULE_BATCH - 1) // _SCHEDULE_BATCH
        window_count = max(1, int((end - now).total_seconds() // window.total_seconds()))
        total_batches = batch_count * window_count
        completed_batches = 0
        _progress(self, 'epg', 0, total_batches)

        w_start = now
        while w_start < end:
            w_end = min(end, w_start + window)
            for i in range(0, len(resource_ids), _SCHEDULE_BATCH):
                batch = resource_ids[i:i + _SCHEDULE_BATCH]
                params: dict[str, Any] = {
                    'startTime': int(w_start.timestamp() * 1000),
                    'endTime': int(w_end.timestamp() * 1000),
                    'include4K': 'false',
                    'is4KCompatible': 'false',
                    'fisProperties': _SCHEDULE_FIS_PROPERTIES,
                    # Must be ONE comma-joined value — passing a list here lets
                    # `requests` serialize it as repeated `channelIds=` keys,
                    # which the API accepts (200 OK) but silently honors only
                    # the first one, dropping the rest with no error.
                    'channelIds': ','.join(batch),
                }
                if client_context:
                    params['clientContext'] = client_context

                try:
                    r = self.session.get(_SCHEDULE_URL, params=params, timeout=20)
                except requests.RequestException as exc:
                    logger.warning('[directv] schedule request failed: %s', exc)
                    completed_batches += 1
                    _progress(self, 'epg', min(completed_batches, total_batches), total_batches)
                    continue
                if r.status_code != 200:
                    logger.warning('[directv] schedule HTTP %s', r.status_code)
                    completed_batches += 1
                    _progress(self, 'epg', min(completed_batches, total_batches), total_batches)
                    continue
                try:
                    payload = r.json()
                except ValueError:
                    completed_batches += 1
                    _progress(self, 'epg', min(completed_batches, total_batches), total_batches)
                    continue

                for sched in (payload.get('schedules') or []):
                    if not isinstance(sched, dict):
                        continue
                    schedule_channel_id = _pick(sched, 'channelId', 'scheduleChannelId')
                    ccid = resource_to_ccid.get(schedule_channel_id)
                    if not ccid:
                        continue
                    for content in (sched.get('contents') or []):
                        if not isinstance(content, dict):
                            continue
                        for cons in (content.get('consumables') or []):
                            if not isinstance(cons, dict):
                                continue
                            program = self._parse_program(ccid, content, cons, seen_schedule_ids)
                            if program:
                                programs.append(program)
                completed_batches += 1
                _progress(self, 'epg', min(completed_batches, total_batches), total_batches)
            w_start = w_end

        return programs

    @staticmethod
    def _parse_program(channel_id: str, content: dict, cons: dict, seen: set[str]) -> ProgramData | None:
        start = _parse_iso(_pick(cons, 'startTime'))
        end = _parse_iso(_pick(cons, 'endTime'))
        if not start or not end:
            return None

        sched_id = _pick(cons, 'scheduleId', 'resourceId') or _pick(content, 'apgId', 'canonicalId')
        if sched_id:
            if sched_id in seen:
                return None
            seen.add(sched_id)

        title = _pick(content, 'title', 'displayTitle', 'episodeTitle')
        if not title:
            return None
        display_title = _pick(content, 'displayTitle')
        episode_title = _pick(content, 'episodeTitle')
        sub = episode_title or (display_title if display_title != title else '')

        genres = [g for g in (content.get('genres') or []) if isinstance(g, str)]
        cats = [c for c in (content.get('categories') or []) if isinstance(c, str)]
        category = (genres or cats or [None])[0]

        season_raw = _pick(content, 'seasonNumber')
        episode_raw = _pick(content, 'episodeNumber')

        return ProgramData(
            source_channel_id=channel_id,
            title=title,
            start_time=start,
            end_time=end,
            description=_pick(content, 'description') or None,
            poster_url=DirectvScraper._pick_program_image(content.get('images')),
            category=category,
            episode_title=sub or None,
            season=int(season_raw) if season_raw.isdigit() else None,
            episode=int(episode_raw) if episode_raw.isdigit() else None,
            series_id=_pick(content, 'canonicalId') or None,
            episode_id=sched_id or None,
        )

    @staticmethod
    def _pick_program_image(images: Any) -> str | None:
        if not isinstance(images, list):
            return None

        by_type: dict[str, str] = {}
        for image in images:
            if not isinstance(image, dict):
                continue
            image_type = _pick(image, 'imageType').lower()
            image_url = _pick(image, 'imageUrl')
            if image_type and image_url:
                by_type[image_type] = image_url

        for image_type in ('poster', 'series-poster', 'bg-fplayer', 'iconic'):
            if by_type.get(image_type):
                return by_type[image_type]
        return None

    # ── Playback ─────────────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        # DirecTV is intrinsically bridge-only even when the resolved manifest
        # looks like ordinary HLS to the generic audit. Validate entitlement via
        # resolve(), then return the opaque URL sentinel so audit skips manifest
        # inspection and does not clear requires_drm_bridge.
        if raw_url.startswith('directv://'):
            self.resolve(raw_url)
            return raw_url
        return raw_url

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith('directv://'):
            return raw_url
        _, _, rest = raw_url.partition('directv://')
        ccid, _, _encoded_resource_id = rest.partition('/')
        if not ccid:
            raise RuntimeError(f'DirecTV Stream: malformed stream_url: {raw_url}')

        cached = (self.cache.get('directv_playback') or {}).get(ccid)
        if cached and (time.time() - float(cached.get('cached_at', 0))) < _PLAYBACK_CACHE_TTL:
            return cached['fallback_url']

        bearer = self.config.get('bearer_token')
        if not bearer:
            self._mark_auth_stale_and_reauth(f'missing bearer token for ccid={ccid}')
            raise RuntimeError(
                'DirecTV Stream: not authenticated; refreshing authentication in the background'
            )

        try:
            result = _fetch_channel_playback(
                bearer, self.config.get('cookies') or [], self.config.get('client_context'), ccid,
            )
        except DirectvAuthExpiredError as exc:
            self._mark_auth_stale_and_reauth(f'channel/v1 expired token for ccid={ccid}')
            raise RuntimeError(
                f'DirecTV Stream: session expired while resolving channel {ccid}; '
                'refreshing authentication in the background'
            ) from exc
        if not result:
            raise RuntimeError(f'DirecTV Stream: could not resolve playback for channel {ccid}')

        playback = dict(self.cache.get('directv_playback') or {})
        playback[ccid] = result
        self._update_cache('directv_playback', playback)
        return result['fallback_url']

    # ── DRM / license support ────────────────────────────────────────────────

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        # Unlike Roku, the license URL itself never varies per-channel —
        # channel-specific data goes in the POST body (see
        # prepare_license_request), not the URL.
        return cls.license_url

    @classmethod
    def get_activation_url(cls, config: dict) -> str | None:
        return cls.activation_url

    @classmethod
    def community_map_keys(cls, channel) -> list[str]:
        # Mirrors the ccid-then-name fallback in fetch_channels(): most of the
        # community CSV is keyed by ccid (== source_channel_id), but the manually
        # merged rows are keyed by name: (see comment above _directv_gracenote_key).
        keys = []
        if channel.source_channel_id:
            keys.append(channel.source_channel_id)
        if channel.name:
            keys.append(f'name:{_directv_gracenote_key(channel.name)}')
        return keys

    @classmethod
    def license_request_headers(cls, config: dict) -> dict:
        return {
            'Origin': 'https://stream.directv.com',
            'Referer': 'https://stream.directv.com/',
            'User-Agent': _UA,
        }

    @classmethod
    def prepare_activation_request(cls, challenge: bytes, config: dict) -> tuple[bytes, dict]:
        bearer = config.get('bearer_token') or ''
        activation_token = config.get('activation_token') or ''
        body = json.dumps({
            'bearerToken': bearer,
            'activationToken': activation_token,
            'activationChallenge': base64.b64encode(challenge).decode('ascii'),
        }).encode('utf-8')
        headers = {
            **cls.license_request_headers(config),
            'Content-Type': 'application/json',
        }
        if bearer:
            headers['Authorization'] = f'Bearer {bearer}'
        return body, headers

    @classmethod
    def process_activation_response(cls, response_bytes: bytes) -> dict:
        try:
            data = json.loads(response_bytes)
        except Exception as exc:
            logger.warning('[directv] activation response was not parseable JSON (%s): %s',
                            exc, response_bytes[:500])
            return {}
        result = {
            'identity_cookie': data.get('identityCookie') or '',
            'identity_cookie_expires_at': data.get('idCookieExpiration') or '',
            'response_bytes': b'',
        }
        license_data = data.get('licenseData') or []
        if license_data:
            result['response_bytes'] = base64.b64decode(license_data[0])
        elif data.get('serviceCertificateData'):
            result['response_bytes'] = base64.b64decode(data['serviceCertificateData'])
        return result

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        bearer = config.get('bearer_token') or ''
        identity_cookie = config.get('identity_cookie') or ''

        # The license needs THIS channel's playToken — the same one resolve()
        # cached via source_cache (merged into `config` by the license_proxy
        # route). Fall back to a fresh channel/v1 call if it's missing (a
        # license request should never normally precede a resolve, but stay
        # correct if it does).
        play_token = None
        license_content_id = channel_id or ''
        cached = (config.get('directv_playback') or {}).get(channel_id) if channel_id else None
        if cached:
            play_token = cached.get('play_token')
            license_content_id = (
                cached.get('license_content_id')
                or _license_content_id_from_stream_url(cached.get('fallback_url') or '', license_content_id)
            )
        if not play_token and channel_id and bearer:
            try:
                fresh = _fetch_channel_playback(
                    bearer, config.get('cookies') or [], config.get('client_context'), channel_id,
                )
            except DirectvAuthExpiredError as exc:
                logger.warning(
                    '[directv] license fallback could not refresh playToken for channel=%s: %s',
                    channel_id, exc,
                )
                fresh = None
            if fresh:
                play_token = fresh.get('play_token')
                license_content_id = fresh.get('license_content_id') or license_content_id

        body_dict = {
            'contentID': license_content_id,
            'contentType': '2',
            'identityCookie': identity_cookie,
            'authorizationToken': play_token or '',
            'licenseChallenge': base64.b64encode(challenge).decode('ascii'),
        }
        body = json.dumps(body_dict).encode('utf-8')
        headers = {
            **cls.license_request_headers(config),
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {bearer}',
        }
        return body, headers

    @classmethod
    def process_license_response(cls, response_bytes: bytes) -> bytes:
        """DirecTV wraps Widevine responses in JSON — either a real license
        ({"licenseData": ["<base64>"]}) or, when the CDM's first challenge is
        a privacy-mode SERVICE_CERTIFICATE_REQUEST (Shaka does this
        automatically when no certificate is pre-configured — same two-step
        exchange the existing Amazon license proxy also handles),
        {"serviceCertificateData": "<base64>"}. Shaka tells these apart by
        which challenge type IT sent, not by our field name — just hand back
        whichever raw bytes are present."""
        try:
            data = json.loads(response_bytes)
            license_data = data.get('licenseData') or []
            if license_data:
                return base64.b64decode(license_data[0])
            cert_data = data.get('serviceCertificateData')
            if cert_data:
                return base64.b64decode(cert_data)
            logger.warning('[directv] license response had neither licenseData nor '
                            'serviceCertificateData: %s', response_bytes[:500])
        except Exception as exc:
            logger.warning('[directv] license response was not parseable JSON (%s): %s',
                            exc, response_bytes[:500])
        return b''

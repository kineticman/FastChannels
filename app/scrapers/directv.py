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
import hashlib
import json
import logging
import os
import threading
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

logger = logging.getLogger(__name__)

_ALLCHANNELS_URL = "https://api.cld.dtvce.com/discovery/metadata/channel/v5/service/allchannels"
_SCHEDULE_URL = "https://api.cld.dtvce.com/discovery/edge/schedule/v1/service/schedule"
_CHANNEL_AUTH_URL = "https://api.cld.dtvce.com/right/authorization/channel/v1"
_ACTIVATE_URL = "https://api.cld.dtvce.com/rights/management/mdrm/vgemultidrm/v1/widevine/activate"
_LICENSE_URL = "https://api.cld.dtvce.com/rights/management/mdrm/vgemultidrm/v1/widevine/license"
_IDENTITY_AUTH_URL = "https://identity.directv.com/am/IdPwdAuth"
_IDENTITY_AUTHORIZE_URL = "https://identity.directv.com/authorize"
_AUTHN_TOKEN_URL = "https://api.cld.dtvce.com/authn-tokengo/v3/tokens"

_FORGEROCK_CLIENT_ID = "fr_web_02"
_WEB_CLIENT_ID = "UNIFIED_DTV_WEB"
_AUTH_RETURN_URL = "https://stream.directv.com/auth-return"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
)

# Refresh a bit before DirecTV's own session naturally expires — the exact TTL
# isn't documented anywhere; this mirrors the reference tool's daily-refresh
# cadence with headroom.
_TOKEN_TTL = 20 * 3600
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

# Gracenote station IDs from PrismCast DirecTV-capable canonical channel
# definitions. DirecTV API does not expose station IDs in the FAST
# channel payload, so this only fills channels whose names match known linear
# network selectors.
_DIRECTV_GRACENOTE_IDS = {
    'abcnewslive': '113380',
    'accnetwork': '111871',
    'ae': '51529',
    'amc': '59337',
    'animalplanet': '57394',
    'axstv': '28506',
    'bbcamerica': '64492',
    'bbcnews': '101449',
    'bbcnewsnorthamerica': '101449',
    'bet': '63236',
    'bether': '63220',
    'big10': '58321',
    'bigten': '58321',
    'bloombergtelevision': '71799',
    'bloombergtv': '71799',
    'bravo': '58625',
    'cartoonnetwork': '60048',
    'cmt': '59440',
    'cnbc': '58780',
    'cnbcworld': '26849',
    'cnn': '58646',
    'cnnihdeast': '83110',
    'cnninternational': '83110',
    'comedycentral': '62420',
    'cooking': '68065',
    'cookingchannel': '68065',
    'cspan': '68344',
    'cspan2': '68334',
    'destinationamerica': '60468',
    'destinationamericahd': '60468',
    'discovery': '56905',
    'discoverylife': '92204',
    'discoveryturbo': '31046',
    'disney': '59684',
    'disneychannel': '59684',
    'disneyjr': '74885',
    'disneyjunior': '74885',
    'e': '61812',
    'espn': '32645',
    'espn2': '45507',
    'espnews': '59976',
    'espnu': '60696',
    'foodnetwork': '50747',
    'foxbusiness': '58718',
    'foxbusinessnetwork': '58718',
    'foxnews': '60179',
    'foxnewschannel': '60179',
    'foxsports1': '82547',
    'foxsports2': '59305',
    'freeform': '59615',
    'freeformhd': '59615',
    'fs1': '82547',
    'fs2': '59305',
    'fx': '58574',
    'fxm': '70253',
    'fxmoviechannel': '70253',
    'fxx': '66379',
    'fyi': '58988',
    'gameshownetwork': '68827',
    'golf': '61854',
    'golfchannel': '61854',
    'gsnhd': '68827',
    'hallmark': '66268',
    'hallmarkchannel': '66268',
    'hgtv': '49788',
    'history': '57708',
    'hln': '64549',
    'ifc': '59444',
    'investigationdiscovery': '65342',
    'lifetime': '60150',
    'lifetimemovienetwork': '55887',
    'magnolianetwork': '67375',
    'mgm': '65687',
    'mlbnetwork': '62081',
    'msg': '35402',
    'msgsportsnet': '35383',
    'msgsportsnethd635': '35383',
    'msnow': '64241',
    'mtv': '60964',
    'mtv2': '75077',
    'mtvclassic': '92240',
    'natgeowild': '67331',
    'nationalgeographic': '49438',
    'nationalgeographicchannel': '49438',
    'nbatv': '45526',
    'nbcnewsnow': '114174',
    'nflnetwork': '45399',
    'nhlnetwork': '58690',
    'nhlnetworkhd': '58690',
    'own': '70388',
    'oxygen': '70522',
    'oxygentruecrime': '70522',
    'science': '57390',
    'secnetwork': '89714',
    'smithsonianchannel': '58532',
    'smithsonianchannelhd': '58532',
    'sportsnetnewyork': '50038',
    'sportsnetnewyorkhd639': '50038',
    'sundancetv': '71280',
    'syfy': '58623',
    'tbs': '58515',
    'tcm': '64312',
    'tennischannel': '60316',
    'tennischannelhd': '60316',
    'theweatherchannel': '58812',
    'theweatherchannelhd': '58812',
    'tlc': '57391',
    'tnt': '42642',
    'travel': '59303',
    'travelchannel': '59303',
    'trutv': '64490',
    'tvland': '73541',
    'usanetwork': '58452',
    'vh1': '60046',
    'vice': '65732',
    'wetv': '59296',
    'yesnetwork': '63558',
    'yesnetworkhd': '63558',
}

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
        logger.warning('[directv] channel/v1 not authorized for ccid=%s', ccid)
        return None

    pb = data.get('playbackData') or {}
    fallback_url = pb.get('fallbackStreamUrl') or pb.get('streamURL')
    play_token = (data.get('dRights') or {}).get('playToken')
    if not fallback_url or not play_token:
        return None
    return {'fallback_url': fallback_url, 'play_token': play_token, 'cached_at': time.time()}


# ── Playwright auth capture ────────────────────────────────────────────────

_STATUS_TTL = 600  # 10 min
_RESULT_TTL = 600  # 10 min for the caller to consume

_STEALTH_ARGS = [
    '--disable-blink-features=AutomationControlled',
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
]

_STEALTH_SCRIPT = """
(function () {
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    try {
        if (!navigator.plugins || !navigator.plugins.length) {
            Object.defineProperty(navigator, 'plugins', {
                get: () => { const a=[1,2,3,4,5]; a.__proto__=navigator.plugins.__proto__; return a; }
            });
        }
    } catch(e) {}
    try { Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']}); } catch(e) {}
    if (!window.chrome) {
        window.chrome = {runtime:{}, loadTimes:function(){}, csi:function(){}, app:{}};
    }
})();
"""

_GUIDE_URL = "https://stream.directv.com/guide"
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


def _is_identity_page(page) -> bool:
    return 'identity.directv.com' in page.url


def _click_primary_button(page) -> bool:
    """Click the primary action button on a DirecTV login step.

    These are plain <button> elements with no id and no type="submit" —
    unusable as a CSS/JS selector target — and the password step renders
    THREE buttons (Back, a password-visibility toggle, Sign In), so blindly
    clicking "the first button" clicks Back. Match on the known label text
    instead, trying each step's known label before ever falling back to "any
    visible button".
    """
    for text in ('Next', 'Continue', 'Sign In', 'Log In', 'Submit'):
        try:
            btn = page.get_by_role('button', name=text, exact=False)
            if btn.count() > 0:
                btn.first.click(timeout=3000)
                return True
        except Exception:
            continue
    try:
        page.locator('button:visible').first.click(timeout=3000)
        return True
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
    """
    if _cffi_requests is None:
        raise DirectvAuthError('curl_cffi unavailable')

    def _status(state: str, detail: str = '') -> None:
        logger.info('[directv-auth] %s %s', state, detail)
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

    # Prime Akamai cookies on both origins. The actual login remains API-only.
    try:
        session.get(_GUIDE_URL, timeout=20)
        session.get('https://identity.directv.com/', timeout=20, allow_redirects=True)
    except Exception as exc:
        raise DirectvAuthError(f'DirecTV auth cookie priming failed: {exc}') from exc

    auth_headers = {
        'Content-Type': 'application/json',
        'Accept-API-Version': 'resource=2.0, protocol=1.0',
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'client_id': _FORGEROCK_CLIENT_ID,
        'Origin': 'https://stream.directv.com',
        'Referer': 'https://stream.directv.com/',
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
    verifier = _pkce_verifier()
    authz = session.get(
        _IDENTITY_AUTHORIZE_URL,
        params={
            'scope': 'read',
            'response_type': 'code',
            'client_id': _FORGEROCK_CLIENT_ID,
            'redirect_uri': _AUTH_RETURN_URL,
            'code_challenge': _pkce_challenge(verifier),
            'code_challenge_method': 'S256',
        },
        headers={'Referer': 'https://stream.directv.com/'},
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
    headless: bool = True,
    on_status: Callable[[str, str], None] | None = None,
) -> dict:
    """
    Drive a Playwright browser through DirecTV's sign-in flow and capture the
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
        logger.info('[directv-auth] %s %s', state, detail)
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

    with sync_playwright() as p:
        try:
            # Fallback only. The normal DirecTV auth path is curl-cffi; bundled
            # Chromium remains available because other project features use it.
            browser = p.chromium.launch(headless=headless, args=_STEALTH_ARGS)
        except Exception as exc:
            raise DirectvAuthError(f'Playwright Chromium unavailable: {exc}') from exc
        try:
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent=_UA,
                locale='en-US',
                timezone_id='America/New_York',
            )
            context.add_init_script(_STEALTH_SCRIPT)
            page = context.new_page()

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
            page.wait_for_timeout(5000)

            if _is_captcha_or_botcheck(page):
                raise DirectvAuthError('DirecTV served a bot-check/CAPTCHA page')

            if _is_identity_page(page):
                _status('running', 'Entering email…')
                try:
                    page.wait_for_selector(_EMAIL_SELECTOR, timeout=15000)
                    page.fill(_EMAIL_SELECTOR, username)
                    if not _click_primary_button(page):
                        raise DirectvAuthError('Could not find a submit button on the email step')
                except _PWTimeout:
                    raise DirectvAuthError('Email field not found on DirecTV sign-in page')

                _status('running', 'Entering password…')
                try:
                    page.wait_for_selector(_PASSWORD_SELECTOR, timeout=15000)
                    page.fill(_PASSWORD_SELECTOR, password)
                    if not _click_primary_button(page):
                        raise DirectvAuthError('Could not find a submit button on the password step')
                except _PWTimeout:
                    raise DirectvAuthError('Password field not found on DirecTV sign-in page')

                try:
                    page.wait_for_url(lambda u: 'identity.directv.com' not in u, timeout=20000)
                except _PWTimeout:
                    if _is_captcha_or_botcheck(page):
                        raise DirectvAuthError('DirecTV served a bot-check/CAPTCHA page during login')
                    raise DirectvAuthError(f'Login did not complete (still on: {page.url[:100]})')

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
            browser.close()


# ── Manual admin-UI entry point ─────────────────────────────────────────────

def run_directv_auth(redis_url: str, source_id: int, username: str, password: str) -> None:
    """Redis-status-driven wrapper for the admin-UI "Authenticate" button.

    Runs in a daemon thread with no Flask/DB context (mirrors
    amazon_auth.run_amazon_auth) — the polling Flask route
    (app/routes/api.py: directv_auth_status) persists the result on success.
    """
    import redis as _redis
    r = _redis.from_url(redis_url)

    _write_status(r, source_id, 'starting', 'Signing in…')
    logger.info('[directv-auth] starting for source_id=%s', source_id)

    def _on_status(state: str, detail: str) -> None:
        if state == 'running':
            _write_status(r, source_id, 'running', detail)

    try:
        result = capture_directv_auth_fast(username, password, on_status=_on_status)
    except Exception as exc:
        logger.warning('[directv-auth] capture failed for source_id=%s: %s', source_id, exc)
        _write_status(r, source_id, 'failed', str(exc))
        return

    r.set(_result_key(source_id), json.dumps(result), ex=_RESULT_TTL)
    _write_status(r, source_id, 'success', 'Logged in — session captured.')


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

    def _start_background_reauth(self, username: str, password: str) -> None:
        """Kick off a Playwright login in a daemon thread, outside the worker's
        signal-guarded scrape phases. Dedupes via
        a short Redis lock so repeated stale-token scrape ticks don't pile up
        overlapping browser logins."""
        try:
            import redis as _redis
            from flask import current_app
        except Exception:
            logger.warning('[directv] cannot start background re-auth (redis/flask unavailable)')
            return

        try:
            r = _redis.from_url(current_app.config['REDIS_URL'])
            lock_key = f'directv:auth:refreshing:{self.source_name}'
            if not r.set(lock_key, '1', nx=True, ex=_REAUTH_LOCK_TTL):
                logger.info('[directv] background re-auth already in progress')
                return
        except Exception:
            logger.debug('[directv] redis lock unavailable, proceeding without dedup', exc_info=True)

        app = current_app._get_current_object()
        source_name = self.source_name

        def _run():
            try:
                result = capture_directv_auth_fast(username, password)
            except DirectvAuthError as exc:
                logger.warning('[directv] background re-auth failed: %s', exc)
                return
            except Exception:
                logger.exception('[directv] background re-auth crashed')
                return

            with app.app_context():
                from ..extensions import db
                from ..models import Source
                try:
                    source = Source.query.filter_by(name=source_name).first()
                    if source is None:
                        return
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
                    logger.info('[directv] background re-auth succeeded, persisted new token')
                except Exception:
                    db.session.rollback()
                    logger.exception('[directv] failed to persist re-auth result')

        threading.Thread(target=_run, daemon=True).start()

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
                gracenote_id=_DIRECTV_GRACENOTE_IDS.get(_directv_gracenote_key(name)),
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
            raise RuntimeError('DirecTV Stream: not authenticated — use "Authenticate" in source settings')

        result = _fetch_channel_playback(
            bearer, self.config.get('cookies') or [], self.config.get('client_context'), ccid,
        )
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
        cached = (config.get('directv_playback') or {}).get(channel_id) if channel_id else None
        if cached:
            play_token = cached.get('play_token')
        if not play_token and channel_id and bearer:
            fresh = _fetch_channel_playback(
                bearer, config.get('cookies') or [], config.get('client_context'), channel_id,
            )
            if fresh:
                play_token = fresh.get('play_token')

        body_dict = {
            'contentID': channel_id or '',
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

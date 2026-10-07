"""
ESPN — linear networks an ESPN+ (Disney) account can stream directly, played
through the DRM bridge.

Auth is ESPN's TV activation code, never a password: ESPN's web password login
(registerdisney.go.com guest/login) is reCAPTCHA-gated and rejects anything
without a g-recaptcha-token (PALOMINO_CHECK_FAILED). The TV flow isn't:

  1. OneID license plate under the Android TV client (ESPN-OTT.GC.ANDTV-PROD):
     api-key → license-plate → a 6-letter pairingCode + a fastcast topic.
  2. The user enters the code at espn.com/activate while signed in.
  3. ESPN's fastcast websocket publishes {id_token, refresh_token, …} on the topic.
  4. The id_token is exchanged (with an anonymous BAM device token) for an
     account-bound BAM token. Its refreshToken keeps the session alive via
     BAM's refreshToken mutation — the OneID refresh token isn't needed.

Playback (all confirmed live 2026-09-26, NFL Network played on a Fire Stick):
  watch.graph airing → source.playbackId → /v7/playback/ctr-regular → signed
  HLS (SAMPLE-AES-CTR, Widevine) + playbackRightsContext → Widevine license at
  /widevine/v1/channel/obtain-license (linear content; the non-/channel/ route
  400s "not of type vod") with the BAM token + x-playback-rights-authorization.

Each linear network has one stable BAM channelId across all its airings, so a
network is an ordinary 24/7 channel here; the airing is only needed to mint a
playbackId, and the resulting manifest is the network's linear feed.

ESPN Unlimited that a TV provider adds to the MyDisney account works like a
direct plan: the test account's Unlimited comes from DirecTV (isWholesaleUser,
wholesaleUserProvider=DIRECTV_US) and plays here. An account whose only link
is a TV-provider sign-in in the ESPN app gets not-entitled, since the BAM
token carries no plan (forum report 2026-09-29, post #3292).

That case is the second sign-in: the TV provider itself, through Adobe Pass v2
(requestor `ESPN`). No headless browser — Adobe's authenticate link for a
session opens in any browser (no token or cookies needed), so the user signs
in to their provider on their own phone while a job polls /profiles/<mvpd>.
The legacy regcode API 401s for ESPN's web software statement; v2 works.
The statement comes from espn.com's watch bundle at sign-in time. Adobe ties
the sign-in to the client that made it (~90 days), so the client's id and
secret are kept and its ~6h access token refreshed, never re-registered.

TV-provider playback uses an ANONYMOUS BAM token: /v7/playback/dtc-tve/
ctr-regular with a tveAuth block (Adobe client token + device fingerprint +
an RSS resource naming the network and airing); the Widevine license takes
the same anonymous token. ESPN, ESPN2, SEC and ACC played this way
(2026-09-29, ESPN2 on a Fire Stick). NFL Network and MLB Network answer 403
adobe-pass-failed-authorization — they need an ESPN plan, which matches the
same forum report (tested via HENA). With both sign-ins, the account is tried
first and the TV provider covers what its plan doesn't.

ESPN+ events are event-based and aren't handled here yet.
"""

from __future__ import annotations

import base64
import json
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin
from xml.sax.saxutils import escape

import requests

from ..gracenote_map import resolve_gracenote
from ..tve.adobe_pass import TVEAuthError, TVENotAuthorizedError, refresh_adobe_client_token
from .base import BaseScraper, ChannelData, ConfigField, ProgramData

logger = logging.getLogger(__name__)

SCHEME = 'espn://network/'

# Public client identifiers, same trust tier as the watch API key — the web
# SDK ships them to every browser. The BAM one base64-decodes to espn&browser&1.0.0.
_WATCH_API = 'https://watch.graph.api.espn.com/api'
_WATCH_API_KEY = '0dbf88e8-cc6d-41da-aa83-18b5c630bc5c'
_BAM_CLIENT_TOKEN = 'Bearer ZXNwbiZicm93c2VyJjEuMC4w.ptUt7QxsteaRruuPmGZFaJByOoqKvDP2a5YkInHrc7c'
_BAM_DEVICE_GRAPH = 'https://espn.api.edge.bamgrid.com/graph/v1/device/graphql'
_BAM_PUBLIC_GRAPH = 'https://espn.api.edge.bamgrid.com/v1/public/graphql'
_PLAYBACK_URL = 'https://espn.playback.edge.bamgrid.com/v7/playback/ctr-regular'
_LICENSE_URL = 'https://playback.svcs.plus.espn.com/widevine/v1/channel/obtain-license'
_EVENT_LICENSE_URL = 'https://playback.svcs.plus.espn.com/widevine/v1/obtain-license'
_ONEID_TV_BASE = 'https://registerdisney.go.com/jgc/v6/client/ESPN-OTT.GC.ANDTV-PROD'
ACTIVATE_URL = 'https://www.espn.com/activate'

_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
       '(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36')
# ctr-regular 400s (pbo:400-005) without x-application-version, and the web
# SDK sends the rest on every BAM call — copied from a real HAR.
_BAM_HEADERS = {
    'User-Agent': _UA,
    'Origin': 'https://www.espn.com',
    'Referer': 'https://www.espn.com/',
    'x-application-version': '0.0.1',
    'x-bamsdk-client-id': 'espn-a9b93989',
    'x-bamsdk-platform': 'javascript/windows/chrome',
    'x-bamsdk-version': '35.3',
}

# network.id (as the watch graph reports it on airings) → channel metadata.
# Every linear network ESPN streams itself. Which ones an account can play
# depends on its plan (ESPN Unlimited covers all of these; plain ESPN+ covers
# none) — the stream audit disables the rest as NotAuthorized. Note the
# watch graph lists the cable networks as authTypes ["MVPD"] even so: ESPN2
# played with an Unlimited account and no cable login (confirmed 2026-09-26).
# gracenote = the national HD feed's station ID (DirecTV's; Deportes supplied
# by the maintainer). The community CSV (key = network id) can override.
# tve = a TV-provider sign-in can play it (NFL/MLB Network need an ESPN plan).
# logo = used instead of the watch API's logo, whose MLB Network URL 404s.
NETWORKS = {
    'espn1': {'name': 'ESPN', 'gracenote': '32645', 'tve': True},
    'espn2': {'name': 'ESPN2', 'gracenote': '45507', 'tve': True},
    'espnu': {'name': 'ESPNU', 'gracenote': '60696', 'tve': True},
    'espnews': {'name': 'ESPNews', 'gracenote': '59976', 'tve': True},
    'espndeportes': {'name': 'ESPN Deportes', 'gracenote': '25595', 'language': 'es', 'tve': True},
    'sec': {'name': 'SEC Network', 'gracenote': '89714', 'tve': True},
    'acc': {'name': 'ACC Network', 'gracenote': '111871', 'tve': True},
    'nfl_network_domestic': {'name': 'NFL Network', 'gracenote': '45399'},
    'mlb_network': {'name': 'MLB Network', 'gracenote': '62081',
                    'logo': 'https://a.espncdn.com/watchespn/images/web/network_logos/channel_logo_mlb_2x.png'},
}

_ACCESS_REFRESH_MARGIN = 10 * 60   # refresh the 4h BAM token this early
_STREAM_CACHE_TTL = 10 * 60        # reuse a minted manifest + rights context this long
_EPG_DAYS = 3
_ACTIVATION_TIMEOUT = 600

_Q_AIRINGS = '''query($day:String!,$tz:String!){ airings(countryCode:"us", deviceType:SETTOP, tz:$tz, day:$day, limit:3000){
  id name shortName type startDateTime endDateTime description isReAir
  network{ id } sport{ name } league{ name } image{ url } source{ playbackId } } }'''


class ESPNAuthError(RuntimeError):
    pass


def _bam_gql(url: str, query: str, variables: dict, auth: str) -> dict:
    r = requests.post(url, headers={**_BAM_HEADERS, 'Authorization': auth,
                                    'Content-Type': 'application/json'},
                      json={'query': query, 'variables': variables}, timeout=20)
    try:
        data = r.json()
    except ValueError:
        raise ESPNAuthError(f'BAM HTTP {r.status_code}') from None
    if data.get('errors'):
        raise ESPNAuthError(f'BAM error: {data["errors"][0]}')
    return data


def _anonymous_token() -> str:
    """Fresh anonymous device → access token. Only needed once per activation,
    to authorize the id_token exchange."""
    data = _bam_gql(_BAM_DEVICE_GRAPH,
        'mutation registerDevice($input: RegisterDeviceInput!) { registerDevice(registerDevice: $input) { grant { grantType assertion } } }',
        {'input': {
            'deviceFamily': 'browser', 'applicationRuntime': 'chrome', 'deviceProfile': 'windows',
            'deviceLanguage': 'en-US', 'devicePlatformId': 'browser',
            'attributes': {'osDeviceIds': [{'identifier': str(uuid.uuid4()), 'type': 'espnAnonymousSessionId'}],
                           'manufacturer': 'microsoft', 'model': None, 'operatingSystem': 'windows',
                           'operatingSystemVersion': '10.0', 'browserName': 'chrome',
                           'browserVersion': '140.0.0', 'brand': 'web'},
        }}, _BAM_CLIENT_TOKEN)
    grant = data['data']['registerDevice']['grant']['assertion']
    data = _bam_gql(_BAM_DEVICE_GRAPH,
        'mutation exchangeDeviceGrantForAccessToken($input: ExchangeDeviceGrantForAccessTokenInput!) { exchangeDeviceGrantForAccessToken(exchangeDeviceGrantForAccessToken: $input) { accepted } }',
        {'input': {'deviceGrant': grant}}, _BAM_CLIENT_TOKEN)
    return data['extensions']['sdk']['token']['accessToken']


def _exchange_id_token(id_token: str) -> dict:
    data = _bam_gql(_BAM_PUBLIC_GRAPH,
        'mutation exchangeIDTokenForAccessToken($input: ExchangeIDTokenForAccessTokenInput!) { exchangeIDTokenForAccessToken(input: $input) { activeSession { sessionId } } }',
        {'input': {'idToken': id_token}}, 'Bearer ' + _anonymous_token())
    return data['extensions']['sdk']['token']


def _refresh_bam_token(refresh_token: str) -> dict:
    data = _bam_gql(_BAM_DEVICE_GRAPH,
        'mutation refreshToken($input: RefreshTokenInput!) { refreshToken(refreshToken: $input) { activeSession { sessionId } } }',
        {'input': {'refreshToken': refresh_token}}, _BAM_CLIENT_TOKEN)
    return data['extensions']['sdk']['token']


def token_config(token: dict) -> dict:
    """Config keys to persist for a BAM token dict."""
    return {
        'access_token': token['accessToken'],
        'refresh_token': token['refreshToken'],
        'access_expires_at': int(time.time()) + int(token.get('expiresIn') or 14400),
    }


# ── TV activation code ───────────────────────────────────────────────────────

def request_activation_code() -> dict:
    """Ask OneID for a TV activation code. Returns the plate dict:
    {pairingCode, fastCastHost, fastCastProfileId, fastCastTopic}."""
    headers = {'User-Agent': 'okhttp/4.9.2', 'Content-Type': 'application/json',
               'conversation-id': str(uuid.uuid4()), 'correlation-id': str(uuid.uuid4()),
               'expires': '-1'}
    r = requests.post(f'{_ONEID_TV_BASE}/api-key?langPref=en-US', headers=headers, timeout=20)
    api_key = r.headers.get('api-key')
    if not api_key:
        raise ESPNAuthError(f'ESPN activation: no api-key (HTTP {r.status_code})')
    body = {'content': {'adId': str(uuid.uuid4()), 'correlation-id': headers['correlation-id'],
                        'deviceId': str(uuid.uuid4()), 'deviceType': 'ANDTV',
                        'entitlementPath': 'login', 'entitlements': []}, 'ttl': 0}
    r = requests.post(f'{_ONEID_TV_BASE}/license-plate', json=body, timeout=20,
                      headers={**headers, 'Authorization': 'APIKEY ' + api_key})
    plate = (r.json() or {}).get('data') if r.ok else None
    if not plate or not plate.get('pairingCode'):
        raise ESPNAuthError(f'ESPN activation: no code (HTTP {r.status_code})')
    return plate


def wait_for_activation(plate: dict, timeout: float = _ACTIVATION_TIMEOUT,
                        should_stop=lambda: False) -> dict:
    """Block until the code is entered at espn.com/activate. Returns the fastcast
    payload ({id_token, refresh_token, swid, …}); raises TimeoutError otherwise.

    Fastcast: op C (connect → sid), op S (subscribe to the plate's topic), then
    an op P publish carries the tokens. Idle op B frames arrive every ~10s. A
    dropped socket reconnects and resubscribes — the topic outlives it."""
    from websockets.sync.client import connect

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if should_stop():
            raise TimeoutError('stopped')
        host = requests.get(plate['fastCastHost'] + '/public/websockethost', timeout=20).json()
        url = (f"wss://{host['ip']}:{host['securePort']}/FastcastService/pubsub/profiles/"
               f"{plate['fastCastProfileId']}?TrafficManager-Token={host['token']}")
        try:
            with connect(url, max_size=None, open_timeout=20) as ws:
                ws.send(json.dumps({'op': 'C'}))
                sid = json.loads(ws.recv(timeout=20)).get('sid')
                ws.send(json.dumps({'op': 'S', 'sid': sid, 'tc': plate['fastCastTopic'], 'rc': 200}))
                while time.monotonic() < deadline:
                    if should_stop():
                        raise TimeoutError('stopped')
                    try:
                        msg = json.loads(ws.recv(timeout=5))
                    except TimeoutError:
                        continue
                    if msg.get('op') == 'P' and msg.get('pl'):
                        payload = msg['pl']
                        payload = json.loads(payload) if isinstance(payload, str) else payload
                        if payload.get('id_token'):
                            return payload
        except TimeoutError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.info('[espn] activation socket dropped, reconnecting: %s', exc)
            time.sleep(2)
    raise TimeoutError('activation code expired')


ACTIVATION_STATUS_KEY = 'espn:activation:status'


def run_activation(plate: dict) -> None:
    """RQ job: wait for the user to enter `plate`'s code, then save the account
    session onto the espn source. Progress goes to ACTIVATION_STATUS_KEY; the job
    quits quietly once that key names a different code (a newer request) or is
    gone (cancelled)."""
    import redis

    from app.worker import flask_app
    from ..extensions import db
    from ..models import Source

    with flask_app.app_context():
        r = redis.from_url(flask_app.config['REDIS_URL'])
        code = plate['pairingCode']

        def superseded() -> bool:
            raw = r.get(ACTIVATION_STATUS_KEY)
            return not raw or json.loads(raw).get('code') != code

        def set_status(state: str, message: str = '') -> None:
            if not superseded():
                r.setex(ACTIVATION_STATUS_KEY, 720, json.dumps(
                    {'state': state, 'message': message, 'code': code, 'url': ACTIVATE_URL}))

        try:
            payload = wait_for_activation(plate, should_stop=superseded)
            token = _exchange_id_token(payload['id_token'])
        except TimeoutError as exc:
            if str(exc) == 'stopped':
                logger.info('[espn] activation code %s',
                            'replaced by a newer one' if r.get(ACTIVATION_STATUS_KEY) else 'stopped')
            else:
                set_status('expired', 'The code expired before it was entered.')
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning('[espn] activation failed: %s', exc)
            set_status('error', str(exc)[:300])
            return

        src = Source.query.filter_by(name='espn').first()
        if not src:
            set_status('error', 'ESPN source no longer exists.')
            return
        cfg = dict(src.config or {})
        cfg.update(token_config(token))
        cfg['signed_in_at'] = int(time.time())
        src.config = cfg
        db.session.commit()
        logger.info('[espn] signed in via activation code')
        set_status('success')


# ── TV-provider sign-in (Adobe Pass v2) ──────────────────────────────────────

_ADOBE_BASE = 'https://sp.auth.adobe.com'
_ADOBE_REQUESTOR = 'ESPN'
_ADOBE_REDIRECT = 'https://www.espn.com/watch/'
_ADOBE_TOKEN_MAX_AGE = 5 * 3600     # client tokens last ~6h; the sign-in ~90 days
_ADOBE_SIGNIN_TIMEOUT = 30 * 60
_ADOBE_POLL_SECONDS = 3.0
_ADOBE_POLL_MAX_SECONDS = 15.0
_ANON_TOKEN_TTL = 4 * 3600
_TVE_PLAYBACK_URL = 'https://espn.playback.edge.bamgrid.com/v7/playback/dtc-tve/ctr-regular'
ADOBE_STATUS_KEY = 'espn:adobe:status'

# Source config keys the TV-provider sign-in owns (sign-out drops them all).
ADOBE_CONFIG_KEYS = (
    'adobe_mvpd', 'adobe_mvpd_name', 'adobe_client_id', 'adobe_client_secret',
    'adobe_access_token', 'adobe_token_at', 'adobe_device_fingerprint',
    'adobe_signed_in_at', 'adobe_expires_at',
)


def _jwt_claims(token: str) -> dict:
    try:
        part = token.split('.')[1]
        return json.loads(base64.urlsafe_b64decode(part + '=' * (-len(part) % 4)))
    except (IndexError, ValueError):
        return {}


def _software_statements() -> list[str]:
    """Adobe software statements in espn.com's watch bundle, in page order.
    The bundle carries more than one (checked 2026-09-29), so the caller
    registers with each until Adobe accepts one."""
    s = requests.Session()
    s.headers.update({'User-Agent': _UA})
    html = s.get(_ADOBE_REDIRECT, timeout=30).text
    found: list[str] = []
    for src in re.findall(r'src="([^"]+\.js[^"]*)"', html):
        try:
            text = s.get(urljoin(_ADOBE_REDIRECT, src), timeout=20).text
        except requests.RequestException:
            continue
        for jwt in re.findall(r'eyJhbGciOiJSUzI1NiJ9\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+', text):
            if jwt not in found and _jwt_claims(jwt).get('iss') == 'auth.adobe.com':
                found.append(jwt)
    return found


def _adobe_client(device_fingerprint: str, creds: dict | None = None):
    from .nbc_tve import AdobePassV2Client
    client = AdobePassV2Client(_ADOBE_REQUESTOR, '', _ADOBE_REDIRECT, device_fingerprint, client_creds=creds)
    client.session.headers.update({'Origin': 'https://www.espn.com', 'Referer': 'https://www.espn.com/'})
    return client


def start_adobe_signin(mso_id: str) -> dict:
    """Register an Adobe client and open a sign-in session for `mso_id`.
    Returns {code, url, mso_id, mso_name} plus the client details the waiting
    job needs; `url` is the link the user opens on their own device."""
    fingerprint = uuid.uuid4().hex
    client = _adobe_client(fingerprint)
    statements = _software_statements()
    for statement in statements:
        client.software_statement = statement
        try:
            client._register_client()
            break
        except TVEAuthError:
            continue
    else:
        raise ESPNAuthError(f'Adobe Pass: none of the {len(statements)} settings found on espn.com '
                            'were accepted — ESPN may have changed its site.')

    mvpds: dict[str, str] = {}
    try:
        r = client.session.get(f'{_ADOBE_BASE}/api/v2/{_ADOBE_REQUESTOR}/configuration',
                               headers=client._bearer_headers(), timeout=20)
        if r.ok:
            mvpds = {m.get('id'): m.get('displayName') or m.get('id')
                     for m in ((r.json() or {}).get('requestor') or {}).get('mvpds') or []}
    except (requests.RequestException, ValueError):
        pass
    if mvpds and mso_id not in mvpds:
        raise ESPNAuthError(f'ESPN doesn\'t accept "{mso_id}" as a TV provider.')

    try:
        r = client._post(
            f'{_ADOBE_BASE}/api/v2/{_ADOBE_REQUESTOR}/sessions',
            data={'mvpd': mso_id, 'redirectUrl': _ADOBE_REDIRECT, 'domainName': 'espn.com'},
            headers={**client._bearer_headers(), 'Content-Type': 'application/x-www-form-urlencoded'},
        )
        data = r.json()
    except (TVEAuthError, ValueError) as exc:
        raise ESPNAuthError(f'Adobe Pass: could not start a {mso_id} sign-in: {exc}') from exc
    if data.get('actionName') != 'authenticate' or not data.get('url'):
        raise ESPNAuthError(f'Adobe Pass: unexpected session reply ({data.get("actionName")}).')
    return {
        'code': data.get('code'), 'url': _ADOBE_BASE + data['url'],
        'mso_id': mso_id, 'mso_name': mvpds.get(mso_id) or mso_id,
        'client_id': client.client_id, 'client_secret': client.client_secret,
        'access_token': client.access_token, 'device_fingerprint': fingerprint,
    }


def adobe_status(pending: dict, state: str, message: str = '') -> dict:
    """The status blob the card polls — never carries the client secret."""
    return {'state': state, 'message': message, 'code': pending.get('code'),
            'url': pending.get('url'), 'mso_name': pending.get('mso_name')}


def run_adobe_signin(pending: dict) -> None:
    """RQ job: wait for the user to finish signing in to their TV provider at
    pending['url'], then save the Adobe client onto the espn source. Quits
    quietly once ADOBE_STATUS_KEY names a different code or is gone."""
    import redis

    from app.worker import flask_app
    from ..extensions import db
    from ..models import Source

    with flask_app.app_context():
        r = redis.from_url(flask_app.config['REDIS_URL'])
        code, mso_id = pending['code'], pending['mso_id']

        def superseded() -> bool:
            raw = r.get(ADOBE_STATUS_KEY)
            return not raw or json.loads(raw).get('code') != code

        def set_status(state: str, message: str = '') -> None:
            if not superseded():
                r.setex(ADOBE_STATUS_KEY, _ADOBE_SIGNIN_TIMEOUT + 120,
                        json.dumps(adobe_status(pending, state, message)))

        client = _adobe_client(pending['device_fingerprint'], {
            'client_id': pending['client_id'], 'client_secret': pending['client_secret'],
            'access_token': pending['access_token']})
        client._register_client()   # adopts the cached creds, no network
        url = f'{_ADOBE_BASE}/api/v2/{_ADOBE_REQUESTOR}/profiles/{mso_id}'
        deadline = time.monotonic() + _ADOBE_SIGNIN_TIMEOUT
        delay = _ADOBE_POLL_SECONDS
        profile = None
        while time.monotonic() < deadline:
            if superseded():
                logger.info('[espn] TV-provider sign-in %s',
                            'replaced by a newer one' if r.get(ADOBE_STATUS_KEY) else 'stopped')
                return
            try:
                resp = client.session.get(url, headers=client._bearer_headers(), timeout=20)
                if resp.status_code == 401:
                    client.refresh_access_token()
                elif resp.ok:
                    profile = ((resp.json() or {}).get('profiles') or {}).get(mso_id)
                    if profile:
                        break
            except (requests.RequestException, ValueError, TVEAuthError) as exc:
                logger.info('[espn] TV-provider sign-in poll failed, retrying: %s', exc)
            time.sleep(delay)
            delay = min(delay * 1.3, _ADOBE_POLL_MAX_SECONDS)
        if not profile:
            set_status('expired', 'The sign-in link expired before the sign-in finished.')
            return

        src = Source.query.filter_by(name='espn').first()
        if not src:
            set_status('error', 'ESPN source no longer exists.')
            return
        not_after = profile.get('notAfter')
        cfg = dict(src.config or {})
        cfg.update({
            'adobe_mvpd': mso_id, 'adobe_mvpd_name': pending.get('mso_name') or mso_id,
            'adobe_client_id': client.client_id, 'adobe_client_secret': client.client_secret,
            'adobe_access_token': client.access_token, 'adobe_token_at': int(time.time()),
            'adobe_device_fingerprint': pending['device_fingerprint'],
            'adobe_signed_in_at': int(time.time()),
            'adobe_expires_at': int(not_after / 1000) if isinstance(not_after, (int, float)) else None,
        })
        src.config = cfg
        db.session.commit()
        logger.info('[espn] signed in with TV provider %s', mso_id)
        set_status('success')


# ── scraper ──────────────────────────────────────────────────────────────────

class ESPNScraper(BaseScraper):
    source_name = 'espn'
    display_name = 'ESPN'
    source_category = 'premium'
    is_premium = True
    config_required = True
    scrape_interval = 360
    # The audit is what drops networks this account's plan doesn't include
    # (e.g. MLB Network on plain ESPN+): resolve() raises TVENotAuthorizedError
    # on not-entitled, which the audit records as NotAuthorized and re-checks
    # every run, so a later plan upgrade brings the channel back.
    stream_audit_enabled = True
    # Either sign-in (a tuple = any of these keys).
    audit_requires_config = [('refresh_token', 'adobe_client_id')]
    license_url = _LICENSE_URL
    # Widevine-CENC on every channel — the HLS master looks clear to the generic
    # audit, so bridge from the first scrape rather than waiting for detection.
    all_channels_require_drm_bridge = True

    # Sign-in is the activation-code panel (renderEspnConfig), not a typed field.
    # This hidden entry exists so the Sources page offers "Configure" at all —
    # it only renders that button for sources with a non-empty schema.
    config_schema = [
        ConfigField('refresh_token', 'ESPN session', field_type='password', secret=True, hidden=True),
    ]

    # ── auth ──

    def _access_token(self) -> str:
        if not self.config.get('refresh_token'):
            raise ESPNAuthError('ESPN is not signed in — use the activation code on the Sources page.')
        if (self.config.get('access_token')
                and time.time() < float(self.config.get('access_expires_at') or 0) - _ACCESS_REFRESH_MARGIN):
            return self.config['access_token']
        for key, value in token_config(_refresh_bam_token(self.config['refresh_token'])).items():
            self._update_config(key, value)
        return self.config['access_token']

    def _anon_access_token(self) -> str:
        """Anonymous BAM token for TV-provider playback and its license."""
        if (self.config.get('anon_access_token')
                and time.time() < float(self.config.get('anon_expires_at') or 0) - _ACCESS_REFRESH_MARGIN):
            return self.config['anon_access_token']
        self._update_config('anon_access_token', _anonymous_token())
        self._update_config('anon_expires_at', int(time.time()) + _ANON_TOKEN_TTL)
        return self.config['anon_access_token']

    def _adobe_access_token(self, force: bool = False) -> str:
        """Access token for the Adobe client the TV-provider sign-in belongs to.
        Refreshed for the same client: a new one wouldn't carry the sign-in."""
        if not force and time.time() - float(self.config.get('adobe_token_at') or 0) < _ADOBE_TOKEN_MAX_AGE:
            return self.config['adobe_access_token']
        try:
            token = refresh_adobe_client_token(self.config['adobe_client_id'], self.config['adobe_client_secret'])
        except TVEAuthError as exc:
            raise ESPNAuthError(f'ESPN: could not renew the TV-provider sign-in: {exc}') from exc
        self._update_config('adobe_access_token', token)
        self._update_config('adobe_token_at', int(time.time()))
        return token

    # ── channels / EPG ──

    def _network_logos(self) -> dict[str, str]:
        # ESPN's own small (≈100px wide) logos — the ones its web player uses.
        # Gracenote-routed channels get Gracenote's station logo instead.
        try:
            r = self.session.post(_WATCH_API, params={'apiKey': _WATCH_API_KEY},
                                  headers={'User-Agent': _UA, 'Origin': 'https://www.espn.com'},
                                  json={'query': '{ networks(countryCode:"us"){ adobeResource type image { url } } }'},
                                  timeout=20)
            r.raise_for_status()
            return {n['adobeResource']: (n.get('image') or {}).get('url')
                    for n in (r.json().get('data') or {}).get('networks') or []
                    if n.get('type') == 'LINEAR' and n.get('adobeResource')}
        except Exception as exc:  # noqa: BLE001
            logger.warning('[espn] network logos unavailable: %s', exc)
            return {}

    def fetch_channels(self) -> list[ChannelData]:
        logos = self._network_logos()
        return [
            ChannelData(
                source_channel_id=network_id,
                name=meta['name'],
                stream_url=SCHEME + network_id,
                logo_url=meta.get('logo') or logos.get(network_id),
                slug=f'espn-{network_id}',
                category='Sports',
                language=meta.get('language', 'en'),
                stream_type='hls',
                guide_key=network_id,
                gracenote_id=resolve_gracenote('espn', upstream_id=meta.get('gracenote'), lookup_key=network_id),
            )
            for network_id, meta in NETWORKS.items()
        ]

    def _airings(self, day: str, tz: str = 'UTC') -> list[dict]:
        r = self.session.post(_WATCH_API, params={'apiKey': _WATCH_API_KEY, 'features': 'pbov7'},
                              headers={'User-Agent': _UA, 'Origin': 'https://www.espn.com',
                                       'Referer': 'https://www.espn.com/'},
                              json={'query': _Q_AIRINGS, 'variables': {'day': day, 'tz': tz}}, timeout=30)
        r.raise_for_status()
        return ((r.json().get('data') or {}).get('airings')) or []

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {ch.source_channel_id for ch in channels}
        today = datetime.now(timezone.utc).date()
        seen: set[str] = set()
        programs: list[ProgramData] = []
        for offset in range(-1, _EPG_DAYS):
            day = (today + timedelta(days=offset)).isoformat()
            try:
                airings = self._airings(day)
            except Exception as exc:  # noqa: BLE001
                logger.warning('[espn] airings for %s failed: %s', day, exc)
                continue
            for a in airings:
                cid = (a.get('network') or {}).get('id')
                if cid not in wanted or a['id'] in seen:
                    continue
                seen.add(a['id'])
                start, end = _parse_time(a.get('startDateTime')), _parse_time(a.get('endDateTime'))
                if not start or not end or end <= start:
                    continue
                league = (a.get('league') or {}).get('name')
                programs.append(ProgramData(
                    source_channel_id=cid,
                    title=a.get('name') or a.get('shortName') or 'ESPN',
                    start_time=start,
                    end_time=end,
                    description=a.get('description') or None,
                    poster_url=(a.get('image') or {}).get('url'),
                    category=(a.get('sport') or {}).get('name') or 'Sports',
                    episode_title=league if league and league != a.get('name') else None,
                    is_live=a.get('type') == 'LIVE' and not a.get('isReAir'),
                    episode_id=a['id'],
                ))
        return programs

    # ── playback ──

    def _current_airing(self, network_id: str) -> dict:
        now = datetime.now(timezone.utc)
        today = now.date()
        fallback = None
        for day in (today, today - timedelta(days=1)):
            for a in self._airings(day.isoformat()):
                if (a.get('network') or {}).get('id') != network_id:
                    continue
                pid = (a.get('source') or {}).get('playbackId')
                start, end = _parse_time(a.get('startDateTime')), _parse_time(a.get('endDateTime'))
                if pid and start and end and start <= now < end:
                    return a
                # Any airing's playbackId carries the network's stable channelId,
                # so a neighbouring one still tunes the linear feed if the guide
                # has a gap right now.
                if pid and not fallback:
                    fallback = a
        if fallback:
            return fallback
        raise RuntimeError(f'ESPN: no airing found for {network_id}')

    def _mint(self, network_id: str) -> dict:
        airing = self._current_airing(network_id)
        playback_id = airing['source']['playbackId']
        tve = bool(self.config.get('adobe_client_id')) and NETWORKS.get(network_id, {}).get('tve')
        if self.config.get('refresh_token'):
            try:
                return self._mint_playback(playback_id, network_id)
            except (TVENotAuthorizedError, ESPNAuthError):
                if not tve:
                    raise
                # The account's plan doesn't cover it (or its session lapsed):
                # the TV-provider sign-in may still.
        if tve:
            return self._mint_tve(playback_id, airing, network_id)
        if self.config.get('adobe_client_id'):
            raise TVENotAuthorizedError(f'ESPN: {NETWORKS.get(network_id, {}).get("name", network_id)} '
                                        'needs an ESPN plan; a TV-provider sign-in doesn\'t include it')
        raise ESPNAuthError('ESPN is not signed in — use the Sources page to sign in.')

    # Groundwork for ESPN+ events played on behalf of an external lane planner
    # (FruitDeepLinks / ESPN4CC4C): nothing routes here yet. The stream is
    # cached under 'airing:<id>' so /play/espn/license?channel_id=airing:<id>
    # finds its rights context and license route. At the real end of an event
    # ESPN appends #EXT-X-ENDLIST (often well after the scheduled endDateTime),
    # so whatever tunes lanes has to re-tune on that, not on the schedule.
    def resolve_airing(self, airing_id: str) -> str:
        r = self.session.post(_WATCH_API, params={'apiKey': _WATCH_API_KEY, 'features': 'pbov7'},
                              headers={'User-Agent': _UA, 'Origin': 'https://www.espn.com'},
                              json={'query': 'query($id:ID!){ airing(id:$id, countryCode:"us", deviceType:SETTOP, tz:"UTC"){ source{ playbackId } } }',
                                    'variables': {'id': airing_id}}, timeout=20)
        pid = ((((r.json().get('data') or {}).get('airing') or {}).get('source')) or {}).get('playbackId')
        if not pid:
            raise RuntimeError(f'ESPN: no playbackId for airing {airing_id}')
        # Events use the non-channel license route; /channel/ 400s with
        # content-key.invalid-linear-key for them (confirmed 2026-09-26).
        entry = self._mint_playback(pid, f'airing {airing_id}')
        entry['license_url'] = _EVENT_LICENSE_URL
        streams = dict(self.cache.get('espn_streams') or {})
        streams[f'airing:{airing_id}'] = entry
        self._update_cache('espn_streams', streams)
        return entry['manifest_url']

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        entry = (config.get('espn_streams') or {}).get(channel_id or '') or {}
        return entry.get('license_url') or cls.license_url

    @staticmethod
    def _playback_body(playback_id: str) -> dict:
        return {
            'playbackId': playback_id,
            'playback': {
                'attributes': {
                    'resolution': {'max': ['1920x1080']}, 'protocol': 'HTTPS',
                    'assetInsertionStrategies': {'point': 'SGAI', 'range': 'SGAI'},
                    'playbackInitiationContext': 'ONLINE', 'frameRates': [60],
                    'videoSegmentTypes': ['FMP4'], 'encryption': ['ctr'],
                    'codecs': {'video': ['h.264'], 'audio': [{'name': 'aac', 'muxed': False}]},
                    'maxSlideDuration': '4_HOUR', 'supportsPlaylistFiltering': True, 'iframe': False,
                },
                'adTracking': {'limitAdTrackingEnabled': 'NOT_SUPPORTED',
                               'deviceAdId': '00000000-0000-0000-0000-000000000000',
                               'privacyOptOut': 'NO', 'additionalConsent': ''},
                'tracking': {'playbackSessionId': str(uuid.uuid4())},
            },
            'allowedCreatives': [],
            'targeting': {'device': {'deviceOsName': 'windows', 'playerFrameworkName': 'HiVE-DMP'}},
        }

    def _post_playback(self, url: str, body: dict, token: str) -> requests.Response:
        return self.session.post(url, json=body, timeout=20, headers={
            **_BAM_HEADERS, 'Authorization': 'Bearer ' + token,
            'Content-Type': 'application/json', 'Accept': 'application/vnd.media-service+json',
            'x-dss-edge-accept': 'vnd.dss.edge+json; version=2', 'x-dss-feature-filtering': 'true',
            'x-request-id': str(uuid.uuid4()),
        })

    def _mint_playback(self, playback_id: str, label: str) -> dict:
        access_token = self._access_token()
        r = self._post_playback(_PLAYBACK_URL, self._playback_body(playback_id), access_token)
        return self._parse_playback(r, label, 'account')

    def _mint_tve(self, playback_id: str, airing: dict, label: str) -> dict:
        """Mint through the TV-provider sign-in: anonymous BAM token + tveAuth."""
        anon = self._anon_access_token()
        resource = ("<rss version='2.0' xmlns:media='http://search.yahoo.com/mrss/'><channel>"
                    f"<title>{escape(label)}</title><item><title>{escape(airing.get('name') or '')}</title>"
                    f"<guid>{escape(airing.get('id') or '')}</guid>"
                    "<media:rating scheme='urn:v-chip'></media:rating></item></channel></rss>")
        fingerprint = base64.b64encode(self.config['adobe_device_fingerprint'].encode()).decode()
        for attempt in range(2):
            body = {**self._playback_body(playback_id), 'tveAuth': {
                'tokenType': 'ADOBE', 'resource': resource,
                'accessToken': self._adobe_access_token(force=attempt > 0),
                'deviceIdentifier': 'fingerprint ' + fingerprint, 'mvpd': self.config['adobe_mvpd'],
            }}
            r = self._post_playback(_TVE_PLAYBACK_URL, body, anon)
            # 401 adobe-pass-unauthorized: usually just a stale client token.
            rejected = r.status_code == 401 and _error(r).get('code') == 'adobe-pass-unauthorized'
            if not rejected:
                break
        if rejected:
            message = 'ESPN: the TV-provider sign-in was rejected — sign in again on the Sources page.'
            from ..tve.signin_notice import mark_signin_needed
            mark_signin_needed('espn', message)
            raise ESPNAuthError(message)
        return self._parse_playback(r, label, 'anon')

    def _parse_playback(self, r: requests.Response, label: str, license_auth: str) -> dict:
        try:
            data = r.json() if r.content else {}
        except ValueError:
            data = {}
        stream = data.get('stream') or {}
        sources = sorted(stream.get('sources') or [], key=lambda s: s.get('priority') or 99)
        manifest_url = next((((s.get('slide') or s.get('complete')) or {}).get('url')
                             for s in sources if (s.get('slide') or s.get('complete'))), None)
        ctx = (stream.get('playbackRights') or {}).get('playbackRightsContext')
        err = (data.get('errors') or [{}])[0]
        if r.status_code == 403 and err.get('code') in ('not-entitled', 'adobe-pass-failed-authorization'):
            raise TVENotAuthorizedError(f'ESPN: this account is not entitled to {NETWORKS.get(label, {}).get("name", label)}')
        if not r.ok or not manifest_url or not ctx:
            raise RuntimeError(f'ESPN playback {label}: HTTP {r.status_code} '
                               f'{err.get("code", "")} {err.get("description", "")}'.strip())
        # The license must carry the same kind of BAM token the stream was
        # minted with; prepare_license_request reads the current one.
        return {'manifest_url': manifest_url, 'rights_ctx': ctx, 'license_auth': license_auth,
                'cached_at': time.time()}

    def resolve(self, raw_url: str) -> str:
        network_id = raw_url.removeprefix(SCHEME)
        if network_id not in NETWORKS:
            raise ValueError(f'Unsupported ESPN stream URL: {raw_url}')
        streams = dict(self.cache.get('espn_streams') or {})
        cached = streams.get(network_id)
        if cached and time.time() - float(cached.get('cached_at', 0)) < _STREAM_CACHE_TTL:
            return cached['manifest_url']
        streams[network_id] = self._mint(network_id)
        self._update_cache('espn_streams', streams)
        return streams[network_id]['manifest_url']

    def audit_resolve(self, raw_url: str) -> str:
        # Validate entitlement, then hand back the opaque URL so the audit skips
        # manifest inspection (see warner_tve.py — the CENC markers live in the
        # variant playlists, so the master reads as clear HLS).
        self.resolve(raw_url)
        return raw_url

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        # No "Bearer " prefix on this call, unlike every other BAM request.
        headers = {**_BAM_HEADERS, 'Content-Type': 'application/octet-stream'}
        entry = (config.get('espn_streams') or {}).get(channel_id or '') or {}
        token = config.get('anon_access_token' if entry.get('license_auth') == 'anon' else 'access_token')
        if token:
            headers['Authorization'] = token
        if entry.get('rights_ctx'):
            headers['x-playback-rights-authorization'] = entry['rights_ctx']
        return challenge, headers


def _error(r: requests.Response) -> dict:
    try:
        return ((r.json() or {}).get('errors') or [{}])[0]
    except (ValueError, AttributeError):
        return {}


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None

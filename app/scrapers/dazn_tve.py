"""
dazn_tve.py — DAZN's Live TV channels: the free ones every DAZN account gets
(DAZN 1 USA, Rally TV, Red Bull TV, Unbeaten, Fubo Sports) plus YES and MSG
for a TV provider that carries them (DAZN took over the Gotham app's MSG+ and
YES TV Everywhere). Scoped 2026-10-18 from HARs of the web app
(dev/hitlist/hars/dazn*.har) and DAZN's own TV app (tv.dazn.com).

Key facts established by probing:

- DAZN sign-in uses the TV app's "sign in with your phone" flow. POST
  second-screen.ar.indazn.com/v2/subscribe/qr {brand, signIn, country,
  language, deviceId, qrOptions, oemTrackingDetails{platform, manufacturer}}
  → {secondScreenId}; the user opens dazn.com/signin/device/<id> where
  they're signed in, and GET .../v1/subscribed/<id> answers 204 until then
  and 200 {token} after. The token is a ~2-hour JWT; POST
  ott-authz-bff-prod/v5/RefreshAccessToken {DeviceId} with it as Bearer
  returns a fresh one.
- The TV-provider link is per DAZN device. Adobe Pass REST v2 under the
  Gotham requestor (resources MSGGO, YESN): DAZN's getAllTveProviders hands
  out the software statement; register → client token → sessions (the
  sign-in link) → profiles/<mvpd> (userID plus an encrypted zip). Then
  partner-integration-service/initiateMVPDLogin with those returns a DAZN
  token whose productStatus.YESMSG is ActivePaid and whose `geo` claim names
  the zones the user's RSNs are in (yes_national out of market).
- Channel list: the public Livetvschedule rail, filtered by those zones and
  by each tile's EntitlementIds against the token's entitlement sets. A free
  account plays only the tiles with none (Unbeaten); DAZN 1 USA, Red Bull TV
  and Fubo Sports need base_dazn_content (paid DAZN), Rally TV its own tier,
  YES e_r_yes_nt/e_r_yes_ya — the rail lists them all regardless.
- Playback: api.playback.indazn.com/v5/Playback → per-CDN manifest URLs,
  a 24-hour CDN token and a self-authorizing Widevine license URL. Both the
  Playback API and the license server sit behind a bot filter that only
  passes a real Chrome TLS fingerprint (curl_cffi), and the CDN token is bound
  to the user agent that asked for it, so the manifest and segments are
  relayed by the play routes with that same user agent (play.dazn_tve_*).
  Widevine, so the channels are bridged through FastChannels Player.
- Guide: only Now and Next, from each rail tile's LinearSchedule (DAZN's EPG
  service lists the channels as one never-ending block); hourly placeholders
  after that.
"""
from __future__ import annotations

import base64
import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import requests

from .base import BaseScraper, ChannelData, ProgramData
from ..gracenote_map import resolve_gracenote
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import TVEAuthError, TVENotAuthorizedError

logger = logging.getLogger(__name__)

SCHEME = 'dazn://'
NETWORK_KEY = 'dazn'
# The bot filter in front of the Playback API and license server wants a real
# Chrome TLS handshake whose user agent matches it; the CDN token is then
# bound to that user agent. Pin both, so the relay can send the same one.
IMPERSONATE = 'chrome131'
UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
SITE = 'https://www.dazn.com'
SECOND_SCREEN = 'https://second-screen.ar.indazn.com'
DEVICE_LINK_URL = f'{SITE}/signin/device/{{}}'
REFRESH_URL = 'https://ott-authz-bff-prod.ar.indazn.com/v5/RefreshAccessToken'
PARTNER = 'https://partner-integration-service.ar.indazn.com/v1/adobetve'
RAIL_URL = 'https://rail-router.discovery.indazn.com/ca/v11/Rail'
PLAYBACK_URL = 'https://api.playback.indazn.com/v5/Playback'
ADOBE_BASE = 'https://sp.auth.adobe.com'
REQUESTOR_ID = 'Gotham'
DOMAIN_NAME = 'dazn.com'
REDIRECT_URL = (f'{SITE}/tve/adobetve/login?requestorId={REQUESTOR_ID}&resetTveState=false'
                '&tveAuthStatus=Authenticated#tveAuthFlow=tokenRetrieval')
_LICENSE_URL = 'https://license.drm.indazn.com/widevine/v1/license'
_IMAGE = 'https://image.discovery.indazn.com/ca/v3/{kind}/none/{id}/{fit}/center/center/none/80/400/300/png/image?brand=dazn'
_APP_VERSION = '0.150.1-hotfix.f8b078689'
# The zones every account sees the free channels under.
_BASE_ZONES = ('cle_cv',)
# Refresh the DAZN token this long before it expires.
_TOKEN_MARGIN = 20 * 60
# Reuse a resolved stream until this close to its CDN token's expiry.
_STREAM_MARGIN = 30 * 60
_PREFERRED_CDN = 'dcj-ak-live'


def claims(token: str) -> dict:
    try:
        return json.loads(base64.urlsafe_b64decode(token.split('.')[1] + '=='))
    except (IndexError, ValueError):
        return {}


def dazn_headers(token: str) -> dict:
    return {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json', 'Origin': SITE,
            'Referer': SITE + '/', 'x-brand': 'DAZN', 'x-daznid': claims(token).get('user', ''), 'User-Agent': UA}


def new_device_id() -> str:
    return str(uuid.uuid4())


def request_device_link(device_id: str) -> dict:
    """Start a DAZN "sign in with your phone" pairing → {second_screen_id, url}."""
    r = requests.post(f'{SECOND_SCREEN}/v2/subscribe/qr', timeout=20, headers={
        'Content-Type': 'application/json', 'x-brand': 'DAZN', 'User-Agent': UA}, json={
        'brand': 'dazn', 'signIn': True, 'country': 'US', 'language': 'en', 'deviceId': device_id,
        'qrOptions': {'width': 345, 'margin': 0},
        'oemTrackingDetails': {'platform': 'androidtv', 'manufacturer': 'google'}})
    sid = _json(r).get('secondScreenId')
    if not r.ok or not sid:
        raise TVEAuthError(f'DAZN: could not start the sign-in (HTTP {r.status_code} {r.text[:120]})')
    return {'second_screen_id': sid, 'url': DEVICE_LINK_URL.format(sid)}


def poll_device_link(second_screen_id: str) -> str | None:
    """The DAZN token once the user has approved the pairing, else None."""
    r = requests.get(f'{SECOND_SCREEN}/v1/subscribed/{second_screen_id}', timeout=20,
                     headers={'x-brand': 'DAZN', 'User-Agent': UA})
    if r.status_code == 200:
        return _json(r).get('token') or None
    return None


def refresh_token(token: str, device_id: str) -> str:
    r = requests.post(REFRESH_URL, json={'DeviceId': device_id}, timeout=20,
                      headers={**dazn_headers(token), 'x-brand': 'DAZN'})
    new = ((_json(r).get('AuthToken') or {}).get('Token')) if r.ok else None
    if not new:
        raise TVEAuthError(f'DAZN: the sign-in could not be renewed (HTTP {r.status_code}).')
    return new


# ── TV provider (Adobe, requestor Gotham) ────────────────────────────────────

def _device_info() -> str:
    return base64.b64encode(json.dumps({
        'osName': 'web', 'version': UA[8:], 'model': UA, 'os': 'MacIntel', 'browser': UA,
        'applicationId': REQUESTOR_ID}).encode()).decode()


def adobe_device_identifier(device_id: str) -> str:
    return 'fingerprint ' + base64.b64encode(device_id.encode()).decode()


def adobe_headers(access_token: str, device_id: str) -> dict:
    return {'Authorization': f'Bearer {access_token}', 'AP-Device-Identifier': adobe_device_identifier(device_id),
            'X-Device-Info': _device_info(), 'Origin': SITE, 'User-Agent': UA, 'Accept': 'application/json'}


def register_adobe_client(dazn_token: str) -> dict:
    """DAZN's Gotham software statement → a registered Adobe client
    {client_id, client_secret, access_token, resources}."""
    r = requests.get(f'{PARTNER}/getAllTveProviders', params={'requestorId': REQUESTOR_ID},
                     headers=dazn_headers(dazn_token), timeout=20)
    details = _json(r).get('adobetveDetails') or {}
    if not r.ok or not details.get('softwareStatement'):
        raise TVEAuthError(f'DAZN: could not read its TV provider settings (HTTP {r.status_code}).')
    reg = requests.post(f'{ADOBE_BASE}/o/client/register', json={'software_statement': details['softwareStatement']},
                        timeout=20, headers={'Content-Type': 'application/json', 'X-Device-Info': _device_info(),
                                             'Origin': SITE, 'User-Agent': UA})
    reg_data = _json(reg)
    if not reg.ok or not reg_data.get('client_id'):
        raise TVEAuthError(f'DAZN: Adobe client registration failed (HTTP {reg.status_code}).')
    client = {'client_id': reg_data['client_id'], 'client_secret': reg_data['client_secret'],
              'resources': details.get('resourceIds') or ['MSGGO', 'YESN']}
    client['access_token'] = adobe_client_token(client)
    return client


def adobe_client_token(client: dict) -> str:
    r = requests.post(f'{ADOBE_BASE}/o/client/token', timeout=20, data={
        'grant_type': 'client_credentials', 'client_id': client['client_id'], 'client_secret': client['client_secret']})
    token = _json(r).get('access_token')
    if not r.ok or not token:
        raise TVEAuthError(f'DAZN: Adobe client token request failed (HTTP {r.status_code}).')
    return token


def link_tv_provider(dazn_token: str, device_id: str, client: dict, mvpd: str, profile: dict) -> str:
    """Attach a finished Adobe sign-in to the DAZN device → the new DAZN token."""
    attrs = profile.get('attributes') or {}

    def attr(name):
        value = attrs.get(name)
        return (value.get('value') if isinstance(value, dict) else value) or ''

    body = {'deviceId': device_id, 'platform': 'web', 'partnerData': {
        'accessToken': client['access_token'], 'requestorId': REQUESTOR_ID, 'userId': claims(dazn_token).get('user', ''),
        'tveUserId': attr('userID'), 'provider': mvpd, 'deviceIdentifier': adobe_device_identifier(device_id),
        'encryptedZip': attr('zip'), 'deviceInfo': _device_info(), 'resources': client['resources'],
        'vlUserAgent': UA, 'clientId': client['client_id'], 'clientSecret': client['client_secret']}}
    r = requests.post(f'{PARTNER}/initiateMVPDLogin', json=body, headers=dazn_headers(dazn_token), timeout=30)
    token = _json(r).get('daznAccessToken')
    if not r.ok or not token:
        raise TVEAuthError(f'DAZN: linking the TV provider failed (HTTP {r.status_code} {r.text[:120]})')
    return token


def rsn_status(token: str) -> str | None:
    """DAZN's YES/MSG entitlement on a token: 'ActivePaid' once a TV provider
    that carries them is linked."""
    return (claims(token).get('productStatus') or {}).get('YESMSG')


def entitlements(token: str) -> set[str]:
    """Every entitlement in a token's entitlement sets."""
    sets = (claims(token).get('entitlements') or {}).get('entitlementSets') or []
    return {e for s in sets for e in (s.get('entitlements') or [])}


def zones(token: str) -> list[str]:
    """The content zones a token's `geo` claim puts the user in."""
    found = list(_BASE_ZONES)
    for info in (claims(token).get('geo') or {}).values():
        for zone in (info or {}).get('z') or []:
            if zone not in found:
                found.append(zone)
    return found


class DaznTVEScraper(BaseScraper):
    """DAZN Live TV: the free channels plus YES/MSG through a TV provider."""

    source_name = 'dazn_tve'
    display_name = 'DAZN'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 90  # keeps the 2-hour DAZN token renewed
    # Every Playback call takes one of the account's concurrent-stream slots.
    stream_audit_enabled = False
    license_url = _LICENSE_URL
    all_channels_require_drm_bridge = True

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({'User-Agent': UA, 'Origin': SITE, 'Referer': SITE + '/'})

    # ── session ──────────────────────────────────────────────────────────────

    def _auth(self) -> dict:
        account = tve_account_for(self.source_name)
        auth = ((account.config or {}).get('dazn_auth') or {}) if account else {}
        if not auth.get('token') or not auth.get('device_id'):
            raise self._sign_in_needed(
                'DAZN: not signed in yet — use "Sign in" for DAZN under Settings → TV Everywhere.')
        return auth

    def _token(self) -> tuple[str, str]:
        """(DAZN token, device id), renewed when close to expiry. The renewed
        token lives in the source cache; a newer sign-in on the account wins."""
        auth = self._auth()
        cached = self.cache.get('dazn_session') or {}
        token = auth['token']
        if cached.get('device_id') == auth['device_id'] and float(cached.get('signed_in_at') or 0) >= float(
                auth.get('captured_at') or 0) and cached.get('token'):
            token = cached['token']
        if float(claims(token).get('exp') or 0) - time.time() > _TOKEN_MARGIN:
            return token, auth['device_id']
        try:
            token = refresh_token(token, auth['device_id'])
        except TVEAuthError as exc:
            raise self._sign_in_needed(f'{exc} Sign in to DAZN again under Settings → TV Everywhere.') from exc
        self._update_cache('dazn_session', {'token': token, 'device_id': auth['device_id'],
                                            'signed_in_at': auth.get('captured_at') or 0})
        return token, auth['device_id']

    # ── channels + guide ─────────────────────────────────────────────────────

    def _tiles(self, token: str) -> list[dict]:
        """The Live TV rail's tiles this token can play. Each tile names the
        entitlements it needs (e.g. base_dazn_content for a paid DAZN plan,
        e_r_yes_nt for YES); "a_a" is on every tile and isn't one."""
        r = self.session.get(RAIL_URL, timeout=20, params={
            'platform': 'web', 'id': 'Livetvschedule', 'country': 'us', 'brand': 'dazn', 'languageCode': 'en',
            'zones': ','.join(zones(token))})
        r.raise_for_status()
        held = entitlements(token)
        tiles = []
        for tile in (r.json() or {}).get('Tiles') or []:
            needs = set(tile.get('EntitlementIds') or []) - {'a_a'}
            if needs and not needs & held:
                logger.info('[dazn_tve] skipping %s — needs %s', tile.get('Title'), ', '.join(sorted(needs)))
                continue
            tiles.append(tile)
        return tiles

    def fetch_channels(self) -> list[ChannelData]:
        token, _ = self._token()
        out = []
        for tile in self._tiles(token):
            asset = tile.get('AssetId')
            name = (tile.get('Title') or '').split(':')[0].strip()
            if not asset or not name:
                continue
            logo = (tile.get('LogoImage') or {}).get('Id')
            header = (tile.get('Image') or {}).get('Id')
            out.append(ChannelData(
                source_channel_id=asset,
                name=name,
                slug='dazn-' + name.lower().replace(' ', '-'),
                logo_url=(_IMAGE.format(kind='linear-channel', id=logo, fit='contain') if logo else
                          _IMAGE.format(kind='v3', id=header, fit='fill') if header else None),
                stream_url=f'{SCHEME}{asset}',
                stream_type='dash',
                category='Sports',
                language='en',
                country='US',
                description=(tile.get('Description') or '').strip() or None,
                guide_key=f'DAZN:{asset}',
                gracenote_id=resolve_gracenote('dazn_tve', lookup_key=asset),
            ))
        if rsn_status(token) != 'ActivePaid':
            logger.info('[dazn_tve] no TV provider linked — free channels only')
        logger.info('[dazn_tve] %d channels (zones %s)', len(out), ','.join(zones(token)))
        return out

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        # The rail carries each channel's Now and Next airing; DAZN publishes
        # nothing further ahead, so the rest of the window gets hourly blocks
        # named after the channel. Rescrapes (every 90 minutes) roll it on.
        token, _ = self._token()
        schedule = {t.get('AssetId'): t.get('LinearSchedule') or {} for t in self._tiles(token)}
        now = datetime.now(timezone.utc)
        programs = []
        for ch in channels:
            sid = ch.source_channel_id
            real = [p for p in (_airing(sid, (schedule.get(sid) or {}).get(k)) for k in ('Now', 'Next')) if p]
            programs.extend(real)
            fill_from = max([p.end_time for p in real] or [now.replace(minute=0, second=0, microsecond=0)])
            hour = fill_from
            while hour < now + timedelta(hours=48):
                nxt = hour.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                programs.append(ProgramData(source_channel_id=sid, title=ch.name, start_time=hour, end_time=nxt,
                                            description=getattr(ch, 'description', None), category='Sports'))
                hour = nxt
        return programs

    # ── playback ─────────────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        """The channel's tokenized manifest URL. Only the play routes fetch
        it, with UA, since the CDN token is bound to that user agent."""
        return self.resolve_stream(raw_url)['manifest']

    def resolve_stream(self, raw_url: str) -> dict:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported DAZN stream URL: {raw_url}')
        asset = raw_url[len(SCHEME):].strip('/')
        streams = dict(self.cache.get('dazn_streams') or {})
        cached = streams.get(asset)
        if cached and float(cached.get('exp') or 0) - time.time() > _STREAM_MARGIN:
            return cached
        token, device_id = self._token()
        user = claims(token).get('user', '')
        from curl_cffi import requests as cffi_requests
        r = cffi_requests.get(PLAYBACK_URL, impersonate=IMPERSONATE, timeout=20, params={
            'AppVersion': _APP_VERSION, 'DrmType': 'WIDEVINE', 'Format': 'MPEG-DASH',
            'PlayerId': '@dazn/peng-html5-core/web/web', 'Platform': 'web', 'Model': 'unknown', 'Secure': 'true',
            'Manufacturer': 'microsoft', 'PlayReadyInitiator': 'false', 'Capabilities': 'mta', 'AssetId': asset,
            'MtaLanguageCode': '', 'LanguageCode': 'en',
            'SessionId': f'{int(time.time() * 1000)}-{user.split("-")[-1]}-{asset}-{uuid.uuid4().hex[:6].upper()}'},
            headers={'Authorization': f'Bearer {token}', 'Origin': SITE, 'Referer': SITE + '/', 'x-brand': 'DAZN',
                     'x-correlation-id': str(uuid.uuid4()), 'x-dazn-device': device_id, 'x-daznid': user})
        try:
            data = r.json()
        except ValueError:
            data = {}
        details = (data or {}).get('PlaybackDetails') or []
        if r.status_code != 200 or not details:
            reason = str((data or {}).get('odata.error') or (data or {}).get('message') or r.text[:120])
            if r.status_code in (401, 403) and 'ntitle' in reason:
                raise TVENotAuthorizedError(f'DAZN: this account isn\'t entitled to that channel ({reason[:80]}).')
            raise RuntimeError(f'DAZN: playback refused: HTTP {r.status_code} {reason[:160]}')
        pick = next((d for d in details if d.get('CdnName') == _PREFERRED_CDN), details[0])
        cdn = pick.get('CdnToken') or {}
        manifest = pick['ManifestUrl'] + ('&' if '?' in pick['ManifestUrl'] else '?') + \
            f"{cdn.get('Name', 'dazn-token')}={quote(cdn.get('Value', ''), safe='')}"
        entry = {'manifest': manifest, 'la_url': pick.get('LaUrl') or _LICENSE_URL, 'user': user,
                 'token_param': f"{cdn.get('Name', 'dazn-token')}={quote(cdn.get('Value', ''), safe='')}",
                 'exp': time.time() + float(cdn.get('ExpirySeconds') or 3600)}
        streams[asset] = entry
        self._update_cache('dazn_streams', streams)
        return entry

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        return ((config.get('dazn_streams') or {}).get(channel_id or '') or {}).get('la_url') or cls.license_url

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        user = ((config.get('dazn_streams') or {}).get(channel_id or '') or {}).get('user', '')
        headers = {'Content-Type': 'application/octet-stream', 'Origin': SITE, 'Referer': SITE + '/',
                   'x-brand': 'DAZN'}
        if user:
            headers['x-daznid'] = user
        return challenge, headers

    @classmethod
    def post_license_request(cls, license_url: str, body: bytes, headers: dict, timeout: int = 15):
        # The license server is behind the same Chrome-only bot filter.
        from curl_cffi import requests as cffi_requests
        return cffi_requests.post(license_url, data=body, headers=headers, impersonate=IMPERSONATE, timeout=timeout)

    @classmethod
    def fetch_service_certificate(cls, config: dict, timeout: int = 15) -> bytes | None:
        streams = config.get('dazn_streams') or {}
        la_url = next((s.get('la_url') for s in streams.values() if s.get('la_url')), None)
        if not la_url:
            return None
        r = cls.post_license_request(la_url, b'\x08\x04', {'Content-Type': 'application/octet-stream',
                                                           'Origin': SITE, 'x-brand': 'DAZN'}, timeout=timeout)
        return r.content if r.status_code == 200 and r.content[:2] == b'\x08\x05' else None

    @staticmethod
    def _sign_in_needed(message: str) -> TVEAuthError:
        from ..tve.signin_notice import mark_signin_needed
        mark_signin_needed(NETWORK_KEY, message)
        return TVEAuthError(message)


def _airing(source_channel_id: str, item: dict | None) -> ProgramData | None:
    """One LinearSchedule Now/Next entry."""
    if not item:
        return None
    try:
        start = datetime.fromisoformat(item['Start'].replace('Z', '+00:00'))
        end = datetime.fromisoformat(item['End'].replace('Z', '+00:00'))
    except (KeyError, TypeError, ValueError):
        return None
    title = (item.get('Title') or '').strip()
    if not title or end <= start:
        return None
    genre = next((g.get('name') for g in item.get('Genre') or [] if g.get('name')), None)
    return ProgramData(source_channel_id=source_channel_id, title=title, start_time=start, end_time=end,
                       episode_title=(item.get('EpisodeTitle') or '').strip() or None,
                       description=(item.get('Description') or '').strip() or None,
                       category=genre or 'Sports')


def _json(r) -> dict:
    try:
        body = r.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}

"""
univision_tve.py — TelevisaUnivision's channels from tv.univision.com
(Univision Now TVE): Univision, UniMás, Galavisión, TUDN, plus the free
N+ Univision 24/7, Zona TUDN and Canal ViX. Scoped 2026-10-17 —
dev/univision/SCOPED.md has the full trace.

Key facts established by probing:

- tv.univision.com is a Quickplay ("unowtve-web") app. Its launch config
  (config-service-cdn...univisionnow.com/launch/config) lists every service
  URL; the Adobe software statement is in the app bundle (main.*.js,
  `adobeSoftwareStatement`).
- Every channel is clear: the catalog's delivery profiles say `dp: none` for
  both HLS and DASH (Google DAI ad insertion, no DRM).
- Entitlement is per channel package: `urn:package:tve:free` (N+ 24/7, Zona
  TUDN, Canal ViX) plays for a guest; univision / galavision / deportes need a
  TV-provider sign-in through Adobe Pass v2, requestor `univision` (one
  phone-link sign-in covers all three resources).
- Playback is Quickplay's content authorizer: an app OAuth token
  (client_credentials, public web client), a "QPAT" access token — guest, or
  for a signed-in user `{userId: Adobe profile userID, deviceId, deviceInfo}`
  posted to the Adobe QPAT service — a registered device whose `secret` signs
  a 30-second HS256 X-Device-Id JWT, then POST /media/content/authorize →
  `contentUrl` (Unified Streaming HLS, no token in the URL).
- Guide: data-store-cdn's /content/epg, public, one call per day for all
  channels, with Gracenote program ids.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import requests

from .base import BaseScraper, ChannelData, ProgramData
from ..gracenote_map import resolve_gracenote
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import MvpdCooldownMixin, TVEAuthError, TVENotAuthorizedError

logger = logging.getLogger(__name__)

SCHEME = 'univision-tve://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
ORIGIN = 'https://tv.univision.com'
ADOBE_BASE = 'https://sp.auth.adobe.com'
REQUESTOR_ID = 'univision'
# What tv.univision.com sends Adobe as both redirectUrl and domainName.
REDIRECT_URL = 'https://tv.univision.com/'
RESOURCES = ('univision', 'galavision', 'deportes')
NETWORK_KEY = 'univision'

CLIENT = 'unow-unowtve-web'
LAUNCH_CONFIG = f'https://config-service-cdn.api-cms.prd.univisionnow.com/launch/config?device=web&client={CLIENT}'
EPG_URL = 'https://data-store-cdn.api-cms.prd.univisionnow.com/content/epg'
CHANNEL_URL = 'https://data-store-cdn.api-cms.prd.univisionnow.com/content/urn/resource/catalog/channel/{cid}'
# Channel keys the site asks the guide for (the geo lookup's list in the US).
EPG_CHANNELS = 'une,ume,tudn,gae,notc,ztudn,cavix'
# Fallbacks for when the launch config / bundle can't be read.
_DEFAULTS = {
    'oAuthURL': 'https://auth-platform.api.prd.univisionnow.com/oauth2/token',
    'clientID': 'webclient-ui-app',
    'clientSecret': 'dc7c9776-314d-42fb-8fe2-a7d527532a29',
    'contentAuthURL': 'https://playback-auth-service.api.prd.univisionnow.com',
    'clientRegURL': 'https://device-register-service.api.prd.univisionnow.com',
    'guestFlatURL': 'https://auth-gw.api.prd.univisionnow.com',
    'adobeQpatUrl': 'https://auth-gw-ap.api.prd.univisionnow.com/platform/access',
    'imageResizeURL': 'https://image-resizer-cloud-cdn.api-cms.prd.univisionnow.com',
}
# Catalog package -> Adobe resource (None = free for guests).
_PACKAGE_RESOURCE = {
    'urn:package:tve:univision': 'univision',
    'urn:package:tve:galavision': 'galavision',
    'urn:package:tve:deportes': 'deportes',
    'urn:package:tve:free': None,
}
_CATEGORY = {'unieast': 'Entertainment', 'umaeast': 'Entertainment', 'galaeast': 'Entertainment',
             'tudn': 'Sports', 'zona': 'Sports', 'n247': 'News', 'vix': 'Entertainment'}
_NAMES = {'unieast': 'Univision', 'umaeast': 'UniMás', 'galaeast': 'Galavisión', 'tudn': 'TUDN',
          'n247': 'N+ Univision 24/7', 'zona': 'Zona TUDN', 'vix': 'Canal ViX'}

_CONFIG_TTL = 12 * 3600
_GUIDE_DAYS = 3
_TOKEN_MARGIN = 300


class UnivisionTVEScraper(MvpdCooldownMixin, BaseScraper):
    """Univision, UniMás, Galavisión, TUDN (+ free N+ 24/7, Zona TUDN, Canal ViX)."""

    source_name = 'univision_tve'
    display_name = 'Univision TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({'User-Agent': UA, 'Origin': ORIGIN, 'Referer': ORIGIN + '/'})

    # ── config ───────────────────────────────────────────────────────────────

    def _config(self) -> dict:
        cached = self.cache.get('univision_config') or {}
        if cached and time.time() - float(cached.get('cached_at', 0)) < _CONFIG_TTL:
            return cached
        try:
            r = self.session.get(LAUNCH_CONFIG, timeout=20)
            r.raise_for_status()
            values = {c.get('key'): c.get('value') for c in (r.json() or {}).get('config') or []}
            config = {k: values.get(k) or v for k, v in _DEFAULTS.items()}
            page = self.session.get(ORIGIN + '/livetv', timeout=20)
            page.raise_for_status()
            main = re.search(r'src="(main\.[0-9a-f]+\.js)"', page.text)
            bundle = self.session.get(f'{ORIGIN}/{main.group(1)}', timeout=30).text if main else ''
            m = re.search(r'adobeSoftwareStatement:"([A-Za-z0-9_\-.]{100,})"', bundle)
            if not m:
                raise ValueError('no Adobe software statement in the app bundle')
            config['software_statement'] = m.group(1)
            config['cached_at'] = time.time()
        except (requests.RequestException, ValueError) as exc:
            if cached:
                logger.warning('[univision_tve] could not refresh the app config, using the saved one: %s', exc)
                return cached
            raise RuntimeError(f'Univision TVE: could not read tv.univision.com config: {exc}') from exc
        self._update_cache('univision_config', config)
        return config

    # ── channels + guide ─────────────────────────────────────────────────────

    def _epg_day(self, day: datetime) -> list[dict]:
        r = self.session.get(EPG_URL, params={
            'start': day.strftime('%Y-%m-%dT00:00:00.000Z'),
            'end': (day + timedelta(days=1)).strftime('%Y-%m-%dT00:00:00Z'),
            'reg': 'us', 'acl': 'en', 'dt': 'web', 'ipr': 'true', 'client': CLIENT, 'pf': 'main',
            'un_channels': EPG_CHANNELS, 'locale': 'es'}, timeout=30)
        r.raise_for_status()
        return (r.json() or {}).get('data') or []

    def _channel_resource(self, cid: str) -> dict:
        r = self.session.get(CHANNEL_URL.format(cid=cid), params={
            'reg': 'us', 'acl': 'en', 'dt': 'web', 'ipr': 'true', 'client': CLIENT, 'pf': 'main', 'locale': 'es'}, timeout=20)
        r.raise_for_status()
        return (r.json() or {}).get('data') or {}

    def fetch_channels(self) -> list[ChannelData]:
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        image_base = self._config()['imageResizeURL']
        out, channels = [], {}
        for row in self._epg_day(today):
            airing = next(iter(row.get('airing') or []), None)
            if not airing or not airing.get('cid'):
                continue
            cs = (airing.get('ch') or {}).get('cs')
            if not cs or cs in channels:
                continue
            res = self._channel_resource(airing['cid'])
            packages = [p for e in res.get('ent') or [] for p in e.get('sp') or []]
            resource = next((_PACKAGE_RESOURCE[p] for p in packages if p in _PACKAGE_RESOURCE), 'univision')
            if any((d.get('dp') or 'none') != 'none' for d in res.get('dap') or []):
                logger.warning('[univision_tve] %s now has DRM on a delivery profile; skipping it', cs)
                continue
            channels[cs] = {'cid': airing['cid'], 'resource': resource}
            name = _NAMES.get(cs) or next((n['n'] for n in res.get('lon') or [] if n.get('lang') == 'es'), cs)
            out.append(ChannelData(
                source_channel_id=cs,
                name=name,
                slug=f'univision-{cs}',
                logo_url=f'{image_base}/image/{airing["cid"]}/0-1x1.png?width=400',
                stream_url=f'{SCHEME}{cs}/{airing["cid"]}',
                category=_CATEGORY.get(cs),
                language='es',
                country='US',
                number=res.get('cn'),
                guide_key=f'UNIVISION:{cs.upper()}',
                gracenote_id=resolve_gracenote('univision_tve', lookup_key=cs),
            ))
        self._update_cache('univision_channels', channels)
        logger.info('[univision_tve] %d channels', len(out))
        return out

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {c.source_channel_id for c in channels}
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        seen, programs = set(), []
        for i in range(_GUIDE_DAYS):
            try:
                rows = self._epg_day(today + timedelta(days=i))
            except requests.RequestException as exc:
                logger.warning('[univision_tve] guide day %d failed: %s', i, exc)
                continue
            for row in rows:
                for a in row.get('airing') or []:
                    cs = (a.get('ch') or {}).get('cs')
                    if cs not in wanted or a.get('id') in seen:
                        continue
                    seen.add(a.get('id'))
                    prog = _program(cs, a)
                    if prog:
                        programs.append(prog)
        programs.sort(key=lambda p: (p.source_channel_id, p.start_time))
        logger.info('[univision_tve] %d guide entries', len(programs))
        return programs

    # ── playback ─────────────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        return self.resolve(raw_url)

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported Univision TVE stream URL: {raw_url}')
        cs, _, cid = raw_url[len(SCHEME):].partition('/')
        resource = ((self.cache.get('univision_channels') or {}).get(cs) or {}).get('resource')
        if resource is None and cs not in (self.cache.get('univision_channels') or {}):
            resource = 'univision'  # unknown until the next scrape: assume it needs the sign-in
        qpat = self._qpat(signed_in=resource is not None, resource=resource)
        headers = {'X-Authorization': qpat, 'Content-Type': 'application/json'}
        if resource is not None:
            headers['X-Adobe-Authorization'] = self._media_token(resource)
        for attempt in range(2):
            device = self._device(qpat, fresh=attempt > 0)
            body = {'deviceName': 'web', 'deviceId': device['deviceId'], 'contentId': cid, 'contentTypeId': 'live',
                    'catalogType': 'channel', 'mediaFormat': 'hls', 'drm': 'none', 'delivery': 'streaming',
                    'disableSsai': 'false', 'disableCsai': 'false', 'playbackMode': 'live'}
            r = self.session.post(f'{self._config()["contentAuthURL"]}/media/content/authorize', json=body, timeout=20,
                                  headers={**self._oauth_headers(), **headers, 'X-Device-Id': _device_jwt(device)})
            data = _qp_json(r)
            if data.get('contentUrl') or 'x-device-id' not in _qp_error(r).lower():
                break
        if not data.get('contentUrl'):
            err = _qp_error(r)
            if 'entitled' in err.lower():
                raise TVENotAuthorizedError(f'Univision TVE: your TV provider doesn\'t include {_NAMES.get(cs, cs)} ({err}).')
            raise RuntimeError(f'Univision TVE: playback refused for {cs}: HTTP {r.status_code} {err}')
        return data['contentUrl']

    # ── Quickplay tokens ─────────────────────────────────────────────────────

    def _oauth_headers(self) -> dict:
        cached = self.cache.get('univision_oauth') or {}
        if not cached.get('token') or float(cached.get('exp') or 0) - time.time() < _TOKEN_MARGIN:
            cfg = self._config()
            r = self.session.post(cfg['oAuthURL'], data={
                'client_id': cfg['clientID'], 'client_secret': cfg['clientSecret'], 'grant_type': 'client_credentials',
                'audience': 'edge-service', 'scope': 'offline openid'}, timeout=20)
            r.raise_for_status()
            body = r.json()
            cached = {'token': body['access_token'], 'exp': time.time() + float(body.get('expires_in') or 3600)}
            self._update_cache('univision_oauth', cached)
        return {'Authorization': f'Bearer {cached["token"]}', 'X-Client-Id': CLIENT}

    def _device_id(self) -> str:
        dev = self.cache.get('univision_device_ref')
        if not dev:
            dev = str(uuid.uuid4())
            self._update_cache('univision_device_ref', dev)
        return dev

    def _qpat(self, signed_in: bool, resource: str | None = None) -> str:
        key = 'univision_qpat_user' if signed_in else 'univision_qpat_guest'
        cached = self.cache.get(key) or {}
        if cached.get('token') and float(cached.get('exp') or 0) - time.time() > _TOKEN_MARGIN:
            if not signed_in or resource is None or (cached.get('decisions') or {}).get(resource, True):
                return cached['token']
        cfg = self._config()
        if signed_in:
            user = self._signed_in_user()
            if resource and not user['decisions'].get(resource, False):
                raise TVENotAuthorizedError(f'Univision TVE: {user["mso_id"]} doesn\'t include {resource}.')
            url = f'{cfg["adobeQpatUrl"]}/token/v2'
            payload = {'userId': user['user_id'], 'deviceId': self._device_id(), 'deviceInfo': _device_info()}
        else:
            user = {}
            url = f'{cfg["guestFlatURL"]}/platform/access/token'
            payload = {'deviceId': self._device_id(), 'deviceName': 'web'}
        r = self.session.post(url, json=payload, headers=self._oauth_headers(), timeout=20)
        data = _qp_json(r)
        if not data.get('token'):
            msg = f'Univision TVE: access token refused: HTTP {r.status_code} {_qp_error(r)}'
            raise self._sign_in_needed(msg) if signed_in else RuntimeError(msg)
        cached = {'token': data['token'], 'exp': time.time() + float(data.get('expires_in') or 3600),
                  'decisions': user.get('decisions') or {}}
        self._update_cache(key, cached)
        return cached['token']

    def _device(self, qpat: str, fresh: bool = False) -> dict:
        """Quickplay device registration; its secret signs X-Device-Id. Each
        registration issues a new secret that only works with the access
        token it was made with, so a new token means registering again."""
        cached = self.cache.get('univision_device') or {}
        tag = hashlib.sha256(qpat.encode()).hexdigest()[:16]
        if not fresh and cached.get('secret') and cached.get('ref') == self._device_id() and cached.get('qpat') == tag:
            return cached
        r = self.session.post(f'{self._config()["clientRegURL"]}/device/app/register',
                              json={'uniqueId': self._device_id()},
                              headers={**self._oauth_headers(), 'X-Authorization': qpat}, timeout=20)
        data = _qp_json(r)
        if not data.get('secret') or not data.get('deviceId'):
            raise RuntimeError(f'Univision TVE: device registration failed: HTTP {r.status_code} {_qp_error(r)}')
        cached = {'deviceId': data['deviceId'], 'secret': data['secret'], 'expiry': int(data.get('expiry') or 30),
                  'ref': self._device_id(), 'qpat': tag}
        self._update_cache('univision_device', cached)
        return cached

    # ── sign-in ──────────────────────────────────────────────────────────────

    def _account(self):
        account = tve_account_for(self.source_name)
        if not account or not account.is_usable():
            raise TVEAuthError('TVE credentials are not configured in Settings.')
        return account

    @staticmethod
    def adobe_client(statement: str, auth: dict):
        """The Adobe v2 client a saved sign-in belongs to."""
        from .nbc_tve import AdobePassV2Client
        client = AdobePassV2Client(REQUESTOR_ID, statement, REDIRECT_URL, auth['device_fingerprint'],
                                   client_creds=auth if auth.get('client_id') else None, origin=ORIGIN)
        client.client_id = auth.get('client_id')
        client.client_secret = auth.get('client_secret')
        client.access_token = auth.get('access_token')
        return client

    @staticmethod
    def _sign_in_needed(message: str) -> TVEAuthError:
        from ..tve.signin_notice import mark_signin_needed
        mark_signin_needed(NETWORK_KEY, message)
        return TVEAuthError(message)

    def _signed_in_user(self) -> dict:
        """{mso_id, user_id, decisions} from the saved Adobe sign-in."""
        cached = self.cache.get('univision_user') or {}
        if cached.get('user_id') and time.time() - float(cached.get('at') or 0) < 6 * 3600:
            return cached
        account = self._account()
        auth = (account.config or {}).get('univision_mvpd_auth') or {}
        if not auth.get('client_id') or not auth.get('device_fingerprint'):
            raise self._sign_in_needed(
                'Univision TVE: not signed in yet — use "Sign in" for Univision under Settings → TV Everywhere.')
        client = self.adobe_client(self._config()['software_statement'], auth)
        profile = self._adobe_profile(client, auth['mso_id'], account)
        if not profile:
            raise self._sign_in_needed(
                f'Univision TVE: the {auth["mso_id"]} sign-in has expired — sign in again under Settings → TV Everywhere.')
        user = self.user_from_profile(client, auth['mso_id'], profile)
        self._update_cache('univision_user', user)
        return user

    @staticmethod
    def user_from_profile(client, mso_id: str, profile: dict) -> dict:
        """Preauthorize the three resources and keep what the QPAT service needs."""
        r = client._post(
            f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/decisions/preauthorize/{mso_id}',
            json={'resources': list(RESOURCES)},
            headers={**client._bearer_headers(), 'Content-Type': 'application/json'},
        )
        decisions = {d.get('resource'): bool(d.get('authorized')) for d in (r.json() or {}).get('decisions') or []}
        if not any(decisions.values()):
            raise TVENotAuthorizedError(f'Univision TVE: {mso_id} doesn\'t include Univision, Galavisión or TUDN.')
        user_id = ((profile.get('attributes') or {}).get('userID') or {})
        user_id = user_id.get('value') if isinstance(user_id, dict) else user_id
        if not user_id:
            raise TVEAuthError('Univision TVE: the Adobe profile has no userID.')
        return {'mso_id': mso_id, 'user_id': user_id, 'decisions': decisions, 'at': time.time()}

    def _media_token(self, resource: str) -> str:
        """Adobe's short media token for the resource; the content authorizer
        wants it as X-Adobe-Authorization alongside the signed-in QPAT."""
        tokens = self.cache.get('univision_media_tokens') or {}
        cached = tokens.get(resource) or {}
        if cached.get('token') and float(cached.get('exp') or 0) - time.time() > 60:
            return cached['token']
        account = self._account()
        auth = (account.config or {}).get('univision_mvpd_auth') or {}
        if not auth.get('client_id') or not auth.get('mso_id'):
            raise self._sign_in_needed(
                'Univision TVE: not signed in yet — use "Sign in" for Univision under Settings → TV Everywhere.')
        client = self.adobe_client(self._config()['software_statement'], auth)
        url = f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/decisions/authorize/{auth["mso_id"]}'

        def fetch():
            # Not client._post: it raises on any HTTP error, so an expired
            # access token's 401 never reached the refresh below.
            try:
                return client.session.post(url, json={'resources': [resource]}, timeout=20,
                                           headers={**client._bearer_headers(), 'Content-Type': 'application/json'})
            except requests.RequestException as exc:
                raise TVEAuthError(f'Univision TVE: could not reach Adobe: {exc}') from exc

        r = fetch()
        if r.status_code == 401 and client.client_id and client.client_secret:
            self._save_access_token(client, account)
            r = fetch()
        if r.status_code == 429 or r.status_code >= 500:
            raise TVEAuthError(f'Univision TVE: Adobe returned HTTP {r.status_code}; will retry.')
        try:
            decision = next((d for d in (r.json() or {}).get('decisions') or [] if d.get('resource') == resource), {})
        except ValueError:
            decision = {}
        token = (decision.get('token') or {}).get('serializedToken')
        if not decision.get('authorized') or not token:
            if r.status_code == 401:
                raise self._sign_in_needed(
                    f'Univision TVE: the {auth["mso_id"]} sign-in has expired — sign in again under Settings → TV Everywhere.')
            raise TVENotAuthorizedError(f'Univision TVE: {auth["mso_id"]} doesn\'t include {resource}.')
        not_after = float((decision.get('token') or {}).get('notAfter') or 0) / 1000 or time.time() + 300
        tokens[resource] = {'token': token, 'exp': not_after}
        self._update_cache('univision_media_tokens', tokens)
        return token

    @staticmethod
    def _save_access_token(client, account) -> None:
        from .. import db
        client.refresh_access_token()
        cfg = dict(account.config or {})
        cfg['univision_mvpd_auth'] = {**(cfg.get('univision_mvpd_auth') or {}), 'access_token': client.access_token}
        account.config = cfg
        db.session.commit()

    def _adobe_profile(self, client, mso_id: str, account) -> dict | None:
        url = f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/profiles/{mso_id}'

        def fetch():
            try:
                r = client.session.get(url, headers=client._bearer_headers(), timeout=20)
            except requests.RequestException as exc:
                raise TVEAuthError(f'Univision TVE: could not reach Adobe: {exc}') from exc
            if r.status_code == 429 or r.status_code >= 500:
                raise TVEAuthError(f'Univision TVE: Adobe returned HTTP {r.status_code}; will retry.')
            return r

        r = fetch()
        if r.status_code == 401 and client.client_id and client.client_secret:
            self._save_access_token(client, account)
            r = fetch()
        if not r.ok:
            return None
        try:
            return ((r.json() or {}).get('profiles') or {}).get(mso_id) or None
        except ValueError:
            return None


def _device_info() -> str:
    return f'User-Agent: {UA}\nScreen Resolution: 1920x1080\nDevice Type: Desktop\n'


def _device_jwt(device: dict) -> str:
    """The X-Device-Id the content authorizer wants: HS256 over
    {deviceId, aud, iat, exp}, keyed with the registration's base64 secret."""
    def b64(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).decode().rstrip('=')
    now = int(time.time())
    head = b64(json.dumps({'alg': 'HS256', 'typ': 'JWT'}, separators=(',', ':')).encode())
    body = b64(json.dumps({'deviceId': device['deviceId'], 'aud': 'playback-auth-service', 'iat': now,
                           'exp': now + int(device.get('expiry') or 30)}, separators=(',', ':')).encode())
    sig = hmac.new(base64.b64decode(device['secret']), f'{head}.{body}'.encode(), hashlib.sha256).digest()
    return f'{head}.{body}.{b64(sig)}'


def _qp_json(r: requests.Response) -> dict:
    try:
        body = r.json() or {}
    except ValueError:
        return {}
    return body.get('data') or {} if (body.get('header') or {}).get('code', 0) == 0 else {}


def _qp_error(r: requests.Response) -> str:
    try:
        errs = ((r.json() or {}).get('header') or {}).get('errors') or []
    except ValueError:
        return r.text[:120]
    return '; '.join(f'{e.get("code")} {e.get("description")}' for e in errs) or r.text[:120]


def _lang(items, lang: str = 'es') -> str | None:
    items = items or []
    pick = next((i for i in items if i.get('lang') == lang), None) or (items[0] if items else None)
    if not pick:
        return None
    n = pick.get('n')
    return (', '.join(n) if isinstance(n, list) else n) or None


def _date(val):
    try:
        return date.fromisoformat(str(val)[:10]) if val else None
    except ValueError:
        return None


def _program(cs: str, a: dict) -> ProgramData | None:
    try:
        start = datetime.fromisoformat(a['sc_st_dt'].replace('Z', '+00:00'))
        end = datetime.fromisoformat(a['sc_ed_dt'].replace('Z', '+00:00'))
    except (KeyError, ValueError, AttributeError):
        return None
    pgm = a.get('pgm') or {}
    title = (_lang(pgm.get('lon')) or '').strip()
    if not title or end <= start:
        return None
    is_sport = 'sport' in (pgm.get('pgm_ty') or '').lower()
    episode_title = _lang(pgm.get('loen'), 'en') if is_sport else _lang(pgm.get('loepn')) if pgm.get('loepn') else None
    ex_id = pgm.get('ex_id') or ''
    image = next((i.get('u') for i in pgm.get('ex_ia') or [] if i.get('u')), None)
    genres = (pgm.get('log') or [{}])[0].get('n') if pgm.get('log') else None
    return ProgramData(
        source_channel_id=cs,
        title=title,
        start_time=start,
        end_time=end,
        description=_lang(pgm.get('lold')) or _lang(pgm.get('lod')),
        episode_title=episode_title if episode_title and episode_title != title else None,
        poster_url=image,
        category=';'.join(genres) if isinstance(genres, list) else None,
        original_air_date=_date(pgm.get('oadt')),
        program_type='movie' if ex_id.startswith('MV') else ('episode' if ex_id.startswith('EP') else None),
        series_id=pgm.get('ex_stl_id') or None,
        episode_id=ex_id or None,
    )

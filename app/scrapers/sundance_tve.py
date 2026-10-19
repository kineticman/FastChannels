"""
sundance_tve.py — SundanceTV's 24/7 live channel from app.sundancetv.com,
with a Sundance account plus a TV-provider sign-in. Scoped 2026-10-19 from
the site's bundles, a signed-in HAR (dev/hitlist/hars/sundance.har) and
live probes; dev/sundance/findings_2026-10-19.md has the trace.

Key facts established by probing:

- The site is DICE/IMG ("Vesper"): API dce-frontoffice.imggaming.com with the
  web app's shared x-api-key and Realm: dce.sundance. A guest token
  (/v2/login/guest/checkin) reads the live event list and the guide.
- TV-provider access is Adobe Pass REST v1 under requestor SUNDANCE, but DICE
  drives it, so it needs a signed-in Sundance account (a guest gets
  loginNeeded): /v2/login {id, secret} → user token; register an Adobe client
  with the software statement from the realm settings; POST /v3/adobe/regcode
  (user token) → {code, accessToken}; the user signs in at Adobe with that
  code; POST /v3/adobe/token {requestorId, clientId, clientSecret,
  accessToken, mvpd} → a DICE session whose token holds the Adobe licence
  (ent.iai includes 5028). A password login alone only lists it as available.
- DICE tokens: a 10-minute access token and a 60-day refresh token,
  POST /v2/token/refresh {refreshToken} with the old token as Bearer. Refresh
  answers 429 when called again within a few minutes, so the session is
  shared through Redis and only one worker refreshes at a time.
- Playback: /v2/stream/event/<id> → playerUrlCallback → {hls, hlsWidevine,
  dash}, all encrypted (no clear variant). The URL token (hdnea) lasts about
  40 seconds. The DASH manifest has relative segment URLs that need that
  token, so it dies with it; the HLS master instead hands its child playlists
  a ~24-hour hdntl token in their URLs. So this plays hlsWidevine (fMP4,
  SAMPLE-AES-CTR, Widevine) through FastChannels Player.
- License: POST shield-drm.onvesper.com/api/v2/license with
  Authorization: Bearer <drm.jwtToken> and X-DRM-INFO: base64 JSON
  {"system": ...}. The site's code has two enums for the system name, so the
  license call tries both (see post_license_request).
- Guide: /v4/epg/content/programmes?channelId=1254 only reaches about 8 hours
  ahead, so the channel also carries Gracenote 71280 (SundanceTV HD, the ID
  DirecTV and Spectrum both use) for a full guide.

Not verified: the DRM token carries def=sd for a BROWSER session, which may
cap the picture below the stream's 720p.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import requests

from .base import BaseScraper, ChannelData, ConfigField, ProgramData
from ..gracenote_map import resolve_gracenote
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import TVEAuthError, TVENotAuthorizedError, refresh_adobe_client_token

logger = logging.getLogger(__name__)

SCHEME = 'sundance://'
NETWORK_KEY = 'sundance'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
SITE = 'https://app.sundancetv.com'
API = 'https://dce-frontoffice.imggaming.com/api'
API_KEY = '857a1e5d-e35e-4fdf-805b-a87b6f8364bf'
REALM = 'dce.sundance'
ADOBE_API = 'https://api.auth.adobe.com'
REQUESTOR_ID = 'SUNDANCE'
REDIRECT_URL = f'{SITE}/providers/success'
_LICENSE_URL = 'https://shield-drm.onvesper.com/api/v2/license'
# The site's two names for Widevine in X-DRM-INFO. The license server took
# com.widevine.alpha (WIDEVINE: 400) on 2026-10-19; the other stays as a fallback.
_DRM_SYSTEMS = ('com.widevine.alpha', 'WIDEVINE')
# Known ids, for when the live list can't be read.
_DEFAULT_EVENT = '284832'
_EPG_CHANNEL = '1254'
_ADOBE_LICENCE = 5028  # the "Adobe Licence" on the live event
_GRACENOTE = '71280'   # SundanceTV HD
_LOGO = '/static/logos/sundance/sundance.png'

_SESSION_KEY = 'sundance_tve:session'
_LOCK_KEY = 'sundance_tve:refresh_lock'
_TOKEN_MARGIN = 60
_STREAM_MAX_AGE = 20  # the master URL's hdnea lasts ~40s from issue
_GUIDE_HOURS = 24


def headers(token: str | None = None) -> dict:
    h = {'x-api-key': API_KEY, 'Realm': REALM, 'app': 'dice', 'x-app-var': '6.60.0', 'User-Agent': UA,
         'Origin': SITE, 'Referer': SITE + '/', 'Content-Type': 'application/json', 'Accept': 'application/json'}
    if token:
        h['Authorization'] = f'Bearer {token}'
    return h


def claims(token: str) -> dict:
    try:
        part = token.split('.')[1]
        return json.loads(base64.urlsafe_b64decode(part + '=' * (-len(part) % 4)))
    except (IndexError, ValueError):
        return {}


def guest_token() -> str:
    r = requests.post(f'{API}/v2/login/guest/checkin', json={}, headers=headers(), timeout=20)
    token = _json(r).get('authorisationToken')
    if not token:
        raise RuntimeError(f'SundanceTV: guest check-in failed: HTTP {r.status_code}')
    return token


def login(email: str, password: str) -> dict:
    """/v2/login → {authorisationToken, refreshToken} for the Sundance account."""
    r = requests.post(f'{API}/v2/login', json={'id': email, 'secret': password}, headers=headers(guest_token()),
                      timeout=20)
    data = _json(r)
    if not data.get('authorisationToken'):
        raise TVEAuthError(f'SundanceTV: the Sundance email/password sign-in failed (HTTP {r.status_code} '
                           f'{_reason(r)}).')
    return data


def software_statement() -> str:
    r = requests.get(f'{API}/v2/realm-settings/domain/app.sundancetv.com', headers=headers(), timeout=20)
    for provider in _json(r).get('authenticationProviders') or []:
        if provider.get('requestorId') == REQUESTOR_ID and provider.get('softwareStatement'):
            return provider['softwareStatement']
    raise RuntimeError(f'SundanceTV: no Adobe software statement in the realm settings (HTTP {r.status_code}).')


def register_adobe_client() -> dict:
    r = requests.post(f'{ADOBE_API}/o/client/register', json={'software_statement': software_statement()},
                      headers={'User-Agent': UA}, timeout=20)
    data = _json(r)
    if not data.get('client_id'):
        raise TVEAuthError(f'SundanceTV: Adobe client registration failed: HTTP {r.status_code}')
    return {'client_id': data['client_id'], 'client_secret': data.get('client_secret') or ''}


def request_regcode(user_token: str, client: dict) -> dict:
    """DICE's own Adobe regcode for this account → {code, accessToken}."""
    r = requests.post(f'{API}/v3/adobe/regcode', headers=headers(user_token), timeout=20, json={
        'requestorId': REQUESTOR_ID, 'clientId': client['client_id'], 'clientSecret': client['client_secret']})
    data = _json(r)
    if not data.get('code'):
        raise TVEAuthError(f'SundanceTV: no sign-in code from Sundance (HTTP {r.status_code} {_reason(r)}).')
    return data


def authenticate_url(code: str, mso_id: str) -> str:
    return f'{ADOBE_API}/api/v1/authenticate?' + urlencode({
        'requestor_id': REQUESTOR_ID, 'mso_id': mso_id, 'domain_name': SITE, 'noflash': 'true',
        'reg_code': code, 'redirect_url': REDIRECT_URL})


def adobe_signed_in(access_token: str, device_id: str) -> bool | None:
    """Adobe's own answer to "has this device signed in": True/False, or None
    when Adobe gives no usable answer (the caller then asks DICE directly)."""
    try:
        r = requests.get(f'{ADOBE_API}/api/v1/tokens/authn', timeout=15,
                         params={'requestor': REQUESTOR_ID, 'deviceId': device_id},
                         headers={'Authorization': f'Bearer {access_token}', 'User-Agent': UA,
                                  'Accept': 'application/json'})
    except requests.RequestException:
        return None
    if r.status_code == 200:
        return True
    if r.status_code in (403, 404, 410):
        return False
    return None


def exchange_adobe(user_token: str, client: dict, access_token: str, mso_id: str) -> dict | None:
    """/v3/adobe/token → the TV-provider DICE session, or None while Adobe
    has no sign-in for this account yet."""
    r = requests.post(f'{API}/v3/adobe/token', headers=headers(user_token), timeout=20, json={
        'requestorId': REQUESTOR_ID, 'clientId': client['client_id'], 'clientSecret': client['client_secret'],
        'accessToken': access_token, 'mvpd': mso_id})
    data = _json(r)
    if data.get('authorisationToken') and data.get('refreshToken'):
        return data
    logger.info('[sundance_tve] /v3/adobe/token not ready: HTTP %s %s', r.status_code, _reason(r))
    return None


def has_tv_provider(session_token: str) -> bool:
    return _ADOBE_LICENCE in ((claims(session_token).get('ent') or {}).get('iai') or [])


def _redis():
    try:
        import redis
        return redis.from_url(os.environ.get('REDIS_URL', 'redis://localhost:6379/0'), decode_responses=True,
                              socket_timeout=2)
    except Exception:  # noqa: BLE001
        return None


class SundanceTVEScraper(BaseScraper):
    """SundanceTV (24/7) with a Sundance account and a TV-provider sign-in."""

    source_name = 'sundance_tve'
    display_name = 'SundanceTV (AMC Networks)'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 240  # the guide only reaches ~8 hours ahead
    stream_audit_enabled = True
    license_url = _LICENSE_URL
    all_channels_require_drm_bridge = True

    config_schema = [
        ConfigField('email', 'Sundance email', placeholder='you@example.com',
                    help_text='A free account from app.sundancetv.com. Your TV provider is linked to it when you sign in.'),
        ConfigField('password', 'Sundance password', field_type='password', secret=True),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update(headers())

    # ── channels + guide ─────────────────────────────────────────────────────

    def _event(self) -> dict:
        """The live channel's event {id, title}, from the public live list."""
        try:
            r = self.session.get(f'{API}/v2/event/live', params={'rpp': 25}, timeout=20,
                                 headers={'Authorization': f'Bearer {guest_token()}'})
            r.raise_for_status()
            events = (r.json() or {}).get('events') or []
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            logger.warning('[sundance_tve] could not read the live list, using the known event: %s', exc)
            events = []
        event = next((e for e in events if 'sundance' in (e.get('title') or '').lower()), None) or {}
        return {'id': str(event.get('id') or _DEFAULT_EVENT), 'title': event.get('title') or 'SundanceTV'}

    def fetch_channels(self) -> list[ChannelData]:
        event = self._event()
        return [ChannelData(
            source_channel_id=_EPG_CHANNEL,
            name='SundanceTV',
            slug='sundance-tve-sundancetv',
            logo_url=_LOGO,
            stream_url=f'{SCHEME}{event["id"]}',
            stream_type='hls',
            category='Movies',
            language='en',
            country='US',
            guide_key=f'SUNDANCE:{_EPG_CHANNEL}',
            gracenote_id=resolve_gracenote('sundance_tve', lookup_key=_EPG_CHANNEL) or _GRACENOTE,
        )]

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        stamp = lambda d: d.strftime('%Y-%m-%dT%H:%M:%S.000Z')  # noqa: E731
        try:
            r = self.session.get(f'{API}/v4/epg/content/programmes', timeout=20,
                                 headers={'Authorization': f'Bearer {guest_token()}'},
                                 params={'categorisedChannelId': _EPG_CHANNEL, 'channelId': _EPG_CHANNEL,
                                         'from': stamp(now - timedelta(hours=3)),
                                         'to': stamp(now + timedelta(hours=_GUIDE_HOURS))})
            r.raise_for_status()
            rows = ((r.json() or {}).get('channels') or {}).get(_EPG_CHANNEL) or []
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            logger.warning('[sundance_tve] guide fetch failed: %s', exc)
            rows = []
        programs = []
        for ch in channels:
            for row in rows:
                prog = _program(ch.source_channel_id, row)
                if prog:
                    programs.append(prog)
        logger.info('[sundance_tve] %d guide entries', len(programs))
        return programs

    # ── session ──────────────────────────────────────────────────────────────

    def _auth(self) -> tuple:
        account = tve_account_for(self.source_name)
        auth = ((account.config or {}).get('sundance_auth') or {}) if account else {}
        if not auth.get('refresh_token'):
            raise self._sign_in_needed(
                'SundanceTV: not signed in yet — use "Sign in" for SundanceTV under Settings → TV Everywhere.')
        return account, auth

    def _stored_session(self, auth: dict) -> dict:
        """The newest session: Redis, then the DB copy, then the sign-in's own
        refresh token. A session from before the latest sign-in is ignored."""
        signed_in_at = float(auth.get('captured_at') or 0)
        candidates = []
        r = _redis()
        if r is not None:
            try:
                candidates.append(json.loads(r.get(_SESSION_KEY) or '{}'))
            except Exception:  # noqa: BLE001
                pass
        candidates.append(self.cache.get('sundance_session') or {})
        for sess in candidates:
            if sess.get('refresh') and float(sess.get('signed_in_at') or 0) >= signed_in_at:
                return sess
        return {'refresh': auth['refresh_token'], 'signed_in_at': signed_in_at}

    def _save_session(self, sess: dict) -> None:
        r = _redis()
        if r is not None:
            try:
                r.set(_SESSION_KEY, json.dumps(sess), ex=60 * 86400)
            except Exception:  # noqa: BLE001
                pass
        # Written through right away: the refresh token may be single-use, and
        # Redis doesn't survive a restart.
        from ..config_store import persist_source_cache_updates
        from ..models import Source
        source = Source.query.filter_by(name=self.source_name).first()
        if source:
            persist_source_cache_updates(source.id, {'sundance_session': sess})
        if self._cache is not None:
            self._cache['sundance_session'] = sess

    def _token(self) -> str:
        """A current DICE access token with the TV-provider licence."""
        _, auth = self._auth()
        sess = self._stored_session(auth)
        if sess.get('auth') and float(claims(sess['auth']).get('exp') or 0) - time.time() > _TOKEN_MARGIN:
            return sess['auth']
        r = _redis()
        locked = False
        if r is not None:
            for _ in range(40):
                try:
                    locked = bool(r.set(_LOCK_KEY, '1', nx=True, ex=30))
                except Exception:  # noqa: BLE001
                    break
                if locked:
                    break
                time.sleep(0.5)
                sess = self._stored_session(auth)
                if sess.get('auth') and float(claims(sess['auth']).get('exp') or 0) - time.time() > _TOKEN_MARGIN:
                    return sess['auth']
        try:
            sess = self._stored_session(auth)
            if sess.get('auth') and float(claims(sess['auth']).get('exp') or 0) - time.time() > _TOKEN_MARGIN:
                return sess['auth']
            fresh = self._refresh(sess) or self._relink(auth)
            sess = {'auth': fresh['authorisationToken'], 'refresh': fresh['refreshToken'],
                    'signed_in_at': float(auth.get('captured_at') or 0)}
            self._save_session(sess)
            return sess['auth']
        finally:
            if locked:
                try:
                    r.delete(_LOCK_KEY)
                except Exception:  # noqa: BLE001
                    pass

    def _refresh(self, sess: dict) -> dict | None:
        bearer = sess.get('auth') or guest_token()
        resp = requests.post(f'{API}/v2/token/refresh', json={'refreshToken': sess['refresh']},
                             headers=headers(bearer), timeout=20)
        if resp.status_code == 429:
            raise RuntimeError('SundanceTV: Sundance is rate-limiting sign-in renewals; try again in a few minutes.')
        data = _json(resp)
        if data.get('authorisationToken') and has_tv_provider(data['authorisationToken']):
            data.setdefault('refreshToken', sess['refresh'])
            return data
        logger.info('[sundance_tve] session renewal refused (HTTP %s %s); signing in again', resp.status_code,
                    _reason(resp))
        return None

    def _relink(self, auth: dict) -> dict:
        """With the renewal gone, sign in with the saved password and ask
        Sundance to pick the TV-provider sign-in back up from Adobe — it's
        bound to the Sundance account, so no new link is needed while it
        lasts."""
        email, password = self.config.get('email') or '', self.config.get('password') or ''
        if not email or not password or not auth.get('client_id'):
            raise self._sign_in_needed(
                'SundanceTV: the sign-in has expired — sign in again under Settings → TV Everywhere.')
        user = login(email, password)
        client = {'client_id': auth['client_id'], 'client_secret': auth.get('client_secret') or ''}
        data = exchange_adobe(user['authorisationToken'], client,
                              refresh_adobe_client_token(client['client_id'], client['client_secret']),
                              auth.get('mso_id') or '')
        if not data or not has_tv_provider(data['authorisationToken']):
            raise self._sign_in_needed(
                f'SundanceTV: the {auth.get("mso_id") or "TV provider"} sign-in has expired — sign in again '
                'under Settings → TV Everywhere.')
        logger.info('[sundance_tve] signed back in with the saved password')
        return data

    # ── playback ─────────────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        # Validate entitlement, then hand back the opaque URL: the stream is
        # Widevine either way, and its URL token only lasts ~40 seconds.
        self.resolve(raw_url)
        return raw_url

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported SundanceTV stream URL: {raw_url}')
        event = raw_url[len(SCHEME):].strip('/')
        streams = dict(self.cache.get('sundance_streams') or {})
        cached = streams.get(_EPG_CHANNEL) or {}
        if cached.get('event') == event and time.time() - float(cached.get('at') or 0) < _STREAM_MAX_AGE:
            return cached['url']
        token = self._token()
        r = self.session.get(f'{API}/v2/stream/event/{event}', headers={'Authorization': f'Bearer {token}'},
                             timeout=20)
        callback = _json(r).get('playerUrlCallback')
        if not callback:
            if r.status_code == 403:
                raise TVENotAuthorizedError(f'SundanceTV: Sundance refused the stream ({_reason(r) or "no access"}).')
            raise RuntimeError(f'SundanceTV: no stream: HTTP {r.status_code} {_reason(r)}')
        p = self.session.get(callback, headers={'Authorization': f'Bearer {token}'}, timeout=20)
        variant = _json(p).get('hlsWidevine') or {}
        if not variant.get('url'):
            raise RuntimeError(f'SundanceTV: no Widevine HLS stream: HTTP {p.status_code}')
        streams[_EPG_CHANNEL] = {'event': event, 'url': variant['url'], 'at': time.time(),
                                 'drm_token': (variant.get('drm') or {}).get('jwtToken'),
                                 'license': (variant.get('drm') or {}).get('url') or _LICENSE_URL}
        self._update_cache('sundance_streams', streams)
        return variant['url']

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        return ((config.get('sundance_streams') or {}).get(_EPG_CHANNEL) or {}).get('license') or cls.license_url

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        token = ((config.get('sundance_streams') or {}).get(_EPG_CHANNEL) or {}).get('drm_token')
        headers_ = {'Content-Type': 'application/octet-stream', 'User-Agent': UA, 'Origin': SITE,
                    'Referer': SITE + '/', 'X-DRM-INFO': _drm_info(_DRM_SYSTEMS[0])}
        if token:
            headers_['Authorization'] = f'Bearer {token}'
        return challenge, headers_

    @classmethod
    def post_license_request(cls, license_url: str, body: bytes, headers: dict, timeout: int = 15):
        r = requests.post(license_url, data=body, headers=headers, timeout=timeout)
        if r.status_code in (400, 403) and headers.get('X-DRM-INFO') == _drm_info(_DRM_SYSTEMS[0]):
            retry = requests.post(license_url, data=body, timeout=timeout,
                                  headers={**headers, 'X-DRM-INFO': _drm_info(_DRM_SYSTEMS[1])})
            logger.info('[sundance_tve] license: %s answered %s, %s answered %s', _DRM_SYSTEMS[0], r.status_code,
                        _DRM_SYSTEMS[1], retry.status_code)
            if retry.ok:
                return retry
        return r

    @staticmethod
    def _sign_in_needed(message: str) -> TVEAuthError:
        from ..tve.signin_notice import mark_signin_needed
        mark_signin_needed(NETWORK_KEY, message)
        return TVEAuthError(message)


def _drm_info(system: str) -> str:
    return base64.b64encode(json.dumps({'system': system}).encode()).decode()


def _json(r) -> dict:
    try:
        body = r.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _reason(r) -> str:
    body = _json(r)
    messages = body.get('messages') or []
    return str(body.get('code') or '') + (f' {messages[0]}' if messages else '')


def _program(source_channel_id: str, row: dict) -> ProgramData | None:
    try:
        start = datetime.fromisoformat(row['startDate'].replace('Z', '+00:00'))
        end = datetime.fromisoformat(row['endDate'].replace('Z', '+00:00'))
    except (KeyError, AttributeError, ValueError):
        return None
    title = (row.get('episode') or row.get('title') or '').strip()
    if not title or end <= start:
        return None
    return ProgramData(
        source_channel_id=source_channel_id,
        title=title,
        start_time=start,
        end_time=end,
        poster_url=row.get('thumbnailUrl') or None,
        category='Movies',
    )

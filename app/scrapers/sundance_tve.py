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
- DICE's own streams (/v2/stream/event/<id> → hls, hlsWidevine, dash) are
  all encrypted. But AMC Networks' older TVE backend still serves Sundance
  like AMC/IFC/WE tv/BBC America (see amcn_tve.py), in clear HLS up to 1080p
  — Channels DVR plays it this way. Found 2026-10-26: network sundancetv,
  livestream 6398689080112. It takes an Adobe short media token for
  SUNDANCE, which the DICE-driven sign-in already provides: Adobe v1
  /authorize + /tokens/media with the saved Adobe client, and the Sundance
  profile id (the token's pro.id) as the Adobe device id. AMCN then wants
  unauth → refresh → mvpd-auth with the Adobe userId, and
  /playback-id/api/v1/livestream/<id> answers with Brightcove SSAI HLS.
  So DICE is only used for the sign-in, the device id and the guide.
- Guide: /v4/epg/content/programmes?channelId=1254 only reaches about 8 hours
  ahead, so the channel also carries Gracenote 71280 (SundanceTV HD, the ID
  DirecTV and Spectrum both use) for a full guide.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import requests

from .base import BaseScraper, ChannelData, ConfigField, ProgramData
from ..gracenote_map import resolve_gracenote
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import TVEAuthError, TVENotAuthorizedError, refresh_adobe_client_token
from . import amcn_tve

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
# Known ids, for when the live list can't be read.
_DEFAULT_EVENT = '284832'
_EPG_CHANNEL = '1254'
_ADOBE_LICENCE = 5028  # the "Adobe Licence" on the live event
_GRACENOTE = '71280'   # SundanceTV HD
_LOGO = '/static/logos/sundance/sundance.png'

_SESSION_KEY = 'sundance_tve:session'
_LOCK_KEY = 'sundance_tve:refresh_lock'
_TOKEN_MARGIN = 60
_ADOBE_TOKEN_MAX_AGE = 5 * 3600  # Adobe client tokens last ~6h
# AMC Networks' channel for SundanceTV. service_group_id is unconfirmed (the
# web app that sent it is gone) but AMCN accepted it for every call.
_AMCN_CHANNEL = amcn_tve.AMCNChannel(
    channel_id='sundancetv', name='SundanceTV', slug='sundancetv', requestor_id=REQUESTOR_ID,
    domain='sundancetv.com', service_group_id='2', livestream_id='6398689080112', logo_url='',
)
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

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported SundanceTV stream URL: {raw_url}')
        _, auth = self._auth()
        if not auth.get('client_id'):
            raise self._sign_in_needed('SundanceTV: not signed in yet — use "Sign in" for SundanceTV under '
                                       'Settings → TV Everywhere.')
        state = dict(self.cache.get('sundance_streams') or {})
        if state.get('signed_in_at') != auth.get('captured_at'):
            state = {'signed_in_at': auth.get('captured_at')}  # a new sign-in: start over
        media_token = self._adobe_media_token(auth, state)
        mso = auth.get('mso_id') or ''
        token = self._amcn_token(state, mso)
        hdrs = amcn_tve._amcn_headers(
            _AMCN_CHANNEL, token=token, device_id=state['amcn_device'], entitlement='mvpd-auth',
            adobe_id=state['adobe_user'], mvpd=mso, feature_flags=amcn_tve._jwt_payload(token).get('feature_flags'))
        self._update_cache('sundance_streams', state)
        r = self.session.post(
            f'{amcn_tve.API_BASE}/playback-id/api/v1/livestream/{_AMCN_CHANNEL.livestream_id}',
            json={'adobeShortMediaToken': media_token, 'hba': False,
                  'adtags': {'lat': 0, 'url': _AMCN_CHANNEL.live_url, 'playerWidth': 1920, 'playerHeight': 1080}},
            headers=hdrs, timeout=30)
        if r.status_code in (401, 403):
            # A stale AMCN token: drop it so the next tune starts over.
            state.pop('amcn_token', None)
            self._update_cache('sundance_streams', state)
        r.raise_for_status()
        hls = amcn_tve._find_hls_source(r.json())
        if not hls:
            raise RuntimeError('SundanceTV: AMC Networks answered without an HLS stream.')
        return hls

    def _adobe_media_token(self, auth: dict, state: dict) -> str:
        """Adobe's short media token for SUNDANCE, from the TV-provider
        sign-in saved for this Sundance profile."""
        if not state.get('adobe_device'):
            # The profile id never changes, so an expired session token still has it.
            stored = self._stored_session(auth).get('auth') or self._token()
            state['adobe_device'] = (claims(stored).get('pro') or {}).get('id') or ''
            if not state['adobe_device']:
                raise RuntimeError('SundanceTV: the Sundance session has no profile id.')
        if not state.get('adobe_token') or time.time() - float(state.get('adobe_token_at') or 0) > _ADOBE_TOKEN_MAX_AGE:
            state['adobe_token'] = refresh_adobe_client_token(auth['client_id'], auth.get('client_secret') or '')
            state['adobe_token_at'] = time.time()
        params = {'requestor': REQUESTOR_ID, 'deviceId': state['adobe_device']}
        hdrs = {'Authorization': f'Bearer {state["adobe_token"]}', 'User-Agent': UA, 'Accept': 'application/json'}
        if not state.get('adobe_user'):
            r = requests.get(f'{ADOBE_API}/api/v1/tokens/authn', params=params, headers=hdrs, timeout=20)
            if r.status_code in (403, 404, 410):
                raise self._sign_in_needed(
                    f'SundanceTV: the {auth.get("mso_id") or "TV provider"} sign-in has expired — sign in again '
                    'under Settings → TV Everywhere.')
            r.raise_for_status()
            state['adobe_user'] = _json(r).get('userId') or ''
        params['resource'] = REQUESTOR_ID
        r = requests.get(f'{ADOBE_API}/api/v1/authorize', params=params, headers=hdrs, timeout=20)
        if r.status_code == 403:
            raise TVENotAuthorizedError(f'SundanceTV: Adobe did not authorize SundanceTV for {auth.get("mso_id")}.')
        if r.status_code in (404, 410):
            state.pop('adobe_user', None)
            raise self._sign_in_needed(
                f'SundanceTV: the {auth.get("mso_id") or "TV provider"} sign-in has expired — sign in again '
                'under Settings → TV Everywhere.')
        r.raise_for_status()
        r = requests.get(f'{ADOBE_API}/api/v1/tokens/media', params=params, headers=hdrs, timeout=20)
        r.raise_for_status()
        token = _json(r).get('serializedToken')
        if not token:
            raise TVEAuthError('SundanceTV: Adobe returned no media token.')
        return token

    def _amcn_token(self, state: dict, mso: str) -> str:
        """AMC Networks' mvpd-auth bearer: unauth → refresh → mvpd-auth,
        reused until it's near expiry."""
        token = state.get('amcn_token') or ''
        if token and float(amcn_tve._jwt_exp(token) or 0) - time.time() > _TOKEN_MARGIN:
            return token
        ch, adobe_id = _AMCN_CHANNEL, state['adobe_user']
        device = state.get('amcn_device') or str(uuid.uuid4())
        r = self.session.post(f'{amcn_tve.API_BASE}/auth-orchestration-id/api/v1/unauth', json={},
                              headers=amcn_tve._amcn_headers(ch, device_id=device), timeout=20)
        r.raise_for_status()
        unauth = _json(r).get('data') or {}
        device = amcn_tve._jwt_payload(unauth.get('access_token') or '').get('device-id') or device
        r = self.session.post(
            f'{amcn_tve.API_BASE}/auth-orchestration-id/api/v1/refresh', json={}, timeout=20,
            headers=amcn_tve._amcn_headers(
                ch, token=unauth.get('refresh_token') or unauth.get('access_token') or '', device_id=device,
                adobe_id=adobe_id, mvpd=mso, cache_hash=unauth.get('cache_hash'),
                user_cache_hash=unauth.get('user_cache_hash')))
        r.raise_for_status()
        refreshed = (_json(r).get('data') or {}).get('access_token') or ''
        r = self.session.post(
            f'{amcn_tve.API_BASE}/auth-orchestration-id/api/v1/mvpd-auth', json={}, timeout=20,
            headers=amcn_tve._amcn_headers(
                ch, token=refreshed, device_id=device, adobe_id=adobe_id, mvpd=mso,
                feature_flags=amcn_tve._jwt_payload(refreshed).get('feature_flags')))
        r.raise_for_status()
        token = ((_json(r).get('data') or {}).get('access_token') or '').strip()
        if not token:
            raise TVEAuthError('SundanceTV: AMC Networks did not return a signed-in token.')
        state['amcn_token'], state['amcn_device'] = token, device
        return token

    @staticmethod
    def _sign_in_needed(message: str) -> TVEAuthError:
        from ..tve.signin_notice import mark_signin_needed
        mark_signin_needed(NETWORK_KEY, message)
        return TVEAuthError(message)


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
    raw = (row.get('episode') or row.get('title') or '').strip()
    if not raw or end <= start:
        return None
    title, episode_title, season, episode = _split_guide_title(raw)
    return ProgramData(
        source_channel_id=source_channel_id,
        title=title,
        start_time=start,
        end_time=end,
        poster_url=row.get('thumbnailUrl') or None,
        category=None if season else 'Movies',
        episode_title=episode_title,
        season=season,
        episode=episode,
        program_type='episode' if season else 'movie',
    )


_GUIDE_EPISODE_RE = re.compile(r'^S(\d+)\s+E(\d+)\s+(.*)$')


def _split_guide_title(raw: str) -> tuple[str, str | None, int | None, int | None]:
    """The guide gives one string per slot: a movie's name, or for an episode
    'S04 E91 NCIS 421 of NCIS' / 'S02 E5 of A-Team, The of A-Team, The' —
    season, an episode number that's often counted across the whole series
    (NCIS 4x21 is E91), a placeholder episode title, then ' of <series>'."""
    m = _GUIDE_EPISODE_RE.match(raw)
    if not m or ' of ' not in f' {m.group(3)}':
        return raw, None, None, None
    season, number, rest = int(m.group(1)), int(m.group(2)), f' {m.group(3)}'
    middle, series = rest.rsplit(' of ', 1)
    series = _the_first(series.strip())
    middle = _the_first(middle.strip().removeprefix('of ').strip())
    episode = None
    code = re.fullmatch(rf'{re.escape(series)}\s+(\d{{3,4}})', middle)
    if code and code.group(1).startswith(str(season)) and len(code.group(1)) - len(str(season)) == 2:
        episode = int(code.group(1)[len(str(season)):])  # '421' → 4x21
    elif number <= 30:
        episode = number
    episode_title = None if not middle or middle == series or code else middle
    return series or raw, episode_title, season, episode


def _the_first(name: str) -> str:
    """'A-Team, The' → 'The A-Team'."""
    for article in ('The', 'A', 'An'):
        if name.endswith(f', {article}'):
            return f'{article} {name[:-len(article) - 2]}'
    return name

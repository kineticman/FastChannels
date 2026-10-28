"""NFL — NFL Channel, NFL Network, NFL RedZone and live games from the NFL app.

Everything plays as clear HLS from NFL's own Anvato/Lura CDN (dcs-live.mp.lura.live,
MPEG-TS segments, no EXT-X-KEY) — including games carried on FOX/CBS/NBC/Prime.
Checked 2026-10-17: NFL Channel anonymously, NFL Network (1080p) through a
Spectrum TV-provider link. No DRM bridge involved.

Device identity: the NFL app mints device tokens from a client key/secret plus a
base64 deviceInfo blob (POST identity/v3/token). Two client identities matter —
the phone app and the Android TV app. NFL+ (base) only grants live games to
phones and tablets (the asset's nfl_plus requirements list formFactor
MOBILE_PHONE/TABLET), so games are played with the phone identity; the linear
networks use the TV one. Tokens last ~1 hour (expiresIn is an epoch).

Sign-in is NFL's activation code, done by the user on any device at
nfl.com/activate (→ id.nfl.com/account/activate):
  GET utilities/v1/regcode → PUT keystore/v1/mvpd/<code> describing the link →
  the user enters the code → GET keystore/v1/mvpd/<code> returns the result.
  - TV provider: the keystore entry gets {idp, userId, uuid}. The provider
    sign-in goes through Synacor (not Adobe Pass).
  - NFL account (NFL+): OIDC PKCE; the keystore gets an auth_code, exchanged at
    accounts/v1/auth/oidc/token, then /userinfo gives the uid.
Neither result is a token: the claims (mvpd userId/uuid, oidc token + uid) are
sent on every identity/v3/token/refresh and come back baked into the JWT
(`networks` for a TV provider, `plans` for NFL+). So a lost refresh token is
recoverable — mint a fresh device token and refresh it with the same claims.

Playback: POST play/v1/asset/<externalId> returns {accessUrl}. Every airing of a
linear network tunes that network's live feed (any NFL Network airing id, even
one days ahead, returned the same Anvato channel), so the linear channels keep
one cached airing id. A 403 carries decoration.plans explaining which plan
would allow it.
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
import secrets
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests

from ..gracenote_map import resolve_gracenote
from ..tve.adobe_pass import TVENotAuthorizedError
from .base import BaseScraper, ChannelData, ConfigField, ProgramData, EVENT_CHANNEL_TAG

logger = logging.getLogger(__name__)

API = 'https://api.nfl.com'
ACTIVATE_URL = 'https://www.nfl.com/activate'
_UA = 'okhttp/4.11.0'

# The NFL Android apps' identities (same values EPlusTV uses).
_PHONE_CLIENT = ('0q1p5KSsvtu2VJfk5vQ5EdpmNNGrCGU7', 'qGhEv1RtI2SfRQOe')
_TV_CLIENT = ('A3b74wOiSDMrhJKeXAEIqgRICBiBNo7o', 'uoCyykyUwDbfQZr2')
_OIDC_CLIENT_ID = 'mIlix3XYROBfTk1z83pcqPIRzhYYKHk2'
_OIDC_REDIRECT = 'https://id.nfl.com/account/activate'
_OIDC_SCOPE = 'openid profile email nfl_complete'
_PHONE_INFO = {
    'capabilities': {}, 'ctvDevice': 'AndroidTV', 'diskCapacity': 118550667264,
    'displayHeight': 2340, 'displayWidth': 1080, 'idfv': 'unknown', 'isLocationEnabled': True,
    'manufacturer': 'Google', 'memory': 7824363520, 'model': 'Pixel_5', 'networkType': 'wifi',
    'osName': 'Android', 'osVersion': '13', 'vendor': 'google', 'version': 'redfin',
    'versionName': '59.0.29.1346644',
}
_TV_INFO = {
    'capabilities': {}, 'ctvDevice': 'AndroidTV', 'diskCapacity': 6964142080,
    'displayHeight': 2160, 'displayWidth': 3840, 'idfv': 'unknown', 'isLocationEnabled': True,
    'manufacturer': 'onn', 'memory': 2063581184, 'model': 'onn. 4K Streaming Box',
    'networkType': 'wifi', 'osName': 'Android', 'osVersion': '12', 'vendor': 'onn',
    'version': 'goldfish', 'versionName': '18.0.65.101385778',
}
_IDENTITIES = {'phone': (_PHONE_CLIENT, _PHONE_INFO), 'tv': (_TV_CLIENT, _TV_INFO)}

SCHEME = 'nfl://channel/'
GAME_SCHEME = 'nfl://game/'
GAME_CHANNEL_PREFIX = 'game:'

# callSign → channel. Gracenote ids are the stations' own.
NETWORKS = {
    'NFLDIGITAL1_OO_v3': {'name': 'NFL Channel', 'gracenote': '121705',
                          'logo': 'https://tmsimg.fancybits.co/assets/s121705_ll_h15_aa.png'},
    'NFLNETWORK': {'name': 'NFL Network', 'gracenote': '45399',
                   'logo': 'https://tmsimg.fancybits.co/assets/s45399_ll_h15_aa.png'},
    'NFLNRZ': {'name': 'NFL RedZone', 'gracenote': '65025',
               'logo': 'https://tmsimg.fancybits.co/assets/s65025_ll_h3_aa.png'},
}
NFL_CHANNEL = 'NFLDIGITAL1_OO_v3'
NFL_SHIELD = 'https://static.www.nfl.com/image/upload/v1554321393/league/nvfr7ogywskqrfaiu38m.svg'

_PLUS_PLANS = {'NFL_PLUS', 'NFL_PLUS_PREMIUM', 'NFL_PLUS_PLUS'}
_REDZONE_PLANS = {'NFL_PLUS_PREMIUM', 'NFL_PLUS_PLUS'}

_TOKEN_MARGIN = 5 * 60
_SCHEDULE_TTL = 10 * 60
_EPG_DAYS = 3
_GAME_LOOKAHEAD_HOURS = 48
GAME_END_GRACE_SECONDS = 2 * 3600
_LINK_TIMEOUT = 600
_LINK_POLL_SECONDS = 3.0

LINK_STATUS_KEY = 'nfl:link:status'
LINK_KINDS = ('tve', 'account')
# Config keys each sign-in leaves behind; signing out removes them.
LINK_CONFIG_KEYS = {
    'tve': ('mvpd_idp', 'mvpd_user_id', 'mvpd_uuid', 'mvpd_name', 'mvpd_linked_at'),
    'account': ('oidc_access_token', 'nfl_uid', 'nfl_email', 'nfl_linked_at'),
}
TOKEN_CONFIG_KEYS = tuple(f'{ident}_{k}' for ident in _IDENTITIES
                          for k in ('access_token', 'refresh_token', 'expires_at'))


class NFLAuthError(RuntimeError):
    pass


# ── tokens ───────────────────────────────────────────────────────────────────

def _b64_info(info: dict) -> str:
    return base64.b64encode(json.dumps(info).encode()).decode()


def jwt_claims(token: str | None) -> dict:
    try:
        part = (token or '').split('.')[1]
        return json.loads(base64.urlsafe_b64decode(part + '=' * (-len(part) % 4)))
    except (IndexError, ValueError):
        return {}


def link_claims(config: dict) -> dict:
    """The sign-in claims identity/v3/token/refresh carries into the JWT."""
    claims = {}
    if config.get('mvpd_user_id'):
        claims.update(mvpdIdp=config['mvpd_idp'], mvpdUserId=config['mvpd_user_id'],
                      mvpdUUID=config['mvpd_uuid'])
    if config.get('nfl_uid'):
        claims.update(oidcAccessToken=config['oidc_access_token'], uid=config['nfl_uid'])
    return claims


def _identity_body(ident: str, device_id: str) -> dict:
    (key, secret), info = _IDENTITIES[ident]
    return {'clientKey': key, 'clientSecret': secret, 'deviceId': device_id,
            'deviceInfo': _b64_info(info), 'networkType': 'wifi'}


def _post_identity(path: str, body: dict) -> dict:
    r = requests.post(f'{API}/identity/v3/{path}', json=body, timeout=20, headers={'User-Agent': _UA})
    if not r.ok:
        raise NFLAuthError(f'NFL {path}: HTTP {r.status_code} {r.text[:200]}')
    data = r.json()
    if not data.get('accessToken'):
        raise NFLAuthError(f'NFL {path}: no accessToken')
    return data


def mint_token(ident: str, device_id: str, claims: dict | None = None) -> dict:
    """A fresh device token, refreshed with `claims` when there are any."""
    data = _post_identity('token', _identity_body(ident, device_id))
    if claims:
        data = _post_identity('token/refresh', {**_identity_body(ident, device_id),
                                                'refreshToken': data['refreshToken'], **claims})
    return data


def refresh_token(ident: str, device_id: str, refresh: str, claims: dict) -> dict:
    return _post_identity('token/refresh', {**_identity_body(ident, device_id),
                                            'refreshToken': refresh, **claims})


def _token_config(ident: str, data: dict) -> dict:
    return {f'{ident}_access_token': data['accessToken'],
            f'{ident}_refresh_token': data.get('refreshToken'),
            f'{ident}_expires_at': int(data.get('expiresIn') or time.time() + 3600)}


def _active(plan: dict) -> bool:
    status = (plan.get('status') or '').upper()
    if status == 'ACTIVE':
        return True
    if status == 'CANCELLED' and plan.get('expirationDate'):
        try:
            return datetime.fromisoformat(plan['expirationDate'][:10]) > datetime.now()
        except ValueError:
            return False
    return False


def active_plans(claims: dict) -> set[str]:
    return {(p.get('plan') or '').upper() for p in claims.get('plans') or [] if _active(p)}


# ── activation-code sign-in ──────────────────────────────────────────────────

def _code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip('=')


def start_link(kind: str, device_id: str) -> dict:
    """Get an activation code and describe the link it makes. Returns the ctx
    run_link polls with ({kind, code, token, verifier?})."""
    if kind not in LINK_KINDS:
        raise ValueError(f'unknown NFL sign-in {kind!r}')
    token = mint_token('phone', device_id)['accessToken']
    headers = {'Authorization': 'Bearer ' + token, 'User-Agent': _UA, 'Content-Type': 'application/json'}
    r = requests.get(f'{API}/utilities/v1/regcode', headers=headers, timeout=20)
    code = (r.json() or {}).get('regCode') if r.ok else None
    if not code:
        raise NFLAuthError(f'NFL activation: no code (HTTP {r.status_code})')
    body = {'ctvDevice': 'AndroidTV', 'deviceId': device_id, 'visitorMid': secrets.token_hex(16)}
    ctx = {'kind': kind, 'code': code, 'token': token}
    if kind == 'tve':
        body.update(idp='TV_PROVIDER', nflAccount=False, platform='ctv')
    else:
        verifier = secrets.token_urlsafe(48)
        ctx['verifier'] = verifier
        body.update(client_id=_OIDC_CLIENT_ID, code_challenge=_code_challenge(verifier),
                    code_challenge_method='S256', flow='oidc_pkce_v1', issued_at=int(time.time() * 1000),
                    nonce=secrets.token_hex(16), platform='androidtv', redirect_uri=_OIDC_REDIRECT,
                    regCode=code, scope=_OIDC_SCOPE, ttl_sec=_LINK_TIMEOUT)
    r = requests.put(f'{API}/keystore/v1/mvpd/{code}', params={'ttl': _LINK_TIMEOUT * 1000},
                     json=body, headers=headers, timeout=20)
    if not r.ok:
        raise NFLAuthError(f'NFL activation: code setup failed (HTTP {r.status_code})')
    return ctx


def poll_link(ctx: dict) -> dict | None:
    """None while the code hasn't been entered; the config keys to save once it has."""
    r = requests.get(f'{API}/keystore/v1/mvpd/{ctx["code"]}', timeout=20,
                     headers={'Authorization': 'Bearer ' + ctx['token'], 'User-Agent': _UA})
    if r.status_code == 404:
        raise TimeoutError('activation code expired')
    if not r.ok:
        return None
    entry = r.json() or {}
    now = int(time.time())
    if ctx['kind'] == 'tve':
        if not entry.get('userId'):
            return None
        return {'mvpd_idp': entry.get('idp'), 'mvpd_user_id': entry['userId'], 'mvpd_uuid': entry.get('uuid'),
                'mvpd_name': entry.get('mvpdName') or entry.get('idp'), 'mvpd_linked_at': now}
    if not entry.get('auth_code'):
        return None
    r = requests.post(f'{API}/accounts/v1/auth/oidc/token', timeout=20, data={
        'client_id': _OIDC_CLIENT_ID, 'code': entry['auth_code'], 'code_verifier': ctx['verifier'],
        'grant_type': 'authorization_code', 'redirect_uri': _OIDC_REDIRECT, 'scope': _OIDC_SCOPE,
    }, headers={'Authorization': 'Bearer ' + entry['auth_code'], 'User-Agent': _UA})
    oidc = (r.json() or {}).get('access_token') if r.ok else None
    if not oidc:
        raise NFLAuthError(f'NFL account sign-in: token exchange failed (HTTP {r.status_code})')
    r = requests.post(f'{API}/accounts/v1/auth/oidc/userinfo', data={'code': oidc}, timeout=20,
                      headers={'Authorization': 'Bearer ' + oidc, 'User-Agent': _UA})
    info = r.json() if r.ok else {}
    if not info.get('sub'):
        raise NFLAuthError(f'NFL account sign-in: no user id (HTTP {r.status_code})')
    return {'oidc_access_token': oidc, 'nfl_uid': info['sub'], 'nfl_email': info.get('email'),
            'nfl_linked_at': now}


def link_status(ctx: dict, state: str, message: str = '') -> dict:
    return {'state': state, 'message': message, 'kind': ctx['kind'], 'code': ctx['code'], 'url': ACTIVATE_URL}


def run_link(ctx: dict) -> None:
    """RQ job: wait for the user to enter ctx's code, then save the sign-in onto
    the nfl source and rescrape. Quits quietly once LINK_STATUS_KEY names a
    different code (a newer request) or is gone (cancelled)."""
    import redis

    from app.worker import flask_app
    from ..extensions import db
    from ..models import Source

    with flask_app.app_context():
        r = redis.from_url(flask_app.config['REDIS_URL'])

        def superseded() -> bool:
            raw = r.get(LINK_STATUS_KEY)
            return not raw or json.loads(raw).get('code') != ctx['code']

        def set_status(state: str, message: str = '') -> None:
            if not superseded():
                r.setex(LINK_STATUS_KEY, 720, json.dumps(link_status(ctx, state, message)))

        deadline = time.monotonic() + _LINK_TIMEOUT
        result = None
        try:
            while time.monotonic() < deadline and not superseded():
                result = poll_link(ctx)
                if result:
                    break
                time.sleep(_LINK_POLL_SECONDS)
        except TimeoutError:
            pass
        except Exception as exc:  # noqa: BLE001
            logger.warning('[nfl] %s sign-in failed: %s', ctx['kind'], exc)
            set_status('error', str(exc)[:300])
            return
        if not result:
            if superseded():
                logger.info('[nfl] activation code %s stopped', ctx['code'])
            else:
                set_status('expired', 'The code expired before it was entered.')
            return

        src = Source.query.filter_by(name='nfl').first()
        if not src:
            set_status('error', 'NFL source no longer exists.')
            return
        cfg = dict(src.config or {})
        cfg.update(result)
        # The next use re-mints both tokens so the new claims are in them.
        for key in TOKEN_CONFIG_KEYS:
            cfg.pop(key, None)
        src.config = cfg
        db.session.commit()
        logger.info('[nfl] signed in (%s)', ctx['kind'])
        set_status('success')
        if src.is_enabled:
            from ..routes.tasks import trigger_scrape
            trigger_scrape('nfl', force_full=True)


# ── scraper ──────────────────────────────────────────────────────────────────

def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def _item_window(item: dict) -> tuple[datetime | None, datetime | None]:
    start = _parse_time(item.get('startTime'))
    end = _parse_time(item.get('endTime'))
    if start and not end and item.get('duration'):
        end = start + timedelta(seconds=int(item['duration']))
    return start, end


def game_end_timestamp(raw_url: str | None) -> int | None:
    """Scheduled end embedded in a game channel's opaque stream URL."""
    m = re.search(r'[?&]end=(\d+)', raw_url or '')
    return int(m.group(1)) if m else None


class NFLScraper(BaseScraper):
    source_name = 'nfl'
    display_name = 'NFL'
    source_category = 'premium'
    is_premium = True
    scrape_interval = 360
    stream_audit_enabled = False
    # The full schedule (~300 items) is only read when a linear channel's
    # cached airing id runs out, so tunes don't load it.
    LAZY_CACHE_KEYS = frozenset({'nfl_livestreams'})

    config_schema = [
        ConfigField('include_games', 'Include live game channels', field_type='toggle', default='false',
                    help_text='Adds each live game your sign-in can play as a temporary channel.'),
    ]

    # ── auth ──

    def _device_id(self) -> str:
        if not self.config.get('device_id'):
            self._update_config('device_id', str(uuid.uuid4()))
        return self.config['device_id']

    def _token(self, ident: str) -> str:
        """A current access token for `ident`, carrying this source's sign-ins."""
        access = self.config.get(f'{ident}_access_token')
        if access and time.time() < float(self.config.get(f'{ident}_expires_at') or 0) - _TOKEN_MARGIN:
            return access
        claims = link_claims(self.config)
        data = None
        if self.config.get(f'{ident}_refresh_token'):
            try:
                data = refresh_token(ident, self._device_id(), self.config[f'{ident}_refresh_token'], claims)
            except (NFLAuthError, requests.RequestException) as exc:
                logger.info('[nfl] %s token refresh failed, minting a new one: %s', ident, exc)
        if data is None:
            data = mint_token(ident, self._device_id(), claims)
        for key, value in _token_config(ident, data).items():
            self._update_config(key, value)
        return data['accessToken']

    def _claims(self) -> dict:
        return jwt_claims(self._token('phone'))

    def pre_run_setup(self) -> None:
        for ident in _IDENTITIES:
            self._token(ident)

    def _entitled_networks(self, claims: dict) -> set[str]:
        """Linear channels this sign-in can play."""
        plans = active_plans(claims)
        nets = set((claims.get('networks') or {}).keys())
        out = {NFL_CHANNEL}
        if 'NFLN' in nets or plans & _PLUS_PLANS:
            out.add('NFLNETWORK')
        if 'NFLRZ' in nets or plans & _REDZONE_PLANS:
            out.add('NFLNRZ')
        return out

    # ── schedule ──

    def _livestreams(self) -> list[dict]:
        cached = self.load_lazy_cache_key('nfl_livestreams') or {}
        if cached.get('items') is not None and time.time() - float(cached.get('at') or 0) < _SCHEDULE_TTL:
            return cached['items']
        r = self.session.get(f'{API}/experience/v1/livestreams', timeout=30,
                             headers={'Authorization': 'Bearer ' + self._token('phone'), 'User-Agent': _UA})
        r.raise_for_status()
        items = ((r.json() or {}).get('data') or {}).get('items') or []
        self._update_cache('nfl_livestreams', {'at': time.time(), 'items': items})
        return items

    def _nfl_channel_airings(self, days: int = _EPG_DAYS) -> list[dict]:
        now = datetime.now(timezone.utc)
        fmt = lambda d: d.strftime('%Y-%m-%dT%H:%M:%S.000Z')  # noqa: E731
        r = self.session.get(f'{API}/live/v1/nflchannel', timeout=30,
                             params={'starttime': fmt(now - timedelta(hours=6)),
                                     'endtime': fmt(now + timedelta(days=days))},
                             headers={'Authorization': 'Bearer ' + self._token('phone'), 'User-Agent': _UA})
        r.raise_for_status()
        return (r.json() or {}).get('items') or []

    def _game_playable(self, item: dict, claims: dict) -> bool:
        auth = item.get('authorizations') or {}
        plans = active_plans(claims)
        if 'NFL_PLUS_PLUS' in plans and 'nfl_plus_plus' in auth:
            return True
        dma = str(claims.get('dmaCode') or '')
        in_market = dma in (item.get('dmaCodes') or [])
        if not in_market:
            return False
        if plans & _PLUS_PLANS and ('nfl_plus' in auth or 'nfl_plus_premium' in auth):
            return True
        nets = set((claims.get('networks') or {}).keys())
        return bool(('mvpd' in auth or 'mvpd_synacor' in auth) and nets & set(item.get('networks') or []))

    def _games(self, claims: dict) -> list[dict]:
        """Live games in the next two days this sign-in can play, one per game.
        Games on NFL Network's own feed are left to that channel."""
        now = datetime.now(timezone.utc)
        horizon = now + timedelta(hours=_GAME_LOOKAHEAD_HOURS)
        grace = timedelta(seconds=GAME_END_GRACE_SECONDS)
        found: dict[tuple, dict] = {}
        for item in self._livestreams():
            if (item.get('contentType') != 'GAME' or item.get('callSign') in NETWORKS
                    or 'en' not in (item.get('language') or ['en']) or item.get('streamType') == 'alternative'):
                continue
            start, end = _item_window(item)
            if not start or not end or end + grace <= now or start >= horizon:
                continue
            if not self._game_playable(item, claims):
                continue
            found.setdefault((item.get('title'), item.get('startTime')), item)
        return sorted(found.values(), key=lambda i: (i.get('startTime') or '', i.get('externalId') or ''))

    def _games_enabled(self) -> bool:
        return str(self.config.get('include_games', 'false')).strip().lower() in ('1', 'true', 'yes', 'on')

    # ── channels / EPG ──

    def fetch_channels(self) -> list[ChannelData]:
        claims = self._claims()
        entitled = self._entitled_networks(claims)
        channels = [
            ChannelData(
                source_channel_id=call_sign,
                name=meta['name'],
                stream_url=SCHEME + call_sign,
                logo_url=meta['logo'],
                slug=f'nfl-{meta["name"].lower().replace(" ", "-")}',
                category='Sports',
                stream_type='hls',
                guide_key=call_sign,
                gracenote_id=resolve_gracenote('nfl', upstream_id=meta['gracenote'], lookup_key=call_sign),
            )
            for call_sign, meta in NETWORKS.items() if call_sign in entitled
        ]
        self._game_items = []
        if self._games_enabled():
            self._game_items = self._games(claims)
            # A complete game inventory retires finished games on this scrape.
            self.hide_missing_event_channels = True
            for item in self._game_items:
                start, end = _item_window(item)
                network = (item.get('networks') or [''])[0]
                title = item.get('title') or 'NFL Game'
                channels.append(ChannelData(
                    source_channel_id=GAME_CHANNEL_PREFIX + item['externalId'],
                    name=f'NFL - {title}' + (f' ({network})' if network else ''),
                    stream_url=f'{GAME_SCHEME}{item["externalId"]}?net={network}&end={int(end.timestamp())}',
                    logo_url=item.get('preferredImage') or NFL_SHIELD,
                    category='Sports',
                    stream_type='hls',
                    guide_key=GAME_CHANNEL_PREFIX + item['externalId'],
                    tags=[EVENT_CHANNEL_TAG, 'NFL Games'],
                    sport='Football',
                    league='NFL',
                    gracenote_mode='off',
                    retire_at=end + timedelta(seconds=GAME_END_GRACE_SECONDS),
                    delete_at=end + timedelta(seconds=GAME_END_GRACE_SECONDS + 24 * 3600),
                ))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {ch.source_channel_id for ch in channels}
        programs: list[ProgramData] = []
        items = []
        try:
            items = self._livestreams()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[nfl] schedule unavailable: %s', exc)
        if NFL_CHANNEL in wanted:
            try:
                items = items + [{**i, 'callSign': NFL_CHANNEL} for i in self._nfl_channel_airings()]
            except Exception as exc:  # noqa: BLE001
                logger.warning('[nfl] NFL Channel schedule unavailable: %s', exc)
        seen: set[tuple] = set()
        for item in items:
            cid = item.get('callSign')
            if cid not in wanted or item.get('contentType') == 'AUDIO':
                continue
            start, end = _item_window(item)
            if not start or not end or end <= start or (cid, start) in seen:
                continue
            seen.add((cid, start))
            programs.append(self._program(cid, item, start, end))
        for item in getattr(self, '_game_items', []):
            cid = GAME_CHANNEL_PREFIX + item['externalId']
            start, end = _item_window(item)
            if cid in wanted and start and end:
                programs.append(self._program(cid, item, start, end))
        return programs

    @staticmethod
    def _program(cid: str, item: dict, start: datetime, end: datetime) -> ProgramData:
        title = (item.get('title') or 'NFL').strip()
        episode = (item.get('episodeTitle') or '').strip()
        # NFL Channel titles are "<show>: <episodeTitle>", and episodeTitle may
        # itself start with the show: "The NFL Report: The NFL Report: Time to
        # Panic?" → show "The NFL Report", episode "Time to Panic?".
        if episode and title.endswith(': ' + episode):
            title = title[:-len(episode) - 2].strip()
            if episode.startswith(title + ': '):
                episode = episode[len(title) + 2:].strip()
        is_game = item.get('contentType') == 'GAME'
        return ProgramData(
            source_channel_id=cid,
            title=title,
            start_time=start,
            end_time=end,
            description=item.get('description') or None,
            poster_url=item.get('preferredImage'),
            category='Sports Event;Football;NFL' if is_game else 'Sports;Football',
            episode_title=episode if episode and episode != title else None,
            is_live=item.get('broadcastAiringType') == 'LIVE' or (is_game and item.get('broadcastAiringType') != 'REAIR'),
            episode_id=item.get('externalId'),
        )

    # ── playback ──

    def _airing_for(self, call_sign: str) -> str:
        """Any airing id of a linear channel; each one tunes the live feed."""
        ids = dict(self.cache.get('nfl_airing_ids') or {})
        cached = ids.get(call_sign) or {}
        if cached.get('id') and time.time() < float(cached.get('until') or 0):
            return cached['id']
        if call_sign == NFL_CHANNEL:
            items = self._nfl_channel_airings(days=1)
        else:
            items = [i for i in self._livestreams() if i.get('callSign') == call_sign and i.get('contentType') != 'AUDIO']
        now = datetime.now(timezone.utc)
        # Prefer the airing on now; keep it until it ends (at most a day).
        best, until = None, now + timedelta(minutes=10)
        for item in items:
            start, end = _item_window(item)
            if start and end and start <= now < end:
                best, until = item, min(end, now + timedelta(days=1))
                break
            if not best:
                best = item
        if not best:
            raise RuntimeError(f'NFL: nothing scheduled on {NETWORKS[call_sign]["name"]} right now')
        ids[call_sign] = {'id': best['externalId'], 'until': until.timestamp()}
        self._update_cache('nfl_airing_ids', ids)
        return best['externalId']

    def _play(self, external_id: str, ident: str, network: str | None, label: str) -> str:
        body = {}
        if self.config.get('mvpd_user_id'):
            body = {'idp': self.config['mvpd_idp'], 'mvpdUUID': self.config['mvpd_uuid'],
                    'mvpdUserId': self.config['mvpd_user_id'], 'networks': network or 'NFLN'}
        r = self.session.post(f'{API}/play/v1/asset/{external_id}', json=body, timeout=30, headers={
            'Authorization': 'Bearer ' + self._token(ident), 'User-Agent': _UA, 'Content-Type': 'application/json'})
        if r.status_code == 403:
            raise TVENotAuthorizedError(f'NFL: this sign-in does not include {label}')
        if not r.ok:
            raise RuntimeError(f'NFL playback {label}: HTTP {r.status_code} {r.text[:200]}')
        url = (r.json() or {}).get('accessUrl')
        if not url:
            raise RuntimeError(f'NFL playback {label}: no stream URL')
        return url

    def resolve(self, raw_url: str) -> str:
        if raw_url.startswith(GAME_SCHEME):
            external_id = raw_url[len(GAME_SCHEME):].split('?', 1)[0]
            if not re.fullmatch(r'[A-Za-z0-9_-]{8,64}', external_id):
                raise ValueError('Invalid NFL game ID')
            end = game_end_timestamp(raw_url)
            if end is not None and time.time() >= end + GAME_END_GRACE_SECONDS:
                from .base import StreamDeadError
                raise StreamDeadError('NFL game has ended')
            m = re.search(r'[?&]net=([^&]*)', raw_url)
            return self._play(external_id, 'phone', (m.group(1) if m else '') or None, 'this game')
        call_sign = raw_url.removeprefix(SCHEME)
        if call_sign not in NETWORKS:
            raise ValueError(f'Unsupported NFL stream URL: {raw_url}')
        network = {'NFLNETWORK': 'NFLN', 'NFLNRZ': 'NFLRZ'}.get(call_sign)
        return self._play(self._airing_for(call_sign), 'tv', network, NETWORKS[call_sign]['name'])

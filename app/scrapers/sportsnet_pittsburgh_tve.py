"""
sportsnet_pittsburgh_tve.py — SportsNet Pittsburgh and SportsNet Pittsburgh+
from watch.sportsnetpittsburgh.com, with an SNP account plus a TV-provider
sign-in. Scoped 2026-10-27 from a signed-in HAR that played both channels
(dev/hitlist/hars/watch.sportsnetpittsburgh.com.har, notes in
dev/hitlist/viewlift_rsn_scoping_2026-10-27.md).

Key facts established by probing:

- The site is DICE/IMG ("Vesper"), the same platform as sundance_tve.py, on
  NESN's realm: API dce-frontoffice.imggaming.com with the web app's shared
  x-api-key and Realm: dce.nesn (streams come from nesn-live-us.akamaized.net).
  A guest token (/v2/login/guest/checkin) reads the live event list
  (SPORTSNET PITTSBURGH 261049, SPORTSNET PITTSBURGH + 261240) and the guide
  (channels 981 and 984, a full 7 days: maxProgrammeWindow 604800s).
- TV-provider access is Adobe Pass v2 under requestor ATTSPORTSNET (resource
  ATTSportsPT), driven by DICE, so it needs a signed-in SNP account
  (email/password, /v2/login). The web app registers an Adobe client with the
  ADOBE_DCR software statement from the realm settings, uses the account's
  profile id (the token's pro.id) as the Adobe device id, signs in through
  /api/v2/ATTSPORTSNET/sessions, then POST /v3/adobe/token {requestorId,
  clientId, clientSecret, accessToken, mvpd} → a DICE session whose token
  holds licence 3932 "Access Via TV Provider" in ent.iai. A password login
  alone only lists it as available (ent.aai).
- DICE tokens: a 10-minute access token and a refresh token,
  POST /v2/token/refresh {refreshToken} with the old token as Bearer; shared
  through Redis with one refresher at a time, as for Sundance.
- Playback: POST /v5/manifest/event/<id>?includePlaybackDetails=URL with the
  player's mediaCapabilities → streams[0] {url, protocol, drm {url, jwtToken}}.
  An Ohio Spectrum account got accessLevel GRANTED for both channels — no
  market lock seen, unlike the ViewLift RSNs. SNP came back as HLS (CMAF,
  SAMPLE-AES CBC, Widevine/PlayReady/FairPlay, MediaTailor ads), SNP+ as DASH
  (CTR, Widevine/PlayReady). No clear rendition, so bridge only. The master
  URL's hdnea token lasts ~40 seconds; Akamai then moves it into the path
  (HLS child playlists carry hdntl, the DASH manifest 301s to an hdntl/ path),
  good for ~24 hours, so relative segment URLs keep working.
- License: POST shield-drm.onvesper.com/api/v2/license with
  Authorization: Bearer <drm.jwtToken> and X-DRM-INFO: base64
  {"system": "com.widevine.alpha"} — answered 200 in the HAR.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests

from .base import BaseScraper, ChannelData, ConfigField, ProgramData
from ..gracenote_map import resolve_gracenote
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import TVEAuthError, TVENotAuthorizedError, refresh_adobe_client_token

logger = logging.getLogger(__name__)

SCHEME = 'snp://'
NETWORK_KEY = 'snp'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'
SITE = 'https://watch.sportsnetpittsburgh.com'
API = 'https://dce-frontoffice.imggaming.com/api'
API_KEY = '857a1e5d-e35e-4fdf-805b-a87b6f8364bf'
REALM = 'dce.nesn'
ADOBE_API = 'https://api.auth.adobe.com'
REQUESTOR_ID = 'ATTSPORTSNET'
REDIRECT_URL = f'{SITE}/providers/success?provider=ADOBE_DCR'
_LICENSE_URL = 'https://shield-drm.onvesper.com/api/v2/license'
_ADOBE_LICENCE = 3932  # "Access Via TV Provider"

# Our id -> (name, live-list title, guide channel, event id when the live list
# can't be read, stream type the event serves, logo).
_CHANNELS = {
    'snp': ('SportsNet Pittsburgh', 'sportsnet pittsburgh', '981', '261049', 'hls',
            'https://img.dge-prod.dicelaboratory.com/original/2024/02/26210119-srconrdvtfraeonk.png'),
    'snp-plus': ('SportsNet Pittsburgh+', 'sportsnet pittsburgh +', '984', '261240', 'dash',
                 'https://img.dge-prod.dicelaboratory.com/original/2024/04/03092604-sfl52mijuevnkgoy.png'),
}

_SESSION_KEY = 'snp_tve:session'
_LOCK_KEY = 'snp_tve:refresh_lock'
_TOKEN_MARGIN = 60
_STREAM_MAX_AGE = 20  # the master URL's hdnea lasts ~40s from issue
_GUIDE_DAYS = 7

# What the web player offers /v5/manifest (its Widevine entry; we always bridge).
_MEDIA_CAPABILITIES = [
    {'protocols': ['HLS', 'DASH'], 'audioCodecs': ['aac'], 'videoCodecs': ['h264']},
    {'keySystem': 'WIDEVINE', 'robustness': 'software', 'protocols': ['HLS', 'DASH'], 'audioCodecs': ['aac'],
     'encryptionMode': ['CBC', 'CTR'], 'videoCodecs': ['h264']},
]


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


def profile_id(token: str) -> str:
    """The account's profile id, which the web app uses as the Adobe device id."""
    return (claims(token).get('pro') or {}).get('id') or ''


def guest_token() -> str:
    r = requests.post(f'{API}/v2/login/guest/checkin', json={}, headers=headers(), timeout=20)
    token = _json(r).get('authorisationToken')
    if not token:
        raise RuntimeError(f'SportsNet Pittsburgh: guest check-in failed: HTTP {r.status_code}')
    return token


def login(email: str, password: str) -> dict:
    """/v2/login → {authorisationToken, refreshToken} for the SNP account."""
    r = requests.post(f'{API}/v2/login', json={'id': email, 'secret': password}, headers=headers(guest_token()),
                      timeout=20)
    data = _json(r)
    if not data.get('authorisationToken'):
        raise TVEAuthError(f'SportsNet Pittsburgh: the SNP email/password sign-in failed (HTTP {r.status_code} '
                           f'{_reason(r)}).')
    return data


def software_statement() -> str:
    r = requests.get(f'{API}/v2/realm-settings/domain/watch.sportsnetpittsburgh.com', headers=headers(), timeout=20)
    for provider in _json(r).get('authenticationProviders') or []:
        if (provider.get('name') == 'ADOBE_DCR' and provider.get('requestorId') == REQUESTOR_ID
                and provider.get('softwareStatement')):
            return provider['softwareStatement']
    raise RuntimeError(f'SportsNet Pittsburgh: no Adobe software statement in the realm settings '
                       f'(HTTP {r.status_code}).')


def adobe_client(device_id: str, saved: dict | None = None):
    """The Adobe v2 client for this account's profile (Adobe ties a sign-in to
    the client and the device id, so a re-sign-in reuses the saved client)."""
    from .nbc_tve import AdobePassV2Client
    saved = saved or {}
    client = AdobePassV2Client(REQUESTOR_ID, software_statement(), REDIRECT_URL, device_id,
                               client_creds=saved if saved.get('client_id') else None, origin=SITE)
    client.client_id = saved.get('client_id')
    client.client_secret = saved.get('client_secret')
    client.access_token = saved.get('access_token')
    return client


def exchange_adobe(user_token: str, client: dict, access_token: str, mso_id: str) -> dict | None:
    """/v3/adobe/token → the TV-provider DICE session, or None while Adobe
    has no sign-in for this account yet."""
    r = requests.post(f'{API}/v3/adobe/token', headers=headers(user_token), timeout=20, json={
        'requestorId': REQUESTOR_ID, 'clientId': client['client_id'], 'clientSecret': client['client_secret'],
        'accessToken': access_token, 'mvpd': mso_id})
    data = _json(r)
    if data.get('authorisationToken') and data.get('refreshToken'):
        return data
    logger.info('[snp_tve] /v3/adobe/token not ready: HTTP %s %s', r.status_code, _reason(r))
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


class SportsNetPittsburghTVEScraper(BaseScraper):
    """SportsNet Pittsburgh and SNP+ with an SNP account and a TV-provider sign-in."""

    source_name = 'sportsnet_pittsburgh_tve'
    display_name = 'SportsNet Pittsburgh TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True
    license_url = _LICENSE_URL
    all_channels_require_drm_bridge = True
    # Signed in and played both channels through FastChannels Player on test
    # 2026-10-27; a session hasn't been seen through days of renewals yet.
    is_beta = True

    config_schema = [
        ConfigField('email', 'SNP email', placeholder='you@example.com',
                    help_text='A free account from watch.sportsnetpittsburgh.com. Your TV provider is linked to '
                              'it when you sign in.'),
        ConfigField('password', 'SNP password', field_type='password', secret=True),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update(headers())

    # ── channels + guide ─────────────────────────────────────────────────────

    def _events(self) -> dict[str, str]:
        """Our channel id -> its live event id, from the public live list."""
        try:
            r = self.session.get(f'{API}/v2/event/live', params={'rpp': 25}, timeout=20,
                                 headers={'Authorization': f'Bearer {guest_token()}'})
            r.raise_for_status()
            events = (r.json() or {}).get('events') or []
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            logger.warning('[snp_tve] could not read the live list, using the known events: %s', exc)
            events = []
        by_title = {(e.get('title') or '').strip().lower(): str(e.get('id')) for e in events if e.get('id')}
        return {cid: by_title.get(spec[1]) or spec[3] for cid, spec in _CHANNELS.items()}

    def fetch_channels(self) -> list[ChannelData]:
        events = self._events()
        return [
            ChannelData(
                source_channel_id=cid,
                name=name,
                slug=f'snp-tve-{cid}',
                logo_url=logo,
                stream_url=f'{SCHEME}{cid}/{events[cid]}',
                stream_type=stream_type,
                category='Sports',
                language='en',
                country='US',
                guide_key=f'SNP:{guide_channel}',
                gracenote_id=resolve_gracenote(self.source_name, lookup_key=cid),
            )
            for cid, (name, _, guide_channel, _, stream_type, logo) in _CHANNELS.items()
        ]

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        stamp = lambda d: d.strftime('%Y-%m-%dT%H:%M:%S.000Z')  # noqa: E731
        guide_channels = {_CHANNELS[ch.source_channel_id][2]: ch.source_channel_id
                          for ch in channels if ch.source_channel_id in _CHANNELS}
        if not guide_channels:
            return []
        try:
            r = self.session.get(f'{API}/v4/epg/content/programmes', timeout=30,
                                 headers={'Authorization': f'Bearer {guest_token()}'},
                                 params={'categorisedChannelId': list(guide_channels),
                                         'channelId': list(guide_channels),
                                         'from': stamp(now - timedelta(hours=3)),
                                         'to': stamp(now + timedelta(days=_GUIDE_DAYS))})
            r.raise_for_status()
            rows_by_channel = (r.json() or {}).get('channels') or {}
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            logger.warning('[snp_tve] guide fetch failed: %s', exc)
            rows_by_channel = {}
        programs = []
        for guide_channel, cid in guide_channels.items():
            for row in rows_by_channel.get(guide_channel) or []:
                prog = _program(cid, row)
                if prog:
                    programs.append(prog)
        logger.info('[snp_tve] %d guide entries', len(programs))
        return programs

    # ── session ──────────────────────────────────────────────────────────────

    def _auth(self) -> tuple:
        account = tve_account_for(self.source_name)
        auth = ((account.config or {}).get('snp_auth') or {}) if account else {}
        if not auth.get('refresh_token'):
            raise self._sign_in_needed(
                'SportsNet Pittsburgh: not signed in yet — use "Sign in" for SportsNet Pittsburgh under '
                'Settings → TV Everywhere.')
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
        candidates.append(self.cache.get('snp_session') or {})
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
            persist_source_cache_updates(source.id, {'snp_session': sess})
        if self._cache is not None:
            self._cache['snp_session'] = sess

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
            raise RuntimeError('SportsNet Pittsburgh: SNP is rate-limiting sign-in renewals; try again in a few '
                               'minutes.')
        data = _json(resp)
        if data.get('authorisationToken') and has_tv_provider(data['authorisationToken']):
            data.setdefault('refreshToken', sess['refresh'])
            return data
        logger.info('[snp_tve] session renewal refused (HTTP %s %s); signing in again', resp.status_code,
                    _reason(resp))
        return None

    def _relink(self, auth: dict) -> dict:
        """With the renewal gone, sign in with the saved password and ask DICE
        to pick the TV-provider sign-in back up from Adobe — it's bound to the
        Adobe client and the account's profile id, so no new link is needed
        while it lasts."""
        email, password = self.config.get('email') or '', self.config.get('password') or ''
        if not email or not password or not auth.get('client_id'):
            raise self._sign_in_needed(
                'SportsNet Pittsburgh: the sign-in has expired — sign in again under Settings → TV Everywhere.')
        user = login(email, password)
        client = {'client_id': auth['client_id'], 'client_secret': auth.get('client_secret') or ''}
        data = exchange_adobe(user['authorisationToken'], client,
                              refresh_adobe_client_token(client['client_id'], client['client_secret']),
                              auth.get('mso_id') or '')
        if not data or not has_tv_provider(data['authorisationToken']):
            raise self._sign_in_needed(
                f'SportsNet Pittsburgh: the {auth.get("mso_id") or "TV provider"} sign-in has expired — sign in '
                'again under Settings → TV Everywhere.')
        logger.info('[snp_tve] signed back in with the saved password')
        return data

    # ── playback ─────────────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        # Validate entitlement, then hand back the opaque URL: the stream is
        # Widevine either way, and its URL token only lasts ~40 seconds.
        self.resolve(raw_url)
        return raw_url

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported SportsNet Pittsburgh stream URL: {raw_url}')
        cid, _, event = raw_url[len(SCHEME):].strip('/').partition('/')
        if cid not in _CHANNELS or not event:
            raise ValueError(f'Unsupported SportsNet Pittsburgh stream URL: {raw_url}')
        streams = dict(self.cache.get('snp_streams') or {})
        cached = streams.get(cid) or {}
        if (cached.get('event') == event and cached.get('url')
                and time.time() - float(cached.get('at') or 0) < _STREAM_MAX_AGE):
            return cached['url']
        r = self.session.post(f'{API}/v5/manifest/event/{event}', timeout=20,
                              params={'includePlaybackDetails': 'URL', 'displayGeoblocked': 'HIDE'},
                              headers={'Authorization': f'Bearer {self._token()}'},
                              json={'mediaCapabilities': _MEDIA_CAPABILITIES})
        body = _json(r)
        stream = next((s for s in body.get('streams') or [] if s.get('url')), None)
        if not stream:
            if r.status_code == 403 or body.get('accessLevel') == 'DENIED':
                raise TVENotAuthorizedError(
                    f'SportsNet Pittsburgh: SNP refused the stream ({_reason(r) or body.get("accessLevel") or "no access"}).')
            raise RuntimeError(f'SportsNet Pittsburgh: no stream: HTTP {r.status_code} {_reason(r)}')
        drm = stream.get('drm') or {}
        if (stream.get('protocol') or '').lower() != _CHANNELS[cid][4]:
            logger.warning('[snp_tve] %s now serves %s, registered as %s', cid, stream.get('protocol'),
                           _CHANNELS[cid][4])
        streams[cid] = {'event': event, 'url': stream['url'], 'protocol': stream.get('protocol'), 'at': time.time(),
                          'drm_token': drm.get('jwtToken'), 'license': drm.get('url') or _LICENSE_URL}
        self._update_cache('snp_streams', streams)
        return stream['url']

    @staticmethod
    def _stream_for(config: dict, channel_id: str | None) -> dict:
        # resolve() caches it; the license proxy merges source_cache into config.
        return (config.get('snp_streams') or {}).get(channel_id or '') or {}

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        return cls._stream_for(config, channel_id).get('license') or cls.license_url

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        token = cls._stream_for(config, channel_id).get('drm_token')
        headers_ = {'Content-Type': 'application/octet-stream', 'User-Agent': UA, 'Origin': SITE,
                    'Referer': SITE + '/', 'X-DRM-INFO': _drm_info('com.widevine.alpha')}
        if token:
            headers_['Authorization'] = f'Bearer {token}'
        return challenge, headers_

    @staticmethod
    def _sign_in_needed(message: str) -> TVEAuthError:
        from ..tve.signin_notice import mark_signin_needed
        mark_signin_needed(NETWORK_KEY, message)
        return TVEAuthError(message)


def _drm_info(system: str) -> str:
    # Compact, exactly as the web player sends it.
    return base64.b64encode(json.dumps({'system': system}, separators=(',', ':')).encode()).decode()


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
    # Rows read 'World Poker Tour - WPT Choctaw - Part 2': the show, then the episode.
    title, _, episode_title = raw.partition(' - ')
    title, episode_title = title.strip() or raw, episode_title.strip()
    return ProgramData(
        source_channel_id=source_channel_id,
        title=title,
        start_time=start,
        end_time=end,
        description=(row.get('description') or '').strip() or None,
        poster_url=row.get('thumbnailUrl') or None,
        category='Sports',
        episode_title=episode_title if episode_title and episode_title != title else None,
        is_live=row.get('live') == 'LIVE',
    )

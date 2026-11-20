"""MLB (source mlb) — MLB Network, team networks and live MLB.TV games with an MLB account.

Scoped 2026-11-10/15 (dev/mlb/PLAN.md; HARs dev/hitlist/hars/mlb2 for MLB
Network, dev/mlb/mlb2 for a live Mexican Pacific League game).

Sign-in: MLB's Okta device-code flow (since 2026-11-15 the password grant is
refused). POST device/authorize returns a user code for ids.mlb.com/activate;
the token endpoint answers authorization_pending until it's entered, then an
access token plus a refresh token (scope offline_access).

Everything the account can watch comes from its entitlements, read from the
media gateway's initSession:
- MLB Network: MLBN. A TV provider linked once at mlb.com (stored on the MLB
  account as MLBTVMLBNADOBEPASS), MLB+ and MLB.TV all grant it. FastChannels
  never talks to the provider. MLB says the link can expire; the fix is
  re-linking at mlb.com/commerce/mvpd/mlb-network.
- Team networks (SNY, SportsNet LA, Detroit SportsNet): MLB streams these
  24/7 to in-market TV-provider subscribers who link the provider at
  mlb.com/commerce/mvpd/<network>. Same path as MLB Network: an entitlement
  (SNY_121, SNLA_119, DSN), a <NET>_LIVE content collection, then
  initPlaybackSession. Codes and collections are from EPlusTV's MLB handler,
  which carries them; not yet verified here (issue #100).
- Games: MLB.TV's featured list (a public CMS feed) carries the live
  "ad hoc" game streams, tagged with the game's gamePk. On 2026-11-15 an
  account with MLB Network through a provider (no MLB.TV) played those
  Mexican Pacific League games. Their mediaIds are the CMS item ids, not the
  statsapi broadcast mediaIds (those said "Media is not playable" on that
  account; probably the in-season MLB.TV catalog, still to verify).

Playback: initSession → initPlaybackSession(mediaId, adCapabilities []) →
a Fastly URL with a signed token in its path (24 h for MLB Network, 48 h for
games). Clear MPEG-TS HLS up to 1080p60, no headers or cookies needed. The
browser wraps it in Google DAI and sends a heartbeat every 60 s; we do
neither (verified playing without both for a few minutes).
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
from zoneinfo import ZoneInfo

import requests

from ..matchups import matchup_body, matchup_description, matchup_title
from ..tve.adobe_pass import TVEAuthError, TVENotAuthorizedError
from .base import BaseScraper, ChannelData, ConfigField, ProgramData, StreamDeadError, EVENT_CHANNEL_TAG

logger = logging.getLogger(__name__)

OKTA = 'https://ids.mlb.com/oauth2/aus1m088yK07noBfh356/v1'
CLIENT_ID = '0oa3e1nutA1HLzAKG356'
GATEWAY = 'https://media-gateway.mlb.com/graphql'
CMS_LIVE_LIST = 'https://dapi.mlbinfra.com/v2/content/en-us/sel-mlbtv-featured-svod-video-list'
STATSAPI = 'https://statsapi.mlb.com/api/v1'
RELINK_URL = 'https://www.mlb.com/commerce/mvpd/mlb-network'

_CLIENT_VERSION = '1.8.0'
_GATEWAY_HEADERS = {'x-client-name': 'WEB', 'x-client-version': _CLIENT_VERSION,
                    'Origin': 'https://www.mlb.com', 'Referer': 'https://www.mlb.com/',
                    'Content-Type': 'application/json'}

MLBN_CHANNEL_ID = 'mlbn'
MLBN_GRACENOTE = '62081'
MLBN_LOGO = 'https://a.espncdn.com/watchespn/images/web/network_logos/channel_logo_mlb_2x.png'
# mlb.com/network/tv-schedule embeds this page: about two weeks of MLB Network
# listings (show, episode, synopsis, talent, live/premiere flags, show art) in its
# Next.js data stream. The Gracenote guide cache (app/guide_cache.py) is the fallback.
MLBN_SCHEDULE_URL = 'https://www.mlb.com/network/modules/schedule/full'
MLBN_SCHEDULE_CACHE_KEY = 'mlbn_schedule'
_MLBN_SCHEDULE_TTL = 6 * 3600
_MLBN_IMAGE_FORMAT = 'w_800,h_450,c_fill,g_auto,q_auto,f_jpg'
_ET = ZoneInfo('America/New_York')

# Team networks, keyed by channel id. A channel appears when the account has the
# entitlement (the code or the code plus a suffix, e.g. SNY_121); its guide is the
# Gracenote station's, with SNY's own schedule as the fallback.
TEAM_NETWORKS = {
    'sny': {'name': 'SNY', 'entitlement': 'SNY', 'collection': 'SNY_LIVE', 'gracenote': '49603',
            'logo': 'https://tmsimg.fancybits.co/assets/s49603_ll_h9_aa.png?w=360&h=270',
            'link': 'https://www.mlb.com/commerce/mvpd/sny',
            'description': 'SportsNet New York: the Mets, Jets and New York sports, 24/7.'},
    'snla': {'name': 'Spectrum SportsNet LA', 'entitlement': 'SNLA', 'collection': 'SNLA_LIVE', 'gracenote': '87024',
             'logo': 'https://tmsimg.fancybits.co/assets/s87024_ll_h15_aa.png?w=360&h=270',
             'link': 'https://www.mlb.com/commerce/mvpd/getdodgers',
             'description': 'Spectrum SportsNet LA: the Dodgers, 24/7.'},
    'dsn': {'name': 'Detroit SportsNet', 'entitlement': 'DSN', 'collection': 'DSN_LIVE', 'gracenote': '136329',
            'logo': 'https://tmsimg.fancybits.co/assets/s136329_ll_h15_aa.png?w=360&h=270',
            'link': 'https://www.mlb.com/commerce/mvpd/dsn',
            'description': 'Detroit SportsNet: the Tigers and Detroit sports.'},
}
NETWORK_SCHEME = 'mlb://network/'
SNY_SCHEDULE_URL = 'https://production-api.sny.tv/production/api/cms/schedule'
SNY_SCHEDULE_CACHE_KEY = 'sny_schedule'

GAME_SCHEME = 'mlb://game/'
GAME_CHANNEL_PREFIX = 'game:'
# Winter-league games whose streams the featured list carries. Listing them from
# the schedule puts them in the guide two days ahead; the stream itself shows
# up in the list about half an hour before first pitch.
_SCHEDULE_LEAGUES = {132: 'LMP'}  # Liga Mexicana del Pacifico (sportId 17)
_SCHEDULE_SPORT_IDS = '17'
_LEAGUE_NAMES = {'mexican-pacific-league': 'LMP', 'dominican-winter-league': 'LIDOM',
                 'arizona-fall-league': 'AFL'}
# Title labels for statsapi's winter leagues (sportId 17), by league id.
_WINTER_LEAGUE_LABELS = {131: 'LIDOM', 132: 'LMP', 133: 'LBPRC', 135: 'LVBP'}
_LOOKAHEAD_HOURS = 48
_GAME_HOURS = 3.5
_LIVE_EXTENSION = timedelta(minutes=45)
EVENT_END_GRACE_SECONDS = 3600

_ACCESS_MARGIN = 120
_CMS_CACHE_SECONDS = 60
_LINK_TIMEOUT = 600
# Channels DVR gives up on a tuner after 20 s; each play-time call gets this.
_PLAY_TIMEOUT = (4, 8)

LINK_STATUS_KEY = 'mlb:link:status'
_SESSION_KEY = 'mlb:session'
_LOCK_KEY = 'mlb:refresh_lock'
_CMS_KEY = 'mlb:cms_live'
SESSION_CACHE_KEY = 'mlb_session'
SIGNIN_CONFIG_KEYS = ('refresh_token', 'email', 'signed_in_at', 'entitlements', 'entitlements_at', 'zip_code')
_SIGNIN_LOST = 'The MLB sign-in expired or was removed — sign in again.'


class MLBAuthError(TVEAuthError):
    pass


def _redis():
    try:
        import redis
        return redis.from_url(os.environ.get('REDIS_URL', 'redis://localhost:6379/0'), decode_responses=True,
                              socket_timeout=2)
    except Exception:  # noqa: BLE001
        return None


def _claims(jwt: str | None) -> dict:
    try:
        part = (jwt or '').split('.')[1]
        return json.loads(base64.urlsafe_b64decode(part + '=' * (-len(part) % 4)))
    except Exception:  # noqa: BLE001
        return {}


def _session_from_tokens(data: dict, signed_in_at: float, refresh: str | None = None) -> dict:
    return {'access': data['access_token'],
            'access_exp': time.time() + float(data.get('expires_in') or 3600),
            'refresh': data.get('refresh_token') or refresh,
            'signed_in_at': signed_in_at}


def save_session(source_id: int, sess: dict) -> None:
    """Redis for speed, the source cache right away: a rotated refresh token
    must survive a restart, and Redis doesn't."""
    r = _redis()
    if r is not None:
        try:
            r.set(_SESSION_KEY, json.dumps(sess), ex=90 * 86400)
        except Exception:  # noqa: BLE001
            pass
    from ..config_store import persist_source_cache_updates
    persist_source_cache_updates(source_id, {SESSION_CACHE_KEY: sess})


def clear_session(source_id: int) -> None:
    r = _redis()
    if r is not None:
        try:
            r.delete(_SESSION_KEY)
        except Exception:  # noqa: BLE001
            pass
    from ..config_store import persist_source_cache_updates
    persist_source_cache_updates(source_id, {SESSION_CACHE_KEY: None})


# ── Okta ─────────────────────────────────────────────────────────────────────

def _token_request(data: dict, timeout=20) -> requests.Response:
    return requests.post(f'{OKTA}/token', data=dict(data, client_id=CLIENT_ID), timeout=timeout,
                         headers={'Accept': 'application/json'})


def refresh_tokens(refresh_token: str, timeout=20) -> dict | None:
    """New tokens, or None when Okta no longer accepts the refresh token."""
    r = _token_request({'grant_type': 'refresh_token', 'refresh_token': refresh_token,
                        'scope': 'openid offline_access'}, timeout=timeout)
    if r.status_code in (400, 401):
        logger.info('[mlb] refresh refused: %s', r.text[:200])
        return None
    r.raise_for_status()
    data = r.json() or {}
    return data if data.get('access_token') else None


def revoke(refresh_token: str) -> None:
    requests.post(f'{OKTA}/revoke', data={'token': refresh_token, 'token_type_hint': 'refresh_token',
                                          'client_id': CLIENT_ID}, timeout=10)


def start_link() -> dict:
    r = requests.post(f'{OKTA}/device/authorize', data={'client_id': CLIENT_ID, 'scope': 'openid offline_access'},
                      headers={'Accept': 'application/json'}, timeout=20)
    data = (r.json() or {}) if r.ok else {}
    if not data.get('device_code') or not data.get('user_code'):
        raise MLBAuthError(f'MLB: no sign-in code (HTTP {r.status_code})')
    return {'code': data['user_code'], 'device_code': data['device_code'],
            'url': data.get('verification_uri') or 'https://ids.mlb.com/activate',
            'interval': int(data.get('interval') or 5), 'expires_in': int(data.get('expires_in') or 600)}


def link_status(ctx: dict, state: str, message: str = '') -> dict:
    return {'state': state, 'message': message, 'code': ctx['code'], 'url': ctx['url']}


def poll_link(ctx: dict) -> dict | None:
    """None while the code hasn't been entered; the token response after."""
    r = _token_request({'grant_type': 'urn:ietf:params:oauth:grant-type:device_code',
                        'device_code': ctx['device_code']})
    data = r.json() if r.headers.get('content-type', '').startswith('application/json') else {}
    if r.ok and data.get('access_token'):
        return data
    err = data.get('error')
    if err == 'authorization_pending':
        return None
    if err == 'slow_down':
        ctx['interval'] = ctx.get('interval', 5) + 5
        return None
    if err == 'expired_token':
        raise MLBAuthError('The code expired before it was entered.')
    if err == 'access_denied':
        raise MLBAuthError('The sign-in was declined.')
    raise MLBAuthError(f'MLB sign-in: HTTP {r.status_code} {data.get("error_description") or r.text[:200]}')


def run_link(ctx: dict) -> None:
    """RQ job: wait for the code to be entered, then save the session on the
    mlb source and rescrape. Quits quietly once LINK_STATUS_KEY names a
    different code or is gone (cancelled)."""
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

        deadline = time.monotonic() + min(ctx.get('expires_in', 600), _LINK_TIMEOUT)
        tokens = None
        try:
            while time.monotonic() < deadline and not superseded():
                tokens = poll_link(ctx)
                if tokens:
                    break
                time.sleep(ctx.get('interval', 5))
        except Exception as exc:  # noqa: BLE001
            logger.warning('[mlb] sign-in failed: %s', exc)
            set_status('error', str(exc)[:300])
            return
        if not tokens:
            if superseded():
                logger.info('[mlb] sign-in code %s stopped', ctx['code'])
            else:
                set_status('expired', 'The code expired before it was entered.')
            return
        if not tokens.get('refresh_token'):
            set_status('error', 'MLB signed in but gave no refresh token, so the sign-in would last a day.')
            return

        src = Source.query.filter_by(name='mlb').first()
        if not src:
            set_status('error', 'MLB source no longer exists.')
            return
        now = int(time.time())
        cfg = dict(src.config or {})
        cfg.pop('signin_error', None)
        cfg.setdefault('device_id', str(uuid.uuid4()))
        # Games on for a first sign-in; a later sign-in keeps the user's choice.
        cfg.setdefault('include_games', 'true')
        sub = _claims(tokens.get('access_token')).get('sub') or ''
        cfg.update({'refresh_token': tokens['refresh_token'], 'signed_in_at': now,
                    'email': sub if '@' in sub else cfg.get('email')})
        sess = _session_from_tokens(tokens, now)
        try:
            info = init_session(sess['access'], cfg['device_id'], timeout=20)
            cfg['entitlements'] = info['entitlements']
            cfg['entitlements_at'] = now
            cfg['zip_code'] = info.get('zip_code')
        except Exception as exc:  # noqa: BLE001 — the next scrape reads them
            logger.warning('[mlb] entitlements unavailable after sign-in: %s', exc)
        src.config = cfg
        db.session.commit()
        save_session(src.id, sess)
        logger.info('[mlb] signed in (entitlements: %s)', ', '.join(cfg.get('entitlements') or []) or 'unknown')
        set_status('success')
        if src.is_enabled:
            from ..routes.tasks import trigger_scrape
            trigger_scrape('mlb', force_full=True)


# ── media gateway ────────────────────────────────────────────────────────────

_INIT_SESSION = ('mutation initSession($device: InitSessionInput!, $clientType: ClientType!) { '
                 'initSession(device: $device, clientType: $clientType) { deviceId sessionId '
                 'entitlements { code } location { countryCode zipCode } } }')
_CONTENT_COLLECTIONS = ('query contentCollections($categories: [ContentGroupCategory!] $includeRestricted: Boolean '
                        '= false $includeSpoilers: Boolean = false $limit: Int = 10, $skip: Int = 0) { '
                        'contentCollections(categories: $categories includeRestricted: $includeRestricted '
                        'includeSpoilers: $includeSpoilers limit: $limit skip: $skip) { title category contents { '
                        'assetTrackingKey contentType description mediaId mediaState { state } title } } }')
_INIT_PLAYBACK = ('mutation initPlaybackSession($adCapabilities: [AdExperienceType] $mediaId: String! '
                  '$deviceId: String! $sessionId: String! $quality: PlaybackQuality '
                  '$playbackCapabilities: PlaybackCapabilities) { initPlaybackSession(adCapabilities: '
                  '$adCapabilities mediaId: $mediaId deviceId: $deviceId sessionId: $sessionId quality: $quality '
                  'playbackCapabilities: $playbackCapabilities) { playbackSessionId playback { url expiration } } }')


class GatewayUnauthorized(Exception):
    pass


def _gateway(access: str, query: str, variables: dict, timeout=20) -> dict:
    with requests.Session() as s:  # a bare session: no retries on the play path
        r = s.post(GATEWAY, json={'query': query, 'variables': variables}, timeout=timeout,
                   headers=dict(_GATEWAY_HEADERS, Authorization='Bearer ' + access))
    if r.status_code in (401, 403):
        raise GatewayUnauthorized(f'HTTP {r.status_code}')
    r.raise_for_status()
    data = r.json() or {}
    errors = data.get('errors') or []
    # A refused mutation still answers {"data": {"initPlaybackSession": null}}.
    if errors and not any((data.get('data') or {}).values()):
        msg = '; '.join(e.get('message') or '' for e in errors)
        if 'unauthori' in msg.lower():
            raise GatewayUnauthorized(msg)
        raise RuntimeError(f'MLB: {msg}')
    return data


def init_session(access: str, device_id: str, timeout=20) -> dict:
    data = _gateway(access, _INIT_SESSION, {'clientType': 'WEB', 'device': {
        'appVersion': _CLIENT_VERSION, 'deviceFamily': 'desktop', 'knownDeviceId': device_id,
        'languagePreference': 'ENGLISH', 'manufacturer': 'FastChannels', 'model': '', 'os': 'linux',
        'osVersion': '1'}}, timeout=timeout)
    sess = ((data.get('data') or {}).get('initSession')) or {}
    if not sess.get('sessionId'):
        raise RuntimeError('MLB: no session from initSession')
    return {'device_id': sess.get('deviceId') or device_id, 'session_id': sess['sessionId'],
            'entitlements': [e.get('code') for e in sess.get('entitlements') or [] if e.get('code')],
            'zip_code': (sess.get('location') or {}).get('zipCode')}


def live_media_ids(access: str, collection: str, prefer: str | None = None, timeout=20) -> list[str]:
    """The live mediaIds in a linear channel's content collection (MLBN_LIVE,
    SNY_LIVE, ...): feeds whose state is ON, those whose assetTrackingKey
    contains `prefer` first. Team networks list an in-market and an
    out-of-market feed (sny-in-market-…, sny-out-market-…)."""
    data = _gateway(access, _CONTENT_COLLECTIONS, {'categories': [collection], 'includeRestricted': True,
                                                   'includeSpoilers': True, 'limit': 25, 'skip': 0},
                    timeout=timeout)
    items = [x for col in (data.get('data') or {}).get('contentCollections') or []
             for x in col.get('contents') or []
             if x.get('mediaId') and ((x.get('mediaState') or {}).get('state') or '').upper() == 'ON']
    if prefer:
        items.sort(key=lambda x: prefer not in (x.get('assetTrackingKey') or '').lower())
    return [x['mediaId'] for x in items]


def mlbn_media_id(access: str, timeout=20) -> str | None:
    """MLB Network's live mediaId: the MLBN-A feed when it's on."""
    ids = live_media_ids(access, 'MLBN_LIVE', prefer='mlbn-a', timeout=timeout)
    return ids[0] if ids else None


def has_entitlement(entitlements, code: str) -> bool:
    return any(e == code or e.startswith(code + '_') for e in entitlements or [])


def team_networks_on(entitlements) -> list[str]:
    """Channel ids of the team networks these entitlements include."""
    return [cid for cid, net in TEAM_NETWORKS.items() if has_entitlement(entitlements, net['entitlement'])]


def sny_programs(schedule: dict, channel_id: str) -> list[ProgramData]:
    """SNY's own schedule (day -> rows of [weekday, MM/DD/YYYY, start, end, title],
    Eastern time). Titles only; the fallback when Gracenote has nothing."""
    now = datetime.now(timezone.utc)
    programs = {}
    for day in (schedule or {}).values():
        for row in (((day or {}).get('data') or {}).get('rows') or []):
            try:
                _, date, start_s, end_s, title = row[:5]
                start = datetime.strptime(f'{date} {start_s}', '%m/%d/%Y %I:%M %p').replace(tzinfo=_ET)
                end = datetime.strptime(f'{date} {end_s}', '%m/%d/%Y %I:%M %p').replace(tzinfo=_ET)
            except (TypeError, ValueError):
                continue
            if end <= start:  # runs past midnight
                end += timedelta(days=1)
            # Stored in UTC: SQLite drops the zone.
            start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
            if end < now - timedelta(hours=1) or not (title or '').strip():
                continue
            programs.setdefault(start, ProgramData(
                source_channel_id=channel_id, title=title.strip(), start_time=start, end_time=end,
                category='Sports'))
    return [programs[k] for k in sorted(programs)]


# ── MLB Network schedule ─────────────────────────────────────────────────────

_SCHEDULE_FIELDS = ('startdate', 'starttime', 'enddate', 'endtime', 'utcDate', 'seriestitle', 'episodetitle',
                    'programTitle', 'synopsis', 'originalShowSynopsis', 'talent', 'live', 'episodepremiere',
                    'episodenumber', 'matid', 'airingid')


def mlbn_schedule(html: str) -> list[dict]:
    """The airings in the schedule page's Next.js data stream, oldest first, trimmed
    to the fields the guide uses. Empty when the page's shape has changed."""
    chunks = []
    for m in re.finditer(r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)', html):
        try:
            chunks.append(json.loads('"' + m.group(1) + '"'))
        except ValueError:
            continue
    blob = ''.join(chunks)
    decoder = json.JSONDecoder()
    airings: dict[str, dict] = {}
    for m in re.finditer(r'\{"station":', blob):
        try:
            obj, _ = decoder.raw_decode(blob, m.start())
        except ValueError:
            continue
        if not obj.get('utcDate'):
            continue
        item = {k: obj.get(k) for k in _SCHEDULE_FIELDS}
        item['image'] = ((obj.get('thumbnail') or {}).get('templateUrl')
                         or (obj.get('heroThumbnail') or {}).get('templateUrl'))
        airings.setdefault(obj['utcDate'], item)
    return [airings[k] for k in sorted(airings)]


def _airing_end(item: dict) -> datetime | None:
    try:
        # In UTC like every start: SQLite drops the zone, so an ET datetime would be stored 5 hours early.
        return (datetime.strptime(f"{item['enddate']} {item['endtime']}", '%m/%d/%Y %I:%M %p')
                .replace(tzinfo=_ET).astimezone(timezone.utc))
    except (KeyError, TypeError, ValueError):
        return None


# "Hot Stove [LIVE]", "MLB Now … [PREMIERE]": the live and premiere flags say that already.
_FLAG_TAGS = re.compile(r'\s*\[(?:LIVE|PREMIERE)\]', re.I)


def _strip_flags(text: str | None) -> str:
    return _FLAG_TAGS.sub('', text or '').strip()


def mlbn_programs(airings: list[dict], channel_id: str) -> list[ProgramData]:
    now = datetime.now(timezone.utc)
    programs = []
    for item in airings:
        start, end = _parse_time(item.get('utcDate')), _airing_end(item)
        if not start or not end or end <= start or end < now - timedelta(hours=1):
            continue
        show = _strip_flags(item.get('seriestitle') or item.get('programTitle')) or 'MLB Network'
        episode = _strip_flags(item.get('episodetitle') or item.get('programTitle'))
        desc = (item.get('synopsis') or item.get('originalShowSynopsis') or '').strip()
        talent = (item.get('talent') or '').strip().replace(',', ', ')
        if talent:
            desc = f'{desc} With {talent}.' if desc else f'With {talent}.'
        image = item.get('image')
        programs.append(ProgramData(
            source_channel_id=channel_id,
            title=show,
            episode_title=episode if episode and episode.casefold() != show.casefold() else None,
            start_time=start,
            end_time=end,
            description=desc or None,
            poster_url=image.replace('{formatInstructions}', _MLBN_IMAGE_FORMAT) if image else None,
            category='Sports;Baseball',
            is_live=(item.get('live') or '').upper() == 'LIVE',
            # A premiere's original air date is today, which is how DVRs tell it's new.
            original_air_date=start.astimezone(_ET).date() if item.get('episodepremiere') == 'Yes' else None,
            episode_id=item.get('episodenumber') or item.get('matid') or None,
        ))
    return programs


# ── games ────────────────────────────────────────────────────────────────────

def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def _tag_slugs(item: dict) -> list[str]:
    return [t.get('slug') or '' for t in item.get('tags') or []]


def _game_pk(item: dict) -> int | None:
    for t in item.get('tags') or []:
        pk = (t.get('extraData') or {}).get('gamePk')
        if t.get('externalSourceName') == 'game' and pk:
            return int(pk)
    return None


def live_streams(timeout=20) -> dict[int, dict]:
    """{gamePk: CMS item} for the live game streams in MLB.TV's featured list."""
    r = requests.get(CMS_LIVE_LIST, params={'$limit': 100}, timeout=timeout,
                     headers={'Accept': 'application/json'})
    r.raise_for_status()
    out = {}
    for item in (r.json() or {}).get('items') or []:
        slugs = _tag_slugs(item)
        pk = _game_pk(item)
        if pk and 'slive' in slugs and item.get('_translationId'):
            out.setdefault(pk, item)
    return out


def _cached_live_streams(timeout) -> dict[int, dict]:
    r = _redis()
    if r is not None:
        try:
            raw = r.get(_CMS_KEY)
            if raw:
                return {int(k): v for k, v in json.loads(raw).items()}
        except Exception:  # noqa: BLE001
            pass
    streams = live_streams(timeout=timeout)
    if r is not None:
        try:
            r.setex(_CMS_KEY, _CMS_CACHE_SECONDS, json.dumps(streams))
        except Exception:  # noqa: BLE001
            pass
    return streams


def schedule(start: datetime, end: datetime) -> dict[int, dict]:
    """{gamePk: statsapi game} for the scheduled winter-league games with an MLB.TV broadcast."""
    r = requests.get(f'{STATSAPI}/schedule', timeout=20, params={
        'sportId': _SCHEDULE_SPORT_IDS, 'startDate': start.date().isoformat(), 'endDate': end.date().isoformat(),
        'hydrate': 'broadcasts,team'})
    r.raise_for_status()
    out = {}
    for day in (r.json() or {}).get('dates') or []:
        for g in day.get('games') or []:
            league = (((g.get('teams') or {}).get('home') or {}).get('team') or {}).get('league') or {}
            if league.get('id') in _SCHEDULE_LEAGUES and any(
                    b.get('name') == 'MLB.TV' for b in g.get('broadcasts') or []):
                out[int(g['gamePk'])] = g
    return out


def _games_by_pk(pks: list[int]) -> dict[int, dict]:
    if not pks:
        return {}
    r = requests.get(f'{STATSAPI}/schedule', params={'gamePks': ','.join(map(str, pks)), 'hydrate': 'team'},
                     timeout=20)
    r.raise_for_status()
    return {int(g['gamePk']): g for day in (r.json() or {}).get('dates') or [] for g in day.get('games') or []}


def _league_label(game: dict | None, item: dict | None) -> str:
    """The title's league label. Major League teams say "MLB", not their
    league (statsapi's league for them is the AL or NL)."""
    team = (((game or {}).get('teams') or {}).get('home') or {}).get('team') or {}
    if (team.get('sport') or {}).get('id') == 1:
        return 'MLB'
    league = team.get('league') or {}
    if league.get('id') in _WINTER_LEAGUE_LABELS:
        return _WINTER_LEAGUE_LABELS[league['id']]
    for slug in _tag_slugs(item or {}):
        if slug in _LEAGUE_NAMES:
            return _LEAGUE_NAMES[slug]
    # An unknown league keeps its own name rather than passing for MLB.
    return league.get('abbreviation') or league.get('name') or 'MLB.TV'


def _matchup(game: dict | None) -> dict | None:
    teams = (game or {}).get('teams') or {}
    away = ((teams.get('away') or {}).get('team') or {}).get('name')
    home = ((teams.get('home') or {}).get('team') or {}).get('name')
    return {'away': away, 'home': home, 'neutral': False} if away and home else None


def parse_game_url(raw_url: str) -> tuple[int, int | None]:
    m = re.match(r'mlb://game/(\d+)(?:\?end=(\d+))?$', raw_url or '')
    if not m:
        raise ValueError(f'Unsupported MLB stream URL: {raw_url}')
    return int(m.group(1)), int(m.group(2)) if m.group(2) else None


# ── scraper ──────────────────────────────────────────────────────────────────

class MLBScraper(BaseScraper):
    source_name = 'mlb'
    added_in = '6.0.0'
    display_name = 'MLB'
    source_category = 'premium'
    is_premium = True
    scrape_interval = 30
    # Game channels have no stream until shortly before first pitch.
    stream_audit_enabled = False
    # Neither CDN allows our page's Origin: svod-fst (games) sends CORS only for an
    # allowlist (mlb.com, localhost), tve-live (MLB Network) answers 403 to any
    # Origin but mlb.com's. So the admin player gets playlists and segments relayed.
    browser_master_proxy = True
    browser_relay_hosts = frozenset({'svod-fst.mlb.com', 'tve-live.mlb.com'})

    config_required = True
    config_schema = [
        ConfigField('include_games', 'Include live game channels', field_type='toggle', default='false',
                    help_text='Adds each live game the account can play as a temporary channel.'),
    ]

    # ── auth ──

    def _signed_in_at(self) -> float:
        return float(self.config.get('signed_in_at') or 0)

    def _stored_session(self) -> dict:
        """The newest session: Redis, then the source cache, then the sign-in's
        own refresh token. One from before the latest sign-in is ignored."""
        signed_in_at = self._signed_in_at()
        candidates = []
        r = _redis()
        if r is not None:
            try:
                candidates.append(json.loads(r.get(_SESSION_KEY) or '{}'))
            except Exception:  # noqa: BLE001
                pass
        candidates.append(self.cache.get(SESSION_CACHE_KEY) or {})
        for sess in candidates:
            if sess.get('refresh') and float(sess.get('signed_in_at') or 0) >= signed_in_at:
                return sess
        if self.config.get('refresh_token'):
            return {'refresh': self.config['refresh_token'], 'signed_in_at': signed_in_at}
        return {}

    def _source_id(self) -> int | None:
        from ..models import Source
        src = Source.query.filter_by(name=self.source_name).first()
        return src.id if src else None

    def _save(self, sess: dict) -> None:
        source_id = self._source_id()
        if source_id:
            save_session(source_id, sess)
        if self._cache is not None:
            self._cache[SESSION_CACHE_KEY] = sess

    def _signin_lost(self) -> None:
        logger.warning('[mlb] %s', _SIGNIN_LOST)
        for key in SIGNIN_CONFIG_KEYS:
            self._update_config(key, None)
        self._update_config('signin_error', _SIGNIN_LOST)
        source_id = self._source_id()
        if source_id:
            clear_session(source_id)
        if self._cache is not None:
            self._cache.pop(SESSION_CACHE_KEY, None)

    def _access_token(self, force: bool = False, timeout=20) -> str:
        sess = self._stored_session()
        if not sess.get('refresh'):
            raise MLBAuthError('MLB: sign in on the Sources page.')
        if not force and sess.get('access') and float(sess.get('access_exp') or 0) - time.time() > _ACCESS_MARGIN:
            return sess['access']
        r = _redis()
        locked = False
        if r is not None:
            for _ in range(20):
                try:
                    locked = bool(r.set(_LOCK_KEY, '1', nx=True, ex=30))
                except Exception:  # noqa: BLE001
                    break
                if locked:
                    break
                time.sleep(0.5)
                fresh = self._stored_session()
                if (fresh.get('access') and fresh.get('access') != sess.get('access')
                        and float(fresh.get('access_exp') or 0) - time.time() > _ACCESS_MARGIN):
                    return fresh['access']
        try:
            sess = self._stored_session()
            if (not force and sess.get('access')
                    and float(sess.get('access_exp') or 0) - time.time() > _ACCESS_MARGIN):
                return sess['access']
            tokens = refresh_tokens(sess['refresh'], timeout=timeout)
            if not tokens:
                self._signin_lost()
                raise MLBAuthError(_SIGNIN_LOST)
            sess = _session_from_tokens(tokens, self._signed_in_at(), refresh=sess['refresh'])
            self._save(sess)
            return sess['access']
        finally:
            if locked:
                try:
                    r.delete(_LOCK_KEY)
                except Exception:  # noqa: BLE001
                    pass

    def _init_session(self, timeout=20) -> dict:
        device_id = self.config.get('device_id') or str(uuid.uuid4())
        if not self.config.get('device_id'):
            self._update_config('device_id', device_id)
        try:
            return init_session(self._access_token(timeout=timeout), device_id, timeout=timeout)
        except GatewayUnauthorized:
            return init_session(self._access_token(force=True, timeout=timeout), device_id, timeout=timeout)

    def pre_run_setup(self) -> None:
        self._entitlements = None
        if not self.config.get('refresh_token'):
            return
        try:
            info = self._init_session()
        except MLBAuthError:
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning('[mlb] initSession failed: %s', exc)
            return
        self._entitlements = info['entitlements']
        self._update_config('entitlements', info['entitlements'])
        self._update_config('entitlements_at', int(time.time()))
        self._update_config('zip_code', info.get('zip_code'))

    def _include_games(self) -> bool:
        return str(self.config.get('include_games', 'false')).strip().lower() in ('1', 'true', 'yes', 'on')

    def _ents(self) -> list[str]:
        ents = getattr(self, '_entitlements', None)
        if ents is None:
            ents = self.config.get('entitlements') or []
        return ents

    def _has_mlbn(self) -> bool:
        return 'MLBN' in self._ents()

    # ── channels / EPG ──

    def _games(self) -> list[tuple[int, dict | None, dict | None, datetime, datetime]]:
        """(gamePk, statsapi game, CMS item, start, end) for live and upcoming games."""
        now = datetime.now(timezone.utc)
        streams = live_streams()  # raises: a failed listing must not retire live games
        try:
            scheduled = schedule(now - timedelta(hours=12), now + timedelta(hours=_LOOKAHEAD_HOURS))
        except requests.RequestException as exc:
            logger.warning('[mlb] schedule unavailable: %s', exc)
            scheduled = {}
        missing = [pk for pk in streams if pk not in scheduled]
        try:
            scheduled.update(_games_by_pk(missing))
        except requests.RequestException as exc:
            logger.info('[mlb] game details unavailable: %s', exc)
        out = []
        for pk in set(streams) | set(scheduled):
            game, item = scheduled.get(pk), streams.get(pk)
            status = (game or {}).get('status') or {}
            state, code = status.get('abstractGameState'), status.get('codedGameState')
            # Finished, postponed or cancelled games stay only while their stream is still listed.
            if not item and (state == 'Final' or code in ('D', 'C', 'U')):
                continue
            start = _parse_time((game or {}).get('gameDate')) or _parse_time((item or {}).get('contentDate'))
            if not start:
                continue
            end = start + timedelta(hours=_GAME_HOURS)
            if item and end <= now:
                end = now + _LIVE_EXTENSION
            if end <= now or start >= now + timedelta(hours=_LOOKAHEAD_HOURS):
                continue
            out.append((pk, game, item, start, end))
        return sorted(out, key=lambda g: (g[3], g[0]))

    def _describe(self, game: dict | None, item: dict | None) -> tuple[str, str, dict | None]:
        league = _league_label(game, item)
        matchup = _matchup(game)
        title = (item or {}).get('title') or 'MLB.TV Live'
        body = matchup_body(matchup, 'Baseball') if matchup else re.sub(r'^[A-Z]+ LIVE:\s*', '', title)
        return body, league, matchup

    def fetch_channels(self) -> list[ChannelData]:
        self._game_items = []
        self.hide_missing_event_channels = True
        if not self.config.get('refresh_token'):
            return []
        channels = []
        if self._has_mlbn():
            channels.append(ChannelData(
                source_channel_id=MLBN_CHANNEL_ID, name='MLB Network', stream_url='mlb://mlbn',
                slug='mlb_network', logo_url=MLBN_LOGO, category='Sports', gracenote_id=MLBN_GRACENOTE,
                description='MLB Network, 24/7 baseball.'))
        for cid in team_networks_on(self._ents()):
            net = TEAM_NETWORKS[cid]
            channels.append(ChannelData(
                source_channel_id=cid, name=net['name'], stream_url=f'{NETWORK_SCHEME}{cid}',
                slug=f'mlb_{cid}', logo_url=net['logo'], category='Sports', gracenote_id=net['gracenote'],
                description=net['description']))
        if not self._include_games():
            return channels
        self._game_items = self._games()
        for pk, game, item, start, end in self._game_items:
            body, league, _ = self._describe(game, item)
            home_id = ((((game or {}).get('teams') or {}).get('home') or {}).get('team') or {}).get('id')
            # PNG: the logo cache (Pillow) and many players can't read the SVG logos.
            logo = (f'https://midfield.mlbstatic.com/v1/team/{home_id}/spots/256' if home_id
                    else ((item or {}).get('thumbnail') or {}).get('thumbnailUrl'))
            channels.append(ChannelData(
                source_channel_id=f'{GAME_CHANNEL_PREFIX}{pk}',
                name=f'{league} - {body}'[:256],
                stream_url=f'{GAME_SCHEME}{pk}?end={int(end.timestamp())}',
                logo_url=logo,
                category='Sports',
                guide_key=f'{GAME_CHANNEL_PREFIX}{pk}',
                tags=[EVENT_CHANNEL_TAG, 'MLB.TV'],
                sport='Baseball',
                league=league,
                gracenote_mode='off',
                retire_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS),
                delete_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS + 24 * 3600),
            ))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {ch.source_channel_id for ch in channels}
        programs = []
        if MLBN_CHANNEL_ID in wanted:
            mlbn = mlbn_programs(self._mlbn_airings(), MLBN_CHANNEL_ID)
            if not mlbn:
                gracenote = next((ch.gracenote_id for ch in channels if ch.source_channel_id == MLBN_CHANNEL_ID),
                                 None) or MLBN_GRACENOTE
                from ..guide_cache import station_programs
                mlbn = station_programs(MLBN_CHANNEL_ID, gracenote, 'mlb')
            programs.extend(mlbn)
        for ch in channels:
            net = TEAM_NETWORKS.get(ch.source_channel_id)
            if not net:
                continue
            from ..guide_cache import station_programs
            guide = station_programs(ch.source_channel_id, ch.gracenote_id or net['gracenote'], 'mlb')
            if not guide and ch.source_channel_id == 'sny':
                guide = sny_programs(self._sny_schedule(), ch.source_channel_id)
            programs.extend(guide)
        for pk, game, item, start, end in getattr(self, '_game_items', []):
            cid = f'{GAME_CHANNEL_PREFIX}{pk}'
            if cid not in wanted:
                continue
            body, league, matchup = self._describe(game, item)
            venue = ((game or {}).get('venue') or {}).get('name')
            desc = f'At {venue}.' if venue else None
            programs.append(ProgramData(
                source_channel_id=cid,
                title=matchup_title(league, matchup, 'Baseball') if matchup else f'{league}: {body}',
                start_time=start,
                end_time=end,
                description=matchup_description(matchup, 'Baseball', desc) if matchup else desc,
                poster_url=((item or {}).get('thumbnail') or {}).get('thumbnailUrl'),
                category=';'.join(('Sports Event', 'Baseball', league)),
                is_live=True,
                episode_id=str(pk),
            ))
        return programs

    def _mlbn_airings(self) -> list[dict]:
        """MLB Network's schedule, refetched every few hours; the last good copy
        when the page fails or comes back without listings."""
        cached = self.cache.get(MLBN_SCHEDULE_CACHE_KEY) or {}
        if time.time() - float(cached.get('fetched_at') or 0) < _MLBN_SCHEDULE_TTL and cached.get('airings'):
            return cached['airings']
        try:
            r = self.session.get(MLBN_SCHEDULE_URL, timeout=30, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/130.0 Safari/537.36'})
            r.raise_for_status()
            airings = mlbn_schedule(r.text)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[mlb] MLB Network schedule unavailable: %s', exc)
            return cached.get('airings') or []
        if not airings:
            logger.warning('[mlb] MLB Network schedule page had no listings (page changed?)')
            return cached.get('airings') or []
        self._update_cache(MLBN_SCHEDULE_CACHE_KEY, {'fetched_at': int(time.time()), 'airings': airings})
        return airings

    def _sny_schedule(self) -> dict:
        cached = self.cache.get(SNY_SCHEDULE_CACHE_KEY) or {}
        if time.time() - float(cached.get('fetched_at') or 0) < _MLBN_SCHEDULE_TTL and cached.get('schedule'):
            return cached['schedule']
        try:
            r = self.session.get(SNY_SCHEDULE_URL, timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
            r.raise_for_status()
            sched = (r.json() or {}).get('schedule') or {}
        except Exception as exc:  # noqa: BLE001
            logger.warning('[mlb] SNY schedule unavailable: %s', exc)
            return cached.get('schedule') or {}
        if sched:
            self._update_cache(SNY_SCHEDULE_CACHE_KEY, {'fetched_at': int(time.time()), 'schedule': sched})
        return sched or cached.get('schedule') or {}

    # ── playback ──

    def audit_resolve(self, raw_url: str) -> str:
        return raw_url

    def resolve(self, raw_url: str) -> str:
        net_id = raw_url[len(NETWORK_SCHEME):] if raw_url.startswith(NETWORK_SCHEME) else None
        if raw_url == 'mlb://mlbn' or net_id:
            media_id = None
        else:
            pk, end = parse_game_url(raw_url)
            if end is not None and time.time() >= end + EVENT_END_GRACE_SECONDS:
                raise StreamDeadError('MLB game has ended')
            item = _cached_live_streams(_PLAY_TIMEOUT).get(pk)
            if not item:
                raise RuntimeError("MLB: this game's stream isn't up yet (it starts about 30 minutes before first pitch)")
            media_id = item['_translationId']
        session = self._init_session(timeout=_PLAY_TIMEOUT)
        access = self._access_token(timeout=_PLAY_TIMEOUT)
        if net_id:
            net = TEAM_NETWORKS.get(net_id)
            if not net:
                raise ValueError(f'Unsupported MLB stream URL: {raw_url}')
            if not has_entitlement(session['entitlements'], net['entitlement']):
                raise TVENotAuthorizedError(
                    f"MLB: this account has no {net['name']} access. Link your TV provider at {net['link']}")
            media_ids = live_media_ids(access, net['collection'], prefer='in-market', timeout=_PLAY_TIMEOUT)
            if not media_ids:
                raise RuntimeError(f"MLB: {net['name']} has no live feed right now")
            refused = None
            for mid in media_ids:  # in market first; the out-of-market feed when that's refused
                try:
                    return self._playback_url(access, session, mid)
                except TVENotAuthorizedError as exc:
                    refused = exc
            raise TVENotAuthorizedError(
                f"MLB: {net['name']} isn't playable on this account (not linked, or outside its market). "
                f"Link your TV provider at {net['link']}") from refused
        if media_id is None:
            if 'MLBN' not in session['entitlements']:
                raise TVENotAuthorizedError(
                    f'MLB: this account has no MLB Network access. Link your TV provider at {RELINK_URL}')
            media_id = mlbn_media_id(access, timeout=_PLAY_TIMEOUT)
            if not media_id:
                raise RuntimeError('MLB: MLB Network has no live feed right now')
        return self._playback_url(access, session, media_id)

    def _playback_url(self, access: str, session: dict, media_id: str) -> str:
        try:
            data = _gateway(access, _INIT_PLAYBACK, {
                'adCapabilities': [], 'mediaId': media_id, 'deviceId': session['device_id'],
                'sessionId': session['session_id'], 'quality': 'PLACEHOLDER', 'playbackCapabilities': {}},
                timeout=_PLAY_TIMEOUT)
        except RuntimeError as exc:
            if 'not playable' in str(exc).lower() or 'not entitled' in str(exc).lower():
                raise TVENotAuthorizedError(
                    'MLB: this stream isn\'t playable on this account (no access, blacked out, or not live)') from exc
            raise
        url = ((((data.get('data') or {}).get('initPlaybackSession') or {}).get('playback')) or {}).get('url')
        if not url:
            raise RuntimeError('MLB: no stream URL returned')
        return url

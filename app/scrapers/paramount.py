"""Paramount+ (source paramount) — live sports matches as event channels, plus
the "Paramount+ Channels" (South Park, SpongeBob, CSI, …) as bridge-only DRM
channels.

Scoped 2026-10-21 (dev/paramount/SCOPED.md). Paramount+'s live sports leagues
(UEFA, Serie A, EFL, UFC, rugby, …) are "discrete" live channels: each match
is its own stream, listed anonymously by the app API (live/channels.json,
channelTypes 'discrete'; live/channels/<slug>/listings.json per league).

Streams are Google DAI linear HLS encrypted with plain AES-128 (assetType
live_hls_aes), not DRM. The key URL (cbsi.live.ott.irdeto.com/licenseServer/
hls/v1/cbsi/key) needs `Authorization: Bearer <token>`. Paramount issues that
token, about 8 hours, in the web player's per-match metadata.json for a
signed-in session. Playback goes through /play/paramount/<id>/proxy.m3u8
(app/routes/paramount_proxy.py), which points the key lines at our own key
endpoint; segments load straight from the CDN.

Sign-in is the Paramount+ TV app's activation code: POST apps-api/v2.0/
androidtv/ott/auth/code.json → code; the user enters it at
paramountplus.com/activate/androidtv and signs in with their Paramount+
account (that page takes only email and password); status.json then answers
success and sets the CBS_COM session cookie (1 year). Viewers who get
Paramount+ through a TV provider connect it to their Paramount+ account first
(paramountplus.com → Account → TV Provider).

The saved session is checked for a valid sign-in. Paramount+ determines
match availability when playback metadata is requested.

Paramount+ Channels (channelTypes 'vod_to_live', live/channels.json) are 24/7
loops of Paramount's library. Each has a fixed videoContentId and Widevine
DASH (assetType live_dash_drm), so they play only through a DRM bridge.
apps-api v3.1 irdeto-control/session-token.json, called with the signed-in
session, returns the manifest (streamingUrl), the license URL
(cbsi.live.ott.irdeto.com/widevine/getlicense?…&ContentId=<id>) and the
license token (ls_session, a JWT for the account, 8 hours). The license
proxy sends `Authorization: Bearer <ls_session>`. Without the session the
endpoint answers with no token and no manifest. The web player's
metadata.json now hands out an anonymous token and no manifest for this
session, so it can't be used for these channels. Each channel's
listings.json gives about 37 hours of guide with TMS ids.

Showtime East and West (slugs showtime-east / showtime-west, fixed content
ids, assetType live_hls_drm) aren't in live/channels.json for an account
without Showtime, but their listings.json is public. session-token.json
gives every signed-in account a token for them, but only an entitled one gets
a streamingUrl, so the scrape adds them only when it does
(config showtime_available). Not yet tested with a Showtime account: the
manifest may turn out to be HLS, which is logged and skipped.
"""
from __future__ import annotations

import base64
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone

import requests

from ..matchups import (MATCHUP_CACHE_KEY, MatchupResolver, event_label, matchup_body,
                        matchup_description, matchup_title)
from ..tve.adobe_pass import TVEAuthError
from ..sports import normalize_sport, sport_for_league
from .base import (BaseScraper, ChannelData, ConfigField, ProgramData,
                   StreamDeadError, EVENT_CHANNEL_TAG, infer_language)

logger = logging.getLogger(__name__)

_PPLUS = 'https://www.paramountplus.com'
_APPS_API = f'{_PPLUS}/apps-api'
_APP_TOKEN = 'ABCvvU1Pv0BRR9aWYFLAm+m8bcIJXm7a9GYpMwXFtDuq1P5ARAg6o60yilK8oQ2Eaxc='
_TV_UA = 'Paramount+/15.0 (androidtv)'
_WEB_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
           '(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36')
_IMAGES = 'https://wwwimage-us.pplusstatic.com/base/'
_THUMBS = 'https://wwwimage-us.pplusstatic.com/thumbnails/photos/w370-q80/'
ACTIVATE_URL = f'{_PPLUS}/activate/androidtv/'

EVENT_SCHEME = 'paramount://event/'
EVENT_CHANNEL_PREFIX = 'event:'
CHANNEL_SCHEME = 'paramount://channel/'
CHANNEL_PREFIX = 'channel:'
LICENSE_URL = 'https://cbsi.live.ott.irdeto.com/widevine/getlicense'
_SESSION_TOKEN_PATH = 'v3.1/androidtv/irdeto-control/session-token.json'
_LISTING_ROWS = 100
# Paramount+ channel category slug → FastChannels category.
_CHANNEL_CATEGORIES = {'comedy': 'Comedy', 'drama': 'Drama', 'kids-family': 'Kids',
                       'reality': 'Reality TV', 'news': 'News', 'sports': 'Sports',
                       'showtime': 'Movies'}
SHOWTIME_SLUGS = {'showtime-east': 'Showtime East', 'showtime-west': 'Showtime West'}
# Hosts the play proxy may fetch for this source.
KEY_HOST = 'cbsi.live.ott.irdeto.com'
PLAYLIST_HOSTS = ('dai.google.com',)

_EVENT_LOOKAHEAD_HOURS = 48
# Listings' stream window already pads the match by an hour or more.
EVENT_END_GRACE_SECONDS = 3600
_STATUS_TTL = 6 * 3600
_TOKEN_MARGIN = 10 * 60
_LINK_TIMEOUT = 600
_LINK_POLL_SECONDS = 5.0

LINK_STATUS_KEY = 'paramount:link:status'
SIGNIN_CONFIG_KEYS = ('cookies', 'user_id', 'signed_in_at', 'account_status', 'status_at')
_SIGNIN_LOST = 'The Paramount+ sign-in expired or was removed — sign in again.'

# contentId → (Authorization header value, expiry epoch). Per process; a miss
# costs one metadata.json fetch.
_KEY_TOKENS: dict[str, tuple[str, float]] = {}
# contentId → {'manifest', 'license_url', 'auth', 'exp'} from session-token.json
# for the DRM channels. Per process; the license proxy refetches on a miss.
_DRM_SESSIONS: dict[str, dict] = {}


class ParamountAuthError(TVEAuthError):
    pass


# ── account ──────────────────────────────────────────────────────────────────

def _app_params(**params) -> dict:
    return {'at': _APP_TOKEN, 'locale': 'en-us', **params}


def account_status(cookies: dict) -> dict:
    """Sign-in state for a saved session."""
    r = requests.get(f'{_APPS_API}/v3.0/androidtv/login/status.json', params=_app_params(),
                     cookies=cookies, headers={'User-Agent': _TV_UA}, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    return {'logged_in': bool(data.get('isLoggedIn')),
            'status': (data.get('userStatus') or {}).get('description'),
            'user_id': data.get('userId')}


def _signin_config(cookies: dict, status: dict) -> dict:
    now = int(time.time())
    return {'cookies': cookies, 'user_id': status.get('user_id'), 'signed_in_at': now,
            'account_status': status.get('status'), 'status_at': now}


# ── activation-code sign-in ──────────────────────────────────────────────────

def start_link(device_id: str) -> dict:
    r = requests.post(f'{_APPS_API}/v2.0/androidtv/ott/auth/code.json', params=_app_params(),
                      data={'deviceId': device_id}, headers={'User-Agent': _TV_UA}, timeout=20)
    data = (r.json() or {}) if r.ok else {}
    if not data.get('activationCode'):
        raise ParamountAuthError(f'Paramount+: no activation code (HTTP {r.status_code})')
    return {'code': data['activationCode'], 'device_token': data['deviceToken'], 'device_id': device_id}


def link_status(ctx: dict, state: str, message: str = '') -> dict:
    return {'state': state, 'message': message, 'code': ctx['code'], 'url': ACTIVATE_URL}


def run_link(ctx: dict) -> None:
    """RQ job: wait for the code to be entered, then save the session onto the
    paramount source and rescrape. Quits quietly once LINK_STATUS_KEY names a
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

        session = requests.Session()
        session.headers['User-Agent'] = _TV_UA
        deadline = time.monotonic() + _LINK_TIMEOUT
        cookies = None
        try:
            while time.monotonic() < deadline and not superseded():
                resp = session.get(f'{_APPS_API}/v2.0/androidtv/ott/auth/status.json', timeout=20,
                                   params=_app_params(activationCode=ctx['code'], deviceToken=ctx['device_token'],
                                                      deviceId=ctx['device_id']))
                if resp.ok and (resp.json() or {}).get('success'):
                    cookies = {c.name: c.value for c in session.cookies if c.name.startswith('CBS_')}
                    break
                time.sleep(_LINK_POLL_SECONDS)
            if cookies is not None and not cookies.get('CBS_COM'):
                raise ParamountAuthError('Paramount+ accepted the code but returned no session.')
            status = account_status(cookies) if cookies else None
            if status and not status['logged_in']:
                raise ParamountAuthError('Paramount+ accepted the code but the session is not signed in.')
        except Exception as exc:  # noqa: BLE001
            logger.warning('[paramount] sign-in failed: %s', exc)
            set_status('error', str(exc)[:300])
            return
        if not cookies:
            if superseded():
                logger.info('[paramount] activation code %s stopped', ctx['code'])
            else:
                set_status('expired', 'The code expired before the sign-in finished.')
            return

        src = Source.query.filter_by(name='paramount').first()
        if not src:
            set_status('error', 'Paramount+ source no longer exists.')
            return
        cfg = dict(src.config or {})
        cfg.pop('signin_error', None)
        cfg.update(_signin_config(cookies, status))
        src.config = cfg
        db.session.commit()
        logger.info('[paramount] signed in (%s)', status.get('status'))
        set_status('success')
        if src.is_enabled:
            from ..routes.tasks import trigger_scrape
            trigger_scrape('paramount', force_full=True)


# ── helpers ──────────────────────────────────────────────────────────────────

def _epoch_ms(value) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value) / 1000, timezone.utc) if value else None
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _item_sport(item: dict) -> str | None:
    """The sport in a listing's category path, e.g. "Soccer" from
    "Live Streaming/Sports/Soccer/EFL Cup"; None when there's no Sports level."""
    path = [part.strip() for part in ((item.get('contentCANVideo') or {}).get('primaryCategoryName') or '').split('/')]
    folded = [part.casefold() for part in path]
    if 'sports' not in folded:
        return None
    rest = path[folded.index('sports') + 1:]
    return rest[0] if rest and rest[0] else None


def _jwt_exp(header_value: str) -> float:
    try:
        payload = header_value.split()[-1].split('.')[1]
        payload += '=' * (-len(payload) % 4)
        return float(json.loads(base64.urlsafe_b64decode(payload)).get('exp') or 0)
    except (IndexError, ValueError, TypeError):
        return 0.0


def parse_channel_url(raw_url: str) -> tuple[str, str]:
    """(contentId, channel slug) from a Paramount+ Channels stream_url."""
    m = re.match(r'paramount://channel/([\w-]+)\?slug=([\w-]+)$', raw_url or '')
    if not m:
        raise ValueError(f'Unsupported Paramount+ stream URL: {raw_url}')
    return m.group(1), m.group(2)


def drm_session(cookies: dict, content_id: str, force: bool = False) -> dict:
    """Manifest, license URL and license Authorization for one DRM channel,
    cached per process until 10 minutes before the token expires."""
    cached = _DRM_SESSIONS.get(content_id)
    if not force and cached and time.time() < cached['exp'] - _TOKEN_MARGIN:
        return cached
    r = requests.get(f'{_APPS_API}/{_SESSION_TOKEN_PATH}', params=_app_params(contentId=content_id),
                     cookies=cookies, headers={'User-Agent': _TV_UA}, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    token = (data.get('ls_session') or '').strip()
    if not token or not data.get('streamingUrl'):
        raise ParamountAuthError('Paramount+: this channel isn\'t available to this account')
    auth = f'Bearer {token}'
    session = {'manifest': data['streamingUrl'], 'auth': auth,
               'license_url': data.get('url') or f'{LICENSE_URL}?CrmId=cbsi&AccountId=cbsi&SubContentType=Default&ContentId={content_id}',
               'exp': _jwt_exp(auth) or time.time() + 3600}
    _DRM_SESSIONS[content_id] = session
    return session


def parse_event_url(raw_url: str) -> tuple[str, str, int | None]:
    """(contentId, league slug, stream end epoch) from an event stream_url."""
    m = re.match(r'paramount://event/([\w-]+)\?slug=([\w-]+)&end=(\d+)$', raw_url or '')
    if not m:
        raise ValueError(f'Unsupported Paramount+ stream URL: {raw_url}')
    return m.group(1), m.group(2), int(m.group(3))


# ── scraper ──────────────────────────────────────────────────────────────────

class ParamountScraper(BaseScraper):
    source_name = 'paramount'
    added_in = '5.7.0'
    display_name = 'Paramount+'
    source_category = 'premium'
    is_premium = True
    scrape_interval = 360
    # The audit's default (linear) scope covers the Paramount+ Channels: it
    # resolves each manifest, confirms Widevine (bridge) and fills the
    # resolution badge. Matches are event channels, skipped unless the audit
    # is run with events included; then their listing is the check.
    stream_audit_enabled = True
    # Marks the source DRM-capable: its DASH channels (Paramount+ Channels) are
    # bridge-only. The real per-channel URL comes from get_license_url.
    license_url = LICENSE_URL

    config_required = True
    # Set by the activation-code sign-in, never typed; hidden from the card.
    config_schema = [
        ConfigField('cookies', 'Paramount+ session', secret=True, hidden=True),
        ConfigField('include_events', 'Include match channels', field_type='toggle', default='true',
                    help_text='Live sports matches as temporary channels, one per match.'),
        ConfigField('include_channels', 'Include Paramount+ Channels', field_type='toggle', default='true',
                    help_text='24/7 channels such as South Park and SpongeBob (DRM bridge only).'),
    ]

    # ── auth ──

    def _cookies(self) -> dict:
        return self.config.get('cookies') or {}

    def _signed_in(self) -> bool:
        return bool(self._cookies().get('CBS_COM'))

    def _flag(self, key: str) -> bool:
        return str(self.config.get(key, 'true')).strip().lower() in ('1', 'true', 'yes', 'on')

    def _refresh_status(self) -> None:
        """Re-check the saved sign-in every 6 hours."""
        if not self._signed_in() or time.time() - float(self.config.get('status_at') or 0) < _STATUS_TTL:
            return
        status = account_status(self._cookies())
        if not status['logged_in']:
            self._signin_lost()
            return
        self._update_config('account_status', status.get('status'))
        self._update_config('status_at', int(time.time()))

    def _signin_lost(self) -> None:
        logger.warning('[paramount] %s', _SIGNIN_LOST)
        for key in SIGNIN_CONFIG_KEYS:
            self._update_config(key, None)
        self._update_config('signin_error', _SIGNIN_LOST)

    def pre_run_setup(self) -> None:
        self._refresh_status()

    # ── schedule ──

    def _get(self, path: str, **params) -> dict:
        r = self.session.get(f'{_APPS_API}/v3.0/androidphone/{path}', timeout=30,
                             params=_app_params(_clientRegion='US', **params),
                             headers={'User-Agent': _TV_UA})
        r.raise_for_status()
        return r.json() or {}

    def _live_channels(self) -> list[dict]:
        cached = getattr(self, '_live_channel_list', None)
        if cached is None:
            cached = self._get('live/channels.json', rows=200, start=0).get('channels') or []
            self._live_channel_list = cached
        return cached

    def _leagues(self) -> list[dict]:
        return [c for c in self._live_channels()
                if 'discrete' in (c.get('channelTypes') or []) and c.get('slug')]

    def _drm_channels(self) -> list[dict]:
        """Paramount+ Channels: 24/7 Widevine loops with a fixed content id,
        plus Showtime East/West when the account can play them."""
        listed = self._live_channels()
        out = [c for c in listed
               if 'vod_to_live' in (c.get('channelTypes') or []) and c.get('slug') and c.get('videoContentId')]
        return out + self._showtime_channels({c.get('slug'): c for c in listed})

    def _showtime_channels(self, listed: dict[str, dict]) -> list[dict]:
        out, available = [], False
        for slug, name in SHOWTIME_SLUGS.items():
            chan = dict(listed.get(slug) or {})
            if not chan.get('videoContentId'):
                try:
                    items = self._get(f'live/channels/{slug}/listings.json', rows=1, start=0,
                                      showListing='true').get('listing') or []
                except requests.RequestException as exc:
                    logger.warning('[paramount] %s listings unavailable: %s', slug, exc)
                    continue
                if not items or not items[0].get('videoContentId'):
                    continue
                chan.update({'slug': slug, 'videoContentId': items[0]['videoContentId']})
            chan.setdefault('channelName', name)
            chan['channelCategorySlugs'] = ['showtime']
            try:
                manifest = drm_session(self._cookies(), chan['videoContentId'])['manifest']
            except ParamountAuthError:
                continue   # signed in, but the plan has no Showtime
            except requests.RequestException as exc:
                logger.warning('[paramount] %s check failed: %s', slug, exc)
                continue
            available = True
            if '.mpd' not in manifest.split('?')[0]:
                logger.warning('[paramount] %s manifest isn\'t DASH (%s); skipped until supported',
                               slug, manifest.split('?')[0][-40:])
                continue
            out.append(chan)
        self._update_config('showtime_available', available)
        return out

    def _events(self) -> list[tuple[dict, dict]]:
        """(league, listing) for every match whose stream window overlaps the next two days."""
        now = datetime.now(timezone.utc)
        horizon = now + timedelta(hours=_EVENT_LOOKAHEAD_HOURS)
        found: dict[str, tuple[dict, dict]] = {}
        for league in self._leagues():
            try:
                listing = self._get(f'live/channels/{league["slug"]}/listings.json', rows=100, start=0,
                                    showListing='true').get('listing') or []
            except requests.RequestException as exc:
                logger.warning('[paramount] %s listings unavailable: %s', league['slug'], exc)
                continue
            for item in listing:
                start = _epoch_ms(item.get('streamStartTimestamp') or item.get('startTimestamp'))
                end = _epoch_ms(item.get('streamEndTimestamp') or item.get('endTimestamp'))
                cid = item.get('videoContentId')
                if not cid or not start or not end or end <= start or end <= now or start >= horizon:
                    continue
                found.setdefault(cid, (league, item))
        return sorted(found.values(), key=lambda li: (li[1].get('startTimestamp') or 0, li[1]['videoContentId']))

    # ── channels / EPG ──

    def fetch_channels(self) -> list[ChannelData]:
        self._event_items = []
        self._drm_items = []
        self.hide_missing_event_channels = True
        if not self._signed_in():
            return []
        self._live_channel_list = None
        self.excluded_channel_ids = set()
        self._event_items = self._events() if self._flag('include_events') else []
        self._matchups = {}
        resolver = MatchupResolver(self.cache.get(MATCHUP_CACHE_KEY), self.source_name)
        channels = []
        for league, item in self._event_items:
            cid = item['videoContentId']
            end = _epoch_ms(item.get('streamEndTimestamp') or item.get('endTimestamp'))
            thumb = (item.get('filePathThumb') or '').removeprefix('files/')
            logo = league.get('filePathLogoSelected') or league.get('filePathLogo')
            sport = normalize_sport(_item_sport(item)) or sport_for_league(league.get('channelName'))
            matchup = resolver.lookup(
                cid, item.get('title'), _epoch_ms(item.get('startTimestamp') or item.get('streamStartTimestamp')),
                league=league.get('channelName'), sport=sport)
            if matchup:
                self._matchups[cid] = matchup
            title = matchup_body(matchup, sport) if matchup else item.get('title') or 'Live'
            channels.append(ChannelData(
                source_channel_id=f'{EVENT_CHANNEL_PREFIX}{cid}',
                name=f'{league.get("channelName") or "Paramount+"} - {title}'[:256],
                stream_url=f'{EVENT_SCHEME}{cid}?slug={league["slug"]}&end={int(end.timestamp())}',
                logo_url=_THUMBS + thumb if thumb else (_IMAGES + logo if logo else None),
                category='Sports',
                language=infer_language(item.get('title'), item.get('description')),
                stream_type='hls',
                guide_key=f'{EVENT_CHANNEL_PREFIX}{cid}',
                tags=[EVENT_CHANNEL_TAG, 'Paramount+ Sports'],
                sport=_item_sport(item),
                league=league.get('channelName'),
                gracenote_mode='off',
                retire_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS),
                delete_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS + 24 * 3600),
            ))
        self._update_cache(MATCHUP_CACHE_KEY, resolver.export())
        channels.extend(self._drm_channel_data())
        return channels

    def _drm_channel_data(self) -> list[ChannelData]:
        if not self._flag('include_channels'):
            # Turned off: retire the rows now rather than after missed scrapes.
            self.excluded_channel_ids = {f'{CHANNEL_PREFIX}{slug}' for slug in self._known_channel_slugs()}
            return []
        try:
            self._drm_items = self._drm_channels()
        except requests.RequestException as exc:
            logger.warning('[paramount] Paramount+ Channels unavailable: %s', exc)
            self._drm_items = []
        out = []
        for c in self._drm_items:
            slug = c['slug']
            cats = c.get('channelCategorySlugs') or []
            category = next((_CHANNEL_CATEGORIES[x] for x in cats if x in _CHANNEL_CATEGORIES), 'Entertainment')
            if slug == 'movies':
                category = 'Movies'
            logo = c.get('filePathLogoSelected') or c.get('filePathLogo')
            name = (c.get('channelName') or slug).strip()
            out.append(ChannelData(
                source_channel_id=f'{CHANNEL_PREFIX}{slug}',
                name=name,
                slug=f'paramount-plus-{slug}',
                stream_url=f'{CHANNEL_SCHEME}{c["videoContentId"]}?slug={slug}',
                logo_url=_IMAGES + logo if logo else None,
                category=category,
                language='en',
                country='US',
                stream_type='dash',
                guide_key=f'PPLUS:{slug}',
                description=(c.get('description') or '').strip() or None,
                tags=['Paramount+ Channels'] + (['Showtime'] if slug in SHOWTIME_SLUGS else []),
            ))
        return out

    def _known_channel_slugs(self) -> set[str]:
        try:
            listed = {c['slug'] for c in self._live_channels()
                      if 'vod_to_live' in (c.get('channelTypes') or []) and c.get('slug')}
        except requests.RequestException:
            listed = set()
        return listed | set(SHOWTIME_SLUGS)

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {ch.source_channel_id for ch in channels}
        programs = self._drm_epg(wanted)
        for league, item in getattr(self, '_event_items', []):
            cid = f'{EVENT_CHANNEL_PREFIX}{item["videoContentId"]}'
            start = _epoch_ms(item.get('startTimestamp') or item.get('streamStartTimestamp'))
            end = _epoch_ms(item.get('endTimestamp') or item.get('streamEndTimestamp'))
            if cid not in wanted or not start or not end or end <= start:
                continue
            thumb = (item.get('filePathThumb') or '').removeprefix('files/')
            league_name = league.get('channelName')
            sport = normalize_sport(_item_sport(item)) or sport_for_league(league_name)
            matchup = getattr(self, '_matchups', {}).get(item['videoContentId'])
            programs.append(ProgramData(
                source_channel_id=cid,
                title=(matchup_title(event_label(league_name, sport), matchup, sport) if matchup
                       else item.get('title') or league_name or 'Paramount+'),
                start_time=start,
                end_time=end,
                description=(matchup_description(matchup, sport, item.get('description')) if matchup
                             else item.get('description') or None),
                poster_url=_THUMBS + thumb if thumb else None,
                category=';'.join(x for x in ('Sports Event', sport, league_name) if x),
                episode_title=league_name if league_name != item.get('title') else None,
                is_live=True,
                episode_id=str(item.get('id') or item['videoContentId']),
            ))
        return programs

    def _drm_epg(self, wanted: set[str]) -> list[ProgramData]:
        programs = []
        for c in getattr(self, '_drm_items', []):
            cid = f'{CHANNEL_PREFIX}{c["slug"]}'
            if cid not in wanted:
                continue
            try:
                listing = self._get(f'live/channels/{c["slug"]}/listings.json', rows=_LISTING_ROWS, start=0,
                                    showListing='true').get('listing') or []
            except requests.RequestException as exc:
                logger.warning('[paramount] %s listings unavailable: %s', c['slug'], exc)
                continue
            for item in listing:
                start = _epoch_ms(item.get('startTimestamp'))
                end = _epoch_ms(item.get('endTimestamp'))
                title = (item.get('title') or '').strip()
                if not start or not end or end <= start or not title:
                    continue
                thumb = (item.get('filePathThumb') or '').removeprefix('files/')
                episode_title = (item.get('episodeTitle') or '').strip()
                aired = _epoch_ms(item.get('originalAirDate'))
                programs.append(ProgramData(
                    source_channel_id=cid,
                    title=title,
                    start_time=start,
                    end_time=end,
                    description=(item.get('description') or '').strip() or None,
                    poster_url=_THUMBS + thumb if thumb else None,
                    rating=item.get('rating') or None,
                    episode_title=episode_title if episode_title and episode_title != title else None,
                    season=item.get('seasonNumber') or None,
                    episode=item.get('episodeNumber') or None,
                    original_air_date=aired.date() if aired else None,
                    # TMS ids: MV… is a movie, EP…/SH… a series episode.
                    program_type='movie' if str(item.get('tmsProgramId') or item.get('tmsSeriesId') or '').startswith('MV') else None,
                    series_id=item.get('tmsSeriesId') or None,
                    episode_id=item.get('tmsProgramId') or (str(item['id']) if item.get('id') else None),
                ))
        return programs

    # ── playback ──

    def _require_signed_in(self) -> None:
        if not self._signed_in():
            raise ParamountAuthError('Paramount+: sign in on the Sources page.')
        self._refresh_status()
        if not self._signed_in():
            raise ParamountAuthError(_SIGNIN_LOST)

    def _metadata(self, content_id: str, slug: str) -> dict:
        r = self.session.get(f'{_PPLUS}/live-tv/live-event/{slug}/{content_id}/metadata.json', timeout=20,
                             cookies=self._cookies(),
                             headers={'User-Agent': _WEB_UA, 'X-Requested-With': 'XMLHttpRequest'})
        r.raise_for_status()
        return r.json() or {}

    def _playback(self, content_id: str, slug: str) -> tuple[str, str | None]:
        """(Authorization header for the key server, DAI asset key) for a match.
        Callers must have passed _require_signed_in."""
        data = self._metadata(content_id, slug)
        aes = (((data.get('CONTENT') or {}).get('drm') or {}).get('aes') or {})
        auth = ((aes.get('header') or {}).get('Authorization') or '').strip()
        if not auth:
            raise RuntimeError('Paramount+: this match isn\'t streaming yet or isn\'t available to this account')
        _KEY_TOKENS[content_id] = (auth, _jwt_exp(auth) or time.time() + 3600)
        video = (data.get('listing') or {}).get('contentCANVideo') or {}
        return auth, video.get('daistreamKey') or (data.get('metaData') or {}).get('daistreamKey')

    def key_authorization(self, raw_url: str, force: bool = False) -> str:
        """The key server's Authorization header for this match, cached until near expiry."""
        content_id, slug, _ = parse_event_url(raw_url)
        self._require_signed_in()
        cached = _KEY_TOKENS.get(content_id)
        if not force and cached and time.time() < cached[1] - _TOKEN_MARGIN:
            return cached[0]
        return self._playback(content_id, slug)[0]

    def audit_resolve(self, raw_url: str) -> str:
        if (raw_url or '').startswith(CHANNEL_SCHEME):
            return self._resolve_channel(raw_url)
        # An upcoming match has no stream yet; its place in the listings is the check.
        return raw_url

    def _resolve_channel(self, raw_url: str) -> str:
        """A Paramount+ Channels DASH manifest (Widevine; bridge-only)."""
        content_id, _ = parse_channel_url(raw_url)
        self._require_signed_in()
        return drm_session(self._cookies(), content_id)['manifest']

    # ── DRM license proxy wiring ──

    @staticmethod
    def _channel_content_id(channel_id: str | None) -> str | None:
        if not channel_id or not channel_id.startswith(CHANNEL_PREFIX):
            return None
        from ..models import Channel, Source
        row = (Channel.query.join(Source).with_entities(Channel.stream_url)
               .filter(Source.name == 'paramount', Channel.source_channel_id == channel_id).first())
        try:
            return parse_channel_url(row[0])[0] if row else None
        except ValueError:
            return None

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        content_id = cls._channel_content_id(channel_id)
        if not content_id:
            # Matches aren't DRM; the base URL only marks the source DRM-capable.
            return cls.license_url if not channel_id else None
        cached = _DRM_SESSIONS.get(content_id)
        return (cached or {}).get('license_url') or (
            f'{LICENSE_URL}?CrmId=cbsi&AccountId=cbsi&SubContentType=Default&ContentId={content_id}')

    @classmethod
    def prepare_license_request(cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs):
        headers = {'Content-Type': 'application/octet-stream', 'Origin': _PPLUS, 'Referer': _PPLUS + '/'}
        content_id = cls._channel_content_id(channel_id)
        cookies = config.get('cookies') or {}
        if content_id and cookies.get('CBS_COM'):
            try:
                headers['Authorization'] = drm_session(cookies, content_id)['auth']
            except (requests.RequestException, ParamountAuthError) as exc:
                logger.warning('[paramount] license token for %s unavailable: %s', channel_id, exc)
        return challenge, headers

    @classmethod
    def get_kodi_props_for_channel(cls, base_url: str, source_channel_id: str) -> dict[str, str]:
        if not (source_channel_id or '').startswith(CHANNEL_PREFIX):
            return {}
        from urllib.parse import quote
        return {'inputstream.adaptive.license_type': 'com.widevine.alpha',
                'inputstream.adaptive.license_key': (
                    f'{base_url}/play/paramount/license?channel_id={quote(source_channel_id, safe="")}||R{{SSM}}|')}

    def resolve(self, raw_url: str) -> str:
        """The match's DAI master playlist. Its key lines need the proxy
        (app/routes/paramount_proxy.py), so play() never redirects here directly. A Paramount+ Channels URL
        resolves to its Widevine DASH manifest instead."""
        if (raw_url or '').startswith(CHANNEL_SCHEME):
            return self._resolve_channel(raw_url)
        content_id, slug, end = parse_event_url(raw_url)
        if end is not None and time.time() >= end + EVENT_END_GRACE_SECONDS:
            raise StreamDeadError('Paramount+ match has ended')
        self._require_signed_in()
        _, asset_key = self._playback(content_id, slug)
        if not asset_key:
            raise RuntimeError('Paramount+: no stream for this match')
        r = self.session.post(f'https://pubads.g.doubleclick.net/ssai/event/{asset_key}/streams', timeout=20,
                              headers={'Content-Type': 'application/x-www-form-urlencoded'})
        manifest = (r.json() or {}).get('stream_manifest') if r.ok else None
        if not manifest:
            raise RuntimeError(f'Paramount+: no stream for this match (HTTP {r.status_code})')
        return manifest

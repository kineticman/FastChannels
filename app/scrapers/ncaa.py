"""NCAA.com: the always-on highlight channels ("24/7 college baseball
highlights", "One Shining Moment", ...) and live championship games, each game
an event channel while it's on.

No login. Everything comes from public NCAA.com endpoints:
  www.ncaa.com/ncaa/liveschedule/today and /monthly → dates[].content[] with
  title, startTimeEpoch, sport ("soccer-women-d3"), url ("/event/<id>" or
  "/game/<id>"), type ("event" | "game"), eventType ("montage" for the 24/7
  channels), isLive. /monthly covers the next 30 days.
  sdataprod.ncaa.com persisted GraphQL queries (the gamecenter page's own):
    GetGamecenterEventById_web {id} → events[].stream[] {mediaId, state, thumbnail}
    GetGamecenterGameById_web {id}  → contests[] {gameState, teams, championship,
      championshipGame.round, location, liveVideos[] {mediaId, url}}
Playback:
  mediaId (the 24/7 channels) → medium.ngtv.io/v2/media/<mediaId>/desktop with
  the web player's public appId → media.desktop.unprotected.unencrypted.url,
  clear HLS (CMAF) on dacis.warnermediacdn.com. Same WBD service as warner_tve.
  liveVideos url = a Hudl vCloud embed (DII/DIII championships, "NCAA
  Championships Pass") → the broadcast's VMAP, as nsn.py does.
The query hashes and appId live in an event page's drupalSettings
(gamecenter.gqlShas, videoConfig.app_id); they're refreshed from there if the
built-in ones stop working.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

from ..sports import normalize_sport
from .base import (BaseScraper, ChannelData, ConfigField, ProgramData, StreamDeadError,
                   EVENT_CHANNEL_TAG)

logger = logging.getLogger(__name__)

SITE = 'https://www.ncaa.com'
TODAY_FEED = f'{SITE}/ncaa/liveschedule/today'
MONTH_FEED = f'{SITE}/ncaa/liveschedule/monthly'
GQL = 'https://sdataprod.ncaa.com/'
NGTV_MEDIA = 'https://medium.ngtv.io/v2/media/{}/desktop'
HUDL_VMAP = 'https://vcloud.hudl.com/api/broadcast/vmap/{}'
HUDL_EMBED_RE = re.compile(r'vcloud\.hudl\.com/broadcast/embed/(\d+)')
_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
       '(KHTML, like Gecko) Chrome/140.0 Safari/537.36')

# The web player's public config (drupalSettings on any event page).
_DEFAULT_APP_ID = (
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuZXR3b3JrIjoibmNhYSIsInByb2R1Y3QiOiJ3ZWJwbGF5ZXIiLCJw'
    'bGF0Zm9ybSI6IndlYiIsImFwcElkIjoibmNhYS13ZWJwbGF5ZXItd2ViLXU3OGV6YyJ9.8bd3aWPvmMr79LNcw3d0IeqqrBs'
    'ZM2gss6jR0oAqheI')
_DEFAULT_SHAS = {
    'GetGamecenterEventById_web': '1d7b19c4f56371728e351440dd8054056668bfe0950c5dffd9c3298c1f08192a',
    'GetGamecenterGameById_web': '26d14df5714c5cd454c9032a1f8ebb1b1dc35173065ab858709b0fa84dd07b5f',
}
# Refreshed values, shared by every scraper instance in this process.
_site_config = {'app_id': _DEFAULT_APP_ID, 'shas': dict(_DEFAULT_SHAS), 'refreshed': 0.0}
_site_lock = threading.Lock()
_REFRESH_MIN_SECONDS = 600

STATION_PREFIX = 'station:'
EVENT_CHANNEL_PREFIX = 'event:'
STATION_SCHEME = 'ncaa://event/'
GAME_SCHEME = 'ncaa://game/'
_EVENT_LOOKAHEAD_HOURS = 48
_EPG_BLOCK = timedelta(hours=4)
_DEFAULT_HOURS = 2.5
_SPORT_HOURS = {'Basketball': 2.25, 'Hockey': 2.75, 'Football': 3.5, 'Volleyball': 2.25,
                'Soccer': 2.25, 'Lacrosse': 2.5, 'Baseball': 3.25, 'Softball': 2.5,
                'Field Hockey': 2.0, 'Wrestling': 3.0, 'Cross Country': 2.0}
_LIVE_EXTENSION = timedelta(minutes=45)
EVENT_END_GRACE_SECONDS = 3600

# NCAA sport slugs ("soccer-women-d3" → sport "soccer") → display name.
_SPORT_NAMES = {
    'basketball': 'Basketball', 'baseball': 'Baseball', 'softball': 'Softball', 'football': 'Football',
    'soccer': 'Soccer', 'fieldhockey': 'Field Hockey', 'volleyball': 'Volleyball',
    'beach-volleyball': 'Beach Volleyball', 'icehockey': 'Hockey', 'lacrosse': 'Lacrosse',
    'wrestling': 'Wrestling', 'swimming': 'Swimming', 'cross-country': 'Cross Country',
    'trackfield-outdoor': 'Track and Field', 'trackfield-indoor': 'Indoor Track and Field',
    'gymnastics': 'Gymnastics', 'golf': 'Golf', 'tennis': 'Tennis', 'rowing': 'Rowing',
    'bowling': 'Bowling', 'waterpolo': 'Water Polo', 'rifle': 'Rifle', 'skiing': 'Skiing',
    'fencing': 'Fencing', 'acrobatics-tumbling': 'Acrobatics & Tumbling',
}
_DIVISIONS = {'d1': 'DI', 'd2': 'DII', 'd3': 'DIII', 'fbs': 'FBS', 'fcs': 'FCS', 'nc': ''}
_ONE_GENDER = frozenset({'Football', 'Baseball', 'Softball', 'Field Hockey'})

# Game filter choices (checklists store the UNticked values).
_FILTER_SPORTS = ('Basketball', 'Baseball', 'Softball', 'Football', 'Soccer', 'Field Hockey',
                  'Volleyball', 'Hockey', 'Lacrosse', 'Wrestling')
_FILTER_DIVISIONS = (('d1', 'Division I (incl. FBS/FCS)'), ('d2', 'Division II'), ('d3', 'Division III'))


def parse_sport(slug: str | None) -> tuple[str | None, str | None, str]:
    """("Soccer", "W", "d3") from "soccer-women-d3"; ("Football", None, "fcs") from "football-fcs"."""
    parts = str(slug or '').lower().split('-')
    division = parts.pop() if parts and parts[-1] in _DIVISIONS else ''
    gender = None
    if parts and parts[-1] in ('men', 'women'):
        gender = 'M' if parts.pop() == 'men' else 'W'
    key = '-'.join(parts)
    name = _SPORT_NAMES.get(key) or (normalize_sport(key.replace('-', ' ').title()) if key else None)
    return name, gender, division


def _gender_word(sport: str | None, gender: str | None) -> str:
    if not gender or sport in _ONE_GENDER:
        return ''
    return "Men's " if gender == 'M' else "Women's "


def league_for(slug: str | None) -> str:
    """League in the feed filter's vocabulary (NSN's and B1G+'s for college)."""
    sport, gender, _ = parse_sport(slug)
    if sport == 'Basketball' and gender in ('M', 'W'):
        return 'NCAAM' if gender == 'M' else 'NCAAW'
    if not sport:
        return 'NCAA'
    return f'NCAA {_gender_word(sport, gender)}{sport}'


def display_label(slug: str | None) -> str:
    """"NCAA DIII Women's Soccer", "NCAA FCS Football", "NCAA Women's Gymnastics"."""
    sport, gender, division = parse_sport(slug)
    div = _DIVISIONS.get(division, '')
    words = ['NCAA', div, f'{_gender_word(sport, gender)}{sport or ""}'.strip()]
    return ' '.join(w for w in words if w)


def division_group(slug: str | None) -> str:
    _, _, division = parse_sport(slug)
    return 'd1' if division in ('d1', 'fbs', 'fcs') else division


SCHOOL_LOGO = 'https://www.ncaa.com/sites/default/files/images/logos/schools/{}/{}.200.png'
SPORT_BADGE = 'https://www.ncaa.com/modules/custom/casablanca_core/img/sportbanners/{}.png'
# Sport slugs (gender and division stripped) with a badge at SPORT_BADGE; others get ncaa.png.
_BADGE_SPORTS = frozenset({
    'basketball', 'baseball', 'softball', 'football', 'soccer', 'cross-country', 'trackfield-outdoor',
    'gymnastics', 'wrestling', 'volleyball', 'icehockey', 'swimming', 'fieldhockey'})


def sport_badge(slug: str | None) -> str:
    """NCAA's 900x1125 sport badge (the event page's banner, which also comes as PNG)."""
    parts = str(slug or '').lower().split('-')
    if parts and parts[-1] in _DIVISIONS:
        parts.pop()
    if parts and parts[-1] in ('men', 'women'):
        parts.pop()
    key = '-'.join(parts)
    return SPORT_BADGE.format(key if key in _BADGE_SPORTS else 'ncaa')


def school_logo(detail: dict) -> str | None:
    """The home team's logo (200px PNG; the site's newer logos are SVG only)."""
    teams = detail.get('teams') or []
    team = next((t for t in teams if t.get('isHome')), None) or (teams[0] if teams else None)
    seo = str((team or {}).get('seoname') or '').strip().lower()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]*', seo):
        return None
    return SCHOOL_LOGO.format(seo[0], seo)


def _item_id(item: dict) -> str:
    return str(item.get('url') or '').rstrip('/').rsplit('/', 1)[-1]


def _epoch(value) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError):
        return None


def parse_stream_url(raw_url: str) -> tuple[str, str, int | None]:
    """('event'|'game', id, end epoch) from ncaa://event/4821 or ncaa://game/6644905?end=..."""
    for kind, scheme in (('event', STATION_SCHEME), ('game', GAME_SCHEME)):
        if raw_url.startswith(scheme):
            rest = raw_url[len(scheme):]
            ident, _, query = rest.partition('?')
            m = re.search(r'(?:^|&)end=(\d+)', query)
            if ident.isdigit():
                return kind, ident, int(m.group(1)) if m else None
    raise ValueError(f'bad NCAA stream url: {raw_url}')


class NCAAScraper(BaseScraper):
    source_name = 'ncaa'
    added_in = '5.9.5'
    display_name = 'NCAA'
    source_category = 'network'   # a network's own website: Sources → Direct from Networks
    scrape_interval = 120
    # Games are events (an upcoming one has nothing to audit); audit_candidates
    # leaves them out and the audit checks the 24/7 channels.
    stream_audit_enabled = True
    config_schema = [
        ConfigField('disabled_game_sports', 'Game sports', field_type='checklist',
                    options=[{'value': s, 'label': s} for s in _FILTER_SPORTS] + [{'value': 'Other', 'label': 'Other sports'}],
                    help_text='Live championship games carried as event channels. Untick a sport to skip its games. '
                              'The 24/7 channels are always listed.'),
        ConfigField('disabled_game_divisions', 'Game divisions', field_type='checklist',
                    options=[{'value': v, 'label': label} for v, label in _FILTER_DIVISIONS]),
    ]

    # ── site config / GraphQL ──

    def _headers(self, accept: str = 'application/json') -> dict:
        return {'User-Agent': _UA, 'Accept': accept, 'Origin': SITE, 'Referer': f'{SITE}/'}

    def _refresh_site_config(self, page_path: str) -> bool:
        """Re-read the query hashes and player appId from an event/game page."""
        with _site_lock:
            if time.time() - _site_config['refreshed'] < _REFRESH_MIN_SECONDS:
                return False
            _site_config['refreshed'] = time.time()
        try:
            r = self.session.get(f'{SITE}{page_path}', headers=self._headers('text/html'), timeout=20)
            r.raise_for_status()
            m = re.search(r'data-drupal-selector="drupal-settings-json">(.*?)</script>', r.text, re.S)
            settings = json.loads(m.group(1)) if m else {}
        except Exception as exc:
            logger.warning('[ncaa] could not refresh site config from %s: %s', page_path, exc)
            return False
        shas = ((settings.get('gamecenter') or {}).get('gqlShas') or {})
        app_id = (settings.get('videoConfig') or {}).get('app_id')
        with _site_lock:
            for op in _DEFAULT_SHAS:
                if shas.get(op):
                    _site_config['shas'][op] = shas[op]
            if app_id:
                _site_config['app_id'] = app_id
        logger.info('[ncaa] refreshed site config from %s (shas=%d, app_id=%s)',
                    page_path, len(shas), 'yes' if app_id else 'no')
        return bool(shas or app_id)

    def _gql(self, op: str, ident: str, page_path: str) -> dict:
        for attempt in (0, 1):
            ext = json.dumps({'persistedQuery': {'version': 1, 'sha256Hash': _site_config['shas'][op]}},
                             separators=(',', ':'))
            r = self.session.get(GQL, headers=self._headers(), timeout=20, params={
                'meta': op, 'extensions': ext, 'variables': json.dumps({'id': ident}, separators=(',', ':'))})
            body = None
            try:
                body = r.json()
            except ValueError:
                pass
            errors = (body or {}).get('errors') if isinstance(body, dict) else None
            if r.ok and isinstance(body, dict) and body.get('data') and not errors:
                return body['data']
            if attempt == 0 and self._refresh_site_config(page_path):
                continue
            detail = errors[0].get('message') if errors and isinstance(errors[0], dict) else f'HTTP {r.status_code}'
            raise RuntimeError(f'NCAA: {op} failed ({detail})')
        raise RuntimeError(f'NCAA: {op} failed')

    def _event(self, event_id: str) -> dict:
        events = self._gql('GetGamecenterEventById_web', int(event_id), f'/event/{event_id}').get('events') or []
        return events[0] if events else {}

    def _game(self, game_id: str) -> dict:
        contests = self._gql('GetGamecenterGameById_web', game_id, f'/game/{game_id}').get('contests') or []
        return contests[0] if contests else {}

    # ── schedule ──

    def _schedule(self) -> list[dict]:
        items: dict[str, dict] = {}
        errors = []
        for url in (TODAY_FEED, MONTH_FEED):
            try:
                r = self.session.get(url, headers=self._headers(), timeout=30)
                r.raise_for_status()
                for day in (r.json() or {}).get('dates') or []:
                    for item in day.get('content') or []:
                        if item.get('url'):
                            # today's copy (fresher isLive) wins
                            items.setdefault(item['url'], item)
            except Exception as exc:
                errors.append(f'{url.rsplit("/", 1)[-1]}: {exc}')
        if errors and not items:
            raise RuntimeError('NCAA schedule unavailable (' + '; '.join(errors) + ')')
        for e in errors:
            logger.warning('[ncaa] schedule feed failed: %s', e)
        return list(items.values())

    @staticmethod
    def _csv_set(config: dict, key: str) -> set[str]:
        return {v.strip() for v in str((config or {}).get(key) or '').split(',') if v.strip()}

    def _game_wanted(self, item: dict) -> bool:
        sport, _, _ = parse_sport(item.get('sport'))
        sports_off = self._csv_set(self.config, 'disabled_game_sports')
        label = sport if sport in _FILTER_SPORTS else 'Other'
        if label in sports_off:
            return False
        return division_group(item.get('sport')) not in self._csv_set(self.config, 'disabled_game_divisions')

    def _collect(self) -> None:
        now = datetime.now(timezone.utc)
        horizon = now + timedelta(hours=_EVENT_LOOKAHEAD_HOURS)
        self._stations: list[tuple[dict, dict]] = []
        self._games: list[tuple[dict, dict, datetime, datetime]] = []
        self.excluded_channel_ids = set()
        for item in self._schedule():
            ident = _item_id(item)
            if not ident.isdigit():
                continue
            if item.get('type') == 'event' and item.get('eventType') == 'montage':
                if not item.get('isLive'):
                    continue
                try:
                    detail = self._event(ident)
                except Exception as exc:
                    logger.warning('[ncaa] event %s details failed: %s', ident, exc)
                    detail = {}
                self._stations.append((item, detail))
            elif item.get('type') == 'game':
                start = _epoch(item.get('startTimeEpoch'))
                if not start or start >= horizon:
                    continue
                sport, _, _ = parse_sport(item.get('sport'))
                end = start + timedelta(hours=_SPORT_HOURS.get(sport, _DEFAULT_HOURS))
                if item.get('isLive') and end <= now:
                    end = now + _LIVE_EXTENSION
                if end <= now:
                    continue
                if not self._game_wanted(item):
                    self.excluded_channel_ids.add(f'{EVENT_CHANNEL_PREFIX}{ident}')
                    continue
                try:
                    detail = self._game(ident)
                except Exception as exc:
                    logger.warning('[ncaa] game %s details failed: %s', ident, exc)
                    detail = {}
                if str(detail.get('gameState') or '').upper() == 'F':
                    continue
                if detail and not detail.get('liveVideos'):
                    continue   # no stream for this game
                self._games.append((item, detail, start, end))
        self._stations.sort(key=lambda s: int(_item_id(s[0])))
        self._games.sort(key=lambda g: (g[2], _item_id(g[0])))

    # ── channels / guide ──

    @staticmethod
    def _station_thumb(detail: dict) -> str | None:
        for s in detail.get('stream') or []:
            if s.get('thumbnail'):
                return s['thumbnail']
        return detail.get('thumbnail') or None

    @staticmethod
    def _station_sport_label(item: dict) -> str:
        """"NCAA Softball 24/7", "NCAA FCS Football 24/7", "NCAA Women's Gymnastics 24/7".
        The 24/7 channels' slugs default to men's and DI, so only women's, FBS
        and FCS are named."""
        sport, gender, division = parse_sport(item.get('sport'))
        words = ['NCAA', division.upper() if division in ('fbs', 'fcs') else '',
                 "Women's" if gender == 'W' and sport not in _ONE_GENDER else '', sport or '', '24/7']
        return ' '.join(w for w in words if w)

    def _station_name(self, item: dict) -> str:
        """By sport; the site's own title when several channels share a sport
        (basketball's Classic Games, Legends of March, One Shining Moment)."""
        label = self._station_sport_label(item)
        shared = sum(1 for other, _ in self._stations if self._station_sport_label(other) == label) > 1
        if not shared:
            return label
        title = ' '.join(str(item.get('title') or '').split())
        return title if title.lower().startswith('ncaa') else f'NCAA {title}'

    def fetch_channels(self) -> list[ChannelData]:
        self.hide_missing_event_channels = True
        self._collect()
        channels = []
        for item, detail in self._stations:
            ident = _item_id(item)
            sport, _, _ = parse_sport(item.get('sport'))
            channels.append(ChannelData(
                source_channel_id=f'{STATION_PREFIX}{ident}',
                name=self._station_name(item)[:256],
                stream_url=f'{STATION_SCHEME}{ident}',
                logo_url=sport_badge(item.get('sport')),
                category='Sports',
                stream_type='hls',
                guide_key=f'{STATION_PREFIX}{ident}',
                tags=['NCAA'],
                sport=sport,
                # The slugs default to men's; only basketball's are really split.
                league=(league_for(item.get('sport')) if sport == 'Basketball'
                        else league_for(item.get('sport')).replace("Men's ", '')),
                gracenote_mode='off',
            ))
        for item, detail, start, end in self._games:
            ident = _item_id(item)
            sport, _, _ = parse_sport(item.get('sport'))
            title = ' '.join(str(item.get('title') or '').split()) or 'NCAA Live'
            channels.append(ChannelData(
                source_channel_id=f'{EVENT_CHANNEL_PREFIX}{ident}',
                name=f'{display_label(item.get("sport"))} - {title}'[:256],
                stream_url=f'{GAME_SCHEME}{ident}?end={int(end.timestamp())}',
                logo_url=school_logo(detail) or sport_badge(item.get('sport')),
                category='Sports',
                stream_type='hls',
                guide_key=f'{EVENT_CHANNEL_PREFIX}{ident}',
                tags=[EVENT_CHANNEL_TAG, 'NCAA'],
                sport=sport,
                league=league_for(item.get('sport')),
                gracenote_mode='off',
                retire_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS),
                delete_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS + 24 * 3600),
            ))
        return channels

    @staticmethod
    def _game_description(item: dict, detail: dict) -> str:
        parts = []
        champ = (detail.get('championship') or {}).get('title')
        rnd = ((detail.get('championshipGame') or {}).get('round') or {}).get('title')
        if champ:
            parts.append(f'{champ}, {rnd}' if rnd else champ)
        teams = detail.get('teams') or []
        away = next((t for t in teams if not t.get('isHome')), None)
        home = next((t for t in teams if t.get('isHome')), None)
        if away and home:
            def side(t):
                rec = f" {t['record']}" if t.get('record') else ''
                return f"{t.get('nameFull') or t.get('nameShort')}{rec}"
            parts.append(f'{side(away)} at {side(home)}')
        loc = detail.get('location') or {}
        where = ', '.join(x for x in (loc.get('venue'), loc.get('city'), loc.get('stateUsps')) if x)
        if where:
            parts.append(where)
        return '. '.join(parts) + ('.' if parts else '') + ' Live on NCAA.com.'

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {ch.source_channel_id for ch in channels}
        if not hasattr(self, '_stations'):
            self._collect()
        programs = []
        now = datetime.now(timezone.utc)
        block_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=now.hour % 4)
        blocks = int(_EVENT_LOOKAHEAD_HOURS / 4) + 1
        for item, detail in self._stations:
            cid = f'{STATION_PREFIX}{_item_id(item)}'
            if cid not in wanted:
                continue
            sport, _, _ = parse_sport(item.get('sport'))
            title = ' '.join(str(item.get('title') or '').split()) or display_label(item.get('sport'))
            desc = (detail.get('description') or '').strip() or f'{display_label(item.get("sport"))} highlights, all day.'
            for i in range(blocks):
                start = block_start + i * _EPG_BLOCK
                programs.append(ProgramData(
                    source_channel_id=cid,
                    title=title,
                    start_time=start,
                    end_time=start + _EPG_BLOCK,
                    description=desc,
                    poster_url=self._station_thumb(detail),
                    category=';'.join(x for x in ('Sports', sport) if x),
                ))
        for item, detail, start, end in self._games:
            ident = _item_id(item)
            cid = f'{EVENT_CHANNEL_PREFIX}{ident}'
            if cid not in wanted:
                continue
            sport, _, _ = parse_sport(item.get('sport'))
            league = league_for(item.get('sport'))
            title = ' '.join(str(item.get('title') or '').split()) or 'NCAA Live'
            programs.append(ProgramData(
                source_channel_id=cid,
                title=f'{display_label(item.get("sport"))}: {title}',
                start_time=start,
                end_time=end,
                description=self._game_description(item, detail),
                poster_url=school_logo(detail),
                category=';'.join(x for x in ('Sports Event', sport, league) if x),
                is_live=True,
                episode_id=ident,
            ))
        return programs

    # ── playback ──

    audit_skip_reason = 'Game channel: checked when it plays, not by the audit'

    def audit_candidates(self, query):
        """The 24/7 channels only. A game between now and its start has no
        stream, and passing it through as healthy would re-activate an ended
        game the scrape had hidden."""
        from ..models import Channel
        return query.filter(Channel.stream_url.notlike(f'{GAME_SCHEME}%'))

    def audit_resolve(self, raw_url: str) -> str:
        return self.resolve(raw_url)

    def _ngtv(self, media_id: str, page_path: str) -> str:
        for attempt in (0, 1):
            r = self.session.get(NGTV_MEDIA.format(media_id), headers=self._headers(), timeout=20,
                                 params={'appId': _site_config['app_id']})
            try:
                body = r.json()
            except ValueError:
                body = {}
            desktop = ((body.get('media') or {}).get('desktop') or {}) if isinstance(body, dict) else {}
            clear = ((desktop.get('unprotected') or {}).get('unencrypted') or {}).get('url')
            if clear:
                return clear
            if desktop and not desktop.get('unprotected'):
                raise StreamDeadError('NCAA: stream is DRM-protected')
            if attempt == 0 and r.status_code in (400, 401, 403) and self._refresh_site_config(page_path):
                continue
            err = (body.get('error') or {}).get('message') if isinstance(body, dict) else None
            raise RuntimeError(f'NCAA: no playable stream ({err or f"HTTP {r.status_code}"})')
        raise RuntimeError('NCAA: no playable stream')

    def _hudl(self, broadcast_id: str) -> str:
        r = self.session.get(HUDL_VMAP.format(broadcast_id), timeout=20, headers={
            'User-Agent': _UA, 'Referer': f'https://vcloud.hudl.com/broadcast/embed/{broadcast_id}'})
        r.raise_for_status()
        try:
            root = ET.fromstring(r.text)
        except ET.ParseError as exc:
            raise RuntimeError(f'NCAA: unreadable Hudl player config ({exc})') from exc
        for content in root.iter('Content'):
            url = (content.text or '').strip()
            if content.get('type') == 'live' and content.get('streaming') == 'true' and '.m3u8' in url:
                return url
        b = root.find('.//Broadcast')
        status = (b.get('status') if b is not None else None) or 'Upcoming'
        if status.lower() in ('archived', 'finished', 'unavailable'):
            raise StreamDeadError(f'NCAA game has ended ({status})')
        raise RuntimeError(f'NCAA: no live stream yet ({status})')

    def resolve(self, raw_url: str) -> str:
        kind, ident, end = parse_stream_url(raw_url)
        if kind == 'event':
            detail = self._event(ident)
            for s in detail.get('stream') or []:
                if s.get('enabled') is not False and s.get('mediaId'):
                    return self._ngtv(s['mediaId'], f'/event/{ident}')
            raise StreamDeadError('NCAA: channel has no stream')
        if end is not None and time.time() >= end + EVENT_END_GRACE_SECONDS:
            raise StreamDeadError('NCAA game has ended')
        detail = self._game(ident)
        if not detail:
            raise StreamDeadError('NCAA: game not found')
        if str(detail.get('gameState') or '').upper() == 'F':
            raise StreamDeadError('NCAA game has ended')
        videos = [v for v in detail.get('liveVideos') or [] if v.get('enabled') is not False]
        for v in videos:
            if v.get('mediaId'):
                return self._ngtv(v['mediaId'], f'/game/{ident}')
        for v in videos:
            m = HUDL_EMBED_RE.search(v.get('url') or '')
            if m:
                return self._hudl(m.group(1))
        if any(v.get('url') for v in videos):
            raise RuntimeError(f'NCAA: unsupported stream host ({videos[0].get("url")})')
        raise RuntimeError('NCAA: no live stream yet')

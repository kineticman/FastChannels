from __future__ import annotations

import html
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from urllib.parse import quote, urlparse

import requests

from .base import BaseScraper, ChannelData, ProgramData
from ..models import TVEAccount
from ..tve.adobe_pass import (
    TVEAuthError,
    TVENotAuthorizedError,
    authorize_mvpd,
    discover_aenetworks_software_statement,
    invalidate_aenetworks_software_statement,
)

logger = logging.getLogger(__name__)

_SCHEME = 'aenetworks-tve://'
_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'


@dataclass(frozen=True)
class AENetwork:
    channel_id: str
    name: str
    brand: str
    requestor_id: str
    asset_key: str
    play_url: str
    schedule_url: str
    logo_url: str
    category: str = 'Entertainment'
    schedule_data_prefix: str = ''
    schedule_days_before: int = 0
    schedule_days_after: int = 0

    @property
    def stream_url(self) -> str:
        return f'{_SCHEME}{self.channel_id}'

    @property
    def resource(self) -> str:
        return f'<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/"><channel><title>{self.requestor_id}</title><item></item></channel></rss>'

    @property
    def redirect_url(self) -> str:
        domain = urlparse(self.schedule_url).netloc
        return f'https://{domain}/mvpd-auth?redirect_url={quote(self.play_url, safe="")}'

    @property
    def dai_master(self) -> str:
        return f'https://dai.google.com/linear/hls/event/{self.asset_key}/master.m3u8'


_NETWORKS: dict[str, AENetwork] = {
    'history': AENetwork(
        channel_id='history',
        name='History',
        brand='history',
        requestor_id='HISTORY',
        asset_key='3pCfCAVSTz24VQ7jZDXLzw',
        play_url='https://play.history.com/live',
        schedule_url='https://www.history.com/schedule',
        logo_url='https://www.history.com/assets/images/history/apple-touch-icon.png',
        schedule_data_prefix='https://www.history.com/schedule/data',
        schedule_days_before=0,
        schedule_days_after=2,
    ),
    'aetv': AENetwork(
        channel_id='aetv',
        name='A&E',
        brand='aetv',
        requestor_id='AETV',
        asset_key='4elNLJnvT_ifl3Qs6G67yQ',
        play_url='https://play.aetv.com/live',
        schedule_url='https://www.aetv.com/schedule',
        logo_url='https://www.aetv.com/assets/images/aetv/apple-touch-icon.png',
        schedule_data_prefix='https://www.aetv.com/schedule/data',
        schedule_days_before=0,
        schedule_days_after=2,
    ),
    'lifetime': AENetwork(
        channel_id='lifetime',
        name='Lifetime',
        brand='lifetime',
        requestor_id='LIFETIME',
        asset_key='E54WsMQ2RjCPjg5DriaUCA',
        play_url='https://play.mylifetime.com/live',
        schedule_url='https://www.mylifetime.com/schedule',
        logo_url='https://www.mylifetime.com/assets/images/lifetime/apple-touch-icon.png',
        category='Movies',
        schedule_data_prefix='https://www.mylifetime.com/schedule/data',
        schedule_days_before=0,
        schedule_days_after=14,
    ),
    'fyi': AENetwork(
        channel_id='fyi',
        name='FYI',
        brand='fyi',
        requestor_id='FYI',
        asset_key='-umds2mLSeCPi0vUgua6Kg',
        play_url='https://play.fyi.tv/live',
        schedule_url='https://www.fyi.tv/schedule',
        logo_url='https://www.fyi.tv/assets/images/fyi/apple-touch-icon.png',
        category='Lifestyle',
        schedule_data_prefix='https://www.fyi.tv/schedule/data',
        schedule_days_before=0,
        schedule_days_after=2,
    ),
}

_ITEM_START_RE = re.compile(r'<li\b[^>]*class="[^"]*listing-item[^"]*"[^>]*data-starttime="(\d+)"[^>]*>', re.S | re.I)
_SHOW_RE = re.compile(r'<h3\b[^>]*class="[^"]*show-name[^"]*"[^>]*>(.*?)</h3>', re.S | re.I)
_EPISODE_RE = re.compile(r'<h4\b[^>]*class="[^"]*episode-title[^"]*"[^>]*>(.*?)</h4>', re.S | re.I)
_DESC_RE = re.compile(r'<p\b[^>]*class="[^"]*description[^"]*"[^>]*>(.*?)</p>', re.S | re.I)
_RATING_RE = re.compile(r'<li\b[^>]*class="[^"]*episode-rating[^"]*"[^>]*>(.*?)</li>', re.S | re.I)
_IMG_RE = re.compile(r'<img\b[^>]*src="([^"]+)"', re.S | re.I)
_SEASON_EP_RE = re.compile(r'S\s*(\d+)\s*[|E]\s*E?\s*(\d+)', re.I)
_TAG_RE = re.compile(r'<[^>]+>')


def _clean(text: str | None) -> str | None:
    if not text:
        return None
    text = _TAG_RE.sub(' ', text)
    text = html.unescape(text).replace('\xa0', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    return text or None


def _nested_text(data: dict, *keys: str) -> str | None:
    cur = data
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return _clean(str(cur)) if cur is not None else None


def _nested_int(data: dict, *keys: str) -> int | None:
    value = _nested_text(data, *keys)
    if not value:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed > 0 and parsed < 90000 else None


def _json_image(row: dict) -> str | None:
    video_sizes = (((row.get('video') or {}).get('images') or {}).get('sizes') or {})
    image = video_sizes.get('video_16x9')
    if image:
        return f'{image}?w=320'

    for key in ('program', 'series', 'parent'):
        sizes = (((row.get(key) or {}).get('images') or {}).get('sizes') or {})
        if isinstance(sizes, dict):
            for size_key in ('video_16x9', 'thumbnail', 'medium', 'medium_large'):
                image = sizes.get(size_key)
                if isinstance(image, str) and image.startswith('http'):
                    return image
    return None


def _parse_schedule_json(network: AENetwork) -> list[ProgramData]:
    if not network.schedule_data_prefix:
        return []

    local_today = datetime.now(ZoneInfo('America/New_York')).date()
    seen: set[str] = set()
    rows: list[dict] = []
    headers = {
        'User-Agent': _UA,
        'Accept': 'application/json,*/*',
        'X-Requested-With': 'XMLHttpRequest',
    }
    for offset in range(-network.schedule_days_before, network.schedule_days_after + 1):
        day = local_today + timedelta(days=offset)
        url = f'{network.schedule_data_prefix}/{day.month}/{day.day}/{day.year}/ET'
        try:
            r = requests.get(url, headers=headers, timeout=25)
            r.raise_for_status()
            data = r.json()
        except (requests.RequestException, ValueError):
            continue
        if not isinstance(data, list):
            continue
        for row in data:
            if not isinstance(row, dict):
                continue
            row_id = str(row.get('timeslotId') or row.get('id') or '')
            if row_id and row_id in seen:
                continue
            if row_id:
                seen.add(row_id)
            start_ms = row.get('startTime')
            end_ms = row.get('endTime')
            if not isinstance(start_ms, int) or not isinstance(end_ms, int) or end_ms <= start_ms:
                continue
            series_title = _nested_text(row, 'series', 'title')
            parent_title = _nested_text(row, 'parent', 'title')
            program_title = _nested_text(row, 'program', 'title')
            title = series_title or program_title or parent_title or f'{network.name} Live'
            episode_title = program_title if program_title and program_title != title else None
            rows.append({
                'start': datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc),
                'end': datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc),
                'title': title,
                'episode_title': episode_title,
                'description': _nested_text(row, 'program', 'description') or _nested_text(row, 'program', 'shortDescription'),
                'rating': _nested_text(row, 'program', 'rating') or _nested_text(row, 'rating'),
                'poster_url': _json_image(row),
                'season': _nested_int(row, 'program', 'tvSeasonNumber'),
                'episode': _nested_int(row, 'program', 'tvSeasonEpisodeNumber'),
                'program_type': _nested_text(row, 'program', 'programType'),
                'series_id': _nested_text(row, 'series', 'id') or _nested_text(row, 'program', 'seriesId'),
                'episode_id': _nested_text(row, 'program', 'id') or _nested_text(row, 'programId'),
            })

    rows.sort(key=lambda row: row['start'])
    return [
        ProgramData(
            source_channel_id=network.channel_id,
            title=row['title'],
            description=row['description'],
            start_time=row['start'],
            end_time=row['end'],
            poster_url=row['poster_url'],
            category=network.category,
            rating=row['rating'],
            episode_title=row['episode_title'],
            season=row['season'],
            episode=row['episode'],
            is_live=True,
            program_type=row['program_type'],
            series_id=row['series_id'],
            episode_id=row['episode_id'],
        )
        for row in rows
    ]


_NOW_PLAYING_QUERY = '''query getNowPlaying { nowPlaying { id startTime endTime program { __typename ... on Episode { id title description tvSeasonEpisodeNumber tvSeasonNumber rating genres series { title id } } ... on Movie { id title description rating genres } ... on Special { id title description rating genres } } } }'''


def _parse_live_now_playing(network: AENetwork) -> ProgramData | None:
    endpoint = f'https://yoga.appsvcs.aetnd.com?brand={network.brand}&mode=live&platform=web'
    try:
        r = requests.post(
            endpoint,
            headers={
                'User-Agent': _UA,
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Origin': network.play_url.rsplit('/', 1)[0],
                'Referer': network.play_url,
                'x-apollo-operation-name': 'getNowPlaying',
            },
            json={'operationName': 'getNowPlaying', 'query': _NOW_PLAYING_QUERY, 'variables': {}},
            timeout=20,
        )
        r.raise_for_status()
        payload = r.json()
    except (requests.RequestException, ValueError):
        return None

    row = ((payload.get('data') or {}).get('nowPlaying') or {}) if isinstance(payload, dict) else {}
    program = row.get('program') or {}
    start_s = row.get('startTime')
    end_s = row.get('endTime')
    if not isinstance(start_s, int) or not isinstance(end_s, int) or end_s <= start_s:
        return None

    typename = _nested_text(program, '__typename')
    series_title = _nested_text(program, 'series', 'title')
    program_title = _nested_text(program, 'title')
    is_episode = typename == 'Episode'
    title = series_title if is_episode and series_title else program_title or f'{network.name} Live'
    episode_title = program_title if is_episode and program_title and program_title != title else None
    return ProgramData(
        source_channel_id=network.channel_id,
        title=title,
        description=_nested_text(program, 'description'),
        start_time=datetime.fromtimestamp(start_s, tz=timezone.utc),
        end_time=datetime.fromtimestamp(end_s, tz=timezone.utc),
        category=network.category,
        rating=_nested_text(program, 'rating'),
        episode_title=episode_title,
        season=_nested_int(program, 'tvSeasonNumber'),
        episode=_nested_int(program, 'tvSeasonEpisodeNumber'),
        is_live=True,
        program_type=(typename or '').lower() or None,
        series_id=_nested_text(program, 'series', 'id'),
        episode_id=_nested_text(program, 'id') or _nested_text(row, 'id'),
    )


def _has_current_program(programs: list[ProgramData]) -> bool:
    now = datetime.now(timezone.utc)
    return any(p.start_time <= now < p.end_time for p in programs)


def _parse_schedule_page(network: AENetwork) -> list[ProgramData]:
    try:
        r = requests.get(network.schedule_url, headers={'User-Agent': _UA}, timeout=25)
        r.raise_for_status()
    except requests.RequestException:
        return []

    rows = []
    matches = list(_ITEM_START_RE.finditer(r.text))
    for idx, match in enumerate(matches):
        start_ms = int(match.group(1))
        body_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(r.text)
        body = r.text[match.end():body_end]
        show = _clean((_SHOW_RE.search(body) or [None, None])[1])
        episode_title = _clean((_EPISODE_RE.search(body) or [None, None])[1])
        description = _clean((_DESC_RE.search(body) or [None, None])[1])
        rating = _clean((_RATING_RE.search(body) or [None, None])[1])
        poster_url = None
        img = _IMG_RE.search(body)
        if img:
            poster_url = html.unescape(img.group(1)).strip() or None
        season = episode = None
        se = _SEASON_EP_RE.search(_clean(body) or '')
        if se:
            season, episode = int(se.group(1)), int(se.group(2))
        title = show or episode_title or f'{network.name} Live'
        rows.append({
            'start': datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc),
            'title': title,
            'episode_title': episode_title if episode_title and episode_title != title else None,
            'description': description,
            'rating': rating,
            'poster_url': poster_url,
            'season': season,
            'episode': episode,
        })

    programs: list[ProgramData] = []
    rows.sort(key=lambda row: row['start'])
    for idx, row in enumerate(rows):
        end = rows[idx + 1]['start'] if idx + 1 < len(rows) else row['start'] + timedelta(hours=1)
        if end <= row['start']:
            continue
        programs.append(ProgramData(
            source_channel_id=network.channel_id,
            title=row['title'],
            description=row['description'],
            start_time=row['start'],
            end_time=end,
            poster_url=row['poster_url'],
            category=network.category,
            rating=row['rating'],
            episode_title=row['episode_title'],
            season=row['season'],
            episode=row['episode'],
            is_live=True,
        ))
    return programs


def _placeholder_epg(network: AENetwork) -> list[ProgramData]:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return [
        ProgramData(
            source_channel_id=network.channel_id,
            title=f'{network.name} Live',
            description=f'Live {network.name} TV Everywhere programming.',
            start_time=now + timedelta(hours=i),
            end_time=now + timedelta(hours=i + 1),
            category=network.category,
            is_live=True,
        )
        for i in range(24)
    ]


class AENetworksTVEScraper(BaseScraper):
    source_name = 'aenetworks_tve'
    display_name = 'A+E Networks TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True

    def fetch_channels(self):
        return [
            ChannelData(
                source_channel_id=network.channel_id,
                name=network.name,
                slug=f'{network.channel_id}-tve',
                logo_url=network.logo_url,
                stream_url=network.stream_url,
                stream_type='hls',
                category=network.category,
                language='en',
                country='US',
                guide_key=network.requestor_id,
                description='TV Everywhere live stream authorized through the configured Cox account.',
            )
            for network in _NETWORKS.values()
        ]

    def fetch_epg(self, channels, **kwargs):
        wanted = {ch.source_channel_id for ch in channels}
        total = sum(1 for channel_id in _NETWORKS if channel_id in wanted)
        done = 0
        programs: list[ProgramData] = []
        for channel_id, network in _NETWORKS.items():
            if channel_id not in wanted:
                continue
            parsed = _parse_schedule_json(network) if network.schedule_data_prefix else _parse_schedule_page(network)
            live_now = _parse_live_now_playing(network)
            if live_now and not _has_current_program(parsed):
                parsed.append(live_now)
                parsed.sort(key=lambda p: p.start_time)
            programs.extend(parsed or _placeholder_epg(network))
            done += 1
            if self._progress_cb:
                self._progress_cb('epg', done, total)
        return programs

    def resolve(self, raw_url: str) -> str:
        parsed = urlparse(raw_url or '')
        channel_id = parsed.netloc or parsed.path.lstrip('/')
        network = _NETWORKS.get(channel_id)
        if not raw_url.startswith(_SCHEME) or not network:
            raise TVEAuthError(f'Unsupported A+E TVE stream URL: {raw_url}')

        # The Google DAI event stream itself is unauthenticated today — verified
        # live against all four networks, no MVPD/Cox token required. Still
        # attempt Cox auth when an account is configured, on the chance that's
        # an oversight on A+E's end rather than a deliberate design, but never
        # let it block or slow down what already plays without it.
        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if account and account.is_enabled and account.has_credentials():
            cfg = account.config or {}
            configured_statement = (cfg.get('software_statement') or '').strip()
            try:
                statement = configured_statement or discover_aenetworks_software_statement(network.brand)
                authorize_mvpd(
                    account,
                    requestor_id=network.requestor_id,
                    resource=network.resource,
                    software_statement=statement,
                    redirect_url=network.redirect_url,
                )
            except TVENotAuthorizedError as exc:
                logger.warning('[aenetworks-tve] MVPD not authorized for %s: %s', network.brand, exc)
            except TVEAuthError as exc:
                if not configured_statement:
                    invalidate_aenetworks_software_statement(network.brand)
                logger.warning('[aenetworks-tve] MVPD auth failed for %s: %s', network.brand, exc)
        return network.dai_master

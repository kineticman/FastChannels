"""Adult Swim's public 24/7 marathon streams (adultswim.com/streams).

Was its own source ('adultswim') until it was folded into Free Networks; the
network key is the old source name, channel ids are the old ones (the stream
slug), and the stream ref is the old opaque adultswim://stream?... URL, so
moved channel rows keep their tvg-id, play URL and settings
(app/folded_sources.py).

The streams page's __NEXT_DATA__ Redux state lists the streams (slug, title,
video id, marathon id, poster) and embeds today's marathon schedules. Each
stream's stable HLS asset (adultswim-vodlive.cdn.turner.com/live/...) comes
from the shows video API and is kept in the ref, so play time needs no call.
The guide is the marathon schedule API merged with the embedded schedule,
with episode descriptions, ratings and posters from each show's page.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from ._common import Network, NetChannel
from ..base import ProgramData

logger = logging.getLogger('app.scrapers.free_networks')

_BASE = 'https://www.adultswim.com'
_CATALOG = _BASE + '/streams/aqua-teen-hunger-force'
_VIDEO = _BASE + '/api/shows/v1/videos/{video_id}'
_SCHEDULE = _BASE + '/api/schedule/marathons/{marathon_id}'
_HEADERS = {'Accept': 'application/json, text/plain, */*', 'Origin': _BASE, 'Referer': _BASE + '/streams/'}
_SCHEME = 'adultswim://stream'
_LIVE = 'https://adultswim-vodlive.cdn.turner.com/live/{path}/stream.m3u8'
_NO_SHOW_PAGE = frozenset({'channel-5'})
# Catalog rows whose video API entry lists no stream, though the stream is live.
_ASSET_FALLBACKS = {'off-the-air-smalls': _LIVE.format(path='off-the-air')}
# Catalog slugs whose show page lives under another slug.
_SHOW_PAGE_SLUGS = {'off-the-air-smalls': 'off-the-air'}
# Live marathon streams with schedules that the catalog no longer lists
# (Channels DVR plays Mr. Pickles too): slug, title, marathon id, stream path,
# poster (the show page's og:image). Added only while the stream is up.
_UNLISTED = (
    ('mr-pickles', 'Mr. Pickles', '5801264b1bb9661440d5f848', 'mr-pickles',
     'https://i.cdn.turner.com/adultswim/big/img/2018/04/18/MrPicklesS02_fbsearchTn.jpg'),
    ('squidbillies', 'Squidbillies', '5801264b1bb9661440d5f84a', 'squidbillies',
     'https://media.cdn.adultswim.com/uploads/20211107/21117198155-SquidbilliesMetaThumb420x236.png'),
    ('check-it-out-with-dr-steve-brule', 'Check It Out! with Dr. Steve Brule', '5801264b1bb9661440d5f84d',
     'check-it-out', 'https://i.cdn.turner.com/adultswim/big/img/2014/08/22/CheckitOut2.jpg'),
)
_VIDEO_FIELDS = ('title,type,duration,collection_title,poster,stream,title_id,auth,'
                 'media_id,launch_date,auth_launch_date,disable_ads,season_number,episode_number')

_EPISODE_CACHE_TTL = 6 * 3600
_episode_cache: dict[str, tuple[float, dict[str, list[dict[str, Any]]]]] = {}
_episode_cache_lock = threading.Lock()


class AdultSwim(Network):
    key = 'adultswim'
    name = 'Adult Swim'

    def channels(self, s) -> list[NetChannel]:
        state = self._state(s)
        streams = state.get('streams')
        if not isinstance(streams, list):
            logger.warning('[free_networks] Adult Swim: streams page state has no stream catalog')
            return []
        rows = [r for r in streams if isinstance(r, dict)
                and all(_clean(r.get(k)) for k in ('id', 'title', 'stream', 'vod_to_live_id'))]

        assets: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(_stable_asset, s, _clean(r['stream'])): r for r in rows}
            for fut in as_completed(futures):
                vid = _clean(futures[fut]['stream'])
                try:
                    url = fut.result()
                except Exception as exc:  # noqa: BLE001
                    logger.debug('[free_networks] Adult Swim video metadata failed for %s: %s', vid, exc)
                    continue
                if url:
                    assets[vid] = url

        out: list[NetChannel] = []
        no_asset = []
        for r in rows:
            slug, title, vid = _clean(r['id']), _clean(r['title']), _clean(r['stream'])
            asset = assets.get(vid) or _ASSET_FALLBACKS.get(slug)
            if not asset:
                no_asset.append(title)
                continue
            out.append(NetChannel(slug, title, _ref(slug, vid, _clean(r['vod_to_live_id']), asset),
                                  logo=_clean(r.get('poster')) or None, category='Comedy',
                                  description=_clean(r.get('description')) or None))
        listed = {c.id for c in out}
        for slug, title, marathon_id, path, poster in _UNLISTED:
            asset = _LIVE.format(path=path)
            if slug not in listed and _stream_is_up(s, asset):
                out.append(NetChannel(slug, title, _ref(slug, '', marathon_id, asset), logo=poster, category='Comedy'))
        if no_asset:
            # Usually a stream that's been off the air for months, not a fault.
            logger.debug('[free_networks] Adult Swim: skipped %d with no stable HLS asset: %s',
                         len(no_asset), ', '.join(no_asset))
        return out

    def programs(self, s, channels: list[NetChannel]) -> list[ProgramData]:
        state = self._state(s)
        embedded = state.get('marathon') if isinstance(state.get('marathon'), dict) else {}
        posters = {_clean(r.get('id')): _clean(r.get('poster'))
                   for r in state.get('streams') or [] if isinstance(r, dict)}
        posters.update({u[0]: u[4] for u in _UNLISTED})

        def one(ch: NetChannel) -> list[ProgramData]:
            marathon_id = _parse_ref(ch.stream).get('marathon_id')
            if not marathon_id:
                return []
            rows: list[dict] = []
            r = s.get(_SCHEDULE.format(marathon_id=marathon_id), headers=_HEADERS)
            if r:
                try:
                    rows = _schedule_rows(r.json())
                except Exception as exc:  # noqa: BLE001
                    logger.debug('[free_networks] Adult Swim: bad schedule JSON for %s: %s', ch.id, exc)
            rows = _merge_rows(rows, _schedule_rows(embedded.get(marathon_id)))
            episodes = _episode_metadata(s, ch.id) if rows else {}
            out = []
            for i, row in enumerate(rows):
                nxt = _start(rows[i + 1]) if i + 1 < len(rows) else None
                p = _program(ch, posters.get(ch.id) or None, row, _match_episode(row, episodes), nxt)
                if p:
                    out.append(p)
            return out

        programs: list[ProgramData] = []
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(one, ch): ch for ch in channels}
            for fut in as_completed(futures):
                try:
                    programs.extend(fut.result())
                except Exception as exc:  # noqa: BLE001
                    logger.warning('[free_networks] Adult Swim schedule failed for %s: %s', futures[fut].id, exc)
        return programs

    def resolve(self, s, ref: str) -> str:
        if not ref.startswith(_SCHEME):
            return ref
        return _parse_ref(ref).get('asset_url') or ref

    @staticmethod
    def _state(s) -> dict:
        def fetch():
            r = s.get(_CATALOG, headers=_HEADERS)
            return _redux_state(r.text) if r else {}
        return s.memo('adultswim.state', fetch)


def _ref(slug: str, video_id: str, marathon_id: str, asset_url: str) -> str:
    return _SCHEME + '?' + urlencode({'slug': slug, 'video_id': video_id, 'marathon_id': marathon_id,
                                      'asset_url': asset_url})


def _parse_ref(ref: str) -> dict[str, str]:
    if not ref.startswith(_SCHEME):
        return {}
    return {k: v[0] for k, v in parse_qs(urlparse(ref).query).items() if v}


def _stable_asset(s, video_id: str) -> str | None:
    r = s.get(_VIDEO.format(video_id=video_id), params={'fields': _VIDEO_FIELDS}, headers=_HEADERS)
    if not r:
        return None
    try:
        payload = r.json()
    except Exception as exc:  # noqa: BLE001
        logger.debug('[free_networks] Adult Swim: bad video JSON for %s: %s', video_id, exc)
        return None
    assets = ((payload.get('data') or {}).get('video') or {}).get('stream', {}).get('assets', []) \
        if isinstance(payload, dict) else []
    for a in assets if isinstance(assets, list) else []:
        if not isinstance(a, dict):
            continue
        url, mime = _clean(a.get('url')), _clean(a.get('mime_type')).lower()
        if (url.startswith('https://') and '/live/' in url
                and (urlparse(url).path.endswith('.m3u8')
                     or mime in {'application/x-mpegurl', 'application/vnd.apple.mpegurl'})):
            return url
    return None


def _stream_is_up(s, url: str) -> bool:
    try:
        return s.session.get(url, timeout=10).status_code == 200
    except Exception:  # noqa: BLE001
        return False


def _next_data(html: str) -> dict:
    from bs4 import BeautifulSoup
    script = BeautifulSoup(html, 'html.parser').find('script', id='__NEXT_DATA__')
    if script is None:
        return {}
    try:
        payload = json.loads(script.string or script.get_text())
    except (TypeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _redux_state(html: str) -> dict:
    state = (_next_data(html).get('props') or {}).get('__REDUX_STATE__') or {}
    return state if isinstance(state, dict) else {}


def _episode_metadata(s, slug: str) -> dict[str, list[dict[str, Any]]]:
    if slug in _NO_SHOW_PAGE:
        return {}
    slug = _SHOW_PAGE_SLUGS.get(slug, slug)
    now = time.monotonic()
    with _episode_cache_lock:
        cached = _episode_cache.get(slug)
        if cached and now - cached[0] < _EPISODE_CACHE_TTL:
            return cached[1]
    r = s.get(f'{_BASE}/videos/{slug}', headers=_HEADERS)
    if not r:
        return {}
    apollo = ((_next_data(r.text).get('props') or {}).get('pageProps') or {}).get('__APOLLO_STATE__') or {}
    by_title: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for key, val in apollo.items() if isinstance(apollo, dict) else []:
        if (isinstance(key, str) and key.startswith('Video:') and isinstance(val, dict)
                and str(val.get('type') or '').upper() == 'EPISODE'):
            t = _norm_title(val.get('title'))
            if t:
                by_title[t].append(val)
    meta = dict(by_title)
    if meta:
        with _episode_cache_lock:
            _episode_cache[slug] = (now, meta)
    return meta


def _match_episode(row: dict, meta: dict[str, list[dict[str, Any]]]) -> dict[str, Any] | None:
    cands = meta.get(_norm_title(row.get('name') or row.get('episodeName')), [])
    if not cands:
        return None
    season = _int(row.get('season') or row.get('seasonNumber'))
    episode = _int(row.get('episodeNumber') or row.get('episode'))
    if season is not None and episode is not None:
        exact = [c for c in cands if _int(c.get('seasonNumber')) == season and _int(c.get('episodeNumber')) == episode]
        if len(exact) == 1:
            return exact[0]
    return cands[0] if len(cands) == 1 else None


def _schedule_rows(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ('data', 'schedule', 'items', 'programs'):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


def _merge_rows(primary: list[dict], fallback: list[dict]) -> list[dict]:
    """The API's rows plus the embedded schedule's rows it lacks (same title
    within two minutes counts as the same)."""
    merged = [r for r in primary if _start(r)]
    slots = [(_norm_title(r.get('name') or r.get('episodeName')), _start(r)) for r in merged]
    for r in fallback:
        start = _start(r)
        title = _norm_title(r.get('name') or r.get('episodeName'))
        dup = any(title and title == t and abs((start - st).total_seconds()) <= 120
                  for t, st in slots if start and st)
        if start and not dup:
            merged.append(r)
    return sorted(merged, key=_start)


def _program(ch: NetChannel, poster: str | None, row: dict, meta: dict | None,
             next_start: datetime | None) -> ProgramData | None:
    meta = meta or {}
    start = _start(row)
    duration = _float(row.get('duration'))
    end = _iso(row.get('endTime') or row.get('end_time'))
    if not start or (not end and (duration is None or duration <= 0)):
        return None
    if end is None:
        end = start + timedelta(seconds=duration)
    if next_start and next_start > end:
        end = next_start
    if end <= start:
        return None
    series = _clean(row.get('seriesName') or row.get('series_name'))
    ep_title = _clean(row.get('name') or row.get('episodeName') or row.get('episode_name') or row.get('title'))
    title = series or ch.name
    if ep_title == title:
        ep_title = ''
    has_row_season = row.get('season') not in (None, '') or row.get('seasonNumber') not in (None, '')
    has_row_episode = row.get('episodeNumber') not in (None, '') or row.get('episode') not in (None, '')
    return ProgramData(
        source_channel_id=ch.id,
        title=title,
        start_time=start,
        end_time=end,
        description=_clean(meta.get('description')) or None,
        poster_url=_clean(meta.get('poster')) or poster,
        category='Comedy',
        rating=_clean(meta.get('tvRating')) or None,
        episode_title=ep_title or None,
        season=(_int(row.get('season') or row.get('seasonNumber')) if has_row_season
                else _int(meta.get('seasonNumber'))),
        episode=(_int(row.get('episodeNumber') or row.get('episode')) if has_row_episode
                 else _int(meta.get('episodeNumber'))),
        original_air_date=_iso(meta.get('firstAiring')),
        program_type='episode',
        series_id=_clean(row.get('seriesId')) or None,
        episode_id=_clean(row.get('titleId')) or None,
    )


def _start(row: dict) -> datetime | None:
    raw = row.get('startTime') or row.get('start_time')
    start = _iso(raw)
    if start:
        return start
    ms = _float(raw) or _float(row.get('time'))
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _iso(val: Any) -> datetime | None:
    if not isinstance(val, str) or not val.strip():
        return None
    try:
        dt = datetime.fromisoformat(val.strip().replace('Z', '+00:00'))
    except ValueError:
        return None
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


def _clean(val: Any) -> str:
    return val.strip() if isinstance(val, str) else ''


def _norm_title(val: Any) -> str:
    title = _clean(val)
    m = re.match(r'^(.*),\s+(The|A|An)$', title, flags=re.IGNORECASE)
    if m:
        title = f'{m.group(2)} {m.group(1)}'
    title = ''.join(c for c in unicodedata.normalize('NFKD', title) if not unicodedata.combining(c))
    title = title.casefold().replace('&', ' and ')
    return re.sub(r'[^a-z0-9]+', ' ', title).strip()


def _int(val: Any) -> int | None:
    if val in (None, ''):
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _float(val: Any) -> float | None:
    if val in (None, ''):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

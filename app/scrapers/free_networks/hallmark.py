"""Hallmark: the free Non-Stop channels on hallmarkplus.com.

Was its own source ('hallmark') until it was folded into Free Networks; the
network key is the old source name, channel ids are the old ones, and the
stream ref is the old opaque hallmark://<id> URL, so moved channel rows keep
their tvg-id, play URL and settings (app/folded_sources.py).

The lineup is fixed. Streams are stable Akamai HLS URLs with no auth. The
guide comes from Hallmark's schedule API, keyed by a content-item id Hallmark
has renamed before (e.g. "Linear-1543-Hallmark" -> "...-Non-Stop-Hallmark-
Hits-DRM", which 404s the schedule); it is re-read from the catalog each run
by the stable channelIdentifier, with the ids below as a fallback.

Program art: a schedule entry's own images hang off its per-airing item
(program-Linear-...-<epoch>), which Hallmark deletes after it airs, so a
recording's art 404s days later. Each airing carries programHouseNumber, the
id of the permanent Hallmark+ catalog item for the movie (e.g. MOVINA035982);
that item's poster is used instead, falling back to the airing's image.
"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

from ._common import Network, NetChannel, parse_iso
from ..base import ProgramData

logger = logging.getLogger('app.scrapers.free_networks')

_BASE = 'https://www.hallmarkplus.com'
_CATALOG = _BASE + '/api/core/catalog/channels'
_SCHEDULE = _BASE + '/api/core/catalog/channels/{linear_id}/schedule'
_ITEM = _BASE + '/api/core/catalog/item/{item_id}?locale=en'
_HEADERS = {'Accept': 'application/json, text/plain, */*', 'Origin': _BASE, 'Referer': _BASE + '/'}
_GUIDE_HOURS = 48
_ALLOWED_MINUTES = (0, 30, 45)   # the schedule API only takes these boundaries
_LOGO = '/static/logos/hallmark/{}'
_SCHEME = 'hallmark://'

# id (the old source_channel_id), name, fallback linear id, channelIdentifier, stream, logo
_CHANNELS = (
    ('hallmark_hits', 'Non-Stop Hallmark Hits', 'Linear-1543-Non-Stop-Hallmark-Hits-DRM', 'HLIVE_HMH',
     'https://crwn-hits.akamaized.net/hls/playlist.m3u8', 'hits_improved.png'),
    ('hallmark_romcoms', 'Non-Stop Rom-Coms', 'Linear-1268-Non-Stop-RomComs-DRM', 'HLIVE_ROMCOM',
     'https://crwn-nsrc.akamaized.net/hls/playlist.m3u8', 'romcoms_improved.png'),
    ('hallmark_christmas', 'Non-Stop Christmas', 'Linear-1187-Non-Stop-Christmas-DRM', 'HLIVE_NSC',
     'https://crwn-christmas.akamaized.net/hls/playlist.m3u8', 'christmas_improved.png'),
    ('hallmark_mysteries', 'Non-Stop Mysteries', 'Linear-1186-Non-Stop-Mysteries-DRM', 'HLIVE_MSM',
     'https://crwn-mysteries.akamaized.net/hls/playlist.m3u8', 'mysteries_improved.png'),
)
_STREAMS = {c[0]: c[4] for c in _CHANNELS}
_FALLBACK_LINEAR = {c[0]: c[2] for c in _CHANNELS}
_BY_IDENTIFIER = {c[3]: c[0] for c in _CHANNELS}

# House number -> permanent catalog poster URL (hits only). A
# movie's art doesn't change, so this lives for the process; ~80 airings per
# 48h guide, mostly repeats, so it stays small.
_ART: dict[str, str | None] = {}
_ART_LOCK = threading.Lock()
_ART_MAX = 5000


class Hallmark(Network):
    key = 'hallmark'
    name = 'Hallmark'

    def channels(self, s) -> list[NetChannel]:
        return [NetChannel(cid, name, f'{_SCHEME}{cid}', logo=_LOGO.format(logo), category='Movies')
                for cid, name, _lid, _ident, _url, logo in _CHANNELS]

    def programs(self, s, channels: list[NetChannel]) -> list[ProgramData]:
        now = datetime.now(timezone.utc)
        start = _align(now)
        end = _align(now + timedelta(hours=_GUIDE_HOURS))
        linear = {**_FALLBACK_LINEAR, **self._linear_ids(s)}
        airings: list[tuple[str, dict]] = []
        for ch in channels:
            lid = linear.get(ch.id)
            if not lid:
                continue
            airings.extend((ch.id, item) for item in self._schedule(s, lid, start, end))
        art = self._catalog_art(s, {_house_number(item) for _cid, item in airings} - {None})
        out: list[ProgramData] = []
        for cid, item in airings:
            p = _program(cid, item, art.get(_house_number(item)))
            if p:
                out.append(p)
        return out

    def resolve(self, s, ref: str) -> str:
        if ref.startswith(_SCHEME):
            return _STREAMS.get(ref[len(_SCHEME):], ref)
        return ref

    @staticmethod
    def _linear_ids(s) -> dict[str, str]:
        r = s.get(_CATALOG, headers=_HEADERS)
        if not r:
            return {}
        try:
            data = r.json().get('data', [])
        except Exception as exc:  # noqa: BLE001
            logger.warning('[free_networks] Hallmark catalog JSON parse error: %s', exc)
            return {}
        found: dict[str, str] = {}
        for item in data if isinstance(data, list) else []:
            cid = _BY_IDENTIFIER.get((item.get('extras') or {}).get('channelIdentifier'))
            if cid and item.get('id'):
                found[cid] = item['id']
        stale = {cid: (_FALLBACK_LINEAR[cid], lid) for cid, lid in found.items() if lid != _FALLBACK_LINEAR[cid]}
        if stale:
            logger.info('[free_networks] Hallmark linear ids changed upstream (update _CHANNELS): %s', stale)
        missing = set(_FALLBACK_LINEAR) - set(found)
        if missing:
            logger.warning('[free_networks] Hallmark catalog is missing: %s', ', '.join(sorted(missing)))
        return found

    @staticmethod
    def _catalog_art(s, house_numbers: set[str]) -> dict[str, str | None]:
        """Permanent poster URL per house number, from the movie's catalog item."""
        with _ART_LOCK:
            todo = [h for h in house_numbers if h not in _ART]

        def fetch(h: str) -> str | None:
            r = s.get(_ITEM.format(item_id=h), headers=_HEADERS)
            if not r:
                return None
            try:
                data = r.json()
            except Exception:  # noqa: BLE001
                return None
            data = data.get('data', data) if isinstance(data, dict) else {}
            return _first_image(data.get('images')) if isinstance(data, dict) else None

        if todo:
            with ThreadPoolExecutor(max_workers=8) as pool:
                found = dict(zip(todo, pool.map(fetch, todo)))
            with _ART_LOCK:
                if len(_ART) + len(found) > _ART_MAX:
                    _ART.clear()
                # A failed lookup isn't cached, so the next run retries it.
                _ART.update({h: u for h, u in found.items() if u})
            missing = [h for h, u in found.items() if not u]
            if missing:
                logger.info('[free_networks] Hallmark: no catalog art for %d of %d titles (using airing art): %s',
                            len(missing), len(todo), ', '.join(sorted(missing)[:10]))
        with _ART_LOCK:
            return {h: _ART.get(h) for h in house_numbers}

    @staticmethod
    def _schedule(s, linear_id: str, start: datetime, end: datetime) -> list[dict]:
        params = urlencode({'from': start.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                            'until': end.strftime('%Y-%m-%dT%H:%M:%S.000Z'), 'locale': 'en'})
        r = s.get(_SCHEDULE.format(linear_id=linear_id) + '?' + params, headers=_HEADERS)
        if not r:
            return []
        try:
            data = r.json().get('data', [])
        except Exception as exc:  # noqa: BLE001
            logger.warning('[free_networks] Hallmark schedule JSON parse error for %s: %s', linear_id, exc)
            return []
        return data if isinstance(data, list) else []


def _align(dt: datetime) -> datetime:
    """dt floored to the last allowed schedule boundary (0/30/45 past the hour)."""
    dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
    minute = max(m for m in _ALLOWED_MINUTES if m <= dt.minute)
    return dt.replace(minute=minute)


def _house_number(item: dict) -> str | None:
    h = (item.get('extras') or {}).get('programHouseNumber')
    return str(h).strip() or None if h else None


def _program(channel_id: str, item: dict, catalog_poster: str | None = None) -> ProgramData | None:
    sched = item.get('schedule') or {}
    epg = (item.get('display') or {}).get('epg') or {}
    start, stop = parse_iso(sched.get('startsAt')), parse_iso(sched.get('endsAt'))
    if not start or not stop:
        return None
    ratings = (item.get('video') or {}).get('parentalRatings') or []
    return ProgramData(
        source_channel_id=channel_id,
        title=str(epg.get('title') or item.get('title') or 'Unknown'),
        start_time=start,
        end_time=stop,
        description=epg.get('description') or epg.get('summary') or item.get('description') or None,
        episode_title=epg.get('subtitle') or epg.get('episodeTitle') or None,
        poster_url=catalog_poster or _first_image(item.get('images')),
        category='Movies',
        rating=ratings[0].get('value') if ratings and isinstance(ratings[0], dict) else None,
    )


def _first_image(images: Any) -> str | None:
    if not isinstance(images, dict):
        return None
    for group, shape in (('poster', 'portrait'), ('poster', 'landscape'), ('tile', 'portrait'),
                         ('tile', 'landscape'), ('background', 'landscape')):
        for c in (images.get(group) or {}).get(shape) or []:
            if isinstance(c, dict) and c.get('url'):
                return str(c['url'])
    return None

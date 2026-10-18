from __future__ import annotations

import json
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from .base import BaseScraper, ChannelData, ProgramData

logger = logging.getLogger(__name__)


class ZeamScraper(BaseScraper):
    """
    Scraper for Zeam (zeam.com) — Syncbak's free local-news + FAST service,
    formerly VUit. No login.

    The full "All Channels" lineup (~400, every market) only exists as the
    `var json={...}` blob inlined in zeam.com/live; /api/services/livechannels
    returns just the ~90 channels near the requester. Each channel carries a
    public schedule.json URL for the guide.

    resolve() calls StreamInfo, which returns a master playlist with a 90-second
    access_token bound to the requester's IP — so it's resolved per tune and
    never cached. Streams are clear AES-128 HLS; out-of-market stations play.

    Local stations here are the stations' streaming news channels, not their
    over-the-air broadcasts, so they're never given the OTA Gracenote ID.
    """

    source_name = 'zeam'
    display_name = 'Zeam'
    scrape_interval = 360
    stream_audit_enabled = True

    _BASE_URL = 'https://zeam.com'
    _LIVE_PAGE = _BASE_URL + '/live'
    _STREAM_INFO = _BASE_URL + '/api/services/StreamInfo'
    _ALL_CHANNELS_GROUP = 'All Channels'
    _CALL_SIGN_RE = re.compile(r'^[KW][A-Z]{2,3}\b')
    _EPG_WORKERS = 6

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                           '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'),
            'Referer': self._BASE_URL + '/live',
        })
        self._channel_objects: list[dict] | None = None

    # ── Required ─────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        channels: list[ChannelData] = []
        for obj in self._load_channel_objects():
            station_id = str(obj.get('stationId') or '')
            name = str(obj.get('name') or '').strip()
            if not station_id or not name:
                continue
            channels.append(ChannelData(
                source_channel_id=station_id,
                name=name,
                slug='zeam-' + re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-'),
                stream_url=f'zeam://{station_id}',
                logo_url=self._logo(obj.get('images')),
                category=self._category(name),
                description=(obj.get('description') or '').strip() or None,
                guide_key=f'ZEAM:{station_id}',
                country='US',
                language='en',
                stream_type='hls',
            ))
        logger.info('[%s] %d channels', self.source_name, len(channels))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        schedule_urls = {}
        for obj in self._load_channel_objects():
            urls = obj.get('scheduleUrls') or {}
            url = urls.get('v2') or obj.get('scheduleUrl')
            if obj.get('stationId') and url:
                schedule_urls[str(obj['stationId'])] = url

        wanted = [(ch.source_channel_id, schedule_urls[ch.source_channel_id])
                  for ch in channels if ch.source_channel_id in schedule_urls]
        programs: list[ProgramData] = []
        lock = threading.Lock()
        done = 0

        def _one(channel_id: str, url: str) -> list[ProgramData]:
            r = self.get(url)
            if not r:
                return []
            try:
                rows = r.json().get('schedule') or []
            except (ValueError, AttributeError) as exc:
                logger.warning('[%s] schedule parse error for %s: %s', self.source_name, channel_id, exc)
                return []
            return [p for p in (self._parse_program(channel_id, row) for row in rows) if p]

        with ThreadPoolExecutor(max_workers=self._EPG_WORKERS) as pool:
            futures = [pool.submit(_one, cid, url) for cid, url in wanted]
            for future in as_completed(futures):
                result = future.result()
                with lock:
                    programs.extend(result)
                    done += 1
                    if self._progress_cb:
                        self._progress_cb('epg', done, len(wanted))

        logger.info('[%s] %d EPG entries for %d channels', self.source_name, len(programs), len(wanted))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith('zeam://'):
            return raw_url
        station_id = raw_url[len('zeam://'):]
        r = self.session.get(self._STREAM_INFO, params={'stationId': station_id}, timeout=20)
        r.raise_for_status()
        stream_url = (r.json() or {}).get('streamUrl') or ''
        if not stream_url:
            raise RuntimeError(f'Zeam StreamInfo returned no streamUrl for station {station_id}')
        return stream_url.replace('http:', 'https:', 1)

    # ── Internals ─────────────────────────────────────────────

    def _load_channel_objects(self) -> list[dict]:
        """The All Channels group from zeam.com/live's inline JSON, fetched once
        per scraper instance (fetch_channels and fetch_epg share it)."""
        if self._channel_objects is not None:
            return self._channel_objects
        r = self.get(self._LIVE_PAGE)
        if not r:
            raise RuntimeError('Zeam live page unavailable')
        page = r.text
        marker = page.find('var json=')
        if marker < 0:
            raise RuntimeError('Zeam live page has no inline channel JSON')
        data, _ = json.JSONDecoder().raw_decode(page[marker + len('var json='):])
        groups = data.get('groups') or []
        group = next((g for g in groups if g.get('displayName') == self._ALL_CHANNELS_GROUP), None)
        if group is None:
            # Fall back to every group combined if the name ever changes.
            logger.warning('[%s] no "%s" group; merging %d groups', self.source_name,
                           self._ALL_CHANNELS_GROUP, len(groups))
        objects: dict[str, dict] = {}
        for g in [group] if group else groups:
            for obj in g.get('channelobjects') or []:
                if isinstance(obj, dict) and obj.get('stationId'):
                    objects.setdefault(str(obj['stationId']), obj)
        self._channel_objects = list(objects.values())
        return self._channel_objects

    @staticmethod
    def _logo(images) -> str | None:
        """Square logo (typeId 1), largest size; image URLs come without a scheme."""
        best = None
        for img in images or []:
            if isinstance(img, dict) and img.get('typeId') == 1 and img.get('url'):
                if best is None or (img.get('size') or 0) > (best.get('size') or 0):
                    best = img
        if not best:
            return None
        url = best['url']
        return url if url.startswith('http') else 'https://' + url.lstrip('/')

    @classmethod
    def _category(cls, name: str) -> str | None:
        if cls._CALL_SIGN_RE.match(name) or 'news' in name.lower():
            return 'News'
        return None

    def _parse_program(self, channel_id: str, row: dict) -> ProgramData | None:
        try:
            start_ts = int(row['startTime'])
            end_ts = int(row.get('endTime') or start_ts + int(row.get('duration') or 0))
        except (KeyError, TypeError, ValueError):
            return None
        if end_ts <= start_ts:
            return None
        title = str(row.get('name') or '').strip()
        if not title:
            return None
        start = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        return ProgramData(
            source_channel_id=channel_id,
            title=title,
            start_time=start,
            end_time=start + timedelta(seconds=end_ts - start_ts),
            description=(row.get('description') or '').strip() or None,
            episode_title=(row.get('episodeTitle') or '').strip() or None,
        )

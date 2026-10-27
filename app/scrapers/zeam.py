from __future__ import annotations

import json
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from ..gracenote_map import resolve_gracenote
from .base import BaseScraper, ChannelData, ProgramData

logger = logging.getLogger(__name__)


class ZeamScraper(BaseScraper):
    """
    Scraper for Zeam (zeam.com) — Syncbak's free local-news + FAST service,
    formerly VUit. No login.

    The full "All Channels" lineup (~400, every market) only exists as the
    `var json={...}` blob inlined in zeam.com/live; /api/services/livechannels
    returns just the ~90 channels near the requester. Each channel carries
    public schedule.json URLs for the guide; v4 is the one zeam.com's own guide
    reads. v2 lists only live newscasts (holes between them on looped
    channels) and v3 is the stations' OTA schedule with most rows blacked out.

    resolve() calls StreamInfo, which returns a master playlist with a 90-second
    access_token bound to the requester's IP — so it's resolved per tune and
    never cached. Streams are clear AES-128 HLS; out-of-market stations play.
    The session's variant/key URLs only answer the user agent that fetched the
    master most recently, so nothing else may fetch it after the client does.

    Local stations here are the stations' streaming news channels, not their
    over-the-air broadcasts, so they're never given the OTA Gracenote ID.
    """

    source_name = 'zeam'
    display_name = 'Zeam'
    scrape_interval = 360
    stream_audit_enabled = True
    # Zeam binds the stream session to the user agent of the latest master
    # fetch; the play proxy's follow-up fetch would steal it from the player.
    play_manifest_check = False

    _BASE_URL = 'https://zeam.com'
    _LIVE_PAGE = _BASE_URL + '/live'
    _STREAM_INFO = _BASE_URL + '/api/services/StreamInfo'
    _ALL_CHANNELS_GROUP = 'All Channels'
    # Station call sign at the start of the name: "KGW 8", "KQ2 St. Joseph",
    # "KTHV11", "WSYX ABC 6". The shared category rule only catches 4-letter
    # call signs, so locals are tagged here.
    _CALL_SIGN_RE = re.compile(r'^[KW][A-Z]{2,3}(?=\d|\b)|^KQ2\b')

    # Zeam's own studio channels and the national nets the name alone can't place.
    _CATEGORY_BY_NAME = {
        'local news live': 'Local News',
        'ky3 news': 'Local News',
        'et': 'Entertainment',
        'jtv': 'Shopping',
        'big apple live': 'Entertainment',
        "zeam's big apple live (previously recorded)": 'Entertainment',
        'live at the west end': 'Entertainment',
        'recent podcasts from zeam studios': 'Entertainment',
        'going full jack with john stamos': 'Entertainment',
        'jax perry. indie creator.': 'Entertainment',
        'mike young presents': 'Comedy',
        'fast lane tv': 'Automotive',
        'food. curated.': 'Food',
        'the martin yan channel': 'Food',
        'fun roads tv': 'Travel',
        'northern michigan from above': 'Travel',
        'utah-tv': 'Travel',
        'zeamextreme route 66 roadtrip': 'Travel',
        'zeamextreme van': 'Travel',
        'zeamextreme replay': 'Travel',
        'zeam new york studios street cam': 'Ambiance',
        'quili.tv': 'Ambiance',
        'police in pusuit': 'Reality TV',
        'premier guitar channel': 'Music',
        'the fly-over music hour': 'Music',
        'tcn+': 'True Crime',
        'zeaming behind the race': 'Sports',
        'locked on nba': 'Sports',
        'sky link tv': 'International',
        'sky link tv drama channel': 'Drama',
        'itv+': 'News',  # InvestigateTV+ — Gray's investigative news magazine
    }
    _LANGUAGE_BY_NAME = {
        'kese telemundo': 'es',
        'kuna 15 telemundo': 'es',
        'wfmz edicion en espanol': 'es',
        'sky link tv': 'zh',
        'sky link tv drama channel': 'zh',
    }
    _EPG_WORKERS = 6
    # Looped stations replay their last newscast until the next one, which v4
    # gives as one block of up to several days under the newscast's title.
    # Blocks this long are cut into hourly pieces so a DVR rule for the show
    # never records the whole stretch.
    _SPLIT_BLOCKS_OVER = 4 * 3600
    _PIECE = 3600

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
                language=self._LANGUAGE_BY_NAME.get(name.lower(), 'en'),
                stream_type='hls',
                gracenote_id=resolve_gracenote('zeam', lookup_key=station_id),
            ))
        logger.info('[%s] %d channels', self.source_name, len(channels))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        schedule_urls = {}
        looped = set()
        for obj in self._load_channel_objects():
            if obj.get('offAirProfile') == 'looped':
                looped.add(str(obj.get('stationId')))
            urls = obj.get('scheduleUrls') or {}
            url = urls.get('v4') or obj.get('scheduleUrl')
            if obj.get('stationId') and url:
                schedule_urls[str(obj['stationId'])] = url

        wanted = [(ch.source_channel_id, schedule_urls[ch.source_channel_id])
                  for ch in channels if ch.source_channel_id in schedule_urls]
        programs: list[ProgramData] = []
        lock = threading.Lock()
        done = 0

        def _one(channel_id: str, url: str) -> list[ProgramData]:
            # A few stations list a schedule file CloudFront no longer serves
            # (403/404); that just means no guide for them, not a failed scrape.
            try:
                r = self.session.get(url, timeout=30)
            except Exception as exc:
                logger.warning('[%s] schedule fetch failed for %s: %s', self.source_name, channel_id, exc)
                return []
            if r.status_code in (403, 404):
                logger.debug('[%s] no schedule for %s (HTTP %s)', self.source_name, channel_id, r.status_code)
                return []
            if not r.ok:
                logger.warning('[%s] schedule fetch for %s returned HTTP %s', self.source_name, channel_id, r.status_code)
                return []
            try:
                rows = r.json().get('schedule') or []
            except (ValueError, AttributeError) as exc:
                logger.warning('[%s] schedule parse error for %s: %s', self.source_name, channel_id, exc)
                return []
            replay = channel_id in looped
            parsed = [p for row in rows for p in self._parse_program(channel_id, row, replay)]
            # Some stations' schedule files stopped updating months ago; skip rows
            # that are already over rather than handing them to the worker.
            now = datetime.now(timezone.utc)
            return self._drop_overlaps([p for p in parsed if p.end_time > now])

        with ThreadPoolExecutor(max_workers=self._EPG_WORKERS) as pool:
            futures = [pool.submit(_one, cid, url) for cid, url in wanted]
            for future in as_completed(futures):
                result = future.result()
                with lock:
                    programs.extend(result)
                    done += 1
                    if self._progress_cb:
                        self._progress_cb('epg', done, len(wanted))

        # A few channels have no current guide at all (a schedule file that
        # stopped updating, or one CloudFront 403s); zeam.com shows nothing for
        # them either. Give them hourly blocks with the channel's name.
        covered = {p.source_channel_id for p in programs}
        bare = [ch for ch in channels if ch.source_channel_id not in covered]
        start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        for ch in bare:
            programs.extend(ProgramData(source_channel_id=ch.source_channel_id, title=ch.name,
                                        start_time=start + timedelta(hours=i),
                                        end_time=start + timedelta(hours=i + 1), category=ch.category)
                            for i in range(48))

        logger.info('[%s] %d EPG entries for %d channels (%d on placeholder blocks)',
                    self.source_name, len(programs), len(wanted), len(bare))
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
        known = cls._CATEGORY_BY_NAME.get(name.lower())
        if known:
            return known
        if cls._CALL_SIGN_RE.match(name):
            return 'Local News'
        if 'news' in name.lower():
            return 'News'
        return None

    @staticmethod
    def _drop_overlaps(programs: list[ProgramData]) -> list[ProgramData]:
        """Some v4 feeds nest their rows (KOTV: each day's block runs to the
        same end time), so later rows start inside earlier ones. Keep the guide
        a single timeline: a row starting before the previous one ends is
        trimmed to start there, or dropped if nothing is left."""
        out: list[ProgramData] = []
        for p in sorted(programs, key=lambda p: (p.start_time, p.end_time)):
            if out and p.start_time < out[-1].end_time:
                if p.end_time <= out[-1].end_time:
                    continue
                p.start_time = out[-1].end_time
            out.append(p)
        return out

    def _parse_program(self, channel_id: str, row: dict, looped: bool = False) -> list[ProgramData]:
        try:
            start_ts = int(row['startTime'])
            end_ts = int(row.get('endTime') or start_ts + int(row.get('duration') or 0))
        except (KeyError, TypeError, ValueError):
            return []
        if end_ts <= start_ts:
            return []
        title = str(row.get('name') or '').strip()
        if not title:
            return []
        description = (row.get('description') or '').strip() or None
        episode_title = (row.get('episodeTitle') or '').strip() or None

        pieces = [(start_ts, end_ts)]
        if end_ts - start_ts > self._SPLIT_BLOCKS_OVER:
            # Cut on the hour so the pieces line up with the rest of the guide.
            pieces, t = [], start_ts
            while t < end_ts:
                nxt = min(end_ts, (t // self._PIECE + 1) * self._PIECE)
                pieces.append((t, nxt))
                t = nxt
            if looped and 'replay' not in title.lower():
                title = f'{title} (Replay)'

        return [
            ProgramData(
                source_channel_id=channel_id,
                title=title,
                start_time=datetime.fromtimestamp(a, tz=timezone.utc),
                end_time=datetime.fromtimestamp(b, tz=timezone.utc),
                description=description,
                episode_title=episode_title,
            )
            for a, b in pieces
        ]

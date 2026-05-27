from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import requests

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, StreamDeadError, infer_language_from_metadata
from .category_utils import infer_category_from_name

logger = logging.getLogger(__name__)

_BOX_ID = 'SHIELD30X8X4X0'
_TENANT_CODE = 'frndlytv'
_DEVICE_ID = 43
_LOGO_URL = 'https://d229kpbsb5jevy.cloudfront.net/frndlytv/400/400/content/{bucket}/logos/{path}'
_EPG_IMAGE_URL = 'https://d229kpbsb5jevy.cloudfront.net/frndlytv/content/common/epgs/{path}'
_TOKEN_URL = 'https://frndlytv-api.revlet.net/service/api/v1/get/token'
_SIGNIN_URL = 'https://frndlytv-api.revlet.net/service/api/auth/signin'
_CHANNELS_URL = 'https://frndlytv-api.revlet.net/service/api/v1/tvguide/channels?skip_tabs=0'
_STREAM_URL = 'https://frndlytv-api.revlet.net/service/api/v1/page/stream'
_GUIDE_URL = 'https://frndlytv-tvguideapi.revlet.net/service/api/v1/static/tvguide'
_TEMPLATE_URL = 'https://frndlytv-api.revlet.net/service/api/v1/template/data'
_SESSION_END_URL = 'https://frndlytv-api.revlet.net/service/api/v1/stream/session/end'

_LOGIN_TTL = 60 * 60 * 5  # force re-login after 5 hours
_EPG_DAYS = 3
_GUIDE_CHUNK = 20       # channel IDs per guide request
_ENRICH_WORKERS = 2    # conservative concurrency for template/data enrichment
_ENRICH_DELAY = 0.4    # seconds between requests per worker to avoid rate-limiting
_CACHE_MAX = 8000      # max content_cache entries to keep in source config

# "S2 Ep14 | Manager Meltdown"  or  "S1 Ep3"  (no episode title)
_SE_RE = re.compile(r'S(\d+)\s+Ep(\d+)(?:\s*\|\s*(.+))?', re.IGNORECASE)


class FrndlyTVScraper(BaseScraper):
    """
    Scraper for Frndly TV (frndlytv.com) — subscription live TV service.

    Requires a paid Frndly TV account. Streams are HLS; some channels may
    be Widevine-protected (those will fail playback on non-DRM clients).

    Auth tokens are cached in source config and refreshed automatically.
    The service is US-only (geo-restricted).
    """

    source_name = 'frndlytv'
    display_name = 'Frndly TV'
    scrape_interval = 360
    stream_audit_enabled = True
    config_required = True
    is_premium = True

    config_schema = [
        ConfigField('username', 'Username / Email', required=True,
                    placeholder='you@example.com',
                    help_text='Your Frndly TV login email'),
        ConfigField('password', 'Password', field_type='password', required=True,
                    secret=True, help_text='Your Frndly TV password'),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._frndly_headers = {
            'user-agent': 'okhttp/3.12.5',
            'box-id': _BOX_ID,
            'tenant-code': _TENANT_CODE,
        }
        session_id = self.config.get('session_id')
        if session_id:
            self._frndly_headers['session-id'] = session_id

    # ── Auth ────────────────────────────────────────────────────────────────

    def _login(self) -> None:
        username = self.config.get('username', '').strip()
        password = self.config.get('password', '').strip()
        if not username or not password:
            raise RuntimeError('Frndly TV username and password are required')

        params = {
            'box_id': _BOX_ID,
            'device_id': _DEVICE_ID,
            'tenant_code': _TENANT_CODE,
            'device_sub_type': 'nvidia,8.1.0,7.4.4',
            'product': _TENANT_CODE,
            'display_lang_code': 'eng',
            'timezone': 'America/New_York',
        }
        headers = {k: v for k, v in self._frndly_headers.items() if k != 'session-id'}
        r = self.session.get(_TOKEN_URL, params=params, headers=headers, timeout=15)
        r.raise_for_status()
        session_id = r.json()['response']['sessionId']
        headers['session-id'] = session_id

        payload = {
            'login_id': username,
            'login_key': password,
            'login_mode': 1,
            'os_version': '8.1.0',
            'app_version': '7.4.4',
            'manufacturer': 'nvidia',
        }
        r = self.session.post(_SIGNIN_URL, json=payload, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        if not data.get('status'):
            msg = (data.get('error') or {}).get('message') or 'unknown error'
            raise RuntimeError(f'Frndly TV login failed: {msg}')

        self._frndly_headers = headers
        self._update_config('session_id', session_id)
        self._update_config('login_time', time.time())
        logger.info('[frndlytv] logged in successfully')

    def _ensure_session(self) -> None:
        login_time = self.config.get('login_time', 0)
        session_id = self.config.get('session_id')
        if session_id and (time.time() - login_time) < _LOGIN_TTL:
            self._frndly_headers['session-id'] = session_id
            return
        self._login()

    def pre_run_setup(self) -> None:
        self._ensure_session()

    # ── API helpers ──────────────────────────────────────────────────────────

    def _api_get(self, url: str, params: dict | None = None) -> Any:
        """GET with Frndly auth headers; re-logins once on auth failure."""
        for attempt in range(2):
            r = self.session.get(url, params=params, headers=self._frndly_headers, timeout=15)
            data = r.json()
            if 'response' in data:
                return data['response']
            error_code = (data.get('error') or {}).get('code')
            if error_code == 404:
                raise requests.HTTPError(response=r)
            if attempt == 0:
                logger.debug('[frndlytv] API error %s on %s, re-logging in', error_code, url)
                self._login()
        raise RuntimeError(f'Frndly TV API failed for {url}')

    @staticmethod
    def _logo(image_url: str) -> str | None:
        if not image_url or ',' not in image_url:
            return None
        bucket, path = image_url.split(',', 1)
        return _LOGO_URL.format(bucket=bucket, path=path)

    @staticmethod
    def _epg_image(image_field: str) -> str | None:
        """Convert 'epg,{path}' template image field to a CDN URL."""
        if not image_field or ',' not in image_field:
            return None
        _, path = image_field.split(',', 1)
        path = path.strip()
        return _EPG_IMAGE_URL.format(path=path) if path else None

    # ── fetch_channels ────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        rows = self._api_get(_CHANNELS_URL)['data']
        rows = [r for r in rows if r.get('metadata', {}).get('isChannelBanner', '').lower() != 'true']

        try:
            from app.gracenote_map import resolve_gracenote
        except ImportError:
            resolve_gracenote = None

        channels: list[ChannelData] = []
        for row in rows:
            ch_id = str(row['id'])
            name = (row.get('display') or {}).get('title') or ''
            if not name:
                continue

            image_url = (row.get('display') or {}).get('imageUrl') or ''
            logo = self._logo(image_url)
            gracenote_id = resolve_gracenote('frndlytv', lookup_key=ch_id) if resolve_gracenote else None
            category = infer_category_from_name(name) or 'Entertainment'

            channels.append(ChannelData(
                source_channel_id=ch_id,
                name=name,
                stream_url=f'frndly:///{ch_id}',
                logo_url=logo,
                category=category,
                language=infer_language_from_metadata(name),
                country='US',
                stream_type='hls',
                gracenote_id=gracenote_id,
            ))

        channels.sort(key=lambda c: c.name.lower())
        logger.info('[frndlytv] %d channels', len(channels))
        return channels

    # ── fetch_epg ─────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        now = int(time.time())
        ids = [ch.source_channel_id for ch in channels]

        # Channels with no Gracenote coverage get enriched via template/data.
        # The call count is small enough to be safe; Gracenote-covered channels
        # don't need it.
        enrich_ids = {ch.source_channel_id for ch in channels if not ch.gracenote_id}

        content_map: dict[str, tuple[int, list[ProgramData]]] = {}
        programs: list[ProgramData] = []

        for i in range(0, len(ids), _GUIDE_CHUNK):
            chunk = ids[i:i + _GUIDE_CHUNK]
            start = now
            for _ in range(_EPG_DAYS):
                end = start + 86400
                params = {
                    'channel_ids': ','.join(chunk),
                    'page': 0,
                    'start_time': start * 1000,
                    'end_time': end * 1000,
                }
                try:
                    data = self._api_get(_GUIDE_URL, params=params)
                    for row in data.get('data', []):
                        ch_id = str(row['channelId'])
                        for prog in row.get('programs', []):
                            p, sched_id, content_id = self._parse_program(ch_id, prog)
                            if p is None:
                                continue
                            programs.append(p)
                            if ch_id in enrich_ids and content_id:
                                if content_id not in content_map:
                                    content_map[content_id] = (sched_id, [])
                                content_map[content_id][1].append(p)
                except Exception as exc:
                    logger.warning('[frndlytv] guide fetch failed for chunk %s: %s', chunk, exc)
                start = end

            if self._progress_cb:
                self._progress_cb('epg', min(i + _GUIDE_CHUNK, len(ids)), len(ids))

        logger.info('[frndlytv] %d EPG entries, %d unique content IDs to enrich (no-gracenote channels)',
                    len(programs), len(content_map))

        if content_map:
            self._enrich_programs(content_map)

        return programs

    def _parse_program(self, ch_id: str, prog: dict) -> tuple[ProgramData | None, int, str]:
        """Return (ProgramData, scheduling_id, searchFeedContentId)."""
        markers = (prog.get('display') or {}).get('markers') or {}
        start_ms = (markers.get('startTime') or {}).get('value')
        end_ms = (markers.get('endTime') or {}).get('value')
        title = (prog.get('display') or {}).get('title') or ''
        if not start_ms or not end_ms or not title:
            return None, 0, ''

        start = datetime.fromtimestamp(int(start_ms) / 1000, tz=timezone.utc)
        end = datetime.fromtimestamp(int(end_ms) / 1000, tz=timezone.utc)
        sched_id = prog.get('id') or 0
        content_id = prog.get('searchFeedContentId') or ''

        return ProgramData(
            source_channel_id=ch_id,
            title=title,
            start_time=start,
            end_time=end,
            episode_id=content_id or None,
        ), sched_id, content_id

    # ── EPG enrichment via template/data ──────────────────────────────────────

    @staticmethod
    def _meta_from_response(data: dict) -> dict:
        """Extract the fields we care about from a template/data response body."""
        desc = (data.get('description') or '').strip() or None
        rating = (data.get('subtitle4') or '').strip() or None
        poster = FrndlyTVScraper._epg_image(data.get('image') or '')
        cast_raw = (data.get('cast') or '').strip() or None
        season = episode = None
        ep_title = None
        sub3 = (data.get('subtitle3') or '').strip()
        if sub3:
            m = _SE_RE.match(sub3)
            if m:
                season = int(m.group(1))
                episode = int(m.group(2))
                ep_title = (m.group(3) or '').strip() or None
        return {
            'desc': desc,
            'rating': rating,
            'poster': poster,
            'cast': cast_raw,
            'season': season,
            'episode': episode,
            'ep_title': ep_title,
        }

    @staticmethod
    def _apply_meta(prog_list: list[ProgramData], meta: dict) -> None:
        desc = meta.get('desc')
        cast_raw = meta.get('cast')
        season = meta.get('season')
        episode = meta.get('episode')
        prog_type = 'episode' if (season or episode) else None
        for p in prog_list:
            p.description = desc or (f'Cast: {cast_raw}' if cast_raw else None)
            p.rating = meta.get('rating')
            p.poster_url = meta.get('poster')
            p.season = season
            p.episode = episode
            p.episode_title = meta.get('ep_title')
            p.program_type = prog_type

    def _enrich_programs(self, content_map: dict[str, tuple[int, list[ProgramData]]]) -> None:
        """Fetch template/data for unseen content IDs; serve cached data for known ones."""
        if not content_map:
            return

        # Load persistent cache (content_id → metadata dict).
        cache: dict[str, dict] = self.config.get('content_cache') or {}
        cache_hits = 0

        # Apply cache hits immediately; collect misses for network fetch.
        misses: dict[str, tuple[int, list[ProgramData]]] = {}
        for cid, (sched_id, prog_list) in content_map.items():
            if cid in cache:
                self._apply_meta(prog_list, cache[cid])
                cache_hits += 1
            else:
                misses[cid] = (sched_id, prog_list)

        logger.info('[frndlytv] content cache: %d hits, %d misses to fetch', cache_hits, len(misses))

        if not misses:
            return

        headers_snapshot = dict(self._frndly_headers)
        rate_lock = threading.Lock()
        last_request_time = [0.0]

        def fetch_one(content_id: str, sched_id: int) -> dict | None:
            # Rate-limit: enforce minimum gap between all outgoing requests.
            with rate_lock:
                now = time.time()
                gap = _ENRICH_DELAY - (now - last_request_time[0])
                if gap > 0:
                    time.sleep(gap)
                last_request_time[0] = time.time()

            try:
                r = self.session.get(
                    _TEMPLATE_URL,
                    params={'template_code': 'tvguide_overlay', 'path': f'epg/play/{sched_id}'},
                    headers=headers_snapshot,
                    timeout=10,
                )
                if not r.ok:
                    logger.debug('[frndlytv] template/%s HTTP %s', sched_id, r.status_code)
                    return None
                data = r.json()
                if data.get('status') and 'response' in data:
                    return (data['response'].get('data') or {})
            except Exception as exc:
                logger.debug('[frndlytv] template fetch failed for %s: %s', sched_id, exc)
            return None

        fetched = ok = 0
        with ThreadPoolExecutor(max_workers=_ENRICH_WORKERS) as pool:
            futures = {
                pool.submit(fetch_one, cid, sched_id): (cid, prog_list)
                for cid, (sched_id, prog_list) in misses.items()
            }
            for future in as_completed(futures):
                cid, prog_list = futures[future]
                raw = future.result()
                fetched += 1
                if raw is None:
                    continue
                ok += 1
                meta = self._meta_from_response(raw)
                self._apply_meta(prog_list, meta)
                cache[cid] = meta

        logger.info('[frndlytv] enriched %d/%d new content IDs', ok, fetched)

        # Trim cache to prevent unbounded growth; keep newest entries.
        if len(cache) > _CACHE_MAX:
            keys = list(cache.keys())
            for k in keys[:len(cache) - _CACHE_MAX]:
                del cache[k]

        self._update_config('content_cache', cache)

    # ── resolve ────────────────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith('frndly://'):
            return raw_url

        ch_id = raw_url.split('frndly:///', 1)[-1].strip().strip('/')
        if not ch_id:
            raise RuntimeError(f'Invalid frndly URL: {raw_url}')

        self._ensure_session()
        path = self._channel_path_from_guide(ch_id)
        return self._get_play_url(path)

    def _get_play_url(self, path: str) -> str:
        params = {
            'path': path,
            'code': path,
            'include_ads': 'false',
            'is_casted': 'true',
        }
        data = self._api_get(_STREAM_URL, params=params)

        streams = data.get('streams') or []
        if not streams:
            raise RuntimeError(f'No streams returned for path: {path}')

        clear_streams = [s for s in streams if s.get('streamType', '').lower().strip() != 'widevine']
        if not clear_streams:
            raise StreamDeadError(f'Frndly TV channel {path} is Widevine DRM-only')

        stream = sorted(clear_streams, key=lambda s: s.get('keys', {}).get('licenseKey', ''))[0]
        url = stream['url']

        try:
            offset_ms = data['playerSettings'][0]['value']
            url += '&start={0}&startTime={0}'.format(int(int(offset_ms) / 1000))
        except (KeyError, IndexError, TypeError, ValueError):
            pass

        try:
            poll_key = data.get('sessionInfo', {}).get('streamPollKey')
            if poll_key:
                self.session.post(_SESSION_END_URL, data={'poll_key': poll_key},
                                  headers=self._frndly_headers, timeout=5)
        except Exception:
            pass

        return url

    def _channel_path_from_guide(self, ch_id: str) -> str:
        now = int(time.time())
        params = {
            'channel_ids': ch_id,
            'page': 0,
            'start_time': now * 1000,
            'end_time': (now + 7200) * 1000,
        }
        data = self._api_get(_GUIDE_URL, params=params)
        for row in data.get('data', []):
            for prog in row.get('programs', []):
                start_ms = int((prog.get('display', {}).get('markers', {}).get('startTime', {}).get('value') or 0))
                end_ms = int((prog.get('display', {}).get('markers', {}).get('endTime', {}).get('value') or 0))
                if start_ms <= now * 1000 <= end_ms:
                    path = (prog.get('target') or {}).get('path')
                    if path:
                        return path
        raise RuntimeError(f'No live program found in guide for channel {ch_id}')

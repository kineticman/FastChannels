# app/scrapers/tubi.py
"""
Tubi TV scraper for FastChannels.

Two auth modes:
  - Anonymous (default): GETs tubitv.com/live, extracts the embedded
    window.__data JSON blob to get channel IDs + groups, then calls the
    public /oz/epg/programming endpoint for stream URLs and EPG.
  - Authenticated: POSTs email/password for a Bearer token, then uses the
    authenticated tensor-cdn EPG API (richer metadata, more channels).

No extra dependencies beyond what's already in requirements.txt.
"""
from __future__ import annotations

import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import unquote

import requests

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata
from ..gracenote_map import resolve_gracenote

logger = logging.getLogger(__name__)


def _language_from_metadata(name: str | None, category: str | None) -> str:
    return infer_language_from_metadata(name, category)

# ── API endpoints ────────────────────────────────────────────────────────────
_LIVE_PAGE_URL   = 'https://tubitv.com/live'
_EPG_ANON_URL    = 'https://tubitv.com/oz/epg/programming'
_EPG_AUTH_URL    = 'https://epg-cdn.production-public.tubi.io/content/epg/programming'
_CHANNELS_URL    = 'https://tensor-cdn.production-public.tubi.io/api/v2/epg'
_LOGIN_URL       = 'https://account.production-public.tubi.io/user/login'

# Container slugs to exclude (personalisation / recommendation buckets)
_SKIP_SLUGS = frozenset({
    'favorite_linear_channels',
    'recommended_linear_channels',
    'featured_channels',
    'recently_added_channels',
})

_BATCH = 150   # max channel IDs per EPG request

_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/131.0.0.0 Safari/537.36'
)

_BASE_HEADERS = {
    'accept':             '*/*',
    'accept-language':    'en-US,en;q=0.9',
    'origin':             'https://tubitv.com',
    'referer':            'https://tubitv.com/',
    'user-agent':         _UA,
    'sec-fetch-dest':     'empty',
    'sec-fetch-mode':     'cors',
    'sec-fetch-site':     'same-origin',
}


# ── Scraper ──────────────────────────────────────────────────────────────────

class TubiScraper(BaseScraper):
    source_name     = 'tubi'
    display_name    = 'Tubi TV'
    stream_audit_enabled  = True
    scrape_before_audit   = True   # fetch_channels() warms _url_cache + session cookies before audit
    scrape_interval = 360

    config_schema = [
        ConfigField(
            key='username', label='Tubi Username',
            field_type='text', secret=False,
            placeholder='email@example.com',
            help_text='Optional — anonymous access works for most channels.',
        ),
        ConfigField(
            key='password', label='Tubi Password',
            field_type='password', secret=True,
            help_text='Optional. Only needed if username is set.',
        ),
    ]

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update(_BASE_HEADERS)

        self._username: Optional[str] = self.config.get('username') or None
        self._password: Optional[str] = self.config.get('password') or None
        self._device_id               = str(uuid.uuid4())

        # Bearer token cache
        self._token: Optional[str]     = None
        self._token_at: float          = 0.0
        self._token_ttl: float         = 0.0

        # stream URL cache: channel_id → real HLS URL
        # populated by fetch_channels(), consumed by resolve()
        self._url_cache: dict[str, str] = {}

    # ── Required ─────────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        if self._username and self._password:
            channels = self._channels_auth()
            if channels:
                return channels
            logger.warning('[tubi] auth channel fetch failed, falling back to anonymous')
        return self._channels_anon()

    # ── Optional ─────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        ids = [ch.source_channel_id for ch in channels]
        if not ids:
            return []
        if self._username and self._password:
            programs = self._epg_auth(ids)
            if programs:
                return programs
        return self._epg_anon(ids)

    def resolve(self, raw_url: str) -> str:
        """
        stream_url is stored as  tubi://<channel_id>
        At play time we look up the real CDN HLS URL from the cache
        (populated during fetch_channels). On a cache miss we do a
        lightweight re-fetch of just that channel's EPG row.
        """
        if not raw_url.startswith('tubi://'):
            return raw_url

        cid = raw_url[len('tubi://'):]

        url = self._url_cache.get(cid)
        if url:
            return url

        # Cache miss — re-fetch a single channel's stream URL via the anon EPG API
        logger.debug('[tubi] resolve: cache miss for %s, fetching fresh URL', cid)
        try:
            r = self.session.get(
                _EPG_ANON_URL,
                params={'content_id': cid},
                timeout=10,
            )
            r.raise_for_status()
            rows = r.json().get('rows', [])
            if rows:
                resources = rows[0].get('video_resources') or []
                if resources:
                    manifest_url = resources[0].get('manifest', {}).get('url', '')
                    if manifest_url:
                        url = f'{unquote(manifest_url)}&content_id={cid}'
                        self._url_cache[cid] = url
                        return url
        except Exception as e:
            logger.warning('[tubi] resolve single-channel fetch failed for %s: %s', cid, e)

        return raw_url  # last resort

    # ── Anonymous channel fetch ───────────────────────────────────────────────

    def _channels_anon(self) -> list[ChannelData]:
        """
        1. GET tubitv.com/live
        2. Extract the window.__data JSON blob (contains channel IDs + group labels)
        3. GET /oz/epg/programming in batches for stream URLs + metadata
        """
        channel_ids, groups, err = self._parse_live_page()
        if err:
            logger.error('[tubi] live page parse failed: %s', err)
            return []

        epg_rows = self._fetch_epg_rows_anon(channel_ids)

        channels: list[ChannelData] = []
        for row in epg_rows:
            cid  = str(row.get('content_id', ''))
            name = (row.get('title') or '').strip()
            if not cid or not name:
                continue

            logo = None
            thumb = (row.get('images') or {}).get('thumbnail')
            if isinstance(thumb, list) and thumb:
                logo = thumb[0]
            elif isinstance(thumb, str):
                logo = thumb

            # Stream URL
            stream_url = ''
            resources  = row.get('video_resources') or []
            if resources and isinstance(resources[0], dict):
                raw = (resources[0].get('manifest') or {}).get('url', '')
                if raw:
                    stream_url = f'{unquote(raw)}&content_id={cid}'

            if not stream_url:
                logger.debug('[tubi] skipping %s — no stream URL', name)
                continue

            self._url_cache[cid] = stream_url

            group_list = [k for k, v in groups.items() if cid in v]
            category   = group_list[0] if group_list else None

            # Gracenote ID: prefer what the EPG row returns, fall back to lookup table
            gracenote_id = resolve_gracenote(
                "tubi",
                upstream_id=row.get('gracenote_id'),
                lookup_key=cid,
            )

            channels.append(ChannelData(
                source_channel_id = cid,
                name              = name,
                stream_url        = f'tubi://{cid}',
                logo_url          = logo,
                category          = category,
                language          = _language_from_metadata(name, category),
                country           = 'US',
                stream_type       = 'hls',
                gracenote_id      = gracenote_id,
                tags              = group_list,
                description       = (row.get('description') or row.get('summary') or '').strip() or None,
            ))

        logger.info('[tubi] %d channels (anonymous)', len(channels))
        return channels

    def _parse_live_page(self) -> tuple[list, dict, Optional[str]]:
        """
        Fetches tubitv.com/live and extracts window.__data using brace-counting
        — no BeautifulSoup or extra deps needed.
        """
        try:
            r = self.session.get(_LIVE_PAGE_URL, timeout=20)
            r.raise_for_status()
        except Exception as e:
            return [], {}, f'GET {_LIVE_PAGE_URL} failed: {e}'

        html = r.text

        # Find  window.__data = {
        marker = html.find('window.__data')
        if marker == -1:
            return [], {}, 'window.__data marker not found'

        open_brace = html.find('{', marker)
        if open_brace == -1:
            return [], {}, 'window.__data opening brace not found'

        # Walk forward counting braces to find the matching close
        depth = 0
        close_brace = open_brace
        for i in range(open_brace, len(html)):
            c = html[i]
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    close_brace = i
                    break
        else:
            return [], {}, 'window.__data brace matching failed (unbalanced JSON)'

        blob = html[open_brace:close_brace + 1]

        # Sanitise non-JSON JS constructs before parsing
        blob = re.sub(r'\bundefined\b', 'null', blob)
        blob = re.sub(
            r'new\s+Date\(("[^"]*")\)',
            r'\1',
            blob,
        )

        try:
            data = json.loads(blob)
        except json.JSONDecodeError as e:
            return [], {}, f'JSON decode failed: {e}'

        by_container = (data.get('epg') or {}).get('contentIdsByContainer', {})

        # Flat list of unique channel IDs, excluding personalisation buckets
        channel_ids = list({
            cid
            for bucket in by_container.values()
            for item in bucket
            if item.get('container_slug') not in _SKIP_SLUGS
            for cid in item.get('contents', [])
        })

        # Group name → list of channel IDs
        groups: dict[str, list] = {}
        for bucket in by_container.values():
            for item in bucket:
                slug = item.get('container_slug', '')
                if slug not in _SKIP_SLUGS and item.get('name'):
                    groups[item['name']] = item.get('contents', [])

        logger.debug('[tubi] live page: %d channel IDs, %d groups', len(channel_ids), len(groups))
        return channel_ids, groups, None

    def _fetch_epg_rows_anon(self, channel_ids: list) -> list[dict]:
        """Calls the public EPG endpoint in batches, returns raw row dicts."""
        rows: list[dict] = []
        for i in range(0, len(channel_ids), _BATCH):
            batch  = channel_ids[i:i + _BATCH]
            params = {'content_id': ','.join(str(x) for x in batch)}
            try:
                r = self.session.get(_EPG_ANON_URL, params=params, timeout=30)
                r.raise_for_status()
                rows.extend(r.json().get('rows', []))
                logger.debug('[tubi] anon EPG batch %d–%d: %d rows', i, i + len(batch), len(rows))
            except Exception as e:
                logger.warning('[tubi] anon EPG batch %d failed: %s', i, e)
        return rows

    # ── Authenticated channel fetch ───────────────────────────────────────────

    def _channels_auth(self) -> list[ChannelData]:
        bearer = self._get_token()
        if not bearer:
            return []

        headers = {**self.session.headers,
                   'authorization':   f'Bearer {bearer}',
                   'x-tubi-mode':     'all',
                   'x-tubi-platform': 'web',
                   'content-type':    'application/json'}
        params  = {'mode': 'tubitv_us_linear', 'platform': 'web', 'device_id': self._device_id}

        try:
            r = self.session.get(_CHANNELS_URL, params=params, headers=headers, timeout=30)
            r.raise_for_status()
            resp = r.json()
        except Exception as e:
            logger.error('[tubi] auth channels API failed: %s', e)
            return []

        containers = resp.get('containers', [])
        groups: dict[str, list] = {}
        for item in containers:
            if item.get('container_slug') not in _SKIP_SLUGS and item.get('name'):
                groups[item['name']] = item.get('contents', [])

        contents  = resp.get('contents', {})
        channels: list[ChannelData] = []

        for cid, elem in contents.items():
            if elem.get('needs_login'):
                continue
            name      = (elem.get('title') or '').strip()
            resources = elem.get('video_resources') or []
            raw_url   = (resources[0].get('manifest') or {}).get('url', '') if resources else ''
            if not name or not raw_url:
                continue

            logos = (elem.get('images') or {}).get('thumbnail', [])
            logo  = logos[0] if isinstance(logos, list) and logos else None

            group_list = [k for k, v in groups.items() if cid in v]
            category   = group_list[0] if group_list else None
            scid       = str(cid)

            self._url_cache[scid] = raw_url

            # Gracenote ID from EPG content or lookup table
            gracenote_id = resolve_gracenote("tubi", lookup_key=scid)

            channels.append(ChannelData(
                source_channel_id = scid,
                name              = name,
                stream_url        = f'tubi://{scid}',
                logo_url          = logo,
                category          = category,
                language          = _language_from_metadata(name, category),
                country           = 'US',
                stream_type       = 'hls',
                gracenote_id      = gracenote_id,
                tags              = group_list,
                description       = (elem.get('description') or elem.get('summary') or '').strip() or None,
            ))

        logger.info('[tubi] %d channels (authenticated)', len(channels))
        return channels

    # ── EPG ──────────────────────────────────────────────────────────────────

    def _epg_anon(self, channel_ids: list[str]) -> list[ProgramData]:
        rows = self._fetch_epg_rows_anon(channel_ids)
        return self._parse_epg_rows(rows)

    def _epg_auth(self, channel_ids: list[str]) -> list[ProgramData]:
        bearer = self._get_token()
        if not bearer:
            return []

        headers = {**self.session.headers,
                   'authorization':   f'Bearer {bearer}',
                   'x-tubi-mode':     'all',
                   'x-tubi-platform': 'web'}
        params  = {'platform': 'web', 'device_id': self._device_id, 'lookahead': 1}

        rows: list[dict] = []
        for i in range(0, len(channel_ids), _BATCH):
            batch = channel_ids[i:i + _BATCH]
            params['content_id'] = ','.join(batch)
            try:
                r = self.session.get(_EPG_AUTH_URL, params=params, headers=headers, timeout=30)
                r.raise_for_status()
                rows.extend(r.json().get('rows', []))
            except Exception as e:
                logger.warning('[tubi] auth EPG batch %d failed: %s', i, e)

        return self._parse_epg_rows(rows)

    def _parse_epg_rows(self, rows: list[dict]) -> list[ProgramData]:
        programs: list[ProgramData] = []
        for station in rows:
            cid      = str(station.get('content_id', ''))
            for p in (station.get('programs') or []):
                try:
                    start = datetime.fromisoformat(
                        p['start_time'].replace('Z', '+00:00')
                    ).replace(tzinfo=timezone.utc)
                    end   = datetime.fromisoformat(
                        p['end_time'].replace('Z', '+00:00')
                    ).replace(tzinfo=timezone.utc)
                except (KeyError, ValueError, AttributeError):
                    continue

                title    = (p.get('title') or '').strip() or 'Unknown'
                ep_title = (p.get('episode_title') or '').strip() or None
                if ep_title and ep_title.lower() == title.lower():
                    ep_title = None

                # Rating
                rating  = None
                ratings = p.get('ratings') or []
                if ratings and isinstance(ratings[0], dict):
                    rating = ratings[0].get('code')

                # Best available artwork
                poster = None
                images = p.get('images') or {}
                for key in ('landscape', 'poster', 'hero'):
                    lst = images.get(key)
                    if lst:
                        poster = lst[0] if isinstance(lst, list) else lst
                        break

                programs.append(ProgramData(
                    source_channel_id = cid,
                    title             = title,
                    start_time        = start,
                    end_time          = end,
                    description       = (p.get('description') or '').strip() or None,
                    poster_url        = poster,
                    rating            = rating,
                    episode_title     = ep_title,
                    season            = p.get('season_number') or None,
                    episode           = p.get('episode_number') or None,
                ))

        logger.info('[tubi] %d EPG entries', len(programs))
        return programs

    # ── Token management ─────────────────────────────────────────────────────

    def _get_token(self) -> Optional[str]:
        now = time.time()
        if self._token and (now - self._token_at) < (self._token_ttl - 60):
            return self._token

        payload = {
            'type':     'email',
            'platform': 'web',
            'device_id': self._device_id,
            'credentials': {'email': self._username, 'password': self._password},
            'errorLog': False,
        }
        try:
            r = self.session.post(_LOGIN_URL, json=payload,
                                  headers={**self.session.headers, 'content-type': 'application/json'},
                                  timeout=15)
            r.raise_for_status()
        except Exception as e:
            logger.error('[tubi] login failed: %s', e)
            return None

        resp = r.json()
        self._token    = resp.get('access_token')
        self._token_at = now
        self._token_ttl = float(resp.get('expires_in', 3600))
        logger.debug('[tubi] token refreshed, ttl=%.0fs', self._token_ttl)
        return self._token

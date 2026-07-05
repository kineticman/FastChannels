"""
Fubo TV scraper for FastChannels.

Supports both paid and free (FAST channel) Fubo TV accounts.  In FAST-only
mode (default) only the ~180 free ad-supported channels are included; all
have plain HLS streams with no DRM.  Paid accounts can disable FAST-only to
include all subscription channels (~835 total, some DRM-protected).

Auth: email + password.  Tokens are cached in source config and refreshed
automatically via POST /refresh (refresh token valid ~1 year).

Note: Fubo's API uses TLS fingerprinting to reject automated clients.
Auth requests use curl_cffi with Chrome impersonation to pass this check.
EPG requests are unauthenticated and work with plain requests.
"""
from __future__ import annotations

import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
try:
    from curl_cffi import requests as _cffi_requests
    _CFFI_IMPERSONATE = 'chrome124'
except ImportError:
    _cffi_requests = None
    _CFFI_IMPERSONATE = None

from .base import (BaseScraper, ChannelData, ConfigField, ProgramData,
                   ScrapeSkipError, StreamDeadError, infer_language_from_metadata)
from .category_utils import category_for_channel, infer_category_from_name

logger = logging.getLogger(__name__)

_API = 'https://api.fubo.tv'
_SIGNIN_URL   = f'{_API}/v2/signin'
_REFRESH_URL  = f'{_API}/refresh'
_EPG_URL      = f'{_API}/epg'
_PAPI_EPG_URL = f'{_API}/papi/v1/guide/epg'
_ASSET_URL    = f'{_API}/vapi/asset/v1'

_TOKEN_TTL    = 60 * 60 * 8   # refresh access token after 8 hours (issued for 10h)
_EPG_HOURS    = 6              # hours per EPG request window
_EPG_DAYS     = 7              # days of EPG to fetch
_RICH_EPG_HOURS = 24           # metadata enrichment window

# Minimal headers for auth calls (PUT /v2/signin, POST /refresh).
# curl_cffi with Chrome impersonation already injects its own device/OS headers;
# sending the full _DEFAULT_HEADERS on top clashes and triggers 401/429.
_AUTH_HEADERS = {
    'accept': '*/*',
    'origin': 'https://www.fubo.tv',
    'referer': 'https://www.fubo.tv/',
    'user-agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36'
    ),
    'x-application-id': 'fubo',
    'x-client-version': '6.9.0',
    'x-drm-scheme': 'widevine',
}

_DEFAULT_HEADERS = {
    'accept': '*/*',
    'origin': 'https://www.fubo.tv',
    'referer': 'https://www.fubo.tv/',
    'user-agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36'
    ),
    'x-application-id': 'fubo',
    'x-browser': 'Chrome',
    'x-browser-engine': 'Blink',
    'x-browser-version': '148.0.0.0',
    'x-client-version': '6.9.0',
    'x-device-app': 'web',
    'x-device-group': 'desktop',
    'x-device-model': 'Windows NT 10.0 Chrome 148.0.0.0',
    'x-device-platform': 'desktop',
    'x-device-type': 'desktop',
    'x-drm-scheme': 'widevine',
    'x-os': 'Windows',
    'x-os-version': 'NT 10.0',
    'x-player-version': '7.10.3',
    'x-preferred-language': 'en-US',
}

_GENRE_MAP: dict[str, str] = {
    'News':          'News',
    'Sports':        'Sports',
    'Sports talk':   'Sports',
    'Talk':          'News/Talk',
    'Reality':       'Reality',
    'Documentary':   'Documentary',
    'Drama':         'Drama',
    'Comedy':        'Comedy',
    'Movies':        'Movies',
    'Movie':         'Movies',
    'Kids':          'Kids & Family',
    'Children':      'Kids & Family',
    'Animated':      'Kids & Family',
    'Animation':     'Kids & Family',
    'Music':         'Music',
    'Food':          'Lifestyle',
    'Cooking':       'Lifestyle',
    'Home':          'Lifestyle',
    'Travel':        'Lifestyle',
    'Nature':        'Outdoors',
    'Outdoors':      'Outdoors',
    'Science':       'Science & Tech',
    'Technology':    'Science & Tech',
    'History':       'History',
    'Educational':   'Educational',
    'Business':      'Business',
    'Finance':       'Business',
    'Fitness':       'Health & Fitness',
    'Health':        'Health & Fitness',
    'Religion':      'Religious',
    'Faith':         'Religious',
    'Horror':        'Horror',
    'Thriller':      'Drama',
    'Action':        'Action & Adventure',
    'Adventure':     'Action & Adventure',
}


def _map_genre(genres: list[dict]) -> str | None:
    for g in genres:
        raw = g.get('name', '')
        for key, mapped in _GENRE_MAP.items():
            if key.lower() in raw.lower():
                return mapped
    return None


class FuboScraper(BaseScraper):
    """
    Scraper for Fubo TV — subscription live TV service.

    Streams are AES-128 HLS (live channels). The stream URL is resolved at
    play time via Fubo's asset API; the stored opaque URL is fubo://<channel_id>.

    NOTE: Fubo's Akamai CDN tokens are bound to the requesting IP. FastChannels
    should run on the same local network as the clients (home-server use). Remote
    clients on different public IPs will see broken streams.
    """

    source_name      = 'fubo'
    display_name     = 'Fubo TV'
    scrape_interval  = 360
    config_required  = True
    is_premium       = True
    source_category  = 'premium'
    stream_audit_enabled = True

    config_schema = [
        ConfigField('username', 'Email', required=True,
                    placeholder='you@example.com',
                    help_text='Your Fubo TV login email.'),
        ConfigField('password', 'Password', field_type='password', required=True,
                    secret=True,
                    help_text='Your Fubo TV password.'),
        ConfigField('fast_only', 'FAST Channels Only', field_type='toggle',
                    required=False, default='true',
                    help_text='Include only free FAST channels (recommended). '
                              'Disable to include all channels from your subscription.'),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        # Stable device ID — generated once, persisted in config
        if not self.config.get('device_id'):
            self._update_config('device_id', _make_device_id())
        self._api_headers = {
            **_DEFAULT_HEADERS,
            'x-device-id': self.config['device_id'],
        }
        # Set once a fresh full login has been forced this run (e.g. after a
        # 'not in allowed list' 403) so repeated resolves against other
        # channels in the same scrape/audit reuse it instead of re-logging in
        # per channel.
        self._forced_relogin = False

    # ── Auth ─────────────────────────────────────────────────────────────────

    def _cffi_request(self, method: str, url: str, **kwargs):
        """Send a request using curl_cffi Chrome impersonation to bypass TLS fingerprinting.
        Falls back to plain requests if curl_cffi is unavailable."""
        if _cffi_requests:
            return _cffi_requests.request(method, url, impersonate=_CFFI_IMPERSONATE, **kwargs)
        return self.session.request(method, url, **kwargs)

    def _login(self) -> None:
        username = (self.config.get('username') or '').strip()
        password = (self.config.get('password') or '').strip()
        if not username or not password:
            raise ScrapeSkipError('Fubo TV: username and password are required')
        auth_headers = {**_AUTH_HEADERS, 'x-device-id': self.config['device_id']}
        r = self._cffi_request(
            'PUT', _SIGNIN_URL,
            json={'username': username, 'password': password},
            headers=auth_headers,
            timeout=20,
        )
        if not r.ok:
            err = (r.json().get('error') or {}).get('message', r.text[:100])
            raise ScrapeSkipError(f'Fubo TV login failed ({err})')
        self._store_tokens(r.json())
        logger.info('[fubo] logged in as %s', username)

    def _do_refresh(self, refresh_token: str) -> None:
        auth_headers = {**_AUTH_HEADERS, 'x-device-id': self.config['device_id'],
                        'authorization': f'Bearer {refresh_token}'}
        r = self._cffi_request(
            'POST', _REFRESH_URL,
            json={},
            headers=auth_headers,
            timeout=20,
        )
        if not r.ok:
            raise RuntimeError(f'Fubo TV token refresh failed ({r.status_code}): {r.text[:100]}')
        self._store_tokens(r.json())
        logger.debug('[fubo] access token refreshed')

    def _store_tokens(self, data: dict) -> None:
        data = data.get('payload', data)  # v2/signin wraps in {"type":"TOKEN","payload":{...}}
        access = data.get('access_token') or data.get('token') or ''
        refresh = data.get('refresh_token', '')
        self._update_config('access_token', access)
        if refresh:
            self._update_config('refresh_token', refresh)
        self._update_config('token_time', time.time())
        self._api_headers['authorization'] = f'Bearer {access}'

    def _ensure_auth(self) -> None:
        access_token = self.config.get('access_token', '')
        token_time   = self.config.get('token_time', 0)
        token_stale  = not access_token or (time.time() - token_time) > _TOKEN_TTL

        if not token_stale:
            self._api_headers['authorization'] = f'Bearer {access_token}'
            return

        refresh_token = self.config.get('refresh_token', '').strip()
        if refresh_token:
            try:
                self._do_refresh(refresh_token)
                return
            except Exception as exc:
                logger.warning('[fubo] refresh failed (%s), trying login', exc)

        self._login()

    def pre_run_setup(self) -> None:
        self._ensure_auth()

    # ── Channels ──────────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        end = now + timedelta(hours=12)
        data = self._epg_request(now, end)

        try:
            from app.gracenote_map import resolve_gracenote
        except ImportError:
            resolve_gracenote = None

        fast_only = str(self.config.get('fast_only', 'true')).lower() != 'false'

        channels: list[ChannelData] = []
        for item in data:
            ch = item['data']['channel']
            ch_id = str(ch['id'])
            name  = ch.get('displayName') or ch.get('name') or ''
            if not name:
                continue

            # Skip PPV/pay-per-view channels
            if any(t in ch_id for t in ['168364', '168365', '36230000']):
                continue
            tags = [t for t in (ch.get('tags') or []) if not t.lower().startswith('us-compare')]
            tags_lower = [t.lower() for t in tags]
            if 'ppv' in tags_lower:
                continue

            # Skip ESPN+ VOD event slots and channels marked epg_false —
            # these are not real live channels and always fail stream resolution
            if 'espn_plus' in tags_lower or 'epg_false' in tags_lower:
                continue

            # In FAST-only mode, include only free ad-supported channels
            if fast_only and 'fast_channel' not in tags_lower:
                continue

            logo  = ch.get('logoOnDarkUrl') or ch.get('logoOnWhiteUrl') or ''
            desc  = ch.get('description') or ''
            call  = ch.get('callSign') or ''

            gracenote_id = (
                resolve_gracenote('fubo', lookup_key=ch_id) if resolve_gracenote else None
            )
            category = category_for_channel(name, None) or infer_category_from_name(name) or 'Entertainment'

            channels.append(ChannelData(
                source_channel_id=ch_id,
                name=name,
                stream_url=f'fubo://{ch_id}',
                logo_url=logo or None,
                category=category,
                language='es' if 'spanish' in tags_lower else infer_language_from_metadata(name),
                country='US',
                stream_type='hls',
                gracenote_id=gracenote_id,
                description=desc or None,
                guide_key=call or None,
                tags=tags,
            ))

        # Fubo EPG sometimes contains duplicate entries for the same channel
        # (e.g. id=123605 and id=1236050001) — alternate feed slots. Deduplicate
        # by name, keeping the shortest channel ID (the canonical original).
        seen: dict[str, ChannelData] = {}
        for ch in channels:
            name = ch.name
            if name not in seen or len(ch.source_channel_id) < len(seen[name].source_channel_id):
                seen[name] = ch
        pre_dedup = len(channels)
        channels = list(seen.values())

        logger.info('[fubo] fetched %d channels (%d after dedup)', pre_dedup, len(channels))
        return channels

    # ── EPG ───────────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        resolve_channel_id = _channel_id_resolver(channels)
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        end_total = now + timedelta(days=_EPG_DAYS)

        self._ensure_auth()

        schedule: dict[tuple[str, datetime], dict[str, Any]] = {}
        papi_ok = True
        window_start = now
        while window_start < end_total:
            window_end = min(window_start + timedelta(hours=_EPG_HOURS), end_total)
            try:
                data = self._papi_epg_request(window_start, window_end)
            except Exception as exc:
                logger.warning('[fubo] PAPI EPG window %s failed: %s', window_start, exc)
                papi_ok = False
                break

            for channel in _papi_channels(data):
                ch_id = resolve_channel_id(
                    channel.get('id'), _text_value(channel.get('name'))
                )
                if not ch_id:
                    continue
                for prog in channel.get('components') or []:
                    if prog.get('type') != 'program-cell':
                        continue
                    start_raw = prog.get('start_time')
                    end_raw = prog.get('end_time')
                    start_key = _parse_dt(start_raw)
                    if not start_key or not end_raw:
                        continue
                    schedule.setdefault((ch_id, start_key), {
                        'source_channel_id': ch_id,
                        'title': _text_value(prog.get('title')) or 'Unknown',
                        'episode_title': _text_value(prog.get('subtitle')),
                        'start_time': start_raw,
                        'end_time': end_raw,
                        'poster_url': _image_value(prog.get('picture')),
                    })

            window_start = window_end

        if not papi_ok:
            schedule.clear()

        rich_schedule: dict[tuple[str, datetime], dict[str, Any]] = {}
        window_start = now
        while window_start < end_total:
            window_end = min(window_start + timedelta(hours=_RICH_EPG_HOURS), end_total)
            try:
                data = self._epg_request(window_start, window_end)
            except Exception as exc:
                logger.warning('[fubo] rich EPG window %s failed: %s', window_start, exc)
                window_start = window_end
                continue

            for item in data:
                ch_data = item.get('data') or {}
                upstream_channel = ch_data.get('channel') or {}
                ch_id = resolve_channel_id(
                    upstream_channel.get('id'),
                    upstream_channel.get('displayName') or upstream_channel.get('name'),
                )
                if not ch_id:
                    continue
                for entry in ch_data.get('programsWithAssets') or []:
                    parsed = _parse_rich_program(ch_id, entry)
                    if parsed:
                        start_key = _parse_dt(parsed['start_time'])
                        if start_key:
                            rich_schedule[(ch_id, start_key)] = parsed

            window_start = window_end

        for key, rich in rich_schedule.items():
            if key in schedule:
                schedule[key].update(rich)
            else:
                schedule[key] = rich

        programs: list[ProgramData] = []
        for raw in schedule.values():
            start_dt = _parse_dt(raw.get('start_time'))
            end_dt = _parse_dt(raw.get('end_time'))
            if not start_dt or not end_dt:
                continue
            title = raw.get('title') or 'Unknown'
            programs.append(ProgramData(
                source_channel_id=raw['source_channel_id'],
                title=title,
                start_time=start_dt,
                end_time=end_dt,
                description=raw.get('description') or None,
                poster_url=raw.get('poster_url') or None,
                category=raw.get('category') or infer_category_from_name(title),
                rating=raw.get('rating') or None,
                episode_title=raw.get('episode_title') or None,
                season=raw.get('season'),
                episode=raw.get('episode'),
                original_air_date=raw.get('original_air_date'),
                is_live=raw.get('is_live'),
                program_type=raw.get('program_type'),
                series_id=raw.get('series_id'),
                episode_id=raw.get('episode_id'),
            ))

        # Visibility into EPG quality: how many cells got rich enrichment vs
        # shipped bare (title/times only) from PAPI, and how many channel ids
        # had to be resolved via the fuzzy suffix/name heuristics.
        enriched = sum(1 for key in schedule if key in rich_schedule)
        _rs = resolve_channel_id.stats
        logger.info(
            '[fubo] fetched %d EPG entries (%d enriched, %d bare); '
            'id match: %d exact, %d suffix, %d name, %d unresolved',
            len(programs), enriched, len(schedule) - enriched,
            _rs['exact'], _rs['suffix'], _rs['name'], _rs['unresolved'],
        )
        return programs

    # ── Stream resolution ─────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        ch_id = raw_url.removeprefix('fubo://')
        self._ensure_auth()
        r = self._cffi_request(
            'GET', _ASSET_URL,
            params={'channelId': ch_id, 'type': 'live'},
            headers=self._api_headers,
            timeout=15,
        )
        if not r.ok:
            err = r.json().get('error', {}).get('message', '') if r.content else ''
            if 'not in allowed list' in err or r.status_code == 403:
                # The cached access/refresh token pair carries the entitlements
                # from whenever it was first issued — refreshing it just mints
                # a new access token off the same session, it doesn't re-check
                # the account's current plan. A user who upgraded Fubo (same
                # login, no credential change) keeps getting the pre-upgrade
                # 403 forever otherwise. Force one real re-login per run before
                # concluding the channel is genuinely not entitled.
                if not self._forced_relogin:
                    self._forced_relogin = True
                    logger.info('[fubo] %s not in allowed list — forcing fresh login to rule out a stale entitlement', ch_id)
                    self._login()
                    r = self._cffi_request(
                        'GET', _ASSET_URL,
                        params={'channelId': ch_id, 'type': 'live'},
                        headers=self._api_headers,
                        timeout=15,
                    )
                    if r.ok:
                        return self._extract_stream_url(r, ch_id)
                    err = r.json().get('error', {}).get('message', '') if r.content else ''
                raise StreamDeadError(f'Fubo channel {ch_id} not in subscription: {err}')
            raise RuntimeError(f'Fubo stream resolution failed for channel {ch_id}: {err or r.status_code}')
        return self._extract_stream_url(r, ch_id)

    def _extract_stream_url(self, r, ch_id: str) -> str:
        stream = r.json().get('stream') or {}
        if stream.get('drmProvider') == 'wurl':
            raise StreamDeadError(
                f'Fubo channel {ch_id} uses WURL proprietary DRM — not supported'
            )
        stream_url = stream.get('url', '')
        if not stream_url:
            raise RuntimeError(f'Fubo: no stream URL returned for channel {ch_id}')
        return stream_url

    # ── Internal ──────────────────────────────────────────────────────────────

    def _epg_request(self, start: datetime, end: datetime) -> list[dict]:
        fmt = lambda d: d.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        r = self.session.get(
            _EPG_URL,
            params={'startTime': fmt(start), 'endTime': fmt(end), 'enrichments': 'follow'},
            headers=_DEFAULT_HEADERS,
            timeout=120,
        )
        r.raise_for_status()
        return r.json().get('response', [])

    def _papi_epg_request(self, start: datetime, end: datetime) -> dict:
        fmt = lambda d: d.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        r = self.session.get(
            _PAPI_EPG_URL,
            params={'start_time': fmt(start), 'end_time': fmt(end)},
            headers=self._api_headers,
            timeout=120,
        )
        r.raise_for_status()
        return r.json()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalize_channel_name(value) -> str:
    return ' '.join(str(value or '').casefold().split())


def _channel_id_resolver(channels: list[ChannelData]):
    exact_ids = {str(ch.source_channel_id) for ch in channels}
    names: dict[str, list[str]] = {}
    for ch in channels:
        names.setdefault(_normalize_channel_name(ch.name), []).append(str(ch.source_channel_id))

    # Track how each upstream id was matched so a high reliance on the fuzzy
    # heuristics (which can graft EPG onto the wrong channel) is visible in logs.
    stats = {'exact': 0, 'suffix': 0, 'name': 0, 'unresolved': 0}

    def resolve(upstream_id, upstream_name=None) -> str | None:
        raw_id = str(upstream_id or '')
        if raw_id in exact_ids:
            stats['exact'] += 1
            return raw_id

        # PAPI appends 4-char suffixes to some IDs (e.g. 1236050001 → 123605);
        # fetch_channels deduplicates these to the shorter canonical ID.
        if len(raw_id) > 4 and raw_id[-4:].startswith('000'):
            base_id = raw_id[:-4]
            if base_id in exact_ids:
                stats['suffix'] += 1
                return base_id

        name_matches = names.get(_normalize_channel_name(upstream_name), [])
        if len(name_matches) == 1:
            stats['name'] += 1
            return name_matches[0]
        stats['unresolved'] += 1
        return None

    resolve.stats = stats
    return resolve

def _papi_channels(data: dict) -> list[dict]:
    epg = ((data.get('content') or {}).get('epg')) or {}
    if isinstance(epg, list):
        return epg
    if not isinstance(epg, dict):
        return []
    if epg.get('type') == 'channel-cell':
        return [epg]
    channels = epg.get('components') or epg.get('channels') or []
    return channels if isinstance(channels, list) else []


def _text_value(value) -> str | None:
    if isinstance(value, dict):
        value = value.get('text')
    value = str(value or '').strip()
    return value or None


def _image_value(value) -> str | None:
    if isinstance(value, dict):
        value = value.get('url')
    value = str(value or '').strip()
    return value or None


def _parse_rich_program(ch_id: str, entry: dict) -> dict[str, Any] | None:
    prog = entry.get('program') or {}
    assets = entry.get('assets') or []
    if not assets:
        return None
    asset = assets[0]
    rights = asset.get('accessRights') or (asset.get('accessRightsV2') or {}).get('live') or {}
    start_raw = rights.get('startTime')
    end_raw = rights.get('endTime')
    if not start_raw or not end_raw:
        return None

    heading = prog.get('heading') or prog.get('title') or 'Unknown'
    title = prog.get('title')
    meta = prog.get('metadata') or {}
    p_type = prog.get('metadataType') or prog.get('type')
    if p_type in ('episode', 'series'):
        p_type = 'episode'
    elif p_type != 'movie':
        p_type = None

    return {
        'source_channel_id': ch_id,
        'title': heading,
        'episode_title': title if title and title != heading else None,
        'start_time': start_raw,
        'end_time': end_raw,
        'description': prog.get('longDescription') or prog.get('shortDescription') or None,
        'poster_url': (
            prog.get('horizontalImage') or prog.get('featuredImage') or
            prog.get('verticalImage') or None
        ),
        'category': _map_genre(prog.get('genres') or []),
        'rating': prog.get('rating') or None,
        'season': meta.get('seasonNumber'),
        'episode': meta.get('episodeNumber'),
        'original_air_date': _parse_date(meta.get('originalAiringDate')),
        'is_live': (asset.get('qualifiers') or {}).get('isLive'),
        'program_type': p_type,
        'series_id': str(meta['seriesId']) if meta.get('seriesId') else None,
        'episode_id': prog.get('programId'),
    }

def _make_device_id() -> str:
    raw = uuid.uuid4().hex[:18]
    return raw[:10] + '-' + raw[10:]


def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None


def _parse_date(value: str):
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], '%Y-%m-%d').date()
    except (ValueError, AttributeError):
        return None

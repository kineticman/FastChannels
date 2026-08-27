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
import math
import os
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
                   ScrapeSkipError, StreamDeadError, infer_language_from_metadata,
                   null_placeholder_season_episode, dedupe_dominant_episode_id)
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
_DASH_TTL     = 6 * 60 * 60    # Irdeto drm.token JWT is valid ~24h; refresh well before expiry
_RICH_EPG_HOURS = 24           # metadata enrichment window

# Play-time resolve() forces one real login to rule out a stale entitlement on a
# 403 (see resolve()). Each play request builds a fresh FuboScraper instance, so
# an in-memory per-instance guard doesn't bound repeat logins across requests —
# a channel stuck 403ing would get a real login PUT per client request/reconnect,
# risking Fubo flagging the account for anomalous login activity. Gate with a
# Redis cooldown shared across all gunicorn workers/processes instead.
_FORCED_RELOGIN_COOLDOWN = 300  # seconds; one forced relogin attempt per account per window
_redis_client = None


def _get_redis():
    """Lazy Redis client for cross-process forced-relogin cooldown. Returns None
    if unavailable (falls back to a per-instance-only guard)."""
    global _redis_client
    if _redis_client is None:
        try:
            import redis as _r
            _redis_client = _r.from_url(
                os.environ.get('REDIS_URL', 'redis://localhost:6379/0'),
                decode_responses=True,
                socket_timeout=1,
                socket_connect_timeout=1,
            )
        except Exception:
            pass
    return _redis_client

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

    # Presence of license_url marks the source DRM-capable and enables the generic
    # PrismCast bridge (worker.py's _bridge_capable check + /play/fubo/license proxy).
    # NOT all channels are DRM — most FAST channels are plain HLS/AES-128. Only
    # channels resolve_dash() actually confirms as DASH-capable (see below) get
    # routed through the bridge; get_license_url() returns None for the rest.
    license_url = 'https://irdeto.fubo.tv/licenseServer/widevine/v1/FuboTV/license'

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
        # Fallback-only guard for when Redis is unavailable (see
        # _claim_forced_relogin): still bounds repeated resolves against other
        # channels within this one instance's lifetime (a scrape/audit run).
        self._forced_relogin = False
        self._dash_cache: dict[str, dict] = {}
        self._load_dash_cache()

    def _claim_forced_relogin(self) -> bool:
        """Atomically claim the right to force one fresh login to rule out a
        stale entitlement, at most once per _FORCED_RELOGIN_COOLDOWN across all
        processes/requests for this account. Falls back to a per-instance flag
        if Redis is unavailable."""
        username = (self.config.get('username') or '').strip().lower()
        rdb = _get_redis()
        if rdb:
            try:
                return bool(rdb.set(
                    f'fubo:forced_relogin:{username}', '1',
                    nx=True, ex=_FORCED_RELOGIN_COOLDOWN,
                ))
            except Exception:
                logger.debug('[fubo] Redis unavailable for forced-relogin cooldown, '
                             'falling back to per-instance guard', exc_info=True)
        if not self._forced_relogin:
            self._forced_relogin = True
            return True
        return False

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

            # logoOnWhiteUrl is the real full-color logo (meant for a white background);
            # logoOnDarkUrl is a white monochrome silhouette (meant for a dark background,
            # e.g. some are literally named "..._logo_white.png") — prefer color.
            logo  = ch.get('logoOnWhiteUrl') or ch.get('logoOnDarkUrl') or ''
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

        total_hours = _EPG_DAYS * 24
        total_windows = math.ceil(total_hours / _EPG_HOURS) + math.ceil(total_hours / _RICH_EPG_HOURS)
        windows_done = 0

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
            windows_done += 1
            if self._progress_cb:
                self._progress_cb('epg', windows_done, total_windows)

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
                windows_done += 1
                if self._progress_cb:
                    self._progress_cb('epg', windows_done, total_windows)
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
            windows_done += 1
            if self._progress_cb:
                self._progress_cb('epg', windows_done, total_windows)

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
        dedupe_dominant_episode_id(programs)
        null_placeholder_season_episode(programs)
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
                # 403 forever otherwise. Force one real re-login before
                # concluding the channel is genuinely not entitled — but at
                # most once per cooldown window account-wide, since play-time
                # resolve() gets a fresh scraper instance per request and a
                # persistently-403ing channel must not trigger a real login on
                # every single client request/reconnect.
                if self._claim_forced_relogin():
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
                    if not ('not in allowed list' in err or r.status_code == 403):
                        # Retry failed for an unrelated reason (transient 500,
                        # rate limit, etc.) — not a real entitlement loss.
                        raise RuntimeError(f'Fubo stream resolution failed for channel {ch_id}: {err or r.status_code}')
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

    # ── DASH + Widevine (browser/EME → PrismCast bridge) ────────────────────────
    # resolve()/_extract_stream_url above always requests Fubo's default (HLS)
    # packaging, which for DRM-protected channels is FairPlay-only — Chrome/EME
    # can't decrypt it, which is why these channels used to just get disabled.
    # The SAME content is also available as a CENC DASH manifest (Widevine +
    # PlayReady, confirmed live 2026-08-27 by diffing a real browser HAR capture
    # against our own requests) but only when the client advertises DASH support
    # via x-supported-streaming-protocols — Fubo's web player always sends it,
    # our scraper never did. Kept entirely separate from the plain resolve()
    # path/cache so standard (non-bridge) playback is completely unaffected —
    # most Fubo channels aren't DRM at all, and genuinely non-DRM channels keep
    # packagingProtocol=hls even with this header (confirmed live), so this
    # never misroutes a clean channel into the license flow.

    def _load_dash_cache(self) -> None:
        raw = self.cache.get('dash_cache') or {}
        if not isinstance(raw, dict):
            return
        now = time.time()
        for cid, entry in raw.items():
            if not isinstance(entry, dict):
                continue
            cached_at = entry.get('cached_at')
            if not entry.get('mpd_url') or not isinstance(cached_at, (int, float)):
                continue
            if (now - float(cached_at)) >= _DASH_TTL:
                continue
            self._dash_cache[cid] = entry

    def _cache_dash(self, cid: str, mpd_url: str, license_url: str | None, token: str | None) -> None:
        self._dash_cache[cid] = {
            'mpd_url': mpd_url,
            'license_url': license_url,
            'token': token,
            'cached_at': time.time(),
        }
        self._update_cache('dash_cache', self._dash_cache)

    def _cached_dash(self, cid: str) -> dict | None:
        entry = self._dash_cache.get(cid)
        if not entry:
            return None
        cached_at = entry.get('cached_at')
        if not entry.get('mpd_url') or not isinstance(cached_at, (int, float)):
            return None
        if (time.time() - float(cached_at)) >= _DASH_TTL:
            self._dash_cache.pop(cid, None)
            self._update_cache('dash_cache', self._dash_cache)
            return None
        return entry

    def resolve_dash(self, raw_url: str, *, allow_cached: bool = True) -> dict:
        """Resolve a Fubo channel to its CENC DASH+Widevine variant for browser EME
        playback. Returns {'mpd_url': str|None, 'license_url': str|None} — mpd_url is
        None when this channel has no DASH variant (genuinely not DRM-protected)."""
        cid = raw_url.removeprefix('fubo://')

        if allow_cached:
            cached = self._cached_dash(cid)
            if cached:
                return {'mpd_url': cached['mpd_url'], 'license_url': cached.get('license_url')}

        self._ensure_auth()
        headers = {**self._api_headers, 'x-supported-streaming-protocols': 'hls,dash'}
        r = self._cffi_request(
            'GET', _ASSET_URL,
            params={'channelId': cid, 'type': 'live'},
            headers=headers,
            timeout=15,
        )
        if not r.ok:
            raise RuntimeError(f'Fubo DASH resolve failed for channel {cid}: HTTP {r.status_code}')
        data = r.json()
        stream = data.get('stream') or {}
        if (stream.get('packagingProtocol') or '').lower() != 'dash':
            # Not offered for this channel — genuinely not DRM (or DASH isn't
            # available for this content pipeline). Not an error.
            return {'mpd_url': None, 'license_url': None}
        mpd_url = stream.get('url', '')
        if not mpd_url:
            raise RuntimeError(f'Fubo: no DASH URL returned for channel {cid}')
        drm = data.get('drm') or {}
        license_url = drm.get('licenseUrl') or None
        token = drm.get('token') or None
        self._cache_dash(cid, mpd_url, license_url, token)
        logger.info('[fubo] resolve_dash %s -> DASH (license=%s)', cid, 'yes' if token else 'no')
        return {'mpd_url': mpd_url, 'license_url': license_url}

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        # Per-channel and only present once resolve_dash() has actually confirmed
        # this channel is DASH-capable — returning the bare class-level license_url
        # as a fallback would attach a URL with no contentId to every non-DRM
        # channel too (license_url's only other job is the _bridge_capable check
        # in worker.py, which just needs it non-None at the class level).
        if channel_id:
            entry = (config.get('dash_cache') or {}).get(channel_id)
            if isinstance(entry, dict) and entry.get('license_url'):
                return entry['license_url']
        return None

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs
    ) -> tuple[bytes, dict]:
        """Attach the per-channel Irdeto Widevine token captured during resolve_dash().
        NOTE: Authorization: Bearer is the convention Fubo's REST API uses everywhere
        else; the real Irdeto license exchange itself wasn't observed in the captured
        HAR (no live CDM available to generate a genuine challenge) — verify against
        PrismCast before trusting this end-to-end."""
        headers = {
            'Origin': 'https://www.fubo.tv',
            'Referer': 'https://www.fubo.tv/',
        }
        token = None
        if channel_id:
            entry = (config.get('dash_cache') or {}).get(channel_id)
            if isinstance(entry, dict):
                token = entry.get('token')
        if token:
            headers['Authorization'] = f'Bearer {token}'
        return challenge, headers

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

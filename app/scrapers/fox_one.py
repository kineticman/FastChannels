from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

from .base import BaseScraper, ChannelData, ConfigField, ProgramData
from .fox_tve import FoxTVEScraper, CHANNELS as FOX_TVE_CHANNELS

_SCHEME = 'fox-one://'
_API_BASE = 'https://api.fox.com/dtc'
_LIVE_PAGE = '/product/page/v1/landing/653897111489?page=1&size=25'
_API_KEY = 'sVMTVogE67faKqBLOlZBVoYuay2FquzW'
_PLAYBACK_API = 'https://prod.api.digitalvideoplatform.com/foxdtc/v3.0/watchlive'
_HYDRA_TOKEN_API = 'https://id.fox.com/identityhydra/oauth2/token'
_HYDRA_CLIENT_ID = '21af937a-0ed4-4321-b87f-51d3b93976d4'
_ENT_BASE = 'https://ent.fox.com'
_TOKEN_REFRESH_SKEW = 300
_LOCATION_TTL = 6 * 60 * 60
_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'


@dataclass(frozen=True)
class FoxOneChannel:
    source_channel_id: str
    name: str
    container_id: str
    call_sign: str
    target_fox_tve_id: str | None
    logo_url: str | None
    category: str
    gracenote_id: str | None = None

    @property
    def stream_url(self) -> str:
        target = self.target_fox_tve_id or '-'
        return f'{_SCHEME}{self.source_channel_id}/{target}/{self.container_id}'


_SUPPORTED_BY_CALL_SIGN: dict[str, tuple[str, str, str]] = {
    'FOX': ('fox_sports_fox', 'FOX', 'Sports'),
    'FS1': ('fox_sports_fs1', 'FS1', 'Sports'),
    'FS2': ('fox_sports_fs2', 'FS2', 'Sports'),
    'BTN': ('fox_sports_btn', 'Big Ten Network', 'Sports'),
    'FOXD': ('fox_deportes', 'FOX Deportes', 'Sports'),
    'FWX': ('fox_weather', 'Fox Weather', 'News'),
    'FBN': ('fox_business', 'Fox Business Network', 'Business'),
    'FNC': ('fox_news', 'Fox News Channel', 'News'),
}

_TITLE_TO_CALL_SIGN = {
    'B1G': 'BTN',
    'B10': 'BTN',
    'FOXD': 'FOXD',
    'FWX': 'FWX',
    'FBN': 'FBN',
    'FNC': 'FNC',
}

_CHANNEL_OVERRIDES: dict[str, tuple[str, str]] = {
    'FSD': ('FOX Sports Digital', 'Sports'),
    'FMSC': ('FOX Movies', 'Movies'),
    'FMSCFO': ('FOX Movies', 'Movies'),
    'LIVENOW': ('LiveNOW from FOX', 'News'),
    'SOUL': ('FOX Soul', 'Entertainment'),
    'TMZ': ('TMZ', 'Entertainment'),
    'TMZFO': ('TMZ', 'Entertainment'),
    'FXNA': ('FOX Nation', 'Entertainment'),
    'FXNB': ('FOX Nation Binge', 'Entertainment'),
    'FXN1': ('FOX Nation 1', 'Entertainment'),
    'FXN2': ('FOX Nation 2', 'Entertainment'),
}


def _headers() -> dict[str, str]:
    return {
        'User-Agent': _UA,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Origin': 'https://www.fox.com',
        'Referer': 'https://www.fox.com/',
        'x-fox-apikey': _API_KEY,
        'x-fox-client': 'app_version:3.61.0;os:Windows;os_version:19.0.0;platform:web',
    }


def _slug(value: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', value.lower()).strip('-')


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def _parse_date(value: str | None):
    dt = _parse_time(value)
    return dt.date() if dt else None


def _image(images: dict | None, *keys: str) -> str | None:
    if not isinstance(images, dict):
        return None
    for key in keys:
        url = images.get(key)
        if isinstance(url, str) and url:
            return url
    return None


def _genre(item: dict, fallback: str) -> str:
    for genre in item.get('genre_metadata') or []:
        if isinstance(genre, dict) and genre.get('genre_type') == 'primary' and genre.get('display_name'):
            return genre['display_name']
    if item.get('is_sportingevent'):
        return 'Sports'
    return fallback


def _stream_parts(raw_url: str) -> tuple[str, str | None, str | None]:
    if not raw_url.startswith(_SCHEME):
        raise ValueError(f'Unsupported FOX One stream URL: {raw_url}')
    parsed = urlparse(raw_url)
    parts = [part for part in parsed.path.split('/') if part]
    target = parts[0] if parts and parts[0] != '-' else None
    container_id = parts[1] if len(parts) > 1 else None
    return parsed.netloc, target, container_id


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class FoxOneScraper(BaseScraper):
    source_name = 'fox_one'
    display_name = 'FOX One'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True
    config_schema = [
        ConfigField(
            'refresh_token',
            'FOX One refresh token',
            field_type='password',
            secret=True,
            help_text='Required for FOX One-only channels. Capture the identityhydra refresh_token from an authenticated fox.com browser session.',
        ),
        ConfigField(
            'home_zip_code',
            'Home ZIP code',
            placeholder='10001',
            help_text='Optional. Used when FOX asks to initialize home location; otherwise the current locator ZIP is used.',
        ),
        ConfigField(
            'access_token',
            'FOX One access token (advanced)',
            field_type='password',
            secret=True,
            placeholder='Bearer ...',
            help_text='Optional fallback for debugging. The scraper refreshes this automatically when refresh_token is present.',
        ),
        ConfigField(
            'platform_location',
            'FOX One platform location (advanced)',
            field_type='password',
            secret=True,
            help_text='Optional fallback for debugging. The scraper derives and refreshes this automatically from FOX location services.',
        ),
    ]

    def _get_json(self, path_or_url: str) -> dict:
        if path_or_url.startswith('http'):
            url = path_or_url
        else:
            url = f'{_API_BASE}/{path_or_url.lstrip("/")}'
        r = self.session.get(url, headers=_headers(), timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data.get('status'):
            error = data.get('error') or {}
            raise RuntimeError(error.get('err_msg') or error.get('err_str') or f'FOX One API failed for {url}')
        return data.get('data') or {}

    def _container_page(self, uri: str) -> dict:
        return self._get_json(uri)

    def _discover_channels(self) -> list[FoxOneChannel]:
        page = self._get_json(_LIVE_PAGE)
        channels: list[FoxOneChannel] = []
        seen: set[str] = set()

        for container in page.get('containers') or []:
            if not isinstance(container, dict):
                continue
            uri = container.get('uri')
            container_id = str(container.get('container_id') or '')
            if not uri or not container_id:
                continue

            detail = self._container_page(uri)
            items = detail.get('items') or []
            first = next((item for item in items if isinstance(item, dict)), {})
            call_sign = str(first.get('call_sign') or _TITLE_TO_CALL_SIGN.get(str(container.get('title') or ''), '')).upper()
            if not call_sign:
                network = str(first.get('network') or '').upper()
                if network == 'FOX':
                    call_sign = 'FOX'
            if not call_sign or call_sign in seen:
                continue

            supported = _SUPPORTED_BY_CALL_SIGN.get(call_sign)
            override = _CHANNEL_OVERRIDES.get(call_sign)
            target_id = supported[0] if supported else None
            fallback_name = (supported[1] if supported else None) or (override[0] if override else call_sign)
            category = (supported[2] if supported else None) or (override[1] if override else 'Entertainment')
            logo = (
                _image(first.get('networks_info', [{}])[0].get('images') if first.get('networks_info') else None, 'primary', 'secondary')
                or first.get('station_affiliate_logo')
                or _image(detail.get('images'), 'primary')
                or _image(container.get('images'), 'primary')
                or (FOX_TVE_CHANNELS[target_id].logo_url if target_id in FOX_TVE_CHANNELS else None)
            )
            gracenote = first.get('gracenote') if isinstance(first.get('gracenote'), dict) else {}
            channels.append(FoxOneChannel(
                source_channel_id=call_sign.lower(),
                name=(override[0] if override else str(first.get('network') or first.get('station_name') or fallback_name)),
                container_id=container_id,
                call_sign=call_sign,
                target_fox_tve_id=target_id,
                logo_url=logo,
                category=category,
                gracenote_id=str(gracenote.get('station_id') or '') or None,
            ))
            seen.add(call_sign)
        return channels

    def fetch_channels(self):
        return [
            ChannelData(
                source_channel_id=channel.source_channel_id,
                name=channel.name,
                slug=f'fox-one-{_slug(channel.name)}',
                logo_url=channel.logo_url,
                stream_url=channel.stream_url,
                stream_type='hls',
                category=channel.category,
                language='en',
                country='US',
                gracenote_id=channel.gracenote_id,
                guide_key=f'FOXONE:{channel.source_channel_id.upper()}',
                description='FOX One live channel. Playback uses FOX One native DTC when a refresh token is configured; overlapping channels can fall back to FOX TVE.',
            )
            for channel in self._discover_channels()
        ]

    def fetch_epg(self, channels, **kwargs):
        by_id = {channel.source_channel_id: channel for channel in self._discover_channels()}
        wanted = {ch.source_channel_id for ch in channels}
        programs: list[ProgramData] = []

        for source_channel_id in wanted:
            channel = by_id.get(source_channel_id)
            if not channel:
                continue
            uri = f'/product/curated/container/v1/live-geo/detail/{channel.container_id}?page=1&size=15'
            for _ in range(30):
                data = self._container_page(uri)
                for item in data.get('items') or []:
                    if not isinstance(item, dict):
                        continue
                    start = _parse_time(item.get('start_time'))
                    end = _parse_time(item.get('end_time'))
                    if not start or not end or end <= start:
                        continue
                    images = item.get('images') if isinstance(item.get('images'), dict) else {}
                    programs.append(ProgramData(
                        source_channel_id=source_channel_id,
                        title=item.get('title') or channel.name,
                        description=item.get('description'),
                        start_time=start,
                        end_time=end,
                        poster_url=_image(images, 'still', 'video_list', 'series_list', 'series_detail', 'dcg_mark_poster'),
                        category=_genre(item, channel.category),
                        rating=(item.get('ratings') or [{}])[0].get('rating') if item.get('ratings') else None,
                        season=item.get('season_number') or None,
                        episode=item.get('episode_number') or None,
                        original_air_date=_parse_date(item.get('first_release_date')),
                        is_live=item.get('airing_type') == 'live',
                        series_id=item.get('series_id'),
                        episode_id=item.get('entity_id') or item.get('tms_id') or item.get('foxipedia_id'),
                    ))
                next_uri = ((data.get('scroll') or {}).get('next') or '').replace('\\u0026', '&')
                if not next_uri or next_uri == uri:
                    break
                uri = next_uri
        return programs

    def _current_asset_id(self, container_id: str | None) -> str | None:
        if not container_id:
            return None
        data = self._container_page(f'/product/curated/container/v1/live-geo/detail/{container_id}?page=1&size=15')
        now = _now_utc()
        fallback = None
        for item in data.get('items') or []:
            if not isinstance(item, dict):
                continue
            asset_id = item.get('entity_id')
            if not asset_id:
                continue
            fallback = fallback or asset_id
            start = _parse_time(item.get('start_time'))
            end = _parse_time(item.get('end_time'))
            if start and end and start <= now < end:
                return asset_id
        return fallback

    def _clean_token(self, value: str | None) -> str:
        token = (value or '').strip()
        if token.lower().startswith('bearer '):
            token = token[7:].strip()
        return token

    def _ensure_device_id(self) -> str:
        device_id = (self.config.get('device_id') or '').strip()
        if not device_id:
            device_id = str(uuid.uuid4())
            self._update_config('device_id', device_id)
        return device_id

    def _token_expires_at(self) -> float:
        for key in ('access_expires_at', 'access_token_expires_at'):
            try:
                expires = float(self.config.get(key) or 0)
            except (TypeError, ValueError):
                expires = 0
            if expires > 10_000_000_000:
                expires /= 1000
            if expires:
                return expires
        return 0

    def _ensure_access_token(self) -> str:
        access_token = self._clean_token(self.config.get('access_token'))
        if access_token and self._token_expires_at() > time.time() + _TOKEN_REFRESH_SKEW:
            return access_token

        refresh_token = self._clean_token(self.config.get('refresh_token') or self.config.get('fox_one_refresh_token'))
        if not refresh_token:
            if access_token:
                return access_token
            raise ValueError('FOX One native playback requires refresh_token config.')

        r = self.session.post(
            _HYDRA_TOKEN_API,
            headers={
                'User-Agent': _UA,
                'Accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://www.fox.com',
                'Referer': 'https://www.fox.com/',
                'x-api-key': _API_KEY,
            },
            data={
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'scope': 'openid offline',
                'client_id': _HYDRA_CLIENT_ID,
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        access_token = self._clean_token(data.get('access_token'))
        if not access_token:
            raise RuntimeError('FOX One token refresh did not return access_token')

        expires_at = data.get('expires_at')
        try:
            expires_at = float(expires_at or 0)
        except (TypeError, ValueError):
            expires_at = 0
        if expires_at > 10_000_000_000:
            expires_at /= 1000
        if not expires_at:
            expires_at = time.time() + float(data.get('expires_in') or 86400)

        self._update_config('access_token', access_token)
        self._update_config('access_expires_at', expires_at)
        new_refresh = self._clean_token(data.get('refresh_token'))
        if new_refresh:
            self._update_config('refresh_token', new_refresh)
        return access_token

    def _ent_headers(self, access_token: str, *, include_device: bool = True) -> dict[str, str]:
        headers = {
            'User-Agent': _UA,
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Origin': 'https://www.fox.com',
            'Referer': 'https://www.fox.com/',
            'Authorization': f'Bearer {access_token}',
            'x-access-token': f'Bearer {access_token}',
            'x-api-key': _API_KEY,
        }
        if include_device:
            headers['x-device-xid'] = self._ensure_device_id()
        return headers

    def _metadata_header(self, data: dict, key: str) -> str | None:
        metadata = ((data.get('data') or {}).get('metadata') or data.get('metadata') or {})
        value = metadata.get(key)
        return value.strip() if isinstance(value, str) and value.strip() else None

    def _ensure_platform_location(self, access_token: str) -> str:
        configured = (self.config.get('platform_location') or '').strip()
        try:
            cached_at = float(self.config.get('platform_location_cached_at') or 0)
        except (TypeError, ValueError):
            cached_at = 0
        if configured and (not self.config.get('refresh_token') or cached_at > time.time() - _LOCATION_TTL):
            return configured

        locator = self.session.get(
            f'{_ENT_BASE}/locator/v1/location',
            headers=self._ent_headers(access_token),
            timeout=30,
        )
        locator.raise_for_status()
        locator_data = locator.json()
        platform_location = self._metadata_header(locator_data, 'x-platform-location')
        if not platform_location:
            raise RuntimeError('FOX locator response did not include x-platform-location')

        location_data = (locator_data.get('data') or {}).get('location') or {}
        home_zip = (self.config.get('home_zip_code') or location_data.get('zip_code') or '').strip()
        home_location = None
        home_headers = self._ent_headers(access_token, include_device=False)
        home = self.session.get(
            f'{_ENT_BASE}/user-preferences/v1/home-location',
            headers=home_headers,
            timeout=30,
        )
        if home.status_code == 404 and home_zip:
            home = self.session.put(
                f'{_ENT_BASE}/user-preferences/v1/home-location',
                headers=home_headers,
                json={'home_zip_code': home_zip},
                timeout=30,
            )
        if home.status_code != 404:
            home.raise_for_status()
            home_location = self._metadata_header(home.json(), 'x-ent-home-location')

        combined = ','.join(part for part in (platform_location, home_location) if part)
        self._update_config('platform_location', combined)
        self._update_config('platform_location_cached_at', time.time())
        if home_zip and not self.config.get('home_zip_code'):
            self._update_config('home_zip_code', home_zip)
        return combined

    def _dtc_headers(self) -> dict[str, str]:
        access_token = self._ensure_access_token()
        platform_location = self._ensure_platform_location(access_token)
        return {
            'User-Agent': _UA,
            'Accept': '*/*',
            'Content-Type': 'application/json',
            'Origin': 'https://www.fox.com',
            'Referer': 'https://www.fox.com/',
            'x-access-token': f'Bearer {access_token}',
            'x-api-key': _API_KEY,
            'x-device-capabilities': 'drm/widevine',
            'x-platform-location': platform_location,
        }

    def _has_native_playback_config(self) -> bool:
        return bool(
            self.config.get('refresh_token')
            or (self.config.get('access_token') and self.config.get('platform_location'))
        )

    def _resolve_dtc(self, container_id: str | None) -> str:
        asset_id = self._current_asset_id(container_id)
        if not asset_id:
            raise ValueError(f'FOX One could not determine current asset for container {container_id}')
        device_id = self._ensure_device_id()
        payload = {
            'asset': {'id': asset_id},
            'stream': {'type': 'live'},
            'device': {
                'capabilities': ['drm/widevine'],
                'model': ' WPF Device',
                'width': 1215,
                'height': 922,
                'os': 'Windows',
                'osv': '10',
            },
            'ad': {
                'did': device_id,
                'customParams': {'xid': device_id, 'isNationSku': False, 'isNationSub': False},
                'capabilities': ['ssai'],
            },
            'debug': {'traceId': ''},
            'privacy': {'us': '1YNN', 'lat': False},
        }
        r = self.session.post(_PLAYBACK_API, headers=self._dtc_headers(), json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        playback_url = ((data.get('stream') or {}).get('playbackUrl') or '').strip()
        if not playback_url:
            raise RuntimeError(f'FOX One playback response did not include playbackUrl for {asset_id}')
        return playback_url

    def _delegate(self) -> FoxTVEScraper:
        return FoxTVEScraper(config=self.config)

    def _absorb_delegate_updates(self, delegate: FoxTVEScraper) -> None:
        for key, value in getattr(delegate, '_pending_config_updates', {}).items():
            self._pending_config_updates[key] = value
        for key, value in getattr(delegate, '_pending_cache_updates', {}).items():
            self._pending_cache_updates[key] = value

    def audit_resolve(self, raw_url: str) -> str:
        _, target_id, container_id = _stream_parts(raw_url)
        if self._has_native_playback_config():
            return self._resolve_dtc(container_id)
        if not target_id or target_id not in FOX_TVE_CHANNELS:
            raise ValueError(f'FOX One stream requires native playback config: {raw_url}')
        delegate = self._delegate()
        try:
            return delegate.audit_resolve(FOX_TVE_CHANNELS[target_id].stream_url)
        finally:
            self._absorb_delegate_updates(delegate)

    def resolve(self, raw_url: str) -> str:
        _, target_id, container_id = _stream_parts(raw_url)
        if self._has_native_playback_config():
            return self._resolve_dtc(container_id)
        if not target_id or target_id not in FOX_TVE_CHANNELS:
            raise ValueError(f'FOX One stream requires native playback config: {raw_url}')
        delegate = self._delegate()
        try:
            return delegate.resolve(FOX_TVE_CHANNELS[target_id].stream_url)
        finally:
            self._absorb_delegate_updates(delegate)

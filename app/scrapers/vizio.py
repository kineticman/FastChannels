# app/scrapers/vizio.py
#
# Vizio WatchFree+ scraper for FastChannels
#
# API host: watchfreeplus-epg-prod.smartcasttv.com
# Auth:     none — guide endpoint is publicly anonymous
# Channels: /api/channels → ~425 mobile channels, clear HLS
#           Delivery mix: Amagi (~60%), Wurl, CloudFront, Frequency, Ottera
# EPG:      /api/airings/?start=<ISO>&end=<ISO>&startChannel=<id>&channelCount=<n>
#
# One channel (NFL Channel / 118NFLDigitalChannel1) requires a Bearer JWT to
# fetch a playback token; it is skipped automatically.
#
# Stream URLs contain ad-macro placeholders ({ADID}, {USPRIVACY}, etc.) that
# the app substitutes at playback time.  We substitute them here with
# privacy-neutral defaults matching the app's WatchFree+ DI config.

from __future__ import annotations

import logging
import urllib.parse
from datetime import datetime, timedelta, timezone

from .base import BaseScraper, ChannelData, ProgramData, infer_language_from_metadata
from .category_utils import category_for_channel
from ..gracenote_map import normalize_gracenote_id

logger = logging.getLogger(__name__)

_CHANNELS_URL      = 'https://watchfreeplus-epg-prod.smartcasttv.com/api/channels'
_AIRINGS_BASE_URL  = 'https://watchfreeplus-epg-prod.smartcasttv.com/api/airings/'

_EPG_LOOKAHEAD_HOURS = 24

# Privacy-neutral ad macro defaults matching the app's anonymous WatchFree DI
_DEFAULT_MACROS: dict[str, str] = {
    'ADID':          '00000000-0000-0000-0000-000000000000',
    'USPRIVACY':     '1---',
    'IFATYPE':       'aaid',
    'LMT':           '0',
    'TARGETOPT':     'False',
    'APP_NAME':      'VIZIO',
    'APP_BUNDLE':    'com.vizio.vue.launcher',
    'APP_STORE_URL': 'https://play.google.com/store/apps/details?id=com.vizio.vue.launcher',
    'DOMAIN':        'https://www.vizio.com',
    'DNT':           '0',
    'COPPA':         '0',
    'DEVICE_MAKE':   'Google',
    'WIDTH':         '1080',
    'HEIGHT':        '1920',
    'DEVICE_MODEL':  'Pixel 7',
    'APP_VERSION':   '5.0.0',
    'DEVICE_TYPE':   'mobile',
    'SKIPPABLE':     '1',
}

_UA = 'okhttp/4.12.0'


def _expand_macros(url: str, macros: dict[str, str]) -> str:
    """Substitute {KEY} ad-macro placeholders with URL-encoded values.

    Matches ChannelMapperUseCaseKt.mapChannelUrl() behavior: decode the full
    URL first, then re-encode each substituted value individually.
    """
    decoded = urllib.parse.unquote(url)
    for key, value in macros.items():
        decoded = decoded.replace('{' + key + '}', urllib.parse.quote(value, safe=''))
    return decoded


class VizioScraper(BaseScraper):
    source_name          = 'vizio'
    display_name         = 'Vizio WatchFree+'
    scrape_interval      = 360
    stream_audit_enabled = True
    config_schema        = []

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update({
            'User-Agent': _UA,
            'Accept':     'application/json',
        })

    # ── fetch_channels ────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        logger.info('[vizio] fetching channel list')
        r = self.session.get(_CHANNELS_URL, timeout=30)
        r.raise_for_status()
        channels_raw = r.json().get('channels') or []

        channels: list[ChannelData] = []
        for ch in channels_raw:
            ch_id = ch.get('channelId')
            name  = (ch.get('channelName') or '').strip()
            if not ch_id or not name:
                continue

            # Skip token-gated channels (NFL Channel and any future additions)
            if ch.get('tokenUrl'):
                logger.debug('[vizio] skipping token-gated channel %s (%s)', ch_id, name)
                continue

            urls = ch.get('channelUrls') or []
            if not urls or not isinstance(urls[0], str):
                logger.debug('[vizio] skipping %s — no channelUrl', name)
                continue

            stream_url = _expand_macros(urls[0], _DEFAULT_MACROS)
            if not stream_url.startswith('http'):
                logger.debug('[vizio] skipping %s — non-HTTP URL after macro expansion', name)
                continue

            raw_category = ch.get('category') or None
            number       = ch.get('channelNumber')

            logo = (
                ch.get('channelIcon')
                or ch.get('portraitIcon')
                or ch.get('bwIcon')
            )

            raw_tms      = ch.get('tmsStationId')
            gracenote_id = normalize_gracenote_id(raw_tms) if raw_tms and str(raw_tms) != '-1' else None

            channels.append(ChannelData(
                source_channel_id = ch_id,
                name              = name,
                stream_url        = stream_url,
                logo_url          = logo,
                category          = category_for_channel(name, raw_category),
                language          = infer_language_from_metadata(name, raw_category),
                country           = 'US',
                stream_type       = 'hls',
                number            = int(number) if number else None,
                guide_key         = ch.get('airingsKey'),
                description       = (ch.get('channelDescription') or '').strip() or None,
                gracenote_id      = gracenote_id,
            ))

        logger.info('[vizio] %d channels', len(channels))
        return channels

    # ── fetch_epg ─────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        if not channels:
            return []

        now   = datetime.now(timezone.utc)
        start = now.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        end   = (now + timedelta(hours=_EPG_LOOKAHEAD_HOURS)).strftime('%Y-%m-%dT%H:%M:%S.000Z')

        logger.info('[vizio] fetching EPG for %d channels (%s → +%dh)',
                    len(channels), start, _EPG_LOOKAHEAD_HOURS)

        # stationId in airings (e.g. "93VZFAF") matches airingsKey stored as guide_key,
        # NOT the channelId field (which is a numeric CDN ID).  Build a reverse map.
        station_map = {ch.guide_key: ch.source_channel_id for ch in channels if ch.guide_key}

        programs = self._fetch_airings_bulk(channels, start, end, station_map)
        if not programs:
            programs = self._fetch_airings_per_channel(channels, start, end, station_map)

        logger.info('[vizio] %d EPG entries', len(programs))
        return programs

    def _fetch_airings_bulk(
        self, channels: list[ChannelData], start: str, end: str,
        station_map: dict[str, str],
    ) -> list[ProgramData]:
        params = {
            'start':        start,
            'end':          end,
            'startChannel': channels[0].source_channel_id,
            'channelCount': len(channels),
        }
        try:
            r = self.session.get(_AIRINGS_BASE_URL, params=params, timeout=60)
            r.raise_for_status()
            airings = r.json().get('airings') or []
            if airings:
                logger.debug('[vizio] bulk EPG returned %d airings', len(airings))
                return self._parse_airings(airings, station_map)
        except Exception as exc:
            logger.warning('[vizio] bulk EPG fetch failed: %s', exc)
        return []

    def _fetch_airings_per_channel(
        self, channels: list[ChannelData], start: str, end: str,
        station_map: dict[str, str],
    ) -> list[ProgramData]:
        logger.info('[vizio] falling back to per-channel EPG')
        programs: list[ProgramData] = []
        for ch in channels:
            params = {
                'start':        start,
                'end':          end,
                'startChannel': ch.source_channel_id,
                'channelCount': 1,
            }
            try:
                r = self.session.get(_AIRINGS_BASE_URL, params=params, timeout=20)
                r.raise_for_status()
                airings = r.json().get('airings') or []
                programs.extend(self._parse_airings(airings, station_map))
            except Exception as exc:
                logger.warning('[vizio] EPG fetch failed for %s: %s', ch.source_channel_id, exc)
        return programs

    def _parse_airings(self, airings: list[dict], station_map: dict[str, str]) -> list[ProgramData]:
        programs: list[ProgramData] = []
        for airing in airings:
            # stationId matches airingsKey; channelId is a numeric CDN identifier
            station_id = airing.get('stationId')
            source_channel_id = station_map.get(station_id) if station_id else None
            if not source_channel_id:
                continue
            try:
                start = datetime.fromisoformat(airing['timeStart'].replace('Z', '+00:00'))
                end   = datetime.fromisoformat(airing['timeEnd'].replace('Z', '+00:00'))
            except (KeyError, ValueError, AttributeError):
                continue

            raw_rating = airing.get('rating')
            rating = (raw_rating.get('code') if isinstance(raw_rating, dict) else raw_rating) or None

            programs.append(ProgramData(
                source_channel_id = source_channel_id,
                title             = (airing.get('title') or '').strip() or 'Unknown',
                start_time        = start,
                end_time          = end,
                description       = (airing.get('description') or '').strip() or None,
                poster_url        = airing.get('airingIcon') or None,
                rating            = rating,
            ))
        return programs

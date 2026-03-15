# app/scrapers/samsung.py
#
# Samsung TV Plus scraper for FastChannels
#
# Channel metadata and EPG are sourced from Matt Huisman's public mirror:
#   https://github.com/matthuisman/samsung-tvplus-for-channels
#   https://i.mjh.nz/SamsungTVPlus/
#
# All credit for the data aggregation, channel/EPG mirroring, and stream URL
# resolution (jmp2.uk) goes to Matt Huisman (@matthuisman).  We are simply
# consuming his publicly available endpoints — please support his work.
#
#   Channels: https://i.mjh.nz/SamsungTVPlus/.channels.json.gz
#   EPG:      https://i.mjh.nz/SamsungTVPlus/{region}.xml.gz
#
# Stream URLs: https://jmp2.uk/stvp-{channel_id}
#   These redirect to Google DAI HLS streams — standard HTTP 302, clients follow fine.
#
# No auth required. Data refreshes ~hourly upstream; we scrape every 6 hours.

from __future__ import annotations

import gzip
import io
import json
import logging
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata

logger = logging.getLogger(__name__)

_CHANNELS_URL = 'https://i.mjh.nz/SamsungTVPlus/.channels.json.gz'
_EPG_URL      = 'https://i.mjh.nz/SamsungTVPlus/{region}.xml.gz'
_STREAM_URL   = 'https://jmp2.uk/stvp-{id}'

# XMLTV datetime format used by this EPG source
_XMLTV_TS_FMT = '%Y%m%d%H%M%S %z'


def _parse_xmltv_ts(ts: str) -> datetime | None:
    try:
        return datetime.strptime(ts.strip(), _XMLTV_TS_FMT)
    except (ValueError, TypeError):
        return None


class SamsungScraper(BaseScraper):
    source_name           = 'samsung'
    display_name          = 'Samsung TV Plus'
    scrape_interval       = 360   # 6 hours — upstream data refreshes ~hourly
    stream_audit_enabled  = True

    config_schema = [
        ConfigField(
            'region',
            'Region',
            field_type='text',
            required=False,
            default='us',
            placeholder='us',
            help_text='Region code to scrape: us, ca, gb, de, fr, es, it, au, kr, in, at, ch',
        ),
    ]

    # ── helpers ────────────────────────────────────────────────────────────────

    def _region(self) -> str:
        return (self.config.get('region') or 'us').lower().strip()

    def _fetch_gz_json(self, url: str) -> dict:
        r = self.session.get(url, timeout=30, headers={'User-Agent': 'okhttp/4.12.0'})
        r.raise_for_status()
        with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
            return json.load(gz)

    def _fetch_gz_xml(self, url: str) -> ET.Element:
        r = self.session.get(url, timeout=30, headers={'User-Agent': 'okhttp/4.12.0'})
        r.raise_for_status()
        with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
            return ET.parse(gz).getroot()

    # ── fetch_channels ─────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        region = self._region()
        logger.info('[samsung] fetching channel list for region=%s', region)

        data = self._fetch_gz_json(_CHANNELS_URL)
        regions = data.get('regions', {})
        region_data = regions.get(region)
        if not region_data:
            logger.warning('[samsung] region %r not found; available: %s',
                           region, list(regions.keys()))
            return []

        channels_raw = region_data.get('channels', {})
        channels: list[ChannelData] = []

        for ch_id, ch in channels_raw.items():
            # Skip DRM/licensed channels — they won't play without the license
            if ch.get('license_url'):
                continue

            name     = ch.get('name') or ch_id
            logo     = ch.get('logo')
            group    = ch.get('group') or 'Live TV'
            chno     = ch.get('chno')
            language = infer_language_from_metadata(name, group)

            channels.append(ChannelData(
                source_channel_id = ch_id,
                name              = name,
                stream_url        = _STREAM_URL.format(id=ch_id),
                logo_url          = logo,
                category          = group,
                language          = language,
                country           = region.upper(),
                stream_type       = 'hls',
                number            = int(chno) if chno else None,
            ))

        logger.info('[samsung] %d channels fetched (region=%s)', len(channels), region)
        return channels

    # ── fetch_epg ──────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        region = self._region()
        known_ids = {ch.source_channel_id for ch in channels}

        logger.info('[samsung] fetching EPG for region=%s (%d channels)', region, len(known_ids))

        try:
            root = self._fetch_gz_xml(_EPG_URL.format(region=region))
        except Exception as exc:
            logger.warning('[samsung] EPG fetch failed: %s', exc)
            return []

        programs: list[ProgramData] = []
        for prog in root.iter('programme'):
            ch_id = prog.get('channel', '')
            if ch_id not in known_ids:
                continue

            start = _parse_xmltv_ts(prog.get('start', ''))
            stop  = _parse_xmltv_ts(prog.get('stop', ''))
            if not start or not stop:
                continue

            title     = (prog.findtext('title') or '').strip() or 'Unknown'
            desc      = (prog.findtext('desc') or '').strip() or None
            rating    = (prog.findtext('rating/value') or '').strip() or None
            icon_el   = prog.find('icon')
            poster    = icon_el.get('src') if icon_el is not None else None

            programs.append(ProgramData(
                source_channel_id = ch_id,
                title             = title,
                start_time        = start,
                end_time          = stop,
                description       = desc,
                poster_url        = poster,
                rating            = rating,
            ))

        logger.info('[samsung] %d programs parsed (region=%s)', len(programs), region)
        return programs

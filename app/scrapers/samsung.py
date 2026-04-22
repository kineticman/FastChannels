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
#   These redirect through Samsung's SIS CDN to Google DAI or Akamai HLS streams.
#   resolve() follows the jmp2.uk redirect server-side so the play route can issue
#   a single-hop redirect to the final CDN URL (which has Access-Control-Allow-Origin: *),
#   avoiding a multi-hop cross-origin chain in the browser.
#
# No auth required. Data refreshes ~hourly upstream; we scrape every 6 hours.

from __future__ import annotations

import gzip
import io
import json
import logging
import re
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata

logger = logging.getLogger(__name__)

_CHANNELS_URL = 'https://i.mjh.nz/SamsungTVPlus/.channels.json.gz'
_EPG_URL      = 'https://i.mjh.nz/SamsungTVPlus/{region}.xml.gz'
_STREAM_URL   = 'https://jmp2.uk/stvp-{id}'

# XMLTV datetime format used by this EPG source
_XMLTV_TS_FMT = '%Y%m%d%H%M%S %z'
_ONSCREEN_EPISODE_RE = re.compile(r'^S(?P<season>\d+)E(?P<episode>\d+)$', re.IGNORECASE)


def _parse_xmltv_ts(ts: str) -> datetime | None:
    try:
        return datetime.strptime(ts.strip(), _XMLTV_TS_FMT)
    except (ValueError, TypeError):
        return None


def _parse_onscreen_episode_num(prog: ET.Element) -> tuple[int | None, int | None]:
    for epnum in prog.findall('episode-num'):
        if (epnum.get('system') or '').lower() != 'onscreen':
            continue
        text = (epnum.text or '').strip()
        match = _ONSCREEN_EPISODE_RE.match(text)
        if not match:
            continue
        try:
            return int(match.group('season')), int(match.group('episode'))
        except (TypeError, ValueError):
            return None, None
    return None, None


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
            help_text='One or more region codes separated by commas: us, ca, gb, de, fr, es, it, au, kr, in, at, ch',
        ),
    ]

    # ── helpers ────────────────────────────────────────────────────────────────

    def _regions(self) -> list[str]:
        """Return a list of normalised region codes from config (supports comma/pipe/space separators)."""
        import re
        raw = self.config.get('region') or 'us'
        codes = [c.strip().lower() for c in re.split(r'[,|/\s]+', raw) if c.strip()]
        return codes or ['us']

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

    # ── resolve ────────────────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        """
        Follow the jmp2.uk short-URL redirect chain server-side and return the
        final CDN URL.  This lets the play route issue a single-hop redirect to
        a CDN that already returns Access-Control-Allow-Origin: *, instead of
        sending the browser through multiple cross-origin hops (jmp2.uk →
        sis-global.prod.samsungtv.plus → akamaized.net) which can trigger CORS
        failures in hls.js.
        """
        try:
            # SIS CDN (intermediate hop) rejects HEAD → use GET with stream=True
            # so we follow all redirects and get the final URL without reading the body.
            r = self.session.get(
                raw_url,
                allow_redirects=True,
                timeout=8,
                stream=True,
                headers={'User-Agent': 'okhttp/4.12.0'},
            )
            r.close()
            final_url = r.url
            if final_url and final_url != raw_url:
                logger.debug('[samsung] resolved %s → %s', raw_url, final_url[:80])
                return final_url
        except Exception as e:
            logger.debug('[samsung] resolve follow-redirect failed (%s), using raw URL', e)
        return raw_url

    # ── fetch_channels ─────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        regions = self._regions()
        logger.info('[samsung] fetching channel list for regions=%s', regions)

        data = self._fetch_gz_json(_CHANNELS_URL)
        all_regions = data.get('regions', {})

        channels: list[ChannelData] = []
        seen_ids: set[str] = set()

        for region in regions:
            region_data = all_regions.get(region)
            if not region_data:
                logger.warning('[samsung] region %r not found; available: %s',
                               region, list(all_regions.keys()))
                continue

            channels_raw = region_data.get('channels', {})
            region_count = 0

            for ch_id, ch in channels_raw.items():
                if ch_id in seen_ids:
                    continue
                # Skip DRM/licensed channels — they won't play without the license
                if ch.get('license_url'):
                    continue

                seen_ids.add(ch_id)
                name     = ch.get('name') or ch_id
                logo     = ch.get('logo')
                group    = ch.get('group') or 'Live TV'
                chno     = ch.get('chno')
                language = infer_language_from_metadata(name, group)

                description = (ch.get('description') or '').strip() or None

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
                    description       = description,
                ))
                region_count += 1

            logger.info('[samsung] %d channels fetched (region=%s)', region_count, region)

        logger.info('[samsung] %d total channels across %d region(s)', len(channels), len(regions))
        return channels

    # ── fetch_epg ──────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        regions = self._regions()
        known_ids = {ch.source_channel_id for ch in channels}

        logger.info('[samsung] fetching EPG for regions=%s (%d channels)', regions, len(known_ids))

        programs: list[ProgramData] = []

        for region in regions:
            try:
                root = self._fetch_gz_xml(_EPG_URL.format(region=region))
            except Exception as exc:
                logger.warning('[samsung] EPG fetch failed for region=%s: %s', region, exc)
                continue

            region_count = 0
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
                episode_title = (prog.findtext('sub-title') or '').strip() or None
                season, episode = _parse_onscreen_episode_num(prog)
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
                    episode_title     = episode_title,
                    season            = season,
                    episode           = episode,
                ))
                region_count += 1

            logger.info('[samsung] %d programs parsed (region=%s)', region_count, region)

        logger.info('[samsung] %d total programs across %d region(s)', len(programs), len(regions))
        return programs

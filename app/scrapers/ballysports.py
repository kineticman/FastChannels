from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from .base import BaseScraper, ChannelData, ProgramData

logger = logging.getLogger(__name__)


class BallysScraper(BaseScraper):
    """
    Scraper for Bally Sports Live (ballysports.com).

    All channels are free / unauthenticated. Channel metadata and EPG are
    embedded in the Next.js RSC payload of any channel page.  The public CDN
    stream URL (linear{NN}.channels.ballys.tv) requires no auth tokens and is
    stable per channel, so resolve() derives it from the stored opaque URI.

    The opaque URI stores the CDN host number as it appears in the API response
    (e.g. "2", not "02") so resolve() can reconstruct it verbatim without any
    zero-padding assumptions.
    """

    source_name = "ballysports"
    display_name = "Bally Sports Live"
    scrape_interval = 360
    stream_audit_enabled = True

    config_schema = []

    # Any channel page works; the RSC payload includes all channels + EPG.
    _PAGE_URL = "https://www.ballysports.com/channels/2-milb"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/148.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._epg_cache: list[dict] = []

    # ── Required ─────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        html = self._fetch_page()
        if not html:
            return []

        channel_entries, epg_events = self._parse_page(html)
        self._epg_cache = epg_events

        channels: list[ChannelData] = []
        for entry in channel_entries:
            uuid_val = entry.get("channelUuid")
            name = entry.get("channelName")
            cdn_url = entry.get("public_cdn_url")

            if not uuid_val or not name or not cdn_url:
                continue

            m = re.match(r'https://linear(\d+)\.channels\.ballys\.tv/', cdn_url)
            if not m:
                continue
            linear_num = m.group(1)  # preserve as-is ("2", not "02")

            manifest = cdn_url.rsplit("/", 1)[-1] or "index_dvr.m3u8"

            channels.append(ChannelData(
                source_channel_id=str(uuid_val),
                name=name,
                stream_url=f"bally://channel/{linear_num}/{manifest}",
                logo_url=entry.get("logo") or None,
                category="Sports",
                number=entry.get("order"),
                country="US",
                language="en",
                stream_type="hls",
                description=entry.get("metadata_description") or None,
            ))

        logger.info("[%s] %d channels fetched", self.source_name, len(channels))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        if not self._epg_cache:
            html = self._fetch_page()
            if html:
                _, self._epg_cache = self._parse_page(html)

        if not self._epg_cache:
            return []

        valid_ids = {ch.source_channel_id for ch in channels}

        programs: list[ProgramData] = []
        for event in self._epg_cache:
            ch_id = str(event.get("channelUuid", ""))
            if ch_id not in valid_ids:
                continue

            title = event.get("title") or ""
            since = self._parse_iso(event.get("since"))
            till = self._parse_iso(event.get("till"))
            if not title or not since or not till:
                continue

            programs.append(ProgramData(
                source_channel_id=ch_id,
                title=title,
                start_time=since,
                end_time=till,
                description=event.get("comment") or None,
            ))

        logger.info("[%s] %d EPG events fetched", self.source_name, len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("bally://channel/"):
            return raw_url
        try:
            parts = raw_url[len("bally://channel/"):].split("/", 1)
            linear_num = parts[0]
            if not linear_num.isdigit():
                return raw_url
            manifest = parts[1] if len(parts) == 2 else "index_dvr.m3u8"
        except IndexError:
            return raw_url
        return f"https://linear{linear_num}.channels.ballys.tv/abr_default/{manifest}"

    # ── Internals ─────────────────────────────────────────────

    def _fetch_page(self) -> str | None:
        r = self.get(self._PAGE_URL)
        return r.text if r else None

    def _parse_page(self, html: str) -> tuple[list[dict], list[dict]]:
        """
        Extract channelData and epgData from the Next.js RSC payload embedded
        in the HTML.  The payload is in self.__next_f.push([1, "<json-string>"])
        script blocks; the JSON string uses \\$D prefix for Date values and
        \\$undefined for undefined.
        """
        import json

        rsc_str = self._find_rsc_string(html)
        if not rsc_str:
            logger.warning("[%s] RSC payload not found in page", self.source_name)
            return [], []

        # Replace RSC special values so the string is valid JSON
        rsc_clean = re.sub(r'"\$D([^"]+)"', r'"\1"', rsc_str)
        rsc_clean = re.sub(r'"\$undefined"', "null", rsc_clean)

        # The object containing channelData/epgData sits at the end of the RSC block
        obj_start = rsc_clean.find('{"channelData"')
        if obj_start == -1:
            logger.warning("[%s] channelData not found in RSC payload", self.source_name)
            return [], []

        obj_end = rsc_clean.rfind("}")
        try:
            obj = json.loads(rsc_clean[obj_start:obj_end + 1])
        except Exception as exc:
            logger.warning("[%s] Failed to parse RSC object: %s", self.source_name, exc)
            return [], []

        channel_entries = obj.get("channelData") or []
        epg_events = obj.get("epgData") or []
        return channel_entries, epg_events

    @staticmethod
    def _find_rsc_string(html: str) -> str | None:
        """
        Locate the __next_f.push([1, "..."]) call that contains channelData
        and return the decoded inner string.
        """
        import json

        for m in re.finditer(r'self\.__next_f\.push\((\[.*?\])\)', html, re.DOTALL):
            try:
                arr = json.loads(m.group(1))
            except Exception:
                continue
            if (
                isinstance(arr, list)
                and len(arr) >= 2
                and arr[0] == 1
                and isinstance(arr[1], str)
                and "channelData" in arr[1]
            ):
                return arr[1]
        return None

    @staticmethod
    def _parse_iso(val: Any) -> datetime | None:
        if not val or not isinstance(val, str):
            return None
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

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
    stream URL (<sub>.channels.ballys.tv) requires no auth tokens and is
    stable per channel, so resolve() derives it from the stored opaque URI.

    CDN subdomains vary by channel — observed prefixes include "fast01",
    "lnr02", and "linear30" — so the stored opaque URI captures the full
    subdomain plus the path verbatim.  Older DB rows stored only a numeric
    suffix ("6") and assumed a "linear{NN}" host; resolve() still handles
    those by zero-padding to two digits.
    """

    source_name = "ballysports"
    display_name = "Bally Sports Live"
    scrape_interval = 360
    stream_audit_enabled = True
    epg_quality = 'partial'   # descriptions present; no poster artwork
    source_category = 'specialty'

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

            m = re.match(r'https://([a-z0-9]+)\.channels\.ballys\.tv/(.+)$', cdn_url)
            if not m:
                continue
            subdomain = m.group(1)   # e.g. "fast02", "lnr02", "linear30"
            path = m.group(2)        # e.g. "abr_default/index_dvr.m3u8"

            channels.append(ChannelData(
                source_channel_id=str(uuid_val),
                name=name,
                stream_url=f"bally://channel/{subdomain}/{path}",
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

            is_live = event.get("is_live")
            programs.append(ProgramData(
                source_channel_id=ch_id,
                title=title,
                start_time=since,
                end_time=till,
                description=event.get("comment") or None,
                is_live=is_live if isinstance(is_live, bool) else None,
            ))

        logger.info("[%s] %d EPG events fetched", self.source_name, len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("bally://channel/"):
            return raw_url
        parts = raw_url[len("bally://channel/"):].split("/", 1)
        host = parts[0]
        path = parts[1] if len(parts) == 2 else ""

        # Legacy rows stored a bare numeric suffix and assumed a "linear{NN}"
        # host with a fixed abr_default path; the manifest sat in `path`.
        if host.isdigit():
            manifest = path or "index_dvr.m3u8"
            return f"https://linear{host.zfill(2)}.channels.ballys.tv/abr_default/{manifest}"

        path = path or "abr_default/index_dvr.m3u8"
        return f"https://{host}.channels.ballys.tv/{path}"

    # ── Internals ─────────────────────────────────────────────

    def _fetch_page(self) -> str | None:
        r = self.get(self._PAGE_URL)
        return r.text if r else None

    def _parse_page(self, html: str) -> tuple[list[dict], list[dict]]:
        """
        Extract channelData and epgData from the Next.js RSC payload embedded
        in the HTML.  The payload is spread across several
        self.__next_f.push([1, "<chunk-id>:<json-fragment>"]) script blocks —
        channelData and epgData often land in *different* chunks, and later
        chunks may only carry a back-reference (e.g. "$1c:props:...") to an
        earlier chunk instead of the real array.  So each chunk is scanned
        independently for a real `"channelData":[...]` / `"epgData":[...]`
        array rather than assuming one chunk holds a single top-level object.
        The JSON string uses a \\$D prefix for Date values and \\$undefined
        for undefined.
        """
        import json

        chunks = self._find_rsc_chunks(html)
        if not chunks:
            logger.warning("[%s] RSC payload not found in page", self.source_name)
            return [], []

        channel_entries: list[dict] = []
        epg_events: list[dict] = []
        for chunk in chunks:
            # Replace RSC special values so extracted arrays are valid JSON
            cleaned = re.sub(r'"\$D([^"]+)"', r'"\1"', chunk)
            cleaned = re.sub(r'"\$undefined"', "null", cleaned)

            if not channel_entries:
                arr = self._extract_json_array(cleaned, "channelData")
                if arr:
                    channel_entries = arr
            if not epg_events:
                arr = self._extract_json_array(cleaned, "epgData")
                if arr:
                    epg_events = arr
            if channel_entries and epg_events:
                break

        if not channel_entries:
            logger.warning("[%s] channelData not found in RSC payload", self.source_name)

        return channel_entries, epg_events

    @staticmethod
    def _find_rsc_chunks(html: str) -> list[str]:
        """
        Return the decoded inner strings of every __next_f.push([1, "..."])
        call whose payload mentions channelData or epgData.
        """
        import json

        chunks = []
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
                and ("channelData" in arr[1] or "epgData" in arr[1])
            ):
                chunks.append(arr[1])
        return chunks

    @staticmethod
    def _extract_json_array(s: str, key: str) -> list | None:
        """
        Find `"<key>":` in `s` and, if followed by a real JSON array (not a
        `"$..."` back-reference string), bracket-match and parse just that
        array. Skips past false matches (e.g. the key appearing as a
        reference value) to the next occurrence.
        """
        import json

        pattern = f'"{key}":'
        idx = 0
        while True:
            pos = s.find(pattern, idx)
            if pos == -1:
                return None

            val_start = pos + len(pattern)
            while val_start < len(s) and s[val_start] in " \t\n\r":
                val_start += 1
            if val_start >= len(s) or s[val_start] != "[":
                idx = pos + 1
                continue

            depth = 0
            in_str = False
            esc = False
            i = val_start
            while i < len(s):
                c = s[i]
                if in_str:
                    if esc:
                        esc = False
                    elif c == "\\":
                        esc = True
                    elif c == '"':
                        in_str = False
                else:
                    if c == '"':
                        in_str = True
                    elif c == "[":
                        depth += 1
                    elif c == "]":
                        depth -= 1
                        if depth == 0:
                            i += 1
                            break
                i += 1

            try:
                return json.loads(s[val_start:i])
            except Exception:
                idx = pos + 1

    @staticmethod
    def _parse_iso(val: Any) -> datetime | None:
        if not val or not isinstance(val, str):
            return None
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

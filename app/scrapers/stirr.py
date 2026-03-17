# app/scrapers/stirr.py
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from html import unescape
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter

from .base import BaseScraper, ChannelData, ConfigField, ProgramData

logger = logging.getLogger(__name__)


class StirrScraper(BaseScraper):
    source_name = "stirr"
    display_name = "STIRR"
    scrape_interval = 360

    CHANNELS_URL = (
        "https://stirr.com/api/videos/list/"
        "?categories=all_categories&content_type=4&no_limit=true"
    )
    PLAYABLE_URL_TEMPLATE = "https://stirr.com/api/v2/videos/{videoid}/playable"
    EPG_FALLBACK_URL = "https://stirr.com/api/epg"

    config_schema = []
    stream_audit_enabled = True

    CATEGORY_MAP = {
        "News Flash Live":              "News",
        "Sports Live":                  "Sports",
        "Entertainment Live":           "Entertainment",
        "Music Live":                   "Music",
        "Food and Fitness Live":        "Lifestyle",
        "Comedy Live":                  "Comedy",
        "Shopping Live":                "Shopping",
        "Default Category":             "General",
        "Crime Files":                  "Crime",
        "Documentary Series":           "Documentary",
        "STIRR Kids":                   "Kids",
        "Finance and Business":         "Business",
        "Paranormal Series":            "Entertainment",
        "Science to Space, Amplified":  "Science",
        "Pack your Bag Travel":         "Travel",
    }

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/145.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://stirr.com",
                "Referer": "https://stirr.com/",
            }
        )
        # No-retry session for EPG fetches — dead EPG hosts shouldn't burn 3 attempts each
        self._epg_session = requests.Session()
        self._epg_session.headers.update(self.session.headers)
        _no_retry = HTTPAdapter(max_retries=0)
        self._epg_session.mount("https://", _no_retry)
        self._epg_session.mount("http://", _no_retry)

    # ── Required ─────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        r = self.get(self.CHANNELS_URL)
        if not r:
            return []

        try:
            payload = r.json()
        except Exception as exc:
            logger.error("[%s] Invalid channel JSON: %s", self.source_name, exc)
            return []

        rows = self._extract_channel_rows(payload)
        channels: list[ChannelData] = []

        for row in rows:
            source_channel_id = self._pick_source_channel_id(row)
            if not source_channel_id:
                continue

            name = self._normalize_name(
                row.get("title")
                or row.get("name")
                or row.get("channel_name")
                or f"STIRR {source_channel_id}"
            )

            # Bundle metadata into a custom stirr:// URI for the resolve phase
            stream_url = self._build_stirr_uri(
                source_channel_id=source_channel_id,
                epg_url=self._sanitize_url(row.get("epg_url") or ""),
                epg_channel_id=row.get("epg_channel_id") or "",
            )

            channels.append(
                ChannelData(
                    source_channel_id=source_channel_id,
                    name=name,
                    stream_url=stream_url,
                    logo_url=self._pick_logo(row),
                    category=self._pick_category(row),
                    number=self._coerce_int(row.get("channel_number")),
                    country="US",
                    language="en",
                    stream_type="hls",
                )
            )

        logger.info("[%s] %d channels fetched", self.source_name, len(channels))
        return channels

    # ── Optional ─────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        def _fetch_one(ch: ChannelData) -> list[ProgramData]:
            meta = self._parse_stirr_uri(ch.stream_url)
            epg_url = self._sanitize_url(meta.get("epg_url") or "")
            epg_channel_id = meta.get("epg_channel_id") or ""
            result: list[ProgramData] = []

            # 1. Try Provider EPG URL (no-retry, short timeout — dead hosts fail fast)
            if epg_url:
                try:
                    r = self._epg_session.get(epg_url, timeout=8)
                    r.raise_for_status()
                    if r:
                        text = r.text.strip()
                        if text.startswith("<"):
                            result.extend(self._extract_programs_from_xmltv(
                                ch.source_channel_id, text, epg_channel_id
                            ))
                        else:
                            payload = r.json()
                            if isinstance(payload, dict) and "schedules" in payload:
                                result.extend(self._extract_programs_from_wurl(
                                    ch.source_channel_id, payload
                                ))
                            else:
                                for entry in self._extract_generic_json_programs(payload):
                                    prog = self._program_from_entry(ch.source_channel_id, entry)
                                    if prog:
                                        result.append(prog)
                except Exception as exc:
                    logger.debug("[%s] Provider EPG failed for %s: %s", self.source_name, ch.source_channel_id, exc)

            # 2. Fallback to STIRR EPG endpoint (use session directly — 400s are
            #    expected for unsupported video IDs and shouldn't log as ERROR)
            if not result:
                try:
                    fallback_url = f"{self.EPG_FALLBACK_URL}?channel_id={ch.source_channel_id}&tz=UTC"
                    r = self._epg_session.get(fallback_url, timeout=10)
                    if r.status_code == 200:
                        for entry in self._extract_generic_json_programs(r.json()):
                            prog = self._program_from_entry(ch.source_channel_id, entry)
                            if prog:
                                result.append(prog)
                    else:
                        logger.debug("[%s] Fallback EPG %s for %s", self.source_name, r.status_code, ch.source_channel_id)
                except Exception as exc:
                    logger.debug("[%s] Fallback EPG failed for %s: %s", self.source_name, ch.source_channel_id, exc)

            logger.debug("[%s] %d EPG rows for %s", self.source_name, len(result), ch.source_channel_id)
            return result

        programs: list[ProgramData] = []
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(_fetch_one, ch): ch for ch in channels}
            try:
                for future in as_completed(futures, timeout=300):
                    try:
                        programs.extend(future.result())
                    except Exception as exc:
                        ch = futures[future]
                        logger.debug("[%s] EPG worker error for %s: %s", self.source_name, ch.source_channel_id, exc)
            except FuturesTimeoutError:
                incomplete = sum(1 for f in futures if not f.done())
                logger.warning("[%s] EPG fetch timed out after 300s; %d channel(s) incomplete", self.source_name, incomplete)

        logger.info("[%s] %d total programs fetched", self.source_name, len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("stirr://"):
            return raw_url

        meta = self._parse_stirr_uri(raw_url)
        videoid = meta.get("source_channel_id")
        if not videoid:
            return raw_url

        playable_url = self.PLAYABLE_URL_TEMPLATE.format(videoid=videoid)
        
        # STIRR playable endpoint requires POST
        try:
            r = self.session.post(playable_url, timeout=10)
            r.raise_for_status()
            payload = r.json()
        except Exception as exc:
            logger.error("[%s] Resolution failed for %s: %s", self.source_name, videoid, exc)
            return raw_url

        media_url = self._extract_media_url_from_payload(payload)
        if not media_url:
            return raw_url

        # Handle HLS master to variant resolution
        try:
            rr = self.session.get(media_url, timeout=10)
            rr.raise_for_status()
            if "#EXT-X-STREAM-INF" in rr.text:
                best = self._pick_best_variant(media_url, rr.text)
                return best or media_url
        except Exception as exc:
            logger.debug("[%s] HLS variant resolution failed for %s: %s", self.source_name, videoid, exc)

        return media_url

    # ── Internal Helpers ─────────────────────────────────────

    def _extract_channel_rows(self, payload: Any) -> list[dict]:
        rows: list[dict] = []
        def walk(obj: Any):
            if isinstance(obj, dict):
                if "videoid" in obj and "title" in obj:
                    rows.append(obj)
                    return
                for v in obj.values(): walk(v)
            elif isinstance(obj, list):
                for item in obj: walk(item)
        walk(payload)
        
        # Dedup by videoid
        deduped = []
        seen = set()
        for row in rows:
            vid = str(row.get("videoid", ""))
            if vid and vid not in seen:
                seen.add(vid)
                deduped.append(row)
        return deduped

    def _pick_source_channel_id(self, row: dict) -> str | None:
        for key in ("videoid", "id", "channel_id"):
            val = row.get(key)
            if val: return str(val).strip()
        return None

    def _pick_logo(self, row: dict) -> str | None:
        if str(row.get("logo", "")).startswith("http"):
            return row["logo"]
        for b in ("thumbs", "square_thumbs"):
            bucket = row.get(b)
            if isinstance(bucket, dict):
                for v in bucket.values():
                    if str(v).startswith("http"): return v
        return None

    def _pick_category(self, row: dict) -> str | None:
        cats = row.get("categories")
        if isinstance(cats, list) and cats:
            first = cats[0]
            if isinstance(first, dict):
                raw = first.get("category_name") or first.get("name")
            else:
                raw = str(first)
        else:
            raw = row.get("category") or row.get("genre")
        return self.CATEGORY_MAP.get(raw, raw) if raw else None

    def _normalize_name(self, name: str) -> str:
        """Clean up messy local-news style channel names from Stirr's API."""
        # Strip outer whitespace including tabs and non-breaking spaces
        name = name.strip().strip("\t\u00a0")
        # Remove trailing asterisks used on state abbreviations
        name = name.replace("*", "")
        # Collapse runs of spaces
        name = re.sub(r"  +", " ", name)
        # Normalize dash spacing: ensure " - " around dashes between words
        name = re.sub(r"\s*-\s*\(", " - (", name)
        name = re.sub(r"(\w)-\s{2,}", r"\1 - ", name)
        name = self._normalize_local_news_name(name)
        return name.strip()

    def _normalize_local_news_name(self, name: str) -> str:
        """
        Simplify local-news channel names:
          'FOX 9 - WTOV - (Steubenville, OH)'  →  'FOX 9 Steubenville OH'
          'ABC 5 - KSTP - (Minneapolis-St. Paul, MN)'  →  'ABC 5 Minneapolis-St. Paul MN'
        Only applied when the name contains the '- (Location)' pattern.
        """
        if ' - (' not in name:
            return name

        m = re.search(r'\(([^)]+)\)\s*(?:#(\d+))?\s*$', name)
        if not m:
            return name

        location_raw = m.group(1)
        number_suffix = m.group(2)

        # Only transform geographic locations: comma (City, ST), ampersand, or trailing 2-letter code
        if ',' not in location_raw and '&' not in location_raw \
                and not re.search(r'\b[A-Z]{2}$', location_raw):
            return name

        # Clean location: strip commas, collapse spaces
        location = re.sub(r'\s+', ' ', location_raw.replace(',', '')).strip()

        # Prefix = everything before '(', strip trailing dashes/spaces
        prefix = name[:m.start()].rstrip(' -').strip()

        # Remove trailing standalone callsign: last ' - WXYZ' segment (2–5 all-caps)
        parts = re.split(r'\s+-\s+', prefix)
        if len(parts) > 1 and re.fullmatch(r'[A-Z]{2,5}', parts[-1]):
            parts = parts[:-1]
        prefix = ' - '.join(parts).strip()

        result = f"{prefix} {location}"
        if number_suffix:
            result += f" {number_suffix}"
        return result.strip()

    def _coerce_int(self, val: Any) -> int | None:
        try: return int(val)
        except (TypeError, ValueError): return None

    # File extensions that indicate a value is a filename, not a URL hostname
    _FILE_EXTS = frozenset(("xml", "json", "m3u8", "m3u", "csv", "txt", "zip"))

    def _sanitize_url(self, url: str) -> str:
        """Strip whitespace, decode HTML entities, add https:// if scheme missing.
        Returns empty string if the URL doesn't look like a real HTTP URL."""
        url = unescape(url.strip().rstrip("\u00a0").strip())
        if not url:
            return url
        parsed = urlparse(url)
        if not parsed.scheme:
            url = "https://" + url
            parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return ""
        netloc = parsed.netloc.lower().split(":")[0]  # strip port
        if not netloc or "." not in netloc:
            return ""
        # Reject if the netloc looks like a filename (e.g. "Foo_Bar_2024.xml")
        tld = netloc.rsplit(".", 1)[-1]
        if tld in self._FILE_EXTS:
            return ""
        return url

    def _build_stirr_uri(self, **kwargs) -> str:
        return f"stirr://channel?{urlencode(kwargs)}"

    def _parse_stirr_uri(self, uri: str) -> dict:
        parsed = urlparse(uri)
        return dict(parse_qsl(parsed.query))

    def _extract_media_url_from_payload(self, payload: Any) -> str | None:
        # Expected structure: data[0].media[0]
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list) and data:
                media = data[0].get("media")
                if isinstance(media, list) and media:
                    return media[0]
                elif isinstance(media, str):
                    return media
        return None

    def _pick_best_variant(self, master_url: str, text: str) -> str | None:
        best_bw = -1
        best_uri = None
        lines = text.splitlines()
        for i, line in enumerate(lines):
            if line.startswith("#EXT-X-STREAM-INF"):
                match = re.search(r"BANDWIDTH=(\d+)", line)
                bw = int(match.group(1)) if match else 0
                j = i + 1
                while j < len(lines) and lines[j].startswith("#"): j += 1
                if j < len(lines):
                    uri = urljoin(master_url, lines[j].strip())
                    if bw > best_bw:
                        best_bw, best_uri = bw, uri
        return best_uri

    def _extract_programs_from_wurl(self, channel_id: str, payload: dict) -> list[ProgramData]:
        movies = {str(m["id"]): m for m in payload.get("movies", []) if "id" in m}
        shorts = {str(s["id"]): s for s in payload.get("shortFormVideos", []) if "id" in s}
        specials = {str(t["id"]): t for t in payload.get("tvSpecials", []) if "id" in t}
        
        results = []
        for sched in payload.get("schedules", []):
            sid = str(sched.get("id", ""))
            start = self._parse_dt(sched.get("startDateTime"))
            dur = sched.get("durSecs")
            if not sid or not start or not dur: continue
            
            item = movies.get(sid) or shorts.get(sid) or specials.get(sid)
            if not item: continue
            
            title = item.get("title", {}).get("value") or item.get("name")
            if not title: continue
            
            results.append(ProgramData(
                source_channel_id = channel_id,
                title             = title,
                start_time        = start,
                end_time          = start + timedelta(seconds=int(dur)),
                description       = item.get("description", {}).get("value"),
                poster_url        = (item.get("thumbnails") or [{}])[0].get("url"),
                category          = (item.get("genres") or [{}])[0].get("description"),
            ))
        return results

    def _extract_programs_from_xmltv(self, channel_id: str, xml_text: str, epg_id: str) -> list[ProgramData]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.debug("[%s] XMLTV parse error for %s: %s", self.source_name, channel_id, exc)
            return []

        programmes = root.findall(".//programme")
        if not programmes:
            return []

        # Build case-insensitive target set
        targets = {channel_id.lower(), epg_id.lower()} if epg_id else {channel_id.lower()}

        def _matches(p):
            return p.attrib.get("channel", "").lower() in targets

        matched = [p for p in programmes if _matches(p)]

        # Fallback: if feed has exactly one channel and nothing matched by ID,
        # assume it's a single-channel feed and take all programmes
        if not matched:
            xml_channels = root.findall(".//channel")
            if len(xml_channels) <= 1:
                matched = programmes

        results = []
        for p in matched:
            start = self._parse_xmltv_dt(p.attrib.get("start"))
            stop  = self._parse_xmltv_dt(p.attrib.get("stop"))
            title_elem = p.find("title")
            if not start or not stop or title_elem is None: continue

            results.append(ProgramData(
                source_channel_id = channel_id,
                title             = title_elem.text or "Unknown",
                start_time        = start,
                end_time          = stop,
                description       = (p.find("desc").text if p.find("desc") is not None else None),
                poster_url        = (p.find("icon").attrib.get("src") if p.find("icon") is not None else None),
                category          = (p.find("category").text if p.find("category") is not None else None),
            ))
        return results

    def _extract_generic_json_programs(self, payload: Any) -> list[dict]:
        rows = []
        def walk(obj: Any):
            if isinstance(obj, dict):
                if any(k in obj for k in ("start", "start_time", "starts_at")):
                    rows.append(obj)
                    return  # don't recurse into matched entry
                for v in obj.values(): walk(v)
            elif isinstance(obj, list):
                for item in obj: walk(item)
        walk(payload)
        return rows

    def _program_from_entry(self, channel_id: str, entry: dict) -> ProgramData | None:
        title = entry.get("title") or entry.get("program_title") or entry.get("name")
        start = self._parse_dt(entry.get("start") or entry.get("start_time") or entry.get("airing_start_time"))
        end   = self._parse_dt(entry.get("end") or entry.get("end_time") or entry.get("airing_end_time"))
        if not title or not start or not end: return None
        
        return ProgramData(
            source_channel_id = channel_id,
            title             = title,
            start_time        = start,
            end_time          = end,
            description       = entry.get("description") or entry.get("summary"),
            poster_url        = entry.get("poster") or entry.get("image"),
            category          = entry.get("category") or entry.get("genre"),
            rating            = entry.get("rating"),
            episode_title     = entry.get("episode_title") or entry.get("subtitle"),
            season            = self._coerce_int(entry.get("season")),
            episode           = self._coerce_int(entry.get("episode")),
        )

    def _parse_dt(self, val: Any) -> datetime | None:
        if not val: return None
        if isinstance(val, (int, float)):
            if val > 2e12: val /= 1000
            return datetime.fromtimestamp(val, tz=timezone.utc)
        if isinstance(val, str):
            try:
                dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                return dt.replace(tzinfo=timezone.utc) if not dt.tzinfo else dt.astimezone(timezone.utc)
            except ValueError:
                pass
        return None

    def _parse_xmltv_dt(self, val: str | None) -> datetime | None:
        if not val: return None
        s = val.strip()
        try:
            return datetime.strptime(s[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return None

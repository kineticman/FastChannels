# app/scrapers/plex.py
#
# Plex — FAST live TV scraper
#
# Auth flow (fully anonymous, no credentials):
#   1. GET https://watch.plex.tv/                         → session cookies
#   2. POST https://plex.tv/api/v2/users/anonymous        → authToken (anon JWT)
#   3. GET https://watch.plex.tv/live-tv?_rsc=<rand>      → RSC text blob
#      contains all channel metadata + current/next EPG airings
#   4. resolve(): POST epg.provider.plex.tv/channels/{id}/tune   (best-effort)
#                 GET  epg.provider.plex.tv/library/parts/{id}.m3u8?X-Plex-Token=…
#                 → follows 302 redirect to AWS MediaTailor stream URL
#
# stream_url stored as: plex://{channel_id}
# UUID identifiers generated once and persisted in source.config for consistency.

from __future__ import annotations

import json
import logging
import random
import re as _re
import subprocess
import string
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus
from uuid import uuid4

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base import BaseScraper, ChannelData, ProgramData, StreamDeadError, format_http_reason, infer_language_from_metadata
from .category_utils import infer_category_from_name, category_for_channel as _category_for_channel
from ..gracenote_map import resolve_gracenote

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)
_PRODUCT  = "Plex Mediaverse"
_EPG_HOST = "https://epg.provider.plex.tv"
_PLEX_EXTRA_DAYS = 4
_PLEX_GUIDE_WORKERS = 6
_PLEX_LUMA_WORKERS = 4

# Plex channel IDs arrive as "{server_id}-{channel_id}" (both 24-char hex).
# The server prefix rotates when Plex migrates infrastructure; the channel
# part (same as gridKey) is stable.  Always normalise to the channel part.
_PLEX_COMPOUND_ID_RE = _re.compile(r'^[0-9a-f]{24}-([0-9a-f]{24})$')

# Encoded Next.js router state expected by watch.plex.tv RSC endpoint
_NEXT_ROUTER_STATE_TREE = (
    "%5B%22%22%2C%7B%22children%22%3A%5B%5B%22locale%22%2C%22en%22%2C%22d%22%5D%2C"
    "%7B%22children%22%3A%5B%22(shell)%22%2C%7B%22children%22%3A%5B%22(home)%22%2C"
    "%7B%22children%22%3A%5B%22__PAGE__%22%2C%7B%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%5D%7D"
    "%2Cnull%2Cnull%5D%2C%22modal%22%3A%5B%22(slot)%22%2C%7B%22children%22%3A%5B%22__PAGE__"
    "%22%2C%7B%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%2Ctrue%5D"
)

# JSON anchor strings that appear before channel-list objects in the RSC blob
_RSC_ANCHORS = ('{"categories":[', '{"channel":{', '{"channels":[')
_CHANNEL_KEYS = {"id", "slug", "title", "thumb"}

# Map Plex category slugs → normalized category labels.
# "featured" is an editorial pick list, not a genre — excluded.
# "en-espanol" / "international" indicate language; handled separately.
_PLEX_CATEGORY_MAP = {
    "entertainment":    "Entertainment",
    "drama":            "Drama",
    "movies":           "Movies",
    "crime":            "True Crime",
    "news":             "News",
    "sports":           "Sports",
    "reality":          "Reality TV",
    "classic-tv":       "Classics",
    "action":           "Action",
    "thriller":         "Thriller",
    "comedy":           "Comedy",
    "daytime-tv":       "Entertainment",
    "game-show":        "Game Shows",
    "nature-travel":    "Nature",
    "history-science":  "History",
    "food-home":        "Food",
    "lifestyle":        "Lifestyle",
    "kids-family":      "Kids",
    "en-espanol":       "En Español",
    "international":    "International",
    "gaming-anime":     "Anime",
    "music":            "Music",
}


# ── RSC parsing helpers ────────────────────────────────────────────────────────

def _find_json_end(text: str, start: int) -> int | None:
    """Return index just past the closing `}` for the JSON object at `start`."""
    depth = 0
    in_str = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i + 1
    return None


def _extract_rsc_objects(text: str) -> list[dict]:
    """Pull all balanced JSON objects that begin with one of _RSC_ANCHORS."""
    results = []
    for anchor in _RSC_ANCHORS:
        pos = 0
        while True:
            start = text.find(anchor, pos)
            if start == -1:
                break
            end = _find_json_end(text, start)
            pos = start + len(anchor)
            if end is None:
                continue
            try:
                obj = json.loads(text[start:end])
                if isinstance(obj, dict):
                    results.append(obj)
            except json.JSONDecodeError:
                pass
    return results


def _walk(node: Any):
    """Depth-first walk over nested dicts/lists."""
    stack = [node]
    while stack:
        cur = stack.pop()
        yield cur
        if isinstance(cur, dict):
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)


def _parse_ts(value) -> datetime | None:
    """Parse epoch int, float, or ISO-8601 string to a UTC datetime."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        raw = str(value).strip()
        if raw.isdigit():
            return datetime.fromtimestamp(int(raw), tz=timezone.utc)
        if raw.replace(".", "", 1).isdigit() and raw.count(".") <= 1:
            return datetime.fromtimestamp(float(raw), tz=timezone.utc)
        return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


_PLEX_EP_ONLY  = _re.compile(r'^Episode\s+\d+$', _re.IGNORECASE)
_PLEX_EP_COLON = _re.compile(r'^Episode\s+\d+\s*:\s*(.+)$', _re.IGNORECASE)


def _clean_ep_title(raw: str | None) -> str | None:
    """Drop or fix generic Plex episode titles like 'Episode 3' or 'Episode 1 : Real Name'."""
    if not raw:
        return None
    t = raw.strip()
    if not t or t in ('.', '-', '_'):
        return None
    m = _PLEX_EP_COLON.match(t)
    if m:
        return m.group(1).strip() or None
    if _PLEX_EP_ONLY.match(t):
        return None
    return t


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        if "T" in raw:
            raw = raw.split("T", 1)[0]
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None


def _normalize_plex_id(raw: str | None) -> str | None:
    """Strip the rotating server prefix from a compound Plex channel ID."""
    if not raw:
        return None
    m = _PLEX_COMPOUND_ID_RE.match(raw)
    return m.group(1) if m else raw


def _rand_rsc() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=5))


def _build_category_map(rsc_objects: list[dict]) -> tuple[dict[str, str], set[str], dict[str, list[str]]]:
    """
    Parse the categories list from RSC objects.
    Returns:
      cat_map   — channel_id → primary category label
      spanish   — set of channel_ids in the en-espanol category (language hint)
      tags_map  — channel_id → list of all category labels (for display)
    """
    cat_map: dict[str, str] = {}
    spanish: set[str] = set()
    tags_map: dict[str, list[str]] = {}

    for obj in rsc_objects:
        cats = obj.get("categories")
        if not isinstance(cats, list) or not cats:
            continue
        for cat in cats:
            slug = cat.get("slug", "")
            for ch in (cat.get("channels") or []):
                cid = ch.get("id")
                if not cid:
                    continue
                if slug == "en-espanol":
                    spanish.add(cid)
                    label = "En Español"
                    if cid not in cat_map:
                        cat_map[cid] = label
                elif slug != "featured":
                    label = _PLEX_CATEGORY_MAP.get(slug)
                    if label:
                        if cid not in cat_map:
                            cat_map[cid] = label
                        if label not in tags_map.get(cid, []):
                            tags_map.setdefault(cid, []).append(label)
        break  # categories list only appears once

    return cat_map, spanish, tags_map


def _parse_luma_fragment(content: str, source_channel_id: str) -> list[ProgramData]:
    """Parse a luma.plex.tv RSC text/x-component payload into ProgramData objects.

    Response format (as of May 2026):
      [{"id":"<24-hex>","title":"...","subtitle":"7:00 PM - 8:00 PM",
        ["badge":{...},] "data":{"guid":"plex://...","beginsAt":...,"endsAt":...},
        "previewData":{...}}, ...]
    The top-level "subtitle" is a time-range display string, not the episode title.
    Episode title lives in previewData.subtitle as before.
    """
    programs: list[ProgramData] = []
    # Match airing objects by their 24-char hex id — robust to new fields inserted
    # between "title" and "data" (subtitle, badge, etc.).
    for m in _re.finditer(r'\{"id":"[a-f0-9]{24}","title":', content):
        pos = m.start()
        depth = 0
        for i in range(pos, min(pos + 8000, len(content))):
            c = content[i]
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    try:
                        obj = json.loads(content[pos:i + 1])
                    except json.JSONDecodeError:
                        break
                    data    = obj.get("data", {})
                    preview = obj.get("previewData", {})
                    begins_at = data.get("beginsAt")
                    ends_at   = data.get("endsAt")
                    if not begins_at or not ends_at:
                        break
                    start_dt = _parse_ts(begins_at)
                    end_dt   = _parse_ts(ends_at)
                    if not start_dt or not end_dt:
                        break
                    title    = preview.get("title") or obj.get("title") or "Unknown"
                    # previewData.subtitle is the episode title;
                    # obj["subtitle"] is a time-range string ("7:00 PM - 8:00 PM") — skip it
                    ep_title = preview.get("subtitle") or None
                    if ep_title and _re.match(r'^\d+:\d+ [AP]M', ep_title):
                        ep_title = None
                    summary = preview.get("summary") or None
                    poster  = (preview.get("poster") or {}).get("image", {}).get("url") or None
                    rating  = None
                    for badge in (preview.get("badges") or []):
                        if badge.get("_component") == "Badge":
                            rating = badge.get("label")
                            break
                    season = episode = None
                    for fact in (preview.get("facts") or []):
                        fact_m = _re.match(r'S(\d+)\s*[·•]\s*E(\d+)', fact.get("content", ""))
                        if fact_m:
                            season  = int(fact_m.group(1))
                            episode = int(fact_m.group(2))
                            break
                    # guid encodes media type: plex://episode/... or plex://movie/...
                    # Only trust it when there's real program-specific metadata; gap-fill
                    # filler slots (title == channel name, no ep_title, no S/E) use a
                    # generic guid that misidentifies stand-up specials etc. as movies.
                    guid = data.get("guid") or ""
                    guid_type = guid.split("://")[1].split("/")[0] if "://" in guid else None
                    if guid_type in ("movie", "episode") and (ep_title or season or episode):
                        program_type = guid_type
                    else:
                        program_type = None
                    programs.append(ProgramData(
                        source_channel_id = source_channel_id,
                        title             = title,
                        episode_title     = ep_title,
                        description       = summary,
                        start_time        = start_dt,
                        end_time          = end_dt,
                        poster_url        = poster,
                        rating            = rating,
                        season            = season,
                        episode           = episode,
                        program_type      = program_type,
                    ))
                    break
    return programs


def _find_gaps(
    programs: list[ProgramData],
    horizon_hours: int = 24,
) -> list[tuple[datetime, datetime]]:
    """Return (start, end) gap windows > 30 min within the next horizon_hours."""
    now     = datetime.now(tz=timezone.utc)
    horizon = now + timedelta(hours=horizon_hours)
    windows = sorted(
        (max(p.start_time, now), min(p.end_time, horizon))
        for p in programs
        if p.end_time > now and p.start_time < horizon
    )
    gaps: list[tuple[datetime, datetime]] = []
    cursor = now
    for s, e in windows:
        if s - cursor > timedelta(minutes=30):
            gaps.append((cursor, s))
        cursor = max(cursor, e)
    if horizon - cursor > timedelta(minutes=30):
        gaps.append((cursor, horizon))
    return gaps


def _merge_luma_into_gaps(
    existing: list[ProgramData],
    luma_programs: list[ProgramData],
) -> list[ProgramData]:
    """Return luma airings that don't overlap any existing program."""
    covered = [(p.start_time, p.end_time) for p in existing]
    return [
        lp for lp in luma_programs
        if not any(lp.start_time < e and lp.end_time > s for s, e in covered)
    ]


# ── Scraper ────────────────────────────────────────────────────────────────────

class PlexScraper(BaseScraper):

    source_name           = "plex"
    display_name          = "Plex"
    scrape_interval       = 180  # Multi-day guide horizon; 3h cadence is sufficient
    channel_refresh_hours = 0    # fetch channel list every run — cheap (one grid call + genre maps); avoids channel-list staleness between refreshes
    stream_audit_enabled  = True

    # Fully anonymous — no user credentials required
    config_schema = []

    def __init__(self, config: dict = None):
        super().__init__(config)

        # Stable UUIDs — generated once and persisted so Plex recognises the
        # same "client" across runs (helps with token caching on their end).
        self._client_id = self.config.get("client_id") or str(uuid4())
        self._session_id = self.config.get("session_id") or str(uuid4())
        self._psid = self.config.get("playback_session_id") or str(uuid4())
        self._pid  = self.config.get("playback_id") or str(uuid4())
        self._auth_token: str | None = self.config.get("auth_token")

        if not self.config.get("client_id"):
            self._update_config("client_id",            self._client_id)
            self._update_config("session_id",           self._session_id)
            self._update_config("playback_session_id",  self._psid)
            self._update_config("playback_id",          self._pid)

        self.session.headers.update({
            "User-Agent":                 _UA,
            "Accept-Encoding":            "gzip, deflate",
            "Origin":                     "https://watch.plex.tv",
            "Referer":                    "https://watch.plex.tv/",
            "X-Plex-Client-Identifier":   self._client_id,
            "X-Plex-Device":              "Linux",
            "X-Plex-Language":            "en",
            "X-Plex-Platform":            "Chrome",
            "X-Plex-Platform-Version":    "145.0.0.0",
            "X-Plex-Playback-Session-Id": self._psid,
            "X-Plex-Product":             _PRODUCT,
            "X-Plex-Provider-Version":    "6.5.0",
            "X-Plex-Session-Id":          self._session_id,
        })

        self._rsc_cache: str | None = None  # reused within a single scrape run

    # ── Auth ───────────────────────────────────────────────────────────────────

    def pre_run_setup(self) -> None:
        """Acquire anonymous token early so it can be persisted before EPG."""
        self._ensure_auth()

    def _ensure_auth(self, force: bool = False) -> bool:
        if self._auth_token and not force:
            return True
        t0 = time.monotonic()
        try:
            # Seed watch.plex.tv session cookies — required for anonymous auth to succeed
            self.session.get("https://watch.plex.tv/", timeout=15)
        except Exception as exc:
            logger.warning("[plex] watch.plex.tv cookie seed failed: %s", exc)
        try:
            r = self.session.post(
                "https://plex.tv/api/v2/users/anonymous",
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                data=b"",
                timeout=15,
            )
            r.raise_for_status()
            self._auth_token = r.json()["authToken"]
            self._update_config("auth_token", self._auth_token)
            logger.info(
                "[plex] anonymous auth OK in %.1fs, token=%s…",
                time.monotonic() - t0,
                self._auth_token[:8],
            )
            return True
        except Exception as exc:
            logger.warning("[plex] anonymous auth failed (%s) — provider API calls may fail", exc)
            return False

    # ── RSC fetch (shared by channels + EPG) ───────────────────────────────────

    def _fetch_rsc(self) -> str:
        if self._rsc_cache:
            return self._rsc_cache
        # Seed watch.plex.tv session cookies — required for RSC endpoint.
        # Do NOT call _ensure_auth() here: the anonymous auth POST to plex.tv
        # sets cross-domain cookies that cause the RSC endpoint to return 500.
        try:
            self.session.get("https://watch.plex.tv/", timeout=15)
        except Exception as exc:
            logger.warning("[plex] watch.plex.tv cookie seed failed: %s", exc)
        t0 = time.monotonic()
        r = self.session.get(
            f"https://watch.plex.tv/live-tv?_rsc={_rand_rsc()}",
            headers={
                "Accept":    "*/*",
                "RSC":       "1",
                "Next-Url":  "/en",
            },
            timeout=30,
        )
        if r.status_code != 200:
            logger.error("[plex] live-tv RSC returned %d", r.status_code)
            return ""
        self._rsc_cache = r.text
        logger.info(
            "[plex] live-tv RSC fetched in %.1fs (%d bytes)",
            time.monotonic() - t0,
            len(self._rsc_cache),
        )
        return self._rsc_cache

    def _provider_headers(self, *, accept: str = "application/json", provider_version: str | None = None) -> dict[str, str]:
        headers = {
            "Accept":                   accept,
            "X-Plex-Token":             self._auth_token,
            "X-Plex-Client-Identifier": self._client_id,
            "X-Plex-Product":           _PRODUCT,
            "X-Plex-Platform":          "Chrome",
            "X-Plex-Platform-Version":  "145.0.0.0",
        }
        if provider_version:
            headers["X-Plex-Provider-Version"] = provider_version
            headers["X-Plex-Version"] = "4.145.1"
        return headers

    def _fetch_genre_maps(self) -> tuple[dict[str, str], dict[str, list[str]], set[str]]:
        """
        Fetch per-genre channel lists to build category/tag/language metadata.
        Returns:
          cat_map     — channel_id → primary category label
          tags_map    — channel_id → list of all category labels
          spanish_ids — channel_ids in the 'en-espanol' genre bucket
        Returns empty collections if the genre endpoint is unavailable.
        """
        cat_map:     dict[str, str]       = {}
        tags_map:    dict[str, list[str]] = {}
        spanish_ids: set[str]             = set()

        try:
            root = self.session.get(
                f"{_EPG_HOST}/",
                params={"X-Plex-Token": self._auth_token},
                headers=self._provider_headers(),
                timeout=30,
            )
            root.raise_for_status()
        except Exception as exc:
            logger.warning("[plex] genre root fetch failed: %s", exc)
            return cat_map, tags_map, spanish_ids

        genre_slugs: list[str] = []
        for elem in root.json().get("MediaProvider", {}).get("Feature", []):
            if "GridChannelFilter" in elem:
                genre_slugs = [
                    g.get("identifier")
                    for g in elem.get("GridChannelFilter") or []
                    if g.get("identifier")
                ]
                break

        if not genre_slugs:
            return cat_map, tags_map, spanish_ids

        headers = self._provider_headers()
        for genre_slug in genre_slugs:
            label = _PLEX_CATEGORY_MAP.get(genre_slug)
            try:
                r = self.session.get(
                    f"{_EPG_HOST}/lineups/plex/channels",
                    params={"genre": genre_slug, "X-Plex-Token": self._auth_token},
                    headers=headers,
                    timeout=30,
                )
                r.raise_for_status()
            except Exception as exc:
                logger.warning("[plex] genre fetch failed for %s: %s", genre_slug, exc)
                continue

            for channel in r.json().get("MediaContainer", {}).get("Channel", []):
                cid = channel.get("id")
                if not cid:
                    continue
                if genre_slug == "en-espanol":
                    spanish_ids.add(cid)
                if label:
                    if cid not in cat_map:
                        cat_map[cid] = label
                    if label not in tags_map.get(cid, []):
                        tags_map.setdefault(cid, []).append(label)

        logger.info(
            "[plex] genre map: %d categorised, %d with tags, %d Spanish",
            len(cat_map), len(tags_map), len(spanish_ids),
        )
        # Normalize keys to the stable channel part so lookups work regardless
        # of which server prefix the genre endpoint happens to return.
        cat_map     = {_normalize_plex_id(k) or k: v for k, v in cat_map.items()}
        tags_map    = {_normalize_plex_id(k) or k: v for k, v in tags_map.items()}
        spanish_ids = {_normalize_plex_id(k) or k for k in spanish_ids}
        return cat_map, tags_map, spanish_ids

    # ── fetch_channels ─────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        t0 = time.monotonic()
        if not self._ensure_auth():
            return []

        def _do_channel_fetch():
            return self.session.get(
                f"{_EPG_HOST}/lineups/plex/channels",
                params={"X-Plex-Token": self._auth_token},
                headers=self._provider_headers(accept="application/xml"),
                timeout=30,
            )

        try:
            r = _do_channel_fetch()
            if r.status_code in (401, 403):
                logger.info("[plex] channel list %d — refreshing token and retrying", r.status_code)
                self._auth_token = None
                if not self._ensure_auth(force=True):
                    return []
                r = _do_channel_fetch()
            r.raise_for_status()
            root = ET.fromstring(r.text)
        except Exception as exc:
            logger.error("[plex] channel list fetch failed: %s", exc)
            return []

        cat_map, tags_map, spanish_ids = self._fetch_genre_maps()

        channels: dict[str, ChannelData] = {}
        for ch in root.findall(".//Channel"):
            raw_id   = ch.get("id") or ""
            grid_key = ch.get("gridKey") or ""
            # gridKey is the stable 24-hex channel identifier (no server prefix).
            # Fall back to stripping the prefix from id if gridKey is absent.
            channel_id = grid_key or _normalize_plex_id(raw_id) or raw_id
            if not channel_id or channel_id in channels:
                continue

            name    = ch.get("title") or ch.get("callSign") or channel_id
            slug    = ch.get("slug") or channel_id
            logo    = ch.get("thumb")
            summary = (ch.get("summary") or "").strip() or None

            lang = "es" if channel_id in spanish_ids else infer_language_from_metadata(name, cat_map.get(channel_id))

            channels[channel_id] = ChannelData(
                source_channel_id = channel_id,
                name              = name,
                # stream_url keeps the full compound ID (server-prefix + channel_id) so
                # that resolve() can pass it unchanged to the Plex play API, which requires
                # the compound form.  source_channel_id uses only the stable channel part.
                stream_url        = f"plex://{raw_id or channel_id}",
                logo_url          = logo,
                slug              = slug,
                category          = cat_map.get(channel_id) or _category_for_channel(name, None),
                language          = lang,
                country           = "US",
                stream_type       = "hls",
                gracenote_id      = resolve_gracenote("plex", lookup_key=channel_id),
                guide_key         = grid_key or channel_id,
                tags              = tags_map.get(channel_id, []),
                description       = summary,
            )

        result = sorted(channels.values(), key=lambda c: (c.name or "").lower())
        logger.info(
            "[plex] %d channels fetched in %.1fs",
            len(result),
            time.monotonic() - t0,
        )
        return result

    def _parse_grid_xml_programs(
        self,
        source_channel_id: str,
        xml_text: str,
        seen: set[str] | None = None,
    ) -> list[ProgramData]:
        programs: list[ProgramData] = []
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return programs

        for video in root.findall(".//Video"):
            media = video.find("Media")
            if media is None:
                continue

            airing_id = media.attrib.get("id")
            if airing_id and seen is not None and airing_id in seen:
                continue
            if airing_id and seen is not None:
                seen.add(airing_id)

            start = _parse_ts(media.attrib.get("beginsAt"))
            end = _parse_ts(media.attrib.get("endsAt"))
            if not start or not end:
                continue

            raw_title = video.attrib.get("title") or "Unknown"
            gp_title = video.attrib.get("grandparentTitle") or ""
            if gp_title and gp_title.lower() != raw_title.lower():
                title = gp_title
                ep_title = _clean_ep_title(raw_title)
            else:
                title = raw_title
                ep_title = None

            # For TV episodes prefer show art (grandparentThumb) over the 16:9
            # episode still (thumb). For movies thumb is already portrait.
            if gp_title:
                poster = (
                    video.attrib.get("grandparentThumb")
                    or video.attrib.get("thumb")
                    or next(
                        (img.attrib.get("url") for img in video.findall("Image")
                         if img.attrib.get("type") == "coverArt"),
                        None,
                    )
                )
            else:
                poster = (
                    video.attrib.get("thumb")
                    or next(
                        (img.attrib.get("url") for img in video.findall("Image")
                         if img.attrib.get("type") == "coverArt"),
                        None,
                    )
                )
            genres = [g.attrib.get("tag") for g in video.findall("Genre") if g.attrib.get("tag")]
            category = genres[0] if genres else None

            # Plex tags hour-long music-video blocks (Euro Hits, FilmRise Music,
            # XITE, …) as type="movie", which otherwise leaks a bogus
            # <category>Movie</category> into the guide. These carry a "Music"
            # genre; real films — even on a music-misclassified channel like a
            # "Non-Stop '90s" movie loop — never do (their genres are Action,
            # Comedy, Movies, …). Keying off the genre rather than the channel
            # category avoids demoting real movies and works on both the full
            # and EPG-only scrape paths.
            program_type = video.attrib.get("type") or None
            if program_type == "movie" and any((g or "").casefold() == "music" for g in genres):
                program_type = None

            programs.append(ProgramData(
                source_channel_id = source_channel_id,
                title             = title,
                description       = video.attrib.get("summary") or None,
                start_time        = start,
                end_time          = end,
                poster_url        = poster,
                rating            = video.attrib.get("contentRating") or None,
                category          = category,
                season            = int(video.attrib["parentIndex"]) if video.attrib.get("parentIndex", "").isdigit() else None,
                episode           = int(video.attrib["index"]) if video.attrib.get("index", "").isdigit() else None,
                episode_title     = ep_title,
                original_air_date = _parse_date(video.attrib.get("originalAvailableAt")),
                program_type      = program_type,
            ))

        return programs

    def _fetch_extra_day_programs(self, channels: list[ChannelData], enabled_ids: set[str] | None) -> list[ProgramData]:
        if not channels or not enabled_ids:
            return []

        guide_channels = [
            ch for ch in channels
            if ch.source_channel_id in enabled_ids and getattr(ch, "guide_key", None)
        ]
        if not guide_channels:
            return []

        headers = self._provider_headers(accept="application/xml", provider_version="7.2")
        today = datetime.now(timezone.utc).date()
        results: list[ProgramData] = []
        logger.info(
            "[plex] targeted guide fetch starting: channels=%d days=%d workers=%d",
            len(guide_channels),
            _PLEX_EXTRA_DAYS + 1,
            _PLEX_GUIDE_WORKERS,
        )

        def _fetch_one(ch: ChannelData, day_offset: int) -> list[ProgramData]:
            target_date = (today + timedelta(days=day_offset)).strftime("%Y-%m-%d")
            try:
                r = requests.get(
                    f"{_EPG_HOST}/grid",
                    params={"channelGridKey": ch.guide_key, "date": target_date},
                    headers=headers,
                    timeout=10,
                )
                r.raise_for_status()
            except Exception as exc:
                logger.debug("[plex] targeted grid fetch failed for %s day+%d: %s", ch.name, day_offset, exc)
                return []
            return self._parse_grid_xml_programs(ch.source_channel_id, r.text)

        start_t = time.monotonic()
        futures = []
        n_days = _PLEX_EXTRA_DAYS + 1  # today (0) + extra days
        total_tasks = len(guide_channels) * n_days
        done_tasks = 0
        with ThreadPoolExecutor(max_workers=_PLEX_GUIDE_WORKERS) as pool:
            for day_offset in range(0, _PLEX_EXTRA_DAYS + 1):
                for ch in guide_channels:
                    futures.append(pool.submit(_fetch_one, ch, day_offset))
            for future in as_completed(futures):
                try:
                    results.extend(future.result())
                except Exception:
                    logger.debug("[plex] targeted guide future failed", exc_info=True)
                done_tasks += 1
                if self._progress_cb:
                    self._progress_cb('epg', done_tasks, total_tasks)

        logger.info(
            "[plex] targeted guide fetch complete: channels=%d days=%d programs=%d in %.1fs",
            len(guide_channels),
            n_days,
            len(results),
            time.monotonic() - start_t,
        )
        return results

    # ── Luma gap-fill helpers ──────────────────────────────────────────────────

    def _build_luma_map(self, rsc_text: str) -> dict[str, str]:
        """Extract guide_key → luma URL map from the RSC blob (no extra HTTP call)."""
        luma_map: dict[str, str] = {}
        for m in _re.finditer(
            r'luma\.plex\.tv/api/fragment/live-tv/airings/([a-f0-9]+)(\?[^"]+)?',
            rsc_text,
        ):
            gk = m.group(1)
            if gk not in luma_map:
                luma_map[gk] = "https://" + m.group(0)
        return luma_map

    def _fetch_luma_programs(
        self,
        guide_key: str,
        url: str,
        source_channel_id: str,
        session: requests.Session | None = None,
    ) -> list[ProgramData]:
        """Fetch and parse one channel's schedule from the luma fragment endpoint."""
        sess = session or self.session
        try:
            r = sess.get(
                url,
                headers={
                    "Accept":                 "text/x-component, */*",
                    "RSC":                    "1",
                    "Next-Url":               "/en/live-tv",
                    "Next-Router-State-Tree": "%5B%22%22%2C%7B%7D%5D",
                    "X-Plex-Token":           self._auth_token,
                },
                timeout=15,
            )
        except Exception as exc:
            logger.warning("[plex] luma fetch failed for %s: %s", guide_key, exc)
            return []
        if r.status_code == 401:
            logger.warning("[plex] luma 401 for %s — RSC token may be stale", guide_key)
            return []
        if r.status_code != 200:
            logger.debug("[plex] luma HTTP %d for %s", r.status_code, guide_key)
            return []
        return _parse_luma_fragment(r.text, source_channel_id)

    def _new_luma_session(self) -> requests.Session:
        """Best-effort session for luma gap-fill.

        Disable read retries so slow upstream fragments fail once instead of
        emitting urllib3 retry warnings and stretching a single miss across
        multiple 15s timeout windows.
        """
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=Retry(
            total=3,
            connect=3,
            read=0,
            status=2,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=None,
            raise_on_status=False,
        ))
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(self.session.headers)
        session.cookies.update(self.session.cookies.get_dict())
        return session

    def _luma_gap_fill(
        self,
        channels: list[ChannelData],
        enabled_ids: set[str],
        programs: list[ProgramData],
    ) -> list[ProgramData]:
        """
        For channels whose grid/targeted schedules have gaps > 30 min, fetch
        from luma.plex.tv and merge in only the airings that fill those holes.
        """
        rsc_text = self._fetch_rsc()
        if not rsc_text:
            return []
        luma_map = self._build_luma_map(rsc_text)
        if not luma_map:
            logger.warning("[plex] luma gap-fill: no luma URLs found in RSC")
            return []

        by_channel: dict[str, list[ProgramData]] = defaultdict(list)
        for p in programs:
            by_channel[p.source_channel_id].append(p)

        gapped: list[tuple[ChannelData, list]] = []
        for ch in channels:
            if ch.source_channel_id not in enabled_ids:
                continue
            guide_key = getattr(ch, "guide_key", None)
            if not guide_key or guide_key not in luma_map:
                continue
            gaps = _find_gaps(by_channel.get(ch.source_channel_id, []))
            if gaps:
                gapped.append((ch, gaps))

        if not gapped:
            return []

        logger.info("[plex] luma gap-fill: %d channels have schedule gaps", len(gapped))

        import threading
        extras: list[ProgramData] = []
        lock = threading.Lock()
        thread_local = threading.local()

        def _fetch_one(ch: ChannelData, gaps: list) -> None:
            sess = getattr(thread_local, "session", None)
            if sess is None:
                sess = self._new_luma_session()
                thread_local.session = sess
            sess.cookies.update(self.session.cookies.get_dict())
            luma_progs = self._fetch_luma_programs(
                ch.guide_key,
                luma_map[ch.guide_key],
                ch.source_channel_id,
                session=sess,
            )
            if not luma_progs:
                return
            filled = _merge_luma_into_gaps(by_channel.get(ch.source_channel_id, []), luma_progs)
            if filled:
                with lock:
                    extras.extend(filled)

        with ThreadPoolExecutor(max_workers=_PLEX_LUMA_WORKERS) as pool:
            futs = [pool.submit(_fetch_one, ch, gaps) for ch, gaps in gapped]
            for fut in as_completed(futs):
                try:
                    fut.result()
                except Exception:
                    logger.debug("[plex] luma gap-fill future failed", exc_info=True)

        if extras:
            logger.info("[plex] luma gap-fill total: +%d programs", len(extras))
        return extras

    # ── fetch_epg ──────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        """
        Fetch EPG using per-channel targeted grid API (channelGridKey + date).

        The bulk beginningAt/endingAt grid endpoint was retired by Plex (May 2026)
        and now returns 400.  The per-channel channelGridKey endpoint is the sole
        EPG source; it covers today through _PLEX_EXTRA_DAYS forward.
        Luma fragment fetches fill any remaining gaps.
        """
        if not self._ensure_auth():
            return []

        enabled_ids = set(kwargs.get("enabled_ids") or [])
        if not enabled_ids:
            # Outside the worker context (e.g., BaseScraper.run()) no enabled_ids
            # kwarg is passed; treat all channels as enabled.
            enabled_ids = {ch.source_channel_id for ch in channels if ch.source_channel_id}

        programs = self._fetch_extra_day_programs(channels, enabled_ids)

        luma_extras = self._luma_gap_fill(channels, enabled_ids, programs)
        if luma_extras:
            programs.extend(luma_extras)
            logger.info("[plex] %d total EPG entries after luma gap-fill", len(programs))

        return programs

    # ── resolve ────────────────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        """
        raw_url format: plex://{channel_id}
        Returns a live AWS MediaTailor HLS URL (short-lived, fetched fresh each time).
        """
        if not raw_url.startswith("plex://"):
            return raw_url

        channel_id = raw_url[len("plex://"):]


        if not self._ensure_auth():
            raise RuntimeError("[plex] cannot resolve — auth failed")

        # Tune: wakes the channel on Plex's infrastructure (best-effort)
        try:
            self.session.post(
                f"{_EPG_HOST}/channels/{channel_id}/tune",
                headers={
                    "Accept":               "application/json",
                    "Content-Type":         "application/json",
                    "X-Plex-Playback-Id":   self._pid,
                    "X-Plex-Token":         self._auth_token,
                },
                data=b"",
                timeout=10,
            )
        except Exception as exc:
            logger.debug("[plex] tune request failed (non-fatal): %s", exc)

        # Manifest request — Plex issues a 302 redirect to MediaTailor
        manifest_url = (
            f"{_EPG_HOST}/library/parts/{channel_id}.m3u8"
            f"?includeAllStreams=1"
            f"&X-Plex-Product={quote_plus(_PRODUCT)}"
            f"&X-Plex-Token={quote_plus(self._auth_token)}"
        )
        r = self.session.get(manifest_url, timeout=15, allow_redirects=True)

        if r.status_code == 200:
            final = r.url
            logger.debug("[plex] resolved %s → %s…", channel_id, final[:60])
            return final

        # Token may have expired — refresh once and retry
        if r.status_code in (401, 403):
            logger.info("[plex] token rejected (%d), refreshing…", r.status_code)
            self._auth_token = None
            if self._ensure_auth(force=True):
                manifest_url = (
                    f"{_EPG_HOST}/library/parts/{channel_id}.m3u8"
                    f"?includeAllStreams=1"
                    f"&X-Plex-Product={quote_plus(_PRODUCT)}"
                    f"&X-Plex-Token={quote_plus(self._auth_token)}"
                )
                r2 = self.session.get(manifest_url, timeout=15, allow_redirects=True)
                if r2.status_code == 200:
                    return r2.url

        if r.status_code in (400, 404, 410, 422, 504):
            raise StreamDeadError(format_http_reason("[plex] channel not playable", r.status_code, channel_id))
        raise RuntimeError(f"[plex] manifest HTTP {r.status_code} for {channel_id}")

    def _audit_manifest_status(self, manifest_url: str) -> int:
        try:
            proc = subprocess.run(
                [
                    "curl", "-skI", "--max-time", "8",
                    "-o", "/dev/null", "-w", "%{http_code}",
                    manifest_url,
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            code_text = (proc.stdout or "").strip()
            if len(code_text) >= 3 and code_text[-3:].isdigit():
                return int(code_text[-3:])
            logger.debug("[plex] audit curl status failed: rc=%s stderr=%s", proc.returncode, (proc.stderr or "").strip()[:200])
        except Exception as exc:
            logger.debug("[plex] audit curl status failed: %s", exc)

        r = self.session.get(manifest_url, timeout=(3, 7), allow_redirects=False)
        return r.status_code

    def audit_resolve(self, raw_url: str) -> str:
        """
        Lightweight health check for stream audits.
        Skips the tune POST and does not follow the MediaTailor redirect.
        Plex may either redirect to MediaTailor or return the playlist directly;
        either response confirms the channel is live.
        Returns raw_url on success so the audit knows the channel is alive.
        """
        if not raw_url.startswith("plex://"):
            return raw_url

        channel_id = raw_url[len("plex://"):]

        if not self._ensure_auth():
            raise RuntimeError("[plex] cannot audit_resolve — auth failed")

        manifest_url = (
            f"{_EPG_HOST}/library/parts/{channel_id}.m3u8"
            f"?includeAllStreams=1"
            f"&X-Plex-Product={quote_plus(_PRODUCT)}"
            f"&X-Plex-Token={quote_plus(self._auth_token)}"
        )
        status_code = self._audit_manifest_status(manifest_url)

        if status_code == 200 or status_code in (301, 302, 303, 307, 308):
            return raw_url

        if status_code in (401, 403):
            self._auth_token = None
            if self._ensure_auth(force=True):
                manifest_url = (
                    f"{_EPG_HOST}/library/parts/{channel_id}.m3u8"
                    f"?includeAllStreams=1"
                    f"&X-Plex-Product={quote_plus(_PRODUCT)}"
                    f"&X-Plex-Token={quote_plus(self._auth_token)}"
                )
                status_code = self._audit_manifest_status(manifest_url)
                if status_code == 200 or status_code in (301, 302, 303, 307, 308):
                    return raw_url

        if status_code in (400, 404, 410, 422, 504):
            raise StreamDeadError(format_http_reason("[plex] channel not playable", status_code, channel_id))
        raise RuntimeError(f"[plex] audit manifest HTTP {status_code} for {channel_id}")

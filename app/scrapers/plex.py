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
import string
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus
from uuid import uuid4

import requests

from .base import BaseScraper, ChannelData, ProgramData, StreamDeadError, infer_language_from_metadata
from ..gracenote_map import resolve_gracenote

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)
_PRODUCT  = "Plex Mediaverse"
_EPG_HOST = "https://epg.provider.plex.tv"
_PLEX_EXTRA_DAYS = 2
_PLEX_GUIDE_WORKERS = 6

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


def _rand_rsc() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=5))


def _build_category_map(rsc_objects: list[dict]) -> tuple[dict[str, str], set[str]]:
    """
    Parse the categories list from RSC objects.
    Returns:
      cat_map   — channel_id → category label
      spanish   — set of channel_ids in the en-espanol category (language hint)
    """
    cat_map: dict[str, str] = {}
    spanish: set[str] = set()

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
                    if cid not in cat_map:
                        cat_map[cid] = "En Español"
                elif slug != "featured":
                    # Only set if not already assigned (first non-featured wins)
                    if cid not in cat_map:
                        label = _PLEX_CATEGORY_MAP.get(slug)
                        if label:
                            cat_map[cid] = label
        break  # categories list only appears once

    return cat_map, spanish


# ── Scraper ────────────────────────────────────────────────────────────────────

class PlexScraper(BaseScraper):

    source_name           = "plex"
    display_name          = "Plex"
    scrape_interval       = 60   # EPG refreshed every hour
    channel_refresh_hours = 24   # channel list once a day
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
            self.session.get("https://watch.plex.tv/", timeout=15)
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
            logger.error("[plex] auth failed: %s", exc)
            return False

    # ── RSC fetch (shared by channels + EPG) ───────────────────────────────────

    def _fetch_rsc(self) -> str:
        if self._rsc_cache:
            return self._rsc_cache
        if not self._ensure_auth():
            return ""
        t0 = time.monotonic()
        r = self.session.get(
            f"https://watch.plex.tv/live-tv?_rsc={_rand_rsc()}",
            headers={
                "Accept":                 "*/*",
                "RSC":                    "1",
                "Next-Url":               "/en",
                "Next-Router-State-Tree": _NEXT_ROUTER_STATE_TREE,
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

    def _fetch_lineup_grid_keys(self) -> dict[str, str]:
        if not self._ensure_auth():
            return {}

        try:
            root = self.session.get(
                f"{_EPG_HOST}/",
                params={"X-Plex-Token": self._auth_token},
                headers=self._provider_headers(),
                timeout=30,
            )
            root.raise_for_status()
        except Exception as exc:
            logger.warning("[plex] lineup root fetch failed: %s", exc)
            return {}

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
            return {}

        grid_keys: dict[str, str] = {}
        headers = self._provider_headers()
        for genre_slug in genre_slugs:
            try:
                r = self.session.get(
                    f"{_EPG_HOST}/lineups/plex/channels",
                    params={"genre": genre_slug, "X-Plex-Token": self._auth_token},
                    headers=headers,
                    timeout=30,
                )
                r.raise_for_status()
            except Exception as exc:
                logger.warning("[plex] lineup fetch failed for genre=%s: %s", genre_slug, exc)
                continue

            for channel in r.json().get("MediaContainer", {}).get("Channel", []):
                channel_id = channel.get("id")
                grid_key = channel.get("gridKey")
                if channel_id and grid_key and channel_id not in grid_keys:
                    grid_keys[channel_id] = grid_key

        logger.info("[plex] lineup gridKey map built for %d channels", len(grid_keys))
        return grid_keys

    # ── fetch_channels ─────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        t0 = time.monotonic()
        text = self._fetch_rsc()
        if not text:
            return []

        rsc_objects = _extract_rsc_objects(text)
        cat_map, spanish_ids = _build_category_map(rsc_objects)
        grid_keys = self._fetch_lineup_grid_keys()

        channels: dict[str, ChannelData] = {}
        for obj in rsc_objects:
            for node in _walk(obj):
                if not isinstance(node, dict):
                    continue
                if not _CHANNEL_KEYS <= node.keys():
                    continue
                channel_id = node.get("id")
                if not channel_id or channel_id in channels:
                    continue

                data  = node.get("data") or {}
                logo  = node.get("thumb") or (
                    ((data.get("cast") or {}).get("image") or {}).get("url")
                )
                category = cat_map.get(channel_id)
                lang = "es" if channel_id in spanish_ids else infer_language_from_metadata(
                    node.get("title"),
                    category,
                )
                channels[channel_id] = ChannelData(
                    source_channel_id = channel_id,
                    name              = node.get("title") or node.get("slug") or channel_id,
                    stream_url        = f"plex://{channel_id}",
                    logo_url          = logo,
                    slug              = node.get("slug") or channel_id,
                    category          = category,
                    language          = lang,
                    country           = "US",
                    stream_type       = "hls",
                    gracenote_id      = resolve_gracenote("plex", lookup_key=channel_id),
                    guide_key         = grid_keys.get(channel_id),
                )

        result = sorted(channels.values(), key=lambda c: (c.name or "").lower())
        logger.info(
            "[plex] %d channels fetched from %d RSC objects in %.1fs",
            len(result),
            len(rsc_objects),
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
                ep_title = raw_title
            else:
                title = raw_title
                ep_title = None

            poster = (
                video.attrib.get("thumb")
                or video.attrib.get("grandparentThumb")
                or next(
                    (img.attrib.get("url") for img in video.findall("Image")
                     if img.attrib.get("type") == "coverArt"),
                    None,
                )
            )
            category = next(
                (genre.attrib.get("tag") for genre in video.findall("Genre") if genre.attrib.get("tag")),
                None,
            )

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
            "[plex] targeted extra-day fetch starting: channels=%d days=%d workers=%d",
            len(guide_channels),
            _PLEX_EXTRA_DAYS,
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
        with ThreadPoolExecutor(max_workers=_PLEX_GUIDE_WORKERS) as pool:
            for day_offset in range(1, _PLEX_EXTRA_DAYS + 1):
                for ch in guide_channels:
                    futures.append(pool.submit(_fetch_one, ch, day_offset))
            for future in as_completed(futures):
                try:
                    results.extend(future.result())
                except Exception:
                    logger.debug("[plex] targeted guide future failed", exc_info=True)

        logger.info(
            "[plex] targeted extra-day fetch complete: channels=%d days=%d programs=%d in %.1fs",
            len(guide_channels),
            _PLEX_EXTRA_DAYS,
            len(results),
            time.monotonic() - start_t,
        )
        return results

    # ── fetch_epg ──────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        """
        Fetch full EPG from the Plex grid API (epg.provider.plex.tv/grid).

        The grid endpoint returns ALL channels in one shot regardless of any
        channelIds filter, so we fetch 3 consecutive 8-hour windows (24 h total)
        and filter the results in-memory by our known channel IDs.
        """
        if not self._ensure_auth():
            return []

        known_ids = {ch.source_channel_id for ch in channels}
        enabled_ids = set(kwargs.get("enabled_ids") or [])
        programs: list[ProgramData] = []
        seen: set[str] = set()   # deduplicate by media airing id
        fetch_t0 = time.monotonic()

        headers = {
            "Accept":                   "application/json",
            "X-Plex-Token":             self._auth_token,
            "X-Plex-Client-Identifier": self._client_id,
            "X-Plex-Product":           _PRODUCT,
            "X-Plex-Platform":          "Chrome",
            "X-Plex-Platform-Version":  "145.0.0.0",
            # Suppress session-level provider-version header — it triggers a
            # channelGridKey requirement on the grid endpoint.
            "X-Plex-Provider-Version":  None,
        }

        import time as _time
        window_hours = 8
        window_secs  = window_hours * 3600
        n_windows    = 3   # 24 h total

        t_start = int(_time.time())
        logger.info(
            "[plex] grid fetch starting: channels=%d windows=%d span_hours=%d",
            len(known_ids),
            n_windows,
            window_hours * n_windows,
        )
        for w in range(n_windows):
            begins_at = t_start + w * window_secs
            ends_at   = begins_at + window_secs
            window_t0 = time.monotonic()
            try:
                r = self.session.get(
                    f"{_EPG_HOST}/grid",
                    params={"beginningAt": begins_at, "endingAt": ends_at},
                    headers=headers,
                    timeout=30,
                )
                r.raise_for_status()
                entries = r.json().get("MediaContainer", {}).get("Metadata", [])
            except Exception as exc:
                logger.warning("[plex] grid fetch window %d failed: %s", w + 1, exc)
                continue

            for entry in entries:
                for media in entry.get("Media", []):
                    ch_id = media.get("channelIdentifier")
                    if not ch_id or ch_id not in known_ids:
                        continue

                    airing_id = media.get("id")
                    if airing_id and airing_id in seen:
                        continue
                    if airing_id:
                        seen.add(airing_id)

                    start = _parse_ts(media.get("beginsAt"))
                    end   = _parse_ts(media.get("endsAt"))
                    if not start or not end:
                        continue

                    raw_title = entry.get("title") or "Unknown"
                    gp_title = entry.get("grandparentTitle") or ""
                    # Plex grid entries for episodic content typically use:
                    #   title            = episode title
                    #   grandparentTitle = series title
                    # XMLTV expects:
                    #   <title>     = series/program title
                    #   <sub-title> = episode title
                    # For movies/specials grandparentTitle is absent, so keep the
                    # original title and omit episode_title.
                    if gp_title and gp_title.lower() != raw_title.lower():
                        title = gp_title
                        ep_title = raw_title
                    else:
                        title = raw_title
                        ep_title = None

                    # Prefer episode thumb; fall back to grandparent art
                    poster = (
                        entry.get("thumb")
                        or entry.get("grandparentThumb")
                        or next(
                            (img["url"] for img in entry.get("Image", [])
                             if img.get("type") == "coverArt"),
                            None,
                        )
                    )

                    programs.append(ProgramData(
                        source_channel_id = ch_id,
                        title             = title,
                        description       = entry.get("summary") or None,
                        start_time        = start,
                        end_time          = end,
                        poster_url        = poster,
                        rating            = entry.get("contentRating") or None,
                        season            = entry.get("parentIndex"),
                        episode           = entry.get("index"),
                        episode_title     = ep_title,
                    ))

            logger.info(
                "[plex] grid window %d/%d fetched in %.1fs: metadata=%d cumulative_programs=%d seen_airings=%d",
                w + 1,
                n_windows,
                time.monotonic() - window_t0,
                len(entries),
                len(programs),
                len(seen),
            )

        logger.info(
            "[plex] %d EPG entries fetched from grid API in %.1fs",
            len(programs),
            time.monotonic() - fetch_t0,
        )
        extra_programs = self._fetch_extra_day_programs(channels, enabled_ids)
        if extra_programs:
            programs.extend(extra_programs)
            logger.info("[plex] %d total EPG entries after targeted extension", len(programs))
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

        raise RuntimeError(f"[plex] manifest HTTP {r.status_code} for {channel_id}")

    def audit_resolve(self, raw_url: str) -> str:
        """
        Lightweight health check for stream audits.
        Skips the tune POST and does not follow the MediaTailor redirect —
        just confirms the manifest endpoint returns 302 (channel is live).
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
        r = self.session.get(manifest_url, timeout=10, allow_redirects=True)

        if r.status_code == 200:
            return r.url

        if r.status_code in (401, 403):
            self._auth_token = None
            if self._ensure_auth(force=True):
                manifest_url = (
                    f"{_EPG_HOST}/library/parts/{channel_id}.m3u8"
                    f"?includeAllStreams=1"
                    f"&X-Plex-Product={quote_plus(_PRODUCT)}"
                    f"&X-Plex-Token={quote_plus(self._auth_token)}"
                )
                r2 = self.session.get(manifest_url, timeout=10, allow_redirects=True)
                if r2.status_code == 200:
                    return r2.url

        if r.status_code == 400:
            raise StreamDeadError(f"[plex] channel not playable: {channel_id}")
        raise RuntimeError(f"[plex] audit manifest HTTP {r.status_code} for {channel_id}")

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
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus
from uuid import uuid4

from .base import BaseScraper, ChannelData, ProgramData

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)
_PRODUCT  = "Plex Mediaverse"
_EPG_HOST = "https://epg.provider.plex.tv"

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
        return datetime.strptime(str(value), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _rand_rsc() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=5))


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
            logger.info("[plex] anonymous auth OK, token=%s…", self._auth_token[:8])
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
        return self._rsc_cache

    # ── fetch_channels ─────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        text = self._fetch_rsc()
        if not text:
            return []

        channels: dict[str, ChannelData] = {}
        for obj in _extract_rsc_objects(text):
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
                channels[channel_id] = ChannelData(
                    source_channel_id = channel_id,
                    name              = node.get("title") or node.get("slug") or channel_id,
                    stream_url        = f"plex://{channel_id}",
                    logo_url          = logo,
                    slug              = node.get("slug") or channel_id,
                    language          = "en",
                    country           = "US",
                    stream_type       = "hls",
                )

        result = sorted(channels.values(), key=lambda c: (c.name or "").lower())
        logger.info("[plex] %d channels fetched", len(result))
        return result

    # ── fetch_epg ──────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData]) -> list[ProgramData]:
        text = self._fetch_rsc()
        if not text:
            return []

        known_ids = {ch.source_channel_id for ch in channels}
        programs: list[ProgramData] = []
        seen: set[str] = set()

        for obj in _extract_rsc_objects(text):
            for node in _walk(obj):
                if not isinstance(node, dict):
                    continue
                if not _CHANNEL_KEYS <= node.keys():
                    continue
                channel_id = node.get("id")
                if not channel_id or channel_id not in known_ids:
                    continue

                for airing in (node.get("airings") or []):
                    airing_data = airing.get("data") or {}
                    preview     = airing.get("previewData") or {}

                    start = _parse_ts(airing_data.get("beginsAt"))
                    end   = _parse_ts(airing_data.get("endsAt"))
                    title = (
                        airing.get("title")
                        or preview.get("title")
                        or "Unknown"
                    )
                    if not start or not end:
                        continue

                    key = f"{channel_id}|{start.isoformat()}|{title}"
                    if key in seen:
                        continue
                    seen.add(key)

                    subtitle = preview.get("subtitle") or airing.get("subtitle")
                    poster   = ((preview.get("poster") or {}).get("image") or {}).get("url")

                    programs.append(ProgramData(
                        source_channel_id = channel_id,
                        title             = title,
                        start_time        = start,
                        end_time          = end,
                        description       = preview.get("summary"),
                        poster_url        = poster,
                        episode_title     = subtitle if subtitle and subtitle != title else None,
                    ))

        logger.info("[plex] %d EPG entries fetched", len(programs))
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

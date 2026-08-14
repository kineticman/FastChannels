from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

from .base import BaseScraper, ChannelData, ProgramData

logger = logging.getLogger(__name__)


class HallmarkScraper(BaseScraper):
    """
    Scraper for Hallmark free live channels (hallmarkplus.com).

    Channels are hardcoded — the lineup is stable. EPG is fetched directly
    from the Hallmark schedule API. Stream URLs are stable Akamai CDN HLS
    endpoints that require no auth; resolve() maps the opaque URI to them.
    """

    source_name = "hallmark"
    display_name = "Hallmark"
    scrape_interval = 360
    stream_audit_enabled = True
    source_category = 'specialty'

    config_schema = []

    _BASE_URL = "https://www.hallmarkplus.com"
    _CATALOG_URL = _BASE_URL + "/api/core/catalog/channels"
    _SCHEDULE_URL = _BASE_URL + "/api/core/catalog/channels/{linear_id}/schedule"
    _GUIDE_HOURS = 48
    _ALLOWED_MINUTES = (0, 30, 45)

    _LOGO_BASE = "/static/logos/hallmark"

    # "linear_id" is a fallback only — Hallmark has renamed these content-item
    # IDs before (e.g. "Linear-1543-Hallmark" -> "...-Non-Stop-Hallmark-Hits-DRM"),
    # which 404s the schedule endpoint. fetch_epg() re-resolves the live ID each
    # scrape via the catalog endpoint, keyed on the (so far stable) channel_identifier.
    _CHANNELS = [
        {
            "linear_id": "Linear-1543-Non-Stop-Hallmark-Hits-DRM",
            "channel_identifier": "HLIVE_HMH",
            "tvg_id":    "hallmark_hits",
            "name":      "Non-Stop Hallmark Hits",
            "stream_url": "https://crwn-hits.akamaized.net/hls/playlist.m3u8",
            "logo_url":  f"{_LOGO_BASE}/hits_improved.png",
            "number":    9001,
        },
        {
            "linear_id": "Linear-1268-Non-Stop-RomComs-DRM",
            "channel_identifier": "HLIVE_ROMCOM",
            "tvg_id":    "hallmark_romcoms",
            "name":      "Non-Stop Rom-Coms",
            "stream_url": "https://crwn-nsrc.akamaized.net/hls/playlist.m3u8",
            "logo_url":  f"{_LOGO_BASE}/romcoms_improved.png",
            "number":    9002,
        },
        {
            "linear_id": "Linear-1187-Non-Stop-Christmas-DRM",
            "channel_identifier": "HLIVE_NSC",
            "tvg_id":    "hallmark_christmas",
            "name":      "Non-Stop Christmas",
            "stream_url": "https://crwn-christmas.akamaized.net/hls/playlist.m3u8",
            "logo_url":  f"{_LOGO_BASE}/christmas_improved.png",
            "number":    9003,
        },
        {
            "linear_id": "Linear-1186-Non-Stop-Mysteries-DRM",
            "channel_identifier": "HLIVE_MSM",
            "tvg_id":    "hallmark_mysteries",
            "name":      "Non-Stop Mysteries",
            "stream_url": "https://crwn-mysteries.akamaized.net/hls/playlist.m3u8",
            "logo_url":  f"{_LOGO_BASE}/mysteries_improved.png",
            "number":    9004,
        },
    ]

    _STREAM_URLS = {ch["tvg_id"]: ch["stream_url"] for ch in _CHANNELS}
    _TVG_TO_LINEAR = {ch["tvg_id"]: ch["linear_id"] for ch in _CHANNELS}
    _IDENTIFIER_TO_TVG = {ch["channel_identifier"]: ch["tvg_id"] for ch in _CHANNELS}

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Origin": self._BASE_URL,
            "Referer": self._BASE_URL + "/",
        })

    # ── Required ─────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        channels = [
            ChannelData(
                source_channel_id=ch["tvg_id"],
                name=ch["name"],
                stream_url=f"hallmark://{ch['tvg_id']}",
                logo_url=ch["logo_url"],
                category="Movies",
                number=ch["number"],
                country="US",
                language="en",
                stream_type="hls",
            )
            for ch in self._CHANNELS
        ]
        logger.info("[%s] %d channels", self.source_name, len(channels))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        now = datetime.now(timezone.utc)
        guide_start = self._align(now, "floor")
        guide_end = self._align(now + timedelta(hours=self._GUIDE_HOURS), "floor")

        linear_ids = {**self._TVG_TO_LINEAR, **self._resolve_linear_ids()}

        programs: list[ProgramData] = []
        for ch in channels:
            linear_id = linear_ids.get(ch.source_channel_id)
            if not linear_id:
                continue
            items = self._fetch_schedule(linear_id, guide_start, guide_end)
            for item in items:
                prog = self._parse_program(ch.source_channel_id, item)
                if prog:
                    programs.append(prog)
            logger.debug("[%s] %d EPG entries for %s", self.source_name, len(items), ch.name)

        logger.info("[%s] %d total EPG entries", self.source_name, len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("hallmark://"):
            return raw_url
        tvg_id = raw_url[len("hallmark://"):]
        return self._STREAM_URLS.get(tvg_id, raw_url)

    # ── Internals ─────────────────────────────────────────────

    def _resolve_linear_ids(self) -> dict[str, str]:
        """Look up current linear_ids from the catalog endpoint, keyed by the
        stable channel_identifier. Returns {} (falling back to the hardcoded
        defaults) if the catalog is unreachable or its shape changes too."""
        r = self.get(self._CATALOG_URL)
        if not r:
            return {}
        try:
            data = r.json().get("data", [])
        except Exception as exc:
            logger.warning("[%s] catalog JSON parse error: %s", self.source_name, exc)
            return {}

        resolved: dict[str, str] = {}
        for item in data if isinstance(data, list) else []:
            identifier = item.get("extras", {}).get("channelIdentifier")
            item_id = item.get("id")
            tvg_id = self._IDENTIFIER_TO_TVG.get(identifier)
            if tvg_id and item_id:
                resolved[tvg_id] = item_id

        stale = {
            tvg_id: (self._TVG_TO_LINEAR[tvg_id], resolved[tvg_id])
            for tvg_id in resolved
            if resolved[tvg_id] != self._TVG_TO_LINEAR.get(tvg_id)
        }
        if stale:
            logger.warning("[%s] linear_id changed upstream, update _CHANNELS: %s", self.source_name, stale)

        missing = set(self._IDENTIFIER_TO_TVG.values()) - set(resolved)
        if missing:
            logger.warning("[%s] catalog lookup missing channel_identifier for: %s", self.source_name, missing)

        return resolved

    def _fetch_schedule(self, linear_id: str, start: datetime, end: datetime) -> list[dict]:
        params = urlencode({
            "from":   start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "until":  end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "locale": "en",
        })
        url = self._SCHEDULE_URL.format(linear_id=linear_id) + "?" + params
        r = self.get(url)
        if not r:
            return []
        try:
            data = r.json().get("data", [])
            return data if isinstance(data, list) else []
        except Exception as exc:
            logger.error("[%s] JSON parse error for %s: %s", self.source_name, linear_id, exc)
            return []

    def _parse_program(self, channel_id: str, item: dict) -> ProgramData | None:
        schedule = item.get("schedule", {})
        epg = item.get("display", {}).get("epg", {})

        start = self._parse_iso(schedule.get("startsAt"))
        stop = self._parse_iso(schedule.get("endsAt"))
        if not start or not stop:
            return None

        title = epg.get("title") or item.get("title") or "Unknown"

        ratings = item.get("video", {}).get("parentalRatings", [])
        rating = ratings[0].get("value") if ratings else None

        return ProgramData(
            source_channel_id=channel_id,
            title=str(title),
            start_time=start,
            end_time=stop,
            description=epg.get("description") or epg.get("summary") or item.get("description") or None,
            episode_title=epg.get("subtitle") or epg.get("episodeTitle") or None,
            poster_url=self._first_image_url(item.get("images", {})),
            category="Movies",
            rating=rating,
        )

    def _align(self, dt: datetime, direction: str) -> datetime:
        """Snap dt to the nearest allowed schedule boundary (0/30/45 min past the hour)."""
        dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
        candidates = []
        for offset_days in (-1, 0, 1, 2):
            base = (dt + timedelta(days=offset_days)).replace(hour=0, minute=0)
            for hour in range(24):
                for minute in self._ALLOWED_MINUTES:
                    candidates.append(base + timedelta(hours=hour, minutes=minute))
        if direction == "floor":
            return max(c for c in candidates if c <= dt)
        return min(c for c in candidates if c >= dt)

    @staticmethod
    def _first_image_url(images: Any) -> str | None:
        if not isinstance(images, dict):
            return None
        for group, shape in (
            ("poster", "portrait"), ("poster", "landscape"),
            ("tile", "portrait"), ("tile", "landscape"),
            ("background", "landscape"),
        ):
            candidates = images.get(group, {}).get(shape, [])
            if isinstance(candidates, list):
                for c in candidates:
                    if isinstance(c, dict) and c.get("url"):
                        return str(c["url"])
        return None

    @staticmethod
    def _parse_iso(val: str | None) -> datetime | None:
        if not val:
            return None
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

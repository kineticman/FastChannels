
from __future__ import annotations

import html as _html
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata
from .category_utils import infer_category_from_name

logger = logging.getLogger(__name__)


class AmazonPrimeFreeScraper(BaseScraper):
    """
    First-pass FastChannels scraper for Amazon Prime Video free linear channels.

    What it does well:
    - Scrapes channel metadata from the Live TV page and paginated collection API.
    - Builds near-term EPG from the station.schedule arrays embedded in those responses.
    - Uses stable upstream station IDs as source_channel_id.

    Current limitation:
    - Playback is intentionally not implemented. The analyzed web flow returned
      encrypted DASH manifests with Widevine license calls, not clear HLS.
      stream_url is stored as an opaque internal URI so the source can still be
      scraped, inspected, and exported while playback work continues.
    """

    source_name = "amazon_prime_free"
    source_aliases = ("amazon-prime-free",)
    display_name = "Amazon Prime Free Channels"
    scrape_interval = 360

    config_schema = [
        ConfigField(
            "cookie_header",
            "Amazon Cookie Header",
            field_type="password",
            secret=True,
            help_text="Paste a valid Cookie header from a logged-in amazon.com browser session.",
        ),
        ConfigField(
            "user_agent",
            "User-Agent",
            field_type="text",
            required=False,
            help_text="Optional browser User-Agent override. A desktop Chrome UA works best.",
        ),
        ConfigField(
            "marketplace_id",
            "Marketplace ID",
            field_type="text",
            required=False,
            help_text="Defaults to ATVPDKIKX0DER for amazon.com / US.",
        ),
        ConfigField(
            "ux_locale",
            "UX Locale",
            field_type="text",
            required=False,
            help_text="Defaults to en_US.",
        ),
    ]

    LIVE_TV_URL = "https://www.amazon.com/gp/video/livetv"
    PAGINATE_URL = "https://www.amazon.com/gp/video/api/paginateCollection"

    DEFAULT_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.amazon.com/gp/video/storefront/",
    }

    # Observed in the analyzed HAR. Keep these centralized so they are easy to adjust.
    PAGINATE_DEFAULT_PARAMS: dict[str, Any] = {
        "pageType": "home",
        "pageId": "live",
        "collectionType": "Container",
        "actionScheme": "default",
        "payloadScheme": "default",
        "decorationScheme": "web-decoration-asin-v4",
        "featureScheme": "web-features-v6",
        "widgetScheme": "web-explore-v33",
        "variant": "desktopWindows",
        "journeyIngressContext": "28|CgVQcmltZQoLZnJlZXdpdGhhZHM=",
        "dynamicFeatures": [
            "integration",
            "CLIENT_DECORATION_ENABLE_DAAPI",
            "ENABLE_DRAPER_CONTENT",
            "HorizontalPagination",
            "CleanSlate",
            "EpgContainerPagination",
            "ENABLE_GPCI",
            "SupportsImageTextLinkTextInStandardHero",
            "Remaster",
            "SupportsChannelWidget",
            "PromotionalBannerSupported",
            "HERO_IMAGE_OPTIONAL",
            "RemoveFromContinueWatching",
            "ENABLE_CSIR",
            "SearchChannelBundles",
            "LinearStationsInHero",
            "LinearStationInAllCarousels",
            "SupportChannelItemDecoration",
            "TvodMovieBundles",
        ],
    }

    _STATION_NEEDLE = '"station":{'

    def __init__(self, config: dict | None = None):
        super().__init__(config)

        self._cookie_header = (self.config.get("cookie_header") or "").strip()
        self._marketplace_id = (self.config.get("marketplace_id") or "ATVPDKIKX0DER").strip()
        self._ux_locale = (self.config.get("ux_locale") or "en_US").strip()
        user_agent = (
            self.config.get("user_agent")
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
        )

        self.session.headers.update(self.DEFAULT_HEADERS)
        self.session.headers.update({"User-Agent": user_agent})
        if self._cookie_header:
            self.session.headers["Cookie"] = self._cookie_header

        # fetch_channels() populates this; fetch_epg() reads from it.
        self._station_cache: dict[str, dict[str, Any]] = {}

    def fetch_channels(self) -> list[ChannelData]:
        self._station_cache = {}

        page = self.get(self.LIVE_TV_URL)
        if not page:
            logger.error("[%s] failed to load Live TV page", self.source_name)
            return []

        html = page.text
        stations = self._extract_initial_stations(html)

        seed = self._extract_pagination_seed(html)
        if seed:
            paged = self._paginate_stations(seed)
            for station in paged.values():
                station_id = self._station_id(station)
                if station_id and station_id not in stations:
                    stations[station_id] = station
        else:
            logger.warning("[%s] could not find pagination seed in Live TV HTML", self.source_name)

        channels: list[ChannelData] = []
        for station_id, station in stations.items():
            channel = self._channel_from_station(station_id, station)
            if channel:
                channels.append(channel)
                self._station_cache[station_id] = station

        channels.sort(key=lambda c: c.name.lower())
        logger.info("[%s] %d channels", self.source_name, len(channels))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        programs: list[ProgramData] = []
        seen: set[tuple[str, int, int, str]] = set()

        for channel in channels:
            station = self._station_cache.get(channel.source_channel_id)
            if not station:
                continue

            for airing in station.get("schedule", []):
                program = self._program_from_schedule(channel.source_channel_id, airing)
                if not program:
                    continue

                dedupe_key = (
                    program.source_channel_id,
                    int(program.start_time.timestamp()),
                    int(program.end_time.timestamp()),
                    program.title,
                )
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                programs.append(program)

        programs.sort(key=lambda p: (p.source_channel_id, p.start_time, p.title))
        logger.info("[%s] %d EPG entries", self.source_name, len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("primefree://"):
            return raw_url

        # Playback not yet implemented — streams appear DRM-gated (DASH/Widevine).
        # Return the opaque URI so the play proxy fails gracefully rather than raising.
        logger.warning(
            "[%s] resolve() called for %s — playback not implemented (likely DRM)",
            self.source_name, raw_url,
        )
        return raw_url

    # ------------------------------------------------------------------
    # HTML / JSON extraction helpers
    # ------------------------------------------------------------------

    def _extract_initial_stations(self, html: str) -> dict[str, dict[str, Any]]:
        stations: dict[str, dict[str, Any]] = {}
        start = 0

        while True:
            idx = html.find(self._STATION_NEEDLE, start)
            if idx == -1:
                break

            obj_start = idx + len('"station":')
            blob = self._extract_balanced_json(html, obj_start)
            start = obj_start + 1
            if not blob:
                continue

            try:
                station = json.loads(blob)
            except json.JSONDecodeError:
                continue

            station_id = self._station_id(station)
            if station_id:
                stations[station_id] = station

        logger.debug("[%s] extracted %d stations from initial HTML", self.source_name, len(stations))
        return stations

    def _extract_pagination_seed(self, html: str) -> dict[str, Any] | None:
        # We prefer the EPG/live carousel seed because it is the one that yielded
        # linearStationCard data in the analyzed HAR. This keeps the scraper simple
        # while still matching observed behavior.
        match = re.search(
            r'"paginationStartIndex":(?P<start>\d+),"paginationTargetId":"(?P<target>[^"]+)"',
            html,
        )
        if not match:
            return None

        token_match = re.search(r'"serviceToken":"(?P<token>[^"]+)"', html)
        if not token_match:
            return None

        return {
            "start_index": int(match.group("start")),
            "pagination_target_id": match.group("target"),
            "service_token": token_match.group("token"),
        }

    def _paginate_stations(self, seed: dict[str, Any]) -> dict[str, dict[str, Any]]:
        stations: dict[str, dict[str, Any]] = {}
        start_index = int(seed["start_index"])
        has_more = True
        page_no = 0

        while has_more and page_no < 50:
            params = dict(self.PAGINATE_DEFAULT_PARAMS)
            params.update(
                {
                    "paginationTargetId": seed["pagination_target_id"],
                    "serviceToken": seed["service_token"],
                    "startIndex": str(start_index),
                }
            )

            response = self.get(self.PAGINATE_URL, params=params)
            if not response:
                break

            try:
                payload = response.json()
            except ValueError:
                if self._cookie_header:
                    logger.warning("[%s] non-JSON paginateCollection response at startIndex=%s — cookies may be expired", self.source_name, start_index)
                else:
                    logger.info("[%s] pagination requires auth (no cookie configured) — using %d channels from initial page only", self.source_name, len(stations))
                break

            entities = payload.get("entities", []) or []
            for entity in entities:
                station = entity.get("station") or {}
                station_id = self._station_id(station)
                if station_id:
                    stations[station_id] = station

            has_more = bool(payload.get("hasMoreItems"))
            next_index = payload.get("startIndex")
            if has_more:
                if isinstance(next_index, int) and next_index > start_index:
                    start_index = next_index
                else:
                    start_index += len(entities)
                    if not entities:
                        break
            page_no += 1

        logger.debug("[%s] extracted %d stations from pagination", self.source_name, len(stations))
        return stations

    @staticmethod
    def _extract_balanced_json(text: str, start_idx: int) -> str | None:
        depth = 0
        in_str = False
        esc = False
        started = False

        for i in range(start_idx, len(text)):
            ch = text[i]
            if not started:
                if ch == "{":
                    started = True
                    depth = 1
                else:
                    continue
                continue

            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
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
                    return text[start_idx : i + 1]

        return None

    # ------------------------------------------------------------------
    # Station / EPG mapping helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _station_id(station: dict[str, Any]) -> str | None:
        station_id = station.get("id")
        if not station_id:
            return None
        return str(station_id)

    def _channel_from_station(self, station_id: str, station: dict[str, Any]) -> ChannelData | None:
        name = _html.unescape((station.get("name") or "").strip())
        if not name:
            return None

        if station.get("isOnLinearNewsPage") or station.get("genre") == "news":
            category = "News"
        elif station.get("genre"):
            category = station["genre"].title()
        else:
            category = infer_category_from_name(name) or "Entertainment"

        return ChannelData(
            source_channel_id=station_id,
            name=name,
            stream_url=f"primefree://{station_id}",
            logo_url=station.get("logo"),
            category=category,
            language=infer_language_from_metadata(name, category),
        )

    def _program_from_schedule(self, source_channel_id: str, airing: dict[str, Any]) -> ProgramData | None:
        try:
            start_ms = int(airing["start"])
            end_ms = int(airing["end"])
        except (KeyError, TypeError, ValueError):
            return None

        if end_ms <= start_ms:
            return None

        metadata = airing.get("metadata") or {}
        title = (metadata.get("title") or "").strip() or "Unknown"
        synopsis = metadata.get("synopsis")
        poster = self._pick_image_url(metadata)
        rating = self._pick_rating(metadata)
        episode_title = metadata.get("episodeTitle") or None
        release_year = metadata.get("releaseYear")

        description = synopsis
        if release_year:
            description = f"{synopsis} ({release_year})" if synopsis else str(release_year)

        return ProgramData(
            source_channel_id=source_channel_id,
            title=title,
            start_time=datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc),
            end_time=datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc),
            description=description,
            poster_url=poster,
            category=self._guess_program_category(metadata),
            rating=rating,
            episode_title=episode_title,
        )

    @staticmethod
    def _pick_image_url(metadata: dict[str, Any]) -> str | None:
        for key in ("image", "modalImage"):
            image = metadata.get(key) or {}
            url = image.get("url")
            if url:
                return str(url)
        return None

    @staticmethod
    def _pick_rating(metadata: dict[str, Any]) -> str | None:
        rating = metadata.get("contentMaturityRating") or {}
        value = rating.get("rating")
        return str(value) if value else None

    @staticmethod
    def _guess_program_category(metadata: dict[str, Any]) -> str | None:
        badge = (metadata.get("linearBadge") or {}).get("label")
        if badge == "LIVE":
            return "Live"
        if badge == "ON NOW":
            return "Current"
        return None

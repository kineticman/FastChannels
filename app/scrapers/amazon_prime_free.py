
from __future__ import annotations

import html as _html
import json
import logging
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata
from .category_utils import infer_category_from_name

logger = logging.getLogger(__name__)


class AmazonPrimeFreeScraper(BaseScraper):
    """
    FastChannels scraper for Amazon Prime Video free linear (FAST) channels.

    - Scrapes channel metadata from the Live TV page and paginated collection API.
    - Builds near-term EPG from the station.schedule arrays embedded in those responses.
    - Resolves live DASH stream URLs via direct HTTP calls to Amazon's PRS endpoint
      (no browser required). All channels are bulk-resolved during scrape in ~30s using
      parallel requests. URLs are cached ~1.5 h in source.config.
    - Streams are CENC-encrypted DASH (Widevine + PlayReady); DRM-capable clients only
      (e.g. Kodi + inputstream.adaptive).
    """

    source_name = "amazon_prime_free"
    source_aliases = ("amazon-prime-free",)
    display_name = "Amazon Prime Free Channels"
    scrape_interval = 100  # minutes — keep well under the 2-hour DASH URL TTL

    phase_timeouts = {
        "init":      30,
        "bootstrap": 60,
        "channels":  180,   # bulk PRS resolution: ~30s typical, 180s generous ceiling
        "epg":       300,
    }

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

    PAGINATE_HEADERS = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.amazon.com/gp/video/livetv",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest",
        "X-Amzn-Client-Ttl-Seconds": "15",
    }

    # Observed in the analyzed HAR. Keep these centralized so they are easy to adjust.
    # The full dynamicFeatures list (matching what browsers send) is required for
    # hasMoreItems=True — a shorter list causes Amazon to truncate pagination at ~10 items.
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

    # Direct HTTP stream URL resolution (no browser required)
    _PRS_URL = "https://atv-ps.amazon.com/cdp/catalog/GetPlaybackResources"
    _STREAM_URL_TTL = 5400   # 1.5 hours — well under Amazon's 2-hour TTL
    _PRS_WORKERS = 20        # parallel HTTP workers for bulk resolution
    _PRS_TIMEOUT = 10        # per-request timeout (seconds)

    def __init__(self, config: dict | None = None):
        super().__init__(config)

        self._cookie_header = (self.config.get("cookie_header") or "").strip()
        self._marketplace_id = (self.config.get("marketplace_id") or "ATVPDKIKX0DER").strip()
        self._ux_locale = (self.config.get("ux_locale") or "en_US").strip()
        user_agent = (
            self.config.get("user_agent")
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
        )

        self.session.headers.update(self.DEFAULT_HEADERS)
        self.session.headers.update({"User-Agent": user_agent})
        if self._cookie_header:
            self.session.headers["Cookie"] = self._cookie_header

        # Stable device UUID for PRS calls — Amazon associates the deviceID with auth state.
        # Generate once and persist so we reuse the same identity across scrapes.
        self._prs_device_id: str = (
            self.config.get("prs_device_id") or str(uuid.uuid4())
        )
        if not self.config.get("prs_device_id"):
            self._pending_config_updates["prs_device_id"] = self._prs_device_id

        # PRS request headers (shared across all parallel workers)
        at_main = self._extract_cookie("at-main") or ""
        self._prs_headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Authorization": f"Bearer {at_main}",
            "Cookie": self._cookie_header,
            "Origin": "https://www.amazon.com",
            "Referer": "https://www.amazon.com/",
            "User-Agent": user_agent,
        }

        # fetch_channels() populates this; fetch_epg() reads from it.
        self._station_cache: dict[str, dict[str, Any]] = {}

        # Stream URL cache: {station_id: {"url": str, "expires_at": float}}
        # Persisted in source.config["stream_url_cache"] across scrapes.
        raw_cache = self.config.get("stream_url_cache") or {}
        self._stream_url_cache: dict[str, dict[str, Any]] = (
            raw_cache if isinstance(raw_cache, dict) else {}
        )

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

        # Bulk-resolve all stream URLs via direct PRS HTTP calls (~30 s for 884 channels).
        if self._cookie_header and channels:
            station_ids = [ch.source_channel_id for ch in channels]
            url_map = self._resolve_channels(station_ids)
            if url_map:
                expires_at = time.time() + self._STREAM_URL_TTL
                for gip, url in url_map.items():
                    self._stream_url_cache[gip] = {"url": url, "expires_at": expires_at}
                self._pending_config_updates["stream_url_cache"] = dict(self._stream_url_cache)
                logger.info("[%s] cached %d/%d stream URLs (TTL ~%.0f min)",
                            self.source_name, len(url_map), len(station_ids),
                            self._STREAM_URL_TTL / 60)

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

        station_id = raw_url[len("primefree://"):]
        cached = self._stream_url_cache.get(station_id)
        if cached and cached.get("expires_at", 0) > time.time():
            logger.debug("[%s] resolve cache hit for %s", self.source_name, station_id[:40])
            return cached["url"]

        # Cache miss or expired — resolve single channel on-demand.
        if not self._cookie_header:
            logger.warning("[%s] no cookie_header — cannot resolve stream URL for %s",
                           self.source_name, station_id[:40])
            return raw_url

        logger.info("[%s] cache miss — resolving stream URL for %s", self.source_name, station_id[:40])
        url_map = self._resolve_channels([station_id])
        if url_map.get(station_id):
            url = url_map[station_id]
            self._stream_url_cache[station_id] = {
                "url": url,
                "expires_at": time.time() + self._STREAM_URL_TTL,
            }
            updated = dict(self.config.get("stream_url_cache") or {})
            updated[station_id] = self._stream_url_cache[station_id]
            self._pending_config_updates["stream_url_cache"] = updated
            return url

        logger.warning("[%s] could not resolve stream URL for %s", self.source_name, station_id[:40])
        return raw_url

    # ------------------------------------------------------------------
    # Direct HTTP stream URL resolution (Amazon PRS endpoint)
    # ------------------------------------------------------------------

    def _extract_cookie(self, name: str) -> str | None:
        m = re.search(rf'(?:^|;\s*){re.escape(name)}=([^;]+)', self._cookie_header)
        return m.group(1).strip() if m else None

    def _resolve_channels(self, station_ids: list[str]) -> dict[str, str]:
        """
        Resolves DASH manifest URLs for the given station GIPs via direct HTTP calls
        to Amazon's GetPlaybackResources PRS endpoint.  Uses a thread pool for parallel
        resolution — typical throughput is 20+ channels/second.

        Returns {station_id: dash_url} for all successfully resolved channels.
        Streams are CENC-encrypted (Widevine + PlayReady); DRM-capable clients only.
        """
        if not self._cookie_header:
            return {}

        results: dict[str, str] = {}

        import requests as _requests  # local import to avoid shadowing module-level name

        def _resolve_one(gip: str) -> tuple[str, str]:
            try:
                r = _requests.get(
                    self._PRS_URL,
                    params={
                        "deviceID": self._prs_device_id,
                        "deviceTypeID": "AOAGZA014O5RE",
                        "gascEnabled": "false",
                        "marketplaceID": self._marketplace_id,
                        "uxLocale": self._ux_locale,
                        "firmware": "1",
                        "playerType": "xp",
                        "operatingSystemName": "Windows",
                        "operatingSystemVersion": "10.0",
                        "deviceApplicationName": "Chrome",
                        "asin": gip,
                        "consumptionType": "Streaming",
                        "desiredResources": "PlaybackUrls,PlaybackSettings",
                        "resourceUsage": "CacheResources",
                        "videoMaterialType": "LiveStreaming",
                        "userWatchSessionId": str(uuid.uuid4()),
                        "displayWidth": "1920",
                        "displayHeight": "1080",
                        "deviceStreamingTechnologyOverride": "DASH",
                        "deviceDrmOverride": "CENC",
                        "deviceAdInsertionTypeOverride": "SSAI",
                        "deviceVideoCodecOverride": "H264",
                        "deviceVideoQualityOverride": "HD",
                        "liveManifestType": "accumulating,live",
                        "playerAttributes": json.dumps({
                            "middlewareName": "Chrome",
                            "middlewareVersion": "146.0.0.0",
                            "nativeApplicationName": "Chrome",
                            "nativeApplicationVersion": "146.0.0.0",
                            "supportedAudioCodecs": "AAC",
                            "frameRate": "HFR",
                            "H264.codecLevel": "4.2",
                            "H265.codecLevel": "0.0",
                            "AV1.codecLevel": "0.0",
                        }, separators=(",", ":")),
                    },
                    headers=self._prs_headers,
                    timeout=self._PRS_TIMEOUT,
                )
                body = r.json()
            except Exception as exc:
                logger.debug("[%s] PRS request failed for %s: %s", self.source_name, gip[:40], exc)
                return gip, ""

            url_sets = body.get("playbackUrls", {}).get("urlSets", {})
            if not url_sets:
                err = body.get("errorsByResource", {}).get("PlaybackUrls", {}).get("errorCode", "")
                if err:
                    logger.debug("[%s] PRS error for %s: %s", self.source_name, gip[:40], err)
                return gip, ""

            # Prefer Qwilt CDN (clean URL, no obfuscating auth tokens in path)
            for sdata in url_sets.values():
                m = sdata.get("urls", {}).get("manifest", {})
                if m.get("cdn") == "Qwilt" and m.get("url"):
                    return gip, m["url"]
            # Fall back to default or first available
            default_id = body.get("playbackUrls", {}).get("defaultUrlSetId", "")
            manifest = (
                url_sets.get(default_id, {}).get("urls", {}).get("manifest", {}).get("url", "")
                or next(
                    (s.get("urls", {}).get("manifest", {}).get("url", "")
                     for s in url_sets.values()
                     if s.get("urls", {}).get("manifest", {}).get("url")),
                    "",
                )
            )
            return gip, manifest

        logger.info("[%s] resolving %d stream URLs via PRS (%d workers)...",
                    self.source_name, len(station_ids), self._PRS_WORKERS)
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=self._PRS_WORKERS) as pool:
            for gip, url in pool.map(_resolve_one, station_ids):
                if url:
                    results[gip] = url
        elapsed = time.time() - t0
        logger.info("[%s] PRS resolved %d/%d stream URLs in %.1fs",
                    self.source_name, len(results), len(station_ids), elapsed)
        return results

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

        logger.info("[%s] extracted %d stations from initial HTML", self.source_name, len(stations))
        return stations

    def _extract_pagination_seed(self, html: str) -> dict[str, Any] | None:
        # All three fields live in the same JSON object in the page HTML.
        # There may be multiple pagination blocks (e.g. "Live events" and "Your stations").
        # We want the one whose EpgGroup entities contain station objects.
        pattern = re.compile(
            r'"paginationServiceToken":"(?P<token>[^"]+)"'
            r'[^}]{0,300}?"paginationStartIndex":(?P<start>\d+)'
            r'[^}]{0,300}?"paginationTargetId":"(?P<target>[^"]+)"',
            re.DOTALL,
        )
        best = None
        for m in pattern.finditer(html):
            # Check if station objects appear in the 5000 chars following this block —
            # that indicates this is the linear-station carousel, not a content carousel.
            window = html[m.start(): m.start() + 5000]
            if '"station":{' in window:
                best = m
                break  # take the first block that has station entities after it

        if best is None:
            # Fallback: use the last pagination block found (stations tend to be last)
            matches = list(pattern.finditer(html))
            if not matches:
                return None
            best = matches[-1]

        return {
            "start_index": int(best.group("start")),
            "pagination_target_id": best.group("target"),
            "service_token": best.group("token"),
        }

    def _paginate_stations(self, seed: dict[str, Any]) -> dict[str, dict[str, Any]]:
        stations: dict[str, dict[str, Any]] = {}
        start_index = int(seed["start_index"])
        pagination_target_id = seed["pagination_target_id"]
        service_token = seed["service_token"]
        has_more = True
        page_no = 0

        while has_more and page_no < 200:
            params = dict(self.PAGINATE_DEFAULT_PARAMS)
            params.update(
                {
                    "paginationTargetId": pagination_target_id,
                    "serviceToken": service_token,
                    "startIndex": str(start_index),
                }
            )

            response = self.get(self.PAGINATE_URL, params=params, headers=self.PAGINATE_HEADERS)
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
            logger.debug("[%s] page startIndex=%s: %d entities, hasMoreItems=%s",
                         self.source_name, start_index, len(entities), payload.get("hasMoreItems"))
            for entity in entities:
                station = entity.get("station") or {}
                station_id = self._station_id(station)
                if station_id:
                    stations[station_id] = station

            has_more = bool(payload.get("hasMoreItems"))

            # Amazon returns an updated serviceToken in each response — must use it
            # for the next request or subsequent pages return empty results.
            pagination = payload.get("pagination") or {}
            next_token = pagination.get("serviceToken") or pagination.get("token")
            if next_token:
                service_token = next_token

            next_index = payload.get("startIndex")
            if has_more:
                if isinstance(next_index, int) and next_index > start_index:
                    start_index = next_index
                else:
                    start_index += len(entities)
                    if not entities:
                        break
            page_no += 1

        logger.info("[%s] extracted %d stations from pagination (%d pages)", self.source_name, len(stations), page_no)
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


from __future__ import annotations

import html as _html
import json
import logging
import re
import time
import urllib.parse
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
    - Stream URLs are CENC-encrypted DASH (Widevine + PlayReady); resolved lazily on
      first play via resolve() using Playwright (cached ~1.5 h in source.config).
      DRM-capable clients only (e.g. Kodi + inputstream.adaptive).
    """

    source_name = "amazon_prime_free"
    source_aliases = ("amazon-prime-free",)
    display_name = "Amazon Prime Free Channels"
    scrape_interval = 100  # minutes — keep well under the 2-hour DASH URL TTL

    phase_timeouts = {
        "init":      30,
        "bootstrap": 60,
        "channels":  120,
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

    # Playwright-based stream URL resolution
    _PW_ROUTE_PATTERN = "**/*.amazon.com/**"
    _DESIRED_RESOURCES = "PlaybackUrls,PlaybackSettings"
    _STREAM_URL_TTL = 5400  # 1.5 hours — well under Amazon's 2-hour TTL
    _PW_INIT_SCRIPT = """
    let _sdk = null;
    Object.defineProperty(window, 'ATVWebPlayerSDK', {
        get() { return _sdk; },
        set(val) {
            if (typeof val === 'function') {
                const Orig = val;
                function Patched(config) {
                    const inst = new Orig(config);
                    window.__sdkInst = inst;
                    return inst;
                }
                Patched.prototype = Orig.prototype;
                _sdk = Patched;
            } else { _sdk = val; }
        },
        configurable: true,
    });
    """

    def __init__(self, config: dict | None = None):
        super().__init__(config)

        self._cookie_header = (self.config.get("cookie_header") or "").strip()
        self._marketplace_id = (self.config.get("marketplace_id") or "ATVPDKIKX0DER").strip()
        self._ux_locale = (self.config.get("ux_locale") or "en_US").strip()
        self._user_agent = (
            self.config.get("user_agent")
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
        )

        self.session.headers.update(self.DEFAULT_HEADERS)
        self.session.headers.update({"User-Agent": self._user_agent})
        if self._cookie_header:
            self.session.headers["Cookie"] = self._cookie_header

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

        # Prune expired stream URL cache entries so the stored config doesn't grow unbounded.
        # Active (non-expired) entries are kept — they will be served by resolve() as-is.
        # New URLs are resolved lazily on first play via resolve(), not pre-cached here.
        # (Bulk pre-resolution is infeasible: the SDK serializes requests at ~13 s/channel,
        #  so 800+ channels would take ~3 hours — far beyond any scrape phase timeout.)
        now = time.time()
        valid_cache = {
            gip: entry for gip, entry in self._stream_url_cache.items()
            if entry.get("expires_at", 0) > now
        }
        if len(valid_cache) != len(self._stream_url_cache):
            pruned = len(self._stream_url_cache) - len(valid_cache)
            logger.info("[%s] pruned %d expired stream URL cache entries (%d still valid)",
                        self.source_name, pruned, len(valid_cache))
            self._stream_url_cache = valid_cache
            self._pending_config_updates["stream_url_cache"] = dict(valid_cache)

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

        # Cache miss or expired — run single-channel Playwright resolution.
        if not self._cookie_header:
            logger.warning("[%s] no cookie_header — cannot resolve stream URL for %s",
                           self.source_name, station_id[:40])
            return raw_url

        logger.info("[%s] cache miss — resolving stream URL for %s", self.source_name, station_id[:40])
        url_map = self._playwright_resolve_channels([station_id])
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
    # Playwright-based live stream URL resolution
    # ------------------------------------------------------------------

    def _extract_cookie(self, name: str) -> str | None:
        m = re.search(rf'(?:^|;\s*){re.escape(name)}=([^;]+)', self._cookie_header)
        return m.group(1).strip() if m else None

    def _cookie_header_to_list(self) -> list[dict]:
        cookies = []
        for part in self._cookie_header.split(";"):
            part = part.strip()
            if "=" not in part:
                continue
            name, _, value = part.partition("=")
            cookies.append({"name": name.strip(), "value": value.strip(),
                             "domain": ".amazon.com", "path": "/"})
        return cookies

    def _playwright_resolve_channels(self, station_ids: list[str]) -> dict[str, str]:
        """
        Launches a Playwright browser, loads amazon.com/gp/video/livetv, and calls
        ATVWebPlayerSDK.requestResources() for each station GIP.  The route
        intercept forces desiredResources=PlaybackUrls,PlaybackSettings so we get
        DASH manifest URLs in the response.

        Returns {station_id: dash_url} for all successfully resolved channels.
        Streams are CENC-encrypted (Widevine + PlayReady); DRM-capable clients only.
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.warning("[%s] playwright not installed — stream URL resolution unavailable",
                           self.source_name)
            return {}

        at_main = self._extract_cookie("at-main")
        if not at_main:
            logger.warning("[%s] at-main cookie not found — cannot authenticate SDK",
                           self.source_name)
            return {}

        auth_ctx = {
            "headers": {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Authorization": f"Bearer {at_main}",
            }
        }
        results: dict[str, str] = {}

        def _on_response(response):
            url = response.url
            if "GetPlaybackResources" not in url or response.status != 200:
                return
            try:
                body = response.json()
                pu = body.get("playbackUrls", {})
                if not pu:
                    return
                url_sets = pu.get("urlSets", {})
                default_id = pu.get("defaultUrlSetId", "")
                manifest_url = (
                    url_sets.get(default_id, {})
                    .get("urls", {})
                    .get("manifest", {})
                    .get("url", "")
                )
                if not manifest_url:
                    # Prefer Qwilt CDN (no obfuscating auth token in URL path)
                    for sdata in url_sets.values():
                        m = sdata.get("urls", {}).get("manifest", {})
                        if m.get("cdn") == "Qwilt" and m.get("url"):
                            manifest_url = m["url"]
                            break
                if not manifest_url:
                    for sdata in url_sets.values():
                        manifest_url = sdata.get("urls", {}).get("manifest", {}).get("url", "")
                        if manifest_url:
                            break
                if manifest_url:
                    params = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(url).query))
                    station_id = params.get("asin", "")
                    if station_id:
                        results[station_id] = manifest_url
            except Exception as exc:
                logger.debug("[%s] PRS response parse error: %s", self.source_name, exc)

        def _handle_route(route):
            url = route.request.url
            if "GetPlaybackResources" in url and "videoMaterialType=LiveStreaming" in url:
                parsed = urllib.parse.urlparse(url)
                params = dict(urllib.parse.parse_qsl(parsed.query))
                params["desiredResources"] = self._DESIRED_RESOURCES
                new_url = parsed._replace(query=urllib.parse.urlencode(params)).geturl()
                route.continue_(url=new_url)
            else:
                route.continue_()

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox",
                          "--disable-dev-shm-usage", "--disable-gpu"],
                )
                ctx = browser.new_context(
                    user_agent=self._user_agent,
                    viewport={"width": 1920, "height": 1080},
                    locale="en-US",
                    timezone_id="America/New_York",
                    extra_http_headers={
                        "sec-ch-ua": '"Chromium";v="146", "Google Chrome";v="146", "Not-A.Brand";v="24"',
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": '"Windows"',
                    },
                )
                ctx.add_cookies(self._cookie_header_to_list())

                page = ctx.new_page()
                page.on("response", _on_response)
                page.route(self._PW_ROUTE_PATTERN, _handle_route)
                page.add_init_script(self._PW_INIT_SCRIPT)

                logger.info("[%s] loading livetv page (Playwright)...", self.source_name)
                page.goto("https://www.amazon.com/gp/video/livetv",
                          wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(4000)
                for _ in range(8):
                    page.evaluate("window.scrollBy(0, 500)")
                    page.wait_for_timeout(200)
                page.wait_for_timeout(2000)

                try:
                    page.wait_for_function("!!window.__sdkInst", timeout=15000)
                except Exception:
                    logger.warning("[%s] ATVWebPlayerSDK not found on livetv page", self.source_name)
                    browser.close()
                    return {}

                page.evaluate(
                    "(ctx) => { const i = window.__sdkInst; if (i) i.setAuthContext(ctx); }",
                    auth_ctx,
                )
                page.wait_for_timeout(500)

                # Fire requestResources for all channels.
                # The SDK dispatches each call as an independent async HTTP request,
                # so we fire rapidly and then wait for all responses to arrive.
                logger.info("[%s] firing requestResources for %d channels...",
                            self.source_name, len(station_ids))
                for i, gip in enumerate(station_ids):
                    page.evaluate(
                        """(gip) => {
                            const inst = window.__sdkInst;
                            const cp = inst && inst.constructedPlayers;
                            const p = cp && (cp[0] || Object.values(cp)[0]);
                            if (p && p.player) {
                                p.player.requestResources({
                                    titleId: gip,
                                    videoMaterialType: "LiveStreaming",
                                });
                            }
                        }""",
                        gip,
                    )
                    page.wait_for_timeout(80)

                    if (i + 1) % 100 == 0:
                        logger.debug("[%s] fired %d/%d requests, %d resolved so far",
                                     self.source_name, i + 1, len(station_ids), len(results))

                # Wait for in-flight PRS responses to arrive
                logger.info("[%s] all %d requests fired, waiting for responses...",
                            self.source_name, len(station_ids))
                deadline = time.time() + 120  # up to 2 min for responses to drain
                while len(results) < len(station_ids) and time.time() < deadline:
                    page.wait_for_timeout(2000)
                    logger.debug("[%s] %d/%d resolved",
                                 self.source_name, len(results), len(station_ids))

                page.close()
                ctx.close()
                browser.close()

        except Exception as exc:
            logger.error("[%s] Playwright resolution failed: %s", self.source_name, exc)

        logger.info("[%s] Playwright resolved %d/%d stream URLs",
                    self.source_name, len(results), len(station_ids))
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

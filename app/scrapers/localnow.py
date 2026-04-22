from __future__ import annotations

import base64
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

_CALL_SIGN_RE = re.compile(r'^[WK][A-Z]{2,4}\b')
_LOCALNOW_COMPOSITE_TITLE_RE = re.compile(
    r'^(?P<series>.+):\s+S(?P<season>\d+)\s+E(?P<episode>\d+)\s*-\s*(?P<episode_title>.+?)\s*$',
    re.IGNORECASE,
)
_LOCALNOW_SE_RATING_RE = re.compile(
    r'^(?P<series>.+):\s+S(?P<season>\d+)\s+E(?P<episode>\d+)\s*\([^)]*\)\s*$',
    re.IGNORECASE,
)
_LOCALNOW_EPISODE_SUBTITLE_RE = re.compile(
    r'^(?P<series>.+?)\s[-–]\sEpisode\s(?P<episode>\d+)\s[-–]\s(?P<episode_title>.+?)\s*$',
    re.IGNORECASE,
)
_LOCALNOW_EPISODE_ONLY_RE = re.compile(
    r'^(?P<series>.+?)(?::\s*|,\s*|\s[-–]\s)Episode\s(?P<episode>\d+)\s*$',
    re.IGNORECASE,
)
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata
from .category_utils import infer_category_from_name

logger = logging.getLogger(__name__)


def _parse_localnow_title(
    raw: Optional[str],
    api_season: Optional[int],
    api_episode: Optional[int],
    api_episode_title: Optional[str],
) -> Tuple[Optional[str], Optional[int], Optional[int], Optional[str]]:
    """Normalize Local Now composite titles into XMLTV-friendly fields."""
    if not raw:
        return raw, api_season, api_episode, api_episode_title

    title = raw.strip()
    match = _LOCALNOW_COMPOSITE_TITLE_RE.match(title)
    if match:
        series_title = match.group("series").strip()
        season = int(match.group("season"))
        episode = int(match.group("episode"))
        episode_title = (api_episode_title or match.group("episode_title") or "").strip() or None
        return series_title, season, episode, episode_title

    match = _LOCALNOW_SE_RATING_RE.match(title)
    if match:
        return (
            match.group("series").strip(),
            int(match.group("season")),
            int(match.group("episode")),
            api_episode_title,
        )

    match = _LOCALNOW_EPISODE_SUBTITLE_RE.match(title)
    if match:
        episode = int(match.group("episode"))
        episode_title = (api_episode_title or match.group("episode_title") or "").strip() or None
        return match.group("series").strip(), api_season, api_episode or episode, episode_title

    match = _LOCALNOW_EPISODE_ONLY_RE.match(title)
    if match:
        episode = int(match.group("episode"))
        episode_title = (api_episode_title or f"Episode {episode}").strip() or None
        return match.group("series").strip(), api_season, api_episode or episode, episode_title

    return title, api_season, api_episode, api_episode_title


class LocalNowScraper(BaseScraper):
    """
    FastChannels scraper for Local Now.

    Design notes:
    - No network calls in __init__
    - Bootstraps runtime config from https://localnow.com/
    - Extracts DSP_TOKEN from __NEXT_DATA__
    - Uses live/epg endpoint for channels + inline programme data
    - Stores opaque internal URI in stream_url and resolves at playback time
    """

    source_name = "localnow"
    display_name = "Local Now"
    scrape_interval = 60  # API returns exactly 5 programs per channel; worst channels cover ~1h, so scrape hourly for continuous EPG
    stream_audit_enabled = True
    config_required = True  # local city channels are the point; without config defaults to New York

    config_schema = [
        ConfigField(
            "dma",
            "DMA",
            field_type="text",
            required=False,
            help_text="Optional DMA override. Leave blank to auto-discover from Local Now bootstrap.",
            default="",
        ),
        ConfigField(
            "market",
            "Market",
            field_type="text",
            required=False,
            help_text="Optional market override such as ohColumbus,pbs-wosu. Leave blank to auto-discover.",
            default="",
        ),
        ConfigField(
            "program_size",
            "Program Size",
            field_type="number",
            required=False,
            help_text="Requested programme rows per channel sent to the Local Now API. The API currently ignores this and always returns exactly 5 programs per channel regardless of this value.",
            default=10,
        ),
        ConfigField(
            "resolve_best_variant",
            "Resolve Best Variant",
            field_type="toggle",
            required=False,
            help_text="If enabled, resolve master playlists to the highest-bandwidth media playlist at play time.",
            default=True,
        ),
        ConfigField(
            "prefer_session_m3u8",
            "Prefer Session M3U8",
            field_type="toggle",
            required=False,
            help_text="If enabled, use session_m3u8 before video_m3u8 when both are present.",
            default=False,
        ),
    ]

    HOME_URL = "https://localnow.com/"
    CHANNELS_PAGE_URL = "https://localnow.com/channels"
    EPG_URL_TMPL = "https://{host}/live/epg/US/website"
    PLAY_URL_TMPL = "https://{host}/video/play/{video_id}/{width}/{height}"
    CITY_SEARCH_URL = "https://prod.localnowapi.com/gis/api/v2/City/Search"

    def __init__(self, config: dict = None):
        super().__init__(config)

        self.session.trust_env = False
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://localnow.com",
                "Referer": "https://localnow.com/",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/145.0.0.0 Safari/537.36"
                ),
            }
        )

        self._runtime_bootstrapped = False
        self._runtime_config: Dict[str, Any] = {}
        self._token: Optional[str] = None
        self._token_expires: Optional[datetime] = None
        self._dsp_host: str = "data-store-trans-cdn.api.cms.amdvids.com"
        self._ln_api_key: Optional[str] = None

        self._dma: Optional[str] = None
        self._market: Optional[str] = None
        self._program_size: int = self._safe_int(self.config.get("program_size"), 10)
        self._resolve_best_variant: bool = self._safe_bool(self.config.get("resolve_best_variant"), True)
        self._prefer_session_m3u8: bool = self._safe_bool(self.config.get("prefer_session_m3u8"), False)

        self._channels_payload_cache: Optional[List[Dict[str, Any]]] = None
        self._channels_by_id: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Required scraper methods
    # ------------------------------------------------------------------

    def fetch_channels(self) -> list[ChannelData]:
        self._ensure_runtime_bootstrapped()
        payload_channels = self._fetch_epg_payload_channels()

        channels: List[ChannelData] = []
        self._channels_by_id = {}
        seen_ids: set[str] = set()

        for ch in payload_channels:
            source_channel_id = str(ch.get("video_id") or ch.get("_id") or "").strip()
            if not source_channel_id or source_channel_id in seen_ids:
                continue
            seen_ids.add(source_channel_id)

            # Skip channels that require a subscription (not free to watch)
            sub_access = ch.get("subscription_access") or {}
            if isinstance(sub_access, dict) and sub_access.get("unlocked") is False:
                continue

            slug = (ch.get("slug") or "").strip()
            name = (ch.get("name") or "").strip() or source_channel_id
            genres = ch.get("genres") or []
            iab_genres = ch.get("iab_genres") or []
            # Detect local broadcast stations:
            #  - 'My City' genre from the API
            #  - slug contains 'hyperlocal' or 'local-now' (LocalNow marker)
            #  - name contains a TV call sign anywhere (W/K + 2-4 letters, e.g. WXYZ,
            #    KERO) — covers "WTAE", "KCRA-TV", "Very Pittsburgh by WTAE", etc.
            slug_lower = slug.lower()
            if (
                "My City" in genres
                or "hyperlocal" in slug_lower
                or slug_lower.startswith("epg-local-now")
                or _CALL_SIGN_RE.search(name)
            ):
                category = "Local News"
            else:
                category = (
                    (iab_genres[0] if iab_genres else None)
                    or (genres[0] if genres else None)
                    or infer_category_from_name(name)
                )

            raw_url = f"localnow://{source_channel_id}"
            if slug:
                raw_url += f"?slug={quote(slug, safe='')}"

            description = (ch.get("description") or "").strip() or None

            channels.append(
                ChannelData(
                    source_channel_id=source_channel_id,
                    name=name,
                    stream_url=raw_url,
                    logo_url=ch.get("logo"),
                    slug=slug or None,
                    category=category,
                    language=infer_language_from_metadata(ch.get("language"), name, category),
                    country="US",
                    stream_type="hls",
                    number=self._safe_int(ch.get("channel_number"), None),
                    description=description,
                )
            )
            self._channels_by_id[source_channel_id] = ch

        logger.info("[localnow] %d channels fetched", len(channels))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        self._ensure_runtime_bootstrapped()
        if not self._channels_by_id:
            self._fetch_epg_payload_channels()

        programs: List[ProgramData] = []
        wanted_ids = {str(ch.source_channel_id) for ch in channels}

        for source_channel_id in wanted_ids:
            payload = self._channels_by_id.get(source_channel_id)
            if not payload:
                continue

            for item in payload.get("program") or []:
                start_ts = item.get("starts_at")
                end_ts = item.get("ends_at")
                if start_ts is None or end_ts is None:
                    continue

                try:
                    start_dt = datetime.fromtimestamp(int(start_ts), tz=timezone.utc)
                    end_dt = datetime.fromtimestamp(int(end_ts), tz=timezone.utc)
                except Exception:
                    continue

                raw_title = (item.get("program_title") or payload.get("name") or "Unknown").strip()
                api_episode_title = (item.get("episode_title") or "").strip() or None
                title, season, episode, episode_title = _parse_localnow_title(
                    raw_title,
                    self._safe_int(item.get("season"), None),
                    self._safe_int(item.get("episode"), None),
                    api_episode_title,
                )

                programs.append(
                    ProgramData(
                        source_channel_id=source_channel_id,
                        title=title or "Unknown",
                        start_time=start_dt,
                        end_time=end_dt,
                        description=(item.get("program_description") or payload.get("description") or "").strip() or None,
                        poster_url=(item.get("image") or payload.get("poster") or None),
                        category=((payload.get("iab_genres") or payload.get("genres") or [None])[0]),
                        rating=(payload.get("rating") or None),
                        episode_title=episode_title,
                        season=season,
                        episode=episode,
                    )
                )

        logger.info("[localnow] %d EPG entries fetched", len(programs))
        return programs

    def audit_resolve(self, raw_url: str) -> str:
        """Like resolve() but always returns the master playlist URL so the
        stream inspector and audit can see all variants (stats for nerds)."""
        if not raw_url.startswith("localnow://"):
            return raw_url
        self._ensure_runtime_bootstrapped()
        source_channel_id, slug = self._parse_internal_url(raw_url)
        playback = self._fetch_playback(source_channel_id=source_channel_id, slug=slug)
        preferred = (
            [playback.get("session_m3u8"), playback.get("video_m3u8")]
            if self._prefer_session_m3u8
            else [playback.get("video_m3u8"), playback.get("session_m3u8")]
        )
        return next((u for u in preferred if u), raw_url)

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("localnow://"):
            return raw_url

        self._ensure_runtime_bootstrapped()
        source_channel_id, slug = self._parse_internal_url(raw_url)
        playback = self._fetch_playback(source_channel_id=source_channel_id, slug=slug)

        preferred = []
        if self._prefer_session_m3u8:
            preferred = [playback.get("session_m3u8"), playback.get("video_m3u8")]
        else:
            preferred = [playback.get("video_m3u8"), playback.get("session_m3u8")]

        stream_url = next((u for u in preferred if u), None)
        if not stream_url:
            logger.warning("[localnow] no playback URL for %s", source_channel_id)
            return raw_url

        if self._resolve_best_variant:
            try:
                stream_url = self._resolve_best_variant_url(stream_url)
            except Exception as exc:
                logger.warning("[localnow] variant resolution failed for %s: %s", source_channel_id, exc)

        return stream_url

    # ------------------------------------------------------------------
    # Runtime bootstrap
    # ------------------------------------------------------------------

    def _ensure_runtime_bootstrapped(self) -> None:
        now = datetime.now(timezone.utc)
        if (
            self._runtime_bootstrapped
            and self._token
            and self._token_expires
            and now < self._token_expires - timedelta(minutes=2)
        ):
            return

        logger.info("[localnow] bootstrapping runtime config from homepage")
        resp = self.session.get(self.HOME_URL, timeout=20)
        resp.raise_for_status()

        next_data = self._extract_next_data(resp.text)
        runtime_config = next_data.get("runtimeConfig") or {}
        if not runtime_config:
            raise RuntimeError("Local Now runtimeConfig missing from __NEXT_DATA__")

        self._runtime_config = runtime_config
        self._dsp_host = runtime_config.get("DSP_API_URL", self._dsp_host)
        self._ln_api_key = runtime_config.get("LN_API_KEY")

        token_raw = runtime_config.get("DSP_TOKEN")
        if not token_raw:
            raise RuntimeError("Local Now DSP_TOKEN missing from runtime config")

        try:
            token_obj = json.loads(token_raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Failed to parse Local Now DSP_TOKEN JSON: {exc}") from exc

        token = token_obj.get("token")
        if not token:
            raise RuntimeError("Local Now token missing inside DSP_TOKEN")

        self._token = token
        self._token_expires = self._decode_jwt_exp(token) or (now + timedelta(hours=12))
        self.session.headers["x-access-token"] = token

        configured_dma = (self.config.get("dma") or "").strip()
        configured_market = (self.config.get("market") or "").strip()

        if configured_dma and configured_market:
            self._dma = configured_dma
            self._market = configured_market
        else:
            self._dma, self._market = self._discover_dma_market(next_data)

            if configured_dma:
                self._dma = configured_dma
            if configured_market:
                self._market = configured_market

        if not self._dma or not self._market:
            # Known-good fallback — New York City.
            self._dma = self._dma or "501"
            self._market = self._market or "nyNewYorkCity,pbs-wnet,pbs-wedh,pbs-wliw,pbs-wnjt"

        self._runtime_bootstrapped = True
        self._channels_payload_cache = None

        logger.info(
            "[localnow] runtime bootstrapped host=%s dma=%s market=%s token_exp=%s",
            self._dsp_host,
            self._dma,
            self._market,
            self._token_expires.isoformat() if self._token_expires else "unknown",
        )

    @staticmethod
    def _extract_next_data(html: str) -> Dict[str, Any]:
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">\s*(.*?)\s*</script>',
            html,
            re.S,
        )
        if not m:
            raise RuntimeError("Could not find __NEXT_DATA__ in Local Now homepage")
        return json.loads(m.group(1))

    def _discover_dma_market(self, next_data: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        page_props = next_data.get("props", {}).get("pageProps", {}) or {}
        server_cookies = page_props.get("serverCookies", {}) or {}

        dma = None
        market_parts: List[str] = []

        my_market = server_cookies.get("_ln_myMarket")
        my_city = server_cookies.get("_ln_myCity")
        detected_city = server_cookies.get("_ln_myDetectedCity")

        dma = self._extract_dma_id(my_market) or self._extract_dma_id(detected_city) or self._extract_dma_id(my_city)

        city_slug = (
            self._extract_market_slug(my_market)
            or self._extract_market_slug(my_city)
            or self._extract_market_slug(detected_city)
        )
        if city_slug:
            market_parts.append(city_slug)

        # Pull PBS market if present.
        pbs_markets = None
        config = page_props.get("config", {}) or {}
        local_now_cfg = config.get("localNow", {}) or {}
        pbs_markets = local_now_cfg.get("pbsMarkets") or server_cookies.get("_ln_pbsMarkets")
        if isinstance(pbs_markets, str) and pbs_markets.strip():
            market_parts.extend([x.strip() for x in pbs_markets.split(",") if x.strip()])

        market = ",".join(dict.fromkeys(market_parts)) if market_parts else None
        return dma, market

    @staticmethod
    def _extract_dma_id(value: Any) -> Optional[str]:
        if not value:
            return None
        if isinstance(value, dict):
            for key in ("dmaId", "dma_id", "dma"):
                if value.get(key):
                    return str(value.get(key))
            return None
        s = str(value)
        m = re.search(r'"?dmaId"?\s*[:=]\s*"?(?P<dma>\d+)', s)
        if m:
            return m.group("dma")
        m = re.search(r'\b(\d{3,4})\b', s)
        return m.group(1) if m else None

    @staticmethod
    def _extract_market_slug(value: Any) -> Optional[str]:
        if not value:
            return None
        if isinstance(value, dict):
            for key in ("market", "slug", "citySlug", "marketSlug"):
                if value.get(key):
                    return str(value.get(key))
            return None
        s = str(value)
        m = re.search(r'"?(market|slug|citySlug|marketSlug)"?\s*[:=]\s*"?(?P<slug>[A-Za-z0-9_-]+)', s)
        if m:
            return m.group("slug")
        m = re.search(r'\b([a-z]{2}[A-Z][A-Za-z0-9]+)\b', s)
        return m.group(1) if m else None

    @staticmethod
    def _decode_jwt_exp(token: str) -> Optional[datetime]:
        try:
            parts = token.split(".")
            if len(parts) < 2:
                return None
            payload = parts[1]
            payload += "=" * (-len(payload) % 4)
            data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
            exp = data.get("exp")
            if not exp:
                return None
            return datetime.fromtimestamp(int(exp), tz=timezone.utc)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # City search (for admin UI)
    # ------------------------------------------------------------------

    def search_cities(self, query: str) -> List[Dict[str, Any]]:
        """Search cities/markets via the Local Now GIS API.

        Returns a list of dicts with keys: label, dma, market.
        """
        self._ensure_runtime_bootstrapped()
        if not self._ln_api_key:
            logger.warning("[localnow] LN_API_KEY not available — city search unavailable")
            return []

        resp = self.session.get(
            self.CITY_SEARCH_URL,
            params={"text": query},
            headers={"x-api-key": self._ln_api_key},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        results = []
        for city in (data if isinstance(data, list) else []):
            market = (city.get("market") or "").strip()
            pbs = (city.get("pbsMarkets") or "").strip()
            combined_market = ",".join(filter(None, [market, pbs]))
            if not combined_market:
                continue
            dma = str(city.get("dmaId") or city.get("zipDma") or "")
            results.append({
                "label":  city.get("cityStateName") or city.get("name") or "Unknown",
                "dma":    dma,
                "market": combined_market,
            })
        return results

    # ------------------------------------------------------------------
    # Upstream fetchers
    # ------------------------------------------------------------------

    def _fetch_epg_payload_channels(self) -> List[Dict[str, Any]]:
        if self._channels_payload_cache is not None:
            return self._channels_payload_cache

        self._ensure_runtime_bootstrapped()
        epg_url = self.EPG_URL_TMPL.format(host=self._dsp_host)
        params = {
            "program_size": str(self._program_size),
            "dma": self._dma,
            "market": self._market,
        }

        resp = self.session.get(epg_url, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        channels = payload.get("channels") or []
        self._channels_payload_cache = channels
        self._channels_by_id = {
            str(ch.get("video_id") or ch.get("_id")): ch
            for ch in channels
            if ch.get("video_id") or ch.get("_id")
        }

        logger.info("[localnow] fetched %d channels from live EPG", len(channels))
        return channels

    def _fetch_playback(self, source_channel_id: str, slug: Optional[str] = None) -> Dict[str, Any]:
        self._ensure_runtime_bootstrapped()

        play_url = self.PLAY_URL_TMPL.format(
            host=self._dsp_host,
            video_id=source_channel_id,
            width=1920,
            height=1080,
        )

        if not slug:
            ch = self._channels_by_id.get(source_channel_id)
            slug = (ch or {}).get("slug")

        page_url = f"{self.CHANNELS_PAGE_URL}/{slug}" if slug else self.CHANNELS_PAGE_URL
        params = {
            "page_url": quote(page_url, safe=""),
            "device_devicetype": "desktop_web",
            "app_version": "0.0.0",
            "app_bundle": "web.localnow",
            "ccpa_us_privacy": "1YNY",
        }

        resp = self.session.get(play_url, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # HLS helpers
    # ------------------------------------------------------------------

    def _resolve_best_variant_url(self, url: str) -> str:
        resp = self.session.get(url, timeout=15)
        resp.raise_for_status()
        text = resp.text

        if "#EXT-X-STREAM-INF" not in text:
            return url

        variants = self._parse_master_variants(base_url=url, text=text)
        if not variants:
            return url

        best = sorted(variants, key=lambda item: item[0], reverse=True)[0]
        return best[1]

    @staticmethod
    def _parse_master_variants(base_url: str, text: str) -> List[Tuple[int, str]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        variants: List[Tuple[int, str]] = []

        for i, line in enumerate(lines):
            if not line.startswith("#EXT-X-STREAM-INF:"):
                continue

            bw = 0
            m = re.search(r"BANDWIDTH=(\d+)", line)
            if m:
                bw = int(m.group(1))

            j = i + 1
            while j < len(lines) and lines[j].startswith("#"):
                j += 1
            if j >= len(lines):
                continue

            child_url = urljoin(base_url, lines[j])
            variants.append((bw, child_url))

        return variants

    # ------------------------------------------------------------------
    # Internal URL helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_internal_url(raw_url: str) -> Tuple[str, Optional[str]]:
        value = raw_url.split("localnow://", 1)[1]
        if "?" not in value:
            return value, None

        source_channel_id, query = value.split("?", 1)
        slug = None
        for chunk in query.split("&"):
            if not chunk:
                continue
            if "=" not in chunk:
                continue
            key, val = chunk.split("=", 1)
            if key == "slug":
                slug = val
                break
        return source_channel_id, slug

    @staticmethod
    def _safe_int(value: Any, default: Optional[int]) -> Optional[int]:
        try:
            if value is None or value == "":
                return default
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _safe_bool(value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        if s in {"1", "true", "yes", "on"}:
            return True
        if s in {"0", "false", "no", "off"}:
            return False
        return default

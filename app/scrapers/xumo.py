from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from .base import BaseScraper, ChannelData, ProgramData, infer_language_from_metadata

logger = logging.getLogger(__name__)


class XumoScraper(BaseScraper):
    """
    Xumo Play scraper for FastChannels.

    Design notes:
    - fetch_channels() stores an opaque raw URL: xumo://channel/<channel_id>
    - fetch_epg() uses Valencia web EPG endpoints
    - resolve() performs live channel -> broadcast -> asset -> provider source resolution

    This intentionally mirrors the Xumo Play web flow rather than older community
    scripts that mixed Valencia and Android TV endpoints.
    """

    source_name = "xumo"
    display_name = "Xumo Play"
    scrape_interval = 720
    stream_audit_enabled = True
    config_schema = []

    BASE_URL = "https://valencia-app-mds.xumo.com"
    MARKET_ID = "10006"
    DEFAULT_GEO_ID = "2f08a9b3"

    CHANNEL_LIST_FIELDS = (
        "/v2/proxy/channels/list/{market_id}.json"
        "?sort=hybrid&geoId={geo_id}&deviceId={device_id}&ifaId={ifa_id}"
    )
    BROADCAST_URL = "/v2/channels/channel/{channel_id}/broadcast.json?hour={hour}"
    ASSET_URL = (
        "/v2/assets/asset/{asset_id}.json"
        "?f=providers&f=cuePoints&f=connectorId&f=genres&f=title"
        "&f=episodeTitle&f=runtime&f=ratings&f=keywords&f=season&f=episode"
    )
    EPG_URL = (
        "/v2/epg/{market_id}/{date_str}/{page}.json"
        "?f=asset.title&f=asset.descriptions&limit={limit}&offset={offset}"
    )

    CHANNEL_SCHEME = "xumo://channel/"
    MAX_EPG_DAYS = 2
    EPG_PAGES_PER_DAY = 24
    EPG_LIMIT = 50
    EPG_MAX_OFFSET = 1000
    EPG_OFFSET_STEP = 50
    REQUEST_TIMEOUT_SECONDS = 12

    DRM_CALLSIGN_SUFFIXES = (
        "-DRM",
        "DRM-CMS",
    )

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/145.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://play.xumo.com",
                "Referer": "https://play.xumo.com/",
            }
        )
        self.market_id = str(self.config.get("market_id") or self.MARKET_ID)
        self.geo_id = str(self.config.get("geo_id") or self.DEFAULT_GEO_ID)
        # Stable per-process IDs help keep ad/stream requests closer to the web client.
        self._device_id = str(uuid.uuid4())
        self._ifa_id = str(uuid.uuid4())
        self._asset_cache: dict[str, dict[str, Any]] = {}

    # ---------------------------------------------------------------------
    # Required FastChannels methods
    # ---------------------------------------------------------------------
    def fetch_channels(self) -> list[ChannelData]:
        url = self.BASE_URL + self.CHANNEL_LIST_FIELDS.format(
            market_id=self.market_id,
            geo_id=self.geo_id,
            device_id=self._device_id,
            ifa_id=self._ifa_id,
        )
        payload = self._get_json(url)
        if not payload:
            return []

        items = []
        if isinstance(payload.get("channel"), dict):
            items = payload["channel"].get("item") or []
        elif isinstance(payload.get("items"), list):
            items = payload.get("items") or []

        channels: list[ChannelData] = []
        for item in items:
            if not isinstance(item, dict):
                continue

            props = item.get("properties") or {}
            callsign = str(item.get("callsign") or "")
            is_live = str(props.get("is_live", "")).lower() == "true"
            if not is_live:
                continue
            if any(callsign.endswith(suffix) for suffix in self.DRM_CALLSIGN_SUFFIXES):
                continue

            channel_id = self._nested_str(item, "guid", "value")
            name = str(item.get("title") or "").strip()
            if not channel_id or not name:
                continue
            category = self._extract_genre(item)

            channels.append(
                ChannelData(
                    source_channel_id=channel_id,
                    name=name,
                    stream_url=f"{self.CHANNEL_SCHEME}{channel_id}",
                    logo_url=self._extract_logo(item),
                    category=category,
                    language=infer_language_from_metadata(name, category),
                )
            )

        logger.info("[xumo] %d channels", len(channels))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        if not channels:
            return []

        wanted_ids = {str(ch.source_channel_id) for ch in channels}
        programmes: list[ProgramData] = []
        seen: set[tuple[str, str, str]] = set()

        now_utc = datetime.now(timezone.utc)
        # Xumo's "day" buckets run on a US-local boundary, so the current
        # overnight UTC window lives under the previous date key.
        dates = [
            (now_utc + timedelta(days=day)).strftime("%Y%m%d")
            for day in range(-1, self.MAX_EPG_DAYS)
        ]

        for date_str in dates:
            for page in range(1, self.EPG_PAGES_PER_DAY + 1):
                found_any_for_page = False

                page_no_data = False
                for offset in range(0, self.EPG_MAX_OFFSET + 1, self.EPG_OFFSET_STEP):
                    url = self.BASE_URL + self.EPG_URL.format(
                        market_id=self.market_id,
                        date_str=date_str,
                        page=page,
                        limit=self.EPG_LIMIT,
                        offset=offset,
                    )
                    payload = self._get_epg_json(url)
                    if payload is self._EPG_NO_DATA:
                        page_no_data = True
                        break
                    if not payload:
                        continue

                    assets = payload.get("assets") or {}
                    if isinstance(assets, dict):
                        self._asset_cache.update(assets)

                    page_channels = payload.get("channels") or []
                    if not page_channels:
                        if offset == 0:
                            # No data at offset 0 strongly suggests this page is empty.
                            break
                        continue

                    found_any_for_page = True
                    matched_channel_count = 0

                    for channel_row in page_channels:
                        channel_id = str(channel_row.get("channelId") or "")
                        if channel_id not in wanted_ids:
                            continue
                        matched_channel_count += 1

                        for slot in channel_row.get("schedule") or []:
                            asset_id = str(slot.get("assetId") or "")
                            start = self._parse_xumo_dt(slot.get("start"))
                            end = self._parse_xumo_dt(slot.get("end"))
                            if not asset_id or not start or not end:
                                continue
                            if end <= now_utc:
                                continue

                            asset = assets.get(asset_id) or self._asset_cache.get(asset_id) or {}
                            title = str(asset.get("title") or "Unknown")
                            descriptions = asset.get("descriptions") or {}
                            description = (
                                descriptions.get("large")
                                or descriptions.get("medium")
                                or descriptions.get("small")
                                or descriptions.get("tiny")
                            )
                            poster_url = self._extract_poster(asset)
                            category = self._extract_asset_genre(asset)

                            dedupe_key = (channel_id, start.isoformat(), asset_id)
                            if dedupe_key in seen:
                                continue
                            seen.add(dedupe_key)

                            programmes.append(
                                ProgramData(
                                    source_channel_id=channel_id,
                                    title=title,
                                    start_time=start,
                                    end_time=end,
                                    description=description,
                                    poster_url=poster_url,
                                    category=category,
                                )
                            )


                if page_no_data or not found_any_for_page:
                    # 400 = server has no data for this hour; later hours won't
                    # have data either.  Also stop when a page is completely empty.
                    break

        logger.info("[xumo] %d EPG entries", len(programmes))
        return programmes

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(self.CHANNEL_SCHEME):
            return raw_url

        channel_id = raw_url.split(self.CHANNEL_SCHEME, 1)[1].strip()
        if not channel_id:
            return raw_url

        hour = datetime.now(timezone.utc).hour
        broadcast_url = self.BASE_URL + self.BROADCAST_URL.format(channel_id=channel_id, hour=hour)
        broadcast = self._get_json(broadcast_url)
        if not broadcast:
            return raw_url

        assets = broadcast.get("assets") or []
        live_asset_id = None
        for asset in assets:
            if not isinstance(asset, dict):
                continue
            if asset.get("live") is True and asset.get("id"):
                live_asset_id = str(asset["id"])
                break
        if not live_asset_id and assets:
            candidate = assets[0]
            if isinstance(candidate, dict) and candidate.get("id"):
                live_asset_id = str(candidate["id"])
        if not live_asset_id:
            return raw_url

        asset = self._get_asset(live_asset_id)
        if not asset:
            return raw_url

        source_url = self._extract_stream_source(asset)
        if not source_url:
            return raw_url

        return self._process_stream_uri(source_url)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    # Sentinel returned by _get_epg_json when the server says "no data for this
    # hour" (HTTP 400).  Callers should stop iterating offsets *and* pages.
    _EPG_NO_DATA = object()

    def _get_json(self, url: str) -> dict[str, Any] | None:
        """Safely fetch JSON via BaseScraper.get()."""
        try:
            response = self.get(url)
        except Exception as exc:
            logger.warning("[xumo] request failed for %s: %s", url, exc)
            return None

        if not response:
            return None

        try:
            data = response.json()
        except Exception as exc:
            logger.warning("[xumo] invalid JSON for %s: %s", url, exc)
            return None

        if not isinstance(data, dict):
            return None
        return data

    def _get_epg_json(self, url: str):
        """
        Fetch EPG JSON.  Three return values:
          - dict           → success
          - None           → transient error; skip this offset, try the next
          - _EPG_NO_DATA   → HTTP 400 "no data for this hour"; break offset
                             loop *and* page loop immediately
        """
        try:
            response = self.session.get(url, timeout=self.REQUEST_TIMEOUT_SECONDS)
        except Exception as exc:
            logger.debug("[xumo] EPG request failed for %s: %s", url, exc)
            return None

        if response.status_code == 400:
            return self._EPG_NO_DATA

        if not response.ok:
            logger.debug("[xumo] EPG HTTP %d for %s", response.status_code, url)
            return None

        try:
            data = response.json()
        except Exception:
            return None

        return data if isinstance(data, dict) else None

    def _get_asset(self, asset_id: str) -> dict[str, Any] | None:
        cached = self._asset_cache.get(asset_id)
        if isinstance(cached, dict) and cached.get("providers"):
            return cached

        url = self.BASE_URL + self.ASSET_URL.format(asset_id=asset_id)
        asset = self._get_json(url)
        if asset:
            self._asset_cache[asset_id] = asset
        return asset

    def _extract_stream_source(self, asset: dict[str, Any]) -> str | None:
        providers = asset.get("providers") or []
        for provider in providers:
            if not isinstance(provider, dict):
                continue
            sources = provider.get("sources") or []
            for source in sources:
                if not isinstance(source, dict):
                    continue
                uri = source.get("uri")
                produces = str(source.get("produces") or "")
                if not uri:
                    continue
                if "mpegurl" in produces.lower() or uri.endswith(".m3u8") or ".m3u8?" in uri:
                    return str(uri)
        return None

    def _process_stream_uri(self, uri: str) -> str:
        """
        The web player sometimes leaves placeholder macros in query params.
        Replace the ones that appear useful, then strip any remaining [PLACEHOLDER]
        fragments so the URL stays usable.
        """
        replacements = {
            "[PLATFORM]": "web",
            "[APP_VERSION]": "1.0.0",
            "[timestamp]": str(int(datetime.now(timezone.utc).timestamp() * 1000)),
            "[app_bundle]": "play.xumo.com",
            "[device_make]": "OpenAI",
            "[device_model]": "FastChannels",
            "[content_language]": "en",
            "[IS_LAT]": "0",
            "[IFA]": self._ifa_id,
            "[IFA_TYPE]": "aaid",
            "[SESSION_ID]": str(uuid.uuid4()),
            "[DEVICE_ID]": self._device_id.replace("-", ""),
            "[CCPA_Value]": "1---",
            "[publica_site_id]": "",
            "[LAT]": "",
            "[LON]": "",
            "[OS]": "web",
            "[OS_VERSION]": "",
            "[AMZN_APP_ID]": "",
            "[app_store_url]": "",
            "[CONTENT_IMDB_GENRE]": "",
            "[IAB_content_category]": "",
            "[content_genre]": "",
            "[content_rating]": "",
        }

        processed = uri
        for old, new in replacements.items():
            processed = processed.replace(old, new)

        # Strip any leftover bracketed macros.
        processed = re.sub(r"\[[^\]]+\]", "", processed)

        # Remove empty query params introduced by stripped macros.
        parsed = urlparse(processed)
        query_pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=False) if v != ""]
        cleaned_query = urlencode(query_pairs, doseq=True)
        return urlunparse(parsed._replace(query=cleaned_query))

    @staticmethod
    def _parse_xumo_dt(value: Any) -> datetime | None:
        if not value:
            return None
        if isinstance(value, (int, float)):
            # Defensive support for epoch timestamps.
            try:
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
            except Exception:
                return None

        text = str(value).strip()
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                dt = datetime.strptime(text, fmt)
                return dt.astimezone(timezone.utc)
            except ValueError:
                continue
        return None

    @staticmethod
    def _nested_str(obj: dict[str, Any], *path: str) -> str | None:
        cur: Any = obj
        for key in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        if cur is None:
            return None
        return str(cur)

    @staticmethod
    def _join_genres(values: list[Any] | None) -> str | None:
        if not values:
            return None
        labels: list[str] = []
        for value in values:
            if isinstance(value, dict):
                label = value.get("value") or value.get("title")
            else:
                label = str(value) if value is not None else None
            if not label:
                continue
            label = label.strip()
            if label and label not in labels:
                labels.append(label)
        return ';'.join(labels) or None

    @staticmethod
    def _extract_genre(item: dict[str, Any]) -> str | None:
        genres = item.get("genre") or []
        if isinstance(genres, list):
            return XumoScraper._join_genres(genres)
        return None

    @staticmethod
    def _extract_asset_genre(asset: dict[str, Any]) -> str | None:
        genres = asset.get("genres") or []
        if isinstance(genres, list):
            return XumoScraper._join_genres(genres)
        return None

    @staticmethod
    def _extract_logo(item: dict[str, Any]) -> str | None:
        images = item.get("images") or {}
        if isinstance(images, dict):
            for key in ("logo", "logoHorizontal", "logo_vertical", "thumbnail"):
                value = images.get(key)
                if value:
                    return str(value)
        if item.get("logo"):
            return str(item["logo"])
        return None

    @staticmethod
    def _extract_poster(asset: dict[str, Any]) -> str | None:
        for key in ("image", "images", "artwork"):
            value = asset.get(key)
            if isinstance(value, str) and value:
                return value
            if isinstance(value, dict):
                for subkey in ("poster", "hero", "thumbnail", "url"):
                    subval = value.get(subkey)
                    if subval:
                        return str(subval)
            if isinstance(value, list):
                for entry in value:
                    if isinstance(entry, str) and entry:
                        return entry
                    if isinstance(entry, dict):
                        subval = entry.get("url") or entry.get("src")
                        if subval:
                            return str(subval)
        return None

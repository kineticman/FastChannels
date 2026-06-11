from __future__ import annotations

import json
import logging
import re
import threading
import time
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from bs4 import BeautifulSoup

from .base import BaseScraper, ChannelData, ProgramData

logger = logging.getLogger(__name__)

_EPISODE_CACHE_TTL = 6 * 60 * 60
_episode_cache: dict[str, tuple[float, dict[str, list[dict[str, Any]]]]] = {}
_episode_cache_lock = threading.Lock()


class AdultSwimScraper(BaseScraper):
    """Adult Swim's public 24/7 marathon streams."""

    source_name = "adultswim"
    display_name = "Adult Swim"
    scrape_interval = 180
    channel_refresh_hours = 12
    channel_miss_threshold = 5
    stream_audit_enabled = True
    epg_quality = "partial"
    source_category = 'specialty'
    config_schema = []

    BASE_URL = "https://www.adultswim.com"
    CATALOG_URL = BASE_URL + "/streams/aqua-teen-hunger-force"
    VIDEO_URL = BASE_URL + "/api/shows/v1/videos/{video_id}"
    SCHEDULE_URL = BASE_URL + "/api/schedule/marathons/{marathon_id}"
    STREAM_SCHEME = "adultswim://stream"
    NO_SHOW_PAGE_SLUGS = frozenset({"channel-5"})
    VIDEO_FIELDS = (
        "title,type,duration,collection_title,poster,stream,title_id,auth,"
        "media_id,launch_date,auth_launch_date,disable_ads,season_number,episode_number"
    )

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._embedded_schedules: dict[str, Any] = {}
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Origin": self.BASE_URL,
                "Referer": self.BASE_URL + "/streams/",
            }
        )

    def fetch_channels(self) -> list[ChannelData]:
        response = self.get(self.CATALOG_URL)
        if not response:
            return []

        state = self._extract_redux_state(response.text)
        streams = state.get("streams") if isinstance(state, dict) else None
        if not isinstance(streams, list):
            logger.error("[%s] Next.js state did not contain a stream catalog", self.source_name)
            return []

        marathon = state.get("marathon")
        self._embedded_schedules = marathon if isinstance(marathon, dict) else {}

        valid_rows = [
            row
            for row in streams
            if isinstance(row, dict)
            and self._clean(row.get("id"))
            and self._clean(row.get("title"))
            and self._clean(row.get("stream"))
            and self._clean(row.get("vod_to_live_id"))
        ]

        assets: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {
                pool.submit(self._fetch_stable_stream_url, self._clean(row["stream"])): row
                for row in valid_rows
            }
            for future in as_completed(futures):
                row = futures[future]
                video_id = self._clean(row["stream"])
                try:
                    asset_url = future.result()
                except Exception as exc:
                    logger.warning(
                        "[%s] Video metadata failed for %s: %s",
                        self.source_name,
                        video_id,
                        exc,
                    )
                    continue
                if asset_url:
                    assets[video_id] = asset_url

        channels: list[ChannelData] = []
        for number, row in enumerate(valid_rows, start=1):
            slug = self._clean(row["id"])
            title = self._clean(row["title"])
            video_id = self._clean(row["stream"])
            marathon_id = self._clean(row["vod_to_live_id"])
            asset_url = assets.get(video_id)
            if not asset_url:
                logger.warning("[%s] No stable HLS asset for %s; skipping", self.source_name, title)
                continue

            channels.append(
                ChannelData(
                    source_channel_id=slug,
                    name=title,
                    stream_url=self._build_stream_uri(
                        slug=slug,
                        video_id=video_id,
                        marathon_id=marathon_id,
                        asset_url=asset_url,
                    ),
                    logo_url=self._clean(row.get("poster")) or None,
                    slug=slug,
                    category="Comedy",
                    language="en",
                    country="US",
                    stream_type="hls",
                    number=number,
                    description=self._clean(row.get("description")) or None,
                )
            )

        logger.info("[%s] %d channels fetched", self.source_name, len(channels))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        def fetch_one(channel: ChannelData) -> list[ProgramData]:
            metadata = self._parse_stream_uri(channel.stream_url)
            marathon_id = metadata.get("marathon_id")
            if not marathon_id:
                return []

            rows: list[dict] = []
            response = self.get(self.SCHEDULE_URL.format(marathon_id=marathon_id))
            if response:
                try:
                    rows = self._schedule_rows(response.json())
                except Exception as exc:
                    logger.warning(
                        "[%s] Invalid schedule JSON for %s: %s",
                        self.source_name,
                        channel.source_channel_id,
                        exc,
                    )

            embedded_rows = self._schedule_rows(self._embedded_schedules.get(marathon_id))
            rows = self._merge_schedule_rows(rows, embedded_rows)

            episode_metadata = self._episode_metadata_for_slug(channel.source_channel_id) if rows else {}
            programs = []
            for index, row in enumerate(rows):
                metadata = self._match_episode_metadata(row, episode_metadata)
                next_start = (
                    self._parse_start(rows[index + 1])
                    if index + 1 < len(rows)
                    else None
                )
                program = self._parse_program(channel, row, metadata, next_start)
                if program:
                    programs.append(program)
            return programs

        programs: list[ProgramData] = []
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(fetch_one, channel): channel for channel in channels}
            done = 0
            for future in as_completed(futures):
                channel = futures[future]
                try:
                    programs.extend(future.result())
                except Exception as exc:
                    logger.warning(
                        "[%s] Schedule fetch failed for %s: %s",
                        self.source_name,
                        channel.source_channel_id,
                        exc,
                    )
                done += 1
                if self._progress_cb:
                    self._progress_cb("epg", done, len(channels))

        logger.info("[%s] %d total programs fetched", self.source_name, len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(self.STREAM_SCHEME):
            return raw_url
        return self._parse_stream_uri(raw_url).get("asset_url") or raw_url

    def _fetch_stable_stream_url(self, video_id: str) -> str | None:
        response = self.get(
            self.VIDEO_URL.format(video_id=video_id),
            params={"fields": self.VIDEO_FIELDS},
        )
        if not response:
            return None
        try:
            payload = response.json()
        except Exception as exc:
            logger.warning("[%s] Invalid video JSON for %s: %s", self.source_name, video_id, exc)
            return None
        return self._select_stable_asset(payload)

    def _episode_metadata_for_slug(self, slug: str) -> dict[str, list[dict[str, Any]]]:
        if slug in self.NO_SHOW_PAGE_SLUGS:
            return {}
        now = time.monotonic()
        with _episode_cache_lock:
            cached = _episode_cache.get(slug)
            if cached and now - cached[0] < _EPISODE_CACHE_TTL:
                return cached[1]

        response = self.get(f"{self.BASE_URL}/videos/{slug}")
        if not response:
            return {}
        metadata = self._extract_episode_metadata(response.text)
        if metadata:
            with _episode_cache_lock:
                _episode_cache[slug] = (now, metadata)
        return metadata

    @staticmethod
    def _extract_redux_state(html: str) -> dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        script = soup.find("script", id="__NEXT_DATA__")
        if script is None:
            return {}
        try:
            payload = json.loads(script.string or script.get_text())
        except (TypeError, json.JSONDecodeError):
            return {}
        state = payload.get("props", {}).get("__REDUX_STATE__", {})
        return state if isinstance(state, dict) else {}

    @classmethod
    def _extract_episode_metadata(cls, html: str) -> dict[str, list[dict[str, Any]]]:
        soup = BeautifulSoup(html, "html.parser")
        script = soup.find("script", id="__NEXT_DATA__")
        if script is None:
            return {}
        try:
            payload = json.loads(script.string or script.get_text())
        except (TypeError, json.JSONDecodeError):
            return {}

        apollo = payload.get("props", {}).get("pageProps", {}).get("__APOLLO_STATE__", {})
        if not isinstance(apollo, dict):
            return {}

        by_title: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for key, value in apollo.items():
            if (
                isinstance(key, str)
                and key.startswith("Video:")
                and isinstance(value, dict)
                and str(value.get("type") or "").upper() == "EPISODE"
            ):
                title_key = cls._normalize_title(value.get("title"))
                if title_key:
                    by_title[title_key].append(value)
        return dict(by_title)

    @classmethod
    def _match_episode_metadata(
        cls,
        row: dict,
        metadata: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any] | None:
        title = row.get("name") or row.get("episodeName")
        candidates = metadata.get(cls._normalize_title(title), [])
        if not candidates:
            return None

        season = cls._as_int(row.get("season") or row.get("seasonNumber"))
        episode = cls._as_int(row.get("episodeNumber") or row.get("episode"))
        if season is not None and episode is not None:
            exact = [
                candidate
                for candidate in candidates
                if cls._as_int(candidate.get("seasonNumber")) == season
                and cls._as_int(candidate.get("episodeNumber")) == episode
            ]
            if len(exact) == 1:
                return exact[0]
        return candidates[0] if len(candidates) == 1 else None

    @staticmethod
    def _select_stable_asset(payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None
        video = payload.get("data", {}).get("video", {})
        assets = video.get("stream", {}).get("assets", [])
        if not isinstance(assets, list):
            return None

        hls_urls = []
        for asset in assets:
            if not isinstance(asset, dict):
                continue
            url = AdultSwimScraper._clean(asset.get("url"))
            mime_type = AdultSwimScraper._clean(asset.get("mime_type")).lower()
            if url.startswith("https://") and (
                urlparse(url).path.endswith(".m3u8")
                or mime_type in {"application/x-mpegurl", "application/vnd.apple.mpegurl"}
            ):
                hls_urls.append(url)

        return next((url for url in hls_urls if "/live/" in url), None)

    @classmethod
    def _build_stream_uri(
        cls,
        *,
        slug: str,
        video_id: str,
        marathon_id: str,
        asset_url: str,
    ) -> str:
        return cls.STREAM_SCHEME + "?" + urlencode(
            {
                "slug": slug,
                "video_id": video_id,
                "marathon_id": marathon_id,
                "asset_url": asset_url,
            }
        )

    @classmethod
    def _parse_stream_uri(cls, raw_url: str) -> dict[str, str]:
        if not raw_url.startswith(cls.STREAM_SCHEME):
            return {}
        return {
            key: values[0]
            for key, values in parse_qs(urlparse(raw_url).query).items()
            if values
        }

    @staticmethod
    def _schedule_rows(payload: Any) -> list[dict]:
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if not isinstance(payload, dict):
            return []
        for key in ("data", "schedule", "items", "programs"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        return []

    @classmethod
    def _merge_schedule_rows(cls, primary: list[dict], fallback: list[dict]) -> list[dict]:
        merged = [row for row in primary if cls._parse_start(row)]
        primary_slots = [
            (cls._normalize_title(row.get("name") or row.get("episodeName")), cls._parse_start(row))
            for row in merged
        ]
        for row in fallback:
            start = cls._parse_start(row)
            title = cls._normalize_title(row.get("name") or row.get("episodeName"))
            duplicate = any(
                title and title == primary_title and abs((start - primary_start).total_seconds()) <= 120
                for primary_title, primary_start in primary_slots
                if start and primary_start
            )
            if start and not duplicate:
                merged.append(row)
        return sorted(merged, key=cls._parse_start)

    @classmethod
    def _parse_program(
        cls,
        channel: ChannelData,
        row: dict,
        metadata: dict[str, Any] | None = None,
        next_start: datetime | None = None,
    ) -> ProgramData | None:
        metadata = metadata or {}
        start = cls._parse_start(row)
        duration = cls._as_float(row.get("duration"))
        end = cls._parse_iso(row.get("endTime") or row.get("end_time"))
        if not start or (not end and (duration is None or duration <= 0)):
            return None
        if end is None:
            end = start + timedelta(seconds=duration)
        if next_start and next_start > end:
            end = next_start
        if end <= start:
            return None

        series_name = cls._clean(row.get("seriesName") or row.get("series_name"))
        episode_title = cls._clean(
            row.get("name")
            or row.get("episodeName")
            or row.get("episode_name")
            or row.get("title")
        )
        title = series_name or channel.name
        if episode_title == title:
            episode_title = None

        return ProgramData(
            source_channel_id=channel.source_channel_id,
            title=title,
            start_time=start,
            end_time=end,
            description=cls._clean(metadata.get("description")) or None,
            poster_url=cls._clean(metadata.get("poster")) or channel.logo_url,
            category="Comedy",
            rating=cls._clean(metadata.get("tvRating")) or None,
            episode_title=episode_title or None,
            season=(
                cls._as_int(row.get("season") or row.get("seasonNumber"))
                if row.get("season") not in (None, "") or row.get("seasonNumber") not in (None, "")
                else cls._as_int(metadata.get("seasonNumber"))
            ),
            episode=(
                cls._as_int(row.get("episodeNumber") or row.get("episode"))
                if row.get("episodeNumber") not in (None, "") or row.get("episode") not in (None, "")
                else cls._as_int(metadata.get("episodeNumber"))
            ),
            original_air_date=cls._parse_iso(metadata.get("firstAiring")),
            program_type="episode",
            series_id=cls._clean(row.get("seriesId")) or None,
            episode_id=cls._clean(row.get("titleId")) or None,
        )

    @classmethod
    def _parse_start(cls, row: dict) -> datetime | None:
        raw_start = row.get("startTime") or row.get("start_time")
        start = cls._parse_iso(raw_start)
        if start:
            return start
        milliseconds = cls._as_float(raw_start) or cls._as_float(row.get("time"))
        if milliseconds is None:
            return None
        try:
            return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    @staticmethod
    def _parse_iso(value: Any) -> datetime | None:
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _clean(value: Any) -> str:
        return value.strip() if isinstance(value, str) else ""

    @staticmethod
    def _normalize_title(value: Any) -> str:
        title = AdultSwimScraper._clean(value)
        article = re.match(r"^(.*),\s+(The|A|An)$", title, flags=re.IGNORECASE)
        if article:
            title = f"{article.group(2)} {article.group(1)}"
        title = unicodedata.normalize("NFKD", title)
        title = "".join(char for char in title if not unicodedata.combining(char))
        title = title.casefold().replace("&", " and ")
        return re.sub(r"[^a-z0-9]+", " ", title).strip()

    @staticmethod
    def _as_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_float(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

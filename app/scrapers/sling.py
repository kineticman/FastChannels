from __future__ import annotations

import json
import logging
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import requests

try:
    from .base import BaseScraper, ChannelData, ProgramData, StreamDeadError, format_http_reason, infer_language_from_metadata
    from .category_utils import infer_category_from_name
except ImportError:  # pragma: no cover - local staging outside FastChannels package
    from app.scrapers.base import BaseScraper, ChannelData, ProgramData, StreamDeadError, format_http_reason, infer_language_from_metadata
    from app.scrapers.category_utils import infer_category_from_name

logger = logging.getLogger(__name__)

# Matches internal Sling test/slate channel call signs regardless of any display-name overrides.
_TEST_CHANNEL_RE = re.compile(r'^SLATEPO\d|^HYBRID-SIGNALTEST-', re.IGNORECASE)


def _join_categories(values: list[str] | tuple[str, ...] | None) -> str | None:
    if not values:
        return None
    unique = list(dict.fromkeys(v.strip() for v in values if v and v.strip()))
    return ';'.join(unique) or None


class SlingScraper(BaseScraper):
    """
    FastChannels scraper for Sling Freestream.

    - Channel inventory: public channel summary feed (no auth).
    - EPG: per-channel schedule.qvt windows (no auth).
    - Streams: CENC-encrypted DASH (Widevine + PlayReady).
    - DRM: Widevine proxy at p-drmwv.movetv.com accepts the challenge wrapped in a
      JSON envelope {"env":..,"user_id":..,"channel_id":..,"message":[bytes]}.
      No auth token is required for Freestream channels — any UUID works as user_id.
      The /play/sling/license?channel_id=<guid> proxy handles the envelope wrapping.
    """

    source_name = "sling"
    display_name = "Sling Freestream"
    scrape_interval = 360
    stream_audit_enabled = True
    epg_quality = 'basic'     # thumbnails only; no program descriptions
    license_url = 'https://p-drmwv.movetv.com/widevine/proxy'
    kodi_props = {
        'inputstream': 'inputstream.adaptive',
        'inputstream.adaptive.manifest_type': 'mpd',
        'inputstream.adaptive.license_type': 'com.widevine.alpha',
    }
    channel_refresh_hours = 12

    CMW_FAST = "https://p-cmwnext-fast.movetv.com"
    CMS = "https://cbd46b77.cdn.cms.movetv.com"

    DEFAULT_FOCUS_CHANNEL_ID = "21ec280634b247cfa0688744fb7a7e8a"

    SUMMARY_URL = f"{CMS}/cms/publish3/domain/summary/ums/1.json"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update(
            {
                "accept": "application/json, text/plain, */*",
                "origin": "https://watch.sling.com",
                "referer": "https://watch.sling.com/",
                "client-config": "rn-client-config",
                "client-version": "7.1.32",
                "device-model": "Chrome",
                "player-version": "9.1.0",
                "response-config": "ar_browser_1_1",
                "dma": "535",
                "geo-zipcode": "43017",
                "time-zone-id": "America/New_York",
                "timezone": "-0500",
                "features": "enable_ad_tracking,web_browser",
            }
        )

    # ------------------------------------------------------------------ DRM

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None
    ) -> tuple[bytes, dict]:
        """Wrap the Widevine challenge in Sling's JSON envelope.
        No auth token needed — any UUID works as user_id for Freestream channels."""
        if not channel_id:
            logger.warning('[sling] license request missing channel_id')
            return challenge, {}
        body = json.dumps({
            'env': 'production',
            'user_id': str(uuid.uuid4()),
            'channel_id': channel_id,
            'message': list(challenge),
        }).encode()
        return body, {
            'Content-Type': 'text/plain;charset=UTF-8',
            'Origin': 'https://watch.sling.com',
            'Referer': 'https://watch.sling.com/',
        }

    @classmethod
    def get_kodi_props_for_channel(cls, base_url: str, source_channel_id: str) -> dict[str, str]:
        props = dict(cls.kodi_props)
        props['inputstream.adaptive.license_key'] = (
            f'{base_url}/play/sling/license?channel_id={source_channel_id}||R{{SSM}}|'
        )
        return props

    # ------------------------------------------------------------------ setup

    def pre_run_setup(self) -> None:
        return None

    def fetch_channels(self) -> list[ChannelData]:
        payload = self._get_json(self.SUMMARY_URL)
        summary_channels = payload.get("channels") or []
        channels: dict[str, ChannelData] = {}

        for item in summary_channels:
            channel = self._channel_from_summary(item)
            if channel is not None:
                channels[channel.source_channel_id] = channel

        result = sorted(channels.values(), key=lambda c: (c.name or "", c.source_channel_id))
        logger.info("[%s] %d channels", self.source_name, len(result))
        return result

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        max_windows = 4
        max_workers = 20
        selected    = channels
        total       = len(selected)

        headers_snapshot = dict(self.session.headers)

        programs: list[ProgramData] = []
        lock     = threading.Lock()
        thread_local = threading.local()
        done     = [0]   # mutable counter accessible from threads

        def fetch_one(channel_id: str) -> None:
            sess = getattr(thread_local, "session", None)
            if sess is None:
                sess = self.new_session(headers=headers_snapshot)
                thread_local.session = sess
            try:
                result = self._fetch_epg_for_channel_with_session(channel_id, max_windows, sess)
            except Exception as exc:  # noqa: BLE001
                resp = getattr(exc, 'response', None)
                if resp is not None and resp.status_code == 404:
                    logger.debug("[%s] no EPG for %s", self.source_name, channel_id)
                else:
                    logger.warning("[%s] EPG fetch failed for %s: %s", self.source_name, channel_id, exc)
                result = []
            with lock:
                programs.extend(result)
                done[0] += 1
                if self._progress_cb:
                    self._progress_cb('epg', done[0], total)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_one, ch.source_channel_id) for ch in selected}
            for future in as_completed(futures):
                exc = future.exception()
                if exc and type(exc).__name__ == 'JobTimeoutException':
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise exc

        programs.sort(key=lambda p: (p.source_channel_id, p.start_time, p.title))
        logger.info("[%s] %d EPG entries", self.source_name, len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("sling://"):
            return raw_url

        channel_guid = raw_url.split("sling://", 1)[1].strip()
        if not channel_guid:
            return raw_url

        try:
            payload = self._get_json(self._channel_schedule_url(channel_guid))
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                raise StreamDeadError(format_http_reason("[sling] channel not found", 404, channel_guid)) from exc
            raise

        playback = payload.get("playback_info") or {}
        for key in ("dash_manifest_url", "live_m3u8_url_template", "m3u8_url_template"):
            url = (playback.get(key) or "").strip()
            if url and "{" not in url and url.startswith("http"):
                return url
        raise RuntimeError(f"No playable URL found for sling channel {channel_guid}")

    def _get_json(self, url: str) -> dict[str, Any]:
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _channel_schedule_url(self, channel_guid: str) -> str:
        return f"{self.CMS}/playermetadata/sling/v1/api/channels/{channel_guid}/current/schedule.qvt"

    def _fetch_epg_for_channel(self, channel_guid: str, max_windows: int) -> list[ProgramData]:
        return self._fetch_epg_for_channel_with_session(channel_guid, max_windows, self.session)

    def _fetch_epg_for_channel_with_session(self, channel_guid: str, max_windows: int, session) -> list[ProgramData]:
        url = self._channel_schedule_url(channel_guid)
        seen_urls: set[str] = set()
        programs: dict[tuple[str, str], ProgramData] = {}

        for _ in range(max_windows):
            if not url or url in seen_urls:
                break
            seen_urls.add(url)

            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
            playback = payload.get("playback_info") or {}
            asset = playback.get("asset") or {}
            program = self._program_from_asset(channel_guid, asset, payload)
            if program is not None:
                key = (program.source_channel_id, program.start_time.isoformat())
                programs[key] = program
            url = payload.get("_next")

        return sorted(programs.values(), key=lambda p: p.start_time)


    def _channel_from_summary(self, item: dict[str, Any]) -> ChannelData | None:
        metadata = item.get("metadata") or {}
        visibility = item.get("visibility") or {}
        channel_guid = (item.get("channel_guid") or item.get("external_id") or "").strip()
        qvt_url = (item.get("qvt_url") or "").strip()
        if not channel_guid or not qvt_url:
            return None
        if not visibility.get("visible", True):
            return None
        if not metadata.get("is_linear_channel"):
            return None
        if not metadata.get("is_free"):
            return None
        if item.get("cmw_hide_channel"):
            return None

        # Skip internal test/slate channels.  call_sign is the stable internal
        # identifier — display names are sometimes overridden with real channel
        # names (e.g. SLATEPO11 → "Jalsha Movies") which would defeat name-only
        # filtering.  Genre "Test" is a secondary signal for channels that don't
        # follow the SLATEPO/HYBRID-SIGNALTEST naming convention.
        call_sign = metadata.get("call_sign", "")
        genres = metadata.get("genre", [])
        if _TEST_CHANNEL_RE.search(call_sign) or _TEST_CHANNEL_RE.search(item.get("title", "")):
            return None
        if isinstance(genres, list) and any(g.lower() == "test" for g in genres):
            return None

        name = self._best_summary_channel_name(item)
        if not name:
            return None

        logo_url = ((item.get("thumbnail") or {}).get("url"))
        category = self._infer_summary_group(item, name)

        return ChannelData(
            source_channel_id=channel_guid,
            name=name,
            stream_url=f"sling://{channel_guid}",
            logo_url=logo_url,
            slug=self._slugify(name),
            category=category,
            language=infer_language_from_metadata(name, category, metadata.get("language")),
            country="US",
            stream_type="dash",
            number=self._to_int(item.get("channel_number")),
            gracenote_id=str(item.get("gracenote_channel_id") or "").strip() or None,
            guide_key=qvt_url,
        )

    def _best_summary_channel_name(self, item: dict[str, Any]) -> str | None:
        metadata = item.get("metadata") or {}
        for candidate in (
            metadata.get("channel_name"),
            item.get("network_affiliate_name"),
            item.get("title"),
            metadata.get("call_sign"),
        ):
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return None

    def _infer_summary_group(self, item: dict[str, Any], name: str = "") -> str | None:
        if name:
            inferred = infer_category_from_name(name)
            if inferred:
                return inferred

        metadata = item.get("metadata") or {}
        genres = metadata.get("genre") or []
        if isinstance(genres, list):
            filtered = [
                genre.strip()
                for genre in genres
                if isinstance(genre, str)
                and genre.strip()
                and genre.strip().lower() not in {"sling free", "freestream", "international"}
            ]
            if filtered:
                return filtered[0]
        return "Entertainment"

    def _program_from_asset(
        self,
        channel_guid: str,
        asset: dict[str, Any],
        payload: dict[str, Any],
    ) -> ProgramData | None:
        start = self._parse_dt(asset.get("schedule_start"))
        end = self._parse_dt(asset.get("schedule_end"))
        title = asset.get("title") or asset.get("franchise_title")
        if not start or not end or not title:
            return None

        thumbnail = None
        shows = payload.get("shows") or []
        if shows:
            thumbnail = ((shows[0].get("thumbnail") or {}).get("url"))

        genre = asset.get("genre") or asset.get("channel_genre")
        if isinstance(genre, list):
            genre = _join_categories(genre)

        rating = asset.get("rating")
        if isinstance(rating, list):
            rating = ", ".join(x for x in rating if x)

        _season  = self._to_int(asset.get("season_number"))
        _episode = self._to_int(asset.get("episode_number"))
        return ProgramData(
            source_channel_id=channel_guid,
            title=title,
            start_time=start,
            end_time=end,
            description=None,
            poster_url=thumbnail,
            category=genre,
            rating=rating,
            episode_title=asset.get("episode_title"),
            season=_season,
            episode=_episode,
            program_type="episode" if (_season or _episode) else None,
        )

    def _slugify(self, value: str) -> str:
        cleaned = []
        last_dash = False
        for char in value.lower():
            if char.isalnum():
                cleaned.append(char)
                last_dash = False
            elif not last_dash:
                cleaned.append("-")
                last_dash = True
        return "".join(cleaned).strip("-") or "sling"

    def _parse_dt(self, value: str | None) -> datetime | None:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _to_int(self, value: Any) -> int | None:
        try:
            if value in (None, ""):
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

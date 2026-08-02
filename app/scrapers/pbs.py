from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, StreamDeadError

logger = logging.getLogger(__name__)

PBS_SCHEME = "pbs://"
LOCALIZE_ZIP_URL = "https://localization.services.pbs.org/localize/zipcode/{zip}/"
STATION_URL = "https://www.pbs.org/api/station/{station_id}/stations-list/"
SCHEDULE_URL = "https://player.pbs.org/api/livestream-schedule/{station_id}/"

_PROFILE_LABELS = {
    "ga-create": "Create",
    "ga-fnx": "FNX",
    "ga-local-subchannel-1": "Local Subchannel 1",
    "ga-local-subchannel-2": "Local Subchannel 2",
    "ga-main": "Main",
    "ga-nhk": "NHK World",
    "ga-world": "World",
    "kids-main": "PBS KIDS",
}

_NATIONAL_SECONDARY_PROFILES = frozenset({
    "ga-create",
    "ga-fnx",
    "ga-nhk",
    "ga-world",
})

_DEFAULT_STATIONS = {
    # Curated from official PBS stations-list metadata: these stations expose
    # clear non_drm_url local subchannels. National secondary profiles are
    # deduped separately, so stations with only duplicate World/Create/NHK/FNX
    # feeds are intentionally omitted. Main PBS/PBS KIDS remain DRM-only in the
    # checked markets and are skipped by the scraper.
    "92d89794-5ff0-4fe6-a443-cc888104e021": "WETA",
    "5be2a7e3-fc57-471e-80ff-c5ea79229078": "New Hampshire PBS",
    "43e7850f-48a2-4e3e-b045-c880ea935fbd": "PBS39",
    "7e1c137a-9e83-4416-8b5a-b81042382e91": "Buffalo Toronto Public Media",
    "f36a8eaa-cc11-4e0d-a3c1-638b9ebd3023": "West Virginia Public Broadcasting",
    "86532025-6a98-4ed7-87ef-cce5615bfcf0": "VPM",
    "fa2384b8-9593-4d58-a129-4deb3a1f764a": "PBS North Carolina",
    "61059d95-3011-4270-b9d1-17b88ca25b13": "KET",
    "50ac3de0-09e0-43db-8bb6-d83826fd637c": "SCETV",
    "a4907776-c769-452f-ab2f-74bb1f86676f": "Georgia Public Broadcasting",
    "c4ab80c1-ca8c-4a6e-a8b5-c5995919792b": "WEDU PBS",
    "42559186-b19b-45b5-9861-078ec38bb981": "Nashville PBS",
    "6b3ba9b1-843d-4c44-a8e3-fada6005f081": "WKNO",
    "3f02e9df-66c8-4f3d-8f51-a76c7255a45d": "PBS Fort Wayne",
    "a3a25e36-3ba3-40b4-990f-7460511f0127": "PBS Wisconsin",
    "b107784d-680c-48d5-8d48-ccb870cbb9dc": "Nebraska Public Media",
    "27146fbb-5f51-4924-9e00-bb46e89e18a8": "Milwaukee PBS",
    "7dd0e811-0ad6-4a39-a579-f2cbb20d9800": "KMOS-TV",
    "b3291387-78a4-41e1-beb0-da2f61a96a3e": "KERA",
    "a0c19534-b8cd-4d9f-8856-442a473c4aff": "Houston Public Media",
    "6a6a875e-4af4-4e1e-8502-422071b9cdac": "PBS Reno",
    "1978f5fc-903b-44c5-8db4-76335078ea74": "KQED",
    "4d8fea94-86b8-4663-9c69-4c4cb62257e4": "PBS KVIE",
}


class PBSScraper(BaseScraper):
    """
    PBS station/feed scraper.

    Imports only official PBS feeds that expose a clear non_drm_url. Local main
    PBS and PBS KIDS feeds were DRM-only in checked markets, so they are ignored.
    """

    source_name = "pbs"
    display_name = "PBS"
    scrape_interval = 360
    stream_audit_enabled = True
    source_category = "specialty"
    config_required = False
    epg_quality = "basic"

    config_schema = [
        ConfigField(
            key="include_preconfigured",
            label="Curated Station Set",
            field_type="toggle",
            default=True,
            help_text=(
                "Use the built-in station list for clear PBS secondary feeds "
                "such as World, Create, NHK, FNX, and local subchannels."
            ),
        ),
        ConfigField(
            key="zip_codes",
            label="Additional ZIP Codes",
            field_type="text",
            placeholder="20001, 94105",
            help_text="Optional comma-separated ZIP codes to search for more clear PBS feeds.",
        ),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.pbs.org",
            "Referer": "https://www.pbs.org/",
            "User-Agent": "Mozilla/5.0 (compatible; FastChannels PBS scraper)",
        })

    def fetch_channels(self) -> list[ChannelData]:
        station_ids = self._configured_station_ids()
        if not station_ids:
            logger.info("[%s] no station ids discovered/configured", self.source_name)
            return []

        channels: list[ChannelData] = []
        seen: set[str] = set()

        for station_id in station_ids:
            try:
                station = self._station(station_id)
            except Exception as exc:
                logger.warning("[%s] station lookup failed for %s: %s", self.source_name, station_id, exc)
                continue

            attrs = station.get("attributes") or {}
            call_sign = (attrs.get("call_sign") or "").strip()
            station_name = attrs.get("full_common_name") or attrs.get("short_common_name") or call_sign or station_id
            logo_url = self._logo(attrs)

            for feed in attrs.get("livestream_feeds") or []:
                profile = (feed.get("profile") or "").strip()
                non_drm_url = (feed.get("non_drm_url") or "").strip()
                if not non_drm_url:
                    continue
                feed_cid = (feed.get("associated_tvss_feed") or feed.get("cid") or profile).strip()
                if not feed_cid:
                    continue
                source_channel_id = f"{station_id}:{profile}:{feed_cid}"
                dedupe_key = self._dedupe_key(station_id, profile, feed_cid)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)

                label = _PROFILE_LABELS.get(profile) or self._profile_label(profile)
                name = f"PBS {label}"
                if call_sign and profile.startswith("ga-local-subchannel"):
                    name = f"PBS {call_sign} {label}"
                elif call_sign:
                    name = f"{name} ({call_sign})"

                channels.append(ChannelData(
                    source_channel_id=source_channel_id,
                    name=name,
                    stream_url=f"{PBS_SCHEME}{station_id}/{profile}/{feed_cid}",
                    logo_url=logo_url,
                    category="Education",
                    country="US",
                    language="en",
                    stream_type="hls",
                    guide_key=f"pbs:{feed_cid}",
                    tags=["PBS", label],
                ))

        logger.info("[%s] %d non-DRM channels from %d stations", self.source_name, len(channels), len(station_ids))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        by_station: dict[str, set[str]] = {}
        channel_id_by_feed: dict[tuple[str, str], str] = {}
        for ch in channels:
            parsed = self._parse_opaque(ch.stream_url)
            if not parsed:
                continue
            station_id, _profile, feed_cid = parsed
            by_station.setdefault(station_id, set()).add(feed_cid)
            channel_id_by_feed[(station_id, feed_cid)] = ch.source_channel_id

        now_utc = datetime.now(timezone.utc)
        programs: list[ProgramData] = []
        for station_id, feed_cids in by_station.items():
            timezone_name = self._station_timezone(station_id)
            today = self._local_date(now_utc, timezone_name)
            for day_offset in range(2):
                date_str = (today + timedelta(days=day_offset)).isoformat()
                for channel in self._schedule_channels(station_id, date_str, timezone_name):
                    feed_cid = channel.get("feed_cid")
                    if feed_cid not in feed_cids:
                        continue
                    source_channel_id = channel_id_by_feed.get((station_id, feed_cid))
                    if not source_channel_id:
                        continue
                    for item in channel.get("listings") or []:
                        program = self._program(source_channel_id, item)
                        if program:
                            programs.append(program)

        logger.info("[%s] %d EPG entries", self.source_name, len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        parsed = self._parse_opaque(raw_url)
        if not parsed:
            return raw_url
        station_id, profile, feed_cid = parsed
        station = self._station(station_id)
        attrs = station.get("attributes") or {}
        for feed in attrs.get("livestream_feeds") or []:
            candidate_profile = (feed.get("profile") or "").strip()
            candidate_cid = (feed.get("associated_tvss_feed") or feed.get("cid") or candidate_profile).strip()
            if candidate_profile == profile and candidate_cid == feed_cid:
                non_drm_url = (feed.get("non_drm_url") or "").strip()
                if non_drm_url:
                    return non_drm_url
                raise StreamDeadError(f"PBS feed {profile} for {station_id} no longer exposes non-DRM HLS")
        raise StreamDeadError(f"PBS feed {profile} not found for {station_id}")

    def audit_resolve(self, raw_url: str) -> str:
        return self.resolve(raw_url)

    def _configured_station_ids(self) -> list[str]:
        seen: set[str] = set()
        station_ids: list[str] = []

        if self._truthy(self.config.get("include_preconfigured", True)):
            for station_id in _DEFAULT_STATIONS:
                seen.add(station_id)
                station_ids.append(station_id)

        for station_id in self._csv_values(self.config.get("station_ids")):
            if self._valid_station_id(station_id) and station_id not in seen:
                seen.add(station_id)
                station_ids.append(station_id)

        for zip_code in self._csv_values(self.config.get("zip_codes")):
            zip_code = re.sub(r"\D", "", zip_code)[:5]
            if len(zip_code) != 5:
                continue
            try:
                payload = self._get_json(LOCALIZE_ZIP_URL.format(zip=zip_code))
            except Exception as exc:
                logger.warning("[%s] ZIP lookup failed for %s: %s", self.source_name, zip_code, exc)
                continue
            for raw in payload.get("stations") or []:
                station_id = raw.get("pbs_id") or raw.get("station_id") or raw.get("id")
                if self._valid_station_id(station_id) and station_id not in seen:
                    seen.add(station_id)
                    station_ids.append(station_id)
        return station_ids

    def _station(self, station_id: str) -> dict:
        payload = self._get_json(STATION_URL.format(station_id=station_id))
        return payload.get("stationData") or {}

    def _station_timezone(self, station_id: str) -> str:
        try:
            attrs = (self._station(station_id).get("attributes") or {})
            return attrs.get("timezone") or "America/New_York"
        except Exception:
            return "America/New_York"

    def _schedule_channels(self, station_id: str, date_str: str, timezone_name: str) -> list[dict]:
        url = SCHEDULE_URL.format(station_id=station_id)
        payload = self._get_json(url, params={"date": date_str, "timezone": timezone_name})
        channels: list[dict] = []
        for collection in payload.get("collections") or []:
            for block in collection.get("content") or []:
                channels.extend(block.get("channels") or [])
        return channels

    def _get_json(self, url: str, params: dict | None = None) -> dict:
        response = self.session.get(url, params=params, timeout=20)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _local_date(now_utc: datetime, timezone_name: str):
        try:
            return now_utc.astimezone(ZoneInfo(timezone_name)).date()
        except ZoneInfoNotFoundError:
            return now_utc.date()

    @staticmethod
    def _program(source_channel_id: str, item: dict) -> ProgramData | None:
        start = PBSScraper._parse_time(item.get("start_time"))
        duration = item.get("duration")
        if not start or not duration:
            return None
        try:
            end = start + timedelta(seconds=int(duration))
        except Exception:
            return None
        title = item.get("show_title") or item.get("episode_title") or "PBS Programming"
        return ProgramData(
            source_channel_id=source_channel_id,
            title=str(title),
            start_time=start,
            end_time=end,
            description=item.get("episode_description") or item.get("show_description"),
            poster_url=item.get("listing_image"),
            episode_title=item.get("episode_title") or None,
            series_id=item.get("show_tms_id") or item.get("tvss_show_id"),
            episode_id=item.get("episode_tms_id") or item.get("tvss_episode_id"),
        )

    @staticmethod
    def _parse_time(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _parse_opaque(raw_url: str) -> tuple[str, str, str] | None:
        if not raw_url.startswith(PBS_SCHEME):
            return None
        parts = raw_url[len(PBS_SCHEME):].split("/", 2)
        if len(parts) != 3:
            return None
        station_id, profile, feed_cid = parts
        if not PBSScraper._valid_station_id(station_id):
            return None
        return station_id, profile, feed_cid

    @staticmethod
    def _valid_station_id(value: str | None) -> bool:
        return bool(value and re.fullmatch(r"[0-9a-fA-F-]{36}", str(value).strip()))

    @staticmethod
    def _dedupe_key(station_id: str, profile: str, feed_cid: str) -> str:
        if profile in _NATIONAL_SECONDARY_PROFILES:
            return profile
        return f"{station_id}:{profile}:{feed_cid}"

    @staticmethod
    def _truthy(value) -> bool:
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "on")
        return bool(value)

    @staticmethod
    def _csv_values(value) -> list[str]:
        if isinstance(value, list):
            raw_values = value
        else:
            raw_values = re.split(r"[\s,]+", str(value or ""))
        return [str(v).strip() for v in raw_values if str(v).strip()]

    @classmethod
    def _csv_set(cls, value) -> set[str]:
        return set(cls._csv_values(value))

    @staticmethod
    def _profile_label(profile: str) -> str:
        label = profile.removeprefix("ga-").replace("-", " ").strip()
        return label.title() if label else "Livestream"

    @staticmethod
    def _logo(attrs: dict) -> str | None:
        images = attrs.get("images") or []
        for preferred in ("color-cobranded-logo", "color-logo", "white-cobranded-logo", "white-logo"):
            for image in images:
                if image.get("profile") == preferred and image.get("url"):
                    return image["url"]
        return None

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..gracenote_map import resolve_gracenote
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

# A local subchannel (ga-local-subchannel-1/2) sometimes just simulcasts one of
# the national secondary networks under station branding — e.g. "BTPM Create",
# "WNPT2 WORLD", "NE-World" — rather than carrying unique local programming
# (compare "WEDQ" or "The Wisconsin Channel", which are genuinely distinct).
# Matched against the PBS-published full_name via word-boundary regex.
_NATIONAL_SIMULCAST_MARKERS = {
    "create": "Create",
    "world": "World",
    "nhk": "NHK World",
    "fnx": "FNX",
}

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

    Auto-imports official PBS feeds that expose a clear non_drm_url from the
    curated station list / ZIP codes. Individual feeds (clear or DRM) can also
    be hand-picked via the `manual_feeds` config field (populated by the admin
    Station & Feed Finder) — this is the only way to get a DRM-protected
    main/PBS KIDS feed, which plays through the PrismCast DRM bridge like
    Roku/Philo/Sling rather than the standard feed.
    """

    source_name = "pbs"
    display_name = "PBS"
    scrape_interval = 360
    stream_audit_enabled = True
    source_category = "specialty"
    config_required = False
    epg_quality = "basic"

    # Marks this source DRM-capable for the generic PrismCast bridge machinery
    # (worker._sync_intrinsic_drm_bridge, routes/play.py license_proxy). PBS's
    # real per-feed license URL is deterministic from the feed's tvss_feed id
    # (see get_license_url) — this class-level value is just the truthy marker
    # / Kodi-prop base.
    license_url = "https://proxy.drm.pbs.org/license/widevine/"

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
        ConfigField(
            key="manual_feeds",
            label="Manually Added Feeds",
            field_type="text",
            placeholder="92d89794-5ff0-4fe6-a443-cc888104e021:ga-main",
            help_text=(
                "Comma-separated station_id:profile pairs for individually hand-picked "
                "feeds — clear or DRM — not already covered by the curated list or ZIP "
                "codes above. DRM feeds (main/PBS KIDS) play through the PrismCast "
                "bridge. Use the Station & Feed Finder below to search by ZIP and add "
                "specific feeds."
            ),
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
        channels: list[ChannelData] = []
        seen: set[str] = set()

        if not station_ids:
            logger.info("[%s] no clear-feed station ids discovered/configured", self.source_name)

        for station_id in station_ids:
            try:
                station = self._station(station_id)
            except Exception as exc:
                logger.warning("[%s] station lookup failed for %s: %s", self.source_name, station_id, exc)
                continue

            attrs = station.get("attributes") or {}
            call_sign = (attrs.get("call_sign") or "").strip()
            logo_url = self._logo(attrs)
            feeds = attrs.get("livestream_feeds") or []
            subchannel_labels, simulcast_profiles = self._local_subchannel_labels(
                station_id, call_sign, attrs, feeds,
            )

            for feed in feeds:
                # Bulk auto-discovery stays clear-only (allow_drm=False) — DRM feeds
                # are opt-in only, via manual_feeds below.
                ch = self._build_channel(
                    station_id, call_sign, logo_url, feed, seen, allow_drm=False,
                    subchannel_labels=subchannel_labels, skip_profiles=simulcast_profiles,
                )
                if ch:
                    channels.append(ch)

        manual_count = self._add_manual_feeds(channels, seen)

        logger.info(
            "[%s] %d clear + %d manually-added channel(s) from %d stations",
            self.source_name, len(channels) - manual_count, manual_count, len(station_ids),
        )
        return channels

    def _add_manual_feeds(self, channels: list[ChannelData], seen: set[str]) -> int:
        """Adds individually hand-picked feeds (clear or DRM) via manual_feeds config
        ("station_id:profile" pairs from the admin Station & Feed Finder). Unlike the
        bulk clear-feed loop, these are never auto-discovered and may be either type —
        a user might want just one station's Main DRM feed plus one clear secondary
        feed, without pulling in every feed that station has."""
        by_station: dict[str, set[str]] = {}
        for entry in self._csv_values(self.config.get("manual_feeds")):
            station_id, _, profile = entry.partition(":")
            station_id, profile = station_id.strip(), profile.strip()
            if self._valid_station_id(station_id) and profile:
                by_station.setdefault(station_id, set()).add(profile)

        added = 0
        for station_id, profiles in by_station.items():
            try:
                station = self._station(station_id)
            except Exception as exc:
                logger.warning("[%s] manual feed station lookup failed for %s: %s", self.source_name, station_id, exc)
                continue

            attrs = station.get("attributes") or {}
            call_sign = (attrs.get("call_sign") or "").strip()
            logo_url = self._logo(attrs)
            feeds = attrs.get("livestream_feeds") or []
            # Manual picks are explicit user selections via the Station & Feed
            # Finder — don't second-guess them with the simulcast skip below.
            subchannel_labels, _simulcast_profiles = self._local_subchannel_labels(
                station_id, call_sign, attrs, feeds,
            )

            for feed in feeds:
                if (feed.get("profile") or "").strip() not in profiles:
                    continue
                ch = self._build_channel(
                    station_id, call_sign, logo_url, feed, seen, allow_drm=True,
                    subchannel_labels=subchannel_labels,
                )
                if ch:
                    channels.append(ch)
                    added += 1
        return added

    def _build_channel(
        self, station_id: str, call_sign: str, logo_url: str | None,
        feed: dict, seen: set[str], *, allow_drm: bool,
        subchannel_labels: dict[str, str] | None = None,
        skip_profiles: set[str] | None = None,
    ) -> ChannelData | None:
        """Builds a ChannelData for a single feed dict, preferring its clear
        (non_drm_url) stream; falls back to the DRM (drm_dash_url) one only when
        allow_drm=True. Returns None if the feed has no usable URL under those
        rules, if its profile is in `skip_profiles` (a local subchannel that's a
        verbatim simulcast of a national secondary network — see
        _local_subchannel_labels), or if it's a duplicate per `seen` (mutated in
        place, shared across both the bulk clear-feed loop and _add_manual_feeds)."""
        profile = (feed.get("profile") or "").strip()
        if not profile:
            return None
        if skip_profiles and profile in skip_profiles:
            return None
        feed_cid = (feed.get("associated_tvss_feed") or feed.get("cid") or profile).strip()
        if not feed_cid:
            return None
        non_drm_url = (feed.get("non_drm_url") or "").strip()
        drm_dash_url = (feed.get("drm_dash_url") or "").strip() if allow_drm else ""
        if not non_drm_url and not drm_dash_url:
            return None
        is_drm = not non_drm_url

        dedupe_key = self._dedupe_key(station_id, profile, feed_cid)
        if dedupe_key in seen:
            return None
        seen.add(dedupe_key)

        label = self._profile_label_for(profile)
        name = f"PBS {label}"
        if call_sign and profile.startswith("ga-local-subchannel"):
            derived = (subchannel_labels or {}).get(profile)
            name = f"PBS {call_sign} ({derived})" if derived else f"PBS {call_sign} {label}"
        elif call_sign:
            name = f"{name} ({call_sign})"

        gracenote_id = resolve_gracenote("pbs", lookup_key=self._gracenote_key(call_sign, profile))

        return ChannelData(
            source_channel_id=f"{station_id}:{profile}:{feed_cid}",
            name=name,
            stream_url=f"{PBS_SCHEME}{station_id}/{profile}/{feed_cid}",
            logo_url=logo_url,
            category="Education",
            country="US",
            language="en",
            stream_type="dash" if is_drm else "hls",
            guide_key=f"pbs:{feed_cid}",
            tags=["PBS", label] + (["DRM"] if is_drm else []),
            gracenote_id=gracenote_id,
        )

    @staticmethod
    def _gracenote_key(call_sign: str, profile: str) -> str | None:
        """Community Gracenote CSV lookup key, matching the
        "PBS_<CALLSIGN>[_<profile-with-underscores>]" scheme from the
        community-maintained pbs_gracenote_map.csv (main profile has no
        suffix; e.g. PBS_KACV, PBS_KACV_ga_create, PBS_KACV_kids_main)."""
        if not call_sign:
            return None
        if profile == "ga-main":
            return f"PBS_{call_sign}"
        return f"PBS_{call_sign}_{profile.replace('-', '_')}"

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
        total_stations = len(by_station)
        for index, (station_id, feed_cids) in enumerate(by_station.items(), start=1):
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
            if self._progress_cb:
                self._progress_cb("epg", index, total_stations)

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
                drm_dash_url = (feed.get("drm_dash_url") or "").strip()
                if drm_dash_url:
                    return drm_dash_url
                raise StreamDeadError(f"PBS feed {profile} for {station_id} no longer exposes a playable URL")
        raise StreamDeadError(f"PBS feed {profile} not found for {station_id}")

    def audit_resolve(self, raw_url: str) -> str:
        return self.resolve(raw_url)

    # ── DRM (Widevine) bridge support ───────────────────────────────────────
    #
    # PBS's Widevine license URL is deterministic from the feed's tvss_feed id
    # (confirmed against the live station API: widevine_license is always
    # f"https://proxy.drm.pbs.org/license/widevine/{associated_tvss_feed}-dash"),
    # so no per-session capture/caching is needed — unlike Roku's resolve_dash().

    @classmethod
    def license_request_headers(cls, config: dict) -> dict:
        return {
            "Origin": "https://player.pbs.org",
            "Referer": "https://player.pbs.org/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
        }

    @classmethod
    def is_license_error_response(cls, response_bytes: bytes) -> bool:
        # proxy.drm.pbs.org (a Node/Axios lambda) returns HTTP 200 even when its own
        # upstream call failed — the failure only shows up as a JSON error envelope
        # in the body (e.g. {"message":"Request failed with status code 403",
        # "name":"AxiosError",...}), confirmed live for the PBS KIDS DRM feed. A real
        # Widevine SignedMessage never starts with '{' (0x7B), so this is a safe check.
        return bool(response_bytes) and response_bytes[:1] == b"{"

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        """Looks up the feed's real widevine_license URL from the live station API.

        NOTE: this is NOT reliably f"{tvss_feed}-dash" — confirmed live that PBS
        KIDS (profile kids-main) uses a shared national identifier ("est") instead
        of its own associated_tvss_feed, e.g. WETA's kids-main widevine_license is
        ".../license/widevine/est-dash", not ".../license/widevine/
        1f51028b-dea2-4a68-ae47-da1d1833b871-dash". Guessing the URL from the feed
        id produced a plausible-looking but wrong URL that PBS's license server
        403'd on every station's PBS KIDS feed. Always read the field PBS actually
        publishes instead of assuming a pattern.
        """
        if not channel_id:
            return cls.license_url
        parts = channel_id.split(":", 2)
        if len(parts) != 3:
            return None
        station_id, profile, feed_cid = parts
        try:
            station = cls(config=config)._station(station_id)
        except Exception as exc:
            logger.warning("[%s] license URL lookup failed for %s: %s", cls.source_name, channel_id, exc)
            return None
        attrs = station.get("attributes") or {}
        for feed in attrs.get("livestream_feeds") or []:
            candidate_profile = (feed.get("profile") or "").strip()
            candidate_cid = (feed.get("associated_tvss_feed") or feed.get("cid") or candidate_profile).strip()
            if candidate_profile == profile and candidate_cid == feed_cid:
                return (feed.get("widevine_license") or "").strip() or None
        return None

    def search_stations(self, zip_code: str) -> list[dict]:
        """ZIP → candidate PBS stations with per-feed (clear/DRM) availability, for
        the admin Station & Feed Finder. Fetches full station detail per candidate
        (the ZIP endpoint alone doesn't expose feed URLs)."""
        zip_code = re.sub(r"\D", "", zip_code or "")[:5]
        if len(zip_code) != 5:
            raise ValueError("valid 5-digit ZIP code required")
        payload = self._get_json(LOCALIZE_ZIP_URL.format(zip=zip_code))
        results: list[dict] = []
        seen: set[str] = set()
        for raw in payload.get("stations") or []:
            station_id = raw.get("pbs_id") or raw.get("station_id") or raw.get("id")
            if not self._valid_station_id(station_id) or station_id in seen:
                continue
            seen.add(station_id)
            callsign = raw.get("callsign") or raw.get("flagship") or ""
            name = (raw.get("common_name") or raw.get("common_name_full")
                    or raw.get("common_name_short") or callsign or station_id)
            try:
                summary = self.station_feed_summary(station_id)
            except Exception as exc:
                logger.warning("[%s] station detail lookup failed for %s: %s", self.source_name, station_id, exc)
                summary = {"feeds": [], "already_curated": station_id in _DEFAULT_STATIONS}
            results.append({
                "station_id": station_id,
                "callsign": callsign,
                "name": name,
                "city_state": ", ".join(x for x in (raw.get("city"), raw.get("state")) if x),
                **summary,
            })
        return results

    def station_feed_summary(self, station_id: str) -> dict:
        """A station's individually-addable feeds — each with its profile (the
        opaque id used in manual_feeds), a friendly label, and whether it's clear
        or DRM. Skips any feed with no playable URL at all."""
        station = self._station(station_id)
        attrs = station.get("attributes") or {}
        feeds_out = []
        for feed in attrs.get("livestream_feeds") or []:
            profile = (feed.get("profile") or "").strip()
            if not profile:
                continue
            has_clear = bool((feed.get("non_drm_url") or "").strip())
            has_drm = bool((feed.get("drm_dash_url") or "").strip())
            if not has_clear and not has_drm:
                continue
            feeds_out.append({
                "profile": profile,
                "label": self._profile_label_for(profile),
                "type": "clear" if has_clear else "drm",
            })
        return {"feeds": feeds_out, "already_curated": station_id in _DEFAULT_STATIONS}

    @staticmethod
    def _profile_label_for(profile: str) -> str:
        return _PROFILE_LABELS.get(profile) or PBSScraper._profile_label(profile)

    def _local_subchannel_labels(
        self, station_id: str, call_sign: str, attrs: dict, feeds: list[dict],
    ) -> tuple[dict[str, str], set[str]]:
        """Looks up PBS's own human-friendly names for a station's local
        subchannel feeds (e.g. "KERA Create", "BTPM Create", "The West
        Virginia Channel") — these vary station to station since many local
        subchannels actually simulcast a national secondary network (Create,
        World) under their own branding, rather than being generic "extra"
        channels. The stations-list API (used for channel discovery) doesn't
        expose this name; only the per-day schedule API does, via each
        channel entry's `full_name` field. Best-effort: falls back to the
        generic "Local Subchannel N" label (in _build_channel) if this lookup
        fails or a profile isn't present in today's schedule.

        Also flags, in the second return value, which subchannel profiles are
        verbatim simulcasts of a national secondary network (Create, World,
        NHK World, FNX) per _NATIONAL_SIMULCAST_MARKERS — e.g. "BTPM Create",
        "NE-World" — so the caller can skip them as duplicates of that
        national feed. Some OTA affiliates cut in their own programming over
        these, but that isn't reflected on the web stream, so on the web
        they're identical to the national feed. Local subchannels that don't
        match (e.g. "WEDQ", "The Wisconsin Channel") are genuinely unique and
        are left alone."""
        if not any((f.get("profile") or "").startswith("ga-local-subchannel") for f in feeds):
            return {}, set()
        labels: dict[str, str] = {}
        simulcast_profiles: set[str] = set()
        try:
            timezone_name = attrs.get("timezone") or "America/New_York"
            today = self._local_date(datetime.now(timezone.utc), timezone_name).isoformat()
            for channel in self._schedule_channels(station_id, today, timezone_name):
                profile = channel.get("profile") or ""
                full_name = (channel.get("full_name") or "").strip()
                if profile.startswith("ga-local-subchannel") and full_name:
                    labels[profile] = self._derive_subchannel_label(full_name, call_sign)
                    if self._national_simulcast_label(full_name):
                        simulcast_profiles.add(profile)
        except Exception as exc:
            logger.warning("[%s] subchannel label lookup failed for %s: %s", self.source_name, station_id, exc)
        return labels, simulcast_profiles

    @staticmethod
    def _derive_subchannel_label(full_name: str, call_sign: str) -> str:
        label = full_name.strip()
        if call_sign and label.upper().startswith(f"{call_sign.upper()} "):
            label = label[len(call_sign):].strip()
        elif label.upper().startswith("PBS "):
            label = label[4:].strip()
        return label or full_name.strip()

    @staticmethod
    def _national_simulcast_label(full_name: str) -> str | None:
        name = full_name.lower()
        for marker, label in _NATIONAL_SIMULCAST_MARKERS.items():
            if re.search(rf"\b{marker}\b", name):
                return label
        return None

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
        except (ValueError, AttributeError, TypeError):
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

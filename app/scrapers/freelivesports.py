# app/scrapers/freelivesports.py
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone

from .base import BaseScraper, ChannelData, ProgramData, infer_language_from_metadata

logger = logging.getLogger(__name__)

EPG_URL = (
    "https://epg.unreel.me/v2/sites/freelivesports/live-channels/public/"
    "081f73704b56aaceb6b459804761ec54"
    "?__site=freelivesports&__source=web"
)

PLAY_URL_TEMPLATE = (
    "https://ga-prod-api.powr.tv/v2/sites/freelivesports/live-channels/"
    "{channel_id}/play-url"
    "?__site=freelivesports&__source=web&embed=false&protocol=https&tv=false"
)

DEVICE_ID = str(uuid.uuid4())

MACRO_REPLACEMENTS: dict[str, str] = {
    "[DEVICE_ID]":    DEVICE_ID,
    "[DEVICE_MODEL]": "web",
    "[REF]":          "https://www.freelivesports.tv/",
    "[LAT]":          "0",
    "[GDPR]":         "0",
    "[CONSENT_STRING]": "",
    "[US_PRIVACY]":   "",
}

MACRO_RE = re.compile(r"\[[A-Z_]+\]")
_FLS_SXE_DASH_RE = re.compile(
    r"^(?P<series>.+?)\s[-–]\sS(?P<season>\d+)E(?P<episode>\d+)\s[-–]\s(?P<episode_title>.+?)\s*$",
    re.IGNORECASE,
)
_FLS_EPISODE_WITH_SUBTITLE_RE = re.compile(
    r"^(?P<series>.+?)\s[-–]\sEpisode\s(?P<episode>\d+)(?::|\s[-–]\s)(?P<episode_title>.+?)\s*$",
    re.IGNORECASE,
)
_FLS_EPISODE_ONLY_RE = re.compile(
    r"^(?P<series>.+?)\s[-–]\sEpisode\s(?P<episode>\d+)\s*$",
    re.IGNORECASE,
)
_FLS_SEASON_EPISODE_RE = re.compile(
    r"^(?P<series>.+?)\sSeason\s(?P<season>\d+)\sEpisode\s(?P<episode>\d+)(?:\s*[-:]\s*(?P<episode_title>.+?))?\s*$",
    re.IGNORECASE,
)
_FLS_SEASON_COMMA_EPISODE_RE = re.compile(
    r"^(?P<series>.+?)\sSeason\s(?P<season>\d+),\s*Episode\s(?P<episode>\d+)(?:,\s*(?P<episode_title>.+?))?\s*$",
    re.IGNORECASE,
)
_FLS_SERIES_EPISODE_RE = re.compile(
    r"^(?P<series>.+?)(?::\s*|\s[-–]\s|\s+)Episode\s*(?P<episode>\d+)(?:(?:\s*[-:]\s*|\s+)(?P<episode_title>.+?))?\s*$",
    re.IGNORECASE,
)
_FLS_REPEATED_SERIES_EPISODE_RE = re.compile(
    r"^(?P<series>.+?)\s[-–]\s(?P<dup>.+?)\s(?P<code>\d+):\s*Episode\s(?P<episode>\d+)\s*$",
    re.IGNORECASE,
)


def _replace_macros(url: str) -> str:
    for key, value in MACRO_REPLACEMENTS.items():
        url = url.replace(key, value)
    url = MACRO_RE.sub("", url)
    return url


def _parse_freelivesports_title(raw: str | None) -> tuple[str | None, int | None, int | None, str | None]:
    if not raw:
        return raw, None, None, None

    title = raw.strip()

    def _normalize_episode_title(series: str, episode: int, raw_episode_title: str | None) -> str | None:
        episode_label = f"Episode {episode}"
        candidate = (raw_episode_title or "").strip(" -:") or None
        if not candidate:
            return episode_label
        if candidate.upper() == "NEW":
            return episode_label
        if candidate == title or candidate == f"{series} {episode_label}":
            return episode_label
        return candidate

    match = _FLS_SXE_DASH_RE.match(title)
    if match:
        return (
            match.group("series").strip(),
            int(match.group("season")),
            int(match.group("episode")),
            match.group("episode_title").strip() or None,
        )

    match = _FLS_EPISODE_WITH_SUBTITLE_RE.match(title)
    if match:
        episode = int(match.group("episode"))
        return (
            match.group("series").strip(),
            None,
            episode,
            match.group("episode_title").strip() or None,
        )

    match = _FLS_EPISODE_ONLY_RE.match(title)
    if match:
        episode = int(match.group("episode"))
        return match.group("series").strip(), None, episode, f"Episode {episode}"

    match = _FLS_SEASON_EPISODE_RE.match(title)
    if match:
        series = match.group("series").strip()
        season = int(match.group("season"))
        episode = int(match.group("episode"))
        episode_title = _normalize_episode_title(series, episode, match.group("episode_title"))
        return series, season, episode, episode_title

    match = _FLS_SEASON_COMMA_EPISODE_RE.match(title)
    if match:
        series = match.group("series").strip()
        season = int(match.group("season"))
        episode = int(match.group("episode"))
        episode_title = _normalize_episode_title(series, episode, match.group("episode_title"))
        return series, season, episode, episode_title

    match = _FLS_SERIES_EPISODE_RE.match(title)
    if match:
        series = match.group("series").strip()
        episode = int(match.group("episode"))
        episode_title = _normalize_episode_title(series, episode, match.group("episode_title"))
        return series, None, episode, episode_title

    match = _FLS_REPEATED_SERIES_EPISODE_RE.match(title)
    if match:
        series = match.group("series").strip()
        dup = match.group("dup").strip()
        if dup.startswith(series):
            episode = int(match.group("episode"))
            return series, None, episode, f"Episode {episode}"

    return title, None, None, None


class FreeLiveSportsScraper(BaseScraper):
    source_name     = "freelivesports"
    display_name    = "FreeLiveSports"
    stream_audit_enabled = True
    scrape_interval = 360
    config_schema   = []   # public API, no credentials needed

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.freelivesports.tv/",
            "Origin":  "https://www.freelivesports.tv",
        })

    # ── Required ──────────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        r = self.get(EPG_URL)
        if not r:
            return []

        try:
            data = r.json()
        except Exception as e:
            logger.error("[freelivesports] JSON decode failed: %s", e)
            return []

        raw = data if isinstance(data, list) else data.get("channels", [])

        # Sort by channel number upstream
        raw.sort(key=lambda c: c.get("channelNumber", 9999))

        channels = []
        for ch in raw:
            channel_id = ch.get("_id", "")
            if not channel_id:
                continue

            name = (ch.get("name") or "").strip()
            if not name:
                continue

            stream_url = ch.get("url", "")
            if not stream_url:
                logger.warning("[freelivesports] no stream URL for '%s', skipping", name)
                continue

            description = (ch.get("description") or "").strip() or None

            channels.append(ChannelData(
                source_channel_id = channel_id,
                name              = name,
                # Store the raw URL with macros intact; resolve() will expand them
                stream_url        = stream_url,
                stream_type       = "hls",
                logo_url          = ch.get("thumbnail") or None,
                category          = "Sports",
                language          = infer_language_from_metadata(ch.get("language"), name),
                country           = "US",
                number            = ch.get("channelNumber") or None,
                description       = description,
            ))

        logger.info("[freelivesports] %d channels", len(channels))
        return channels

    # ── Optional: EPG ─────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        # EPG data is bundled in the same catalog response — re-fetch to get entries
        r = self.get(EPG_URL)
        if not r:
            return []

        try:
            data = r.json()
        except Exception as e:
            logger.warning("[freelivesports] EPG JSON decode failed: %s", e)
            return []

        raw = data if isinstance(data, list) else data.get("channels", [])

        # Build a quick lookup: channel name → source_channel_id
        # (The EPG entries live inside each channel object)
        id_by_name: dict[str, str] = {ch.name: ch.source_channel_id for ch in channels}
        # Also map by _id directly
        id_set = {ch.source_channel_id for ch in channels}

        programs: list[ProgramData] = []

        for ch in raw:
            channel_id = ch.get("_id", "")
            if channel_id not in id_set:
                continue

            epg = ch.get("epg") or {}
            entries = epg.get("entries") or []

            for entry in entries:
                start_ts = _parse_ts(entry.get("start", ""))
                stop_ts  = _parse_ts(entry.get("stop", ""))
                if not start_ts or not stop_ts:
                    continue
                title, season, episode, episode_title = _parse_freelivesports_title(entry.get("title"))

                programs.append(ProgramData(
                    source_channel_id = channel_id,
                    title             = title or "Unknown",
                    description       = (entry.get("description") or "").strip() or None,
                    start_time        = start_ts,
                    end_time          = stop_ts,
                    poster_url        = entry.get("image") or None,
                    category          = "Sports",
                    episode_title     = episode_title,
                    season            = season,
                    episode           = episode,
                ))

        logger.info("[freelivesports] %d EPG entries", len(programs))
        return programs

    # ── Optional: resolve ─────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        """
        Expand URL macros at playback time so values are always fresh.
        The [CB] (cache-buster) macro in particular must be current.
        """
        # Refresh the timestamp-based macros on every call
        fresh = {
            **MACRO_REPLACEMENTS,
            "[CB]": str(int(datetime.now(timezone.utc).timestamp())),
            "[UA]": self.session.headers.get("User-Agent", ""),
        }
        url = raw_url
        for key, value in fresh.items():
            url = url.replace(key, value)
        url = MACRO_RE.sub("", url)
        return url


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_ts(iso: str) -> datetime | None:
    """Parse ISO-8601 UTC string to a timezone-aware datetime."""
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
    except Exception:
        return None

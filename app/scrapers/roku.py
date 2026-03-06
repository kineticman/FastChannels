# app/scrapers/roku.py
#
# The Roku Channel — FAST live TV scraper
#
# Auth flow (fully headless, no browser):
#   1. GET /live-tv              → session cookies
#   2. GET /api/v1/csrf          → csrf token
#   3. GET content proxy         → playId + linearSchedule (now/next EPG)
#   4. POST /api/v3/playback     → JWT-signed osm.sr.roku.com stream URL
#
# stream_url stored as: roku://{station_id}
# resolve() boots a fresh session on demand and calls /api/v3/playback
# Token caching: csrf + cookies cached for 55 minutes (they last ~1hr)

from __future__ import annotations

import logging
import re
import base64
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote

from .base import BaseScraper, ChannelData, ProgramData

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

_BASE        = "https://therokuchannel.roku.com"
_LIVE_TV     = f"{_BASE}/live-tv"
_CSRF_URL    = f"{_BASE}/api/v1/csrf"
_PLAYBACK    = f"{_BASE}/api/v3/playback"
_CONTENT_TPL = "https://content.sr.roku.com/content/v1/roku-trc/{sid}"
_PROXY_BASE  = f"{_BASE}/api/v2/homescreen/content/"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

_EPG_URL = f"{_BASE}/api/v2/epg"

# Tags in the EPG station object → human-readable category (checked in order)
_TAG_CATEGORY_PRIORITY = [
    ('news',            'News'),
    ('spanish-language','Spanish'),
    ('music',           'Music'),
    ('kids_music',      'Kids'),
    ('kids_linear',     'Kids'),
    ('ages_1-3',        'Kids'),
    ('ages_4-6',        'Kids'),
    ('ages_7-9',        'Kids'),
    ('ages_10plus',     'Kids'),
    ('educational',     'Kids'),
    ('preschool_specials', 'Kids'),
]

_SESSION_TTL = 55 * 60  # seconds before we refresh cookies + csrf


# ── Category helpers ───────────────────────────────────────────────────────────

def _category_from_station(station: dict) -> str:
    """Derive a human-readable category from an EPG station object."""
    if station.get("kidsDirected"):
        return "Kids"
    tags = set(station.get("tags", []))
    for tag, label in _TAG_CATEGORY_PRIORITY:
        if tag in tags:
            return label
    # channelcode_* tags hint at genre
    for tag in tags:
        tl = tag.lower()
        if "reality" in tl or "wedding" in tl:
            return "Reality TV"
        if "thriller" in tl or "movie" in tl or "film" in tl or "ifc" in tl:
            return "Movies"
        if "comedy" in tl:
            return "Comedy"
        if "drama" in tl or "stories" in tl:
            return "Drama"
    return "Live TV"


def _cat_id_to_label(cat_id: str) -> str:
    """Convert a Roku cat-* ID to a friendly label (best effort)."""
    _MAP = {
        "cat-news": "News", "cat-national-news": "News", "cat-epg-news-opinion": "News",
        "cat-sports": "Sports", "cat-sports-general": "Sports",
        "cat-movies": "Movies", "cat-movie": "Movies",
        "cat-comedy": "Comedy", "cat-drama": "Drama",
        "cat-reality": "Reality TV", "cat-lifestyle": "Lifestyle",
        "cat-food": "Food", "cat-music": "Music",
        "cat-kids": "Kids", "cat-family": "Kids",
    }
    return _MAP.get(cat_id, "Live TV")


# ── Scraper ────────────────────────────────────────────────────────────────────

class RokuScraper(BaseScraper):

    source_name     = "roku"
    display_name    = "The Roku Channel"
    scrape_interval = 60
    drm_check_enabled = True          # EPG is now/next only, refresh hourly

    # No config needed — fully anonymous, no credentials
    config_schema = []

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update({
            "User-Agent":      _UA,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept":          "application/json",
        })

        # Session state — refreshed when expired
        self._csrf_token:    Optional[str]   = None
        self._session_born:  Optional[float] = None   # epoch seconds

    # ── Session management ─────────────────────────────────────────────────────

    def _session_is_fresh(self) -> bool:
        if not self._csrf_token or not self._session_born:
            return False
        return (time.time() - self._session_born) < _SESSION_TTL

    def _refresh_session(self) -> bool:
        """Boot a fresh Roku browser session. Returns True on success."""
        try:
            # Step 1: hit live-tv to collect cookies
            r1 = self.session.get(_LIVE_TV, timeout=15)
            if r1.status_code != 200:
                logger.error("[roku] live-tv returned %d", r1.status_code)
                return False

            # Step 2: fetch csrf token (retry up to 4 times)
            csrf = None
            for attempt in range(5):
                r2 = self.session.get(_CSRF_URL, timeout=10)
                if r2.status_code == 200:
                    csrf = r2.json().get("csrf")
                    break
                wait = 2 ** attempt
                logger.warning("[roku] csrf attempt %d returned %d, retry in %ds",
                               attempt + 1, r2.status_code, wait)
                time.sleep(wait)

            if not csrf:
                logger.error("[roku] could not obtain csrf token")
                return False

            self._csrf_token   = csrf
            self._session_born = time.time()
            logger.debug("[roku] session refreshed, csrf=%s…", csrf[:12])
            return True

        except Exception as exc:
            logger.error("[roku] session refresh failed: %s", exc)
            return False

    def _ensure_session(self) -> bool:
        if not self._session_is_fresh():
            return self._refresh_session()
        return True

    def _api_headers(self) -> dict:
        return {
            "csrf-token":                         self._csrf_token or "",
            "origin":                             _BASE,
            "referer":                            _LIVE_TV,
            "content-type":                       "application/json",
            "x-roku-reserved-amoeba-ids":         "",
            "x-roku-reserved-experiment-configs": "e30=",
            "x-roku-reserved-experiment-state":   "W10=",
            "x-roku-reserved-lat":                "0",
        }

    # ── Content proxy helper ───────────────────────────────────────────────────

    def _fetch_content(self, station_id: str, feature_include: str = "") -> Optional[dict]:
        """Call the therokuchannel content proxy for a given station_id."""
        qs = f"?featureInclude={feature_include}" if feature_include else ""
        content_url = _CONTENT_TPL.format(sid=station_id) + qs
        proxy_url   = _PROXY_BASE + quote(content_url, safe="")
        try:
            r = self.session.get(proxy_url, headers=self._api_headers(), timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception as exc:
            logger.warning("[roku] content fetch error for %s: %s", station_id, exc)
        return None

    # ── fetch_channels ─────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        if not self._ensure_session():
            return []

        channels: list[ChannelData] = []
        seen: set[str] = set()

        # ── Phase 1: /api/v2/epg — returns all ~795 live channels ─────────────
        # Each collection item has features.station with full channel metadata.
        try:
            r = self.session.get(_EPG_URL, headers=self._api_headers(), timeout=20)
            if r.status_code == 200:
                for col in r.json().get("collections", []):
                    station = col.get("features", {}).get("station")
                    if not station:
                        continue
                    sid = station.get("meta", {}).get("id")
                    if not sid or sid in seen:
                        continue
                    self._add_channel_from_station(channels, seen, sid, station)
            else:
                logger.warning("[roku] EPG returned %d", r.status_code)
        except Exception as exc:
            logger.warning("[roku] EPG fetch failed: %s", exc)

        # ── Phase 2: billboard (hero channels, fills any EPG gaps) ────────────
        try:
            r2 = self.session.get(
                f"{_BASE}/api/v1/billboard/landing/trc-us-live-ml-page-en-current",
                headers=self._api_headers(),
                timeout=10,
            )
            if r2.status_code == 200:
                for item in r2.json():
                    sid = (item.get("meta") or {}).get("id")
                    if not sid or sid in seen:
                        continue
                    self._add_channel_from_content(channels, seen, sid, item)
        except Exception as exc:
            logger.warning("[roku] billboard fetch failed: %s", exc)

        if not channels:
            logger.error("[roku] fetch_channels returned 0 channels — session may be bad")
        else:
            logger.info("[roku] %d channels fetched", len(channels))

        return channels

    def _add_channel_from_station(
        self,
        channels: list[ChannelData],
        seen: set[str],
        station_id: str,
        station: dict,
    ) -> None:
        """Add a channel parsed from an EPG features.station object."""
        seen.add(station_id)

        title  = station.get("title") or station.get("shortName") or "Unknown"
        number = station.get("displayNumber")

        # Logo: prefer gridEpg → epgLogo → liveHudLogo
        logo = None
        image_map = station.get("imageMap") or {}
        for key in ("gridEpg", "epgLogo", "liveHudLogo", "epgLogoDark"):
            img = image_map.get(key)
            if img and img.get("path"):
                logo = img["path"]
                break

        # Category from kidsDirected flag or tags
        category = _category_from_station(station)

        channels.append(ChannelData(
            source_channel_id = station_id,
            name              = title,
            stream_url        = f"roku://{station_id}",
            logo_url          = logo,
            category          = category,
            language          = "en",
            country           = "US",
            stream_type       = "hls",
            number            = number,
            slug              = f"|",  # playId resolved on demand; no gracenoteId from EPG
        ))

    def _add_channel_from_content(
        self,
        channels: list[ChannelData],
        seen: set[str],
        station_id: str,
        item: dict,
    ) -> None:
        """Add a channel parsed from a content-proxy / billboard item."""
        seen.add(station_id)

        title = item.get("title", "Unknown")

        # Logo: prefer grid thumbnail
        logo = None
        image_map = item.get("imageMap") or {}
        for key in ("grid", "gridEpg", "detailBackground", "detailPoster"):
            img = image_map.get(key)
            if img and img.get("path"):
                logo = img["path"]
                break

        # Category from categories list
        cats = item.get("categories") or []
        category = _cat_id_to_label(cats[0]) if cats else None

        # playId from viewOptions
        view_opts = item.get("viewOptions") or [{}]
        play_id   = view_opts[0].get("playId") if view_opts else None

        # Gracenote station ID (numeric stationId in the EPG schedule)
        gracenote_id = item.get("gracenoteStationId") or item.get("stationId") or ""
        if gracenote_id and not str(gracenote_id).isdigit():
            gracenote_id = ""

        channels.append(ChannelData(
            source_channel_id = station_id,
            name              = title,
            stream_url        = f"roku://{station_id}",
            logo_url          = logo,
            category          = category,
            language          = "en",
            country           = "US",
            stream_type       = "hls",
            slug              = f"{play_id or ''}|{gracenote_id}",
        ))

    # ── fetch_epg ──────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData]) -> list[ProgramData]:
        if not self._ensure_session():
            return []

        programs: list[ProgramData] = []
        total = len(channels)

        for i, ch in enumerate(channels):
            sid = ch.source_channel_id
            try:
                data = self._fetch_content(sid, feature_include="linearSchedule")
                if not data:
                    continue

                schedule = data.get("features", {}).get("linearSchedule", [])
                for entry in schedule:
                    prog = self._parse_program(sid, entry)
                    if prog:
                        programs.append(prog)

            except Exception as exc:
                logger.warning("[roku] EPG error for %s (%s): %s", ch.name, sid, exc)

            if (i + 1) % 50 == 0:
                logger.info("[roku] EPG progress: %d/%d channels", i + 1, total)

            time.sleep(0.25)  # be polite

        logger.info("[roku] %d EPG entries fetched for %d channels", len(programs), total)
        return programs

    def _parse_program(self, station_id: str, entry: dict) -> Optional[ProgramData]:
        try:
            start_str = entry.get("date", "")
            start = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            duration = entry.get("duration", 0)
            end = start + timedelta(seconds=duration)
        except (ValueError, TypeError):
            return None

        c       = entry.get("content", {})
        series  = c.get("series", {})
        ep_title = c.get("title", "")
        series_title = series.get("title", "")
        title   = series_title or ep_title or "Unknown"

        # Description
        descs = c.get("descriptions") or {}
        description = (
            descs.get("250") or descs.get("60") or descs.get("40") or c.get("description")
        )

        # Artwork — prefer gridEpg, fall back to grid
        image_map = c.get("imageMap") or {}
        poster = (
            (image_map.get("gridEpg") or {}).get("path")
            or (image_map.get("grid") or {}).get("path")
        )

        # Rating
        ratings = c.get("parentalRatings") or []
        rating  = ratings[0].get("code") if ratings else None

        # Season / Episode
        season  = c.get("seasonNumber")
        episode = c.get("episodeNumber")
        try:
            season  = int(season)  if season  else None
            episode = int(episode) if episode else None
        except (ValueError, TypeError):
            season = episode = None

        # Category from genres
        genres = c.get("genres") or []
        category = genres[0].capitalize() if genres else None

        return ProgramData(
            source_channel_id = station_id,
            title             = title,
            start_time        = start,
            end_time          = end,
            description       = description,
            poster_url        = poster,
            category          = category,
            rating            = rating,
            episode_title     = ep_title if series_title and ep_title != series_title else None,
            season            = season,
            episode           = episode,
        )

    # ── resolve ────────────────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        """
        raw_url format: roku://{station_id}
        Returns a live osm.sr.roku.com HLS/DASH stream URL.
        Calls /api/v3/playback with a fresh session each time.
        The JWT in the stream URL is short-lived so we always fetch fresh.
        """
        if not raw_url.startswith("roku://"):
            return raw_url

        station_id = raw_url[len("roku://"):]

        if not self._ensure_session():
            logger.error("[roku] resolve failed — could not obtain session")
            return raw_url

        # Step 1: get playId from content proxy
        data    = self._fetch_content(station_id)
        play_id = None
        if data:
            view_opts = data.get("viewOptions") or [{}]
            play_id   = view_opts[0].get("playId") if view_opts else None

        if not play_id:
            # Try regex fallback from raw response
            content_url = _CONTENT_TPL.format(sid=station_id)
            proxy_url   = _PROXY_BASE + quote(content_url, safe="")
            try:
                r = self.session.get(proxy_url, headers=self._api_headers(), timeout=10)
                pids = re.findall(r's-[a-z0-9_]+\.[A-Za-z0-9+/=]+', r.text)
                play_id = pids[0] if pids else None
            except Exception:
                pass

        if not play_id:
            logger.warning("[roku] no playId found for %s", station_id)
            return raw_url

        # Decode to determine media format
        try:
            decoded = base64.b64decode(play_id.split(".", 1)[1]).decode()
            media_format = "mpeg-dash" if "dash" in decoded.lower() else "m3u"
        except Exception:
            media_format = "m3u"

        # Step 2: call /api/v3/playback
        session_id = self.session.cookies.get("_usn", "roku-scraper")
        body = {
            "rokuId":      station_id,
            "playId":      play_id,
            "mediaFormat": media_format,
            "drmType":     "widevine",
            "quality":     "fhd",
            "bifUrl":      None,
            "adPolicyId":  "",
            "providerId":  "rokuavod",
            "playbackContextParams": (
                f"sessionId={session_id}"
                "&pageId=trc-us-live-ml-page-en-current"
                "&isNewSession=0&idType=roku-trc"
            ),
        }
        try:
            r2 = self.session.post(
                _PLAYBACK,
                headers=self._api_headers(),
                json=body,
                timeout=10,
            )
            if r2.status_code == 200:
                stream_url = r2.json().get("url", "")
                if stream_url:
                    logger.debug("[roku] resolved %s -> %s…", station_id, stream_url[:60])
                    return stream_url
            logger.warning("[roku] playback returned %d for %s", r2.status_code, station_id)
        except Exception as exc:
            logger.error("[roku] playback request failed for %s: %s", station_id, exc)

        return raw_url

    # ── M3U extras ─────────────────────────────────────────────────────────────
    # FastChannels calls generate_m3u() which uses ChannelData fields.
    # We stuffed "playId|gracenoteId" into slug. Override the M3U line builder
    # to emit tvc-guide-stationid for channels that have a Gracenote ID.
    # BaseScraper's generate_m3u() calls channel_m3u_tags() if it exists.

    def channel_m3u_tags(self, ch: ChannelData) -> dict[str, str]:
        """
        Return extra M3U tags for this channel.
        Called by BaseScraper.generate_m3u() if the method exists.
        """
        tags: dict[str, str] = {}

        # Unpack gracenoteId from slug field (format: "playId|gracenoteId")
        if ch.slug and "|" in ch.slug:
            _, gracenote_id = ch.slug.split("|", 1)
            if gracenote_id and gracenote_id.isdigit():
                # This tells Channels DVR to pull full guide data from Gracenote
                tags["tvc-guide-stationid"] = gracenote_id

        return tags

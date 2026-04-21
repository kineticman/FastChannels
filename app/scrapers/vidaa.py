from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urlparse, urlunparse

from .base import BaseScraper, ChannelData, ProgramData, infer_language_from_metadata
from .category_utils import category_for_channel, infer_category_from_name

logger = logging.getLogger(__name__)

_USER_AGENT   = "NitroX/1.17.0-2 (Google sdk_gphone_x86; Android 11; mobile; release)"
_CONFIG_URL   = "https://vtvapp-ovp.vidaahub.com/cms/vidaa-adrenalin8/clientconfiguration/versions/2"
_LOCATION_URL = "https://vtvapp-ovp.vidaahub.com/sso-login/location"
_STATIONS_FIELDS = (
    "taxonomyTerms,title,uid,assetType,externalResources,relations,"
    "channelNumber,isFast,externalIds,language,taxonomyParentTerms,"
    "androidDeeplinkBaseUrl,organizationDomainUrl"
)
_EPG_CHUNK_SIZE = 50
_EPG_HOURS      = 168  # ~7-day window; actual upstream horizon is ~5 days

_MACRO_RE       = re.compile(r'^\[.*\]$|^REPLACEME$|^\{.*\}$')
_GENRE_PARAM_RE = re.compile(r'(?:AV_CONTENT_GENRE|content_genre|_fw_content_genre)=([^&]+)', re.I)
_RATING_PARAM_RE = re.compile(r'(?:AV_CONTENT_RATING|content_rating|_fw_content_rating)=([^&]+)', re.I)
_IAB_PARAM_RE   = re.compile(r'(?:AV_CONTENT_CAT|content_category|_fw_content_category)=([^&]+)', re.I)

_IAB_GENRES: dict[str, str] = {
    "IAB1":     "Entertainment",
    "IAB1-5":   "Movies",
    "IAB1-6":   "Music",
    "IAB1-7":   "Entertainment",
    "IAB6":     "Kids",
    "IAB12":    "News",
    "IAB12-1":  "News",
    "IAB17":    "Sports",
    "IAB17-6":  "Sports",
    "IAB17-9":  "Sports",
    "IAB17-10": "Sports",
    "IAB17-44": "Sports",
    "IAB18":    "Lifestyle",
    "IAB20":    "Travel",
    "IAB22":    "Shopping",
    "IAB23-2":  "Faith",
}

_GENRE_NORM: dict[str, str] = {
    "television":    "Entertainment",
    "entertainment": "Entertainment",
    "movies":        "Movies",
    "movie":         "Movies",
    "gameshow":      "Game Shows",
    "realitytv":     "Reality TV",
    "reality":       "Reality TV",
    "music":         "Music",
    "sports":        "Sports",
    "sport":         "Sports",
    "soccer":        "Sports",
    "news":          "News",
    "religious":     "Faith",
    "religion":      "Faith",
    "animation":     "Kids",
    "shopping":      "Shopping",
    "lifestyle":     "Lifestyle",
    "drama":         "Drama",
    "comedy":        "Comedy",
    "variedades":    "Entertainment",
    "soapopera":     "Drama",
    "daytimadrama":  "Drama",
}

_US_TV_RATINGS = {"TV-Y", "TV-Y7", "TV-G", "TV-PG", "TV-14", "TV-MA"}
_MPAA_RATINGS  = {"G", "PG", "PG-13", "R", "NC-17"}


def _normalize_genre(raw: str) -> str | None:
    raw = unquote(raw).strip()
    first = re.split(r',', raw)[0].strip()
    key = re.sub(r'[\s_-]', '', first.lower())
    return _GENRE_NORM.get(key, first if first else None)


def _extract_genre(station: dict[str, Any], stream_url: str) -> str | None:
    tax = station.get("taxonomyTerms") or {}
    if isinstance(tax, dict):
        genres = list((tax.get("genres") or {}).values())
        if genres:
            return _normalize_genre(genres[0]) or genres[0]
    m = _GENRE_PARAM_RE.search(stream_url)
    if m:
        raw = unquote(m.group(1)).strip()
        if not _MACRO_RE.match(raw):
            g = _normalize_genre(raw)
            if g:
                return g
    m = _IAB_PARAM_RE.search(stream_url)
    if m:
        iab = unquote(m.group(1)).strip().upper()
        if not _MACRO_RE.match(iab):
            return _IAB_GENRES.get(iab)
    return None


def _extract_rating(stream_url: str) -> str | None:
    m = _RATING_PARAM_RE.search(stream_url)
    if not m:
        return None
    raw = unquote(m.group(1)).strip().upper().replace("TVG", "TV-G").replace("TV14", "TV-14")
    if raw in _US_TV_RATINGS or raw in _MPAA_RATINGS:
        return raw
    return None


def _event_genre(event: dict[str, Any]) -> str | None:
    tax = event.get("taxonomyTerms") or {}
    if isinstance(tax, dict):
        genres = list((tax.get("genres") or {}).values())
        if genres:
            return genres[0]
    return None


def _event_rating(event: dict[str, Any]) -> str | None:
    for r in event.get("parentalRatings") or []:
        if not isinstance(r, dict):
            continue
        val = (r.get("rating") or "").upper().replace("TVG", "TV-G")
        if val in _US_TV_RATINGS or val in _MPAA_RATINGS:
            return val
    return None


def _clean_stream_url(url: str) -> str:
    """Strip ad-macro placeholder params and fix the Triton Poker malformed URL."""
    parsed = urlparse(url)
    params = parse_qsl(parsed.query, keep_blank_values=True)
    seen: set[str] = set()
    clean = []
    for k, v in params:
        # Triton Poker has raw JSON appended after an unescaped '"' — truncate at it
        if '"' in v:
            v = v[:v.index('"')]
        if not _MACRO_RE.match(v) and k not in seen:
            clean.append((k, v))
            seen.add(k)
    return urlunparse(parsed._replace(query=urlencode(clean)))


def _pick_logo(station: dict[str, Any]) -> str | None:
    imgs = (station.get("externalResources") or {}).get("image") or []
    if not imgs:
        return None
    def _norm(ar: str | None) -> str:
        return (ar or "").replace(":", "x").lower()
    for preferred in ("1x1", "16x9"):
        for img in imgs:
            if _norm((img.get("metadata") or {}).get("aspectRatio")) == preferred:
                return img.get("cdnUrl")
    return imgs[0].get("cdnUrl")


class VidaaScraper(BaseScraper):
    source_name          = "vidaa"
    display_name         = "Vidaa Free TV"
    scrape_interval      = 720   # 12 hours; EPG horizon is ~5 days
    stream_audit_enabled = True
    config_schema        = []

    # EPG fetches ~4 chunked requests for 191 stations; give it room
    phase_timeouts = {
        "init":      30,
        "bootstrap": 60,
        "channels":  120,
        "epg":       900,
    }

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({"User-Agent": _USER_AGENT})
        self._bo_url: str | None = None
        self._tenant: str | None = None
        self._geo_code: str = "us"
        self._station_headers: dict[str, str] | None = None

    # ── bootstrap ─────────────────────────────────────────────────────────────

    def _bootstrap(self) -> None:
        r = self.session.get(_CONFIG_URL, timeout=30)
        r.raise_for_status()
        outer = r.json()
        raw_inner = outer.get("configuration")
        if not raw_inner:
            raise RuntimeError("Vidaa client configuration missing 'configuration' key")
        app_config = json.loads(raw_inner)
        self._bo_url = app_config["Environment"]["BOURL"]
        self._tenant = app_config["Environment"]["Tenant"]

        r = self.session.get(
            _LOCATION_URL, timeout=15,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        r.raise_for_status()
        self._geo_code = r.json().get("geo_code", "us")

        self._station_headers = {
            "Accept-Language": "en",
            "x-language":      "en",
            "x-user-device":   "android-mobile",
            "x-user-domain":   self._geo_code,
        }
        logger.info("[vidaa] bootstrap — BOURL=%s tenant=%s geo=%s",
                    self._bo_url, self._tenant, self._geo_code)

    def _ensure_bootstrap(self) -> None:
        if self._bo_url is None:
            self._bootstrap()

    # ── fetch_channels ─────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        self._ensure_bootstrap()

        stations_url = (
            f"{self._bo_url}/catalogue-search/{self._tenant}"
            f"/search/public/usercontext/epg/stations"
            f"?{urlencode({'fields': _STATIONS_FIELDS})}"
        )
        r = self.session.get(stations_url, timeout=60, headers=self._station_headers)
        r.raise_for_status()
        stations = r.json()

        if not isinstance(stations, list):
            raise RuntimeError(f"Vidaa stations response was not a list: {type(stations)}")

        channels: list[ChannelData] = []
        for station in stations:
            uid  = station.get("uid")
            name = station.get("title")
            if not uid or not name:
                continue

            streams = (station.get("externalResources") or {}).get("liveStream") or []
            if not streams:
                logger.debug("[vidaa] skipping %r — no streams", name)
                continue

            primary    = streams[0]
            meta       = primary.get("metadata") or {}
            raw_url    = primary.get("url") or ""
            if not raw_url:
                continue

            stream_url   = _clean_stream_url(raw_url)
            drm          = meta.get("drmType")
            stream_type  = "dash" if drm else "hls"
            genre        = _extract_genre(station, raw_url)
            logo         = _pick_logo(station)
            number       = station.get("channelNumber")
            station_id   = (station.get("externalIds") or {}).get("tva-stationId")

            # API returns "en" for all stations regardless of actual language.
            # AV_CONTENT_LANGUAGE in Aniview stream URLs is the most reliable signal.
            # 'ara' is ISO 639-2; normalise to ISO 639-1 'ar'.
            # 'pt' and 'sp' (non-standard) collapse to 'es' — app has no separate pt slot.
            m = re.search(r'AV_CONTENT_LANGUAGE=([a-z]{2,3})', raw_url, re.I)
            url_lang = m.group(1).lower() if m else None
            _LANG_MAP = {'es': 'es', 'pt': 'es', 'sp': 'es',
                         'ara': 'ar', 'hi': 'hi', 'pa': 'pa', 'de': 'de'}
            if url_lang and url_lang in _LANG_MAP:
                language = _LANG_MAP[url_lang]
            else:
                language = infer_language_from_metadata(name)

            channels.append(ChannelData(
                source_channel_id = uid,
                name              = name,
                stream_url        = stream_url,
                logo_url          = logo,
                category          = category_for_channel(name, genre) or infer_category_from_name(name),
                language          = language,
                country           = "US",
                stream_type       = stream_type,
                number            = int(number) if number is not None else None,
                guide_key         = station_id,
            ))

        logger.info("[vidaa] %d channels fetched", len(channels))
        return channels

    # ── fetch_epg ──────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        self._ensure_bootstrap()

        station_ids = [ch.source_channel_id for ch in channels]
        if not station_ids:
            return []

        now       = datetime.now(UTC)
        end       = now + timedelta(hours=_EPG_HOURS)
        start_str = now.strftime("%Y-%m-%dT%H:%MZ")
        end_str   = end.strftime("%Y-%m-%dT%H:%MZ")

        # Channel-level genre for EPG fallback
        uid_to_genre = {ch.source_channel_id: ch.category for ch in channels if ch.category}

        all_entries: list[dict] = []
        for i in range(0, len(station_ids), _EPG_CHUNK_SIZE):
            chunk = station_ids[i : i + _EPG_CHUNK_SIZE]
            epg_url = (
                f"{self._bo_url}/catalogue-search/{self._tenant}"
                f"/search/public/usercontext/epg/grid"
                f"?{urlencode({'stationIds': ','.join(chunk), 'startTime': start_str, 'endTime': end_str})}"
            )
            try:
                r = self.session.get(epg_url, timeout=60, headers=self._station_headers)
                r.raise_for_status()
                chunk_data = r.json()
                if isinstance(chunk_data, list):
                    all_entries.extend(chunk_data)
            except Exception as exc:
                logger.warning("[vidaa] EPG chunk %d-%d failed: %s", i, i + _EPG_CHUNK_SIZE, exc)

        programs: list[ProgramData] = []
        for entry in all_entries:
            station_uid = entry.get("uid")
            if not station_uid:
                continue
            chan_genre = uid_to_genre.get(station_uid)

            for event in entry.get("events") or []:
                raw_title = (event.get("title") or "").strip()
                start_raw = event.get("startTime")
                end_raw   = event.get("endTime")
                if not raw_title or not start_raw or not end_raw:
                    continue
                try:
                    start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
                    end_dt   = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
                except ValueError:
                    continue
                if end_dt <= start_dt:
                    continue

                # Korean EPG encodes episode number as <N> at the end of the title
                ep_num = None
                ep_match = re.search(r'\s*<(\d+)>\s*$', raw_title)
                if ep_match:
                    ep_num = int(ep_match.group(1))
                    raw_title = raw_title[:ep_match.start()].strip()

                related = ((event.get("relations") or {}).get("event-related-asset") or [{}])[0]
                desc    = (related.get("longDescription") or "").strip() or None

                images = (event.get("externalResources") or {}).get("image") or []
                poster = images[0].get("cdnUrl") if images else None

                programs.append(ProgramData(
                    source_channel_id = station_uid,
                    title             = raw_title,
                    start_time        = start_dt,
                    end_time          = end_dt,
                    description       = desc,
                    poster_url        = poster,
                    category          = _event_genre(event) or chan_genre,
                    rating            = _event_rating(event),
                    episode           = ep_num,
                ))

        logger.info("[vidaa] %d EPG programs fetched across %d stations",
                    len(programs), len(all_entries))
        return programs

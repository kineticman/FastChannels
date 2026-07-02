# app/scrapers/roku.py
#
# The Roku Channel — FAST live TV scraper
#
# Auth flow (fully headless, no browser):
#   1. GET /                     → session cookies
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
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import parse_qs, quote, urlparse

from .base import BaseScraper, ChannelData, ProgramData, StreamDeadError, ScrapeSkipError, format_http_reason, infer_language_from_metadata, is_transient_network_error
from .category_utils import infer_category_from_name
from ..gracenote_map import resolve_gracenote

logger = logging.getLogger(__name__)


def _join_categories(values: list[str] | tuple[str, ...] | None) -> str | None:
    if not values:
        return None
    normalized = []
    for value in values:
        if not value:
            continue
        clean = value.strip()
        if not clean:
            continue
        label = clean[0].upper() + clean[1:]
        if label not in normalized:
            normalized.append(label)
    return ';'.join(normalized) or None


def _language_from_metadata(*values: str | None) -> str:
    return infer_language_from_metadata(*values)

# ── Constants ──────────────────────────────────────────────────────────────────

_BASE        = "https://therokuchannel.roku.com"
_HOME        = f"{_BASE}/"
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
_SESSION_HARD_TTL = 12 * 60 * 60  # discard persisted session state after 12h
_PLAY_ID_TTL = 6 * 60 * 60  # reuse playIds for a few hours to reduce tune-time content lookups
_STREAM_URL_TTL = 5 * 60 * 60  # reuse exact HLS URLs; Roku traceId URLs stay valid ~6h
# DASH+Widevine variant (browser/EME → PrismCast bridge). Roku's playback API will
# serve a CENC DASH manifest (Widevine+PlayReady) for the same channels whose HLS
# variant carries FairPlay (unplayable in Chrome). The Widevine license server URL,
# carrying a per-session token, comes back IN the playback response (not the MPD), so
# the manifest URL and license URL are a matched pair cached together per station.
_DASH_TTL = 5 * 60 * 60  # match HLS TTL — both derive from the same playback session JWT
# Base license host (presence flags the source DRM-capable; real per-session URL is
# captured in resolve_dash() and surfaced via get_license_url()).
_WV_LICENSE_BASE = "https://wv-license.sr.roku.com/license/v2/license/wv"
_OSM_BASE = "https://osm.sr.roku.com"
# OSM stream URL pattern: /osm/v1/hls/master/{selector_uuid}/{session_token}/index.m3u8
_OSM_STREAM_RE = re.compile(r"/osm/v1/hls/master/([0-9a-f-]{36})/([0-9a-f]+)/index\.m3u8")
_SELECTOR_UUID_RE = re.compile(r"/v1/([0-9a-f-]{36})$")
_LIVE_TV_403_RETRIES = 3
_CACHE_WARM_RETRY_WORKERS = 3
_EPG_WORKERS = 3
_DESC_WORKERS = 3
_ROKU_403_COOLDOWN = 5 * 60
_PREWARM_SEED_MAX_ATTEMPTS = 5  # best-effort osm seed: try a few channels, then give up
_DESC_CACHE_TTL = 14 * 24 * 60 * 60  # keep descriptions for 14 days; content doesn't change



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
    # Fall back to name-based keyword matching
    return infer_category_from_name(station.get("title") or "") or "Live TV"


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

    source_name           = "roku"
    display_name          = "The Roku Channel"
    scrape_interval       = 60    # EPG refreshed every hour
    channel_refresh_hours = 0     # fetch channel list every run — it's only ~2 API calls (the /epg call happens in fetch_epg anyway); skipping it saved ~1 call/run and risked silent staleness
    stream_audit_enabled  = True
    # Presence of license_url marks the source DRM-capable and enables the generic
    # /play/roku/license proxy. The real per-session license URL (with token) is
    # captured in resolve_dash() and returned by get_license_url() keyed by station.
    license_url           = _WV_LICENSE_BASE
    # description_cache is large (~1.5MB) and only used on the EPG path, so keep it off
    # the play/resolve hot path — loaded lazily in fetch_epg, not on every tune.
    LAZY_CACHE_KEYS       = frozenset({"description_cache"})
    phase_timeouts        = {
        'init': 30,
        'bootstrap': 60,
        'channels': 120,
        'epg': 900,
    }

    # No config needed — fully anonymous, no credentials
    config_schema = []

    def _retry_config(self):
        from urllib3.util.retry import Retry
        # Disable read retries — content proxy timeouts don't recover on retry,
        # they just add 10s × retry_count of wasted time and log noise.
        # Status retries (429/5xx) still apply via the status_forcelist.
        return Retry(
            total=3,
            connect=3,
            read=0,
            status=2,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=None,
            raise_on_status=False,
        )

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update({
            "User-Agent":      _UA,
            "Accept-Language": "en-US,en;q=0.9",
        })

        # Session state — refreshed when expired
        self._csrf_token:    Optional[str]   = None
        self._session_born:  Optional[float] = None   # epoch seconds
        self._play_id_cache: dict[str, dict[str, object]] = {}
        self._selector_url_cache: dict[str, dict[str, object]] = {}
        self._stream_url_cache: dict[str, dict[str, object]] = {}
        self._osm_session: tuple[str, str] | None = None  # (session_token, traceId) from latest cached OSM URL
        self._last_epg_ok: float = 0.0  # epoch of last successful EPG API response
        self._cooldown_until: float | None = None
        self._cooldown_reason: str | None = None
        self._description_cache: dict[str, str] = {}       # content_id → description text
        self._description_cache_times: dict[str, float] = {}  # content_id → epoch when cached
        self._dash_cache: dict[str, dict[str, object]] = {}  # station_id → {mpd_url, license_url, cached_at}
        self._load_cached_session()
        self._load_play_id_cache()
        self._load_selector_url_cache()
        self._load_osm_session()
        self._load_stream_url_cache()
        self._load_403_cooldown()
        # description_cache is loaded lazily at the start of fetch_epg (see LAZY_CACHE_KEYS)
        # so the play/resolve hot path never deserializes its ~1.5MB blob.
        self._load_dash_cache()

    # ── Session management ─────────────────────────────────────────────────────

    def _session_is_fresh(self) -> bool:
        if not self._csrf_token or not self._session_born:
            return False
        age = time.time() - self._session_born
        return age < _SESSION_HARD_TTL and bool(self.session.cookies)

    def _load_cached_session(self) -> None:
        csrf = (self.config.get("csrf_token") or "").strip()
        born = self.config.get("session_born")
        cookies = self.config.get("session_cookies") or {}
        if not csrf or not isinstance(born, (int, float)) or not isinstance(cookies, dict):
            return
        age = time.time() - float(born)
        if age >= _SESSION_HARD_TTL:
            return
        self._csrf_token = csrf
        self._session_born = float(born)
        self.session.cookies.update(cookies)

    def _persist_session(self) -> None:
        self._update_config("csrf_token", self._csrf_token)
        self._update_config("session_born", self._session_born)
        self._update_config("session_cookies", self.session.cookies.get_dict())

    def _load_403_cooldown(self) -> None:
        until = self.config.get("roku_403_cooldown_until")
        reason = self.config.get("roku_403_cooldown_reason")
        if isinstance(until, (int, float)) and float(until) > time.time():
            self._cooldown_until = float(until)
            self._cooldown_reason = str(reason or "Roku returned 403")

    def _persist_403_cooldown(self) -> None:
        self._update_config("roku_403_cooldown_until", self._cooldown_until)
        self._update_config("roku_403_cooldown_reason", self._cooldown_reason)

    def _cooldown_active(self) -> bool:
        if not self._cooldown_until:
            return False
        if time.time() >= self._cooldown_until:
            self._cooldown_until = None
            self._cooldown_reason = None
            self._persist_403_cooldown()
            return False
        return True

    def _cooldown_remaining(self) -> int:
        if not self._cooldown_until:
            return 0
        return max(0, int(self._cooldown_until - time.time()))

    def _set_403_cooldown(self, reason: str) -> None:
        self._cooldown_until = time.time() + _ROKU_403_COOLDOWN
        self._cooldown_reason = reason
        self._persist_403_cooldown()
        logger.warning("[roku] entering %ds cooldown after 403 (%s)", _ROKU_403_COOLDOWN, reason)

    def _clear_403_cooldown(self) -> None:
        if self._cooldown_until or self._cooldown_reason:
            self._cooldown_until = None
            self._cooldown_reason = None
            self._persist_403_cooldown()

    def _load_play_id_cache(self) -> None:
        raw = self.cache.get("play_id_cache") or {}
        if not isinstance(raw, dict):
            return
        now = time.time()
        for station_id, entry in raw.items():
            if not isinstance(station_id, str) or not isinstance(entry, dict):
                continue
            play_id = entry.get("play_id")
            cached_at = entry.get("cached_at")
            if not play_id or not isinstance(cached_at, (int, float)):
                continue
            if (now - float(cached_at)) >= _PLAY_ID_TTL:
                continue
            self._play_id_cache[station_id] = {
                "play_id": play_id,
                "cached_at": float(cached_at),
            }

    def _persist_play_id_cache(self) -> None:
        self._update_cache("play_id_cache", self._play_id_cache)

    def _cache_play_id(self, station_id: str, play_id: str | None) -> None:
        if not station_id or not play_id:
            return
        self._play_id_cache[station_id] = {
            "play_id": play_id,
            "cached_at": time.time(),
        }
        self._persist_play_id_cache()

    def _cached_play_id(self, station_id: str) -> str | None:
        entry = self._play_id_cache.get(station_id)
        if not entry:
            return None
        play_id = entry.get("play_id")
        cached_at = entry.get("cached_at")
        if not play_id or not isinstance(cached_at, (int, float)):
            return None
        if (time.time() - float(cached_at)) >= _PLAY_ID_TTL:
            self._play_id_cache.pop(station_id, None)
            self._persist_play_id_cache()
            return None
        return str(play_id)

    def _invalidate_play_id(self, station_id: str) -> None:
        if station_id in self._play_id_cache:
            self._play_id_cache.pop(station_id, None)
            self._persist_play_id_cache()

    def _load_selector_url_cache(self) -> None:
        raw = self.cache.get("selector_url_cache") or {}
        if not isinstance(raw, dict):
            return
        now = time.time()
        for station_id, entry in raw.items():
            if not isinstance(station_id, str) or not isinstance(entry, dict):
                continue
            selector_url = entry.get("selector_url")
            cached_at = entry.get("cached_at")
            if not selector_url or not isinstance(cached_at, (int, float)):
                continue
            if (now - float(cached_at)) >= _PLAY_ID_TTL:
                continue
            self._selector_url_cache[station_id] = {
                "selector_url": selector_url,
                "cached_at": float(cached_at),
            }

    def _persist_selector_url_cache(self) -> None:
        self._update_cache("selector_url_cache", self._selector_url_cache)

    def _cache_selector_url(self, station_id: str, selector_url: str | None) -> None:
        if not station_id or not selector_url:
            return
        self._selector_url_cache[station_id] = {
            "selector_url": selector_url,
            "cached_at": time.time(),
        }
        self._persist_selector_url_cache()

    def _cached_selector_url(self, station_id: str) -> str | None:
        entry = self._selector_url_cache.get(station_id)
        if not entry:
            return None
        selector_url = entry.get("selector_url")
        cached_at = entry.get("cached_at")
        if not selector_url or not isinstance(cached_at, (int, float)):
            return None
        if (time.time() - float(cached_at)) >= _PLAY_ID_TTL:
            self._selector_url_cache.pop(station_id, None)
            self._persist_selector_url_cache()
            return None
        return str(selector_url)

    def _invalidate_selector_url(self, station_id: str) -> None:
        if station_id in self._selector_url_cache:
            self._selector_url_cache.pop(station_id, None)
            self._persist_selector_url_cache()

    @staticmethod
    def _extract_selector_url(view_opts: list[dict] | None) -> str | None:
        media = (view_opts[0].get("media") or {}) if view_opts else {}
        videos = media.get("videos") or []
        return next(
            (
                video.get("url")
                for video in videos
                if isinstance(video, dict)
                and str(video.get("videoType", "")).upper() == "HLS"
                and video.get("url")
            ),
            None,
        )

    def _load_stream_url_cache(self) -> None:
        raw = self.cache.get("stream_url_cache") or {}
        if not isinstance(raw, dict):
            return
        now = time.time()
        best_cached_at = 0.0
        for station_id, entry in raw.items():
            if not isinstance(station_id, str) or not isinstance(entry, dict):
                continue
            stream_url = entry.get("stream_url")
            cached_at = entry.get("cached_at")
            if not stream_url or not isinstance(cached_at, (int, float)):
                continue
            if (now - float(cached_at)) >= _STREAM_URL_TTL:
                continue
            self._stream_url_cache[station_id] = {
                "stream_url": stream_url,
                "cached_at": float(cached_at),
            }
            if float(cached_at) > best_cached_at:
                self._try_update_osm_session(str(stream_url))
                if self._osm_session:
                    best_cached_at = float(cached_at)
        # Persist the extracted session token if it wasn't already saved (e.g. first
        # run after the feature was added, or after a cache flush).  Use the original
        # stream-URL timestamp so we don't falsely extend the token's apparent age.
        # Skip if osm_session key is explicitly present (even as None) — that means it
        # was deliberately cleared (expired token) and should not be re-populated here.
        if self._osm_session and "osm_session" not in self.cache and best_cached_at:
            session_token, trace_id = self._osm_session
            self._update_cache("osm_session", {
                "session_token": session_token,
                "trace_id": trace_id,
                "cached_at": best_cached_at,
            })

    def _persist_stream_url_cache(self) -> None:
        self._update_cache("stream_url_cache", self._stream_url_cache)

    def _try_update_osm_session(self, stream_url: str) -> None:
        """Extract session_token+traceId from an OSM stream URL and update _osm_session."""
        m = _OSM_STREAM_RE.search(stream_url)
        if m:
            trace = (parse_qs(urlparse(stream_url).query).get("traceId") or [""])[0]
            self._osm_session = (m.group(2), trace)

    def _load_osm_session(self) -> None:
        """Load a previously persisted OSM session token from the source cache."""
        raw = self.cache.get("osm_session") or {}
        if not isinstance(raw, dict):
            return
        session_token = raw.get("session_token")
        trace_id = raw.get("trace_id", "")
        cached_at = raw.get("cached_at")
        if not session_token or not isinstance(cached_at, (int, float)):
            return
        if (time.time() - float(cached_at)) >= _STREAM_URL_TTL:
            return
        self._osm_session = (str(session_token), str(trace_id))
        logger.debug("[roku] loaded persisted osm_session (age=%.0fs)", time.time() - float(cached_at))

    def _persist_osm_session(self) -> None:
        """Save the current OSM session token to the source cache so it survives restarts."""
        if not self._osm_session:
            return
        session_token, trace_id = self._osm_session
        self._update_cache("osm_session", {
            "session_token": session_token,
            "trace_id": trace_id,
            "cached_at": time.time(),
        })

    def _cache_stream_url(self, station_id: str, stream_url: str | None) -> None:
        if not station_id or not stream_url:
            return
        self._stream_url_cache[station_id] = {
            "stream_url": stream_url,
            "cached_at": time.time(),
        }
        self._persist_stream_url_cache()
        self._try_update_osm_session(stream_url)
        self._persist_osm_session()

    def _cached_stream_url(self, station_id: str) -> str | None:
        entry = self._stream_url_cache.get(station_id)
        if not entry:
            return None
        stream_url = entry.get("stream_url")
        cached_at = entry.get("cached_at")
        if not stream_url or not isinstance(cached_at, (int, float)):
            return None
        if (time.time() - float(cached_at)) >= _STREAM_URL_TTL:
            self._stream_url_cache.pop(station_id, None)
            self._persist_stream_url_cache()
            return None
        return str(stream_url)

    def _invalidate_stream_url(self, station_id: str) -> None:
        if station_id in self._stream_url_cache:
            self._stream_url_cache.pop(station_id, None)
            self._persist_stream_url_cache()

    def _load_description_cache(self) -> None:
        raw = self.load_lazy_cache_key("description_cache") or {}
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                return
        if not isinstance(raw, dict):
            return
        now = time.time()
        for cid, entry in raw.items():
            if not isinstance(entry, dict):
                continue
            desc = entry.get("d")
            t = entry.get("t")
            if not desc or not isinstance(t, (int, float)):
                continue
            if (now - float(t)) >= _DESC_CACHE_TTL:
                continue
            self._description_cache[cid] = str(desc)
            self._description_cache_times[cid] = float(t)
        if len(self._description_cache) > self._DESC_CACHE_MAX_ENTRIES:
            keep = sorted(self._description_cache, key=lambda c: self._description_cache_times.get(c, 0), reverse=True)[:self._DESC_CACHE_MAX_ENTRIES]
            self._description_cache = {c: self._description_cache[c] for c in keep}
            self._description_cache_times = {c: self._description_cache_times[c] for c in keep}

    _DESC_CACHE_MAX_ENTRIES = 5000  # cap to ~1.5MB worst-case; evict oldest first

    def _persist_description_cache(self) -> None:
        cache = self._description_cache
        times = self._description_cache_times
        if len(cache) > self._DESC_CACHE_MAX_ENTRIES:
            keep = sorted(cache, key=lambda c: times.get(c, 0), reverse=True)[:self._DESC_CACHE_MAX_ENTRIES]
            cache = {c: cache[c] for c in keep}
            times = {c: times.get(c, 0) for c in keep}
        serialized = {
            cid: {"d": desc, "t": times.get(cid, time.time())}
            for cid, desc in cache.items()
        }
        # Stored in the source_cache table, which fully replaces the row value on
        # every write (no recursive merge), so expired entries prune correctly even
        # as a plain dict. _load_description_cache still accepts the legacy JSON-string
        # form so values copied verbatim from the old config by the migration load fine.
        self._update_cache("description_cache", serialized)

    def _cache_descriptions(self, new_descs: dict[str, str]) -> None:
        if not new_descs:
            return
        now = time.time()
        for cid, desc in new_descs.items():
            self._description_cache[cid] = desc
            self._description_cache_times[cid] = now
        self._persist_description_cache()

    def _warm_missing_metadata(
        self,
        missing_channels: list[ChannelData],
        headers_snapshot: dict,
        cookies_snapshot: dict,
    ) -> tuple[int, int]:
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if not missing_channels:
            return 0, 0

        thread_local = threading.local()
        warmed_play = 0
        warmed_selector = 0
        lock = threading.Lock()

        def warm_one(ch: ChannelData) -> tuple[str, str | None, str | None]:
            sess = getattr(thread_local, "session", None)
            if sess is None:
                sess = self.new_session(headers=headers_snapshot, cookies=cookies_snapshot)
                thread_local.session = sess
            sess.cookies.update(cookies_snapshot)
            sid = ch.source_channel_id
            qs = "?featureInclude=linearSchedule"
            content_url = _CONTENT_TPL.format(sid=sid) + qs
            proxy_url = _PROXY_BASE + quote(content_url, safe="")
            try:
                response = sess.get(proxy_url, timeout=10)
                if response.status_code != 200:
                    return sid, None, None
                data = response.json()
                view_opts = data.get("viewOptions") or [{}]
                play_id = view_opts[0].get("playId") if view_opts else None
                selector_url = self._extract_selector_url(view_opts)
                return sid, play_id, selector_url
            except Exception:
                return sid, None, None

        with ThreadPoolExecutor(max_workers=_CACHE_WARM_RETRY_WORKERS) as executor:
            futures = {executor.submit(warm_one, ch): ch.source_channel_id for ch in missing_channels}
            for future in as_completed(futures):
                sid, play_id, selector_url = future.result()
                with lock:
                    if play_id:
                        self._cache_play_id(sid, play_id)
                        warmed_play += 1
                    if selector_url:
                        self._cache_selector_url(sid, selector_url)
                        warmed_selector += 1
        return warmed_play, warmed_selector

    def _cached_osm_session(self) -> tuple[str, str] | None:
        """Return (session_token, traceId) from the most recently cached OSM stream URL, or None.

        Updated eagerly on every _cache_stream_url call and on load, so this is O(1).
        """
        return self._osm_session

    def _synthetic_osm_url(self, selector_url: str, session_token: str, trace_id: str) -> str | None:
        """Build a synthetic OSM stream URL by combining selector UUID with a reusable session token."""
        m = _SELECTOR_UUID_RE.search(urlparse(selector_url).path)
        if not m:
            return None
        selector_uuid = m.group(1)
        url = f"{_OSM_BASE}/osm/v1/hls/master/{selector_uuid}/{session_token}/index.m3u8"
        if trace_id:
            url += f"?traceId={trace_id}"
        return url

    def _seed_osm_session(self, channels: list[ChannelData],
                          enabled_ids: set[str] | None = None) -> bool:
        """Call the playback API for one channel to seed _osm_session.

        Used when _osm_session is None (cold start / cache flush) so prewarm
        can proceed without waiting for a user tune.  Best-effort: tries up to
        _PREWARM_SEED_MAX_ATTEMPTS channels until one succeeds.  Returns True
        if the session was seeded.

        Only enabled channels are considered — disabled channels are frequently
        DRM-locked or dead and reliably 403 on playback, which would otherwise
        waste every seed attempt on a channel that can never succeed.  And
        because a per-channel playback 403 is not a site-wide rate-limit, the
        seed never trips the global cooldown (set_cooldown_on_403=False).
        """
        if self._cooldown_active():
            logger.debug("[roku] prewarm seed skipped — cooldown active")
            return False
        if not self._ensure_session():
            logger.debug("[roku] prewarm seed skipped — could not obtain session")
            return False

        session_id = self.session.cookies.get("_usn", "roku-scraper")
        attempts = 0
        for ch in channels:
            sid = ch.source_channel_id
            if enabled_ids is not None and sid not in enabled_ids:
                continue
            play_id = self._cached_play_id(sid)
            selector_url = self._cached_selector_url(sid)
            if not play_id or not selector_url:
                continue
            try:
                decoded = base64.b64decode(play_id.split(".", 1)[1]).decode()
                media_format = "mpeg-dash" if "dash" in decoded.lower() else "m3u"
            except Exception:
                media_format = "m3u"
            if media_format != "m3u":
                continue
            body = {
                "rokuId":      sid,
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
            r = self._api_post(_PLAYBACK, json_body=body, timeout=10,
                               label=f"prewarm seed for {sid}", set_cooldown_on_403=False)
            if r and r.status_code == 200:
                stream_url = r.json().get("url", "")
                if stream_url:
                    self._cache_stream_url(sid, stream_url)
                    logger.info("[roku] prewarm seed: seeded osm_session from %s", sid)
                    return True
            # This channel failed (404, 401, 403 DRM, etc.) — try the next one.
            attempts += 1
            if attempts >= _PREWARM_SEED_MAX_ATTEMPTS:
                break
        logger.debug("[roku] prewarm seed: could not seed osm_session in %d attempt(s)", attempts)
        return False

    def _validate_stream_url(self, stream_url: str) -> bool:
        try:
            response = self.session.get(stream_url, timeout=8, stream=True)
            if response.status_code != 200:
                logger.debug("[roku] validate_stream_url %s → %d", stream_url, response.status_code)
                response.close()
                return False
            chunk = next(response.iter_content(chunk_size=4096), b"")
            response.close()
            return b"#EXTM3U" in chunk
        except Exception:
            return False

    def _clear_cached_session(self) -> None:
        self._csrf_token = None
        self._session_born = None
        self.session.cookies.clear()
        self._update_config("csrf_token", None)
        self._update_config("session_born", None)
        self._update_config("session_cookies", {})

    @staticmethod
    def _live_tv_headers() -> dict:
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Cache-Control": "max-age=0",
            "Pragma": "no-cache",
            "Referer": _HOME,
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }

    def _refresh_session(self) -> bool:
        """Boot a fresh Roku browser session. Returns True on success."""
        if self._cooldown_active():
            logger.warning(
                "[roku] bootstrap cooldown active for %ds (%s)",
                self._cooldown_remaining(),
                self._cooldown_reason or "Roku returned 403",
            )
            return False
        try:
            self._clear_cached_session()
            # Step 1: hit home page to collect cookies. /live-tv is intermittently
            # blocked by CloudFront, but the root page yields the same anonymous
            # cookies and works for csrf + API bootstrap.
            r1 = None
            for attempt in range(_LIVE_TV_403_RETRIES + 1):
                r1 = self.session.get(_HOME, headers=self._live_tv_headers(), timeout=15)
                if r1.status_code == 200:
                    self._clear_403_cooldown()
                    break
                if r1.status_code == 403:
                    self._log_bootstrap_403(r1)
                    # CloudFront 403s are often edge-specific; retry on a fresh
                    # connection (drop keep-alive so DNS/edge re-resolves) before
                    # giving up and arming the cooldown.
                    if attempt < _LIVE_TV_403_RETRIES:
                        self.session.close()
                        time.sleep(2 ** attempt)
                        continue
                    self._set_403_cooldown("bootstrap")
                logger.error("[roku] home bootstrap returned %d", r1.status_code)
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
            self._persist_session()
            logger.debug("[roku] session refreshed, csrf=%s…", csrf[:12])
            return True

        except Exception as exc:
            if is_transient_network_error(exc):
                raise
            logger.error("[roku] session refresh failed: %s", exc)
            return False

    def _ensure_session(self) -> bool:
        if not self._session_is_fresh():
            return self._refresh_session()
        if self._session_born and (time.time() - self._session_born) >= _SESSION_TTL:
            logger.debug("[roku] reusing cached session older than soft TTL; will refresh only if Roku rejects it")
        return True

    def _api_get(self, url: str, *, timeout: int, label: str) -> Optional[object]:
        for attempt in range(2):
            headers = self._api_headers()
            response = self.session.get(url, headers=headers, timeout=timeout)
            if response.status_code == 403:
                self._set_403_cooldown(label)
                return response
            if response.status_code not in (401, 403) or attempt == 1:
                return response
            logger.warning("[roku] %s returned %d, refreshing session and retrying once",
                           label, response.status_code)
            if not self._refresh_session():
                return response
        return None

    def _api_post(self, url: str, *, json_body: dict, timeout: int, label: str,
                  set_cooldown_on_403: bool = True):
        for attempt in range(2):
            headers = self._api_headers()
            response = self.session.post(url, headers=headers, json=json_body, timeout=timeout)
            if response.status_code == 403:
                # A per-channel 403 (e.g. a DRM-locked channel) is not a reliable
                # site-wide rate-limit signal; best-effort callers opt out of the cooldown.
                if set_cooldown_on_403:
                    self._set_403_cooldown(label)
                return response
            if response.status_code not in (401, 403) or attempt == 1:
                return response
            logger.warning("[roku] %s returned %d, refreshing session and retrying once",
                           label, response.status_code)
            if not self._refresh_session():
                return response
        return None

    @staticmethod
    def _log_bootstrap_403(response) -> None:
        body = ""
        try:
            body = (response.text or "").strip().replace("\n", " ").replace("\r", " ")
        except Exception:
            body = ""
        if len(body) > 160:
            body = body[:160] + "..."
        logger.warning(
            "[roku] bootstrap 403 details: cf_pop=%s x_cache=%s server=%s content_type=%s body=%r",
            response.headers.get("x-amz-cf-pop"),
            response.headers.get("x-cache"),
            response.headers.get("server"),
            response.headers.get("content-type"),
            body,
        )

    def _api_headers(self) -> dict:
        return {
            "csrf-token":                         self._csrf_token or "",
            "origin":                             _BASE,
            "referer":                            _HOME,
            "content-type":                       "application/json",
            "x-roku-reserved-amoeba-ids":         "",
            "x-roku-reserved-experiment-configs": "e30=",
            "x-roku-reserved-experiment-state":   "W10=",
            "x-roku-reserved-lat":                "0",
        }

    # ── Content proxy helper ───────────────────────────────────────────────────

    def _fetch_content(self, station_id: str, feature_include: str = "", _raise_on_404: bool = False) -> Optional[dict]:
        """Call the therokuchannel content proxy for a given station_id."""
        qs = f"?featureInclude={feature_include}" if feature_include else ""
        content_url = _CONTENT_TPL.format(sid=station_id) + qs
        proxy_url   = _PROXY_BASE + quote(content_url, safe="")
        try:
            r = self._api_get(proxy_url, timeout=10, label=f"content proxy for {station_id}")
            if r.status_code == 200:
                return r.json()
            if _raise_on_404 and r.status_code == 404:
                raise StreamDeadError(format_http_reason("[roku] channel not found", 404, station_id))
        except StreamDeadError:
            raise
        except Exception as exc:
            logger.warning("[roku] content fetch error for %s: %s", station_id, exc)
        return None

    # ── fetch_channels ─────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        if self._cooldown_active():
            remaining = self._cooldown_remaining()
            mins = max(1, (remaining + 59) // 60)
            raise ScrapeSkipError(
                f"Roku rate-limited (403) — cooldown active, ~{mins} min remaining. Previous channel data kept."
            )
        if not self._ensure_session():
            raise ScrapeSkipError(
                "[roku] could not establish a session (Roku returned 403 or no data) — "
                "often a regional/CDN block. Previous channel data kept; try a US VPN if this persists."
            )

        def _fetch_channels_once() -> tuple[list[ChannelData], dict[str, int | str | None]]:
            channels: list[ChannelData] = []
            seen: set[str] = set()
            epg_status = None
            epg_collections = 0
            epg_station_rows = 0
            billboard_status = None
            billboard_items = 0

            # ── Phase 1: /api/v2/epg — returns all ~795 live channels ────────
            # Each collection item has features.station with full channel metadata.
            try:
                r = self._api_get(_EPG_URL, timeout=20, label="epg")
                epg_status = getattr(r, 'status_code', None)
                if r and r.status_code == 200:
                    payload = r.json()
                    collections = payload.get("collections", [])
                    epg_collections = len(collections)
                    self._last_epg_ok = time.time()
                    for col in collections:
                        station = col.get("features", {}).get("station")
                        if not station:
                            continue
                        epg_station_rows += 1
                        sid = station.get("meta", {}).get("id")
                        if not sid or sid in seen:
                            continue
                        self._add_channel_from_station(channels, seen, sid, station)
                elif r is not None:
                    if r.status_code == 403:
                        self._clear_cached_session()
                    logger.warning("[roku] EPG returned %d", r.status_code)
            except Exception as exc:
                logger.warning("[roku] EPG fetch failed: %s", exc)

            # ── Phase 2: billboard (hero channels, fills any EPG gaps) ───────
            try:
                r2 = self.session.get(
                    f"{_BASE}/api/v1/billboard/landing/trc-us-live-ml-page-en-current",
                    headers=self._api_headers(),
                    timeout=10,
                )
                billboard_status = r2.status_code
                if r2.status_code == 200:
                    items = r2.json()
                    billboard_items = len(items)
                    for item in items:
                        sid = (item.get("meta") or {}).get("id")
                        if not sid or sid in seen:
                            continue
                        self._add_channel_from_content(channels, seen, sid, item)
            except Exception as exc:
                logger.warning("[roku] billboard fetch failed: %s", exc)

            return channels, {
                'epg_status': epg_status,
                'epg_collections': epg_collections,
                'epg_station_rows': epg_station_rows,
                'billboard_status': billboard_status,
                'billboard_items': billboard_items,
            }

        channels, stats = _fetch_channels_once()
        if not channels:
            logger.warning(
                "[roku] empty channel payload; refreshing session and retrying once "
                "(epg_status=%s epg_collections=%s epg_station_rows=%s billboard_status=%s billboard_items=%s)",
                stats['epg_status'],
                stats['epg_collections'],
                stats['epg_station_rows'],
                stats['billboard_status'],
                stats['billboard_items'],
            )
            if self._refresh_session():
                channels, stats = _fetch_channels_once()

        if not channels:
            logger.error(
                "[roku] fetch_channels returned 0 channels after retry "
                "(epg_status=%s epg_collections=%s epg_station_rows=%s billboard_status=%s billboard_items=%s)",
                stats['epg_status'],
                stats['epg_collections'],
                stats['epg_station_rows'],
                stats['billboard_status'],
                stats['billboard_items'],
            )
            raise ScrapeSkipError("[roku] channel fetch returned 0 channels; keeping previous channel data")

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

        view_opts = station.get("viewOptions") or []
        if view_opts:
            self._cache_play_id(station_id, view_opts[0].get("playId") if view_opts else None)
            self._cache_selector_url(station_id, self._extract_selector_url(view_opts))

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
        tags_str = ' '.join(station.get('tags') or [])

        channels.append(ChannelData(
            source_channel_id = station_id,
            name              = title,
            stream_url        = f"roku://{station_id}",
            logo_url          = logo,
            category          = category,
            language          = _language_from_metadata(title, category, tags_str),
            country           = "US",
            stream_type       = "hls",
            number            = number,
            slug              = (f"|{_gn}" if (_gn := resolve_gracenote('roku', lookup_key=station_id) or '') else None),
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
        selector_url = self._extract_selector_url(view_opts)
        self._cache_play_id(station_id, play_id)
        self._cache_selector_url(station_id, selector_url)

        # Gracenote station ID — prefer upstream field, fall back to CSV map
        gracenote_id = item.get("gracenoteStationId") or item.get("stationId") or ""
        if gracenote_id and not str(gracenote_id).isdigit():
            gracenote_id = ""
        if not gracenote_id:
            gracenote_id = resolve_gracenote("roku", lookup_key=station_id) or ""

        channels.append(ChannelData(
            source_channel_id = station_id,
            name              = title,
            stream_url        = f"roku://{station_id}",
            logo_url          = logo,
            category          = category,
            language          = _language_from_metadata(title, category),
            country           = "US",
            stream_type       = "hls",
            slug              = (f"{play_id or ''}|{gracenote_id}" if gracenote_id else None),
        ))

    # ── fetch_epg ──────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], skip_ids: set[str] | None = None, **kwargs) -> list[ProgramData]:
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed

        enabled_ids = kwargs.get("enabled_ids")

        # Load the large description_cache now (deferred from __init__ so the play/resolve
        # hot path never pays for it); the EPG fanout below reuses cached descriptions.
        if not self._description_cache:
            self._load_description_cache()

        if self._cooldown_active():
            remaining = self._cooldown_remaining()
            mins = max(1, (remaining + 59) // 60)
            raise ScrapeSkipError(
                f"Roku rate-limited (403) — cooldown active, ~{mins} min remaining. Previous EPG data kept."
            )
        if not self._ensure_session():
            raise ScrapeSkipError("[roku] session bootstrap failed before EPG fetch; keeping previous EPG data")

        # Validate the cached session against a real Roku API before starting
        # the threaded content-proxy fanout. Otherwise an upstream-expired
        # session can yield a misleading "0 programs" success on EPG-only runs.
        # Skip probe if fetch_channels() already confirmed the session within 60s.
        # When the probe succeeds, mine its response to batch-warm play_id/selector
        # caches and identify channels that don't carry schedule data.
        no_schedule_ids: set[str] = set()
        if (time.time() - self._last_epg_ok) > 60:
            epg_probe = self._api_get(_EPG_URL, timeout=20, label="epg")
            if not epg_probe or epg_probe.status_code != 200:
                if epg_probe and epg_probe.status_code == 403:
                    self._clear_cached_session()
                logger.warning("[roku] EPG validation returned %s before threaded fetch",
                               getattr(epg_probe, "status_code", "no response"))
                raise ScrapeSkipError("[roku] session rejected before EPG fetch; keeping previous EPG data")
            self._last_epg_ok = time.time()
            now = time.time()
            warmed_from_epg = 0
            for col in epg_probe.json().get("collections", []):
                station = col.get("features", {}).get("station") or {}
                sid = (station.get("meta") or {}).get("id")
                if not sid:
                    continue
                if not station.get("shouldRequestSchedule", True):
                    no_schedule_ids.add(sid)
                view_opts = station.get("viewOptions") or []
                if view_opts and not self._cached_play_id(sid):
                    play_id = view_opts[0].get("playId") if view_opts else None
                    selector_url = self._extract_selector_url(view_opts)
                    if play_id:
                        self._play_id_cache[sid] = {"play_id": play_id, "cached_at": now}
                        warmed_from_epg += 1
                    if selector_url:
                        self._selector_url_cache[sid] = {"selector_url": selector_url, "cached_at": now}
            if warmed_from_epg:
                self._persist_play_id_cache()
                self._persist_selector_url_cache()
                logger.debug("[roku] warmed %d play_id/selector entries from EPG probe", warmed_from_epg)
            if no_schedule_ids:
                logger.debug("[roku] %d stations have shouldRequestSchedule=False, skipping content proxy for those", len(no_schedule_ids))

        # Disabled channels are never served in M3U/EPG output and never tuned,
        # so they need no EPG fetch and no metadata warm — skip them entirely.
        # (enabled_ids is None only when fetch_epg is called without the worker's
        # context, e.g. a manual run; then fall back to processing all channels.)
        if enabled_ids is not None:
            disabled_skip = {ch.source_channel_id for ch in channels
                             if ch.source_channel_id not in enabled_ids}
        else:
            disabled_skip = set()
        effective_skip = (skip_ids or set()) | no_schedule_ids | disabled_skip
        channel_ids = {ch.source_channel_id for ch in channels}
        skipped = len(effective_skip & channel_ids) if effective_skip else 0
        total = len(channels) - skipped  # only count channels that will hit the network
        if skipped:
            logger.info("[roku] EPG skip: %d/%d channels (fresh, no-schedule, or disabled), skipping content proxy", skipped, len(channels))
        if self._progress_cb:
            self._progress_cb('epg', 0, total)
        # Snapshot merged headers (session defaults + API-specific) and cookies
        # so each worker thread can reuse its own independent session without
        # mutating the shared scraper session or opening a fresh pool per task.
        headers_snapshot = {**self.session.headers, **self._api_headers()}
        cookies_snapshot  = self.session.cookies.get_dict()

        programs: list[ProgramData] = []
        # Map content_id → programs within 48h that need a description backfill
        cid_to_progs: dict[str, list[ProgramData]] = {}
        lock = threading.Lock()
        thread_local = threading.local()
        done = [0]

        cutoff_48h = datetime.now(timezone.utc) + timedelta(hours=48)

        def fetch_one(ch: ChannelData) -> tuple[list[ProgramData], dict, str | None, str | None]:
            sess = getattr(thread_local, "session", None)
            if sess is None:
                sess = self.new_session(headers=headers_snapshot, cookies=cookies_snapshot)
                thread_local.session = sess
            sess.cookies.update(cookies_snapshot)
            sid = ch.source_channel_id
            if sid in effective_skip:
                return [], {}, self._cached_play_id(sid), self._cached_selector_url(sid)
            try:
                qs = "?featureInclude=linearSchedule"
                content_url = _CONTENT_TPL.format(sid=sid) + qs
                proxy_url   = _PROXY_BASE + quote(content_url, safe="")
                r = sess.get(proxy_url, timeout=10)
                if r.status_code != 200:
                    logger.debug("[roku] content proxy returned %d for %s", r.status_code, sid)
                    return [], {}, None, None
                data = r.json()
                view_opts = data.get("viewOptions") or [{}]
                play_id = view_opts[0].get("playId") if view_opts else None
                selector_url = self._extract_selector_url(view_opts)
                schedule = data.get("features", {}).get("linearSchedule", [])
                result = []
                local_cid_map: dict[str, list[ProgramData]] = {}
                for entry in schedule:
                    prog = self._parse_program(sid, entry)
                    if not prog:
                        continue
                    result.append(prog)
                    # Track content_id for programs in the 48h window so we
                    # can backfill descriptions in a second pass.
                    if prog.start_time <= cutoff_48h:
                        cid = (entry.get("content") or {}).get("meta", {}).get("id")
                        if cid:
                            local_cid_map.setdefault(cid, []).append(prog)
                return result, local_cid_map, play_id, selector_url
            except Exception as exc:
                if is_transient_network_error(exc):
                    raise
                logger.warning("[roku] EPG error for %s (%s): %s", ch.name, sid, exc)
                return [], {}, None, None

        with ThreadPoolExecutor(max_workers=_EPG_WORKERS) as executor:
            futures = {executor.submit(fetch_one, ch): ch for ch in channels}
            for future in as_completed(futures):
                exc = future.exception()
                if exc and type(exc).__name__ == 'JobTimeoutException':
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise exc
                if exc and is_transient_network_error(exc):
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise exc
                result, local_cid_map, play_id, selector_url = future.result() if not exc else ([], {}, None, None)
                sid = futures[future].source_channel_id
                with lock:
                    programs.extend(result)
                    for cid, progs in local_cid_map.items():
                        cid_to_progs.setdefault(cid, []).extend(progs)
                    self._cache_play_id(sid, play_id)
                    self._cache_selector_url(sid, selector_url)
                    if sid not in effective_skip:
                        done[0] += 1
                        if self._progress_cb:
                            self._progress_cb('epg', done[0], total)

        missing_channels = [
            ch for ch in channels
            if ch.source_channel_id not in disabled_skip
            and (not self._cached_play_id(ch.source_channel_id) or not self._cached_selector_url(ch.source_channel_id))
        ]
        retried_play = 0
        retried_selector = 0
        if missing_channels:
            retried_play, retried_selector = self._warm_missing_metadata(
                missing_channels,
                headers_snapshot,
                cookies_snapshot,
            )

        # ── Description backfill for 48h window ───────────────────────────────
        if cid_to_progs:
            uncached_cids = [cid for cid in cid_to_progs if cid not in self._description_cache]
            if uncached_cids:
                new_descs = self._fetch_descriptions(uncached_cids, headers_snapshot, cookies_snapshot,
                                                     progress_cb=self._progress_cb)
                self._cache_descriptions(new_descs)
            filled = 0
            for cid, progs in cid_to_progs.items():
                desc = self._description_cache.get(cid)
                if desc:
                    for prog in progs:
                        if not prog.description:
                            prog.description = desc
                            filled += 1
            logger.debug(
                "[roku] description backfill: %d unique IDs (%d cached, %d fetched) → %d programs filled",
                len(cid_to_progs),
                len(cid_to_progs) - len(uncached_cids),
                len(uncached_cids),
                filled,
            )

        programs.sort(key=lambda p: (p.source_channel_id, p.start_time))
        play_cached = selector_cached = stream_cached = 0
        for ch in channels:
            sid = ch.source_channel_id
            if self._cached_play_id(sid):
                play_cached += 1
            if self._cached_selector_url(sid):
                selector_cached += 1
            if self._cached_stream_url(sid):
                stream_cached += 1
        if not self._cached_osm_session():
            self._seed_osm_session(channels, enabled_ids=enabled_ids)
        n_channels = len(channels)
        logger.debug(
            "[roku] cache warm summary: play_id=%d/%d selector=%d/%d stream_url=%d/%d retry_play=%d retry_selector=%d",
            play_cached,
            n_channels,
            selector_cached,
            n_channels,
            stream_cached,
            total,
            retried_play,
            retried_selector,
        )
        logger.debug("[roku] %d EPG entries fetched for %d channels", len(programs), n_channels)
        return programs

    def _fetch_descriptions(
        self,
        content_ids: list[str],
        headers_snapshot: dict,
        cookies_snapshot: dict,
        progress_cb=None,
    ) -> dict[str, str]:
        """Fetch program descriptions in parallel via the content proxy."""
        import requests as _req
        from concurrent.futures import ThreadPoolExecutor

        desc_map: dict[str, str] = {}
        lock = __import__('threading').Lock()
        total = len(content_ids)

        def fetch_desc(cid: str):
            sess = _req.Session()
            sess.headers.update(headers_snapshot)
            sess.cookies.update(cookies_snapshot)
            prog_url  = f"https://content.sr.roku.com/content/v1/roku-trc/{cid}"
            proxy_url = _PROXY_BASE + quote(prog_url, safe="")
            try:
                r = sess.get(proxy_url, timeout=10)
                if r.status_code == 200:
                    d = r.json()
                    descs = d.get("descriptions") or {}
                    desc = None
                    for key in ("250", "100", "60"):
                        entry = descs.get(key)
                        if entry:
                            desc = entry.get("text") if isinstance(entry, dict) else entry
                            break
                    if not desc:
                        desc = d.get("description")
                    if desc:
                        return cid, str(desc)
            except Exception:
                pass
            return cid, None

        done = 0
        with ThreadPoolExecutor(max_workers=_DESC_WORKERS) as executor:
            for cid, desc in executor.map(fetch_desc, content_ids):
                if desc:
                    with lock:
                        desc_map[cid] = desc
                done += 1
                if progress_cb:
                    progress_cb('desc', done, total)

        return desc_map

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
        category = _join_categories(genres)

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
            program_type      = "episode" if series_title else None,
        )

    # ── resolve ────────────────────────────────────────────────────────────────

    def _resolve_stream_url(self, raw_url: str, *, allow_cached_stream_url: bool) -> str:
        """Resolve a Roku channel URL to a fresh stream URL.

        Normal playback may reuse a recently cached OSM URL, but audit-time
        checks should force a fresh playback lookup so we do not classify
        channels as dead because an older cached URL expired.
        """
        if not raw_url.startswith("roku://"):
            return raw_url

        station_id = raw_url[len("roku://"):]
        had_play_id = False
        had_selector_url = False
        need_content_details = False
        failure_stage = "cache"

        try:
            cached_stream_url = self._cached_stream_url(station_id) if allow_cached_stream_url else None
            if cached_stream_url:
                logger.info("[roku] resolve %s via stream_url cache", station_id)
                return cached_stream_url

            if self._cooldown_active():
                raise RuntimeError(
                    f"[roku] resolve blocked by temporary 403 cooldown for {self._cooldown_remaining()}s"
                )

            failure_stage = "bootstrap"
            if not self._ensure_session():
                raise RuntimeError(f"[roku] resolve failed — could not obtain session for {station_id}")

            # Step 1: prefer cached playId to avoid content lookups on tune.
            failure_stage = "content"
            content_data = None
            play_id = self._cached_play_id(station_id)
            selector_url = self._cached_selector_url(station_id)
            had_play_id = bool(play_id)
            had_selector_url = bool(selector_url)
            need_content_details = not play_id
            if need_content_details:
                content_data = self._fetch_content(station_id, _raise_on_404=True)
                if content_data:
                    view_opts = content_data.get("viewOptions") or [{}]
                    play_id = view_opts[0].get("playId") if view_opts else None
                    self._cache_play_id(station_id, play_id)
                    selector_url = self._extract_selector_url(view_opts)
                    self._cache_selector_url(station_id, selector_url)

            if not play_id and content_data is not None:
                # Try regex fallback — only when content API returned data but no playId.
                # Skip if content_data is None (404/403): making a second request risks
                # triggering another 403 cooldown for a channel that won't resolve anyway.
                content_url = _CONTENT_TPL.format(sid=station_id)
                proxy_url   = _PROXY_BASE + quote(content_url, safe="")
                try:
                    r = self._api_get(proxy_url, timeout=10, label=f"content fallback for {station_id}")
                    pids = re.findall(r's-[a-z0-9_]+\.[A-Za-z0-9+/=]+', r.text)
                    play_id = pids[0] if pids else None
                    self._cache_play_id(station_id, play_id)
                except Exception:
                    pass

            if not play_id:
                logger.warning("[roku] no playId found for %s", station_id)
                raise RuntimeError(f"[roku] no playId found for {station_id}")

            # Decode to determine media format
            try:
                decoded = base64.b64decode(play_id.split(".", 1)[1]).decode()
                media_format = "mpeg-dash" if "dash" in decoded.lower() else "m3u"
            except Exception:
                media_format = "m3u"

            # Step 2: call /api/v3/playback
            failure_stage = "playback"
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
            r2 = self._api_post(_PLAYBACK, json_body=body, timeout=10, label=f"playback for {station_id}")
            if r2.status_code == 200:
                stream_url = r2.json().get("url", "")
                if stream_url:
                    self._persist_session()
                    self._cache_play_id(station_id, play_id)
                    self._cache_selector_url(station_id, selector_url)
                    self._cache_stream_url(station_id, stream_url)
                    logger.info(
                        "[roku] resolve %s via playback_api play_id_cache=%s selector_cache=%s content_lookup=%s",
                        station_id,
                        had_play_id,
                        had_selector_url,
                        need_content_details,
                    )
                    return stream_url
            if r2.status_code in (401, 403, 404, 502):
                self._invalidate_play_id(station_id)
                self._invalidate_selector_url(station_id)
                self._invalidate_stream_url(station_id)
            raise RuntimeError(f"[roku] playback returned {r2.status_code} for {station_id}")
        except StreamDeadError:
            logger.warning(
                "[roku] resolve %s failed stage=%s play_id_cache=%s selector_cache=%s content_lookup=%s",
                station_id,
                failure_stage,
                had_play_id,
                had_selector_url,
                need_content_details,
            )
            raise
        except RuntimeError:
            logger.warning(
                "[roku] resolve %s failed stage=%s play_id_cache=%s selector_cache=%s content_lookup=%s",
                station_id,
                failure_stage,
                had_play_id,
                had_selector_url,
                need_content_details,
            )
            raise
        except Exception as exc:
            self._invalidate_stream_url(station_id)
            logger.warning(
                "[roku] resolve %s failed stage=%s play_id_cache=%s selector_cache=%s content_lookup=%s",
                station_id,
                failure_stage,
                had_play_id,
                had_selector_url,
                need_content_details,
            )
            raise RuntimeError(f"[roku] playback request failed for {station_id}: {exc}") from exc

    def audit_resolve(self, raw_url: str) -> str:
        """Force a fresh Roku playback lookup during stream audits.

        Roku stream URLs are cached for playback convenience, but audit runs
        need to bypass that cache so an expired OSM URL does not look like a
        dead channel.
        """
        return self._resolve_stream_url(raw_url, allow_cached_stream_url=False)

    def resolve(self, raw_url: str) -> str:
        """
        raw_url format: roku://{station_id}
        Returns a live osm.sr.roku.com HLS/DASH stream URL.
        Calls /api/v3/playback with a fresh session each time.
        The JWT in the stream URL is short-lived so we always fetch fresh.
        """
        return self._resolve_stream_url(raw_url, allow_cached_stream_url=True)

    # ── DASH + Widevine (browser/EME → PrismCast bridge) ────────────────────────
    # The HLS variant Roku serves can carry FairPlay (Apple-only; Chrome can't
    # decrypt it), which is what trips the DRM auto-disable. The SAME channel has a
    # CENC DASH variant with Widevine that Chrome CAN decrypt — we just have to ask
    # for mediaFormat='mpeg-dash'. resolve_dash() returns the MPD URL and stashes the
    # per-session Widevine license URL so get_license_url() can hand it to the license
    # proxy. Kept entirely separate from the HLS stream_url cache (whose OSM-session
    # extraction parses HLS URL shapes).

    def _load_dash_cache(self) -> None:
        raw = self.cache.get("dash_cache") or {}
        if not isinstance(raw, dict):
            return
        now = time.time()
        for sid, entry in raw.items():
            if not isinstance(sid, str) or not isinstance(entry, dict):
                continue
            cached_at = entry.get("cached_at")
            if not entry.get("mpd_url") or not isinstance(cached_at, (int, float)):
                continue
            if (now - float(cached_at)) >= _DASH_TTL:
                continue
            self._dash_cache[sid] = {
                "mpd_url": entry.get("mpd_url"),
                "license_url": entry.get("license_url"),
                "cached_at": float(cached_at),
            }

    def _cache_dash(self, sid: str, mpd_url: str, license_url: str | None) -> None:
        if not sid or not mpd_url:
            return
        self._dash_cache[sid] = {
            "mpd_url": mpd_url,
            "license_url": license_url,
            "cached_at": time.time(),
        }
        self._update_cache("dash_cache", self._dash_cache)

    def _cached_dash(self, sid: str) -> dict | None:
        entry = self._dash_cache.get(sid)
        if not entry:
            return None
        cached_at = entry.get("cached_at")
        if not entry.get("mpd_url") or not isinstance(cached_at, (int, float)):
            return None
        if (time.time() - float(cached_at)) >= _DASH_TTL:
            self._dash_cache.pop(sid, None)
            self._update_cache("dash_cache", self._dash_cache)
            return None
        return entry

    def resolve_dash(self, raw_url: str, *, allow_cached: bool = True) -> dict:
        """Resolve a Roku channel to its CENC DASH variant for browser EME playback.

        Returns {'mpd_url': str, 'license_url': str|None}. The license URL carries a
        per-session token bound to the SAME playback call as the manifest, so both are
        cached together and expire together.
        """
        if not raw_url.startswith("roku://"):
            return {"mpd_url": raw_url, "license_url": None}
        station_id = raw_url[len("roku://"):]

        if allow_cached:
            cached = self._cached_dash(station_id)
            if cached:
                logger.info("[roku] resolve_dash %s via dash cache", station_id)
                return {"mpd_url": cached["mpd_url"], "license_url": cached.get("license_url")}

        if self._cooldown_active():
            raise RuntimeError(
                f"[roku] resolve_dash blocked by 403 cooldown for {self._cooldown_remaining()}s"
            )
        if not self._ensure_session():
            raise RuntimeError(f"[roku] resolve_dash — could not obtain session for {station_id}")

        play_id = self._cached_play_id(station_id)
        if not play_id:
            content_data = self._fetch_content(station_id, _raise_on_404=True)
            if content_data:
                view_opts = content_data.get("viewOptions") or [{}]
                play_id = view_opts[0].get("playId") if view_opts else None
                self._cache_play_id(station_id, play_id)
        if not play_id:
            raise RuntimeError(f"[roku] resolve_dash — no playId for {station_id}")

        session_id = self.session.cookies.get("_usn", "roku-scraper")
        body = {
            "rokuId":      station_id,
            "playId":      play_id,
            "mediaFormat": "mpeg-dash",   # force DASH even when the play_id hints m3u
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
        r = self._api_post(_PLAYBACK, json_body=body, timeout=12, label=f"dash playback for {station_id}")
        if r.status_code != 200:
            if r.status_code in (401, 403, 404, 502):
                self._invalidate_play_id(station_id)
            raise RuntimeError(f"[roku] resolve_dash playback returned {r.status_code} for {station_id}")
        data = r.json()
        mpd_url = data.get("url", "")
        license_url = (((data.get("drm") or {}).get("widevine") or {}).get("licenseServer")) or None
        if not mpd_url:
            raise RuntimeError(f"[roku] resolve_dash — no DASH url for {station_id}")
        self._persist_session()
        self._cache_dash(station_id, mpd_url, license_url)
        logger.info("[roku] resolve_dash %s via playback_api (license=%s)",
                    station_id, "yes" if license_url else "no")
        return {"mpd_url": mpd_url, "license_url": license_url}

    @classmethod
    def license_request_headers(cls, config: dict) -> dict:
        # Roku's Widevine server authenticates via the per-session token in the URL
        # query string; it just needs a browser-shaped Origin/UA. The raw EME challenge
        # is forwarded as the POST body (default prepare_license_request) and the raw
        # license is returned verbatim (default process_license_response).
        return {
            "Origin":  _BASE,
            "Referer": f"{_BASE}/",
            "User-Agent": _UA,
        }

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        """Per-session Widevine license URL captured by resolve_dash(), keyed by station.

        With no channel_id this returns the bare host as a truthy "DRM-capable" signal.
        With a channel_id it returns the cached per-session URL (token included), or None
        when no DASH session has been resolved yet — never the tokenless base, so we don't
        issue an unauthorized request to Roku's license server."""
        if channel_id:
            entry = (config.get("dash_cache") or {}).get(channel_id)
            if isinstance(entry, dict) and entry.get("license_url"):
                return entry["license_url"]
            return None
        return cls.license_url

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

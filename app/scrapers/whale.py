"""
Whale TV+ scraper for FastChannels.
Uses the rlaxx/zeasn.tv API — no user credentials required.
"""
from __future__ import annotations

import logging
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from requests.adapters import HTTPAdapter

from .base import BaseScraper, ChannelData, ProgramData
from .category_utils import category_for_channel

logger = logging.getLogger(__name__)

_API_TOKEN      = "4ef13b5f3d2744e3b0a569feb8dde298"
_AUTH_URL       = "https://rlaxx.zeasn.tv/livetv/api/v1/auth/access"
_CHANNELS_URL   = "https://rlaxx.zeasn.tv/livetv/api/device/browser/v1/category/channels"
_EPG_URL        = "https://rlaxx.zeasn.tv/livetv/api/device/browser/v1/epg"
_EPG_DETAIL_URL = "https://rlaxx.zeasn.tv/livetv/api/device/browser/v1/epg/detail"
_LOGO_BASE      = "https://d3b6luslimvglo.cloudfront.net/images/79/rlaxximages/channels-rescaled/icon-white"

_CHANNEL_SCHEME = "whale://"
_TOKEN_TTL      = 82800   # 23 hr (API gives ~24 hr)
_URL_CACHE_TTL  = 7200    # 2 hr between resolve() cache refreshes
_EPG_DAYS           = 7
_EPG_BATCH_SIZE     = 10   # channels per EPG request (matches website's batched approach)
_EPG_DETAIL_HOURS   = 24   # enrich programs starting within this many hours
_EPG_DETAIL_WORKERS = 15   # concurrent detail requests

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/148.0.0.0 Safari/537.36"
    ),
    "Origin":  "https://watch.whaletvplus.com",
    "Referer": "https://watch.whaletvplus.com/",
    "Accept":  "application/json, text/plain, */*",
}

# Translates rlaxx API category bucket names to approximate canonical categories.
# category_for_channel() then applies name-level overrides on top of this.
_CATEGORY_MAP = {
    "Movies and Series": "Movies",
    "Documentary":       "Documentary",
    "Lifestyle":         "Lifestyle",
    "Music":             "Music",
    "Travel":            "Travel",
    "DIY":               "Home & DIY",
    "Food and Drink":    "Food",
    "Sports":            "Sports",
    "Motorsports":       "Sports",
    "News":              "News",
    "Slow TV":           "Ambiance",
}

# Categories that are effectively "all channels" buckets — skip for grouping
_SKIP_CATEGORIES = frozenset({"All", "Featured all other countries"})

# Module-level auth token cache (shared across instances within the process)
_token_lock   = threading.Lock()
_token_value: str | None = None
_token_expiry: float = 0.0

# Module-level chlId → (url, fetched_at) cache for resolve()
_url_cache: dict[str, tuple[str, float]] = {}
_url_cache_lock = threading.Lock()

# prgchId → description, populated from currentProgram during fetch_channels()
_current_prog_desc: dict[str, str] = {}
_current_prog_desc_lock = threading.Lock()

# Stable per-process device ID for ad targeting params
_DEVICE_ID = str(uuid.uuid4())

# Matches [placeholder] and [%placeholder%] ad-targeting macros in stream URLs
_MACRO_RE = re.compile(r'\[%?[^\]]+%?\]')


def _fill_url_macros(url: str) -> str:
    """
    Fill ad-targeting template macros in Ottera/SSAI stream URLs.
    Required params (did, cachebuster, session_id) must be present or the
    Ottera loggingmediaurlpassthrough endpoint returns 500.
    """
    filled = (url
              .replace('[did]',              _DEVICE_ID)
              .replace('[session_id]',       str(uuid.uuid4()))
              .replace('[cachebuster]',      str(int(time.time() * 1000)))
              .replace('[dnt]',              '0')
              .replace('[lmt]',              '0')
              .replace('[consent]',          '')
              .replace('[content_id]',       '')
              .replace('[content_language]', 'en')
              .replace('[content_duration]', '')
              .replace('[content_season]',   '')
              .replace('[content_episode]',  ''))
    return _MACRO_RE.sub('', filled)


class WhaleScraper(BaseScraper):
    source_name          = "whale"
    display_name         = "Whale TV+"
    scrape_interval      = 720
    stream_audit_enabled = True
    epg_quality          = 'partial'  # near-term (24h) has descriptions, posters, episode metadata; outer days titles+times only
    config_schema        = []

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update(_HEADERS)
        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=_EPG_DETAIL_WORKERS)
        self.session.mount("https://", adapter)

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _get_token(self) -> str | None:
        global _token_value, _token_expiry
        with _token_lock:
            if _token_value and time.time() < _token_expiry:
                return _token_value
            try:
                r = self.session.get(
                    _AUTH_URL,
                    params={"uuid": "1", "apiToken": _API_TOKEN, "langCode": "en"},
                    timeout=15,
                )
                r.raise_for_status()
                payload = r.json()
                token = (payload.get("data") or payload).get("token", "")
                if not token:
                    logger.error("[whale] auth returned no token: %s", payload)
                    return None
                _token_value  = token
                _token_expiry = time.time() + _TOKEN_TTL
                logger.debug("[whale] token refreshed")
                return _token_value
            except Exception as exc:
                logger.error("[whale] auth failed: %s", exc)
                return None

    # ── Raw channel list ──────────────────────────────────────────────────────

    def _fetch_raw_channels(self) -> list[dict]:
        token = self._get_token()
        if not token:
            return []
        try:
            r = self.session.get(
                _CHANNELS_URL,
                params={"langCode": "en", "countryCode": "US"},
                headers={"token": token},
                timeout=20,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            logger.error("[whale] channel fetch failed: %s", exc)
            return []

        seen: set[str] = set()
        channels: list[dict] = []
        for cat in (data.get("data") or []):
            cat_name = cat.get("ctgName", "")
            if cat_name in _SKIP_CATEGORIES:
                continue
            for ch in (cat.get("channels") or []):
                chl_id = ch.get("chlId", "")
                if not chl_id or chl_id in seen:
                    continue
                seen.add(chl_id)
                ch["_category"] = cat_name
                channels.append(ch)
        return channels

    # ── BaseScraper interface ─────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        raw = self._fetch_raw_channels()
        if not raw:
            return []

        now = time.time()
        channels: list[ChannelData] = []

        with _url_cache_lock:
            for ch in raw:
                chl_id  = ch.get("chlId", "")
                chl_url = ch.get("chlUrl", "")
                if chl_id and chl_url:
                    _url_cache[chl_id] = (chl_url, now)

        with _current_prog_desc_lock:
            for ch in raw:
                cp = ch.get("currentProgram") or {}
                prgch_id = str(cp.get("prgchId") or "")
                desc = (cp.get("prgDesc") or "").strip()
                if prgch_id and desc:
                    _current_prog_desc[prgch_id] = desc

        for ch in raw:
            chl_id   = ch.get("chlId", "")
            name     = (ch.get("chlName") or "").strip()
            chl_url  = ch.get("chlUrl", "")
            img_id   = ch.get("imageIdentifier", "")
            cat_name = ch.get("_category", "")
            desc     = (ch.get("description") or "").strip() or None
            lang     = (ch.get("chlLangCode") or "en").strip() or "en"
            # Normalize platform-specific subtags to plain ISO 639-1 (e.g. es-XL → es)
            if "-" in lang:
                lang = lang.split("-")[0]
            chl_num  = ch.get("chlNum", "")

            if not chl_id or not name or not chl_url:
                continue

            category = category_for_channel(name, _CATEGORY_MAP.get(cat_name))
            logo     = f"{_LOGO_BASE}/{img_id}_white.png" if img_id else None

            try:
                number = int(chl_num) if chl_num else None
            except (ValueError, TypeError):
                number = None

            channels.append(ChannelData(
                source_channel_id = chl_id,
                name              = name,
                stream_url        = f"{_CHANNEL_SCHEME}{chl_id}",
                logo_url          = logo,
                category          = category,
                language          = lang,
                country           = "US",
                description       = desc,
                number            = number,
            ))

        logger.info("[whale] %d channels", len(channels))
        return channels

    def _enrich_near_term(self, near_term: dict[str, ProgramData], token: str) -> None:
        """Call /epg/detail for near-term programs to add descriptions, posters, episode metadata."""
        if not near_term:
            return

        def _fetch_one(prgch_id: str) -> tuple[str, dict | None]:
            try:
                r = self.session.get(
                    f"{_EPG_DETAIL_URL}/{prgch_id}",
                    headers={"token": token},
                    timeout=15,
                )
                if r.status_code == 200:
                    return prgch_id, r.json().get("data") or {}
            except Exception:
                pass
            return prgch_id, None

        enriched = errors = 0
        with ThreadPoolExecutor(max_workers=_EPG_DETAIL_WORKERS) as pool:
            futures = {pool.submit(_fetch_one, pid): pid for pid in near_term}
            for fut in as_completed(futures):
                prgch_id, detail = fut.result()
                if detail is None:
                    errors += 1
                    continue
                if not detail:
                    continue

                prog = near_term[prgch_id]

                desc = (detail.get("prgDesc") or "").strip()
                if desc:
                    prog.description = desc

                images = detail.get("images") or []
                if images:
                    img_url = (images[0].get("pimgUrl") or "").strip()
                    if img_url:
                        prog.poster_url = img_url

                rating = (detail.get("prgRating") or "").strip()
                if rating:
                    prog.rating = rating

                if (detail.get("prgType") or "").upper() == "EPISODE":
                    prog.program_type = "episode"
                    series_title = (detail.get("seriesTitle") or "").strip()
                    ep_title     = (detail.get("prgTitle") or "").strip()
                    if series_title:
                        prog.title         = series_title
                        prog.episode_title = ep_title or None
                    try:
                        if detail.get("seasonNumber"):
                            prog.season = int(detail["seasonNumber"])
                        if detail.get("episodeNumber"):
                            prog.episode = int(detail["episodeNumber"])
                    except (ValueError, TypeError):
                        pass

                enriched += 1

        logger.info("[whale] detail enriched %d/%d near-term programs (%d errors)",
                    enriched, len(near_term), errors)

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        if not channels:
            return []

        token = self._get_token()
        if not token:
            return []

        chl_ids = [str(ch.source_channel_id) for ch in channels]
        now_utc  = datetime.now(timezone.utc)
        start_ms = int(now_utc.timestamp() * 1000)
        end_ms   = int((now_utc.timestamp() + _EPG_DAYS * 86400) * 1000)

        # Fetch in batches using comma-separated channelIds (matches the website's API usage)
        all_ch_rows: list[dict] = []
        for i in range(0, len(chl_ids), _EPG_BATCH_SIZE):
            batch = chl_ids[i : i + _EPG_BATCH_SIZE]
            url = (f"{_EPG_URL}?channelIds={','.join(batch)}"
                   f"&startTime={start_ms}&endTime={end_ms}&langCode=en&countryCode=US")
            try:
                r = self.session.get(url, headers={"token": token}, timeout=30)
                r.raise_for_status()
                batch_data = r.json()
                all_ch_rows.extend(batch_data.get("data") or [])
            except Exception as exc:
                logger.error("[whale] EPG batch fetch failed: %s", exc)

        with _current_prog_desc_lock:
            prog_descs = dict(_current_prog_desc)

        detail_cutoff = now_utc.timestamp() + _EPG_DETAIL_HOURS * 3600
        near_term: dict[str, ProgramData] = {}

        programs: list[ProgramData] = []
        for ch_row in all_ch_rows:
            chl_id = str(ch_row.get("chlId", ""))
            for pt in (ch_row.get("ptList") or []):
                title    = (pt.get("prgTitle") or "").strip() or "Unknown"
                stm      = pt.get("prgStm")
                etm      = pt.get("prgEtm")
                if not stm or not etm:
                    continue
                try:
                    start = datetime.fromtimestamp(int(stm) / 1000, tz=timezone.utc)
                    end   = datetime.fromtimestamp(int(etm) / 1000, tz=timezone.utc)
                except (ValueError, OSError):
                    continue
                if end <= now_utc:
                    continue
                prgch_id = str(pt.get("prgchId") or "")
                prog = ProgramData(
                    source_channel_id = chl_id,
                    title             = title,
                    start_time        = start,
                    end_time          = end,
                    description       = prog_descs.get(prgch_id),
                )
                programs.append(prog)
                if prgch_id and start.timestamp() < detail_cutoff:
                    near_term[prgch_id] = prog

        self._enrich_near_term(near_term, token)

        logger.info("[whale] %d EPG entries", len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(_CHANNEL_SCHEME):
            return raw_url

        chl_id = raw_url[len(_CHANNEL_SCHEME):]
        now    = time.time()

        with _url_cache_lock:
            cached = _url_cache.get(chl_id)
            if cached and (now - cached[1]) < _URL_CACHE_TTL:
                return _fill_url_macros(cached[0])

        # Cache miss or stale — re-fetch all channels to repopulate
        raw = self._fetch_raw_channels()
        with _url_cache_lock:
            for ch in raw:
                cid = ch.get("chlId", "")
                url = ch.get("chlUrl", "")
                if cid and url:
                    _url_cache[cid] = (url, now)
            cached = _url_cache.get(chl_id)
            if cached:
                return _fill_url_macros(cached[0])

        logger.warning("[whale] could not resolve URL for chlId=%s", chl_id)
        return raw_url

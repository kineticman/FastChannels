"""
Whale TV+ scraper for FastChannels.
Uses the rlaxx/zeasn.tv API — no user credentials required.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import json
import os
import re
import sqlite3
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
_API_BASE       = "https://rlaxx.zeasn.tv/livetv"
_AUTH_PATH      = "/api/device/v2/auth/access"
_CHANNELS_PATH  = "/api/device/v2/browser/category/channels"
_EPG_PATH       = "/api/device/v2/browser/epg"
_EPG_DETAIL_PATH = "/api/device/v2/browser/epg/detail"
_AUTH_URL       = _API_BASE + _AUTH_PATH
_CHANNELS_URL   = _API_BASE + _CHANNELS_PATH
_EPG_URL        = _API_BASE + _EPG_PATH
_EPG_DETAIL_URL = _API_BASE + _EPG_DETAIL_PATH

# The v2 API (2026-11) wants a signed `authorization` header on every call:
# accessKey:base64(HMAC-SHA1("/livetv" + path + ts, secretKey)):ts. These keys
# are the ones the public watch.whaletvplus.com web player sends itself.
_ACCESS_KEY = "12dd64aa233691458793d0e026178f45f6"
_SECRET_KEY = "1161280c2d031447e19ec14aa29a689713"


def _signed_headers(path: str, token: str | None = None) -> dict:
    ts = str(int(time.time() * 1000))
    mac = hmac.new(_SECRET_KEY.encode(), f"/livetv{path}{ts}".encode(), hashlib.sha1).digest()
    h = {"authorization": f"{_ACCESS_KEY}:{base64.b64encode(mac).decode()}:{ts}"}
    if token:
        h["token"] = token
    return h
_LOGO_BASE      = "https://d3b6luslimvglo.cloudfront.net/images/79/rlaxximages/channels-rescaled/icon-white"

_CHANNEL_SCHEME = "whale://"
_TOKEN_TTL      = 82800   # 23 hr (API gives ~24 hr)
_URL_CACHE_TTL  = 7200    # 2 hr between resolve() cache refreshes
_EPG_DAYS           = 7
_EPG_BATCH_SIZE     = 10   # channels per EPG request (matches website's batched approach)
_EPG_DETAIL_WORKERS = 8    # concurrent detail requests
# The v2 API rate-limits every endpoint to 100 requests per fixed 30 s window per
# IP (HTTP 429, auth included; measured 2026-11-25). Detail is therefore fetched
# only for what a guide shows first: the airing-now and up-next program of each
# channel (~2 per channel), now-airing first, through a shared throttle that
# stays under the limit and leaves headroom for play-time resolve().
_DETAIL_HORIZON_H = 24    # background fill covers programs starting within this many hours
_FILL_RATE        = 2.0   # background requests/s (the scrape's own throttle uses the rest)
_FILL_DEADLINE    = 180   # s per fill slice; the worker re-enqueues for the rest so the
                          # single maintenance worker is never held for long
_CACHE_PATH = os.environ.get('FASTCHANNELS_WHALE_DETAIL_DB', '/data/cache/whale_detail.db')
_RATE_LIMIT      = 90
_RATE_WINDOW     = 30.5
_DETAIL_DEADLINE = 150     # s of wall-clock spent on enrichment per scrape
_rate_lock  = threading.Lock()
_rate_times: list[float] = []
_throttle_waited = [0.0]   # total seconds spent waiting in _throttle() (debug summary)
_dbg = {'on': False}       # per-run debug flag, see _debug_start()


def _debug_start() -> None:
    """Resolve the debug flag once per run (FC_WHALE_DEBUG / FC_DEBUG / the
    Settings toggle; see app/debug_flag.py). Fails closed outside an app context."""
    try:
        from app.debug_flag import debug_logging_enabled
        _dbg['on'] = debug_logging_enabled('FC_WHALE_DEBUG')
    except Exception:
        _dbg['on'] = False


def _debug(msg: str, *args) -> None:
    if _dbg['on']:
        logger.info('[whale][debug] ' + msg, *args)


# ── Detail cache ──────────────────────────────────────────────────────────────
# Detail (description, poster, rating, episode info) belongs to the program
# (`id`), not the airing (`prgchId`), and never changes, so it is cached forever
# in a small SQLite file (~0.5 KB/program, <2 MB for a day of guide). The scrape
# applies it to every airing for free; fill_detail_cache() fetches the rest.

def _cache_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(_CACHE_PATH), exist_ok=True)
    con = sqlite3.connect(_CACHE_PATH, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("CREATE TABLE IF NOT EXISTS detail (prog_id TEXT PRIMARY KEY, body TEXT NOT NULL, fetched_at REAL NOT NULL)")
    con.execute("CREATE TABLE IF NOT EXISTS pending (prgch_id TEXT PRIMARY KEY, prog_id TEXT NOT NULL, "
                "chl_id TEXT NOT NULL, start_ts REAL NOT NULL)")
    return con


def _slim_detail(d: dict) -> dict:
    images = d.get("images") or []
    return {
        "desc":   (d.get("prgDesc") or "").strip(),
        "poster": ((images[0].get("pimgUrl") or "").strip() if images else ""),
        "rating": (d.get("prgRating") or "").strip(),
        "type":   (d.get("prgType") or "").upper(),
        "series": (d.get("seriesTitle") or "").strip(),
        "title":  (d.get("prgTitle") or "").strip(),
        "season": d.get("seasonNumber"),
        "ep":     d.get("episodeNumber"),
    }


def _apply_detail(prog: ProgramData, d: dict) -> None:
    if d.get("desc"):
        prog.description = d["desc"]
    if d.get("poster"):
        prog.poster_url = d["poster"]
    if d.get("rating"):
        prog.rating = d["rating"]
    if d.get("type") == "EPISODE":
        prog.program_type = "episode"
        if d.get("series"):
            prog.title = d["series"]
            prog.episode_title = d.get("title") or None
        try:
            if d.get("season"):
                prog.season = int(d["season"])
            if d.get("ep"):
                prog.episode = int(d["ep"])
        except (ValueError, TypeError):
            pass


def _cache_get(con: sqlite3.Connection, prog_ids: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for i in range(0, len(prog_ids), 500):
        chunk = prog_ids[i:i + 500]
        q = ",".join("?" * len(chunk))
        for pid, body in con.execute(f"SELECT prog_id, body FROM detail WHERE prog_id IN ({q})", chunk):
            out[pid] = json.loads(body)
    return out


def _cache_put(con: sqlite3.Connection, prog_id: str, d: dict) -> None:
    con.execute("INSERT OR REPLACE INTO detail VALUES (?,?,?)", (prog_id, json.dumps(d), time.time()))


def _throttle() -> None:
    """Block until a request slot is free in the current rate window."""
    while True:
        with _rate_lock:
            now = time.monotonic()
            _rate_times[:] = [t for t in _rate_times if now - t < _RATE_WINDOW]
            if len(_rate_times) < _RATE_LIMIT:
                _rate_times.append(now)
                return
            wait = _RATE_WINDOW - (now - _rate_times[0])
        _throttle_waited[0] += max(wait, 0.05)
        time.sleep(max(wait, 0.05))

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
    added_in = '4.2.0'
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
                    headers=_signed_headers(_AUTH_PATH),
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
                headers=_signed_headers(_CHANNELS_PATH, token),
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

        logger.debug("[whale] %d channels", len(channels))
        return channels

    def _fetch_detail(self, prgch_id: str, token: str) -> tuple[str, dict | None]:
        """One detail call. Returns (status, slim detail); status 'ok'|'limited'|'error'."""
        path = f"{_EPG_DETAIL_PATH}/{prgch_id}"
        for attempt in (0, 1):
            _throttle()
            try:
                r = self.session.get(_API_BASE + path, headers=_signed_headers(path, token), timeout=15)
            except Exception:
                return "error", None
            if r.status_code == 200:
                try:
                    return "ok", _slim_detail(r.json().get("data") or {})
                except Exception:
                    return "error", None
            _debug("detail %s -> HTTP %d (attempt %d)", prgch_id, r.status_code, attempt + 1)
            if r.status_code == 429 and attempt == 0:
                time.sleep(_RATE_WINDOW)   # window miscounted (other traffic): wait it out, retry once
                continue
            return ("limited" if r.status_code == 429 else "error"), None
        return "error", None

    def _enrich_airing(self, airing: list[tuple[str, str, ProgramData]], token: str,
                       con: sqlite3.Connection, progress_base: int, total_channels: int) -> set[str]:
        """Fetch + cache detail for programs airing now that aren't cached yet."""
        got: set[str] = set()
        if not airing:
            return got
        deadline = time.monotonic() + _DETAIL_DEADLINE
        total = progress_base + len(airing)
        done = enriched = errors = 0
        limited = threading.Event()

        def work(item):
            prgch_id, prog_id, _prog = item
            if limited.is_set() or time.monotonic() > deadline:
                return item, "skipped", None
            st, d = self._fetch_detail(prgch_id, token)
            if st == "limited":
                limited.set()
            return item, st, d

        with ThreadPoolExecutor(max_workers=_EPG_DETAIL_WORKERS) as pool:
            for item, st, d in pool.map(work, airing):
                done += 1
                if self._progress_cb:
                    self._progress_cb('epg', progress_base + done, total)
                if st == "ok" and d is not None:
                    _cache_put(con, item[1], d)
                    _apply_detail(item[2], d)
                    got.add(item[1])
                    enriched += 1
                elif st == "error":
                    errors += 1
        con.commit()
        logger.log(logging.INFO if errors else logging.DEBUG, "[whale] airing-now detail: %d fetched, %d errors, %d not attempted",
                    enriched, errors, len(airing) - enriched - errors)
        return got

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        if not channels:
            return []

        token = self._get_token()
        if not token:
            return []

        _debug_start()
        _t_epg = time.monotonic()
        _throttle_waited[0] = 0.0
        chl_ids = [str(ch.source_channel_id) for ch in channels]
        total_channels = len(chl_ids)
        now_utc  = datetime.now(timezone.utc)
        start_ms = int(now_utc.timestamp() * 1000)
        end_ms   = int((now_utc.timestamp() + _EPG_DAYS * 86400) * 1000)

        # Fetch in batches using comma-separated channelIds (matches the website's API usage)
        all_ch_rows: list[dict] = []
        for i in range(0, total_channels, _EPG_BATCH_SIZE):
            batch = chl_ids[i : i + _EPG_BATCH_SIZE]
            url = (f"{_EPG_URL}?channelIds={','.join(batch)}"
                   f"&startTime={start_ms}&endTime={end_ms}&langCode=en&countryCode=US")
            _throttle()
            try:
                r = self.session.get(url, headers=_signed_headers(_EPG_PATH, token), timeout=30)
                r.raise_for_status()
                batch_data = r.json()
                all_ch_rows.extend(batch_data.get("data") or [])
            except Exception as exc:
                logger.error("[whale] EPG batch fetch failed: %s", exc)
            if self._progress_cb:
                self._progress_cb('epg', min(i + _EPG_BATCH_SIZE, total_channels), total_channels)

        _debug("EPG batches: %d channel rows in %.1fs", len(all_ch_rows), time.monotonic() - _t_epg)
        with _current_prog_desc_lock:
            prog_descs = dict(_current_prog_desc)

        now_ts = now_utc.timestamp()
        horizon = now_ts + _DETAIL_HORIZON_H * 3600
        # (prgch_id, prog_id, chl_id, start_ts, prog)
        rows: list[tuple[str, str, str, float, ProgramData]] = []
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
                prog_id = str(pt.get("id") or "")
                if prgch_id and prog_id:
                    rows.append((prgch_id, prog_id, chl_id, start.timestamp(), prog))

        try:
            con = _cache_conn()
            cached = _cache_get(con, list({r[1] for r in rows}))
            for _prgch, prog_id, _chl, _st, prog in rows:
                d = cached.get(prog_id)
                if d:
                    _apply_detail(prog, d)

            # Airing now and not cached: fetch immediately (≤ ~1 per channel).
            airing = [(pc, pid, pr) for pc, pid, _c, st, pr in rows
                      if st <= now_ts and pid not in cached]
            seen: set[str] = set()
            airing = [a for a in airing if not (a[1] in seen or seen.add(a[1]))]
            _debug("%d airings (%d distinct programs), %d already cached, %d airing-now to fetch",
                   len(rows), len({r[1] for r in rows}), len(cached), len(airing))
            have = set(cached) | self._enrich_airing(airing, token, con, total_channels, total_channels)

            # Queue the rest of the next 24 h for the background fill job.
            con.execute("DELETE FROM pending")
            con.executemany(
                "INSERT OR REPLACE INTO pending VALUES (?,?,?,?)",
                [(pc, pid, c, st) for pc, pid, c, st, _pr in rows
                 if st <= horizon and pid not in have],
            )
            # Drop cache entries no airing in the 7-day window references, once stale.
            live_ids = {r[1] for r in rows}
            if live_ids:
                old = [pid for (pid,) in con.execute(
                    "SELECT prog_id FROM detail WHERE fetched_at < ?", (time.time() - 2 * 86400,))
                    if pid not in live_ids]
                con.executemany("DELETE FROM detail WHERE prog_id=?", [(p,) for p in old])
            con.commit()
            n_pending = con.execute("SELECT COUNT(*) FROM pending").fetchone()[0]
            _debug("cache pruned %d stale entries; throttle waited %.1fs; epg phase %.1fs",
                   len(old) if live_ids else 0, _throttle_waited[0], time.monotonic() - _t_epg)
            con.close()
            logger.debug("[whale] detail cache: %d programs applied, %d queued for background fill",
                        len(cached), n_pending)
        except Exception as exc:
            logger.warning("[whale] detail cache unavailable (%s); continuing without detail", exc)

        logger.debug("[whale] %d EPG entries", len(programs))
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


# ── Background fill (maintenance queue) ──────────────────────────────────────

def fill_detail_cache() -> tuple[int, int, bool]:
    """Fetch detail for programs the scrape queued (next 24 h, not yet cached),
    cache it, and write it straight into the programs table so guides update
    without waiting for the next scrape. Needs a Flask app context. Returns the
    (rows updated, airings still pending, rate-limited). One slice of at most
    _FILL_DEADLINE seconds. Does nothing when Whale is disabled."""
    from sqlalchemy.exc import OperationalError
    from app.extensions import db
    from app.models import Source, Channel, Program

    _debug_start()
    con = _cache_conn()
    source = Source.query.filter_by(name="whale").first()
    if not source or not source.is_enabled:
        _debug("fill skipped: whale source %s", "missing" if not source else "disabled")
        con.execute("DELETE FROM pending")
        con.commit()
        con.close()
        return 0, 0, False

    pend = con.execute("SELECT prgch_id, prog_id, chl_id, start_ts FROM pending ORDER BY start_ts").fetchall()
    have = set(_cache_get(con, list({p[1] for p in pend})))
    groups: dict[str, list[tuple[str, str, float]]] = {}
    for prgch_id, prog_id, chl_id, start_ts in pend:
        groups.setdefault(prog_id, []).append((prgch_id, chl_id, start_ts))
    todo = [(pid, g) for pid, g in groups.items() if pid not in have]
    # Programs already cached but still pending only need their rows written.
    todo_rows = [(pid, g, _cache_get(con, [pid])[pid]) for pid, g in groups.items() if pid in have]
    _debug("fill slice start: %d pending airings, %d distinct programs, %d to fetch, %d cached (row write only)",
           len(pend), len(groups), len(todo), len(todo_rows))
    if not todo and not todo_rows:
        con.close()
        return 0, 0, False

    scraper = WhaleScraper()
    token = scraper._get_token()
    if not token and todo:
        con.close()
        logger.warning("[whale] detail fill: no auth token")
        return 0, 0, True

    chan_ids = {c.source_channel_id: c.id for c in
                Channel.query.filter_by(source_id=source.id).with_entities(Channel.source_channel_id, Channel.id)}
    updated = 0

    def write_rows(batch):
        nonlocal updated
        for _pid, g, d in batch:
            vals = {}
            if d.get("desc"):   vals["description"] = d["desc"]
            if d.get("poster"): vals["poster_url"] = d["poster"]
            if d.get("rating"): vals["rating"] = d["rating"]
            if d.get("type") == "EPISODE":
                vals["program_type"] = "episode"
                if d.get("series"):
                    vals["title"] = d["series"]
                    vals["episode_title"] = d.get("title") or None
                try:
                    if d.get("season"): vals["season"] = int(d["season"])
                    if d.get("ep"):     vals["episode"] = int(d["ep"])
                except (ValueError, TypeError):
                    pass
            if not vals:
                continue
            for _prgch, chl_id, start_ts in g:
                cid = chan_ids.get(chl_id)
                if cid is None:
                    _debug("row write: unknown channel %s", chl_id)
                    continue
                n = Program.query.filter_by(
                    channel_id=cid, start_time=datetime.fromtimestamp(start_ts, tz=timezone.utc),
                ).update(vals, synchronize_session=False)
                if not n:
                    _debug("row write: no program row yet for channel %s at %s (not saved by a scrape; the next scrape applies the cached detail)",
                           chl_id, datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat())
                updated += n
        try:
            db.session.commit()
        except OperationalError as exc:
            db.session.rollback()
            logger.warning("[whale] detail fill: DB busy, rows will pick detail up on next scrape (%s)", exc)

    write_rows(todo_rows)
    for pid, g, _d in todo_rows:
        con.executemany("DELETE FROM pending WHERE prgch_id=?", [(x[0],) for x in g])
    con.commit()

    pace_lock = threading.Lock()
    next_slot = [time.monotonic()]
    stop = threading.Event()
    deadline = time.monotonic() + _FILL_DEADLINE

    def fetch(item):
        pid, g = item
        if stop.is_set() or time.monotonic() > deadline:
            return item, "skipped", None
        with pace_lock:
            wait = next_slot[0] - time.monotonic()
            next_slot[0] = max(next_slot[0], time.monotonic()) + 1.0 / _FILL_RATE
        if wait > 0:
            time.sleep(wait)
        st, d = scraper._fetch_detail(g[0][0], token)
        if st == "limited":
            stop.set()
        return item, st, d

    fetched = errors = 0
    t_slice = time.monotonic()
    batch: list = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        for (pid, g), st, d in pool.map(fetch, todo):
            if st == "ok" and d is not None:
                _cache_put(con, pid, d)
                batch.append((pid, g, d))
                fetched += 1
                if fetched % 25 == 0:
                    el = time.monotonic() - t_slice
                    _debug("fill progress: %d/%d fetched, %.1f req/s, %d errors",
                           fetched, len(todo), fetched / el if el else 0, errors)
            elif st == "error":
                errors += 1
            if len(batch) >= 100:
                write_rows(batch)
                for bpid, bg, _ in batch:
                    con.executemany("DELETE FROM pending WHERE prgch_id=?", [(x[0],) for x in bg])
                con.commit()
                batch = []
    if batch:
        write_rows(batch)
        for bpid, bg, _ in batch:
            con.executemany("DELETE FROM pending WHERE prgch_id=?", [(x[0],) for x in bg])
    con.commit()
    left = con.execute("SELECT COUNT(*) FROM pending").fetchone()[0]
    con.close()
    logger.info("[whale] detail fill: %d fetched, %d errors, %d program rows updated, %d still pending%s",
                fetched, errors, updated, left, " (rate-limited)" if stop.is_set() else "")
    _debug("fill slice end: %.1fs, reason=%s, throttle waited %.1fs", time.monotonic() - t_slice,
           "rate-limited" if stop.is_set() else ("drained" if not left else "deadline"), _throttle_waited[0])
    if stop.is_set():
        logger.info("[whale] detail fill stopped by the rate limit; %d pending, the 3-hourly job resumes it", left)
    elif not left:
        logger.info("[whale] detail fill complete: every queued program has its detail")
    return updated, left, stop.is_set()

"""
tvtv_lookup.py

Lightweight now-playing lookup for FAST channel stationIds via tvtv.us.

Designed for the Gracenote Suggestions helper: given a Gracenote/TMS stationId,
return what is currently airing so a user can compare it against their own
channel's program to verify a Gracenote mapping is correct.

Features:
- Loads the bundled station_index.json (stationId → lineup mapping)
- Caches the full grid response per lineup for 5 minutes — so looking up 5
  suggestions from the same lineup costs 1 API call, not 5
- Uses curl_cffi (Chrome TLS impersonation) if installed, falls back to requests
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_INDEX_PATH = Path(__file__).resolve().parent / "data" / "station_index.json"
_TVTV_BASE = "https://tvtv.us"

# Grid cache: (lineup_slug, station_id) → (fetched_at_epoch, items_list)
_grid_cache: dict[tuple[str, str], tuple[float, list]] = {}
_GRID_CACHE_TTL = 300  # 5 minutes


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

try:
    from curl_cffi import requests as _http
    _CURL_CFFI = True
except ImportError:
    import requests as _http  # type: ignore[no-redef]
    _CURL_CFFI = False


def _make_session():
    if _CURL_CFFI:
        s = _http.Session(impersonate="chrome120")
    else:
        s = _http.Session()
    s.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Referer": f"{_TVTV_BASE}/",
        "Origin": _TVTV_BASE,
    })
    return s


# ---------------------------------------------------------------------------
# Station index
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_index() -> dict[str, Any]:
    import json
    if not _INDEX_PATH.exists():
        log.warning("[tvtv] station_index.json not found at %s", _INDEX_PATH)
        return {}
    data = json.loads(_INDEX_PATH.read_text())
    return data.get("stations", {})


def get_station_entry(station_id: str) -> dict[str, Any] | None:
    return _load_index().get(str(station_id))


# ---------------------------------------------------------------------------
# Grid window (matches tvtv's fixed 04:00Z anchor)
# ---------------------------------------------------------------------------

def _grid_window(now_utc: datetime) -> tuple[datetime, datetime]:
    anchor = now_utc.replace(hour=4, minute=0, second=0, microsecond=0)
    start = anchor if now_utc >= anchor else anchor - timedelta(days=1)
    end = start + timedelta(days=1) - timedelta(minutes=1)
    return start, end


def _iso_z(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


# ---------------------------------------------------------------------------
# Grid fetch with cache
# ---------------------------------------------------------------------------

def _fetch_items_cached(lineup: str, station_id: str, session) -> list:
    """
    Return the airing list for one station in a lineup, with 5-minute caching.
    """
    cache_key = (lineup, station_id)
    cached = _grid_cache.get(cache_key)
    now_ts = time.monotonic()

    if cached and (now_ts - cached[0]) < _GRID_CACHE_TTL:
        return cached[1]

    now_utc = datetime.now(timezone.utc)
    start, end = _grid_window(now_utc)
    url = (
        f"{_TVTV_BASE}/api/v1/lineup/{lineup}/grid/"
        f"{_iso_z(start)}/{_iso_z(end)}/{station_id}"
    )
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
        grid = r.json()
    except Exception as exc:
        log.warning("[tvtv] grid fetch failed for %s/%s: %s", lineup, station_id, exc)
        # curl_cffi HTTPError inherits from OSError (no .response attr), so check the message.
        if "429" in str(exc) or getattr(getattr(exc, 'response', None), 'status_code', None) == 429:
            return None  # Distinguish rate-limit from empty schedule
        return []

    items = grid[0] if isinstance(grid, list) and grid and isinstance(grid[0], list) else []
    _grid_cache[cache_key] = (now_ts, items)
    return items


# ---------------------------------------------------------------------------
# Now/next extraction
# ---------------------------------------------------------------------------

def _parse_start(item: dict) -> datetime | None:
    value = item.get("startTime")
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _pick_now_next(items: list[dict], now_utc: datetime):
    parsed = []
    for item in items:
        start = _parse_start(item)
        if not start:
            continue
        end = start + timedelta(minutes=int(item.get("duration") or 0))
        parsed.append((start, end, item))
    parsed.sort(key=lambda x: x[0])

    now_entry = next_entry = None
    for i, (start, end, item) in enumerate(parsed):
        if start <= now_utc < end:
            now_entry = (start, end, item)
            if i + 1 < len(parsed):
                next_entry = parsed[i + 1]
            break

    if now_entry is None:
        next_entry = next((e for e in parsed if e[0] > now_utc), None)

    return now_entry, next_entry


def _fmt(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def _program_dict(entry) -> dict | None:
    if entry is None:
        return None
    start, end, item = entry
    return {
        "title":      item.get("title") or item.get("programTitle") or "Unknown",
        "subtitle":   item.get("subtitle") or None,
        "program_id": item.get("programId"),
        "start":      _fmt(start),
        "end":        _fmt(end),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _lookup_from_db_cache(station_id: str) -> dict[str, Any] | None:
    """
    Try to serve now/next from the DB cache (tvtv_program_cache).
    Returns None if no usable cache entry exists or if outside a Flask context.
    """
    try:
        from .tvtv_cache import get_now_next
        result = get_now_next(station_id)
        if result.get("now") or result.get("next"):
            entry = get_station_entry(station_id)
            result["call_sign"] = entry.get("call_sign") if entry else None
            result["lineup"]    = (entry.get("lineups") or [None])[0] if entry else None
            result["found"]     = True
            result["error"]     = None
            return result
    except Exception:
        pass
    return None


def lookup_now_playing(station_id: str) -> dict[str, Any]:
    """
    Return now/next for a stationId.

    Response shape:
    {
        "station_id": "141469",
        "call_sign": "FOXFAST",
        "lineup": "USA-SAMSUNG-X",
        "found": true,
        "now":  {"title": "...", "program_id": "...", "start": "...", "end": "..."},
        "next": {"title": "...", "program_id": "...", "start": "..."},
        "error": null
    }

    On any failure, "found" is false and "error" is set.
    """
    result: dict[str, Any] = {
        "station_id": station_id,
        "call_sign": None,
        "lineup": None,
        "found": False,
        "now": None,
        "next": None,
        "error": None,
    }

    # Check DB cache first — populated by the nightly refresh job.
    cached = _lookup_from_db_cache(station_id)
    if cached:
        return cached

    entry = get_station_entry(station_id)
    if not entry:
        result["error"] = "not_in_index"
        return result

    lineup = entry["lineups"][0] if entry.get("lineups") else None
    if not lineup:
        result["error"] = "no_lineup"
        return result

    result["call_sign"] = entry.get("call_sign")
    result["lineup"] = lineup
    result["found"] = True

    session = _make_session()
    items = _fetch_items_cached(lineup, station_id, session)
    if items is None:
        result["error"] = "rate_limited"
        return result

    now_utc = datetime.now(timezone.utc)
    now_entry, next_entry = _pick_now_next(items, now_utc)

    result["now"] = _program_dict(now_entry)
    result["next"] = _program_dict(next_entry)
    return result

"""
tvtv_lookup.py

Now-playing lookup for Gracenote stationIds, plus the bundled station index.

Designed for the Gracenote Suggestions helper: given a Gracenote/TMS stationId,
return what is currently airing so a user can compare it against their own
channel's program to verify a Gracenote mapping is correct.

- Serves now/next from the guide cache (app/guide_cache.py, filled nightly
  from Gracenote's listings with tvtv as fallback); a station the cache
  doesn't cover (e.g. a suggested id not mapped to any channel yet) is
  fetched live through the same provider order, memoised for 5 minutes.
- Loads the bundled station_index.json (stationId → call sign/lineups), used
  for display metadata and the cache rows' lineup label.

The module keeps its historical name; nothing in it is tvtv-specific now.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_INDEX_PATH = Path(__file__).resolve().parent / "data" / "station_index.json"


# ---------------------------------------------------------------------------
# Station index
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_index() -> dict[str, Any]:
    import json
    if not _INDEX_PATH.exists():
        log.warning("[station-index] station_index.json not found at %s", _INDEX_PATH)
        return {}
    data = json.loads(_INDEX_PATH.read_text())
    return data.get("stations", {})


def get_station_entry(station_id: str) -> dict[str, Any] | None:
    return _load_index().get(str(station_id))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def lookup_now_playing(station_id: str) -> dict[str, Any]:
    """
    Return now/next for a stationId.

    Response shape:
    {
        "station_id": "141469",
        "call_sign": "FOXFAST",
        "lineup": "USA-SAMSUNG-X",
        "found": true,
        "source": "cache" | "live",
        "now":  {"title": "...", "subtitle": "...", "program_id": "...", "start": "...", "end": "..."},
        "next": {...},
        "error": null
    }

    On any failure, "found" is false and "error" is set.
    """
    from . import guide_cache

    entry = get_station_entry(station_id)
    result: dict[str, Any] = {
        "station_id": station_id,
        "call_sign": entry.get("call_sign") if entry else None,
        "lineup": (entry.get("lineups") or [None])[0] if entry else None,
        "found": False,
        "now": None,
        "next": None,
        "error": None,
    }

    try:
        data = guide_cache.now_next(station_id)
        if not (data.get("now") or data.get("next")):
            data = guide_cache.live_now_next(station_id)
    except Exception as exc:
        log.warning("[now-playing] lookup for %s failed: %s", station_id, exc)
        data = None
    if not data:
        result["error"] = "unavailable"
        return result
    result.update(found=True, source=data.get("source"), now=data.get("now"), next=data.get("next"))
    return result

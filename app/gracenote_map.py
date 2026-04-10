from __future__ import annotations

import csv
import logging
import os
import time
from pathlib import Path

log = logging.getLogger(__name__)

_BUILTIN_PATH = Path(__file__).resolve().parent / "data" / "gracenote_map.csv"
_OVERRIDE_PATH = Path(os.environ.get("FASTCHANNELS_GRACENOTE_MAP_PATH") or "/data/gracenote_map_overrides.csv")
_REMOTE_CACHE_PATH = Path("/data/gracenote_map_remote.csv")

# Timestamp of the last successful remote fetch (epoch seconds, 0 = never).
_remote_fetched_at: float = 0.0

# mtime-based in-process cache — shared across requests in this worker,
# but automatically invalidated when any source file changes on disk.
# This means all gunicorn workers pick up new files without an explicit
# cache-clear signal.
_map_cache: dict[tuple[str, str], dict[str, str]] | None = None
_map_cache_mtimes: tuple[float, ...] = ()


def _normalize_station_id(value) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        return raw if len(raw) >= 5 else None
    return raw


def normalize_gracenote_id(value) -> str | None:
    return _normalize_station_id(value)


def _iter_rows(path: Path):
    if not path.exists():
        return
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                yield row
    except Exception as exc:
        log.warning("[gracenote-map] failed to read %s: %s", path, exc)


def _source_mtimes() -> tuple[float, ...]:
    mtimes = []
    for path in (_BUILTIN_PATH, _REMOTE_CACHE_PATH, _OVERRIDE_PATH):
        try:
            mtimes.append(path.stat().st_mtime)
        except FileNotFoundError:
            mtimes.append(0.0)
    return tuple(mtimes)


def _load_map() -> dict[tuple[str, str], dict[str, str]]:
    global _map_cache, _map_cache_mtimes
    current_mtimes = _source_mtimes()
    if _map_cache is not None and current_mtimes == _map_cache_mtimes:
        return _map_cache

    # Priority: builtin < remote cache < local overrides
    mapping: dict[tuple[str, str], dict[str, str]] = {}
    for path in (_BUILTIN_PATH, _REMOTE_CACHE_PATH, _OVERRIDE_PATH):
        for row in _iter_rows(path) or ():
            provider = (row.get("provider") or "").strip().lower()
            key = (row.get("key") or "").strip()
            tmsid = normalize_gracenote_id(row.get("tmsid"))
            if not provider or not key or not tmsid:
                continue
            payload = {"tmsid": tmsid}
            time_shift = (row.get("time_shift") or "").strip()
            if time_shift:
                payload["time_shift"] = time_shift
            notes = (row.get("notes") or "").strip()
            if notes:
                payload["notes"] = notes
            mapping[(provider, key)] = payload
            # Plex channel IDs can carry a volatile left-hand prefix while the
            # right-hand segment stays stable across environments. Seed a
            # secondary lookup by suffix so curated external mappings remain
            # useful even when Plex rotates the leading token.
            if provider == "plex" and "-" in key:
                _, suffix = key.split("-", 1)
                if suffix:
                    mapping.setdefault((provider, suffix), payload)

    _map_cache = mapping
    _map_cache_mtimes = current_mtimes
    return mapping


def reload_gracenote_map() -> None:
    global _map_cache
    _map_cache = None


def fetch_remote_gracenote_map(url: str) -> tuple[bool, str]:
    """Download the remote community map CSV and cache it to disk.

    Returns (success, message).  Clears the in-memory cache on success so the
    next lookup picks up the new data.
    """
    global _remote_fetched_at
    if not url:
        return False, "No remote URL configured."

    import requests
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        content = r.text
    except Exception as exc:
        log.warning("[gracenote-map] remote fetch failed: %s", exc)
        return False, f"Fetch failed: {exc}"

    # Basic sanity check — must look like a CSV with a header row
    lines = content.strip().splitlines()
    if not lines or "provider" not in lines[0].lower():
        return False, "Remote file does not look like a valid gracenote_map CSV (missing 'provider' header)."

    try:
        _REMOTE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _REMOTE_CACHE_PATH.write_text(content, encoding="utf-8")
    except Exception as exc:
        log.warning("[gracenote-map] could not write remote cache: %s", exc)
        return False, f"Could not write cache file: {exc}"

    _remote_fetched_at = time.time()
    reload_gracenote_map()
    row_count = sum(1 for ln in lines[1:] if ln.strip())
    log.info("[gracenote-map] remote map refreshed — %d rows from %s", row_count, url)
    return True, f"OK — {row_count} rows loaded."


def remote_map_status() -> dict:
    """Return metadata about the remote map for display in the UI."""
    return {
        "cached": _REMOTE_CACHE_PATH.exists(),
        "fetched_at": _remote_fetched_at or None,
        "cache_path": str(_REMOTE_CACHE_PATH),
        "row_count": sum(1 for _ in _iter_rows(_REMOTE_CACHE_PATH)) if _REMOTE_CACHE_PATH.exists() else 0,
    }


def lookup_gracenote(provider: str, key: str | None) -> dict[str, str] | None:
    provider_name = (provider or "").strip().lower()
    key_name = (key or "").strip()
    if not provider_name or not key_name:
        return None
    mapping = _load_map()
    match = mapping.get((provider_name, key_name))
    if match:
        return match
    if provider_name == "plex" and "-" in key_name:
        _, suffix = key_name.split("-", 1)
        if suffix:
            return mapping.get((provider_name, suffix))
    return None


def get_all_tmsids() -> list[str]:
    """Return all unique tmsid values in the loaded community map."""
    mapping = _load_map()
    seen: set[str] = set()
    result: list[str] = []
    for payload in mapping.values():
        tmsid = payload.get("tmsid")
        if tmsid and tmsid not in seen:
            seen.add(tmsid)
            result.append(tmsid)
    return result


def resolve_gracenote(provider: str, *, upstream_id=None, lookup_key: str | None = None) -> str | None:
    direct = normalize_gracenote_id(upstream_id)
    if direct:
        return direct
    if lookup_key:
        match = lookup_gracenote(provider, lookup_key)
        if match:
            return match.get("tmsid")
    return None

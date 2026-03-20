from __future__ import annotations

import csv
import logging
import os
from functools import lru_cache
from pathlib import Path

log = logging.getLogger(__name__)

_BUILTIN_PATH = Path(__file__).resolve().parent / "data" / "gracenote_map.csv"
_OVERRIDE_PATH = Path(os.environ.get("FASTCHANNELS_GRACENOTE_MAP_PATH") or "/data/gracenote_map_overrides.csv")


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


@lru_cache(maxsize=1)
def _load_map() -> dict[tuple[str, str], dict[str, str]]:
    mapping: dict[tuple[str, str], dict[str, str]] = {}
    for path in (_BUILTIN_PATH, _OVERRIDE_PATH):
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
    return mapping


def reload_gracenote_map() -> None:
    _load_map.cache_clear()


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


def resolve_gracenote(provider: str, *, upstream_id=None, lookup_key: str | None = None) -> str | None:
    direct = normalize_gracenote_id(upstream_id)
    if direct:
        return direct
    if lookup_key:
        match = lookup_gracenote(provider, lookup_key)
        if match:
            return match.get("tmsid")
    return None

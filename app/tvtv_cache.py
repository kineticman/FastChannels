"""
tvtv_cache.py

Nightly bulk cache of tvtv.us guide data for all FAST channel stations in
the bundled station index.  Fetches 2 days of grid data (today + 1) and
stores it in the tvtv_program_cache table.

Called by the background worker on a cron schedule (default: 03:00 UTC).

Typical cost: ~70-170 batched API calls when tvtv's JSON grid endpoint is
available. If that endpoint 404s, falls back to tvtv's htmx fragments with
bounded per-station concurrency. Uses curl_cffi for a browser-like HTTP session
without a headless browser bootstrap.

Standalone dry run (prints stats, writes nothing):
    docker exec fastchannels python -m app.tvtv_cache --dry-run
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

log = logging.getLogger(__name__)

_BATCH_SIZE  = 20    # station IDs per grid request — Cloudflare blocks >20
_BATCH_DELAY = 1.0   # seconds between batches within a lineup-day
_FRAGMENT_WORKERS = 8  # concurrent per-station htmx fallback fetches per batch
_FRAGMENT_DELAY = 0.0  # optional pace between fallback per-station fragment calls
_DAY_DELAY   = 1.0   # seconds between lineup-day pairs
_DAYS        = 2     # days of guide data to cache (today + 1)

_TVTV_BASE = "https://tvtv.us"
_FRAGMENT_FALLBACK_LOGGED = False
_FRAGMENT_THREAD_STATE = threading.local()


# ---------------------------------------------------------------------------
# Helpers (shared with tvtv_lookup — kept in sync manually)
# ---------------------------------------------------------------------------

def _grid_window(day_offset: int, now_utc: datetime) -> tuple[datetime, datetime]:
    anchor = now_utc.replace(hour=4, minute=0, second=0, microsecond=0)
    today_start = anchor if now_utc >= anchor else anchor - timedelta(days=1)
    start = today_start + timedelta(days=day_offset)
    end   = start + timedelta(days=1) - timedelta(minutes=1)
    return start, end


def _iso_z(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _parse_start(item: dict) -> datetime | None:
    value = item.get("startTime")
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _fragment_day_keys(start: datetime, end: datetime) -> list[int]:
    keys: list[int] = []
    current = start.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    final = end.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    while current <= final:
        keys.append(int(current.timestamp() * 1000))
        current += timedelta(days=1)
    return keys


def _parse_fragment_airings(html: str, start: datetime, end: datetime) -> list[dict]:
    """Parse tvtv's current htmx grid fragment format."""
    try:
        from bs4 import BeautifulSoup, NavigableString
    except Exception:
        log.warning("[tvtv-cache] BeautifulSoup unavailable; cannot parse tvtv fragment fallback")
        return []

    soup = BeautifulSoup(html or "", "html.parser")
    items: list[dict] = []
    seen: set[tuple[str, int]] = set()
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)

    for airing in soup.select(".gridAiring"):
        raw_time = airing.get("data-time")
        raw_runtime = airing.get("data-runtime")
        if not raw_time or not raw_runtime:
            continue
        try:
            start_ms = int(raw_time)
            runtime = int(raw_runtime)
        except (TypeError, ValueError):
            continue

        item_start = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        if item_start < start_utc or item_start > end_utc:
            continue

        subtitle_el = airing.select_one(".gridSubtitle")
        subtitle = subtitle_el.get_text(" ", strip=True) if subtitle_el else None
        title_el = airing.find("div", recursive=False)
        if title_el is not None:
            title = title_el.get_text(" ", strip=True)
        else:
            title_parts: list[str] = []
            for child in airing.children:
                if isinstance(child, NavigableString):
                    text = str(child).strip()
                    if text:
                        title_parts.append(text)
                elif getattr(child, "name", None) != "span":
                    text = child.get_text(" ", strip=True)
                    if text:
                        title_parts.append(text)
            title = " ".join(title_parts).strip()

        program_id = airing.get("data-id")
        key = (program_id or "", start_ms)
        if key in seen:
            continue
        seen.add(key)
        items.append({
            "programId": program_id,
            "title": title or "Unknown",
            "subtitle": subtitle,
            "startTime": _iso_z(item_start),
            "duration": runtime,
        })

    items.sort(key=lambda item: item.get("startTime") or "")
    return items


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


def _get_session():
    """
    Prime a curl_cffi-backed session for tvtv.us.

    tvtv's API currently responds directly to a browser-like HTTP client, so
    we avoid a headless-browser Cloudflare bootstrap here. A best-effort GET
    to the homepage warms any cookies the site wants to set before the batch
    API calls start.
    """
    s = _make_session()
    try:
        r = s.get(f"{_TVTV_BASE}/", timeout=15)
        log.info("[tvtv-cache] primed session via homepage: HTTP %s", r.status_code)
    except Exception as exc:
        log.warning("[tvtv-cache] homepage prime failed: %s — continuing with direct API session", exc)
    return s


# ---------------------------------------------------------------------------
# Core fetch
# ---------------------------------------------------------------------------

def _fetch_fragment_station(session, station_id: str, start: datetime, end: datetime) -> list[dict] | None:
    airings: list[dict] = []
    seen: set[tuple[str, str]] = set()
    old_headers = dict(getattr(session, "headers", {}) or {})
    try:
        session.headers.update({
            "Accept": "text/html, */*",
            "HX-Request": "true",
            "Referer": f"{_TVTV_BASE}/",
        })
        for day_key in _fragment_day_keys(start, end):
            url = f"{_TVTV_BASE}/partial/source/{day_key}/{station_id}"
            try:
                r = session.get(url, timeout=20)
                r.raise_for_status()
            except Exception as exc:
                log.debug("[tvtv-cache] fragment failed %s day=%s: %s", station_id, day_key, exc)
                return None

            for item in _parse_fragment_airings(r.text, start, end):
                key = (str(item.get("programId") or ""), str(item.get("startTime") or ""))
                if key in seen:
                    continue
                seen.add(key)
                airings.append(item)
            time.sleep(_FRAGMENT_DELAY)
    finally:
        session.headers.clear()
        session.headers.update(old_headers)

    airings.sort(key=lambda item: item.get("startTime") or "")
    return airings


def _fragment_worker_session():
    session = getattr(_FRAGMENT_THREAD_STATE, "session", None)
    if session is None:
        session = _make_session()
        _FRAGMENT_THREAD_STATE.session = session
    return session


def _fetch_fragment_station_threaded(station_id: str, start: datetime, end: datetime) -> list[dict] | None:
    return _fetch_fragment_station(_fragment_worker_session(), station_id, start, end)


def _fetch_batch_via_fragments(session, station_ids: list[str],
                               start: datetime, end: datetime) -> tuple[dict[str, list[dict]], str | None]:
    global _FRAGMENT_FALLBACK_LOGGED
    if not _FRAGMENT_FALLBACK_LOGGED:
        log.warning(
            "[tvtv-cache] JSON grid endpoint returned 404; using per-station fragment fallback (%d workers)",
            _FRAGMENT_WORKERS,
        )
        _FRAGMENT_FALLBACK_LOGGED = True

    result: dict[str, list[dict]] = {}
    failures = 0
    max_workers = max(1, min(_FRAGMENT_WORKERS, len(station_ids)))
    if max_workers == 1:
        for station_id in station_ids:
            airings = _fetch_fragment_station(session, station_id, start, end)
            if airings is None:
                failures += 1
                continue
            result[station_id] = airings
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(_fetch_fragment_station_threaded, station_id, start, end): station_id
                for station_id in station_ids
            }
            for future in as_completed(future_map):
                station_id = future_map[future]
                try:
                    airings = future.result()
                except Exception as exc:
                    log.debug("[tvtv-cache] fragment worker failed %s: %s", station_id, exc)
                    failures += 1
                    continue
                if airings is None:
                    failures += 1
                    continue
                result[station_id] = airings

    if result:
        return result, None
    return {}, "fragment fallback failed" if failures else "empty fragment response"


def _fetch_batch(session, lineup: str, station_ids: list[str],
                 start: datetime, end: datetime) -> tuple[dict[str, list[dict]], str | None]:
    """
    Fetch one batch of station IDs for a lineup-day window.
    Returns ({station_id: [airing, ...]}, failure_reason).
    """
    url = (
        f"{_TVTV_BASE}/api/v1/lineup/{lineup}/grid/"
        f"{_iso_z(start)}/{_iso_z(end)}/{','.join(station_ids)}"
    )
    try:
        r = session.get(url, timeout=25)
        r.raise_for_status()
        grid = r.json()
    except Exception as exc:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        reason = f"HTTP {status}" if status else type(exc).__name__
        log.debug("[tvtv-cache] batch failed %s %s...: %s", lineup, station_ids[:3], exc)
        if status == 404 or "404" in str(exc):
            return _fetch_batch_via_fragments(session, station_ids, start, end)
        return {}, reason

    result: dict[str, list[dict]] = {}
    for i, sid in enumerate(station_ids):
        if i < len(grid) and isinstance(grid[i], list):
            result[sid] = grid[i]
    return result, None


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------

def _upsert_rows(rows: list[dict]) -> int:
    """Bulk-insert rows, ignoring duplicates (station_id, start_time)."""
    from .extensions import db
    from .models import TvtvProgramCache
    if not rows:
        return 0
    # SQLite INSERT OR IGNORE honours the UNIQUE constraint.
    db.session.execute(
        TvtvProgramCache.__table__.insert().prefix_with("OR IGNORE"),
        rows,
    )
    return len(rows)


def _delete_expired(now_utc: datetime) -> int:
    """Remove entries whose end_time is more than 1 hour in the past."""
    from .extensions import db
    from .models import TvtvProgramCache
    cutoff = now_utc - timedelta(hours=1)
    deleted = TvtvProgramCache.query.filter(TvtvProgramCache.end_time < cutoff).delete()
    return deleted


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def refresh_tvtv_cache(days: int = _DAYS, dry_run: bool = False,
                       station_ids: list[str] | None = None) -> dict[str, Any]:
    """
    Fetch `days` days of guide data for the given station IDs (or all mapped
    gracenote_ids from the Channel table if station_ids is None) and store
    the results in tvtv_program_cache.

    Returns a summary dict:
        {lineups_fetched, days, batches, rows_inserted, rows_deleted, errors, elapsed_s}
    """
    from .tvtv_lookup import _load_index
    from .extensions import db

    t0 = time.monotonic()
    now_utc = datetime.now(timezone.utc)
    fetched_at = now_utc

    index = _load_index()
    if not index:
        return {"error": "station index is empty"}

    # Determine which station IDs to fetch.
    if station_ids is None:
        from .models import Channel
        import sqlalchemy as sa
        rows = db.session.execute(
            sa.select(sa.func.distinct(Channel.gracenote_id)).where(Channel.gracenote_id.isnot(None))
        ).scalars().all()
        station_ids = [str(sid) for sid in rows if sid]
        log.info("[tvtv-cache] fetching %d mapped gracenote station IDs", len(station_ids))

    station_set = set(station_ids)

    # Group stations by their primary lineup (first entry in lineups list).
    lineup_stations: dict[str, list[str]] = {}
    for sid, entry in index.items():
        if sid not in station_set:
            continue
        lineup = (entry.get("lineups") or [None])[0]
        if lineup:
            lineup_stations.setdefault(lineup, []).append(sid)

    total_batches = 0
    total_rows    = 0
    total_errors  = 0

    # Reuse one warmed session for the entire refresh run.
    session = _get_session() if not dry_run else None

    for lineup, station_ids in lineup_stations.items():
        for day_offset in range(days):
            start, end = _grid_window(day_offset, now_utc)
            batches = [
                station_ids[i: i + _BATCH_SIZE]
                for i in range(0, len(station_ids), _BATCH_SIZE)
            ]

            log.info("[tvtv-cache] %s day+%d: %d stations in %d batches",
                     lineup, day_offset, len(station_ids), len(batches))

            day_errors = 0
            day_error_reasons: dict[str, int] = {}
            for batch in batches:
                if dry_run:
                    total_batches += 1
                    continue

                results, failure_reason = _fetch_batch(session, lineup, batch, start, end)
                if not results:
                    total_errors += 1
                    day_errors   += 1
                    reason = failure_reason or "empty response"
                    day_error_reasons[reason] = day_error_reasons.get(reason, 0) + 1
                    time.sleep(_BATCH_DELAY)
                    continue

                rows = []
                for sid, airings in results.items():
                    for item in airings:
                        item_start = _parse_start(item)
                        if not item_start:
                            continue
                        duration = int(item.get("duration") or 0)
                        item_end = item_start + timedelta(minutes=duration)
                        rows.append({
                            "station_id": sid,
                            "lineup":     lineup,
                            "program_id": item.get("programId"),
                            "title":      (item.get("title") or item.get("programTitle") or "Unknown").strip(),
                            "subtitle":   (item.get("subtitle") or "").strip() or None,
                            "start_time": item_start,
                            "end_time":   item_end,
                            "fetched_at": fetched_at,
                        })

                total_rows    += _upsert_rows(rows)
                db.session.commit()
                total_batches += 1
                time.sleep(_BATCH_DELAY)

            if day_errors:
                reason_summary = ", ".join(
                    f"{reason}={count}"
                    for reason, count in sorted(day_error_reasons.items())
                ) or "unknown"
                log.warning("[tvtv-cache] %s day+%d: %d/%d batches failed (%s)",
                            lineup, day_offset, day_errors, len(batches), reason_summary)

            time.sleep(_DAY_DELAY)

    if not dry_run:
        deleted = _delete_expired(now_utc)
        db.session.commit()
    else:
        deleted = 0

    elapsed = round(time.monotonic() - t0, 1)
    summary = {
        "lineups_fetched": len(lineup_stations),
        "days":            days,
        "batches":         total_batches,
        "rows_inserted":   total_rows,
        "rows_deleted":    deleted,
        "errors":          total_errors,
        "elapsed_s":       elapsed,
        "dry_run":         dry_run,
    }
    log.info("[tvtv-cache] refresh complete: %s", summary)
    return summary


# ---------------------------------------------------------------------------
# Query helpers (for future use)
# ---------------------------------------------------------------------------

def get_now_next(station_id: str, now_utc: datetime | None = None) -> dict[str, Any]:
    """
    Return now/next from the DB cache for a stationId.
    Returns None values if no cache entry covers the current time.
    """
    from .models import TvtvProgramCache
    now_utc = now_utc or datetime.now(timezone.utc)

    rows = (
        TvtvProgramCache.query
        .filter(
            TvtvProgramCache.station_id == station_id,
            TvtvProgramCache.end_time   > now_utc - timedelta(minutes=5),
            TvtvProgramCache.start_time < now_utc + timedelta(hours=4),
        )
        .order_by(TvtvProgramCache.start_time)
        .limit(10)
        .all()
    )

    now_row = next_row = None
    for row in rows:
        if row.start_time <= now_utc < row.end_time:
            now_row = row
        elif row.start_time > now_utc and now_row is not None:
            next_row = row
            break

    if now_row is None:
        next_row = next((r for r in rows if r.start_time > now_utc), None)

    def _row_dict(r):
        if r is None:
            return None
        return {
            "title":      r.title,
            "subtitle":   r.subtitle,
            "program_id": r.program_id,
            "start":      r.start_time.isoformat(),
            "end":        r.end_time.isoformat(),
        }

    return {
        "station_id": station_id,
        "source":     "cache",
        "now":        _row_dict(now_row),
        "next":       _row_dict(next_row),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse, json, sys
    p = argparse.ArgumentParser(description="Refresh tvtv program cache")
    p.add_argument("--dry-run",  action="store_true", help="Count batches without fetching or writing")
    p.add_argument("--days",     type=int, default=_DAYS, help="Days of guide to cache")
    args = p.parse_args()

    # Need Flask app context for DB access.
    import os
    os.chdir(os.path.dirname(os.path.dirname(__file__)))
    from app import create_app
    app = create_app()
    with app.app_context():
        result = refresh_tvtv_cache(days=args.days, dry_run=args.dry_run)
        print(json.dumps(result, indent=2))
    sys.exit(0 if not result.get("error") else 1)

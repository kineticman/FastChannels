"""
tvtv_cache.py

Nightly bulk cache of tvtv.us guide data for all FAST channel stations in
the bundled station index.  Fetches 3 days of grid data (today + 2) and
stores it in the tvtv_program_cache table.

Called by the background worker on a cron schedule (default: 03:00 UTC).

Typical cost: ~100-250 batched API calls (4 lineups × 3 days × ~20 batches
each), taking ~2-4 minutes including pacing delays.  Uses curl_cffi for
Cloudflare bypass with a fresh session per lineup-day pair.

Standalone dry run (prints stats, writes nothing):
    docker exec fastchannels python -m app.tvtv_cache --dry-run
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

log = logging.getLogger(__name__)

_BATCH_SIZE  = 20    # station IDs per grid request — Cloudflare blocks >20
_BATCH_DELAY = 1.5   # seconds between batches within a lineup-day
_DAY_DELAY   = 2.0   # seconds between lineup-day pairs
_DAYS        = 3     # days of guide data to cache (today + 2)

_TVTV_BASE = "https://tvtv.us"


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


def _get_cf_session():
    """
    Launch headless Chromium via Playwright, let it pass the Cloudflare
    challenge on tvtv.us, extract cf_clearance + User-Agent, and return
    a curl_cffi Session pre-loaded with those credentials.

    Falls back to a plain _make_session() if Playwright is unavailable or
    the challenge doesn't complete.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("[tvtv-cache] playwright not available, using plain session")
        return _make_session()

    log.info("[tvtv-cache] bootstrapping Cloudflare session via Playwright...")
    cookies = []
    user_agent = None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                ctx = browser.new_context()
                page = ctx.new_page()
                try:
                    page.goto(f"{_TVTV_BASE}/", wait_until="domcontentloaded", timeout=30_000)
                except Exception:
                    pass
                # Wait up to 15s for Cloudflare to issue cf_clearance
                for _ in range(15):
                    all_cookies = ctx.cookies(_TVTV_BASE)
                    if any(c["name"] == "cf_clearance" for c in all_cookies):
                        break
                    page.wait_for_timeout(1_000)
                cookies = ctx.cookies(_TVTV_BASE)
                user_agent = page.evaluate("navigator.userAgent")
            finally:
                browser.close()
    except Exception as exc:
        log.warning("[tvtv-cache] Playwright bootstrap failed: %s — using plain session", exc)
        return _make_session()

    cf = next((c for c in cookies if c["name"] == "cf_clearance"), None)
    if cf:
        log.info("[tvtv-cache] cf_clearance obtained")
    else:
        log.warning("[tvtv-cache] cf_clearance not found after Playwright bootstrap — Cloudflare challenge may not have completed")

    if _CURL_CFFI:
        s = _http.Session(impersonate="chrome120")
    else:
        s = _http.Session()

    s.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Referer": f"{_TVTV_BASE}/",
        "Origin": _TVTV_BASE,
        "User-Agent": user_agent or "",
    })
    # Inject all tvtv.us cookies (cf_clearance, __cf_bm, etc.)
    cookie_header = "; ".join(
        f"{c['name']}={c['value']}"
        for c in cookies
        if "tvtv" in c.get("domain", "")
    )
    if cookie_header:
        s.headers["Cookie"] = cookie_header

    return s


# ---------------------------------------------------------------------------
# Core fetch
# ---------------------------------------------------------------------------

def _fetch_batch(session, lineup: str, station_ids: list[str],
                 start: datetime, end: datetime) -> dict[str, list[dict]]:
    """
    Fetch one batch of station IDs for a lineup-day window.
    Returns {station_id: [airing, ...]} for stations that had data.
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
        log.warning("[tvtv-cache] batch failed %s %s...: %s", lineup, station_ids[:3], exc)
        return {}

    result: dict[str, list[dict]] = {}
    for i, sid in enumerate(station_ids):
        if i < len(grid) and isinstance(grid[i], list):
            result[sid] = grid[i]
    return result


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

    # Bootstrap one Cloudflare-cleared session for the entire run.
    session = _get_cf_session() if not dry_run else None

    for lineup, station_ids in lineup_stations.items():
        for day_offset in range(days):
            start, end = _grid_window(day_offset, now_utc)
            batches = [
                station_ids[i: i + _BATCH_SIZE]
                for i in range(0, len(station_ids), _BATCH_SIZE)
            ]

            log.info("[tvtv-cache] %s day+%d: %d stations in %d batches",
                     lineup, day_offset, len(station_ids), len(batches))

            for batch in batches:
                if dry_run:
                    total_batches += 1
                    continue

                results = _fetch_batch(session, lineup, batch, start, end)
                if not results:
                    total_errors += 1
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
                total_batches += 1
                time.sleep(_BATCH_DELAY)

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

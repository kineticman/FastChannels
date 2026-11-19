"""
tvtv_cache.py

tvtv.us provider for the guide cache (app/guide_cache.py owns the tables,
the provider order and every read). Gracenote's listings site is tried first;
this module fetches whatever it didn't store.

tvtv retired their JSON grid API (/api/v1/lineup/.../grid/...) in a site
redesign — it 404s unconditionally now, for every lineup — so this fetches
per-station via tvtv's htmx fragment endpoint instead, with bounded
concurrency (see _fetch_batch_via_fragments). Uses curl_cffi for a
browser-like HTTP session without a headless browser bootstrap.

program_details() reads tvtv's program dialog (/dlg/program?id=<TMS id>) for
guide_cache.program_details().

Standalone tvtv-only dry run (prints stats, writes nothing):
    docker exec fastchannels python -m app.tvtv_cache --dry-run
"""

from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.exc import OperationalError

log = logging.getLogger(__name__)

_BATCH_SIZE  = 20    # station IDs fetched (threaded) per pacing chunk
_BATCH_DELAY = 1.0   # seconds between pacing chunks within a day
_FRAGMENT_WORKERS = 4  # concurrent per-station htmx fetches per chunk
_FRAGMENT_DELAY = 0.0  # optional pace between per-station fragment calls
_DAYS        = 2     # days of guide data to cache (today + 1)
_PROGRESS_LOG_EVERY = 10  # log a progress line every N batches within a day
# tvtv refusing us (see refresh_tvtv_cache): after _BLOCK_AFTER failed batches
# in a row pause _BLOCK_PAUSE seconds; _BLOCK_GRACE more failures after that
# end the pass. Failed batches get one retry pass _RETRY_COOLDOWN seconds later.
_BLOCK_AFTER    = 3
_BLOCK_PAUSE    = 60
_BLOCK_GRACE    = 2
_RETRY_COOLDOWN = 300

_TVTV_BASE = "https://tvtv.us"
_FRAGMENT_THREAD_STATE = threading.local()
# Why fragment fetches failed this pass ("HTTP 429", "Timeout", ...), for the log.
_FAILURES: dict[str, int] = {}
_FAILURES_LOCK = threading.Lock()


def _note_failure(exc: Exception) -> None:
    status = getattr(getattr(exc, "response", None), "status_code", None)
    key = f"HTTP {status}" if status else type(exc).__name__
    with _FAILURES_LOCK:
        _FAILURES[key] = _FAILURES.get(key, 0) + 1


def _failure_summary() -> str:
    with _FAILURES_LOCK:
        return ", ".join(f"{k}={v}" for k, v in sorted(_FAILURES.items(), key=lambda kv: -kv[1])) or "no detail"

# Lineup label stored on cache rows for stations not present in the bundled
# station_index.json (display/record-keeping only — the fragment fetch
# itself is keyed by station_id alone and needs no lineup).
_UNINDEXED_LINEUP = "UNINDEXED"


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
        # The airing is either `Title<span class=gridSubtitle>…</span>` or the
        # same wrapped in a <div>; take the title from whichever holds it,
        # skipping the subtitle span so it isn't glued onto the title.
        title_el = airing.find("div", recursive=False) or airing
        title_parts: list[str] = []
        for child in title_el.children:
            if isinstance(child, NavigableString):
                text = str(child).strip()
                if text:
                    title_parts.append(text)
            elif getattr(child, "name", None) != "span":
                text = child.get_text(" ", strip=True)
                if text:
                    title_parts.append(text)
        title = " ".join(title_parts).strip()
        if subtitle and subtitle == title:
            subtitle = None

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
                # 400/404 is "no such station" (unknown id), not a refusal:
                # an empty result, so it doesn't feed the block/retry logic.
                if r.status_code in (400, 404):
                    return []
                r.raise_for_status()
            except Exception as exc:
                log.debug("[tvtv-cache] fragment failed %s day=%s: %s", station_id, day_key, exc)
                _note_failure(exc)
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
    """
    Fetch a chunk of station IDs via tvtv's htmx fragment endpoint (their
    JSON grid API was retired in a site redesign and now 404s
    unconditionally), with bounded per-station concurrency.
    Returns ({station_id: [airing, ...]}, failure_reason).
    """
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
    return {}, "fragment fetch failed" if failures else "empty fragment response"


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------

def _delete_expired(now_utc: datetime) -> int:
    """Remove entries whose end_time is more than 1 hour in the past."""
    from .db_batches import delete_in_batches
    from .models import TvtvProgramCache
    cutoff = now_utc - timedelta(hours=1)
    return delete_in_batches(TvtvProgramCache, TvtvProgramCache.end_time < cutoff)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def refresh_tvtv_cache(days: int = _DAYS, dry_run: bool = False,
                       station_ids: list[str] | None = None, prune: bool = True) -> dict[str, Any]:
    """
    Fetch `days` days of guide data for the given station IDs (or all mapped
    gracenote_ids from the Channel table if station_ids is None) and store
    the results in tvtv_program_cache.

    Returns a summary dict:
        {stations_fetched, days, batches, rows_inserted, rows_deleted, errors, elapsed_s}
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
    all_station_ids = sorted(station_set)

    # Lineup label per station, for the DB row only (display/record-keeping —
    # the fragment fetch itself doesn't need it). Falls back to
    # _UNINDEXED_LINEUP for stations outside the bundled index.
    lineup_by_station = {
        sid: ((index.get(sid, {}).get("lineups") or [None])[0] or _UNINDEXED_LINEUP)
        for sid in all_station_ids
    }

    total_batches = 0
    total_rows    = 0
    total_errors  = 0

    # Reuse one warmed session for the entire refresh run.
    session = _get_session() if not dry_run else None

    # Every (day, batch) to fetch, in order. tvtv starts refusing an IP after a
    # burst of requests: on 2026-11-16..18 prod and test (one public IP, both at
    # 03:00) lost every batch from about a minute in. So a run of failed
    # batches pauses, then stops the pass, and what failed is tried again
    # once after a cooldown instead of being hammered through.
    work = []
    for day_offset in range(days):
        start, end = _grid_window(day_offset, now_utc)
        batches = [all_station_ids[i: i + _BATCH_SIZE]
                   for i in range(0, len(all_station_ids), _BATCH_SIZE)]
        log.info("[tvtv-cache] day+%d: %d stations in %d batches",
                 day_offset, len(all_station_ids), len(batches))
        work.extend((day_offset, start, end, batch) for batch in batches)

    if dry_run:
        total_batches = len(work)
        work = []
    _FAILURES.clear()

    def _store(day_offset: int, results: dict[str, list[dict]]) -> bool:
        nonlocal total_rows
        from .guide_cache import store_station, tvtv_item_to_airing
        start, end = _grid_window(day_offset, now_utc)
        try:
            for sid, items in results.items():
                airings = [a for a in (tvtv_item_to_airing(i) for i in items) if a]
                total_rows += store_station(sid, airings, "tvtv", start, end + timedelta(minutes=1),
                                            lineup=lineup_by_station[sid], now=fetched_at)
            db.session.commit()
            return True
        except OperationalError as exc:
            db.session.rollback()
            if "locked" not in str(exc).lower():
                raise
            # A concurrent scrape (separate worker process, separate
            # connection) can hold SQLite's single writer lock past our
            # busy_timeout. Skip this batch and keep going rather than
            # letting it abort the whole run.
            log.warning("[tvtv-cache] day+%d: batch write hit 'database is locked', skipping batch",
                        day_offset)
            return False

    def _run_pass(items: list, label: str) -> tuple[list, int]:
        """Fetch and store items; returns (items whose fetch failed, batches
        lost to a locked database)."""
        nonlocal total_batches
        failed: list = []
        locked = 0
        consecutive = 0
        for n, item in enumerate(items, start=1):
            day_offset, start, end, batch = item
            results, _reason = _fetch_batch_via_fragments(session, batch, start, end)
            if not results:
                failed.append(item)
                consecutive += 1
                if consecutive == _BLOCK_AFTER:
                    log.warning("[tvtv-cache] %s: %d batches in a row failed (%s); pausing %ds",
                                label, consecutive, _failure_summary(), _BLOCK_PAUSE)
                    time.sleep(_BLOCK_PAUSE)
                elif consecutive >= _BLOCK_AFTER + _BLOCK_GRACE:
                    log.warning("[tvtv-cache] %s: tvtv still refusing after the pause; "
                                "leaving %d batches for later", label, len(items) - n)
                    failed.extend(items[n:])
                    break
                else:
                    time.sleep(_BATCH_DELAY)
                continue
            consecutive = 0
            if _store(day_offset, results):
                total_batches += 1
            else:
                locked += 1
            if n % _PROGRESS_LOG_EVERY == 0 or n == len(items):
                log.info("[tvtv-cache] %s: %d/%d batches (%d rows, %d failed so far)",
                         label, n, len(items), total_rows, len(failed))
            time.sleep(_BATCH_DELAY)
        return failed, locked

    failed, locked = _run_pass(work, "pass 1") if work else ([], 0)
    if failed:
        log.warning("[tvtv-cache] pass 1: %d/%d batches failed (%s); retrying them in %ds",
                    len(failed), len(work), _failure_summary(), _RETRY_COOLDOWN)
        _FAILURES.clear()
        time.sleep(_RETRY_COOLDOWN)
        failed, locked2 = _run_pass(failed, "retry")
        locked += locked2
        if failed:
            log.warning("[tvtv-cache] retry: %d batches still failed (%s)", len(failed), _failure_summary())
    total_errors = len(failed) + locked

    if not dry_run and prune:
        deleted = _delete_expired(now_utc)
        db.session.commit()
    else:
        deleted = 0

    elapsed = round(time.monotonic() - t0, 1)
    summary = {
        "stations_fetched": len(all_station_ids),
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
# Program details by TMS id (tvtv's program dialog)
# ---------------------------------------------------------------------------

class TvtvRateLimited(Exception):
    pass


def program_details(session, program_id: str) -> dict:
    """Episode details from tvtv's program dialog, in guide_cache field names
    (title, episode_title, season, episode, rating, description). {} when tvtv
    has nothing for the id; raises TvtvRateLimited on HTTP 429."""
    from bs4 import BeautifulSoup
    r = session.get(f"{_TVTV_BASE}/dlg/program", params={"id": program_id}, timeout=20,
                    headers={"HX-Request": "true", "Accept": "text/html, */*"})
    if r.status_code == 429:
        raise TvtvRateLimited()
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    panel = soup.find(id="main-panel")
    if not panel:
        return {}
    out: dict = {}
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        out["title"] = h1.get_text(" ", strip=True)
    h2 = panel.find("h2", recursive=False)
    if h2 and h2.get_text(strip=True) and not re.fullmatch(r"\d{4}", h2.get_text(strip=True)):
        out["episode_title"] = h2.get_text(" ", strip=True)   # a movie's h2 is its year
    paras = [x for x in panel.find_all("p", recursive=False) if not x.attrs]
    for x in paras:
        m = re.match(r"Season (\d+); Episode (\d+)", x.get_text(" ", strip=True))
        if m:
            out["season"], out["episode"] = int(m.group(1)), int(m.group(2))
            break
    label = panel.find(string=re.compile("Parental Rating"))
    rating_el = label.parent.find_previous_sibling("div") if label else None
    if rating_el and rating_el.get_text(strip=True):
        out["rating"] = rating_el.get_text(strip=True)
    desc = paras[-1].get_text(" ", strip=True) if paras else ""
    if desc and not re.match(r"(Season \d|Director|Writer)", desc) and desc != out.get("episode_title"):
        out["description"] = desc
    return out


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

"""
guide_cache.py

Provider-neutral cache of Gracenote station schedules and TMS program details,
kept only for the stations FastChannels actually reads guide data from.

Demand-driven: the stations kept are those in guide_stations: pinned ones
from app/data/guide_stations.json (maintained during scraper development),
plus any a reader needed in the last _NEED_WINDOW_DAYS. Readers (borrowed
guides and hole filling via station_programs(), Paramount+ filler via
airings(need=...)) note a station in Redis, lock-free mid-scrape; each
refresh writes those notes into guide_stations. One nightly refresh
(worker.run_tvtv_cache_refresh → refresh()) fetches the needed stations whose
stored airings reach less than FRESH_DAYS ahead: about a hundred requests,
far under the providers' rate limits. Stations nothing reads (most applied
Gracenote ids, the community map) are never fetched in bulk; the Gracenote
Suggestions modal's now-playing for those is a live fetch (live_now_next()).

Tables (everything else reads them through this module):
  tvtv_program_cache  airings: station, start/end, TMS program id, title,
                      episode title, source. (Historical table name.)
  tms_programs        per TMS id: episode title, season/episode, description,
                      rating, release year, series id, image. Merged across
                      providers: an empty field never blanks a stored one.

Providers, tried per station in order (env GUIDE_PROVIDERS, default
"gracenote,tvtv"):
  gracenote  tvlistings.gracenote.com /api/sslgrid (app/gracenote_listings.py):
             14 days per request, details inline. Rate-limited in short
             windows (~400 requests per few minutes drew HTTP 429 on
             2026-11-19); a 429 backs Gracenote off for every run (Redis key).
  tvtv       tvtv.us htmx fragments (app/tvtv_cache.py): a day per request,
             title + episode title only; its /dlg/program dialog fills
             details by TMS id (program_details()).

Read API: airings(), station_programs(), to_program_data(), now_next(),
live_now_next(), program_details(), movie_years().

Standalone:  docker exec fastchannels python -m app.guide_cache --needed
             docker exec fastchannels python -m app.guide_cache --dry-run
             docker exec fastchannels python -m app.guide_cache --station 121813
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

log = logging.getLogger(__name__)

STORE_DAYS = 3           # airings kept ahead of now (Gracenote returns 14)
FRESH_DAYS = 2.5         # a needed station is due once its airings reach less than this far (~nightly)
_REFETCH_GAP_HOURS = 20  # ...and its last fetch attempt is at least this old
_NEED_WINDOW_DAYS = 7    # an unpinned station counts as needed if read within this window
_FORGET_DAYS = 30        # unpinned guide_stations rows unread this long are deleted
_PINNED_PATH = os.path.join(os.path.dirname(__file__), "data", "guide_stations.json")
_PROGRAM_TTL_DAYS = 120  # tms_programs rows unseen this long are pruned
_DETAIL_RETRY_DAYS = 7   # a by-id detail miss isn't retried sooner
_LIVE_TTL = 300          # live now-playing memo (seconds)
_UNINDEXED_LINEUP = "UNINDEXED"

# Gracenote: one request at a time, _GN_PACE s apart; a 429 backs it off for
# max(Retry-After, _GN_BACKOFF) seconds across runs; _GN_STOP_AFTER other
# errors in a row hand the rest of a run to the next provider.
_GN_PACE = 2.0
_GN_BACKOFF = 1800
_GN_STOP_AFTER = 5

# Redis keys (no persistence: lost on restart, rebuilt as scrapes read).
_DEMAND_KEY = "guide_cache:demand"          # zset station_id -> last needed (epoch), until flushed
_DEMAND_SOURCE_KEY = "guide_cache:demand_source"   # hash station_id -> reading source
_FETCHED_KEY = "guide_cache:fetched"        # hash station_id -> last fetch attempt (epoch)
_GN_BACKOFF_KEY = "guide_cache:gracenote_backoff"
_NOTE_EVERY = 6 * 3600                      # per process: re-note a station at most this often

_PROGRAM_COLS = ("title", "episode_title", "season", "episode", "description",
                 "rating", "release_year", "series_id", "image")
# Gracenote movie airings carry "(1961, Western)" as their tvtv subtitle.
_MOVIE_SUBTITLE = re.compile(r"^\((\d{4})\b[^)]*\)$")


def providers() -> list[str]:
    raw = os.environ.get("GUIDE_PROVIDERS", "gracenote,tvtv")
    order = [p.strip().lower() for p in raw.split(",")]
    order = [p for p in order if p in ("gracenote", "tvtv")]
    return list(dict.fromkeys(order)) or ["gracenote", "tvtv"]


def _redis():
    """The app's Redis, or None outside an app context / when unavailable."""
    try:
        import redis
        from flask import current_app
        return redis.from_url(current_app.config["REDIS_URL"])
    except Exception:
        return None


_noted: dict[str, float] = {}


def note_needed(station_ids: Iterable[str], source: str | None = None) -> None:
    """Record that something reads these stations' guide data, so refresh()
    keeps them. Lock-free (Redis; written to guide_stations by the next
    refresh), throttled per process."""
    mono = time.monotonic()
    ids = [s for s in {str(x) for x in station_ids if x} if mono - _noted.get(s, -1e12) > _NOTE_EVERY]
    if not ids:
        return
    r = _redis()
    if r is None:
        return
    try:
        r.zadd(_DEMAND_KEY, {s: time.time() for s in ids})
        if source:
            r.hset(_DEMAND_SOURCE_KEY, mapping={s: source for s in ids})
        for s in ids:
            _noted[s] = mono
    except Exception:
        log.debug("[guide-cache] could not record demand", exc_info=True)


def _flush_demand() -> int:
    """Write Redis demand notes into guide_stations (caller commits)."""
    from sqlalchemy import func
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
    from .extensions import db
    from .models import GuideStation
    r = _redis()
    if r is None:
        return 0
    try:
        notes = r.zrange(_DEMAND_KEY, 0, -1, withscores=True)
        sources = r.hgetall(_DEMAND_SOURCE_KEY)
    except Exception:
        log.debug("[guide-cache] could not read demand", exc_info=True)
        return 0
    dec = lambda v: v.decode() if isinstance(v, bytes) else str(v)
    sources = {dec(k): dec(v) for k, v in sources.items()}
    rows = []
    for member, score in notes:
        sid = dec(member)
        ts = datetime.fromtimestamp(score, tz=timezone.utc)
        rows.append({"station_id": sid, "source": sources.get(sid), "pinned": False,
                     "first_needed": ts, "last_needed": ts})
    if not rows:
        return 0
    table = GuideStation.__table__
    stmt = sqlite_insert(table)
    stmt = stmt.on_conflict_do_update(index_elements=["station_id"], set_={
        "last_needed": func.max(func.coalesce(table.c.last_needed, stmt.excluded.last_needed),
                                stmt.excluded.last_needed),
        "source": func.coalesce(table.c.source, stmt.excluded.source),
    })
    db.session.execute(stmt, rows)
    try:
        r.zrem(_DEMAND_KEY, *[row["station_id"] for row in rows])
        r.hdel(_DEMAND_SOURCE_KEY, *[row["station_id"] for row in rows])
    except Exception:
        pass
    return len(rows)


def _load_pinned() -> list[dict]:
    import json
    try:
        with open(_PINNED_PATH) as f:
            return [s for s in json.load(f).get("stations", []) if s.get("station_id")]
    except FileNotFoundError:
        return []
    except Exception:
        log.warning("[guide-cache] could not read %s", _PINNED_PATH, exc_info=True)
        return []


def _sync_pinned() -> int:
    """Make guide_stations' pinned rows match guide_stations.json (caller commits)."""
    import sqlalchemy as sa
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
    from .extensions import db
    from .models import GuideStation
    pinned = _load_pinned()
    ids = {str(s["station_id"]) for s in pinned}
    GuideStation.query.filter(GuideStation.pinned.is_(True),
                              GuideStation.station_id.notin_(ids) if ids else sa.true()
                              ).update({"pinned": False}, synchronize_session=False)
    if pinned:
        stmt = sqlite_insert(GuideStation.__table__)
        stmt = stmt.on_conflict_do_update(index_elements=["station_id"], set_={
            "pinned": True, "source": stmt.excluded.source, "name": stmt.excluded.name})
        db.session.execute(stmt, [{"station_id": str(s["station_id"]), "source": s.get("source"),
                                   "name": s.get("name"), "pinned": True} for s in pinned])
    return len(pinned)


def needed_stations(now: datetime | None = None, update: bool = True) -> list[str]:
    """Stations to keep cached: pinned, or read within _NEED_WINDOW_DAYS.
    update=True first writes pending demand notes and the pinned list into
    guide_stations and forgets long-unread rows (commits)."""
    from .extensions import db
    from .models import GuideStation
    now = now or _now()
    if update:
        _sync_pinned()
        _flush_demand()
        GuideStation.query.filter(GuideStation.pinned.is_(False),
                                  GuideStation.last_needed < now - timedelta(days=_FORGET_DAYS)
                                  ).delete(synchronize_session=False)
        _commit_or_skip("guide_stations")
    cutoff = now - timedelta(days=_NEED_WINDOW_DAYS)
    ids = {sid for (sid,) in db.session.query(GuideStation.station_id).filter(
        (GuideStation.pinned.is_(True)) | (GuideStation.last_needed >= cutoff)).all()}
    if not update:   # dry run: include notes not flushed yet
        r = _redis()
        try:
            ids |= {m.decode() if isinstance(m, bytes) else str(m)
                    for m in (r.zrange(_DEMAND_KEY, 0, -1) if r is not None else [])}
        except Exception:
            pass
    return sorted(ids)


def _gracenote_backed_off() -> bool:
    r = _redis()
    try:
        return bool(r is not None and r.exists(_GN_BACKOFF_KEY))
    except Exception:
        return False


def _back_off_gracenote(seconds: int) -> None:
    r = _redis()
    try:
        if r is not None:
            r.set(_GN_BACKOFF_KEY, "1", ex=max(60, int(seconds)))
    except Exception:
        pass


def _utc(dt: datetime) -> datetime:
    # SQLite hands back naive UTC.
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _lineup_for(station_id: str, index: dict | None = None) -> str:
    if index is None:
        from .tvtv_lookup import _load_index
        index = _load_index()
    return ((index.get(station_id) or {}).get("lineups") or [None])[0] or _UNINDEXED_LINEUP


def _movie_year(program_id: str | None, subtitle: str | None) -> int | None:
    if program_id and not program_id.startswith("MV"):
        return None
    m = _MOVIE_SUBTITLE.match(subtitle or "")
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# tvtv items → provider-neutral airings
# ---------------------------------------------------------------------------

def tvtv_item_to_airing(item: dict) -> dict | None:
    """A tvtv fragment item {programId,title,subtitle,startTime,duration} →
    airing dict. A movie's "(year, genre)" subtitle becomes release_year."""
    from .tvtv_cache import _parse_start
    start = _parse_start(item)
    if not start:
        return None
    end = start + timedelta(minutes=int(item.get("duration") or 0))
    title = (item.get("title") or item.get("programTitle") or "").strip()
    if not title or end <= start:
        return None
    pid = item.get("programId") or None
    subtitle = (item.get("subtitle") or "").strip() or None
    year = _movie_year(pid, subtitle)
    return {
        "program_id": pid, "title": title,
        "episode_title": None if year else (subtitle if subtitle != title else None),
        "start": start, "end": end,
        "season": None, "episode": None, "description": None, "rating": None,
        "release_year": year, "series_id": None,
        "program_type": "movie" if (pid or "").startswith("MV") else None,
        "image": None,
    }


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def upsert_programs(rows: Iterable[dict], source: str, now: datetime | None = None,
                    checked: bool = False) -> int:
    """Merge program details into tms_programs (caller commits). A None field
    keeps the stored value; checked=True stamps detail_checked_at."""
    from sqlalchemy import func
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
    from .extensions import db
    from .models import TmsProgram
    now = now or _now()
    values = []
    for r in rows:
        if not r.get("program_id"):
            continue
        v = {c: (r.get(c) if r.get(c) not in ("", None) else None) for c in _PROGRAM_COLS}
        v.update(program_id=r["program_id"], source=source, last_seen=now,
                 detail_checked_at=now if checked else None)
        values.append(v)
    if not values:
        return 0
    table = TmsProgram.__table__
    stmt = sqlite_insert(table)
    set_ = {c: func.coalesce(stmt.excluded[c], table.c[c]) for c in _PROGRAM_COLS}
    set_["source"] = func.coalesce(stmt.excluded.source, table.c.source)
    set_["last_seen"] = stmt.excluded.last_seen
    set_["detail_checked_at"] = func.coalesce(stmt.excluded.detail_checked_at, table.c.detail_checked_at)
    for i in range(0, len(values), 500):
        db.session.execute(stmt.on_conflict_do_update(index_elements=["program_id"], set_=set_),
                           values[i:i + 500])
    return len(values)


def store_station(station_id: str, airings: list[dict], source: str,
                  window_start: datetime, window_end: datetime,
                  horizon: datetime | None = None, lineup: str | None = None,
                  now: datetime | None = None) -> int:
    """Replace a station's airings starting in [window_start, min(window_end,
    horizon)) with `airings`, and merge their program details. Airings ending
    over an hour ago or starting after the horizon (default now+STORE_DAYS)
    are dropped. Caller commits. Returns airings written."""
    import sqlalchemy as sa
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
    from .extensions import db
    from .models import TvtvProgramCache
    now = now or _now()
    horizon = horizon or now + timedelta(days=STORE_DAYS)
    lineup = lineup or _lineup_for(station_id)
    keep, seen = [], set()
    for a in sorted(airings, key=lambda a: a["start"]):
        if a["end"] <= now - timedelta(hours=1) or a["start"] >= horizon or a["start"] in seen:
            continue
        seen.add(a["start"])
        keep.append(a)
    cut = min(_utc(window_end), horizon)
    db.session.execute(sa.delete(TvtvProgramCache).where(
        TvtvProgramCache.station_id == station_id,
        TvtvProgramCache.start_time >= _utc(window_start),
        TvtvProgramCache.start_time < cut,
    ))
    if keep:
        stmt = sqlite_insert(TvtvProgramCache.__table__)
        stmt = stmt.on_conflict_do_update(
            index_elements=["station_id", "start_time"],
            set_={c: stmt.excluded[c] for c in
                  ("lineup", "program_id", "title", "subtitle", "end_time", "fetched_at", "source")},
        )
        db.session.execute(stmt, [{
            "station_id": station_id, "lineup": lineup, "program_id": a.get("program_id"),
            "title": a["title"][:512], "subtitle": (a.get("episode_title") or None),
            "start_time": a["start"], "end_time": a["end"], "fetched_at": now, "source": source,
        } for a in keep])
    progs: dict[str, dict] = {}
    for a in keep:
        pid = a.get("program_id")
        if not pid:
            continue
        p = progs.setdefault(pid, {"program_id": pid})
        for c in _PROGRAM_COLS:
            if p.get(c) in (None, "") and a.get(c) not in (None, ""):
                p[c] = a[c]
    upsert_programs(progs.values(), source, now)
    return len(keep)


def prune(now: datetime | None = None) -> dict[str, int]:
    """Drop ended airings, airings past the storage horizon, and long-unseen
    program details. Caller commits."""
    from .db_batches import delete_in_batches, yield_writer_lock
    from .extensions import db
    from .models import TmsProgram, TvtvProgramCache
    now = now or _now()
    airings_deleted = delete_in_batches(TvtvProgramCache, TvtvProgramCache.end_time < now - timedelta(hours=1))
    airings_deleted += delete_in_batches(TvtvProgramCache,
                                         TvtvProgramCache.start_time > now + timedelta(days=STORE_DAYS + 1))
    # tms_programs is keyed by program_id, which delete_in_batches (id-based) can't page.
    stale = TmsProgram.last_seen < now - timedelta(days=_PROGRAM_TTL_DAYS)
    programs_deleted = 0
    while True:
        ids = [r[0] for r in db.session.query(TmsProgram.program_id).filter(stale).limit(2000).all()]
        if not ids:
            break
        programs_deleted += TmsProgram.query.filter(TmsProgram.program_id.in_(ids)).delete(
            synchronize_session=False)
        db.session.commit()
        yield_writer_lock()
    return {"airings": airings_deleted, "programs": programs_deleted}


# ---------------------------------------------------------------------------
# Provider fetches (single station)
# ---------------------------------------------------------------------------

def _fetch_gracenote(station_id: str, session=None) -> list[dict]:
    from . import gracenote_listings as gl
    return gl.fetch_station(session or gl.make_session(), station_id)


def _fetch_tvtv(station_id: str, start: datetime, end: datetime, session=None) -> list[dict] | None:
    from . import tvtv_cache
    items = tvtv_cache._fetch_fragment_station(session or tvtv_cache._make_session(), station_id, start, end)
    if items is None:
        return None
    return [a for a in (tvtv_item_to_airing(i) for i in items) if a]


def fetch_live(station_id: str, until: datetime | None = None) -> tuple[list[dict], str | None]:
    """One station from the first provider that has it: (airings, provider)."""
    now = _now()
    until = until or now + timedelta(days=1)
    for p in providers():
        try:
            if p == "gracenote":
                airings = _fetch_gracenote(station_id)
            else:
                airings = _fetch_tvtv(station_id, now - timedelta(hours=1), until)
        except Exception as exc:
            log.info("[guide-cache] live %s fetch for %s failed: %s", p, station_id, exc)
            continue
        if airings:
            return airings, p
    return [], None


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def _row_to_airing(a, p) -> dict:
    pid = a.program_id
    subtitle = a.subtitle
    year = (p.release_year if p else None) or _movie_year(pid, subtitle)
    if _movie_year(pid, subtitle):
        subtitle = None   # legacy tvtv movie row: "(2025, Drama)" isn't an episode title
    return {
        "program_id": pid,
        "title": a.title,
        "episode_title": subtitle or (p.episode_title if p else None),
        "start": _utc(a.start_time),
        "end": _utc(a.end_time),
        "season": p.season if p else None,
        "episode": p.episode if p else None,
        "description": p.description if p else None,
        "rating": p.rating if p else None,
        "release_year": year,
        "series_id": p.series_id if p else None,
        "image": p.image if p else None,
        "program_type": "movie" if (pid or "").startswith("MV") else None,
        "source": a.source,
    }


def airings(station_id: str, since: datetime | None = None, until: datetime | None = None,
            fetch_missing: bool = False, need: bool | str = False) -> list[dict]:
    """A station's cached airings (with program details) ending after `since`
    (default an hour ago) and starting before `until`, oldest first.
    need=True (or the reading source's name) records the station as needed,
    so refresh() keeps it cached.
    fetch_missing=True tops up a cache that stops short of `until` with a live
    provider fetch (returned, not stored: the scheduled refresh stores it, and
    this never writes from inside a scrape's session)."""
    from .models import TmsProgram, TvtvProgramCache
    if not station_id:
        return []
    station_id = str(station_id)
    if need:
        note_needed([station_id], need if isinstance(need, str) else None)
    now = _now()
    since = since or now - timedelta(hours=1)
    try:
        q = (TvtvProgramCache.query
             .outerjoin(TmsProgram, TmsProgram.program_id == TvtvProgramCache.program_id)
             .add_entity(TmsProgram)
             .filter(TvtvProgramCache.station_id == station_id,
                     TvtvProgramCache.end_time > since))
        if until is not None:
            q = q.filter(TvtvProgramCache.start_time < until)
        out = [_row_to_airing(a, p) for a, p in q.order_by(TvtvProgramCache.start_time).all()]
    except Exception:
        log.debug("[guide-cache] cache unavailable for %s", station_id, exc_info=True)
        return []
    covered = max((a["end"] for a in out), default=since)
    if not fetch_missing or until is None or covered >= until - timedelta(hours=1):
        return out
    fetched, provider = fetch_live(station_id, until)
    extra = [dict(a, source=provider) for a in fetched if covered <= a["start"] < until]
    return out + extra


def to_program_data(scid: str, a: dict):
    """An airing dict → ProgramData for a scraper's guide."""
    from .scrapers.base import ProgramData
    return ProgramData(
        source_channel_id=scid, title=a["title"], start_time=a["start"], end_time=a["end"],
        description=a.get("description"), poster_url=a.get("image"), rating=a.get("rating"),
        episode_title=a.get("episode_title"), season=a.get("season"), episode=a.get("episode"),
        program_type=a.get("program_type"), series_id=a.get("series_id"),
        episode_id=a.get("program_id"),
    )


def station_programs(scid: str, station_id: str, source: str | None = None) -> list:
    """A Gracenote station's guide as ProgramData for channel `scid`: what
    scrapers borrow for channels with no guide of their own (or holes in it).
    Marks the station needed; a station with nothing cached yet gets a day
    from a live fetch until the scheduled refresh caches it."""
    rows = airings(station_id, need=source or True)
    if not rows and station_id:
        rows = airings(station_id, until=_now() + timedelta(days=1), fetch_missing=True)
    return [to_program_data(scid, a) for a in rows]


def _now_next_from(rows: list[dict], now: datetime) -> tuple[dict | None, dict | None]:
    cur = next((a for a in rows if a["start"] <= now < a["end"]), None)
    after = cur["end"] if cur else now
    nxt = next((a for a in rows if a["start"] >= after and a is not cur), None)
    return cur, nxt


def _brief(a: dict | None) -> dict | None:
    if not a:
        return None
    return {"title": a["title"], "subtitle": a.get("episode_title"), "program_id": a.get("program_id"),
            "start": a["start"].isoformat(), "end": a["end"].isoformat()}


def now_next(station_id: str) -> dict[str, Any]:
    """Now/next for a station from the cache (None values when not covered)."""
    now = _now()
    rows = airings(station_id, since=now - timedelta(minutes=5), until=now + timedelta(hours=4))
    cur, nxt = _now_next_from(rows, now)
    return {"station_id": station_id, "source": "cache", "now": _brief(cur), "next": _brief(nxt)}


_live_memo: dict[str, tuple[float, list[dict]]] = {}
_live_lock = threading.Lock()


def live_now_next(station_id: str) -> dict[str, Any] | None:
    """Now/next fetched live (memoised 5 min, not stored) for a station the
    cache doesn't cover, e.g. a suggested id not mapped to any channel yet.
    None when no provider has it."""
    with _live_lock:
        hit = _live_memo.get(station_id)
    if hit and time.monotonic() - hit[0] < _LIVE_TTL:
        rows = hit[1]
    else:
        rows, provider = fetch_live(station_id)
        if not provider:
            return None
        with _live_lock:
            _live_memo[station_id] = (time.monotonic(), rows)
    cur, nxt = _now_next_from(rows, _now())
    return {"station_id": station_id, "source": "live", "now": _brief(cur), "next": _brief(nxt)}


def _program_dict(p) -> dict:
    return {"program_id": p.program_id, **{c: getattr(p, c) for c in _PROGRAM_COLS}}


def program_details(program_ids: Iterable[str], cap: int = 30, pace: float = 6.0) -> dict[str, dict]:
    """Details for TMS program ids from tms_programs. Ids with no description
    there (and not looked up in the last _DETAIL_RETRY_DAYS) get a by-id
    lookup on tvtv's program dialog: at most `cap`, `pace` seconds apart, in
    the order given, stopping at its rate limit. Returns {program_id: details}
    for every id known afterwards."""
    from .extensions import db
    from .models import TmsProgram
    ids = list(dict.fromkeys(str(i) for i in program_ids if i))
    if not ids:
        return {}

    def _load() -> dict[str, Any]:
        rows = {}
        for i in range(0, len(ids), 500):
            for p in TmsProgram.query.filter(TmsProgram.program_id.in_(ids[i:i + 500])).all():
                rows[p.program_id] = p
        return rows

    rows = _load()
    now = _now()
    retry_after = now - timedelta(days=_DETAIL_RETRY_DAYS)
    todo = [pid for pid in ids
            if not (rows.get(pid) and rows[pid].description)
            and not (rows.get(pid) and rows[pid].detail_checked_at
                     and _utc(rows[pid].detail_checked_at) > retry_after)][:cap]
    if todo and "tvtv" in providers():
        from . import tvtv_cache
        try:
            session = tvtv_cache._get_session()
        except Exception:
            log.debug("[guide-cache] tvtv session unavailable", exc_info=True)
            session = None
        found = looked = 0
        for n, pid in enumerate(todo if session else []):
            if n:
                time.sleep(pace)
            try:
                details = tvtv_cache.program_details(session, pid)
            except tvtv_cache.TvtvRateLimited:
                log.info("[guide-cache] tvtv rate-limited after %d detail lookups; rest next time", n)
                break
            except Exception as exc:
                log.debug("[guide-cache] tvtv details for %s failed: %s", pid, exc)
                continue
            looked += 1
            found += bool(details.get("description") or details.get("episode_title"))
            upsert_programs([{**details, "program_id": pid}], "tvtv", now, checked=True)
        if looked:
            db.session.commit()
            rows = _load()
            log.info("[guide-cache] tvtv program details: %d found of %d looked up", found, looked)
    return {pid: _program_dict(p) for pid, p in rows.items()}


# ---------------------------------------------------------------------------
# Movie release years (admin guide page)
# ---------------------------------------------------------------------------

_LEADING_ARTICLE = re.compile(r"^(the|a|an)\s+")
_BRACKETED = re.compile(r"\[[^\]]*\]|\([^)]*\)")
_NON_ALNUM = re.compile(r"[^0-9a-z]+")
# A FAST airing and its Gracenote row rarely start on the same second.
_MOVIE_MATCH_SLOP = 300


def _norm_title(title: str | None) -> str:
    t = _BRACKETED.sub(" ", (title or "").casefold()).strip()
    t = _LEADING_ARTICLE.sub("", t)
    return _NON_ALNUM.sub("", t)


def _titles_agree(ours: str, theirs: str) -> bool:
    if not ours or not theirs:
        return False
    if ours == theirs:
        return True
    shorter, longer = sorted((ours, theirs), key=len)
    return len(shorter) >= 6 and longer.startswith(shorter)


class MovieYearLookup:
    """Release years for movie airings on Gracenote-mapped stations. An airing
    gets a year only when the cache has a Gracenote movie (MV…) starting
    within a few minutes on the same station *and* the titles agree — FAST
    schedules drift from Gracenote's, and a time-only match would hand one
    movie another's year."""

    def __init__(self, rows):
        """rows: (station_id, title, release_year, start)."""
        self._by_station: dict[str, list[tuple[float, str, int]]] = {}
        max_year = _now().year + 1
        for station_id, title, year, start in rows:
            if not year or not 1900 <= int(year) <= max_year:
                continue
            self._by_station.setdefault(station_id, []).append(
                (_utc(start).timestamp(), _norm_title(title), int(year)))

    def __bool__(self) -> bool:
        return bool(self._by_station)

    def year_for(self, station_id: str | None, title: str | None, start: datetime) -> int | None:
        rows = self._by_station.get(station_id or "")
        if not rows:
            return None
        ours = _norm_title(title)
        t = _utc(start).timestamp()
        for row_start, theirs, year in rows:
            if abs(row_start - t) <= _MOVIE_MATCH_SLOP and _titles_agree(ours, theirs):
                return year
        return None


def movie_years(station_ids, start: datetime, end: datetime) -> MovieYearLookup:
    """Cached Gracenote movie airings for `station_ids` in [start, end)."""
    from .models import TmsProgram, TvtvProgramCache as A
    ids = sorted({str(s) for s in station_ids if s})
    rows = []
    for i in range(0, len(ids), 500):
        q = (A.query
             .outerjoin(TmsProgram, TmsProgram.program_id == A.program_id)
             .with_entities(A.station_id, A.title, A.subtitle, A.program_id,
                            TmsProgram.release_year, A.start_time)
             .filter(A.station_id.in_(ids[i:i + 500]), A.program_id.like("MV%"),
                     A.end_time > start, A.start_time < end))
        for sid, title, subtitle, pid, year, st in q.all():
            rows.append((sid, title, year or _movie_year(pid, subtitle), st))
    return MovieYearLookup(rows)


# ---------------------------------------------------------------------------
# Nightly refresh
# ---------------------------------------------------------------------------

def _coverage(station_ids: list[str]) -> dict[str, datetime]:
    import sqlalchemy as sa
    from .extensions import db
    from .models import TvtvProgramCache as A
    out = {}
    for i in range(0, len(station_ids), 500):
        for sid, end in db.session.execute(
            sa.select(A.station_id, sa.func.max(A.end_time))
            .where(A.station_id.in_(station_ids[i:i + 500]))
            .group_by(A.station_id)
        ).all():
            if end is not None:
                out[sid] = _utc(end if isinstance(end, datetime) else datetime.fromisoformat(str(end)))
    return out


def _commit_or_skip(label: str) -> bool:
    from sqlalchemy.exc import OperationalError
    from .extensions import db
    try:
        db.session.commit()
        return True
    except OperationalError as exc:
        db.session.rollback()
        if "locked" not in str(exc).lower():
            raise
        log.warning("[guide-cache] %s: write hit 'database is locked', skipped", label)
        return False


def _refresh_gracenote(station_ids: list[str], index: dict, stats: dict) -> list[str]:
    """Fetch stations from Gracenote one at a time and store them. Returns the
    ids it didn't store (empty, failed, or left after a refusal)."""
    from . import gracenote_listings as gl
    session = gl.make_session()
    leftover: list[str] = []
    batch: list[tuple[str, list[dict]]] = []
    consecutive = 0

    def _flush():
        now = _now()
        for sid, rows in batch:
            store_station(sid, rows, "gracenote",
                          window_start=min(a["start"] for a in rows),
                          window_end=max(a["end"] for a in rows),
                          lineup=_lineup_for(sid, index), now=now)
        if _commit_or_skip("gracenote batch"):
            stats["gracenote"] += len(batch)
        else:
            leftover.extend(sid for sid, _ in batch)
            stats["locked"] += len(batch)
        batch.clear()

    for n, sid in enumerate(station_ids):
        if n:
            time.sleep(_GN_PACE)
        try:
            rows = gl.fetch_station(session, sid)
        except gl.GracenoteRefused as exc:
            wait = max(_GN_BACKOFF, exc.retry_after or 0)
            log.warning("[guide-cache] gracenote refused (%s); backing off %ds, "
                        "%d stations to the next provider", exc, wait, len(station_ids) - n)
            _back_off_gracenote(wait)
            stats["gracenote_refused"] = True
            leftover.extend(station_ids[n:])
            break
        except Exception as exc:
            stats["gracenote_failed"] += 1
            leftover.append(sid)
            consecutive += 1
            log.debug("[guide-cache] gracenote error for %s: %s", sid, exc)
            if consecutive >= _GN_STOP_AFTER:
                log.warning("[guide-cache] gracenote: %d errors in a row (last: %s); "
                            "rest to the next provider", consecutive, exc)
                leftover.extend(station_ids[n + 1:])
                break
            continue
        consecutive = 0
        if rows:
            batch.append((sid, rows))
            if len(batch) >= 10:
                _flush()
        else:
            stats["gracenote_empty"] += 1
            leftover.append(sid)
    if batch:
        _flush()
    return leftover


def refresh(station_ids: list[str] | None = None, max_stations: int | None = None,
            dry_run: bool = False) -> dict[str, Any]:
    """Fetch the due stations: needed ones (or `station_ids`) whose airings
    reach less than FRESH_DAYS ahead and that weren't tried in the last
    _REFETCH_GAP_HOURS, least covered first, at most `max_stations`. Each goes
    to the providers in order; what one doesn't store falls to the next.
    Prunes expired rows at the end."""
    from .tvtv_lookup import _load_index
    t0 = time.monotonic()
    now = _now()
    explicit = station_ids is not None
    ids = sorted({str(s) for s in (station_ids if explicit else needed_stations(now, update=not dry_run))
                  if s})
    coverage = _coverage(ids)
    r = _redis()
    try:
        tried = {(k.decode() if isinstance(k, bytes) else k): float(v)
                 for k, v in (r.hgetall(_FETCHED_KEY) if r is not None else {}).items()}
    except Exception:
        tried = {}
    fresh_until = now + timedelta(days=FRESH_DAYS)
    gap = now.timestamp() - _REFETCH_GAP_HOURS * 3600
    due = [sid for sid in ids
           if (coverage.get(sid) is None or coverage[sid] < fresh_until)
           and (explicit or tried.get(sid, 0) < gap)]
    due.sort(key=lambda s: coverage.get(s) or datetime.min.replace(tzinfo=timezone.utc))
    if max_stations:
        due = due[:max_stations]
    stats = {"needed": len(ids), "due": len(due), "gracenote": 0, "gracenote_empty": 0,
             "gracenote_failed": 0, "tvtv_stations": 0, "locked": 0, "unfilled": 0,
             "providers": providers(), "dry_run": dry_run}
    if dry_run:
        stats["elapsed_s"] = round(time.monotonic() - t0, 1)
        return stats
    if not due:
        stats["pruned"] = prune(now)
        _commit_or_skip("prune")
        log.info("[guide-cache] refresh: nothing due (%d needed stations), pruned %s",
                 len(ids), stats["pruned"])
        return stats
    index = _load_index()
    remaining = due
    for p in providers():
        if not remaining:
            break
        if p == "gracenote":
            if _gracenote_backed_off():
                log.info("[guide-cache] gracenote backing off after a refusal; skipping it this run")
                continue
            remaining = _refresh_gracenote(remaining, index, stats)
        elif p == "tvtv":
            from .tvtv_cache import refresh_tvtv_cache
            summary = refresh_tvtv_cache(days=2, station_ids=remaining, prune=False)
            stats["tvtv_stations"] = len(remaining)
            stats["tvtv"] = {k: summary.get(k) for k in ("rows_inserted", "errors")}
            remaining = []
    stats["unfilled"] = len(remaining)
    try:
        if r is not None:
            r.hset(_FETCHED_KEY, mapping={sid: time.time() for sid in due})
    except Exception:
        pass
    stats["pruned"] = prune(now)
    _commit_or_skip("prune")
    stats["elapsed_s"] = round(time.monotonic() - t0, 1)
    log.info("[guide-cache] refresh complete: %s", stats)
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import json
    import sys

    ap = argparse.ArgumentParser(description="Refresh the Gracenote guide cache")
    ap.add_argument("--dry-run", action="store_true", help="Report what's due without fetching")
    ap.add_argument("--station", action="append", help="Refresh these station ids (repeatable) "
                                                       "instead of the needed set")
    ap.add_argument("--max", type=int, default=None, help="At most this many stations")
    ap.add_argument("--needed", action="store_true", help="List the stations kept cached and exit")
    args = ap.parse_args()

    os.chdir(os.path.dirname(os.path.dirname(__file__)))
    from app import create_app
    with create_app().app_context():
        if args.needed:
            from app.models import GuideStation
            keep = set(needed_stations(update=False))
            cov = _coverage(sorted(keep))
            for g in GuideStation.query.order_by(GuideStation.pinned.desc(), GuideStation.source,
                                                 GuideStation.station_id).all():
                if g.station_id in keep:
                    print(f"{g.station_id:>8}  {'pinned' if g.pinned else 'auto  '}  {g.source or '-':<24} "
                          f"{(g.name or '')[:32]:<32}  through {cov.get(g.station_id)}")
            sys.exit(0)
        print(json.dumps(refresh(args.station, max_stations=args.max, dry_run=args.dry_run),
                         indent=2, default=str))
    sys.exit(0)

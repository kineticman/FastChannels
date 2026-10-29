import copy
import fcntl
import logging
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy.exc import OperationalError as _SAOperationalError
from sqlalchemy.orm.attributes import flag_modified

from app.extensions import db
from app.models import Source, SourceCache
from app.scrapers.base import merge_config_updates

logger = logging.getLogger(__name__)

# fcntl.flock is a raw OS-level lock — unlike sockets/threading/subprocess/time.sleep,
# gevent's monkey.patch_all() does NOT make it cooperative. A plain blocking
# flock(LOCK_EX) call doesn't yield to the gevent hub, so it freezes the ENTIRE
# worker (every other concurrent request on it, not just this one) for as long as
# another holder keeps the lock. Confirmed 2026-09-12 as the likely mechanism behind
# a community-reported gunicorn WORKER TIMEOUT: this lock is held across a 3x SQLite
# retry loop (persist_source_config_updates / persist_source_cache_updates) that can
# itself burn up to the full 30s PRAGMA busy_timeout per attempt, so one call could
# legitimately hold the old blocking lock for well over a minute under contention —
# and this lock guards the play/resolve hot path, shared across virtually every
# scraper's cache/config writes for a given source.
_LOCK_ACQUIRE_TIMEOUT_S = 10
_LOCK_POLL_INTERVAL_S = 0.1

# SQLite's busy handler sleeps in C, which gevent can't switch out of: a write
# waiting on another connection's writer lock freezes the whole gunicorn worker,
# not just its own request, for up to the connection's busy_timeout (30s, set in
# app/__init__.py). Writes on the play path use a much shorter wait and, if the
# database is busy, hand the write to a background job that can wait it out
# (issue #86: a TBS play failed twice, 30s each, because a token-cache UPDATE
# couldn't get the lock). Deferred rather than dropped: some of these writes
# carry a refresh token the provider just rotated.
_DEFAULT_BUSY_TIMEOUT_MS = 30000
_SHORT_BUSY_TIMEOUT_MS = 2000
_SHORT_LOCK_ACQUIRE_TIMEOUT_S = 2


@contextmanager
def short_busy_timeout(ms: int = _SHORT_BUSY_TIMEOUT_MS):
    """Lower busy_timeout on the session's connection for the writes in this block.

    The connection goes back to the pool on commit, but it's the same sqlite3
    connection, so the restore in finally still lands on it (no greenlet switch
    can happen in between: nothing here yields)."""
    raw = db.session.connection().connection.driver_connection
    if not isinstance(raw, sqlite3.Connection):
        yield
        return
    raw.execute(f'PRAGMA busy_timeout={int(ms)}')
    try:
        yield
    finally:
        try:
            raw.execute(f'PRAGMA busy_timeout={_DEFAULT_BUSY_TIMEOUT_MS}')
        except Exception:  # noqa: BLE001
            logger.warning('[config-store] could not restore busy_timeout', exc_info=True)


@contextmanager
def _source_config_lock(source_id: int, timeout_s: float = _LOCK_ACQUIRE_TIMEOUT_S):
    """Exclusive per-source advisory lock serializing config/cache writes.

    Polls with LOCK_NB and a plain (gevent-patched) time.sleep() between attempts
    instead of blocking on the raw syscall, so a worker waiting on a busy lock keeps
    running its other greenlets meanwhile. Giving up after _LOCK_ACQUIRE_TIMEOUT_S is
    safe: every caller here is a best-effort cache/config write — persist_* already
    returns a bool for "did this actually get saved" — so skipping one just means the
    next resolve() call redoes the underlying fetch, never a correctness issue, unlike
    freezing the whole worker for a minute-plus.

    Yields whether the lock was actually acquired; callers must check it.
    """
    lock_path = Path('/tmp') / f'fastchannels-source-config-{source_id}.lock'
    lock_path.touch(exist_ok=True)
    with lock_path.open('r+') as lock_file:
        fd = lock_file.fileno()
        deadline = time.monotonic() + timeout_s
        acquired = False
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    break
                time.sleep(_LOCK_POLL_INTERVAL_S)
        try:
            yield acquired
        finally:
            if acquired:
                fcntl.flock(fd, fcntl.LOCK_UN)


def persist_source_config_updates(source_id: int, updates: dict | None, *, wait: bool = True) -> bool:
    """Safely merge scraper-generated config updates for a Source row.

    wait=False is for the play path: one attempt with a short wait, and if the
    database is busy the update goes to a background job (returns False)
    instead of holding up the request."""
    if not updates:
        return False
    lock_timeout = _LOCK_ACQUIRE_TIMEOUT_S if wait else _SHORT_LOCK_ACQUIRE_TIMEOUT_S
    with _source_config_lock(source_id, lock_timeout) as acquired:
        if not acquired:
            if not wait:
                return _defer_persist('config', source_id, updates)
            logger.warning('[config-store] could not acquire source %s config lock within %ss; '
                            'skipping this update (will retry on the next resolve)',
                            source_id, lock_timeout)
            return False
        db.session.expire_all()
        live_source = db.session.get(Source, source_id, populate_existing=True)
        if not live_source:
            return False
        updated = merge_config_updates(live_source.config, copy.deepcopy(updates))
        live_source.config = updated
        flag_modified(live_source, 'config')
        if not wait:
            return _commit_without_waiting('config', source_id, updates)
        for _attempt in range(3):
            try:
                db.session.commit()
                return True
            except _SAOperationalError:
                db.session.rollback()
                if _attempt == 2:
                    raise
                time.sleep(5 * (_attempt + 1))
        return False


def _commit_without_waiting(kind: str, source_id: int, updates: dict) -> bool:
    try:
        with short_busy_timeout():
            db.session.commit()
        return True
    except _SAOperationalError:
        db.session.rollback()
        return _defer_persist(kind, source_id, updates)


def _defer_persist(kind: str, source_id: int, updates: dict) -> bool:
    """Queue a busy play-path write for app.worker.run_deferred_persist, which
    makes it with the normal wait. Returns False: nothing is saved yet."""
    try:
        from app.routes.tasks import get_fast_queue
        get_fast_queue().enqueue('app.worker.run_deferred_persist', kind, source_id,
                                 copy.deepcopy(updates), job_timeout=300)
        logger.warning('[config-store] database busy; queued source %s %s update (%s) '
                       'to save in the background', source_id, kind, ', '.join(updates))
    except Exception:  # noqa: BLE001
        logger.error('[config-store] database busy and could not queue source %s %s update (%s); '
                     'it is lost', source_id, kind, ', '.join(updates), exc_info=True)
    return False


def load_source_cache(source_id: int) -> dict:
    """Return {cache_key: value} for every source_cache row of this source."""
    rows = (
        db.session.query(SourceCache.cache_key, SourceCache.value)
        .filter(SourceCache.source_id == source_id)
        .all()
    )
    return {key: value for key, value in rows}


def load_source_cache_by_name(source_name: str, *, keys=None, exclude=None) -> dict:
    """Like load_source_cache but keyed by Source.name — used by BaseScraper,
    which knows its source_name but not its source_id at init time.

    `keys`    — if given, load only these cache keys (a single-key fetch never
                deserializes the large cold caches, e.g. Roku's description_cache).
    `exclude` — if given, load every key EXCEPT these (used to keep large EPG-only
                caches off the play/resolve hot path)."""
    q = (
        db.session.query(SourceCache.cache_key, SourceCache.value)
        .join(Source, Source.id == SourceCache.source_id)
        .filter(Source.name == source_name)
    )
    if keys is not None:
        q = q.filter(SourceCache.cache_key.in_(list(keys)))
    if exclude:
        q = q.filter(SourceCache.cache_key.notin_(list(exclude)))
    return {key: value for key, value in q.all()}


# Caches that ACCUMULATE entries keyed by station/content id and prune only via
# TTL-on-load (never by key deletion). These are written from the play/resolve hot
# path, where two concurrent requests each hold a snapshot loaded at scraper init
# and add a different entry. A blind full-replace would let the second writer clobber
# the first's new entry (lost update). For these keys we union the incoming dict over
# the freshly-read DB row (read under the per-source lock), so concurrent additions
# both survive — restoring the recursive-merge behaviour the old config path had.
# Caches NOT listed here (description_cache, content_cache, channel_pe, osm_session,
# audit reports) prune by deleting keys, so they MUST fully replace the row value.
_MERGE_CACHE_KEYS = frozenset({
    "stream_url_cache", "play_id_cache", "selector_url_cache", "dash_cache",
    "drm_param_cache",
})


def persist_source_cache_updates(source_id: int, updates: dict | None, *, wait: bool = True) -> bool:
    """UPSERT scraper-generated cache key/values into source_cache (one row per key).

    Mirrors persist_source_config_updates: reuses the same per-source file lock so
    a source's config and cache writes serialize together, with the same 3x
    OperationalError retry. Only the rows being written are loaded (not the whole
    cache), so persisting a small cache never deserializes the large ones.

    Additive runtime caches (_MERGE_CACHE_KEYS) union the incoming dict over the
    freshly-read DB row so concurrent play-path writers don't clobber each other;
    all other keys fully replace the row value so key-deletion pruning works.

    A value of None is stored as-is; the scraper-side loaders treat it as "empty",
    which is how callers clear a cache (e.g. an expired Roku osm_session).

    wait=False behaves as in persist_source_config_updates."""
    if not updates:
        return False
    lock_timeout = _LOCK_ACQUIRE_TIMEOUT_S if wait else _SHORT_LOCK_ACQUIRE_TIMEOUT_S
    attempts = 3 if wait else 1
    with _source_config_lock(source_id, lock_timeout) as acquired:
        if not acquired:
            if not wait:
                return _defer_persist('cache', source_id, updates)
            logger.warning('[config-store] could not acquire source %s cache lock within %ss; '
                            'skipping this update (will retry on the next resolve)',
                            source_id, lock_timeout)
            return False
        for _attempt in range(attempts):
            try:
                db.session.expire_all()
                existing = {
                    row.cache_key: row
                    for row in db.session.query(SourceCache)
                    .filter(
                        SourceCache.source_id == source_id,
                        SourceCache.cache_key.in_(list(updates.keys())),
                    )
                    .all()
                }
                for key, value in updates.items():
                    row = existing.get(key)
                    if row is None:
                        db.session.add(SourceCache(
                            source_id=source_id,
                            cache_key=key,
                            value=copy.deepcopy(value),
                        ))
                    elif (
                        key in _MERGE_CACHE_KEYS
                        and isinstance(value, dict)
                        and isinstance(row.value, dict)
                    ):
                        # Union over the fresh DB value (new wins per entry); a new
                        # dict object so SQLAlchemy flags the JSON column dirty.
                        row.value = {**row.value, **copy.deepcopy(value)}
                    else:
                        row.value = copy.deepcopy(value)
                if not wait:
                    return _commit_without_waiting('cache', source_id, updates)
                db.session.commit()
                return True
            except _SAOperationalError:
                db.session.rollback()
                if _attempt == attempts - 1:
                    raise
                time.sleep(5 * (_attempt + 1))
        return False

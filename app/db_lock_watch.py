"""Log write transactions that hold SQLite's writer lock for a long time.

pysqlite (legacy isolation mode, which this engine uses) opens a transaction
implicitly at the first INSERT/UPDATE/DELETE and keeps it, with the
database-wide writer lock, until commit or rollback. Anything slow between
the two (an HTTP call, a sign-in, a long loop, autoflush issuing the write
early) blocks every other writer, and one past busy_timeout (30s) fails them
with "database is locked" (issue #86). This names the code that held it.
"""
import logging
import os
import threading
import time
import traceback

from sqlalchemy import event

logger = logging.getLogger(__name__)

_THRESHOLD_S = float(os.environ.get('FC_DB_LOCK_WARN_SECONDS', '5'))
# Other writers fail after 30s (busy_timeout), so WARNING means getting close to
# that. A first statement's time can be waiting for the lock rather than holding
# it, but a wait gives up at about 30s, so past that it must be real work.
_WARN_HOLD_S = 20.0
_WARN_FIRST_S = 31.0
_WRITE_PREFIXES = ('INSERT', 'UPDATE', 'DELETE', 'REPLACE')
_KEY = 'fc_write_began'
_PENDING_KEY = 'fc_write_pending'
_APP_DIR = os.path.dirname(os.path.abspath(__file__))


def _where() -> str:
    """The innermost few app frames, skipping this module and libraries."""
    frames = [f for f in traceback.extract_stack()[:-2]
              if f.filename.startswith(_APP_DIR) and not f.filename.endswith('db_lock_watch.py')]
    return ' < '.join(f'{os.path.relpath(f.filename, _APP_DIR)}:{f.lineno} {f.name}'
                      for f in reversed(frames[-3:])) or '?'


def _context() -> str:
    try:
        from flask import has_request_context, request
        if has_request_context():
            return f'{request.method} {request.path}'
    except Exception:  # noqa: BLE001
        pass
    try:
        from rq import get_current_job
        job = get_current_job()
        if job is not None:
            return f'job {job.func_name}'
    except Exception:  # noqa: BLE001
        pass
    return f'pid {os.getpid()}'


def install(engine) -> None:
    # Timed from before the first write, so one huge statement counts in full.
    # If it fails (it never got the lock), nothing was held.
    @event.listens_for(engine, 'before_cursor_execute')
    def _before_write(conn, cursor, statement, parameters, context, executemany):
        info = conn.connection.info
        if _KEY not in info and statement.lstrip().upper().startswith(_WRITE_PREFIXES):
            info[_PENDING_KEY] = time.monotonic()

    @event.listens_for(engine, 'after_cursor_execute')
    def _note_first_write(conn, cursor, statement, parameters, context, executemany):
        info = conn.connection.info
        t0 = info.pop(_PENDING_KEY, None)
        if t0 is not None:
            now = time.monotonic()
            info[_KEY] = (t0, now - t0, ' '.join(statement.split())[:100], _where(), _context())

    def _report(began, how, commit_s=0.0):
        t0, first_s, stmt, where, ctx = began
        held = time.monotonic() - t0
        if held < _THRESHOLD_S:
            return
        details = (f'{ctx}; first statement {first_s:.1f}s, commit {commit_s:.1f}s: began at {where} '
                   f'with {stmt!r}; {how} at {_where()}')
        if held - first_s >= _WARN_HOLD_S or first_s >= _WARN_FIRST_S:
            logger.warning('[db-lock] long write held the lock up to %.1fs; other writes may fail (%s)', held, details)
            return
        if first_s >= commit_s and first_s >= held - first_s - commit_s:
            why = 'mostly the first statement, which may just be waiting on another writer'
        elif commit_s >= held - first_s - commit_s:
            why = 'mostly the commit, usually a slow disk'
        else:
            why = 'mostly between the first write and the commit'
        logger.info('[db-lock] slow write: %.1fs, %s (%s)', held, why, details)

    def _take(info):
        info.pop(_PENDING_KEY, None)
        return info.pop(_KEY, None)

    # The engine's commit event fires before the commit runs, and on a slow disk
    # the commit (fsync) is the slow part, so the report waits for do_commit.
    # The event and do_commit run back to back with nothing to yield to another
    # greenlet, so a thread-local (greenlet-local under gevent) carries it across.
    committing = threading.local()

    @event.listens_for(engine, 'commit')
    def _on_commit(conn):
        committing.began = _take(conn.connection.info)

    do_commit = engine.dialect.do_commit

    def _timed_do_commit(dbapi_connection):
        began, committing.began = getattr(committing, 'began', None), None
        t0 = time.monotonic()
        try:
            do_commit(dbapi_connection)
        finally:
            if began:
                _report(began, 'committed', time.monotonic() - t0)

    engine.dialect.do_commit = _timed_do_commit

    @event.listens_for(engine, 'rollback')
    def _on_rollback(conn):
        began = _take(conn.connection.info)
        if began:
            _report(began, 'rolled back')

    # A connection returned to the pool mid-transaction is rolled back there,
    # without the engine-level rollback event.
    @event.listens_for(engine, 'reset')
    def _on_reset(dbapi_connection, connection_record, reset_state):
        began = _take(connection_record.info)
        if began:
            _report(began, 'rolled back on return to the pool')

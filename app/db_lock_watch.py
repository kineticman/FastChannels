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
import time
import traceback

from sqlalchemy import event

logger = logging.getLogger(__name__)

_THRESHOLD_S = float(os.environ.get('FC_DB_LOCK_WARN_SECONDS', '5'))
_WRITE_PREFIXES = ('INSERT', 'UPDATE', 'DELETE', 'REPLACE')
_KEY = 'fc_write_began'
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
    @event.listens_for(engine, 'after_cursor_execute')
    def _note_first_write(conn, cursor, statement, parameters, context, executemany):
        info = conn.connection.info
        if _KEY in info or not statement.lstrip().upper().startswith(_WRITE_PREFIXES):
            return
        info[_KEY] = (time.monotonic(), ' '.join(statement.split())[:100], _where(), _context())

    def _end(conn, how):
        began = conn.connection.info.pop(_KEY, None)
        if not began:
            return
        held = time.monotonic() - began[0]
        if held >= _THRESHOLD_S:
            t0, stmt, where, ctx = began
            logger.warning('[db-lock] write transaction held the lock %.1fs (%s): began at %s with %r; '
                           '%s at %s', held, ctx, where, stmt, how, _where())

    event.listen(engine, 'commit', lambda conn: _end(conn, 'committed'))
    event.listen(engine, 'rollback', lambda conn: _end(conn, 'rolled back'))

    # A connection returned to the pool mid-transaction is rolled back there,
    # without the engine-level rollback event.
    @event.listens_for(engine, 'reset')
    def _on_reset(dbapi_connection, connection_record, *args):
        began = connection_record.info.pop(_KEY, None)
        if began and time.monotonic() - began[0] >= _THRESHOLD_S:
            t0, stmt, where, ctx = began
            logger.warning('[db-lock] write transaction held the lock %.1fs (%s): began at %s with %r; '
                           'rolled back on return to the pool', time.monotonic() - t0, ctx, where, stmt)

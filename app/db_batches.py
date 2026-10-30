"""Large deletes and inserts that leave room for other SQLite writers.

SQLite has one database-wide writer. A large write in one transaction blocks
every other writer until it commits, and on slow storage that can pass their
30s busy_timeout (issue #86: 118k programs deleted in one go held the lock for
over two minutes). Small commits alone aren't enough: a waiting writer polls,
sleeping up to 100ms between tries, so a loop that takes the lock straight back
after each commit starves it. In a simulation with 1.2s holds, a competing
writer got nothing through in 40s with no gap, and at most a 1s wait with 100ms.

Pacing by time rather than row count keeps fast disks fast (a commit and a gap
after every fixed batch tripled a Zeam guide write on an SSD) while a slow disk
commits after nearly every batch. Commit time counts as hold time: on a slow disk
the commit's fsync is usually the longest part of it.
"""
import time

from .extensions import db

_WRITER_GAP_S = 0.1
_MAX_HOLD_S = 1.0
DELETE_BATCH = 1000


def yield_writer_lock() -> None:
    """Call after a commit in a batch loop, before writing again."""
    time.sleep(_WRITER_GAP_S)


class WritePacer:
    """Commits a batch loop's work once it has held the writer lock for about
    _MAX_HOLD_S, then pauses so a waiting writer gets in. Call wrote() after
    each batch and finish() after the loop."""

    def __init__(self):
        self._since = time.monotonic()
        self._last_commit_s = 0.0

    def wrote(self) -> None:
        if time.monotonic() - self._since >= _MAX_HOLD_S - self._last_commit_s:
            t0 = time.monotonic()
            db.session.commit()
            self._last_commit_s = time.monotonic() - t0
            yield_writer_lock()
            self._since = time.monotonic()

    def finish(self) -> None:
        db.session.commit()


def delete_in_batches(model, *criteria, batch_size: int = DELETE_BATCH, pacer: WritePacer | None = None) -> int:
    """Delete matching rows in paced batches. Returns the number deleted.

    Commits whatever the session has pending, so call it where that's safe. With
    a shared pacer the caller commits at the end, via pacer.finish()."""
    active = pacer or WritePacer()
    total = 0
    while True:
        ids = [row[0] for row in db.session.query(model.id).filter(*criteria).limit(batch_size).all()]
        if ids:
            total += model.query.filter(model.id.in_(ids)).delete(synchronize_session=False) or 0
            active.wrote()
        if len(ids) < batch_size:
            break
    if pacer is None:
        active.finish()
    return total

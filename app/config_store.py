import copy
import fcntl
import time
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy.exc import OperationalError as _SAOperationalError
from sqlalchemy.orm.attributes import flag_modified

from app.extensions import db
from app.models import Source, SourceCache
from app.scrapers.base import merge_config_updates


@contextmanager
def _source_config_lock(source_id: int):
    lock_path = Path('/tmp') / f'fastchannels-source-config-{source_id}.lock'
    lock_path.touch(exist_ok=True)
    with lock_path.open('r+') as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def persist_source_config_updates(source_id: int, updates: dict | None) -> bool:
    """Safely merge scraper-generated config updates for a Source row."""
    if not updates:
        return False
    with _source_config_lock(source_id):
        db.session.expire_all()
        live_source = db.session.get(Source, source_id, populate_existing=True)
        if not live_source:
            return False
        updated = merge_config_updates(live_source.config, copy.deepcopy(updates))
        live_source.config = updated
        flag_modified(live_source, 'config')
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


def load_source_cache(source_id: int) -> dict:
    """Return {cache_key: value} for every source_cache row of this source."""
    rows = (
        db.session.query(SourceCache.cache_key, SourceCache.value)
        .filter(SourceCache.source_id == source_id)
        .all()
    )
    return {key: value for key, value in rows}


def load_source_cache_by_name(source_name: str) -> dict:
    """Like load_source_cache but keyed by Source.name — used by BaseScraper,
    which knows its source_name but not its source_id at init time."""
    rows = (
        db.session.query(SourceCache.cache_key, SourceCache.value)
        .join(Source, Source.id == SourceCache.source_id)
        .filter(Source.name == source_name)
        .all()
    )
    return {key: value for key, value in rows}


def persist_source_cache_updates(source_id: int, updates: dict | None) -> bool:
    """UPSERT scraper-generated cache key/values into source_cache (one row per key).

    Mirrors persist_source_config_updates: reuses the same per-source file lock so
    a source's config and cache writes serialize together, with the same 3x
    OperationalError retry. Only the rows being written are loaded (not the whole
    cache), so persisting a small cache never deserializes the large ones.

    A value of None is stored as-is; the scraper-side loaders treat it as "empty",
    which is how callers clear a cache (e.g. an expired Roku osm_session)."""
    if not updates:
        return False
    with _source_config_lock(source_id):
        for _attempt in range(3):
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
                    else:
                        row.value = copy.deepcopy(value)
                db.session.commit()
                return True
            except _SAOperationalError:
                db.session.rollback()
                if _attempt == 2:
                    raise
                time.sleep(5 * (_attempt + 1))
        return False

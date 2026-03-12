import copy
import fcntl
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy.orm.attributes import flag_modified

from app.extensions import db
from app.models import Source
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
        db.session.commit()
        return True

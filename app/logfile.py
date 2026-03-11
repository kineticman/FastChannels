"""
Shared log-file setup for Flask app and RQ worker.
Both processes write to the same file so /admin/logs can show everything.
"""
import logging
import os
from collections import deque
from logging.handlers import RotatingFileHandler

LOG_PATH = os.environ.get('LOG_FILE', '/tmp/fastchannels.log')
LOG_MAX_BYTES = int(os.environ.get('LOG_MAX_BYTES', str(5 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(os.environ.get('LOG_BACKUP_COUNT', '3'))
_FORMATTER = logging.Formatter('%(asctime)s %(levelname)-8s %(name)s: %(message)s')


def setup():
    """Attach a FileHandler to the root logger (idempotent)."""
    root = logging.getLogger()
    if root.getEffectiveLevel() > logging.INFO:
        root.setLevel(logging.INFO)

    has_stream = any(isinstance(h, logging.StreamHandler) for h in root.handlers)
    if not has_stream:
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(_FORMATTER)
        root.addHandler(sh)

    for h in root.handlers:
        if isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', '') == LOG_PATH:
            return

    parent = os.path.dirname(LOG_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)

    fh = RotatingFileHandler(
        LOG_PATH,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(_FORMATTER)
    root.addHandler(fh)


def tail(n: int = 2500) -> list[str]:
    """Return the last n lines from the log file."""
    try:
        with open(LOG_PATH, 'r', errors='replace') as f:
            return list(deque(f, maxlen=n))
    except FileNotFoundError:
        return []

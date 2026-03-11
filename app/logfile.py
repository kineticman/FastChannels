"""
Shared log-file setup for Flask app and RQ worker.
Both processes write to the same file so /admin/logs can show everything.
"""
import logging
import os
from collections import deque

LOG_PATH = os.environ.get('LOG_FILE', '/tmp/fastchannels.log')
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
    fh = logging.FileHandler(LOG_PATH)
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

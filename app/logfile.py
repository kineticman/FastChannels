"""
Shared log-file setup for Flask app and RQ worker.
Both processes write to the same file so /admin/logs can show everything.
"""
import logging
import os
import re
from collections import deque
from datetime import datetime
from logging.handlers import RotatingFileHandler
from .debug_flag import debug_logging_cached
from .timezone_utils import current_zoneinfo

LOG_PATH = os.environ.get('LOG_FILE', '/tmp/fastchannels.log')
LOG_MAX_BYTES = int(os.environ.get('LOG_MAX_BYTES', str(5 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(os.environ.get('LOG_BACKUP_COUNT', '3'))


class _TimezoneFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=current_zoneinfo())
        if datefmt:
            return dt.strftime(datefmt)
        return f"{dt.strftime('%Y-%m-%d %H:%M:%S')},{int(record.msecs):03d}"


_FORMATTER = _TimezoneFormatter('%(asctime)s %(levelname)-8s %(name)s: %(message)s')


# What a debug line must not carry into a log someone pastes into a forum.
_SECRET_PATTERNS = tuple((re.compile(pattern), repl) for pattern, repl in (
    (r"""(https?://)[^/\s'"@]+@""", r'\1[redacted]@'),                       # user:password@host
    (r"""(https?://[^\s'"?#]*)[?#][^\s'"]+""", r'\1?[redacted]'),            # query and fragment
    (r"""\?[\w.%\[\]~-]+=[^\s'"]*""", '?[redacted]'),                        # relative URL's query
    (r"""/(live|movie|series|timeshift)/[^/\s'"]+/[^/\s'"]+/""", r'/\1/[redacted]/[redacted]/'),  # Xtream
    (r"""\b(hdnts|hdntl|hdnea)=[^/\s'"]+""", r'\1=[redacted]'),              # Akamai token in a path
    (r'\beyJ[\w-]{8,}\.[\w-]+\.[\w-]*', '[redacted-token]'),                 # JWT
    (r'\b(Bearer)\s+[\w.~+/=-]{8,}', r'\1 [redacted]'),
))


def redact(text: str) -> str:
    for pattern, repl in _SECRET_PATTERNS:
        text = pattern.sub(repl, text)
    return text


class _DebugGate(logging.Filter):
    """On every output: drops debug lines unless debug logging is on
    (Settings → Advanced, or FC_DEBUG), and strips secrets from those it lets
    through. INFO and above pass untouched."""

    def filter(self, record):
        if record.levelno >= logging.INFO:
            return True
        if not debug_logging_cached():
            return False
        try:
            record.msg, record.args = redact(record.getMessage()), ()
            if record.exc_info:
                record.exc_text = redact(logging.Formatter().formatException(record.exc_info))
                record.exc_info = None
        except Exception:    # a line we can't clean is a line we don't write
            return False
        return True


_DEBUG_GATE = _DebugGate()


def setup():
    """Attach a FileHandler to the root logger (idempotent)."""
    root = logging.getLogger()
    if root.getEffectiveLevel() > logging.INFO:
        root.setLevel(logging.INFO)

    # RQ's queue housekeeping is very noisy at INFO and doesn't add much
    # operational value compared with our own scrape start/finish logs.
    logging.getLogger('rq.worker').setLevel(logging.WARNING)
    logging.getLogger('rq.registry').setLevel(logging.WARNING)

    has_stream = any(isinstance(h, logging.StreamHandler) for h in root.handlers)
    if not has_stream:
        sh = logging.StreamHandler()
        sh.setFormatter(_FORMATTER)
        root.addHandler(sh)

    for h in root.handlers:
        if isinstance(h, (logging.StreamHandler, logging.FileHandler)):
            h.setFormatter(_FORMATTER)

    # Our own loggers make debug records all the time; the gate on each output
    # decides, so the setting works without a restart. Libraries stay at INFO.
    for name in ('app', '__main__'):
        logging.getLogger(name).setLevel(logging.DEBUG)

    has_file = any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', '') == LOG_PATH
                   for h in root.handlers)
    if has_file:
        _gate(root)
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
    _gate(root)


def _gate(root: logging.Logger) -> None:
    for h in root.handlers:
        h.setLevel(logging.DEBUG)
        h.addFilter(_DEBUG_GATE)    # a no-op if it already has it


def tail(n: int = 2500) -> list[str]:
    """Return the last n lines from the log file."""
    try:
        with open(LOG_PATH, 'r', errors='replace') as f:
            return list(deque(f, maxlen=n))
    except FileNotFoundError:
        return []

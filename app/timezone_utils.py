import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, available_timezones

_TIMEZONE_CACHE_PATH = os.environ.get('FASTCHANNELS_TIMEZONE_CACHE_FILE', '/data/cache/timezone.txt')
_TIMEZONE_CACHE_TTL_SECONDS = 30
_VALID_TIMEZONES = tuple(sorted(available_timezones()))
_VALID_TIMEZONE_SET = set(_VALID_TIMEZONES)
_cache_state = {
    'checked_at': 0.0,
    'name': None,
}


def timezone_choices() -> tuple[str, ...]:
    return _VALID_TIMEZONES


def normalize_timezone_name(value: str | None) -> str | None:
    raw = (value or '').strip()
    if not raw:
        return None
    return raw if raw in _VALID_TIMEZONE_SET else None


def _system_timezone_name() -> str | None:
    local_tz = datetime.now().astimezone().tzinfo
    key = getattr(local_tz, 'key', None)
    return key if key in _VALID_TIMEZONE_SET else None


def timezone_health(configured: str | None = None) -> dict:
    """Diagnose how the *effective* timezone resolves, so the admin UI can warn
    about silent UTC fallbacks.

    The common failure (seen in the wild): a container `TZ` env var set to a
    legacy alias like ``US/Central`` that this container's tzdata can't resolve.
    ``zoneinfo`` rejects it, the system clock falls back to UTC, and scheduled
    jobs / timestamps quietly run on the wrong wall clock with no error.

    Returns a dict: ``{ok, level, env_tz, env_tz_valid, system_timezone,
    configured, message}`` where ``level`` is ``'ok'`` or ``'warning'``.
    """
    env_tz = (os.environ.get('TZ') or '').strip() or None
    env_tz_valid = env_tz is None or env_tz in _VALID_TIMEZONE_SET
    configured = (configured or '').strip() or None
    configured_valid = configured is None or configured in _VALID_TIMEZONE_SET
    system = _system_timezone_name()

    result = {
        'env_tz': env_tz,
        'env_tz_valid': env_tz_valid,
        'system_timezone': system,
        'configured': configured,
        'configured_valid': configured_valid,
        'level': 'ok',
        'message': None,
    }

    if configured is not None and not configured_valid:
        # Shouldn't normally happen (the API gate rejects invalid names), but a
        # value written before validation existed, or via direct DB edit, would
        # land here.
        result['level'] = 'warning'
        result['message'] = (
            f"The saved Time Zone '{configured}' isn't a valid IANA name this "
            f"container can resolve, so timestamps fall back to UTC. Pick a "
            f"canonical name like 'America/Chicago' below."
        )
    elif env_tz is not None and not env_tz_valid:
        result['level'] = 'warning'
        result['message'] = (
            f"The container's TZ environment variable is set to '{env_tz}', "
            f"which this container's timezone database can't resolve. The system "
            f"clock and scheduled jobs are falling back to UTC. Use a canonical "
            f"IANA name such as 'America/Chicago' instead of a legacy alias like "
            f"'{env_tz}' — either in your compose file's TZ, or by saving an "
            f"explicit Time Zone below (which overrides TZ for the app)."
        )
    elif configured is None and env_tz is None and system is None:
        result['level'] = 'warning'
        result['message'] = (
            "No TZ environment variable is set and the system's local timezone "
            "couldn't be identified, so timestamps default to UTC. Set TZ in your "
            "compose file or save an explicit Time Zone below."
        )

    result['ok'] = result['level'] == 'ok'
    return result


def default_timezone_name() -> str:
    return _system_timezone_name() or 'UTC'


def read_timezone_cache(*, force: bool = False) -> str | None:
    now = time.time()
    if not force and (now - _cache_state['checked_at']) < _TIMEZONE_CACHE_TTL_SECONDS:
        return _cache_state['name']

    try:
        with open(_TIMEZONE_CACHE_PATH, 'r', encoding='utf-8') as fp:
            name = normalize_timezone_name(fp.read())
    except OSError:
        name = None

    if name is None:
        # File missing or empty — keep the last known good value rather than
        # blanking the cache and causing a UTC fallback.
        _cache_state['checked_at'] = now
        return _cache_state['name']

    _cache_state['checked_at'] = now
    _cache_state['name'] = name
    return name


def write_timezone_cache(value: str | None) -> str | None:
    name = normalize_timezone_name(value)
    if name is None:
        # Don't overwrite a valid cached timezone with nothing — a transient
        # None (e.g. AppSettings not yet committed) would blank the file and
        # cause all processes to fall back to UTC until the next valid write.
        return None
    parent = os.path.dirname(_TIMEZONE_CACHE_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(_TIMEZONE_CACHE_PATH, 'w', encoding='utf-8') as fp:
        fp.write(name)
    _cache_state['checked_at'] = time.time()
    _cache_state['name'] = name
    return name


def current_timezone_name(value: str | None = None) -> str:
    return normalize_timezone_name(value) or read_timezone_cache() or default_timezone_name()


def current_zoneinfo(value: str | None = None):
    name = current_timezone_name(value)
    try:
        return ZoneInfo(name)
    except Exception:
        return datetime.now().astimezone().tzinfo or timezone.utc


def make_tz_formatter(fmt: str) -> 'logging.Formatter':
    """Return a logging.Formatter whose timestamps use the configured timezone."""
    import logging
    class _TZFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            from datetime import datetime, timezone as _utc
            dt = datetime.fromtimestamp(record.created, tz=_utc.utc).astimezone(current_zoneinfo())
            if datefmt:
                return dt.strftime(datefmt)
            return dt.strftime('%Y-%m-%d %H:%M:%S') + f',{int(record.msecs):03d}'
    return _TZFormatter(fmt)


def format_datetime(dt, *, timezone_name: str | None = None, fallback: str = 'Never') -> str:
    if dt is None:
        return fallback
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(current_zoneinfo(timezone_name)).strftime('%Y-%m-%d %H:%M %Z')

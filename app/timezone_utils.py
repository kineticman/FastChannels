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

    _cache_state['checked_at'] = now
    _cache_state['name'] = name
    return name


def write_timezone_cache(value: str | None) -> str | None:
    name = normalize_timezone_name(value)
    parent = os.path.dirname(_TIMEZONE_CACHE_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(_TIMEZONE_CACHE_PATH, 'w', encoding='utf-8') as fp:
        fp.write(name or '')
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


def format_datetime(dt, *, timezone_name: str | None = None, fallback: str = 'Never') -> str:
    if dt is None:
        return fallback
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(current_zoneinfo(timezone_name)).strftime('%Y-%m-%d %H:%M %Z')

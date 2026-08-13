import importlib
import pkgutil
import logging
import time
from pathlib import Path
from .base import BaseScraper

logger = logging.getLogger(__name__)
_registry: dict[str, type[BaseScraper]] = {}
_last_discovered_at = 0.0
# Long-lived RQ worker processes never re-import app/scrapers on their own, so
# re-discovering on every call is what lets the bind-mounted dev workflow (edit
# a scraper file, no restart) show up without a restart. But callers like the
# per-channel DRM-capability check in generators/m3u.py can call get_all() tens
# of thousands of times within one artifact-refresh batch — re-scanning the
# filesystem on every one of those turned into the dominant cost of that job.
# A short TTL keeps live-reload responsive while collapsing that burst to
# effectively one real scan.
_DISCOVER_TTL_SECONDS = 2.0


def _discover():
    scrapers_path = Path(__file__).parent
    for _, module_name, _ in pkgutil.iter_modules([str(scrapers_path)]):
        if module_name in ('base', 'registry'):
            continue
        try:
            importlib.import_module(f'.{module_name}', package=__package__)
        except Exception as e:
            logger.warning(f'Failed to import scraper {module_name}: {e}')

    for cls in BaseScraper.__subclasses__():
        if cls.source_name:
            _registry[cls.source_name] = cls
            for alias in getattr(cls, 'source_aliases', ()) or ():
                _registry[alias] = cls


def get_all() -> dict[str, type[BaseScraper]]:
    global _last_discovered_at
    now = time.monotonic()
    if now - _last_discovered_at > _DISCOVER_TTL_SECONDS:
        _discover()
        _last_discovered_at = now
    return _registry


def get(source_name: str) -> type[BaseScraper] | None:
    return get_all().get(source_name)


def drm_capable_source_names() -> list[str]:
    """Source names whose scraper exposes DRM license handling (a `license_url`).

    A DASH channel from one of these is bridge-only — it can never play on a normal
    client — so it's treated as an intrinsic PrismCast-bridge channel (no audit needed)
    and kept out of the standard feed. Single source of truth for "is this source
    bridgeable"; callers in the feed query, audit, PrismCast test, and Settings nudge
    all route through here so they can never disagree."""
    return sorted({cls.source_name for cls in get_all().values() if getattr(cls, 'license_url', None)})


def source_is_drm_capable(source_name: str | None) -> bool:
    """True if this source's scraper exposes license handling (a `license_url`)."""
    if not source_name:
        return False
    cls = get(source_name)
    return bool(cls and getattr(cls, 'license_url', None))

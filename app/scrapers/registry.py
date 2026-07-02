import importlib
import pkgutil
import logging
from pathlib import Path
from .base import BaseScraper

logger = logging.getLogger(__name__)
_registry: dict[str, type[BaseScraper]] = {}


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
    _discover()   # always re-discover; fast filesystem scan, safe to call repeatedly
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
    return [name for name, cls in get_all().items() if getattr(cls, 'license_url', None)]


def source_is_drm_capable(source_name: str | None) -> bool:
    """True if this source's scraper exposes license handling (a `license_url`)."""
    if not source_name:
        return False
    cls = get(source_name)
    return bool(cls and getattr(cls, 'license_url', None))

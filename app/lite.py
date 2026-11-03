"""Request-scoped UI preferences. Never use these to filter generated outputs."""

import time
from functools import wraps

from flask import g

from .extensions import db
from .models import AppSettings, Source, TVEAccount
from .scrapers import registry


def _per_request(func):
    @wraps(func)
    def cached():
        cache = g.setdefault('_lite_ui_cache', {})
        if func.__name__ not in cache:
            cache[func.__name__] = func()
        return cache[func.__name__]
    return cached


@_per_request
def _settings():
    return AppSettings.get()


@_per_request
def lite_active() -> bool:
    return bool(_settings().lite_mode)


def source_visible(source, cls) -> bool:
    # Keep retired/unknown scrapers visible so their warnings remain accessible.
    return not lite_active() or cls is None or not cls.is_advanced() or bool(source.is_enabled)


@_per_request
def bridge_in_use() -> bool:
    settings = _settings()
    return bool(
        settings.bridge_enabled or settings.drm_bridge_enabled
        or settings.prismcast_enabled or settings.fc_player_bridge_enabled
        or settings.fc_player_bridge_hdmi_enabled
        or settings.effective_fc_player_bridge_ah4c_url()
    )


def _enabled_source_exists(names) -> bool:
    return bool(db.session.query(db.exists().where(
        Source.is_enabled.is_(True), Source.name.in_(names),
    )).scalar())


@_per_request
def tve_in_use() -> bool:
    return bool(db.session.query(db.exists().where(TVEAccount.is_enabled.is_(True))).scalar()) or _enabled_source_exists(
        [name for name, cls in registry.get_all().items() if cls.source_category == 'tve']
    )


@_per_request
def drm_in_use() -> bool:
    return bridge_in_use() or _enabled_source_exists(
        [name for name, cls in registry.get_all().items() if cls.is_advanced()]
    )


@_per_request
def native_in_use() -> bool:
    from .xml_cache import native_output_activity
    last_requested = native_output_activity()['last_requested_at']
    return last_requested is not None and last_requested >= time.time() - 30 * 86400


@_per_request
def ui_context() -> dict:
    lite = lite_active()
    return {
        'lite': lite,
        'show_bridge': not lite or bridge_in_use(),
        'show_tve': not lite or tve_in_use(),
        'show_drm': not lite or drm_in_use(),
        'show_native': not lite or native_in_use(),
    }

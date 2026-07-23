from __future__ import annotations

from urllib.parse import urlsplit

from flask import current_app


def is_placeholder_public_base_url(value: str | None) -> bool:
    raw = (value or "").strip()
    if not raw:
        return False
    try:
        parsed = urlsplit(raw if "://" in raw else f"http://{raw}")
        host = (parsed.hostname or "").lower().rstrip(".")
    except ValueError:
        return False
    return (
        host in {"example.com", "myserver.example.com", "your-server", "your-server-ip", "192.168.1.x"}
        or host.endswith(".example.com")
    )


def public_base_url_config(app_settings) -> tuple[str, str | None, bool]:
    db_value = (app_settings.public_base_url or "").strip().rstrip("/")
    fastchannels_env = app_settings.env_public_base_url()
    legacy_env = (current_app.config.get("PUBLIC_BASE_URL") or "").strip().rstrip("/")

    if db_value:
        value, source = db_value, "database"
    elif fastchannels_env:
        value, source = fastchannels_env, "FASTCHANNELS_SERVER_URL"
    elif legacy_env:
        value, source = legacy_env, "PUBLIC_BASE_URL"
    else:
        value, source = "", None

    return value, source, not value or is_placeholder_public_base_url(value)


def has_meaningful_source_config(scraper_cls, values: dict | None) -> bool:
    schema = getattr(scraper_cls, 'config_schema', []) or []
    if not schema:
        return False

    saved = values or {}
    for field in schema:
        value = saved.get(field.key)
        default = field.default
        if value in (None, ''):
            continue
        if str(value) == str(default):
            continue
        return True
    return False


def is_source_config_complete(source_name: str, scraper_cls, values: dict | None) -> bool:
    schema = getattr(scraper_cls, 'config_schema', []) or []
    saved = values or {}

    # Amazon playback may work with a manually pasted cookie, but the source
    # should not be presented as fully configured until the account credentials
    # and the captured session cookie are all present.
    if source_name == 'amazon_prime_free':
        return all((saved.get(key) or '').strip() for key in (
            'amazon_email', 'amazon_password', 'cookie_header',
        ))

    required_fields = [field for field in schema if getattr(field, 'required', False)]
    if required_fields:
        return all((saved.get(field.key) or '').strip() for field in required_fields)

    if source_name == 'localnow':
        return bool((saved.get('dma') or '').strip() or (saved.get('market') or '').strip())

    # Philo is passwordless — "configured" means a signed-in session exists
    # (set by the in-app email→code flow), not any typed field.
    if source_name == 'philo':
        return bool(saved.get('session_cookies'))

    return has_meaningful_source_config(scraper_cls, saved)


def build_setup_checklist(app_settings, sources_by_name: dict, scrapers_by_name: dict) -> list[dict]:
    items: list[dict] = []

    _, _, public_base_url_needs_config = public_base_url_config(app_settings)
    if public_base_url_needs_config:
        items.append({
            'key': 'public_base_url',
            'label': 'Set FastChannels Server URL',
            'href': '/admin/settings#settings-card-public-base-url',
            'section': 'settings',
        })

    if not (app_settings.channels_dvr_url or '').strip() and app_settings.env_channels_dvr_url() is None:
        items.append({
            'key': 'channels_dvr_url',
            'label': 'Set Channels DVR URL',
            'href': '/admin/settings#settings-card-channels-dvr',
            'section': 'settings',
        })

    if not (app_settings.timezone_name or '').strip():
        items.append({
            'key': 'timezone_name',
            'label': 'Set Time Zone',
            'href': '/admin/settings#settings-card-timezone',
            'section': 'settings',
        })

    for source_name, label in (('pluto', 'Configure Pluto TV'), ('localnow', 'Configure Local Now')):
        source = sources_by_name.get(source_name)
        scraper_cls = scrapers_by_name.get(source_name)
        if not source or not scraper_cls:
            continue
        if not source.is_enabled:
            continue
        if not is_source_config_complete(source_name, scraper_cls, source.config or {}):
            items.append({
                'key': source_name,
                'label': label,
                'href': '/admin/sources',
                'section': 'sources',
            })

    return items

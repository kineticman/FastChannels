from __future__ import annotations


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

    required_fields = [field for field in schema if getattr(field, 'required', False)]
    if required_fields:
        return all((saved.get(field.key) or '').strip() for field in required_fields)

    if source_name == 'localnow':
        return bool((saved.get('dma') or '').strip() or (saved.get('market') or '').strip())

    return has_meaningful_source_config(scraper_cls, saved)


def build_setup_checklist(app_settings, sources_by_name: dict, scrapers_by_name: dict) -> list[dict]:
    items: list[dict] = []

    if not (app_settings.public_base_url or '').strip() and app_settings.env_public_base_url() is None:
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
        if not is_source_config_complete(source_name, scraper_cls, source.config or {}):
            items.append({
                'key': source_name,
                'label': label,
                'href': '/admin/sources',
                'section': 'sources',
            })

    return items

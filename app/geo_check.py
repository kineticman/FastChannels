"""Server public-IP country check.

Almost every source is US-only: from outside the US (or through a non-US VPN
exit) they fail with 403/451/452 or bogus sign-in errors, and the stream audit
aborts with a vague "20 consecutive errors" while every channel stays enabled
(GH #64). Looking up the server's own country once lets the dashboard say so up
front and lets audit failures name the likely cause.

Advisory only — never used to block a scrape or audit. Split-tunnel VPNs and
stale GeoIP data can make the lookup wrong, and a wrong hard-block would be far
worse than a wrong warning. Opt out with GEO_CHECK_ENABLED=0.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path

import requests


log = logging.getLogger(__name__)

_CACHE_PATH = Path(os.environ.get('FASTCHANNELS_GEO_CACHE_FILE', '/data/cache/server_geo.json'))
_TTL_SECONDS = 3600
_FAILURE_RETRY_SECONDS = 600
_REFRESH_LOCK = threading.Lock()
_REFRESH_IN_PROGRESS = False


def geo_check_enabled() -> bool:
    return os.environ.get('GEO_CHECK_ENABLED', '1').strip() != '0'


def _read_cache() -> dict:
    try:
        if _CACHE_PATH.exists():
            return json.loads(_CACHE_PATH.read_text(encoding='utf-8')) or {}
    except Exception:
        log.warning('[geo-check] failed reading cache', exc_info=True)
    return {}


def _write_cache(payload: dict) -> None:
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(_CACHE_PATH) + '.tmp')
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding='utf-8')
        tmp.replace(_CACHE_PATH)
    except Exception:
        log.warning('[geo-check] failed writing cache', exc_info=True)


def _lookup() -> dict:
    """Return ``{'country': 'US', 'provider': ...}``. Tries Cloudflare's trace
    endpoint first (plain text, no key, no rate limit), then ipinfo.io."""
    headers = {'User-Agent': 'FastChannels-GeoCheck'}
    try:
        r = requests.get('https://www.cloudflare.com/cdn-cgi/trace', headers=headers, timeout=4)
        r.raise_for_status()
        fields = dict(line.split('=', 1) for line in r.text.splitlines() if '=' in line)
        loc = (fields.get('loc') or '').strip().upper()
        if len(loc) == 2 and loc != 'XX':
            return {'country': loc, 'provider': 'cloudflare'}
    except Exception as exc:
        log.info('[geo-check] cloudflare lookup failed: %s', exc)
    r = requests.get('https://ipinfo.io/json', headers=headers, timeout=4)
    r.raise_for_status()
    country = ((r.json() or {}).get('country') or '').strip().upper()
    if len(country) != 2:
        raise RuntimeError('ipinfo returned no country')
    return {'country': country, 'provider': 'ipinfo'}


def _refresh() -> dict:
    try:
        payload = {**_lookup(), 'checked_at': time.time(), 'error': None}
        prev = _read_cache().get('country')
        if prev and prev != payload['country']:
            log.warning('[geo-check] server country changed %s → %s', prev, payload['country'])
        elif not prev and payload['country'] != 'US':
            log.warning('[geo-check] server appears to be outside the US (%s) — '
                        'most sources are US-only and will fail', payload['country'])
    except Exception as exc:
        log.warning('[geo-check] lookup failed: %s', exc)
        # Keep the last known country; retry sooner than the normal TTL.
        payload = {**_read_cache(), 'error': str(exc),
                   'checked_at': time.time() - _TTL_SECONDS + _FAILURE_RETRY_SECONDS}
    _write_cache(payload)
    return payload


def _refresh_async() -> None:
    global _REFRESH_IN_PROGRESS
    with _REFRESH_LOCK:
        if _REFRESH_IN_PROGRESS:
            return
        _REFRESH_IN_PROGRESS = True

    def _run():
        global _REFRESH_IN_PROGRESS
        try:
            _refresh()
        finally:
            with _REFRESH_LOCK:
                _REFRESH_IN_PROGRESS = False

    threading.Thread(target=_run, name='geo-check', daemon=True).start()


def _is_stale(cache: dict) -> bool:
    checked_at = cache.get('checked_at')
    return not checked_at or (time.time() - float(checked_at)) >= _TTL_SECONDS


def get_server_country(*, blocking: bool = False) -> str | None:
    """Two-letter country code of the server's public IP, or None if unknown or
    disabled. Non-blocking by default (web requests): a stale cache is returned
    as-is and refreshed in the background. Workers pass blocking=True."""
    if not geo_check_enabled():
        return None
    cache = _read_cache()
    if _is_stale(cache):
        if blocking:
            cache = _refresh()
        else:
            _refresh_async()
    return cache.get('country') or None


def outside_us_hint(source_countries) -> str | None:
    """Explanation to attach to an audit failure when the server is outside the
    US and the source is US-only (every channel's country is US), else None."""
    countries = {(c or 'US').upper() for c in source_countries}
    if countries and countries != {'US'}:
        return None
    country = get_server_country(blocking=True)
    if not country or country == 'US':
        return None
    return (f'this server appears to be in {country}, and this source only works '
            f'from a US internet connection')

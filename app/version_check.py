from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from pathlib import Path

import requests


log = logging.getLogger(__name__)

_CACHE_PATH = Path(os.environ.get('FASTCHANNELS_VERSION_CACHE_FILE', '/data/cache/version_check.json'))
_REFRESH_LOCK = threading.Lock()
_REFRESH_IN_PROGRESS = False


def _version_key(value: str | None) -> tuple[int, ...]:
    raw = (value or '').strip().lower()
    if raw.startswith('v'):
        raw = raw[1:]
    parts: list[int] = []
    for part in raw.split('.'):
        match = re.match(r'(\d+)', part)
        parts.append(int(match.group(1)) if match else 0)
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


def _ensure_parent() -> None:
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)


def _read_cache() -> dict | None:
    try:
        if not _CACHE_PATH.exists():
            return None
        return json.loads(_CACHE_PATH.read_text(encoding='utf-8'))
    except Exception:
        log.exception('[version-check] failed reading cache')
        return None


def _write_cache(payload: dict) -> None:
    try:
        _ensure_parent()
        tmp = Path(str(_CACHE_PATH) + '.tmp')
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding='utf-8')
        tmp.replace(_CACHE_PATH)
    except Exception:
        log.exception('[version-check] failed writing cache')


def _fetch_latest(repo: str) -> dict:
    headers = {
        'Accept': 'application/vnd.github+json',
        'User-Agent': 'FastChannels-VersionCheck',
    }
    release_url = f'https://api.github.com/repos/{repo}/releases/latest'
    tags_url = f'https://api.github.com/repos/{repo}/tags?per_page=1'

    try:
        resp = requests.get(release_url, headers=headers, timeout=3)
        if resp.ok:
            data = resp.json()
            version = (data.get('tag_name') or data.get('name') or '').strip()
            if version:
                return {
                    'latest_version': version.lstrip('v'),
                    'latest_url': data.get('html_url') or f'https://github.com/{repo}/releases/latest',
                    'source': 'release',
                }
    except Exception as exc:
        log.warning('[version-check] release lookup failed: %s', exc)

    resp = requests.get(tags_url, headers=headers, timeout=3)
    resp.raise_for_status()
    tags = resp.json() or []
    if not tags:
        raise RuntimeError('no tags returned')
    first = tags[0] or {}
    version = (first.get('name') or '').strip()
    if not version:
        raise RuntimeError('latest tag missing name')
    return {
        'latest_version': version.lstrip('v'),
        'latest_url': f'https://github.com/{repo}/tags',
        'source': 'tag',
    }


def _refresh_cache(current_version: str, repo: str) -> None:
    global _REFRESH_IN_PROGRESS
    try:
        latest = _fetch_latest(repo)
        payload = {
            'checked_at': time.time(),
            'current_version': current_version,
            'repo': repo,
            **latest,
        }
        _write_cache(payload)
    except Exception as exc:
        log.warning('[version-check] refresh failed: %s', exc)
        cached = _read_cache() or {}
        cached.update({
            'checked_at': time.time(),
            'current_version': current_version,
            'repo': repo,
        })
        _write_cache(cached)
    finally:
        with _REFRESH_LOCK:
            _REFRESH_IN_PROGRESS = False


def _refresh_async(current_version: str, repo: str) -> None:
    global _REFRESH_IN_PROGRESS
    with _REFRESH_LOCK:
        if _REFRESH_IN_PROGRESS:
            return
        _REFRESH_IN_PROGRESS = True
    t = threading.Thread(
        target=_refresh_cache,
        args=(current_version, repo),
        name='version-check',
        daemon=True,
    )
    t.start()


def get_version_status(current_version: str, *, enabled: bool, repo: str, ttl_hours: int = 12) -> dict:
    status = {
        'enabled': bool(enabled),
        'current_version': current_version,
        'latest_version': None,
        'latest_url': f'https://github.com/{repo}',
        'update_available': False,
        'checked_at': None,
    }
    if not enabled:
        return status

    cache = _read_cache() or {}
    checked_at = cache.get('checked_at')
    latest_version = cache.get('latest_version')
    latest_url = cache.get('latest_url') or status['latest_url']
    status.update({
        'checked_at': checked_at,
        'latest_version': latest_version,
        'latest_url': latest_url,
    })
    if latest_version and _version_key(latest_version) > _version_key(current_version):
        status['update_available'] = True

    ttl_seconds = max(int(ttl_hours or 12), 1) * 3600
    stale = (not checked_at) or ((time.time() - float(checked_at)) >= ttl_seconds)
    if stale:
        _refresh_async(current_version, repo)
    return status

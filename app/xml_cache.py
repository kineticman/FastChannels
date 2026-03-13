from __future__ import annotations

import os
from pathlib import Path
from typing import Callable


_CACHE_ROOT = Path(os.environ.get('FASTCHANNELS_XML_CACHE_DIR', '/data/cache/xml'))


def _ensure_cache_dir() -> None:
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)


def _cache_path(cache_key: str) -> Path:
    safe_key = ''.join(ch if ch.isalnum() or ch in ('-', '_') else '_' for ch in cache_key)
    return _CACHE_ROOT / f'{safe_key}.xml'


def get_cached_xml(cache_key: str) -> str | None:
    path = _cache_path(cache_key)
    if not path.exists():
        return None
    return path.read_text(encoding='utf-8')


def write_cached_xml(cache_key: str, content: str) -> None:
    _ensure_cache_dir()
    path = _cache_path(cache_key)
    tmp = path.with_suffix('.xml.tmp')
    tmp.write_text(content, encoding='utf-8')
    tmp.replace(path)


def get_or_build_xml(cache_key: str, builder: Callable[[], str]) -> str:
    cached = get_cached_xml(cache_key)
    if cached is not None:
        return cached
    content = builder()
    write_cached_xml(cache_key, content)
    return content


def invalidate_xml_cache(cache_key: str | None = None) -> int:
    if cache_key is not None:
        path = _cache_path(cache_key)
        if path.exists():
            path.unlink()
            return 1
        return 0

    if not _CACHE_ROOT.exists():
        return 0

    removed = 0
    for path in _CACHE_ROOT.glob('*.xml'):
        path.unlink(missing_ok=True)
        removed += 1
    return removed

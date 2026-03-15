from __future__ import annotations

import os
from pathlib import Path
from typing import Callable


_CACHE_ROOT = Path(os.environ.get('FASTCHANNELS_XML_CACHE_DIR', '/data/cache/xml'))


def _ensure_cache_dir() -> None:
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)


def _cache_path(cache_key: str, ext: str = 'xml') -> Path:
    safe_key = ''.join(ch if ch.isalnum() or ch in ('-', '_') else '_' for ch in cache_key)
    return _CACHE_ROOT / f'{safe_key}.{ext}'


def get_or_build(cache_key: str, builder: Callable[[], str], ext: str = 'xml') -> str:
    """Return cached content, building and persisting it if missing.

    Multiple workers may build simultaneously on a cold cache — that's fine.
    The atomic tmp→rename write ensures clients never see a partial file.
    """
    _ensure_cache_dir()
    path = _cache_path(cache_key, ext)
    if path.exists():
        return path.read_text(encoding='utf-8')
    content = builder()
    tmp = Path(str(path) + '.tmp')
    tmp.write_text(content, encoding='utf-8')
    tmp.replace(path)
    return content


def get_or_build_xml(cache_key: str, builder: Callable[[], str]) -> str:
    return get_or_build(cache_key, builder, ext='xml')


def invalidate_xml_cache(cache_key: str | None = None) -> int:
    if cache_key is not None:
        removed = 0
        for ext in ('xml', 'm3u'):
            path = _cache_path(cache_key, ext)
            if path.exists():
                path.unlink()
                removed += 1
        return removed

    if not _CACHE_ROOT.exists():
        return 0

    removed = 0
    for path in _CACHE_ROOT.glob('*'):
        if path.suffix in ('.xml', '.m3u') and path.is_file():
            path.unlink(missing_ok=True)
            removed += 1
    return removed

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Callable, TextIO


_CACHE_ROOT = Path(os.environ.get('FASTCHANNELS_XML_CACHE_DIR', '/data/cache/xml'))
_GLOBAL_XML_STALE = _CACHE_ROOT / '.xml-stale'


def _ensure_cache_dir() -> None:
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)


def _cache_path(cache_key: str, ext: str = 'xml') -> Path:
    safe_key = ''.join(ch if ch.isalnum() or ch in ('-', '_') else '_' for ch in cache_key)
    return _CACHE_ROOT / f'{safe_key}.{ext}'


def _xml_stale_path(cache_key: str) -> Path:
    return _cache_path(cache_key, ext='xml.stale')


def _xml_lock_path(cache_key: str) -> Path:
    return _cache_path(cache_key, ext='xml.lock')


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


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


def _xml_is_stale(cache_key: str, path: Path) -> bool:
    if not path.exists():
        return True
    file_mtime = path.stat().st_mtime
    key_stale = _xml_stale_path(cache_key)
    if key_stale.exists() and key_stale.stat().st_mtime >= file_mtime:
        return True
    return _GLOBAL_XML_STALE.exists() and _GLOBAL_XML_STALE.stat().st_mtime >= file_mtime


def _clear_xml_stale(cache_key: str) -> None:
    _xml_stale_path(cache_key).unlink(missing_ok=True)


def xml_artifact_path(cache_key: str) -> Path:
    return _cache_path(cache_key, ext='xml')


def get_xml_artifact(cache_key: str) -> tuple[Path | None, bool]:
    """Return `(path, stale)` for the current XML artifact without rebuilding it."""
    _ensure_cache_dir()
    path = xml_artifact_path(cache_key)
    if not path.exists():
        return None, True
    return path, _xml_is_stale(cache_key, path)


def mark_xml_stale(cache_key: str | None = None) -> None:
    _ensure_cache_dir()
    if cache_key is None:
        _touch(_GLOBAL_XML_STALE)
    else:
        _touch(_xml_stale_path(cache_key))


def write_xml_artifact(cache_key: str, writer: Callable[[TextIO], None]) -> Path:
    _ensure_cache_dir()
    path = _cache_path(cache_key, ext='xml')
    tmp = Path(str(path) + '.tmp')
    with tmp.open('w', encoding='utf-8') as fp:
        writer(fp)
    tmp.replace(path)
    _clear_xml_stale(cache_key)
    return path


def ensure_xml_artifact(cache_key: str, writer: Callable[[TextIO], None], *, wait_if_locked: bool = True) -> Path:
    """Return the XML artifact path, rebuilding if missing or stale.

    Stale files remain serveable while another process refreshes the artifact.
    """
    _ensure_cache_dir()
    path = _cache_path(cache_key, ext='xml')
    if not _xml_is_stale(cache_key, path):
        return path

    lock = _xml_lock_path(cache_key)
    lock_fd = None
    try:
        lock_fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        if path.exists():
            return path
        if not wait_if_locked:
            raise
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if path.exists() and not _xml_is_stale(cache_key, path):
                return path
            time.sleep(0.05)
        if path.exists():
            return path
        raise TimeoutError(f'timed out waiting for XML artifact {cache_key}')

    try:
        return write_xml_artifact(cache_key, writer)
    finally:
        if lock_fd is not None:
            os.close(lock_fd)
        lock.unlink(missing_ok=True)


def invalidate_xml_cache(cache_key: str | None = None) -> int:
    if cache_key is not None:
        mark_xml_stale(cache_key)
        removed = 0
        path = _cache_path(cache_key, 'm3u')
        if path.exists():
            path.unlink()
            removed += 1
        return removed

    mark_xml_stale()
    if not _CACHE_ROOT.exists():
        return 0

    removed = 0
    for path in _CACHE_ROOT.glob('*.m3u'):
        if path.is_file():
            path.unlink(missing_ok=True)
            removed += 1
    return removed

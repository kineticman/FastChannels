"""
Image proxy — caches remote logo/poster images locally so clients
(e.g. Channels DVR) fetch from us instead of hitting source CDNs directly.

Cache layout:
  /data/logo_cache/logos/    — channel station logos  (3-day TTL)
  /data/logo_cache/posters/  — programme artwork       (kept until program ends + 2h,
                                                         enforced by worker DB query;
                                                         4-day safety-net TTL as fallback)

On cache miss the image is fetched inline and served directly — no redirect.
Under gevent workers the outbound fetch yields to other greenlets so it does
not block concurrent requests.  The background worker pre-warms logo cache
after each scrape so most logo requests are cache hits.
"""
import hashlib
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests as _req
from flask import Blueprint, Response, abort, request

logger = logging.getLogger(__name__)

images_bp = Blueprint('images', __name__)

_LOGO_DIR     = '/data/logo_cache/logos'
_POSTER_DIR   = '/data/logo_cache/posters'
_LOGO_TTL     = 3 * 24 * 60 * 60   # 3 days
_POSTER_TTL   = 4 * 24 * 60 * 60   # safety-net; primary expiry is DB-driven
_PREWARM_WORKERS = 8


def _cache_dir(img_type: str) -> str:
    return _POSTER_DIR if img_type == 'poster' else _LOGO_DIR


def _cache_paths(url: str, img_type: str = 'logo') -> tuple[str, str]:
    key = hashlib.md5(url.encode()).hexdigest()
    d = _cache_dir(img_type)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, key), os.path.join(d, key + '.ct')


def _is_fresh(img_path: str, ttl: int) -> bool:
    try:
        return (time.time() - os.path.getmtime(img_path)) < ttl
    except OSError:
        return False


def _fetch_and_cache(url: str, img_path: str, ct_path: str) -> bool:
    """Fetch *url* and write it to *img_path*/*ct_path*. Returns True on success."""
    try:
        r = _req.get(url, timeout=10, headers={'User-Agent': 'FastChannels/1.0'})
        if not r.ok:
            logger.debug('[images] fetch HTTP %s for %s', r.status_code, url)
            return False
        content_type = (r.headers.get('content-type') or 'image/jpeg').split(';')[0].strip()
        with open(img_path, 'wb') as f:
            f.write(r.content)
        with open(ct_path, 'w') as f:
            f.write(content_type)
        return True
    except Exception as exc:
        logger.debug('[images] fetch failed for %s: %s', url, exc)
        return False


def _image_response(img_path: str, content_type: str, ttl: int) -> Response:
    """Return a plain image response — no Content-Disposition, no ETag magic."""
    with open(img_path, 'rb') as f:
        data = f.read()
    return Response(
        data,
        status=200,
        mimetype=content_type,
        headers={
            'Content-Length': str(len(data)),
            'Cache-Control': f'public, max-age={ttl}',
            'Connection': 'close',
        },
    )


@images_bp.route('/images/proxy/<img_type>/image.<ext>')
def proxy_image(img_type='logo', ext='jpg'):
    url = request.args.get('url', '').strip()
    if not url:
        abort(400)

    ttl = _POSTER_TTL if img_type == 'poster' else _LOGO_TTL
    img_path, ct_path = _cache_paths(url, img_type)

    if _is_fresh(img_path, ttl) and os.path.exists(ct_path):
        logger.debug('[images] cache hit (%s): %s', img_type, url)
        content_type = open(ct_path).read().strip() or 'image/jpeg'
        return _image_response(img_path, content_type, ttl)

    logger.debug('[images] cache miss (%s): %s', img_type, url)
    if _fetch_and_cache(url, img_path, ct_path):
        content_type = open(ct_path).read().strip() or 'image/jpeg'
        return _image_response(img_path, content_type, ttl)

    abort(404)


def cache_logo(url: str, img_type: str = 'logo') -> bool:
    """
    Fetch *url* and store it in the cache.  Returns True on success.
    Skips URLs that are already cached and fresh.
    """
    if not url:
        return False
    ttl = _POSTER_TTL if img_type == 'poster' else _LOGO_TTL
    img_path, ct_path = _cache_paths(url, img_type)
    if _is_fresh(img_path, ttl):
        return True
    return _fetch_and_cache(url, img_path, ct_path)


def prewarm_logo_cache(urls: list[str]) -> tuple[int, int]:
    """
    Download channel logo *urls* into the logo cache using a thread pool.
    Skips URLs already cached and fresh.  Returns (cached, failed) counts.
    """
    urls = [u for u in urls if u]
    if not urls:
        return 0, 0
    stale, skipped = [], 0
    for u in urls:
        img_path, _ = _cache_paths(u, 'logo')
        if _is_fresh(img_path, _LOGO_TTL):
            skipped += 1
        else:
            stale.append(u)
    total = len(urls)
    logger.info('[images] pre-warm starting: %d URLs — %d already fresh, %d to fetch (%d workers)',
                total, skipped, len(stale), _PREWARM_WORKERS)
    cached = failed = 0
    with ThreadPoolExecutor(max_workers=_PREWARM_WORKERS) as pool:
        futures = {pool.submit(cache_logo, u, 'logo'): u for u in stale}
        for fut in as_completed(futures):
            if fut.result():
                cached += 1
            else:
                failed += 1
            done = cached + failed
            if done % 100 == 0:
                logger.info('[images] pre-warm progress: %d/%d fetched (cached=%d failed=%d)',
                            done, len(stale), cached, failed)
    logger.info('[images] pre-warm done: %d cached, %d already fresh, %d failed (of %d total)',
                cached, skipped, failed, total)
    return cached, failed


def _cleanup_dir(directory: str, ttl: int) -> int:
    """Delete files in *directory* older than *ttl* seconds. Returns count removed."""
    if not os.path.exists(directory):
        return 0
    cutoff = time.time() - ttl
    removed = 0
    for fname in os.listdir(directory):
        fpath = os.path.join(directory, fname)
        try:
            if os.path.getmtime(fpath) < cutoff:
                os.unlink(fpath)
                removed += 1
        except OSError:
            pass
    return removed


def cleanup_logo_cache() -> int:
    """Delete logo cache files older than _LOGO_TTL. Returns count removed."""
    return _cleanup_dir(_LOGO_DIR, _LOGO_TTL)


def cleanup_poster_cache(expired_urls: list[str]) -> int:
    """
    Delete cached poster files for programs whose end_time has passed.
    *expired_urls* is a list of poster_url values from the DB query in worker.py.
    Also prunes any poster files older than _POSTER_TTL as a safety net.
    Returns total count removed.
    """
    removed = 0
    for url in expired_urls:
        if not url:
            continue
        img_path, ct_path = _cache_paths(url, 'poster')
        for p in (img_path, ct_path):
            try:
                os.unlink(p)
                removed += 1
            except OSError:
                pass
    removed += _cleanup_dir(_POSTER_DIR, _POSTER_TTL)
    return removed

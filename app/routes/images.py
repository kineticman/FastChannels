"""
Image proxy — caches remote logo/poster images locally so clients
(e.g. Channels DVR) fetch from us instead of hitting source CDNs directly.

Cache location : /data/logo_cache/
TTL            : 24 hours (enforced by scheduled cleanup in worker)

On cache miss the proxy redirects the client to the original URL rather than
blocking a gunicorn worker on an outbound fetch.  The background worker
pre-warms the cache after each scrape so most requests are cache hits.
"""
import hashlib
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests as _req
from flask import Blueprint, Response, abort, redirect, request

logger = logging.getLogger(__name__)

images_bp = Blueprint('images', __name__)

_CACHE_DIR = '/data/logo_cache'
_CACHE_TTL  = 7 * 24 * 60 * 60  # seconds (7 days — logos rarely change)
_PREWARM_WORKERS = 8


def _cache_paths(url: str) -> tuple[str, str]:
    key = hashlib.md5(url.encode()).hexdigest()
    os.makedirs(_CACHE_DIR, exist_ok=True)
    return os.path.join(_CACHE_DIR, key), os.path.join(_CACHE_DIR, key + '.ct')


def _is_fresh(img_path: str) -> bool:
    try:
        return (time.time() - os.path.getmtime(img_path)) < _CACHE_TTL
    except OSError:
        return False


@images_bp.route('/images/proxy')
def proxy_image():
    url = request.args.get('url', '').strip()
    if not url:
        abort(400)

    img_path, ct_path = _cache_paths(url)

    # Serve from cache if fresh
    if _is_fresh(img_path) and os.path.exists(ct_path):
        content_type = open(ct_path).read().strip() or 'image/jpeg'
        logger.debug('[images] cache hit: %s', url)
        return Response(
            open(img_path, 'rb').read(),
            content_type=content_type,
            headers={
                'Cache-Control': f'public, max-age={_CACHE_TTL}',
                'Connection': 'close',
            },
        )

    # Cache miss — redirect to origin so the client isn't blocked waiting on
    # our outbound fetch.  The worker pre-warms the cache after each scrape so
    # subsequent requests are served from disk.
    logger.debug('[images] cache miss, redirecting: %s', url)
    resp = redirect(url, code=302)
    resp.headers['Connection'] = 'close'
    return resp


def cache_logo(url: str) -> bool:
    """
    Fetch *url* and store it in the logo cache.  Returns True on success.
    Skips URLs that are already cached and fresh.
    """
    if not url:
        return False
    img_path, ct_path = _cache_paths(url)
    if _is_fresh(img_path):
        return True
    try:
        r = _req.get(url, timeout=10, headers={'User-Agent': 'FastChannels/1.0'})
        if not r.ok:
            logger.debug('[images] cache_logo HTTP %s for %s', r.status_code, url)
            return False
        content_type = (r.headers.get('content-type') or 'image/jpeg').split(';')[0].strip()
        with open(img_path, 'wb') as f:
            f.write(r.content)
        with open(ct_path, 'w') as f:
            f.write(content_type)
        return True
    except Exception as exc:
        logger.debug('[images] cache_logo failed for %s: %s', url, exc)
        return False


def prewarm_logo_cache(urls: list[str]) -> tuple[int, int]:
    """
    Download *urls* into the logo cache using a thread pool.
    Skips URLs already cached and fresh.
    Returns (cached, failed) counts.
    """
    urls = [u for u in urls if u]
    if not urls:
        return 0, 0
    # Split into already-fresh (skip) and stale/missing (fetch)
    stale, skipped = [], 0
    for u in urls:
        img_path, _ = _cache_paths(u)
        if _is_fresh(img_path):
            skipped += 1
        else:
            stale.append(u)
    total = len(urls)
    logger.info('[images] pre-warm starting: %d URLs — %d already fresh, %d to fetch (%d workers)',
                total, skipped, len(stale), _PREWARM_WORKERS)
    cached = failed = 0
    with ThreadPoolExecutor(max_workers=_PREWARM_WORKERS) as pool:
        futures = {pool.submit(cache_logo, u): u for u in stale}
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


def cleanup_logo_cache() -> int:
    """Delete cache files older than _CACHE_TTL. Returns count removed."""
    if not os.path.exists(_CACHE_DIR):
        return 0
    cutoff = time.time() - _CACHE_TTL
    removed = 0
    for fname in os.listdir(_CACHE_DIR):
        fpath = os.path.join(_CACHE_DIR, fname)
        try:
            if os.path.getmtime(fpath) < cutoff:
                os.unlink(fpath)
                removed += 1
        except OSError:
            pass
    return removed

"""
Image proxy — caches remote logo/poster images locally so clients
(e.g. Channels DVR) fetch from us instead of hitting source CDNs directly.

Cache location : /data/logo_cache/
TTL            : 24 hours (enforced by scheduled cleanup in worker)
"""
import hashlib
import logging
import os
import time

import requests as _req
from flask import Blueprint, Response, abort, request

logger = logging.getLogger(__name__)

images_bp = Blueprint('images', __name__)

_CACHE_DIR = '/data/logo_cache'
_CACHE_TTL  = 24 * 60 * 60  # seconds


def _cache_paths(url: str) -> tuple[str, str]:
    key = hashlib.md5(url.encode()).hexdigest()
    os.makedirs(_CACHE_DIR, exist_ok=True)
    return os.path.join(_CACHE_DIR, key), os.path.join(_CACHE_DIR, key + '.ct')


@images_bp.route('/images/proxy')
def proxy_image():
    url = request.args.get('url', '').strip()
    if not url:
        abort(400)

    img_path, ct_path = _cache_paths(url)

    # Serve from cache if fresh
    if os.path.exists(img_path) and os.path.exists(ct_path):
        if (time.time() - os.path.getmtime(img_path)) < _CACHE_TTL:
            content_type = open(ct_path).read().strip() or 'image/jpeg'
            return Response(
                open(img_path, 'rb').read(),
                content_type=content_type,
                headers={'Cache-Control': f'public, max-age={_CACHE_TTL}'},
            )

    # Fetch from remote
    try:
        r = _req.get(url, timeout=10, headers={'User-Agent': 'FastChannels/1.0'})
        if not r.ok:
            abort(r.status_code)
        content_type = (r.headers.get('content-type') or 'image/jpeg').split(';')[0].strip()
        data = r.content
    except _req.RequestException as exc:
        logger.debug('[images] proxy fetch failed for %s: %s', url, exc)
        abort(502)

    # Write to cache (non-fatal on failure)
    try:
        with open(img_path, 'wb') as f:
            f.write(data)
        with open(ct_path, 'w') as f:
            f.write(content_type)
    except OSError as exc:
        logger.warning('[images] cache write failed: %s', exc)

    return Response(
        data,
        content_type=content_type,
        headers={'Cache-Control': f'public, max-age={_CACHE_TTL}'},
    )


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

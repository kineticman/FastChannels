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
import io
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests as _req
from flask import Blueprint, Response, abort, request, send_file
from PIL import Image

logger = logging.getLogger(__name__)

images_bp = Blueprint('images', __name__)

_LOGO_DIR     = '/data/logo_cache/logos'
_POSTER_DIR   = '/data/logo_cache/posters'
_LOGO_TTL     = 3 * 24 * 60 * 60   # 3 days
_POSTER_TTL   = 4 * 24 * 60 * 60   # safety-net; primary expiry is DB-driven
_PREWARM_WORKERS = 4
_LOGO_MAX_BYTES = 150 * 1024

# Channels DVR logo constraints (community-confirmed):
#   - max ~150 KB file size; oversized logos cause silent failures / crashes
#   - recommended 720x540 (4:3) with padding; 1:1 squares also work
#   - PNG preferred; WebP/SVG unsupported by native apps
_LOGO_TARGET  = (360, 270)  # final canvas size (4:3, well under 150 KB)
_LOGO_SAFE_MAX = (720, 540)


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


def _normalize_logo(data: bytes, source_content_type: str | None = None) -> tuple[bytes, str]:
    """
    Resize and reformat a logo image for Channels DVR compatibility.

    Strategy:
    - If the source is already a reasonably sized PNG/JPEG and under the
      safety byte limit, keep it as-is to avoid shrinking wide logos.
    - Otherwise fit it inside the 4:3 target box (not a square box), pad to
      _LOGO_TARGET, strip metadata, and save as PNG.

    Returns (image_bytes, content_type). Falls back to original data/content-type
    on any error so a bad image never causes a cache miss.
    """
    try:
        img = Image.open(io.BytesIO(data))
        source_format = (img.format or '').upper()
        safe_ct = (source_content_type or '').split(';')[0].strip().lower()

        # Leave already-good assets alone. This avoids making some wide logos
        # visibly smaller just to force them onto a padded 4:3 canvas.
        if (
            source_format in {'PNG', 'JPEG', 'JPG'}
            and safe_ct in {'image/png', 'image/jpeg'}
            and len(data) <= _LOGO_MAX_BYTES
            and img.width <= _LOGO_SAFE_MAX[0]
            and img.height <= _LOGO_SAFE_MAX[1]
        ):
            return data, safe_ct or ('image/png' if source_format == 'PNG' else 'image/jpeg')

        # Convert to RGBA so transparency padding works for all source modes.
        img = img.convert('RGBA')
        img.thumbnail(_LOGO_TARGET, Image.LANCZOS)
        canvas = Image.new('RGBA', _LOGO_TARGET, (0, 0, 0, 0))
        x = (_LOGO_TARGET[0] - img.width)  // 2
        y = (_LOGO_TARGET[1] - img.height) // 2
        canvas.paste(img, (x, y), img)
        buf = io.BytesIO()
        canvas.save(buf, format='PNG', optimize=True, compress_level=9)
        return buf.getvalue(), 'image/png'
    except Exception as exc:
        logger.debug('[images] logo normalize failed: %s', exc)
        return data, 'image/jpeg'


def _fetch_and_cache(url: str, img_path: str, ct_path: str,
                     img_type: str = 'logo') -> bool:
    """Fetch *url* and write it to *img_path*/*ct_path*. Returns True on success."""
    try:
        r = _req.get(url, timeout=10, headers={'User-Agent': 'FastChannels/1.0'})
        if not r.ok:
            logger.debug('[images] fetch HTTP %s for %s', r.status_code, url)
            return False
        content_type = (r.headers.get('content-type') or 'image/jpeg').split(';')[0].strip()
        data = r.content
        if img_type == 'logo':
            data, content_type = _normalize_logo(data, content_type)
        with open(img_path, 'wb') as f:
            f.write(data)
        with open(ct_path, 'w') as f:
            f.write(content_type)
        url_path = img_path + '.url'
        with open(url_path, 'w') as f:
            f.write(url)
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


def _resolve_poster_url_from_db(key: str) -> str | None:
    """Best-effort lookup for a Roku poster URL when an old XML artifact references /posters/<hash>.

    XML artifacts can outlive the poster cache itself, so a client may request a
    stale static poster URL after the cache has been cleared. In that case, walk
    the currently relevant Roku poster URLs and reconstruct the original URL
    from the md5 hash.
    """
    try:
        from datetime import datetime, timedelta, timezone
        from app.extensions import db
        from app.models import Program, Channel, Source

        now = datetime.now(timezone.utc)
        window_end = now + timedelta(days=5)
        rows = (
            db.session.query(Program.poster_url)
            .join(Channel, Program.channel_id == Channel.id)
            .join(Source, Channel.source_id == Source.id)
            .filter(
                Source.name == 'roku',
                Program.poster_url.isnot(None),
                Program.end_time > now - timedelta(hours=2),
                Program.start_time < window_end,
            )
            .distinct()
            .yield_per(500)
        )
        for (url,) in rows:
            if url and hashlib.md5(url.encode()).hexdigest() == key:
                return url
    except Exception:
        logger.exception('[images] poster URL lookup failed for key=%s', key)
    return None


def _prime_hash_cache_from_lookup(key: str, img_type: str) -> tuple[str | None, str, str]:
    d = _cache_dir(img_type)
    img_path = os.path.join(d, key)
    ct_path = os.path.join(d, key + '.ct')
    url_path = os.path.join(d, key + '.url')
    if os.path.exists(url_path):
        try:
            return open(url_path).read().strip(), img_path, ct_path
        except Exception:
            return None, img_path, ct_path
    if img_type != 'poster':
        return None, img_path, ct_path
    url = _resolve_poster_url_from_db(key)
    if not url:
        return None, img_path, ct_path
    try:
        with open(url_path, 'w') as f:
            f.write(url)
    except Exception:
        logger.debug('[images] could not write poster sidecar for key=%s', key)
    return url, img_path, ct_path


@images_bp.route('/logos/<filename>')
def serve_logo_static(filename):
    """Serve a cached channel logo as a static file — no proxy, no fetching."""
    if '.' not in filename:
        abort(404)
    key      = filename.rsplit('.', 1)[0]
    img_path = os.path.join(_LOGO_DIR, key)
    ct_path  = os.path.join(_LOGO_DIR, key + '.ct')
    if not os.path.exists(img_path):
        abort(404)
    content_type = open(ct_path).read().strip() if os.path.exists(ct_path) else 'image/jpeg'
    return send_file(img_path, mimetype=content_type or 'image/jpeg',
                     download_name=filename, max_age=_LOGO_TTL, conditional=True)


@images_bp.route('/posters/<filename>')
def serve_poster_static(filename):
    """Serve a cached poster image as a static file — no proxy, no fetching."""
    if '.' not in filename:
        abort(404)
    key      = filename.rsplit('.', 1)[0]
    img_path = os.path.join(_POSTER_DIR, key)
    ct_path  = os.path.join(_POSTER_DIR, key + '.ct')
    if not os.path.exists(img_path):
        url, img_path, ct_path = _prime_hash_cache_from_lookup(key, 'poster')
        if not url or not _fetch_and_cache(url, img_path, ct_path, 'poster'):
            abort(404)
    content_type = open(ct_path).read().strip() if os.path.exists(ct_path) else 'image/jpeg'
    return send_file(img_path, mimetype=content_type or 'image/jpeg',
                     download_name=filename, max_age=_POSTER_TTL, conditional=True)


@images_bp.route('/images/proxy/<img_type>/<hash_ext>')
def proxy_image(img_type='logo', hash_ext=''):
    """Hash-based image proxy — URL ends cleanly in an image extension.

    hash_ext is "{md5_of_original_url}.{ext}".  The original URL is read from
    a .url sidecar file written by `_fetch_and_cache()` after the first successful
    fetch so later cache misses can be refreshed without query-string URLs.
    """
    if '.' not in hash_ext:
        abort(400)
    key = hash_ext.rsplit('.', 1)[0]

    ttl = _POSTER_TTL if img_type == 'poster' else _LOGO_TTL
    d = _cache_dir(img_type)
    img_path = os.path.join(d, key)
    ct_path  = os.path.join(d, key + '.ct')
    url_path = os.path.join(d, key + '.url')

    if _is_fresh(img_path, ttl) and os.path.exists(ct_path):
        logger.debug('[images] cache hit (%s): %s', img_type, key)
        content_type = open(ct_path).read().strip() or 'image/jpeg'
        return _image_response(img_path, content_type, ttl)

    url = None
    if os.path.exists(url_path):
        url = open(url_path).read().strip()
    else:
        url, img_path, ct_path = _prime_hash_cache_from_lookup(key, img_type)
    if not url:
        abort(404)

    logger.debug('[images] cache miss (%s): %s', img_type, key)
    if _fetch_and_cache(url, img_path, ct_path, img_type):
        content_type = open(ct_path).read().strip() or 'image/jpeg'
        return _image_response(img_path, content_type, ttl)

    abort(404)


@images_bp.route('/images/proxy/<img_type>/image.<ext>')
def proxy_image_legacy(img_type='logo', ext='jpg'):
    """Legacy query-param route — kept for backward compat with cached M3U/EPG output."""
    url = request.args.get('url', '').strip()
    if not url:
        abort(400)

    ttl = _POSTER_TTL if img_type == 'poster' else _LOGO_TTL
    img_path, ct_path = _cache_paths(url, img_type)

    if _is_fresh(img_path, ttl) and os.path.exists(ct_path):
        content_type = open(ct_path).read().strip() or 'image/jpeg'
        return _image_response(img_path, content_type, ttl)

    if _fetch_and_cache(url, img_path, ct_path, img_type):
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
    return _fetch_and_cache(url, img_path, ct_path, img_type)


def prewarm_logo_cache(urls: list[str]) -> tuple[int, int]:
    """
    Download channel logo *urls* into the logo cache using a thread pool.
    Skips URLs already cached and fresh.  Returns (cached, failed) counts.
    """
    urls = [u for u in urls if u]
    if not urls:
        return 0, 0
    # Avoid redundant freshness checks and duplicate fetches when multiple
    # channels share the same logo URL.
    urls = list(dict.fromkeys(urls))
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

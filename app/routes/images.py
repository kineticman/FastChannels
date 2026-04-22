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
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests as _req
from requests.adapters import HTTPAdapter
from flask import Blueprint, Response, abort, request, send_file
from PIL import Image

logger = logging.getLogger(__name__)

images_bp = Blueprint('images', __name__)

# urllib3.connection logs WARNINGs for malformed response headers (e.g.
# NoBoundaryInMultipartDefect from some CDNs).  Responses succeed despite the
# malformed Content-Type, so these are pure noise.  Raise the level to ERROR
# so only genuine connection-level failures surface.
logging.getLogger('urllib3.connection').setLevel(logging.ERROR)

_session = _req.Session()
_session.mount("http://", HTTPAdapter())
_session.mount("https://", HTTPAdapter())

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


def _normalize_logo(data: bytes, source_content_type: str | None = None) -> tuple[bytes, str] | None:
    """
    Resize and reformat a logo image for Channels DVR compatibility.

    Strategy:
    - If the source is already a reasonably sized PNG/JPEG and under the
      safety byte limit, keep it as-is to avoid shrinking wide logos.
    - Otherwise fit it inside the 4:3 target box (not a square box), pad to
      _LOGO_TARGET, strip metadata, and save as PNG.

    Returns (image_bytes, content_type), or None if the data is not a valid
    decodable image (e.g. HTML error page, corrupt file, unsupported format).
    Returning None prevents bad content from being written to the cache.
    """
    try:
        img = Image.open(io.BytesIO(data))
        img.verify()  # catches truncated / corrupt files
        # Re-open after verify() — verify() leaves the file pointer in an unusable state.
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
        logger.debug('[images] logo normalize failed (invalid image data): %s', exc)
        return None


def _fetch_and_cache(url: str, img_path: str, ct_path: str,
                     img_type: str = 'logo') -> bool:
    """Fetch *url* and write it to *img_path*/*ct_path*. Returns True on success."""
    try:
        r = _session.get(url, timeout=10, headers={'User-Agent': 'FastChannels/1.0'})
        if not r.ok:
            logger.debug('[images] fetch HTTP %s for %s', r.status_code, url)
            return False
        content_type = (r.headers.get('content-type') or 'image/jpeg').split(';')[0].strip()
        data = r.content
        if img_type == 'logo':
            result = _normalize_logo(data, content_type)
            if result is None:
                logger.debug('[images] skipping cache — invalid image data from %s', url)
                return False
            data, content_type = result
        cache_dir = os.path.dirname(img_path)
        fd, tmp_path = tempfile.mkstemp(dir=cache_dir)
        try:
            with os.fdopen(fd, 'wb') as f:
                f.write(data)
            os.replace(tmp_path, img_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
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


def delete_cached_logo(url: str) -> None:
    """Delete all cached files (image, content-type, url sidecar) for a logo URL."""
    if not url:
        return
    img_path, ct_path = _cache_paths(url, 'logo')
    for path in (img_path, ct_path, img_path + '.url'):
        try:
            os.unlink(path)
        except OSError:
            pass


def sweep_orphaned_logos(active_urls: list[str]) -> int:
    """Delete cached logo files whose URL is no longer present in the DB.

    active_urls: every distinct logo_url currently stored on Channel rows.
    Returns count of files removed (counting each sidecar separately).
    """
    if not os.path.exists(_LOGO_DIR):
        return 0
    active_keys = {hashlib.md5(u.encode()).hexdigest() for u in active_urls if u}
    removed = 0
    try:
        for fname in os.listdir(_LOGO_DIR):
            if fname.endswith(('.ct', '.url')):
                continue  # sidecars — handled together with their parent
            if fname not in active_keys:
                for suffix in ('', '.ct', '.url'):
                    fpath = os.path.join(_LOGO_DIR, fname + suffix)
                    try:
                        os.unlink(fpath)
                        removed += 1
                    except OSError:
                        pass
    except OSError:
        pass
    return removed


def cache_logo(url: str, img_type: str = 'logo') -> bool:
    """
    Fetch *url* and store it in the cache.  Returns True on success.
    For logos: skips if the file already exists (URL-driven expiry — the file
    is only removed when the channel's logo URL changes or the channel is gone).
    For posters: uses TTL-based freshness.
    """
    if not url:
        return False
    img_path, ct_path = _cache_paths(url, img_type)
    if img_type == 'logo':
        if os.path.exists(img_path):
            return True
    else:
        if _is_fresh(img_path, _POSTER_TTL):
            return True
    return _fetch_and_cache(url, img_path, ct_path, img_type)


def prewarm_logo_cache(urls: list[str], progress_cb=None) -> tuple[int, int]:
    """
    Download channel logo *urls* into the logo cache using a thread pool.
    Skips URLs already cached and fresh.  Returns (cached, failed) counts.
    progress_cb(done, total), if provided, is called after each completed fetch.
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
        if os.path.exists(img_path):
            skipped += 1
        else:
            stale.append(u)
    total = len(urls)
    logger.info('[images] pre-warm starting: %d URLs — %d already fresh, %d to fetch (%d workers)',
                total, skipped, len(stale), _PREWARM_WORKERS)
    if progress_cb:
        progress_cb(skipped, total)
    cached = failed = 0
    with ThreadPoolExecutor(max_workers=_PREWARM_WORKERS) as pool:
        futures = {pool.submit(cache_logo, u, 'logo'): u for u in stale}
        for fut in as_completed(futures):
            if fut.result():
                cached += 1
            else:
                failed += 1
            done = skipped + cached + failed
            if progress_cb:
                progress_cb(done, total)
            if (cached + failed) % 100 == 0:
                logger.info('[images] pre-warm progress: %d/%d fetched (cached=%d failed=%d)',
                            cached + failed, len(stale), cached, failed)
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
    """Replaced by sweep_orphaned_logos — logos are now expired by URL change, not TTL."""
    return 0


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

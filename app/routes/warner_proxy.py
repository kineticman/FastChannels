"""Warner TVE HLS proxy for the AES-128 channels (TBS/TNT/truTV/TCM).

Every URL under the master carries an Akamai `hdntl` token whose HMAC covers
its literal text, lowercase `%2f` included. python-requests, urllib3 and ffmpeg
all normalize that to `%2F` and get a 403, so a plain 302 only works in
clients that send URLs untouched (Channels DVR does; Jellyfin/Emby/Plex don't).
Here the master, variants, AES keys and segments all come through us, fetched
with http.client, which sends the request target byte for byte. See
app.scrapers.warner_tve.
"""
import http.client
import logging
import re
import time as _time
from urllib.parse import quote, unquote, urljoin, urlsplit

from flask import Blueprint, Response, abort, current_app, request

from app.config_store import persist_source_cache_updates, persist_source_config_updates
from ..models import Channel, Source
from ..scrapers import registry
from ..scrapers.warner_tve import CHANNELS, UA, aes_cdn_url_allowed

logger = logging.getLogger(__name__)
warner_proxy_bp = Blueprint('warner_proxy', __name__)
_unavailable_response = None
_gone_response = None
_refresh_stream_info_async = None
_channel_is_retired = None

_URI_RE = re.compile(r'URI="([^"]+)"')
# Key URL → (key bytes, fetched at). Keys rotate every few minutes and each
# rotation brings a new URL; players re-fetch them on every playlist reload.
_KEYS: dict[str, tuple[bytes, float]] = {}
_KEY_TTL = 1800
_CHUNK = 64 * 1024


def configure_warner_proxy(*, unavailable_response, gone_response, refresh_stream_info_async,
                           channel_is_retired) -> None:
    global _unavailable_response, _gone_response, _refresh_stream_info_async, _channel_is_retired
    _unavailable_response = unavailable_response
    _gone_response = gone_response
    _refresh_stream_info_async = refresh_stream_info_async
    _channel_is_retired = channel_is_retired


def _open(url: str, timeout: float = 10):
    """GET url sending its path and query exactly as given. Caller closes the connection."""
    parts = urlsplit(url)
    target = parts.path + (f'?{parts.query}' if parts.query else '')
    conn = http.client.HTTPSConnection(parts.hostname, parts.port or 443, timeout=timeout)
    try:
        conn.request('GET', target, headers={'User-Agent': UA, 'Accept': '*/*'})
        return conn, conn.getresponse()
    except Exception:
        conn.close()
        raise


def _get(url: str, timeout: float = 10) -> tuple[int, bytes]:
    conn, resp = _open(url, timeout)
    try:
        return resp.status, resp.read()
    finally:
        conn.close()


def _proxy_url(kind: str, channel_id: str, url: str) -> str:
    return (f'{request.host_url.rstrip("/")}/play/warner_tve/{quote(channel_id, safe="")}/{kind}'
            f'?url={quote(url, safe="")}')


def _playlist_response(body: str) -> Response:
    return Response(body, mimetype='application/vnd.apple.mpegurl',
                    headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'})


def _upstream_arg() -> str:
    url = request.args.get('url', '')
    if not aes_cdn_url_allowed(url):
        abort(403)
    return url


def _persist(channel: Channel, scraper) -> None:
    for attr, persist in (('_pending_config_updates', persist_source_config_updates),
                          ('_pending_cache_updates', persist_source_cache_updates)):
        if getattr(scraper, attr, None):
            try:
                persist(channel.source_id, getattr(scraper, attr))
            except Exception as exc:  # noqa: BLE001
                from ..extensions import db
                db.session.rollback()
                logger.warning('[warner-tve] failed to persist %s: %s', attr, exc)


@warner_proxy_bp.route('/play/warner_tve/<channel_id>/proxy.m3u8')
def warner_proxy(channel_id: str):
    raw_id = unquote(channel_id)
    if raw_id not in CHANNELS:
        abort(404)
    channel = (Channel.query.join(Source)
               .filter(Source.name == 'warner_tve', Channel.source_channel_id == raw_id)
               .first())
    if not channel:
        abort(404)
    if _channel_is_retired(channel):
        return _gone_response()
    scraper_cls = registry.get('warner_tve')
    if not scraper_cls:
        return _unavailable_response()
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        master_url = scraper.resolve(channel.stream_url)
    except Exception as exc:  # noqa: BLE001
        logger.warning('[warner-tve] resolve failed for %s: %s', raw_id, exc)
        return _unavailable_response()
    finally:
        _persist(channel, scraper)
    if not aes_cdn_url_allowed(master_url):
        logger.warning('[warner-tve] unexpected master host for %s: %s', raw_id, urlsplit(master_url).hostname)
        return _unavailable_response()
    try:
        status, body = _get(master_url)
    except Exception as exc:  # noqa: BLE001
        logger.warning('[warner-tve] master fetch failed for %s: %s', raw_id, exc)
        return _unavailable_response()
    if status != 200:
        logger.warning('[warner-tve] master for %s answered HTTP %s', raw_id, status)
        return _unavailable_response()
    text = body.decode('utf-8', 'replace')
    _refresh_stream_info_async(current_app._get_current_object(), channel.id, channel.stream_info, text)

    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            line = _proxy_url('variant.m3u8', raw_id, urljoin(master_url, stripped))
        elif stripped.startswith(('#EXT-X-MEDIA', '#EXT-X-I-FRAME-STREAM-INF')) and 'URI=' in stripped:
            line = _URI_RE.sub(
                lambda m: f'URI="{_proxy_url("variant.m3u8", raw_id, urljoin(master_url, m.group(1)))}"', line)
        lines.append(line)
    return _playlist_response('\n'.join(lines) + '\n')


@warner_proxy_bp.route('/play/warner_tve/<channel_id>/variant.m3u8')
def warner_variant_proxy(channel_id: str):
    url = _upstream_arg()
    raw_id = unquote(channel_id)
    try:
        status, body = _get(url)
    except Exception as exc:  # noqa: BLE001
        logger.warning('[warner-tve] variant fetch failed for %s: %s', raw_id, exc)
        abort(502)
    if status != 200:
        logger.warning('[warner-tve] variant for %s answered HTTP %s', raw_id, status)
        abort(502)

    def _key_uri(m):
        return f'URI="{_proxy_url("key", raw_id, urljoin(url, m.group(1)))}"'

    lines = []
    for line in body.decode('utf-8', 'replace').splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            line = _proxy_url('segment.ts', raw_id, urljoin(url, stripped))
        elif stripped.startswith('#EXT-X-KEY') and 'URI=' in stripped:
            line = _URI_RE.sub(_key_uri, line)
        elif stripped.startswith('#EXT-X-MAP') and 'URI=' in stripped:
            line = _URI_RE.sub(lambda m: f'URI="{_proxy_url("segment.ts", raw_id, urljoin(url, m.group(1)))}"', line)
        lines.append(line)
    return _playlist_response('\n'.join(lines) + '\n')


@warner_proxy_bp.route('/play/warner_tve/<channel_id>/key')
def warner_key_proxy(channel_id: str):
    url = _upstream_arg()
    cached = _KEYS.get(url)
    if cached and _time.time() - cached[1] < _KEY_TTL:
        key = cached[0]
    else:
        try:
            status, key = _get(url)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[warner-tve] key fetch failed for %s: %s', unquote(channel_id), exc)
            abort(502)
        if status != 200 or len(key) != 16:
            logger.warning('[warner-tve] key for %s answered HTTP %s (%d bytes)',
                           unquote(channel_id), status, len(key))
            abort(502)
        now = _time.time()
        for stale in [u for u, (_k, t) in _KEYS.items() if now - t >= _KEY_TTL]:
            _KEYS.pop(stale, None)
        _KEYS[url] = (key, now)
    return Response(key, content_type='application/octet-stream',
                    headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'})


@warner_proxy_bp.route('/play/warner_tve/<channel_id>/segment.ts')
def warner_segment_proxy(channel_id: str):
    url = _upstream_arg()
    try:
        conn, resp = _open(url, timeout=20)
    except Exception as exc:  # noqa: BLE001
        logger.warning('[warner-tve] segment fetch failed for %s: %s', unquote(channel_id), exc)
        abort(502)
    if resp.status != 200:
        conn.close()
        logger.warning('[warner-tve] segment for %s answered HTTP %s', unquote(channel_id), resp.status)
        abort(502)

    def generate():
        try:
            while True:
                chunk = resp.read(_CHUNK)
                if not chunk:
                    break
                yield chunk
        except Exception as exc:  # noqa: BLE001
            logger.warning('[warner-tve] segment stream interrupted for %s: %s', unquote(channel_id), exc)
        finally:
            conn.close()

    headers = {'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'}
    length = resp.getheader('Content-Length')
    if length:
        headers['Content-Length'] = length
    return Response(generate(), content_type=resp.getheader('Content-Type') or 'video/mp2t',
                    headers=headers, direct_passthrough=True)

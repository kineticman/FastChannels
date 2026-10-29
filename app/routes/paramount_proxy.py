"""Paramount+ HLS proxy: match playlists with the AES-128 key routed through us.

The key server wants the account's Bearer token, which players can't send, so
the master and media playlists come through here and each #EXT-X-KEY URI is
pointed at /play/paramount/<id>/key. Segments stay absolute and load straight
from Paramount's CDN. See app.scrapers.paramount.
"""
import logging
import re
import time as _time
from urllib.parse import quote, unquote, urljoin, urlsplit

import requests as _requests
from flask import Blueprint, Response, abort, current_app, request

from app.config_store import persist_source_config_updates
from ..models import Channel, Source
from ..scrapers import registry
from ..scrapers.base import StreamDeadError
from ..scrapers.paramount import KEY_HOST, PLAYLIST_HOSTS

logger = logging.getLogger(__name__)
paramount_proxy_bp = Blueprint('paramount_proxy', __name__)
_unavailable_response = None
_gone_response = None
_refresh_stream_info_async = None
_channel_is_retired = None

_URI_RE = re.compile(r'URI="([^"]+)"')
_HTTP = _requests.Session()
# Key URL → (key bytes, fetched at). Keys are per match and fixed; players
# re-fetch them on every reload, so a short cache spares the key server.
_KEYS: dict[str, tuple[bytes, float]] = {}
_KEY_TTL = 1800


def configure_paramount_proxy(*, unavailable_response, gone_response, refresh_stream_info_async,
                              channel_is_retired) -> None:
    global _unavailable_response, _gone_response, _refresh_stream_info_async, _channel_is_retired
    _unavailable_response = unavailable_response
    _gone_response = gone_response
    _refresh_stream_info_async = refresh_stream_info_async
    _channel_is_retired = channel_is_retired


def _host(url: str) -> str:
    parts = urlsplit(url)
    return parts.netloc.split(':', 1)[0].lower() if parts.scheme == 'https' else ''


def _playlist_allowed(url: str) -> bool:
    return _host(url) in PLAYLIST_HOSTS


def _channel(channel_id: str) -> Channel:
    channel = (Channel.query.join(Source)
               .filter(Source.name == 'paramount', Channel.source_channel_id == unquote(channel_id))
               .first())
    if not channel:
        abort(404)
    return channel


def _scraper(channel: Channel):
    scraper_cls = registry.get('paramount')
    return scraper_cls(config=channel.source.config or {}) if scraper_cls else None


def _persist(channel: Channel, scraper) -> None:
    if getattr(scraper, '_pending_config_updates', None):
        try:
            persist_source_config_updates(channel.source_id, scraper._pending_config_updates, wait=False)
        except Exception as exc:  # noqa: BLE001
            from ..extensions import db
            db.session.rollback()
            logger.warning('[paramount] failed to persist config updates: %s', exc)


def _playlist_response(body: str) -> Response:
    return Response(body, mimetype='application/vnd.apple.mpegurl',
                    headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'})


def _variant_url(channel_id: str, url: str) -> str:
    return f'{request.host_url.rstrip("/")}/play/paramount/{quote(channel_id, safe="")}/variant.m3u8?url={quote(url, safe="")}'


def _key_url(channel_id: str, url: str) -> str:
    return f'{request.host_url.rstrip("/")}/play/paramount/{quote(channel_id, safe="")}/key?url={quote(url, safe="")}'


@paramount_proxy_bp.route('/play/paramount/<channel_id>/proxy.m3u8')
def paramount_proxy(channel_id: str):
    channel = _channel(channel_id)
    if _channel_is_retired(channel):
        return _gone_response()
    scraper = _scraper(channel)
    if not scraper:
        return _unavailable_response()
    try:
        master_url = scraper.resolve(channel.stream_url)
    except StreamDeadError as exc:
        logger.info('[paramount] %s: %s', channel.source_channel_id, exc)
        return _gone_response()
    except Exception as exc:  # noqa: BLE001
        logger.warning('[paramount] resolve failed for %s: %s', channel.source_channel_id, exc)
        return _unavailable_response()
    finally:
        _persist(channel, scraper)
    if not _playlist_allowed(master_url):
        logger.warning('[paramount] unexpected master host for %s: %s', channel.source_channel_id, _host(master_url))
        return _unavailable_response()
    try:
        r = _HTTP.get(master_url, timeout=10)
        r.raise_for_status()
    except _requests.RequestException as exc:
        logger.warning('[paramount] master fetch failed for %s: %s', channel.source_channel_id, exc)
        return _unavailable_response()
    _refresh_stream_info_async(current_app._get_current_object(), channel.id, channel.stream_info, r.text)

    sid = channel.source_channel_id
    lines = []
    for line in r.text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            line = _variant_url(sid, urljoin(r.url, stripped))
        elif stripped.startswith('#EXT-X-MEDIA') and 'URI=' in stripped:
            line = _URI_RE.sub(lambda m: f'URI="{_variant_url(sid, urljoin(r.url, m.group(1)))}"', line)
        lines.append(line)
    return _playlist_response('\n'.join(lines) + '\n')


@paramount_proxy_bp.route('/play/paramount/<channel_id>/variant.m3u8')
def paramount_variant_proxy(channel_id: str):
    url = request.args.get('url', '')
    if not _playlist_allowed(url):
        abort(403)
    channel = _channel(channel_id)
    try:
        r = _HTTP.get(url, timeout=10)
        r.raise_for_status()
    except _requests.RequestException as exc:
        logger.warning('[paramount] variant fetch failed for %s: %s', channel.source_channel_id, exc)
        abort(502)

    sid = channel.source_channel_id

    def _key_uri(m):
        key = urljoin(r.url, m.group(1))
        return f'URI="{_key_url(sid, key) if _host(key) == KEY_HOST else key}"'

    lines = []
    for line in r.text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            line = urljoin(r.url, stripped)
        elif stripped.startswith(('#EXT-X-KEY', '#EXT-X-MAP')) and 'URI=' in stripped:
            line = _URI_RE.sub(_key_uri, line) if stripped.startswith('#EXT-X-KEY') else \
                _URI_RE.sub(lambda m: f'URI="{urljoin(r.url, m.group(1))}"', line)
        lines.append(line)
    return _playlist_response('\n'.join(lines) + '\n')


@paramount_proxy_bp.route('/play/paramount/<channel_id>/key')
def paramount_key_proxy(channel_id: str):
    url = request.args.get('url', '')
    if _host(url) != KEY_HOST:
        abort(403)
    channel = _channel(channel_id)
    scraper = _scraper(channel)
    if not scraper or not scraper._signed_in():
        abort(503)
    cached = _KEYS.get(url)
    if cached and _time.time() - cached[1] < _KEY_TTL:
        key = cached[0]
    else:
        try:
            r = None
            for force in (False, True):
                r = _HTTP.get(url, timeout=10,
                              headers={'Authorization': scraper.key_authorization(channel.stream_url, force=force)})
                if r.status_code not in (401, 403):
                    break
        except Exception as exc:  # noqa: BLE001
            logger.warning('[paramount] key token unavailable for %s: %s', channel.source_channel_id, exc)
            abort(503)
        finally:
            _persist(channel, scraper)
        if r.status_code != 200 or len(r.content) != 16:
            logger.warning('[paramount] key fetch for %s answered HTTP %s', channel.source_channel_id, r.status_code)
            abort(502)
        key = r.content
        now = _time.time()
        # Every match and key rotation brings a new key URL; drop expired ones
        # so the cache doesn't grow for the life of the worker.
        for stale in [u for u, (_k, t) in _KEYS.items() if now - t >= _KEY_TTL]:
            _KEYS.pop(stale, None)
        _KEYS[url] = (key, now)
    return Response(key, content_type='application/octet-stream',
                    headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'})

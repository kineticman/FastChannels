"""DirecTV license identity and browser/PrismCast playback proxies."""
import json
import logging
import re
import secrets
import time as _time
from datetime import datetime, timezone
from urllib.parse import quote as _url_quote, urljoin, urlsplit

import requests as _requests
from flask import Blueprint, Response, abort, request

from app.config_store import persist_source_cache_updates, persist_source_config_updates
from ..models import AppSettings, Channel, Source
from ..scrapers import registry

logger = logging.getLogger(__name__)
directv_proxy_bp = Blueprint('directv_proxy', __name__)

_BROWSER_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/145.0.0.0 Safari/537.36'
)
_unavailable_response = None
_stream_upstream_response = None


def configure_directv_proxy(*, unavailable_response, stream_upstream_response) -> None:
    global _unavailable_response, _stream_upstream_response
    _unavailable_response = unavailable_response
    _stream_upstream_response = stream_upstream_response


def _host_in_cdn_suffix(host: str, suffix: str) -> bool:
    host, suffix = host.lower(), suffix.lower()
    return host == suffix or host.endswith('.' + suffix)


_DIRECTV_DEVICE_COOKIE = 'fc_dtv_device'
_DIRECTV_IDENTITY_KEY_PREFIX = 'directv_identity:'
_DIRECTV_DEFAULT_IDENTITY_TTL = 30 * 24 * 3600


def _directv_identity_ttl(expires_at) -> int:
    if not expires_at:
        return _DIRECTV_DEFAULT_IDENTITY_TTL
    try:
        raw = str(expires_at).strip()
        if raw.isdigit():
            ts = int(raw)
            if ts > 10_000_000_000:
                ts = ts / 1000
            ttl = int(ts - _time.time())
        else:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(raw.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ttl = int(dt.timestamp() - _time.time())
        return max(300, min(ttl, _DIRECTV_DEFAULT_IDENTITY_TTL))
    except Exception:
        return _DIRECTV_DEFAULT_IDENTITY_TTL


def _directv_device_session_id() -> tuple[str, bool]:
    sid = (request.cookies.get(_DIRECTV_DEVICE_COOKIE) or '').strip()
    if re.fullmatch(r'[A-Za-z0-9_-]{16,96}', sid):
        return sid, False
    return secrets.token_urlsafe(24), True


def _directv_identity_cache_key(source_id: int, sid: str) -> str:
    return f'{_DIRECTV_IDENTITY_KEY_PREFIX}{source_id}:{sid}'


def _directv_response(payload: bytes, status: int, sid: str, set_cookie: bool = True) -> Response:
    resp = Response(payload, status=status, content_type='application/octet-stream')
    if set_cookie:
        resp.set_cookie(
            _DIRECTV_DEVICE_COOKIE, sid, max_age=_DIRECTV_DEFAULT_IDENTITY_TTL,
            httponly=True, samesite='Lax', path='/'
        )
    return resp


def _directv_activate_identity(scraper_cls, cfg: dict, challenge: bytes):
    get_url = getattr(scraper_cls, 'get_activation_url', None)
    prep = getattr(scraper_cls, 'prepare_activation_request', None)
    parse = getattr(scraper_cls, 'process_activation_response', None)
    if not get_url or not prep or not parse:
        return None, None, None, None, None
    activate_url = get_url(cfg)
    if not activate_url:
        return None, None, None, None, None
    body, headers = prep(challenge, cfg)
    try:
        r = _requests.post(activate_url, data=body, headers=headers, timeout=15)
    except Exception as exc:
        logger.warning('[directv-license] activation request failed: %s', exc)
        return None, None, None, None, None
    logger.info('[directv-license] activation -> HTTP %s (%d bytes)', r.status_code, len(r.content))
    if r.status_code < 200 or r.status_code >= 300:
        logger.warning('[directv-license] activation HTTP %s: %s', r.status_code, r.content[:500])
        return None, None, None, r.status_code, r.content
    parsed = parse(r.content) or {}
    return (
        parsed.get('identity_cookie') or None,
        parsed.get('identity_cookie_expires_at') or None,
        parsed.get('response_bytes') or None,
        r.status_code,
        None,
    )


def _directv_error_payload(content: bytes) -> dict:
    try:
        data = json.loads(content or b'{}')
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _directv_error_requires_reauth(status: int, content: bytes) -> bool:
    if status not in (400, 401, 403):
        return False
    data = _directv_error_payload(content)
    haystack = ' '.join(str(data.get(k) or '') for k in ('errorReason', 'description', 'message')).lower()
    if not haystack:
        return False
    if 'identity cookie' in haystack or 'identitycookie' in haystack:
        return False
    token_markers = (
        'invalid token', 'expired token', 'error decrypting token',
        'missing parameter activationtoken', 'missing parameter bearertoken',
        'activationtoken', 'bearertoken',
    )
    return any(marker in haystack for marker in token_markers)


def _directv_trigger_reauth(source, reason: str) -> None:
    cfg = dict(source.config or {})
    username = (cfg.get('username') or '').strip()
    password = (cfg.get('password') or '').strip()
    if not username or not password:
        logger.warning('[directv-auth] cannot auto-reauth after %s: username/password missing', reason)
        return
    try:
        from ..extensions import db
        cfg['token_captured_at'] = 0
        cfg.pop('activation_token', None)
        cfg.pop('identity_cookie', None)
        cfg.pop('identity_cookie_expires_at', None)
        source.config = cfg
        db.session.commit()
    except Exception:
        try:
            from ..extensions import db
            db.session.rollback()
        except Exception:
            pass
        logger.debug('[directv-auth] could not mark tokens stale after %s', reason, exc_info=True)
    try:
        from ..scrapers.directv import DirectvScraper
        DirectvScraper(cfg)._start_background_reauth(username, password)
        logger.info('[directv-auth] queued background reauth after %s', reason)
    except Exception:
        logger.exception('[directv-auth] failed to queue background reauth after %s', reason)


def _fetch_prismcast_with_retry(play_url: str, channel_id, label: str, stream: bool = False):
    """GET play_url from PrismCast with one safe retry.

    PrismCast serializes capture setup through a single navigation queue: a
    request can wait up to its own navigationTimeout just for a hung/slow
    predecessor's turn, THEN pay navigationTimeout again for its own capture —
    confirmed live 2026-08-12 (dev/todo.md) that a cold Warner TVE capture
    alone can take ~8-10s against PrismCast's default 10s navigationTimeout,
    so a queued request can plausibly need close to double that. A single
    generous timeout absorbs that worst case in one attempt.

    Retrying after OUR OWN timeout is deliberately avoided: PrismCast has no
    cancel/status API, so a slow response doesn't mean the capture failed —
    it may still be running server-side. Firing a second `/play?url=...
    &profile=keyboardFullscreen` trigger while the first is still in flight
    risks a second concurrent capture (prod PrismCast shares the operator's
    real desktop, so this isn't just a backend resource issue). A timeout is
    therefore treated as final. Only a fast connection-level failure or a
    fast 5xx response — cases where nothing was left in-flight — get one
    retry after a short pause.

    Returns the response (which may carry a 4xx/5xx status for the caller to
    handle), or aborts 502 if PrismCast never returned anything usable.
    """
    def _attempt(is_retry: bool):
        try:
            resp = _requests.get(play_url, timeout=90, allow_redirects=True, stream=stream)
            if stream:
                resp.close()
            return resp
        except _requests.exceptions.Timeout as exc:
            logger.warning('[%s] capture timed out for channel=%s%s: %s',
                            label, channel_id, ' (retry)' if is_retry else '', exc)
            abort(502)
        except Exception as exc:
            logger.info('[%s] attempt failed for channel=%s%s: %s',
                        label, channel_id, ' (retry)' if is_retry else '', exc)
            return None

    r = _attempt(is_retry=False)
    if r is None or r.status_code >= 500:
        logger.info('[%s] retrying once for channel=%s', label, channel_id)
        _time.sleep(3)
        r = _attempt(is_retry=True)

    if r is None:
        logger.warning('[%s] resolve failed for channel=%s after retry', label, channel_id)
        abort(502)
    if r.status_code >= 500:
        logger.warning('[%s] PrismCast returned %s for channel=%s after retry', label, r.status_code, channel_id)
    return r


def _directv_prismcast_play_url(channel) -> str | None:
    from ..models import AppSettings
    settings = AppSettings.get()
    prismcast_url = (settings.effective_prismcast_url() or '').strip().rstrip('/')
    if not prismcast_url:
        return None
    selector = (channel.name or channel.source_channel_id or '').strip()
    if not selector:
        return None
    guide_url = 'https://stream.directv.com/guide'
    return (
        f'{prismcast_url}/play?'
        f'url={_url_quote(guide_url, safe="")}'
        f'&selector={_url_quote(selector, safe="")}'
    )


def _directv_prismcast_asset_proxy_url(upstream_url: str) -> str:
    return f'/play/directv/prismcast-asset?url={_url_quote(upstream_url, safe="")}'


def _rewrite_directv_prismcast_playlist(text: str, playlist_url: str) -> str:
    def _rewrite_uri(match):
        return f'URI="{_directv_prismcast_asset_proxy_url(urljoin(playlist_url, match.group(1)))}"'

    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if 'URI="' in line:
            line = re.sub(r'URI="([^"]+)"', _rewrite_uri, line)
        if stripped and not stripped.startswith('#'):
            line = _directv_prismcast_asset_proxy_url(urljoin(playlist_url, stripped))
        lines.append(line)
    return '\n'.join(lines)


_DIRECTV_BROWSER_CDN_SUFFIXES = (
    'akamaized.net',
    'directv.fastly-edge.com',
    'live.cf.dtvcdn.com',
    'live.cflare.dtvcdn.com',
)


def _directv_browser_cdn_allowed(host: str) -> bool:
    return any(_host_in_cdn_suffix(host, suffix) for suffix in _DIRECTV_BROWSER_CDN_SUFFIXES)


def _directv_browser_asset_proxy_url(upstream_url: str) -> str:
    return f'/play/directv/browser-asset?url={_url_quote(upstream_url, safe="")}'


def _directv_browser_proxyable_url(raw_url: str, playlist_url: str) -> str:
    resolved = urljoin(playlist_url, raw_url)
    parsed = urlsplit(resolved)
    if parsed.scheme in ('http', 'https') and _directv_browser_cdn_allowed(parsed.hostname or ''):
        return _directv_browser_asset_proxy_url(resolved)
    return resolved


def _rewrite_directv_browser_playlist(text: str, playlist_url: str) -> str:
    def _rewrite_uri(match):
        return f'URI="{_directv_browser_proxyable_url(match.group(1), playlist_url)}"'

    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if 'URI="' in line:
            line = re.sub(r'URI="([^"]+)"', _rewrite_uri, line)
        if stripped and not stripped.startswith('#'):
            line = _directv_browser_proxyable_url(stripped, playlist_url)
        lines.append(line)
    return '\n'.join(lines)


@directv_proxy_bp.route('/play/directv/<channel_id>/browser.m3u8')
def directv_browser_manifest(channel_id: str):
    """Same-origin HLS master for DirecTV browser/Shaka playback.

    DirecTV's /play endpoint redirects to the live CDN master. Shaka can fail
    that redirected manifest request in Chromium capture even when the CDN sends
    CORS headers. Fetch the master server-side and absolutize child playlist
    references; child playlists and media segments are proxied through FastChannels.
    """
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'directv', Channel.source_channel_id == channel_id)
        .first_or_404()
    )
    scraper_cls = registry.get('directv')
    if not scraper_cls:
        abort(404)
    scraper = scraper_cls(channel.source.config or {})
    try:
        resolved_url = scraper.resolve(channel.stream_url)
        r = _requests.get(resolved_url, timeout=12)
    except Exception as exc:
        logger.warning('[directv-browser] manifest fetch failed for channel=%s: %s', channel_id, exc)
        return _unavailable_response()
    finally:
        # Without this, the fresh play_token/license_content_id this resolve()
        # just minted never reaches the source_cache table, so the license
        # request that follows (a separate request/route) reads a stale or
        # empty directv_playback entry and fetches its own mismatched session
        # — the CDM gets a license for the wrong content ID and never decrypts.
        if scraper._pending_config_updates:
            try:
                persist_source_config_updates(channel.source_id, scraper._pending_config_updates)
            except Exception as ce:
                from ..extensions import db
                db.session.rollback()
                logger.warning('[directv-browser] failed to persist config updates: %s', ce)
        if getattr(scraper, '_pending_cache_updates', None):
            try:
                persist_source_cache_updates(channel.source_id, scraper._pending_cache_updates)
            except Exception as ce:
                from ..extensions import db
                db.session.rollback()
                logger.warning('[directv-browser] failed to persist cache updates: %s', ce)
    if r.status_code >= 400:
        return Response(r.content, status=r.status_code, content_type=r.headers.get('Content-Type') or 'text/plain')
    return Response(
        _rewrite_directv_browser_playlist(r.text, r.url),
        status=200,
        mimetype='application/vnd.apple.mpegurl',
        headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
    )


@directv_proxy_bp.route('/play/directv/browser-asset')
def directv_browser_asset():
    raw_url = (request.args.get('url') or '').strip()
    if not raw_url:
        abort(400)
    target = urlsplit(raw_url)
    if target.scheme != 'https' or not _directv_browser_cdn_allowed(target.hostname or ''):
        abort(400)
    range_header = request.headers.get('Range')
    headers = {'User-Agent': _BROWSER_UA}
    if range_header:
        headers['Range'] = range_header
    try:
        r = _requests.get(raw_url, headers=headers, timeout=(5, 30), stream=True)
    except Exception as exc:
        logger.warning('[directv-browser] asset fetch failed for %s: %s', raw_url[:160], exc)
        abort(502)

    content_type = r.headers.get('Content-Type') or 'application/octet-stream'
    if 'mpegurl' in content_type.lower() or raw_url.lower().split('?', 1)[0].endswith(('.m3u8', '.m3u')):
        return Response(
            _rewrite_directv_browser_playlist(r.text, r.url),
            status=r.status_code,
            mimetype='application/vnd.apple.mpegurl',
            headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
        )

    response_headers = {'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'}
    for key in ('Content-Length', 'Accept-Ranges', 'Content-Range'):
        value = r.headers.get(key)
        if value:
            response_headers[key] = value
    return _stream_upstream_response(
        r,
        status=r.status_code,
        content_type=content_type,
        headers=response_headers,
        label='directv-browser-asset',
    )


@directv_proxy_bp.route('/play/directv/<channel_id>/prismcast.m3u')
@directv_proxy_bp.route('/play/directv/<channel_id>/prismcast.m3u8')
def directv_prismcast_playlist(channel_id: str):
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'directv', Channel.source_channel_id == channel_id)
        .first_or_404()
    )
    play_url = _directv_prismcast_play_url(channel)
    if not play_url:
        return Response('PrismCast is not configured.\n', status=503, mimetype='text/plain')
    r = _fetch_prismcast_with_retry(play_url, channel_id, 'directv-prismcast')
    if r.status_code >= 400:
        return Response(r.content, status=r.status_code, content_type=r.headers.get('Content-Type') or 'text/plain')
    playlist = _rewrite_directv_prismcast_playlist(r.text, r.url)
    return Response(
        playlist,
        status=200,
        mimetype='application/vnd.apple.mpegurl',
        headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
    )


@directv_proxy_bp.route('/play/directv/prismcast-asset')
def directv_prismcast_asset():
    from ..models import AppSettings
    raw_url = (request.args.get('url') or '').strip()
    if not raw_url:
        abort(400)
    settings = AppSettings.get()
    prismcast_url = (settings.effective_prismcast_url() or '').strip().rstrip('/')
    allowed = urlsplit(prismcast_url)
    target = urlsplit(raw_url)
    if not allowed.scheme or not allowed.netloc or target.scheme not in ('http', 'https') or target.netloc != allowed.netloc:
        abort(400)
    try:
        r = _requests.get(raw_url, timeout=(5, 30), stream=True)
    except Exception as exc:
        logger.warning('[directv-prismcast] asset fetch failed for %s: %s', raw_url[:160], exc)
        abort(502)
    headers = {'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'}
    content_type = r.headers.get('Content-Type') or 'application/octet-stream'
    return _stream_upstream_response(
        r,
        status=r.status_code,
        content_type=content_type,
        headers=headers,
        label='directv-prismcast-asset',
    )

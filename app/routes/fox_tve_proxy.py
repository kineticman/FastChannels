"""FOX TVE source-specific HLS proxy routes."""
import logging
import re
import time as _time
from datetime import datetime
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests as _requests
from flask import Blueprint, Response, abort, current_app, request

from app.config_store import persist_source_cache_updates, persist_source_config_updates
from ..models import Channel, Source
from ..scrapers import registry

logger = logging.getLogger(__name__)
fox_tve_proxy_bp = Blueprint('fox_tve_proxy', __name__)
_unavailable_response = None
_refresh_stream_info_async = None


def configure_fox_tve_proxy(*, unavailable_response, refresh_stream_info_async) -> None:
    global _unavailable_response, _refresh_stream_info_async
    _unavailable_response = unavailable_response
    _refresh_stream_info_async = refresh_stream_info_async


def _host_in_cdn_suffix(host: str, suffix: str) -> bool:
    host, suffix = host.lower(), suffix.lower()
    return host == suffix or host.endswith('.' + suffix)


_FOX_TVE_SESSION = _requests.Session()
_FOX_TVE_SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Referer': 'https://www.foxweather.com/live',
})

# fox_weather (static VOD-tagged Uplynk asset needing live-sequence synthesis) and
# the signed_page_hls channels (fox_news/fox_business, needing 247.fox*.com segment
# auth-query rewriting) genuinely need the manifest proxy. The fox_sports_* channels
# resolve through a different, already-live-tagged backend that needs no rewriting —
# see the routing check in play() for why they get a direct redirect instead.
def _fox_tve_proxy_required_channels() -> frozenset[str]:
    from ..scrapers.fox_tve import CHANNELS as _FOX_TVE_CHANNELS
    return frozenset(
        cid for cid, ch in _FOX_TVE_CHANNELS.items()
        if cid == 'fox_weather' or ch.signed_page_hls
    )


_FOX_TVE_PROXY_REQUIRED_CHANNELS = _fox_tve_proxy_required_channels()
_FOX_TVE_SEGMENT_SECONDS = 4.096
_FOX_TVE_KEY_RE = re.compile(r'URI="([^"]+)"')


def _fox_tve_allowed_uplynk_url(url: str) -> bool:
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or not parsed.netloc:
        return False
    host = parsed.netloc.split(':', 1)[0].lower()
    return _host_in_cdn_suffix(host, 'uplynk.com')


def _fox_tve_allowed_247_url(url: str) -> bool:
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or not parsed.netloc:
        return False
    host = parsed.netloc.split(':', 1)[0].lower()
    return host in {'247.foxnews.com', '247.foxbusiness.com'}


def _fox_tve_auth_query(url: str) -> str:
    query = urlsplit(url).query
    if query.startswith('hdnea=') or query.startswith('hdnts='):
        return query
    for part in query.split('&'):
        if part.startswith('hdnea=') or part.startswith('hdnts='):
            return part
    return ''


def _fox_tve_with_auth_query(url: str, auth_query: str) -> str:
    if not auth_query or not _fox_tve_allowed_247_url(url):
        return url
    parts = urlsplit(url)
    if 'hdnea=' in parts.query or 'hdnts=' in parts.query:
        return url
    query = f'{parts.query}&{auth_query}' if parts.query else auth_query
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def _fox_tve_fetch_media(upstream_master_url: str) -> tuple[str, str, str, str] | None:
    try:
        master_r = _FOX_TVE_SESSION.get(upstream_master_url, timeout=8)
        master_r.raise_for_status()
        master_text = master_r.text
        auth_query = _fox_tve_auth_query(master_r.url) or _fox_tve_auth_query(upstream_master_url)
        media_url = None
        for line in master_text.splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                media_url = _fox_tve_with_auth_query(urljoin(master_r.url, line), auth_query)
                break
        if not media_url:
            media_url = master_r.url
            media_text = master_text
        else:
            media_r = _FOX_TVE_SESSION.get(media_url, timeout=8)
            media_r.raise_for_status()
            media_text = media_r.text
        return master_text, media_url, media_text, auth_query
    except Exception as exc:
        logger.warning('[fox-tve] upstream playlist fetch failed: %s', exc)
        return None


def _fox_tve_live_sequence(media_text: str) -> int:
    match = re.search(r'#EXT-X-PROGRAM-DATE-TIME:([^\n]+)', media_text)
    if match:
        try:
            dt = datetime.fromisoformat(match.group(1).strip().replace('Z', '+00:00'))
            return int(dt.timestamp() / _FOX_TVE_SEGMENT_SECONDS)
        except Exception:
            pass
    return int(_time.time() / _FOX_TVE_SEGMENT_SECONDS)


def _fox_tve_rewrite_media_playlist(media_text: str, media_url: str, auth_query: str = '') -> str:
    from urllib.parse import quote as _quote
    base_url = request.host_url.rstrip('/')
    live_sequence = _fox_tve_live_sequence(media_text)
    saw_sequence = False
    out: list[str] = []
    base = media_url.rsplit('/', 1)[0] + '/'

    for raw_line in media_text.splitlines():
        line = raw_line.strip()
        if line == '#EXT-X-PLAYLIST-TYPE:VOD' or line == '#EXT-X-ENDLIST':
            continue
        if line.startswith('#UPLYNK-KEY:'):
            continue
        if line.startswith('#EXT-X-MEDIA-SEQUENCE:'):
            out.append(f'#EXT-X-MEDIA-SEQUENCE:{live_sequence}')
            saw_sequence = True
            continue
        if line.startswith('#EXT-X-KEY:'):
            def _key_uri(match):
                key_url = urljoin(base, match.group(1))
                if _fox_tve_allowed_uplynk_url(key_url):
                    key_url = f'{base_url}/play/fox_tve/key?url={_quote(key_url, safe="")}'
                return f'URI="{key_url}"'
            out.append(_FOX_TVE_KEY_RE.sub(_key_uri, raw_line))
            continue
        if line and not line.startswith('#'):
            out.append(_fox_tve_with_auth_query(urljoin(base, line), auth_query))
            continue
        out.append(raw_line)

    if not saw_sequence:
        for idx, line in enumerate(out):
            if line.startswith('#EXT-X-TARGETDURATION:'):
                out.insert(idx + 1, f'#EXT-X-MEDIA-SEQUENCE:{live_sequence}')
                break
        else:
            out.insert(1, f'#EXT-X-MEDIA-SEQUENCE:{live_sequence}')
    return '\n'.join(out).rstrip() + '\n'


@fox_tve_proxy_bp.route('/play/fox_tve/<channel_id>/proxy.m3u8', defaults={'proxy_source_name': 'fox_tve'})
@fox_tve_proxy_bp.route('/play/fox_one/<channel_id>/proxy.m3u8', defaults={'proxy_source_name': 'fox_one'})
def fox_tve_proxy(channel_id: str, proxy_source_name: str):
    from urllib.parse import unquote as _unquote

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == proxy_source_name, Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper_cls = registry.get(proxy_source_name)
    if not scraper_cls:
        return _unavailable_response()
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        upstream_url = scraper.resolve(channel.stream_url)
    except Exception as exc:
        logger.warning('[%s] resolve failed for %s: %s', proxy_source_name, raw_id, exc)
        return _unavailable_response()
    finally:
        if getattr(scraper, '_pending_config_updates', None):
            try:
                persist_source_config_updates(channel.source_id, scraper._pending_config_updates)
            except Exception as ce:
                from ..extensions import db
                db.session.rollback()
                logger.warning('[%s] failed to persist config updates: %s', proxy_source_name, ce)
        if getattr(scraper, '_pending_cache_updates', None):
            try:
                persist_source_cache_updates(channel.source_id, scraper._pending_cache_updates)
            except Exception as ce:
                from ..extensions import db
                db.session.rollback()
                logger.warning('[%s] failed to persist cache updates: %s', proxy_source_name, ce)

    fetched = _fox_tve_fetch_media(upstream_url)
    if not fetched:
        return _unavailable_response()
    master_text, media_url, media_text, auth_query = fetched
    _refresh_stream_info_async(current_app._get_current_object(), channel.id, channel.stream_info, master_text)
    body = _fox_tve_rewrite_media_playlist(media_text, media_url, auth_query)
    return Response(
        body,
        mimetype='application/vnd.apple.mpegurl',
        headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
    )


@fox_tve_proxy_bp.route('/play/fox_tve/key')
def fox_tve_key_proxy():
    from urllib.parse import unquote as _unquote

    raw = request.args.get('url', '')
    if not raw:
        abort(400)
    url = _unquote(raw)
    if not _fox_tve_allowed_uplynk_url(url):
        abort(403)
    try:
        r = _FOX_TVE_SESSION.get(url, timeout=8)
        if r.status_code != 200:
            abort(r.status_code)
        return Response(
            r.content,
            status=200,
            content_type=r.headers.get('Content-Type', 'application/octet-stream'),
            headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
        )
    except Exception as exc:
        logger.warning('[fox-tve] key fetch failed: %s', exc)
        abort(502)

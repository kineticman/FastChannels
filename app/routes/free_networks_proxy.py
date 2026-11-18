"""Free Networks playlist proxy, for networks whose streams a client can't
follow from a plain 302 (Network.proxied; first used by TrillerTV, whose
CloudFront signature reaches the variants only as cookies).

/play/free_networks/<id>/proxy.m3u8 resolves the channel, fetches the master
and points each variant (and alternate rendition) at
/play/free_networks/<id>/variant.m3u8?url=..., which fetches that playlist and
makes every segment, key and map URI absolute through Network.child_url().
Segments and keys load straight from the CDN; only playlist text comes
through here. Playlists are fetched only from the network's proxy_hosts.

A signed URL expires while a player keeps polling the same variant URL, so a
401/403 on a variant re-resolves the channel and swaps in the fresh query;
the newest query is remembered per channel so later polls use it directly.
"""
import logging
import re
import time as _time
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import requests as _requests
from flask import Blueprint, Response, abort, current_app, redirect, request

from ..models import Channel, Source
from ..scrapers import registry
from ..scrapers.base import StreamDeadError

logger = logging.getLogger(__name__)
free_networks_proxy_bp = Blueprint('free_networks_proxy', __name__)
_unavailable_response = None
_gone_response = None
_refresh_stream_info_async = None
_channel_is_retired = None

_URI_RE = re.compile(r'URI="([^"]+)"')
_HTTP = _requests.Session()
_UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36'
# source_channel_id → (newest signed query, when it was fetched). Signed
# queries we've seen last ~30 minutes; this only saves a re-resolve per poll.
_FRESH_QUERY: dict[str, tuple[str, float]] = {}
_FRESH_QUERY_TTL = 20 * 60


def configure_free_networks_proxy(*, unavailable_response, gone_response, refresh_stream_info_async,
                                  channel_is_retired) -> None:
    global _unavailable_response, _gone_response, _refresh_stream_info_async, _channel_is_retired
    _unavailable_response = unavailable_response
    _gone_response = gone_response
    _refresh_stream_info_async = refresh_stream_info_async
    _channel_is_retired = channel_is_retired


def _host(url: str) -> str:
    parts = urlsplit(url)
    return parts.netloc.split(':', 1)[0].lower() if parts.scheme in ('https', 'http') else ''


def _channel(channel_id: str) -> Channel:
    channel = (Channel.query.join(Source)
               .filter(Source.name == 'free_networks', Channel.source_channel_id == unquote(channel_id))
               .first())
    if not channel:
        abort(404)
    return channel


def _scraper_and_network(channel: Channel):
    scraper_cls = registry.get('free_networks')
    if not scraper_cls:
        return None, None
    scraper = scraper_cls(config=channel.source.config or {})
    net = scraper.network_for(channel.stream_url)
    return scraper, (net if net is not None and getattr(net, 'proxied', False) else None)


def needs_proxy(channel: Channel) -> bool:
    """Whether /play should send this Free Networks channel through the proxy."""
    try:
        _scraper, net = _scraper_and_network(channel)
    except Exception:  # noqa: BLE001
        return False
    return net is not None


def _allowed(net, url: str) -> bool:
    return bool(_host(url)) and net.proxy_allows(url)


def _playlist_response(body: str) -> Response:
    return Response(body, mimetype='application/vnd.apple.mpegurl',
                    headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'})


def _variant_url(channel_id: str, url: str) -> str:
    return (f'{request.host_url.rstrip("/")}/play/free_networks/{quote(channel_id, safe="")}'
            f'/variant.m3u8?url={quote(url, safe="")}')


def _with_query(url: str, query: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def _get(url: str, net=None):
    return _HTTP.get(url, timeout=10, headers={'User-Agent': _UA, **(net.proxy_headers() if net else {})})


@free_networks_proxy_bp.route('/play/free_networks/<channel_id>/proxy.m3u8')
def free_networks_proxy(channel_id: str):
    channel = _channel(channel_id)
    if _channel_is_retired(channel):
        return _gone_response()
    scraper, net = _scraper_and_network(channel)
    if net is None:
        abort(404)
    sid = channel.source_channel_id
    try:
        master_url = scraper.resolve(channel.stream_url)
    except StreamDeadError as exc:
        logger.info('[free_networks proxy] %s: %s', sid, exc)
        return _gone_response()
    except Exception as exc:  # noqa: BLE001
        logger.warning('[free_networks proxy] resolve failed for %s: %s', sid, exc)
        return _unavailable_response()
    if not net.proxies(master_url):
        # This stream plays as-is; only some of the network's need the proxy.
        return redirect(master_url, 302)
    if not _allowed(net, master_url):
        logger.warning('[free_networks proxy] unexpected master host for %s: %s', sid, _host(master_url))
        return _unavailable_response()
    try:
        r = _get(master_url, net)
        r.raise_for_status()
    except _requests.RequestException as exc:
        logger.warning('[free_networks proxy] master fetch failed for %s: %s', sid, exc)
        return _unavailable_response()
    if urlsplit(master_url).query:
        _FRESH_QUERY[sid] = (urlsplit(master_url).query, _time.time())
    _refresh_stream_info_async(current_app._get_current_object(), channel.id, channel.stream_info, r.text)

    # child_url works from the URL we asked for: a redirect may drop the query.
    base = master_url if urlsplit(r.url).path == urlsplit(master_url).path else r.url
    def variant(uri: str) -> str:
        # A variant on a host the proxy won't fetch (Sotal's origin hands some
        # off to the channel's own Wowza server) plays straight from there.
        url = net.child_url(base, uri)
        return _variant_url(sid, url) if _allowed(net, url) else url

    lines = []
    for line in r.text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            line = variant(stripped)
        elif stripped.startswith(('#EXT-X-MEDIA', '#EXT-X-I-FRAME-STREAM-INF')) and 'URI=' in stripped:
            line = _URI_RE.sub(lambda m: f'URI="{variant(m.group(1))}"', line)
        lines.append(line)
    return _playlist_response('\n'.join(lines) + '\n')


@free_networks_proxy_bp.route('/play/free_networks/<channel_id>/variant.m3u8')
def free_networks_variant_proxy(channel_id: str):
    url = request.args.get('url', '')
    channel = _channel(channel_id)
    scraper, net = _scraper_and_network(channel)
    if net is None or not _allowed(net, url):
        abort(403)
    sid = channel.source_channel_id
    fresh = _FRESH_QUERY.get(sid)
    if fresh and urlsplit(url).query and _time.time() - fresh[1] < _FRESH_QUERY_TTL:
        url = _with_query(url, fresh[0])
    try:
        r = _get(url, net)
        if r.status_code in (401, 403) and urlsplit(url).query:
            # The signature expired: sign again and retry with the new query.
            master_url = scraper.resolve(channel.stream_url)
            query = urlsplit(master_url).query
            if query and _allowed(net, master_url):
                _FRESH_QUERY[sid] = (query, _time.time())
                url = _with_query(url, query)
                r = _get(url, net)
        r.raise_for_status()
    except StreamDeadError as exc:
        logger.info('[free_networks proxy] %s: %s', sid, exc)
        abort(410)
    except Exception as exc:  # noqa: BLE001
        logger.warning('[free_networks proxy] variant fetch failed for %s: %s', sid, exc)
        abort(502)

    base = url if urlsplit(r.url).path == urlsplit(url).path else r.url
    lines = []
    for line in r.text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            line = net.child_url(base, stripped)
        elif stripped.startswith(('#EXT-X-KEY', '#EXT-X-MAP', '#EXT-X-PART', '#EXT-X-PRELOAD-HINT')) \
                and 'URI=' in stripped:
            line = _URI_RE.sub(lambda m: f'URI="{net.child_url(base, m.group(1))}"', line)
        lines.append(line)
    return _playlist_response('\n'.join(lines) + '\n')


# ── Browser relay (admin preview only) ──────────────────────────────────────
# Some CDNs lock CORS to their own site (BEATMAG: Access-Control-Allow-Origin
# https://ott.shift.tv on playlists and segments), so the admin player can't
# load them from our page however the playlist reaches it. For networks that
# list such hosts in browser_relay_hosts, the watch preview points here and
# every playlist and segment on those hosts is fetched server-side. /play and
# M3U clients, which ignore CORS, never come through here.

def browser_relay_url(channel: Channel) -> str | None:
    """The admin preview URL for a channel whose CDN refuses our page, or None."""
    try:
        scraper = registry.get('free_networks')(config=channel.source.config or {})
        net = scraper.network_for(channel.stream_url)
        parsed = scraper._split_url(channel.stream_url)
    except Exception:  # noqa: BLE001
        return None
    hosts = getattr(net, 'browser_relay_hosts', ()) if net is not None else ()
    # A network's other channels may sit on an open CDN (SHIFT TV itself).
    ref = parsed[1] if parsed else ''
    if not hosts or (ref.startswith('http') and (urlsplit(ref).hostname or '').lower() not in hosts):
        return None
    return f'/play/free_networks/{quote(channel.source_channel_id, safe="")}/browser.m3u8'


@free_networks_proxy_bp.route('/play/free_networks/<channel_id>/browser.m3u8')
@free_networks_proxy_bp.route('/play/free_networks/<channel_id>/browser.ts')
def free_networks_browser_relay(channel_id: str):
    from ..hls import rewrite_playlist

    channel = _channel(channel_id)
    scraper_cls = registry.get('free_networks')
    scraper = scraper_cls(config=channel.source.config or {}) if scraper_cls else None
    net = scraper.network_for(channel.stream_url) if scraper else None
    hosts = tuple(getattr(net, 'browser_relay_hosts', ()) or ())
    if not hosts:
        abort(404)
    url = request.args.get('url', '')
    if not url:
        try:
            url = scraper.resolve(channel.stream_url)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[free_networks browser] resolve failed for %s: %s', channel.source_channel_id, exc)
            return _unavailable_response()
    parts = urlsplit(url)
    if parts.scheme != 'https' or (parts.hostname or '').lower() not in hosts:
        if not request.args.get('url') and parts.scheme in ('http', 'https'):
            return redirect(url, 302)  # this channel's CDN takes our page as is
        abort(403)
    try:
        r = _HTTP.get(url, timeout=(5, 20), stream=True,
                      headers={'User-Agent': _UA, **(net.proxy_headers() or {})})
    except _requests.RequestException as exc:
        logger.warning('[free_networks browser] fetch failed for %s: %s', channel.source_channel_id,
                       type(exc).__name__)
        abort(502)
    if r.status_code != 200:
        r.close()
        abort(502)
    base = f'/play/free_networks/{quote(channel.source_channel_id, safe="")}/browser'

    def relay(u: str, kind: str) -> str:
        if (urlsplit(u).hostname or '').lower() not in hosts:
            return u
        ext = 'm3u8' if kind == 'playlist' else 'ts'
        return f'{base}.{ext}?url={quote(u, safe="")}'

    if parts.path.endswith('.m3u8') or 'mpegurl' in (r.headers.get('Content-Type') or '').lower():
        text = r.text
        return _playlist_response(rewrite_playlist(text, r.url, relay))

    def generate():
        try:
            for chunk in r.iter_content(64 * 1024):
                if chunk:
                    yield chunk
        finally:
            r.close()

    headers = {'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'}
    if r.headers.get('Content-Length'):
        headers['Content-Length'] = r.headers['Content-Length']
    return Response(generate(), content_type=r.headers.get('Content-Type') or 'video/mp2t', headers=headers,
                    direct_passthrough=True)

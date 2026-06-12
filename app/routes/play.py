"""
/play/<source>/<channel_id>.m3u8

Resolves the real stream URL at request time and issues a 302 redirect.
If the resolved manifest contains DRM (SAMPLE-AES or AES-128), the channel
is automatically marked is_active=False so it drops out of M3U/EPG output.
It remains visible in the admin channels page so users can see what was
disabled and manually re-enable if desired.
"""
import logging
import re
import threading
import time as _time
from urllib.parse import urljoin, parse_qs as _parse_qs

import requests as _requests

from flask import Blueprint, redirect, abort, request, Response, render_template
from app.config_store import persist_source_config_updates
from ..hls import inspect_hls_drm
from ..models import Channel, Source
from ..scrapers import registry
from ..scrapers.distro import (
    CHANNEL_SCHEME as _DISTRO_SCHEME,
    SESSION_CDN_HOSTS as _DISTRO_SESSION_CDN_HOSTS,
    HLS_HEADERS as _DISTRO_HLS_HEADERS,
    _resolve_from_feed as _distro_resolve_from_feed,
    _split_qualified_channel_id as _distro_split_id,
    _pick_best_variant as _distro_pick_best_variant,
    DistroScraper,
)

# Persistent session for Distro CDN fetches (manifest proxy + segment proxy).
# Reuses TCP/TLS connections across the ~5s manifest poll interval, cutting
# per-poll latency by avoiding repeated handshakes to the same CloudFront host.
_DISTRO_PROXY_SESSION = _requests.Session()
_DISTRO_PROXY_SESSION.headers.update(_DISTRO_HLS_HEADERS)

# Variant URL stored in Redis so ALL gunicorn workers share a single CloudFront
# session per channel. Each master fetch creates a new session token
# (e.g. /hls/WLFH5KA/...) whose EXT-X-MEDIA-SEQUENCE restarts from 1.
# If workers hold different sessions, the client sees the sequence alternate
# between two independent counters → backward jumps → stutter. Redis ensures
# every worker serves the same session, so MEDIA-SEQUENCE advances monotonically.
_DISTRO_REDIS_KEY_PREFIX = 'distro_variant:'
_DISTRO_REDIS: 'redis.Redis | None' = None  # lazily initialised per worker


def _distro_redis() -> 'redis.Redis | None':
    """Return a lazily-initialised Redis client, or None if unavailable."""
    global _DISTRO_REDIS
    if _DISTRO_REDIS is None:
        try:
            import redis as _r
            from flask import current_app
            _DISTRO_REDIS = _r.from_url(
                current_app.config['REDIS_URL'],
                decode_responses=True,
                socket_timeout=1,
                socket_connect_timeout=1,
            )
        except Exception:
            pass
    return _DISTRO_REDIS


def _distro_variant_key(upstream_url: str) -> str:
    import hashlib
    return _DISTRO_REDIS_KEY_PREFIX + hashlib.md5(upstream_url.encode()).hexdigest()


from ..scrapers.base import StreamDeadError
from .tasks import trigger_channel_auto_disable

logger = logging.getLogger(__name__)

play_bp = Blueprint('play', __name__)

_BROWSER_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/145.0.0.0 Safari/537.36'
)

# TTL-cache for custom channel re-detection results.
# Key: channel.id  Value: (stream_url, headers, monotonic_timestamp, resolver)
_CUSTOM_STREAM_CACHE: dict[int, tuple[str, dict, float, str]] = {}
_REDETECT_TTL = 300  # seconds
_REDETECT_TTL_LIVE = 60  # seconds for rolling direct-video clips behind a live wrapper

# Variant URL stored in Redis so ALL gunicorn workers share the same Wowza worker
# session for custom HLS channels that have a master playlist.  Each master fetch
# returns a random chunklist_w<id> — if workers hold different worker IDs the client
# sees EXT-X-MEDIA-SEQUENCE from two independent counters → backward jumps → drop.
# Redis key encodes (channel_id, master_stream_url) so it auto-invalidates when the
# detection cache expires and a new token is issued.
_CUSTOM_VARIANT_REDIS_KEY_PREFIX = 'custom_variant:'
_CUSTOM_VARIANT_REDIS: 'redis.Redis | None' = None  # lazily initialised per worker


def _custom_variant_redis() -> 'redis.Redis | None':
    global _CUSTOM_VARIANT_REDIS
    if _CUSTOM_VARIANT_REDIS is None:
        try:
            import redis as _r
            from flask import current_app
            _CUSTOM_VARIANT_REDIS = _r.from_url(
                current_app.config['REDIS_URL'],
                decode_responses=True,
                socket_timeout=1,
                socket_connect_timeout=1,
            )
        except Exception:
            pass
    return _CUSTOM_VARIANT_REDIS


def _custom_variant_key(channel_id: int, stream_url: str) -> str:
    import hashlib
    return _CUSTOM_VARIANT_REDIS_KEY_PREFIX + str(channel_id) + ':' + hashlib.md5(stream_url.encode()).hexdigest()

# Tracks the synthetic live manifest sequence per custom channel so clients see
# a new media sequence whenever the upstream clip URL rotates.
_CUSTOM_LIVE_SEQ: dict[int, tuple[str, int]] = {}

# Detects frozen SSAI sessions: tracks the last segment URL seen per channel and
# when we first saw it.  If the last segment hasn't changed for this long, the
# upstream session has expired and is returning a stale HTTP-200 snapshot.
_CUSTOM_LAST_FRESH_SEG: dict[int, tuple[str, float]] = {}
_SESSION_VARIANT_STALE_AFTER = 30.0  # seconds


def _get_custom_live_seq(channel_id: int, stream_url: str) -> int:
    last_url, seq = _CUSTOM_LIVE_SEQ.get(channel_id, ('', 0))
    if stream_url != last_url:
        seq += 1
        _CUSTOM_LIVE_SEQ[channel_id] = (stream_url, seq)
    return seq


def _last_segment_url(manifest_text: str) -> str:
    """Return the last non-comment segment line in an HLS manifest."""
    last = ''
    for line in manifest_text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            last = stripped
    return last


def _variant_is_stale(channel_id: int, last_seg: str) -> bool:
    """Return True if last_seg hasn't changed for _SESSION_VARIANT_STALE_AFTER seconds.

    Detects SSAI sessions that expired but still return HTTP 200 with a frozen
    snapshot.  The in-memory tracker is per-worker; any worker that detects
    staleness invalidates the shared Redis cache so all workers recover.
    """
    if not last_seg:
        return False
    now = _time.monotonic()
    prev_seg, first_seen = _CUSTOM_LAST_FRESH_SEG.get(channel_id, ('', 0.0))
    if last_seg != prev_seg:
        _CUSTOM_LAST_FRESH_SEG[channel_id] = (last_seg, now)
        return False
    return (now - first_seen) > _SESSION_VARIANT_STALE_AFTER


def _url_is_hls(url: str) -> bool:
    from urllib.parse import urlsplit
    return '.m3u8' in urlsplit(url).path.lower()


def _custom_proxy_headers(channel, extra_headers: dict | None = None) -> dict:
    """
    Build request headers for custom-channel proxy fetches.

    Custom channels often need the original page context for segment requests
    (notably YouTube/googlevideo).  Start with a browser UA, layer stored
    custom headers on top, and synthesize Referer/Origin from page_url when
    the channel doesn't already define them.

    Keys starting with '_' in custom_headers are internal metadata, not HTTP
    headers — they are stripped here before being sent upstream.
    """
    from urllib.parse import urlsplit

    stored = channel.custom_headers or {}
    explicit_headers = {k: v for k, v in stored.items() if not k.startswith('_')}
    headers = {'User-Agent': _BROWSER_UA, **explicit_headers}
    if extra_headers:
        headers.update({k: v for k, v in extra_headers.items() if v})
    page_url = getattr(channel, 'page_url', None) or ''
    if page_url:
        parsed = urlsplit(page_url)
        origin = f'{parsed.scheme}://{parsed.netloc}' if parsed.scheme and parsed.netloc else ''
        explicit_referer = bool(explicit_headers.get('Referer')) or bool((extra_headers or {}).get('Referer'))
        explicit_origin = bool(explicit_headers.get('Origin')) or bool((extra_headers or {}).get('Origin'))
        if not explicit_referer:
            headers.setdefault('Referer', page_url)
        if origin and not explicit_origin and not explicit_referer:
            headers.setdefault('Origin', origin)
    return headers


def _resolve_videolinq_fast(vl_id: str, page_url: str) -> str | None:
    """Call the VideoLinq public API directly to get a live HLS URL (~300ms)."""
    try:
        r = _requests.get(
            f'https://control.videolinq.com/playerwizard/public/{vl_id}',
            headers={'Referer': page_url, 'User-Agent': _BROWSER_UA},
            timeout=5,
        )
        if r.ok:
            hls = (r.json().get('hlsPath') or '').strip()
            return hls or None
    except Exception:
        pass
    return None


def _redetect_custom_stream_with_info(channel, ttl: int = _REDETECT_TTL) -> tuple[str, dict, dict]:
    """
    Re-detect a custom channel's stream URL from its source page, caching the
    result for _REDETECT_TTL seconds.  Blocks in the request path, but the
    typical case is a fast cache hit; only the first play (or post-expiry play)
    runs the actual page fetch + probe.
    Updates channel.stream_url / custom_headers in the DB when the URL changes.
    """
    channel_id = channel.id
    now = _time.monotonic()
    started = now
    cached = _CUSTOM_STREAM_CACHE.get(channel_id)
    if cached:
        cached_url, cached_hdrs, fetched_at = cached[:3]
        cached_resolver = cached[3] if len(cached) > 3 else 'unknown'
        if now - fetched_at < ttl:
            return cached_url, cached_hdrs, {
                'path': 'cache',
                'resolver': cached_resolver,
                'elapsed_ms': int((_time.monotonic() - started) * 1000),
                'cache_age_s': int(now - fetched_at),
            }

    from ..extensions import db as _db

    # Fast path: if we previously identified a VideoLinq source (ID stored in
    # custom_headers['_videolinq_id']), skip the full page re-fetch and call the
    # VideoLinq API directly (~300ms vs. 5-15s for PerimeterX page + probe).
    stored_hdrs = channel.custom_headers or {}
    vl_id = stored_hdrs.get('_videolinq_id')
    if vl_id and channel.page_url:
        hls_url = _resolve_videolinq_fast(vl_id, channel.page_url)
        if hls_url:
            _CUSTOM_STREAM_CACHE[channel_id] = (hls_url, {}, now, 'videolinq')
            logger.info('[custom-redetect] videolinq fast path for %s → %s…', vl_id, hls_url[:60])
            return hls_url, {}, {
                'path': 'provider-fast',
                'resolver': 'videolinq',
                'elapsed_ms': int((_time.monotonic() - started) * 1000),
            }
        logger.warning('[custom-redetect] videolinq fast path failed for %s, falling back to full detect', vl_id)

    from ..scrapers.stream_detector import StreamDetector

    page_url = channel.page_url or channel.stream_url
    result = StreamDetector().detect(page_url)
    if result.success and result.stream_url:
        stream_url = result.stream_url
        headers = result.headers or {}
        resolver = result.resolver or 'detector'
        _CUSTOM_STREAM_CACHE[channel_id] = (stream_url, headers, now, resolver)
        detected_type = result.stream_type or channel.stream_type
        # Persist provider metadata alongside headers; _-prefixed keys are
        # internal only and stripped before any upstream HTTP request.
        stored_new = dict(headers)
        if result.opaque_id and result.opaque_id.startswith('videolinq://'):
            stored_new['_videolinq_id'] = result.opaque_id[len('videolinq://'):]
        # Carry the session-variants flag forward so re-detections don't re-probe.
        # On first detection, check the master for _uid= in variant URLs; if found,
        # set the flag so the play proxy routes this channel through the HLS relay.
        if stored_hdrs.get('_session_variants'):
            stored_new['_session_variants'] = True
        elif _url_is_hls(stream_url) and not stored_new.get('_session_variants'):
            if _master_has_session_variants(stream_url, stored_new):
                stored_new['_session_variants'] = True
                logger.info(
                    '[custom-redetect] session-variant master detected for channel %d, enabling HLS relay',
                    channel_id,
                )
        if (
            stream_url != channel.stream_url
            or stored_new != stored_hdrs
            or detected_type != channel.stream_type
        ):
            try:
                channel.stream_url = stream_url
                channel.custom_headers = stored_new
                channel.stream_type = detected_type
                _db.session.commit()
            except Exception as e:
                logger.warning('[custom-redetect] DB update failed for channel %d: %s', channel_id, e)
                _db.session.rollback()
        return stream_url, headers, {
            'path': 'detect',
            'resolver': resolver,
            'elapsed_ms': int((_time.monotonic() - started) * 1000),
            'stream_type': detected_type,
        }

    logger.warning('[custom-redetect] detection failed for channel %d (%s): %s',
                   channel_id, (page_url or '')[:80], result.error)
    return channel.stream_url or '', stored_hdrs, {
        'path': 'detect-failed',
        'resolver': result.resolver or 'detector',
        'elapsed_ms': int((_time.monotonic() - started) * 1000),
        'error': result.error,
    }


def _redetect_custom_stream(channel, ttl: int = _REDETECT_TTL) -> tuple[str, dict]:
    stream_url, headers, _info = _redetect_custom_stream_with_info(channel, ttl=ttl)
    return stream_url, headers


def _log_custom_play_path(
    *,
    client_ip: str,
    channel,
    channel_id: str,
    lookup: dict,
    resolved_url: str,
    redirect_kind: str,
) -> None:
    log = logger.debug if lookup.get('path') == 'cache' else logger.info
    log(
        '[play] custom path ip=%s channel_id=%s channel_name=%s lookup=%s resolver=%s elapsed_ms=%s '
        'cache_age_s=%s stream_type=%s headers=%s proxy_segments=%s redirect=%s url=%s',
        client_ip,
        channel_id,
        channel.name,
        lookup.get('path') or 'stored',
        lookup.get('resolver') or '-',
        lookup.get('elapsed_ms'),
        lookup.get('cache_age_s'),
        (channel.stream_type or '-'),
        bool(channel.custom_headers),
        bool(getattr(channel, 'proxy_segments', False)),
        redirect_kind,
        (resolved_url or '')[:80],
    )

def _client_ip() -> str:
    forwarded = (request.headers.get('X-Forwarded-For') or '').strip()
    if forwarded:
        return forwarded.split(',', 1)[0].strip()
    real_ip = (request.headers.get('X-Real-IP') or '').strip()
    if real_ip:
        return real_ip
    return request.remote_addr or 'unknown'


def _check_manifest(url: str, session) -> str | None:
    """
    Fetch the HLS manifest at url and return a disable reason string if the
    stream is unplayable, or None if it looks fine.
    Returns None on any fetch error (fail open — don't disable on network hiccups).
    Returns 'Unauthorized' on 401 so callers can handle expired session tokens.
    """
    try:
        from urllib.parse import urljoin
        r = session.get(url, timeout=8)
        if r.status_code == 401:
            return 'Unauthorized'
        if r.status_code != 200:
            return None
        text = r.text

        # EXT-X-KEY and EXT-X-PLAYLIST-TYPE only appear in media playlists,
        # not master playlists. If we landed on a master, fetch the first variant.
        if '#EXT-X-STREAM-INF' in text:
            for line in text.splitlines():
                line = line.strip()
                if line and not line.startswith('#'):
                    try:
                        rv = session.get(urljoin(url, line), timeout=8)
                        if rv.status_code == 200:
                            text = rv.text
                    except Exception:
                        pass
                    break

        if '#EXT-X-PLAYLIST-TYPE:VOD' in text and '#EXT-X-ENDLIST' in text:
            logger.info('[play] finished VOD playlist in manifest: %s', url[:80])
            return 'VOD'

        drm = inspect_hls_drm(text)
        if drm:
            logger.info('[play] DRM detected (%s) in manifest: %s', drm['drm_type'], url[:80])
            return 'DRM'
    except Exception as e:
        logger.debug('[play] manifest check fetch failed (ignoring): %s', e)
    return None


@play_bp.route('/play/<source_name>/<channel_id>.m3u')
def play_vlc(source_name: str, channel_id: str):
    """Return a tiny M3U playlist so VLC (or any media player) can open the stream directly."""
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == source_name, Channel.source_channel_id == channel_id)
        .first()
    )
    if not channel and source_name == 'distro' and ':' not in channel_id:
        channel = (
            Channel.query
            .join(Source)
            .filter(Source.name == source_name, Channel.source_channel_id == f'US:{channel_id}')
            .first()
        )
    if not channel:
        abort(404)
    base_url = request.host_url.rstrip('/')
    stream_url = f'{base_url}/play/{source_name}/{channel_id}.m3u8'
    playlist = f'#EXTM3U\n#EXTINF:-1,{channel.name}\n{stream_url}\n'
    return Response(
        playlist,
        mimetype='audio/x-mpegurl',
        headers={'Content-Disposition': f'attachment; filename="{channel_id}.m3u"'},
    )


_PRIVATE_IP_RE = re.compile(
    r'^(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|::1|0\.0\.0\.0)',
    re.IGNORECASE,
)

# Variant URL pattern indicating a session token that rotates after each ad break
# (e.g. ViewTV's _uid=).  Masters that embed this in variant URLs need the relay
# proxy so the player never holds a stale session reference.
_SESSION_VARIANT_RE = re.compile(r'[?&]_uid=', re.IGNORECASE)


def _master_has_session_variants(master_url: str, req_headers: dict) -> bool:
    """Return True if the master playlist's variant lines contain _uid= session tokens."""
    try:
        clean = {k: v for k, v in req_headers.items() if not k.startswith('_')}
        r = _requests.get(
            master_url,
            headers={'User-Agent': _BROWSER_UA, **clean},
            timeout=8,
        )
        if not r.ok or '#EXT-X-STREAM-INF' not in r.text:
            return False
        for line in r.text.splitlines():
            if line and not line.startswith('#') and _SESSION_VARIANT_RE.search(line):
                return True
    except Exception:
        pass
    return False


@play_bp.route('/play/distro/segment')
def distro_segment_proxy():
    """
    Segment proxy for Distro CDNs that require Origin/Referer headers.

    Segment URLs come from manifests we already fetched from known Distro CDNs,
    so we trust their content.  We only block HTTPS requirement and private/internal
    IPs to prevent SSRF — no static host allowlist needed.
    """
    from urllib.parse import urlsplit, unquote as _unquote
    raw = request.args.get('url', '')
    if not raw:
        abort(400)
    url = _unquote(raw)
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or not parsed.netloc:
        abort(400)
    if _PRIVATE_IP_RE.match(parsed.netloc.split(':')[0]):
        logger.warning('[distro-seg-proxy] blocked SSRF attempt to: %s', parsed.netloc)
        abort(403)
    try:
        r = _DISTRO_PROXY_SESSION.get(url, timeout=15, stream=True)
        if r.status_code != 200:
            abort(r.status_code)
        return Response(
            r.iter_content(65536),
            status=200,
            content_type=r.headers.get('Content-Type', 'video/MP2T'),
            headers={'Cache-Control': 'no-cache'},
        )
    except Exception as e:
        logger.warning('[distro-seg-proxy] fetch failed for %s: %s', url[:80], e)
        abort(502)


def _distro_fetch_variant(upstream_url: str, channel_id: str) -> tuple[str, _requests.Response] | None:
    """
    Resolve master URL → best variant URL → fetch variant content.

    The variant URL is stored in Redis so all gunicorn workers share the same
    CloudFront session. Only evicted on actual fetch failure (CDN session expired).
    Falls back to per-request master fetch if Redis is unavailable.
    """
    rdb = _distro_redis()
    rkey = _distro_variant_key(upstream_url)

    variant_url = rdb.get(rkey) if rdb else None
    if variant_url:
        try:
            variant_r = _DISTRO_PROXY_SESSION.get(variant_url, timeout=8)
            if variant_r.status_code == 200:
                return variant_url, variant_r
        except Exception:
            pass
        logger.debug('[distro-proxy] cached variant expired for %s, re-resolving', channel_id)
        try:
            if rdb:
                rdb.delete(rkey)
        except Exception:
            pass

    # Fetch master to get a fresh session and best variant URL
    try:
        master_r = _DISTRO_PROXY_SESSION.get(upstream_url, timeout=8)
        master_r.raise_for_status()
    except Exception as e:
        logger.warning('[distro-proxy] master fetch failed for %s: %s', channel_id, e)
        return None

    effective_master_url = master_r.url
    best_variant = _distro_pick_best_variant(master_r.text, effective_master_url)
    if not best_variant:
        logger.warning('[distro-proxy] no variants in master for %s', channel_id)
        return None

    try:
        variant_r = _DISTRO_PROXY_SESSION.get(best_variant, timeout=8)
        variant_r.raise_for_status()
    except Exception as e:
        logger.warning('[distro-proxy] variant fetch failed for %s: %s', channel_id, e)
        return None

    try:
        if rdb:
            rdb.set(rkey, best_variant, ex=7200)  # 2h — matches Distro CDN session lifetime
    except Exception:
        pass
    return best_variant, variant_r


@play_bp.route('/play/distro/<channel_id>/proxy.m3u8')
def distro_manifest_proxy(channel_id: str):
    """
    Proxy for Distro channels on Referer-restricted CDNs.

    Fetches master + best-variant manifests using correct Origin/Referer headers,
    rewrites segment URLs to go through distro_segment_proxy (which adds the
    required headers), then returns the rewritten manifest to the client.

    Variant URLs are cached per upstream URL to avoid re-fetching the master on
    every ~5s manifest poll — a fresh session is only obtained when the cache
    misses or the variant fetch fails (session expired).
    """
    from urllib.parse import urlsplit, unquote, quote as _quote

    geo, raw_id = _distro_split_id(unquote(channel_id))
    scraper = DistroScraper()
    upstream_url = _distro_resolve_from_feed(scraper, geo, raw_id)
    if not upstream_url:
        abort(502)

    result = _distro_fetch_variant(upstream_url, channel_id)
    if result is None:
        # One retry with a forced feed refresh in case the upstream URL itself changed
        upstream_url = _distro_resolve_from_feed(scraper, geo, raw_id, force_refresh=True)
        if upstream_url:
            result = _distro_fetch_variant(upstream_url, channel_id)
    if result is None:
        abort(502)

    best_variant, variant_r = result

    # Only proxy segments whose CDN host requires Origin/Referer headers.
    # Segments on other hosts (e.g. b.jsrdn.com) are publicly accessible and
    # can be served as direct URLs, avoiding unnecessary proxy overhead.
    base_url = request.host_url.rstrip('/')
    variant_base = best_variant.rsplit('/', 1)[0] + '/'
    lines = []
    for line in variant_r.text.splitlines():
        if line and not line.startswith('#'):
            abs_url = urljoin(variant_base, line)
            seg_host = urlsplit(abs_url).netloc
            if seg_host in _DISTRO_SESSION_CDN_HOSTS:
                line = f'{base_url}/play/distro/segment?url={_quote(abs_url, safe="")}'
            else:
                line = abs_url
        lines.append(line)

    return Response(
        '\n'.join(lines),
        mimetype='application/vnd.apple.mpegurl',
        headers={'Cache-Control': 'no-cache'},
    )


@play_bp.route('/play/pluto/<channel_id>/proxy.m3u8')
def pluto_manifest_proxy(channel_id: str):
    """
    Manifest proxy for Pluto TV channels.

    Pluto's stitcher CDN only echoes the Origin header back for pluto.tv origins;
    any other origin gets Access-Control-Allow-Origin: http://pluto.tv — a mismatch
    that blocks Shaka Player.  We proxy both the master and variant manifests so the
    browser never sends a cross-origin request to the stitcher CDN directly.
    Segment and AES-key URLs inside variant playlists point to a different CDN
    (siloh-ns1.plutotv.net) that returns Access-Control-Allow-Origin: * and can be
    fetched directly by the browser.
    """
    from urllib.parse import unquote as _unquote, quote as _quote
    import re as _re

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'pluto', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper_cls = registry.get('pluto')
    if not scraper_cls:
        abort(502)
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        master_url = scraper.resolve(channel.stream_url)
    except Exception as e:
        logger.warning('[pluto-proxy] resolve failed for %s: %s', raw_id[:40], e)
        abort(502)

    if not master_url or not master_url.startswith('http'):
        abort(502)

    from ..scrapers.pluto import X_FORWARD as _PLUTO_X_FORWARD, BOOT_HEADERS as _PLUTO_BOOT_HEADERS
    _stream_url = channel.stream_url or ''
    _parts = _stream_url[len('pluto://'):].split('/', 1) if _stream_url.startswith('pluto://') else []
    _country = _parts[0] if _parts else 'us_east'
    _master_hdrs = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'origin': 'https://pluto.tv',
        'referer': 'https://pluto.tv/',
        'user-agent': _PLUTO_BOOT_HEADERS.get('user-agent', ''),
    }
    if _country in _PLUTO_X_FORWARD:
        _master_hdrs.update(_PLUTO_X_FORWARD[_country])

    try:
        r = _requests.get(master_url, headers=_master_hdrs, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[pluto-proxy] master fetch failed for %s: %s', raw_id[:40], e)
        abort(502)

    effective_url = r.url
    base_url = request.host_url.rstrip('/')

    def _proxy_url(abs_url: str) -> str:
        return f'{base_url}/play/pluto/variant?url={_quote(abs_url, safe="")}'

    def _abs(rel: str) -> str:
        return rel if rel.startswith('http') else urljoin(effective_url, rel)

    def _rewrite_uri(m):
        return f'URI="{_proxy_url(_abs(m.group(1)))}"'

    lines = []
    for line in r.text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            line = _proxy_url(_abs(stripped))
        elif stripped.startswith('#EXT-X-MEDIA') and 'URI=' in stripped:
            line = _re.sub(r'URI="([^"]+)"', _rewrite_uri, line)
        lines.append(line)

    return Response(
        '\n'.join(lines),
        mimetype='application/vnd.apple.mpegurl',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


@play_bp.route('/play/pluto/variant')
def pluto_variant_proxy():
    """
    Proxy a Pluto TV variant/subtitle/audio playlist through the server so the
    stitcher CDN always sees the server's origin rather than the browser's.
    Segment and AES-key URLs inside the variant are absolute siloh-ns1.plutotv.net
    URLs that return ACAO: * and are fetched directly by the browser.
    """
    from urllib.parse import urlsplit, unquote as _unquote
    raw = request.args.get('url', '')
    if not raw:
        abort(400)
    url = _unquote(raw)
    parsed = urlsplit(url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        abort(400)
    host = parsed.netloc.split(':')[0].lower()
    if not (host.endswith('.pluto.tv') or host.endswith('.plutotv.net')):
        logger.warning('[pluto-variant] blocked non-Pluto host: %s', host)
        abort(403)
    if _PRIVATE_IP_RE.match(host):
        abort(403)
    from ..scrapers.pluto import BOOT_HEADERS as _PLUTO_BOOT_HEADERS
    _variant_hdrs = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'origin': 'https://pluto.tv',
        'referer': 'https://pluto.tv/',
        'user-agent': _PLUTO_BOOT_HEADERS.get('user-agent', ''),
    }
    try:
        r = _requests.get(url, headers=_variant_hdrs, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[pluto-variant] fetch failed for %s: %s', url[:80], e)
        abort(502)
    return Response(
        r.content,
        mimetype='application/vnd.apple.mpegurl',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


@play_bp.route('/play/tubi/<channel_id>/proxy.m3u8')
def tubi_manifest_proxy(channel_id: str):
    """
    Tubi master manifest proxy.

    Shaka follows the 302 redirect from our play route to Tubi's CDN with
    Origin: null (cross-origin redirect). Even though Tubi returns ACAO: *,
    some Shaka/Chrome combinations reject null-origin + wildcard ACAO.  Fetching
    the master server-side and returning it as a same-origin response eliminates
    the redirect entirely.  Variant playlists and segments are left direct —
    both use ACAO: * or echo-origin CORS which works fine for Shaka's real origin.
    """
    from urllib.parse import unquote as _unquote

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'tubi', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper_cls = registry.get('tubi')
    if not scraper_cls:
        abort(502)
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        master_url = scraper.resolve(channel.stream_url)
    except Exception as e:
        logger.warning('[tubi-proxy] resolve failed for %s: %s', raw_id[:40], e)
        abort(502)

    if not master_url or not master_url.startswith('http'):
        abort(502)

    try:
        r = _requests.get(master_url, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[tubi-proxy] master fetch failed for %s: %s', raw_id[:40], e)
        abort(502)

    effective_url = r.url

    lines = []
    for line in r.text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            line = stripped if stripped.startswith('http') else urljoin(effective_url, stripped)
        lines.append(line)

    return Response(
        '\n'.join(lines),
        mimetype='application/vnd.apple.mpegurl',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


@play_bp.route('/play/fubo/<channel_id>/proxy.m3u8')
def fubo_manifest_proxy(channel_id: str):
    from urllib.parse import unquote as _unquote, quote as _quote
    import re as _re

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'fubo', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper_cls = registry.get('fubo')
    if not scraper_cls:
        abort(502)
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        master_url = scraper.resolve(channel.stream_url)
    except Exception as e:
        logger.warning('[fubo-proxy] resolve failed for %s: %s', raw_id[:40], e)
        abort(502)

    if not master_url or not master_url.startswith('http'):
        abort(502)

    try:
        from curl_cffi import requests as _cffi
        r = _cffi.get(master_url, impersonate='chrome', timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[fubo-proxy] master fetch failed for %s: %s', raw_id[:40], e)
        abort(502)

    effective_url = r.url
    base_url = request.host_url.rstrip('/')

    def _proxy_url(abs_url: str) -> str:
        return f'{base_url}/play/fubo/variant?url={_quote(abs_url, safe="")}'

    def _abs(rel: str) -> str:
        return rel if rel.startswith('http') else urljoin(effective_url, rel)

    def _rewrite_uri(m):
        return f'URI="{_proxy_url(_abs(m.group(1)))}"'

    lines = []
    for line in r.text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            line = _proxy_url(_abs(stripped))
        elif stripped.startswith('#EXT-X-MEDIA') and 'URI=' in stripped:
            line = _re.sub(r'URI="([^"]+)"', _rewrite_uri, line)
        lines.append(line)

    return Response(
        '\n'.join(lines),
        mimetype='application/vnd.apple.mpegurl',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


@play_bp.route('/play/fubo/variant')
def fubo_variant_proxy():
    from urllib.parse import urlsplit, unquote as _unquote, quote as _quote
    import re as _re

    raw = request.args.get('url', '')
    if not raw:
        abort(400)
    url = _unquote(raw)
    parsed = urlsplit(url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        abort(400)
    host = parsed.netloc.split(':')[0].lower()
    if not host.endswith('.fubo.tv'):
        logger.warning('[fubo-variant] blocked non-Fubo host: %s', host)
        abort(403)
    if _PRIVATE_IP_RE.match(host):
        abort(403)

    try:
        from curl_cffi import requests as _cffi
        r = _cffi.get(url, impersonate='chrome', timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[fubo-variant] fetch failed for %s: %s', url[:80], e)
        abort(502)

    base_url = request.host_url.rstrip('/')

    def _seg_proxy(abs_url: str) -> str:
        return f'{base_url}/play/fubo/seg?url={_quote(abs_url, safe="")}'

    def _abs(rel: str) -> str:
        return rel if rel.startswith('http') else urljoin(url, rel)

    def _rewrite_key_uri(m):
        return f'URI="{_seg_proxy(_abs(m.group(1)))}"'

    lines = []
    for line in r.text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            # Segment URL
            line = _seg_proxy(_abs(stripped))
        elif stripped.startswith('#EXT-X-KEY') and 'URI=' in stripped:
            # AES-128 key URI — proxy so it also comes from the server IP
            line = _re.sub(r'URI="([^"]+)"', _rewrite_key_uri, line)
        lines.append(line)

    return Response(
        '\n'.join(lines),
        mimetype='application/vnd.apple.mpegurl',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


@play_bp.route('/play/fubo/seg')
def fubo_segment_proxy():
    from urllib.parse import urlsplit, unquote as _unquote
    raw = request.args.get('url', '')
    if not raw:
        abort(400)
    url = _unquote(raw)
    parsed = urlsplit(url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        abort(400)
    host = parsed.netloc.split(':')[0].lower()
    if not host.endswith('.fubo.tv'):
        logger.warning('[fubo-seg] blocked non-Fubo host: %s', host)
        abort(403)
    if _PRIVATE_IP_RE.match(host):
        abort(403)

    try:
        r = _requests.get(url, timeout=15, stream=True)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[fubo-seg] fetch failed for %s: %s', url[:80], e)
        abort(502)

    content_type = r.headers.get('Content-Type', 'application/octet-stream')
    # Stream the segment chunk-by-chunk; r.content would buffer the whole
    # (multi-MB) segment into the worker before sending, defeating stream=True.
    return Response(
        r.iter_content(65536),
        mimetype=content_type,
        headers={'Access-Control-Allow-Origin': '*'},
    )


_STIRR_PROXY_SESSION: _requests.Session | None = None

# Redis client for Amazon SHT (sessionHandoffToken) caching across gunicorn workers.
# SHT is required in every Widevine license body and normally costs ~500ms to fetch
# from GetLivePlaybackResources.  Caching it avoids a PRS round-trip on every renewal.
_AMAZON_SHT_REDIS: 'redis.Redis | None' = None


def _amazon_sht_redis() -> 'redis.Redis | None':
    global _AMAZON_SHT_REDIS
    if _AMAZON_SHT_REDIS is None:
        try:
            import redis as _r
            from flask import current_app
            _AMAZON_SHT_REDIS = _r.from_url(
                current_app.config['REDIS_URL'],
                decode_responses=True,
                socket_timeout=1,
                socket_connect_timeout=1,
            )
        except Exception:
            pass
    return _AMAZON_SHT_REDIS


def _stirr_session() -> _requests.Session:
    """Persistent lax-TLS session for Stirr CDN fetches. Created once, reused per worker."""
    global _STIRR_PROXY_SESSION
    if _STIRR_PROXY_SESSION is None:
        from ..scrapers.stirr import StirrScraper
        _STIRR_PROXY_SESSION = StirrScraper._make_cdn_session()
    return _STIRR_PROXY_SESSION


@play_bp.route('/play/stirr/<channel_id>/proxy.m3u8')
def stirr_manifest_proxy(channel_id: str):
    """
    Manifest proxy for STIRR channels.

    STIRR resolves to IP-bound URLs (ssai.aniview.com, weathernationtv.com, etc.)
    whose vx_token JWT is bound to the server's IP.  If the client follows a 302
    redirect directly it fails token validation because the client has a different IP.
    Instead we proxy both the master and variant manifests through FastChannels (so
    the CDN always sees the server IP), then rewrite variant URLs so the client hits
    this proxy again on each refresh.  Segments go straight to the CDN.
    """
    import secrets
    from urllib.parse import quote as _quote, unquote as _unquote
    from ..scrapers.stirr import StirrScraper

    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'stirr', Channel.source_channel_id == _unquote(channel_id))
        .first_or_404()
    )

    scraper = StirrScraper(config=channel.source.config or {})
    try:
        master_url = scraper.resolve(channel.stream_url)
    except Exception as e:
        logger.warning('[stirr-proxy] resolve failed for %s: %s', channel_id, e)
        abort(502)

    if not master_url or not master_url.startswith(('http://', 'https://')):
        logger.warning('[stirr-proxy] resolve returned non-HTTP URL for %s: %s', channel_id, (master_url or '')[:60])
        abort(502)

    # Stirr SSAI URLs contain an unfilled nonce template [vx_nonce] that must be
    # substituted before the request — aniview returns 422 if it's left as-is.
    master_url = master_url.replace('[vx_nonce]', secrets.token_hex(16))

    # Fetch master playlist with the correct server-side headers/IP.
    # Use a lax-TLS session so CDN hosts with non-standard cipher requirements work.
    _hdrs = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    _sess = _stirr_session()
    try:
        master_r = _sess.get(master_url, headers=_hdrs, timeout=10)
        master_r.raise_for_status()
    except Exception as e:
        logger.warning('[stirr-proxy] master fetch failed for %s: %s', channel_id, e)
        abort(502)

    # Rewrite variant playlist lines AND EXT-X-MEDIA URI= attributes to go through
    # this proxy so every manifest fetch uses the server IP.  The URI= attribute in
    # #EXT-X-MEDIA tags (e.g. subtitle playlists) is a relative path that must also
    # be proxied — clients with AUTOSELECT=YES will fetch it automatically, and a 404
    # on a DEFAULT subtitle track causes Channels DVR to drop the stream entirely.
    import re as _re
    base_url = request.host_url.rstrip('/')
    effective_master_url = master_r.url

    def _rewrite_uri(m):
        rel = m.group(1)
        abs_url = urljoin(effective_master_url, rel)
        encoded = _quote(abs_url, safe='')
        return f'URI="{base_url}/play/stirr/variant?url={encoded}"'

    lines = []
    for line in master_r.text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            # Bare URL line (variant playlist reference)
            abs_url = urljoin(effective_master_url, stripped)
            encoded = _quote(abs_url, safe='')
            line = f'{base_url}/play/stirr/variant?url={encoded}'
        elif stripped.startswith('#EXT-X-MEDIA') and 'URI=' in stripped:
            # Rewrite URI= attribute inside EXT-X-MEDIA tags (subtitles, audio, etc.)
            line = _re.sub(r'URI="([^"]+)"', _rewrite_uri, line)
        lines.append(line)

    return Response(
        '\n'.join(lines),
        mimetype='application/vnd.apple.mpegurl',
        headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
    )


@play_bp.route('/play/stirr/variant')
def stirr_variant_proxy():
    """
    Proxy a STIRR variant playlist through the server so the IP-bound session
    token in the URL is always validated against the server's IP.
    Segment URLs inside the variant are absolute CDN URLs — left as-is.
    """
    from urllib.parse import urlsplit, unquote as _unquote
    raw = request.args.get('url', '')
    if not raw:
        abort(400)
    url = _unquote(raw)
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or not parsed.netloc:
        abort(400)
    if _PRIVATE_IP_RE.match(parsed.netloc.split(':')[0]):
        logger.warning('[stirr-variant] blocked SSRF attempt to: %s', parsed.netloc)
        abort(403)
    _hdrs = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        r = _stirr_session().get(url, headers=_hdrs, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[stirr-variant] fetch failed for %s: %s', url[:80], e)
        abort(502)
    return Response(
        r.content,
        status=200,
        mimetype='application/vnd.apple.mpegurl',
        headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
    )



@play_bp.route('/play/custom/<channel_id>/proxy.m3u8')
def custom_manifest_proxy(channel_id: str):
    """
    Manifest proxy for custom channels with proxy_segments=True.

    Fetches the master + best-variant HLS manifests using the channel's stored
    custom_headers, then rewrites segment URLs to route through custom_segment_proxy
    (which re-adds the headers).  Returns the rewritten variant manifest.
    """
    from urllib.parse import quote as _quote, unquote as _unquote

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'custom', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel or not channel.stream_url:
        abort(404)

    if getattr(channel, 'redetect_on_play', False) and channel.page_url:
        stream_url, custom_headers = _redetect_custom_stream(channel)
        if not stream_url:
            abort(502)
    else:
        custom_headers = channel.custom_headers or {}
        stream_url = channel.stream_url

    proxy_hdrs = _custom_proxy_headers(channel, custom_headers)

    # For master playlists: look up the cached variant URL from Redis so ALL gunicorn
    # workers use the same Wowza worker ID (chunklist_w<id>) across polls.  Every
    # master request returns a random worker with its own EXT-X-MEDIA-SEQUENCE counter;
    # if two workers each get a different worker ID, the client sees the sequence
    # alternate between independent counters → backward jumps → stream drop.
    rdb = _custom_variant_redis()
    rkey = _custom_variant_key(channel.id, stream_url) if rdb else None
    text: str | None = None
    effective_url: str = stream_url

    cached_variant_url = rdb.get(rkey) if rdb and rkey else None
    if cached_variant_url:
        try:
            cv_r = _requests.get(cached_variant_url, headers=proxy_hdrs, timeout=10)
            if cv_r.status_code == 200:
                text = cv_r.text
                effective_url = cv_r.url
                # Detect frozen SSAI sessions: if the last segment URL hasn't changed
                # for _SESSION_VARIANT_STALE_AFTER seconds the upstream session has
                # expired and is returning a stale HTTP-200 snapshot.  Force a master
                # re-fetch to get a new session token.
                if _variant_is_stale(channel.id, _last_segment_url(text)):
                    logger.info('[custom-proxy] frozen SSAI session for channel %d (%s), forcing master re-fetch',
                                channel.id, raw_id[:40])
                    try:
                        rdb.delete(rkey)
                    except Exception:
                        pass
                    # Clear the stale tracker so the timer restarts after the
                    # master re-fetch, regardless of whether the new master is a
                    # true master playlist or a variant-level manifest.
                    _CUSTOM_LAST_FRESH_SEG.pop(channel.id, None)
                    text = None
            else:
                logger.info('[custom-proxy] cached variant HTTP %s for channel %d (%s), re-fetching master',
                            cv_r.status_code, channel.id, raw_id[:40])
                try:
                    rdb.delete(rkey)
                except Exception:
                    pass
        except Exception as e:
            logger.info('[custom-proxy] cached variant fetch failed for channel %d (%s): %s',
                        channel.id, raw_id[:40], e)
            try:
                rdb.delete(rkey)
            except Exception:
                pass

    if text is None:
        try:
            master_r = _requests.get(stream_url, headers=proxy_hdrs, timeout=10)
            if master_r.status_code in (401, 403) and channel.page_url:
                fresh_url, fresh_headers, retry_info = _redetect_custom_stream_with_info(channel, ttl=0)
                if fresh_url:
                    logger.info(
                        '[custom-proxy] retrying master fetch for %s after %s using resolver=%s',
                        raw_id,
                        master_r.status_code,
                        retry_info.get('resolver') or '-',
                    )
                    stream_url = fresh_url
                    custom_headers = fresh_headers
                    proxy_hdrs = _custom_proxy_headers(channel, custom_headers)
                    if rdb:
                        rkey = _custom_variant_key(channel.id, stream_url)
                    master_r = _requests.get(stream_url, headers=proxy_hdrs, timeout=10)
            master_r.raise_for_status()
        except Exception as e:
            logger.warning('[custom-proxy] master fetch failed for %s: %s', raw_id, e)
            abort(502)

        text = master_r.text
        effective_url = master_r.url

        # If it's a master playlist, resolve and store the variant URL in Redis
        if '#EXT-X-STREAM-INF' in text:
            best = _distro_pick_best_variant(text, effective_url)
            if not best:
                abort(502)
            try:
                variant_r = _requests.get(best, headers=proxy_hdrs, timeout=10)
                variant_r.raise_for_status()
                text = variant_r.text
                effective_url = variant_r.url
                if rdb and rkey:
                    try:
                        rdb.set(rkey, best, ex=7200)  # 2h; relies on failure path to refresh early
                    except Exception:
                        pass
                # Reset the stale tracker — fresh content from master, timer starts clean.
                fresh_seg = _last_segment_url(text)
                if fresh_seg:
                    _CUSTOM_LAST_FRESH_SEG[channel.id] = (fresh_seg, _time.monotonic())
            except Exception as e:
                logger.warning('[custom-proxy] variant fetch failed for %s: %s', raw_id, e)
                abort(502)

    # Unless the channel explicitly requested segment proxying, leave segments
    # as direct absolute URLs.  YouTube/googlevideo HLS segment URLs already
    # work when fetched directly and the extra proxy hop can introduce 403s.
    base_url = request.host_url.rstrip('/')
    variant_base = effective_url.rsplit('/', 1)[0] + '/'
    encoded_id = _quote(raw_id, safe='')
    proxy_segments = bool(getattr(channel, 'proxy_segments', False))

    session_variants = bool((channel.custom_headers or {}).get('_session_variants'))
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            abs_url = stripped if stripped.startswith('http') else urljoin(variant_base, stripped)
            # ViewTV mrouter URLs are cross-domain 302 redirects to the real segment.
            # Many players won't follow cross-domain segment redirects and drop.
            # Unwrap by extracting the seg= parameter directly.
            if session_variants and '/mrouter?' in abs_url:
                from urllib.parse import urlsplit as _us
                _seg = (_parse_qs(_us(abs_url).query).get('seg') or [''])[0]
                if _seg:
                    abs_url = _seg
            if proxy_segments:
                line = f'{base_url}/play/custom/segment?url={_quote(abs_url, safe="")}&src={encoded_id}'
            else:
                line = abs_url
        lines.append(line)

    body = '\n'.join(lines)

    return Response(
        body,
        mimetype='application/vnd.apple.mpegurl',
        headers={'Cache-Control': 'no-cache'},
    )


@play_bp.route('/play/custom/<channel_id>/direct')
def custom_direct_proxy(channel_id: str):
    """
    Direct-media proxy for custom channels that need request headers.

    This is used for non-HLS streams where yt-dlp returned playback headers
    that the client cannot send on a raw redirect.
    """
    from urllib.parse import unquote as _unquote

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'custom', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel or not channel.stream_url:
        abort(404)

    if getattr(channel, 'redetect_on_play', False) and channel.page_url:
        stream_url, custom_headers = _redetect_custom_stream(channel)
        if not stream_url:
            abort(502)
    else:
        custom_headers = channel.custom_headers or {}
        stream_url = channel.stream_url

    headers = _custom_proxy_headers(channel, custom_headers)
    range_header = request.headers.get('Range')
    if range_header:
        headers['Range'] = range_header

    try:
        r = _requests.get(stream_url, headers=headers, timeout=15, stream=True)
        if r.status_code not in (200, 206):
            abort(r.status_code)

        response_headers = {'Cache-Control': 'no-cache'}
        for key in ('Content-Type', 'Content-Length', 'Accept-Ranges', 'Content-Range', 'Last-Modified', 'ETag'):
            value = r.headers.get(key)
            if value:
                response_headers[key] = value

        return Response(
            r.iter_content(65536),
            status=r.status_code,
            headers=response_headers,
        )
    except Exception as e:
        logger.warning('[custom-direct] fetch failed for %s: %s', raw_id, e)
        abort(502)


@play_bp.route('/play/custom/<channel_id>/live.m3u8')
def custom_live_manifest(channel_id: str):
    """
    Synthetic live HLS manifest for custom channels that currently resolve to a
    direct video URL instead of HLS.

    The manifest contains a single segment entry pointing at the latest direct
    video clip URL. Clients keep polling because there is no EXT-X-ENDLIST, and
    the media sequence increments whenever the clip URL changes.
    """
    from urllib.parse import unquote as _unquote

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'custom', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    if channel.page_url:
        stream_url, custom_headers = _redetect_custom_stream(channel, ttl=_REDETECT_TTL_LIVE)
    else:
        stream_url = channel.stream_url or ''
        custom_headers = channel.custom_headers or {}

    if not stream_url:
        abort(502)

    seq = _get_custom_live_seq(channel.id, stream_url)
    use_proxy = bool(custom_headers)
    if use_proxy:
        from urllib.parse import quote as _quote
        encoded_id = _quote(raw_id, safe='')
        stream_url = f'{request.host_url.rstrip("/")}/play/custom/{encoded_id}/direct'
    manifest = (
        '#EXTM3U\n'
        '#EXT-X-VERSION:3\n'
        '#EXT-X-TARGETDURATION:65\n'
        f'#EXT-X-MEDIA-SEQUENCE:{seq}\n'
        '#EXTINF:60.0,\n'
        f'{stream_url}\n'
    )
    return Response(
        manifest,
        mimetype='application/vnd.apple.mpegurl',
        headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
    )


@play_bp.route('/play/custom/segment')
def custom_segment_proxy():
    """
    Segment proxy for custom channels.  Fetches the segment with the channel's
    stored custom_headers and streams the bytes back to the client.

    SSRF protection: requires https, blocks private IP ranges, and validates
    that the segment host shares a domain root with the channel's stored stream_url.
    """
    from urllib.parse import urlsplit, unquote as _unquote

    raw_url = request.args.get('url', '')
    raw_id = request.args.get('src', '')
    if not raw_url or not raw_id:
        abort(400)

    url = _unquote(raw_url)
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or not parsed.netloc:
        abort(400)
    if _PRIVATE_IP_RE.match(parsed.netloc.split(':')[0]):
        logger.warning('[custom-seg-proxy] blocked SSRF attempt to: %s', parsed.netloc)
        abort(403)

    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'custom', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    # Validate segment host shares a domain root with the stored stream URL
    # to prevent this endpoint being used as an open proxy.
    try:
        stored_host = urlsplit(channel.stream_url or '').netloc
        seg_host = parsed.netloc
        if stored_host and seg_host != stored_host:
            def _root(h: str) -> str:
                parts = h.split('.')
                return '.'.join(parts[-2:]) if len(parts) >= 2 else h
            if _root(seg_host) != _root(stored_host):
                logger.warning('[custom-seg-proxy] host mismatch %s vs %s', seg_host, stored_host)
                abort(403)
    except Exception:
        pass

    try:
        r = _requests.get(url, headers=_custom_proxy_headers(channel), timeout=15, stream=True)
        if r.status_code != 200:
            abort(r.status_code)
        return Response(
            r.iter_content(65536),
            status=200,
            content_type=r.headers.get('Content-Type', 'video/MP2T'),
            headers={'Cache-Control': 'no-cache'},
        )
    except Exception as e:
        logger.warning('[custom-seg-proxy] fetch failed for %s: %s', url[:80], e)
        abort(502)


@play_bp.route('/play/amazon_prime_free/<channel_id>/dash.mpd')
def amazon_dash_proxy(channel_id: str):
    """
    DASH manifest proxy for Amazon Prime Free channels.

    Amazon's CDN allows only Origin: https://www.amazon.com, so browsers
    can't fetch the MPD directly.  This endpoint proxies the manifest through
    our server with permissive CORS headers and rewrites relative <BaseURL>
    elements to absolute CDN URLs so Shaka can resolve segment URLs.
    The endpoint is polled every 5 s by Shaka for live content updates.
    """
    from urllib.parse import unquote as _unquote, urljoin as _urljoin, quote as _quote
    from ..scrapers.amazon_prime_free import AmazonPrimeFreeScraper

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'amazon_prime_free', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper = AmazonPrimeFreeScraper(config=channel.source.config or {})
    dash_url = scraper.resolve(channel.stream_url)

    if scraper._pending_config_updates:
        try:
            from app.config_store import persist_source_config_updates
            persist_source_config_updates(channel.source_id, scraper._pending_config_updates)
        except Exception:
            pass

    if not dash_url or not dash_url.startswith('http'):
        logger.warning('[amazon-dash] no resolved URL for %s', raw_id[:40])
        abort(502)

    try:
        r = _requests.get(dash_url, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[amazon-dash] manifest fetch failed for %s: %s', raw_id[:40], e)
        abort(502)

    # Rewrite relative <BaseURL> elements to absolute CDN URLs.
    # Amazon's MPD uses relative paths (e.g. ../../../../iad-nitro/...) that
    # Shaka would try to resolve against our proxy origin — not the CDN.
    # The regex handles optional XML attributes (e.g. serviceLocation=, dvb:priority=).
    def _abs(m):
        url = m.group(1).strip()
        if not url.startswith('http'):
            url = _urljoin(dash_url, url)
        return f'<BaseURL>{url}</BaseURL>'

    mpd = re.sub(r'<BaseURL[^>]*>([^<]+)</BaseURL>', _abs, r.text)

    # Some Amazon manifests have no <BaseURL> at all and use relative paths in
    # SegmentTemplate media/initialization attributes.  Without a base, the DASH
    # player resolves those paths against our proxy URL and requests segments from
    # us (→ 404).  Inject a global <BaseURL> pointing to the CDN directory so
    # segment requests go directly to the CDN.
    if not re.search(r'<BaseURL\b', mpd):
        cdn_base = _urljoin(dash_url, '.')  # CDN directory containing the .mpd
        mpd = mpd.replace('<Period ', f'<BaseURL>{cdn_base}</BaseURL>\n  <Period ', 1)

    return Response(
        mpd,
        mimetype='application/dash+xml',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


@play_bp.route('/play/<source_name>/license', methods=['POST'])
def license_proxy(source_name: str):
    """DRM license proxy — forwards Widevine challenges to the scraper's license_url
    with source-specific auth headers so clients don't need credentials.
    Pass ?channel_id=<id> for scrapers that need per-channel auth (e.g. Sling)."""
    from ..models import Source
    scraper_cls = registry.get(source_name)
    if not scraper_cls or not getattr(scraper_cls, 'license_url', None):
        abort(404)
    source = Source.query.filter_by(name=source_name).first()
    if not source:
        abort(404)
    challenge = request.get_data()
    if not challenge:
        abort(400)
    if len(challenge) > 65536:
        abort(413)
    cfg = source.config or {}
    channel_id = request.args.get('channel_id') or None
    license_url = scraper_cls.get_license_url(cfg, channel_id=channel_id)
    if not license_url:
        abort(404)

    # Fetch (or reuse) the sessionHandoffToken before building the license body.
    # _get_session_handoff_token() calls GetLivePlaybackResources (~500ms) on every
    # invocation because sht_cache is never written anywhere.  Cache the token in
    # Redis so repeated license renewals skip the PRS round-trip.
    sht = None
    if channel_id and source_name == 'amazon_prime_free':
        rdb = _amazon_sht_redis()
        _sht_key = f'amz_sht:{channel_id}'
        if rdb:
            try:
                sht = rdb.get(_sht_key)
            except Exception:
                pass
        if not sht:
            sht = scraper_cls._get_session_handoff_token(cfg, channel_id)
            if sht and rdb:
                try:
                    rdb.setex(_sht_key, 600, sht)
                except Exception:
                    pass

    body, headers = scraper_cls.prepare_license_request(challenge, cfg, channel_id=channel_id, sht=sht)
    headers.setdefault('Content-Type', 'application/octet-stream')
    # Cache the raw challenge in Redis so test scripts can replay it
    if channel_id:
        _rdb = _amazon_sht_redis()
        if _rdb:
            try:
                import base64 as _b64
                _rdb.setex(f'amz_challenge:{channel_id}', 300, _b64.b64encode(challenge))
            except Exception:
                pass
    try:
        r = _requests.post(license_url, data=body, headers=headers, timeout=15)
    except Exception as e:
        logger.warning('[license-proxy] %s request failed: %s', source_name, e)
        abort(502)
    logger.debug('[license-proxy] %s channel=%s -> HTTP %s (%d bytes)',
                 source_name, channel_id or '-', r.status_code, len(r.content))
    response_bytes = scraper_cls.process_license_response(r.content)
    # If Amazon returned a SERVICE_CERTIFICATE (Widevine type 5), cache it for
    # the /certificate endpoint so Shaka can pre-fetch it via serverCertificateUri.
    if response_bytes and response_bytes[:2] == b'\x08\x05':
        _rdb = _amazon_sht_redis()
        if _rdb:
            try:
                _rdb.setex(f'amz_service_cert:{source_name}', 86400, response_bytes)
            except Exception:
                pass
    return Response(response_bytes, status=r.status_code, content_type='application/octet-stream')


@play_bp.route('/play/<source_name>/certificate', methods=['GET'])
def license_certificate(source_name: str):
    """Return the Widevine service certificate for this source so Shaka can configure
    privacy-mode license requests (serverCertificateUri).  Amazon returns the same static
    certificate for all channels — we cache it in Redis after the first license round-trip
    and fall back to fetching it on demand with a dummy SERVICE_CERTIFICATE_REQUEST."""
    from ..models import Source
    scraper_cls = registry.get(source_name)
    if not scraper_cls or not getattr(scraper_cls, 'license_url', None):
        abort(404)
    source = Source.query.filter_by(name=source_name).first()
    if not source:
        abort(404)
    # Try cache first
    _rdb = _amazon_sht_redis()
    if _rdb:
        try:
            cached = _rdb.get(f'amz_service_cert:{source_name}')
            if cached:
                return Response(cached, status=200, content_type='application/octet-stream')
        except Exception:
            pass
    # Not cached — fetch live with a minimal SERVICE_CERTIFICATE_REQUEST challenge
    cfg = source.config or {}
    dummy_challenge = b'\x08\x04'  # Widevine SERVICE_CERTIFICATE_REQUEST
    channel_id = request.args.get('channel_id') or None
    license_url = scraper_cls.get_license_url(cfg, channel_id=channel_id)
    if not license_url:
        abort(404)
    body, headers = scraper_cls.prepare_license_request(dummy_challenge, cfg, channel_id=channel_id, sht='')
    try:
        r = _requests.post(license_url, data=body, headers=headers, timeout=15)
    except Exception as e:
        logger.warning('[cert] %s fetch failed: %s', source_name, e)
        abort(502)
    cert_bytes = scraper_cls.process_license_response(r.content)
    if cert_bytes and cert_bytes[:2] == b'\x08\x05':
        rdb = _amazon_sht_redis()
        if rdb:
            try:
                rdb.setex(f'amz_service_cert:{source_name}', 86400, cert_bytes)
            except Exception:
                pass
        return Response(cert_bytes, status=200, content_type='application/octet-stream')
    logger.warning('[cert] %s returned unexpected response (not a service certificate)', source_name)
    abort(502)


@play_bp.route('/play/<source_name>/<channel_id>.m3u8')
def play(source_name: str, channel_id: str):
    client_ip = _client_ip()
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == source_name, Channel.source_channel_id == channel_id)
        .first()
    )
    if not channel and source_name == 'distro' and ':' not in channel_id:
        # Legacy Distro IDs were bare integers (e.g. "39730"); multi-region
        # support prefixed them with "US:" — fall back so old cached playlists
        # still work.
        channel = (
            Channel.query
            .join(Source)
            .filter(Source.name == source_name, Channel.source_channel_id == f'US:{channel_id}')
            .first()
        )
    if not channel:
        logger.warning('[play] request ip=%s unknown channel %s/%s', client_ip, source_name, channel_id)
        abort(404)

    # Custom channels log full detail (incl. cache/resolver) via _log_custom_play_path
    _log_play_req = logger.debug if source_name == 'custom' else logger.info
    _log_play_req(
        '[play] request ip=%s source=%s channel_id=%s channel_name=%s',
        client_ip, source_name, channel_id, channel.name,
    )

    scraper_cls = registry.get(source_name)
    scraper = None
    if scraper_cls:
        scraper = scraper_cls(config=channel.source.config or {})
        try:
            resolved_url = scraper.resolve(channel.stream_url)
        except StreamDeadError as e:
            logger.error(
                '[play] channel confirmed dead ip=%s source=%s channel_id=%s channel_name=%s: %s',
                client_ip, source_name, channel_id, channel.name, e,
            )
            trigger_channel_auto_disable(channel.id, 'Dead')
            resolved_url = None
        except Exception as e:
            logger.error(
                '[play] resolve failed ip=%s source=%s channel_id=%s channel_name=%s: %s',
                client_ip, source_name, channel_id, channel.name, e,
            )
            resolved_url = None
        finally:
            if scraper._pending_config_updates:
                try:
                    persist_source_config_updates(
                        channel.source_id,
                        scraper._pending_config_updates,
                    )
                except Exception as ce:
                    db.session.rollback()
                    logger.warning('[play] failed to persist config updates: %s', ce)
            if getattr(scraper, '_requested_rescrape', False):
                try:
                    from .tasks import trigger_scrape
                    trigger_scrape(source_name)
                    logger.info('[play] triggered background rescrape for %s after resolve failure', source_name)
                except Exception as rs_e:
                    logger.warning('[play] trigger_scrape failed for %s: %s', source_name, rs_e)
    else:
        resolved_url = channel.stream_url

    if not resolved_url or not resolved_url.startswith(('http://', 'https://')):
        abort(502)

    # STIRR channels resolve to URLs with IP-bound session tokens — proxy all
    # Stirr streams so every manifest fetch goes through the server IP, regardless
    # of which CDN (ssai.aniview.com, weathernationtv.com, etc.) is serving.
    if source_name == 'stirr':
        from urllib.parse import quote as _quote
        encoded_id = _quote(channel.source_channel_id, safe='')
        return redirect(
            f"{request.host_url.rstrip('/')}/play/stirr/{encoded_id}/proxy.m3u8",
            302,
        )

    # Custom channels with a page_url: re-detect the stream URL at play time
    # (TTL-cached so the page fetch only runs once per 5 minutes).
    custom_lookup = {'path': 'stored', 'resolver': '-', 'elapsed_ms': 0}
    if source_name == 'custom' and channel.page_url:
        fresh_url, custom_headers, custom_lookup = _redetect_custom_stream_with_info(channel)
        if fresh_url:
            resolved_url = fresh_url
    else:
        custom_headers = channel.custom_headers or {}

    # For custom channels that currently resolve to a direct video clip instead
    # of HLS, either hand the client the raw MP4 directly or fall back to the
    # synthetic live manifest for other direct-video types.
    if source_name == 'custom' and channel.page_url and resolved_url:
        has_proxy_headers = bool(custom_headers)
        if _url_is_hls(resolved_url):
            _needs_relay = (
                getattr(channel, 'proxy_segments', False)
                or has_proxy_headers
                or bool((channel.custom_headers or {}).get('_session_variants'))
            )
            if _needs_relay:
                from urllib.parse import quote as _quote
                encoded_id = _quote(channel.source_channel_id, safe='')
                _log_custom_play_path(
                    client_ip=client_ip,
                    channel=channel,
                    channel_id=channel_id,
                    lookup=custom_lookup,
                    resolved_url=resolved_url,
                    redirect_kind='hls-proxy',
                )
                return redirect(
                    f"{request.host_url.rstrip('/')}/play/custom/{encoded_id}/proxy.m3u8",
                    302,
                )
        elif (channel.stream_type or '').lower() == 'mp4':
            if has_proxy_headers:
                from urllib.parse import quote as _quote
                encoded_id = _quote(channel.source_channel_id, safe='')
                _log_custom_play_path(
                    client_ip=client_ip,
                    channel=channel,
                    channel_id=channel_id,
                    lookup=custom_lookup,
                    resolved_url=resolved_url,
                    redirect_kind='direct-proxy',
                )
                return redirect(
                    f"{request.host_url.rstrip('/')}/play/custom/{encoded_id}/direct",
                    302,
                )
            _log_custom_play_path(
                client_ip=client_ip,
                channel=channel,
                channel_id=channel_id,
                lookup=custom_lookup,
                resolved_url=resolved_url,
                redirect_kind='direct-mp4',
            )
            return redirect(resolved_url, 302)
        else:
            from urllib.parse import quote as _quote
            encoded_id = _quote(channel.source_channel_id, safe='')
            _log_custom_play_path(
                client_ip=client_ip,
                channel=channel,
                channel_id=channel_id,
                lookup=custom_lookup,
                resolved_url=resolved_url,
                redirect_kind='synthetic-live',
            )
            return redirect(
                f"{request.host_url.rstrip('/')}/play/custom/{encoded_id}/live.m3u8",
                302,
            )

    # Custom channels with segment proxying, required headers, or session-variant
    # masters: serve a manifest proxy so the client never needs to send custom
    # headers and stale session tokens are transparently refreshed.
    if source_name == 'custom' and (
        getattr(channel, 'proxy_segments', False)
        or custom_headers
        or bool((channel.custom_headers or {}).get('_session_variants'))
    ):
        from urllib.parse import quote as _quote
        encoded_id = _quote(channel.source_channel_id, safe='')
        _log_custom_play_path(
            client_ip=client_ip,
            channel=channel,
            channel_id=channel_id,
            lookup=custom_lookup,
            resolved_url=resolved_url,
            redirect_kind='proxy',
        )
        return redirect(
            f"{request.host_url.rstrip('/')}/play/custom/{encoded_id}/proxy.m3u8",
            302,
        )

    # Distro channels on the Referer-restricted CDN: serve a manifest proxy
    # instead of a direct redirect so IPTV clients can access the segments
    # (which are on an open CDN) without needing Origin/Referer headers.
    if source_name == 'distro' and resolved_url:
        from urllib.parse import urlsplit as _urlsplit
        if _urlsplit(resolved_url).netloc in _DISTRO_SESSION_CDN_HOSTS:
            from urllib.parse import quote as _quote
            encoded_id = _quote(channel.source_channel_id, safe='')
            return redirect(
                f"{request.host_url.rstrip('/')}/play/distro/{encoded_id}/proxy.m3u8",
                302,
            )

    # Fire-and-forget manifest check — detect DRM or dead streams without
    # blocking the redirect. The check runs in a background thread so Channels
    # DVR gets the 302 immediately, avoiding 504s on slow upstream sources.
    #
    # Skip for muxed/non-HLS streams (e.g. HDHomeRun MPEG-TS): the probe parses
    # HLS manifests, so it's useless here, AND a plain GET on a continuous live
    # TS never trips its inactivity timeout — it would buffer the stream forever
    # and pin a tuner. LAN OTA streams also can't carry DRM, so there's nothing
    # to detect.
    _is_muxed = (channel.stream_type or '').lower() in ('mpegts', 'ts', 'mp4')
    if channel.is_active and resolved_url and resolved_url.startswith('http') and not _is_muxed:
        from flask import current_app
        _app = current_app._get_current_object()
        _channel_id = channel.id
        _source_name = source_name
        _source_id = channel.source_id
        def _bg_check():
            import requests
            # Use a plain session without retry adapters — this is a one-shot
            # health probe; retries just add latency in the background thread.
            s = requests.Session()
            reason = _check_manifest(resolved_url, s)
            if not reason:
                return
            if reason == 'Unauthorized' and _source_name == 'roku':
                # OSM session token has expired. Clear both osm_session AND
                # stream_url_cache — all cached OSM URLs embed the same stale
                # token, and _load_stream_url_cache() would otherwise extract it
                # and rebuild _osm_session from the cache, defeating the clear.
                logger.warning('[play] Roku OSM token expired (401) — clearing osm_session and stream_url_cache')
                with _app.app_context():
                    try:
                        persist_source_config_updates(_source_id, {
                            'osm_session': None,
                            'stream_url_cache': None,  # None replaces; {} would merge (no-op)
                        })
                    except Exception as e:
                        logger.warning('[play] failed to clear osm_session: %s', e)
                return
            with _app.app_context():
                trigger_channel_auto_disable(_channel_id, reason)

        threading.Thread(target=_bg_check, daemon=True).start()

    logger.debug(
        '[play] redirect ip=%s source=%s channel_id=%s channel_name=%s → %s',
        client_ip, source_name, channel_id, channel.name, resolved_url[:80],
    )
    if source_name == 'custom':
        _log_custom_play_path(
            client_ip=client_ip,
            channel=channel,
            channel_id=channel_id,
            lookup=custom_lookup,
            resolved_url=resolved_url,
            redirect_kind='direct',
        )
    return redirect(resolved_url, 302)


@play_bp.route('/watch/<int:channel_id>')
def watch(channel_id):
    from .api import _get_playback_info
    from flask import make_response
    # Version param busts stale browser caches from before this route existed.
    if '_v' not in request.args:
        return redirect(f'{request.path}?_v=3', 302)
    channel = Channel.query.get_or_404(channel_id)
    info = _get_playback_info(channel, fast_mode=False)
    resp = make_response(render_template(
        'watch.html',
        channel=channel,
        play_url=info.get('preview_url') or info.get('play_url') or '',
        playback_mode=info.get('playback_mode', 'hls'),
        stream_type=info.get('stream_type', 'hls'),
        license_url=info.get('license_url') or '',
    ))
    resp.headers['Cache-Control'] = 'no-store'
    return resp

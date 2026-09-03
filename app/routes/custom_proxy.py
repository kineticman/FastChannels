"""Custom-channel playback detection and proxy routes."""
import logging
import re
import time as _time
from urllib.parse import parse_qs as _parse_qs, urljoin, urlsplit

import requests as _requests
from flask import Blueprint, Response, abort, request

from ..hls import inspect_hls_drm, parse_stream_info
from ..models import Channel, Source
from ..scrapers.distro import _pick_best_variant as _pick_best_hls_variant

logger = logging.getLogger(__name__)
custom_proxy_bp = Blueprint('custom_proxy', __name__)

_BROWSER_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/145.0.0.0 Safari/537.36'
)
_PRIVATE_IP_RE = re.compile(
    r'^(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|::1|0\.0\.0\.0)',
    re.IGNORECASE,
)
_unavailable_response = None
_stream_upstream_response = None


def configure_custom_proxy(*, unavailable_response, stream_upstream_response) -> None:
    """Inject generic playback responses owned by the parent play blueprint."""
    global _unavailable_response, _stream_upstream_response
    _unavailable_response = unavailable_response
    _stream_upstream_response = stream_upstream_response

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


_LIVE_PLAYLIST_MAX_SEGMENTS = 4

# True playlist-level tags — anything else seen before the first segment URI
# (e.g. a leading #EXT-X-DISCONTINUITY) actually describes that first
# segment, not the playlist as a whole, and must be dropped/kept together
# with it rather than pinned into the always-kept header.
_PLAYLIST_HEADER_TAG_PREFIXES = (
    '#EXTM3U',
    '#EXT-X-VERSION',
    '#EXT-X-TARGETDURATION',
    '#EXT-X-MEDIA-SEQUENCE',
    '#EXT-X-DISCONTINUITY-SEQUENCE',
    '#EXT-X-PLAYLIST-TYPE',
    '#EXT-X-INDEPENDENT-SEGMENTS',
    '#EXT-X-START',
    '#EXT-X-ALLOW-CACHE',
)


def _trim_live_playlist(text: str, max_segments: int = _LIVE_PLAYLIST_MAX_SEGMENTS) -> str:
    """
    Drop the oldest segments from a live HLS variant playlist, keeping only
    the last `max_segments`.

    Some upstream CDNs (e.g. ViewTV) advertise a sliding window in the
    manifest that is longer than how long they actually retain the segment
    files — segments toward the front of the window 404 well before they
    age out of the playlist.  Trimming forces every client to stay near the
    live edge, inside the CDN's real retention window, regardless of how far
    back that client would otherwise buffer.  No-op for VOD/finished
    playlists (EXT-X-ENDLIST present) or playlists already at/under the cap.
    """
    if '#EXT-X-ENDLIST' in text:
        return text

    header: list[str] = []
    groups: list[tuple[list[str], str]] = []
    pending_tags: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith('#EXTINF'):
            pending_tags.append(line)
        elif stripped and not stripped.startswith('#'):
            groups.append((pending_tags, line))
            pending_tags = []
        elif not groups and (not stripped or stripped.startswith(_PLAYLIST_HEADER_TAG_PREFIXES)):
            header.append(line)
        else:
            pending_tags.append(line)

    dropped = len(groups) - max_segments
    if dropped <= 0:
        return text
    kept = groups[dropped:]
    # Tags after the last segment URI (e.g. a trailing #EXT-X-DISCONTINUITY
    # ahead of a segment the CDN hasn't published yet) describe none of the
    # dropped groups — keep them instead of silently discarding them.
    trailing_tags = pending_tags
    # Any #EXT-X-DISCONTINUITY tags riding along with a dropped segment take
    # its discontinuity with it, so DISCONTINUITY-SEQUENCE (which counts how
    # many discontinuities preceded the playlist's first segment) has to
    # advance by the same amount, the same way MEDIA-SEQUENCE does below.
    discontinuities_dropped = sum(
        1 for tag_lines, _ in groups[:dropped] for t in tag_lines if t.strip() == '#EXT-X-DISCONTINUITY'
    )

    out_header = []
    saw_discontinuity_seq = False
    for line in header:
        stripped = line.strip()
        if stripped.startswith('#EXT-X-MEDIA-SEQUENCE:'):
            try:
                seq = int(stripped.split(':', 1)[1])
                line = f'#EXT-X-MEDIA-SEQUENCE:{seq + dropped}'
            except ValueError:
                pass
        elif stripped.startswith('#EXT-X-DISCONTINUITY-SEQUENCE:'):
            saw_discontinuity_seq = True
            if discontinuities_dropped:
                try:
                    seq = int(stripped.split(':', 1)[1])
                    line = f'#EXT-X-DISCONTINUITY-SEQUENCE:{seq + discontinuities_dropped}'
                except ValueError:
                    pass
        elif stripped.startswith('#EXT-X-PROGRAM-DATE-TIME:'):
            continue  # no longer describes the new first segment
        out_header.append(line)

    if discontinuities_dropped and not saw_discontinuity_seq:
        insert_at = 1 if out_header and out_header[0].strip() == '#EXTM3U' else 0
        out_header.insert(insert_at, f'#EXT-X-DISCONTINUITY-SEQUENCE:{discontinuities_dropped}')

    out_lines = out_header
    for tag_lines, uri in kept:
        out_lines.extend(tag_lines)
        out_lines.append(uri)
    out_lines.extend(trailing_tags)
    return '\n'.join(out_lines)


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


def _absolutize_hls_manifest(manifest_text: str, manifest_url: str) -> str:
    """Resolve playlist and URI attribute references against the fetched manifest."""
    def _rewrite_uri(match):
        return f'URI="{urljoin(manifest_url, match.group(1))}"'

    lines = []
    for line in manifest_text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            line = urljoin(manifest_url, stripped)
        elif 'URI="' in line:
            line = re.sub(r'URI="([^"]+)"', _rewrite_uri, line)
        lines.append(line)
    return '\n'.join(lines)


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


@custom_proxy_bp.route('/play/custom/<channel_id>/proxy.m3u8')
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

    # browser=1 marks a same-origin browser preview (Shaka).  Re-detect up front
    # so the variant we proxy is fresh (shares the main path's TTL cache), rather
    # than fetching a possibly-expired stored URL and waiting on the 403 retry.
    browser_preview = request.args.get('browser') == '1'
    if (getattr(channel, 'redetect_on_play', False) or (browser_preview and channel.page_url)) and channel.page_url:
        stream_url, custom_headers = _redetect_custom_stream(channel)
        if not stream_url:
            return _unavailable_response()
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
            return _unavailable_response()

        text = master_r.text
        effective_url = master_r.url

        # If it's a master playlist, resolve and store the variant URL in Redis
        if '#EXT-X-STREAM-INF' in text:
            best = _pick_best_hls_variant(text, effective_url)
            if not best:
                return _unavailable_response()
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
                return _unavailable_response()

    # _session_variants marks ViewTV-style SSAI ad-stitched manifests, whose
    # origin servers have been observed evicting segments (~35-45s) faster
    # than the window they advertise (~58s), so untrimmed clients can be
    # handed a segment URL that's already gone.  Scoped to that marker rather
    # than all custom manifests, so channels without this CDN behavior keep
    # their full look-ahead buffer.
    session_variants = bool((channel.custom_headers or {}).get('_session_variants'))
    if session_variants:
        text = _trim_live_playlist(text)

    # Unless the channel explicitly requested segment proxying, leave segments
    # as direct absolute URLs.  YouTube/googlevideo HLS segment URLs already
    # work when fetched directly (from a matching IP, no CORS) for IPTV clients,
    # and the extra proxy hop can introduce 403s.  Browser previews (browser=1)
    # are the exception: the browser enforces CORS that googlevideo doesn't
    # satisfy, so the segments must come back same-origin through our proxy.
    base_url = request.host_url.rstrip('/')
    variant_base = effective_url.rsplit('/', 1)[0] + '/'
    encoded_id = _quote(raw_id, safe='')
    proxy_segments = bool(getattr(channel, 'proxy_segments', False)) or browser_preview

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


@custom_proxy_bp.route('/play/custom/<channel_id>/direct')
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
            return _unavailable_response()
    else:
        custom_headers = channel.custom_headers or {}
        stream_url = channel.stream_url

    headers = _custom_proxy_headers(channel, custom_headers)
    range_header = request.headers.get('Range')
    if range_header:
        headers['Range'] = range_header

    try:
        r = _requests.get(stream_url, headers=headers, timeout=(5, 30), stream=True)
        if r.status_code not in (200, 206):
            abort(r.status_code)

        response_headers = {'Cache-Control': 'no-cache'}
        for key in ('Content-Type', 'Content-Length', 'Accept-Ranges', 'Content-Range', 'Last-Modified', 'ETag'):
            value = r.headers.get(key)
            if value:
                response_headers[key] = value

        return _stream_upstream_response(
            r,
            status=r.status_code,
            headers=response_headers,
            label='custom-direct',
        )
    except Exception as e:
        logger.warning('[custom-direct] fetch failed for %s: %s', raw_id, e)
        return _unavailable_response()


@custom_proxy_bp.route('/play/custom/<channel_id>/live.m3u8')
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
        return _unavailable_response()

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


@custom_proxy_bp.route('/play/custom/segment')
def custom_segment_proxy():
    """
    Segment proxy for custom channels.  Fetches the segment with the channel's
    stored custom_headers and streams the bytes back to the client.

    SSRF protection: requires https, blocks private IP ranges, and validates
    that the segment host shares a domain root with the channel's stored stream_url.
    """
    from urllib.parse import urlsplit

    # request.args already percent-decodes the query string once, so use the
    # value as-is.  A second unquote here corrupts segment URLs that contain
    # literal %XX sequences (e.g. googlevideo's xpc/…%3D%3D, sgoap/gir%3Dyes),
    # which mangles the signature and gets the chunk host to 403.
    url = request.args.get('url', '')
    raw_id = request.args.get('src', '')
    if not url or not raw_id:
        abort(400)

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
    # to prevent this endpoint being used as an open proxy.  The abort() must
    # live OUTSIDE the try — abort() raises an HTTPException, and a bare except
    # here would swallow it and fail the guard open.
    seg_host = parsed.netloc
    try:
        stored_host = urlsplit(channel.stream_url or '').netloc

        def _root(h: str) -> str:
            parts = h.split('.')
            return '.'.join(parts[-2:]) if len(parts) >= 2 else h

        host_ok = (
            not stored_host
            or seg_host == stored_host
            or _root(seg_host) == _root(stored_host)
        )
    except Exception:
        host_ok = True  # parse failure — fail open rather than break playback
        stored_host = '?'
    if not host_ok:
        logger.warning('[custom-seg-proxy] host mismatch seg=%s stored=%s', seg_host, stored_host)
        abort(403)

    # Network/timeout errors → 502.  A non-200 from the CDN is relayed with its
    # real status (e.g. 403/410) instead of being masked as 502, so players and
    # logs reflect what the upstream actually returned.
    try:
        r = _requests.get(url, headers=_custom_proxy_headers(channel), timeout=(5, 30), stream=True)
    except Exception as e:
        logger.warning('[custom-seg-proxy] upstream fetch error host=%s url=%s: %s', seg_host, url[:160], e)
        abort(502)

    if r.status_code != 200:
        logger.warning('[custom-seg-proxy] upstream HTTP %s host=%s url=%s', r.status_code, seg_host, url[:200])
        status = r.status_code if 400 <= r.status_code <= 599 else 502
        return Response(f'upstream returned HTTP {r.status_code}', status=status, content_type='text/plain')

    logger.debug('[custom-seg-proxy] 200 host=%s ct=%s len=%s',
                 seg_host, r.headers.get('Content-Type'), r.headers.get('Content-Length', '?'))
    return _stream_upstream_response(
        r,
        status=200,
        content_type=r.headers.get('Content-Type', 'video/MP2T'),
        headers={'Cache-Control': 'no-cache'},
        label='custom-seg-proxy',
    )

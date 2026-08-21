"""
/play/<source>/<channel_id>.m3u8

Resolves the real stream URL at request time and issues a 302 redirect.
If the resolved manifest contains DRM (SAMPLE-AES or AES-128), the channel
is automatically marked is_active=False so it drops out of M3U/EPG output.
It remains visible in the admin channels page so users can see what was
disabled and manually re-enable if desired.
"""
import json
import logging
import re
import secrets
import threading
import time as _time
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
from urllib.parse import quote as _url_quote, urljoin, urlsplit, urlunsplit, parse_qs as _parse_qs

import requests as _requests

from flask import Blueprint, redirect, abort, request, Response, render_template, g, stream_with_context, current_app
from app.config_store import persist_source_config_updates, persist_source_cache_updates, load_source_cache
from ..hls import inspect_hls_drm, parse_stream_info
from ..models import Channel, Source
from ..url import public_base_url
from ..scrapers import registry
from ..scrapers.distro import (
    CHANNEL_SCHEME as _DISTRO_SCHEME,
    SESSION_CDN_HOSTS as _DISTRO_SESSION_CDN_HOSTS,
    MANIFEST_PROXY_HOSTS as _DISTRO_MANIFEST_PROXY_HOSTS,
    HLS_HEADERS as _DISTRO_HLS_HEADERS,
    _resolve_from_feed as _distro_resolve_from_feed,
    _sanitize_url as _distro_sanitize_url,
    _split_qualified_channel_id as _distro_split_id,
    _pick_best_variant as _distro_pick_best_variant,
    DistroScraper,
)

# Persistent session for Distro CDN fetches (manifest proxy + segment proxy).
# Reuses TCP/TLS connections across the ~5s manifest poll interval, cutting
# per-poll latency by avoiding repeated handshakes to the same CloudFront host.
_DISTRO_PROXY_SESSION = _requests.Session()
_DISTRO_PROXY_SESSION.headers.update(_DISTRO_HLS_HEADERS)

_DISTRO_SEGMENT_SEQ_RE = re.compile(r'-(\d+)\.ts(?:[?#]|%3[fF]|%23|$)')
_DISTRO_PROXY_LOG_LAST: dict[str, float] = {}
_DISTRO_PROXY_LOG_INTERVAL = 30.0


def _distro_throttled_debug(channel_id: str, msg: str, *args) -> None:
    now = _time.monotonic()
    last = _DISTRO_PROXY_LOG_LAST.get(channel_id, 0.0)
    if now - last >= _DISTRO_PROXY_LOG_INTERVAL:
        _DISTRO_PROXY_LOG_LAST[channel_id] = now
        logger.debug(msg, *args)


def _distro_rewrite_media_playlist(
    text: str,
    playlist_url: str,
    channel_id: str,
    base_url: str,
    quote,
) -> tuple[str, dict]:
    """Rewrite Distro media playlists for stricter HLS clients.

    Caton playlists are browser-hostile in two ways: they set MEDIA-SEQUENCE to
    the last segment number instead of the first, and their AAC packets can crash
    Chrome after Shaka transmuxes TS to MSE. Serve Caton through the segment
    proxy as video-only TS and lag its live window behind the still-writing edge.
    """
    playlist_host = urlsplit(playlist_url).netloc
    trim_to_segments = None
    lag_segments = 2 if playlist_host == 'global.cgtn.cicc.media.caton.cloud' else 0
    variant_base = playlist_url.rsplit('/', 1)[0] + '/'

    header_lines: list[str] = []
    pending_tags: list[str] = []
    segment_blocks: list[list[str]] = []
    first_seg_seq: int | None = None
    last_seg_seq: int | None = None
    old_seq: int | None = None
    proxied_segments = 0
    seen_segment = False

    for raw_line in text.splitlines():
        line = raw_line
        stripped = line.strip()
        if stripped.startswith('#EXT-X-MEDIA-SEQUENCE:'):
            try:
                old_seq = int(stripped.split(':', 1)[1])
            except Exception:
                old_seq = None
            if not seen_segment:
                header_lines.append(line)
            else:
                pending_tags.append(line)
            continue

        if stripped and not stripped.startswith('#'):
            abs_url = urljoin(variant_base, stripped)
            seg_match = _DISTRO_SEGMENT_SEQ_RE.search(abs_url)
            if seg_match:
                seg_seq = int(seg_match.group(1))
                if first_seg_seq is None:
                    first_seg_seq = seg_seq
                last_seg_seq = seg_seq
            seg_host = urlsplit(abs_url).netloc
            if seg_host == 'global.cgtn.cicc.media.caton.cloud':
                proxied_segments += 1
                line = f'{base_url}/play/distro/segment?url={quote(abs_url, safe="")}&video_only=1'
            elif seg_host in _DISTRO_SESSION_CDN_HOSTS:
                proxied_segments += 1
                line = f'{base_url}/play/distro/segment?url={quote(abs_url, safe="")}'
            else:
                line = abs_url
            segment_blocks.append(pending_tags + [line])
            pending_tags = []
            seen_segment = True
        elif seen_segment or pending_tags or stripped.startswith(('#EXTINF', '#EXT-X-BYTERANGE', '#EXT-X-DISCONTINUITY')):
            pending_tags.append(line)
        else:
            header_lines.append(line)

    original_segment_count = len(segment_blocks)
    trimmed_segments = 0
    if lag_segments and len(segment_blocks) > lag_segments:
        segment_blocks = segment_blocks[:-lag_segments]
        trimmed_segments += lag_segments
    if trim_to_segments and len(segment_blocks) > trim_to_segments:
        trimmed_segments += len(segment_blocks) - trim_to_segments
        segment_blocks = segment_blocks[-trim_to_segments:]
    if lag_segments or trim_to_segments:
        kept_uris = [ln for block in segment_blocks for ln in block if ln.startswith(('http://', 'https://'))]
        first_match = _DISTRO_SEGMENT_SEQ_RE.search(kept_uris[0]) if kept_uris else None
        last_match = _DISTRO_SEGMENT_SEQ_RE.search(kept_uris[-1]) if kept_uris else None
        if first_match:
            first_seg_seq = int(first_match.group(1))
        if last_match:
            last_seg_seq = int(last_match.group(1))

    fixed_seq = None
    if old_seq is not None and first_seg_seq is not None and old_seq != first_seg_seq:
        fixed_seq = first_seg_seq
        replaced = False
        for idx, line in enumerate(header_lines):
            if line.strip().startswith('#EXT-X-MEDIA-SEQUENCE:'):
                header_lines[idx] = f'#EXT-X-MEDIA-SEQUENCE:{first_seg_seq}'
                replaced = True
                break
        if not replaced:
            insert_at = 1 if header_lines and header_lines[0].strip() == '#EXTM3U' else 0
            header_lines.insert(insert_at, f'#EXT-X-MEDIA-SEQUENCE:{first_seg_seq}')

    lines = header_lines[:]
    for block in segment_blocks:
        lines.extend(block)
    lines.extend(pending_tags)

    info = {
        'old_seq': old_seq,
        'fixed_seq': fixed_seq,
        'first_seg_seq': first_seg_seq,
        'last_seg_seq': last_seg_seq,
        'segment_count': len(segment_blocks),
        'original_segment_count': original_segment_count,
        'trimmed_segments': trimmed_segments,
        'lag_segments': lag_segments,
        'proxied_segments': proxied_segments,
    }
    return '\n'.join(lines), info

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


_VIDEO_STREAM_TYPES = {0x01, 0x02, 0x10, 0x1B, 0x24, 0x27, 0x42, 0xD1}


def _mpeg_crc32(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte << 24
        for _ in range(8):
            if crc & 0x80000000:
                crc = ((crc << 1) ^ 0x04C11DB7) & 0xFFFFFFFF
            else:
                crc = (crc << 1) & 0xFFFFFFFF
    return crc & 0xFFFFFFFF


def _ts_payload_offset(packet: bytes) -> int | None:
    if len(packet) != 188 or packet[0] != 0x47:
        return None
    afc = (packet[3] >> 4) & 0x03
    if afc == 0 or afc == 2:
        return None
    off = 4
    if afc == 3:
        off += 1 + packet[4]
    return off if off < 188 else None


def _psi_section_from_packet(packet: bytes) -> bytes | None:
    off = _ts_payload_offset(packet)
    if off is None or not (packet[1] & 0x40):
        return None
    pointer = packet[off]
    start = off + 1 + pointer
    if start + 3 > 188:
        return None
    section_length = ((packet[start + 1] & 0x0F) << 8) | packet[start + 2]
    end = start + 3 + section_length
    if end > 188:
        return None
    return packet[start:end]


def _pat_pmt_pid(packet: bytes) -> int | None:
    section = _psi_section_from_packet(packet)
    if not section or section[0] != 0x00:
        return None
    section_length = ((section[1] & 0x0F) << 8) | section[2]
    pos = 8
    end = 3 + section_length - 4
    while pos + 4 <= end:
        program_number = (section[pos] << 8) | section[pos + 1]
        pid = ((section[pos + 2] & 0x1F) << 8) | section[pos + 3]
        if program_number != 0:
            return pid
        pos += 4
    return None


def _rewrite_pmt_video_only(packet: bytes) -> tuple[bytes, set[int]] | None:
    section = _psi_section_from_packet(packet)
    if not section or section[0] != 0x02:
        return None
    section_length = ((section[1] & 0x0F) << 8) | section[2]
    section_end = 3 + section_length - 4
    if section_end > len(section) or len(section) < 12:
        return None
    program_info_len = ((section[10] & 0x0F) << 8) | section[11]
    pos = 12 + program_info_len
    if pos > section_end:
        return None

    kept = bytearray(section[:pos])
    video_pids: set[int] = set()
    while pos + 5 <= section_end:
        stream_type = section[pos]
        elem_pid = ((section[pos + 1] & 0x1F) << 8) | section[pos + 2]
        es_info_len = ((section[pos + 3] & 0x0F) << 8) | section[pos + 4]
        entry_end = pos + 5 + es_info_len
        if entry_end > section_end:
            return None
        if stream_type in _VIDEO_STREAM_TYPES:
            kept.extend(section[pos:entry_end])
            video_pids.add(elem_pid)
        pos = entry_end

    if not video_pids:
        return None
    new_section = bytearray(kept)
    new_section_length = len(new_section) - 3 + 4
    new_section[1] = (new_section[1] & 0xF0) | ((new_section_length >> 8) & 0x0F)
    new_section[2] = new_section_length & 0xFF
    crc = _mpeg_crc32(bytes(new_section))
    new_section.extend(crc.to_bytes(4, 'big'))

    pid_hi = packet[1] & 0x1F
    pid_lo = packet[2]
    continuity = packet[3] & 0x0F
    payload = b'\x00' + bytes(new_section)
    if len(payload) > 184:
        return None
    new_packet = bytes([0x47, 0x40 | pid_hi, pid_lo, 0x10 | continuity]) + payload
    new_packet += b'\xFF' * (188 - len(new_packet))
    return new_packet, video_pids


def _distro_filter_ts_video_only(data: bytes) -> bytes:
    """Return MPEG-TS bytes with audio streams removed from the PMT and payload.

    Used only for Caton Distro browser preview segments whose AAC packets crash
    Chrome after Shaka transmuxing. If parsing fails, return original bytes.
    """
    if len(data) < 188 or len(data) % 188 != 0:
        return data
    packets = [data[i:i + 188] for i in range(0, len(data), 188)]
    pmt_pid = None
    for packet in packets[:200]:
        pid = ((packet[1] & 0x1F) << 8) | packet[2]
        if pid == 0:
            pmt_pid = _pat_pmt_pid(packet)
            if pmt_pid is not None:
                break
    if pmt_pid is None:
        return data

    video_pids: set[int] = set()
    rewritten_pmt: bytes | None = None
    for packet in packets[:500]:
        pid = ((packet[1] & 0x1F) << 8) | packet[2]
        if pid == pmt_pid:
            rewritten = _rewrite_pmt_video_only(packet)
            if rewritten:
                rewritten_pmt, video_pids = rewritten
                break
    if not rewritten_pmt or not video_pids:
        return data

    keep_pids = {0, pmt_pid, 0x1FFF} | video_pids
    out = bytearray()
    for packet in packets:
        pid = ((packet[1] & 0x1F) << 8) | packet[2]
        if pid == pmt_pid:
            out.extend(rewritten_pmt)
        elif pid in keep_pids:
            out.extend(packet)
    return bytes(out) or data


from ..scrapers.cspan import (
    CDN_HEADERS as _CSPAN_CDN_HEADERS,
    CDN_HOST_SUFFIX as _CSPAN_CDN_HOST_SUFFIX,
    STALE_VOD_MINUTES as _CSPAN_STALE_VOD_MINUTES,
    pick_best_variant as _cspan_pick_best_variant,
    build_live_window as _cspan_build_live_window,
    rewrite_media_playlist as _cspan_rewrite_media_playlist,
    latest_program_date_time as _cspan_latest_program_date_time,
    CSpanScraper,
)

# Persistent session for C-SPAN segment/manifest fetches. Carries the Referer
# the segment CDN (m3u8-l2.c-spanvideo.org) requires; reuses TCP/TLS across the
# ~6s manifest poll interval.
_CSPAN_PROXY_SESSION = _requests.Session()
_CSPAN_PROXY_SESSION.headers.update(_CSPAN_CDN_HEADERS)


from ..scrapers.base import StreamDeadError
from ..tve.adobe_pass import TVENotAuthorizedError
from .tasks import trigger_channel_auto_disable

logger = logging.getLogger(__name__)

play_bp = Blueprint('play', __name__)

_BROWSER_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/145.0.0.0 Safari/537.36'
)


def _gone_response() -> Response:
    """410 for a confirmed-dead channel (which is also being auto-disabled).
    Tells clients the resource is permanently gone so they stop retrying."""
    return Response(
        'Channel is no longer available.\n',
        status=410,
        mimetype='text/plain',
    )


def _unavailable_response() -> Response:
    """503 + Retry-After for a transient upstream failure (timeout, CDN hiccup,
    expired token). Well-behaved clients back off and retry instead of hammering
    or giving up permanently."""
    return Response(
        'Stream temporarily unavailable.\n',
        status=503,
        mimetype='text/plain',
        headers={'Retry-After': '30'},
    )

def _stream_upstream_response(
    upstream: _requests.Response,
    *,
    status: int = 200,
    content_type: str | None = None,
    headers: dict | None = None,
    label: str = 'upstream',
) -> Response:
    request_id = getattr(g, 'request_id', '-')
    upstream_status = upstream.status_code
    bytes_sent = 0

    def generate():
        nonlocal bytes_sent
        try:
            for chunk in upstream.iter_content(chunk_size=64 * 1024):
                if chunk:
                    bytes_sent += len(chunk)
                    yield chunk
        except _requests.RequestException as exc:
            _record_proxy_failure(
                label=label,
                request_id=request_id,
                upstream_status=upstream_status,
                bytes_sent=bytes_sent,
                error=exc,
            )
            logger.warning(
                '[%s] stream interrupted request_id=%s upstream_status=%s bytes=%s: %s',
                label, request_id, upstream_status, bytes_sent, exc,
            )
        except Exception as exc:
            _record_proxy_failure(
                label=label,
                request_id=request_id,
                upstream_status=upstream_status,
                bytes_sent=bytes_sent,
                error=type(exc).__name__,
            )
            logger.exception(
                '[%s] stream failed request_id=%s upstream_status=%s bytes=%s',
                label, request_id, upstream_status, bytes_sent,
            )
        finally:
            upstream.close()
            logger.debug(
                '[%s] stream closed request_id=%s upstream_status=%s bytes=%s',
                label, request_id, upstream_status, bytes_sent,
            )

    response = Response(
        stream_with_context(generate()),
        status=status,
        content_type=content_type,
        headers=headers or {},
    )
    response.call_on_close(upstream.close)
    return response


_PROXY_FAILURE_REDIS = None


def _proxy_failure_redis():
    global _PROXY_FAILURE_REDIS
    if _PROXY_FAILURE_REDIS is not None:
        return _PROXY_FAILURE_REDIS
    try:
        import redis as _redis
        _PROXY_FAILURE_REDIS = _redis.from_url(
            current_app.config['REDIS_URL'],
            socket_connect_timeout=1,
            socket_timeout=1,
        )
    except Exception:
        _PROXY_FAILURE_REDIS = False
    return _PROXY_FAILURE_REDIS


def _record_proxy_failure(
    *,
    label: str,
    request_id: str,
    upstream_status: int,
    bytes_sent: int,
    error: str,
) -> None:
    rdb = _proxy_failure_redis()
    if not rdb:
        return
    try:
        event = json.dumps({
            'timestamp': _time.time(),
            'label': label,
            'request_id': request_id,
            'upstream_status': upstream_status,
            'bytes': bytes_sent,
            'error': str(error)[:240],
        })
        key = 'fc:proxy:failures'
        rdb.lpush(key, event)
        rdb.ltrim(key, 0, 49)
        rdb.expire(key, 900)
    except Exception:
        pass


def get_recent_proxy_failures(since: float | None = None) -> list[dict]:
    rdb = _proxy_failure_redis()
    if not rdb:
        return []
    try:
        events = []
        for raw in rdb.lrange('fc:proxy:failures', 0, 49):
            event = json.loads(raw)
            if since is not None and float(event.get('timestamp', 0)) < since:
                continue
            events.append(event)
        return events
    except Exception:
        return []


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

def _client_ip() -> str:
    forwarded = (request.headers.get('X-Forwarded-For') or '').strip()
    if forwarded:
        return forwarded.split(',', 1)[0].strip()
    real_ip = (request.headers.get('X-Real-IP') or '').strip()
    if real_ip:
        return real_ip
    return request.remote_addr or 'unknown'


def _stream_info_summary(si: dict | None) -> tuple:
    """Comparable tuple of the badge-relevant fields of a stream_info dict.
    Used to skip a DB write when only volatile fields (per-session variant
    bandwidths/ordering) changed but the displayed resolution/codec did not."""
    si = si or {}
    return (
        si.get('max_resolution'), si.get('max_height'), si.get('video_codec'),
        bool(si.get('has_4k')), bool(si.get('has_hd')), bool(si.get('resolution_estimated')),
    )


def _refresh_stream_info_async(app, channel_id: int, current_stream_info: dict | None, master_text: str) -> None:
    """Parse stream_info from an already-fetched HLS master playlist and persist it
    on a background thread when the displayed resolution/codec summary changed.

    Used by the manifest-proxy endpoints (stirr/…), which fetch the master server-side
    on every poll. The summary guard means a watched channel is written at most once
    per actual resolution change — the per-poll DB read uses the caller's already-loaded
    `current_stream_info`, so a steady stream does no DB work at all after the first write.
    """
    new_info = parse_stream_info(master_text)
    if not new_info:
        return
    if _stream_info_summary(current_stream_info) == _stream_info_summary(new_info):
        return

    def _persist():
        with app.app_context():
            try:
                from ..extensions import db
                ch = db.session.get(Channel, channel_id)
                # Re-check under the fresh row to avoid a redundant write if another
                # worker/thread already refreshed it between the guard and here.
                if ch is not None and (
                    _stream_info_summary(ch.stream_info) != _stream_info_summary(new_info)
                ):
                    ch.stream_info = new_info
                    db.session.commit()
                    logger.debug('[play] refreshed stream_info for channel %s: %s',
                                 channel_id, new_info.get('max_resolution') or '?')
            except Exception as exc:
                from ..extensions import db
                db.session.rollback()
                logger.debug('[play] stream_info refresh failed for channel %s: %s', channel_id, exc)

    threading.Thread(target=_persist, daemon=True).start()


def _check_manifest(url: str, session) -> tuple[str | None, dict | None]:
    """
    Fetch the HLS manifest at url and return (reason, stream_info):
      reason       — a disable reason string if the stream is unplayable, else None.
                     'Unauthorized' on 401 so callers can handle expired tokens.
                     None on any fetch error (fail open — don't disable on hiccups).
      stream_info  — parsed master-playlist variant stats (resolution/codec) when
                     `url` resolved to a master playlist, else None. Lets the caller
                     refresh the resolution badge for free off this same fetch.
    """
    stream_info = None
    try:
        from urllib.parse import urljoin
        r = session.get(url, timeout=8)
        if r.status_code == 401:
            return 'Unauthorized', None
        if r.status_code != 200:
            return None, None
        text = r.text

        # EXT-X-KEY and EXT-X-PLAYLIST-TYPE only appear in media playlists,
        # not master playlists. If we landed on a master, parse stream_info from
        # it (before drilling in) then fetch the first variant for the DRM/VOD check.
        if '#EXT-X-STREAM-INF' in text:
            stream_info = parse_stream_info(text)
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
            return 'VOD', stream_info

        drm = inspect_hls_drm(text)
        if drm:
            logger.info('[play] DRM detected (%s) in manifest: %s', drm['drm_type'], url[:80])
            return 'DRM', stream_info
    except Exception as e:
        logger.debug('[play] manifest check fetch failed (ignoring): %s', e)
    return None, stream_info


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


def _host_in_cdn_suffix(host: str, suffix: str) -> bool:
    """True only if ``host`` is exactly ``suffix`` or a real subdomain of it.

    A bare ``host.endswith(suffix)`` matches attacker-registrable siblings like
    ``xc-spanvideo.org`` for the suffix ``c-spanvideo.org`` (no label boundary),
    which turns the segment proxy into an SSRF/open relay. Anchor on a leading
    dot so only the domain and its subdomains pass.
    """
    host = host.lower()
    suffix = suffix.lower()
    return host == suffix or host.endswith('.' + suffix)

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
        video_only = request.args.get('video_only') == '1' and parsed.netloc == 'global.cgtn.cicc.media.caton.cloud'
        r = _DISTRO_PROXY_SESSION.get(url, timeout=(5, 30), stream=not video_only)
        if r.status_code != 200:
            abort(r.status_code)
        if video_only:
            body = _distro_filter_ts_video_only(r.content)
            return Response(
                body,
                status=200,
                content_type=r.headers.get('Content-Type', 'video/MP2T'),
                headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
            )
        return _stream_upstream_response(
            r,
            status=200,
            content_type=r.headers.get('Content-Type', 'video/MP2T'),
            headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
            label='distro-seg-proxy',
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
    if '#EXTINF' in master_r.text and '#EXT-X-STREAM-INF' not in master_r.text:
        return effective_master_url, master_r

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


@play_bp.route('/play/cspan/segment')
def cspan_segment_proxy():
    """
    Segment proxy for C-SPAN's media CDN (m3u8-l2.c-spanvideo.org), which 403s
    any request lacking `Referer: https://www.c-span.org/`. Segment URLs come
    from manifests we already fetched from C-SPAN's own hosts, so we only guard
    HTTPS + host suffix + private IPs against SSRF.
    """
    from urllib.parse import urlsplit, unquote as _unquote
    raw = request.args.get('url', '')
    if not raw:
        abort(400)
    url = _unquote(raw)
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or not parsed.netloc:
        abort(400)
    host = parsed.netloc.split(':')[0].lower()
    if not _host_in_cdn_suffix(host, _CSPAN_CDN_HOST_SUFFIX):
        logger.warning('[cspan-seg] blocked non-C-SPAN host: %s', host)
        abort(403)
    if _PRIVATE_IP_RE.match(host):
        abort(403)
    try:
        # allow_redirects=False: the host allowlist only vets the original URL,
        # so a 3xx to an internal address would otherwise bypass it (SSRF).
        r = _CSPAN_PROXY_SESSION.get(url, timeout=(5, 30), stream=True, allow_redirects=False)
        if r.status_code != 200:
            abort(r.status_code)
        return _stream_upstream_response(
            r,
            status=200,
            content_type=r.headers.get('Content-Type', 'video/MP2T'),
            headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
            label='cspan-seg-proxy',
        )
    except _requests.RequestException as e:
        # Narrow to request errors so the abort(r.status_code) above (a werkzeug
        # HTTPException, itself an Exception) propagates the real CDN status
        # instead of being masked as 502 — a rolled-segment 404 / Referer 403
        # should surface as-is for logging and client backoff.
        logger.warning('[cspan-seg] fetch failed for %s: %s', url[:80], e)
        abort(502)


@play_bp.route('/play/cspan/<channel_id>/proxy.m3u8')
def cspan_manifest_proxy(channel_id: str):
    """
    Manifest proxy for C-SPAN floor feeds.

    Resolves the live event's master (via the scraper's cached /congress/ scrape),
    picks the best variant, fetches its media playlist, and rewrites segment URLs
    to go through cspan_segment_proxy (which adds the required Referer). Manifests
    are on an open host, but segments are Referer-gated, so the client can't fetch
    them directly.
    """
    from urllib.parse import urlsplit, unquote as _unquote, quote as _quote

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'cspan', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper = CSpanScraper(config=channel.source.config or {})

    def _fetch_live(force: bool):
        """Resolve → master → best variant. Returns ((variant_r, master_r), None)
        on success, or (None, reason) where reason is 'gone' (nothing live now),
        'dead' (event manifest 403'd — a floor session rolled to a new Part), or
        'error'."""
        try:
            m_url = scraper.resolve(channel.stream_url, force=force)
        except Exception as e:
            logger.warning('[cspan-proxy] resolve failed for %s: %s', raw_id, e)
            return None, 'error'
        if not m_url or not m_url.startswith('http'):
            return None, 'gone'
        # The master URL comes from a scraped /congress page (data-videofile).
        # Vet its host against the C-SPAN CDN allowlist before fetching it
        # server-side, so a poisoned/injected page can't turn this into an SSRF.
        m_host = urlsplit(m_url).netloc.split(':')[0].lower()
        if not _host_in_cdn_suffix(m_host, _CSPAN_CDN_HOST_SUFFIX):
            logger.warning('[cspan-proxy] blocked non-C-SPAN master host for %s: %s', raw_id, m_host)
            return None, 'gone'
        try:
            m_r = _CSPAN_PROXY_SESSION.get(m_url, timeout=10)
        except Exception as e:
            logger.warning('[cspan-proxy] master fetch failed for %s: %s', raw_id, e)
            return None, 'error'
        if m_r.status_code != 200:
            return None, 'dead'
        v_url = _cspan_pick_best_variant(m_r.text, m_r.url)
        if v_url:
            try:
                v_r = _CSPAN_PROXY_SESSION.get(v_url, timeout=10)
                v_r.raise_for_status()
            except Exception as e:
                logger.warning('[cspan-proxy] variant fetch failed for %s: %s', raw_id, e)
                return None, 'error'
        elif '#EXTINF' in m_r.text:
            # Some event manifests return a media playlist directly, not a master.
            v_r = m_r
        else:
            return None, 'error'
        return (v_r, m_r), None

    result, reason = _fetch_live(force=False)
    # A dead master (403) means the cached event id 403'd — a floor session rolled
    # to a new Part. Force one fresh discovery (rate-limited in the scraper).
    if reason == 'dead':
        result, reason = _fetch_live(force=True)
    if result is None:
        # 'gone' (recess) / 'dead' / 'error' — all transient, not a dead channel.
        return _unavailable_response()
    variant_r, master_r = result

    # An #EXT-X-ENDLIST means this event has ended (a floor session's Part is done).
    # Prefer a newer LIVE Part if one exists; otherwise fall through and serve the
    # ended session as a finite VOD (below) so the player shows the recorded session
    # rather than failing.
    if '#EXT-X-ENDLIST' in variant_r.text:
        logger.info('[cspan-proxy] %s event ended — checking for a newer live Part', raw_id)
        alt, _alt_reason = _fetch_live(force=True)
        if alt is not None and '#EXT-X-ENDLIST' not in alt[0].text:
            variant_r, master_r = alt

    # Still ended after the recheck above: only serve it as a "just wrapped up"
    # VOD if it's actually recent. /congress/?chamber=<x> has no live/ended
    # signal of its own — it keeps pointing at the last-aired event's manifest
    # long after it ended (observed: still serving a session from the PREVIOUS
    # day) — so without this check a quiet chamber replays a stale clip from the
    # top on every play, which looks like the stream is "stuck on repeat".
    if '#EXT-X-ENDLIST' in variant_r.text:
        last = _cspan_latest_program_date_time(variant_r.text)
        if last is not None:
            age = datetime.now(timezone.utc) - last
            if age > timedelta(minutes=_CSPAN_STALE_VOD_MINUTES):
                logger.info(
                    '[cspan-proxy] %s ended session is stale (last segment %s, %.0fm old) — '
                    'unavailable instead of replaying it',
                    raw_id, last.isoformat(), age.total_seconds() / 60,
                )
                return _unavailable_response()

    # Refresh the resolution/codec badge off the master we just fetched.
    from flask import current_app as _current_app
    _refresh_stream_info_async(
        _current_app._get_current_object(), channel.id, channel.stream_info, master_r.text,
    )

    base_url = request.host_url.rstrip('/')

    def _rewrite_segment(abs_url: str) -> str:
        seg_host = urlsplit(abs_url).netloc.split(':')[0].lower()
        # Route C-SPAN-hosted segments through the Referer-adding proxy; leave
        # anything else (should not occur) direct.
        if _host_in_cdn_suffix(seg_host, _CSPAN_CDN_HOST_SUFFIX):
            return f'{base_url}/play/cspan/segment?url={_quote(abs_url, safe="")}'
        return abs_url

    if '#EXT-X-ENDLIST' in variant_r.text:
        # Ended session → serve the whole thing as a finite VOD (keep ENDLIST) so
        # the player shows the recorded session/slate instead of failing. Keeping
        # ENDLIST is what prevents the stall: the player treats it as VOD, not a
        # live stream waiting on segments that will never come.
        body = _cspan_rewrite_media_playlist(variant_r.text, variant_r.url, _rewrite_segment)
    else:
        # Live session → C-SPAN's EVENT playlist holds the whole session from
        # segment 0, so trim to a sliding window that begins at the live edge.
        body = _cspan_build_live_window(variant_r.text, variant_r.url, _rewrite_segment)

    return Response(
        body,
        mimetype='application/vnd.apple.mpegurl',
        headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
    )


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
        return _unavailable_response()
    upstream_url = _distro_sanitize_url(upstream_url)

    result = _distro_fetch_variant(upstream_url, channel_id)
    if result is None:
        # One retry with a forced feed refresh in case the upstream URL itself changed
        upstream_url = _distro_resolve_from_feed(scraper, geo, raw_id, force_refresh=True)
        if upstream_url:
            upstream_url = _distro_sanitize_url(upstream_url)
            result = _distro_fetch_variant(upstream_url, channel_id)
    if result is None:
        return _unavailable_response()

    best_variant, variant_r = result

    # Proxy segments only when needed: header-gated Distro CDNs need the normal
    # segment proxy, while Caton needs the video-only segment proxy for Chrome/Shaka.
    # Other hosts are rewritten to absolute CDN URLs and fetched directly.
    base_url = request.host_url.rstrip('/')
    body, info = _distro_rewrite_media_playlist(
        variant_r.text,
        best_variant,
        channel_id,
        base_url,
        _quote,
    )
    if info.get('fixed_seq') is not None:
        _distro_throttled_debug(
            channel_id,
            '[distro-proxy] normalized media sequence channel=%s old=%s fixed=%s first_seg=%s last_seg=%s segments=%s trimmed=%s lag=%s',
            channel_id,
            info.get('old_seq'),
            info.get('fixed_seq'),
            info.get('first_seg_seq'),
            info.get('last_seg_seq'),
            info.get('segment_count'),
            info.get('trimmed_segments'),
            info.get('lag_segments'),
        )
    else:
        _distro_throttled_debug(
            channel_id,
            '[distro-proxy] served channel=%s host=%s seq=%s first_seg=%s last_seg=%s segments=%s trimmed=%s lag=%s proxied_segments=%s',
            channel_id,
            urlsplit(best_variant).netloc,
            info.get('old_seq'),
            info.get('first_seg_seq'),
            info.get('last_seg_seq'),
            info.get('segment_count'),
            info.get('trimmed_segments'),
            info.get('lag_segments'),
            info.get('proxied_segments'),
        )

    return Response(
        body,
        mimetype='application/vnd.apple.mpegurl',
        headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
    )


@play_bp.route('/play/samsung/<channel_id>/proxy.m3u8')
def samsung_manifest_proxy(channel_id: str):
    """Fetch one Samsung master and make its relative playlist URLs absolute."""
    from urllib.parse import unquote as _unquote

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'samsung', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    if request.args.get('via') != 'play':
        logger.info(
            '[play] request ip=%s source=samsung channel_id=%s channel_name=%s',
            _client_ip(), raw_id, channel.name,
        )

    try:
        r = _requests.get(
            channel.stream_url,
            allow_redirects=True,
            timeout=10,
            headers={'User-Agent': 'okhttp/4.12.0'},
        )
        r.raise_for_status()
    except Exception as e:
        logger.warning('[samsung-proxy] master fetch failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

    return Response(
        _absolutize_hls_manifest(r.text, r.url),
        mimetype='application/vnd.apple.mpegurl',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
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
        return _unavailable_response()
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        master_url = scraper.resolve(channel.stream_url)
    except Exception as e:
        logger.warning('[pluto-proxy] resolve failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

    if not master_url or not master_url.startswith('http'):
        return _unavailable_response()

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
        return _unavailable_response()

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
        return _unavailable_response()
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        master_url = scraper.resolve(channel.stream_url)
    except Exception as e:
        logger.warning('[tubi-proxy] resolve failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

    if not master_url or not master_url.startswith('http'):
        return _unavailable_response()

    try:
        r = _requests.get(master_url, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[tubi-proxy] master fetch failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

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
        return _unavailable_response()
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        master_url = scraper.resolve(channel.stream_url)
    except StreamDeadError as e:
        logger.error('[fubo-proxy] channel confirmed dead for %s: %s', raw_id[:40], e)
        trigger_channel_auto_disable(channel.id, 'Dead')
        return _gone_response()
    except Exception as e:
        logger.warning('[fubo-proxy] resolve failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

    if not master_url or not master_url.startswith('http'):
        return _unavailable_response()

    try:
        from curl_cffi import requests as _cffi
        r = _cffi.get(master_url, impersonate='chrome', timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[fubo-proxy] master fetch failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

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
        r = _requests.get(url, timeout=(5, 30), stream=True)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[fubo-seg] fetch failed for %s: %s', url[:80], e)
        abort(502)

    content_type = r.headers.get('Content-Type', 'application/octet-stream')
    # Stream the segment chunk-by-chunk; r.content would buffer the whole
    # (multi-MB) segment into the worker before sending, defeating stream=True.
    return _stream_upstream_response(
        r,
        content_type=content_type,
        headers={'Access-Control-Allow-Origin': '*'},
        label='fubo-seg',
    )


@play_bp.route('/play/tcl/<channel_id>/proxy.m3u8')
def tcl_manifest_proxy(channel_id: str):
    """
    Manifest proxy for TCL TV+ channels routed through FutureToday/Publica's
    SSAI pipeline (stream-us-east-1.getpublica.com). That manifest CDN reflects
    Access-Control-Allow-Origin correctly, but the raw content segments it
    references live on a separate origin (e.g. ftlive.cachefly.net) that sends
    no CORS headers at all — Shaka's browser fetch() gets blocked outright
    (NETWORK.HTTP_ERROR). Proxy every tier so segments are always fetched
    server-side, regardless of which CDN Publica points to.
    """
    from urllib.parse import unquote as _unquote, quote as _quote
    import re as _re

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'tcl', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper_cls = registry.get('tcl')
    if not scraper_cls:
        return _unavailable_response()
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        master_url = scraper.resolve(channel.stream_url)
    except Exception as e:
        logger.warning('[tcl-proxy] resolve failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

    if not master_url or not master_url.startswith('http'):
        return _unavailable_response()

    try:
        r = _requests.get(master_url, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[tcl-proxy] master fetch failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

    effective_url = r.url
    base_url = request.host_url.rstrip('/')

    def _proxy_url(abs_url: str) -> str:
        return f'{base_url}/play/tcl/variant?url={_quote(abs_url, safe="")}'

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


@play_bp.route('/play/tcl/variant')
def tcl_variant_proxy():
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
    if not host.endswith('.getpublica.com'):
        logger.warning('[tcl-variant] blocked non-Publica host: %s', host)
        abort(403)
    if _PRIVATE_IP_RE.match(host):
        abort(403)

    try:
        r = _requests.get(url, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[tcl-variant] fetch failed for %s: %s', url[:80], e)
        abort(502)

    base_url = request.host_url.rstrip('/')

    def _seg_proxy(abs_url: str) -> str:
        return f'{base_url}/play/tcl/seg?url={_quote(abs_url, safe="")}'

    def _abs(rel: str) -> str:
        return rel if rel.startswith('http') else urljoin(url, rel)

    def _rewrite_key_uri(m):
        return f'URI="{_seg_proxy(_abs(m.group(1)))}"'

    lines = []
    for line in r.text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            # Segment URL — may point at any CDN Publica's SSAI stitches in
            # (content origin, ad-network creatives, etc.), not just cachefly.
            line = _seg_proxy(_abs(stripped))
        elif stripped.startswith('#EXT-X-KEY') and 'URI=' in stripped:
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


@play_bp.route('/play/tcl/seg')
def tcl_segment_proxy():
    """
    Fetch a TCL/Publica content segment server-side and hand it back with a
    permissive CORS header. Segments are only reachable via URLs we ourselves
    extracted from a variant playlist already validated in tcl_variant_proxy,
    so no host allowlist beyond blocking private IPs is applied here — Publica's
    SSAI stitches in segments from whatever CDN a given ad/content slot uses.
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
    if _PRIVATE_IP_RE.match(host):
        abort(403)

    try:
        r = _requests.get(url, timeout=(5, 30), stream=True)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[tcl-seg] fetch failed for %s: %s', url[:80], e)
        abort(502)

    # cachefly's Wasabi-backed origin sends a bogus Content-Type for .ts
    # segments (observed: text/vnd.trolltech.linguist) which can confuse
    # Shaka's segment sniffing. Only override for .ts video segments — this
    # proxy also serves the subtitle track's .vtt segments, which need their
    # real Content-Type preserved.
    upstream_ct = r.headers.get('Content-Type', '')
    if parsed.path.lower().endswith('.ts') and not upstream_ct.startswith(('video/', 'audio/')):
        content_type = 'video/mp2t'
    else:
        content_type = upstream_ct or 'application/octet-stream'

    return _stream_upstream_response(
        r,
        content_type=content_type,
        headers={'Access-Control-Allow-Origin': '*'},
        label='tcl-seg',
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



_WATCH_DEBUG_REDIS: 'redis.Redis | None' = None
_WATCH_DEBUG_KEY_PREFIX = 'watch_debug:'
_WATCH_DEBUG_TTL = 300


def _watch_debug_redis() -> 'redis.Redis | None':
    global _WATCH_DEBUG_REDIS
    if _WATCH_DEBUG_REDIS is None:
        try:
            import redis as _r
            from flask import current_app
            _WATCH_DEBUG_REDIS = _r.from_url(
                current_app.config['REDIS_URL'],
                decode_responses=True,
                socket_timeout=1,
                socket_connect_timeout=1,
            )
        except Exception:
            pass
    return _WATCH_DEBUG_REDIS


def _watch_debug_key(request_id: str) -> str:
    return _WATCH_DEBUG_KEY_PREFIX + re.sub(r'[^A-Za-z0-9_.:-]', '_', request_id[:80])


def get_watch_debug_snapshot(request_id: str) -> dict:
    request_id = (request_id or '').strip()[:80]
    if not request_id:
        return {}
    rdb = _watch_debug_redis()
    if not rdb:
        return {}
    try:
        raw = rdb.get(_watch_debug_key(request_id))
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def _record_watch_debug_snapshot(request_id: str, channel: Channel, event: str, state: dict, extra: dict) -> None:
    request_id = (request_id or '').strip()[:80]
    if not request_id or request_id == '-':
        return
    rdb = _watch_debug_redis()
    if not rdb:
        return
    key = _watch_debug_key(request_id)
    now = _time.time()
    try:
        existing_raw = rdb.get(key)
        existing = json.loads(existing_raw) if existing_raw else {}
    except Exception:
        existing = {}
    try:
        decoded = int(state.get('decoded') or 0)
    except Exception:
        decoded = 0
    try:
        ready_state = int(state.get('ready_state') or 0)
    except Exception:
        ready_state = 0
    snapshot = dict(existing)
    snapshot.update({
        'request_id': request_id,
        'channel_id': channel.id,
        'source': channel.source.name if channel.source else None,
        'source_channel_id': channel.source_channel_id,
        'channel_name': channel.name,
        'last_event': event,
        'last_state': {k: v for k, v in state.items() if v is not None},
        'last_extra': {str(k)[:40]: str(v)[:300] for k, v in extra.items()},
        'updated_at': now,
    })
    snapshot['events'] = int(snapshot.get('events') or 0) + 1
    snapshot['max_ready_state'] = max(int(snapshot.get('max_ready_state') or 0), ready_state)
    snapshot['max_decoded'] = max(int(snapshot.get('max_decoded') or 0), decoded)
    if event in {'video-playing', 'video-canplay', 'video-loadeddata'}:
        snapshot['browser_playback'] = True
    if event == 'player-error':
        snapshot['player_error'] = True
        snapshot['player_error_extra'] = snapshot['last_extra']
    # A single "last event" overwrites on every call, so a fast cascade (a fatal
    # decode error followed by appendBuffer failures once the media element is
    # dead, or transient status lines that flash by faster than a person can
    # read them) silently loses everything but the tail. Keep the full
    # chronological sequence instead — skip 'heartbeat' since that's just a
    # periodic liveness ping, not a state transition worth keeping.
    if event != 'heartbeat':
        event_log = list(existing.get('event_log') or [])
        event_log.append({'age_ms': state.get('age_ms'), 'event': event, 'extra': snapshot['last_extra']})
        snapshot['event_log'] = event_log[-60:]
    try:
        rdb.setex(key, _WATCH_DEBUG_TTL, json.dumps(snapshot))
    except Exception:
        pass


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
    except StreamDeadError as e:
        logger.error('[stirr-proxy] channel confirmed dead for %s: %s', channel_id, e)
        trigger_channel_auto_disable(channel.id, 'Dead')
        return _gone_response()
    except Exception as e:
        logger.warning('[stirr-proxy] resolve failed for %s: %s', channel_id, e)
        return _unavailable_response()

    if not master_url or not master_url.startswith(('http://', 'https://')):
        logger.warning('[stirr-proxy] resolve returned non-HTTP URL for %s: %s', channel_id, (master_url or '')[:60])
        return _unavailable_response()

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
        return _unavailable_response()

    # Refresh the resolution/codec badge off this same master fetch. STIRR redirects
    # here instead of through the generic play path, so this is the only play-time
    # hook that sees its manifest — and the lax-TLS session above reaches CDN hosts
    # (weathernationtv etc.) the audit's plain session can't. Fire-and-forget; the
    # summary guard means at most one write per actual resolution change.
    from flask import current_app as _current_app
    _refresh_stream_info_async(
        _current_app._get_current_object(), channel.id, channel.stream_info, master_r.text,
    )

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

    Segment URI handling depends on whether the CDN emits absolute or relative
    references:
      - Absolute (aniview SSAI): left as-is — the client fetches them directly.
      - Relative (e.g. stream.weathernationtv.com origin): resolved to absolute
        and routed through /play/stirr/segment.  These origins require a legacy
        SECLEVEL=1 TLS handshake many clients can't negotiate, so the server's
        lax-TLS session must do the fetch.  (Such segments are tokenless, so the
        relative form is a reliable signal that they share the variant's origin.)
    """
    import re as _re
    from urllib.parse import urlsplit, unquote as _unquote, quote as _quote
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

    base_url = request.host_url.rstrip('/')
    effective_url = r.url

    def _seg_proxy(abs_url: str) -> str:
        return f'{base_url}/play/stirr/segment?url={_quote(abs_url, safe="")}'

    def _rewrite_tag_uri(m):
        ref = m.group(1)
        if ref.startswith(('http://', 'https://')):
            return f'URI="{ref}"'
        return f'URI="{_seg_proxy(urljoin(effective_url, ref))}"'

    out_lines = []
    for line in r.text.splitlines():
        stripped = line.strip()
        if not stripped:
            out_lines.append(line)
        elif stripped.startswith('#'):
            # Relative key/init-segment refs (EXT-X-KEY / EXT-X-MAP) need the same
            # treatment; absolute ones are left untouched.
            if 'URI="' in stripped:
                line = _re.sub(r'URI="([^"]+)"', _rewrite_tag_uri, line)
            out_lines.append(line)
        elif stripped.startswith(('http://', 'https://')):
            out_lines.append(line)
        else:
            out_lines.append(_seg_proxy(urljoin(effective_url, stripped)))

    return Response(
        '\n'.join(out_lines),
        status=200,
        mimetype='application/vnd.apple.mpegurl',
        headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
    )


@play_bp.route('/play/stirr/segment')
def stirr_segment_proxy():
    """
    Proxy a STIRR media segment (or key / init segment) through the server's
    lax-TLS session.

    Used for CDN origins (e.g. stream.weathernationtv.com) that emit relative
    segment URIs and require a legacy SECLEVEL=1 TLS handshake.  Fetching these
    server-side guarantees playback regardless of the client's TLS stack.  The
    segments carry no session token, so nothing client-specific is forwarded.
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
        logger.warning('[stirr-segment] blocked SSRF attempt to: %s', parsed.netloc)
        abort(403)
    _hdrs = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        r = _stirr_session().get(url, headers=_hdrs, timeout=(5, 30), stream=True)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[stirr-segment] fetch failed for %s: %s', url[:80], e)
        abort(502)
    content_type = (r.headers.get('Content-Type') or 'video/mp2t').split(';')[0].strip()
    return _stream_upstream_response(
        r,
        status=200,
        content_type=content_type,
        headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
        label='stirr-segment',
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
            best = _distro_pick_best_variant(text, effective_url)
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


@play_bp.route('/play/custom/segment')
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
            persist_source_config_updates(channel.source_id, scraper._pending_config_updates)
        except Exception:
            pass
    if getattr(scraper, '_pending_cache_updates', None):
        try:
            persist_source_cache_updates(channel.source_id, scraper._pending_cache_updates)
        except Exception:
            pass

    if not dash_url or not dash_url.startswith('http'):
        logger.warning('[amazon-dash] no resolved URL for %s', raw_id[:40])
        return _unavailable_response()

    try:
        r = _requests.get(dash_url, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[amazon-dash] manifest fetch failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

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

    if request.args.get('kodi_bridge'):
        mpd = _mpd_keep_highest_bitrate_video(mpd, raw_id)

    return Response(
        mpd,
        mimetype='application/dash+xml',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


@play_bp.route('/play/roku/<channel_id>/dash.mpd')
def roku_dash_proxy(channel_id: str):
    """DASH (Widevine) variant for a Roku channel — the browser/EME path that the
    watch page and PrismCast bridge use when the HLS variant is FairPlay.

    Unlike Amazon, Roku's MPD CDN sends Access-Control-Allow-Origin and uses an
    absolute <BaseURL>, so no body proxy/rewrite is needed — we 302-redirect Shaka
    straight to the osm MPD. resolve_dash() also caches the per-session Widevine
    license URL (persisted to source_cache) so /play/roku/license can read it back.
    """
    from urllib.parse import unquote as _unquote

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'roku', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper_cls = registry.get('roku')
    if not scraper_cls:
        return _unavailable_response()
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        result = scraper.resolve_dash(channel.stream_url)
    except Exception as e:
        logger.warning('[roku-dash] resolve failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()
    finally:
        # Persist the dash_cache (license URL) so the license proxy — a separate
        # request — can read the per-session URL just resolved here.
        if getattr(scraper, '_pending_cache_updates', None):
            try:
                persist_source_cache_updates(channel.source_id, scraper._pending_cache_updates)
            except Exception:
                pass
        if scraper._pending_config_updates:
            try:
                persist_source_config_updates(channel.source_id, scraper._pending_config_updates)
            except Exception:
                pass

    mpd_url = (result or {}).get('mpd_url')
    if not mpd_url or not mpd_url.startswith('http'):
        logger.warning('[roku-dash] no DASH URL for %s', raw_id[:40])
        return _unavailable_response()

    if request.args.get('kodi_bridge'):
        try:
            r = _requests.get(mpd_url, timeout=10)
            r.raise_for_status()
        except Exception as e:
            logger.warning('[roku-dash] kodi-bridge manifest fetch failed for %s: %s', raw_id[:40], e)
            return redirect(mpd_url, code=302)
        mpd = _mpd_keep_highest_bitrate_video(r.text, raw_id)
        return Response(mpd, mimetype='application/dash+xml')

    return redirect(mpd_url, code=302)


def _mpd_keep_highest_bitrate_video(mpd: str, channel_id: str) -> str:
    """Strip every video Representation except the single highest-bandwidth one.

    Confirmed live 2026-08-21 on the Kodi HDMI-bridge Fire TV Stick: its MediaTek
    secure decoder only permits one secure MediaCodec instance at a time
    (CDVDVideoCodecAndroidMediaCodec::Open - InstanceGuard locked), so
    inputstream.adaptive's normal mid-stream ABR bump to a better representation
    fails and playback is stuck at whichever representation the codec opened
    first — usually the lowest, since that's what inputstream.adaptive starts
    conservatively. Advertising only the top rendition means the codec opens
    once at full quality and this device never attempts (or needs) a switch.

    Handles both ABR layouts seen across trusted sources: multiple Representations
    inside one video AdaptationSet (Roku, Amazon, Sling), and — confirmed on Philo —
    the ladder split across multiple sibling video AdaptationSets (same `group`,
    fewer/one Representation each) instead. Either way, exactly one Representation
    in exactly one video AdaptationSet per Period survives.
    """
    try:
        root = ET.fromstring(mpd.encode('utf-8'))
    except ET.ParseError:
        logger.debug('[dash] MPD parse failed for %s; returning original manifest', channel_id[:40])
        return mpd

    namespace = ''
    if root.tag.startswith('{'):
        namespace = root.tag[1:].split('}', 1)[0]
        ET.register_namespace('', namespace)

    period_tag = f'{{{namespace}}}Period' if namespace else 'Period'
    adaptation_tag = f'{{{namespace}}}AdaptationSet' if namespace else 'AdaptationSet'
    representation_tag = f'{{{namespace}}}Representation' if namespace else 'Representation'

    changed = False
    for period in root.iter(period_tag):
        video_sets = [a for a in period.findall(adaptation_tag) if a.get('mimeType', '').startswith('video/')]
        best = None  # (bandwidth, adaptation_set, representation)
        for adaptation_set in video_sets:
            for rep in adaptation_set.findall(representation_tag):
                bandwidth = int(rep.get('bandwidth') or 0)
                if best is None or bandwidth > best[0]:
                    best = (bandwidth, adaptation_set, rep)
        if best is None:
            continue
        _, best_set, best_rep = best
        for adaptation_set in video_sets:
            if adaptation_set is best_set:
                for rep in list(adaptation_set.findall(representation_tag)):
                    if rep is not best_rep:
                        adaptation_set.remove(rep)
                        changed = True
            else:
                period.remove(adaptation_set)
                changed = True

    if not changed:
        return mpd

    logger.debug('[dash] kodi-bridge: stripped to highest-bitrate video representation for %s', channel_id[:40])
    return ET.tostring(root, encoding='unicode', xml_declaration=True)



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


@play_bp.route('/play/fox_tve/<channel_id>/proxy.m3u8', defaults={'proxy_source_name': 'fox_tve'})
@play_bp.route('/play/fox_one/<channel_id>/proxy.m3u8', defaults={'proxy_source_name': 'fox_one'})
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


@play_bp.route('/play/fox_tve/key')
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



@play_bp.route('/play/cox/<channel_id>/dash.mpd')
def cox_dash_proxy(channel_id: str):
    """DASH (Widevine) manifest proxy for Cox Contour TVE channels.

    Cox exposes TVE channel URLs as .m3u8 in channelmap, but the matching
    .mpd?trred=false endpoint returns a Widevine DASH manifest. Proxy the MPD
    with permissive CORS and inject a BaseURL so Shaka resolves relative
    segment templates against the Cox CDN, not this FastChannels route.
    """
    from urllib.parse import unquote as _unquote, urljoin as _urljoin

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'cox', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper_cls = registry.get('cox')
    if not scraper_cls:
        return _unavailable_response()
    scraper_config = dict(channel.source.config or {})
    scraper = scraper_cls(config=scraper_config)
    try:
        dash_url = scraper.resolve(channel.stream_url)
    except Exception as e:
        logger.warning('[cox-dash] resolve failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()
    finally:
        if getattr(scraper, '_pending_cache_updates', None):
            try:
                persist_source_cache_updates(channel.source_id, scraper._pending_cache_updates)
            except Exception:
                pass
        if scraper._pending_config_updates:
            try:
                persist_source_config_updates(channel.source_id, scraper._pending_config_updates)
            except Exception:
                pass

    if not dash_url or not dash_url.startswith('http'):
        logger.warning('[cox-dash] no DASH URL for %s', raw_id[:40])
        return _unavailable_response()

    try:
        r = _requests.get(dash_url, timeout=10, headers={
            'Origin': 'https://watchtv.cox.com',
            'Referer': 'https://watchtv.cox.com/',
            'Accept': '*/*',
        })
        r.raise_for_status()
    except Exception as e:
        logger.warning('[cox-dash] manifest fetch failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

    mpd = _cox_strip_empty_mpd_periods(r.text, raw_id)
    if not re.search(r'<BaseURL\b', mpd):
        cdn_base = _urljoin(dash_url, '.')
        mpd = mpd.replace('<Period ', f'<BaseURL>{cdn_base}</BaseURL>\n  <Period ', 1)

    return Response(
        mpd,
        mimetype='application/dash+xml',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


def _cox_strip_empty_mpd_periods(mpd: str, channel_id: str) -> str:
    """Remove Cox event-only/empty periods that Shaka rejects before playback."""
    if '<Period' not in mpd:
        return mpd

    try:
        root = ET.fromstring(mpd.encode('utf-8'))
    except ET.ParseError:
        logger.debug('[cox-dash] MPD parse failed for %s; returning original manifest', channel_id[:40])
        return mpd

    namespace = ''
    if root.tag.startswith('{'):
        namespace = root.tag[1:].split('}', 1)[0]
        ET.register_namespace('', namespace)

    period_tag = f'{{{namespace}}}Period' if namespace else 'Period'
    adaptation_tag = f'{{{namespace}}}AdaptationSet' if namespace else 'AdaptationSet'
    periods = list(root.findall(period_tag))
    if not periods:
        return mpd

    empty_periods = [period for period in periods if not list(period.iter(adaptation_tag))]
    if not empty_periods or len(empty_periods) == len(periods):
        return mpd

    for period in empty_periods:
        root.remove(period)

    logger.info(
        '[cox-dash] stripped %d empty MPD period(s) for channel=%s',
        len(empty_periods),
        channel_id[:40],
    )
    return ET.tostring(root, encoding='unicode', xml_declaration=True)


@play_bp.route('/play/philo/<channel_id>/dash.mpd')
def philo_dash_proxy(channel_id: str):
    """DASH (Widevine) manifest proxy for a Philo channel — the browser/EME path
    the watch page and PrismCast bridge use.

    Philo's MPD is served from www.philo.com with CORS locked to its own origin,
    so Shaka can't fetch it cross-origin — we proxy the manifest body here with
    permissive CORS.  Segment URLs in the MPD are absolute CDN URLs whose hosts
    send Access-Control-Allow-Origin: * , so Shaka fetches those directly (no
    body rewrite needed).  resolve() also caches the per-session x-dt-auth-token
    (persisted to source_cache) so /play/philo/license can read it back.
    """
    from urllib.parse import unquote as _unquote

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'philo', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper_cls = registry.get('philo')
    if not scraper_cls:
        return _unavailable_response()
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        dash_url = scraper.resolve(channel.stream_url)
    except Exception as e:
        logger.warning('[philo-dash] resolve failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()
    finally:
        # Persist the dash_cache (auth token) so the license proxy — a separate
        # request — can read the per-session token just resolved here.
        if getattr(scraper, '_pending_cache_updates', None):
            try:
                persist_source_cache_updates(channel.source_id, scraper._pending_cache_updates)
            except Exception:
                pass
        if scraper._pending_config_updates:
            try:
                persist_source_config_updates(channel.source_id, scraper._pending_config_updates)
            except Exception:
                pass

    if not dash_url or not dash_url.startswith('http'):
        logger.warning('[philo-dash] no DASH URL for %s', raw_id[:40])
        return _unavailable_response()

    try:
        r = _requests.get(dash_url, timeout=10, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36',
            'Origin': 'https://www.philo.com',
            'Referer': 'https://www.philo.com/',
        })
        r.raise_for_status()
    except Exception as e:
        logger.warning('[philo-dash] manifest fetch failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

    # Philo's dynamic MPD includes a <Location> pointing back to Philo's
    # CORS-locked manifest URL. Shaka honors that on refresh and bypasses this
    # proxy, causing periodic NETWORK.HTTP_ERROR stalls. Remove it so refreshes
    # continue against the same same-origin /play/philo/.../dash.mpd URL.
    manifest_text = re.sub(r'<Location>.*?</Location>\s*', '', r.text, flags=re.DOTALL)

    if request.args.get('kodi_bridge'):
        manifest_text = _mpd_keep_highest_bitrate_video(manifest_text, raw_id)

    return Response(
        manifest_text,
        mimetype='application/dash+xml',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


@play_bp.route('/play/sling/<channel_id>/dash.mpd')
def sling_dash_proxy(channel_id: str):
    """DASH (Widevine) manifest proxy for Sling bridge playback.

    Sling's MPD is currently CORS-open and uses absolute BaseURL entries, so this
    route is mainly a same-origin debug hook matching the other DASH DRM sources.
    Segment URLs stay direct to Sling's CDN.
    """
    from urllib.parse import unquote as _unquote

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'sling', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper_cls = registry.get('sling')
    if not scraper_cls:
        return _unavailable_response()
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        dash_url = scraper.resolve(channel.stream_url)
    except Exception as e:
        logger.warning('[sling-dash] resolve failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()
    finally:
        if getattr(scraper, '_pending_cache_updates', None):
            try:
                persist_source_cache_updates(channel.source_id, scraper._pending_cache_updates)
            except Exception:
                pass
        if scraper._pending_config_updates:
            try:
                persist_source_config_updates(channel.source_id, scraper._pending_config_updates)
            except Exception:
                pass

    if not dash_url or not dash_url.startswith('http'):
        logger.warning('[sling-dash] no DASH URL for %s', raw_id[:40])
        return _unavailable_response()

    try:
        r = _requests.get(dash_url, timeout=10, headers={
            'Origin': 'https://watch.sling.com',
            'Referer': 'https://watch.sling.com/',
            'Accept': '*/*',
        })
        r.raise_for_status()
    except Exception as e:
        logger.warning('[sling-dash] manifest fetch failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

    mpd = r.text
    if request.args.get('kodi_bridge'):
        mpd = _mpd_keep_highest_bitrate_video(mpd, raw_id)

    return Response(
        mpd,
        mimetype='application/dash+xml',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


@play_bp.route('/play/vidaa/<channel_id>/dash.mpd')
def vidaa_dash_proxy(channel_id: str):
    """DASH (Widevine) manifest proxy for Vidaa's DRM tiles, for the watch page
    and PrismCast bridge. The upstream evrideo manifest has no CORS headers at
    all (unlike its segments, which reflect Origin fine), so proxy the manifest
    body server-side with permissive CORS; segments stay direct."""
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'vidaa', Channel.source_channel_id == channel_id)
        .first()
    )
    if not channel or not channel.stream_url:
        abort(404)

    try:
        r = _requests.get(channel.stream_url, timeout=10, allow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[vidaa-dash] manifest fetch failed for %s: %s', channel_id, e)
        return _unavailable_response()

    mpd = r.text
    if request.args.get('kodi_bridge'):
        mpd = _mpd_keep_highest_bitrate_video(mpd, channel_id)

    return Response(
        mpd,
        mimetype='application/dash+xml',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


@play_bp.route('/play/pbs/<channel_id>/dash.mpd')
def pbs_dash_proxy(channel_id: str):
    """DASH (Widevine) manifest proxy for an opt-in PBS DRM station feed — the
    browser/EME path the watch page and PrismCast bridge use.

    PBS's drm_dash_url is a redirect stub (urs-anonymous-detect.pbs.org/redirect/...)
    and CORS on the resolved CDN manifest is unconfirmed, so this proxies the
    manifest body server-side with permissive CORS, same as Philo/Sling.
    """
    from urllib.parse import unquote as _unquote, urljoin as _urljoin

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'pbs', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper_cls = registry.get('pbs')
    if not scraper_cls:
        return _unavailable_response()
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        dash_url = scraper.resolve(channel.stream_url)
    except Exception as e:
        logger.warning('[pbs-dash] resolve failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()
    finally:
        if getattr(scraper, '_pending_cache_updates', None):
            try:
                persist_source_cache_updates(channel.source_id, scraper._pending_cache_updates)
            except Exception:
                pass
        if scraper._pending_config_updates:
            try:
                persist_source_config_updates(channel.source_id, scraper._pending_config_updates)
            except Exception:
                pass

    if not dash_url or not dash_url.startswith('http'):
        logger.warning('[pbs-dash] no DASH URL for %s', raw_id[:40])
        return _unavailable_response()

    try:
        r = _requests.get(dash_url, timeout=10, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36',
            'Origin': 'https://player.pbs.org',
            'Referer': 'https://player.pbs.org/',
        })
        r.raise_for_status()
    except Exception as e:
        logger.warning('[pbs-dash] manifest fetch failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

    # Same issue as Philo: the dynamic MPD includes a <Location> pointing back to
    # PBS's own redirect stub. Shaka honors that on refresh and bypasses this
    # proxy, so strip it — refreshes then keep hitting this same-origin route.
    manifest_text = re.sub(r'<Location>.*?</Location>\s*', '', r.text, flags=re.DOTALL)

    # Some PBS feed packagers (confirmed on the ga-main/PBS-KIDS profiles) omit
    # <BaseURL> and use bare relative SegmentTemplate paths, which the DASH spec
    # resolves against the *manifest's own retrieval URL* — since we're serving
    # this manifest from our own /play/pbs/... URL, Shaka would resolve segments
    # against our origin instead of the real CDN, 404ing every segment. Inject an
    # explicit BaseURL pointing at the real upstream directory (r.url, the final
    # URL after redirects) so relative paths resolve correctly. Other profiles
    # (e.g. kids-main via MediaTailor) already ship an absolute BaseURL — leave
    # those alone.
    if '<BaseURL>' not in manifest_text:
        upstream_dir = _urljoin(str(r.url), '.')
        manifest_text = re.sub(
            r'(<MPD\b[^>]*>)',
            lambda m: f'{m.group(1)}<BaseURL>{upstream_dir}</BaseURL>',
            manifest_text, count=1,
        )

    if request.args.get('kodi_bridge'):
        manifest_text = _mpd_keep_highest_bitrate_video(manifest_text, raw_id)

    return Response(
        manifest_text,
        mimetype='application/dash+xml',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


@play_bp.route('/play/nbc_tve/<channel_id>/dash.mpd')
def nbc_tve_dash_proxy(channel_id: str):
    """DASH (Widevine) manifest proxy for nbc_tve — drops the Dolby Digital+
    (ec-3) audio AdaptationSet before handing the manifest to Shaka.

    NBC publishes both an ec-3 and a plain AAC (mp4a.40.2) audio track, but
    Chrome refuses to negotiate a Widevine key system at all for ec-3: asking
    for it fails the whole load with Shaka 6001
    (DRM.REQUESTED_KEY_SYSTEM_CONFIG_UNAVAILABLE), verified on Chrome/Windows.
    AAC is therefore the only playable track here, and dropping ec-3 outright
    is more reliable than the player-side `preferredAudioCodecs` hint.

    Also injects a <BaseURL>: NBC's manifest has none and uses relative
    SegmentTemplate paths, which the DASH spec resolves against the manifest's
    own URL — i.e. our /play/... origin — so without it every segment 404s.
    Same fix as pbs_dash_proxy.
    """
    from urllib.parse import unquote as _unquote, urljoin as _urljoin

    raw_id = _unquote(channel_id)
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == 'nbc_tve', Channel.source_channel_id == raw_id)
        .first()
    )
    if not channel:
        abort(404)

    scraper_cls = registry.get('nbc_tve')
    if not scraper_cls:
        return _unavailable_response()
    scraper = scraper_cls(config=channel.source.config or {})
    try:
        dash_url = scraper.resolve(channel.stream_url)
    except Exception as e:
        logger.warning('[nbc-tve-dash] resolve failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()
    finally:
        if getattr(scraper, '_pending_cache_updates', None):
            try:
                persist_source_cache_updates(channel.source_id, scraper._pending_cache_updates)
            except Exception:
                pass
        if scraper._pending_config_updates:
            try:
                persist_source_config_updates(channel.source_id, scraper._pending_config_updates)
            except Exception:
                pass

    if not dash_url or not dash_url.startswith('http'):
        logger.warning('[nbc-tve-dash] no DASH URL for %s', raw_id[:40])
        return _unavailable_response()

    try:
        r = _requests.get(dash_url, timeout=10, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36',
            'Referer': 'https://www.nbc.com/',
        })
        r.raise_for_status()
    except Exception as e:
        logger.warning('[nbc-tve-dash] manifest fetch failed for %s: %s', raw_id[:40], e)
        return _unavailable_response()

    # Same issue as Philo/PBS: NBC's manifest includes an absolute <Location>
    # pointing straight back at MediaTailor's own origin. Shaka honors that on
    # its next periodic manifest refresh and bypasses this proxy entirely —
    # undoing both the ec-3 audio drop and the injected <BaseURL> below, and
    # breaking playback shortly after it starts. Strip it so refreshes keep
    # hitting this same-origin route.
    manifest_text = re.sub(r'<Location>.*?</Location>\s*', '', r.text, flags=re.DOTALL)

    # NBC publishes two audio AdaptationSets: AAC (mp4a.40.2) and Dolby Digital+
    # (ec-3). Which one actually plays is client-dependent, so make it selectable
    # via ?audio= for diagnosis:
    #   aac (default) — drop ec-3. The only combination that plays under Widevine.
    #   ec3           — drop AAC. Fails with Shaka 6001 on Chrome; kept as a
    #                   diagnostic to re-confirm that, not as a usable mode.
    #   all           — leave both and let the client pick.
    audio_pref = (request.args.get('audio') or 'aac').strip().lower()
    drop_codec = {'aac': 'ec-3', 'ec3': 'mp4a.40.2'}.get(audio_pref)

    def _drop_audio(match):
        return '' if drop_codec and f'codecs="{drop_codec}"' in match.group(0) else match.group(0)

    manifest_text = re.sub(
        r'<AdaptationSet mimeType="audio/mp4".*?</AdaptationSet>\s*',
        _drop_audio, manifest_text, flags=re.DOTALL,
    )

    if '<BaseURL>' not in manifest_text:
        upstream_dir = _urljoin(str(r.url), '.')
        manifest_text = re.sub(
            r'(<MPD\b[^>]*>)',
            lambda m: f'{m.group(1)}<BaseURL>{upstream_dir}</BaseURL>',
            manifest_text, count=1,
        )

    if request.args.get('kodi_bridge'):
        manifest_text = _mpd_keep_highest_bitrate_video(manifest_text, raw_id)

    return Response(
        manifest_text,
        mimetype='application/dash+xml',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        },
    )


@play_bp.route('/play/<source_name>/license', methods=['POST'])
@play_bp.route('/play/<source_name>/license/<channel_id>', methods=['POST'])
def license_proxy(source_name: str, channel_id: str | None = None):
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
    # Merge source_cache (e.g. Amazon's channel_pe) over config so the scraper
    # classmethods that read playback envelopes see them — they moved out of config.
    cfg = {**(source.config or {}), **load_source_cache(source.id)}
    channel_id = channel_id or request.args.get('channel_id') or None
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

    directv_sid = None
    if source_name == 'directv':
        directv_sid, _ = _directv_device_session_id()
        rdb = _amazon_sht_redis()
        identity_key = _directv_identity_cache_key(source.id, directv_sid)
        if request.args.get('reset_identity') == '1' and rdb:
            try:
                rdb.delete(identity_key)
            except Exception:
                pass
        identity_cookie = None
        if rdb:
            try:
                identity_cookie = rdb.get(identity_key) or None
            except Exception:
                identity_cookie = None
        if identity_cookie:
            logger.debug('[directv-license] identity cache hit sid=%s', directv_sid[:8])
        else:
            logger.debug('[directv-license] identity cache miss sid=%s', directv_sid[:8])
        if not identity_cookie:
            identity_cookie, expires_at, activation_payload, activation_status, activation_error = _directv_activate_identity(
                scraper_cls, cfg, challenge,
            )
            if activation_error and _directv_error_requires_reauth(activation_status or 0, activation_error):
                _directv_trigger_reauth(source, f'activation HTTP {activation_status}')
            if identity_cookie and rdb:
                try:
                    rdb.setex(identity_key, _directv_identity_ttl(expires_at), identity_cookie)
                except Exception:
                    pass
            if activation_payload:
                if activation_payload[:2] == b'\x08\x05' and rdb:
                    try:
                        rdb.setex(f'amz_service_cert:{source_name}', 86400, activation_payload)
                    except Exception:
                        pass
                return _directv_response(activation_payload, activation_status or 200, directv_sid)
        if identity_cookie:
            cfg = {**cfg, 'identity_cookie': identity_cookie}

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
    post_license = getattr(scraper_cls, 'post_license_request', None)

    def _send_license():
        if callable(post_license):
            return post_license(license_url, body, headers, timeout=15)
        return _requests.post(license_url, data=body, headers=headers, timeout=15)

    try:
        r = _send_license()
    except Exception as e:
        logger.warning('[license-proxy] %s request failed: %s', source_name, e)
        abort(502)

    # Philo's DRMtoday token is tied to the channel's playback session. The
    # session cache can outlive the token, so refresh only this channel once
    # after a 403 rather than borrowing another channel's token.
    if r.status_code == 403 and source_name == 'philo' and channel_id:
        try:
            fresh_cfg = {**(source.config or {}), **load_source_cache(source.id)}
            fresh_scraper = scraper_cls(config=fresh_cfg)
            fresh_scraper.expire_cached_dash(channel_id)
            fresh_scraper.resolve(f'philo://{channel_id}')
            if getattr(fresh_scraper, '_pending_cache_updates', None):
                persist_source_cache_updates(source.id, fresh_scraper._pending_cache_updates)
            if getattr(fresh_scraper, '_pending_config_updates', None):
                persist_source_config_updates(source.id, fresh_scraper._pending_config_updates)
            cfg = {**(source.config or {}), **load_source_cache(source.id)}
            body, headers = scraper_cls.prepare_license_request(
                challenge, cfg, channel_id=channel_id, sht=sht)
            headers.setdefault('Content-Type', 'application/octet-stream')
            logger.info('[philo-license] refreshed playback session after HTTP 403 channel=%s', channel_id)
            r = _send_license()
        except Exception as e:
            logger.warning('[philo-license] channel refresh after HTTP 403 failed: %s', e)

    # Cox's MDS license server appears to grant only one license per
    # content_metadata/xsct session; Kodi's inputstream.adaptive routinely opens a
    # second CDM session (even though every track shares the same default_KID) and
    # that second request gets a bare 403. Mint a fresh session and retry once.
    if r.status_code == 403 and source_name == 'cox' and channel_id:
        try:
            fresh_cfg = {**(source.config or {}), **load_source_cache(source.id)}
            fresh_scraper = scraper_cls(config=fresh_cfg)
            fresh_scraper.expire_cached_dash(channel_id)
            channel = (
                Channel.query.join(Source)
                .filter(Source.name == 'cox', Channel.source_channel_id == channel_id)
                .first()
            )
            if channel and channel.stream_url:
                fresh_scraper.resolve(channel.stream_url)
            if getattr(fresh_scraper, '_pending_cache_updates', None):
                persist_source_cache_updates(source.id, fresh_scraper._pending_cache_updates)
            if getattr(fresh_scraper, '_pending_config_updates', None):
                persist_source_config_updates(source.id, fresh_scraper._pending_config_updates)
            cfg = {**(source.config or {}), **load_source_cache(source.id)}
            body, headers = scraper_cls.prepare_license_request(
                challenge, cfg, channel_id=channel_id, sht=sht)
            headers.setdefault('Content-Type', 'application/octet-stream')
            logger.info('[cox-license] refreshed playback session after HTTP 403 channel=%s', channel_id)
            r = _send_license()
        except Exception as e:
            logger.warning('[cox-license] channel refresh after HTTP 403 failed: %s', e)
    logger.debug('[license-proxy] %s channel=%s -> HTTP %s (%d bytes)',
                 source_name, channel_id or '-', r.status_code, len(r.content))
    if r.status_code >= 400:
        _record_proxy_failure(
            label=f'{source_name}-license',
            request_id=getattr(g, 'request_id', '-'),
            upstream_status=r.status_code,
            bytes_sent=len(r.content or b''),
            error=f'license HTTP {r.status_code}',
        )
        logger.warning(
            '[license-proxy] %s channel=%s upstream returned HTTP %s (%d bytes): %s',
            source_name, channel_id or '-', r.status_code, len(r.content or b''),
            (r.content or b'')[:300],
        )
    response_bytes = scraper_cls.process_license_response(r.content)
    # Some license servers (e.g. PBS's proxy.drm.pbs.org) report failure with an HTTP
    # 2xx and a JSON error envelope in the body instead of a real 4xx/5xx. Handing that
    # straight to the CDM produces a confusing DRM.LICENSE_RESPONSE_REJECTED instead of
    # an obvious network error, so report it as one here.
    _effective_status = r.status_code
    _is_error_body = getattr(scraper_cls, 'is_license_error_response', None)
    if callable(_is_error_body) and r.status_code < 400 and _is_error_body(response_bytes):
        _effective_status = 502
        logger.warning(
            '[license-proxy] %s channel=%s upstream returned HTTP %s but the body looks '
            'like an error envelope — reporting 502: %s',
            source_name, channel_id or '-', r.status_code, (response_bytes or b'')[:300],
        )
        _record_proxy_failure(
            label=f'{source_name}-license',
            request_id=getattr(g, 'request_id', '-'),
            upstream_status=r.status_code,
            bytes_sent=len(response_bytes or b''),
            error='license response looks like an error envelope despite HTTP 2xx',
        )
    # If Amazon returned a SERVICE_CERTIFICATE (Widevine type 5), cache it for
    # the /certificate endpoint so Shaka can pre-fetch it via serverCertificateUri.
    if response_bytes and response_bytes[:2] == b'\x08\x05':
        _rdb = _amazon_sht_redis()
        if _rdb:
            try:
                _rdb.setex(f'amz_service_cert:{source_name}', 86400, response_bytes)
            except Exception:
                pass
    if source_name == 'directv' and r.status_code == 403:
        _rdb = _amazon_sht_redis()
        if _rdb and directv_sid:
            try:
                _rdb.delete(_directv_identity_cache_key(source.id, directv_sid))
            except Exception:
                pass
        if _directv_error_requires_reauth(r.status_code, r.content):
            _directv_trigger_reauth(source, f'license HTTP {r.status_code}')
        logger.warning('[directv-license] license HTTP 403 channel=%s body=%s',
                       channel_id or '-', r.content[:500])
    if source_name == 'directv' and directv_sid:
        return _directv_response(response_bytes, _effective_status, directv_sid)
    return Response(response_bytes, status=_effective_status, content_type='application/octet-stream')


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
    # Not cached — fetch live with a minimal SERVICE_CERTIFICATE_REQUEST challenge.
    # Merge source_cache (Amazon channel_pe) over config — see license_proxy.
    cfg = {**(source.config or {}), **load_source_cache(source.id)}
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


@play_bp.route('/play/directv/<channel_id>/browser.m3u8')
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


@play_bp.route('/play/directv/browser-asset')
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


@play_bp.route('/play/directv/<channel_id>/prismcast.m3u')
@play_bp.route('/play/directv/<channel_id>/prismcast.m3u8')
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


@play_bp.route('/play/directv/prismcast-asset')
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
        '[play] request_id=%s ip=%s source=%s channel_id=%s channel_name=%s',
        getattr(g, 'request_id', '-'), client_ip, source_name, channel_id, channel.name,
    )

    if source_name == 'samsung':
        from urllib.parse import quote as _quote
        encoded_id = _quote(channel.source_channel_id, safe='')
        return redirect(
            f"{request.host_url.rstrip('/')}/play/samsung/{encoded_id}/proxy.m3u8?via=play",
            302,
        )

    # FOX TVE is not architecturally uniform: fox_weather is served as a static
    # PLAYLIST-TYPE:VOD Uplynk asset with a frozen MEDIA-SEQUENCE that our proxy
    # rewrites into a live-looking one (removing the proxy would break it), and
    # fox_news/fox_business need 247.fox*.com segment auth-query rewriting. Both
    # genuinely need the manifest proxy. The fox_sports_* channels (FS1/FS2/FOX/
    # BTN/Deportes/Soccer Plus) are the opposite: resolve_fox_sports_hls() mints
    # a fresh, uncached DVP session on every single client poll (~0.87s Fox API
    # round trip each time) even though they're genuinely live-tagged already and
    # need no rewriting — confirmed via live testing 2026-08-06 that the derived
    # media playlist URL stays valid and correctly advancing for 2+ minutes past
    # the master ticket's own ~90s expiry, same shape as the FOX One stutter fix.
    if source_name == 'fox_tve' and channel.source_channel_id in _FOX_TVE_PROXY_REQUIRED_CHANNELS:
        from urllib.parse import quote as _quote
        encoded_id = _quote(channel.source_channel_id, safe='')
        return redirect(
            f"{request.host_url.rstrip('/')}/play/{source_name}/{encoded_id}/proxy.m3u8",
            302,
        )

    scraper_cls = registry.get(source_name)
    scraper = None
    # Distinguish a confirmed-dead channel (permanent — being auto-disabled) from
    # a transient resolve failure (timeout, CDN hiccup, expired token). They get
    # different HTTP responses below so clients back off correctly.
    resolve_dead = False
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
            resolve_dead = True
        except TVENotAuthorizedError as e:
            # A definitive "no" from Adobe Pass (the TV-provider account
            # simply isn't entitled to this network) — same certainty class
            # as StreamDeadError, not a transient resolve failure, so it
            # gets the same immediate-disable treatment instead of silently
            # retrying this exact doomed resolve() on every future play
            # request. Audit already special-cases this identically (see
            # run_stream_audit's own TVENotAuthorizedError branch); this was
            # the one place it fell through to the generic branch below
            # instead (confirmed live 2026-08-17: Discovery TVE's TLC).
            logger.error(
                '[play] channel not authorized ip=%s source=%s channel_id=%s channel_name=%s: %s',
                client_ip, source_name, channel_id, channel.name, e,
            )
            trigger_channel_auto_disable(channel.id, 'NotAuthorized')
            resolved_url = None
            resolve_dead = True
        except Exception as e:
            logger.error(
                '[play] resolve failed request_id=%s ip=%s source=%s channel_id=%s channel_name=%s: %s',
                getattr(g, 'request_id', '-'), client_ip, source_name, channel_id, channel.name, e,
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
            if getattr(scraper, '_pending_cache_updates', None):
                try:
                    persist_source_cache_updates(
                        channel.source_id,
                        scraper._pending_cache_updates,
                    )
                except Exception as ce:
                    db.session.rollback()
                    logger.warning('[play] failed to persist cache updates: %s', ce)
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
        return _gone_response() if resolve_dead else _unavailable_response()

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

    # C-SPAN floor feeds: manifests are open but segments are Referer-gated, so
    # every stream goes through the manifest proxy (which rewrites segments through
    # the Referer-adding segment proxy).
    if source_name == 'cspan':
        from urllib.parse import quote as _quote
        encoded_id = _quote(channel.source_channel_id, safe='')
        return redirect(
            f"{request.host_url.rstrip('/')}/play/cspan/{encoded_id}/proxy.m3u8",
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

    # Distro channels with browser-sensitive manifests: serve a manifest proxy
    # so Shaka sees absolute segment URLs and so header-gated CDNs are fetched
    # server-side. The proxy still leaves public segment URLs direct.
    if source_name == 'distro' and resolved_url:
        from urllib.parse import urlsplit as _urlsplit
        if _urlsplit(resolved_url).netloc in _DISTRO_MANIFEST_PROXY_HOSTS:
            from urllib.parse import quote as _quote
            encoded_id = _quote(channel.source_channel_id, safe='')
            return redirect(
                f"{request.host_url.rstrip('/')}/play/distro/{encoded_id}/proxy.m3u8",
                302,
            )

    # TCL channels routed through FutureToday/Publica's SSAI pipeline: the
    # manifest CDN (getpublica.com) sets CORS correctly, but the content
    # segments it stitches in live on a separate origin (e.g. cachefly.net)
    # that sends none at all, blocking Shaka's browser fetch(). Gated on the
    # resolved host (not the stored source= tag) so any future TCL channel
    # that resolves through this same CDN picks up the proxy automatically.
    if source_name == 'tcl' and resolved_url:
        from urllib.parse import urlsplit as _urlsplit
        if _urlsplit(resolved_url).netloc.lower().endswith('.getpublica.com'):
            from urllib.parse import quote as _quote
            encoded_id = _quote(channel.source_channel_id, safe='')
            return redirect(
                f"{request.host_url.rstrip('/')}/play/tcl/{encoded_id}/proxy.m3u8",
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
            reason, stream_info = _check_manifest(resolved_url, s)
            # Refresh the resolution/codec badge off the same manifest fetch, for the
            # redirect-to-CDN sources that reach this generic path (xumo/roku/plex/
            # localnow). Proxied sources (stirr/distro) refresh in their own proxy
            # endpoints instead. Only write when the displayed summary changes, so the
            # per-tune probe doesn't churn the DB on volatile session metadata.
            if stream_info:
                with _app.app_context():
                    try:
                        from ..extensions import db
                        _ch = db.session.get(Channel, _channel_id)
                        if _ch is not None and (
                            _stream_info_summary(_ch.stream_info) != _stream_info_summary(stream_info)
                        ):
                            _ch.stream_info = stream_info
                            db.session.commit()
                            logger.debug('[play] refreshed stream_info for channel %s: %s',
                                         _channel_id, stream_info.get('max_resolution') or '?')
                    except Exception as _si_exc:
                        from ..extensions import db
                        db.session.rollback()
                        logger.debug('[play] stream_info refresh failed for channel %s: %s',
                                     _channel_id, _si_exc)
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
                        # osm_session + stream_url_cache now live in source_cache;
                        # None overwrites the row value, which the loaders treat as empty.
                        persist_source_cache_updates(_source_id, {
                            'osm_session': None,
                            'stream_url_cache': None,
                        })
                    except Exception as e:
                        logger.warning('[play] failed to clear osm_session: %s', e)
                return
            with _app.app_context():
                trigger_channel_auto_disable(_channel_id, reason)

        threading.Thread(target=_bg_check, daemon=True).start()

    logger.debug(
        '[play] redirect request_id=%s ip=%s source=%s channel_id=%s channel_name=%s → %s',
        getattr(g, 'request_id', '-'), client_ip, source_name, channel_id, channel.name, resolved_url[:80],
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


from ..kodi_bridge import KODI_BRIDGE_TRUSTED_SOURCES as _KODI_BRIDGE_TRUSTED_SOURCES


@play_bp.route('/play/kodi-bridge/<source_name>/<channel_id>.m3u8')
def play_kodi_bridge(source_name: str, channel_id: str):
    """
    Single play URL for the Kodi/HDMI-encoder bridge feed (dev/kodi/IMPLEMENTATION_PLAN.md
    section 2). Every channel in the feed hits this route, DRM or not — the branch is
    invisible to the caller:
      - normal channel -> identical to /play/<source>/<id>.m3u8 (resolve + 302 to CDN)
      - bridge channel -> trigger_channel() on Kodi, then redirect to the encoder's fixed
        stream URL immediately. Does NOT wait for confirm_playback() — that's the
        watchdog's job, never inline with this request (speed-first design, agreed
        2026-08-16: the JSON-RPC call itself returns in well under 100ms).
    """
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

    needs_bridge = bool(channel.requires_drm_bridge) and source_name in _KODI_BRIDGE_TRUSTED_SOURCES
    if not needs_bridge:
        return play(source_name, channel_id)

    from .. import kodi_bridge
    from ..models import AppSettings
    from .api import _get_playback_info

    settings = AppSettings.get()
    encoder_url = settings.effective_kodi_bridge_encoder_url()
    if not encoder_url:
        logger.error(
            '[kodi-bridge] play request for %s/%s but kodi_bridge_encoder_url is not configured',
            source_name, channel_id,
        )
        return Response('Kodi HDMI bridge encoder URL is not configured.\n', status=503, mimetype='text/plain')

    if not kodi_bridge.is_configured():
        logger.error(
            '[kodi-bridge] play request for %s/%s but the bridge is not enabled/configured',
            source_name, channel_id,
        )
        return Response('Kodi HDMI bridge is not enabled or configured.\n', status=503, mimetype='text/plain')

    info = _get_playback_info(channel, fast_mode=False)
    manifest_url = info.get('preview_url') or info.get('play_url') or ''
    if manifest_url.startswith('/'):
        manifest_url = urljoin(request.host_url, manifest_url.lstrip('/'))
    if manifest_url.endswith('/dash.mpd') or '/dash.mpd?' in manifest_url:
        sep = '&' if '?' in manifest_url else '?'
        manifest_url = f'{manifest_url}{sep}kodi_bridge=1'
    license_url = info.get('license_url') or None

    if not manifest_url:
        logger.error('[kodi-bridge] no manifest URL resolved for %s/%s', source_name, channel_id)
        return _unavailable_response()

    triggered = kodi_bridge.trigger_channel(
        manifest_url, license_url, name=channel.name or 'FastChannels',
        channel_key=f'{source_name}:{channel_id}',
    )
    logger.info(
        '[kodi-bridge] request_id=%s ip=%s source=%s channel_id=%s channel_name=%s triggered=%s -> encoder',
        getattr(g, 'request_id', '-'), _client_ip(), source_name, channel_id, channel.name, triggered,
    )
    return redirect(encoder_url, 302)


@play_bp.route('/play/kodi-bridge/<source_name>/<channel_id>.m3u')
def play_kodi_bridge_vlc(source_name: str, channel_id: str):
    """.m3u sibling of play_kodi_bridge, same purpose as play_vlc: hand VLC (or any
    media player) a downloadable playlist instead of a bare .m3u8 URL the browser
    would otherwise try to render inline."""
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
    stream_url = f'{base_url}/play/kodi-bridge/{source_name}/{channel_id}.m3u8'
    playlist = f'#EXTM3U\n#EXTINF:-1,{channel.name}\n{stream_url}\n'
    return Response(
        playlist,
        mimetype='audio/x-mpegurl',
        headers={'Content-Disposition': f'attachment; filename="{channel_id}.m3u"'},
    )


_PRISMCAST_HLS_SESSION_RE = re.compile(r'^(.*)/hls/([^/]+)/stream\.m3u8(?:\?.*)?$')


def _prismcast_ts_url(hls_url: str) -> str | None:
    """PrismCast serves each capture two ways — HLS at /hls/<id>/stream.m3u8 and
    raw MPEG-TS at /stream/<id>. Same session, sibling path."""
    m = _PRISMCAST_HLS_SESSION_RE.match(hls_url)
    if not m:
        return None
    return f'{m.group(1)}/stream/{m.group(2)}'


@play_bp.route('/play/prismcast/<int:channel_id>.ts')
def prismcast_bridge_ts(channel_id):
    """MPEG-TS variant of the generic PrismCast DRM bridge (see generate_prismcast_m3u).

    The M3U's normal bridge entry hands clients PrismCast's ad-hoc `/play?url=`
    trigger directly, which resolves to an HLS capture. Some downstream players
    (e.g. TS-only fallback chains) need a plain MPEG-TS stream instead, and
    PrismCast exposes the same capture at a sibling /stream/<id> path — so this
    route triggers the capture server-side, follows it to the resolved HLS
    session URL, and 302s the client to the MPEG-TS sibling instead.
    """
    from ..models import AppSettings
    from ..generators.m3u import _prismcast_bridge_url
    channel = Channel.query.get_or_404(channel_id)
    settings = AppSettings.get()
    prismcast_url = (settings.effective_prismcast_url() or '').strip().rstrip('/')
    if not prismcast_url:
        return Response('PrismCast is not configured.\n', status=503, mimetype='text/plain')
    inner_base_url = (settings.effective_prismcast_inner_url() or public_base_url()).strip().rstrip('/')
    play_url = _prismcast_bridge_url(channel, prismcast_url, inner_base_url)

    r = _fetch_prismcast_with_retry(play_url, channel_id, 'prismcast-ts', stream=True)
    if r.status_code >= 400:
        abort(r.status_code)
    ts_url = _prismcast_ts_url(r.url)
    if not ts_url:
        logger.warning('[prismcast-ts] unexpected PrismCast URL shape for channel=%s: %s', channel_id, r.url)
        abort(502)
    return redirect(ts_url, 302)


@play_bp.route('/watch/<int:channel_id>')
def watch(channel_id):
    capture_request_id = (request.args.get('fc_request_id') or '').strip()
    if capture_request_id:
        g.request_id = capture_request_id[:80]
    from .api import _get_playback_info
    from flask import make_response
    from ..models import AppSettings
    # Version param busts stale browser caches from before this route existed.
    if '_v' not in request.args:
        qs = request.args.to_dict(flat=True)
        qs['_v'] = '3'
        from urllib.parse import urlencode as _urlencode
        return redirect(f'{request.path}?{_urlencode(qs)}', 302)
    channel = Channel.query.get_or_404(channel_id)
    info = _get_playback_info(channel, fast_mode=False)
    settings = AppSettings.get()
    max_height = int(settings.prismcast_max_height or 0)
    play_url = info.get('preview_url') or info.get('play_url') or ''
    if play_url and play_url.startswith('/'):
        play_url = urljoin(request.host_url, play_url.lstrip('/'))
    resp = make_response(render_template(
        'watch.html',
        channel=channel,
        play_url=play_url,
        playback_mode=info.get('playback_mode', 'hls'),
        stream_type=info.get('stream_type', 'hls'),
        license_url=info.get('license_url') or '',
        watch_debug=request.args.get('debug') == '1',
        request_id=getattr(g, 'request_id', ''),
        max_height=max_height,
        live_edge_sync=bool(channel.source and channel.source.name == 'philo'),
    ))
    resp.headers['Cache-Control'] = 'no-store'
    return resp


@play_bp.route('/watch/<int:channel_id>/debug', methods=['POST'])
def watch_debug(channel_id):
    channel = Channel.query.get_or_404(channel_id)
    payload = request.get_json(silent=True) or {}
    debug_request_id = str(payload.get('request_id') or getattr(g, 'request_id', '-'))[:80]
    event = str(payload.get('event') or 'event')[:48]
    extra = payload.get('extra') if isinstance(payload.get('extra'), dict) else {}
    state = {
        'seq': payload.get('seq'),
        'age_ms': payload.get('age_ms'),
        'current_time': payload.get('current_time'),
        'ready_state': payload.get('ready_state'),
        'network_state': payload.get('network_state'),
        'paused': payload.get('paused'),
        'ended': payload.get('ended'),
        'seeking': payload.get('seeking'),
        'buffered_end': payload.get('buffered_end'),
        'buffered_ahead': payload.get('buffered_ahead'),
        'video_size': payload.get('video_size'),
        'dropped': payload.get('dropped'),
        'decoded': payload.get('decoded'),
    }
    compact_state = {k: v for k, v in state.items() if v is not None}
    compact_extra = {str(k)[:40]: str(v)[:800] for k, v in extra.items()}
    _record_watch_debug_snapshot(debug_request_id, channel, event, compact_state, extra)
    logger.info(
        '[watch-debug] request_id=%s event=%s ip=%s channel_id=%s source=%s source_channel_id=%s channel_name=%s state=%s extra=%s',
        debug_request_id,
        event,
        request.headers.get('X-Forwarded-For') or request.remote_addr,
        channel.id,
        channel.source.name if channel.source else None,
        channel.source_channel_id,
        channel.name,
        compact_state,
        compact_extra,
    )
    return Response(status=204)

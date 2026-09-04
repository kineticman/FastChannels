"""DistroTV-specific playback proxies.

This module deliberately owns all of Distro's non-standard HLS handling so
the generic play route remains independent of a dormant upstream integration.
"""
import hashlib
import logging
import re
import time
from urllib.parse import quote, unquote, urljoin, urlsplit

import requests
from flask import Blueprint, Response, abort, current_app, request

from ..scrapers.distro import (
    HLS_HEADERS,
    MANIFEST_PROXY_HOSTS,
    SESSION_CDN_HOSTS,
    DistroScraper,
    _pick_best_variant,
    _resolve_from_feed,
    _sanitize_url,
    _split_qualified_channel_id,
)

logger = logging.getLogger(__name__)

distro_proxy_bp = Blueprint('distro_proxy', __name__)
_stream_upstream_response = None

# Public to let the generic /play redirect decide whether this proxy is needed.
manifest_proxy_hosts = MANIFEST_PROXY_HOSTS


def configure_distro_proxy(*, stream_upstream_response) -> None:
    """Inject the shared streaming response and failure telemetry handler."""
    global _stream_upstream_response
    _stream_upstream_response = stream_upstream_response

_SESSION = requests.Session()
_SESSION.headers.update(HLS_HEADERS)
_SEGMENT_SEQ_RE = re.compile(r'-(\d+)\.ts(?:[?#]|%3[fF]|%23|$)')
_PRIVATE_IP_RE = re.compile(
    r'^(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|::1|0\.0\.0\.0)',
    re.IGNORECASE,
)
_REDIS_KEY_PREFIX = 'distro_variant:'
_redis = None
_proxy_log_last: dict[str, float] = {}


def _unavailable_response() -> Response:
    return Response(
        'Stream temporarily unavailable.\n', status=503, mimetype='text/plain',
        headers={'Retry-After': '30'},
    )


def _throttled_debug(channel_id: str, msg: str, *args) -> None:
    now = time.monotonic()
    if now - _proxy_log_last.get(channel_id, 0.0) >= 30.0:
        _proxy_log_last[channel_id] = now
        logger.debug(msg, *args)


def _redis_client():
    global _redis
    if _redis is None:
        try:
            import redis
            _redis = redis.from_url(
                current_app.config['REDIS_URL'], decode_responses=True,
                socket_timeout=1, socket_connect_timeout=1,
            )
        except Exception:
            pass
    return _redis


def _variant_key(upstream_url: str) -> str:
    return _REDIS_KEY_PREFIX + hashlib.md5(upstream_url.encode()).hexdigest()


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


def _filter_ts_video_only(data: bytes) -> bytes:
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


def _rewrite_media_playlist(text: str, playlist_url: str, base_url: str) -> tuple[str, dict]:
    playlist_host = urlsplit(playlist_url).netloc
    lag_segments = 2 if playlist_host == 'global.cgtn.cicc.media.caton.cloud' else 0
    variant_base = playlist_url.rsplit('/', 1)[0] + '/'
    header_lines, pending_tags, segment_blocks = [], [], []
    first_seq = last_seq = old_seq = None
    proxied_segments = 0
    seen_segment = False

    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith('#EXT-X-MEDIA-SEQUENCE:'):
            try:
                old_seq = int(stripped.split(':', 1)[1])
            except ValueError:
                old_seq = None
            (pending_tags if seen_segment else header_lines).append(raw_line)
        elif stripped and not stripped.startswith('#'):
            absolute_url = urljoin(variant_base, stripped)
            match = _SEGMENT_SEQ_RE.search(absolute_url)
            if match:
                first_seq = first_seq if first_seq is not None else int(match.group(1))
                last_seq = int(match.group(1))
            host = urlsplit(absolute_url).netloc
            if host == 'global.cgtn.cicc.media.caton.cloud':
                absolute_url = f'{base_url}/play/distro/segment?url={quote(absolute_url, safe="")}&video_only=1'
                proxied_segments += 1
            elif host in SESSION_CDN_HOSTS:
                absolute_url = f'{base_url}/play/distro/segment?url={quote(absolute_url, safe="")}'
                proxied_segments += 1
            segment_blocks.append(pending_tags + [absolute_url])
            pending_tags, seen_segment = [], True
        elif seen_segment or pending_tags or stripped.startswith(('#EXTINF', '#EXT-X-BYTERANGE', '#EXT-X-DISCONTINUITY')):
            pending_tags.append(raw_line)
        else:
            header_lines.append(raw_line)

    original_segment_count = len(segment_blocks)
    if lag_segments and len(segment_blocks) > lag_segments:
        segment_blocks = segment_blocks[:-lag_segments]
    trimmed_segments = original_segment_count - len(segment_blocks)
    if lag_segments and segment_blocks:
        kept_urls = [line for block in segment_blocks for line in block if line.startswith(('http://', 'https://'))]
        if kept_urls:
            first = _SEGMENT_SEQ_RE.search(kept_urls[0])
            last = _SEGMENT_SEQ_RE.search(kept_urls[-1])
            first_seq = int(first.group(1)) if first else first_seq
            last_seq = int(last.group(1)) if last else last_seq

    fixed_seq = None
    if old_seq is not None and first_seq is not None and old_seq != first_seq:
        fixed_seq = first_seq
        for index, line in enumerate(header_lines):
            if line.strip().startswith('#EXT-X-MEDIA-SEQUENCE:'):
                header_lines[index] = f'#EXT-X-MEDIA-SEQUENCE:{first_seq}'
                break
        else:
            header_lines.insert(1 if header_lines and header_lines[0].strip() == '#EXTM3U' else 0, f'#EXT-X-MEDIA-SEQUENCE:{first_seq}')

    lines = header_lines[:]
    for block in segment_blocks:
        lines.extend(block)
    lines.extend(pending_tags)
    return '\n'.join(lines), {
        'old_seq': old_seq, 'fixed_seq': fixed_seq, 'first_seq': first_seq,
        'last_seq': last_seq, 'segment_count': len(segment_blocks),
        'trimmed_segments': trimmed_segments, 'lag_segments': lag_segments,
        'proxied_segments': proxied_segments,
    }


def _fetch_variant(upstream_url: str, channel_id: str):
    rdb, key = _redis_client(), _variant_key(upstream_url)
    variant_url = rdb.get(key) if rdb else None
    if variant_url:
        try:
            response = _SESSION.get(variant_url, timeout=8)
            if response.status_code == 200:
                return variant_url, response
        except Exception:
            pass
        if rdb:
            try:
                rdb.delete(key)
            except Exception:
                pass
    try:
        master = _SESSION.get(upstream_url, timeout=8)
        master.raise_for_status()
    except Exception as exc:
        logger.warning('[distro-proxy] master fetch failed for %s: %s', channel_id, exc)
        return None
    if '#EXTINF' in master.text and '#EXT-X-STREAM-INF' not in master.text:
        return master.url, master
    variant_url = _pick_best_variant(master.text, master.url)
    if not variant_url:
        logger.warning('[distro-proxy] no variants in master for %s', channel_id)
        return None
    try:
        response = _SESSION.get(variant_url, timeout=8)
        response.raise_for_status()
    except Exception as exc:
        logger.warning('[distro-proxy] variant fetch failed for %s: %s', channel_id, exc)
        return None
    if rdb:
        try:
            rdb.set(key, variant_url, ex=7200)
        except Exception:
            pass
    return variant_url, response


@distro_proxy_bp.route('/play/distro/segment')
def segment_proxy():
    raw_url = request.args.get('url', '')
    if not raw_url:
        abort(400)
    url = unquote(raw_url)
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or not parsed.netloc:
        abort(400)
    if _PRIVATE_IP_RE.match(parsed.netloc.split(':')[0]):
        logger.warning('[distro-seg-proxy] blocked SSRF attempt to: %s', parsed.netloc)
        abort(403)
    try:
        video_only = request.args.get('video_only') == '1' and parsed.netloc == 'global.cgtn.cicc.media.caton.cloud'
        response = _SESSION.get(url, timeout=(5, 30), stream=not video_only)
        if response.status_code != 200:
            abort(response.status_code)
        if video_only:
            return Response(
                _filter_ts_video_only(response.content),
                content_type=response.headers.get('Content-Type', 'video/MP2T'),
                headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
            )
        return _stream_upstream_response(
            response,
            status=200,
            content_type=response.headers.get('Content-Type', 'video/MP2T'),
            headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
            label='distro-seg-proxy',
        )
    except Exception as exc:
        logger.warning('[distro-seg-proxy] fetch failed for %s: %s', url[:80], exc)
        abort(502)


@distro_proxy_bp.route('/play/distro/<channel_id>/proxy.m3u8')
def manifest_proxy(channel_id: str):
    geo, raw_id = _split_qualified_channel_id(unquote(channel_id))
    upstream_url = _resolve_from_feed(DistroScraper(), geo, raw_id)
    if not upstream_url:
        return _unavailable_response()
    result = _fetch_variant(_sanitize_url(upstream_url), channel_id)
    if result is None:
        refreshed_url = _resolve_from_feed(DistroScraper(), geo, raw_id, force_refresh=True)
        result = _fetch_variant(_sanitize_url(refreshed_url), channel_id) if refreshed_url else None
    if result is None:
        return _unavailable_response()
    variant_url, response = result
    body, info = _rewrite_media_playlist(response.text, variant_url, request.host_url.rstrip('/'))
    _throttled_debug(channel_id, '[distro-proxy] served channel=%s seq=%s first=%s last=%s segments=%s trimmed=%s lag=%s proxied=%s', channel_id, info['old_seq'], info['first_seq'], info['last_seq'], info['segment_count'], info['trimmed_segments'], info['lag_segments'], info['proxied_segments'])
    return Response(body, mimetype='application/vnd.apple.mpegurl', headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'})

"""Built-in capture add-on (beta) — FastChannels side. See app/capture/__init__.py.

Public (DVR clients can't sign in):
    /m3u/capture                         bridge-only playlist: DRM channels via capture
    /play/capture/<id>.m3u8              start/keep alive a capture, return its HLS playlist
    /play/capture/s/<session>/<segment>  HLS segments, read straight from disk

Admin:
    GET  /api/capture/status
    POST /api/capture/install | uninstall | update-chrome
    POST /api/capture/config             {enabled, max_tuners, quality, encoder}

The playlist route is the session's keep-alive: the daemon stops a capture once
nobody has fetched its playlist for ~45 s. Segments are served from SESSION_ROOT
by this process, so a long recording is a stream of short requests and survives
gunicorn worker recycling.
"""
from __future__ import annotations

import logging
import os
import re
import time

from flask import Blueprint, Response, abort, jsonify, request, send_file

from .. import capture
from ..models import AppSettings, Channel
from ..url import public_base_url

logger = logging.getLogger(__name__)

capture_bp = Blueprint('capture', __name__)

_SESSION_RE = re.compile(r'^[0-9a-f]{16}$')
_SEGMENT_RE = re.compile(r'^seg_\d{6}\.ts$')
_TUNE_WAIT_SECONDS = 50
# Players start ~3 target durations back from the live edge (HLS spec 6.3.3). With
# fewer segments than that they start AT the edge and rebuffer before every new
# segment — VLC logged a stall per 2 s segment for the first ~45 s with 2 here.
_MIN_SEGMENTS = 3
_HLS_HEADERS = {'Cache-Control': 'no-cache, no-store'}


_last_warn: dict[int, float] = {}


def _warn_limited(channel_id: int, fmt: str, *args) -> None:
    """Players retry a refused playlist many times a second (VLC every ~20 ms):
    log the reason once per channel per 30 s, not per retry."""
    now = time.monotonic()
    if now - _last_warn.get(channel_id, 0) >= 30:
        _last_warn[channel_id] = now
        logger.warning(fmt, *args)


def _text(msg: str, status: int, retry_after: int | None = None) -> Response:
    headers = {'Retry-After': str(retry_after)} if retry_after else {}
    return Response(msg.rstrip() + '\n', status=status, mimetype='text/plain', headers=headers)


# ── playlist output ───────────────────────────────────────────────────────────

@capture_bp.route('/m3u/capture')
def m3u_capture():
    """DRM channels routed through the built-in capture add-on. Import alongside the
    regular M3U (it carries only the channels that one leaves out). Gracenote IDs are
    emitted as tvc-guide-stationid where known, tvg-id (pair with /epg.xml) otherwise."""
    from ..generators.m3u import generate_prismcast_m3u
    from .output import _filters
    if not capture.is_active():
        return _text('Built-in capture is not installed or is turned off (Bridge page).', 503)
    if not AppSettings.get().bridge_enabled:
        return _text('Bridge mode is off (Bridge page), so no DRM channels are marked for capture.', 503)
    base_url = public_base_url()
    content = generate_prismcast_m3u(
        _filters(), base_url=base_url, prismcast_url='', gracenote=None,
        bridge_url_for=lambda ch: f'{base_url}/play/capture/{ch.id}.m3u8',
        exclude_sources=capture.INCOMPATIBLE_SOURCES,
    )
    return Response(content, mimetype='application/x-mpegurl',
                    headers={'Content-Disposition': 'attachment; filename="fastchannels-capture.m3u"'})


# ── live HLS ──────────────────────────────────────────────────────────────────

def _segment_count(playlist: str) -> int:
    return sum(1 for line in playlist.splitlines() if line and not line.startswith('#'))


def _rewrite(playlist: str, session_id: str) -> str:
    out = []
    for line in playlist.splitlines():
        if line and not line.startswith('#'):
            line = f'/play/capture/s/{session_id}/{os.path.basename(line)}'
        out.append(line)
    return '\n'.join(out) + '\n'


@capture_bp.route('/play/capture/<int:channel_id>.m3u8')
def capture_playlist(channel_id: int):
    if not capture.is_active():
        return _text('Built-in capture is not installed or is turned off.', 503)
    channel = Channel.query.get_or_404(channel_id)
    if channel.source and channel.source.name in capture.INCOMPATIBLE_SOURCES:
        return _text(f'{channel.source.display_name} refuses Linux Chrome; it cannot be captured.', 422)

    deadline = time.monotonic() + _TUNE_WAIT_SECONDS
    started = time.monotonic()
    logged = False
    while True:
        code, body = capture.daemon_request('POST', '/sessions', json={'channel_id': channel_id}, timeout=5)
        if code != 200:
            msg = body.get('error') or f'capture service returned HTTP {code}'
            # Busy/starting are usually seconds from clearing: a channel change leaves
            # the old session unpolled, and the daemon hands its tuner over once it's
            # PREEMPT_IDLE_SECONDS stale. Hold the request rather than refuse, since
            # players (VLC) give up on a 503 instead of retrying sensibly.
            if body.get('retry_after') and time.monotonic() + 1 < deadline:
                time.sleep(1)
                continue
            _warn_limited(channel_id, '[capture] channel %s (%s): %s', channel_id, channel.name, msg)
            return _text(msg, 503, retry_after=body.get('retry_after') or 10)
        if not logged and body.get('state') == 'tuning':
            # Only new tunes: the playlist is re-polled every ~2s while someone watches.
            logger.info('[capture] channel %s (%s) tuning, session %s', channel_id, channel.name,
                        body.get('session'))
            logged = True
        if body.get('state') == 'failed':
            return _text(f"Capture failed: {body.get('error') or 'unknown error'}", 502)
        session_id = body.get('session') or ''
        if body.get('state') == 'live' and _SESSION_RE.match(session_id):
            path = os.path.join(capture.SESSION_ROOT, session_id, 'index.m3u8')
            try:
                with open(path, encoding='utf-8') as fp:
                    playlist = fp.read()
            except OSError:
                playlist = ''
            if _segment_count(playlist) >= _MIN_SEGMENTS:
                if time.monotonic() - started > 1:
                    logger.info('[capture] channel %s ready after %.1fs (source video %s)',
                                channel_id, time.monotonic() - started, body.get('video_size'))
                return Response(_rewrite(playlist, session_id),
                                mimetype='application/vnd.apple.mpegurl', headers=_HLS_HEADERS)
        if time.monotonic() > deadline:
            return _text(f'Capture did not produce video within {_TUNE_WAIT_SECONDS}s.', 504, retry_after=5)
        time.sleep(1)


@capture_bp.route('/play/capture/s/<session_id>/<segment>')
def capture_segment(session_id: str, segment: str):
    if not _SESSION_RE.match(session_id) or not _SEGMENT_RE.match(segment):
        abort(404)
    path = os.path.join(capture.SESSION_ROOT, session_id, segment)
    if not os.path.isfile(path):
        abort(404)
    resp = send_file(path, mimetype='video/mp2t', conditional=False, etag=False)
    resp.headers['Cache-Control'] = 'no-cache'
    return resp


# ── admin API ─────────────────────────────────────────────────────────────────

@capture_bp.get('/api/capture/status')
def capture_status():
    code, health = capture.daemon_request('GET', '/health', timeout=3)
    if code != 200:
        return jsonify({
            'platform_supported': capture.supported_platform(),
            'install': capture.read_status(),
            'config': capture.read_config(),
            'service_error': health.get('error') or f'capture service returned HTTP {code}',
            'tuners': [], 'sessions': [],
        })
    health['playlist_url'] = f'{public_base_url()}/m3u/capture'
    return jsonify(health)


@capture_bp.post('/api/capture/<action>')
def capture_action(action: str):
    if action == 'config':
        data = request.get_json(silent=True) or {}
        cfg = capture.read_config()
        cfg.update({k: data[k] for k in ('enabled', 'max_tuners', 'quality', 'encoder') if k in data})
        cfg = capture.write_config(cfg)
        capture.daemon_request('POST', '/reconcile', timeout=3)
        return jsonify({'ok': True, 'config': cfg})
    if action not in ('install', 'uninstall', 'update-chrome'):
        abort(404)
    code, body = capture.daemon_request('POST', f'/{action}', timeout=5)
    if code == 0:
        return jsonify({'ok': False, 'message': body.get('error')}), 503
    return jsonify(body), (200 if code in (200, 202) else 409)

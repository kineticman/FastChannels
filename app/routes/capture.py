"""Built-in capture add-on (beta) — FastChannels side. See app/capture/__init__.py.

Public (DVR clients can't sign in):
    /m3u/capture[/gracenote]             bridge-only playlists: DRM channels via capture
    /feeds/<slug>/m3u/capture[/gracenote|/mixed]   the same per feed
    /feeds/<slug>/m3u/combined/capture   whole feed in one playlist (auto routing)
    /play/capture/<id>.m3u8              start/keep alive a capture, return its HLS playlist
    /play/capture/s/<session>/<segment>  HLS segments, read straight from disk
    /play/capture/<id>.ts                the same capture as one MPEG-TS stream (experimental)

Admin:
    GET  /api/capture/status
    POST /api/capture/install | uninstall | update-chrome
    POST /api/capture/config             {enabled, max_tuners, quality, encoder, keep_warm, startup, stream_format}
    POST /api/capture/sessions/<id>/stop end a capture (a client still watching retunes)

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
# How long a tune waits for a busy tuner. Covers a channel change (the old session
# is handed over once unpolled for ~6 s) but turns a real overflow away quickly, so
# a single-playlist source with no stream limit fails fast instead of hanging.
_BUSY_WAIT_SECONDS = 10
# POST /sessions can take this long to answer: taking over an abandoned tuner, the
# daemon first waits up to 10 s for the old session to shut down. A shorter timeout
# here turned a slow shutdown into a 503 (2026-11-26).
_SESSION_CALL_TIMEOUT = 15
# Players start ~3 target durations back from the live edge (HLS spec 6.3.3). With
# fewer segments than that they start AT the edge and rebuffer before every new
# segment — VLC logged a stall per 2 s segment for the first ~45 s with 2 here.
_MIN_SEGMENTS = 3
# Channels DVR gives an HLS request 15 s in all (its developer's figure; they won't
# raise it), and sending the headers early doesn't extend that. Past this long, send
# what there is, even one segment; a thinner start beats a failed tune.
_SERVE_BY_SECONDS = 12
# Marks a session served that way, so its next polls aren't held for three segments.
_SERVED_MARK = '.served'
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

def _not_ready() -> Response | None:
    """Capture playlists exist while the add-on is installed and on and Bridge mode
    is on (the same rule as every bridge method's playlists)."""
    if not capture.is_active():
        return _text('Built-in Capture is not installed or is turned off (Bridge page).', 503)
    if not AppSettings.get().bridge_enabled:
        return _text('Bridge mode is off (Bridge page), so no DRM channels are marked for capture.', 503)
    return None


def _serve_artifact(key: str, download_name: str, what: str) -> Response:
    from .output import _artifact, _send_feed_artifact
    path = _artifact(key, ext='m3u')
    if path is None:
        return _text(f'{what} is being built. Retry shortly.', 503, retry_after=15)
    return _send_feed_artifact(path, mimetype='application/x-mpegurl', download_name=download_name)


@capture_bp.route('/m3u/capture')
def m3u_capture():
    """DRM channels through the built-in capture add-on, standard-guide half (pair
    with /epg.xml). Import alongside the regular M3U: it carries only the channels
    that one leaves out. Filters (?source= etc.) build it live."""
    if (blocked := _not_ready()):
        return blocked
    from ..generators.m3u import generate_capture_m3u
    from .output import _filters
    if (filters := _filters()):
        return Response(generate_capture_m3u(filters, base_url=public_base_url()),
                        mimetype='application/x-mpegurl',
                        headers={'Content-Disposition': 'attachment; filename="fastchannels-capture.m3u"'})
    return _serve_artifact('master-capture-m3u', 'fastchannels-capture.m3u', 'The capture playlist')


@capture_bp.route('/m3u/capture/gracenote')
def m3u_capture_gracenote():
    """Gracenote-guide half of /m3u/capture (tvc-guide-stationid, no XMLTV)."""
    if (blocked := _not_ready()):
        return blocked
    from ..generators.m3u import generate_capture_m3u, _MASTER_GRACENOTE_START
    from .output import _filters
    if (filters := _filters()):
        return Response(generate_capture_m3u(filters, base_url=public_base_url(), gracenote=True,
                                             namespace_start=_MASTER_GRACENOTE_START),
                        mimetype='application/x-mpegurl',
                        headers={'Content-Disposition': 'attachment; filename="fastchannels-capture-gracenote.m3u"'})
    return _serve_artifact('master-capture-gracenote-m3u', 'fastchannels-capture-gracenote.m3u',
                           'The capture Gracenote playlist')


def _feed(slug: str):
    from ..models import Feed
    return Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()


def _mixed_only(feed) -> Response | None:
    if (feed.guide_mode or 'split') != 'mixed':
        return _text(f'Feed {feed.slug} is not in mixed guide mode. Turn it on from the Feeds page.', 404)
    return None


@capture_bp.route('/feeds/<slug>/m3u/capture')
def feed_m3u_capture(slug):
    """A feed's capture playlist, standard-guide half. Pair with /feeds/<slug>/epg.xml."""
    if (blocked := _not_ready()):
        return blocked
    feed = _feed(slug)
    return _serve_artifact(f'feed-{feed.slug}-capture-m3u', f'{feed.slug}-capture.m3u',
                           f'The capture playlist for {feed.slug}')


@capture_bp.route('/feeds/<slug>/m3u/capture/gracenote')
def feed_m3u_capture_gracenote(slug):
    if (blocked := _not_ready()):
        return blocked
    feed = _feed(slug)
    from .output import _split_guide_only
    if (blocked := _split_guide_only(feed)):
        return blocked
    return _serve_artifact(f'feed-{feed.slug}-capture-gracenote-m3u', f'{feed.slug}-capture-gracenote.m3u',
                           f'The capture Gracenote playlist for {feed.slug}')


@capture_bp.route('/feeds/<slug>/m3u/capture/mixed')
def feed_m3u_capture_mixed(slug):
    """A mixed-guide feed's capture playlist: one Channels DVR source for both guides."""
    if (blocked := _not_ready()):
        return blocked
    feed = _feed(slug)
    if (blocked := _mixed_only(feed)):
        return blocked
    return _serve_artifact(f'feed-{feed.slug}-capture-mixed-m3u', f'{feed.slug}-capture-mixed.m3u',
                           f'The capture playlist for {feed.slug}')


@capture_bp.route('/feeds/<slug>/m3u/combined/capture')
def feed_m3u_combined_capture(slug):
    """The whole feed in one playlist: standard channels direct, DRM channels through
    Built-in Capture, decided at tune time (/play/auto-capture/...). Mixed guide mode
    only; pair with /feeds/<slug>/epg.xml."""
    if (blocked := _not_ready()):
        return blocked
    feed = _feed(slug)
    if (blocked := _mixed_only(feed)):
        return blocked
    return _serve_artifact(f'feed-{feed.slug}-combined-capture-m3u', f'{feed.slug}-combined-capture.m3u',
                           f'The single playlist for {feed.slug}')


# ── live HLS ──────────────────────────────────────────────────────────────────

def _segment_count(playlist: str) -> int:
    return sum(1 for line in playlist.splitlines() if line and not line.startswith('#'))


def _rewrite(playlist: str, session_id: str) -> str:
    out = []
    for line in playlist.splitlines():
        if line.startswith('#EXT-X-TARGETDURATION:'):
            # With short startup segments ffmpeg says 1 while only those are listed,
            # then 2. The target may not change during a stream: it is 2 from the start.
            line = '#EXT-X-TARGETDURATION:2'
        elif line and not line.startswith('#'):
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
    if channel.id in capture.restricted_channel_ids():
        return _text(f'{channel.name} is only allowed on hardware players (its license keys are '
                     'restricted), so Built-in Capture skips it. Use FastChannels Player for it.', 422)

    deadline = time.monotonic() + _TUNE_WAIT_SECONDS
    started = time.monotonic()
    busy_since = None
    logged = False
    while True:
        code, body = capture.daemon_request('POST', '/sessions', json={'channel_id': channel_id},
                                              timeout=_SESSION_CALL_TIMEOUT)
        if code != 200:
            msg = body.get('error') or f'capture service returned HTTP {code}'
            # Busy/starting are usually seconds from clearing: a channel change leaves
            # the old session unpolled, and the daemon hands its tuner over once it's
            # PREEMPT_IDLE_SECONDS stale. Hold the request rather than refuse, since
            # players (VLC) give up on a 503 instead of retrying sensibly.
            now = time.monotonic()
            if body.get('reason') == 'busy':
                busy_since = busy_since or now
                wait_until = min(deadline, busy_since + _BUSY_WAIT_SECONDS)
            else:
                wait_until = deadline
            if body.get('retry_after') and now + 1 < wait_until:
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
            session_dir = os.path.join(capture.SESSION_ROOT, session_id)
            path = os.path.join(session_dir, 'index.m3u8')
            mark = os.path.join(session_dir, _SERVED_MARK)
            try:
                with open(path, encoding='utf-8') as fp:
                    playlist = fp.read()
            except OSError:
                playlist = ''
            count = _segment_count(playlist)
            waited = time.monotonic() - started
            short = 0 < count < _MIN_SEGMENTS and (waited >= _SERVE_BY_SECONDS or os.path.exists(mark))
            if count >= _MIN_SEGMENTS or short:
                if waited > 1:
                    logger.info('[capture] channel %s ready after %.1fs (source video %s)%s',
                                channel_id, waited, body.get('video_size'),
                                f', starting with {count} segment(s)' if short else '')
                if short:
                    try:
                        open(mark, 'a').close()
                    except OSError:
                        pass
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


# ── one MPEG-TS stream (experimental) ─────────────────────────────────────────
# Channels DVR gives an HLS request 15 s, but an MPEG-TS stream that answers at once
# and sends NULL packets was still accepted with video at 28 s (measured 2026-11-26).
# The video is the HLS session's own segments, passed along as they appear.
# One long request per viewer: gunicorn.conf.py keeps that worker from recycling.

_TS_NULL = (b'\x47\x1f\xff\x10' + b'\xff' * 184) * 7
_TS_STALL_SECONDS = 45      # no new segment for this long (tune or mid-stream): give up
_TS_JOIN_SEGMENTS = 2       # joining a capture already running: start this far back
# How long a busy tune stays silent (so a real "all tuners busy" can still be a 503)
# before starting the filler for a tuner that is being freed.
_TS_QUIET_SECONDS = 4
# Not polled for this long, in whole seconds: its viewer has left (a watched capture
# is polled every second or so).
_TS_ABANDONED_IDLE = 3
_SEGMENT_LINE_RE = re.compile(r'^(?:.*/)?(seg_(\d{6})\.ts)$', re.M)


def _ts_stream(channel_id: int, name: str):
    started = last_data = last_poll = time.monotonic()
    session_id, state, sent_upto, total, video_at = '', '', None, 0, None
    busy_since = None
    try:
        yield _TS_NULL
        while True:
            now = time.monotonic()
            if now - last_poll >= 1 or not session_id:
                # Also the session's keep-alive.
                code, body = capture.daemon_request('POST', '/sessions', json={'channel_id': channel_id},
                                              timeout=_SESSION_CALL_TIMEOUT)
                last_poll = now
                if code == 200:
                    state = body.get('state')
                    if state == 'failed':
                        logger.warning('[capture-ts] channel %s (%s): capture failed: %s',
                                       channel_id, name, body.get('error'))
                        return
                    new_id = body.get('session') or ''
                    if new_id != session_id and _SESSION_RE.match(new_id):
                        session_id, sent_upto = new_id, None
                    busy_since = None
                elif body.get('reason') == 'busy' and not session_id:
                    busy_since = busy_since or now
                    if now - busy_since > _BUSY_WAIT_SECONDS:
                        logger.warning('[capture-ts] channel %s (%s): %s', channel_id, name, body.get('error'))
                        return
                elif code and not body.get('retry_after'):
                    logger.warning('[capture-ts] channel %s (%s): %s', channel_id, name, body.get('error'))
                    return
            wrote = False
            if state == 'live' and session_id:
                session_dir = os.path.join(capture.SESSION_ROOT, session_id)
                try:
                    with open(os.path.join(session_dir, 'index.m3u8'), encoding='utf-8') as fp:
                        segments = [(int(n), f) for f, n in _SEGMENT_LINE_RE.findall(fp.read())]
                except OSError:
                    segments = []
                if segments:
                    if sent_upto is None:
                        sent_upto = segments[max(0, len(segments) - _TS_JOIN_SEGMENTS)][0] - 1
                    elif segments[-1][0] < sent_upto:
                        sent_upto = -1      # FFmpeg was restarted (GPU fallback)
                for number, filename in segments:
                    if number <= sent_upto:
                        continue
                    try:
                        with open(os.path.join(session_dir, filename), 'rb') as fp:
                            data = fp.read()
                    except OSError:
                        data = b''          # already deleted: we fell behind
                    sent_upto = number
                    if not data:
                        continue
                    if video_at is None:
                        video_at = time.monotonic()
                        logger.info('[capture-ts] channel %s (%s): video after %.1fs',
                                    channel_id, name, video_at - started)
                    yield data
                    total += len(data)
                    last_data, wrote = time.monotonic(), True
            if time.monotonic() - last_data > _TS_STALL_SECONDS:
                logger.warning('[capture-ts] channel %s (%s): no video for %ds', channel_id, name,
                               _TS_STALL_SECONDS)
                return
            if not wrote:
                if video_at is None:
                    yield _TS_NULL          # keeps the DVR waiting through the tune
                time.sleep(0.25)
    finally:
        # Also reached when the client goes away, which only a write notices.
        logger.info('[capture-ts] channel %s (%s): stream closed after %.0fs, %.1f MB',
                    channel_id, name, time.monotonic() - started, total / 1e6)


def _tuner_being_freed() -> bool:
    code, health = capture.daemon_request('GET', '/health', timeout=5)
    return code == 200 and any(
        x.get('state') in ('tuning', 'live') and (x.get('idle_s') or 0) >= _TS_ABANDONED_IDLE
        for x in health.get('sessions') or [])


@capture_bp.route('/play/capture/<int:channel_id>.ts')
def capture_ts(channel_id: int):
    if not capture.is_active():
        return _text('Built-in capture is not installed or is turned off.', 503)
    channel = Channel.query.get_or_404(channel_id)
    if channel.source and channel.source.name in capture.INCOMPATIBLE_SOURCES:
        return _text(f'{channel.source.display_name} refuses Linux Chrome; it cannot be captured.', 422)
    if channel.id in capture.restricted_channel_ids():
        return _text(f'{channel.name} is only allowed on hardware players (its license keys are '
                     'restricted), so Built-in Capture skips it. Use FastChannels Player for it.', 422)
    name = channel.name
    # A busy or unavailable tuner is refused with a status before the stream starts.
    # But a channel change must not stay silent while the old capture is freed
    # (headers took 12.5 s once; Channels allows about 20): once a tuner's viewer
    # is clearly gone, start the filler and wait inside the stream.
    started = time.monotonic()
    deadline = started + _BUSY_WAIT_SECONDS
    while True:
        code, body = capture.daemon_request('POST', '/sessions', json={'channel_id': channel_id},
                                              timeout=_SESSION_CALL_TIMEOUT)
        if code == 200 or body.get('reason') == 'starting':
            break
        if (body.get('reason') == 'busy' and time.monotonic() - started >= _TS_QUIET_SECONDS
                and _tuner_being_freed()):
            break
        if body.get('reason') != 'busy' or time.monotonic() + 1 >= deadline:
            msg = body.get('error') or f'capture service returned HTTP {code}'
            _warn_limited(channel_id, '[capture-ts] channel %s (%s): %s', channel_id, name, msg)
            return _text(msg, 503, retry_after=body.get('retry_after') or 10)
        time.sleep(1)
    return Response(_ts_stream(channel_id, name), mimetype='video/mp2t',
                    headers={**_HLS_HEADERS, 'X-Accel-Buffering': 'no'})


# ── admin API ─────────────────────────────────────────────────────────────────

_last_active: bool | None = None


def _apply_capture_change() -> None:
    """Built-in Capture went on or off: re-check which DRM channels have a bridge
    (drm_bridge_mode_for counts the add-on) and rebuild the playlists so the capture
    ones appear or go without waiting for the next scrape."""
    from ..extensions import db
    from .api_dvr import _reconcile_drm_bridge_mode
    from .tasks import trigger_xml_refresh
    try:
        _reconcile_drm_bridge_mode()
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception('[capture] bridge eligibility re-check failed')
    trigger_xml_refresh()


def _note_active_state() -> None:
    """Catch the add-on becoming active or inactive outside a config save (an
    install finishing, an uninstall) the next time the Bridge page polls."""
    global _last_active
    active = capture.is_active()
    if _last_active is not None and active != _last_active:
        logger.info('[capture] Built-in Capture is now %s', 'active' if active else 'inactive')
        _apply_capture_change()
    _last_active = active


@capture_bp.get('/api/capture/status')
def capture_status():
    _note_active_state()
    code, health = capture.daemon_request('GET', '/health', timeout=3)
    if code != 200:
        return jsonify({
            'platform_supported': capture.supported_platform(),
            'install': capture.read_status(),
            'config': capture.read_config(),
            'service_error': health.get('error') or f'capture service returned HTTP {code}',
            'tuners': [], 'sessions': [],
            'gpu': capture.gpu_info(),
        })
    health['incompatible_names'] = _source_names(capture.INCOMPATIBLE_SOURCES)
    health['gpu'] = capture.gpu_info()
    health['channel_names'], health['channel_sources'] = _channel_labels(
        {x.get('channel_id') for x in health.get('sessions') or [] if x.get('channel_id')})
    return jsonify(health)


def _channel_labels(ids) -> tuple[dict, dict]:
    """({channel id: channel name}, {channel id: source's display name})."""
    if not ids:
        return {}, {}
    from ..models import Channel, Source
    # Named columns only: Source.config can be megabytes.
    rows = (Channel.query.join(Source)
            .with_entities(Channel.id, Channel.name, Source.name, Source.display_name)
            .filter(Channel.id.in_(ids)).all())
    return ({str(cid): name for cid, name, _, _ in rows},
            {str(cid): display or source for cid, _, source, display in rows})


@capture_bp.get('/api/capture/gpu-diagnostics')
def capture_gpu_diagnostics():
    """GPU checks and test encodes for the Bridge page's diagnostics modal. Runs a few
    10-frame test encodes, so it takes a few seconds."""
    code, body = capture.daemon_request('POST', '/gpu-diagnose', timeout=120)
    if code != 200:
        return jsonify({'error': body.get('error') or f'capture service returned HTTP {code}'}), 503
    return jsonify(body)


@capture_bp.post('/api/capture/sessions/<session_id>/stop')
def capture_stop_session(session_id: str):
    if not session_id.isalnum():
        abort(404)
    code, body = capture.daemon_request('POST', f'/sessions/{session_id}/stop', timeout=5)
    return jsonify(body), (200 if code == 200 else 404)


def _source_names(names) -> list[str]:
    from ..scrapers import registry
    scrapers = registry.get_all()
    return sorted((getattr(scrapers.get(n), 'display_name', None) or n) for n in names)


@capture_bp.post('/api/capture/<action>')
def capture_action(action: str):
    if action == 'config':
        data = request.get_json(silent=True) or {}
        was_active = capture.is_active()
        cfg = capture.read_config()
        was_format = cfg['stream_format']
        cfg.update({k: data[k] for k in ('enabled', 'max_tuners', 'quality', 'encoder', 'keep_warm', 'startup', 'stream_format') if k in data})
        cfg = capture.write_config(cfg)
        capture.daemon_request('POST', '/reconcile', timeout=3)
        if capture.is_active() != was_active:
            global _last_active
            _last_active = capture.is_active()
            _apply_capture_change()
        elif cfg['stream_format'] != was_format:
            from .tasks import trigger_xml_refresh
            trigger_xml_refresh()           # the capture playlists' links change
        return jsonify({'ok': True, 'config': cfg})
    if action not in ('install', 'uninstall', 'update-chrome', 'intel-newer', 'intel-standard'):
        abort(404)
    code, body = capture.daemon_request('POST', f'/{action}', timeout=5)
    if code == 0:
        return jsonify({'ok': False, 'message': body.get('error')}), 503
    return jsonify(body), (200 if code in (200, 202) else 409)

import json
import logging
import re
import time as _time
import requests as _req

logger = logging.getLogger(__name__)
from urllib.parse import urljoin as _urljoin, urlsplit
from flask import Blueprint, jsonify, request, current_app
from ..extensions import db
from ..models import Source, Channel

from .api_shared import _invalidate_and_refresh_xml

custom_channels_bp = Blueprint('api_custom_channels', __name__)

_CUSTOM_DIRECT_SUFFIXES = ('.m3u8', '.mpd', '.mp4', '.webm', '.ts')
_CUSTOM_DETECT_KEY_PREFIX = 'custom-detect:'
_CUSTOM_DETECT_TTL_SECONDS = 600
_CUSTOM_DETECT_DONE_TTL_SECONDS = 180


def _normalize_custom_stream_type(raw_type, stream_url: str | None = None) -> str:
    from ..scrapers.stream_detector import StreamDetector

    explicit = (raw_type or '').strip().lower()
    if explicit:
        return explicit

    inferred = StreamDetector.infer_stream_type(stream_url)
    return inferred or 'hls'


@custom_channels_bp.route('/custom-channels', methods=['GET'])
def list_custom_channels():
    """Return all channels belonging to the custom source."""
    custom_source = Source.query.filter_by(name='custom').first()
    if not custom_source:
        return jsonify([])
    channels = (
        Channel.query
        .filter_by(source_id=custom_source.id)
        .order_by(Channel.name)
        .all()
    )
    return jsonify([ch.to_dict() for ch in channels])


@custom_channels_bp.route('/custom-channels/catalog', methods=['GET'])
def list_channel_catalog():
    """Return the pre-configured channel catalog with already_added flags."""
    from .channel_catalog import CHANNEL_CATALOG

    custom_source = Source.query.filter_by(name='custom').first()
    existing_urls = set()
    if custom_source:
        for ch in Channel.query.filter_by(source_id=custom_source.id).all():
            # page_url holds the original player URL for redetect channels;
            # stream_url may have been overwritten with the resolved CDN URL
            # after first play, so check both.
            if ch.stream_url:
                existing_urls.add(ch.stream_url)
            if ch.page_url:
                existing_urls.add(ch.page_url)

    result = []
    for group in CHANNEL_CATALOG:
        channels = []
        for ch in group["channels"]:
            channels.append({**ch, "already_added": ch["stream_url"] in existing_urls})
        result.append({**group, "channels": channels})
    return jsonify(result)


_PREVIEW_PROXY_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/145.0.0.0 Safari/537.36'
)
_PREVIEW_SSRF_RE = re.compile(
    r'^(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|169\.254\.|::1|0\.0\.0\.0)',
    re.IGNORECASE,
)


@custom_channels_bp.route('/custom-channels/preview-manifest')
def custom_channel_preview_manifest():
    """
    Proxy an HLS manifest for the custom-channel stream preview UI.

    Fetches the manifest server-side (bypassing browser CORS/header restrictions),
    resolves master→variant if needed, rewrites segment URLs to go through
    /api/custom-channels/preview-segment, and returns the manifest.
    """
    from urllib.parse import quote as _quote

    raw_url = request.args.get('url', '').strip()
    headers_json = request.args.get('h', '{}')

    if not raw_url.startswith('https://'):
        return ('HTTPS only', 400)
    parsed = urlsplit(raw_url)
    if _PREVIEW_SSRF_RE.match(parsed.netloc.split(':')[0]):
        return ('Blocked', 403)

    try:
        extra = json.loads(headers_json)
    except Exception:
        extra = {}

    req_headers = {k: v for k, v in extra.items() if not k.startswith('_')}
    req_headers.setdefault('User-Agent', _PREVIEW_PROXY_UA)

    try:
        r = _req.get(raw_url, headers=req_headers, timeout=12)
        r.raise_for_status()
    except Exception as e:
        return (f'Upstream error: {e}', 502)

    text = r.text
    effective_url = r.url

    if '#EXT-X-STREAM-INF' in text:
        from app.scrapers.distro import _pick_best_variant
        best = _pick_best_variant(text, effective_url)
        if not best:
            return ('No variant found', 502)
        try:
            vr = _req.get(best, headers=req_headers, timeout=12)
            vr.raise_for_status()
            text = vr.text
            effective_url = vr.url
        except Exception as e:
            return (f'Variant fetch error: {e}', 502)

    variant_base = effective_url.rsplit('/', 1)[0] + '/'
    base_url = request.host_url.rstrip('/')
    encoded_h = _quote(headers_json, safe='')

    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            abs_seg = stripped if stripped.startswith('http') else _urljoin(variant_base, stripped)
            line = f'{base_url}/api/custom-channels/preview-segment?url={_quote(abs_seg, safe="")}&h={encoded_h}'
        lines.append(line)

    from flask import Response
    return Response(
        '\n'.join(lines),
        mimetype='application/vnd.apple.mpegurl',
        headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
    )


@custom_channels_bp.route('/custom-channels/preview-segment')
def custom_channel_preview_segment():
    """Proxy a single HLS segment for the custom-channel stream preview UI."""
    from flask import Response

    # Flask already decodes query params once; do NOT call unquote again or
    # percent-encoded chars in auth tokens (e.g. %2F, %2B) get over-decoded.
    raw_url = request.args.get('url', '')
    headers_json = request.args.get('h', '{}')

    if not raw_url.startswith('https://'):
        return ('HTTPS only', 400)
    parsed = urlsplit(raw_url)
    if _PREVIEW_SSRF_RE.match(parsed.netloc.split(':')[0]):
        return ('Blocked', 403)

    try:
        extra = json.loads(headers_json)
    except Exception:
        extra = {}

    req_headers = {k: v for k, v in extra.items() if not k.startswith('_')}
    req_headers.setdefault('User-Agent', _PREVIEW_PROXY_UA)

    try:
        r = _req.get(raw_url, headers=req_headers, timeout=20, stream=True)
        r.raise_for_status()
    except Exception as e:
        logger.warning('[preview-segment] fetch failed for %s: %s', raw_url[:120], e)
        return (f'Segment fetch failed: {e}', 502)

    content_type = r.headers.get('Content-Type', 'video/MP2T')
    return Response(
        r.iter_content(8192),
        content_type=content_type,
        headers={'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'},
    )


def _custom_detect_key(detect_id: str) -> str:
    return f'{_CUSTOM_DETECT_KEY_PREFIX}{detect_id}'


def _custom_detect_stage_text(stage: str, detail: str | None = None) -> str:
    labels = {
        'queued': 'Queued',
        'starting': 'Starting detection…',
        'yt-dlp': 'Resolving via yt-dlp…',
        'twitch': 'Resolving Twitch…',
        'fetching page': 'Fetching page…',
        'checking embedded APIs': 'Checking embedded APIs…',
        'following iframe': 'Following iframe…',
        'trying browser fallback': 'Trying browser fallback…',
        'probing candidate': 'Probing candidate stream…',
        'done': 'Complete',
        'error': 'Detection error',
    }
    text = labels.get(stage, stage.replace('_', ' ').strip().capitalize() if stage else 'Running…')
    if detail:
        return f'{text} · {detail}'
    return text


def _custom_detect_payload(result, elapsed_ms: int, is_page_url: bool) -> dict:
    return {
        'success': result.success,
        'stream_url': result.stream_url,
        'stream_type': result.stream_type,
        'headers': result.headers,
        'needs_proxy': result.needs_proxy,
        'error': result.error,
        'resolver': result.resolver,
        'is_page_url': is_page_url or result.is_youtube,
        'is_youtube': result.is_youtube,
        'elapsed_ms': elapsed_ms,
    }


def _custom_detect_write_state(redis_client, detect_id: str, state: dict, ttl: int) -> None:
    state['updated_ms'] = int(_time.time() * 1000)
    redis_client.set(_custom_detect_key(detect_id), json.dumps(state), ex=ttl)


def _custom_detect_run_job(redis_url: str, detect_id: str, url: str, is_page_url: bool) -> None:
    import redis as _redis
    from ..scrapers.stream_detector import StreamDetector

    r = _redis.from_url(redis_url)
    started_ms = int(_time.time() * 1000)
    state = {
        'detect_id': detect_id,
        'status': 'running',
        'stage': 'starting',
        'stage_text': 'Starting detection…',
        'detail': None,
        'url': url,
        'started_ms': started_ms,
        'elapsed_ms': 0,
        'success': False,
        'stream_url': None,
        'stream_type': None,
        'headers': {},
        'needs_proxy': False,
        'error': None,
        'resolver': None,
        'is_page_url': is_page_url,
        'is_youtube': False,
    }
    _custom_detect_write_state(r, detect_id, state, _CUSTOM_DETECT_TTL_SECONDS)

    def report(stage: str, detail: str | None = None) -> None:
        state['stage'] = stage
        state['detail'] = detail
        state['stage_text'] = _custom_detect_stage_text(stage, detail)
        _custom_detect_write_state(r, detect_id, state, _CUSTOM_DETECT_TTL_SECONDS)

    try:
        result = StreamDetector(stage_callback=report).detect(url)
        elapsed_ms = int(_time.time() * 1000) - started_ms
        state.update(_custom_detect_payload(result, elapsed_ms, is_page_url))
        state['status'] = 'done'
        state['stage'] = 'done'
        state['detail'] = None
        state['stage_text'] = 'Complete' if result.success else (_custom_detect_stage_text('error') if result.error else 'Done')
        _custom_detect_write_state(r, detect_id, state, _CUSTOM_DETECT_DONE_TTL_SECONDS)
    except Exception as exc:
        elapsed_ms = int(_time.time() * 1000) - started_ms
        state.update({
            'status': 'done',
            'stage': 'error',
            'stage_text': 'Detection error',
            'detail': None,
            'success': False,
            'stream_url': None,
            'stream_type': None,
            'headers': {},
            'needs_proxy': False,
            'error': str(exc),
            'resolver': None,
            'is_youtube': False,
            'elapsed_ms': elapsed_ms,
        })
        _custom_detect_write_state(r, detect_id, state, _CUSTOM_DETECT_DONE_TTL_SECONDS)


@custom_channels_bp.route('/custom-channels/detect/start', methods=['POST'])
def start_custom_stream_detect():
    data = request.get_json() or {}
    url = (data.get('url') or '').strip()
    if not url:
        return jsonify({'error': 'url is required'}), 400
    if not url.startswith(('http://', 'https://')):
        return jsonify({'error': 'URL must start with http:// or https://'}), 400

    import threading
    import uuid as _uuid
    import redis as _redis
    from urllib.parse import urlsplit as _us

    _path = _us(url).path.lower()
    is_page_url = not any(suffix in _path for suffix in _CUSTOM_DIRECT_SUFFIXES)
    detect_id = _uuid.uuid4().hex
    started_ms = int(_time.time() * 1000)
    redis_url = current_app.config['REDIS_URL']
    r = _redis.from_url(redis_url)
    state = {
        'detect_id': detect_id,
        'status': 'queued',
        'stage': 'queued',
        'stage_text': 'Queued',
        'detail': None,
        'url': url,
        'started_ms': started_ms,
        'elapsed_ms': 0,
        'success': False,
        'stream_url': None,
        'stream_type': None,
        'headers': {},
        'needs_proxy': False,
        'error': None,
        'resolver': None,
        'is_page_url': is_page_url,
        'is_youtube': False,
    }
    _custom_detect_write_state(r, detect_id, state, _CUSTOM_DETECT_TTL_SECONDS)

    t = threading.Thread(
        target=_custom_detect_run_job,
        args=(redis_url, detect_id, url, is_page_url),
        daemon=True,
    )
    t.start()
    return jsonify({
        'detect_id': detect_id,
        'status': 'queued',
        'status_url': f'/api/custom-channels/detect/{detect_id}',
    })


@custom_channels_bp.route('/custom-channels/detect/<detect_id>', methods=['GET'])
def custom_stream_detect_status(detect_id):
    import redis as _redis

    try:
        r = _redis.from_url(current_app.config['REDIS_URL'])
        raw = r.get(_custom_detect_key(detect_id))
        if not raw:
            return jsonify({'status': 'expired', 'error': 'Detection session expired'}), 404
        data = json.loads(raw)
        started_ms = int(data.get('started_ms') or 0)
        if data.get('status') == 'running' and started_ms:
            elapsed_ms = max(0, int(_time.time() * 1000) - started_ms)
            data['elapsed_ms'] = elapsed_ms
            # If the job has been running longer than the detection budget + a
            # generous slack, the worker thread was killed (gunicorn recycle,
            # OOM, etc.) and will never write a done state.  Synthesize a
            # timeout so the UI doesn't spin indefinitely.
            from ..scrapers.stream_detector import StreamDetector
            timeout_ms = (StreamDetector.DETECT_BUDGET_SECONDS + 30) * 1000
            if elapsed_ms > timeout_ms:
                data['status'] = 'done'
                data['stage'] = 'error'
                data['stage_text'] = 'Detection timed out'
                data['success'] = False
                data['error'] = 'Detection timed out (worker may have been recycled)'
        return jsonify(data)
    except Exception as exc:
        return jsonify({'status': 'error', 'error': str(exc)}), 500


@custom_channels_bp.route('/custom-channels/detect', methods=['POST'])
def detect_custom_stream():
    """Probe a URL (web page or direct stream) and return the working stream URL + type + headers."""
    data = request.get_json() or {}
    url = (data.get('url') or '').strip()
    if not url:
        return jsonify({'error': 'url is required'}), 400
    if not url.startswith(('http://', 'https://')):
        return jsonify({'error': 'URL must start with http:// or https://'}), 400

    from ..scrapers.stream_detector import StreamDetector
    import time
    from urllib.parse import urlsplit as _us
    _path = _us(url).path.lower()
    is_page_url = not any(suffix in _path for suffix in _CUSTOM_DIRECT_SUFFIXES)

    started_at = time.perf_counter()
    result = StreamDetector().detect(url)
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)

    return jsonify({
        'success': result.success,
        'stream_url': result.stream_url,
        'stream_type': result.stream_type,
        'headers': result.headers,
        'needs_proxy': result.needs_proxy,
        'error': result.error,
        'resolver': result.resolver,
        'is_page_url': is_page_url or result.is_youtube,
        'is_youtube': result.is_youtube,
        'elapsed_ms': elapsed_ms,
    })


@custom_channels_bp.route('/custom-channels', methods=['POST'])
def create_custom_channel():
    """Create a user-defined custom channel under the 'custom' source."""
    import uuid as _uuid
    from datetime import datetime, timezone as _tz

    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    stream_url = (data.get('stream_url') or '').strip()

    if not name:
        return jsonify({'error': 'name is required'}), 400
    if not stream_url or not stream_url.startswith('http'):
        return jsonify({'error': 'stream_url must be a valid HTTP(S) URL'}), 400

    custom_source = Source.query.filter_by(name='custom').first()
    if not custom_source:
        return jsonify({'error': 'Custom source not found — restart the server to seed it'}), 500

    redetect = bool(data.get('redetect_on_play', False))
    explicit_page_url = (data.get('page_url') or '').strip() or None
    # When redetect_on_play is set but no page_url is provided, treat the
    # stream_url as a player page — the play route uses page_url to decide
    # whether to run the stream detector.
    page_url = explicit_page_url or (stream_url if redetect else None)

    channel = Channel(
        source_id=custom_source.id,
        source_channel_id=str(_uuid.uuid4()),
        name=name,
        description=(data.get('description') or '').strip() or None,
        logo_url=(data.get('logo_url') or '').strip() or None,
        category=(data.get('category') or '').strip() or None,
        language=(data.get('language') or 'en').strip() or 'en',
        stream_url=stream_url,
        stream_type=_normalize_custom_stream_type(data.get('stream_type'), stream_url),
        custom_headers=data.get('custom_headers') or {},
        proxy_segments=bool(data.get('proxy_segments', False)),
        page_url=page_url,
        redetect_on_play=redetect,
        guide_block_minutes=data.get('guide_block_minutes') or None,
        is_active=True,
        is_enabled=True,
        last_seen_at=datetime.now(_tz.utc),
    )
    db.session.add(channel)
    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        if 'database is locked' in str(exc).lower():
            return jsonify({'error': 'Database busy — a scrape job is running. Try again in a moment.'}), 503
        raise
    _invalidate_and_refresh_xml()
    return jsonify(channel.to_dict()), 201


@custom_channels_bp.route('/custom-channels/batch', methods=['POST'])
def create_custom_channels_batch():
    """Create multiple custom channels in a single transaction."""
    import uuid as _uuid
    from datetime import datetime, timezone as _tz

    items = request.get_json() or []
    if not isinstance(items, list) or not items:
        return jsonify({'error': 'expected a non-empty list'}), 400

    custom_source = Source.query.filter_by(name='custom').first()
    if not custom_source:
        return jsonify({'error': 'Custom source not found'}), 500

    created = []
    for data in items:
        name = (data.get('name') or '').strip()
        stream_url = (data.get('stream_url') or '').strip()
        if not name or not stream_url or not stream_url.startswith('http'):
            continue
        redetect = bool(data.get('redetect_on_play', False))
        explicit_page_url = (data.get('page_url') or '').strip() or None
        page_url = explicit_page_url or (stream_url if redetect else None)
        channel = Channel(
            source_id=custom_source.id,
            source_channel_id=str(_uuid.uuid4()),
            name=name,
            description=(data.get('description') or '').strip() or None,
            logo_url=(data.get('logo_url') or '').strip() or None,
            category=(data.get('category') or '').strip() or None,
            language=(data.get('language') or 'en').strip() or 'en',
            stream_url=stream_url,
            stream_type=_normalize_custom_stream_type(data.get('stream_type'), stream_url),
            custom_headers=data.get('custom_headers') or {},
            proxy_segments=bool(data.get('proxy_segments', False)),
            page_url=page_url,
            redetect_on_play=redetect,
            guide_block_minutes=data.get('guide_block_minutes') or None,
            is_active=True,
            is_enabled=True,
            last_seen_at=datetime.now(_tz.utc),
        )
        db.session.add(channel)
        created.append(channel)

    if not created:
        return jsonify({'error': 'no valid channels in request'}), 400

    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        if 'database is locked' in str(exc).lower():
            return jsonify({'error': 'Database busy — a scrape job is running. Try again in a moment.'}), 503
        raise

    _invalidate_and_refresh_xml()
    return jsonify([ch.to_dict() for ch in created]), 201


@custom_channels_bp.route('/custom-channels/<int:channel_id>', methods=['PUT'])
def update_custom_channel(channel_id):
    """Update a custom channel's metadata or stream settings."""
    channel = Channel.query.get_or_404(channel_id)
    if not channel.source or channel.source.name != 'custom':
        return jsonify({'error': 'Not a custom channel'}), 403

    data = request.get_json() or {}

    if 'name' in data:
        name = (data['name'] or '').strip()
        if not name:
            return jsonify({'error': 'name cannot be empty'}), 400
        channel.name = name
    if 'description' in data:
        channel.description = (data['description'] or '').strip() or None
    if 'logo_url' in data:
        channel.logo_url = (data['logo_url'] or '').strip() or None
    if 'category' in data:
        channel.category = (data['category'] or '').strip() or None
    if 'language' in data:
        channel.language = (data['language'] or 'en').strip() or 'en'
    if 'stream_url' in data:
        stream_url = (data['stream_url'] or '').strip()
        if not stream_url.startswith('http'):
            return jsonify({'error': 'stream_url must be a valid HTTP(S) URL'}), 400
        channel.stream_url = stream_url
        if 'stream_type' not in data:
            channel.stream_type = _normalize_custom_stream_type(None, stream_url)
    if 'stream_type' in data:
        channel.stream_type = _normalize_custom_stream_type(data.get('stream_type'), data.get('stream_url') or channel.stream_url)
    if 'custom_headers' in data:
        channel.custom_headers = data['custom_headers'] or {}
    if 'proxy_segments' in data:
        channel.proxy_segments = bool(data['proxy_segments'])
    if 'page_url' in data:
        channel.page_url = (data['page_url'] or '').strip() or None
    if 'redetect_on_play' in data:
        channel.redetect_on_play = bool(data['redetect_on_play'])
    if 'guide_block_minutes' in data:
        channel.guide_block_minutes = data['guide_block_minutes'] or None

    db.session.commit()
    _invalidate_and_refresh_xml()
    return jsonify(channel.to_dict())


@custom_channels_bp.route('/custom-channels/<int:channel_id>', methods=['DELETE'])
def delete_custom_channel(channel_id):
    """Permanently delete a custom channel."""
    channel = Channel.query.get_or_404(channel_id)
    if not channel.source or channel.source.name != 'custom':
        return jsonify({'error': 'Not a custom channel'}), 403
    db.session.delete(channel)
    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        if 'database is locked' in str(exc).lower():
            return jsonify({'error': 'Database busy — a scrape job is running. Try again in a moment.'}), 503
        raise
    _invalidate_and_refresh_xml()
    return jsonify({'status': 'deleted', 'id': channel_id})



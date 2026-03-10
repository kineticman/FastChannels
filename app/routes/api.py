import json
import re
import requests as _req
from urllib.parse import urljoin as _urljoin
from flask import Blueprint, jsonify, request, current_app
from ..extensions import db
from ..models import Source, Channel, AppSettings
from ..scrapers import registry
from ..scrapers.base import StreamDeadError
from .tasks import trigger_scrape, trigger_stream_audit
from ..generators.m3u import get_chnum_overlaps
from .. import logfile

api_bp = Blueprint('api', __name__)

_GRACENOTE_RE = re.compile(r'^(\d+|(EP|SH|MV|SP|TR)\d+)$')


@api_bp.route('/sources')
def list_sources():
    return jsonify([s.to_dict() for s in Source.query.order_by(Source.display_name).all()])


@api_bp.route('/sources/<int:source_id>/run', methods=['POST'])
def run_source(source_id):
    source = Source.query.get_or_404(source_id)
    trigger_scrape(source.name)
    return jsonify({'status': 'queued', 'source': source.name})


@api_bp.route('/sources/<int:source_id>/scrape-status')
def scrape_status(source_id):
    import redis as _redis
    from rq import Queue
    from rq.registry import StartedJobRegistry

    source = Source.query.get_or_404(source_id)
    try:
        r = _redis.from_url(current_app.config['REDIS_URL'])
        # Active progress written by the worker
        raw = r.get(f'scrape:progress:{source.name}')
        if raw:
            data = json.loads(raw)
            return jsonify({'status': 'running', **data})
        # Check if queued but not yet started
        q = Queue('scraper', connection=r)
        for job_id in q.get_job_ids():
            try:
                job = q.fetch_job(job_id)
                if job and job.args and job.args[0] == source.name \
                        and 'stream_audit' not in (job.func_name or ''):
                    return jsonify({'status': 'queued'})
            except Exception:
                pass
        # Check started registry (job may have just started before writing progress)
        registry = StartedJobRegistry('scraper', connection=r)
        for job_id in registry.get_job_ids():
            try:
                from rq.job import Job
                job = Job.fetch(job_id, connection=r)
                if job.args and job.args[0] == source.name \
                        and 'stream_audit' not in (job.func_name or ''):
                    return jsonify({'status': 'running', 'phase': 'starting'})
            except Exception:
                pass
    except Exception:
        pass
    return jsonify({'status': 'idle'})


@api_bp.route('/sources/<int:source_id>/stream-audit', methods=['POST'])
def stream_audit_source(source_id):
    source = Source.query.get_or_404(source_id)
    trigger_stream_audit(source.name)
    return jsonify({'status': 'queued', 'source': source.name})


@api_bp.route('/sources/<int:source_id>/audit-status')
def audit_status(source_id):
    import time as _time
    import redis as _redis
    from rq import Queue
    from rq.registry import StartedJobRegistry

    source = Source.query.get_or_404(source_id)
    try:
        r = _redis.from_url(current_app.config['REDIS_URL'])
        key = f'audit:progress:{source.name}'
        raw = r.get(key)
        if raw:
            data = json.loads(raw)
            # Stale check — progress is written every ~25 channels (~20s); treat
            # as dead if no heartbeat for 90s (catches mid-job container restarts).
            if _time.time() - data.get('ts', 0) > 90:
                r.delete(key)
            else:
                return jsonify({'status': 'running', **data})
        q = Queue('scraper', connection=r)
        for job_id in q.get_job_ids():
            try:
                job = q.fetch_job(job_id)
                if job and job.args and job.args[0] == source.name \
                        and 'stream_audit' in (job.func_name or ''):
                    return jsonify({'status': 'queued'})
            except Exception:
                pass
        registry = StartedJobRegistry('scraper', connection=r)
        for job_id in registry.get_job_ids():
            try:
                from rq.job import Job
                job = Job.fetch(job_id, connection=r)
                if job.args and job.args[0] == source.name \
                        and 'stream_audit' in (job.func_name or ''):
                    return jsonify({'status': 'running', 'phase': 'starting'})
            except Exception:
                pass
    except Exception:
        pass
    return jsonify({'status': 'idle'})


@api_bp.route('/sources/chnum-overlaps')
def chnum_overlaps():
    """Return a list of channel-number overlap warnings for the current source config."""
    return jsonify({'warnings': get_chnum_overlaps()})


@api_bp.route('/sources/<int:source_id>', methods=['PATCH'])
def update_source(source_id):
    source = Source.query.get_or_404(source_id)
    data = request.get_json()
    if 'is_enabled' in data:
        source.is_enabled = bool(data['is_enabled'])
    if 'scrape_interval' in data:
        source.scrape_interval = int(data['scrape_interval'])
    if 'chnum_start' in data:
        val = data['chnum_start']
        if val is None or val == '':
            source.chnum_start = None
        else:
            try:
                n = int(val)
                source.chnum_start = n if n > 0 else None
            except (ValueError, TypeError):
                return jsonify({'error': 'chnum_start must be a positive integer'}), 422
    if 'epg_only' in data:
        source.epg_only = bool(data['epg_only'])
    db.session.commit()
    return jsonify(source.to_dict())


@api_bp.route('/sources/<int:source_id>/channels', methods=['DELETE'])
def delete_source_channels(source_id):
    """Delete all channels (and their programs via cascade) for a source."""
    source = Source.query.get_or_404(source_id)
    deleted = source.channels.delete()
    db.session.commit()
    return jsonify({'status': 'deleted', 'source': source.name, 'deleted': deleted})


@api_bp.route('/sources/<int:source_id>/config', methods=['GET'])
def get_source_config(source_id):
    source      = Source.query.get_or_404(source_id)
    scraper_cls = registry.get(source.name)
    schema      = [f.to_dict() for f in (scraper_cls.config_schema if scraper_cls else [])]
    saved       = source.config or {}
    secret_keys = {f['key'] for f in schema if f['secret']}
    values = {}
    for f in schema:
        key = f['key']
        if key in secret_keys and saved.get(key):
            values[key] = '••••••••'
        else:
            values[key] = saved.get(key, f['default'] or '')
    return jsonify({'schema': schema, 'values': values})


@api_bp.route('/sources/<int:source_id>/config', methods=['POST'])
def save_source_config(source_id):
    source      = Source.query.get_or_404(source_id)
    scraper_cls = registry.get(source.name)
    schema      = scraper_cls.config_schema if scraper_cls else []
    secret_keys = {f.key for f in schema if f.secret}
    data        = request.get_json() or {}
    current     = dict(source.config or {})
    for field in schema:
        key = field.key
        if key not in data:
            continue
        val = data[key]
        if key in secret_keys and val == '••••••••':
            continue
        if val == '' and not field.required:
            current.pop(key, None)
        else:
            current[key] = val
    source.config = current
    db.session.commit()
    return jsonify({'status': 'saved', 'source': source.name})


@api_bp.route('/channels')
def list_channels():
    page     = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    q        = Channel.query.join(Source)
    if s := request.args.get('source'):
        q = q.filter(Source.name == s)
    if c := request.args.get('category'):
        q = q.filter(Channel.category.ilike(f'%{c}%'))
    if search := request.args.get('search'):
        q = q.filter(Channel.name.ilike(f'%{search}%'))
    pag = q.order_by(Channel.name).paginate(page=page, per_page=per_page, error_out=False)
    return jsonify({
        'channels': [ch.to_dict() for ch in pag.items],
        'total': pag.total, 'page': page, 'pages': pag.pages,
    })


@api_bp.route('/channels/<int:channel_id>', methods=['PATCH'])
def update_channel(channel_id):
    ch   = Channel.query.get_or_404(channel_id)
    data = request.get_json()
    for field in ('name', 'logo_url', 'category', 'is_active', 'is_enabled', 'number', 'disable_reason'):
        if field in data:
            setattr(ch, field, data[field])
    if 'gracenote_id' in data:
        raw = (data['gracenote_id'] or '').strip()
        if raw == '' or _GRACENOTE_RE.match(raw):
            ch.gracenote_id = raw or None
        else:
            return jsonify({'error': 'Invalid Gracenote ID — must be numeric (e.g. 122912) or start with EP/SH/MV/SP/TR (e.g. EP012345678)'}), 422
    db.session.commit()
    return jsonify(ch.to_dict())


@api_bp.route('/channels/<int:channel_id>/inspect', methods=['POST'])
def inspect_channel(channel_id):
    """
    Single-channel inspector: resolve the stream URL directly, parse the HLS manifest,
    check for DRM/VOD, then pull one segment to confirm video data is flowing.
    Returns: { status, detail, segment_bytes }
      status: 'live' | 'drm' | 'dead' | 'vod' | 'no_data' | 'error'
    """
    _DRM_METHODS = ('SAMPLE-AES',)

    ch     = Channel.query.get_or_404(channel_id)
    source = ch.source

    if len(ch.source_channel_id) > 128 or '/' in ch.source_channel_id:
        return jsonify({'status': 'error', 'detail': 'Malformed channel ID'})

    # Resolve the stream URL directly — avoids a self-referential HTTP request to the
    # gunicorn server itself, which can deadlock all workers under concurrent inspect calls.
    scraper_cls = registry.get(source.name)
    if scraper_cls:
        scraper = scraper_cls(config=source.config or {})
        try:
            resolved_url = scraper.resolve(ch.stream_url)
        except StreamDeadError as e:
            return jsonify({'status': 'dead', 'detail': str(e)})
        except Exception as e:
            return jsonify({'status': 'error', 'detail': f'URL resolve failed: {e}'})
        finally:
            if scraper._pending_config_updates:
                try:
                    updated = dict(source.config or {})
                    updated.update(scraper._pending_config_updates)
                    source.config = updated
                    db.session.commit()
                except Exception:
                    db.session.rollback()
        sess = scraper.session
    else:
        resolved_url = ch.stream_url
        sess = _req.Session()
        sess.headers['User-Agent'] = 'FastChannels-Inspector/1.0'

    if not resolved_url:
        return jsonify({'status': 'error', 'detail': 'No stream URL'})

    try:
        r = sess.get(resolved_url, timeout=15, allow_redirects=True)

        if r.status_code in (404, 410):
            return jsonify({'status': 'dead', 'detail': f'HTTP {r.status_code} — stream not found'})

        if r.status_code in (403, 429, 451, 503):
            return jsonify({'status': 'error', 'detail': f'HTTP {r.status_code} — blocked or restricted'})

        if r.status_code != 200:
            return jsonify({'status': 'error', 'detail': f'HTTP {r.status_code}'})

        manifest_text = r.text
        manifest_url  = r.url

        # ── DASH/MPD manifest ─────────────────────────────────────────────
        is_mpd = ('<MPD ' in manifest_text or manifest_text.lstrip().startswith('<?xml')
                  and '<MPD' in manifest_text)
        if is_mpd:
            # VOD check
            if 'type="static"' in manifest_text:
                return jsonify({'status': 'vod', 'detail': 'DASH VOD stream — not a live channel'})
            # DRM check (Widevine / PlayReady)
            widevine_uuid = 'edef8ba9-79d6-4ace-a3c8-27dcd51d21ed'
            playready_uuid = '9a04f079-9840-4286-ab92-e65be0885f95'
            if widevine_uuid in manifest_text.lower() or playready_uuid in manifest_text.lower():
                return jsonify({'status': 'drm', 'detail': 'DASH DRM detected (Widevine/PlayReady)'})
            return jsonify({'status': 'live', 'detail': 'DASH manifest OK (live)'})

        # Master playlist → drill into first variant to get media playlist
        if '#EXT-X-STREAM-INF' in manifest_text:
            for line in manifest_text.splitlines():
                line = line.strip()
                if line and not line.startswith('#'):
                    variant_url = _urljoin(manifest_url, line)
                    try:
                        rv = sess.get(variant_url, timeout=10)
                        if rv.status_code == 200:
                            manifest_text = rv.text
                            manifest_url  = rv.url
                    except Exception:
                        pass
                    break

        if 'EXT-X-PLAYLIST-TYPE:VOD' in manifest_text:
            return jsonify({'status': 'vod', 'detail': 'VOD stream — not a live channel'})

        drm_method = next((m for m in _DRM_METHODS if f'METHOD={m}' in manifest_text), None)
        if drm_method:
            return jsonify({'status': 'drm', 'detail': f'DRM encryption detected ({drm_method})'})

        # Find the first media segment and try to pull a chunk to confirm data flows
        segment_url = None
        for line in manifest_text.splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                segment_url = _urljoin(manifest_url, line)
                break

        if not segment_url:
            return jsonify({'status': 'live', 'detail': 'Manifest OK (no segments listed yet)'})

        try:
            rs = sess.get(segment_url, timeout=10, stream=True)
            if rs.status_code != 200:
                return jsonify({'status': 'no_data',
                                'detail': f'Manifest OK but segment returned HTTP {rs.status_code}'})
            chunk = next(rs.iter_content(8192), None)
            rs.close()
            seg_bytes = len(chunk) if chunk else 0
            if seg_bytes == 0:
                return jsonify({'status': 'no_data', 'detail': 'Segment returned 0 bytes'})
            return jsonify({'status': 'live',
                            'detail': f'Stream OK — {seg_bytes} bytes received from segment',
                            'segment_bytes': seg_bytes})
        except Exception as e:
            return jsonify({'status': 'error', 'detail': f'Segment fetch failed: {e}'})

    except Exception as e:
        return jsonify({'status': 'error', 'detail': str(e)})


@api_bp.route('/logs')
def get_logs():
    n = request.args.get('n', 2500, type=int)
    lines = logfile.tail(n)
    return jsonify({'lines': lines})


@api_bp.route('/stats')
def stats():
    q = Channel.query.join(Source).filter(
        Channel.is_active == True,
        Channel.is_enabled == True,
        Source.is_enabled == True,
    )
    if sources := request.args.getlist('source'):
        q = q.filter(Source.name.in_(sources))
    if categories := request.args.getlist('category'):
        q = q.filter(Channel.category.in_(categories))
    if languages := request.args.getlist('language'):
        q = q.filter(Channel.language.in_(languages))
    cat_rows = db.session.query(Channel.category, db.func.count(Channel.id))\
        .filter(Channel.is_active == True).group_by(Channel.category)\
        .order_by(db.func.count(Channel.id).desc()).all()
    return jsonify({
        'total_channels': q.count(),
        'total_sources':  Source.query.filter_by(is_enabled=True).count(),
        'categories':     [{'name': c or 'Uncategorized', 'count': n} for c, n in cat_rows],
    })


@api_bp.route('/channels/duplicate-summary', methods=['GET'])
def duplicate_summary():
    """Return per-source stats for channels involved in duplicates, sorted by gracenote coverage."""
    from collections import defaultdict

    dup_names_sq = db.session.query(Channel.name)\
        .group_by(Channel.name)\
        .having(db.func.count(Channel.id) > 1)\
        .subquery()

    dup_channels = Channel.query.join(Source)\
        .filter(Channel.name.in_(dup_names_sq))\
        .all()

    if not dup_channels:
        return jsonify({'sources': [], 'total_groups': 0, 'total_affected': 0})

    # Count unique name groups
    unique_names = {ch.name for ch in dup_channels}

    stats = defaultdict(lambda: {'display_name': '', 'total': 0, 'with_gn': 0, 'epg_only': False})
    for ch in dup_channels:
        s = stats[ch.source.name]
        s['display_name'] = ch.source.display_name
        s['epg_only'] = ch.source.epg_only
        s['total'] += 1
        if ch.gracenote_id:
            s['with_gn'] += 1

    sources = []
    for name, s in stats.items():
        pct = round(100 * s['with_gn'] / s['total']) if s['total'] else 0
        sources.append({
            'name':         name,
            'display_name': s['display_name'],
            'dup_count':    s['total'],
            'gn_pct':       pct,
            'epg_only':     s['epg_only'],
        })

    # EPG-only sources always rank last; within each tier sort by gracenote coverage descending
    sources.sort(key=lambda x: (1 if x['epg_only'] else 0, -x['gn_pct']))

    return jsonify({
        'sources':       sources,
        'total_groups':  len(unique_names),
        'total_affected': len(dup_channels),
    })


@api_bp.route('/channels/resolve-duplicates', methods=['POST'])
def resolve_duplicates():
    """Disable duplicate channels, keeping one winner per name group based on source priority."""
    from collections import defaultdict

    data = request.get_json(force=True) or {}
    priority = data.get('source_priority', [])  # ordered list of source names, index 0 = highest

    dup_names_sq = db.session.query(Channel.name)\
        .group_by(Channel.name)\
        .having(db.func.count(Channel.id) > 1)\
        .subquery()

    dup_channels = Channel.query.join(Source)\
        .filter(Channel.name.in_(dup_names_sq))\
        .all()

    groups = defaultdict(list)
    for ch in dup_channels:
        groups[ch.name].append(ch)

    def priority_key(ch):
        try:
            return priority.index(ch.source.name)
        except ValueError:
            return len(priority)  # unlisted sources rank last

    disabled_count = 0
    for name, channels in groups.items():
        channels.sort(key=priority_key)
        for ch in channels[1:]:
            if ch.is_enabled:
                ch.is_enabled = False
                disabled_count += 1

    db.session.commit()
    return jsonify({'disabled': disabled_count, 'groups_resolved': len(groups)})


@api_bp.route('/feeds/<int:feed_id>/push-to-dvr', methods=['POST'])
def push_feed_to_dvr(feed_id):
    """Register or update this feed as a custom M3U source in Channels DVR."""
    import re as _re
    feed = Feed.query.get_or_404(feed_id)
    settings = AppSettings.get()

    dvr_url = (settings.channels_dvr_url or '').strip()
    if not dvr_url:
        return jsonify({'error': 'Channels DVR URL is not configured in Settings.'}), 400

    # Build absolute URLs for this feed pointing back at FastChannels
    base = request.host_url.rstrip('/')
    m3u_url  = f"{base}/feeds/{feed.slug}/m3u/gracenote"
    epg_url  = f"{base}/feeds/{feed.slug}/epg.xml"

    # Channels DVR requires alphanumeric-only name in the URL path
    display_name  = f"FastChannels – {feed.name}"
    safe_name     = _re.sub(r'[^a-zA-Z0-9]', '', display_name)
    dvr_endpoint  = f"{dvr_url}/providers/m3u/sources/{safe_name}"

    payload = {
        'name':         display_name,
        'type':         'HLS',
        'source':       '',
        'url':          m3u_url,
        'text':         '',
        'refresh':      '24',
        'limit':        '',
        'xmltv_url':    epg_url,
        'xmltv_refresh': '3600',
    }

    try:
        resp = _req.put(dvr_endpoint, json=payload, timeout=8)
        resp.raise_for_status()
    except _req.exceptions.ConnectionError:
        return jsonify({'error': f'Could not connect to Channels DVR at {dvr_url}'}), 502
    except _req.exceptions.Timeout:
        return jsonify({'error': 'Channels DVR timed out.'}), 504
    except _req.exceptions.HTTPError as e:
        return jsonify({'error': f'Channels DVR returned {resp.status_code}: {resp.text}'}), 502

    return jsonify({'ok': True, 'dvr_source': display_name, 'm3u_url': m3u_url, 'epg_url': epg_url})


@api_bp.route('/settings', methods=['GET', 'POST'])
def app_settings():
    row = AppSettings.get()
    if request.method == 'POST':
        data = request.get_json(force=True) or {}
        if 'global_chnum_start' in data:
            val = data['global_chnum_start']
            row.global_chnum_start = int(val) if val is not None else None
        if 'channels_dvr_url' in data:
            val = (data['channels_dvr_url'] or '').strip().rstrip('/')
            row.channels_dvr_url = val or None
        db.session.commit()
    return jsonify({
        'global_chnum_start': row.global_chnum_start,
        'channels_dvr_url':   row.channels_dvr_url,
    })

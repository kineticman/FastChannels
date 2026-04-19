import json
import os as _os
import re
import time as _time
import requests as _req
from datetime import datetime, timezone

_APP_START = _time.time()
from urllib.parse import urljoin as _urljoin, urlsplit
from flask import Blueprint, jsonify, request, current_app
from types import SimpleNamespace
from sqlalchemy import and_, not_, or_, select
from sqlalchemy.exc import OperationalError
from app.config_store import persist_source_config_updates
from app.config import VERSION
from ..extensions import db
from ..models import Source, Channel, Program, AppSettings, Feed
from ..scrapers import registry
from ..scrapers.base import StreamDeadError
from ..gracenote_suggest import SuggestionChannel, suggest_gracenote_matches
from ..gracenote_map import lookup_gracenote, fetch_remote_gracenote_map, remote_map_status
from ..hls import inspect_hls_drm, parse_stream_info as _parse_stream_info
from ..url import public_base_url
from .tasks import (
    trigger_bulk_channel_update,
    trigger_scrape,
    trigger_source_channel_purge,
    trigger_stream_audit,
    trigger_stream_audit_recheck,
    trigger_xml_refresh,
)
from ..generators.m3u import (
    feed_to_query_filters,
    get_global_chnum_overlaps,
)
from .. import logfile
from ..timezone_utils import normalize_timezone_name, write_timezone_cache
from ..xml_cache import (
    get_artifact,
    get_xml_artifact,
    invalidate_xml_cache,
)
from .admin import _apply_admin_feed_membership_filters, _duplicate_name_sets, _feed_split_counts

api_bp = Blueprint('api', __name__)

# Simple in-process cache so repeated city searches don't re-bootstrap every time.
_localnow_city_scraper: dict = {}  # {'scraper': LocalNowScraper, 'expires': float}
_GRACENOTE_RE = re.compile(r'^(\d+|(EP|SH|MV|SP|TR)\d+)$', re.I)
_GRACENOTE_MODES = {'auto', 'manual', 'off'}


def _apply_gracenote_update(channel: Channel, raw_value, raw_mode=None) -> str | None:
    mode = (raw_mode if raw_mode is not None else getattr(channel, 'gracenote_mode', None) or ('manual' if getattr(channel, 'gracenote_locked', False) else 'auto'))
    mode = str(mode).strip().lower()
    if mode not in _GRACENOTE_MODES:
        raise ValueError('Invalid Gracenote mode.')

    raw = (raw_value or '').strip()
    if raw and not _GRACENOTE_RE.match(raw):
        raise ValueError('Invalid Gracenote ID — must be numeric (e.g. 122912) or start with EP/SH/MV/SP/TR (e.g. EP012345678)')

    if mode == 'off':
        channel.gracenote_id = None
        channel.gracenote_mode = 'off'
        channel.gracenote_locked = False
        return None

    if mode == 'manual':
        if not raw:
            raise ValueError('Manual Gracenote mode requires an ID.')
        channel.gracenote_id = raw
        channel.gracenote_mode = 'manual'
        channel.gracenote_locked = True
        return raw

    channel.gracenote_id = raw or None
    channel.gracenote_mode = 'auto'
    channel.gracenote_locked = False
    return channel.gracenote_id


def _manual_gracenote_clause():
    return or_(
        Channel.gracenote_mode == 'manual',
        and_(
            Channel.gracenote_mode == None,
            Channel.gracenote_locked == True,
            Channel.gracenote_id != None,
            Channel.gracenote_id != '',
        ),
    )


def _apply_channel_filters(q, filters: dict | None = None):
    filters = filters or {}

    if feed_slug := filters.get('feed'):
        feed = Feed.query.filter_by(slug=feed_slug).first()
        if feed:
            q = _apply_admin_feed_membership_filters(q, feed)
    if src := filters.get('source'):
        q = q.filter(Source.name == src)
    if cat := filters.get('category'):
        q = q.filter(Channel.category == cat)
    if lang := filters.get('language'):
        q = q.filter(Channel.language == lang)
    if search := filters.get('search'):
        q = q.filter(Channel.name.ilike(f'%{search}%'))
    if drm := filters.get('drm'):
        if drm == '1':
            q = q.filter(Channel.disable_reason.like('DRM%'))
        elif drm == 'dead':
            q = q.filter(Channel.disable_reason == 'Dead')
        elif drm == '0':
            q = q.filter(Channel.disable_reason == None)
    if ef := filters.get('enabled'):
        if ef in ('1', 'enabled'):
            q = q.filter(Channel.is_enabled == True)
        elif ef in ('0', 'disabled'):
            q = q.filter(Channel.is_enabled == False)
    if pf := filters.get('presence'):
        if pf == 'inactive':
            q = q.filter(Channel.is_active == False)
        elif pf == 'enabled_inactive':
            q = q.filter(Channel.is_enabled == True, Channel.is_active == False)
        elif pf == 'missed':
            q = q.filter(Channel.missed_scrapes >= 1)
        elif pf == 'active':
            q = q.filter(Channel.is_active == True)
    if gf := filters.get('gracenote'):
        if gf in ('1', 'has_id'):
            q = q.filter(Channel.gracenote_id != None, Channel.gracenote_id != '')
        elif gf in ('0', 'missing_id'):
            q = q.filter((Channel.gracenote_id == None) | (Channel.gracenote_id == ''))
    if gm := filters.get('gracenote_mode'):
        manual_mode = _manual_gracenote_clause()
        off_mode = Channel.gracenote_mode == 'off'
        if gm == 'manual':
            q = q.filter(manual_mode)
        elif gm == 'off':
            q = q.filter(off_mode)
        elif gm == 'auto':
            q = q.filter(not_(or_(manual_mode, off_mode)))
    if filters.get('duplicates') in ('1', 'unique'):
        exact_duplicate_names, possible_duplicate_names, gn_dup_ids = _duplicate_name_sets()
        all_duplicate_names = exact_duplicate_names | possible_duplicate_names
        if filters['duplicates'] == '1':
            q = q.filter(or_(Channel.name.in_(sorted(all_duplicate_names)), Channel.id.in_(gn_dup_ids), Channel.is_duplicate == True))
        else:
            q = q.filter(Channel.name.notin_(sorted(all_duplicate_names)), Channel.id.notin_(gn_dup_ids), Channel.is_duplicate == False)
    return q


def _scrape_interval_limits(source_name: str) -> tuple[int, int, int]:
    scraper_cls = registry.get(source_name)
    recommended = getattr(scraper_cls, 'scrape_interval', 360) if scraper_cls else 360
    minimum = getattr(scraper_cls, 'min_scrape_interval', 30) if scraper_cls else 30
    maximum = getattr(scraper_cls, 'max_scrape_interval', 10080) if scraper_cls else 10080
    return int(recommended), int(minimum), int(maximum)


def _parse_hls_variants(master_text: str) -> list[dict]:
    """Parse #EXT-X-STREAM-INF variant entries from an HLS master playlist."""
    _CODEC_NAMES = {
        'avc1': 'H.264', 'avc3': 'H.264',
        'hvc1': 'H.265', 'hev1': 'H.265',
        'mp4a': 'AAC',
        'ac-3': 'AC-3', 'ec-3': 'E-AC-3',
        'vp09': 'VP9', 'av01': 'AV1',
    }

    def _friendly_codecs(raw: str) -> str:
        seen, result = set(), []
        for part in raw.split(','):
            prefix = part.strip().split('.')[0].lower()
            name = _CODEC_NAMES.get(prefix, prefix)
            if name not in seen:
                seen.add(name)
                result.append(name)
        return '+'.join(result)

    variants = []
    lines = master_text.splitlines()
    for i, line in enumerate(lines):
        line = line.strip()
        if not line.startswith('#EXT-X-STREAM-INF:'):
            continue
        attrs = line[len('#EXT-X-STREAM-INF:'):]
        v = {}
        m = re.search(r'BANDWIDTH=(\d+)', attrs)
        if m:
            v['bandwidth'] = int(m.group(1))
        m = re.search(r'RESOLUTION=(\d+x\d+)', attrs, re.I)
        if m:
            v['resolution'] = m.group(1)
        m = re.search(r'CODECS="([^"]+)"', attrs)
        if m:
            v['codecs'] = _friendly_codecs(m.group(1))
        m = re.search(r'FRAME-RATE=([\d.]+)', attrs)
        if m:
            v['fps'] = round(float(m.group(1)), 3)
        variants.append(v)

    variants.sort(key=lambda v: v.get('bandwidth', 0), reverse=True)
    return variants

_CHANNELS_DVR_RECOMMENDED_MAX = 750


def _invalidate_and_refresh_xml() -> None:
    invalidate_xml_cache()
    trigger_xml_refresh()


def _isoformat_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def _ensure_feed_dvr_artifacts(feed: Feed, base_url: str, *, has_gracenote: bool) -> None:
    """Wait briefly for feed artifacts to exist before handing URLs to Channels DVR."""
    def _ready() -> bool:
        xml_path, _ = get_xml_artifact(f'feed-{feed.slug}')
        if get_artifact(f'feed-{feed.slug}-m3u', ext='m3u') is None:
            return False
        if xml_path is None:
            return False
        if has_gracenote and get_artifact(f'feed-{feed.slug}-gracenote-m3u', ext='m3u') is None:
            return False
        return True

    if _ready():
        return

    trigger_xml_refresh()
    deadline = _time.time() + 20
    while _time.time() < deadline:
        if _ready():
            return
        _time.sleep(0.2)
    raise TimeoutError(f'timed out waiting for feed artifacts: {feed.slug}')


def _channel_query_summary(query, parse_gracenote) -> tuple[int, bool]:
    """Return count and whether any channel in the query has a valid Gracenote ID."""
    base_query = query.order_by(None)
    count = base_query.count()
    if count == 0:
        return 0, False

    candidates = (
        base_query.with_entities(Channel.gracenote_id, Channel.slug)
        .filter(
            or_(
                (Channel.gracenote_id != None) & (Channel.gracenote_id != ''),
                Channel.slug.like('%|%'),
            )
        )
        .limit(256)
        .all()
    )
    has_gracenote = any(
        parse_gracenote(SimpleNamespace(gracenote_id=row.gracenote_id, slug=row.slug))
        for row in candidates
    )
    return count, has_gracenote


def _read_int(path: str) -> int | None:
    try:
        with open(path, 'r', encoding='utf-8') as fp:
            raw = fp.read().strip()
    except OSError:
        return None
    if not raw or raw == 'max':
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _memory_stats() -> dict:
    # Container/cgroup memory (works for Docker and most modern runtimes).
    cgroup_current = (
        _read_int('/sys/fs/cgroup/memory.current')
        or _read_int('/sys/fs/cgroup/memory/memory.usage_in_bytes')
    )
    cgroup_limit = (
        _read_int('/sys/fs/cgroup/memory.max')
        or _read_int('/sys/fs/cgroup/memory/memory.limit_in_bytes')
    )

    rss_bytes = None
    vm_size_bytes = None
    swap_bytes = None
    try:
        with open('/proc/self/status', 'r', encoding='utf-8') as fp:
            for line in fp:
                if line.startswith('VmRSS:'):
                    rss_bytes = int(line.split()[1]) * 1024
                elif line.startswith('VmSize:'):
                    vm_size_bytes = int(line.split()[1]) * 1024
                elif line.startswith('VmSwap:'):
                    swap_bytes = int(line.split()[1]) * 1024
    except OSError:
        pass

    mem_available = None
    mem_total = None
    anon_bytes = None
    file_bytes = None
    try:
        with open('/proc/meminfo', 'r', encoding='utf-8') as fp:
            for line in fp:
                if line.startswith('MemAvailable:'):
                    mem_available = int(line.split()[1]) * 1024
                elif line.startswith('MemTotal:'):
                    mem_total = int(line.split()[1]) * 1024
    except OSError:
        pass

    for stat_path in ('/sys/fs/cgroup/memory.stat', '/sys/fs/cgroup/memory/memory.stat'):
        try:
            with open(stat_path, 'r', encoding='utf-8') as fp:
                for line in fp:
                    if line.startswith('anon '):
                        anon_bytes = int(line.split()[1])
                    elif line.startswith('file '):
                        file_bytes = int(line.split()[1])
            break
        except OSError:
            continue

    percent = None
    if cgroup_current and cgroup_limit and cgroup_limit > 0:
        percent = round((cgroup_current / cgroup_limit) * 100, 1)

    return {
        'container_bytes': cgroup_current,
        'container_limit_bytes': cgroup_limit,
        'container_percent': percent,
        'container_anon_bytes': anon_bytes,
        'container_file_cache_bytes': file_bytes,
        'process_rss_bytes': rss_bytes,
        'process_vmsize_bytes': vm_size_bytes,
        'process_swap_bytes': swap_bytes,
        'host_mem_available_bytes': mem_available,
        'host_mem_total_bytes': mem_total,
    }


def _cpu_stats() -> dict:
    loadavg = None
    try:
        with open('/proc/loadavg', 'r', encoding='utf-8') as fp:
            parts = fp.read().strip().split()
        if len(parts) >= 3:
            loadavg = [float(parts[0]), float(parts[1]), float(parts[2])]
    except (OSError, ValueError):
        pass

    cpu_count = _os.cpu_count()

    proc_cpu_seconds = None
    try:
        clk_tck = _os.sysconf(_os.sysconf_names['SC_CLK_TCK'])
        with open('/proc/self/stat', 'r', encoding='utf-8') as fp:
            parts = fp.read().split()
        if len(parts) >= 15:
            utime = int(parts[13])
            stime = int(parts[14])
            proc_cpu_seconds = round((utime + stime) / clk_tck, 2)
    except (OSError, ValueError, KeyError):
        pass

    return {
        'loadavg': loadavg,
        'cpu_count': cpu_count,
        'process_cpu_seconds': proc_cpu_seconds,
    }


def _process_stats() -> dict:
    def _proc_fields(pid: int) -> dict | None:
        status_path = f'/proc/{pid}/status'
        try:
            fields = {}
            with open(status_path, 'r', encoding='utf-8') as fp:
                for line in fp:
                    if line.startswith('PPid:'):
                        fields['ppid'] = int(line.split()[1])
                    elif line.startswith('VmRSS:'):
                        fields['rss_bytes'] = int(line.split()[1]) * 1024
            return fields
        except (OSError, ValueError):
            return None

    master_pid = _os.getppid()
    web_worker_rss = []
    bg_worker_rss = []

    for entry in _os.listdir('/proc'):
        if not entry.isdigit():
            continue
        pid = int(entry)
        try:
            with open(f'/proc/{pid}/cmdline', 'rb') as fp:
                cmdline = fp.read().replace(b'\x00', b' ').decode('utf-8', errors='ignore').strip()
        except OSError:
            continue
        if not cmdline:
            continue

        fields = _proc_fields(pid)
        if not fields or fields.get('rss_bytes') is None:
            continue

        if 'gunicorn' in cmdline and 'app:create_app()' in cmdline and fields.get('ppid') == master_pid:
            web_worker_rss.append(fields['rss_bytes'])
        elif 'python -m app.worker' in cmdline:
            bg_worker_rss.append(fields['rss_bytes'])

    web_avg = int(sum(web_worker_rss) / len(web_worker_rss)) if web_worker_rss else None
    bg_avg = int(sum(bg_worker_rss) / len(bg_worker_rss)) if bg_worker_rss else None

    return {
        'web_worker_count': len(web_worker_rss),
        'web_worker_rss_avg_bytes': web_avg,
        'background_worker_count': len(bg_worker_rss),
        'background_worker_rss_avg_bytes': bg_avg,
    }


def _normalize_server_url(value: str | None, default_port: int = 5523) -> str | None:
    raw = (value or '').strip()
    if not raw:
        return None

    if '://' not in raw:
        raw = f'http://{raw}'

    parsed = urlsplit(raw)
    scheme = parsed.scheme or 'http'
    netloc = parsed.netloc or parsed.path
    path = parsed.path if parsed.netloc else ''
    host = netloc.strip()

    if not host:
        return None

    if path not in ('', '/'):
        host = f'{host}{path}'

    if ':' not in host.rsplit(']', 1)[-1]:
        host = f'{host}:{default_port}'

    return f'{scheme}://{host}'.rstrip('/')




@api_bp.route('/sources')
def list_sources():
    return jsonify([s.to_dict() for s in Source.query.order_by(Source.display_name).all()])


@api_bp.route('/sources/<int:source_id>/run', methods=['POST'])
def run_source(source_id):
    source = Source.query.get_or_404(source_id)
    trigger_scrape(source.name, force_full=True)
    return jsonify({'status': 'queued', 'source': source.name})


@api_bp.route('/sources/force-refresh', methods=['POST'])
def force_refresh_sources():
    enabled_sources = Source.query.filter_by(is_enabled=True).order_by(Source.display_name).all()
    queued = []
    for source in enabled_sources:
        source.last_scraped_at = None
        source.last_error = None
        queued.append(source.name)
    db.session.commit()
    for source_name in queued:
        trigger_scrape(source_name)
    return jsonify({
        'status': 'queued',
        'count': len(queued),
        'sources': queued,
    })


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
    last_scraped_ms = int(source.last_scraped_at.timestamp() * 1000) if source.last_scraped_at else 0
    return jsonify({'status': 'idle', 'last_scraped_ms': last_scraped_ms, 'last_error': source.last_error})


@api_bp.route('/sources/<int:source_id>/stream-audit', methods=['POST'])
def stream_audit_source(source_id):
    source = Source.query.get_or_404(source_id)
    data = request.get_json() or {}
    include_inactive = bool(data.get('include_inactive', False))
    trigger_stream_audit(source.name, include_inactive=include_inactive)
    return jsonify({'status': 'queued', 'source': source.name})


@api_bp.route('/sources/<int:source_id>/stream-audit-recheck', methods=['POST'])
def stream_audit_recheck(source_id):
    source = Source.query.get_or_404(source_id)
    data = request.get_json() or {}
    channel_ids = [int(i) for i in (data.get('channel_ids') or []) if str(i).isdigit()]
    if not channel_ids:
        return jsonify({'error': 'No channel_ids provided'}), 400
    trigger_stream_audit_recheck(source.name, channel_ids)
    return jsonify({'status': 'queued', 'count': len(channel_ids)})


def _orphan_cutoff(days: int = 7):
    from datetime import datetime, timezone, timedelta
    return datetime.now(timezone.utc) - timedelta(days=days)


def _source_active_geos(source) -> set | None:
    """Return the configured geo codes for multi-region sources, or None."""
    scraper_cls = registry.get(source.name)
    if not scraper_cls or not hasattr(scraper_cls, '_geos'):
        return None
    scraper = scraper_cls(config=source.config or {})
    return {g.upper() for g in scraper._geos()}


def _orphan_query(source, days: int = 7):
    """
    Inactive channels eligible for deletion:
    - not DRM-disabled
    - either not seen in `days` days, OR their region is no longer configured
    """
    cutoff = _orphan_cutoff(days)
    base = Channel.query.filter(
        Channel.source_id == source.id,
        Channel.is_active == False,
        db.or_(Channel.disable_reason == None, ~Channel.disable_reason.like('DRM%')),
    )
    time_filter = db.or_(
        Channel.last_seen_at == None,
        Channel.last_seen_at < cutoff,
    )
    active_geos = _source_active_geos(source)
    if active_geos:
        # Also catch inactive channels from regions that are no longer configured,
        # even if last_seen_at is recent (e.g. user just unchecked that region).
        region_filter = ~Channel.country.in_(active_geos)
        return base.filter(db.or_(time_filter, region_filter))
    return base.filter(time_filter)


@api_bp.route('/sources/<int:source_id>/inactive-count')
def inactive_channel_count(source_id):
    source = Source.query.get_or_404(source_id)
    days = int(request.args.get('days', 7))
    count = _orphan_query(source, days).count()
    return jsonify({'count': count, 'source': source.name, 'days': days})


@api_bp.route('/sources/<int:source_id>/delete-inactive', methods=['POST'])
def delete_inactive_channels(source_id):
    source = Source.query.get_or_404(source_id)
    days = int((request.get_json() or {}).get('days', 7))
    orphans = _orphan_query(source, days).all()
    count = len(orphans)
    for ch in orphans:
        db.session.delete(ch)
    db.session.commit()
    return jsonify({'deleted': count, 'source': source.name})


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
            # Stale check — treat as dead if no heartbeat for 90s
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
    last_result = (source.config or {}).get('last_audit_result')
    last_report = (source.config or {}).get('last_audit_report')
    return jsonify({'status': 'idle', 'last_result': last_result, 'last_report': last_report})


@api_bp.route('/sources/chnum-overlaps')
def chnum_overlaps():
    """Return a list of channel-number overlap warnings across all M3U outputs."""
    return jsonify({'warnings': get_global_chnum_overlaps()})


@api_bp.route('/sources/<int:source_id>', methods=['PATCH'])
def update_source(source_id):
    source = Source.query.get_or_404(source_id)
    data = request.get_json()
    changed = False
    if 'is_enabled' in data:
        new_enabled = bool(data['is_enabled'])
        should_purge = not new_enabled and source.is_enabled
        source.is_enabled = new_enabled
        changed = True
    else:
        should_purge = False
    if 'scrape_interval' in data:
        try:
            interval = int(data['scrape_interval'])
        except (TypeError, ValueError):
            return jsonify({'error': 'scrape_interval must be an integer number of minutes'}), 422
        recommended, minimum, maximum = _scrape_interval_limits(source.name)
        if interval < minimum or interval > maximum:
            return jsonify({
                'error': f'scrape_interval must be between {minimum} and {maximum} minutes for {source.display_name}',
                'recommended': recommended,
                'min': minimum,
                'max': maximum,
            }), 422
        source.scrape_interval = interval
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
        changed = True
    if 'epg_only' in data:
        source.epg_only = bool(data['epg_only'])
        changed = True
    if changed:
        baseline_warnings = set(get_global_chnum_overlaps())
        db.session.flush()
        new_warnings = [w for w in get_global_chnum_overlaps() if w not in baseline_warnings]
        if new_warnings:
            db.session.rollback()
            return jsonify({'error': 'Channel number overlaps detected', 'warnings': new_warnings}), 409
    db.session.commit()
    _invalidate_and_refresh_xml()
    if should_purge:
        trigger_source_channel_purge(source.id)
    return jsonify(source.to_dict())


@api_bp.route('/sources/<int:source_id>/channels', methods=['DELETE'])
def delete_source_channels(source_id):
    """Delete all channels (and their programs via cascade) for a source."""
    source = Source.query.get_or_404(source_id)
    matched = source.channels.count()
    trigger_source_channel_purge(source.id)
    return jsonify({'status': 'queued', 'source': source.name, 'matched': matched})


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
    if request.args.get('feed_eligible') in ('1', 'true', 'yes'):
        q = q.filter(
            Channel.is_active == True,
            Channel.is_enabled == True,
            Source.is_enabled == True,
            Source.epg_only == False,
            Channel.stream_url != None,
        )
    if s := request.args.get('source'):
        q = q.filter(Source.name == s)
    if c := request.args.get('category'):
        q = q.filter(Channel.category.ilike(f'%{c}%'))
    if search := request.args.get('search'):
        q = q.filter(Channel.name.ilike(f'%{search}%'))
    pag = q.order_by(Channel.name).paginate(page=page, per_page=per_page, error_out=False)
    if request.args.get('slim') in ('1', 'true'):
        items = [{'id': ch.id, 'name': ch.name, 'source_name': ch.source.name,
                  'category': ch.category, 'language': ch.language,
                  'country': ch.country, 'gracenote_id': ch.gracenote_id}
                 for ch in pag.items]
    else:
        items = [ch.to_dict() for ch in pag.items]
    return jsonify({
        'channels': items,
        'total': pag.total, 'page': page, 'pages': pag.pages,
    })


@api_bp.route('/channels/bulk', methods=['POST'])
def bulk_update_channels():
    data    = request.get_json() or {}
    action  = data.get('action')
    filters = data.get('filters') or {}

    if action not in ('enable', 'disable'):
        return jsonify({'error': 'action must be enable or disable'}), 400

    enable = action == 'enable'
    q = _apply_channel_filters(Channel.query.join(Source), filters)

    matched = q.count()
    if matched:
        trigger_bulk_channel_update(filters, enable)
    return jsonify({'status': 'queued' if matched else 'idle', 'updated': matched})


@api_bp.route('/channels/gracenote-bulk', methods=['POST'])
def bulk_update_channel_gracenote():
    data = request.get_json(force=True) or {}
    action = (data.get('action') or '').strip()
    ids = [int(v) for v in (data.get('ids') or []) if str(v).isdigit()]
    filters = data.get('filters') or {}

    if action not in ('set_auto', 'set_manual', 'set_off', 'clear_ids'):
        return jsonify({'error': 'Invalid action.'}), 400

    if ids:
        channels = Channel.query.filter(Channel.id.in_(ids)).all()
    else:
        channels = _apply_channel_filters(Channel.query.join(Source), filters).all()
    if not channels:
        return jsonify({'updated': 0})

    for ch in channels:
        current_id = (ch.gracenote_id or '').strip() or None
        current_mode = getattr(ch, 'gracenote_mode', None) or ('manual' if getattr(ch, 'gracenote_locked', False) and current_id else 'auto')
        if action == 'set_auto':
            _apply_gracenote_update(ch, current_id, 'auto')
        elif action == 'set_manual':
            # Lock whatever ID the channel already has; channels with no ID stay as-is (auto)
            if current_id:
                _apply_gracenote_update(ch, current_id, 'manual')
        elif action == 'set_off':
            _apply_gracenote_update(ch, None, 'off')
        elif action == 'clear_ids':
            _apply_gracenote_update(ch, None, 'off' if current_mode == 'off' else 'auto')

    db.session.commit()
    _invalidate_and_refresh_xml()
    return jsonify({'updated': len(channels)})


@api_bp.route('/channels/<int:channel_id>', methods=['PATCH'])
def update_channel(channel_id):
    ch   = Channel.query.get_or_404(channel_id)
    data = request.get_json()
    for field in ('name', 'logo_url', 'logo_url_pinned', 'category', 'category_override', 'language', 'language_override', 'is_active', 'is_enabled', 'number', 'number_pinned', 'disable_reason', 'is_duplicate'):
        if field in data:
            setattr(ch, field, data[field])
    # Setting a number without explicitly managing the pin auto-pins it.
    if 'number' in data and data['number'] is not None and 'number_pinned' not in data:
        ch.number_pinned = True
    if data.get('is_enabled') is True and 'is_active' not in data:
        ch.is_active = True
        if ch.disable_reason == 'Dead' or (ch.disable_reason or '').startswith('DRM'):
            ch.disable_reason = None
        ch.last_seen_at = datetime.now(timezone.utc)
        ch.missed_scrapes = 0
    if 'gracenote_id' in data or 'gracenote_mode' in data:
        try:
            _apply_gracenote_update(ch, data.get('gracenote_id'), data.get('gracenote_mode'))
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 422
    # Retry commit up to 3× (1s apart) if SQLite is briefly locked by a worker.
    # Do NOT rollback between attempts — in autocommit mode the failed flush leaves
    # the session dirty state intact, so a plain retry will re-attempt the write.
    for _attempt in range(3):
        try:
            db.session.commit()
            break
        except OperationalError as _oe:
            if 'database is locked' not in str(_oe) or _attempt == 2:
                raise
            _time.sleep(1)
    _invalidate_and_refresh_xml()
    return jsonify(ch.to_dict())


@api_bp.route('/channels/<int:channel_id>/category-explain', methods=['GET'])
def channel_category_explain(channel_id):
    from ..scrapers.category_utils import explain_category, CANONICAL_CATEGORIES, category_for_channel
    ch = Channel.query.get_or_404(channel_id)
    if ch.category_override:
        explanation = {
            'source': 'user_override',
            'rule': 'user_override',
            'detail': f'Manually set to "{ch.category_override}" by a user — overrides all automatic logic.',
        }
    else:
        # explain_category works on the auto-resolved category (before override)
        auto_cat = category_for_channel(ch.name, ch.category)
        explanation = explain_category(ch.name, auto_cat)
    return jsonify({
        'channel_id': ch.id,
        'channel_name': ch.name,
        'category': ch.category,
        'category_override': ch.category_override,
        'canonical_categories': list(CANONICAL_CATEGORIES),
        **explanation,
    })


@api_bp.route('/channels/<int:channel_id>/language-explain', methods=['GET'])
def channel_language_explain(channel_id):
    ch = Channel.query.get_or_404(channel_id)
    common_languages = [
        ('en', 'English'), ('es', 'Spanish'), ('fr', 'French'), ('de', 'German'),
        ('pt', 'Portuguese'), ('it', 'Italian'), ('zh', 'Chinese'), ('ja', 'Japanese'),
        ('ko', 'Korean'), ('ar', 'Arabic'), ('hi', 'Hindi'), ('ru', 'Russian'),
        ('pl', 'Polish'), ('nl', 'Dutch'), ('sv', 'Swedish'), ('tr', 'Turkish'),
    ]
    return jsonify({
        'channel_id': ch.id,
        'channel_name': ch.name,
        'language': ch.language or 'en',
        'language_override': ch.language_override,
        'common_languages': common_languages,
    })


@api_bp.route('/channels/<int:channel_id>/inspect', methods=['POST'])
def inspect_channel(channel_id):
    """
    Single-channel inspector: resolve the stream URL directly, parse the HLS manifest,
    check for DRM/VOD, then pull one segment to confirm video data is flowing.
    Returns: { status, detail, segment_bytes }
      status: 'live' | 'drm' | 'dead' | 'vod' | 'no_data' | 'error'
    """
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
                    persist_source_config_updates(
                        source.id,
                        scraper._pending_config_updates,
                    )
                except Exception:
                    db.session.rollback()
        sess = scraper.session
    else:
        resolved_url = ch.stream_url
        sess = _req.Session()
        sess.headers['User-Agent'] = 'FastChannels-Inspector/1.0'

    if not resolved_url:
        return jsonify({'status': 'error', 'detail': 'No stream URL'})

    # For session-based CDNs (e.g. Broadpeak) that return intermittent 404s
    # when fetched server-side, verify via the scraper's audit_resolve() instead.
    # audit_resolve() checks feed/catalogue presence and raises StreamDeadError
    # if the channel is genuinely gone.  Scrapers advertise which CDN hosts need
    # this treatment via a class-level `session_cdn_hosts` frozenset.
    if scraper_cls and hasattr(scraper_cls, 'audit_resolve'):
        from urllib.parse import urlsplit as _us
        _session_cdn = getattr(scraper_cls, 'session_cdn_hosts', frozenset())
        if resolved_url.startswith('http') and _us(resolved_url).netloc in _session_cdn:
            try:
                scraper.audit_resolve(ch.stream_url)
                return jsonify({'status': 'live', 'detail': 'Session-based CDN — verified via feed (client playback should work)'})
            except StreamDeadError as e:
                return jsonify({'status': 'dead', 'detail': str(e)})
            except Exception:
                pass  # fall through to normal manifest fetch

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

        # Master playlist → parse variant stats, persist stream_info, then drill into first variant
        variants = []
        if '#EXT-X-STREAM-INF' in manifest_text:
            variants = _parse_hls_variants(manifest_text)
            stream_info = _parse_stream_info(manifest_text)
            if stream_info:
                ch.stream_info = stream_info
                try:
                    db.session.commit()
                except Exception:
                    db.session.rollback()
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

        if '#EXT-X-PLAYLIST-TYPE:VOD' in manifest_text and '#EXT-X-ENDLIST' in manifest_text:
            return jsonify({'status': 'vod', 'detail': 'Finished VOD — not a live channel'})

        drm = inspect_hls_drm(manifest_text)
        if drm:
            detail = f"HLS DRM detected ({drm['drm_type']}"
            if drm.get('keyformat'):
                detail += f"; KEYFORMAT={drm['keyformat']}"
            detail += ')'
            return jsonify({'status': 'drm', 'detail': detail})

        # Find the first media segment and try to pull a chunk to confirm data flows
        segment_url = None
        for line in manifest_text.splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                segment_url = _urljoin(manifest_url, line)
                break

        if not segment_url:
            return jsonify({'status': 'live', 'detail': 'Manifest OK (no segments listed yet)',
                            'variants': variants})

        try:
            rs = sess.get(segment_url, timeout=10, stream=True)
            if rs.status_code != 200:
                return jsonify({'status': 'no_data',
                                'detail': f'Manifest OK but segment returned HTTP {rs.status_code}',
                                'variants': variants})
            chunk = next(rs.iter_content(8192), None)
            rs.close()
            seg_bytes = len(chunk) if chunk else 0
            if seg_bytes == 0:
                return jsonify({'status': 'no_data', 'detail': 'Segment returned 0 bytes',
                                'variants': variants})
            return jsonify({'status': 'live',
                            'detail': f'Stream OK — {seg_bytes} bytes received from segment',
                            'segment_bytes': seg_bytes,
                            'variants': variants})
        except Exception as e:
            return jsonify({'status': 'error', 'detail': f'Segment fetch failed: {e}'})

    except Exception as e:
        return jsonify({'status': 'error', 'detail': str(e)})


@api_bp.route('/channels/<int:channel_id>/preview', methods=['GET'])
def preview_channel(channel_id):
    ch = Channel.query.get_or_404(channel_id)
    now = datetime.now(timezone.utc)

    current_program = (
        Program.query
        .filter(
            Program.channel_id == ch.id,
            Program.start_time <= now,
            Program.end_time > now,
        )
        .order_by(Program.start_time.asc())
        .first()
    )
    next_program = (
        Program.query
        .filter(
            Program.channel_id == ch.id,
            Program.start_time >= now,
        )
        .order_by(Program.start_time.asc())
        .first()
    )

    if current_program and next_program and current_program.id == next_program.id:
        next_program = (
            Program.query
            .filter(
                Program.channel_id == ch.id,
                Program.start_time >= current_program.end_time,
            )
            .order_by(Program.start_time.asc())
            .first()
        )

    def _program_dict(p):
        if not p:
            return None
        return {
            'title': p.title,
            'description': p.description,
            'start_time': _isoformat_utc(p.start_time),
            'end_time': _isoformat_utc(p.end_time),
            'category': p.category,
            'episode_title': p.episode_title,
            'season': p.season,
            'episode': p.episode,
            'original_air_date': p.original_air_date.isoformat() if p.original_air_date else None,
        }

    play_url = None
    if (
        ch.stream_url
        and ch.source
        and not ch.source.epg_only
        and ch.source.name
        and ch.source_channel_id
    ):
        play_url = f'/play/{ch.source.name}/{ch.source_channel_id}.m3u8'

    return jsonify({
        'channel': {
            'id': ch.id,
            'name': ch.name,
            'source_name': ch.source.name if ch.source else None,
            'source_display_name': ch.source.display_name if ch.source else None,
            'source_channel_id': ch.source_channel_id,
            'category': ch.category,
            'language': ch.language,
            'country': ch.country,
            'tags': [t for t in (ch.tags or '').split(',') if t] if ch.tags else [],
            'logo_url': ch.logo_url,
            'disable_reason': ch.disable_reason,
            'is_active': ch.is_active,
            'is_enabled': ch.is_enabled,
            'description': ch.description,
        },
        'current_program': _program_dict(current_program),
        'next_program': _program_dict(next_program),
        'play_url': play_url,
    })


def _gracenote_source_for(ch) -> str | None:
    """Return the provenance of ch.gracenote_id: 'manual', 'csv', 'native', or None."""
    if not ch.gracenote_id:
        return None
    mode = getattr(ch, 'gracenote_mode', None)
    locked = getattr(ch, 'gracenote_locked', False)
    if mode == 'manual' or locked:
        return 'manual'
    source_name = ch.source.name if ch.source else ''
    csv_match = lookup_gracenote(source_name, ch.source_channel_id)
    if csv_match and csv_match.get('tmsid') == ch.gracenote_id:
        return 'csv'
    return 'native'


def _csv_suggestion_for(ch) -> dict | None:
    """Return the CSV mapping entry for this channel, if one exists."""
    source_name = ch.source.name if ch.source else ''
    match = lookup_gracenote(source_name, ch.source_channel_id)
    if not match:
        return None
    return {
        'tmsid': match.get('tmsid'),
        'notes': match.get('notes') or '',
    }


@api_bp.route('/channels/<int:channel_id>/gracenote-suggestions', methods=['GET'])
def channel_gracenote_suggestions(channel_id):
    ch = Channel.query.get_or_404(channel_id)
    settings = AppSettings.get()
    dvr_url = (settings.effective_channels_dvr_url() or '').strip()

    limit = max(1, min(request.args.get('limit', 10, type=int) or 10, 25))

    if dvr_url:
        try:
            data = suggest_gracenote_matches(
                dvr_url,
                channel=SuggestionChannel(
                    id=ch.id,
                    name=ch.name,
                    source_name=ch.source.name if ch.source else None,
                    country=ch.country,
                    language=ch.language,
                    category=ch.category,
                    gracenote_id=ch.gracenote_id,
                ),
                limit=limit,
            )
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 502
    else:
        data = {'results': [], 'dvr_missing': True}

    data['channel'] = {
        'id': ch.id,
        'name': ch.name,
        'source_name': ch.source.name if ch.source else None,
        'country': ch.country,
        'language': ch.language,
        'category': ch.category,
        'gracenote_id': ch.gracenote_id,
        'gracenote_source': _gracenote_source_for(ch),
        'csv_suggestion': _csv_suggestion_for(ch),
    }
    return jsonify(data)


@api_bp.route('/gracenote-search', methods=['GET'])
def gracenote_search():
    query = (request.args.get('q') or '').strip()
    if not query:
        return jsonify({'error': 'Missing q parameter.'}), 400

    settings = AppSettings.get()
    dvr_url = (settings.effective_channels_dvr_url() or '').strip()
    if not dvr_url:
        return jsonify({'error': 'Channels DVR URL is not configured.'}), 400

    limit = max(1, min(request.args.get('limit', 10, type=int) or 10, 25))
    try:
        return jsonify(suggest_gracenote_matches(dvr_url, query=query, limit=limit))
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 502


@api_bp.route('/stations/<station_id>/now-playing', methods=['GET'])
def station_now_playing(station_id):
    """
    Return now/next program for a Gracenote/TMS stationId via tvtv.us.
    Used by the Gracenote Suggestions helper to let users verify that a
    suggested stationId actually matches what their channel is broadcasting.
    """
    from ..tvtv_lookup import lookup_now_playing
    result = lookup_now_playing(str(station_id).strip())
    if not result.get('found') and result.get('error') == 'not_in_index':
        return jsonify(result), 404
    return jsonify(result)


@api_bp.route('/gracenote/community-summary', methods=['GET'])
def gracenote_community_summary():
    """Fast summary of community map coverage for the dashboard stat card."""
    from ..models import Source
    rows = (
        Channel.query
        .join(Source)
        .filter(Channel.is_active == True)
        .all()
    )
    total = applied = available = 0
    for ch in rows:
        source_name = ch.source.name if ch.source else ''
        match = lookup_gracenote(source_name, ch.source_channel_id)
        if not match or not match.get('tmsid'):
            continue
        total += 1
        if (ch.gracenote_id or '') == match['tmsid']:
            applied += 1
        elif (ch.gracenote_mode or 'auto') not in ('manual', 'off'):
            # Only count as available if auto-mode — manual/off overrides are intentional
            available += 1
    return jsonify({'total': total, 'applied': applied, 'available': available})


@api_bp.route('/gracenote/community-map', methods=['GET'])
def gracenote_community_map():
    """Return all channels that have a community CSV mapping, with their current Gracenote state."""
    from ..models import Source
    rows = (
        Channel.query
        .join(Source)
        .filter(Channel.is_active == True)
        .order_by(Source.name, Channel.name)
        .all()
    )
    results = []
    for ch in rows:
        source_name = ch.source.name if ch.source else ''
        match = lookup_gracenote(source_name, ch.source_channel_id)
        if not match or not match.get('tmsid'):
            continue
        community_tmsid = match['tmsid']
        current_id = ch.gracenote_id or ''
        mode = ch.gracenote_mode or 'auto'
        already_applied = current_id == community_tmsid
        results.append({
            'channel_id':       ch.id,
            'channel_name':     ch.name,
            'source_name':      source_name,
            'category':         ch.category or '',
            'community_tmsid':  community_tmsid,
            'notes':            match.get('notes') or '',
            'current_id':       current_id,
            'gracenote_mode':   mode,
            'already_applied':  already_applied,
            'is_enabled':       ch.is_enabled,
        })
    return jsonify({'results': results, 'total': len(results)})


@api_bp.route('/gracenote/community-apply-all', methods=['POST'])
def gracenote_community_apply_all():
    """
    Bulk-apply community Gracenote IDs to all matching channels.
    Skips channels already correctly applied.
    new_only=true  — only apply channels with no current ID (safe, no conflicts)
    new_only=false — also overwrite manual/off channels (requires confirmation)
    """
    body = request.get_json(silent=True, force=True) or {}
    dry_run  = body.get('dry_run', True)
    new_only = body.get('new_only', False)

    rows = (
        Channel.query
        .join(Source)
        .filter(Channel.is_active == True)
        .order_by(Source.name, Channel.name)
        .all()
    )

    applied = []
    overwritten = []
    already_done = 0

    for ch in rows:
        source_name = ch.source.name if ch.source else ''
        match = lookup_gracenote(source_name, ch.source_channel_id)
        if not match or not match.get('tmsid'):
            continue
        community_tmsid = match['tmsid']
        current_id = ch.gracenote_id or ''
        mode = ch.gracenote_mode or 'auto'

        if current_id == community_tmsid:
            already_done += 1
            continue

        is_override = mode in ('manual', 'off')
        entry = {'channel_id': ch.id, 'channel_name': ch.name, 'source_name': source_name,
                 'current_id': current_id, 'community_tmsid': community_tmsid, 'mode': mode}

        if is_override:
            overwritten.append(entry)
        else:
            applied.append(entry)

        if not dry_run and (not is_override or not new_only):
            _apply_gracenote_update(ch, community_tmsid, 'manual')

    if not dry_run:
        db.session.commit()
        _invalidate_and_refresh_xml()

    return jsonify({
        'dry_run':       dry_run,
        'new_only':      new_only,
        'applied':       applied,
        'overwritten':   overwritten,
        'already_done':  already_done,
        'total_changed': len(applied) + (0 if new_only else len(overwritten)),
    })


@api_bp.route('/gracenote/community-clear-all', methods=['POST'])
def gracenote_community_clear_all():
    """
    Clear community-mapped Gracenote IDs from all matching channels.
    Sets gracenote_id=None and gracenote_mode='auto' for every channel
    that has an entry in the community map (regardless of current state).
    Supports dry_run=true for preview.
    """
    body = request.get_json(silent=True, force=True) or {}
    dry_run = body.get('dry_run', True)

    rows = (
        Channel.query
        .join(Source)
        .filter(Channel.is_active == True)
        .order_by(Source.name, Channel.name)
        .all()
    )

    cleared = []
    already_clear = 0

    for ch in rows:
        source_name = ch.source.name if ch.source else ''
        match = lookup_gracenote(source_name, ch.source_channel_id)
        if not match or not match.get('tmsid'):
            continue

        has_id = bool(ch.gracenote_id)
        not_auto = (ch.gracenote_mode or 'auto') != 'auto'
        if not has_id and not not_auto:
            already_clear += 1
            continue

        cleared.append({
            'channel_id':   ch.id,
            'channel_name': ch.name,
            'source_name':  source_name,
            'current_id':   ch.gracenote_id or '',
            'mode':         ch.gracenote_mode or 'auto',
        })

        if not dry_run:
            ch.gracenote_id   = None
            ch.gracenote_mode = 'auto'

    if not dry_run:
        db.session.commit()
        _invalidate_and_refresh_xml()

    return jsonify({
        'dry_run':       dry_run,
        'cleared':       cleared,
        'already_clear': already_clear,
    })


@api_bp.route('/gracenote/my-contributions', methods=['GET'])
def gracenote_my_contributions():
    """Return channels the user has mapped that are absent from or differ in the community CSV."""
    from ..models import Source as _Source
    rows = (
        Channel.query.join(_Source)
        .filter(
            Channel.is_active == True,
            Channel.gracenote_id.isnot(None),
            Channel.gracenote_id != '',
            Channel.gracenote_mode != 'off',
        )
        .order_by(_Source.name, Channel.name)
        .all()
    )
    results = []
    for ch in rows:
        source_name = ch.source.name if ch.source else ''
        match = lookup_gracenote(source_name, ch.source_channel_id)
        community_tmsid = match.get('tmsid') if match else None
        if community_tmsid == ch.gracenote_id:
            continue  # already in community map with exact same tmsid
        results.append({
            'channel_id':       ch.id,
            'channel_name':     ch.name,
            'source_name':      source_name,
            'source_channel_id': ch.source_channel_id or '',
            'tmsid':            ch.gracenote_id,
            'category':         ch.category or '',
            'gracenote_mode':   ch.gracenote_mode or 'auto',
            'in_community':     community_tmsid is not None,
            'community_tmsid':  community_tmsid or '',
        })
    return jsonify({'results': results, 'total': len(results)})


@api_bp.route('/gracenote/submit-contributions', methods=['POST'])
def gracenote_submit_contributions():
    """POST selected channel mappings to the configured contribution webhook URL."""
    from datetime import datetime, timezone as _tz, timedelta as _td
    settings = AppSettings.get()

    # Server-side rate limit: one submission per 24 hours
    if settings.last_contribution_at:
        last = settings.last_contribution_at
        if last.tzinfo is None:
            last = last.replace(tzinfo=_tz.utc)
        elapsed = datetime.now(_tz.utc) - last
        if elapsed < _td(hours=24):
            remaining_s = int((_td(hours=24) - elapsed).total_seconds())
            h, m = divmod(remaining_s // 60, 60)
            wait = f'{h}h {m}m' if h else f'{m}m'
            return jsonify({
                'ok': False,
                'rate_limited': True,
                'message': f'You already submitted recently. Please wait {wait} before submitting again.',
            }), 429

    webhook_url = settings.effective_gracenote_contribution_url()
    if not webhook_url:
        return jsonify({'ok': False, 'message': 'No contribution webhook URL configured in Settings.'}), 400

    body = request.get_json(silent=True, force=True) or {}
    channel_ids = body.get('channel_ids', [])
    if not channel_ids:
        return jsonify({'ok': False, 'message': 'No channels selected.'}), 400

    from ..models import Source as _Source
    channels = (
        Channel.query.join(_Source)
        .filter(
            Channel.id.in_(channel_ids),
            Channel.gracenote_id.isnot(None),
            Channel.gracenote_id != '',
        )
        .all()
    )

    from ..config import VERSION
    import requests as _req
    submitted_at = datetime.now(_tz.utc).isoformat()
    succeeded = []
    failed = []
    for ch in channels:
        row = {
            'submitted_at': submitted_at,
            'app_version':  VERSION,
            'provider':     ch.source.name if ch.source else '',
            'key':          ch.source_channel_id or '',
            'tmsid':        ch.gracenote_id,
            'channel_name': ch.name,
            'category':     ch.category or '',
        }
        try:
            resp = _req.post(webhook_url, json=row, timeout=15)
            resp.raise_for_status()
            succeeded.append(ch.name)
        except Exception as exc:
            failed.append({'name': ch.name, 'error': str(exc)})

    if succeeded:
        settings.last_contribution_at = datetime.now(_tz.utc)
        db.session.commit()

    ok = len(succeeded) > 0
    return jsonify({
        'ok':          ok,
        'submitted':   len(succeeded),
        'failed':      len(failed),
        'failed_names': [f['name'] for f in failed],
        'message':     f'{len(succeeded)} mapping(s) submitted — thank you!' if ok else 'All submissions failed.',
    }), (200 if ok else 502)


@api_bp.route('/gracenote/community-export', methods=['GET'])
def gracenote_community_export():
    """
    Export all active channels as a JSON file for community Gracenote ID contribution.

    Each record contains the provider (source name), key (source_channel_id), channel name,
    and the current tmsid (blank if not yet mapped). Community members fill in missing tmsids
    and share the file back for merging into the master community map.
    """
    rows = (
        Channel.query
        .join(Source)
        .filter(Channel.is_active == True)
        .order_by(Source.name, Channel.name)
        .all()
    )
    channels = []
    for ch in rows:
        source_name = ch.source.name if ch.source else ''
        match = lookup_gracenote(source_name, ch.source_channel_id)
        community_tmsid = (match.get('tmsid') or '') if match else ''
        channels.append({
            'provider':      source_name,
            'key':           ch.source_channel_id or '',
            'channel_name':  ch.name or '',
            'tmsid':         community_tmsid,
        })
    payload = {
        'schema_version': 1,
        'exported_at':    datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'channel_count':  len(channels),
        'channels':       channels,
    }
    return current_app.response_class(
        json.dumps(payload, indent=2),
        mimetype='application/json',
        headers={'Content-Disposition': 'attachment; filename="gracenote_community_export.json"'},
    )


@api_bp.route('/gracenote/remote-map/status', methods=['GET'])
def gracenote_remote_map_status():
    settings = AppSettings.get()
    status = remote_map_status()
    status['url'] = settings.effective_gracenote_map_url()
    status['url_is_default'] = not (settings.gracenote_map_url or '').strip()
    return jsonify(status)


@api_bp.route('/gracenote/remote-map/refresh', methods=['POST'])
def gracenote_remote_map_refresh():
    settings = AppSettings.get()
    url = settings.effective_gracenote_map_url()
    success, message = fetch_remote_gracenote_map(url)
    status = remote_map_status()
    status['url'] = url
    return jsonify({'ok': success, 'message': message, **status}), (200 if success else 502)


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
    if countries := request.args.getlist('country'):
        q = q.filter(Channel.country.in_(countries))
    if gracenote := request.args.get('gracenote'):
        if gracenote == 'has':
            q = q.filter(Channel.gracenote_id != None, Channel.gracenote_id != '')
        elif gracenote == 'missing':
            q = q.filter((Channel.gracenote_id == None) | (Channel.gracenote_id == ''))
    cat_rows = db.session.query(Channel.category, db.func.count(Channel.id))\
        .filter(Channel.is_active == True).group_by(Channel.category)\
        .order_by(db.func.count(Channel.id).desc()).all()
    return jsonify({
        'total_channels': q.count(),
        'total_sources':  Source.query.filter_by(is_enabled=True).count(),
        'categories':     [{'name': c or 'Uncategorized', 'count': n} for c, n in cat_rows],
    })


@api_bp.route('/channels/<int:channel_id>/duplicates', methods=['GET'])
def channel_duplicates(channel_id):
    """Return channels whose name matches the given channel (strict or soft normalisation)."""
    from .admin import _canonical_duplicate_name, _soft_duplicate_name
    from sqlalchemy import func as _func
    ch = db.session.get(Channel, channel_id)
    if ch is None:
        return jsonify({'error': 'Not found'}), 404

    strict_key = _canonical_duplicate_name(ch.name or '')
    soft_key   = _soft_duplicate_name(ch.name or '')

    candidates = (
        Channel.query.join(Source)
        .filter(Channel.id != channel_id, Channel.name != None, Channel.name != '')
        .all()
    )

    strict, soft, seen = [], [], set()
    for c in candidates:
        if _canonical_duplicate_name(c.name) == strict_key and strict_key:
            strict.append(c)
            seen.add(c.id)
        elif _soft_duplicate_name(c.name) == soft_key and soft_key and c.id not in seen:
            soft.append(c)
            seen.add(c.id)

    # Gracenote-based tier: other channels sharing the same GN ID but different names
    gn_matches = []
    if ch.gracenote_id:
        gn_candidates = (
            Channel.query.join(Source)
            .filter(
                Channel.id != channel_id,
                Channel.gracenote_id == ch.gracenote_id,
                Channel.id.notin_(seen),
            )
            .all()
        )
        for c in gn_candidates:
            if _canonical_duplicate_name(c.name or '') != strict_key:
                gn_matches.append(c)

    # Fetch program counts for all relevant channels in one query
    all_ids = [ch.id] + [c.id for c in strict] + [c.id for c in soft] + [c.id for c in gn_matches]
    prog_counts = dict(
        db.session.query(Program.channel_id, _func.count(Program.id))
        .filter(Program.channel_id.in_(all_ids))
        .group_by(Program.channel_id)
        .all()
    )

    def _fmt(c):
        si = c.stream_info or {}
        return {
            'id':             c.id,
            'name':           c.name,
            'source':         c.source.display_name,
            'logo_url':       c.logo_url,
            'is_duplicate':   c.is_duplicate,
            'is_enabled':     c.is_enabled,
            'is_active':      c.is_active,
            'disable_reason': c.disable_reason,
            'missed_scrapes': c.missed_scrapes or 0,
            'category':       c.category,
            'gracenote_id':   c.gracenote_id,
            'gracenote_mode': c.gracenote_mode or 'auto',
            'program_count':  prog_counts.get(c.id, 0),
            'stream_info': {
                'max_resolution': si.get('max_resolution'),
                'video_codec':    si.get('video_codec'),
                'has_4k':         si.get('has_4k', False),
                'has_hd':         si.get('has_hd', False),
                'drm':            si.get('drm', False),
            } if si else None,
        }

    return jsonify({
        'channel': _fmt(ch),
        'strict':  [_fmt(c) for c in strict],
        'soft':    [_fmt(c) for c in soft],
        'gn':      [_fmt(c) for c in gn_matches],
    })


@api_bp.route('/channels/duplicate-summary', methods=['GET'])
def duplicate_summary():
    """Return strict duplicate stats plus reviewable soft-match groups."""
    from collections import defaultdict
    from .admin import _canonical_duplicate_name, _soft_duplicate_name

    enabled_channels = (
        Channel.query.join(Source)
        .filter(
            Channel.is_enabled == True,
            Channel.name != None,
            Channel.name != '',
        )
        .all()
    )
    strict_groups = defaultdict(list)
    soft_groups = defaultdict(list)
    for ch in enabled_channels:
        strict_key = _canonical_duplicate_name(ch.name or '')
        if strict_key:
            strict_groups[strict_key].append(ch)
        soft_key = _soft_duplicate_name(ch.name or '')
        if soft_key:
            soft_groups[soft_key].append(ch)

    # Exclude groups where all channels share the same source but differ only by
    # region — those are cross-region duplicates, not true duplicates.
    def _is_cross_region_only(channels):
        source_ids = {ch.source_id for ch in channels}
        if len(source_ids) > 1:
            return False
        countries = {ch.country for ch in channels}
        return len(countries) > 1

    dup_channels = [
        ch for channels in strict_groups.values()
        if len(channels) > 1 and not _is_cross_region_only(channels)
        for ch in channels
    ]

    if not dup_channels:
        strict_groups_found = set()
    else:
        strict_groups_found = {key for key, channels in strict_groups.items() if len(channels) > 1}

    soft_group_payload = []
    for key, channels in soft_groups.items():
        names = sorted({(ch.name or '').strip() for ch in channels if (ch.name or '').strip()})
        if len(names) < 2:
            continue
        strict_keys_in_group = {_canonical_duplicate_name(name) for name in names}
        if len(strict_keys_in_group) <= 1:
            continue
        enabled_count = sum(1 for ch in channels if ch.is_enabled)
        if enabled_count < 2:
            continue
        soft_group_payload.append({
            'group_key': key,
            'names': names,
            'channel_count': enabled_count,
            'sources': sorted({ch.source.display_name for ch in channels}),
            'match_reason': 'Matched after soft brand normalization (TV/Channel/Network).',
        })
    soft_group_payload.sort(key=lambda item: (-item['channel_count'], item['names'][0].casefold()))

    # Find which duplicate channels actually have program data
    dup_channel_ids = [ch.id for ch in dup_channels]
    channels_with_epg = {
        row[0] for row in
        db.session.query(Program.channel_id)
        .filter(Program.channel_id.in_(dup_channel_ids))
        .distinct()
        .all()
    }

    stats = defaultdict(lambda: {'display_name': '', 'total': 0, 'with_epg': 0, 'epg_only': False})
    for ch in dup_channels:
        s = stats[ch.source.name]
        s['display_name'] = ch.source.display_name
        s['epg_only'] = ch.source.epg_only
        s['total'] += 1
        if ch.id in channels_with_epg:
            s['with_epg'] += 1

    sources = []
    for name, s in stats.items():
        pct = round(100 * s['with_epg'] / s['total']) if s['total'] else 0
        sources.append({
            'name':         name,
            'display_name': s['display_name'],
            'dup_count':    s['total'],
            'gn_pct':       pct,
            'epg_only':     s['epg_only'],
        })

    # EPG-only sources always rank last; within each tier sort by EPG coverage descending
    sources.sort(key=lambda x: (1 if x['epg_only'] else 0, -x['gn_pct']))

    return jsonify({
        'sources':       sources,
        'total_groups':  len(strict_groups_found),
        'total_affected': len(dup_channels),
        'soft_groups': soft_group_payload,
    })


@api_bp.route('/channels/resolve-duplicates', methods=['POST'])
def resolve_duplicates():
    """Disable duplicate channels, keeping one winner per normalized-name group."""
    from collections import defaultdict
    from .admin import _canonical_duplicate_name, _soft_duplicate_name

    data = request.get_json(force=True) or {}
    priority = data.get('source_priority', [])  # ordered list of source names, index 0 = highest
    mode = (data.get('mode') or 'strict').strip().lower()
    selected_group_keys = {
        (key or '').strip()
        for key in (data.get('group_keys') or [])
        if (key or '').strip()
    }

    groups = defaultdict(list)
    all_named_channels = (
        Channel.query.join(Source)
        .filter(Channel.name != None, Channel.name != '')
        .all()
    )
    for ch in all_named_channels:
        key = _canonical_duplicate_name(ch.name or '') if mode == 'strict' else _soft_duplicate_name(ch.name or '')
        if key:
            groups[key].append(ch)

    def is_unhealthy(ch):
        return ch.disable_reason == 'Dead' or (ch.disable_reason or '').startswith('DRM') or not ch.is_active

    def has_gracenote(ch):
        return bool((ch.gracenote_id or '').strip())

    from ..models import Program as _Program
    from datetime import datetime as _dt, timezone as _tz
    _now = _dt.now(_tz.utc)
    _channels_with_epg = {
        row[0] for row in
        db.session.query(_Program.channel_id)
        .filter(_Program.end_time > _now)
        .distinct()
        .all()
    }

    def has_epg(ch):
        return ch.id in _channels_with_epg

    def priority_key(ch):
        try:
            source_rank = priority.index(ch.source.name)
        except ValueError:
            source_rank = len(priority)  # unlisted sources rank last
        return (
            1 if is_unhealthy(ch) else 0,
            0 if has_gracenote(ch) else 1,
            0 if has_epg(ch) else 1,
            source_rank,
        )

    disabled_count = 0
    enabled_count = 0
    groups_resolved = 0
    for group_key, channels in groups.items():
        if mode == 'soft' and group_key not in selected_group_keys:
            continue
        if mode == 'soft':
            strict_keys_in_group = {_canonical_duplicate_name(ch.name or '') for ch in channels if (ch.name or '').strip()}
            if len(strict_keys_in_group) <= 1:
                continue
        enabled_in_group = [ch for ch in channels if ch.is_enabled]
        if len(enabled_in_group) < 2:
            continue
        channels.sort(key=priority_key)
        winner = channels[0]
        if all(is_unhealthy(ch) for ch in channels):
            for ch in channels:
                if ch.is_enabled:
                    ch.is_enabled = False
                    disabled_count += 1
            groups_resolved += 1
            continue
        if not is_unhealthy(winner) and not winner.is_enabled:
            winner.is_enabled = True
            enabled_count += 1
        for ch in channels[1:]:
            if ch.is_enabled:
                ch.is_enabled = False
                disabled_count += 1
        groups_resolved += 1

    db.session.commit()
    return jsonify({
        'disabled': disabled_count,
        'enabled': enabled_count,
        'groups_resolved': groups_resolved,
    })


@api_bp.route('/feeds/<int:feed_id>/push-to-dvr', methods=['POST'])
def push_feed_to_dvr(feed_id):
    """Register this feed as custom M3U source(s) in Channels DVR.

    Registers up to two sources:
    - Gracenote source (no EPG URL): only if the feed has channels with
      Gracenote IDs — DVR fetches its own guide data via tvc-guide-stationid.
    - Standard source (with our EPG XML): always registered.
    """
    import re as _re
    from ..generators.m3u import _build_channel_query, _parse_gracenote_id, feed_to_query_filters

    feed = Feed.query.get_or_404(feed_id)
    settings = AppSettings.get()

    dvr_url = (settings.effective_channels_dvr_url() or '').strip()
    if not dvr_url:
        return jsonify({'error': 'Channels DVR URL is not configured in Settings.'}), 400

    base = public_base_url()

    # Check if this feed has any channels with Gracenote IDs using the same
    # logic as generate_gracenote_m3u() so we don't register an empty source.
    channel_count, has_gracenote = _channel_query_summary(
        _build_channel_query(feed_to_query_filters(feed.filters or {})),
        _parse_gracenote_id,
    )
    if channel_count == 0:
        return jsonify({'error': 'This feed has no eligible channels to add to Channels DVR.'}), 400

    # standard_count = channels that go into the regular M3U (non-gracenote channels)
    split = _feed_split_counts(feed)
    standard_count = split['standard_count']

    force = bool((request.get_json(silent=True) or {}).get('force'))
    if channel_count > _CHANNELS_DVR_RECOMMENDED_MAX and not force:
        return jsonify({
            'error': f'This feed has {channel_count} channels. Channels DVR usually works best at 750 or fewer.',
            'requires_confirm': True,
            'channel_count': channel_count,
            'recommended_max': _CHANNELS_DVR_RECOMMENDED_MAX,
        }), 409

    try:
        _ensure_feed_dvr_artifacts(feed, base, has_gracenote=has_gracenote)
    except TimeoutError:
        return jsonify({'error': 'Timed out waiting for feed artifacts to build. Try again in a moment.'}), 503

    def _put(name, url, xmltv_url=''):
        safe = _re.sub(r'[^a-zA-Z0-9]', '', name)
        payload = {
            'name':    name,
            'type':    'HLS',
            'source':  'URL',
            'url':     url,
            'refresh': '24',
        }
        if xmltv_url:
            payload['xmltv_url']     = xmltv_url
            payload['xmltv_refresh'] = '3600'
        return _req.put(f"{dvr_url}/providers/m3u/sources/{safe}", json=payload, timeout=30)

    gn_name  = f"FastChannels {feed.name} Gracenote"
    epg_name = f"FastChannels {feed.name}"
    sources_added = []

    try:
        if has_gracenote:
            r1 = _put(gn_name, f"{base}/feeds/{feed.slug}/m3u/gracenote")
            r1.raise_for_status()
            sources_added.append(gn_name)

        if standard_count > 0:
            r2 = _put(epg_name, f"{base}/feeds/{feed.slug}/m3u", f"{base}/feeds/{feed.slug}/epg.xml")
            r2.raise_for_status()
            sources_added.append(epg_name)
    except _req.exceptions.ConnectionError:
        return jsonify({'error': f'Could not connect to Channels DVR at {dvr_url}'}), 502
    except _req.exceptions.Timeout:
        return jsonify({'error': 'Channels DVR timed out.'}), 504
    except _req.exceptions.HTTPError as exc:
        resp = exc.response
        return jsonify({'error': f'DVR {resp.status_code}: {resp.text[:300]}'}), 502

    return jsonify({'ok': True, 'sources_added': sources_added})


@api_bp.route('/sources/<int:source_id>/push-to-dvr', methods=['POST'])
def push_source_to_dvr(source_id):
    """Register a source-filtered raw output as custom M3U source(s) in Channels DVR."""
    import re as _re
    from ..generators.m3u import _build_channel_query, _parse_gracenote_id

    source = Source.query.get_or_404(source_id)
    settings = AppSettings.get()

    dvr_url = (settings.effective_channels_dvr_url() or '').strip()
    if not dvr_url:
        return jsonify({'error': 'Channels DVR URL is not configured in Settings.'}), 400

    base = public_base_url()
    channel_count, has_gracenote = _channel_query_summary(
        _build_channel_query({'source': [source.name]}),
        _parse_gracenote_id,
    )
    if channel_count == 0:
        return jsonify({'error': f'{source.display_name} has no eligible channels to add to Channels DVR.'}), 400
    force = bool((request.get_json(silent=True) or {}).get('force'))
    if channel_count > _CHANNELS_DVR_RECOMMENDED_MAX and not force:
        return jsonify({
            'error': f'{source.display_name} has {channel_count} channels. Channels DVR usually works best at 750 or fewer.',
            'requires_confirm': True,
            'channel_count': channel_count,
            'recommended_max': _CHANNELS_DVR_RECOMMENDED_MAX,
        }), 409

    def _put(name, url, xmltv_url=''):
        safe = _re.sub(r'[^a-zA-Z0-9]', '', name)
        payload = {
            'name': name,
            'type': 'HLS',
            'source': 'URL',
            'url': url,
            'refresh': '24',
        }
        if xmltv_url:
            payload['xmltv_url'] = xmltv_url
            payload['xmltv_refresh'] = '3600'
        return _req.put(f"{dvr_url}/providers/m3u/sources/{safe}", json=payload, timeout=30)

    query_param = f"?source={source.name}"
    std_name = f"FastChannels {source.display_name}"
    gn_name = f"FastChannels {source.display_name} Gracenote"
    sources_added = []

    try:
        if has_gracenote:
            r1 = _put(gn_name, f"{base}/m3u/gracenote{query_param}")
            r1.raise_for_status()
            sources_added.append(gn_name)

        r2 = _put(std_name, f"{base}/m3u{query_param}", f"{base}/epg.xml{query_param}")
        r2.raise_for_status()
        sources_added.append(std_name)
    except _req.exceptions.ConnectionError:
        return jsonify({'error': f'Could not connect to Channels DVR at {dvr_url}'}), 502
    except _req.exceptions.Timeout:
        return jsonify({'error': 'Channels DVR timed out.'}), 504
    except _req.exceptions.HTTPError as exc:
        resp = exc.response
        return jsonify({'error': f'DVR {resp.status_code}: {resp.text[:300]}'}), 502

    return jsonify({'ok': True, 'sources_added': sources_added})


@api_bp.route('/raw-output/push-to-dvr', methods=['POST'])
def push_raw_output_to_dvr():
    """Register the full raw output M3U source(s) in Channels DVR."""
    import re as _re
    from ..generators.m3u import _build_channel_query, _parse_gracenote_id

    settings = AppSettings.get()

    dvr_url = (settings.effective_channels_dvr_url() or '').strip()
    if not dvr_url:
        return jsonify({'error': 'Channels DVR URL is not configured in Settings.'}), 400

    base = public_base_url()
    channel_count, has_gracenote = _channel_query_summary(
        _build_channel_query({}),
        _parse_gracenote_id,
    )
    if channel_count == 0:
        return jsonify({'error': 'Raw Output has no eligible channels to add to Channels DVR.'}), 400
    force = bool((request.get_json(silent=True) or {}).get('force'))
    if channel_count > _CHANNELS_DVR_RECOMMENDED_MAX and not force:
        return jsonify({
            'error': f'Raw Output has {channel_count} channels. Channels DVR usually works best at 750 or fewer.',
            'requires_confirm': True,
            'channel_count': channel_count,
            'recommended_max': _CHANNELS_DVR_RECOMMENDED_MAX,
        }), 409

    def _put(name, url, xmltv_url=''):
        safe = _re.sub(r'[^a-zA-Z0-9]', '', name)
        payload = {
            'name': name,
            'type': 'HLS',
            'source': 'URL',
            'url': url,
            'refresh': '24',
        }
        if xmltv_url:
            payload['xmltv_url'] = xmltv_url
            payload['xmltv_refresh'] = '3600'
        return _req.put(f"{dvr_url}/providers/m3u/sources/{safe}", json=payload, timeout=8)

    std_name = 'FastChannels Raw Output'
    gn_name = 'FastChannels Raw Output Gracenote'
    sources_added = []

    try:
        if has_gracenote:
            r1 = _put(gn_name, f"{base}/m3u/gracenote")
            r1.raise_for_status()
            sources_added.append(gn_name)

        r2 = _put(std_name, f"{base}/m3u", f"{base}/epg.xml")
        r2.raise_for_status()
        sources_added.append(std_name)
    except _req.exceptions.ConnectionError:
        return jsonify({'error': f'Could not connect to Channels DVR at {dvr_url}'}), 502
    except _req.exceptions.Timeout:
        return jsonify({'error': 'Channels DVR timed out.'}), 504
    except _req.exceptions.HTTPError as exc:
        resp = exc.response
        return jsonify({'error': f'DVR {resp.status_code}: {resp.text[:300]}'}), 502

    return jsonify({'ok': True, 'sources_added': sources_added})


@api_bp.route('/settings', methods=['GET', 'POST'])
def app_settings():
    row = AppSettings.get()
    if request.method == 'POST':
        data = request.get_json(force=True) or {}
        if 'channels_dvr_url' in data:
            row.channels_dvr_url = _normalize_server_url(data['channels_dvr_url'], default_port=8089)
        if 'public_base_url' in data:
            row.public_base_url = _normalize_server_url(data['public_base_url'], default_port=5523)
        if 'timezone_name' in data:
            tz_name = normalize_timezone_name(data.get('timezone_name'))
            if data.get('timezone_name') and tz_name is None:
                return jsonify({'error': f"Invalid timezone: {data.get('timezone_name')}"}), 422
            row.timezone_name = tz_name
        if 'gracenote_auto_fill' in data:
            row.gracenote_auto_fill = bool(data['gracenote_auto_fill'])
        if 'gracenote_map_url' in data:
            row.gracenote_map_url = (data['gracenote_map_url'] or '').strip() or None
        if 'gracenote_contribution_url' in data:
            row.gracenote_contribution_url = (data['gracenote_contribution_url'] or '').strip() or None
        db.session.commit()
        write_timezone_cache(row.timezone_name)
        _invalidate_and_refresh_xml()
        row = AppSettings.get()
    return jsonify({
        'channels_dvr_url':  row.effective_channels_dvr_url(),
        'public_base_url':   row.effective_public_base_url(),
        'timezone_name':     row.effective_timezone_name(),
        'gracenote_auto_fill': row.gracenote_auto_fill if row.gracenote_auto_fill is not None else True,
        'gracenote_map_url': row.gracenote_map_url or '',
        'gracenote_contribution_url': row.gracenote_contribution_url or '',
        'channels_dvr_url_source': 'db' if (row.channels_dvr_url or '').strip() else ('env' if row.env_channels_dvr_url() is not None else 'unset'),
        'public_base_url_source': 'db' if (row.public_base_url or '').strip() else ('env' if row.env_public_base_url() is not None else 'unset'),
        'timezone_name_source': 'db' if (row.timezone_name or '').strip() else 'system',
    })


@api_bp.route('/settings/gracenote-auto-clear', methods=['POST'])
def gracenote_auto_clear():
    """Disable auto-fill and clear all auto-assigned Gracenote IDs."""
    from .tasks import trigger_gracenote_auto_clear
    row = AppSettings.get()
    row.gracenote_auto_fill = False
    db.session.commit()
    trigger_gracenote_auto_clear()
    return jsonify({'status': 'queued'})


@api_bp.route('/settings/backup-db')
def backup_db():
    """Download a gzip-compressed copy of the live SQLite database."""
    import gzip, tempfile, os as _os, sqlite3 as _sqlite3
    db_path = '/data/fastchannels.db'
    if not _os.path.exists(db_path):
        return jsonify({'error': 'Database file not found.'}), 404
    tmp_db  = tempfile.NamedTemporaryFile(suffix='.db',    delete=False)
    tmp_gz  = tempfile.NamedTemporaryFile(suffix='.db.gz', delete=False)
    tmp_db.close(); tmp_gz.close()
    try:
        # SQLite online backup — safe while DB is live
        src = _sqlite3.connect(db_path)
        dst = _sqlite3.connect(tmp_db.name)
        src.backup(dst)
        src.close(); dst.close()
        # Compress
        with open(tmp_db.name, 'rb') as f_in, gzip.open(tmp_gz.name, 'wb', compresslevel=6) as f_out:
            f_out.write(f_in.read())
        ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        filename = f'fastchannels_backup_{ts}.db.gz'
        return current_app.response_class(
            open(tmp_gz.name, 'rb').read(),
            mimetype='application/gzip',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'},
        )
    finally:
        _os.unlink(tmp_db.name)
        _os.unlink(tmp_gz.name)


@api_bp.route('/system-stats')
def system_stats():
    # ── Database ──────────────────────────────────────────────────────────
    _DB_FILES = [
        '/data/fastchannels.db',
        '/data/fastchannels.db-shm',
        '/data/fastchannels.db-wal',
    ]
    db_size = sum(_os.path.getsize(f) for f in _DB_FILES if _os.path.exists(f))

    channels_total   = Channel.query.count()
    channels_active  = Channel.query.filter_by(is_active=True, is_enabled=True).count()
    channels_drm     = Channel.query.filter(Channel.disable_reason.like('DRM%')).count()
    channels_dead    = Channel.query.filter_by(disable_reason='Dead').count()
    sources_enabled  = Source.query.filter_by(is_enabled=True).count()
    sources_total    = Source.query.count()
    programs_total   = Program.query.count()

    # ── Image cache ───────────────────────────────────────────────────────
    def _dir_stats(d):
        if not _os.path.exists(d):
            return 0, 0
        files = [f for f in _os.listdir(d) if not f.endswith('.ct') and not f.endswith('.url')]
        size  = sum(_os.path.getsize(_os.path.join(d, f)) for f in files)
        return len(files), size

    logo_count,   logo_bytes   = _dir_stats('/data/logo_cache/logos')
    poster_count, poster_bytes = _dir_stats('/data/logo_cache/posters')

    # ── Uptime ────────────────────────────────────────────────────────────
    uptime_seconds = int(_time.time() - _APP_START)

    return jsonify({
        'uptime_seconds': uptime_seconds,
        'db': {
            'size_bytes':       db_size,
            'channels_total':   channels_total,
            'channels_active':  channels_active,
            'channels_drm':     channels_drm,
            'channels_dead':    channels_dead,
            'sources_enabled':  sources_enabled,
            'sources_total':    sources_total,
            'programs_total':   programs_total,
        },
        'image_cache': {
            'logos_count':    logo_count,
            'logos_bytes':    logo_bytes,
            'posters_count':  poster_count,
            'posters_bytes':  poster_bytes,
            'logo_expiry':    'url-change',
            'poster_ttl_days': 4,
        },
        'processes': _process_stats(),
        'cpu': _cpu_stats(),
        'memory': _memory_stats(),
    })


@api_bp.route('/localnow/cities')
def localnow_cities():
    """Search Local Now cities/markets by name. Returns [{label, dma, market}]."""
    q = request.args.get('q', '').strip()
    if len(q) < 2:
        return jsonify([])
    try:
        from ..scrapers.localnow import LocalNowScraper
        now = _time.time()
        cached = _localnow_city_scraper.get('scraper')
        if not cached or _localnow_city_scraper.get('expires', 0) < now:
            s = LocalNowScraper()
            s._ensure_runtime_bootstrapped()
            _localnow_city_scraper['scraper'] = s
            _localnow_city_scraper['expires'] = now + 3600
        else:
            s = cached
        return jsonify(s.search_cities(q))
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500

import logging
import os as _os
import time as _time

logger = logging.getLogger(__name__)

_APP_START = _time.time()
from flask import Blueprint, jsonify, request
from ..extensions import db
from ..models import Source, Channel, Program
try:
    from croniter import croniter as _croniter
except ImportError:
    _croniter = None
from .tasks import (
    trigger_tvtv_cache_refresh,
)
from .. import logfile

system_bp = Blueprint('api_system', __name__)

_localnow_city_scraper: dict = {}  # {'scraper': LocalNowScraper, 'expires': float}


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


@system_bp.route('/stations/<station_id>/now-playing', methods=['GET'])
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


@system_bp.route('/tvtv/cache/refresh', methods=['POST'])
def tvtv_cache_refresh():
    """Queue an immediate refresh of the tvtv guide cache."""
    status = trigger_tvtv_cache_refresh()
    messages = {
        'queued':   'Refresh queued.',
        'active':   'Refresh is already queued or running.',
        'deferred': 'Refresh deferred: a scraper job is active. Try again once it finishes.',
    }
    return jsonify({
        'status': status,
        'job_id': 'tvtv-cache-refresh',
        'message': messages.get(status, ''),
    }), 202


@system_bp.route('/logs')
def get_logs():
    n = request.args.get('n', 2500, type=int)
    lines = logfile.tail(n)
    return jsonify({'lines': lines})


@system_bp.route('/stats')
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


@system_bp.route('/system-stats')
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
    channels_vod     = Channel.query.filter_by(disable_reason='VOD').count()
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
            'channels_vod':     channels_vod,
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


@system_bp.route('/localnow/cities')
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

import json
import logging
import re
import time as _time

logger = logging.getLogger(__name__)
from datetime import datetime, timezone

_APP_START = _time.time()
from flask import Blueprint, jsonify, request, current_app
from sqlalchemy.orm import defer
from app.config_store import load_source_cache
from ..extensions import db
from ..models import Source, Channel, SourceCache
from ..scrapers import registry
from ..source_config import is_source_config_complete
try:
    from croniter import croniter as _croniter
except ImportError:
    _croniter = None
from .tasks import (
    trigger_scrape,
    trigger_source_channel_purge,
    trigger_source_disable,
    get_source_disable_status,
    cancel_pending_source_disable,
    trigger_stream_audit,
    trigger_stream_audit_recheck,
)
from ..generators.m3u import (
    get_global_chnum_overlaps,
)

from .api_shared import _invalidate_and_refresh_xml

sources_bp = Blueprint('api_sources', __name__)


def _scrape_interval_limits(source_name: str) -> tuple[int, int, int]:
    scraper_cls = registry.get(source_name)
    recommended = getattr(scraper_cls, 'scrape_interval', 360) if scraper_cls else 360
    minimum = getattr(scraper_cls, 'min_scrape_interval', 30) if scraper_cls else 30
    maximum = getattr(scraper_cls, 'max_scrape_interval', 10080) if scraper_cls else 10080
    return int(recommended), int(minimum), int(maximum)


@sources_bp.route('/sources')
def list_sources():
    # defer(Source.config): this endpoint is polled on every UI cycle and to_dict()
    # never reads config, but config is effectively a cache blob (Roku's is ~1.2MB).
    # Loading it on every poll re-parses ~1.2MB of JSON for nothing and feeds
    # allocator fragmentation on the long-lived workers. See project memory:
    # Source.config join hazard.
    return jsonify([
        s.to_dict()
        for s in Source.query.options(defer(Source.config)).order_by(Source.display_name).all()
    ])


@sources_bp.route('/sources/<int:source_id>/run', methods=['POST'])
def run_source(source_id):
    source = Source.query.get_or_404(source_id)
    if not source.is_enabled:
        return jsonify({'error': 'Source is disabled'}), 409
    trigger_scrape(source.name, force_full=True)
    return jsonify({'status': 'queued', 'source': source.name})


@sources_bp.route('/sources/force-refresh', methods=['POST'])
def force_refresh_sources():
    enabled_sources = Source.query.filter_by(is_enabled=True).order_by(Source.display_name).all()
    queued = []
    for source in enabled_sources:
        if not source.scrape_interval:  # 0 = never auto-scraped (e.g. custom channels)
            continue
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


@sources_bp.route('/sources/<int:source_id>/scrape-status')
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


@sources_bp.route('/sources/<int:source_id>/stream-audit', methods=['POST'])
def stream_audit_source(source_id):
    source = Source.query.get_or_404(source_id)
    if not source.is_enabled:
        return jsonify({'error': 'Source is disabled'}), 409
    trigger_stream_audit(source.name)
    return jsonify({'status': 'queued', 'source': source.name})


@sources_bp.route('/sources/stream-audit-all', methods=['POST'])
def stream_audit_all():
    from ..scrapers import registry
    sources = Source.query.filter_by(is_enabled=True).all()
    queued = []
    for src in sources:
        cls = registry.get(src.name)
        if cls and getattr(cls, 'stream_audit_enabled', False):
            trigger_stream_audit(src.name)
            queued.append({'id': src.id, 'name': src.name})
    return jsonify({'status': 'queued', 'sources': queued, 'count': len(queued)})


@sources_bp.route('/sources/<int:source_id>/stream-audit-recheck', methods=['POST'])
def stream_audit_recheck(source_id):
    source = Source.query.get_or_404(source_id)
    if not source.is_enabled:
        return jsonify({'error': 'Source is disabled'}), 409
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


@sources_bp.route('/sources/<int:source_id>/inactive-count')
def inactive_channel_count(source_id):
    source = Source.query.get_or_404(source_id)
    days = int(request.args.get('days', 7))
    count = _orphan_query(source, days).count()
    return jsonify({'count': count, 'source': source.name, 'days': days})


@sources_bp.route('/sources/<int:source_id>/delete-inactive', methods=['POST'])
def delete_inactive_channels(source_id):
    source = Source.query.get_or_404(source_id)
    days = int((request.get_json() or {}).get('days', 7))
    orphans = _orphan_query(source, days).all()
    count = len(orphans)
    for ch in orphans:
        db.session.delete(ch)
    db.session.commit()
    return jsonify({'deleted': count, 'source': source.name})


@sources_bp.route('/channels/inactive-count')
def inactive_channel_count_global():
    from datetime import timedelta
    from collections import defaultdict
    days = int(request.args.get('days', 14))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    rows = (
        Channel.query
        .join(Source, Channel.source_id == Source.id)
        .add_columns(Source.name.label('source_name'))
        .filter(
            Channel.is_active == False,
            Channel.scrape_pinned == False,
            db.or_(Channel.last_seen_at == None, Channel.last_seen_at < cutoff),
        )
        .order_by(Source.name, Channel.name)
        .all()
    )
    total = len(rows)
    by_source = defaultdict(list)
    for ch, source_name in rows:
        by_source[source_name].append({
            'name': ch.name,
            'last_seen_at': ch.last_seen_at.isoformat() if ch.last_seen_at else None,
        })
    breakdown = [
        {'source': src, 'count': len(chs), 'channels': chs}
        for src, chs in sorted(by_source.items(), key=lambda x: -len(x[1]))
    ]
    return jsonify({'count': total, 'days': days, 'by_source': breakdown})


@sources_bp.route('/channels/delete-inactive', methods=['POST'])
def delete_inactive_channels_global():
    from datetime import timedelta
    days = int((request.get_json() or {}).get('days', 14))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    orphans = Channel.query.filter(
        Channel.is_active == False,
        Channel.scrape_pinned == False,
        db.or_(Channel.last_seen_at == None, Channel.last_seen_at < cutoff),
    ).all()
    count = len(orphans)
    for ch in orphans:
        db.session.delete(ch)
    db.session.commit()
    return jsonify({'deleted': count, 'days': days})


@sources_bp.route('/sources/<int:source_id>/audit-status')
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
    _audit_cache = load_source_cache(source.id)
    last_result = _audit_cache.get('last_audit_result')
    last_report = _audit_cache.get('last_audit_report')
    return jsonify({'status': 'idle', 'last_result': last_result, 'last_report': last_report})


@sources_bp.route('/sources/chnum-overlaps')
def chnum_overlaps():
    """Return a list of channel-number overlap warnings across all M3U outputs."""
    return jsonify({'warnings': get_global_chnum_overlaps()})


@sources_bp.route('/sources/<int:source_id>', methods=['PATCH'])
def update_source(source_id):
    source = Source.query.get_or_404(source_id)
    data = request.get_json()

    # Disabling always runs off the request thread via run_source_disable —
    # never inline — regardless of what else is in the payload. SQLite's
    # writer lock is database-wide, not per-row, so committing this inline
    # can otherwise mean waiting out busy_timeout (30s) behind an unrelated
    # active scrape's chunked commits, freezing the gunicorn worker handling
    # this request for that whole window (observed live 2026-08-14: with 2
    # workers, made the whole admin UI look locked up). Keeping this the
    # ONLY place the disable+cancel+purge+xml-refresh sequence is
    # implemented also avoids a second inline copy silently drifting from
    # it (code review, 2026-08-14). Enabling doesn't purge/lock anything, so
    # it's cheap enough to stay inline below.
    disabling_now = data.get('is_enabled') is False and source.is_enabled
    if disabling_now:
        trigger_source_disable(source.id)
        if set(data.keys()) == {'is_enabled'}:
            return jsonify({'status': 'queued'}), 202
        # Other fields are also changing in this same request — apply them
        # below as normal. is_enabled itself is deliberately left untouched
        # here; the queued job above owns flipping it.

    changed = False
    if 'is_enabled' in data and not disabling_now:
        new_enabled = bool(data['is_enabled'])
        if new_enabled and not source.is_enabled:
            # Re-enabling — cancel any disable that's still queued from a
            # moment ago (e.g. a quick off/on click) so it doesn't run after
            # this commit and silently flip the source back off, see
            # cancel_pending_source_disable's docstring.
            cancel_pending_source_disable(source.id)
        source.is_enabled = new_enabled
        changed = True
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
    if 'scrape_cron' in data:
        cron = data['scrape_cron'] or None
        if cron and _croniter is not None:
            try:
                _croniter(cron)
            except Exception:
                return jsonify({'error': 'Invalid cron expression'}), 422
        source.scrape_cron = cron
        changed = True
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
    if 'new_channel_policy' in data:
        policy = (data['new_channel_policy'] or 'inherit').strip().lower()
        if policy not in ('inherit', 'enabled', 'review'):
            return jsonify({'error': 'new_channel_policy must be inherit, enabled, or review'}), 422
        source.new_channel_policy = policy
        changed = True
    if changed:
        baseline_warnings = set(get_global_chnum_overlaps())
        db.session.flush()
        new_warnings = [w for w in get_global_chnum_overlaps() if w not in baseline_warnings]
        if new_warnings:
            db.session.rollback()
            return jsonify({'error': 'Channel number overlaps detected', 'warnings': new_warnings}), 409
    db.session.commit()
    # Disabling's own cancel/purge/xml-refresh follow-up is entirely owned by
    # the queued run_source_disable job triggered above — nothing to do here
    # for that case. For every other change, keep the existing unconditional
    # refresh so M3U/EPG output picks up whatever just changed.
    if not disabling_now:
        _invalidate_and_refresh_xml()
    return jsonify(source.to_dict())


@sources_bp.route('/sources/<int:source_id>/disable-status')
def source_disable_status(source_id):
    """Polled by the admin UI while a disable toggle is queued (see
    setSourceEnabled/update_source's async disable path) to know when it's
    safe to drop the "Disabling…" indicator. status is 'pending'/'done'/
    'error' — see get_source_disable_status."""
    return jsonify(get_source_disable_status(source_id))


@sources_bp.route('/sources/<int:source_id>/channels', methods=['DELETE'])
def delete_source_channels(source_id):
    """Delete all channels (and their programs via cascade) for a source."""
    source = Source.query.get_or_404(source_id)
    matched = source.channels.count()
    trigger_source_channel_purge(source.id)
    return jsonify({'status': 'queued', 'source': source.name, 'matched': matched})


@sources_bp.route('/sources/<int:source_id>/config', methods=['GET'])
def get_source_config(source_id):
    source      = Source.query.get_or_404(source_id)
    scraper_cls = registry.get(source.name)
    schema      = [f.to_dict() for f in (scraper_cls.config_schema if scraper_cls else []) if not f.hidden]
    saved       = source.config or {}
    secret_keys = {f['key'] for f in schema if f['secret']}
    values = {}
    for f in schema:
        key = f['key']
        if key in secret_keys and saved.get(key):
            values[key] = '••••••••'
        else:
            values[key] = saved.get(key, f['default'] or '')
    if source.name == 'pluto' and 'auth_mode' not in saved:
        # Installs saved before 'auth_mode' existed already have real
        # credentials stored (it used to be mandatory) — reflect the mode
        # that's actually in effect rather than the field's schema default.
        values['auth_mode'] = 'login' if (saved.get('username') and saved.get('password')) else 'anonymous'
    config_complete = bool(scraper_cls and is_source_config_complete(source.name, scraper_cls, saved))
    config_status = (
        'configured'
        if config_complete else
        ('required' if scraper_cls and getattr(scraper_cls, 'config_required', False) else 'optional')
    )
    return jsonify({'schema': schema, 'values': values, 'config_complete': config_complete,
                    'config_status': config_status,
                    'oauth_token_time': saved.get('oauth_token_time')})


@sources_bp.route('/sources/<int:source_id>/config', methods=['POST'])
def save_source_config(source_id):
    source      = Source.query.get_or_404(source_id)
    scraper_cls = registry.get(source.name)
    schema      = scraper_cls.config_schema if scraper_cls else []
    secret_keys = {f.key for f in schema if f.secret}
    data        = request.get_json() or {}
    current     = dict(source.config or {})
    old         = dict(source.config or {})
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
    # If login credentials changed, purge any cached auth state so the next run
    # is forced through a fresh login. Otherwise scrapers that cache auth in
    # config keep authenticating as the *old* account — the new credentials are
    # never exercised until the stale token/cookie finally fails:
    #   Fubo   — access_token/refresh_token/token_time (refresh valid ~1 year)
    #   Frndly — session_id/login_time
    #   Amazon — cookie_header (session cookies) + browser_storage_state
    # Pluto fetches session tokens live and Tubi caches its bearer in-memory, so
    # those self-heal with no cached state to clear. Stable per-install
    # identifiers (device_id, freecast client_id) are NOT account-bound and are
    # left untouched. An auth key the user explicitly set in this same save
    # (e.g. a freshly pasted Amazon cookie_header) is preserved.
    # secret_keys (schema-derived, computed above) is unioned in here so any
    # scraper's field marked secret=True is automatically watched for changes
    # — a hardcoded list would silently miss a future credential field.
    _CRED_IDENTITY_KEYS = {'username', 'email', 'amazon_email'}
    _CRED_KEYS   = _CRED_IDENTITY_KEYS | secret_keys
    # Sling's username/password only autofill the interactive browser sign-in
    # form (see sling.py ConfigField help text) — the live session lives in
    # the oauth_token/etc AUTH_STATE fields below, decoupled from these two.
    # Editing them shouldn't purge a working session or force a rescrape.
    if source.name == 'sling':
        _CRED_KEYS = _CRED_KEYS - {'username', 'password'}
    _AUTH_STATE  = ('access_token', 'refresh_token', 'token_time',
                    'bearer_token', 'activation_token', 'token_captured_at',
                    'client_context', 'cookies', 'identity_cookie',
                    'identity_cookie_expires_at', 'auth_method',
                    'session_id', 'login_time',
                    'cookie_header', 'browser_storage_state',
                    'browser_auth_token',
                    'oauth_token', 'oauth_token_secret', 'oauth_token_time',
                    'subscriber_id', 'account_status', 'legacy_subs', 'user_subs',
                    'user_dma', 'user_offset', 'user_zip')

    def _norm_cred(key: str, val) -> str:
        # Whitespace from autofill/paste shouldn't read as a credential change
        # for any field. Case only gets folded for identity fields (username/
        # email) — a secret's case always matters, so folding it there would
        # mask a real credential change and skip the auth-state purge below.
        s = str(val or '').strip()
        return s if key in secret_keys else s.casefold()

    creds_changed = any(
        f.key in _CRED_KEYS and _norm_cred(f.key, old.get(f.key)) != _norm_cred(f.key, current.get(f.key))
        for f in schema
    )
    def _toggle_enabled(cfg: dict, key: str) -> bool:
        return str(cfg.get(key, '')).strip().lower() in {'1', 'true', 'yes', 'on'}

    # Both toggles change the lineup. Only a subscription-mode change needs
    # the existing auth reset; FAST filtering must preserve the browser login.
    sling_subscription_changed = source.name == 'sling' and (
        _toggle_enabled(old, 'include_subscription_channels') != _toggle_enabled(current, 'include_subscription_channels')
    )
    sling_lineup_changed = source.name == 'sling' and (
        _toggle_enabled(old, 'include_subscription_channels') != _toggle_enabled(current, 'include_subscription_channels')
        or _toggle_enabled(old, 'exclude_fast_channels') != _toggle_enabled(current, 'exclude_fast_channels')
    )
    # DirecTV's FAST-exclusion toggle likewise changes the channel inventory
    # (filters out the 4xxx FAST channel-number range) rather than a playback
    # preference, so it also needs an immediate rescrape below.
    directv_lineup_changed = (
        source.name == 'directv'
        and _toggle_enabled(old, 'exclude_fast_channels') != _toggle_enabled(current, 'exclude_fast_channels')
    )
    if creds_changed or sling_subscription_changed:
        for tk in _AUTH_STATE:
            if data.get(tk) in (None, '', '••••••••'):  # skip values set in this save
                current.pop(tk, None)
    # Turning off PBS's curated station set drops those stations from the next
    # scrape's fetch_channels() result, but the normal reconcile path only marks
    # missed channels — it waits out a miss-threshold grace period before deleting,
    # so nothing visibly changes for hours. That grace period exists to protect
    # against a transient scrape failure, not an explicit "stop using this list"
    # config change, so purge immediately here instead — computed entirely from
    # config (no live PBS API calls, so no risk of a transient lookup failure
    # deleting real data). Only clear (non-DRM) channels are affected; DRM feeds
    # are never covered by the curated list.
    pbs_deleted = 0
    if source.name == 'pbs':
        def _pbs_truthy(v):
            return str(v or '').strip().lower() in {'1', 'true', 'yes', 'on'}
        old_curated = _pbs_truthy(old.get('include_preconfigured', True))
        new_curated = _pbs_truthy(current.get('include_preconfigured', True))
        if old_curated and not new_curated:
            from ..scrapers.pbs import _DEFAULT_STATIONS
            manual_station_ids = {
                entry.split(':', 1)[0].strip()
                for entry in re.split(r'[\s,]+', str(current.get('manual_feeds') or ''))
                if entry.strip()
            }
            curated_ids = set(_DEFAULT_STATIONS) - manual_station_ids
            if curated_ids:
                for ch in Channel.query.filter(Channel.source_id == source.id, Channel.stream_type != 'dash').all():
                    if (ch.source_channel_id or '').split(':', 1)[0] in curated_ids:
                        db.session.delete(ch)
                        pbs_deleted += 1

    source.config = current
    auto_enabled = False
    if (
        scraper_cls
        and source.name in {'pluto', 'localnow'}
        and not source.is_enabled
        and is_source_config_complete(source.name, scraper_cls, current)
    ):
        source.is_enabled = True
        auto_enabled = True
    db.session.commit()
    if pbs_deleted:
        _invalidate_and_refresh_xml()
    full_scrape_queued = False
    if source.name == 'sling' and (creds_changed or sling_lineup_changed) and source.is_enabled:
        # Credentials and the subscription/FAST-exclusion toggles can add/remove
        # channels. A normal scheduled scrape may be EPG-only for other sources,
        # so make this refresh deterministic and immediate after the config commit.
        trigger_scrape(source.name, force_full=True)
        full_scrape_queued = True
    elif source.name == 'directv' and directv_lineup_changed and source.is_enabled:
        # Same rationale as Sling above — the FAST-exclusion toggle changes
        # which channels come back from fetch_channels().
        trigger_scrape(source.name, force_full=True)
        full_scrape_queued = True
    elif source.name == 'pbs' and old != current and source.is_enabled:
        # Any PBS config change (curated toggle, ZIP codes, or hand-editing
        # manual_feeds directly) can change the channel set — same immediate-
        # refresh rationale as Sling above, rather than waiting up to
        # scrape_interval (360min) for the next scheduled scrape.
        trigger_scrape('pbs', force_full=True)
        full_scrape_queued = True
    config_complete = bool(scraper_cls and is_source_config_complete(source.name, scraper_cls, current))
    config_status = (
        'configured'
        if config_complete else
        ('required' if scraper_cls and getattr(scraper_cls, 'config_required', False) else 'optional')
    )
    return jsonify({
        'status': 'saved',
        'source': source.name,
        'is_enabled': source.is_enabled,
        'auto_enabled': auto_enabled,
        'full_scrape_queued': full_scrape_queued,
        'deleted_channels': pbs_deleted,
        'config_complete': config_complete,
        'config_status': config_status,
    })


# ── PBS Station & Feed Finder ───────────────────────────────────────────────
# Lets the admin search PBS stations by ZIP and hand-pick individual feeds —
# clear or DRM (Widevine) — that aren't already covered by the curated station
# list / ZIP codes config. DRM feeds play only via the PrismCast bridge.
# Manages the `manual_feeds` config list ("station_id:profile" pairs).

@sources_bp.route('/sources/<int:source_id>/pbs-station-search')
def pbs_station_search(source_id):
    source = Source.query.get_or_404(source_id)
    if source.name != 'pbs':
        return jsonify({'error': 'not a pbs source'}), 400
    zip_code = (request.args.get('zip') or '').strip()
    from ..scrapers.pbs import PBSScraper
    scraper = PBSScraper(config=source.config or {})
    try:
        stations = scraper.search_stations(zip_code)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.warning('[pbs-station-search] lookup failed for zip=%s: %s', zip_code, e)
        return jsonify({'error': f'PBS lookup failed: {e}'}), 502
    return jsonify({'stations': stations})


def _pbs_manual_feed_list(cfg: dict) -> list[dict]:
    """Combines manual_feeds (the scraper's source of truth, "station_id:profile"
    pairs) with the display-only manual_feed_station_names lookup, falling back to
    the raw station id for entries added before name-tracking existed."""
    from ..scrapers.pbs import PBSScraper
    station_names = cfg.get('manual_feed_station_names') or {}
    entries = []
    for raw in re.split(r'[\s,]+', str(cfg.get('manual_feeds') or '')):
        raw = raw.strip()
        if not raw:
            continue
        station_id, _, profile = raw.partition(':')
        if not station_id or not profile:
            continue
        entries.append({
            'station_id': station_id,
            'profile': profile,
            'label': PBSScraper._profile_label_for(profile),
            'station_name': station_names.get(station_id) or station_id,
        })
    return entries


@sources_bp.route('/sources/<int:source_id>/pbs-manual-feeds')
def pbs_manual_feeds(source_id):
    source = Source.query.get_or_404(source_id)
    if source.name != 'pbs':
        return jsonify({'error': 'not a pbs source'}), 400
    return jsonify({'feeds': _pbs_manual_feed_list(source.config or {})})


@sources_bp.route('/sources/<int:source_id>/pbs-add-manual-feed', methods=['POST'])
def pbs_add_manual_feed(source_id):
    source = Source.query.get_or_404(source_id)
    if source.name != 'pbs':
        return jsonify({'error': 'not a pbs source'}), 400
    from ..scrapers.pbs import PBSScraper, _NATIONAL_SECONDARY_PROFILES
    data = request.get_json() or {}
    station_id = (data.get('station_id') or '').strip()
    profile = (data.get('profile') or '').strip()
    if not PBSScraper._valid_station_id(station_id) or not profile:
        return jsonify({'error': 'invalid station_id or profile'}), 400
    display_name = (data.get('name') or '').strip()
    callsign = (data.get('callsign') or '').strip()
    if callsign and callsign not in display_name:
        display_name = f'{display_name} ({callsign})' if display_name else callsign

    # Create/FNX/NHK/World are the same national feed regardless of which station's
    # page you found them on — the scraper dedupes them across stations (first one
    # scraped wins), so adding a second station's copy is a silent no-op rather than
    # a distinct channel. Warn rather than let the "Added" response imply otherwise.
    warning = None
    if profile in _NATIONAL_SECONDARY_PROFILES:
        existing_national = Channel.query.filter(
            Channel.source_id == source.id,
            Channel.source_channel_id.like(f'%:{profile}:%'),
            db.not_(Channel.source_channel_id.like(f'{station_id}:%')),
        ).first()
        if existing_national:
            warning = (
                f"{PBSScraper._profile_label_for(profile)} is a shared national feed — "
                f"{existing_national.name} already provides it, so this station's copy "
                f"won't create an additional channel."
            )

    cfg = dict(source.config or {})
    existing = [s.strip() for s in re.split(r'[\s,]+', str(cfg.get('manual_feeds') or '')) if s.strip()]
    key = f'{station_id}:{profile}'
    is_new = key not in existing
    if is_new:
        existing.append(key)
    names = dict(cfg.get('manual_feed_station_names') or {})
    if display_name:
        names[station_id] = display_name
    cfg['manual_feeds'] = ', '.join(existing)
    cfg['manual_feed_station_names'] = names
    source.config = cfg
    db.session.commit()
    # The feed won't exist until the scraper actually runs — without this, it
    # wouldn't appear until the next scheduled scrape (scrape_interval=360min).
    # Same immediate-refresh rationale as Sling's config save (credentials/lineup
    # changes that add channels trigger a forced full scrape rather than making
    # the user find "Run now" themselves).
    scrape_queued = False
    if is_new and source.is_enabled:
        trigger_scrape('pbs', force_full=True)
        scrape_queued = True
    return jsonify({
        'status': 'ok', 'feeds': _pbs_manual_feed_list(cfg),
        'scrape_queued': scrape_queued, 'warning': warning,
    })


@sources_bp.route('/sources/<int:source_id>/pbs-remove-manual-feed', methods=['POST'])
def pbs_remove_manual_feed(source_id):
    source = Source.query.get_or_404(source_id)
    if source.name != 'pbs':
        return jsonify({'error': 'not a pbs source'}), 400
    from ..scrapers.pbs import PBSScraper
    data = request.get_json() or {}
    station_id = (data.get('station_id') or '').strip()
    profile = (data.get('profile') or '').strip()
    # station_id/profile feed the LIKE pattern below that deletes channels;
    # validate station_id as a UUID (can't contain % or _) and reject LIKE
    # wildcard characters in profile so a bad/malicious value can't broaden
    # the delete beyond the one feed being removed.
    if not PBSScraper._valid_station_id(station_id) or not profile or '%' in profile or '_' in profile:
        return jsonify({'error': 'invalid station_id or profile'}), 400
    key = f'{station_id}:{profile}'

    cfg = dict(source.config or {})
    existing = [s.strip() for s in re.split(r'[\s,]+', str(cfg.get('manual_feeds') or '')) if s.strip()]
    existing = [s for s in existing if s != key]
    cfg['manual_feeds'] = ', '.join(existing)
    if not any(s.startswith(f'{station_id}:') for s in existing):
        names = dict(cfg.get('manual_feed_station_names') or {})
        names.pop(station_id, None)
        cfg['manual_feed_station_names'] = names
    source.config = cfg

    # The scraper won't emit this feed on the next scrape once it's out of
    # manual_feeds, but that scrape may be hours away (scrape_interval) and the
    # reconcile miss-threshold would otherwise leave the stale row active in the
    # meantime. Delete it immediately, same as removing/disabling a source deletes
    # its channels — cascades to programs (ORM) and feed_channel_numbers (DB
    # ondelete=CASCADE). Filters on the exact profile so sibling feeds from the
    # same station (e.g. keeping Main while removing PBS KIDS) are untouched.
    deleted = 0
    if station_id and profile:
        orphans = Channel.query.filter(
            Channel.source_id == source.id,
            Channel.source_channel_id.like(f'{station_id}:{profile}:%'),
        ).all()
        for ch in orphans:
            db.session.delete(ch)
            deleted += 1

    db.session.commit()
    if deleted:
        _invalidate_and_refresh_xml()
    return jsonify({'status': 'ok', 'feeds': _pbs_manual_feed_list(cfg), 'deleted_channels': deleted})


# ── Sling interactive browser login ─────────────────────────────────────────
# A real, human-operated sign-in: a Camoufox (anti-detect Firefox) tab loads
# sling.com in the 'fast' RQ worker, auto-fills the saved credentials, and the
# admin UI streams periodic screenshots of it and forwards the admin's own
# clicks/keystrokes back — so a real person solves the real hCaptcha
# challenge. The job captures the OAuth token from the auth-callback URL (or
# localStorage as a fallback) once sign-in succeeds and saves it. State is
# relayed through Redis (not held in this process) since screenshot/input
# requests can land on a different gunicorn worker than started the job.

@sources_bp.route('/sources/<int:source_id>/sling-browser-login/start', methods=['POST'])
def sling_browser_login_start(source_id):
    from .tasks import trigger_sling_browser_login

    source = Source.query.get_or_404(source_id)
    if source.name != 'sling':
        return jsonify({'error': 'not a sling source'}), 400
    started = trigger_sling_browser_login()
    return jsonify({'status': 'started' if started else 'already_running'})


@sources_bp.route('/sources/<int:source_id>/sling-browser-login/state')
def sling_browser_login_state(source_id):
    import base64
    import redis as _redis

    source = Source.query.get_or_404(source_id)
    if source.name != 'sling':
        return jsonify({'error': 'not a sling source'}), 400
    r = _redis.from_url(current_app.config['REDIS_URL'])
    raw_status = r.get('sling:browser-login:status')
    result = json.loads(raw_status) if raw_status else {'state': 'idle'}
    shot = r.get('sling:browser-login:screenshot')
    if shot:
        result['screenshot'] = base64.b64encode(shot).decode('ascii')
    return jsonify(result)


@sources_bp.route('/sources/<int:source_id>/sling-browser-login/input', methods=['POST'])
def sling_browser_login_input(source_id):
    import redis as _redis

    source = Source.query.get_or_404(source_id)
    if source.name != 'sling':
        return jsonify({'error': 'not a sling source'}), 400
    data = request.get_json() or {}
    kind = data.get('type')
    if kind in ('click', 'mousemove', 'mousedown', 'mouseup'):
        try:
            payload = {'type': kind, 'x': float(data['x']), 'y': float(data['y'])}
        except (KeyError, TypeError, ValueError):
            return jsonify({'error': f'{kind} requires numeric x/y'}), 400
    elif kind == 'key':
        key = str(data.get('key') or '')
        if not key:
            return jsonify({'error': 'key requires a non-empty key'}), 400
        payload = {'type': 'key', 'key': key}
    else:
        return jsonify({'error': 'invalid input type'}), 400
    r = _redis.from_url(current_app.config['REDIS_URL'])
    r.rpush('sling:browser-login:input', json.dumps(payload))
    r.expire('sling:browser-login:input', 60)
    return jsonify({'status': 'ok'})


@sources_bp.route('/sources/<int:source_id>/sling-browser-login/stop', methods=['POST'])
def sling_browser_login_stop(source_id):
    from .tasks import stop_sling_browser_login

    source = Source.query.get_or_404(source_id)
    if source.name != 'sling':
        return jsonify({'error': 'not a sling source'}), 400
    stop_sling_browser_login()
    return jsonify({'status': 'stopping'})


# ── Amazon auto-login ──────────────────────────────────────────────────────────

@sources_bp.route('/sources/<int:source_id>/amazon-auto-login', methods=['POST'])
def amazon_auto_login(source_id):
    import threading
    from ..scrapers.amazon_auth import run_amazon_auth

    source = Source.query.get_or_404(source_id)
    if source.name != 'amazon_prime_free':
        return jsonify({'error': 'not an amazon source'}), 400

    cfg = source.config or {}
    email = (cfg.get('amazon_email') or '').strip()
    password = (cfg.get('amazon_password') or '').strip()
    if not email or not password:
        return jsonify({'error': 'amazon_email and amazon_password must be saved first'}), 400

    redis_url = current_app.config['REDIS_URL']
    storage_state_json = cfg.get('browser_storage_state') or None

    t = threading.Thread(
        target=run_amazon_auth,
        args=(redis_url, source_id, email, password, storage_state_json),
        daemon=True,
    )
    t.start()
    return jsonify({'status': 'started'})


@sources_bp.route('/sources/<int:source_id>/amazon-auth', methods=['DELETE'])
def clear_amazon_auth(source_id):
    """Forget Amazon sign-in state but retain the user's saved credentials.

    This is a recovery path for password/account changes and newly enabled MFA.
    Playback envelopes and resolved URLs are tied to the old Amazon session, so
    discard those along with the browser cookies.
    """
    import redis as _redis

    source = Source.query.get_or_404(source_id)
    if source.name != 'amazon_prime_free':
        return jsonify({'error': 'not an amazon source'}), 400

    cfg = dict(source.config or {})
    cfg.pop('cookie_header', None)
    cfg.pop('browser_storage_state', None)
    source.config = cfg
    SourceCache.query.filter(
        SourceCache.source_id == source.id,
        SourceCache.cache_key.in_(('channel_pe', 'stream_url_cache')),
    ).delete(synchronize_session=False)
    db.session.commit()

    # Remove any completed or waiting browser-login state so it cannot be
    # picked up after the user has explicitly cleared the account session.
    try:
        r = _redis.from_url(current_app.config['REDIS_URL'])
        r.delete(
            f'amazon:auth:status:{source_id}',
            f'amazon:auth:otp:{source_id}',
            f'amazon:auth:result:{source_id}',
        )
    except Exception as exc:
        # The database state is already safely cleared; Redis is only
        # short-lived login coordination state.
        logger.warning('[amazon-auth] unable to clear Redis state for source_id=%s: %s',
                       source_id, exc)

    logger.info('[amazon-auth] cleared saved session for source_id=%s', source_id)
    return jsonify({'status': 'cleared'})


@sources_bp.route('/sources/<int:source_id>/philo-login/send-code', methods=['POST'])
def philo_login_send_code(source_id):
    """Step 1 of Philo's passwordless sign-in: email/phone → Philo sends a 6-digit code.

    Stashes the sign-in context (anon cookies + device_ident) in Redis, keyed by
    source, so the verify step can complete the exchange."""
    import redis as _redis
    from ..scrapers.philo import PhiloScraper

    source = Source.query.get_or_404(source_id)
    if source.name != 'philo':
        return jsonify({'error': 'not a philo source'}), 400
    data = request.get_json() or {}
    ident = (data.get('ident') or '').strip()
    if not ident:
        # Fall back to the email saved on the source, if any.
        ident = ((source.config or {}).get('email') or '').strip()
    try:
        ctx = PhiloScraper.request_login_code(ident, voice=bool(data.get('voice')))
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    r = _redis.from_url(current_app.config['REDIS_URL'])
    r.setex(f'philo:login:ctx:{source_id}', 900, json.dumps(ctx))
    return jsonify({'status': 'sent', 'ident': ident, 'can_resend': ctx.get('can_resend', True)})


@sources_bp.route('/sources/<int:source_id>/philo-login/verify', methods=['POST'])
def philo_login_verify(source_id):
    """Step 2: submit the code → persist the (~1-year) session cookies to the source."""
    import redis as _redis
    from ..scrapers.philo import PhiloScraper

    source = Source.query.get_or_404(source_id)
    if source.name != 'philo':
        return jsonify({'error': 'not a philo source'}), 400
    data = request.get_json() or {}
    code = (data.get('code') or '').strip()
    r = _redis.from_url(current_app.config['REDIS_URL'])
    raw = r.get(f'philo:login:ctx:{source_id}')
    if not raw:
        return jsonify({'error': 'Sign-in session expired — request a new code.'}), 400
    try:
        result = PhiloScraper.verify_login_code(json.loads(raw), code)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    cfg = dict(source.config or {})
    cfg['session_cookies'] = result['session_cookies']
    cfg['session_born'] = _time.time()
    if result.get('user_id'):
        cfg['philo_user_id'] = result['user_id']
    cfg.pop('player_id', None)   # re-mint a player for the new session on next resolve
    source.config = cfg
    db.session.commit()
    r.delete(f'philo:login:ctx:{source_id}')
    config_complete = bool(is_source_config_complete('philo', registry.get('philo'), cfg))
    return jsonify({'status': 'signed_in', 'config_complete': config_complete})


@sources_bp.route('/sources/<int:source_id>/amazon-auth-status')
def amazon_auth_status(source_id):
    import redis as _redis
    r = _redis.from_url(current_app.config['REDIS_URL'])

    raw = r.get(f'amazon:auth:status:{source_id}')
    if not raw:
        return jsonify({'status': 'idle'})

    data = json.loads(raw)

    if data.get('status') == 'success':
        # Atomically claim the result — only the first poller that wins the
        # getdel persists the cookies; subsequent polls see None and skip.
        result_raw = r.getdel(f'amazon:auth:result:{source_id}')
        if result_raw:
            try:
                result = json.loads(result_raw)
                source = Source.query.get(source_id)
                if source:
                    cfg = dict(source.config or {})
                    cfg['cookie_header'] = result['cookie_header']
                    cfg['browser_storage_state'] = result['storage_state']
                    source.config = cfg
                    db.session.commit()
                    logger.info('[amazon-auth] persisted cookies to source config source_id=%s', source_id)
            except Exception as exc:
                logger.error('[amazon-auth] failed to persist result: %s', exc)

    return jsonify(data)


@sources_bp.route('/sources/<int:source_id>/amazon-auth-otp', methods=['POST'])
def amazon_auth_otp(source_id):
    import redis as _redis
    data = request.get_json() or {}
    otp = (data.get('otp') or '').strip()
    if not otp:
        return jsonify({'error': 'otp required'}), 400
    r = _redis.from_url(current_app.config['REDIS_URL'])
    r.set(f'amazon:auth:otp:{source_id}', otp, ex=300)
    return jsonify({'status': 'ok'})


# -- DirecTV Stream auto-login ---------------------------------------------
# No OTP route: the captured flow does normal email/password login. If DirecTV
# serves MFA or a bot check, directv.py reports it through the status poll.

@sources_bp.route('/sources/<int:source_id>/directv-auto-login', methods=['POST'])
def directv_auto_login(source_id):
    import threading
    from ..scrapers.directv import run_directv_auth

    source = Source.query.get_or_404(source_id)
    if source.name != 'directv':
        return jsonify({'error': 'not a directv source'}), 400

    cfg = source.config or {}
    username = (cfg.get('username') or '').strip()
    password = (cfg.get('password') or '').strip()
    if not username or not password:
        return jsonify({'error': 'username and password must be saved first'}), 400

    redis_url = current_app.config['REDIS_URL']
    app = current_app._get_current_object()

    t = threading.Thread(
        target=run_directv_auth,
        args=(redis_url, source_id, username, password, app),
        daemon=True,
    )
    t.start()
    return jsonify({'status': 'started'})


@sources_bp.route('/sources/<int:source_id>/directv-auth-status')
def directv_auth_status(source_id):
    import redis as _redis
    r = _redis.from_url(current_app.config['REDIS_URL'])

    raw = r.get(f'directv:auth:status:{source_id}')
    if not raw:
        return jsonify({'status': 'idle'})

    data = json.loads(raw)

    if data.get('status') == 'success':
        # Atomically claim the result -- only the first poller that wins the
        # getdel persists the token; subsequent polls see None and skip.
        result_raw = r.getdel(f'directv:auth:result:{source_id}')
        if result_raw:
            try:
                result = json.loads(result_raw)
                source = Source.query.get(source_id)
                if source:
                    cfg = dict(source.config or {})
                    cfg['bearer_token'] = result['bearer_token']
                    if result.get('refresh_token'):
                        cfg['refresh_token'] = result['refresh_token']
                    if result.get('client_context'):
                        cfg['client_context'] = result['client_context']
                    else:
                        cfg.pop('client_context', None)
                    if result.get('activation_token'):
                        cfg['activation_token'] = result['activation_token']
                    cfg['cookies'] = result.get('cookies') or []
                    cfg['token_captured_at'] = result['captured_at']
                    if result.get('identity_cookie'):
                        cfg['identity_cookie'] = result['identity_cookie']
                    else:
                        cfg.pop('identity_cookie', None)
                    if result.get('identity_cookie_expires_at'):
                        cfg['identity_cookie_expires_at'] = result['identity_cookie_expires_at']
                    else:
                        cfg.pop('identity_cookie_expires_at', None)
                    if result.get('auth_method'):
                        cfg['auth_method'] = result['auth_method']
                    source.config = cfg
                    db.session.commit()
                    logger.info('[directv-auth] persisted session to source config source_id=%s', source_id)
            except Exception as exc:
                logger.error('[directv-auth] failed to persist result: %s', exc)

    return jsonify(data)

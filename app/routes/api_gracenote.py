import json
import logging

logger = logging.getLogger(__name__)
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request, current_app
from ..extensions import db
from ..models import Source, Channel, AppSettings
from ..scrapers import registry
from ..gracenote_suggest import SuggestionChannel, suggest_gracenote_matches
from ..gracenote_map import lookup_gracenote, fetch_remote_gracenote_map, remote_map_status

from .api_shared import _apply_gracenote_update, _invalidate_and_refresh_xml

gracenote_bp = Blueprint('api_gracenote', __name__)


def _community_gracenote_match(ch) -> dict | None:
    """Look up ch's community CSV mapping.

    Most scrapers key the CSV by source_channel_id, but some (Cox, PBS, NBC TVE,
    part of DirecTV) key it by a call sign, brand, or slugified name instead —
    see BaseScraper.community_map_keys(). Ask the scraper class for the right
    candidate key(s) rather than assuming source_channel_id everywhere."""
    source_name = ch.source.name if ch.source else ''
    if not source_name:
        return None
    scraper_cls = registry.get(source_name)
    keys = scraper_cls.community_map_keys(ch) if scraper_cls else (
        [ch.source_channel_id] if ch.source_channel_id else []
    )
    for key in keys:
        match = lookup_gracenote(source_name, key)
        if match:
            return match
    return None


def _gracenote_source_for(ch) -> str | None:
    """Return the provenance of ch.gracenote_id: 'manual', 'csv', 'native', or None."""
    if not ch.gracenote_id:
        return None
    mode = getattr(ch, 'gracenote_mode', None)
    locked = getattr(ch, 'gracenote_locked', False)
    if mode == 'manual' or locked:
        return 'manual'
    csv_match = _community_gracenote_match(ch)
    if csv_match and csv_match.get('tmsid') == ch.gracenote_id:
        return 'csv'
    return 'native'


def _csv_suggestion_for(ch) -> dict | None:
    """Return the CSV mapping entry for this channel, if one exists."""
    match = _community_gracenote_match(ch)
    if not match:
        return None
    return {
        'tmsid': match.get('tmsid'),
        'notes': match.get('notes') or '',
    }


@gracenote_bp.route('/channels/<int:channel_id>/gracenote-suggestions', methods=['GET'])
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


@gracenote_bp.route('/gracenote-search', methods=['GET'])
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


@gracenote_bp.route('/gracenote/community-summary', methods=['GET'])
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
        match = _community_gracenote_match(ch)
        if not match or not match.get('tmsid'):
            continue
        total += 1
        if (ch.gracenote_id or '') == match['tmsid']:
            applied += 1
        elif (ch.gracenote_mode or 'auto') not in ('manual', 'off'):
            # Only count as available if auto-mode — manual/off overrides are intentional
            available += 1
    return jsonify({'total': total, 'applied': applied, 'available': available})


@gracenote_bp.route('/gracenote/community-map', methods=['GET'])
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
        match = _community_gracenote_match(ch)
        if not match or not match.get('tmsid'):
            continue
        source_name = ch.source.name if ch.source else ''
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


@gracenote_bp.route('/gracenote/community-apply-all', methods=['POST'])
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
        match = _community_gracenote_match(ch)
        if not match or not match.get('tmsid'):
            continue
        source_name = ch.source.name if ch.source else ''
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


@gracenote_bp.route('/gracenote/community-clear-all', methods=['POST'])
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
        match = _community_gracenote_match(ch)
        if not match or not match.get('tmsid'):
            continue
        source_name = ch.source.name if ch.source else ''

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


@gracenote_bp.route('/gracenote/my-contributions', methods=['GET'])
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
        match = _community_gracenote_match(ch)
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


@gracenote_bp.route('/gracenote/submit-contributions', methods=['POST'])
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


@gracenote_bp.route('/gracenote/community-export', methods=['GET'])
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


@gracenote_bp.route('/gracenote/remote-map/status', methods=['GET'])
def gracenote_remote_map_status():
    settings = AppSettings.get()
    status = remote_map_status()
    status['url'] = settings.effective_gracenote_map_url()
    status['url_is_default'] = not (settings.gracenote_map_url or '').strip()
    return jsonify(status)


@gracenote_bp.route('/gracenote/remote-map/refresh', methods=['POST'])
def gracenote_remote_map_refresh():
    settings = AppSettings.get()
    url = settings.effective_gracenote_map_url()
    success, message = fetch_remote_gracenote_map(url)
    status = remote_map_status()
    status['url'] = url
    return jsonify({'ok': success, 'message': message, **status}), (200 if success else 502)



import logging

logger = logging.getLogger(__name__)

from flask import Blueprint, jsonify, request
from ..extensions import db
from ..models import Source, Channel, Program, Feed
from ..generators.m3u import (
    feed_to_query_filters,
    _selected_channel_stubs,
    _resolve_chnum_map,
    feed_namespace_start,
)

duplicates_bp = Blueprint('api_duplicates', __name__)


@duplicates_bp.route('/channels/<int:channel_id>/duplicates', methods=['GET'])
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
            'logo_display_url': c.logo_display_url,
            'is_duplicate':   c.is_duplicate,
            'is_enabled':     c.is_enabled,
            'is_active':      c.is_active,
            'scrape_pinned':  bool(c.scrape_pinned),
            'disable_reason': c.disable_reason,
            'missed_scrapes': c.missed_scrapes or 0,
            'category':       c.category,
            'gracenote_id':   c.gracenote_id,
            'gracenote_mode': c.gracenote_mode or 'auto',
            'program_count':  prog_counts.get(c.id, 0),
            'stream_info': {
                'max_resolution': si.get('max_resolution'),
                'max_height':     si.get('max_height'),
                'video_codec':    si.get('video_codec'),
                'has_4k':         si.get('has_4k', False),
                'has_hd':         si.get('has_hd', False),
                'resolution_estimated': si.get('resolution_estimated', False),
                'drm':            si.get('drm', False),
            } if si else None,
        }

    return jsonify({
        'channel': _fmt(ch),
        'strict':  [_fmt(c) for c in strict],
        'soft':    [_fmt(c) for c in soft],
        'gn':      [_fmt(c) for c in gn_matches],
    })


@duplicates_bp.route('/channels/<int:channel_id>/feed-membership', methods=['GET'])
def channel_feed_membership(channel_id):
    """Return each non-default feed's membership status for a channel."""
    from ..generators.m3u import _build_channel_query
    feeds = Feed.query.filter(Feed.is_enabled == True, Feed.slug != 'default').order_by(Feed.name).all()
    result = []
    for feed in feeds:
        filters = feed.filters or {}
        pinned   = channel_id in (filters.get('pinned_channel_ids')   or [])
        excluded = channel_id in (filters.get('excluded_channel_ids') or [])
        if pinned:
            status = 'pinned'
        elif excluded:
            status = 'excluded'
        else:
            q_filters = feed_to_query_filters(filters)
            # activity='any' so a bridge-only channel (carried by the feed's PrismCast
            # variant) still counts as a member rather than reporting 'absent'.
            in_filter = _build_channel_query(q_filters, activity='any').filter(Channel.id == channel_id).count() > 0
            status = 'filtered' if in_filter else 'absent'
        feed_channel_number = None
        if status != 'absent':
            q_filters = feed_to_query_filters(filters)
            std_channels = _selected_channel_stubs(q_filters, gracenote=False)
            namespace_start = None if feed.chnum_start is not None else feed_namespace_start(feed, gracenote=False)
            chnum_map, _ = _resolve_chnum_map(
                std_channels,
                feed_chnum_start=feed.chnum_start,
                namespace_start=namespace_start,
                feed_id=feed.id if feed.chnum_start is not None else None,
            )
            feed_channel_number = chnum_map.get(channel_id)
        result.append({
            'feed_id': feed.id,
            'status': status,
            'feed_channel_number': feed_channel_number,
        })
    return jsonify(result)


@duplicates_bp.route('/channels/duplicate-summary', methods=['GET'])
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


@duplicates_bp.route('/channels/resolve-duplicates', methods=['POST'])
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
        return ch.disable_reason in ('Dead', 'VOD') or (ch.disable_reason or '').startswith('DRM') or not ch.is_active

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

    from ..scrapers.registry import get as _get_scraper
    # Lower score = higher priority. 'partial' sits between full and basic.
    _EPG_QUALITY_SCORE = {'full': 0.0, 'partial': 0.3, 'basic': 0.6}

    def epg_score(ch):
        """0.0 = full EPG, 0.3 = partial (desc but no art), 0.6 = basic (titles only), 1.0 = no EPG."""
        if not has_epg(ch):
            return 1.0
        scraper_cls = _get_scraper(ch.source.name)
        quality = getattr(scraper_cls, 'epg_quality', 'full') if scraper_cls else 'full'
        return _EPG_QUALITY_SCORE.get(quality, 0.0)

    def priority_key(ch):
        try:
            source_rank = priority.index(ch.source.name)
        except ValueError:
            source_rank = len(priority)  # unlisted sources rank last
        return (
            1 if is_unhealthy(ch) else 0,
            0 if has_gracenote(ch) else 1,
            epg_score(ch),
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



"""
Feed management API endpoints.
Mounted at /api/feeds by app/__init__.py.
"""
import re
from flask import Blueprint, jsonify, request
from sqlalchemy.exc import OperationalError
from ..extensions import db
from ..generators.m3u import get_global_chnum_overlaps, _selected_channel_stubs, feed_to_query_filters
from ..models import Channel, Feed, FeedChannelNumber, Source
from ..url import public_base_url
from ..xml_cache import delete_xml_artifact, invalidate_xml_cache
from .tasks import trigger_xml_refresh, trigger_xml_refresh_catchup

feeds_api_bp = Blueprint('feeds_api', __name__)
SYSTEM_FEED_SLUGS = {'default'}


def _invalidate_and_refresh_xml() -> None:
    invalidate_xml_cache()
    trigger_xml_refresh()


def _safe_commit():
    """Commit session, returning a 503 response tuple if SQLite is locked."""
    try:
        db.session.commit()
        return None
    except OperationalError as exc:
        if 'database is locked' in str(exc).lower():
            db.session.rollback()
            return jsonify({'error': 'Server is busy (a scrape is in progress). Please try again in a moment.'}), 503
        raise


def _safe_flush():
    """Flush session, returning a 503 response tuple if SQLite is locked."""
    try:
        db.session.flush()
        return None
    except OperationalError as exc:
        if 'database is locked' in str(exc).lower():
            db.session.rollback()
            return jsonify({'error': 'Server is busy (a scrape is in progress). Please try again in a moment.'}), 503
        raise


def _slugify(text: str) -> str:
    s = text.lower().strip()
    s = re.sub(r'[^a-z0-9]+', '-', s)
    return s.strip('-')[:64]


@feeds_api_bp.route('/channel-membership', methods=['GET'])
def channel_membership():
    """Return {channel_id: [feed_name, ...]} for all non-default enabled feeds."""
    from ..generators.m3u import _build_channel_query, feed_to_query_filters

    exclude_id = request.args.get('exclude_id', type=int)
    feeds = Feed.query.filter_by(is_enabled=True).order_by(Feed.name).all()
    result: dict[int, list[str]] = {}

    for feed in feeds:
        if feed.slug == 'default':
            continue
        if exclude_id and feed.id == exclude_id:
            continue

        f_filters = feed.filters or {}
        pinned = set(int(i) for i in (f_filters.get('pinned_channel_ids') or []))
        q_filters = feed_to_query_filters(f_filters)
        matched_ids = set(
            r[0] for r in _build_channel_query(q_filters)
            .with_entities(Channel.id)
            .all()
        )

        for ch_id in pinned | matched_ids:
            result.setdefault(ch_id, []).append(feed.name)

    return jsonify(result)


@feeds_api_bp.route('/chnum-ranges', methods=['GET'])
def chnum_ranges():
    """Return the occupied channel number ranges for the master M3U and every enabled feed."""
    from ..generators.m3u import _build_channel_query, feed_namespace_start
    ranges = []
    exclude_id = request.args.get('exclude_id', type=int)

    # Per-feed ranges — gracenote and standard share the same pool, so the
    # reported range covers both (start to start + std_count + gn_count - 1).
    feeds = Feed.query.filter_by(is_enabled=True).order_by(Feed.name).all()
    for feed in feeds:
        if exclude_id and feed.id == exclude_id:
            continue
        filters = feed_to_query_filters(feed.filters or {})
        std_count = len(_selected_channel_stubs(filters, gracenote=False))
        gn_count  = len(_selected_channel_stubs(filters, gracenote=True))
        total_count = std_count + gn_count
        if total_count == 0:
            continue
        if feed.chnum_start:
            start = feed.chnum_start
        else:
            start = feed_namespace_start(feed, gracenote=False)
        ranges.append({
            'feed_id':   feed.id,
            'feed_name': feed.name,
            'start':     start,
            'end':       start + total_count - 1,
            'count':     std_count,
            'gn_count':  gn_count,
            'explicit':  bool(feed.chnum_start),
        })
    return jsonify(ranges)


def _feed_member_stubs(q_filters: dict):
    """
    Load a feed's member channels (both M3U partitions) as lightweight stubs,
    sorted by master number — the same combined ordering the auto-numbering
    worker uses.  Returns (stubs, gracenote_id_set).
    """
    from ..generators.m3u import _selected_channel_stubs
    std = _selected_channel_stubs(q_filters, gracenote=False)
    gn  = _selected_channel_stubs(q_filters, gracenote=True)
    gn_ids = {ch.id for ch in gn}
    stubs = sorted(
        std + gn,
        key=lambda ch: (ch.number is None, ch.number or 0, (ch.name or '').lower()),
    )
    return stubs, gn_ids


@feeds_api_bp.route('/preview-order', methods=['POST'])
def preview_order():
    """
    Anticipated channel list + tvg-chno for a feed definition, computed by the
    same code paths the generator uses (read-only despite POST — the filters
    payload is too large for a query string).

    Body: {filters, chnum_start, feed_id, order}
      - order (optional): explicit channel-id sequence to overlay, so the UI can
        preview an unsaved drag-drop ordering merged with current membership.
    """
    from ..generators.m3u import (
        _build_feed_chnum_map, build_manual_order_map, feed_namespace_start,
    )
    data = request.get_json() or {}
    filters = _clean_filters(data.get('filters') or {})
    q_filters = feed_to_query_filters(filters)

    feed = Feed.query.get(data['feed_id']) if data.get('feed_id') else None

    start = _parse_chnum_start(data.get('chnum_start'))
    if start is None and feed is not None:
        start = feed.chnum_start or feed_namespace_start(feed, gracenote=False)
    if start is None:
        start = 1000

    stubs, gn_ids = _feed_member_stubs(q_filters)

    order_ids = data.get('order') or []
    if order_ids:
        try:
            order_ids = [int(i) for i in order_ids]
        except (TypeError, ValueError):
            return jsonify({'error': 'order must contain integer channel ids'}), 400
        num_map = build_manual_order_map(stubs, order_ids, start)
    else:
        stored = {}
        if feed is not None:
            stored = {
                r.channel_id: r.number
                for r in FeedChannelNumber.query.filter_by(feed_id=feed.id).all()
            }
        num_map = _build_feed_chnum_map(stubs, start, stored_numbers=stored)

    feed_pinned_ids = set(filters.get('pinned_channel_ids') or [])
    rows = [{
        'id':          ch.id,
        'name':        ch.name,
        'source':      ch.source.display_name or ch.source.name,
        'number':      num_map.get(ch.id),
        'pinned':      bool(getattr(ch, 'number_pinned', False) and ch.number is not None),
        'feed_pinned': ch.id in feed_pinned_ids,
        'gracenote':   ch.id in gn_ids,
    } for ch in stubs]
    rows.sort(key=lambda r: (r['number'] is None, r['number'] or 0, r['name'].lower()))
    return jsonify({'start': start, 'channels': rows})


@feeds_api_bp.route('/<int:feed_id>/channel-order', methods=['PUT'])
def set_channel_order(feed_id):
    """
    Persist a manual drag-drop ordering as this feed's channel numbers by
    rewriting its FeedChannelNumber pool sequentially from chnum_start.
    The sticky-number refresh preserves these on later scrapes; channels that
    newly match the filters append after the ordered block.
    """
    from ..generators.m3u import build_manual_order_map, feed_namespace_start
    feed = Feed.query.get_or_404(feed_id)
    if feed.slug in SYSTEM_FEED_SLUGS:
        return jsonify({'error': 'Built-in feeds cannot be reordered.'}), 403

    data = request.get_json() or {}
    order_ids = data.get('order')
    if not isinstance(order_ids, list) or not order_ids:
        return jsonify({'error': 'order (list of channel ids) is required'}), 400
    try:
        order_ids = [int(i) for i in order_ids]
    except (TypeError, ValueError):
        return jsonify({'error': 'order must contain integer channel ids'}), 400

    baseline_warnings = set(get_global_chnum_overlaps())

    # A manual order needs a durable explicit start — the namespace auto-start
    # shifts when feeds are added/renamed, which would drop stored numbers below
    # the new floor and silently shred the user's ordering.
    if 'chnum_start' in data:
        explicit = _parse_chnum_start(data['chnum_start'])
        if explicit is not None:
            feed.chnum_start = explicit
    if feed.chnum_start is None:
        feed.chnum_start = feed_namespace_start(feed, gracenote=False)

    q_filters = feed_to_query_filters(feed.filters or {})
    stubs, _ = _feed_member_stubs(q_filters)
    num_map = build_manual_order_map(stubs, order_ids, feed.chnum_start)

    existing = {r.channel_id: r for r in FeedChannelNumber.query.filter_by(feed_id=feed.id).all()}
    for cid, num in num_map.items():
        if cid in existing:
            if existing[cid].number != num:
                existing[cid].number = num
        else:
            db.session.add(FeedChannelNumber(feed_id=feed.id, channel_id=cid, number=num))
    for cid, row in existing.items():
        if cid not in num_map:
            db.session.delete(row)

    err = _safe_flush()
    if err:
        return err
    new_warnings = [w for w in get_global_chnum_overlaps() if w not in baseline_warnings]
    if new_warnings:
        db.session.rollback()
        return jsonify({'error': 'Channel number overlaps detected', 'warnings': new_warnings}), 409
    err = _safe_commit()
    if err:
        return err
    _invalidate_and_refresh_xml()
    return jsonify(feed.to_dict(public_base_url()))


@feeds_api_bp.route('', methods=['GET'])
def list_feeds():
    base_url = public_base_url()
    feeds = Feed.query.order_by(Feed.name).all()
    return jsonify([f.to_dict(base_url) for f in feeds])


@feeds_api_bp.route('', methods=['POST'])
def create_feed():
    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'name is required'}), 400

    slug = data.get('slug') or _slugify(name)
    if Feed.query.filter_by(slug=slug).first():
        return jsonify({'error': f'slug "{slug}" already exists'}), 409

    # Capture baseline overlaps among existing feeds BEFORE adding the new feed,
    # so we can block only on overlaps that the new feed itself introduces.
    baseline_warnings = set(get_global_chnum_overlaps())

    feed = Feed(
        slug        = slug,
        name        = name,
        description = data.get('description', ''),
        filters     = _clean_filters(data.get('filters', {})),
        chnum_start = _parse_chnum_start(data.get('chnum_start')),
        is_enabled  = data.get('is_enabled', True),
    )
    db.session.add(feed)
    err = _safe_flush()  # make new feed visible to overlap check
    if err:
        return err
    # A new chnum_start feed needs its FeedChannelNumber store populated before
    # the overlap check / XML refresh, so its std and gracenote M3Us draw from one
    # unified pool instead of both numbering from chnum_start and colliding.
    if feed.chnum_start is not None:
        from ..worker import _refresh_auto_channel_numbers
        _refresh_auto_channel_numbers()
        err = _safe_flush()
        if err:
            return err
    new_warnings = [w for w in get_global_chnum_overlaps() if w not in baseline_warnings]
    if new_warnings:
        db.session.rollback()
        return jsonify({'error': 'Channel number overlaps detected', 'warnings': new_warnings}), 409
    err = _safe_commit()
    if err:
        return err
    invalidate_xml_cache()
    if not trigger_xml_refresh():
        # A refresh was already running when the feed was created; it won't include
        # this new feed.  Queue a catch-up job that will run after it finishes.
        trigger_xml_refresh_catchup()
    return jsonify(feed.to_dict(public_base_url())), 201


@feeds_api_bp.route('/<int:feed_id>', methods=['GET'])
def get_feed(feed_id):
    feed = Feed.query.get_or_404(feed_id)
    return jsonify(feed.to_dict(public_base_url()))


@feeds_api_bp.route('/<int:feed_id>', methods=['PATCH'])
def update_feed(feed_id):
    feed = Feed.query.get_or_404(feed_id)
    data = request.get_json() or {}

    # Snapshot baseline overlaps before applying changes so we only block on
    # overlaps that this edit introduces, not pre-existing ones.
    baseline_warnings = set(get_global_chnum_overlaps())

    if feed.slug in SYSTEM_FEED_SLUGS:
        # System feeds only allow chnum_start to be changed.
        disallowed = set(data.keys()) - {'chnum_start'}
        if disallowed:
            return jsonify({'error': 'Built-in feeds cannot be edited.'}), 403
    else:
        if 'name' in data:
            feed.name = data['name'].strip()
        if 'description' in data:
            feed.description = data['description']
        if 'filters' in data:
            feed.filters = _clean_filters(data['filters'])
        if 'is_enabled' in data:
            feed.is_enabled = bool(data['is_enabled'])

    if 'chnum_start' in data:
        feed.chnum_start = _parse_chnum_start(data['chnum_start'])

    err = _safe_flush()  # make changes visible to overlap check
    if err:
        return err
    # Changing chnum_start or filters alters which channels this feed numbers and
    # from where.  For unified-pool (chnum_start) feeds the std and gracenote M3Us
    # rely on the persisted FeedChannelNumber store to avoid colliding with each
    # other, so the store must be rebuilt BEFORE we regenerate XML or check for
    # overlaps — otherwise both partitions number sequentially from chnum_start
    # and every gracenote channel collides with a standard one.
    if any(k in data for k in ('chnum_start', 'filters', 'is_enabled')):
        from ..worker import _refresh_auto_channel_numbers
        _refresh_auto_channel_numbers()
        err = _safe_flush()
        if err:
            return err
    new_warnings = [w for w in get_global_chnum_overlaps() if w not in baseline_warnings]
    if new_warnings:
        db.session.rollback()
        return jsonify({'error': 'Channel number overlaps detected', 'warnings': new_warnings}), 409
    err = _safe_commit()
    if err:
        return err
    _invalidate_and_refresh_xml()
    return jsonify(feed.to_dict(public_base_url()))


def _reset_default_channel_numbers() -> None:
    """Force a fresh global/master numbering pass for channels affected by the master start."""
    channels = (
        Channel.query
        .join(Source)
        .filter(
            Channel.is_active == True,
            Channel.is_enabled == True,
            Channel.number_pinned == False,
            Source.is_enabled == True,
            Source.epg_only == False,
            Channel.stream_url != None,
            (
                (Source.chnum_start == None)
                | ((Channel.gracenote_id != None) & (Channel.gracenote_id != ''))
            ),
        )
        .all()
    )
    for channel in channels:
        channel.number = None


@feeds_api_bp.route('/<int:feed_id>/reset-channel-numbers', methods=['POST'])
def reset_feed_channel_numbers(feed_id):
    feed = Feed.query.get_or_404(feed_id)

    if feed.slug == 'default':
        _reset_default_channel_numbers()
        # The default feed can also carry an explicit chnum_start, in which case
        # it has its own FeedChannelNumber store driving its playlist numbers.
        # Nulling master Channel.number alone leaves that store untouched, and
        # _refresh_auto_channel_numbers keeps the old sticky numbers (>= chnum_start)
        # — making the reset a no-op for this feed.  Clear it so it rebuilds fresh.
        if feed.chnum_start is not None:
            for row in FeedChannelNumber.query.filter_by(feed_id=feed.id).all():
                db.session.delete(row)
    else:
        if feed.chnum_start is None:
            return jsonify({'error': 'Set a Channel Number Start before resetting this feed.'}), 400
        for row in FeedChannelNumber.query.filter_by(feed_id=feed.id).all():
            db.session.delete(row)

    err = _safe_flush()
    if err:
        return err

    from ..worker import _refresh_auto_channel_numbers
    _refresh_auto_channel_numbers()

    err = _safe_commit()
    if err:
        return err
    _invalidate_and_refresh_xml()
    return jsonify({
        'status': 'reset',
        'message': 'Channel numbers regenerated from the current start value.',
        'feed': feed.to_dict(public_base_url()),
    })


@feeds_api_bp.route('/<int:feed_id>', methods=['DELETE'])
def delete_feed(feed_id):
    feed = Feed.query.get_or_404(feed_id)
    if feed.slug in SYSTEM_FEED_SLUGS:
        return jsonify({'error': 'Built-in feeds cannot be deleted.'}), 403
    slug = feed.slug
    db.session.delete(feed)
    err = _safe_commit()
    if err:
        return err
    delete_xml_artifact(f'feed-{slug}')
    delete_xml_artifact(f'feed-{slug}-native')
    delete_xml_artifact(f'feed-{slug}-native-m3u')
    _invalidate_and_refresh_xml()
    return jsonify({'status': 'deleted', 'slug': slug})


@feeds_api_bp.route('/<int:feed_id>/pin', methods=['POST'])
def pin_channel(feed_id):
    feed = Feed.query.get_or_404(feed_id)
    if feed.slug in SYSTEM_FEED_SLUGS:
        return jsonify({'error': 'Built-in feeds cannot be modified.'}), 403
    data = request.get_json() or {}
    try:
        channel_id = int(data.get('channel_id', 0))
    except (TypeError, ValueError):
        channel_id = 0
    if not channel_id:
        return jsonify({'error': 'channel_id is required'}), 400
    filters = dict(feed.filters or {})
    pinned = list(filters.get('pinned_channel_ids', []))
    if channel_id not in pinned:
        pinned.append(channel_id)
        filters['pinned_channel_ids'] = pinned
        feed.filters = filters
        err = _safe_commit()
        if err:
            return err
        _invalidate_and_refresh_xml()
    return jsonify({'status': 'pinned', 'feed_id': feed_id, 'channel_id': channel_id})


@feeds_api_bp.route('/<int:feed_id>/pin/<int:channel_id>', methods=['DELETE'])
def unpin_channel(feed_id, channel_id):
    feed = Feed.query.get_or_404(feed_id)
    if feed.slug in SYSTEM_FEED_SLUGS:
        return jsonify({'error': 'Built-in feeds cannot be modified.'}), 403
    filters = dict(feed.filters or {})
    pinned = [i for i in filters.get('pinned_channel_ids', []) if i != channel_id]
    if pinned:
        filters['pinned_channel_ids'] = pinned
    else:
        filters.pop('pinned_channel_ids', None)
    feed.filters = filters
    err = _safe_commit()
    if err:
        return err
    _invalidate_and_refresh_xml()
    return jsonify({'status': 'unpinned', 'feed_id': feed_id, 'channel_id': channel_id})


@feeds_api_bp.route('/<int:feed_id>/exclude', methods=['POST'])
def exclude_channel(feed_id):
    feed = Feed.query.get_or_404(feed_id)
    if feed.slug in SYSTEM_FEED_SLUGS:
        return jsonify({'error': 'Built-in feeds cannot be modified.'}), 403
    data = request.get_json() or {}
    try:
        channel_id = int(data.get('channel_id', 0))
    except (TypeError, ValueError):
        channel_id = 0
    if not channel_id:
        return jsonify({'error': 'channel_id is required'}), 400
    filters = dict(feed.filters or {})
    excluded = list(filters.get('excluded_channel_ids', []))
    if channel_id not in excluded:
        excluded.append(channel_id)
        filters['excluded_channel_ids'] = excluded
        feed.filters = filters
        err = _safe_commit()
        if err:
            return err
        _invalidate_and_refresh_xml()
    return jsonify({'status': 'excluded', 'feed_id': feed_id, 'channel_id': channel_id})


@feeds_api_bp.route('/<int:feed_id>/exclude/<int:channel_id>', methods=['DELETE'])
def unexclude_channel(feed_id, channel_id):
    feed = Feed.query.get_or_404(feed_id)
    if feed.slug in SYSTEM_FEED_SLUGS:
        return jsonify({'error': 'Built-in feeds cannot be modified.'}), 403
    filters = dict(feed.filters or {})
    excluded = [i for i in filters.get('excluded_channel_ids', []) if i != channel_id]
    if excluded:
        filters['excluded_channel_ids'] = excluded
    else:
        filters.pop('excluded_channel_ids', None)
    feed.filters = filters
    err = _safe_commit()
    if err:
        return err
    _invalidate_and_refresh_xml()
    return jsonify({'status': 'unexcluded', 'feed_id': feed_id, 'channel_id': channel_id})


def _parse_chnum_start(val) -> int | None:
    """Coerce chnum_start to a positive int, or None to clear it."""
    if val is None or val == '':
        return None
    try:
        n = int(val)
        return n if n > 0 else None
    except (ValueError, TypeError):
        return None


def _clean_filters(raw: dict) -> dict:
    """
    Normalise and validate the filters dict.
    Only store keys that have actual values — omit nulls so the query
    builder treats them as 'no filter on this dimension'.
    """
    out = {}
    if channel_ids := raw.get('channel_ids'):
        out['channel_ids'] = [int(i) for i in channel_ids if str(i).isdigit() or isinstance(i, int)]
        if max_ch := raw.get('max_channels'):
            try:
                out['max_channels'] = max(1, int(max_ch))
            except (ValueError, TypeError):
                pass
        return out  # channel_ids overrides all other filters
    if sources := raw.get('sources'):
        out['sources'] = [str(s) for s in sources if s]
    if categories := raw.get('categories'):
        out['categories'] = [str(c) for c in categories if c]
    if languages := raw.get('languages'):
        out['languages'] = [str(l) for l in languages if l]
    elif language := raw.get('language'):
        # backward compat with old single-language saves
        out['languages'] = [str(language)]
    if countries := raw.get('countries'):
        out['countries'] = [str(c) for c in countries if c]
    if gracenote := raw.get('gracenote'):
        if gracenote in ('has', 'missing'):
            out['gracenote'] = gracenote
    if excluded_ids := raw.get('excluded_channel_ids'):
        out['excluded_channel_ids'] = [int(i) for i in excluded_ids if str(i).isdigit() or isinstance(i, int)]
    if pinned_ids := raw.get('pinned_channel_ids'):
        out['pinned_channel_ids'] = [int(i) for i in pinned_ids if str(i).isdigit() or isinstance(i, int)]
    if max_ch := raw.get('max_channels'):
        try:
            out['max_channels'] = max(1, int(max_ch))
        except (ValueError, TypeError):
            pass
    return out

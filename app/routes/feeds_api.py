"""
Feed management API endpoints.
Mounted at /api/feeds by app/__init__.py.
"""
import re
from flask import Blueprint, jsonify, request
from sqlalchemy.exc import OperationalError
from ..extensions import db
from ..generators.m3u import get_global_chnum_overlaps, _selected_channels, feed_to_query_filters
from ..models import Feed
from ..url import public_base_url
from ..xml_cache import delete_xml_artifact, invalidate_xml_cache
from .tasks import trigger_xml_refresh

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


@feeds_api_bp.route('/chnum-ranges', methods=['GET'])
def chnum_ranges():
    """Return the occupied channel number ranges for the master M3U and every enabled feed.

    Uses COUNT queries instead of loading all channel objects so this stays fast
    even with thousands of channels.
    """
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
        std_filters = {**filters, 'gracenote': 'missing'}
        std_count = _build_channel_query(std_filters).count()
        gn_filters  = {**filters, 'gracenote': 'has'}
        gn_count    = _build_channel_query(gn_filters).count()
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
    new_warnings = [w for w in get_global_chnum_overlaps() if w not in baseline_warnings]
    if new_warnings:
        db.session.rollback()
        return jsonify({'error': 'Channel number overlaps detected', 'warnings': new_warnings}), 409
    err = _safe_commit()
    if err:
        return err
    _invalidate_and_refresh_xml()
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
    new_warnings = [w for w in get_global_chnum_overlaps() if w not in baseline_warnings]
    if new_warnings:
        db.session.rollback()
        return jsonify({'error': 'Channel number overlaps detected', 'warnings': new_warnings}), 409
    err = _safe_commit()
    if err:
        return err
    _invalidate_and_refresh_xml()
    return jsonify(feed.to_dict(public_base_url()))


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

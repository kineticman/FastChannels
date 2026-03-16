"""
Feed management API endpoints.
Mounted at /api/feeds by app/__init__.py.
"""
import re
from flask import Blueprint, jsonify, request
from ..extensions import db
from ..generators.m3u import get_global_chnum_overlaps, _selected_channels, feed_to_query_filters
from ..models import Feed
from ..url import public_base_url
from ..xml_cache import invalidate_xml_cache

feeds_api_bp = Blueprint('feeds_api', __name__)
SYSTEM_FEED_SLUGS = {'default'}


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
    from ..models import AppSettings
    ranges = []
    exclude_id = request.args.get('exclude_id', type=int)

    # Master M3U: count non-gracenote enabled channels; gracenote channels live at 100,000+
    master_count = _build_channel_query({'gracenote': 'missing'}).count()
    if master_count:
        master_start = AppSettings.get().effective_global_chnum_start()
        ranges.append({
            'feed_id':   None,
            'feed_name': 'Master M3U',
            'start':     master_start,
            'end':       master_start + max(master_count, 1) - 1,
            'count':     master_count,
            'explicit':  True,
        })

    # Per-feed ranges
    feeds = Feed.query.filter_by(is_enabled=True).order_by(Feed.name).all()
    for feed in feeds:
        if exclude_id and feed.id == exclude_id:
            continue
        filters = feed_to_query_filters(feed.filters or {})
        # Standard M3U excludes gracenote channels; gracenote M3U is the complement.
        # Both start at the same chnum_start, so use std_count for range end.
        std_filters = {**filters, 'gracenote': 'missing'}
        std_count = _build_channel_query(std_filters).count()
        gn_filters = {**filters, 'gracenote': 'has'}
        gn_count  = _build_channel_query(gn_filters).count()
        if std_count + gn_count == 0:
            continue
        if feed.chnum_start:
            start = feed.chnum_start
        else:
            start = feed_namespace_start(feed, gracenote=False)
        ranges.append({
            'feed_id':   feed.id,
            'feed_name': feed.name,
            'start':     start,
            'end':       start + max(std_count, 1) - 1,
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

    feed = Feed(
        slug        = slug,
        name        = name,
        description = data.get('description', ''),
        filters     = _clean_filters(data.get('filters', {})),
        chnum_start = _parse_chnum_start(data.get('chnum_start')),
        is_enabled  = data.get('is_enabled', True),
    )
    db.session.add(feed)
    with db.session.no_autoflush:
        warnings = get_global_chnum_overlaps()
    if warnings:
        db.session.rollback()
        return jsonify({'error': 'Channel number overlaps detected', 'warnings': warnings}), 409
    db.session.commit()
    invalidate_xml_cache()
    return jsonify(feed.to_dict(public_base_url())), 201


@feeds_api_bp.route('/<int:feed_id>', methods=['GET'])
def get_feed(feed_id):
    feed = Feed.query.get_or_404(feed_id)
    return jsonify(feed.to_dict(public_base_url()))


@feeds_api_bp.route('/<int:feed_id>', methods=['PATCH'])
def update_feed(feed_id):
    feed = Feed.query.get_or_404(feed_id)
    if feed.slug in SYSTEM_FEED_SLUGS:
        return jsonify({'error': 'Built-in feeds cannot be edited.'}), 403
    data = request.get_json() or {}

    if 'name' in data:
        feed.name = data['name'].strip()
    if 'description' in data:
        feed.description = data['description']
    if 'filters' in data:
        feed.filters = _clean_filters(data['filters'])
    if 'chnum_start' in data:
        feed.chnum_start = _parse_chnum_start(data['chnum_start'])
    if 'is_enabled' in data:
        feed.is_enabled = bool(data['is_enabled'])

    with db.session.no_autoflush:
        warnings = get_global_chnum_overlaps()
    if warnings:
        db.session.rollback()
        return jsonify({'error': 'Channel number overlaps detected', 'warnings': warnings}), 409
    db.session.commit()
    invalidate_xml_cache()
    return jsonify(feed.to_dict(public_base_url()))


@feeds_api_bp.route('/<int:feed_id>', methods=['DELETE'])
def delete_feed(feed_id):
    feed = Feed.query.get_or_404(feed_id)
    if feed.slug in SYSTEM_FEED_SLUGS:
        return jsonify({'error': 'Built-in feeds cannot be deleted.'}), 403
    db.session.delete(feed)
    db.session.commit()
    invalidate_xml_cache()
    return jsonify({'status': 'deleted', 'slug': feed.slug})


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
    if gracenote := raw.get('gracenote'):
        if gracenote in ('has', 'missing'):
            out['gracenote'] = gracenote
    if excluded_ids := raw.get('excluded_channel_ids'):
        out['excluded_channel_ids'] = [int(i) for i in excluded_ids if str(i).isdigit() or isinstance(i, int)]
    if max_ch := raw.get('max_channels'):
        try:
            out['max_channels'] = max(1, int(max_ch))
        except (ValueError, TypeError):
            pass
    return out

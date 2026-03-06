"""
Feed management API endpoints.
Mounted at /api/feeds by app/__init__.py.
"""
import re
from flask import Blueprint, jsonify, request
from ..extensions import db
from ..models import Feed

feeds_api_bp = Blueprint('feeds_api', __name__)


def _slugify(text: str) -> str:
    s = text.lower().strip()
    s = re.sub(r'[^a-z0-9]+', '-', s)
    return s.strip('-')[:64]


@feeds_api_bp.route('', methods=['GET'])
def list_feeds():
    base_url = request.host_url.rstrip('/')
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
        is_enabled  = data.get('is_enabled', True),
    )
    db.session.add(feed)
    db.session.commit()
    return jsonify(feed.to_dict(request.host_url.rstrip('/'))), 201


@feeds_api_bp.route('/<int:feed_id>', methods=['GET'])
def get_feed(feed_id):
    feed = Feed.query.get_or_404(feed_id)
    return jsonify(feed.to_dict(request.host_url.rstrip('/')))


@feeds_api_bp.route('/<int:feed_id>', methods=['PATCH'])
def update_feed(feed_id):
    feed = Feed.query.get_or_404(feed_id)
    data = request.get_json() or {}

    if 'name' in data:
        feed.name = data['name'].strip()
    if 'description' in data:
        feed.description = data['description']
    if 'filters' in data:
        feed.filters = _clean_filters(data['filters'])
    if 'is_enabled' in data:
        feed.is_enabled = bool(data['is_enabled'])

    db.session.commit()
    return jsonify(feed.to_dict(request.host_url.rstrip('/')))


@feeds_api_bp.route('/<int:feed_id>', methods=['DELETE'])
def delete_feed(feed_id):
    feed = Feed.query.get_or_404(feed_id)
    db.session.delete(feed)
    db.session.commit()
    return jsonify({'status': 'deleted', 'slug': feed.slug})


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
    if max_ch := raw.get('max_channels'):
        try:
            out['max_channels'] = max(1, int(max_ch))
        except (ValueError, TypeError):
            pass
    return out

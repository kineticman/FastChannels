import re
from flask import Blueprint, jsonify, request
from ..extensions import db
from ..models import Source, Channel
from ..scrapers import registry
from .tasks import trigger_scrape, trigger_drm_check
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


@api_bp.route('/sources/<int:source_id>/drm-check', methods=['POST'])
def drm_check_source(source_id):
    source = Source.query.get_or_404(source_id)
    trigger_drm_check(source.name)
    return jsonify({'status': 'queued', 'source': source.name})


@api_bp.route('/sources/<int:source_id>', methods=['PATCH'])
def update_source(source_id):
    source = Source.query.get_or_404(source_id)
    data = request.get_json()
    if 'is_enabled' in data:
        source.is_enabled = bool(data['is_enabled'])
    if 'scrape_interval' in data:
        source.scrape_interval = int(data['scrape_interval'])
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
    for field in ('name', 'logo_url', 'category', 'is_active', 'is_enabled', 'number'):
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

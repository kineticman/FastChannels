from flask import Blueprint, render_template, request
from ..extensions import db
from ..models import Source, Channel, Feed, AppSettings
from ..generators.m3u import _parse_gracenote_id, get_chnum_overlaps, _build_source_chnum_map
from ..scrapers import registry as _scraper_registry

admin_bp = Blueprint('admin', __name__, template_folder='../templates')


@admin_bp.route('/')
def dashboard():
    sources        = Source.query.order_by(Source.display_name).all()
    total_channels = Channel.query.filter_by(is_active=True, is_enabled=True).count()
    base_url       = request.host_url.rstrip('/')
    feeds          = Feed.query.filter_by(is_enabled=True).order_by(Feed.name).all()
    gracenote_count = sum(
        1 for ch in Channel.query.filter_by(is_active=True, is_enabled=True).all()
        if _parse_gracenote_id(ch)
    )
    return render_template('admin/dashboard.html', sources=sources,
                           total_channels=total_channels, base_url=base_url,
                           feeds=feeds, gracenote_count=gracenote_count)


@admin_bp.route('/sources')
def sources():
    chnum_warnings = get_chnum_overlaps()
    all_scrapers   = _scraper_registry.get_all()
    audit_enabled  = {
        name: getattr(cls, 'stream_audit_enabled', False)
        for name, cls in all_scrapers.items()
    }
    return render_template('admin/sources.html',
                           sources=Source.query.order_by(Source.display_name).all(),
                           chnum_warnings=chnum_warnings,
                           audit_enabled=audit_enabled)


@admin_bp.route('/channels')
def channels():
    page             = request.args.get('page', 1, type=int)
    source_filter    = request.args.get('source', '')
    search           = request.args.get('search', '')
    enabled_filter   = request.args.get('enabled', '')
    drm_filter       = request.args.get('drm', '')
    language_filter  = request.args.get('language', '')
    category_filter  = request.args.get('category', '')
    sort_by          = request.args.get('sort', 'name')
    sort_dir         = request.args.get('dir', 'asc')

    q = Channel.query.join(Source)

    # Status filter — can show inactive DRM/Dead channels or exclude them
    if drm_filter == '1':
        q = q.filter(Channel.disable_reason == 'DRM')
    elif drm_filter == 'dead':
        q = q.filter(Channel.disable_reason == 'Dead')
    elif drm_filter == '0':
        q = q.filter(Channel.is_active == True).filter(
            db.or_(Channel.disable_reason == None, Channel.disable_reason != 'DRM')
        )
    else:
        q = q.filter(Channel.is_active == True)

    if enabled_filter == '1':
        q = q.filter(Channel.is_enabled == True)
    elif enabled_filter == '0':
        q = q.filter(Channel.is_enabled == False)

    if source_filter:
        q = q.filter(Source.name == source_filter)
    if language_filter:
        q = q.filter(Channel.language == language_filter)
    if category_filter:
        q = q.filter(Channel.category == category_filter)
    if search:
        q = q.filter(Channel.name.ilike(f'%{search}%'))

    _sort_cols = {
        'name':     [Channel.name],
        'source':   [Source.display_name, Channel.name],
        'category': [Channel.category, Channel.name],
        # Approximate M3U order: sources with explicit start first, then by source name + channel name
        'number':   [db.func.coalesce(Source.chnum_start, 999999), Source.display_name, Channel.name],
    }
    _cols = _sort_cols.get(sort_by, [Channel.name])
    if sort_dir == 'desc':
        _order = [c.desc() for c in _cols]
    else:
        _order = [c.asc() for c in _cols]

    channels = q.order_by(*_order).paginate(page=page, per_page=50, error_out=False)
    sources  = Source.query.order_by(Source.display_name).all()

    # Build computed channel number map for display
    all_active = Channel.query.join(Source).filter(Channel.is_active == True).all()
    chnum_map, _ = _build_source_chnum_map(all_active)

    lang_rows = db.session.query(Channel.language, db.func.count(Channel.id))\
        .filter(Channel.is_active == True, Channel.language != None)\
        .group_by(Channel.language)\
        .order_by(Channel.language).all()
    languages = [(lang, count) for lang, count in lang_rows]

    cat_rows = db.session.query(Channel.category, db.func.count(Channel.id))\
        .filter(Channel.is_active == True, Channel.category != None)\
        .group_by(Channel.category)\
        .order_by(Channel.category).all()
    categories = [(cat, count) for cat, count in cat_rows]

    return render_template('admin/channels.html',
                           channels=channels, sources=sources,
                           source_filter=source_filter, search=search,
                           enabled_filter=enabled_filter, drm_filter=drm_filter,
                           language_filter=language_filter, languages=languages,
                           category_filter=category_filter, categories=categories,
                           sort_by=sort_by, sort_dir=sort_dir,
                           chnum_map=chnum_map)


@admin_bp.route('/feeds')
def feeds():
    sources    = Source.query.filter_by(is_enabled=True).order_by(Source.display_name).all()
    feeds      = Feed.query.order_by(Feed.name).all()
    cats = db.session.query(Channel.category)\
        .filter(Channel.is_active == True, Channel.category != None)\
        .distinct().order_by(Channel.category).all()
    categories = [c[0] for c in cats]
    langs = db.session.query(Channel.language)\
        .filter(Channel.is_active == True, Channel.language != None)\
        .distinct().order_by(Channel.language).all()
    languages  = [{'code': r[0], 'label': r[0]} for r in langs]
    base_url   = request.host_url.rstrip('/')
    return render_template('admin/feeds.html',
                           feeds=feeds, sources=sources,
                           categories=categories, languages=languages,
                           base_url=base_url)


@admin_bp.route('/settings')
def settings():
    app_settings = AppSettings.get()
    return render_template('admin/settings.html',
                           global_chnum_start=app_settings.global_chnum_start)


@admin_bp.route('/logs')
def logs():
    return render_template('admin/logs.html')

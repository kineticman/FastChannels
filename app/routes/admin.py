from datetime import datetime, timezone
from flask import Blueprint, render_template, request
from sqlalchemy import select, case
from ..extensions import db
from ..models import Source, Channel, Feed, AppSettings
from ..generators.m3u import (
    _parse_gracenote_id,
    _build_source_chnum_map,
    _build_channel_query,
    feed_namespace_start,
)
from ..scrapers import registry as _scraper_registry
from ..url import public_base_url, detected_base_url

admin_bp = Blueprint('admin', __name__, template_folder='../templates')


@admin_bp.route('/')
def dashboard():
    sources        = Source.query.order_by(Source.display_name).all()
    total_channels = Channel.query.filter_by(is_active=True, is_enabled=True).count()
    base_url       = public_base_url()
    feeds          = Feed.query.filter_by(is_enabled=True).order_by(Feed.name).all()
    source_output_meta = {}
    for source in sources:
        channels = _build_channel_query({'source': [source.name]}).all()
        source_output_meta[source.id] = {
            'channel_count': len(channels),
        }
    return render_template('admin/dashboard.html', sources=sources,
                           total_channels=total_channels, base_url=base_url,
                           feeds=feeds, source_output_meta=source_output_meta,
                           now=datetime.now(timezone.utc))


@admin_bp.route('/sources')
def sources():
    all_scrapers   = _scraper_registry.get_all()
    audit_enabled  = {
        name: getattr(cls, 'stream_audit_enabled', False)
        for name, cls in all_scrapers.items()
    }
    return render_template('admin/sources.html',
                           sources=Source.query.order_by(Source.display_name).all(),
                           chnum_warnings=[],
                           audit_enabled=audit_enabled)


@admin_bp.route('/channels')
def channels():
    page             = request.args.get('page', 1, type=int)
    source_filter    = request.args.get('source', '')
    search           = request.args.get('search', '')
    enabled_filter   = request.args.get('enabled', '')
    drm_filter       = request.args.get('drm', '')
    gracenote_filter = request.args.get('gracenote', '')
    language_filter  = request.args.get('language', '')
    category_filter  = request.args.get('category', '')
    duplicates_filter = request.args.get('duplicates', '')
    sort_by          = request.args.get('sort', 'name')
    sort_dir         = request.args.get('dir', 'asc')

    q = Channel.query.join(Source)

    # Status filter — admin always shows all channels regardless of is_active
    if drm_filter == '1':
        q = q.filter(Channel.disable_reason == 'DRM')
    elif drm_filter == 'dead':
        q = q.filter(Channel.disable_reason == 'Dead')
    elif drm_filter == '0':
        q = q.filter(Channel.disable_reason == None)

    if enabled_filter == '1':
        q = q.filter(Channel.is_enabled == True)
    elif enabled_filter == '0':
        q = q.filter(Channel.is_enabled == False)

    if gracenote_filter == '1':
        q = q.filter(Channel.gracenote_id != None, Channel.gracenote_id != '')
    elif gracenote_filter == '0':
        q = q.filter((Channel.gracenote_id == None) | (Channel.gracenote_id == ''))

    if source_filter:
        q = q.filter(Source.name == source_filter)
    if language_filter:
        q = q.filter(Channel.language == language_filter)
    if category_filter:
        q = q.filter(Channel.category == category_filter)
    if search:
        q = q.filter(Channel.name.ilike(f'%{search}%'))

    if duplicates_filter == '1':
        dup_names_sq = select(Channel.name)\
            .group_by(Channel.name)\
            .having(db.func.count(Channel.id) > 1)
        q = q.filter(db.or_(Channel.name.in_(dup_names_sq), Channel.is_duplicate == True))

    sort_name = case(
        (db.func.lower(Channel.name).like('the %'), db.func.lower(db.func.substr(Channel.name, 5))),
        (db.func.lower(Channel.name).like('an %'),  db.func.lower(db.func.substr(Channel.name, 4))),
        (db.func.lower(Channel.name).like('a %'),   db.func.lower(db.func.substr(Channel.name, 3))),
        else_=db.func.lower(Channel.name),
    )

    _sort_cols = {
        'name':     [sort_name, Channel.name],
        'source':   [Source.display_name, Channel.name],
        'category': [Channel.category, Channel.name],
        # Approximate M3U order: sources with explicit chnum_start first, then by
        # actual channel number within each source block, then name as tiebreak.
        'number':   [db.func.coalesce(Source.chnum_start, 999999), db.func.coalesce(Channel.number, 999999), Source.display_name, sort_name, Channel.name],
    }
    _cols = _sort_cols.get(sort_by, [Channel.name])
    if sort_dir == 'desc':
        _order = [c.desc() for c in _cols]
    else:
        _order = [c.asc() for c in _cols]

    channels = q.order_by(*_order).paginate(page=page, per_page=50, error_out=False)
    sources_q = Source.query.filter(Source.is_enabled == True)
    if source_filter:
        sources_q = sources_q.union(
            Source.query.filter(Source.name == source_filter)
        )
    sources = sources_q.order_by(Source.display_name).all()

    # Build computed channel number map for display
    all_active = Channel.query.join(Source).filter(Channel.is_active == True).all()
    chnum_map, _ = _build_source_chnum_map(all_active)

    lang_rows = db.session.query(Channel.language, db.func.count(Channel.id))\
        .filter(Channel.language != None)\
        .group_by(Channel.language)\
        .order_by(Channel.language).all()
    languages = [(lang, count) for lang, count in lang_rows]

    cat_rows = db.session.query(Channel.category, db.func.count(Channel.id))\
        .filter(Channel.category != None)\
        .group_by(Channel.category)\
        .order_by(Channel.category).all()
    categories = [(cat, count) for cat, count in cat_rows]

    # Compute duplicate name set for visual grouping in template
    dup_name_rows = db.session.query(Channel.name)\
        .group_by(Channel.name)\
        .having(db.func.count(Channel.id) > 1).all()
    duplicate_names = {row[0] for row in dup_name_rows}

    return render_template('admin/channels.html',
                           channels=channels, sources=sources,
                           source_filter=source_filter, search=search,
                           enabled_filter=enabled_filter, drm_filter=drm_filter,
                           gracenote_filter=gracenote_filter,
                           language_filter=language_filter, languages=languages,
                           category_filter=category_filter, categories=categories,
                           duplicates_filter=duplicates_filter,
                           duplicate_names=duplicate_names,
                           sort_by=sort_by, sort_dir=sort_dir,
                           chnum_map=chnum_map)


@admin_bp.route('/feeds')
def feeds():
    app_settings = AppSettings.get()
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
    base_url   = public_base_url()
    default_feed = next((f for f in feeds if f.slug == 'default'), None)
    # chnum_start is now the single source of truth for all feeds including default.
    # Show the auto-assigned namespace as placeholder for feeds without an explicit value.
    feed_chnum_placeholder = {}
    for feed in feeds:
        if feed.chnum_start is None and feed.slug != 'default':
            feed_chnum_placeholder[feed.id] = feed_namespace_start(feed, gracenote=False)
    return render_template('admin/feeds.html',
                           feeds=feeds, sources=sources,
                           categories=categories, languages=languages,
                           base_url=base_url,
                           feed_chnum_placeholder=feed_chnum_placeholder,
                           default_chnum_from_env=default_feed and default_feed.chnum_start is None and app_settings.env_global_chnum_start() is not None)


@admin_bp.route('/settings')
def settings():
    app_settings = AppSettings.get()
    request_base_url = request.host_url.rstrip('/')
    return render_template('admin/settings.html',
                           channels_dvr_url=app_settings.effective_channels_dvr_url() or '',
                           public_base_url=app_settings.effective_public_base_url() or '',
                           channels_dvr_url_from_env=(not (app_settings.channels_dvr_url or '').strip()) and app_settings.env_channels_dvr_url() is not None,
                           public_base_url_from_env=(not (app_settings.public_base_url or '').strip()) and app_settings.env_public_base_url() is not None,
                           request_base_url=request_base_url,
                           detected_base_url=detected_base_url())


@admin_bp.route('/logs')
def logs():
    return render_template('admin/logs.html')


@admin_bp.route('/help')
def help():
    return render_template('admin/help.html')

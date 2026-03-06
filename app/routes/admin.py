from flask import Blueprint, render_template, request
from ..extensions import db
from ..models import Source, Channel, Feed
from ..generators.m3u import _parse_gracenote_id

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
    return render_template('admin/sources.html',
                           sources=Source.query.order_by(Source.display_name).all())


@admin_bp.route('/channels')
def channels():
    page            = request.args.get('page', 1, type=int)
    source_filter   = request.args.get('source', '')
    search          = request.args.get('search', '')
    enabled_filter  = request.args.get('enabled', '')
    drm_filter      = request.args.get('drm', '')
    language_filter = request.args.get('language', '')

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
    if search:
        q = q.filter(Channel.name.ilike(f'%{search}%'))

    channels = q.order_by(Channel.name).paginate(page=page, per_page=50, error_out=False)
    sources  = Source.query.order_by(Source.display_name).all()

    lang_rows = db.session.query(Channel.language, db.func.count(Channel.id))\
        .filter(Channel.is_active == True, Channel.language != None)\
        .group_by(Channel.language)\
        .order_by(Channel.language).all()
    languages = [(lang, count) for lang, count in lang_rows]

    return render_template('admin/channels.html',
                           channels=channels, sources=sources,
                           source_filter=source_filter, search=search,
                           enabled_filter=enabled_filter, drm_filter=drm_filter,
                           language_filter=language_filter, languages=languages)


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
    return render_template('admin/settings.html')


@admin_bp.route('/logs')
def logs():
    return render_template('admin/logs.html')

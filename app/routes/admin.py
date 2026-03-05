from flask import Blueprint, render_template, request
from ..extensions import db
from ..models import Source, Channel, Feed

admin_bp = Blueprint('admin', __name__, template_folder='../templates')


@admin_bp.route('/')
def dashboard():
    sources        = Source.query.order_by(Source.display_name).all()
    feeds          = Feed.query.filter_by(is_enabled=True).order_by(Feed.name).all()
    total_channels = Channel.query.filter_by(is_active=True, is_enabled=True).count()
    base_url       = request.host_url.rstrip('/')
    return render_template('admin/dashboard.html', sources=sources, feeds=feeds,
                           total_channels=total_channels, base_url=base_url)


@admin_bp.route('/sources')
def sources():
    return render_template('admin/sources.html',
                           sources=Source.query.order_by(Source.display_name).all())


@admin_bp.route('/channels')
def channels():
    page           = request.args.get('page', 1, type=int)
    source_filter  = request.args.get('source', '')
    search         = request.args.get('search', '')
    enabled_filter = request.args.get('enabled', '')

    q = Channel.query.join(Source).filter(Channel.is_active == True)
    if source_filter:
        q = q.filter(Source.name == source_filter)
    if search:
        q = q.filter(Channel.name.ilike(f'%{search}%'))
    if enabled_filter == '1':
        q = q.filter(Channel.is_enabled == True)
    elif enabled_filter == '0':
        q = q.filter(Channel.is_enabled == False)

    channels = q.order_by(Channel.name).paginate(page=page, per_page=50, error_out=False)
    sources  = Source.query.order_by(Source.display_name).all()
    return render_template('admin/channels.html',
                           channels=channels, sources=sources,
                           source_filter=source_filter, search=search,
                           enabled_filter=enabled_filter)


@admin_bp.route('/feeds')
def feeds():
    sources     = Source.query.filter_by(is_enabled=True).order_by(Source.display_name).all()
    feeds       = Feed.query.order_by(Feed.name).all()
    # Distinct categories across all active channels for the filter builder
    from ..models import Channel as Ch
    cats = db.session.query(Ch.category)\
        .filter(Ch.is_active == True, Ch.category != None)\
        .distinct().order_by(Ch.category).all()
    categories = [c[0] for c in cats]
    base_url   = request.host_url.rstrip('/')
    return render_template('admin/feeds.html',
                           feeds=feeds, sources=sources,
                           categories=categories, base_url=base_url)


@admin_bp.route('/settings')
def settings():
    return render_template('admin/settings.html')

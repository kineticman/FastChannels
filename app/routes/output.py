from flask import Blueprint, request, Response, abort
from ..generators.m3u import generate_m3u
from ..generators.xmltv import generate_xmltv
from ..models import Feed

output_bp = Blueprint('output', __name__)


def _filters_from_request():
    """Build a filters dict from URL query params (legacy /m3u and /epg.xml endpoints)."""
    f = {}
    if s := request.args.getlist('source'):
        f['sources'] = s
    if c := request.args.getlist('category'):
        f['categories'] = c
    if l := request.args.get('language'):
        f['language'] = l
    if q := request.args.get('search'):
        f['search'] = q
    return f


# ── Legacy / general endpoints (still work, full output or query-filtered) ──

@output_bp.route('/m3u')
def m3u():
    base_url = request.host_url.rstrip('/')
    content  = generate_m3u(_filters_from_request(), base_url=base_url)
    return Response(content, mimetype='application/x-mpegurl',
                    headers={'Content-Disposition': 'attachment; filename="fastchannels.m3u"'})


@output_bp.route('/epg.xml')
def epg_xml():
    base_url = request.host_url.rstrip('/')
    content  = generate_xmltv(_filters_from_request(), base_url=base_url)
    return Response(content, mimetype='application/xml',
                    headers={'Content-Disposition': 'attachment; filename="fastchannels.xml"'})


# ── Named feed endpoints — stable URLs for Channels DVR sources ─────────────

@output_bp.route('/m3u/<slug>')
def feed_m3u(slug: str):
    feed = Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()
    base_url = request.host_url.rstrip('/')
    content  = generate_m3u(feed.filters or {}, base_url=base_url)
    fname    = f'fastchannels-{slug}.m3u'
    return Response(content, mimetype='application/x-mpegurl',
                    headers={'Content-Disposition': f'attachment; filename="{fname}"'})


@output_bp.route('/epg/<slug>.xml')
def feed_epg(slug: str):
    feed = Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()
    base_url = request.host_url.rstrip('/')
    content  = generate_xmltv(feed.filters or {}, base_url=base_url)
    fname    = f'fastchannels-{slug}.xml'
    return Response(content, mimetype='application/xml',
                    headers={'Content-Disposition': f'attachment; filename="{fname}"'})

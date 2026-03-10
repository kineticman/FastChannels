from flask import Blueprint, request, Response
from ..generators.m3u import generate_m3u, generate_gracenote_m3u, feed_to_query_filters
from ..generators.xmltv import generate_xmltv
from ..models import Feed
from ..url import public_base_url

output_bp = Blueprint('output', __name__)


def _filters():
    f = {}
    if s := request.args.getlist('source'):
        f['source'] = s
    if c := request.args.getlist('category'):
        f['category'] = c
    if l := request.args.get('language'):
        f['language'] = l
    if q := request.args.get('search'):
        f['search'] = q
    return f


@output_bp.route('/m3u')
def m3u():
    base_url = public_base_url()
    content  = generate_m3u(_filters(), base_url=base_url)
    return Response(content, mimetype='application/x-mpegurl',
                    headers={'Content-Disposition': 'attachment; filename="fastchannels.m3u"'})


@output_bp.route('/m3u/gracenote')
def m3u_gracenote():
    """
    Gracenote-backed M3U for Channels DVR.

    Contains only channels with a valid Gracenote ID in their slug
    (stored as "{play_id}|{gracenote_id}" by the Roku scraper).
    Uses tvc-guide-stationid so Channels DVR routes guide data through
    Gracenote rather than our XMLTV — the two cannot be mixed per source.
    Supports the same ?source=, ?category=, ?language=, ?search= filters
    as the standard /m3u endpoint.
    """
    base_url = public_base_url()
    content  = generate_gracenote_m3u(_filters(), base_url=base_url)
    return Response(content, mimetype='application/x-mpegurl',
                    headers={'Content-Disposition': 'attachment; filename="fastchannels-gracenote.m3u"'})


@output_bp.route('/epg.xml')
def epg_xml():
    base_url = public_base_url()
    content  = generate_xmltv(_filters(), base_url=base_url)
    return Response(content, mimetype='application/xml',
                    headers={'Content-Disposition': 'attachment; filename="fastchannels.xml"'})


@output_bp.route('/feeds/<slug>/m3u')
def feed_m3u(slug):
    feed     = Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()
    base_url = public_base_url()
    content  = generate_m3u(feed_to_query_filters(feed.filters or {}), base_url=base_url,
                             feed_chnum_start=feed.chnum_start)
    return Response(content, mimetype='application/x-mpegurl',
                    headers={'Content-Disposition': f'attachment; filename="{slug}.m3u"'})


@output_bp.route('/feeds/<slug>/m3u/gracenote')
def feed_m3u_gracenote(slug):
    feed     = Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()
    base_url = public_base_url()
    content  = generate_gracenote_m3u(feed_to_query_filters(feed.filters or {}), base_url=base_url,
                                      feed_chnum_start=feed.chnum_start)
    return Response(content, mimetype='application/x-mpegurl',
                    headers={'Content-Disposition': f'attachment; filename="{slug}-gracenote.m3u"'})


@output_bp.route('/feeds/<slug>/epg.xml')
def feed_epg(slug):
    feed     = Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()
    base_url = public_base_url()
    content  = generate_xmltv(feed_to_query_filters(feed.filters or {}), base_url=base_url)
    return Response(content, mimetype='application/xml',
                    headers={'Content-Disposition': f'attachment; filename="{slug}.xml"'})

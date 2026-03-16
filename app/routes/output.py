from flask import Blueprint, request, Response
from ..generators.m3u import (
    generate_m3u,
    generate_gracenote_m3u,
    feed_namespace_start,
    feed_gracenote_start,
    feed_to_query_filters,
    _MASTER_GRACENOTE_START,
)
from ..generators.xmltv import generate_xmltv
from ..models import Feed
from ..url import public_base_url
from ..xml_cache import get_or_build_xml, get_or_build

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
    filters  = _filters()
    if filters:
        content = generate_m3u(filters, base_url=base_url)
    else:
        content = get_or_build('master-m3u', lambda: generate_m3u({}, base_url=base_url), ext='m3u')
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
    from ..models import Feed
    base_url     = public_base_url()
    filters      = _filters()
    default_feed = Feed.query.filter_by(slug='default').first()
    gn_start     = feed_gracenote_start(default_feed) if default_feed else _MASTER_GRACENOTE_START
    if filters:
        content = generate_gracenote_m3u(filters, base_url=base_url, namespace_start=gn_start)
    else:
        content = get_or_build(
            'master-gracenote-m3u',
            lambda: generate_gracenote_m3u({}, base_url=base_url, namespace_start=gn_start),
            ext='m3u',
        )
    return Response(content, mimetype='application/x-mpegurl',
                    headers={'Content-Disposition': 'attachment; filename="fastchannels-gracenote.m3u"'})


@output_bp.route('/epg.xml')
def epg_xml():
    base_url = public_base_url()
    filters = _filters()
    if filters:
        content = generate_xmltv(filters, base_url=base_url)
    else:
        content = get_or_build_xml('master', lambda: generate_xmltv({}, base_url=base_url))
    return Response(content, mimetype='application/xml',
                    headers={'Content-Disposition': 'attachment; filename="fastchannels.xml"'})


@output_bp.route('/feeds/<slug>/m3u')
def feed_m3u(slug):
    feed     = Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()
    base_url = public_base_url()
    filters  = feed_to_query_filters(feed.filters or {})
    # Default feed: chnum_start is the global fallback start for ungrouped sources,
    # not a feed-level override — always delegate to _build_source_chnum_map (same
    # as /m3u) so per-source chnum_start values are still respected.
    if feed.slug == 'default':
        kw = {}
    elif feed.chnum_start is not None:
        kw = {'feed_chnum_start': feed.chnum_start}
    else:
        kw = {'namespace_start': feed_namespace_start(feed, gracenote=False)}
    content  = get_or_build(
        f'feed-{slug}-m3u',
        lambda: generate_m3u(filters, base_url=base_url, **kw),
        ext='m3u',
    )
    return Response(content, mimetype='application/x-mpegurl',
                    headers={'Content-Disposition': f'attachment; filename="{slug}.m3u"'})


@output_bp.route('/feeds/<slug>/m3u/gracenote')
def feed_m3u_gracenote(slug):
    feed     = Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()
    base_url = public_base_url()
    filters  = feed_to_query_filters(feed.filters or {})
    # Gracenote channels start immediately after standard channels in the same pool.
    # feed_gracenote_start() computes the right offset for all feed types.
    kw = {'namespace_start': feed_gracenote_start(feed)}
    content  = get_or_build(
        f'feed-{slug}-gracenote-m3u',
        lambda: generate_gracenote_m3u(filters, base_url=base_url, **kw),
        ext='m3u',
    )
    return Response(content, mimetype='application/x-mpegurl',
                    headers={'Content-Disposition': f'attachment; filename="{slug}-gracenote.m3u"'})


@output_bp.route('/feeds/<slug>/epg.xml')
def feed_epg(slug):
    feed     = Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()
    base_url = public_base_url()
    content  = get_or_build_xml(
        f'feed-{feed.slug}',
        lambda: generate_xmltv(feed_to_query_filters(feed.filters or {}), base_url=base_url, feed_name=feed.name),
    )
    return Response(content, mimetype='application/xml',
                    headers={'Content-Disposition': f'attachment; filename="{slug}.xml"'})

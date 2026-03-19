from flask import Blueprint, Response, request, send_file, stream_with_context
from ..generators.m3u import (
    generate_m3u,
    generate_gracenote_m3u,
    feed_namespace_start,
    feed_gracenote_start,
    feed_to_query_filters,
    _MASTER_GRACENOTE_START,
)
from ..generators.xmltv import generate_xmltv_stream
from ..models import Feed
from ..url import public_base_url
from ..xml_cache import get_artifact, get_or_build, get_xml_artifact

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
        return Response(content, mimetype='application/x-mpegurl',
                        headers={'Content-Disposition': 'attachment; filename="fastchannels.m3u"'})
    path = get_artifact('master-m3u', ext='m3u')
    if path is None:
        return Response(
            'M3U artifact is warming. Retry shortly.',
            status=503,
            mimetype='text/plain',
            headers={'Retry-After': '15'},
        )
    return send_file(
        path,
        mimetype='application/x-mpegurl',
        as_attachment=True,
        download_name='fastchannels.m3u',
        conditional=True,
        max_age=0,
    )


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
        return Response(content, mimetype='application/x-mpegurl',
                        headers={'Content-Disposition': 'attachment; filename="fastchannels-gracenote.m3u"'})
    path = get_artifact('master-gracenote-m3u', ext='m3u')
    if path is None:
        return Response(
            'Gracenote M3U artifact is warming. Retry shortly.',
            status=503,
            mimetype='text/plain',
            headers={'Retry-After': '15'},
        )
    return send_file(
        path,
        mimetype='application/x-mpegurl',
        as_attachment=True,
        download_name='fastchannels-gracenote.m3u',
        conditional=True,
        max_age=0,
    )


@output_bp.route('/epg.xml')
def epg_xml():
    base_url = public_base_url()
    filters = _filters()
    if filters:
        return Response(
            stream_with_context(generate_xmltv_stream(filters, base_url=base_url)),
            mimetype='application/xml',
            headers={'Content-Disposition': 'attachment; filename="fastchannels.xml"'},
        )

    path, stale = get_xml_artifact('master')
    if path is None:
        return Response(
            'EPG artifact is warming. Retry shortly.',
            status=503,
            mimetype='text/plain',
            headers={'Retry-After': '15'},
        )
    return send_file(
        path,
        mimetype='application/xml',
        as_attachment=True,
        download_name='fastchannels.xml',
        conditional=True,
        max_age=0,
        etag=not stale,
    )


@output_bp.route('/feeds/<slug>/m3u')
def feed_m3u(slug):
    feed     = Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()
    path = get_artifact(f'feed-{slug}-m3u', ext='m3u')
    if path is None:
        return Response(
            f'Feed M3U artifact for {feed.slug} is warming. Retry shortly.',
            status=503,
            mimetype='text/plain',
            headers={'Retry-After': '15'},
        )
    return send_file(
        path,
        mimetype='application/x-mpegurl',
        as_attachment=True,
        download_name=f'{slug}.m3u',
        conditional=True,
        max_age=0,
    )


@output_bp.route('/feeds/<slug>/m3u/gracenote')
def feed_m3u_gracenote(slug):
    feed     = Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()
    path = get_artifact(f'feed-{slug}-gracenote-m3u', ext='m3u')
    if path is None:
        return Response(
            f'Feed Gracenote M3U artifact for {feed.slug} is warming. Retry shortly.',
            status=503,
            mimetype='text/plain',
            headers={'Retry-After': '15'},
        )
    return send_file(
        path,
        mimetype='application/x-mpegurl',
        as_attachment=True,
        download_name=f'{slug}-gracenote.m3u',
        conditional=True,
        max_age=0,
    )


@output_bp.route('/feeds/<slug>/epg.xml')
def feed_epg(slug):
    feed     = Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()
    path, stale = get_xml_artifact(f'feed-{feed.slug}')
    if path is None:
        return Response(
            f'Feed XML artifact for {feed.slug} is warming. Retry shortly.',
            status=503,
            mimetype='text/plain',
            headers={'Retry-After': '15'},
        )
    return send_file(
        path,
        mimetype='application/xml',
        as_attachment=True,
        download_name=f'{slug}.xml',
        conditional=True,
        max_age=0,
        etag=not stale,
    )

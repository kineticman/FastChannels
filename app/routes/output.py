import logging
import os

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
from ..xml_cache import get_artifact, get_xml_artifact, xml_artifact_path, _xml_stale_path, _GLOBAL_XML_STALE

log = logging.getLogger(__name__)

output_bp = Blueprint('output', __name__)


def _log_epg_request(label: str, path, stale: bool) -> None:
    """Log detailed diagnostics for an EPG XML request."""
    ua      = request.headers.get('User-Agent', '–')
    ip      = request.headers.get('X-Forwarded-For') or request.remote_addr
    ims     = request.headers.get('If-Modified-Since', '–')
    inm     = request.headers.get('If-None-Match', '–')
    enc     = request.headers.get('Accept-Encoding', '–')

    if path is not None and path.exists():
        stat      = path.stat()
        size_mb   = stat.st_size / 1_048_576
        from datetime import datetime, timezone
        mtime_str = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        size_mb   = 0.0
        mtime_str = '–'

    # Stale marker details
    key       = label.replace('/', '-')
    key_stale = _xml_stale_path(key)
    if key_stale.exists():
        from datetime import datetime, timezone
        sm = datetime.fromtimestamp(key_stale.stat().st_mtime, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        stale_src = f'key-marker mtime={sm}'
    elif _GLOBAL_XML_STALE.exists():
        from datetime import datetime, timezone
        gm = datetime.fromtimestamp(_GLOBAL_XML_STALE.stat().st_mtime, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        stale_src = f'global-marker mtime={gm}'
    else:
        stale_src = 'none'

    decision = '503 WARMING' if path is None else f'200 OK ({size_mb:.1f} MB, stale={stale})'

    log.debug(
        '[epg:%s] %s from %s | ua="%s" | '
        'If-Modified-Since=%s If-None-Match=%s Accept-Encoding=%s | '
        'artifact: exists=%s stale=%s mtime=%s size=%.1fMB | '
        'stale-marker: %s | '
        'response: %s',
        label, request.method, ip, ua,
        ims, inm, enc,
        path is not None and path.exists(), stale, mtime_str, size_mb,
        stale_src,
        decision,
    )


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


def _send_feed_artifact(path, *, mimetype: str, download_name: str):
    response = send_file(
        path,
        mimetype=mimetype,
        as_attachment=True,
        download_name=download_name,
        conditional=False,
        etag=False,
        max_age=0,
    )
    # Force clients to fetch the full artifact every time instead of
    # revalidating against ETag/Last-Modified and receiving 304 responses.
    response.headers['Cache-Control'] = 'no-store, max-age=0, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    response.headers.pop('ETag', None)
    response.headers.pop('Last-Modified', None)
    return response

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
    return _send_feed_artifact(
        path,
        mimetype='application/x-mpegurl',
        download_name='fastchannels.m3u',
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
    return _send_feed_artifact(
        path,
        mimetype='application/x-mpegurl',
        download_name='fastchannels-gracenote.m3u',
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
    _log_epg_request('master', path, stale)
    if path is None:
        return Response(
            'EPG artifact is warming. Retry shortly.',
            status=503,
            mimetype='text/plain',
            headers={'Retry-After': '15'},
        )
    return _send_feed_artifact(
        path,
        mimetype='application/xml',
        download_name='fastchannels.xml',
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
    return _send_feed_artifact(
        path,
        mimetype='application/x-mpegurl',
        download_name=f'{slug}.m3u',
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
    return _send_feed_artifact(
        path,
        mimetype='application/x-mpegurl',
        download_name=f'{slug}-gracenote.m3u',
    )


@output_bp.route('/feeds/<slug>/epg.xml')
def feed_epg(slug):
    feed     = Feed.query.filter_by(slug=slug, is_enabled=True).first_or_404()
    path, stale = get_xml_artifact(f'feed-{feed.slug}')
    _log_epg_request(f'feed-{feed.slug}', path, stale)
    if path is None:
        return Response(
            f'Feed XML artifact for {feed.slug} is warming. Retry shortly.',
            status=503,
            mimetype='text/plain',
            headers={'Retry-After': '15'},
        )
    return _send_feed_artifact(
        path,
        mimetype='application/xml',
        download_name=f'{slug}.xml',
    )

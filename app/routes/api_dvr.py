import logging
import secrets
import time as _time
import requests as _req

logger = logging.getLogger(__name__)

from flask import Blueprint, jsonify, request
from types import SimpleNamespace
from sqlalchemy import or_
from ..extensions import db
from ..models import Source, Channel, AppSettings, Feed
from ..scrapers import registry
from ..url import public_base_url
from .tasks import (
    trigger_xml_refresh,
)
from ..xml_cache import (
    get_artifact,
    get_xml_artifact,
)
from .admin import _feed_split_counts

dvr_bp = Blueprint('api_dvr', __name__)

_CHANNELS_DVR_RECOMMENDED_MAX = 750


def _ensure_feed_dvr_artifacts(feed: Feed, base_url: str, *, has_gracenote: bool,
                               prismcast: bool = False, fc_player: bool = False,
                               force_refresh: bool = False) -> None:
    """Ensure feed artifacts exist before handing URLs to Channels DVR.

    Normal output serving is allowed to keep stale M3U files on disk while the
    async refresh job catches up. A DVR import is a user-triggered snapshot, so
    force_refresh rebuilds this feed synchronously to reflect recent channel
    enable/disable edits before Channels DVR fetches the URL.
    """
    if prismcast:
        std_key = f'feed-{feed.slug}-prismcast-m3u'
        gn_key  = f'feed-{feed.slug}-prismcast-gracenote-m3u'
    elif fc_player:
        std_key = f'feed-{feed.slug}-fc-player-m3u'
        gn_key  = f'feed-{feed.slug}-fc-player-gracenote-m3u'
    else:
        std_key = f'feed-{feed.slug}-m3u'
        gn_key  = f'feed-{feed.slug}-gracenote-m3u'

    if force_refresh:
        from ..generators.m3u import (
            generate_gracenote_m3u,
            generate_m3u,
            generate_prismcast_m3u,
            generate_fc_player_m3u,
            feed_gracenote_start,
            feed_namespace_start,
            feed_to_query_filters,
        )
        from ..generators.xmltv import write_xmltv
        from ..xml_cache import write_artifact, write_xml_artifact

        filters = feed_to_query_filters(feed.filters or {})
        if feed.chnum_start is not None:
            std_kw = {'feed_chnum_start': feed.chnum_start, 'feed_id': feed.id}
            gn_kw = {'feed_chnum_start': feed.chnum_start, 'feed_id': feed.id}
        else:
            std_kw = {'namespace_start': feed_namespace_start(feed, gracenote=False)}
            gn_kw = {'namespace_start': feed_gracenote_start(feed)}

        write_xml_artifact(
            f'feed-{feed.slug}',
            lambda fp: write_xmltv(fp, filters, base_url=base_url, feed_name=feed.name),
        )

        if prismcast:
            settings = AppSettings.get()
            prismcast_url = (settings.effective_prismcast_url() or '').strip().rstrip('/')
            prismcast_inner = (settings.effective_prismcast_inner_url() or base_url).strip().rstrip('/')
            write_artifact(
                std_key,
                lambda fp: fp.write(generate_prismcast_m3u(
                    filters,
                    base_url=base_url,
                    prismcast_url=prismcast_url,
                    inner_base_url=prismcast_inner,
                    **std_kw,
                )),
                ext='m3u',
            )
            if has_gracenote:
                write_artifact(
                    gn_key,
                    lambda fp: fp.write(generate_prismcast_m3u(
                        filters,
                        base_url=base_url,
                        prismcast_url=prismcast_url,
                        inner_base_url=prismcast_inner,
                        gracenote=True,
                        **gn_kw,
                    )),
                    ext='m3u',
                )
        elif fc_player:
            write_artifact(
                std_key,
                lambda fp: fp.write(generate_fc_player_m3u(filters, base_url=base_url, **std_kw)),
                ext='m3u',
            )
            if has_gracenote:
                write_artifact(
                    gn_key,
                    lambda fp: fp.write(generate_fc_player_m3u(
                        filters,
                        base_url=base_url,
                        gracenote=True,
                        **gn_kw,
                    )),
                    ext='m3u',
                )
        else:
            write_artifact(
                std_key,
                lambda fp: fp.write(generate_m3u(filters, base_url=base_url, **std_kw)),
                ext='m3u',
            )
            if has_gracenote:
                write_artifact(
                    gn_key,
                    lambda fp: fp.write(generate_gracenote_m3u(
                        filters,
                        base_url=base_url,
                        **gn_kw,
                    )),
                    ext='m3u',
                )
        return

    def _ready() -> bool:
        xml_path, _ = get_xml_artifact(f'feed-{feed.slug}')
        if get_artifact(std_key, ext='m3u') is None:
            return False
        if xml_path is None:
            return False
        if has_gracenote and get_artifact(gn_key, ext='m3u') is None:
            return False
        return True

    if _ready():
        return

    trigger_xml_refresh()
    deadline = _time.time() + 20
    while _time.time() < deadline:
        if _ready():
            return
        _time.sleep(0.2)
    raise TimeoutError(f'timed out waiting for feed artifacts: {feed.slug}')


def _channel_query_summary(query, parse_gracenote) -> tuple[int, bool]:
    """Return count and whether any channel in the query has a valid Gracenote ID."""
    base_query = query.order_by(None)
    count = base_query.count()
    if count == 0:
        return 0, False

    candidates = (
        base_query.with_entities(Channel.gracenote_id, Channel.slug, Channel.gracenote_mode)
        .filter(
            or_(
                (Channel.gracenote_id != None) & (Channel.gracenote_id != ''),
                Channel.slug.like('%|%'),
            )
        )
        .limit(256)
        .all()
    )
    has_gracenote = any(
        parse_gracenote(SimpleNamespace(
            gracenote_id=row.gracenote_id,
            slug=row.slug,
            gracenote_mode=row.gracenote_mode,
        ))
        for row in candidates
    )
    return count, has_gracenote


def _dvr_stream_format(query) -> str:
    """Channels DVR stream format (the source 'type') for an M3U built from `query`.

    Returns 'MPEG-TS' when every channel is a raw transport stream (e.g. an
    HDHomeRun OTA tuner, stream_type='mpegts') so Channels DVR ingests it via
    ffmpeg without the user having to flip the format by hand. Otherwise 'HLS' —
    the correct default for FAST sources whose /play endpoint serves HLS. A
    source carries a single type, so a mixed feed stays 'HLS' (no regression).
    """
    rows = query.order_by(None).with_entities(Channel.stream_type).all()
    types = {(r[0] or 'hls').lower() for r in rows}
    return 'MPEG-TS' if types and types <= {'mpegts'} else 'HLS'


@dvr_bp.route('/feeds/<int:feed_id>/push-to-dvr', methods=['POST'])
def push_feed_to_dvr(feed_id):
    """Register this feed as custom M3U source(s) in Channels DVR.

    Registers up to two sources:
    - Gracenote source (no EPG URL): only if the feed has channels with
      Gracenote IDs — DVR fetches its own guide data via tvc-guide-stationid.
    - Standard source (with our EPG XML): always registered.
    """
    import re as _re
    from ..generators.m3u import _build_channel_query, _parse_gracenote_id, feed_to_query_filters

    feed = Feed.query.get_or_404(feed_id)
    settings = AppSettings.get()

    dvr_url = (settings.effective_channels_dvr_url() or '').strip()
    if not dvr_url:
        return jsonify({'error': 'Channels DVR URL is not configured in Settings.'}), 400

    base = public_base_url()

    # Check if this feed has any channels with Gracenote IDs using the same
    # logic as generate_gracenote_m3u() so we don't register an empty source.
    feed_query = _build_channel_query(feed_to_query_filters(feed.filters or {}))
    channel_count, has_gracenote = _channel_query_summary(feed_query, _parse_gracenote_id)
    dvr_type = _dvr_stream_format(feed_query)
    if channel_count == 0:
        return jsonify({'error': 'This feed has no eligible channels to add to Channels DVR.'}), 400

    # standard_count = channels that go into the regular M3U (non-gracenote channels)
    split = _feed_split_counts(feed)
    standard_count = split['standard_count']

    force = bool((request.get_json(silent=True) or {}).get('force'))
    if channel_count > _CHANNELS_DVR_RECOMMENDED_MAX and not force:
        return jsonify({
            'error': f'This feed has {channel_count} channels. Channels DVR usually works best at 750 or fewer.',
            'requires_confirm': True,
            'channel_count': channel_count,
            'recommended_max': _CHANNELS_DVR_RECOMMENDED_MAX,
        }), 409

    try:
        _ensure_feed_dvr_artifacts(feed, base, has_gracenote=has_gracenote, force_refresh=True)
    except TimeoutError:
        return jsonify({'error': 'Timed out waiting for feed artifacts to build. Try again in a moment.'}), 503

    def _put(name, url, xmltv_url=''):
        safe = _re.sub(r'[^a-zA-Z0-9]', '', name)
        payload = {
            'name':    name,
            'type':    dvr_type,
            'source':  'URL',
            'url':     url,
            'refresh': '24',
        }
        if xmltv_url:
            payload['xmltv_url']     = xmltv_url
            payload['xmltv_refresh'] = '3600'
        return _req.put(f"{dvr_url}/providers/m3u/sources/{safe}", json=payload, timeout=30, verify=False)

    gn_name  = f"FastChannels {feed.name} Gracenote"
    epg_name = f"FastChannels {feed.name}"
    sources_added = []

    try:
        if has_gracenote:
            r1 = _put(gn_name, f"{base}/feeds/{feed.slug}/m3u/gracenote")
            r1.raise_for_status()
            sources_added.append(gn_name)

        if standard_count > 0:
            r2 = _put(epg_name, f"{base}/feeds/{feed.slug}/m3u", f"{base}/feeds/{feed.slug}/epg.xml")
            r2.raise_for_status()
            sources_added.append(epg_name)
    except _req.exceptions.ConnectionError:
        return jsonify({'error': f'Could not connect to Channels DVR at {dvr_url}'}), 502
    except _req.exceptions.Timeout:
        return jsonify({'error': 'Channels DVR timed out.'}), 504
    except _req.exceptions.HTTPError as exc:
        resp = exc.response
        return jsonify({'error': f'DVR {resp.status_code}: {resp.text[:300]}'}), 502

    return jsonify({'ok': True, 'sources_added': sources_added})


@dvr_bp.route('/feeds/<int:feed_id>/push-prismcast-to-dvr', methods=['POST'])
def push_feed_prismcast_to_dvr(feed_id):
    """Register this feed's PrismCast (DRM-bridge) output as custom M3U source(s)
    in Channels DVR.

    Registers up to two sources, named distinctly from the standard push so they
    sit alongside it rather than overwriting it (Channels DVR keys a source by its
    alphanumeric-stripped name):
    - "… PrismCast Gracenote" (no EPG URL): only if the feed has Gracenote channels
      that PrismCast carries — DVR fetches guide data via tvc-guide-stationid.
    - "… PrismCast" (with our EPG XML): the standard-guide DRM-bridge playlist.
    """
    import re as _re
    from ..generators.m3u import _build_channel_query, feed_to_query_filters

    feed = Feed.query.get_or_404(feed_id)
    settings = AppSettings.get()

    dvr_url = (settings.effective_channels_dvr_url() or '').strip()
    if not dvr_url:
        return jsonify({'error': 'Channels DVR URL is not configured in Settings.'}), 400
    if not (settings.effective_prismcast_url() or '').strip():
        return jsonify({'error': 'PrismCast is not configured. Set the PrismCast server URL in Settings.'}), 400

    base = public_base_url()

    feed_query = _build_channel_query(feed_to_query_filters(feed.filters or {}))
    dvr_type = _dvr_stream_format(feed_query)

    # PrismCast partitions the same way as the standard feed; use the same counts.
    split = _feed_split_counts(feed)
    std_count = split.get('prismcast_count', 0)
    gn_count  = split.get('prismcast_gracenote_count', 0)
    has_gracenote = gn_count > 0
    if std_count == 0 and gn_count == 0:
        return jsonify({'error': 'This feed has no eligible PrismCast channels to add to Channels DVR.'}), 400

    # Each partition registers as its own DVR source, so gate the recommended-max
    # warning on the larger of the two.
    largest = max(std_count, gn_count)
    force = bool((request.get_json(silent=True) or {}).get('force'))
    if largest > _CHANNELS_DVR_RECOMMENDED_MAX and not force:
        return jsonify({
            'error': f'This PrismCast feed has {largest} channels in one source. Channels DVR usually works best at 750 or fewer.',
            'requires_confirm': True,
            'channel_count': largest,
            'recommended_max': _CHANNELS_DVR_RECOMMENDED_MAX,
        }), 409

    try:
        _ensure_feed_dvr_artifacts(feed, base, has_gracenote=has_gracenote, prismcast=True, force_refresh=True)
    except TimeoutError:
        return jsonify({'error': 'Timed out waiting for PrismCast feed artifacts to build. Try again in a moment.'}), 503

    def _put(name, url, xmltv_url=''):
        safe = _re.sub(r'[^a-zA-Z0-9]', '', name)
        payload = {
            'name':    name,
            'type':    dvr_type,
            'source':  'URL',
            'url':     url,
            'refresh': '24',
        }
        if xmltv_url:
            payload['xmltv_url']     = xmltv_url
            payload['xmltv_refresh'] = '3600'
        return _req.put(f"{dvr_url}/providers/m3u/sources/{safe}", json=payload, timeout=30, verify=False)

    gn_name  = f"FastChannels {feed.name} PrismCast Gracenote"
    std_name = f"FastChannels {feed.name} PrismCast"
    sources_added = []

    try:
        if has_gracenote:
            r1 = _put(gn_name, f"{base}/feeds/{feed.slug}/m3u/prismcast/gracenote")
            r1.raise_for_status()
            sources_added.append(gn_name)

        if std_count > 0:
            r2 = _put(std_name, f"{base}/feeds/{feed.slug}/m3u/prismcast", f"{base}/feeds/{feed.slug}/epg.xml")
            r2.raise_for_status()
            sources_added.append(std_name)
    except _req.exceptions.ConnectionError:
        return jsonify({'error': f'Could not connect to Channels DVR at {dvr_url}'}), 502
    except _req.exceptions.Timeout:
        return jsonify({'error': 'Channels DVR timed out.'}), 504
    except _req.exceptions.HTTPError as exc:
        resp = exc.response
        return jsonify({'error': f'DVR {resp.status_code}: {resp.text[:300]}'}), 502

    return jsonify({'ok': True, 'sources_added': sources_added})


@dvr_bp.route('/feeds/<int:feed_id>/push-fc-player-to-dvr', methods=['POST'])
def push_feed_fc_player_to_dvr(feed_id):
    """Register this feed's FastChannels Android Bridge Channels (FastChannels
    Player DRM bridge) output as custom M3U source(s) in Channels DVR.

    Registers up to two sources, named distinctly from the standard push so they
    sit alongside it rather than overwriting it:
    - "... Android Bridge Gracenote" (no EPG URL): only if the feed has trusted
      bridge channels with a Gracenote ID.
    - "... Android Bridge" (with our EPG XML): the standard-guide bridge playlist.
    """
    import re as _re
    from .. import fc_player_bridge

    feed = Feed.query.get_or_404(feed_id)
    settings = AppSettings.get()

    dvr_url = (settings.effective_channels_dvr_url() or '').strip()
    if not dvr_url:
        return jsonify({'error': 'Channels DVR URL is not configured in Settings.'}), 400
    if not fc_player_bridge.is_configured():
        return jsonify({'error': 'FastChannels Player is not configured. Set it up in Settings.'}), 400

    base = public_base_url()

    # Every channel here plays through the HDMI encoder's capture URL (raw
    # transport stream, e.g. Channels DVR's own capture:// stream.mpg), not
    # through the original DRM source's own stream type — so unlike the other
    # push-to-dvr routes, this is never derived from the channels' stream_type.
    dvr_type = 'MPEG-TS'

    split = _feed_split_counts(feed)
    std_count = split.get('drm_bridge_count', 0)
    gn_count  = split.get('drm_bridge_gracenote_count', 0)
    has_gracenote = gn_count > 0
    if std_count == 0 and gn_count == 0:
        return jsonify({'error': 'This feed has no eligible FastChannels Android Bridge channels to add to Channels DVR.'}), 400

    largest = max(std_count, gn_count)
    force = bool((request.get_json(silent=True) or {}).get('force'))
    if largest > _CHANNELS_DVR_RECOMMENDED_MAX and not force:
        return jsonify({
            'error': f'This FastChannels Android Bridge feed has {largest} channels in one source. Channels DVR usually works best at 750 or fewer.',
            'requires_confirm': True,
            'channel_count': largest,
            'recommended_max': _CHANNELS_DVR_RECOMMENDED_MAX,
        }), 409

    try:
        _ensure_feed_dvr_artifacts(feed, base, has_gracenote=has_gracenote, fc_player=True, force_refresh=True)
    except TimeoutError:
        return jsonify({'error': 'Timed out waiting for FastChannels Android Bridge feed artifacts to build. Try again in a moment.'}), 503

    def _put(name, url, xmltv_url=''):
        safe = _re.sub(r'[^a-zA-Z0-9]', '', name)
        payload = {
            'name':    name,
            'type':    dvr_type,
            'source':  'URL',
            'url':     url,
            'refresh': '24',
        }
        if xmltv_url:
            payload['xmltv_url']     = xmltv_url
            payload['xmltv_refresh'] = '3600'
        return _req.put(f"{dvr_url}/providers/m3u/sources/{safe}", json=payload, timeout=30, verify=False)

    gn_name  = f"FastChannels {feed.name} Android Bridge Gracenote"
    std_name = f"FastChannels {feed.name} Android Bridge"
    sources_added = []

    try:
        if has_gracenote:
            r1 = _put(gn_name, f"{base}/feeds/{feed.slug}/m3u/fc-player/gracenote")
            r1.raise_for_status()
            sources_added.append(gn_name)

        if std_count > 0:
            r2 = _put(std_name, f"{base}/feeds/{feed.slug}/m3u/fc-player", f"{base}/feeds/{feed.slug}/epg.xml")
            r2.raise_for_status()
            sources_added.append(std_name)
    except _req.exceptions.ConnectionError:
        return jsonify({'error': f'Could not connect to Channels DVR at {dvr_url}'}), 502
    except _req.exceptions.Timeout:
        return jsonify({'error': 'Channels DVR timed out.'}), 504
    except _req.exceptions.HTTPError as exc:
        resp = exc.response
        return jsonify({'error': f'DVR {resp.status_code}: {resp.text[:300]}'}), 502

    return jsonify({'ok': True, 'sources_added': sources_added})


@dvr_bp.route('/sources/<int:source_id>/push-to-dvr', methods=['POST'])
def push_source_to_dvr(source_id):
    """Register a source-filtered raw output as custom M3U source(s) in Channels DVR."""
    import re as _re
    from ..generators.m3u import _build_channel_query, _parse_gracenote_id

    source = Source.query.get_or_404(source_id)
    settings = AppSettings.get()

    dvr_url = (settings.effective_channels_dvr_url() or '').strip()
    if not dvr_url:
        return jsonify({'error': 'Channels DVR URL is not configured in Settings.'}), 400

    base = public_base_url()
    source_query = _build_channel_query({'source': [source.name]})
    channel_count, has_gracenote = _channel_query_summary(source_query, _parse_gracenote_id)
    dvr_type = _dvr_stream_format(source_query)
    if channel_count == 0:
        return jsonify({'error': f'{source.display_name} has no eligible channels to add to Channels DVR.'}), 400
    force = bool((request.get_json(silent=True) or {}).get('force'))
    if channel_count > _CHANNELS_DVR_RECOMMENDED_MAX and not force:
        return jsonify({
            'error': f'{source.display_name} has {channel_count} channels. Channels DVR usually works best at 750 or fewer.',
            'requires_confirm': True,
            'channel_count': channel_count,
            'recommended_max': _CHANNELS_DVR_RECOMMENDED_MAX,
        }), 409

    def _put(name, url, xmltv_url=''):
        safe = _re.sub(r'[^a-zA-Z0-9]', '', name)
        payload = {
            'name': name,
            'type': dvr_type,
            'source': 'URL',
            'url': url,
            'refresh': '24',
        }
        if xmltv_url:
            payload['xmltv_url'] = xmltv_url
            payload['xmltv_refresh'] = '3600'
        return _req.put(f"{dvr_url}/providers/m3u/sources/{safe}", json=payload, timeout=30, verify=False)

    query_param = f"?source={source.name}"
    std_name = f"FastChannels {source.display_name}"
    gn_name = f"FastChannels {source.display_name} Gracenote"
    sources_added = []

    try:
        if has_gracenote:
            r1 = _put(gn_name, f"{base}/m3u/gracenote{query_param}")
            r1.raise_for_status()
            sources_added.append(gn_name)

        r2 = _put(std_name, f"{base}/m3u{query_param}", f"{base}/epg.xml{query_param}")
        r2.raise_for_status()
        sources_added.append(std_name)
    except _req.exceptions.ConnectionError:
        return jsonify({'error': f'Could not connect to Channels DVR at {dvr_url}'}), 502
    except _req.exceptions.Timeout:
        return jsonify({'error': 'Channels DVR timed out.'}), 504
    except _req.exceptions.HTTPError as exc:
        resp = exc.response
        return jsonify({'error': f'DVR {resp.status_code}: {resp.text[:300]}'}), 502

    return jsonify({'ok': True, 'sources_added': sources_added})


@dvr_bp.route('/raw-output/push-to-dvr', methods=['POST'])
def push_raw_output_to_dvr():
    """Register the full raw output M3U source(s) in Channels DVR."""
    import re as _re
    from ..generators.m3u import _build_channel_query, _parse_gracenote_id

    settings = AppSettings.get()

    dvr_url = (settings.effective_channels_dvr_url() or '').strip()
    if not dvr_url:
        return jsonify({'error': 'Channels DVR URL is not configured in Settings.'}), 400

    base = public_base_url()
    raw_query = _build_channel_query({})
    channel_count, has_gracenote = _channel_query_summary(raw_query, _parse_gracenote_id)
    dvr_type = _dvr_stream_format(raw_query)
    if channel_count == 0:
        return jsonify({'error': 'Raw Output has no eligible channels to add to Channels DVR.'}), 400
    force = bool((request.get_json(silent=True) or {}).get('force'))
    if channel_count > _CHANNELS_DVR_RECOMMENDED_MAX and not force:
        return jsonify({
            'error': f'Raw Output has {channel_count} channels. Channels DVR usually works best at 750 or fewer.',
            'requires_confirm': True,
            'channel_count': channel_count,
            'recommended_max': _CHANNELS_DVR_RECOMMENDED_MAX,
        }), 409

    def _put(name, url, xmltv_url=''):
        safe = _re.sub(r'[^a-zA-Z0-9]', '', name)
        payload = {
            'name': name,
            'type': dvr_type,
            'source': 'URL',
            'url': url,
            'refresh': '24',
        }
        if xmltv_url:
            payload['xmltv_url'] = xmltv_url
            payload['xmltv_refresh'] = '3600'
        return _req.put(f"{dvr_url}/providers/m3u/sources/{safe}", json=payload, timeout=8, verify=False)

    std_name = 'FastChannels Raw Output'
    gn_name = 'FastChannels Raw Output Gracenote'
    sources_added = []

    try:
        if has_gracenote:
            r1 = _put(gn_name, f"{base}/m3u/gracenote")
            r1.raise_for_status()
            sources_added.append(gn_name)

        r2 = _put(std_name, f"{base}/m3u", f"{base}/epg.xml")
        r2.raise_for_status()
        sources_added.append(std_name)
    except _req.exceptions.ConnectionError:
        return jsonify({'error': f'Could not connect to Channels DVR at {dvr_url}'}), 502
    except _req.exceptions.Timeout:
        return jsonify({'error': 'Channels DVR timed out.'}), 504
    except _req.exceptions.HTTPError as exc:
        resp = exc.response
        return jsonify({'error': f'DVR {resp.status_code}: {resp.text[:300]}'}), 502

    return jsonify({'ok': True, 'sources_added': sources_added})


@dvr_bp.route('/dvr/test-connection', methods=['GET'])
def dvr_test_connection():
    """Ping the configured Channels DVR server and return version info."""
    settings = AppSettings.get()
    dvr_url = (settings.effective_channels_dvr_url() or '').strip()
    if not dvr_url:
        return jsonify({'error': 'Channels DVR URL is not configured in Settings.'}), 400
    try:
        r = _req.get(f"{dvr_url}/status", timeout=5, verify=False)
        r.raise_for_status()
        data = r.json()
        return jsonify({
            'ok': True,
            'version': data.get('version', ''),
            'username': data.get('username', ''),
            'subscription': data.get('subscription', ''),
        })
    except _req.exceptions.ConnectionError:
        return jsonify({'error': f'Could not connect to Channels DVR at {dvr_url}'}), 502
    except _req.exceptions.Timeout:
        return jsonify({'error': 'Channels DVR timed out.'}), 504
    except Exception as exc:
        return jsonify({'error': str(exc)}), 502


@dvr_bp.route('/settings/prismcast/test', methods=['POST'])
def test_prismcast():
    """End-to-end diagnostic for a PrismCast setup. Runs a series of checks — server
    reachable, watch-page URL is a secure context, a real capture flows, and a
    cross-host firewall note — and returns a per-check pass/warn/fail/info result.
    Designed to catch the common setup mistakes (host networking, loopback secure
    context, blocked ports, wrong URLs)."""
    from urllib.parse import urlparse, quote as _quote
    import socket as _socket
    import os as _os
    import time as _time
    import resource as _resource
    import platform as _platform

    settings = AppSettings.get()
    prismcast_url = (settings.effective_prismcast_url() or '').strip().rstrip('/')
    if not prismcast_url:
        return jsonify({'error': 'PrismCast Server URL is not set.'}), 400
    inner = (settings.effective_prismcast_inner_url() or public_base_url() or '').strip().rstrip('/')
    dvr_url = (settings.effective_channels_dvr_url() or '').strip()
    _runtime_os = _platform.system() or 'unknown'

    client_probe = request.get_json(silent=True) or {}
    browser_origin = str(client_probe.get('browser_origin') or '').strip()
    browser_secure_context = client_probe.get('browser_secure_context')
    diagnostic_started = _time.time()
    diagnostics = {
        'browser_context': {
            'origin': browser_origin,
            'is_secure_context': browser_secure_context,
        },
        'settings': {
            'public_base_url': public_base_url() or '',
            'prismcast_url': prismcast_url,
            'watch_page_url_base': inner,
            'channels_dvr_url': dvr_url,
            'drm_bridge_enabled': bool(settings.drm_bridge_enabled),
            'prismcast_max_height': int(settings.prismcast_max_height or 0),
            'diagnostic_runtime_os': _runtime_os,
        },
        'topology': {},
        'health': None,
        'health_snapshots': [],
        'api_probes': [],
        'port_probes': [],
        'attempts': [],
        'candidates': [],
        'proxy_failures': [],
        'resources': {},
        'timing_note': 'Timing is measured from FastChannels around PrismCast /play and HLS polling; source resolution inside PrismCast is not exposed.',
    }

    def _clip(value, limit=600):
        text = str(value or '').strip().replace('\n', ' ')
        return text if len(text) <= limit else text[:limit - 3] + '...'

    def _resource_snapshot():
        snapshot = {'hostname': _socket.gethostname(), 'pid': _os.getpid(),
                    'load_average': None, 'memory_mb': {}, 'cgroup_memory_mb': {}}
        try:
            snapshot['load_average'] = [round(value, 2) for value in _os.getloadavg()]
        except (AttributeError, OSError):
            pass
        try:
            meminfo = {}
            with open('/proc/meminfo', encoding='utf-8') as _fh:
                for line in _fh:
                    key, _, value = line.partition(':')
                    if key in ('MemTotal', 'MemAvailable', 'MemFree'):
                        meminfo[key] = int(value.strip().split()[0]) / 1024
            snapshot['memory_mb'] = {key: round(value, 1) for key, value in meminfo.items()}
        except (OSError, ValueError):
            pass
        for current_path, max_path in (('/sys/fs/cgroup/memory.current', '/sys/fs/cgroup/memory.max'),
                                       ('/sys/fs/cgroup/memory/memory.usage_in_bytes', '/sys/fs/cgroup/memory/memory.limit_in_bytes')):
            try:
                current = int(open(current_path, encoding='utf-8').read().strip()) / (1024 * 1024)
                raw_max = open(max_path, encoding='utf-8').read().strip()
                limit = None if raw_max == 'max' else int(raw_max) / (1024 * 1024)
                snapshot['cgroup_memory_mb'] = {'current': round(current, 1), 'limit': round(limit, 1) if limit is not None else 'unlimited'}
                break
            except (OSError, ValueError):
                continue
        try:
            usage = _resource.getrusage(_resource.RUSAGE_SELF)
            snapshot['max_rss_mb'] = round(float(usage.ru_maxrss) / 1024, 1)
        except (AttributeError, OSError):
            pass
        return snapshot

    diagnostics['resources'] = _resource_snapshot()

    def _snapshot_health(phase, timeout=4):
        t0 = _time.time()
        snap = {'phase': phase, 'ok': False, 'status_code': None, 'latency_ms': None, 'body': None, 'error': ''}
        try:
            r = _req.get(f'{prismcast_url}/health', timeout=timeout)
            snap['status_code'] = r.status_code
            snap['latency_ms'] = int((_time.time() - t0) * 1000)
            if r.status_code == 200:
                try:
                    snap['body'] = r.json()
                    snap['ok'] = True
                except Exception:
                    snap['error'] = 'Invalid JSON health response'
            else:
                snap['error'] = _clip(r.text, 220)
        except Exception as e:
            snap['latency_ms'] = int((_time.time() - t0) * 1000)
            snap['error'] = type(e).__name__
        diagnostics['health_snapshots'].append(snap)
        return snap

    def _probe_prismcast_api(path, timeout=3):
        t0 = _time.time()
        probe = {'path': path, 'status_code': None, 'latency_ms': None, 'available': False, 'content_type': '', 'body': ''}
        try:
            r = _req.get(f'{prismcast_url}{path}', timeout=timeout)
            probe['status_code'] = r.status_code
            probe['latency_ms'] = int((_time.time() - t0) * 1000)
            probe['available'] = r.status_code < 400
            probe['content_type'] = r.headers.get('content-type', '')
            probe['body'] = _clip(r.text, 220) if r.status_code < 400 else ''
        except Exception as e:
            probe['latency_ms'] = int((_time.time() - t0) * 1000)
            probe['body'] = type(e).__name__
        diagnostics['api_probes'].append(probe)
        return probe

    def _probe_port(label, port, timeout=1.5):
        parsed = urlparse(prismcast_url)
        hostname = parsed.hostname or ''
        probe = {'label': label, 'host': hostname, 'port': port, 'open': False, 'latency_ms': None, 'error': ''}
        if not hostname:
            probe['error'] = 'No PrismCast host configured'
            diagnostics['port_probes'].append(probe)
            return probe
        t0 = _time.time()
        try:
            with _socket.create_connection((hostname, int(port)), timeout=timeout):
                probe['open'] = True
        except Exception as e:
            probe['error'] = type(e).__name__
        probe['latency_ms'] = int((_time.time() - t0) * 1000)
        diagnostics['port_probes'].append(probe)
        return probe

    checks = []
    def add(name, status, detail, fix=None):
        checks.append({'name': name, 'status': status, 'detail': detail, 'fix': fix})

    # 1) PrismCast server reachable + healthy (from FastChannels' vantage point)
    health_ok = False
    health_snap = _snapshot_health('before_capture', timeout=8)
    try:
        if health_snap.get('status_code') == 200:
            health_json = health_snap.get('body') or {}
            diagnostics['health'] = health_json
            healthy = (health_json.get('status') == 'healthy') if health_json else True
            health_bits = []
            if health_json.get('version'):
                health_bits.append(f"version {health_json.get('version')}")
            if health_json.get('captureMode'):
                health_bits.append(f"capture {health_json.get('captureMode')}")
            if health_json.get('chrome'):
                health_bits.append(str(health_json.get('chrome')))
            streams = health_json.get('streams') if isinstance(health_json.get('streams'), dict) else {}
            if streams:
                health_bits.append(f"streams {streams.get('active', '?')}/{streams.get('limit', '?')}")
            health_ok = healthy
            add('PrismCast server reachable', 'ok' if healthy else 'warn',
                f'{prismcast_url} responded'
                + (f" ({', '.join(health_bits)})" if health_bits else '')
                + ('' if healthy else ' but status is not "healthy"'))
        else:
            add('PrismCast server reachable', 'fail',
                f"{prismcast_url}/health returned HTTP {health_snap.get('status_code')}")
    except Exception as e:
        add('PrismCast server reachable', 'fail',
            f'Could not reach {prismcast_url} ({type(e).__name__}).',
            fix='Check the URL and that PrismCast is running. If PrismCast uses Docker host '
                'networking, its ports are subject to the host firewall — open it (e.g. '
                '`sudo ufw allow 5589/tcp`).')

    for _path in ('/status', '/debug/streams', '/metrics'):
        _probe_prismcast_api(_path)

    parsed_pc = urlparse(prismcast_url)
    parsed_inner = urlparse(inner)
    _cgroup_text = ''
    try:
        with open('/proc/1/cgroup', encoding='utf-8') as _fh:
            _cgroup_text = _fh.read().lower()
    except Exception:
        pass
    _fastchannels_containerized = bool(
        _os.path.exists('/.dockerenv')
        or any(token in _cgroup_text for token in ('docker', 'containerd', 'kubepods'))
    )
    _pc_host = (parsed_pc.hostname or '').lower()
    _inner_host = (parsed_inner.hostname or '').lower()
    _pc_loopback = _pc_host in ('localhost', '127.0.0.1', '::1')
    _inner_loopback = _inner_host in ('localhost', '127.0.0.1', '::1')
    _public_url = (diagnostics['settings'].get('public_base_url') or '').strip()
    _public_parsed = urlparse(_public_url)
    _public_host = (_public_parsed.hostname or '').lower()
    _public_loopback = _public_host in ('localhost', '127.0.0.1', '::1')
    _docker_socket = _os.path.exists('/var/run/docker.sock')
    _network_mode = (_os.environ.get('PRISMCAST_NETWORK_MODE') or '').strip() or 'unknown'
    diagnostics['topology'] = {
        'fastchannels_runtime': 'docker/container' if _fastchannels_containerized else 'native/unknown',
        'fastchannels_os': _runtime_os,
        'fastchannels_hostname': _socket.gethostname(),
        'prismcast_runtime': (_os.environ.get('PRISMCAST_RUNTIME') or '').strip() or 'unknown',
        'prismcast_host': _pc_host,
        'prismcast_loopback': _pc_loopback,
        'prismcast_network_mode': _network_mode,
        'watch_page_host': _inner_host,
        'watch_page_loopback': _inner_loopback,
        'public_base_host': _public_host,
        'public_base_loopback': _public_loopback,
        'public_watch_host_match': bool(_public_host and _inner_host and _public_host == _inner_host),
        'docker_socket_available': _docker_socket,
        'network_mode_source': 'PRISMCAST_NETWORK_MODE environment variable' if _network_mode != 'unknown' else 'not available without Docker socket inspection',
        'network_mode_note': 'PrismCast network mode is reported from PRISMCAST_NETWORK_MODE when set; otherwise it cannot be inspected here. End-to-end capture remains authoritative.',
    }
    if _inner_loopback:
        add('Runtime / network topology', 'info',
            f"FastChannels runtime: {diagnostics['topology']['fastchannels_runtime']}; PrismCast runtime: {diagnostics['topology']['prismcast_runtime']}; PrismCast network mode: {diagnostics['topology']['prismcast_network_mode']}; Watch-page host: {_inner_host or '(blank)'}.",
            fix='Loopback requires PrismCast Chrome to share the FastChannels host network namespace. For Docker PrismCast, use network_mode: host.')
    else:
        add('Runtime / network topology', 'info',
            f"FastChannels runtime: {diagnostics['topology']['fastchannels_runtime']}; PrismCast runtime: {diagnostics['topology']['prismcast_runtime']}; PrismCast network mode: {diagnostics['topology']['prismcast_network_mode']}; Watch-page host: {_inner_host or '(blank)'}.",
            fix='PrismCast network mode cannot be verified here because the Docker socket is not mounted; confirm the container can reach the Watch-page host.')
    if _public_loopback and _inner_loopback and not _pc_loopback:
        add('URL consistency', 'warn',
            f'Public/watch URL uses {_public_host}, but PrismCast is addressed at {_pc_host}. A separate PrismCast container may resolve loopback to itself.',
            fix='Use the FastChannels host address reachable from PrismCast, or run PrismCast with network_mode: host and keep the loopback watch URL.')
    elif _public_host and _inner_host and _public_host != _inner_host:
        add('URL consistency', 'info',
            f'Public Base URL host {_public_host} and Watch-page host {_inner_host} differ; this can be intentional, but PrismCast must resolve the Watch-page host.')

    parsed_pc = urlparse(prismcast_url)
    _probe_port('PrismCast API', parsed_pc.port or 5589)
    _probe_port('HDHomeRun emulation (optional)', 5004)
    _probe_port('noVNC (optional)', 6080)

    available_api = [p['path'] for p in diagnostics['api_probes'] if p.get('available')]
    unavailable_api = [f"{p['path']}={p.get('status_code') or p.get('body') or 'error'}" for p in diagnostics['api_probes'] if not p.get('available')]
    add('PrismCast API capabilities (optional endpoints)', 'info',
        'Available optional endpoints: ' + (', '.join(available_api) if available_api else 'none detected')
        + (f"; unavailable: {', '.join(unavailable_api)}" if unavailable_api else ''))

    # 2) Watch-page URL is a browser secure context (required for Widevine/EME)
    host = (urlparse(inner).hostname or '').lower()
    scheme = (urlparse(inner).scheme or '').lower()
    if scheme == 'https' or host == 'localhost' or host.startswith('127.'):
        add('Watch-page URL is a secure context', 'ok',
            f'{inner} — ' + ('trusted HTTPS' if scheme == 'https' else 'loopback')
            + ' — a valid secure-context address. This only checks the URL\'s shape; see '
              '"End-to-end capture" below to confirm PrismCast can actually reach it.')
        if host == 'localhost' or host.startswith('127.'):
            add('Loopback watch-page routing', 'info',
                'The Watch-page URL uses loopback. That is correct only when PrismCast Chrome shares the host network namespace.',
                fix='For Docker PrismCast, use `network_mode: host`. In bridge networking, 127.0.0.1 points inside the PrismCast container.')
    else:
        add('Watch-page URL is a secure context', 'warn',
            f'{inner} is a plain http:// LAN address — not a secure context, so Widevine/EME will not decrypt (black capture).',
            fix='If PrismCast runs on the same host as FastChannels, set the Watch-page URL to '
                'loopback (http://127.0.0.1:<port>) and run PrismCast with host networking. '
                'Otherwise serve FastChannels over trusted HTTPS.')

    # The server cannot read chrome://flags, but the browser can report the
    # effective result for the exact admin origin. This catches the insecure-origin
    # exception without pretending to inspect Chrome's private settings page.
    if browser_secure_context is True:
        add('This browser secure context', 'ok',
            f'{browser_origin or "Current browser origin"} reports window.isSecureContext=true.')
    elif browser_secure_context is False:
        add('This browser secure context', 'warn',
            f'{browser_origin or "Current browser origin"} reports window.isSecureContext=false.',
            'For an HTTP LAN origin, enable chrome://flags/#unsafely-treat-insecure-origin-as-secure for this exact origin and relaunch Chrome, or use HTTPS.')

    # 3) Inventory every DRM source before capturing. A successful Amazon test
    # must not hide an untested Roku source.
    candidate_report = _prismcast_candidate_report()
    diagnostics['candidates'] = candidate_report
    test_channels = _prismcast_test_channels(limit=None)
    if _runtime_os.lower() == 'linux':
        cox_candidate = next((item for item in candidate_report if item.get('source') == 'cox' and item.get('source_enabled')), None)
        if cox_candidate:
            cox_skip = 'Skipped on Linux: Cox browser DRM playback is not supported by Cox. This is an upstream platform limitation, not a FastChannels or PrismCast failure.'
            cox_candidate['test_skipped_reason'] = cox_skip
            test_channels = [tc for tc in test_channels if not (tc.source and tc.source.name == 'cox')]
            add('Cox Linux compatibility', 'info', cox_skip,
                'Run the Cox PrismCast test on a supported non-Linux browser/runtime if Cox support is required.')
    selected_by_source = {tc.source.name: tc for tc in test_channels if tc.source}
    selected_names = ', '.join(
        f"{source} ({selected_by_source[source].name})"
        for source in selected_by_source
    )
    if selected_names:
        add('DRM bridge candidates', 'ok', f'Selected one candidate per source: {selected_names}.')
    else:
        add('DRM bridge candidates', 'warn', 'No eligible DRM-bridge candidates were selected.')
    for item in candidate_report:
        if item.get('source_enabled') and not item.get('candidate_count'):
            add(f"{item['source']} candidate readiness", 'warn', item['reason'], item.get('fix'))
        if item.get('source_enabled') and item.get('missing_config'):
            add(f"{item['source']} authentication configuration", 'fail',
                item['auth_detail'], item.get('auth_fix'))

    def _try_capture(tc):
        """Returns (ok, detail). Drives PrismCast and waits for segments to flow."""
        capture_request_id = secrets.token_hex(8)
        watch = f'{inner}/watch/{tc.id}?debug=1&capture_probe=1&fc_request_id={capture_request_id}'
        play = f'{prismcast_url}/play?url={_quote(watch, safe="")}&profile=keyboardFullscreen'
        attempt = {
            'request_id': capture_request_id,
            'channel_id': tc.id,
            'source': tc.source.name if tc.source else '',
            'source_channel_id': tc.source_channel_id,
            'channel_name': tc.name,
            'watch_url': watch,
            'prismcast_play_url': play,
            'play_status': None,
            'play_body': '',
            'redirect_url': '',
            'hls_statuses': [],
            'segments': 0,
            'elapsed_seconds': None,
            'prismcast_play_elapsed_seconds': None,
            'hls_probe_elapsed_seconds': None,
            'failure_class': '',
        }
        diagnostics['attempts'].append(attempt)
        t0 = _time.time()
        play_t0 = _time.time()
        try:
            attempt['health_before_play'] = _snapshot_health(f'before_play:{tc.id}')
            play_t0 = _time.time()
            r = _req.get(play, timeout=25, allow_redirects=False)
            attempt['prismcast_play_elapsed_seconds'] = round(_time.time() - play_t0, 3)
        except _req.exceptions.Timeout:
            attempt['prismcast_play_elapsed_seconds'] = round(_time.time() - play_t0, 3)
            attempt['elapsed_seconds'] = round(_time.time() - t0, 1)
            attempt['failure_class'] = 'watch_page_unreachable_or_capture_start_timeout'
            return False, f'"{tc.name}": PrismCast /play timed out (couldn\'t reach the watch page or reach a playable state).'
        except Exception as e:
            attempt['prismcast_play_elapsed_seconds'] = round(_time.time() - play_t0, 3)
            attempt['elapsed_seconds'] = round(_time.time() - t0, 1)
            attempt['play_body'] = type(e).__name__
            attempt['failure_class'] = 'prismcast_request_error'
            return False, f'"{tc.name}": {type(e).__name__}.'
        attempt['play_status'] = r.status_code
        attempt['health_after_play'] = _snapshot_health(f'after_play:{tc.id}')
        if r.status_code not in (301, 302) or not r.headers.get('Location'):
            body = _clip(r.text, 220)
            attempt['play_body'] = _clip(r.text)
            attempt['elapsed_seconds'] = round(_time.time() - t0, 1)
            if tc.source and tc.source.name == 'amazon_prime_free' and r.status_code in (401, 403, 500):
                attempt['auth_failure'] = True
                attempt['failure_class'] = 'license_or_auth_failure'
                return False, (f'"{tc.name}": Amazon playback authentication failed '
                               f'(HTTP {r.status_code}); the cookie/session may be missing or expired.')
            attempt['failure_class'] = 'capture_start_failed'
            detail = f'"{tc.name}": PrismCast /play returned HTTP {r.status_code}'
            if body:
                detail += f': {body}'
            return False, detail + '.'
        hls = r.headers['Location']
        if not hls.startswith('http'):
            hls = prismcast_url + hls
        attempt['redirect_url'] = hls
        segs = 0
        hls_t0 = _time.time()
        deadline = _time.time() + 12
        while _time.time() < deadline:
            _time.sleep(3)
            try:
                hls_r = _req.get(hls, timeout=8)
                playlist = hls_r.text
                attempt['hls_statuses'].append(hls_r.status_code)
                segs = (playlist.count('.m4s') + playlist.count('.ts')
                        + sum(1 for line in playlist.splitlines()
                              if line and not line.startswith('#')))
            except Exception:
                attempt['hls_statuses'].append('error')
            if segs >= 2:
                break
        attempt['segments'] = segs
        attempt['hls_probe_elapsed_seconds'] = round(_time.time() - hls_t0, 3)
        attempt['elapsed_seconds'] = round(_time.time() - t0, 1)
        attempt['health_after_hls'] = _snapshot_health(f'after_hls:{tc.id}')
        try:
            from .play import get_watch_debug_snapshot
            watch_debug = get_watch_debug_snapshot(capture_request_id)
        except Exception:
            watch_debug = {}
        if watch_debug:
            attempt['watch_debug'] = watch_debug
        browser_played = bool(watch_debug.get('browser_playback')) or (
            int(watch_debug.get('max_ready_state') or 0) >= 4
            and int(watch_debug.get('max_decoded') or 0) > 0
        )
        if segs >= 2:
            attempt['failure_class'] = 'capture_success'
            return True, f'Captured "{tc.name}" in {_time.time()-t0:.0f}s — {segs}+ segments flowing. The full chain works.'
        if browser_played:
            attempt['failure_class'] = 'hls_probe_failed_after_browser_playback'
            decoded = int(watch_debug.get('max_decoded') or 0)
            return False, (f'"{tc.name}": browser playback succeeded in PrismCast '
                           f'(decoded {decoded} frame(s)), but the diagnostic HLS probe did not see segments.')
        if 404 in attempt['hls_statuses']:
            attempt['failure_class'] = 'manifest_or_segment_failure'
            return False, f'"{tc.name}": session started but the HLS playlist returned 404 while polling.'
        attempt['failure_class'] = 'manifest_or_segment_failure'
        return False, f'"{tc.name}": session started but produced no video.'

    if not health_ok:
        add('End-to-end capture', 'skip', 'Skipped — PrismCast is not reachable.')
    elif not test_channels:
        add('End-to-end capture', 'skip',
            'Skipped — no active, enabled DRM-bridge channel is available to test.',
            fix='Enable Bridge DRM channels, scrape or audit a DRM-capable source, then rerun this setup test.')
    else:
        attempts = []
        captured = []
        for tc in test_channels:
            ok_cap, detail = _try_capture(tc)
            attempts.append(detail)
            if ok_cap:
                captured.append(detail)
        if any(attempt.get('auth_failure') for attempt in diagnostics['attempts']):
            add('Amazon authentication', 'fail',
                'Amazon playback authentication failed; the configured cookie/session is likely expired or invalid.',
                'Complete Amazon login or replace the cookie header, then rerun the test.')
        if captured:
            failed = [detail for detail in attempts if detail not in captured]
            if failed:
                add('End-to-end capture', 'warn',
                    'Captured: ' + ' | '.join(captured) + ' Failed: ' + ' | '.join(failed))
            else:
                add('End-to-end capture', 'ok', ' | '.join(captured))
        else:
            is_loopback = (host == 'localhost' or host.startswith('127.'))
            fix = ('If channels reach a playable state nowhere, PrismCast\'s Chrome is usually '
                   'failing to reach the Watch-page URL at all, not decrypting it. ')
            if is_loopback:
                fix += ('The Watch-page URL is loopback — if PrismCast runs in Docker, confirm '
                        'it\'s using `network_mode: host`. Without it, `127.0.0.1` resolves '
                        'inside PrismCast\'s own container, not this host, so the connection is '
                        'refused. Being on the same physical host is not enough.')
            else:
                fix += ('Confirm the Watch-page URL is loopback (with PrismCast on host '
                        'networking) or trusted HTTPS, and that PrismCast (Chrome) can reach it.')
            fix += ' If only some channels fail, those channels couldn\'t resolve right now (not a PrismCast problem).'
            add('End-to-end capture', 'fail',
                'No test channel captured. Tried: ' + ' | '.join(attempts), fix=fix)

    failed_attempts = [a for a in diagnostics['attempts'] if a.get('failure_class') and a.get('failure_class') != 'capture_success']
    if failed_attempts:
        classes = ', '.join(f"{a.get('source') or 'unknown'}={a.get('failure_class')}" for a in failed_attempts)
        add('Capture failure classification', 'info',
            'Best-effort classification from HTTP status, timing, health, and HLS results: ' + classes,
            'This classification is inferential; PrismCast does not expose its internal browser error in the diagnostic API.')
    if diagnostics.get('health_snapshots'):
        last_health = diagnostics['health_snapshots'][-1].get('body') or {}
        streams = last_health.get('streams') if isinstance(last_health.get('streams'), dict) else {}
        diagnostics['resources']['prismcast_streams'] = {'active': streams.get('active'), 'limit': streams.get('limit')}

    from .play import get_recent_proxy_failures
    diagnostics['proxy_failures'] = get_recent_proxy_failures(since=diagnostic_started)
    license_failures = [
        f for f in diagnostics['proxy_failures']
        if str(f.get('label') or '').endswith('-license') and int(f.get('upstream_status') or 0) >= 400
    ]
    if license_failures:
        details = ', '.join(
            f"{f.get('label')} HTTP {f.get('upstream_status')}"
            for f in license_failures[:5]
        )
        add('DRM license proxy failures', 'fail',
            f'One or more DRM license exchanges failed during capture: {details}.')

    # 4) Cross-host firewall note (FastChannels can't test the DVR→PrismCast path directly)
    dvr_host = (urlparse(dvr_url).hostname or '') if dvr_url else ''
    pc_host = (urlparse(prismcast_url).hostname or '')
    if dvr_host and pc_host in ('localhost', '127.0.0.1') and dvr_host not in ('localhost', '127.0.0.1'):
        add('Channels DVR playlist URL', 'warn',
            f'PrismCast Server URL is {prismcast_url}. If Channels DVR runs on another host, it cannot use that loopback address.',
            fix='Set PrismCast Server URL to a LAN or Tailscale address that Channels DVR can reach, while keeping Watch-page URL loopback if PrismCast shares the FastChannels host.')
    if dvr_host and pc_host and dvr_host != pc_host:
        _port = urlparse(prismcast_url).port or 5589
        add('Channels DVR → PrismCast reachability', 'info',
            f'Your DVR ({dvr_host}) and PrismCast ({pc_host}) are on different hosts. FastChannels '
            'reached PrismCast, but your DVR must reach it too — this test can\'t verify that hop.',
            fix=f'If streams time out, open PrismCast’s port on its host: `sudo ufw allow {_port}/tcp`.')

    overall = ('fail' if any(c['status'] == 'fail' for c in checks)
               else 'warn' if any(c['status'] == 'warn' for c in checks)
               else 'ok')
    overall_label = {
        'ok': 'PASS',
        'warn': 'PASS WITH WARNINGS',
        'fail': 'FAIL',
    }[overall]
    return jsonify({
        'checks': checks,
        'overall': overall,
        'overall_label': overall_label,
        'diagnostics': diagnostics,
    })


def _prismcast_test_channels(limit: int | None = None) -> list[Channel]:
    """Return deterministic DRM-bridge channels for the PrismCast setup test.

    The setup test is meant to validate the DRM bridge, so candidates must be
    real bridge-required channels from sources with license handling. Generic
    FAST channels are intentionally excluded; a random clear channel can fail
    for source-specific reasons that have nothing to do with PrismCast.
    """
    capable = sorted(set(_drm_bridge_capable_sources()))
    if not capable or (limit is not None and limit <= 0):
        return []

    rows = (Channel.query.join(Source)
            .filter(Source.name.in_(capable),
                    Source.is_enabled == True,
                    Source.epg_only == False,
                    Channel.requires_drm_bridge == True,
                    Channel.is_active == True,
                    Channel.is_enabled == True,
                    Channel.stream_url != None,
                    Channel.source_channel_id != None,
                    Channel.source_channel_id != '')
            .order_by(Source.name.asc(),
                      Channel.number.asc().nullslast(),
                      Channel.name.asc(),
                      Channel.id.asc())
            .all())

    selected = []
    seen_sources = set()
    for ch in rows:
        source_name = ch.source.name if ch.source else None
        if not source_name or source_name in seen_sources:
            continue
        selected.append(ch)
        seen_sources.add(source_name)
        if limit is not None and len(selected) >= limit:
            break
    return selected



def _prismcast_candidate_report() -> list[dict]:
    """Report source prerequisites and bridge-flagged candidate counts."""
    report = []
    for source_name in sorted(set(_drm_bridge_capable_sources())):
        scraper_cls = registry.get(source_name)
        source = Source.query.filter_by(name=source_name).first()
        config = (source.config or {}) if source else {}
        required = list(getattr(scraper_cls, 'audit_requires_config', []) or [])
        missing = [key for key in required if not str(config.get(key) or '').strip()]
        total_active_enabled = 0
        candidate_count = 0
        if source:
            base = Channel.query.filter_by(source_id=source.id, is_active=True, is_enabled=True)
            total_active_enabled = base.count()
            candidate_count = base.filter_by(requires_drm_bridge=True).count()
        source_enabled = bool(source and source.is_enabled and not source.epg_only)
        if not source:
            reason = 'Not tested: source is not present; scrape or enable it before testing.'
            fix = 'Enable and scrape this DRM-capable source.'
        elif not source_enabled:
            reason = 'Not tested: source is disabled or EPG-only.'
            fix = 'Enable the source before running the PrismCast test.'
        elif not candidate_count:
            reason = f'Not tested: no active/enabled channels are marked for bridging ({total_active_enabled} active/enabled channels found).'
            fix = 'Run Stream Audit and enable Bridge DRM channels, then rerun this test.'
        else:
            reason = f'{candidate_count} active/enabled bridge candidate(s) available.'
            fix = None
        if missing:
            if source_name == 'amazon_prime_free' and 'cookie_header' in missing:
                auth_detail = 'Amazon cookie/session header is missing; playback cannot authenticate.'
                auth_fix = 'Complete Amazon login or paste a current cookie header, then rerun the test.'
            else:
                auth_detail = 'Required source configuration is missing: ' + ', '.join(missing) + '.'
                auth_fix = 'Complete the source configuration, then rerun the test.'
        elif required:
            auth_detail = 'Required authentication configuration is present; playback will verify whether it is still valid.'
            auth_fix = None
        else:
            auth_detail = 'No source credentials are required.'
            auth_fix = None
        report.append({
            'source': source_name,
            'source_enabled': source_enabled,
            'total_active_enabled': total_active_enabled,
            'candidate_count': candidate_count,
            'selected_channel_id': None,
            'selected_channel_name': None,
            'required_config': required,
            'missing_config': missing,
            'auth_detail': auth_detail,
            'auth_fix': auth_fix,
            'reason': reason,
            'fix': fix,
        })
    selected = {ch.source.name: ch for ch in _prismcast_test_channels(limit=None) if ch.source}
    for item in report:
        channel = selected.get(item['source'])
        if channel:
            item['selected_channel_id'] = channel.id
            item['selected_channel_name'] = channel.name
    return report


def _drm_bridge_capable_sources() -> list[str]:
    """Source names whose scraper has DRM license handling (can be PrismCast-bridged)."""
    from ..scrapers.registry import drm_capable_source_names
    return drm_capable_source_names()


def _reconcile_drm_bridge_mode() -> None:
    """Apply a DRM-bridge toggle change (PrismCast's drm_bridge_enabled or
    FastChannels Player's fc_player_bridge_enabled) to existing channels so it takes
    effect at once, without waiting for the next stream audit.

    Per bridge-capable source, recomputes eligibility via drm_bridge.drm_bridge_mode_for
    (true if EITHER bridge can serve it) and reconciles accordingly:
      now eligible     : disabled-DRM channels are recovered — kept active and marked
                         requires_drm_bridge; intrinsically bridge-only rows are flagged too.
      no longer eligible: bridged channels drop back to the legacy disabled state.

    Important: reconciliation is per-source, not global — a source still eligible via
    one bridge (e.g. FastChannels Player) must not be disabled just because the other
    bridge (PrismCast) was toggled off.
    """
    from ..drm_bridge import drm_bridge_mode_for
    capable = _drm_bridge_capable_sources()
    recovered = 0
    intrinsic_flagged = 0
    disabled = 0
    for source_name in capable:
        if drm_bridge_mode_for(source_name):
            rows = (Channel.query.join(Source)
                    .filter(Source.name == source_name,
                            Channel.disable_reason.like('DRM%'),
                            Channel.is_active == False)
                    .all())
            for ch in rows:
                ch.requires_drm_bridge = True
                ch.is_active = True
                ch.is_enabled = True
                ch.disable_reason = None
            recovered += len(rows)

            # Also flag intrinsically bridge-only rows without waiting for audit.
            # Most DRM-capable sources only need DASH rows; all-DRM HLS sources such
            # as DirecTV Stream opt in with all_channels_require_drm_bridge.
            _cls = registry.get(source_name)
            if _cls:
                q = (Channel.query.join(Source)
                     .filter(Source.name == source_name,
                             Channel.is_active == True,
                             Channel.requires_drm_bridge == False))
                if not getattr(_cls, 'all_channels_require_drm_bridge', False):
                    q = q.filter(Channel.stream_type == 'dash')
                intrinsic = q.all()
                for ch in intrinsic:
                    ch.requires_drm_bridge = True
                intrinsic_flagged += len(intrinsic)
        else:
            rows = (Channel.query.join(Source)
                    .filter(Source.name == source_name,
                            Channel.requires_drm_bridge == True)
                    .all())
            for ch in rows:
                ch.requires_drm_bridge = False
                ch.is_active = False
                ch.is_enabled = False
                ch.disable_reason = ch.disable_reason or 'DRM'
            disabled += len(rows)
    logger.info('[drm-bridge] mode reconciled — recovered %d disabled-DRM + flagged %d intrinsic, '
                'disabled %d no-longer-bridged channel(s)', recovered, intrinsic_flagged, disabled)
    db.session.flush()



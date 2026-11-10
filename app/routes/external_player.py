"""Resolve-only API for external tuners that launch FastChannels Player themselves.

APITuner (github.com/matthewfkoch/APITuner, issue #94) drives Android TV devices
through its own HTTP Agent and relays its own HDMI encoder. It needs three things
from us: which channels need the Player, the Player intent extras for one of them,
and their guide. This API never connects to a device, allocates capture hardware
or registers tune tracking; the external tuner owns playback and its lifecycle.
That means our adb-only extras don't apply to these tunes: idle-stop, the Sling
block-boundary adb retune (the Player's own boundary swap still runs), logcat
diagnostics and the Bridge page's device list.

Off unless FC_EXTERNAL_PLAYER_TOKEN is set; requests then need
`Authorization: Bearer <token>`. The paths and response shapes are APITuner's v1
contract (its v0.1.35-fastchannels.1 client), so keep them compatible: add fields,
don't rename or remove them.
"""
from __future__ import annotations

import hmac
import logging

from flask import Blueprint, Response, jsonify, request, stream_with_context

from .. import fc_player_bridge
from ..drm_bridge import DRM_BRIDGE_TRUSTED_SOURCES
from ..models import Channel, Source

logger = logging.getLogger(__name__)

external_player_bp = Blueprint('external_player', __name__)

API_VERSION = 1
_PLAYER_COMPONENT = f'{fc_player_bridge.PLAYER_PACKAGE}/.PlaybackActivity'
_MAX_ID_LEN = 512


@external_player_bp.before_request
def _authorize():
    expected = fc_player_bridge.external_player_token()
    if not expected:
        return jsonify(error='External player API is disabled'), 503
    supplied = request.headers.get('Authorization', '')
    if not hmac.compare_digest(supplied.encode(), f'Bearer {expected}'.encode()):
        return jsonify(error='External player token required'), 401
    return None


@external_player_bp.after_request
def _no_store(response):
    response.headers['Cache-Control'] = 'no-store'
    return response


def _is_retired(channel) -> bool:
    from .play import _channel_is_retired
    return _channel_is_retired(channel)


def _catalog_channels() -> list:
    """The channels the FastChannels Player bridge playlist carries
    (generate_fc_player_m3u, without its Gracenote split): bridge-only, enabled,
    active, from an enabled source the Player can decrypt."""
    from ..generators.m3u import _build_channel_query
    return [
        ch for ch in _build_channel_query({}, activity='drm_bridge').all()
        if ch.source and ch.source.name in DRM_BRIDGE_TRUSTED_SOURCES and not _is_retired(ch)
    ]


@external_player_bp.get('/capabilities')
def capabilities():
    return jsonify(version=API_VERSION, capabilities=['catalog', 'prepare', 'xmltv'],
                   controls_devices=False, package=fc_player_bridge.PLAYER_PACKAGE)


@external_player_bp.get('/channels')
def channels():
    from ..generators.m3u import _parse_gracenote_id, _tvg_id
    rows = [{
        'source': ch.source.name,
        'channel_id': ch.source_channel_id,
        'name': ch.name,
        'tvg_id': _tvg_id(ch),
        'gracenote_id': _parse_gracenote_id(ch),
        'logo_url': ch.logo_url,
        'group': ch.category,
        'number': ch.number,
    } for ch in _catalog_channels()]
    return jsonify(version=API_VERSION, channels=rows)


def _catalog_channel(source_name: str, channel_id: str):
    """One catalog channel, or None. Same rules as _catalog_channels, for one row."""
    from ..generators.m3u import bridge_only_clause
    if source_name not in DRM_BRIDGE_TRUSTED_SOURCES:
        return None
    return (Channel.query.join(Source)
            .filter(Source.name == source_name,
                    Channel.source_channel_id == channel_id,
                    Source.is_enabled == True,
                    Source.epg_only == False,
                    Channel.is_enabled == True,
                    Channel.is_active == True,
                    Channel.stream_url != None,
                    bridge_only_clause())
            .first())


@external_player_bp.post('/prepare')
def prepare():
    body = request.get_json(silent=True)
    if not isinstance(body, dict) or set(body) != {'source', 'channel_id'}:
        return jsonify(error='Expected source and channel_id only'), 400
    source_name, channel_id = body['source'], body['channel_id']
    if any(not isinstance(v, str) or not v.strip() or len(v) > _MAX_ID_LEN
           for v in (source_name, channel_id)):
        return jsonify(error='Invalid channel identity'), 400

    channel = _catalog_channel(source_name, channel_id)
    if channel is None:
        return jsonify(error='Channel is unavailable or not player-compatible'), 404
    if _is_retired(channel):
        return jsonify(error='Channel has retired'), 410
    try:
        extras = fc_player_bridge.prepare_player_extras(channel, request.host_url)
    except Exception as e:
        # Resolver errors can carry tokens or provider URLs: log the type only.
        logger.warning('[external-player] prepare failed for %s:%s (%s)',
                       source_name, channel_id, type(e).__name__)
        return jsonify(error='Could not prepare channel playback'), 502
    logger.info('[external-player] prepared %s:%s for %s drm=%s',
                source_name, channel_id, request.remote_addr, extras['drm'])
    return jsonify(version=API_VERSION, package=fc_player_bridge.PLAYER_PACKAGE,
                   component=_PLAYER_COMPONENT, intent_extras=extras)


@external_player_bp.get('/xmltv')
def xmltv():
    from ..generators.xmltv import generate_xmltv_stream
    ids = [ch.id for ch in _catalog_channels()]
    if not ids:
        return Response('<?xml version="1.0" encoding="UTF-8"?>\n<tv/>\n', mimetype='application/xml')
    return Response(
        stream_with_context(generate_xmltv_stream({'channel_ids': ids}, base_url=request.host_url,
                                                  native=True)),
        mimetype='application/xml',
    )

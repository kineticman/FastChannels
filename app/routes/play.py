"""
/play/<source>/<channel_id>.m3u8

Resolves the real stream URL at request time and issues a 302 redirect.
If the resolved manifest contains DRM (SAMPLE-AES or AES-128), the channel
is automatically marked is_active=False so it drops out of M3U/EPG output.
It remains visible in the admin channels page so users can see what was
disabled and manually re-enable if desired.
"""
import logging
import threading

from flask import Blueprint, redirect, abort, request
from app.config_store import persist_source_config_updates
from ..hls import inspect_hls_drm
from ..models import Channel, Source
from ..scrapers import registry
from ..scrapers.base import StreamDeadError
from .tasks import trigger_channel_auto_disable

logger = logging.getLogger(__name__)

play_bp = Blueprint('play', __name__)

def _client_ip() -> str:
    forwarded = (request.headers.get('X-Forwarded-For') or '').strip()
    if forwarded:
        return forwarded.split(',', 1)[0].strip()
    real_ip = (request.headers.get('X-Real-IP') or '').strip()
    if real_ip:
        return real_ip
    return request.remote_addr or 'unknown'


def _check_manifest(url: str, session) -> str | None:
    """
    Fetch the HLS manifest at url and return a disable reason string if the
    stream is unplayable, or None if it looks fine.
    Returns None on any fetch error (fail open — don't disable on network hiccups).
    Returns 'Unauthorized' on 401 so callers can handle expired session tokens.
    """
    try:
        from urllib.parse import urljoin
        r = session.get(url, timeout=8)
        if r.status_code == 401:
            return 'Unauthorized'
        if r.status_code != 200:
            return None
        text = r.text

        # EXT-X-KEY and EXT-X-PLAYLIST-TYPE only appear in media playlists,
        # not master playlists. If we landed on a master, fetch the first variant.
        if '#EXT-X-STREAM-INF' in text:
            for line in text.splitlines():
                line = line.strip()
                if line and not line.startswith('#'):
                    try:
                        rv = session.get(urljoin(url, line), timeout=8)
                        if rv.status_code == 200:
                            text = rv.text
                    except Exception:
                        pass
                    break

        if 'EXT-X-PLAYLIST-TYPE:VOD' in text:
            logger.info('[play] VOD playlist (not live) in manifest: %s', url[:80])
            return 'Dead'

        drm = inspect_hls_drm(text)
        if drm:
            logger.info('[play] DRM detected (%s) in manifest: %s', drm['drm_type'], url[:80])
            return 'DRM'
    except Exception as e:
        logger.debug('[play] manifest check fetch failed (ignoring): %s', e)
    return None


@play_bp.route('/play/<source_name>/<channel_id>.m3u8')
def play(source_name: str, channel_id: str):
    client_ip = _client_ip()
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == source_name, Channel.source_channel_id == channel_id)
        .first()
    )
    if not channel:
        logger.warning('[play] request ip=%s unknown channel %s/%s', client_ip, source_name, channel_id)
        abort(404)

    logger.info(
        '[play] request ip=%s source=%s channel_id=%s channel_name=%s',
        client_ip, source_name, channel_id, channel.name,
    )

    scraper_cls = registry.get(source_name)
    scraper = None
    if scraper_cls:
        scraper = scraper_cls(config=channel.source.config or {})
        try:
            resolved_url = scraper.resolve(channel.stream_url)
        except StreamDeadError as e:
            logger.error(
                '[play] channel confirmed dead ip=%s source=%s channel_id=%s channel_name=%s: %s',
                client_ip, source_name, channel_id, channel.name, e,
            )
            trigger_channel_auto_disable(channel.id, 'Dead')
            resolved_url = None
        except Exception as e:
            logger.error(
                '[play] resolve failed ip=%s source=%s channel_id=%s channel_name=%s: %s',
                client_ip, source_name, channel_id, channel.name, e,
            )
            resolved_url = None
        finally:
            if scraper._pending_config_updates:
                try:
                    persist_source_config_updates(
                        channel.source_id,
                        scraper._pending_config_updates,
                    )
                except Exception as ce:
                    db.session.rollback()
                    logger.warning('[play] failed to persist config updates: %s', ce)
    else:
        resolved_url = channel.stream_url

    if not resolved_url or (source_name == 'roku' and resolved_url.startswith('roku://')):
        abort(502)

    # Fire-and-forget manifest check — detect DRM or dead streams without
    # blocking the redirect. The check runs in a background thread so Channels
    # DVR gets the 302 immediately, avoiding 504s on slow upstream sources.
    if channel.is_active and resolved_url and resolved_url.startswith('http'):
        from flask import current_app
        _app = current_app._get_current_object()
        _channel_id = channel.id
        _source_name = source_name
        _source_id = channel.source_id

        def _bg_check():
            import requests
            # Use a plain session without retry adapters — this is a one-shot
            # health probe; retries just add latency in the background thread.
            s = requests.Session()
            reason = _check_manifest(resolved_url, s)
            if not reason:
                return
            if reason == 'Unauthorized' and _source_name == 'roku':
                # OSM session token has expired. Clear both osm_session AND
                # stream_url_cache — all cached OSM URLs embed the same stale
                # token, and _load_stream_url_cache() would otherwise extract it
                # and rebuild _osm_session from the cache, defeating the clear.
                logger.warning('[play] Roku OSM token expired (401) — clearing osm_session and stream_url_cache')
                with _app.app_context():
                    try:
                        persist_source_config_updates(_source_id, {
                            'osm_session': None,
                            'stream_url_cache': None,  # None replaces; {} would merge (no-op)
                        })
                    except Exception as e:
                        logger.warning('[play] failed to clear osm_session: %s', e)
                return
            with _app.app_context():
                trigger_channel_auto_disable(_channel_id, reason)

        threading.Thread(target=_bg_check, daemon=True).start()

    logger.debug(
        '[play] redirect ip=%s source=%s channel_id=%s channel_name=%s → %s',
        client_ip, source_name, channel_id, channel.name, resolved_url[:80],
    )
    return redirect(resolved_url, 302)

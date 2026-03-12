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

from flask import Blueprint, redirect, abort, current_app, request
from sqlalchemy.orm.attributes import flag_modified
from ..models import Channel, Source
from ..extensions import db
from ..scrapers import registry

logger = logging.getLogger(__name__)

play_bp = Blueprint('play', __name__)

# DRM encryption methods that indicate a channel is unplayable on open clients
_DRM_METHODS = ('SAMPLE-AES',)  # AES-128 with key URL is standard HLS, not FairPlay


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
    """
    try:
        from urllib.parse import urljoin
        r = session.get(url, timeout=8)
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

        for method in _DRM_METHODS:
            if f'METHOD={method}' in text:
                logger.info('[play] DRM detected (%s) in manifest: %s', method, url[:80])
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
        except Exception as e:
            logger.error(
                '[play] resolve failed ip=%s source=%s channel_id=%s channel_name=%s: %s',
                client_ip, source_name, channel_id, channel.name, e,
            )
            resolved_url = None
        finally:
            if scraper._pending_config_updates:
                try:
                    updated = dict(channel.source.config or {})
                    updated.update(scraper._pending_config_updates)
                    channel.source.config = updated
                    flag_modified(channel.source, 'config')
                    db.session.commit()
                except Exception as ce:
                    logger.warning('[play] failed to persist config updates: %s', ce)
                    db.session.rollback()
    else:
        resolved_url = channel.stream_url

    if not resolved_url or (source_name == 'roku' and resolved_url.startswith('roku://')):
        abort(502)

    # Fire-and-forget manifest check — detect DRM or dead streams without
    # blocking the redirect. The check runs in a background thread so Channels
    # DVR gets the 302 immediately, avoiding 504s on slow upstream sources.
    if channel.is_active and resolved_url and resolved_url.startswith('http'):
        app = current_app._get_current_object()
        ch_id = channel.id
        check_session = scraper.session if scraper_cls else None

        def _bg_check():
            import requests
            s = check_session or requests.Session()
            reason = _check_manifest(resolved_url, s)
            if not reason:
                return
            with app.app_context():
                try:
                    ch = Channel.query.get(ch_id)
                    if ch and ch.is_active:
                        ch.is_active      = False
                        ch.is_enabled     = False
                        ch.disable_reason = reason
                        db.session.commit()
                        logger.warning(
                            '[play] %s detected — auto-disabled channel %s (%s/%s)',
                            reason, ch.name, source_name, channel_id,
                        )
                except Exception as e:
                    logger.error('[play] failed to persist disable flag for %s: %s', ch_id, e)
                    db.session.rollback()

        threading.Thread(target=_bg_check, daemon=True).start()

    logger.debug(
        '[play] redirect ip=%s source=%s channel_id=%s channel_name=%s → %s',
        client_ip, source_name, channel_id, channel.name, resolved_url[:80],
    )
    return redirect(resolved_url, 302)

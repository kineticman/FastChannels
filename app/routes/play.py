"""
/play/<source>/<channel_id>.m3u8

Resolves the real stream URL at request time and issues a 302 redirect.
If the resolved manifest contains DRM (SAMPLE-AES or AES-128), the channel
is automatically marked is_active=False so it drops out of M3U/EPG output.
It remains visible in the admin channels page so users can see what was
disabled and manually re-enable if desired.
"""
import logging

from flask import Blueprint, redirect, abort
from ..models import Channel, Source
from ..extensions import db
from ..scrapers import registry

logger = logging.getLogger(__name__)

play_bp = Blueprint('play', __name__)

# DRM encryption methods that indicate a channel is unplayable on open clients
_DRM_METHODS = ('SAMPLE-AES',)  # AES-128 with key URL is standard HLS, not FairPlay


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
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == source_name, Channel.source_channel_id == channel_id)
        .first()
    )
    if not channel:
        logger.warning('[play] unknown channel %s/%s', source_name, channel_id)
        abort(404)

    scraper_cls = registry.get(source_name)
    if scraper_cls:
        scraper = scraper_cls()
        try:
            resolved_url = scraper.resolve(channel.stream_url)
        except Exception as e:
            logger.error('[play] resolve failed for %s/%s: %s', source_name, channel_id, e)
            resolved_url = channel.stream_url
    else:
        resolved_url = channel.stream_url

    # Manifest check — detect DRM or stale VOD streams.
    # Only runs once per channel: skip if already marked inactive.
    if channel.is_active and resolved_url and resolved_url.startswith('http'):
        session = scraper.session if scraper_cls else None
        if session is None:
            import requests
            session = requests.Session()

        disable_reason = _check_manifest(resolved_url, session)
        if disable_reason:
            try:
                channel.is_active      = False
                channel.is_enabled     = False
                channel.disable_reason = disable_reason
                db.session.commit()
                logger.warning(
                    '[play] %s detected — auto-disabled channel %s (%s/%s)',
                    disable_reason, channel.name, source_name, channel_id,
                )
            except Exception as e:
                logger.error('[play] failed to persist disable flag for %s: %s', channel_id, e)
                db.session.rollback()
            abort(451)

    logger.debug('[play] %s/%s → %s', source_name, channel_id, resolved_url[:80])
    return redirect(resolved_url, 302)

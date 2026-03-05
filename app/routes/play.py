"""
/play/<source>/<channel_id>.m3u8

Resolves the real stream URL at request time and issues a 302 redirect.
Each scraper can override resolve() for custom logic (macro substitution,
HLS master→variant, auth tokens, etc.).
"""
import logging

from flask import Blueprint, redirect, abort
from ..models import Channel, Source
from ..scrapers import registry

logger = logging.getLogger(__name__)

play_bp = Blueprint('play', __name__)


@play_bp.route('/play/<source_name>/<channel_id>.m3u8')
def play(source_name: str, channel_id: str):
    # Look up channel in DB
    channel = (
        Channel.query
        .join(Source)
        .filter(Source.name == source_name, Channel.source_channel_id == channel_id)
        .first()
    )
    if not channel:
        logger.warning(f'[play] unknown channel {source_name}/{channel_id}')
        abort(404)

    # Get the scraper for this source
    scraper_cls = registry.get(source_name)
    if scraper_cls:
        scraper = scraper_cls()
        try:
            resolved_url = scraper.resolve(channel.stream_url)
        except Exception as e:
            logger.error(f'[play] resolve failed for {source_name}/{channel_id}: {e}')
            resolved_url = channel.stream_url  # fall back to raw stored URL
    else:
        # No scraper registered — serve raw URL directly
        resolved_url = channel.stream_url

    logger.debug(f'[play] {source_name}/{channel_id} → {resolved_url[:80]}')
    return redirect(resolved_url, 302)

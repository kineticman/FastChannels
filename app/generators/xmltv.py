"""
XMLTV / EPG generator.

Performance design:
  - JOIN instead of IN(1001 ids) — SQLite handles joins far better than
    large IN clauses which degrade O(n²).
  - Keyset pagination (WHERE id > last_id) instead of LIMIT/OFFSET —
    OFFSET scans all preceding rows on every page, so page 700 of 200
    scans 140,000 rows just to skip them.
  - Streaming generator — yields chunks so Flask/gunicorn never blocks
    waiting for the full 56MB to build in memory.
  - /epg.xml.gz endpoint serves pre-gzipped content (~5MB vs ~56MB).
    Also honours Accept-Encoding: gzip on /epg.xml.
"""
from __future__ import annotations

import gzip
import io
import logging
from datetime import datetime, timedelta, timezone
from xml.etree.ElementTree import Element, SubElement, tostring

from ..extensions import db
from ..models import Program
from ..url import proxy_logo_url
from .m3u import _selected_channels, _tvg_id, _channel_display_name, _source_multi_country_map, _sanitize

log = logging.getLogger(__name__)

# Maps common scraped category variants to the canonical values Channels DVR
# recognises for guide filtering (Movie, Children, News, Sports, Drama).
_XMLTV_CAT_NORM = {
    'movies': 'Movie',
    'movie': 'Movie',
    'kids': 'Children',
    'children': 'Children',
    'kids & family': 'Children',
    'family': 'Children',
    'news': 'News',
    'news & politics': 'News',
    'sports': 'Sports',
    'sport': 'Sports',
    'drama': 'Drama',
}


def generate_xmltv(filters: dict = None, base_url: str = None, feed_name: str = None) -> str:
    """Compatibility wrapper — full XML as a string. Use streaming for HTTP."""
    return ''.join(generate_xmltv_stream(filters, base_url, feed_name=feed_name))


def write_xmltv(fp, filters: dict = None, base_url: str = None, feed_name: str = None) -> None:
    """Write XMLTV directly to a text file-like object."""
    for chunk in generate_xmltv_stream(filters, base_url, feed_name=feed_name):
        fp.write(chunk)


def generate_xmltv_gz(filters: dict = None, base_url: str = None, feed_name: str = None) -> bytes:
    """Return the full XML gzip-compressed as bytes."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=6) as gz:
        for chunk in generate_xmltv_stream(filters, base_url, feed_name=feed_name):
            gz.write(chunk.encode('utf-8'))
    return buf.getvalue()


def generate_xmltv_stream(filters: dict = None, base_url: str = None, feed_name: str = None):
    """
    Generator — yields UTF-8 text chunks of the XMLTV document.

    Key changes vs previous version:
      - Programs fetched via IN(non-gracenote channel ids), matching tvg_map exactly.
      - Keyset pagination: WHERE program.id > last_seen_id ORDER BY id.
        Avoids the O(n²) OFFSET scan that made page 700 read 140k rows.
    """
    filters  = filters or {}
    base_url = (base_url or 'http://localhost:5523').rstrip('/')

    # Standard XMLTV must expose the same channel identity set as the standard
    # XMLTV-backed M3U; otherwise clients that join on tvg-id see orphaned or
    # shifted channels. Gracenote-backed channels are intentionally excluded.
    channels = _selected_channels(filters, gracenote=False)
    multi_country_map = _source_multi_country_map(channels)

    tvg_map      = {ch.id: _tvg_id(ch) for ch in channels}
    # Channel category map — used as fallback when prog.category is None
    ch_cat_map   = {ch.id: ch.category for ch in channels if ch.category}
    # Source display name map — added as a final <category> tag on every programme
    ch_src_map      = {ch.id: ch.source.display_name for ch in channels}
    # Source internal name map — used to decide poster proxy policy per source
    ch_src_name_map = {ch.id: ch.source.name for ch in channels}
    channel_ids = set(tvg_map.keys())
    # Pre-sorted list for use in SQL IN() — matches exactly the non-gracenote
    # channel set so program fetching is bounded to XMLTV-visible channels only.
    channel_id_list = sorted(channel_ids)

    # Rolling 5-day window: include currently-airing programs (up to 2h ago)
    # through 5 days from now. Naive UTC matches how SQLite stores the values.
    epg_start = datetime.utcnow() - timedelta(hours=2)
    epg_end   = datetime.utcnow() + timedelta(days=5)

    # ── Header ────────────────────────────────────────────────────────────
    now_utc = datetime.now(tz=timezone.utc)
    yield '<?xml version="1.0" encoding="UTF-8"?>\n'
    yield (
        f'<tv generator-info-name="FastChannels"'
        f' generator-info-url="{_esc_attr(base_url)}"'
        f' date="{now_utc.strftime("%Y%m%d%H%M%S %z")}">\n'
    )

    # ── Channel elements ──────────────────────────────────────────────────
    for ch in channels:
        el = Element('channel', id=tvg_map[ch.id])
        display_name = _channel_display_name(ch, multi_country_map)
        SubElement(el, 'display-name').text = display_name
        if display_name != (ch.name or ''):
            SubElement(el, 'display-name').text = ch.name
        if ch.logo_url:
            SubElement(el, 'icon', src=proxy_logo_url(ch.logo_url, base_url) or ch.logo_url)
        yield tostring(el, encoding='unicode') + '\n'

    # ── Programme elements — keyset pagination ────────────────────────────
    # Keyset: track last Program.id seen, next page = WHERE id > last_id.
    # This is O(1) per page regardless of offset depth.
    # channel_id_list is already bounded to XMLTV-visible (non-gracenote) channels.
    BATCH   = 500
    last_id = 0

    while True:
        if not channel_id_list:
            break
        programs = (
            db.session.query(Program)
            .filter(
                Program.channel_id.in_(channel_id_list),
                Program.id > last_id,
                Program.end_time   > epg_start,
                Program.start_time < epg_end,
            )
            .order_by(Program.id.asc())
            .limit(BATCH)
            .all()
        )
        if not programs:
            break

        for prog in programs:
            tvg_id = tvg_map.get(prog.channel_id)
            if not tvg_id:
                continue

            el = Element('programme', attrib={
                'start':   _dt(prog.start_time),
                'stop':    _dt(prog.end_time),
                'channel': tvg_id,
            })
            SubElement(el, 'title', lang='en').text = prog.title or ''
            if prog.description:
                SubElement(el, 'desc', lang='en').text = _sanitize(prog.description)
            channel_cat = ch_cat_map.get(prog.channel_id) or ''
            program_cat = prog.category or ''
            combined_cats = [c.strip() for c in f'{program_cat};{channel_cat}'.split(';') if c.strip()]
            if combined_cats:
                # Use prog.category if set, fall back to channel category.
                # Split semicolon-joined strings into multiple <category> tags —
                # XMLTV allows multiple per programme and clients filter by them.
                seen_categories: set[str] = set()
                for cat in combined_cats:
                    key = cat.casefold()
                    if key in seen_categories:
                        continue
                    seen_categories.add(key)
                    SubElement(el, 'category', lang='en').text = _XMLTV_CAT_NORM.get(key, cat)
            # Always add source name as a category so clients can filter by provider
            src_name = ch_src_map.get(prog.channel_id)
            if src_name:
                SubElement(el, 'category', lang='en').text = src_name
            # Add feed name as a category when generating a feed-specific EPG
            if feed_name:
                SubElement(el, 'category', lang='en').text = feed_name
            if prog.poster_url:
                # Only proxy/cache Roku posters (CDN returns 403 to clients).
                # All other sources serve artwork directly — no caching overhead.
                if ch_src_name_map.get(prog.channel_id) == 'roku':
                    poster_src = proxy_logo_url(prog.poster_url, base_url, 'poster') or prog.poster_url
                else:
                    poster_src = prog.poster_url
                SubElement(el, 'icon', src=poster_src)
            if prog.original_air_date:
                SubElement(el, 'date').text = prog.original_air_date.strftime('%Y%m%d')
            if prog.rating:
                r = SubElement(el, 'rating', system='MPAA')
                SubElement(r, 'value').text = prog.rating
            cats = [c.casefold() for c in combined_cats]
            is_movie = 'movie' in cats or 'movies' in cats
            if prog.episode_title and not is_movie:
                SubElement(el, 'sub-title', lang='en').text = prog.episode_title
            if prog.season and prog.episode and not is_movie:
                SubElement(el, 'episode-num', system='xmltv_ns').text = \
                    f'{prog.season - 1}.{prog.episode - 1}.'
                if prog.season >= 1 and prog.episode >= 1:
                    SubElement(el, 'episode-num', system='onscreen').text = \
                        f'S{prog.season:02d}E{prog.episode:02d}'
            yield tostring(el, encoding='unicode') + '\n'

        last_id = programs[-1].id

    yield '</tv>\n'


# ── Helpers ───────────────────────────────────────────────────────────────

def _dt(dt) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime('%Y%m%d%H%M%S %z')


def _esc_attr(s: str) -> str:
    return (s or '').replace('&', '&amp;').replace('"', '&quot;')

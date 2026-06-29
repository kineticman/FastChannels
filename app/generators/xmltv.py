"""
XMLTV / EPG generator.

Performance design:
  - One query per channel (channel_id equality) so SQLite drives
    idx_programs_channel_end_start — each artifact's cost scales with its
    own channels, not the whole programs table.
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
from ..models import Program, AppSettings
from ..url import proxy_logo_url
from .m3u import (_selected_channels, _tvg_id, _channel_display_name, _source_multi_country_map,
                  _sanitize, _build_channel_query, _prismcast_capturable)

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


def _append_category(el, value: str, seen: set[str]) -> None:
    """Emit a <category> child, normalized and de-duplicated.

    `seen` holds the casefolded normalized labels already emitted on this
    programme so the same category isn't repeated (e.g. a "Plex" source name
    alongside a feed also named "Plex").
    """
    normalized = _XMLTV_CAT_NORM.get(value.casefold(), value)
    norm_key = normalized.casefold()
    if norm_key in seen:
        return
    seen.add(norm_key)
    SubElement(el, 'category', lang='en').text = normalized


def generate_xmltv(filters: dict = None, base_url: str = None, feed_name: str = None) -> str:
    """Compatibility wrapper — full XML as a string. Use streaming for HTTP."""
    return ''.join(generate_xmltv_stream(filters, base_url, feed_name=feed_name))


def write_xmltv(fp, filters: dict = None, base_url: str = None, feed_name: str = None,
                native: bool = False) -> None:
    """Write XMLTV directly to a text file-like object."""
    for chunk in generate_xmltv_stream(filters, base_url, feed_name=feed_name, native=native):
        fp.write(chunk)


def generate_xmltv_gz(filters: dict = None, base_url: str = None, feed_name: str = None) -> bytes:
    """Return the full XML gzip-compressed as bytes."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=6) as gz:
        for chunk in generate_xmltv_stream(filters, base_url, feed_name=feed_name):
            gz.write(chunk.encode('utf-8'))
    return buf.getvalue()


def generate_xmltv_stream(filters: dict = None, base_url: str = None, feed_name: str = None,
                          native: bool = False):
    """
    Generator — yields UTF-8 text chunks of the XMLTV document.

    Programs are fetched one channel at a time (channel_id equality uses the
    composite (channel_id, end_time, start_time) index), bounded to the same
    non-gracenote channel set as tvg_map.  Programmes are therefore emitted
    grouped by channel — XMLTV does not require any particular order.
    """
    filters  = filters or {}
    base_url = (base_url or 'http://localhost:5523').rstrip('/')

    _s = AppSettings.get()
    _image_proxy = _s.image_proxy_enabled if _s.image_proxy_enabled is not None else True

    # Standard XMLTV excludes Gracenote-backed channels (those belong to the
    # Gracenote M3U source and must not be mixed with XMLTV-backed channels).
    # Native mode includes all channels and uses scraped EPG for everything.
    gracenote_filter = None if native else False
    channels = _selected_channels(filters, gracenote=gracenote_filter)

    # Include DRM channels the audit disabled but which the PrismCast feed bridges —
    # they share this XMLTV, so emit their guide too (a superset is harmless for the
    # standard feed, which simply doesn't reference them).
    _seen = {ch.id for ch in channels}
    for ch in _build_channel_query(filters, activity='drm_bridge').all():
        if ch.id not in _seen and _prismcast_capturable(ch):
            channels.append(ch)
            _seen.add(ch.id)

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
            SubElement(el, 'icon', src=proxy_logo_url(ch.logo_url, base_url, image_proxy_enabled=_image_proxy) or ch.logo_url)
        yield tostring(el, encoding='unicode') + '\n'

    # ── Programme elements — one indexed query per channel ───────────────
    # channel_id equality lets SQLite drive idx_programs_channel_end_start
    # (channel_id=?, end_time>?), so each artifact's cost is proportional to
    # its own channels.  The previous global id-keyset walked the entire
    # programs id range for every artifact regardless of feed size, re-binding
    # the full channel-id IN list on every page.  A channel's 5-day window is
    # a few hundred rows, so memory stays bounded per query.
    for ch_id in channel_id_list:
        programs = (
            db.session.query(Program)
            .filter(
                Program.channel_id == ch_id,
                Program.end_time   > epg_start,
                Program.start_time < epg_end,
            )
            .order_by(Program.id.asc())
            .all()
        )

        for prog in programs:
            tvg_id = tvg_map.get(prog.channel_id)
            if not tvg_id:
                continue

            el = Element('programme', attrib={
                'start':   _dt(prog.start_time),
                'stop':    _dt(prog.end_time),
                'channel': tvg_id,
            })
            SubElement(el, 'title', lang='en').text = _sanitize(prog.title)
            if prog.description:
                SubElement(el, 'desc', lang='en').text = _sanitize(prog.description)
            channel_cat = ch_cat_map.get(prog.channel_id) or ''
            program_cat = prog.category or ''
            combined_cats = [c.strip() for c in f'{program_cat};{channel_cat}'.split(';') if c.strip()]
            # Track every category emitted (normalized, casefolded) so the source
            # name, feed name and synthetic Movie tag below don't duplicate one
            # already present — e.g. a feed named "Plex" on the Plex source.
            seen_categories: set[str] = set()
            # Use prog.category if set, fall back to channel category.
            # Split semicolon-joined strings into multiple <category> tags —
            # XMLTV allows multiple per programme and clients filter by them.
            for cat in combined_cats:
                _append_category(el, cat, seen_categories)
            # Always add source name as a category so clients can filter by provider
            src_name = ch_src_map.get(prog.channel_id)
            if src_name:
                _append_category(el, src_name, seen_categories)
            # Add feed name as a category when generating a feed-specific EPG
            if feed_name:
                _append_category(el, feed_name, seen_categories)
            if prog.poster_url:
                # Only proxy/cache Roku posters (CDN returns 403 to clients).
                # All other sources serve artwork directly — no caching overhead.
                if ch_src_name_map.get(prog.channel_id) == 'roku':
                    poster_src = proxy_logo_url(prog.poster_url, base_url, 'poster', image_proxy_enabled=_image_proxy) or prog.poster_url
                else:
                    poster_src = prog.poster_url
                SubElement(el, 'icon', src=poster_src)
            if prog.original_air_date:
                SubElement(el, 'date').text = prog.original_air_date.strftime('%Y%m%d')
            if prog.rating:
                r = SubElement(el, 'rating', system='MPAA')
                SubElement(r, 'value').text = prog.rating
            if prog.is_live:
                SubElement(el, 'live')
            cats = [c.casefold() for c in combined_cats]
            prog_type = getattr(prog, 'program_type', None)
            is_movie = prog_type == 'movie' or 'movie' in cats or 'movies' in cats
            # Ensure <category>Movie</category> is emitted when program_type
            # signals a movie but the scraped category doesn't already say so.
            if prog_type == 'movie' and 'movie' not in cats and 'movies' not in cats:
                _append_category(el, 'Movie', seen_categories)
            if prog.episode_title and not is_movie:
                SubElement(el, 'sub-title', lang='en').text = _sanitize(prog.episode_title)
            if prog.season and prog.episode and not is_movie:
                SubElement(el, 'episode-num', system='xmltv_ns').text = \
                    f'{prog.season - 1}.{prog.episode - 1}.'
                if prog.season >= 1 and prog.episode >= 1:
                    SubElement(el, 'episode-num', system='onscreen').text = \
                        f'S{prog.season:02d}E{prog.episode:02d}'
            series_id  = getattr(prog, 'series_id',  None)
            episode_id = getattr(prog, 'episode_id', None)
            if series_id:
                SubElement(el, 'series-id', system='fastchannels').text = series_id
            if episode_id:
                system = 'dd_progid' if _is_tms_id(episode_id) else 'fastchannels'
                SubElement(el, 'episode-num', system=system).text = episode_id
            yield tostring(el, encoding='unicode') + '\n'

    # ── Synthetic hourly blocks for custom channels ───────────────────────
    # Custom channels have no scraped Program rows.  Emit repeating 1-hour
    # slots so EPG clients show the channel name instead of a blank grid.
    custom_channels = [ch for ch in channels if ch.source.name == 'custom']
    if custom_channels:
        block_start = epg_start.replace(minute=0, second=0, microsecond=0)
        for ch in custom_channels:
            tvg_id = tvg_map.get(ch.id)
            if not tvg_id:
                continue
            block_delta = timedelta(minutes=ch.guide_block_minutes or 60)
            t = block_start
            while t < epg_end:
                slot_end = t + block_delta
                el = Element('programme', attrib={
                    'start':   _dt(t),
                    'stop':    _dt(slot_end),
                    'channel': tvg_id,
                })
                SubElement(el, 'title', lang='en').text = _sanitize(ch.name)
                if ch.description:
                    SubElement(el, 'desc', lang='en').text = _sanitize(ch.description)
                seen_categories = set()
                channel_cat = ch_cat_map.get(ch.id) or ''
                if channel_cat:
                    _append_category(el, channel_cat, seen_categories)
                src_name = ch_src_map.get(ch.id)
                if src_name:
                    _append_category(el, src_name, seen_categories)
                if feed_name:
                    _append_category(el, feed_name, seen_categories)
                if ch.logo_url:
                    SubElement(el, 'icon', src=proxy_logo_url(ch.logo_url, base_url, image_proxy_enabled=_image_proxy) or ch.logo_url)
                yield tostring(el, encoding='unicode') + '\n'
                t = slot_end

    yield '</tv>\n'


import re as _re
_TMS_ID_RE = _re.compile(r'^(?:SH|EP|MV|SP|NO)\d{10,12}$')


# ── Helpers ───────────────────────────────────────────────────────────────

def _is_tms_id(value: str) -> bool:
    return bool(_TMS_ID_RE.match(value or ''))


def _dt(dt) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime('%Y%m%d%H%M%S %z')


def _esc_attr(s: str) -> str:
    return (s or '').replace('&', '&amp;').replace('"', '&quot;')

import logging
import re
from ..models import Channel, Source

log = logging.getLogger(__name__)

# Gracenote ID prefixes recognised by Channels DVR
_GRACENOTE_PREFIX_RE = re.compile(r'^(\d+|(EP|SH|MV|SP|TR)\d+)$')


def _parse_gracenote_id(ch) -> str | None:
    """
    Returns the Gracenote station ID for a channel, or None.

    Resolution order:
      1. channel.gracenote_id  — explicitly stored (set by scraper or user via admin UI)
      2. slug fallback          — Roku scrapers encode "{play_id}|{gracenote_id}" in the
                                  slug before the dedicated column existed; still honoured
                                  so existing data keeps working without a re-scrape.
    """
    # 1. Dedicated column (preferred)
    gid = (ch.gracenote_id or '').strip()
    if gid and _GRACENOTE_PREFIX_RE.match(gid):
        return gid

    # 2. Slug fallback for Roku-style "{play_id}|{gracenote_id}"
    slug = ch.slug or ''
    if '|' in slug:
        candidate = slug.split('|', 1)[1].strip()
        if candidate and _GRACENOTE_PREFIX_RE.match(candidate):
            return candidate

    return None


def _build_channel_query(filters: dict):
    """Shared filtered query for active, enabled channels."""
    query = Channel.query.join(Source).filter(
        Channel.is_active  == True,
        Channel.is_enabled == True,
        Source.is_enabled  == True,
        Source.epg_only    == False,
        Channel.stream_url != None,
    )
    if channel_ids := filters.get('channel_ids'):
        query = query.filter(Channel.id.in_(channel_ids))
    else:
        if sources := filters.get('source'):
            query = query.filter(Source.name.in_(sources))
        if categories := filters.get('category'):
            query = query.filter(Channel.category.in_(categories))
        if languages := filters.get('languages'):
            query = query.filter(Channel.language.in_(languages))
        elif language := filters.get('language'):
            query = query.filter(Channel.language == language)
        if search := filters.get('search'):
            query = query.filter(Channel.name.ilike(f'%{search}%'))
    return query.order_by(Channel.number.asc().nullslast(), Channel.name.asc())


def feed_to_query_filters(feed_filters: dict) -> dict:
    """Translate Feed.filters (plural keys) to _build_channel_query format."""
    f = {}
    if channel_ids := feed_filters.get('channel_ids'):
        # Explicit channel list overrides source/category/language filters.
        f['channel_ids'] = channel_ids
        if max_ch := feed_filters.get('max_channels'):
            f['max_channels'] = max_ch
        return f
    if sources := feed_filters.get('sources'):
        f['source'] = sources
    if categories := feed_filters.get('categories'):
        f['category'] = categories
    if languages := feed_filters.get('languages'):
        f['languages'] = languages
    if max_ch := feed_filters.get('max_channels'):
        f['max_channels'] = max_ch
    return f


def _build_source_chnum_map(channels):
    """
    Build a channel-number assignment map using Source.chnum_start values.

    Channels from sources with chnum_start configured are renumbered sequentially
    starting from that value.  Channels from sources without chnum_start fall back
    to their existing Channel.number (unchanged from scraper output).

    Returns:
        chnum_map  – dict[channel_id -> int]
        warnings   – list of human-readable overlap warning strings
    """
    # Group channels by source, preserving their sorted order
    by_source: dict[str, list] = {}
    source_starts: dict[str, int] = {}
    for ch in channels:
        src = ch.source.name
        if src not in by_source:
            by_source[src] = []
            if ch.source.chnum_start:
                source_starts[src] = ch.source.chnum_start
        by_source[src].append(ch)

    # Detect overlaps between configured sources
    warnings: list[str] = []
    configured = [
        (src, source_starts[src], len(by_source[src]))
        for src in source_starts
    ]
    for i in range(len(configured)):
        for j in range(i + 1, len(configured)):
            a_name, a_start, a_count = configured[i]
            b_name, b_start, b_count = configured[j]
            a_end = a_start + a_count
            b_end = b_start + b_count
            if a_start < b_end and b_start < a_end:
                overlap_lo = max(a_start, b_start)
                overlap_hi = min(a_end, b_end) - 1
                warnings.append(
                    f"'{a_name}' (ch {a_start}–{a_end - 1}, {a_count} channels) overlaps "
                    f"'{b_name}' (ch {b_start}–{b_end - 1}, {b_count} channels) "
                    f"at ch {overlap_lo}–{overlap_hi}"
                )

    # Assign numbers
    chnum_map: dict[int, int] = {}
    for src, chs in by_source.items():
        if src in source_starts:
            start = source_starts[src]
            for idx, ch in enumerate(chs):
                chnum_map[ch.id] = start + idx
        else:
            for ch in chs:
                if ch.number:
                    chnum_map[ch.id] = ch.number

    return chnum_map, warnings


def _build_feed_chnum_map(channels, feed_chnum_start: int):
    """
    Simple sequential numbering for a feed-level chnum_start.
    All channels in the feed are numbered start, start+1, start+2, …
    """
    return {ch.id: feed_chnum_start + idx for idx, ch in enumerate(channels)}


def get_chnum_overlaps() -> list[str]:
    """
    Return a list of overlap warning strings for the current source configuration.
    Used by the admin UI to surface misconfiguration.
    """
    channels = _build_channel_query({}).all()
    _, warnings = _build_source_chnum_map(channels)
    return warnings


def generate_m3u(filters: dict = None, base_url: str = None,
                 feed_chnum_start: int = None) -> str:
    """
    Standard XMLTV-backed playlist.
    Excludes channels with a valid Gracenote ID — those belong in /m3u/gracenote
    so Channels DVR doesn't mix EPG sources within a single M3U source.

    Channel numbering (tvg-chno):
      - feed_chnum_start set  → sequential from that number for all channels in this feed
      - feed_chnum_start None → per-source Source.chnum_start values (source-level config)
      - source without chnum_start → existing Channel.number (or omitted if null)
    """
    filters  = filters or {}
    base_url = (base_url or '').rstrip('/')

    all_channels = _build_channel_query(filters).all()
    channels = [ch for ch in all_channels if not _parse_gracenote_id(ch)]

    if feed_chnum_start is not None:
        chnum_map = _build_feed_chnum_map(channels, feed_chnum_start)
    else:
        chnum_map, warnings = _build_source_chnum_map(channels)
        for w in warnings:
            log.warning('chnum overlap: %s', w)

    lines = ['#EXTM3U']
    for ch in channels:
        tvg_id = _tvg_id(ch)
        attrs = [
            f'tvg-id="{tvg_id}"',
            f'tvg-name="{_esc(ch.name)}"',
            f'group-title="{_esc(ch.category or ch.source.display_name)}"',
        ]
        if ch.logo_url:
            attrs.append(f'tvg-logo="{ch.logo_url}"')
        chnum = chnum_map.get(ch.id)
        if chnum:
            attrs.append(f'tvg-chno="{chnum}"')
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{ch.name}')
        lines.append(f'{base_url}/play/{ch.source.name}/{ch.source_channel_id}.m3u8')

    return '\n'.join(lines)


def generate_gracenote_m3u(filters: dict = None, base_url: str = None,
                            feed_chnum_start: int = None) -> str:
    """
    Gracenote-backed playlist for Channels DVR.

    Only includes channels with a valid Gracenote ID (from channel.gracenote_id
    or the legacy "{play_id}|{gracenote_id}" slug encoding).
    Uses tvc-guide-stationid so Channels DVR routes guide data through Gracenote
    rather than our XMLTV — the two cannot be mixed per source.

    Channel numbering follows the same rules as generate_m3u.
    """
    filters  = filters or {}
    base_url = (base_url or '').rstrip('/')

    all_channels = _build_channel_query(filters).all()
    channels = [ch for ch in all_channels if _parse_gracenote_id(ch)]

    if feed_chnum_start is not None:
        chnum_map = _build_feed_chnum_map(channels, feed_chnum_start)
    else:
        chnum_map, warnings = _build_source_chnum_map(channels)
        for w in warnings:
            log.warning('chnum overlap (gracenote): %s', w)

    lines = ['#EXTM3U']
    for ch in channels:
        gracenote_id = _parse_gracenote_id(ch)
        attrs = [
            f'tvc-guide-stationid="{gracenote_id}"',
            f'tvg-name="{_esc(ch.name)}"',
            f'group-title="{_esc(ch.category or ch.source.display_name)}"',
        ]
        if ch.logo_url:
            attrs.append(f'tvg-logo="{ch.logo_url}"')
        chnum = chnum_map.get(ch.id)
        if chnum:
            attrs.append(f'tvg-chno="{chnum}"')
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{ch.name}')
        lines.append(f'{base_url}/play/{ch.source.name}/{ch.source_channel_id}.m3u8')

    return '\n'.join(lines)


def _tvg_id(ch) -> str:
    return f'{ch.source.name}.{ch.source_channel_id}'


def _esc(s):
    return (s or '').replace('"', "'")

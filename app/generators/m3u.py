import logging
import re
from sqlalchemy.orm import contains_eager
from ..models import Channel, Source, Feed, AppSettings
from ..url import proxy_logo_url

log = logging.getLogger(__name__)

_CHNUM_NAMESPACE_BLOCK = 100000
_MASTER_GRACENOTE_START = 100000
_FEED_NAMESPACE_BASE = 200000

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
    query = Channel.query.join(Source).options(contains_eager(Channel.source)).filter(
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
        if gracenote := filters.get('gracenote'):
            if gracenote == 'has':
                query = query.filter(Channel.gracenote_id != None, Channel.gracenote_id != '')
            elif gracenote == 'missing':
                query = query.filter((Channel.gracenote_id == None) | (Channel.gracenote_id == ''))
        if search := filters.get('search'):
            query = query.filter(Channel.name.ilike(f'%{search}%'))
        if excluded_ids := filters.get('excluded_channel_ids'):
            query = query.filter(Channel.id.notin_(excluded_ids))
    return query.order_by(Channel.number.asc().nullslast(), Channel.name.asc())


def _selected_channels(filters: dict | None = None, *, gracenote: bool | None = False):
    """
    Return the concrete channel list for playlist/XMLTV generation.

    gracenote=False  -> channels for the standard XMLTV-backed M3U
    gracenote=True   -> channels for the Gracenote-backed M3U
    gracenote=None   -> all filtered channels without Gracenote partitioning
    """
    filters = filters or {}
    channels = _build_channel_query(filters).all()

    if gracenote is True:
        channels = [ch for ch in channels if _parse_gracenote_id(ch)]
    elif gracenote is False:
        channels = [ch for ch in channels if not _parse_gracenote_id(ch)]

    max_ch = filters.get('max_channels')
    if max_ch:
        channels = channels[:int(max_ch)]

    return channels


def feed_namespace_start(feed: Feed, *, gracenote: bool) -> int:
    idx = (
        Feed.query
        .filter(Feed.is_enabled == True, Feed.slug < feed.slug)
        .count()
    )
    base = _FEED_NAMESPACE_BASE + (idx * _CHNUM_NAMESPACE_BLOCK * 2)
    return base + (_CHNUM_NAMESPACE_BLOCK if gracenote else 0)


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
    if gracenote := feed_filters.get('gracenote'):
        f['gracenote'] = gracenote
    if excluded_ids := feed_filters.get('excluded_channel_ids'):
        f['excluded_channel_ids'] = excluded_ids
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
    def _channel_sort_key(ch):
        return (
            ch.number is None,
            ch.number if ch.number is not None else 0,
            (ch.name or '').lower(),
            ch.source_channel_id or '',
        )

    # Group channels by source, then sort each source's channels independently.
    # This keeps the global tvg-chno blocks stable even when the mixed query
    # order shifts as scrapers add/remove channels in other sources.
    by_source: dict[str, list] = {}
    source_starts: dict[str, int] = {}
    source_labels: dict[str, str] = {}
    for ch in channels:
        src = ch.source.name
        if src not in by_source:
            by_source[src] = []
            source_labels[src] = ch.source.display_name or ch.source.name or src
            if ch.source.chnum_start:
                source_starts[src] = ch.source.chnum_start
        by_source[src].append(ch)

    for src in by_source:
        by_source[src].sort(key=_channel_sort_key)

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

    # Read global fallback start (sources without their own chnum_start)
    global_start = None
    try:
        settings = AppSettings.get()
        global_start = settings.effective_global_chnum_start()
    except Exception:
        pass

    # Assign numbers
    chnum_map: dict[int, int] = {}
    global_cursor = global_start  # tracks next number for ungrouped sources
    ordered_sources = sorted(
        by_source,
        key=lambda src: (
            src not in source_starts,
            source_starts.get(src, 0),
            source_labels.get(src, src).lower(),
            src,
        ),
    )

    for src in ordered_sources:
        chs = by_source[src]
        if src in source_starts:
            start = source_starts[src]
            for idx, ch in enumerate(chs):
                chnum_map[ch.id] = start + idx
        elif global_cursor is not None:
            for idx, ch in enumerate(chs):
                chnum_map[ch.id] = global_cursor + idx
            global_cursor += len(chs)

    return chnum_map, warnings


def _build_feed_chnum_map(channels, feed_chnum_start: int):
    """
    Simple sequential numbering for a feed-level chnum_start.
    All channels in the feed are numbered start, start+1, start+2, …
    """
    return {ch.id: feed_chnum_start + idx for idx, ch in enumerate(channels)}


def _resolve_chnum_map(channels, *, feed_chnum_start: int = None, namespace_start: int = None):
    if namespace_start is not None:
        return _build_feed_chnum_map(channels, namespace_start), []
    if feed_chnum_start is not None:
        return _build_feed_chnum_map(channels, feed_chnum_start), []
    return _build_source_chnum_map(channels)


def get_chnum_overlaps() -> list[str]:
    """
    Return a list of overlap warning strings for the current source configuration.
    Used by the admin UI to surface misconfiguration.
    """
    channels = _build_channel_query({}).all()
    _, warnings = _build_source_chnum_map(channels)
    return warnings


def get_global_chnum_overlaps() -> list[str]:
    """
    Return warnings for duplicate tvg-chno values across every generated M3U:
    master standard, master gracenote, and all enabled feed outputs.
    """
    outputs: list[tuple[str, list, dict[int, int]]] = []

    master_standard = _selected_channels({}, gracenote=False)
    master_standard_map, _ = _resolve_chnum_map(master_standard)
    outputs.append(('master /m3u', master_standard, master_standard_map))

    master_gracenote = _selected_channels({}, gracenote=True)
    master_gracenote_map, _ = _resolve_chnum_map(
        master_gracenote,
        namespace_start=_MASTER_GRACENOTE_START,
    )
    outputs.append(('master /m3u/gracenote', master_gracenote, master_gracenote_map))

    feeds = Feed.query.filter_by(is_enabled=True).order_by(Feed.slug).all()
    for feed in feeds:
        filters = feed_to_query_filters(feed.filters or {})

        std_channels = _selected_channels(filters, gracenote=False)
        std_ns = None if feed.chnum_start is not None else feed_namespace_start(feed, gracenote=False)
        std_map, _ = _resolve_chnum_map(
            std_channels,
            feed_chnum_start=feed.chnum_start,
            namespace_start=std_ns,
        )
        outputs.append((f'feed {feed.slug} /m3u', std_channels, std_map))

        gn_channels = _selected_channels(filters, gracenote=True)
        gn_ns = None if feed.chnum_start is not None else feed_namespace_start(feed, gracenote=True)
        gn_map, _ = _resolve_chnum_map(
            gn_channels,
            feed_chnum_start=feed.chnum_start,
            namespace_start=gn_ns,
        )
        outputs.append((f'feed {feed.slug} /m3u/gracenote', gn_channels, gn_map))

    seen: dict[int, tuple[str, str]] = {}
    warnings: list[str] = []
    for output_name, channels, chnum_map in outputs:
        for ch in channels:
            chnum = chnum_map.get(ch.id)
            if not chnum:
                continue
            current = (output_name, ch.name)
            previous = seen.get(chnum)
            if previous and previous != current:
                warnings.append(
                    f"ch {chnum} is duplicated: {previous[1]} in {previous[0]} and "
                    f"{ch.name} in {output_name}"
                )
            else:
                seen[chnum] = current
    return warnings


def generate_m3u(filters: dict = None, base_url: str = None,
                 feed_chnum_start: int = None, namespace_start: int = None) -> str:
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

    channels = _selected_channels(filters, gracenote=False)

    chnum_map, warnings = _resolve_chnum_map(
        channels,
        feed_chnum_start=feed_chnum_start,
        namespace_start=namespace_start,
    )
    if feed_chnum_start is None and namespace_start is None:
        for w in warnings:
            log.warning('chnum overlap: %s', w)

    lines = ['#EXTM3U']
    for ch in channels:
        tvg_id = _tvg_id(ch)
        attrs = [
            f'channel-id="{tvg_id}"',
            f'tvg-id="{tvg_id}"',
            f'tvg-name="{_esc(ch.name)}"',
            f'group-title="{_esc(ch.category or ch.source.display_name)}"',
        ]
        if ch.logo_url:
            attrs.append(f'tvg-logo="{proxy_logo_url(ch.logo_url, base_url) or ch.logo_url}"')
        chnum = chnum_map.get(ch.id)
        if chnum:
            attrs.append(f'tvg-chno="{chnum}"')
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{ch.name}')
        lines.append(f'{base_url}/play/{ch.source.name}/{ch.source_channel_id}.m3u8')

    return '\n'.join(lines)


def generate_gracenote_m3u(filters: dict = None, base_url: str = None,
                            feed_chnum_start: int = None, namespace_start: int = None) -> str:
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

    channels = _selected_channels(filters, gracenote=True)

    chnum_map, warnings = _resolve_chnum_map(
        channels,
        feed_chnum_start=feed_chnum_start,
        namespace_start=namespace_start,
    )
    if feed_chnum_start is None and namespace_start is None:
        for w in warnings:
            log.warning('chnum overlap (gracenote): %s', w)

    lines = ['#EXTM3U']
    for ch in channels:
        gracenote_id = _parse_gracenote_id(ch)
        attrs = [
            f'channel-id="{gracenote_id}"',
            f'tvc-guide-stationid="{gracenote_id}"',
            f'tvg-name="{_esc(ch.name)}"',
            f'group-title="{_esc(ch.category or ch.source.display_name)}"',
        ]
        if ch.logo_url:
            attrs.append(f'tvg-logo="{proxy_logo_url(ch.logo_url, base_url) or ch.logo_url}"')
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

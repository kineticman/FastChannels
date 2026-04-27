import logging
import re
from dataclasses import dataclass
from urllib.parse import quote as _url_quote
from sqlalchemy.orm import contains_eager
from ..extensions import db
from ..models import Channel, Source, Feed, AppSettings
from ..url import proxy_logo_url

log = logging.getLogger(__name__)

# Windows-1252 codepoints that arrived as Unicode scalars (sources that decoded
# bytes as Latin-1 instead of UTF-8).  Map them to the proper Unicode characters
# they were always meant to be; undefined C1 slots are stripped (None).
_WIN1252_REMAP = str.maketrans({
    0x80: '€',  # €
    0x81: None,      # undefined
    0x82: '‚',  # ‚
    0x83: 'ƒ',  # ƒ
    0x84: '„',  # „
    0x85: '…',  # …
    0x86: '†',  # †
    0x87: '‡',  # ‡
    0x88: 'ˆ',  # ˆ
    0x89: '‰',  # ‰
    0x8A: 'Š',  # Š
    0x8B: '‹',  # ‹
    0x8C: 'Œ',  # Œ
    0x8D: None,      # undefined
    0x8E: 'Ž',  # Ž
    0x8F: None,      # undefined
    0x90: None,      # undefined
    0x91: '‘',  # '
    0x92: '’',  # '
    0x93: '“',  # "
    0x94: '”',  # "
    0x95: '•',  # •
    0x96: '–',  # –
    0x97: '—',  # —
    0x98: '˜',  # ˜
    0x99: '™',  # ™
    0x9A: 'š',  # š
    0x9B: '›',  # ›
    0x9C: 'œ',  # œ
    0x9D: None,      # undefined
    0x9E: 'ž',  # ž
    0x9F: 'Ÿ',  # Ÿ
    0x00A0: ' ',     # NO-BREAK SPACE → regular space
    0x200B: None,    # ZERO WIDTH SPACE
    0xFFFD: None,    # REPLACEMENT CHARACTER
})

_CHNUM_NAMESPACE_BLOCK = 100000
_MASTER_GRACENOTE_START = 100000
_FEED_NAMESPACE_BASE = 200000
_REGION_LABEL_SOURCES = {"pluto", "samsung"}

# Gracenote ID prefixes recognised by Channels DVR
_GRACENOTE_PREFIX_RE = re.compile(r'^(\d+|(EP|SH|MV|SP|TR)\d+)$')


@dataclass(slots=True)
class _MiniSource:
    name: str
    display_name: str | None
    chnum_start: int | None


@dataclass(slots=True)
class _MiniChannel:
    id: int
    name: str | None
    number: int | None
    number_pinned: bool
    source_channel_id: str | None
    gracenote_id: str | None
    slug: str | None
    source: _MiniSource


def _parse_gracenote_id(ch) -> str | None:
    """
    Returns the Gracenote station ID for a channel, or None.

    Resolution order:
      1. channel.gracenote_id  — explicitly stored (set by scraper or user via admin UI)
      2. slug fallback          — Roku scrapers encode "{play_id}|{gracenote_id}" in the
                                  slug before the dedicated column existed; still honoured
                                  so existing data keeps working without a re-scrape.
    """
    # Respect gracenote_mode — 'off' means never route to Gracenote M3U
    if getattr(ch, 'gracenote_mode', None) == 'off':
        return None

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


def _format_region_label(country: str | None) -> str:
    raw = (country or '').strip()
    if not raw:
        return ''
    parts = [p for p in re.split(r'[-_\s]+', raw) if p]
    if not parts:
        return raw
    return ' '.join(p.upper() if len(p) <= 3 else p.capitalize() for p in parts)


def _source_multi_country_map(channels) -> dict[str, set[str]]:
    by_source: dict[str, set[str]] = {}
    for ch in channels:
        source_name = getattr(ch.source, 'name', None)
        country = (getattr(ch, 'country', None) or '').strip()
        if not source_name or source_name not in _REGION_LABEL_SOURCES or not country:
            continue
        by_source.setdefault(source_name, set()).add(country)
    return {
        source_name: countries
        for source_name, countries in by_source.items()
        if len(countries) > 1
    }


def _channel_display_name(ch, multi_country_map: dict[str, set[str]] | None = None) -> str:
    name = ch.name or ''
    multi_country_map = multi_country_map or {}
    source_name = getattr(ch.source, 'name', None)
    country = (getattr(ch, 'country', None) or '').strip()
    if source_name and country and source_name in multi_country_map:
        region = _format_region_label(country)
        if region:
            return f'{name} ({region})'
    return name


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
        if countries := filters.get('countries'):
            query = query.filter(Channel.country.in_(countries))
        if gracenote := filters.get('gracenote'):
            if gracenote == 'has':
                query = query.filter(Channel.gracenote_id != None, Channel.gracenote_id != '')
            elif gracenote == 'missing':
                query = query.filter(
                    (Channel.gracenote_id == None) | (Channel.gracenote_id == ''),
                    ~Channel.slug.like('%|%'),
                )
        if search := filters.get('search'):
            query = query.filter(Channel.name.ilike(f'%{search}%'))
        if excluded_ids := filters.get('excluded_channel_ids'):
            query = query.filter(Channel.id.notin_(excluded_ids))
    return query.order_by(Channel.number.asc().nullslast(), Channel.name.asc())


def _build_channel_stub_query(filters: dict):
    """Lightweight variant of _build_channel_query for validation-only paths."""
    query = db.session.query(
        Channel.id,
        Channel.name,
        Channel.number,
        Channel.number_pinned,
        Channel.source_channel_id,
        Channel.gracenote_id,
        Channel.slug,
        Source.name.label('source_name'),
        Source.display_name.label('source_display_name'),
        Source.chnum_start.label('source_chnum_start'),
    ).join(Source).filter(
        Channel.is_active == True,
        Channel.is_enabled == True,
        Source.is_enabled == True,
        Source.epg_only == False,
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
        if countries := filters.get('countries'):
            query = query.filter(Channel.country.in_(countries))
        if gracenote := filters.get('gracenote'):
            if gracenote == 'has':
                query = query.filter(Channel.gracenote_id != None, Channel.gracenote_id != '')
            elif gracenote == 'missing':
                query = query.filter(
                    (Channel.gracenote_id == None) | (Channel.gracenote_id == ''),
                    ~Channel.slug.like('%|%'),
                )
        if search := filters.get('search'):
            query = query.filter(Channel.name.ilike(f'%{search}%'))
        if excluded_ids := filters.get('excluded_channel_ids'):
            query = query.filter(Channel.id.notin_(excluded_ids))
    return query.order_by(Channel.number.asc().nullslast(), Channel.name.asc())


def _selected_channel_stubs(filters: dict | None = None, *, gracenote: bool | None = False):
    """Return lightweight channel rows for overlap validation paths."""
    filters = filters or {}
    rows = _build_channel_stub_query(filters).all()
    channels = [
        _MiniChannel(
            id=row.id,
            name=row.name,
            number=row.number,
            number_pinned=bool(row.number_pinned),
            source_channel_id=row.source_channel_id,
            gracenote_id=row.gracenote_id,
            slug=row.slug,
            source=_MiniSource(
                name=row.source_name,
                display_name=row.source_display_name,
                chnum_start=row.source_chnum_start,
            ),
        )
        for row in rows
    ]

    if gracenote is True:
        channels = [ch for ch in channels if _parse_gracenote_id(ch)]
    elif gracenote is False:
        channels = [ch for ch in channels if not _parse_gracenote_id(ch)]

    max_ch = filters.get('max_channels')
    if max_ch:
        channels = channels[:int(max_ch)]

    return channels


def _selected_channels(filters: dict | None = None, *, gracenote: bool | None = False):
    """
    Return the concrete channel list for playlist/XMLTV generation.

    gracenote=False  -> channels for the standard XMLTV-backed M3U
    gracenote=True   -> channels for the Gracenote-backed M3U
    gracenote=None   -> all filtered channels without Gracenote partitioning
    """
    filters = filters or {}
    channels = _build_channel_query(filters).all()

    pinned_ids = filters.get('pinned_channel_ids')
    if pinned_ids:
        existing_ids = {ch.id for ch in channels}
        extra_ids = [i for i in pinned_ids if i not in existing_ids]
        if extra_ids:
            channels = list(channels) + _build_channel_query({'channel_ids': extra_ids}).all()

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


def feed_gracenote_start(feed: Feed) -> int:
    """
    Starting channel number for a feed's gracenote M3U.

    Gracenote channels are placed immediately after the standard (non-gracenote)
    channels so the entire feed shares one contiguous numeric pool with no gaps
    or overlaps between the two M3U variants.

    For feeds with an explicit chnum_start this is a simple COUNT query.
    For the default feed (source-based numbering) we build the full chnum map
    to find the actual highest number in use.
    """
    filters = feed_to_query_filters(feed.filters or {})

    if feed.slug == 'default':
        std_channels = _selected_channels(filters, gracenote=False)
        if not std_channels:
            return AppSettings.get().effective_global_chnum_start() or 1
        chnum_map, _ = _build_source_chnum_map(std_channels)
        if not chnum_map:
            return AppSettings.get().effective_global_chnum_start() or 1
        return max(chnum_map.values()) + 1
    elif feed.chnum_start is not None:
        std_count = len(_selected_channel_stubs(filters, gracenote=False))
        return feed.chnum_start + std_count
    else:
        return feed_namespace_start(feed, gracenote=True)


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
    if countries := feed_filters.get('countries'):
        f['countries'] = countries
    if gracenote := feed_filters.get('gracenote'):
        f['gracenote'] = gracenote
    if excluded_ids := feed_filters.get('excluded_channel_ids'):
        f['excluded_channel_ids'] = excluded_ids
    if pinned_ids := feed_filters.get('pinned_channel_ids'):
        f['pinned_channel_ids'] = pinned_ids
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

    def _is_usable_number(ch, candidate: int | None, *, min_value: int | None) -> bool:
        if candidate is None:
            return False
        if min_value is not None and candidate < min_value:
            return False
        return True

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

    # Collect all pinned numbers up front so assignment can never displace them.
    pinned_numbers: set[int] = set()
    for src_chs in by_source.values():
        for ch in src_chs:
            if getattr(ch, 'number_pinned', False) and ch.number is not None:
                pinned_numbers.add(ch.number)

    # Assign numbers. Existing non-pinned Channel.number values are treated as
    # sticky auto numbers: keep them when still valid and free, only allocate
    # fresh values for channels that are new, missing a number, or now conflict.
    chnum_map: dict[int, int] = {}
    used_numbers: set[int] = set(pinned_numbers)
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
            cursor = start
            unassigned = []
            for ch in chs:
                if getattr(ch, 'number_pinned', False) and ch.number is not None:
                    chnum_map[ch.id] = ch.number
                    used_numbers.add(ch.number)
                    continue
                if _is_usable_number(ch, ch.number, min_value=start) and ch.number not in used_numbers:
                    chnum_map[ch.id] = ch.number
                    used_numbers.add(ch.number)
                else:
                    unassigned.append(ch)
            for ch in unassigned:
                while cursor in used_numbers:
                        cursor += 1
                chnum_map[ch.id] = cursor
                used_numbers.add(cursor)
                cursor += 1
        elif global_cursor is not None:
            unassigned = []
            for ch in chs:
                if getattr(ch, 'number_pinned', False) and ch.number is not None:
                    chnum_map[ch.id] = ch.number
                    used_numbers.add(ch.number)
                    continue
                if _is_usable_number(ch, ch.number, min_value=global_start) and ch.number not in used_numbers:
                    chnum_map[ch.id] = ch.number
                    used_numbers.add(ch.number)
                else:
                    unassigned.append(ch)
            for ch in unassigned:
                while global_cursor in used_numbers:
                        global_cursor += 1
                chnum_map[ch.id] = global_cursor
                used_numbers.add(global_cursor)
                global_cursor += 1

    return chnum_map, warnings


def _build_feed_chnum_map(channels, feed_chnum_start: int,
                          stored_numbers: dict[int, int] | None = None):
    """
    Sticky sequential numbering for a feed-level chnum_start.

    Pinned channels keep their stored number.  Non-pinned channels keep their
    previously-assigned feed number from `stored_numbers` (persisted in
    FeedChannelNumber) if it is >= feed_chnum_start and still free.  Only new
    or displaced channels get freshly assigned sequential numbers.
    """
    used_numbers: set[int] = set()
    result: dict[int, int] = {}
    unassigned = []

    # First pass: honour pinned channels and preserve valid stored assignments.
    for ch in channels:
        if getattr(ch, 'number_pinned', False) and ch.number is not None:
            result[ch.id] = ch.number
            used_numbers.add(ch.number)
        else:
            stored = stored_numbers.get(ch.id) if stored_numbers else None
            if stored is not None and stored >= feed_chnum_start and stored not in used_numbers:
                result[ch.id] = stored
                used_numbers.add(stored)
            else:
                unassigned.append(ch)

    # Second pass: assign fresh sequential numbers to new/displaced channels.
    cursor = feed_chnum_start
    for ch in unassigned:
        while cursor in used_numbers:
            cursor += 1
        result[ch.id] = cursor
        used_numbers.add(cursor)
        cursor += 1

    return result


def _build_sticky_gn_chnum_map(gn_channels, gn_start: int, used_numbers: set) -> dict:
    """
    Assign channel numbers to Gracenote channels starting at gn_start, with
    the same stickiness guarantee as standard channels: existing Channel.number
    is kept if it's >= gn_start and not already taken.  New/displaced channels
    fill in sequentially.
    """
    result = {}
    unassigned = []
    sorted_channels = sorted(
        gn_channels,
        key=lambda c: (c.number is None, c.number or 0, (c.name or '').lower()),
    )
    for ch in sorted_channels:
        if getattr(ch, 'number_pinned', False) and ch.number is not None:
            result[ch.id] = ch.number
            used_numbers.add(ch.number)
            continue
        if ch.number is not None and ch.number >= gn_start and ch.number not in used_numbers:
            result[ch.id] = ch.number
            used_numbers.add(ch.number)
        else:
            unassigned.append(ch)
    cursor = gn_start
    for ch in unassigned:
        while cursor in used_numbers:
            cursor += 1
        result[ch.id] = cursor
        used_numbers.add(cursor)
        cursor += 1
    return result


def _resolve_chnum_map(channels, *, feed_chnum_start: int = None,
                       namespace_start: int = None, feed_id: int = None):
    if namespace_start is not None:
        return _build_feed_chnum_map(channels, namespace_start), []
    if feed_chnum_start is not None:
        stored_numbers: dict[int, int] = {}
        if feed_id is not None:
            from app.models import FeedChannelNumber
            rows = FeedChannelNumber.query.filter_by(feed_id=feed_id).all()
            stored_numbers = {r.channel_id: r.number for r in rows}
        return _build_feed_chnum_map(channels, feed_chnum_start, stored_numbers=stored_numbers), []
    return _build_source_chnum_map(channels)


def get_chnum_overlaps() -> list[str]:
    """
    Return a list of overlap warning strings for the current source configuration.
    Used by the admin UI to surface misconfiguration.
    """
    channels = _selected_channel_stubs({}, gracenote=None)
    _, warnings = _build_source_chnum_map(channels)
    return warnings


def get_global_chnum_overlaps() -> list[str]:
    """
    Return warnings for duplicate tvg-chno values.

    Master outputs are checked against themselves only — overlap between a
    master M3U and a feed M3U is expected (users subscribe to one OR the other,
    not both).  Feed outputs are checked against each other so that a user who
    subscribes to multiple feeds doesn't see duplicate channel numbers.
    """
    master_outputs: list[tuple[str, list, dict[int, int]]] = []
    feed_outputs:   list[tuple[str, list, dict[int, int]]] = []

    master_standard = _selected_channel_stubs({}, gracenote=False)
    master_standard_map, _ = _resolve_chnum_map(master_standard)
    master_outputs.append(('master /m3u', master_standard, master_standard_map))

    master_gracenote = _selected_channel_stubs({}, gracenote=True)
    master_gracenote_map, _ = _resolve_chnum_map(
        master_gracenote,
        namespace_start=_MASTER_GRACENOTE_START,
    )
    master_outputs.append(('master /m3u/gracenote', master_gracenote, master_gracenote_map))

    feeds = Feed.query.filter_by(is_enabled=True).order_by(Feed.slug).all()
    for feed in feeds:
        filters = feed_to_query_filters(feed.filters or {})

        std_channels = _selected_channel_stubs(filters, gracenote=False)
        std_ns = None if feed.chnum_start is not None else feed_namespace_start(feed, gracenote=False)
        std_map, _ = _resolve_chnum_map(
            std_channels,
            feed_chnum_start=feed.chnum_start,
            namespace_start=std_ns,
            feed_id=feed.id if feed.chnum_start is not None else None,
        )
        feed_outputs.append((f'feed {feed.slug} /m3u', std_channels, std_map))

        gn_channels = _selected_channel_stubs(filters, gracenote=True)
        gn_ns = None if feed.chnum_start is not None else feed_namespace_start(feed, gracenote=True)
        gn_map, _ = _resolve_chnum_map(
            gn_channels,
            feed_chnum_start=feed_gracenote_start(feed) if feed.chnum_start is not None else None,
            namespace_start=gn_ns,
            feed_id=feed.id if feed.chnum_start is not None else None,
        )
        feed_outputs.append((f'feed {feed.slug} /m3u/gracenote', gn_channels, gn_map))

    warnings: list[str] = []

    def _check(outputs):
        # seen maps chnum -> (output_name, ch.name, ch.id)
        # Same channel ID appearing in multiple feeds with the same pinned number
        # is not a real conflict — it's the same channel, just in multiple feeds.
        # Only warn when a genuinely different channel claims the same number.
        seen: dict[int, tuple[str, str, int]] = {}
        for output_name, channels, chnum_map in outputs:
            for ch in channels:
                chnum = chnum_map.get(ch.id)
                if not chnum:
                    continue
                previous = seen.get(chnum)
                if previous and previous[2] != ch.id:
                    warnings.append(
                        f"ch {chnum} is duplicated: {previous[1]} in {previous[0]} and "
                        f"{ch.name} in {output_name}"
                    )
                elif not previous:
                    seen[chnum] = (output_name, ch.name, ch.id)

    _check(master_outputs)
    # Check std feeds against each other, gracenote feeds against each other.
    # Never compare std vs gracenote — users subscribe to one OR the other.
    _check([o for o in feed_outputs if not o[0].endswith('/gracenote')])
    _check([o for o in feed_outputs if o[0].endswith('/gracenote')])
    return warnings


def generate_m3u(filters: dict = None, base_url: str = None,
                 feed_chnum_start: int = None, namespace_start: int = None,
                 feed_id: int = None) -> str:
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
        feed_id=feed_id if feed_chnum_start is not None else None,
    )
    if feed_chnum_start is None and namespace_start is None:
        for w in warnings:
            log.warning('chnum overlap: %s', w)

    multi_country_map = _source_multi_country_map(channels)
    lines = ['#EXTM3U']
    for ch in channels:
        tvg_id = _tvg_id(ch)
        display_name = _channel_display_name(ch, multi_country_map)
        attrs = [
            f'channel-id="{tvg_id}"',
            f'tvg-id="{tvg_id}"',
            f'tvg-name="{_esc(display_name)}"',
            f'group-title="{_esc(ch.category or ch.source.display_name)}"',
        ]
        if ch.logo_url:
            attrs.append(f'tvg-logo="{proxy_logo_url(ch.logo_url, base_url) or ch.logo_url}"')
        chnum = chnum_map.get(ch.id)
        if chnum:
            attrs.append(f'tvg-chno="{chnum}"')
        if ch.description:
            attrs.append(f'tvg-description="{_esc(ch.description)}"')
            attrs.append(f'tvc-guide-description="{_esc(ch.description)}"')
        if ch.stream_info:
            vcodec, acodec = _tvc_stream_codecs(ch.stream_info)
            if vcodec:
                attrs.append(f'tvc-stream-vcodec="{vcodec}"')
            if acodec:
                attrs.append(f'tvc-stream-acodec="{acodec}"')
        guide_cat = _tvc_guide_category(ch)
        if guide_cat:
            attrs.append(f'tvc-guide-categories="{guide_cat}"')
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{display_name}')
        lines.append(f'{base_url}/play/{ch.source.name}/{_url_quote(ch.source_channel_id, safe="")}.m3u8')

    return '\n'.join(lines)


def generate_gracenote_m3u(filters: dict = None, base_url: str = None,
                            feed_chnum_start: int = None, namespace_start: int = None,
                            feed_id: int = None) -> str:
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
        feed_id=feed_id if feed_chnum_start is not None else None,
    )
    if feed_chnum_start is None and namespace_start is None:
        for w in warnings:
            log.warning('chnum overlap (gracenote): %s', w)

    multi_country_map = _source_multi_country_map(channels)
    lines = ['#EXTM3U']
    for ch in channels:
        gracenote_id = _parse_gracenote_id(ch)
        display_name = _channel_display_name(ch, multi_country_map)
        attrs = [
            f'channel-id="{_tvg_id(ch)}"',
            f'tvc-guide-stationid="{gracenote_id}"',
            f'tvg-name="{_esc(display_name)}"',
            f'group-title="{_esc(ch.category or ch.source.display_name)}"',
        ]
        if ch.logo_url:
            attrs.append(f'tvg-logo="{proxy_logo_url(ch.logo_url, base_url) or ch.logo_url}"')
        chnum = chnum_map.get(ch.id)
        if chnum:
            attrs.append(f'tvg-chno="{chnum}"')
        if ch.description:
            attrs.append(f'tvg-description="{_esc(ch.description)}"')
            attrs.append(f'tvc-guide-description="{_esc(ch.description)}"')
        if ch.stream_info:
            vcodec, acodec = _tvc_stream_codecs(ch.stream_info)
            if vcodec:
                attrs.append(f'tvc-stream-vcodec="{vcodec}"')
            if acodec:
                attrs.append(f'tvc-stream-acodec="{acodec}"')
        guide_cat = _tvc_guide_category(ch)
        if guide_cat:
            attrs.append(f'tvc-guide-categories="{guide_cat}"')
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{display_name}')
        lines.append(f'{base_url}/play/{ch.source.name}/{_url_quote(ch.source_channel_id, safe="")}.m3u8')

    return '\n'.join(lines)


def _tvg_id(ch) -> str:
    return f'{ch.source.name}.{ch.source_channel_id}'


def _try_fix_mojibake(s: str) -> str:
    """Fix UTF-8 bytes that were decoded as Latin-1 (up to two rounds)."""
    for _ in range(2):
        try:
            fixed = s.encode('latin-1').decode('utf-8')
            if fixed == s:
                break
            s = fixed
        except (UnicodeEncodeError, UnicodeDecodeError):
            break
    return s


def _sanitize(s: str | None) -> str:
    """Strip control characters from text (safe for both M3U attributes and XML text nodes)."""
    if not s:
        return ''
    s = _try_fix_mojibake(s)
    s = s.translate(_WIN1252_REMAP)
    s = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]+', '', s)  # strip remaining C0 controls
    s = re.sub(r'[\r\n\t]+', ' ', s)
    s = re.sub(r'  +', ' ', s).strip()
    return s


def _esc(s):
    """Sanitize and replace double quotes for use inside M3U attribute values."""
    return _sanitize(s).replace('"', "'")


# Channels DVR tvc-guide-categories accepted values: Movie, Sports event, Series
_GUIDE_CATEGORY_MAP = {
    'movies': 'Movie',
    'sports': 'Sports event',
    'series': 'Series',
    'tv shows': 'Series',
    'television': 'Series',
}


def _tvc_guide_category(ch) -> str | None:
    return _GUIDE_CATEGORY_MAP.get((ch.category or '').lower())


_VALID_VCODECS = {'h264', 'mpeg2', 'hevc'}


def _tvc_stream_codecs(stream_info: dict) -> tuple[str | None, str | None]:
    """Return (vcodec, acodec) strings for tvc-stream-vcodec/acodec, or None if unknown.
    Only emits values Channels DVR recognises; 'unknown' and unrecognised codecs are suppressed.
    """
    raw = (stream_info.get('video_codec') or '').lower()
    vcodec = raw if raw in _VALID_VCODECS else None
    acodec = None
    variants = stream_info.get('variants') or []
    if variants:
        codecs_str = (variants[0].get('codecs') or '').upper()
        if 'AAC' in codecs_str:
            acodec = 'aac'
        elif 'AC3' in codecs_str or 'AC-3' in codecs_str:
            acodec = 'ac3'
    return vcodec, acodec

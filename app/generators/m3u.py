import logging
import re
from dataclasses import dataclass
from urllib.parse import quote as _url_quote
from sqlalchemy.orm import contains_eager
from ..extensions import db
from ..models import Channel, Source, Feed, AppSettings
from ..url import proxy_logo_url
from ..scrapers import registry as _scraper_registry

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
    0x00AD: None,    # SOFT HYPHEN (invisible)
    0x200B: None,    # ZERO WIDTH SPACE
    0x2060: None,    # WORD JOINER (invisible)
    0xFEFF: None,    # ZERO WIDTH NO-BREAK SPACE / BOM
    0xFFFD: None,    # REPLACEMENT CHARACTER
})

_CHNUM_NAMESPACE_BLOCK = 100000
_MASTER_GRACENOTE_START = 100000
_FEED_NAMESPACE_BASE = 200000
_REGION_LABEL_SOURCES = {"pluto", "samsung", "tcl"}

# Backstop cursor for _build_source_chnum_map's no-config-at-all branch: mints a
# number for a channel that has never had ANY number (not from pinning, not
# preserved, nothing) when neither the default feed's chnum_start nor
# MASTER_CHANNEL_NUMBER_START is set. Never used to reassign a channel that
# already has some number, however it got there — only to keep a genuinely
# never-numbered channel from staying permanently blank. Matches the value
# schema.py seeds fresh installs' default feed chnum_start with, so a number
# from here reads the same as one from that seed if anyone goes looking.
_UNCONFIGURED_CHNUM_FALLBACK_START = 5000

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
    gracenote_mode: str | None
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


def _has_gracenote_claim(ch) -> bool:
    """
    True if a channel should be excluded from the standard M3U partition.

    Broader than _parse_gracenote_id only for the dedicated gracenote_id
    column: any non-empty value there (even one that fails the format regex)
    counts as a claim, so a malformed-but-deliberately-set gracenote_id
    doesn't bleed into the standard M3U while it waits to be corrected.

    The slug-based fallback must stay as strict as _parse_gracenote_id's own
    check, though — a bare '|' in the slug isn't a reliable signal on its
    own, since ordinary channel names containing a literal pipe (e.g. "FOX
    LOCAL Dallas | Fort Worth") slugify to the same shape as the legacy Roku
    "{play_id}|{gracenote_id}" encoding this fallback exists for. Without
    requiring the part after '|' to actually look like a gracenote ID, such
    a channel claims Gracenote here but fails to parse in
    _parse_gracenote_id, falling through both partitions — dropped from the
    M3U entirely, not just blank in the admin Ch# column.
    """
    if getattr(ch, 'gracenote_mode', None) == 'off':
        return False
    if (getattr(ch, 'gracenote_id', None) or '').strip():
        return True
    slug = (getattr(ch, 'slug', None) or '')
    if '|' in slug:
        candidate = slug.split('|', 1)[1].strip()
        return bool(candidate and _GRACENOTE_PREFIX_RE.match(candidate))
    return False


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


def _build_channel_query(filters: dict, *, activity: str = 'active'):
    """Shared filtered query for channels.

    A channel is "bridge-only" — kept out of standard output, carried only by the
    PrismCast feed — when EITHER the audit flagged it (requires_drm_bridge) OR it's a
    DASH stream from a DRM-capable source (e.g. Amazon, Sling). The latter is
    intrinsically un-playable on a normal client, so we don't need an audit to know it.

    activity='active'      -> the standard set: active + enabled, EXCLUDING bridge-only.
    activity='drm_bridge'  -> active + enabled, ONLY the bridge-only channels. The
                              PrismCast feed and shared EPG union these back in.
    activity='any'         -> active + enabled, WITHOUT the bridge split (standard +
                              bridge-only together). Use for membership/count checks
                              that ask "does this feed carry the channel at all?".
    """
    _capable = _drm_bridge_capable_source_names()
    _all_drm = _all_channels_drm_source_names()
    _intrinsic_dash = db.and_(
        db.func.lower(db.func.coalesce(Channel.stream_type, '')) == 'dash',
        Source.name.in_(_capable),
    ) if _capable else None
    _intrinsic_all_drm = Source.name.in_(_all_drm) if _all_drm else None

    if activity == 'any':
        activity_predicates = (
            Channel.is_active  == True,
            Channel.is_enabled == True,
        )
    elif activity == 'drm_bridge':
        _bridge_only = Channel.requires_drm_bridge == True
        if _intrinsic_dash is not None:
            _bridge_only = db.or_(_bridge_only, _intrinsic_dash)
        if _intrinsic_all_drm is not None:
            _bridge_only = db.or_(_bridge_only, _intrinsic_all_drm)
        activity_predicates = (
            Channel.is_active  == True,
            Channel.is_enabled == True,
            _bridge_only,
        )
    else:
        _preds = [
            Channel.is_active  == True,
            Channel.is_enabled == True,
            db.or_(Channel.requires_drm_bridge == False,
                   Channel.requires_drm_bridge == None),
        ]
        if _intrinsic_dash is not None:
            _preds.append(db.not_(_intrinsic_dash))
        if _intrinsic_all_drm is not None:
            _preds.append(db.not_(_intrinsic_all_drm))
        activity_predicates = tuple(_preds)
    query = Channel.query.join(Source).options(
        contains_eager(Channel.source).load_only(
            Source.id,
            Source.name,
            Source.display_name,
            Source.chnum_start,
        ),
    ).filter(
        *activity_predicates,
        Source.is_enabled  == True,
        Source.epg_only    == False,
        Channel.stream_url != None,
    )
    if channel_ids := filters.get('channel_ids'):
        query = query.filter(Channel.id.in_(channel_ids))
    else:
        _match = []
        if sources := filters.get('source'):
            _match.append(Source.name.in_(sources))
        if categories := filters.get('category'):
            _match.append(Channel.category.in_(categories))
        if languages := filters.get('languages'):
            _match.append(Channel.language.in_(languages))
        elif language := filters.get('language'):
            _match.append(Channel.language == language)
        if countries := filters.get('countries'):
            _match.append(Channel.country.in_(countries))
        if gracenote := filters.get('gracenote'):
            if gracenote == 'has':
                _match.append(db.and_(Channel.gracenote_id != None, Channel.gracenote_id != ''))
            elif gracenote == 'missing':
                _match.append(db.and_(
                    db.or_(Channel.gracenote_id == None, Channel.gracenote_id == ''),
                    ~Channel.slug.like('%|%'),
                ))
        if search := filters.get('search'):
            _match.append(Channel.name.ilike(f'%{search}%'))
        _match_expr = db.and_(*_match) if _match else None
        # Pinned channels join the feed regardless of the source/category filters above
        # (still subject to the active/enabled/bridge predicates), so the counts and the
        # actual M3U/PrismCast output agree on what a pin adds.
        if pinned_ids := filters.get('pinned_channel_ids'):
            _pin = Channel.id.in_(pinned_ids)
            query = query.filter(db.or_(_match_expr, _pin) if _match_expr is not None else _pin)
        elif _match_expr is not None:
            query = query.filter(_match_expr)
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
        Channel.gracenote_mode,
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
        _match = []
        if sources := filters.get('source'):
            _match.append(Source.name.in_(sources))
        if categories := filters.get('category'):
            _match.append(Channel.category.in_(categories))
        if languages := filters.get('languages'):
            _match.append(Channel.language.in_(languages))
        elif language := filters.get('language'):
            _match.append(Channel.language == language)
        if countries := filters.get('countries'):
            _match.append(Channel.country.in_(countries))
        if gracenote := filters.get('gracenote'):
            if gracenote == 'has':
                _match.append(db.and_(Channel.gracenote_id != None, Channel.gracenote_id != ''))
            elif gracenote == 'missing':
                _match.append(db.and_(
                    db.or_(Channel.gracenote_id == None, Channel.gracenote_id == ''),
                    ~Channel.slug.like('%|%'),
                ))
        if search := filters.get('search'):
            _match.append(Channel.name.ilike(f'%{search}%'))
        _match_expr = db.and_(*_match) if _match else None
        # Pinned channels join the feed regardless of the source/category filters above
        # (still subject to the active/enabled/bridge predicates), so the counts and the
        # actual M3U/PrismCast output agree on what a pin adds.
        if pinned_ids := filters.get('pinned_channel_ids'):
            _pin = Channel.id.in_(pinned_ids)
            query = query.filter(db.or_(_match_expr, _pin) if _match_expr is not None else _pin)
        elif _match_expr is not None:
            query = query.filter(_match_expr)
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
            gracenote_mode=row.gracenote_mode,
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
        channels = [ch for ch in channels if not _has_gracenote_claim(ch)]

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
        channels = [ch for ch in channels if not _has_gracenote_claim(ch)]

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
    Starting channel number for a feed's Gracenote namespace M3U.

    Only used for feeds without an explicit chnum_start (namespace-based numbering).
    Feeds with chnum_start use a unified pool (FeedChannelNumber) shared by both
    the standard and Gracenote M3Us, so this function is not called for them.
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


def _is_usable_number(ch, candidate: int | None, *, min_value: int | None) -> bool:
    if candidate is None:
        return False
    if min_value is not None and candidate < min_value:
        return False
    return True


def _partition_unassigned(chs, min_value, used_numbers, out):
    """
    Split `chs` into channels whose existing Channel.number can be claimed
    directly (recorded into `out`/`used_numbers` immediately) and those still
    needing a fresh sequential number (returned as a list).

    A pinned channel always keeps its number. A non-pinned channel keeps its
    existing number only if `_is_usable_number` accepts it (non-null and
    >= min_value) and no earlier channel already claimed it.
    """
    unassigned = []
    for ch in chs:
        if getattr(ch, 'number_pinned', False) and ch.number is not None:
            out[ch.id] = ch.number
            used_numbers.add(ch.number)
            continue
        if _is_usable_number(ch, ch.number, min_value=min_value) and ch.number not in used_numbers:
            out[ch.id] = ch.number
            used_numbers.add(ch.number)
        else:
            unassigned.append(ch)
    return unassigned


def _assign_sequential(unassigned, cursor, used_numbers, out):
    """
    Assign each channel in `unassigned` the next number >= cursor that isn't
    in `used_numbers`, in order, recording into `out`/`used_numbers`.

    Returns the cursor position to resume from — callers that share one
    cursor across multiple calls (e.g. the same global/fallback cursor
    reused for each source in turn) pass the returned value back in as the
    next call's `cursor`.
    """
    for ch in unassigned:
        while cursor in used_numbers:
            cursor += 1
        out[ch.id] = cursor
        used_numbers.add(cursor)
        cursor += 1
    return cursor


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
    # Backstop cursor for the no-config-at-all branch below — only ever draws
    # from here for a channel with no number at all, never to reassign one
    # that already has some number. Shared across every source that falls into
    # that branch so two such channels can't collide with each other.
    fallback_cursor = _UNCONFIGURED_CHNUM_FALLBACK_START
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
            unassigned = _partition_unassigned(chs, start, used_numbers, chnum_map)
            _assign_sequential(unassigned, start, used_numbers, chnum_map)
        elif global_cursor is not None:
            unassigned = _partition_unassigned(chs, global_start, used_numbers, chnum_map)
            global_cursor = _assign_sequential(unassigned, global_cursor, used_numbers, chnum_map)
        else:
            # Neither this source nor AppSettings has a chnum_start configured.
            # Docstring above promises these channels "fall back to their
            # existing Channel.number (unchanged from scraper output)" — but
            # until this branch existed, they were silently skipped entirely
            # (missing tvg-chno in the real M3U, blank Ch# in the admin list).
            # Same shape as the two branches above, just with no floor on what
            # counts as a valid existing number (min_value=None) and drawing
            # any genuinely-unassigned channel from the fallback cursor instead
            # of a configured one — a channel that already has a number never
            # gets reassigned just because this branch exists.
            unassigned = _partition_unassigned(chs, None, used_numbers, chnum_map)
            fallback_cursor = _assign_sequential(unassigned, fallback_cursor, used_numbers, chnum_map)

    return chnum_map, warnings


def _build_feed_chnum_map(channels, feed_chnum_start: int,
                          stored_numbers: dict[int, int] | None = None):
    """
    Sticky sequential numbering for a feed-level chnum_start.

    Pinned channels keep their stored number.  Non-pinned channels keep their
    previously-assigned feed number from `stored_numbers` (persisted in
    FeedChannelNumber) if it is >= feed_chnum_start and still free.  Only new
    or displaced channels get freshly assigned sequential numbers.

    `stored_numbers` may contain entries for channels outside `channels` (e.g.
    the other M3U partition when called for only the std or gracenote subset).
    Those numbers are reserved before the cursor runs so new channels never
    receive a number already claimed by the other partition.
    """
    used_numbers: set[int] = set()
    result: dict[int, int] = {}
    unassigned = []
    channel_ids = {ch.id for ch in channels}

    # First pass: honour pinned channels and preserve valid stored assignments.
    for ch in channels:
        if getattr(ch, 'number_pinned', False) and ch.number is not None and ch.number not in used_numbers:
            result[ch.id] = ch.number
            used_numbers.add(ch.number)
        else:
            stored = stored_numbers.get(ch.id) if stored_numbers else None
            if stored is not None and stored >= feed_chnum_start and stored not in used_numbers:
                result[ch.id] = stored
                used_numbers.add(stored)
            else:
                unassigned.append(ch)

    # Reserve numbers held by channels outside this batch (e.g. the other
    # M3U partition — std vs gracenote) so the cursor never steps on them.
    if stored_numbers:
        for cid, num in stored_numbers.items():
            if cid not in channel_ids:
                used_numbers.add(num)

    # Second pass: assign fresh sequential numbers to new/displaced channels.
    _assign_sequential(unassigned, feed_chnum_start, used_numbers, result)

    return result


def build_manual_order_map(channels, order_ids: list[int], start: int) -> dict[int, int]:
    """
    Number `channels` sequentially from `start` following an explicit
    user-chosen `order_ids` sequence (drag-drop ordering from the feed modal).

    Master-pinned channels always keep their Channel.number; those numbers are
    reserved so the sequential cursor skips over them.  Channels in the feed but
    absent from `order_ids` (e.g. newly matched by the filters since the order
    was captured) are appended after the ordered block in master-number order.
    Ids in `order_ids` that are no longer feed members are ignored.
    """
    by_id = {ch.id: ch for ch in channels}
    ordered = [by_id[cid] for cid in order_ids if cid in by_id]
    seen = {ch.id for ch in ordered}
    ordered += sorted(
        (ch for ch in channels if ch.id not in seen),
        key=lambda c: (c.number is None, c.number or 0, (c.name or '').lower()),
    )

    result: dict[int, int] = {}
    used: set[int] = set()
    for ch in ordered:
        if getattr(ch, 'number_pinned', False) and ch.number is not None:
            result[ch.id] = ch.number
            used.add(ch.number)
    cursor = start
    for ch in ordered:
        if ch.id in result:
            continue
        while cursor in used:
            cursor += 1
        result[ch.id] = cursor
        used.add(cursor)
        cursor += 1
    return result


def _sort_by_assigned_chnum(channels, chnum_map: dict) -> None:
    """
    Sort a channel list in place by its resolved tvg-chno so playlist line
    order matches the numbers we emit.  Only used for feed-pool numbering
    (feed_chnum_start / namespace_start), where a manual reorder can make the
    assigned numbers diverge from master Channel.number order.
    """
    channels.sort(key=lambda c: (chnum_map.get(c.id) is None,
                                 chnum_map.get(c.id) or 0,
                                 (c.name or '').lower()))


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
    _assign_sequential(unassigned, gn_start, used_numbers, result)
    return result


def _resolve_chnum_map(channels, *, feed_chnum_start: int = None,
                       namespace_start: int = None, feed_id: int = None):
    if namespace_start is not None:
        stored_numbers: dict[int, int] = {}
        if feed_id is not None:
            from app.models import FeedChannelNumber
            rows = FeedChannelNumber.query.filter_by(feed_id=feed_id).all()
            stored_numbers = {r.channel_id: r.number for r in rows}
        return _build_feed_chnum_map(channels, namespace_start, stored_numbers=stored_numbers), []
    if feed_chnum_start is not None:
        stored_numbers = {}
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
    # std/gn output pairs for unified-pool (chnum_start) feeds, which DO share a
    # single number range and so must be cross-checked against each other.
    unified_pairs:  list[tuple[tuple, tuple]] = []

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
        std_out = (f'feed {feed.slug} /m3u', std_channels, std_map)
        feed_outputs.append(std_out)

        gn_channels = _selected_channel_stubs(filters, gracenote=True)
        gn_ns = None if feed.chnum_start is not None else feed_namespace_start(feed, gracenote=True)
        gn_map, _ = _resolve_chnum_map(
            gn_channels,
            feed_chnum_start=feed.chnum_start if feed.chnum_start is not None else None,
            namespace_start=gn_ns,
            feed_id=feed.id if feed.chnum_start is not None else None,
        )
        gn_out = (f'feed {feed.slug} /m3u/gracenote', gn_channels, gn_map)
        feed_outputs.append(gn_out)

        if feed.chnum_start is not None:
            unified_pairs.append((std_out, gn_out))

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
    # Across different feeds, check std feeds against each other and gracenote
    # feeds against each other.  Don't compare a std feed against an unrelated
    # gracenote feed — for namespace-numbered feeds the two live in separate
    # 100k blocks and a user adds both halves of a feed, not halves of two feeds.
    _check([o for o in feed_outputs if not o[0].endswith('/gracenote')])
    _check([o for o in feed_outputs if o[0].endswith('/gracenote')])
    # Within a single chnum_start feed, std and gracenote channels DO share one
    # unified pool, so a number used on both sides is a real collision.
    for std_out, gn_out in unified_pairs:
        _check([std_out, gn_out])
    return warnings


def _channel_play_url(ch, base_url: str) -> str:
    source_name = ch.source.name
    channel_id = _url_quote(ch.source_channel_id, safe="")
    if source_name == 'cspan':
        return f'{base_url}/play/cspan/{channel_id}/proxy.m3u8'
    return f'{base_url}/play/{source_name}/{channel_id}.m3u8'

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

    _s = AppSettings.get()
    _image_proxy = _s.image_proxy_enabled if _s.image_proxy_enabled is not None else True

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
    else:
        _sort_by_assigned_chnum(channels, chnum_map)

    multi_country_map = _source_multi_country_map(channels)
    _kodi_props_cache: dict[str, dict] = {}
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
            attrs.append(f'tvg-logo="{proxy_logo_url(ch.logo_url, base_url, image_proxy_enabled=_image_proxy) or ch.logo_url}"')
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
        src_name = ch.source.name
        scraper_cls = _scraper_registry.get(src_name)
        per_ch_props = None
        if scraper_cls and ch.source_channel_id:
            per_ch_props = scraper_cls.get_kodi_props_for_channel(base_url, ch.source_channel_id)
        if per_ch_props is not None:
            for prop_key, prop_val in per_ch_props.items():
                lines.append(f'#KODIPROP:{prop_key}={prop_val}')
        else:
            if src_name not in _kodi_props_cache:
                if scraper_cls and hasattr(scraper_cls, 'get_kodi_props'):
                    _kodi_props_cache[src_name] = scraper_cls.get_kodi_props(base_url)
                else:
                    _kodi_props_cache[src_name] = getattr(scraper_cls, 'kodi_props', {}) if scraper_cls else {}
            for prop_key, prop_val in _kodi_props_cache[src_name].items():
                lines.append(f'#KODIPROP:{prop_key}={prop_val}')
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{_sanitize(display_name)}')
        lines.append(_channel_play_url(ch, base_url))

    return '\n'.join(lines)


def generate_native_m3u(filters: dict = None, base_url: str = None,
                        feed_chnum_start: int = None, namespace_start: int = None,
                        feed_id: int = None, include_description: bool = True) -> str:
    """
    Native (scraped-only) playlist — all feed channels, standard play URLs.

    Includes channels regardless of Gracenote status; uses scraped EPG data.
    Intended to be paired with the native XMLTV endpoint, not the Gracenote
    or standard split variants.

    include_description=False omits the tvg-description/tvc-guide-description
    attributes entirely (the blurb is channel-level only; program data rides in
    the EPG XML). Threadfin/Plex bridges bleed that long comma-bearing value into
    the channel name, so the native artifact is generated without it — cleaner than
    emitting-then-stripping the rendered text.
    """
    filters  = filters or {}
    base_url = (base_url or '').rstrip('/')

    _s = AppSettings.get()
    _image_proxy = _s.image_proxy_enabled if _s.image_proxy_enabled is not None else True

    channels = _selected_channels(filters, gracenote=None)

    chnum_map, warnings = _resolve_chnum_map(
        channels,
        feed_chnum_start=feed_chnum_start,
        namespace_start=namespace_start,
        feed_id=feed_id if feed_chnum_start is not None else None,
    )
    if feed_chnum_start is None and namespace_start is None:
        for w in warnings:
            log.warning('chnum overlap (native): %s', w)
    else:
        _sort_by_assigned_chnum(channels, chnum_map)

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
            attrs.append(f'tvg-logo="{proxy_logo_url(ch.logo_url, base_url, image_proxy_enabled=_image_proxy) or ch.logo_url}"')
        chnum = chnum_map.get(ch.id)
        if chnum:
            attrs.append(f'tvg-chno="{chnum}"')
        if ch.description and include_description:
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
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{_sanitize(display_name)}')
        lines.append(_channel_play_url(ch, base_url))

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

    _s = AppSettings.get()
    _image_proxy = _s.image_proxy_enabled if _s.image_proxy_enabled is not None else True

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
    else:
        _sort_by_assigned_chnum(channels, chnum_map)

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
            attrs.append(f'tvg-logo="{proxy_logo_url(ch.logo_url, base_url, image_proxy_enabled=_image_proxy) or ch.logo_url}"')
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
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{_sanitize(display_name)}')
        lines.append(_channel_play_url(ch, base_url))

    return '\n'.join(lines)


# Sources FastChannels forces to native-HLS playback (AES-128; see
# api.py _get_playback_info ~ "playback_mode = 'native'"). Chrome has no native
# HLS, so PrismCast's headless Chrome can't play them — exclude from the bridge.
_PRISMCAST_INCAPABLE_SOURCES = {'pluto', 'fubo', 'roku'}
# Stream types with no browser playback path: snapshot feeds, and muxed MPEG-TS
# (e.g. HDHomeRun OTA — MPEG-2/AC-3, which Chrome can't decode).
_PRISMCAST_UNSUPPORTED_TYPES = {'mjpeg', 'jpeg_snapshot', 'mpegts', 'ts'}
# PrismCast site profile for the /watch page: presses 'f' to trigger fullscreen so
# the captured video fills the frame (otherwise the tab stays windowed and the
# picture is letterboxed inside the output). See /play?url=...&profile=<name>.
_PRISMCAST_PROFILE = 'keyboardFullscreen'


def _source_is_drm_capable(source_name: str | None) -> bool:
    from ..scrapers import registry as _reg
    return _reg.source_is_drm_capable(source_name)


def _drm_bridge_capable_source_names() -> list[str]:
    from ..scrapers import registry as _reg
    return _reg.drm_capable_source_names()


def _all_channels_drm_source_names() -> list[str]:
    from ..scrapers import registry as _reg
    return [
        name for name, cls in _reg.get_all().items()
        if getattr(cls, 'license_url', None)
        and getattr(cls, 'all_channels_require_drm_bridge', False)
    ]


def _prismcast_capturable(ch) -> bool:
    """True if PrismCast's headless Chrome can render+capture this channel's
    /watch page.

    DRM / DASH channels are served to the watch page as DASH+Widevine (EME), which
    Chrome renders regardless of the native-HLS source exclusion — but only when the
    source actually has license handling, else EME can't complete and capture is black.
    Plain channels fall back to the source rule: AES-128 native-HLS sources can't play
    in Chrome, and non-browser stream types (snapshot/MPEG-TS) never can."""
    stype = (ch.stream_type or '').strip().lower()
    if stype in _PRISMCAST_UNSUPPORTED_TYPES:
        return False
    drm = bool(getattr(ch, 'requires_drm_bridge', False)) \
        or (getattr(ch, 'disable_reason', None) or '').startswith('DRM')
    if drm or stype == 'dash':
        return _source_is_drm_capable(ch.source.name if ch.source else None)
    if ch.source and ch.source.name in _PRISMCAST_INCAPABLE_SOURCES:
        return False
    return True


def _needs_prismcast_bridge(ch) -> bool:
    """True only for channels a normal IPTV client can't play directly — i.e. DRM,
    which needs a browser (EME) to decrypt. Everything else (plain/AES-128 HLS,
    MP4, MPEG-TS) plays fine straight from the /play proxy, so it skips PrismCast
    entirely — no capture slot, no transcode, full quality."""
    if (ch.stream_type or '').strip().lower() == 'dash':
        return True
    if getattr(ch, 'requires_drm_bridge', False):
        return True
    if (getattr(ch, 'disable_reason', None) or '').startswith('DRM'):
        return True
    return False

def _prismcast_bridge_url(ch, prismcast_url: str, inner_base_url: str) -> str:
    # DRM -> route through PrismCast's headless Chrome (EME decrypt + capture).
    # profile=keyboardFullscreen makes PrismCast press 'f', which the /watch
    # page turns into a real fullscreen; without it, capture may stay windowed.
    watch_url = f'{inner_base_url}/watch/{ch.id}'
    return f'{prismcast_url}/play?url={_url_quote(watch_url, safe="")}&profile={_PRISMCAST_PROFILE}'


def generate_prismcast_m3u(filters: dict = None, base_url: str = None, *,
                           prismcast_url: str, inner_base_url: str = None,
                           feed_chnum_start: int = None, namespace_start: int = None,
                           feed_id: int = None, gracenote: bool = False) -> str:
    """
    Hybrid PrismCast playlist where each channel takes the cheapest viable path:

      * DRM channels (browser/EME required) are wrapped through PrismCast's
        `/play?url=` endpoint with a fullscreen profile, so its headless Chrome
        renders the /watch/<id> page fullscreen (decrypting via EME) and
        re-streams clean, full-frame HLS.
      * Everything else (plain/AES-128 HLS, MP4, MPEG-TS) is emitted as the
        normal direct /play/<source>/<id>.m3u8 URL — no capture slot, no
        transcode, full quality — exactly what the standard M3U would send.

    Guide routing partitions the same way as generate_m3u / generate_gracenote_m3u:

      * gracenote=False — excludes channels with a Gracenote ID; emits tvg-id so
        the guide pairs with our XMLTV /epg.xml.
      * gracenote=True  — only channels with a Gracenote ID; emits
        tvc-guide-stationid so Channels DVR routes guide data through Gracenote.

    The URL (bridge vs. direct) is orthogonal to guide routing — both partitions
    apply the same DRM-bridge selection.

    prismcast_url   — PrismCast base, e.g. http://192.168.1.x:5589
    inner_base_url  — base URL PrismCast's Chrome uses to reach this server's
                      /watch pages; should be a secure context for DRM (loopback
                      or HTTPS). Falls back to base_url.
    """
    filters  = filters or {}
    base_url = (base_url or '').rstrip('/')
    prismcast_url  = (prismcast_url or '').rstrip('/')
    inner_base_url = (inner_base_url or base_url).rstrip('/')

    _s = AppSettings.get()
    _image_proxy = _s.image_proxy_enabled if _s.image_proxy_enabled is not None else True

    channels = _selected_channels(filters, gracenote=gracenote)

    # Union in channels the audit disabled purely for DRM but which a browser can
    # decrypt (DRM-capable source). They're absent from the standard feed (a normal
    # client can't play them) but the PrismCast feed bridges them via /watch. Append
    # after the active set, deduped, capturable-only, honoring the Gracenote partition.
    _seen = {ch.id for ch in channels}
    for ch in _build_channel_query(filters, activity='drm_bridge').all():
        if ch.id in _seen or not _prismcast_capturable(ch):
            continue
        if gracenote and not _parse_gracenote_id(ch):
            continue
        if not gracenote and _has_gracenote_claim(ch):
            continue
        channels.append(ch)
        _seen.add(ch.id)

    chnum_map, warnings = _resolve_chnum_map(
        channels,
        feed_chnum_start=feed_chnum_start,
        namespace_start=namespace_start,
        feed_id=feed_id if feed_chnum_start is not None else None,
    )
    if feed_chnum_start is None and namespace_start is None:
        for w in warnings:
            log.warning('chnum overlap (prismcast): %s', w)
    else:
        _sort_by_assigned_chnum(channels, chnum_map)

    multi_country_map = _source_multi_country_map(channels)
    lines = ['#EXTM3U']
    for ch in channels:
        tvg_id = _tvg_id(ch)
        display_name = _channel_display_name(ch, multi_country_map)
        # Guide routing: tvc-guide-stationid → Gracenote, tvg-id → our XMLTV.
        guide_attr = (f'tvc-guide-stationid="{_parse_gracenote_id(ch)}"'
                      if gracenote else f'tvg-id="{tvg_id}"')
        attrs = [
            f'channel-id="{tvg_id}"',
            guide_attr,
            f'tvg-name="{_esc(display_name)}"',
            f'group-title="{_esc(ch.category or ch.source.display_name)}"',
        ]
        if ch.logo_url:
            attrs.append(f'tvg-logo="{proxy_logo_url(ch.logo_url, base_url, image_proxy_enabled=_image_proxy) or ch.logo_url}"')
        chnum = chnum_map.get(ch.id)
        if chnum:
            attrs.append(f'tvg-chno="{chnum}"')
        if ch.description:
            attrs.append(f'tvg-description="{_esc(ch.description)}"')
            attrs.append(f'tvc-guide-description="{_esc(ch.description)}"')
        guide_cat = _tvc_guide_category(ch)
        if guide_cat:
            attrs.append(f'tvc-guide-categories="{guide_cat}"')
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{_sanitize(display_name)}')
        if _needs_prismcast_bridge(ch) and _prismcast_capturable(ch):
            lines.append(_prismcast_bridge_url(ch, prismcast_url, inner_base_url))
        else:
            # Non-DRM → direct play proxy, same URL the standard M3U emits.
            lines.append(_channel_play_url(ch, base_url))

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
    'movie':      'Movie',
    'movies':     'Movie',
    'sports':     'Sports event',
    'sport':      'Sports event',
    'series':     'Series',
    'tv shows':   'Series',
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

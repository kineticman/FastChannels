"""Channel numbering as of 0fda0c8, frozen for the one-time seed of channel_numbers.

The seed (app.chnum.seed_channel_numbers) stores the numbers every playlist
serves right now, and those come from these functions: legacy_refresh() is
the settle step the old code ran before every build, and _resolve_app_chnum_map
is how the old generators numbered a playlist. Nothing else may use this
module; it goes once every install has been seeded.
"""
import logging
from datetime import datetime, timedelta, timezone

from .channel_tags import AUTO_EVENT_REASONS, EVENT_RETIRED_REASON
from .extensions import db
from .generators.m3u import (
    _provider_number_sources, _selected_channel_stubs, feed_to_query_filters,
)
from .models import AppSettings, Channel, Feed, FeedChannelNumber, Source

logger = logging.getLogger(__name__)

NUMBER_HOLD_DAYS = 7
_CHNUM_NAMESPACE_BLOCK = 10000
_FEED_NAMESPACE_BASE = 200000
_UNCONFIGURED_CHNUM_FALLBACK_START = 5000


def _selected_channels(filters, *, gracenote=False):
    from .generators.m3u import _selected_channels as selected
    return selected(filters, gracenote=gracenote)


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


def _held_number_query(columns):
    """Channels out of output that still hold their number for a while.

    A channel that drops out because its source stopped listing it (a short
    page, a free preview ending, a flap) usually comes back within days, and
    its number should be waiting. Allocators treat these numbers as taken until
    NUMBER_HOLD_DAYS after it went inactive. A channel the user turned off holds
    nothing, and neither does a finished event: neither is expected back.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=NUMBER_HOLD_DAYS)
    return (
        db.session.query(*columns)
        .select_from(Channel)
        .join(Source)
        .filter(
            Channel.is_active == False,
            Channel.went_inactive_at.isnot(None),
            Channel.went_inactive_at >= cutoff,
            Channel.number.isnot(None),
            Channel.stream_url != None,
            Source.is_enabled == True,
            Source.epg_only == False,
            db.or_(
                Channel.is_enabled == True,
                Channel.disable_reason.in_(
                    [r for r in AUTO_EVENT_REASONS if r != EVENT_RETIRED_REASON]),
            ),
        )
    )


def held_channel_ids() -> set[int]:
    return {cid for (cid,) in _held_number_query([Channel.id]).all()}


def _held_numbers(exclude: set | None = None) -> set[int]:
    """Numbers held for channels out of output (see _held_number_query), less
    any in `exclude` -- numbers channels in output already have, which keep
    them: a hold never takes a number away from a channel that is showing."""
    held = {n for (n,) in _held_number_query([Channel.number]).all()}
    return held - exclude if exclude else held


def _locked_numbers() -> set[int]:
    """Every whole-number lock, whether or not its channel is in output.

    Unlike a hold, a lock wins: the master allocators treat these as taken
    even for a channel already on that number, so the locked channel never
    shares it -- including across the standard / Gracenote split, where the
    lock and the holder are numbered in different passes. (Turning a locked
    channel off keeps its number free for its return, too.)"""
    return {n for (n,) in db.session.query(Channel.number)
            .filter(Channel.number_pinned == True, Channel.number.isnot(None)).all()}


def _reserved_provider_numbers() -> set[int]:
    """Integer provider numbers of every active, enabled, unpinned channel in an
    opted-in source. Allocators treat these as taken. Sub-channels ("305.1")
    can't collide with an integer tvg-chno, so only whole numbers are reserved."""
    sources = _provider_number_sources()
    if not sources:
        return set()
    rows = (
        db.session.query(Channel.provider_number)
        .join(Source)
        .filter(
            Source.name.in_(sources),
            Channel.is_active == True,
            Channel.is_enabled == True,
            Channel.provider_number.isnot(None),
            db.or_(Channel.number_pinned == False, Channel.number_pinned == None),
            Channel.pinned_chno.is_(None),
        )
        .all()
    )
    return {int(pn) for (pn,) in rows if pn and pn.strip().isdigit()}


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


def _build_source_chnum_map(channels, reserved: set[int] | None = None):
    """
    Build a channel-number assignment map using Source.chnum_start values.

    Channels from sources with chnum_start configured are renumbered sequentially
    starting from that value.  Channels from sources without chnum_start fall back
    to their existing Channel.number (unchanged from scraper output).

    `reserved` numbers are never handed to a channel that doesn't already
    have them (the master allocator passes the Gracenote channels' numbers,
    numbered separately); a channel here that has one keeps it.

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
    own_numbers = {ch.number for ch in channels}
    used_numbers: set[int] = (set(pinned_numbers) | _reserved_provider_numbers() | _locked_numbers()
                              | _held_numbers(own_numbers) | ((reserved or set()) - own_numbers))
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

    # Two passes: every source keeps its existing numbers first, then the
    # channels without one draw new numbers. Doing both per source let a new
    # channel in an earlier source take a number a later source's channel
    # already had, moving that channel.
    unassigned_by_source: dict[str, list] = {}
    for src in ordered_sources:
        chs = by_source[src]
        if src in source_starts:
            min_value = source_starts[src]
        elif global_cursor is not None:
            min_value = global_start
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
            min_value = None
        unassigned_by_source[src] = _partition_unassigned(chs, min_value, used_numbers, chnum_map)

    for src in ordered_sources:
        unassigned = unassigned_by_source[src]
        if src in source_starts:
            _assign_sequential(unassigned, source_starts[src], used_numbers, chnum_map)
        elif global_cursor is not None:
            global_cursor = _assign_sequential(unassigned, global_cursor, used_numbers, chnum_map)
        else:
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
    reserved = _reserved_provider_numbers()

    # First pass: pinned channels claim their numbers before anything else, so
    # a lock always beats another channel's stored number regardless of list
    # order (otherwise an earlier channel's sticky number silently wins and the
    # lock is ignored).
    for ch in channels:
        if getattr(ch, 'number_pinned', False) and ch.number is not None and ch.number not in used_numbers:
            result[ch.id] = ch.number
            used_numbers.add(ch.number)

    # Then preserve valid stored assignments.
    for ch in channels:
        if ch.id in result:
            continue
        stored = stored_numbers.get(ch.id) if stored_numbers else None
        if (stored is not None and stored >= feed_chnum_start
                and stored not in used_numbers and stored not in reserved):
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

    # Locks are channel-wide, so a locked channel outside this batch (turned
    # off, or in the other partition) keeps its number free for its return.
    # Added after the stored pass: a member already on that number keeps it.
    used_numbers |= _locked_numbers()

    # Second pass: assign fresh sequential numbers to new/displaced channels.
    used_numbers |= reserved
    _assign_sequential(unassigned, feed_chnum_start, used_numbers, result)

    return result


def _build_sticky_gn_chnum_map(gn_channels, gn_start: int, used_numbers: set) -> dict:
    """
    Assign channel numbers to Gracenote channels, with the same stickiness
    guarantee as standard channels: existing Channel.number is kept if it's not
    already taken. New/displaced channels fill in sequentially from gn_start.

    Kept numbers are not floored at gn_start: gn_start is just above the
    highest standard number, so it moves whenever a channel loses or gains a
    Gracenote ID (a channel keeps its number across that), and a gn_start
    floor then renumbered every Gracenote channel below the new start.
    """
    result = {}
    unassigned = []
    used_numbers |= (_reserved_provider_numbers() | _locked_numbers()
                     | _held_numbers({ch.number for ch in gn_channels}))
    # A kept number still has to clear the floor _build_source_chnum_map
    # applies (the source's chnum_start, else the global start), or the
    # mixed playlists, which number both groups through that function,
    # would show a different number than the one stored.
    try:
        global_start = AppSettings.get().effective_global_chnum_start()
    except Exception:
        global_start = None
    sorted_channels = sorted(
        gn_channels,
        key=lambda c: (c.number is None, c.number or 0, (c.name or '').lower()),
    )
    for ch in sorted_channels:
        if getattr(ch, 'number_pinned', False) and ch.number is not None:
            result[ch.id] = ch.number
            used_numbers.add(ch.number)
            continue
        floor = ch.source.chnum_start or global_start
        if _is_usable_number(ch, ch.number, min_value=floor) and ch.number not in used_numbers:
            result[ch.id] = ch.number
            used_numbers.add(ch.number)
        else:
            unassigned.append(ch)
    _assign_sequential(unassigned, gn_start, used_numbers, result)
    return result


def _resolve_app_chnum_map(channels, *, feed_chnum_start: int = None,
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


def _release_claimed_numbers(live_channels, live_ids: set[int], held_ids: set[int]) -> None:
    """Clear Channel.number on channels out of output whose number is taken.

    A channel out of output keeps its number only while nobody else has it.
    Once a channel in output holds that number, the absent one lets go, so
    coming back it gets a fresh number instead of taking this one back (ties
    between two channels with the same number are broken by name, so the one
    returning could otherwise win and move the incumbent). Absent channels
    sharing a number with each other keep it on one: a held channel first,
    then the most recently gone. Locked numbers are never cleared.
    """
    claimed = {ch.number for ch in live_channels if ch.number is not None}
    claimed |= _reserved_provider_numbers()
    absent = (
        Channel.query
        .filter(
            Channel.number.isnot(None),
            db.or_(Channel.number_pinned == False, Channel.number_pinned == None),
            db.or_(Channel.pinned_chno.is_(None), Channel.pinned_chno == ''),
        )
        .all()
    )
    absent = [ch for ch in absent if ch.id not in live_ids]
    absent.sort(key=lambda ch: (
        ch.id not in held_ids,
        -(ch.went_inactive_at.timestamp() if ch.went_inactive_at else 0),
        ch.id,
    ))
    released = 0
    for ch in absent:
        if ch.number in claimed:
            ch.number = None
            released += 1
        else:
            claimed.add(ch.number)
    if released:
        logger.info('[chnum] released %d number(s) held by channels out of output '
                    'and now used by another channel', released)


def legacy_refresh() -> None:
    """Assign stable automatic numbers to non-pinned active channels.

    Channel.number is system-managed unless the user pins it. Scraper-supplied
    numbers are ignored; this allocator preserves existing non-pinned numbers
    where possible and only fills gaps for channels that are new or invalid.

    Standard and Gracenote channels share a single contiguous block for the
    master (source-based) numbering: standard channels first, then new
    Gracenote channels starting immediately after the highest standard number.
    A channel that gains or loses a Gracenote ID keeps its number, so the two
    groups can interleave; neither pass hands out a number the other holds.

    For feeds with an explicit chnum_start, all channels (standard and Gracenote)
    share one unified number pool persisted in FeedChannelNumber.  A channel keeps
    its assigned number when its Gracenote status changes — it just moves between
    the standard and Gracenote M3U files without being renumbered.
    """

    with db.session.no_autoflush:
        all_channels = (
            Channel.query
            .join(Source)
            .filter(
                Channel.is_active == True,
                Channel.is_enabled == True,
                Source.is_enabled == True,
                Source.epg_only == False,
                Channel.stream_url != None,
            )
            .all()
        )
    if not all_channels:
        return

    std_channels = [ch for ch in all_channels if not (ch.gracenote_id or '').strip()]
    gn_channels  = [ch for ch in all_channels if (ch.gracenote_id or '').strip()]

    gn_numbers = {ch.number for ch in gn_channels if ch.number is not None}
    std_map, _ = _build_source_chnum_map(std_channels, gn_numbers) if std_channels else ({}, [])

    gn_map = {}
    if gn_channels:
        gn_start = (max(std_map.values()) + 1) if std_map else (AppSettings.get().effective_global_chnum_start() or 1)
        gn_map = _build_sticky_gn_chnum_map(gn_channels, gn_start, set(std_map.values()))

    chnum_map = {**std_map, **gn_map}
    for ch in all_channels:
        if ch.number_pinned:
            continue
        next_number = chnum_map.get(ch.id)
        if ch.number != next_number:
            ch.number = next_number

    all_channel_ids = {ch.id for ch in all_channels}
    held_ids = held_channel_ids()
    _release_claimed_numbers(all_channels, all_channel_ids, held_ids)

    # Persist feed-specific channel numbers for feeds with explicit chnum_start.
    feeds_with_chnum = Feed.query.filter(
        Feed.chnum_start != None,
        Feed.is_enabled == True,
    ).all()
    for feed in feeds_with_chnum:
        filters = feed_to_query_filters(feed.filters or {})
        with db.session.no_autoflush:
            std_stubs = _selected_channel_stubs(filters, gracenote=False)
            gn_stubs  = _selected_channel_stubs(filters, gracenote=True)
        # Sort the combined list by master number so GN and non-GN interleave
        # by position rather than arriving as two separate blocks.
        all_stubs = sorted(
            std_stubs + gn_stubs,
            key=lambda ch: (ch.number is None, ch.number or 0, (ch.name or '').lower()),
        )
        feed_channel_ids = {s.id for s in all_stubs}

        # Load existing stored numbers for stickiness.
        stored = {
            fcn.channel_id: fcn.number
            for fcn in FeedChannelNumber.query.filter_by(feed_id=feed.id).all()
        }
        new_map   = _build_feed_chnum_map(all_stubs, feed.chnum_start, stored_numbers=stored)

        # Upsert new assignments.
        existing_fcn = {fcn.channel_id: fcn for fcn in FeedChannelNumber.query.filter_by(feed_id=feed.id).all()}
        for channel_id, number in new_map.items():
            if channel_id in existing_fcn:
                if existing_fcn[channel_id].number != number:
                    existing_fcn[channel_id].number = number
            else:
                db.session.add(FeedChannelNumber(feed_id=feed.id, channel_id=channel_id, number=number))

        # Remove stale rows for channels no longer in this feed. A held
        # channel's row stays so it gets its number back on return (it's
        # reserved meanwhile as a number outside the batch), unless a member
        # has the same number: the member keeps it, the absent channel lets go.
        member_numbers = set(new_map.values())
        for channel_id, fcn in existing_fcn.items():
            if channel_id in feed_channel_ids:
                continue
            if channel_id in held_ids and fcn.number not in member_numbers:
                continue
            db.session.delete(fcn)

"""Channel numbers (tvg-chno): assigned once, read everywhere.

channel_numbers is the only record of a channel's number. Each row belongs to
a scope:

  MASTER_SCOPE (0)   the main numbers: the Default feed and every master
                     playlist (/m3u, /m3u/gracenote, /m3u/mixed and the
                     master bridge playlists), one number per channel
  a feed id          that feed's standard, Gracenote and mixed playlists

Until 5.10 the Default feed and the master playlists were numbered apart
(and /m3u/gracenote and the master Gracenote bridge playlists apart again,
in scopes -1 and -2), so one channel could show three or four numbers.
unify_main_numbers() made them one, once, keeping the Default feed's numbers;
the old -1/-2/Default rows are left in place, unread, for a downgrade.

settle() is the one allocator. It runs before every artifact build and after
a scrape; playlists, admin pages and APIs only read. Its rules, in order:

  1. Locks. A channel locked to a number (Channel.number_pinned) takes it in
     every scope it is in, moving whoever had it there. In the main scope
     a lock also wins while its channel is off; in a feed, the lock of a
     channel that is not in the feed only keeps new channels off the number.
  2. Keep. A member with a row keeps it, unless a lock or a provider number
     (DirecTV channel numbers) claims it, or it is below its floor (the
     scope's start changed).
  3. Hold. A channel that went inactive keeps its row, released, for
     NUMBER_HOLD_DAYS and gets it back on return. A channel the user turned
     off and a finished event hold nothing.
  4. New. The lowest number at or above the start that no row, lock or
     provider number has.

A Gracenote ID change moves no one: every scope holds both kinds of channel.
Provider numbers and decimal locks stay output overlays
(m3u.apply_provider_numbers).

Channel.number is the user's lock while number_pinned is set. Otherwise
settle() writes the main number there as a copy for pages and sorting;
nothing here reads it.
"""
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, insert, update, bindparam

from .channel_tags import AUTO_EVENT_REASONS, EVENT_RETIRED_REASON
from .extensions import db
from .models import AppSettings, Channel, ChannelNumber, Feed, FeedChannelNumber, Source

logger = logging.getLogger(__name__)

MASTER_SCOPE = 0
# Retired (see the module docstring); rows there are no longer read or written.
MASTER_GRACENOTE_SCOPE = -1
MASTER_BRIDGE_GRACENOTE_SCOPE = -2
NUMBER_HOLD_DAYS = 7
# New main numbers start here when the Default feed has no start.
UNCONFIGURED_START = 5000

_BATCH = 500


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def is_seeded() -> bool:
    try:
        return AppSettings.get().chnum_seeded_at is not None
    except Exception:  # noqa: BLE001 -- a pre-upgrade schema has no column yet
        db.session.rollback()
        return False


def is_unified() -> bool:
    try:
        return AppSettings.get().chnum_unified_at is not None
    except Exception:  # noqa: BLE001 -- a pre-upgrade schema has no column yet
        db.session.rollback()
        return False


def _default_feed():
    return Feed.query.filter_by(slug='default').first()


def feed_scope(feed) -> int:
    """The scope a feed's playlists read: the main numbers for the Default
    feed, else the feed's own. `feed` is a Feed or a feed id."""
    feed_id = feed if isinstance(feed, int) else feed.id
    slug = None if isinstance(feed, int) else feed.slug
    if slug is None:
        slug = db.session.query(Feed.slug).filter(Feed.id == feed_id).scalar()
    if slug == 'default' and is_unified():
        return MASTER_SCOPE
    return feed_id


def main_start(settings=None):
    """Where the main numbers start: the Default feed's start, else the saved
    master start (None: keep any number, new ones from UNCONFIGURED_START)."""
    default = _default_feed()
    if default is not None and default.chnum_start is not None:
        return default.chnum_start
    return (settings or AppSettings.get()).master_chnum_start


# ------------------------------------------------------------------ reads
def read_scope(scope_id: int) -> dict[int, int]:
    """{channel_id: number} for the channels in a scope (held rows excluded)."""
    if not is_seeded():
        return _legacy_read(scope_id)
    rows = (db.session.query(ChannelNumber.channel_id, ChannelNumber.number)
            .filter(ChannelNumber.scope_id == scope_id, ChannelNumber.released_at.is_(None))
            .all())
    return dict(rows)


def _legacy_read(scope_id: int) -> dict[int, int]:
    # Only until the seed has run (it runs at boot; this covers a failed one):
    # the previous stores still hold the numbers being served.
    if scope_id > 0:
        return dict(db.session.query(FeedChannelNumber.channel_id, FeedChannelNumber.number)
                    .filter(FeedChannelNumber.feed_id == scope_id).all())
    return dict(db.session.query(Channel.id, Channel.number)
                .filter(Channel.number.isnot(None)).all())


def held_channel_ids() -> set[int]:
    """Channels out of output holding a number for their return."""
    return {cid for (cid,) in db.session.query(ChannelNumber.channel_id)
            .filter(ChannelNumber.released_at.isnot(None)).distinct().all()}


def lock_map() -> dict[int, int]:
    """{channel_id: number} for every whole-number lock, on or off."""
    return dict(db.session.query(Channel.id, Channel.number)
                .filter(Channel.number_pinned == True, Channel.number.isnot(None)).all())


# ------------------------------------------------------------------ allocation
def _holdable() -> dict[int, datetime]:
    """{channel_id: went_inactive_at} for channels whose rows are held: gone
    inactive within NUMBER_HOLD_DAYS, and not turned off by the user or a
    finished event (neither is expected back)."""
    cutoff = _now() - timedelta(days=NUMBER_HOLD_DAYS)
    rows = (
        db.session.query(Channel.id, Channel.went_inactive_at)
        .join(Source)
        .filter(
            Channel.is_active == False,
            Channel.went_inactive_at.isnot(None),
            Channel.went_inactive_at >= cutoff,
            Channel.stream_url != None,
            Source.is_enabled == True,
            Source.epg_only == False,
            db.or_(
                Channel.is_enabled == True,
                Channel.disable_reason.in_(
                    [r for r in AUTO_EVENT_REASONS if r != EVENT_RETIRED_REASON]),
            ),
        )
        .all()
    )
    return {cid: (wia.replace(tzinfo=None) if wia.tzinfo else wia) for cid, wia in rows}


def allocate(members, existing: dict[int, tuple[int, datetime | None]], *,
             floor, new_floor, order_key, locks: dict[int, int], provider: set[int],
             holdable: dict[int, datetime], absent_locks_win: bool,
             avoid: set[int] = frozenset(),
             refuse=lambda m, n: False) -> dict[int, tuple[int, datetime | None]]:
    """The numbering rules (see the module docstring), as a pure function.

    members    channels in the scope (need .id; floor/new_floor/order_key see them)
    existing   the scope's rows, {channel_id: (number, released_at)}
    floor      member -> lowest number it may keep, or None
    new_floor  member -> where its search for a new number starts
    avoid      numbers new channels must not get (another scope's, see settle)
    refuse     (member, number) -> True when that member may not keep that number

    Returns the scope's rows after settling, {channel_id: (number, released_at)}.
    """
    final: dict[int, tuple[int, datetime | None]] = {}
    taken: set[int] = set()
    member_ids = {m.id for m in members}

    for m in sorted(members, key=lambda m: m.id):
        n = locks.get(m.id)
        if n is not None and n not in taken:
            final[m.id] = (n, None)
            taken.add(n)

    blocked = set(locks.values()) if absent_locks_win else set()
    for m in members:
        row = existing.get(m.id)
        if m.id in final or row is None:
            continue
        n = row[0]
        f = floor(m)
        if (n in taken or n in provider or n in blocked or (f is not None and n < f)
                or refuse(m, n)):
            continue
        final[m.id] = (n, None)
        taken.add(n)

    for cid, (n, released_at) in existing.items():
        if cid in member_ids or cid not in holdable:
            continue
        if n in taken or n in provider or n in blocked:
            continue
        final[cid] = (n, released_at or holdable[cid])
        taken.add(n)

    used = taken | set(locks.values()) | provider | set(avoid)
    cursors: dict[int, int] = {}
    for m in sorted((m for m in members if m.id not in final), key=order_key):
        start = new_floor(m)
        n = cursors.get(start, start)
        while n in used:
            n += 1
        final[m.id] = (n, None)
        used.add(n)
        cursors[start] = n + 1
    return final


def _existing(scope_id: int) -> dict[int, tuple[int, datetime | None]]:
    return {cid: (n, rel) for cid, n, rel in db.session.query(
        ChannelNumber.channel_id, ChannelNumber.number, ChannelNumber.released_at,
    ).filter(ChannelNumber.scope_id == scope_id).all()}


def _write(scope_id: int, existing: dict, final: dict) -> int:
    """Apply a scope's settled rows: delete what changed, then insert, so the
    unique (scope, number) index never sees two rows on one number."""
    table = ChannelNumber.__table__
    changed = [cid for cid, row in existing.items()
               if cid not in final or final[cid][0] != row[0]
               or (final[cid][1] is None) != (row[1] is None)]
    added = [cid for cid in final if cid not in existing or cid in changed]
    for i in range(0, len(changed), _BATCH):
        db.session.execute(delete(table).where(
            table.c.scope_id == scope_id, table.c.channel_id.in_(changed[i:i + _BATCH])))
    if added:
        db.session.execute(insert(table), [
            {'scope_id': scope_id, 'channel_id': cid,
             'number': final[cid][0], 'released_at': final[cid][1]} for cid in added
        ])
    return len(changed) + len(added)


def _source_label(m) -> str:
    return (m.source.display_name or m.source.name or '').lower()


def source_rank(source_order):
    """Sort key for a feed's source order: listed sources first, in the
    listed order, then every other source together (0 for all without one)."""
    ranks = {name: i for i, name in enumerate(source_order or [])}
    rest = len(ranks)
    return lambda m: ranks.get(getattr(m.source, 'name', None), rest)


def feed_order_key(source_order, main_number):
    """Order a feed numbers new channels in. With a source order: those
    sources first, then the rest by source name, channel name within each.
    Without one: the main numbers' order. `main_number(m)` is m's main number."""
    if source_order:
        rank = source_rank(source_order)
        return lambda m: (rank(m), _source_label(m), (m.name or '').lower(), m.id)
    return lambda m: (main_number(m), (m.name or '').lower(), m.id)


def settle() -> None:
    """Settle every scope's numbers. Flushes; the caller commits."""
    from .generators.m3u import (
        _reserved_provider_numbers, _selected_channel_stubs, feed_to_query_filters,
    )
    if not is_seeded() and not seed_channel_numbers():
        logger.error('[chnum] channel numbers are not seeded; not settling')
        return
    if not is_unified() and not unify_main_numbers():
        logger.error('[chnum] main channel numbers are not unified; not settling')
        return
    db.session.flush()
    # SQLite's legacy transaction mode does not begin a transaction for a
    # SELECT. Take its writer lock before reading the allocation inputs, or a
    # scrape can commit new assignments between our read and _write(). A no-op
    # UPDATE also works when the caller already has a write transaction, and
    # leaves commit/rollback (and any pending channel edits) with the caller.
    table = AppSettings.__table__
    db.session.execute(update(table).where(table.c.id == 1).values(id=table.c.id))
    settings = AppSettings.get()
    locks = lock_map()
    provider = _reserved_provider_numbers()
    holdable = _holdable()
    common = dict(locks=locks, provider=provider, holdable=holdable)

    # The main numbers: the Default feed and every master playlist. One
    # number per channel, so playlists a DVR takes together (standard,
    # Gracenote, bridge) never repeat one.
    live = _selected_channel_stubs({}, gracenote=None)
    start = main_start(settings)
    existing = _existing(MASTER_SCOPE)
    default = _default_feed()
    main_rank = source_rank(default.source_order if default is not None else None)
    master = allocate(
        live, existing, floor=lambda m: start,
        new_floor=lambda m: start or UNCONFIGURED_START,
        order_key=lambda m: (main_rank(m), _source_label(m), (m.name or '').lower(),
                             m.source_channel_id or ''),
        absent_locks_win=True, **common)
    writes = _write(MASTER_SCOPE, existing, master)
    master_live = {cid: n for cid, (n, rel) in master.items() if rel is None}
    main_number = lambda m: master_live.get(m.id, float('inf'))  # noqa: E731

    feeds = Feed.query.filter(Feed.is_enabled == True, Feed.slug != 'default').all()
    for feed in feeds:
        if feed.chnum_start is None:
            logger.warning('[chnum] feed %s has no channel number start; not numbered', feed.slug)
            continue
        filters = feed_to_query_filters(feed.filters or {})
        members = (_selected_channel_stubs(filters, gracenote=False)
                   + _selected_channel_stubs(filters, gracenote=True))
        existing = _existing(feed.id)
        rows = allocate(
            members, existing, floor=lambda m, s=feed.chnum_start: s,
            new_floor=lambda m, s=feed.chnum_start: s,
            order_key=feed_order_key(feed.source_order, main_number),
            absent_locks_win=False, **common)
        writes += _write(feed.id, existing, rows)

    # Rows of deleted feeds.
    feed_ids = {f.id for f in Feed.query.with_entities(Feed.id).all()}
    stale = [s for (s,) in db.session.query(ChannelNumber.scope_id).distinct().all()
             if s > 0 and s not in feed_ids]
    if stale:
        db.session.execute(delete(ChannelNumber.__table__).where(
            ChannelNumber.__table__.c.scope_id.in_(stale)))

    _mirror_master_numbers({cid: n for cid, (n, _) in master.items()})
    db.session.flush()
    if writes:
        logger.info('[chnum] settled %d row change(s)', writes)


def _mirror_master_numbers(master: dict[int, int]) -> None:
    """Copy master numbers into Channel.number for unlocked channels."""
    current = dict(db.session.query(Channel.id, Channel.number).filter(
        db.or_(Channel.number_pinned == False, Channel.number_pinned == None)).all())
    changes = [{'cid': cid, 'n': master.get(cid)} for cid, n in current.items()
               if master.get(cid) != n]
    if changes:
        table = Channel.__table__
        db.session.execute(
            update(table).where(table.c.id == bindparam('cid')).values(number=bindparam('n')),
            changes,
        )
        db.session.expire_all()


def plan_numbers(members, start: int, existing: dict | None = None,
                 source_order=None) -> dict[int, int]:
    """Numbers an unsaved feed (or a saved one with edited filters or start)
    would get, by the same rules, without storing anything. `existing` is the
    feed's rows when it has some."""
    from .generators.m3u import _reserved_provider_numbers
    existing = existing or {}
    master = read_scope(MASTER_SCOPE)
    rows = allocate(
        members, existing, floor=lambda m: start, new_floor=lambda m: start,
        order_key=feed_order_key(source_order, lambda m: master.get(m.id, float('inf'))),
        locks=lock_map(), provider=_reserved_provider_numbers(),
        holdable={cid: rel for cid, (_, rel) in existing.items() if rel is not None},
        absent_locks_win=False)
    return {cid: n for cid, (n, rel) in rows.items() if rel is None}


def scope_rows(scope_id: int) -> dict[int, tuple[int, datetime | None]]:
    """A scope's rows, released ones included: {channel_id: (number, released_at)}."""
    return _existing(scope_id)


# ------------------------------------------------------------------ edits
def clear_rows(scope_id: int, channel_ids=None) -> None:
    """Drop a scope's rows (or some channels' rows in it); the next settle
    numbers those channels afresh."""
    table = ChannelNumber.__table__
    stmt = delete(table).where(table.c.scope_id == scope_id)
    if channel_ids is None:
        db.session.execute(stmt)
        return
    ids = list(channel_ids)
    for i in range(0, len(ids), _BATCH):
        db.session.execute(stmt.where(table.c.channel_id.in_(ids[i:i + _BATCH])))


def delete_channel_rows(channel_ids) -> None:
    """Drop every row of channels about to be deleted (bulk deletes skip the
    ON DELETE CASCADE path when foreign keys are off on a connection)."""
    table = ChannelNumber.__table__
    ids = list(channel_ids)
    for i in range(0, len(ids), _BATCH):
        db.session.execute(delete(table).where(table.c.channel_id.in_(ids[i:i + _BATCH])))


def replace_rows(scope_id: int, numbers: dict[int, int]) -> None:
    """Set these channels' numbers in a scope in one go (a manual reorder)."""
    clear_rows(scope_id, numbers.keys())
    db.session.flush()
    if numbers:
        db.session.execute(insert(ChannelNumber.__table__), [
            {'scope_id': scope_id, 'channel_id': cid, 'number': n, 'released_at': None}
            for cid, n in numbers.items()
        ])


def move_rows(old_id: int, new_id: int) -> None:
    """Give new_id every row old_id has (a channel switched to another source)."""
    table = ChannelNumber.__table__
    scopes = [s for (s,) in db.session.query(ChannelNumber.scope_id)
              .filter(ChannelNumber.channel_id == old_id).all()]
    if scopes:
        db.session.execute(delete(table).where(
            table.c.channel_id == new_id, table.c.scope_id.in_(scopes)))
        db.session.execute(update(table).where(table.c.channel_id == old_id)
                           .values(channel_id=new_id, released_at=None))


# ------------------------------------------------------------------ seed
def _bridge_gracenote_selections():
    """The channels of the master PrismCast and FastChannels Player Gracenote
    playlists, in playlist order (as generate_prismcast_m3u and
    generate_fc_player_m3u select them with gracenote=True)."""
    from .drm_bridge import DRM_BRIDGE_TRUSTED_SOURCES
    from .generators.m3u import (
        _build_channel_query, _parse_gracenote_id, _prismcast_capturable,
    )
    bridge = [ch for ch in _build_channel_query({}, activity='drm_bridge').all()
              if _parse_gracenote_id(ch)]
    return (
        [ch for ch in bridge if _prismcast_capturable(ch)],
        [ch for ch in bridge if (ch.source.name if ch.source else None) in DRM_BRIDGE_TRUSTED_SOURCES],
    )


def seed_channel_numbers() -> bool:
    """Fill channel_numbers once from what the previous code serves, so the
    switch moves no number in any playlist. Returns True once seeded."""
    settings = AppSettings.get()
    if settings.chnum_seeded_at is not None:
        return True
    from . import chnum_seed as legacy
    from .generators.m3u import _selected_channel_stubs, feed_to_query_filters
    try:
        rows: dict[int, dict[int, tuple[int, datetime | None]]] = {}
        used: dict[int, set[int]] = {}
        skipped: list[str] = []

        def put(scope_id, cid, n, released_at=None):
            scope = rows.setdefault(scope_id, {})
            if n is None or cid in scope:
                return
            if n in used.setdefault(scope_id, set()):
                skipped.append(f'{scope_id}:{cid}={n}')
                return
            scope[cid] = (int(n), released_at)
            used[scope_id].add(int(n))

        # Read before any feed is given a start below: the master start is
        # the Default feed's, and positional blocks depend on the feeds.
        master_floor = settings.effective_global_chnum_start()
        default_feed = Feed.query.filter_by(slug='default').first()
        feeds = Feed.query.order_by(Feed.slug).all()

        legacy.legacy_refresh()   # the settle step the old code ran before a build
        db.session.flush()
        positional = {
            f.id: (legacy.feed_namespace_start(f, gracenote=False),
                   legacy.feed_gracenote_start(f))
            for f in feeds if f.is_enabled and f.chnum_start is None
        }
        gn_start = (legacy.feed_gracenote_start(default_feed) if default_feed
                    else 100000)
        held = legacy.held_channel_ids()
        went = dict(db.session.query(Channel.id, Channel.went_inactive_at)
                    .filter(Channel.went_inactive_at.isnot(None)).all())
        went = {cid: (w.replace(tzinfo=None) if w.tzinfo else w) for cid, w in went.items()}
        numbers = dict(db.session.query(Channel.id, Channel.number)
                       .filter(Channel.number.isnot(None)).all())

        # Master: the standard and mixed playlists, then the rest of what is
        # in output (bridge-only channels), then held channels.
        for gracenote in (False, 'mixed'):
            nums, _ = legacy._resolve_app_chnum_map(legacy._selected_channels({}, gracenote=gracenote))
            for cid, n in nums.items():
                put(MASTER_SCOPE, cid, n)
        for m in _selected_channel_stubs({}, gracenote=None):
            put(MASTER_SCOPE, m.id, numbers.get(m.id))
        for cid in held:
            put(MASTER_SCOPE, cid, numbers.get(cid), went.get(cid) or _now())

        nums, _ = legacy._resolve_app_chnum_map(legacy._selected_channels({}, gracenote=True),
                                                namespace_start=gn_start)
        for cid, n in nums.items():
            put(MASTER_GRACENOTE_SCOPE, cid, n)
        # The Gracenote bridge playlists numbered their own channels by
        # position from the same start: PrismCast's, then the Player's.
        for channels in _bridge_gracenote_selections():
            nums, _ = legacy._resolve_app_chnum_map(channels, namespace_start=gn_start)
            for cid, n in nums.items():
                put(MASTER_BRIDGE_GRACENOTE_SCOPE, cid, n)

        for feed in feeds:
            filters = feed_to_query_filters(feed.filters or {})
            if feed.chnum_start is None:
                if feed.id in positional:
                    std_start, feed_gn_start = positional[feed.id]
                    for gracenote, ns in ((False, std_start), (True, feed_gn_start)):
                        nums, _ = legacy._resolve_app_chnum_map(
                            legacy._selected_channels(filters, gracenote=gracenote), namespace_start=ns)
                        for cid, n in nums.items():
                            put(feed.id, cid, n)
                    feed.chnum_start = min([std_start, *used.get(feed.id, set())])
                else:
                    feed.chnum_start = legacy.feed_namespace_start(feed, gracenote=False)
                continue
            stored = {r.channel_id: r.number for r in
                      FeedChannelNumber.query.filter_by(feed_id=feed.id).all()}
            if feed.is_enabled:
                for gracenote in (False, True):
                    nums, _ = legacy._resolve_app_chnum_map(
                        legacy._selected_channels(filters, gracenote=gracenote),
                        feed_chnum_start=feed.chnum_start, feed_id=feed.id)
                    for cid, n in nums.items():
                        put(feed.id, cid, n)
                members = {m.id for g in (False, True)
                           for m in _selected_channel_stubs(filters, gracenote=g)}
                for cid, n in stored.items():
                    if cid in members:
                        put(feed.id, cid, n)
                    elif cid in held:
                        put(feed.id, cid, n, went.get(cid) or _now())
            else:
                for cid, n in stored.items():
                    put(feed.id, cid, n)

        db.session.execute(delete(ChannelNumber.__table__))
        for scope_id, scope in rows.items():
            _write(scope_id, {}, scope)
        settings.master_chnum_start = master_floor
        settings.master_gracenote_chnum_start = gn_start
        settings.chnum_seeded_at = _now()
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception('[chnum] could not seed channel numbers; serving the previous numbers')
        return False
    total = sum(len(s) for s in rows.values())
    logger.info('[chnum] seeded %d channel number(s) in %d scope(s)', total, len(rows))
    if skipped:
        logger.warning('[chnum] seed: %d number(s) already taken in their scope, renumbered '
                       'at the next settle: %s', len(skipped), ', '.join(skipped[:20]))
    return True


def unify_main_numbers() -> bool:
    """Make the Default feed and the master playlists one set of numbers,
    once. The Default feed's numbers win: they are what /feeds/default/*
    (the URLs the app hands out) serves, and they were already one number per
    channel, where /m3u, /m3u/gracenote and the master Gracenote bridge
    playlists each numbered on their own. With the Default feed off, the
    master numbers are what is served, so they stay. The old rows are left,
    unread, so a downgrade still finds them. Returns True once unified."""
    if is_unified():
        return True
    if 'chnum_unified_at' not in AppSettings.__table__.c:
        # A process still running a models.py from before this column (a
        # bind-mounted dev container mid-edit) can't record that the copy
        # ran, so it would copy on every settle. Leave it to a current one.
        return False
    try:
        settings = AppSettings.get()
        default = _default_feed()
        rows = _existing(default.id) if default is not None else {}
        copied = bool(default is not None and default.is_enabled and rows)
        if copied:
            clear_rows(MASTER_SCOPE)
            db.session.flush()
            _write(MASTER_SCOPE, {}, rows)
            if default.chnum_start is not None:
                settings.master_chnum_start = default.chnum_start
        settings.chnum_unified_at = _now()
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception('[chnum] could not unify the main channel numbers')
        return False
    if copied:
        logger.info('[chnum] main numbers now follow the Default feed (%d channel(s)); '
                    '/m3u, /m3u/gracenote and the master bridge playlists may show new numbers',
                    len(rows))
    else:
        logger.info('[chnum] main numbers unified; kept the master numbers (Default feed off or empty)')
    return True

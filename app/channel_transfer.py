"""Hand a channel's place in the lineup over to the same channel on another source.

Switching a channel to a different source (e.g. moving off Roku) should look,
from the client's side, like nothing happened: same channel number in every
feed, same feeds, same guide mapping. A channel's place is spread across
several stores, all keyed by channel id:

  * Channel.pinned_chno / number + number_pinned  -- user locks (channel-wide)
  * FeedChannelNumber                             -- sticky numbers in feeds with a chnum_start
  * Feed.filters id lists                         -- channel_ids / pinned / excluded
  * gracenote_id (manual), category/language overrides

transfer_channel() moves all of them, disables the old channel and enables the
new one, then re-resolves every feed's numbers and reports before/after for
each, so callers (and a dry run) see the real outcome rather than the intent.

It only flushes: the caller commits, or rolls back for a preview. Calling it
again with the channels swapped moves everything back.
"""
import re

from .extensions import db
from .models import Channel, Feed, FeedChannelNumber

_ID_LIST_KEYS = ('channel_ids', 'pinned_channel_ids', 'excluded_channel_ids')
_DECIMAL_RE = re.compile(r'\d{1,5}\.(?!0+$)\d{1,3}')  # same as api_channels._DECIMAL_CHNO_RE


class TransferError(ValueError):
    """The transfer can't be done; message is user-facing."""


def _feed_member_numbers(feeds) -> dict[int, dict[int, int]]:
    from .routes.feeds_api import _displayed_feed_chnum_map
    return {feed.id: _displayed_feed_chnum_map(feed) for feed in feeds}


def shown_number(channel: Channel, app_number):
    """The tvg-chno a playlist actually prints for this channel: a decimal lock
    or an opted-in provider number replaces the app number."""
    from .generators.m3u import apply_provider_numbers
    return apply_provider_numbers([channel], {channel.id: app_number}).get(channel.id, app_number)


def _check_can_enable(new: Channel) -> None:
    from .drm_bridge import drm_bridge_mode_for

    source = new.source
    if source is None or not source.is_enabled:
        raise TransferError(f'"{new.name}" belongs to a source that is turned off.')
    if source.epg_only:
        raise TransferError(f'"{new.name}" comes from a guide-only source and has no stream.')
    if not new.stream_url:
        raise TransferError(f'"{new.name}" has no stream.')
    if not new.is_active:
        raise TransferError(f'"{new.name}" is no longer offered by {source.display_name}.')
    reason = new.disable_reason or ''
    if reason.startswith('DRM'):
        if not drm_bridge_mode_for(source.name):
            raise TransferError(
                f'"{new.name}" needs DRM bridging (PrismCast or FastChannels Player) '
                'to play, and none is set up.'
            )
    elif reason:
        raise TransferError(f'"{new.name}" was turned off as {reason}, so it would not play.')


def _swap_ids(ids: list, old_id: int, new_id: int) -> list[int]:
    out: list[int] = []
    for raw in ids:
        cid = new_id if int(raw) == old_id else int(raw)
        if cid not in out:
            out.append(cid)
    return out


def transfer_channel(old: Channel, new: Channel) -> dict:
    """Move old's lineup position onto new. Flushes only; caller commits."""
    from .worker import _refresh_auto_channel_numbers

    if old.id == new.id:
        raise TransferError('Pick a different channel to switch to.')
    if not old.is_enabled:
        raise TransferError(f'"{old.name}" is not turned on, so there is nothing to move.')
    _check_can_enable(new)

    all_feeds = Feed.query.order_by(Feed.name).all()
    live_feeds = [f for f in all_feeds if f.is_enabled]
    feeds_by_id = {f.id: f for f in live_feeds}
    before_maps = _feed_member_numbers(live_feeds)
    before = {
        fid: shown_number(old, nums[old.id])
        for fid, nums in before_maps.items() if old.id in nums
    }
    # Shown numbers that come from the old source's own numbering (provider
    # numbers) don't travel with the stored rows.
    overlaid = any(str(before[fid]) != str(before_maps[fid][old.id]) for fid in before)
    carried: list[str] = []
    notes: list[str] = []

    # Feed id lists: the new channel takes the old one's place everywhere,
    # including disabled feeds, so re-enabling one later finds it right.
    for feed in all_feeds:
        filters = dict(feed.filters or {})
        touched = False
        for key in _ID_LIST_KEYS:
            ids = filters.get(key) or []
            if any(int(i) == old.id for i in ids):
                filters[key] = _swap_ids(ids, old.id, new.id)
                touched = True
        if touched:
            feed.filters = filters

    # Numbers. A lock moves as-is. Without one, sticky per-feed numbers move
    # row-for-row; positional numbering (feeds without a chnum_start) is only
    # reproducible on another source with a lock, so lock it when the old
    # channel had that one number everywhere, otherwise carry Channel.number
    # as a best effort and let the report show what moved.
    new_rows = {r.feed_id: r for r in FeedChannelNumber.query.filter_by(channel_id=new.id).all()}
    for row in FeedChannelNumber.query.filter_by(channel_id=old.id).all():
        if row.feed_id in new_rows:
            new_rows[row.feed_id].number = row.number
        else:
            db.session.add(FeedChannelNumber(feed_id=row.feed_id, channel_id=new.id, number=row.number))
        db.session.delete(row)

    if (old.pinned_chno or '').strip():
        new.pinned_chno, old.pinned_chno = old.pinned_chno, None
        new.number_pinned = False
        carried.append(f'your number lock on {new.pinned_chno}')
    elif old.number_pinned and old.number is not None:
        new.number, new.number_pinned, new.pinned_chno = old.number, True, None
        old.number_pinned = False
        carried.append(f'your number lock on {new.number}')
    else:
        # Feeds without a chnum_start number by position, and a provider
        # number belongs to the old source; only a lock carries either. Skip
        # the lock when the moved rows already cover every feed.
        positional = [fid for fid in before if feeds_by_id[fid].chnum_start is None]
        distinct = {str(n) for n in before.values()}
        only = next(iter(distinct)) if len(distinct) == 1 else None
        if (positional or overlaid) and only and only.isdigit():
            new.number, new.number_pinned, new.pinned_chno = int(only), True, None
            notes.append(f'Locks it to channel {only} so the number stays put.')
        elif (positional or overlaid) and only and _DECIMAL_RE.fullmatch(only):
            new.pinned_chno, new.number_pinned = only, False
            notes.append(f'Locks it to channel {only} so the number stays put.')
        elif old.number is not None:
            new.number = old.number

    # Guide mapping: a user-set ID describes the content, not the source, so it
    # carries -- unless the new channel already maps somewhere else.
    old_gn = (old.gracenote_id or '').strip()
    new_gn = (new.gracenote_id or '').strip()
    if old.gracenote_mode == 'manual' and old_gn:
        if not new_gn or new_gn == old_gn or new.gracenote_mode == 'off':
            if new_gn != old_gn or new.gracenote_mode != 'manual':
                from .routes.api_shared import _apply_gracenote_update
                try:
                    _apply_gracenote_update(new, old_gn, 'manual')
                except ValueError as exc:
                    raise TransferError(f'{old.name} has a saved Gracenote ID ({old_gn}) that '
                                        f'can\'t be carried over: {exc}') from exc
                carried.append(f'Gracenote ID {old_gn}')
        elif new_gn != old_gn:
            notes.append(f'Kept {new.name}\'s own Gracenote ID {new_gn} (yours was {old_gn}).')
    elif old.gracenote_mode == 'off' and new.gracenote_mode != 'off':
        new.gracenote_mode = 'off'
        new.gracenote_locked = False
        carried.append('Gracenote turned off')

    # Overrides can drive feed filters (category/language), so carry them.
    if old.category_override and not new.category_override:
        new.category_override = new.category = old.category_override
        carried.append(f'category "{old.category_override}"')
    if old.language_override and not new.language_override:
        new.language_override = new.language = old.language_override
        carried.append(f'language "{old.language_override}"')

    old.is_enabled = False
    new.is_enabled = True
    new.review_state = 'approved'
    if (new.disable_reason or '').startswith('DRM'):
        new.disable_reason = None
    db.session.flush()

    # A feed that held the old channel through a filter the new one doesn't
    # match (e.g. "Roku only") keeps it by pinning the new channel in.
    member_now = _feed_member_numbers([f for f in live_feeds if f.id in before])
    pinned_into: list[str] = []
    for feed in live_feeds:
        if feed.id not in before or new.id in member_now[feed.id] or feed.slug == 'default':
            continue
        filters = dict(feed.filters or {})
        if filters.get('channel_ids'):
            continue  # explicit list already swapped; absence means the event limit excludes it
        excluded = [i for i in (filters.get('excluded_channel_ids') or []) if int(i) != new.id]
        if excluded:
            filters['excluded_channel_ids'] = excluded
        else:
            filters.pop('excluded_channel_ids', None)
        pinned = [int(i) for i in (filters.get('pinned_channel_ids') or [])]
        if new.id not in pinned:
            pinned.append(new.id)
        filters['pinned_channel_ids'] = pinned
        feed.filters = filters
        pinned_into.append(feed.name)
    if pinned_into:
        db.session.flush()

    _refresh_auto_channel_numbers()
    db.session.flush()

    after_maps = _feed_member_numbers(live_feeds)
    feeds_report = []
    for feed in live_feeds:
        was = before.get(feed.id)
        now = after_maps[feed.id].get(new.id)
        now = shown_number(new, now) if now is not None else None
        if was is None and now is None:
            continue
        feeds_report.append({
            'feed_id': feed.id,
            'feed': feed.name,
            'before': was,
            'after': now,
            'kept': was is not None and str(was) == str(now),
        })
    if pinned_into:
        notes.append(f'Added to {", ".join(pinned_into)} by hand, since the filters there wouldn\'t include it.')

    def _desc(ch):
        return {'id': ch.id, 'name': ch.name, 'source': ch.source.display_name if ch.source else None}

    return {
        'from': _desc(old),
        'to': _desc(new),
        'carried': carried,
        'notes': notes,
        'feeds': feeds_report,
        'all_numbers_kept': all(r['kept'] for r in feeds_report if r['before'] is not None),
    }

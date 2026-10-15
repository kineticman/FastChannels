"""Shared state changes for temporary channels.

Scrapers decide when an event expires: ESPN supplies deadlines, while a user
playlist counts accepted imports that omit a group entry. Both signals use the
same hide, restore, and delete operations here.
"""

from datetime import datetime

from .channel_tags import AUTO_EVENT_REASONS, EVENT_RETIRED_REASON
from .extensions import db
from .models import FeedChannelNumber


def hide_event(channel, reason: str, now: datetime) -> bool:
    """Hide an event and record why only when the app disabled it."""
    before = (channel.is_active, channel.is_enabled, channel.disable_reason)
    if channel.is_active:
        channel.went_inactive_at = now
    channel.is_active = False
    if channel.is_enabled:
        channel.is_enabled = False
        channel.disable_reason = reason
    elif channel.disable_reason in AUTO_EVENT_REASONS:
        # A passed deadline takes priority over an earlier playlist absence.
        # Keep a retirement marker if a later import also omits the channel.
        if reason == EVENT_RETIRED_REASON:
            channel.disable_reason = reason
    return before != (channel.is_active, channel.is_enabled, channel.disable_reason)


def restore_event(channel, reason: str) -> bool:
    """Restore only a channel still marked as automatically hidden for reason."""
    if channel.disable_reason != reason:
        return False
    channel.is_active = True
    channel.is_enabled = True
    channel.disable_reason = None
    channel.went_inactive_at = None
    return True


def delete_event(channel) -> None:
    """Remove an event and its feed number; Program rows cascade with Channel."""
    FeedChannelNumber.query.filter_by(channel_id=channel.id).delete(synchronize_session=False)
    db.session.delete(channel)

"""SQL predicates for exact tags stored as comma-separated channel text."""

from sqlalchemy import func, literal

from .models import Channel
from .scrapers.base import EVENT_CHANNEL_TAG

EVENT_RETIRED_REASON = 'Event ended'
EVENT_MISSING_REASON = 'Event missing from playlist'
EVENT_SCHEDULE_MISSING_REASON = 'Event missing from schedule'
EVENT_FILTERED_REASON = 'Event excluded by filter'
AUTO_EVENT_REASONS = (EVENT_RETIRED_REASON, EVENT_MISSING_REASON,
                      EVENT_SCHEDULE_MISSING_REASON, EVENT_FILTERED_REASON)


def event_channel_clause():
    tags = literal(',') + func.coalesce(Channel.tags, '') + literal(',')
    return func.instr(tags, f',{EVENT_CHANNEL_TAG},') > 0


def event_filter_clause(value):
    if value == '1':
        return event_channel_clause()
    if value == '0':
        return ~event_channel_clause()
    return None

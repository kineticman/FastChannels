"""SQL predicates for exact tags stored as comma-separated channel text."""

from sqlalchemy import func, literal

from .models import Channel
from .scrapers.base import EVENT_CHANNEL_TAG

EVENT_RETIRED_REASON = 'Event ended'
EVENT_MISSING_REASON = 'Event missing from playlist'


def event_channel_clause():
    tags = literal(',') + func.coalesce(Channel.tags, '') + literal(',')
    return func.instr(tags, f',{EVENT_CHANNEL_TAG},') > 0


def event_filter_clause(value):
    if value == '1':
        return event_channel_clause()
    if value == '0':
        return ~event_channel_clause()
    return None

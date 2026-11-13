"""'Since you last looked' dashboard card: new sources and channel changes
newer than AppSettings.whats_new_seen_at.

Sources are new when their row arrived on this install after the marker
(Source.created_at, stamped by seed_sources). Channel counts come from the
same columns as the Channel Changes report, which links back with
window=since for the detail.
"""

from datetime import datetime, timezone

from sqlalchemy import func

from .config import VERSION
from .extensions import db
from .models import AppSettings, Channel, Source

# How far back "recently added to FastChannels" reaches for a source that was
# already on the install before tracking began: this minor release and the one
# before it (5.9.x on a 5.9.5 install also counts 5.8.x).
RECENT_MINOR_RELEASES = 2


def _version_tuple(value: str | None) -> tuple[int, ...] | None:
    try:
        return tuple(int(part) for part in (value or '').split('.')[:2])
    except ValueError:
        return None


def is_recent_release(added_in: str | None, current: str = VERSION) -> bool:
    """True when added_in is within RECENT_MINOR_RELEASES minor releases of current."""
    added, now = _version_tuple(added_in), _version_tuple(current)
    if not added or not now or len(added) < 2 or len(now) < 2:
        return False
    if added[0] != now[0]:
        return False
    return now[1] - RECENT_MINOR_RELEASES < added[1] <= now[1]


def stamp_recent_untracked(scrapers, now) -> int:
    """One-time catch-up for sources that predate Source.created_at: a disabled
    source shipped in a recent release is stamped now, so the first upgrade with
    this card still shows what arrived in the last couple of releases instead of
    nothing. Rows already stamped are never touched again, and enabled sources
    are left alone (the user already knows about them). Caller commits."""
    stamped = 0
    rows = Source.query.filter(Source.created_at.is_(None), Source.is_enabled.is_(False)).all()
    for source in rows:
        cls = scrapers.get(source.name)
        if cls and not getattr(cls, 'under_development', False) and is_recent_release(getattr(cls, 'added_in', None)):
            source.created_at = now
            stamped += 1
    return stamped


def build_whats_new(scrapers, *, lite: bool) -> dict | None:
    """The card's content, or None when there's nothing to show."""
    settings = AppSettings.get()
    seen = settings.whats_new_seen_at
    if seen is None:
        return None

    new_sources = []
    new_source_ids = set()
    for source in (Source.query.filter(Source.created_at > seen)
                   .order_by(Source.display_name).all()):
        cls = scrapers.get(source.name)
        if cls is None or getattr(cls, 'under_development', False):
            continue
        advanced = bool(cls.is_advanced())
        new_source_ids.add(source.id)
        new_sources.append({
            'id': source.id,
            'name': source.name,
            'display_name': source.display_name,
            'enabled': bool(source.is_enabled),
            'advanced': advanced,
            # Lite mode drops disabled advanced sources from the Sources page.
            'hidden': lite and advanced and not source.is_enabled,
            'added_in': getattr(cls, 'added_in', None),
        })

    # Channel changes on sources the user already had, so a source that is
    # itself new doesn't also show up as hundreds of "new channels". Event
    # channels (games, one-offs) churn daily and would drown the real lineup
    # news, so they're left to the report.
    from .channel_tags import event_channel_clause
    base = (db.session.query(Source.display_name, func.count(Channel.id))
            .join(Source, Source.id == Channel.source_id)
            .filter(Source.is_enabled.is_(True), ~event_channel_clause()))
    if new_source_ids:
        base = base.filter(~Source.id.in_(new_source_ids))

    def _by_source(*criteria):
        rows = (base.filter(*criteria).group_by(Source.display_name)
                .order_by(func.count(Channel.id).desc(), Source.display_name).all())
        return [{'source': name, 'count': count} for name, count in rows]

    new_channels = _by_source(Channel.created_at > seen, Channel.is_active.is_(True))
    # A source turned off and on again (or otherwise rebuilt) re-inserts its
    # whole lineup, so every channel looks new. If most of a source's lineup
    # is "new", that's a rebuild the user caused, not lineup news: leave it to
    # the report.
    lineup = dict(base.filter(Channel.is_active.is_(True)).group_by(Source.display_name).all())
    new_channels = [row for row in new_channels
                    if row['count'] * 2 <= lineup.get(row['source'], 0)]
    gone_channels = _by_source(Channel.is_active.is_(False), Channel.went_inactive_at > seen)
    pending_review = sum(row['count'] for row in _by_source(
        Channel.created_at > seen, Channel.is_active.is_(True), Channel.review_state == 'pending'))

    if not (new_sources or new_channels or gone_channels):
        return None
    return {
        'since': seen,
        'sources': new_sources,
        'new_channels': new_channels,
        'new_channel_total': sum(row['count'] for row in new_channels),
        'gone_channels': gone_channels,
        'gone_channel_total': sum(row['count'] for row in gone_channels),
        'pending_review': pending_review,
    }


def mark_seen() -> datetime:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    settings = AppSettings.get()
    settings.whats_new_seen_at = now
    db.session.commit()
    return now

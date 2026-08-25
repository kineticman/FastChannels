import logging
import re

logger = logging.getLogger(__name__)
from datetime import datetime, timezone

from sqlalchemy import and_, not_, or_
from ..extensions import db
from ..models import Source, Channel, Program, Feed
from .tasks import (
    trigger_xml_refresh,
)
from ..xml_cache import (
    invalidate_xml_cache,
)
from .admin import _apply_admin_feed_membership_filters, _category_clause, _duplicate_name_sets, _language_clause

_GRACENOTE_RE = re.compile(r'^(\d+|(EP|SH|MV|SP|TR)\d+)$', re.I)
_GRACENOTE_MODES = {'auto', 'manual', 'off'}


def _apply_gracenote_update(channel: Channel, raw_value, raw_mode=None) -> str | None:
    mode = (raw_mode if raw_mode is not None else getattr(channel, 'gracenote_mode', None) or ('manual' if getattr(channel, 'gracenote_locked', False) else 'auto'))
    mode = str(mode).strip().lower()
    if mode not in _GRACENOTE_MODES:
        raise ValueError('Invalid Gracenote mode.')

    raw = (raw_value or '').strip()
    if raw and not _GRACENOTE_RE.match(raw):
        raise ValueError('Invalid Gracenote ID — must be numeric (e.g. 122912) or start with EP/SH/MV/SP/TR (e.g. EP012345678)')
    if raw and not raw.isdigit():
        # _GRACENOTE_RE validates the prefix case-insensitively, but m3u.py's
        # _parse_gracenote_id / _has_gracenote_claim match it case-sensitively —
        # canonicalize here so a lowercase "ep..." entry doesn't silently end up
        # excluded from the standard/XMLTV output yet never picked up by the
        # Gracenote one either (a channel with guide data nowhere).
        raw = raw[:2].upper() + raw[2:]

    # Any explicit user action counts as reviewing an upstream content change,
    # so dismiss the review marker (the 🔁 flag in the channels list).
    channel.identity_changed_at = None

    if mode == 'off':
        # Turning routing off must NOT destroy the stored/suggested ID — keep it
        # dormant so the channel can be re-enabled later, and so scraper-supplied
        # suggestions (e.g. HDHomeRun station IDs) survive an off toggle. The
        # generators already exclude off-mode channels from the Gracenote M3U
        # regardless of the stored ID. Use the 'clear_ids' bulk action to wipe.
        # An explicit ID supplied alongside mode=off still updates the value.
        channel.gracenote_mode = 'off'
        channel.gracenote_locked = False
        if raw:
            channel.gracenote_id = raw
        return channel.gracenote_id

    if mode == 'manual':
        if not raw:
            raise ValueError('Manual Gracenote mode requires an ID.')
        channel.gracenote_id = raw
        channel.gracenote_mode = 'manual'
        channel.gracenote_locked = True
        return raw

    channel.gracenote_id = raw or None
    channel.gracenote_mode = 'auto'
    channel.gracenote_locked = False
    return channel.gracenote_id


def _manual_gracenote_clause():
    return or_(
        Channel.gracenote_mode == 'manual',
        and_(
            Channel.gracenote_mode == None,
            Channel.gracenote_locked == True,
            Channel.gracenote_id != None,
            Channel.gracenote_id != '',
        ),
    )


def _apply_channel_filters(q, filters: dict | None = None):
    filters = filters or {}

    if channel_ids := filters.get('channel_ids'):
        ids = [int(v) for v in channel_ids if str(v).isdigit()]
        q = q.filter(Channel.id.in_(ids or [-1]))

    if feed_slug := filters.get('feed'):
        feed = Feed.query.filter_by(slug=feed_slug).first()
        if feed:
            q = _apply_admin_feed_membership_filters(q, feed)
    if src := filters.get('source'):
        q = q.filter(Source.name == src)
    if (cat_clause := _category_clause(filters.get('category'))) is not None:
        q = q.filter(cat_clause)
    if (lang_clause := _language_clause(filters.get('language'))) is not None:
        q = q.filter(lang_clause)
    if search := filters.get('search'):
        q = q.filter(Channel.name.ilike(f'%{search}%'))
    if drm := filters.get('drm'):
        if drm == '1':
            # DRM: both disabled-DRM channels and active-but-bridged (PrismCast) ones.
            q = q.filter(db.or_(Channel.disable_reason.like('DRM%'),
                                Channel.requires_drm_bridge == True))
        elif drm == 'bridge':
            q = q.filter(Channel.requires_drm_bridge == True)
        elif drm == 'dead':
            q = q.filter(Channel.disable_reason == 'Dead')
        elif drm == 'vod':
            q = q.filter(Channel.disable_reason == 'VOD')
        elif drm == '0':
            q = q.filter(Channel.disable_reason == None,
                         db.or_(Channel.requires_drm_bridge == False,
                                Channel.requires_drm_bridge == None))
    if ef := filters.get('enabled'):
        if ef in ('1', 'enabled'):
            q = q.filter(Channel.is_enabled == True)
        elif ef in ('0', 'disabled'):
            q = q.filter(Channel.is_enabled == False)
    if filters.get('review') == 'pending':
        q = q.filter(Channel.review_state == 'pending')
    if pf := filters.get('presence'):
        if pf == 'inactive':
            q = q.filter(Channel.is_active == False)
        elif pf == 'enabled_inactive':
            q = q.filter(Channel.is_enabled == True, Channel.is_active == False)
        elif pf == 'missed':
            q = q.filter(Channel.missed_scrapes >= 1)
        elif pf == 'pinned':
            q = q.filter(Channel.scrape_pinned == True)
        elif pf == 'active':
            q = q.filter(Channel.is_active == True)
    if gf := filters.get('gracenote'):
        if gf in ('1', 'has_id'):
            q = q.filter(Channel.gracenote_id != None, Channel.gracenote_id != '')
        elif gf in ('0', 'missing_id'):
            q = q.filter((Channel.gracenote_id == None) | (Channel.gracenote_id == ''))
    if gm := filters.get('gracenote_mode'):
        manual_mode = _manual_gracenote_clause()
        off_mode = Channel.gracenote_mode == 'off'
        if gm == 'manual':
            q = q.filter(manual_mode)
        elif gm == 'off':
            q = q.filter(off_mode)
        elif gm == 'auto':
            q = q.filter(not_(or_(manual_mode, off_mode)))
    if country := filters.get('country'):
        q = q.filter(Channel.country == country)
    if filters.get('featured') == '1':
        q = q.filter(or_(Channel.tags.ilike('%featured%'), Channel.content_swap_count > 0))
    if filters.get('new') in ('1', '3', '7', '14'):
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(filters['new']))
        q = q.filter(Channel.created_at >= cutoff)
    if filters.get('epg') in ('0', '1'):
        has_epg = db.session.query(Program.channel_id).filter(
            Program.channel_id == Channel.id,
            Program.end_time > datetime.now(timezone.utc),
        ).exists()
        q = q.filter(has_epg if filters['epg'] == '1' else ~has_epg)
    if res := filters.get('resolution'):
        _ji = db.func.json_extract
        if res == '4k':
            q = q.filter(_ji(Channel.stream_info, '$.has_4k') == True)
        elif res == 'fhd':
            q = q.filter(_ji(Channel.stream_info, '$.max_height') >= 1080,
                         _ji(Channel.stream_info, '$.has_4k') != True)
        elif res == 'hd':
            q = q.filter(_ji(Channel.stream_info, '$.has_hd') == True,
                         _ji(Channel.stream_info, '$.max_height') < 1080)
        elif res == 'sd':
            q = q.filter(Channel.stream_info.isnot(None),
                         _ji(Channel.stream_info, '$.has_hd') != True)
        elif res == 'hevc':
            q = q.filter(_ji(Channel.stream_info, '$.video_codec') == 'hevc')
        elif res == 'known':
            q = q.filter(Channel.stream_info.isnot(None))
    if filters.get('duplicates') in ('1', 'unique'):
        exact_duplicate_names, possible_duplicate_names, gn_dup_ids = _duplicate_name_sets()
        all_duplicate_names = exact_duplicate_names | possible_duplicate_names
        if filters['duplicates'] == '1':
            q = q.filter(or_(Channel.name.in_(sorted(all_duplicate_names)), Channel.id.in_(gn_dup_ids), Channel.is_duplicate == True))
        else:
            q = q.filter(Channel.name.notin_(sorted(all_duplicate_names)), Channel.id.notin_(gn_dup_ids), Channel.is_duplicate == False)
    return q


def _invalidate_and_refresh_xml() -> None:
    """Invalidate cached output artifacts and enqueue the XML/M3U refresh job.

    The historical helper/job name says XML, but worker.run_xml_refresh rebuilds
    both XMLTV and M3U artifacts. Settings that affect playlist contents, such
    as m3u_rewrite_timestamps, should use this path.
    """
    invalidate_xml_cache()
    trigger_xml_refresh()



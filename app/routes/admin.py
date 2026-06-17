from collections import defaultdict
from datetime import datetime, timedelta, timezone
import re
import unicodedata
from flask import Blueprint, current_app, jsonify, render_template, request
from sqlalchemy import select, case
from sqlalchemy.orm import load_only
from ..extensions import db
from ..models import Source, Channel, Feed, AppSettings, Program
from ..generators.m3u import (
    _build_channel_query,
    _build_feed_chnum_map,
    _build_source_chnum_map,
    _build_sticky_gn_chnum_map,
    _selected_channel_stubs,
    feed_namespace_start,
    feed_to_query_filters,
)
from ..scrapers import registry as _scraper_registry
from ..source_config import (
    build_setup_checklist,
    has_meaningful_source_config,
    is_source_config_complete,
    public_base_url_config,
)
from ..timezone_utils import timezone_choices
from ..url import public_base_url, detected_base_url

admin_bp = Blueprint('admin', __name__, template_folder='../templates')


def _base_duplicate_name(name: str) -> str:
    s = unicodedata.normalize('NFKD', name or '')
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.casefold()
    s = s.replace('&', ' and ')
    s = s.replace('’', "'")
    s = re.sub(r'\s+presented\s+by\s+.+$', '', s).strip()
    s = re.sub(r'\s+by\s+.+$', '', s).strip()
    s = re.sub(r'[^a-z0-9]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _canonical_duplicate_name(name: str) -> str:
    s = _base_duplicate_name(name)
    return s


def _soft_duplicate_name(name: str) -> str:
    s = _base_duplicate_name(name)
    s = re.sub(r'\b(channel|tv|network)\s*$', '', s).strip()
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _duplicate_name_sets() -> tuple[set[str], set[str], set[int]]:
    name_rows = (
        db.session.query(Channel.name, Channel.source_id, Channel.country)
        .filter(Channel.name != None, Channel.name != '')
        .all()
    )
    # strict_by_key → list of (name, source_id, country) tuples
    strict_by_key: dict[str, list] = defaultdict(list)
    soft_by_key: dict[str, set[str]] = defaultdict(set)

    for name, source_id, country in name_rows:
        clean = (name or '').strip()
        if not clean:
            continue
        strict_key = _canonical_duplicate_name(clean)
        if strict_key:
            strict_by_key[strict_key].append((clean, source_id, country))
        soft_key = _soft_duplicate_name(clean)
        if soft_key:
            soft_by_key[soft_key].add(clean)

    duplicate_names: set[str] = set()
    cross_region_names: set[str] = set()

    for key, entries in strict_by_key.items():
        if len(entries) <= 1:
            continue
        source_ids = {e[1] for e in entries}
        if len(source_ids) > 1:
            # Multiple sources → real duplicate regardless of region
            duplicate_names.update(e[0] for e in entries)
        else:
            # Single source — check if it's just the same channel in multiple regions
            countries = {e[2] for e in entries}
            if len(countries) > 1:
                # Same source, different regions → softer flag (DUP?)
                cross_region_names.update(e[0] for e in entries)
            else:
                # Same source, same region → real duplicate
                duplicate_names.update(e[0] for e in entries)

    possible_names: set[str] = set()
    for names in soft_by_key.values():
        if len(names) > 1:
            possible_names.update(names)
    possible_names.update(cross_region_names)
    possible_names.difference_update(duplicate_names)

    # Gracenote-based duplicate detection: channels sharing a GN ID across
    # multiple sources but with different names (missed by name normalizers).
    gn_rows = (
        db.session.query(Channel.id, Channel.name, Channel.gracenote_id, Channel.source_id)
        .filter(Channel.gracenote_id != None, Channel.gracenote_id != '')
        .all()
    )
    gn_by_id: dict[str, list] = defaultdict(list)
    for cid, cname, gn, sid in gn_rows:
        gn_by_id[gn].append((cid, (cname or '').strip(), sid))

    gn_duplicate_ids: set[int] = set()
    for gn, entries in gn_by_id.items():
        if len({e[2] for e in entries}) < 2:
            continue  # all from same source — not cross-source
        canonical_keys = {_canonical_duplicate_name(e[1]) for e in entries if e[1]}
        if len(canonical_keys) <= 1:
            continue  # names already identical — caught by name matching
        for cid, cname, _ in entries:
            if cname not in duplicate_names:
                gn_duplicate_ids.add(cid)

    return duplicate_names, possible_names, gn_duplicate_ids


def _page_source_chnum_map(page_items) -> dict[int, int]:
    if not page_items:
        return {}
    std_channels = _selected_channel_stubs({}, gracenote=False)
    full_map, _ = _build_source_chnum_map(std_channels)
    gn_channels = _selected_channel_stubs({}, gracenote=True)
    if gn_channels:
        gn_start = (max(full_map.values()) + 1) if full_map else 1
        full_map.update(_build_sticky_gn_chnum_map(gn_channels, gn_start, set(full_map.values())))
    page_ids = {ch.id for ch in page_items}
    return {channel_id: chnum for channel_id, chnum in full_map.items() if channel_id in page_ids}


def _page_default_feed_chnum_map(page_items) -> dict[int, int]:
    if not page_items:
        return {}

    default_feed = Feed.query.filter_by(slug='default', is_enabled=True).first()
    if not default_feed:
        return _page_source_chnum_map(page_items)

    filters = feed_to_query_filters(default_feed.filters or {})
    std_channels = _selected_channel_stubs(filters, gracenote=False)
    gn_channels  = _selected_channel_stubs(filters, gracenote=True)

    if default_feed.chnum_start is not None:
        full_map = _build_feed_chnum_map(std_channels, default_feed.chnum_start) if std_channels else {}
    else:
        full_map, _ = _build_source_chnum_map(std_channels) if std_channels else ({}, [])

    if gn_channels:
        gn_start = (max(full_map.values()) + 1) if full_map else (default_feed.chnum_start or 1)
        full_map.update(_build_sticky_gn_chnum_map(gn_channels, gn_start, set(full_map.values())))

    page_ids = {ch.id for ch in page_items}
    return {channel_id: chnum for channel_id, chnum in full_map.items() if channel_id in page_ids}


def _apply_admin_feed_membership_filters(query, feed: Feed):
    """Apply a feed's membership rules without forcing enabled/active output constraints."""
    filters = feed_to_query_filters(feed.filters or {})
    if channel_ids := filters.get('channel_ids'):
        query = query.filter(Channel.id.in_(channel_ids))
        return query
    if sources := filters.get('source'):
        query = query.filter(Source.name.in_(sources))
    if categories := filters.get('category'):
        query = query.filter(Channel.category.in_(categories))
    if languages := filters.get('languages'):
        query = query.filter(Channel.language.in_(languages))
    elif language := filters.get('language'):
        query = query.filter(Channel.language == language)
    if gracenote := filters.get('gracenote'):
        if gracenote == 'has':
            query = query.filter(Channel.gracenote_id != None, Channel.gracenote_id != '')
        elif gracenote == 'missing':
            query = query.filter((Channel.gracenote_id == None) | (Channel.gracenote_id == ''))
    if excluded_ids := filters.get('excluded_channel_ids'):
        query = query.filter(Channel.id.notin_(excluded_ids))
    return query


def _feed_split_counts(feed: Feed) -> dict[str, int]:
    filters = feed_to_query_filters(feed.filters or {})
    from sqlalchemy import func
    has_gracenote = (
        (Channel.gracenote_mode != 'off')
        & (
            ((Channel.gracenote_id != None) & (Channel.gracenote_id != ''))
            | Channel.slug.like('%|%')
        )
    )
    row = (
        _build_channel_query(filters)
        .order_by(None)
        .with_entities(
            func.count().label('total'),
            func.count(case((has_gracenote, Channel.id))).label('gn_count'),
        )
        .one()
    )
    total, gn_count = row.total, row.gn_count
    return {
        'standard_count': max(total - gn_count, 0),
        'gracenote_count': gn_count,
        'total_count': total,
    }


@admin_bp.route('/')
def dashboard():
    sources        = Source.query.order_by(Source.display_name).all()
    total_channels = Channel.query.filter_by(is_active=True, is_enabled=True).count()
    base_url       = public_base_url()
    feeds          = Feed.query.filter_by(is_enabled=True).order_by(Feed.name).all()
    app_settings   = AppSettings.get()
    all_scrapers   = _scraper_registry.get_all()
    setup_checklist = build_setup_checklist(
        app_settings,
        {source.name: source for source in sources},
        all_scrapers,
    )
    count_rows = (
        db.session.query(Source.id, db.func.count(Channel.id))
        .join(Channel)
        .filter(
            Channel.is_active == True,
            Channel.is_enabled == True,
            Source.is_enabled == True,
            Source.epg_only == False,
            Channel.stream_url != None,
        )
        .group_by(Source.id)
        .all()
    )
    count_map = {source_id: count for source_id, count in count_rows}

    # Count channels in any non-default feed per source
    non_default_feeds = [f for f in feeds if f.slug != 'default']
    feed_member_map: dict[int, int] = {}
    if non_default_feeds:
        all_feed_channel_ids: set[int] = set()
        for feed in non_default_feeds:
            ids = (
                _build_channel_query(feed_to_query_filters(feed.filters or {}))
                .with_entities(Channel.id)
                .all()
            )
            all_feed_channel_ids.update(row[0] for row in ids)
        if all_feed_channel_ids:
            feed_count_rows = (
                db.session.query(Channel.source_id, db.func.count(Channel.id))
                .filter(Channel.id.in_(all_feed_channel_ids))
                .group_by(Channel.source_id)
                .all()
            )
            feed_member_map = {sid: cnt for sid, cnt in feed_count_rows}

    source_output_meta = {
        source.id: {
            'channel_count': count_map.get(source.id, 0),
            'feed_channel_count': feed_member_map.get(source.id, 0),
        }
        for source in sources
    }
    return render_template('admin/dashboard.html', sources=sources,
                           total_channels=total_channels, base_url=base_url,
                           feeds=feeds, source_output_meta=source_output_meta,
                           setup_checklist=setup_checklist,
                           setup_complete_count=5 - len(setup_checklist),
                           setup_total_count=5,
                           now=datetime.now(timezone.utc))


@admin_bp.route('/sources')
def sources():
    all_scrapers   = _scraper_registry.get_all()
    audit_enabled  = {
        name: getattr(cls, 'stream_audit_enabled', False)
        for name, cls in all_scrapers.items()
    }
    config_required = {
        name: getattr(cls, 'config_required', False)
        for name, cls in all_scrapers.items()
    }
    premium_sources = {
        name: getattr(cls, 'is_premium', False)
        for name, cls in all_scrapers.items()
    }
    source_categories = {
        name: getattr(cls, 'source_category', 'fast')
        for name, cls in all_scrapers.items()
    }
    source_interval_meta = {
        name: {
            'recommended': getattr(cls, 'scrape_interval', 360),
            'min': getattr(cls, 'min_scrape_interval', 30),
            'max': getattr(cls, 'max_scrape_interval', 10080),
        }
        for name, cls in all_scrapers.items()
    }

    def _config_status(source, cls):
        schema = getattr(cls, 'config_schema', [])
        if not schema:
            return 'none'
        if getattr(cls, 'config_required', False):
            return 'configured' if is_source_config_complete(source.name, cls, source.config or {}) else 'required'
        return 'configured' if has_meaningful_source_config(cls, source.config or {}) else 'optional'

    sources_list = Source.query.order_by(Source.display_name).all()
    source_config_status = {
        s.id: _config_status(s, all_scrapers[s.name])
        for s in sources_list
        if s.name in all_scrapers
    }
    needs_config = [
        s for s in sources_list
        if source_config_status.get(s.id) == 'required' and s.is_enabled
    ]

    _CAT_ORDER = {'fast': 0, 'premium': 1, 'specialty': 2, 'drm': 3}
    sources_list.sort(key=lambda s: (
        _CAT_ORDER.get(source_categories.get(s.name, 'fast') if s.name != 'custom' else 'specialty', 99),
        s.display_name,
    ))

    from ..scrapers.category_utils import CANONICAL_CATEGORIES
    return render_template('admin/sources.html',
                           sources=sources_list,
                           chnum_warnings=[],
                           audit_enabled=audit_enabled,
                           config_required=config_required,
                           premium_sources=premium_sources,
                           source_categories=source_categories,
                           source_interval_meta=source_interval_meta,
                           source_config_status=source_config_status,
                           needs_config=needs_config,
                           canonical_categories=CANONICAL_CATEGORIES)


@admin_bp.route('/channels')
def channels():
    page             = request.args.get('page', 1, type=int)
    feed_filter      = request.args.get('feed', '')
    source_filter    = request.args.get('source', '')
    search           = request.args.get('search', '')
    enabled_filter   = request.args.get('enabled', '')
    presence_filter  = request.args.get('presence', '')
    drm_filter       = request.args.get('drm', '')
    gracenote_filter = request.args.get('gracenote', '')
    gracenote_mode_filter = request.args.get('gracenote_mode', '')
    language_filter  = request.args.get('language', '')
    country_filter   = request.args.get('country', '')
    category_filter  = request.args.get('category', '')
    duplicates_filter = request.args.get('duplicates', '')
    new_filter       = request.args.get('new', '')
    epg_filter       = request.args.get('epg', '')
    resolution_filter = request.args.get('resolution', '')
    sort_by          = request.args.get('sort', 'name')
    sort_dir         = request.args.get('dir', 'asc')

    exact_duplicate_names, possible_duplicate_names, gn_duplicate_ids = _duplicate_name_sets()
    all_duplicate_names = exact_duplicate_names | possible_duplicate_names

    q = Channel.query.join(Source)

    selected_feed = None
    if feed_filter == '__none__':
        _in_any: set[int] = set()
        for _f in Feed.query.filter_by(is_enabled=True).filter(Feed.slug != 'default').all():
            _ids = _apply_admin_feed_membership_filters(
                db.session.query(Channel.id).join(Source), _f
            ).all()
            _in_any.update(r[0] for r in _ids)
        if _in_any:
            _id_list = list(_in_any)
            for _i in range(0, len(_id_list), 900):
                q = q.filter(Channel.id.notin_(_id_list[_i:_i + 900]))
    elif feed_filter:
        selected_feed = Feed.query.filter_by(slug=feed_filter).first()
        if selected_feed:
            q = _apply_admin_feed_membership_filters(q, selected_feed)

    # Status filter — admin always shows all channels regardless of is_active
    if drm_filter == '1':
        q = q.filter(Channel.disable_reason.like('DRM%'))
    elif drm_filter == 'dead':
        q = q.filter(Channel.disable_reason == 'Dead')
    elif drm_filter == 'vod':
        q = q.filter(Channel.disable_reason == 'VOD')
    elif drm_filter == '0':
        q = q.filter(Channel.disable_reason == None)

    if enabled_filter == '1':
        q = q.filter(Channel.is_enabled == True)
    elif enabled_filter == '0':
        q = q.filter(Channel.is_enabled == False)

    if presence_filter == 'inactive':
        q = q.filter(Channel.is_active == False)
    elif presence_filter == 'enabled_inactive':
        q = q.filter(Channel.is_enabled == True, Channel.is_active == False)
    elif presence_filter == 'missed':
        q = q.filter(Channel.missed_scrapes >= 1)
    elif presence_filter == 'pinned':
        q = q.filter(Channel.scrape_pinned == True)
    elif presence_filter == 'active':
        q = q.filter(Channel.is_active == True)

    if gracenote_filter == '1':
        q = q.filter(Channel.gracenote_id != None, Channel.gracenote_id != '')
    elif gracenote_filter == '0':
        q = q.filter((Channel.gracenote_id == None) | (Channel.gracenote_id == ''))
    if gracenote_mode_filter == 'manual':
        q = q.filter(db.or_(
            Channel.gracenote_mode == 'manual',
            db.and_(
                Channel.gracenote_mode == None,
                Channel.gracenote_locked == True,
                Channel.gracenote_id != None,
                Channel.gracenote_id != '',
            ),
        ))
    elif gracenote_mode_filter == 'off':
        q = q.filter(Channel.gracenote_mode == 'off')
    elif gracenote_mode_filter == 'auto':
        q = q.filter(db.not_(db.or_(
            Channel.gracenote_mode == 'off',
            Channel.gracenote_mode == 'manual',
            db.and_(
                Channel.gracenote_mode == None,
                Channel.gracenote_locked == True,
                Channel.gracenote_id != None,
                Channel.gracenote_id != '',
            ),
        )))

    if source_filter:
        q = q.filter(Source.name == source_filter)
    q_before_language = q  # snapshot for language facet (exclude own filter)
    if language_filter:
        q = q.filter(Channel.language == language_filter)
    q_before_country = q  # snapshot for country facet (exclude own filter)
    if country_filter:
        q = q.filter(Channel.country == country_filter)
    q_before_category = q  # snapshot for category facet (exclude own filter)
    if category_filter == '__none__':
        q = q.filter(Channel.category == None)
    elif category_filter:
        q = q.filter(Channel.category == category_filter)
    if search:
        q = q.filter(Channel.name.ilike(f'%{search}%'))

    if duplicates_filter == '1':
        q = q.filter(db.or_(
            Channel.name.in_(sorted(all_duplicate_names)),
            Channel.id.in_(gn_duplicate_ids),
            Channel.is_duplicate == True,
        ))
    elif duplicates_filter == 'unique':
        q = q.filter(
            Channel.name.notin_(sorted(all_duplicate_names)),
            Channel.id.notin_(gn_duplicate_ids),
            Channel.is_duplicate == False,
        )

    if new_filter in ('1', '3', '7', '14'):
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(new_filter))
        q = q.filter(Channel.created_at >= cutoff)

    if epg_filter in ('0', '1'):
        now = datetime.now(timezone.utc)
        has_epg = db.session.query(Program.channel_id).filter(
            Program.channel_id == Channel.id,
            Program.end_time > now,
        ).exists()
        if epg_filter == '1':
            q = q.filter(has_epg)
        else:
            q = q.filter(~has_epg)

    if resolution_filter == '4k':
        q = q.filter(db.func.json_extract(Channel.stream_info, '$.has_4k') == True)
    elif resolution_filter == 'fhd':
        # Full HD: 1080p up to (but not including) 4K — includes 1440p/QHD
        q = q.filter(
            db.func.json_extract(Channel.stream_info, '$.max_height') >= 1080,
            db.func.json_extract(Channel.stream_info, '$.has_4k') != True,
        )
    elif resolution_filter == 'hd':
        # HD only: 720p up to (but not including) 1080p
        q = q.filter(
            db.func.json_extract(Channel.stream_info, '$.has_hd') == True,
            db.func.json_extract(Channel.stream_info, '$.max_height') < 1080,
        )
    elif resolution_filter == 'sd':
        q = q.filter(
            Channel.stream_info.isnot(None),
            db.func.json_extract(Channel.stream_info, '$.has_hd') != True,
        )
    elif resolution_filter == 'hevc':
        q = q.filter(db.func.json_extract(Channel.stream_info, '$.video_codec') == 'hevc')
    elif resolution_filter == 'known':
        q = q.filter(Channel.stream_info.isnot(None))

    sort_name = case(
        (db.func.lower(Channel.name).like('the %'), db.func.lower(db.func.substr(Channel.name, 5))),
        (db.func.lower(Channel.name).like('an %'),  db.func.lower(db.func.substr(Channel.name, 4))),
        (db.func.lower(Channel.name).like('a %'),   db.func.lower(db.func.substr(Channel.name, 3))),
        else_=db.func.lower(Channel.name),
    )

    _sort_cols = {
        'name':     [sort_name, Channel.name],
        'source':   [Source.display_name, Channel.name],
        'category': [Channel.category, Channel.name],
        # Approximate M3U order: sources with explicit chnum_start first, then by
        # actual channel number within each source block, then name as tiebreak.
        'number':   [db.func.coalesce(Source.chnum_start, 999999), db.func.coalesce(Channel.number, 999999), Source.display_name, sort_name, Channel.name],
    }
    _cols = _sort_cols.get(sort_by, [Channel.name])
    if sort_dir == 'desc':
        _order = [c.desc() for c in _cols]
    else:
        _order = [c.asc() for c in _cols]

    ordered_q = q.order_by(*_order)
    all_channel_ids = [r[0] for r in ordered_q.with_entities(Channel.id).all()]
    channels = ordered_q.paginate(page=page, per_page=50, error_out=False)
    feeds_q = Feed.query.filter(Feed.is_enabled == True)
    if feed_filter:
        feeds_q = feeds_q.union(Feed.query.filter(Feed.slug == feed_filter))
    feeds = feeds_q.order_by(Feed.name).all()
    sources_q = Source.query.filter(Source.is_enabled == True)
    if source_filter:
        sources_q = sources_q.union(
            Source.query.filter(Source.name == source_filter)
        )
    sources = sources_q.order_by(Source.display_name).all()

    # Drive facet counts from the current filtered set so counts reflect active filters.
    filtered_ids = q.with_entities(Channel.id).scalar_subquery()

    lang_facet_ids = q_before_language.with_entities(Channel.id).scalar_subquery()
    lang_rows = db.session.query(Channel.language, db.func.count(Channel.id))\
        .filter(Channel.id.in_(lang_facet_ids), Channel.language != None)\
        .group_by(Channel.language)\
        .order_by(Channel.language).all()
    languages = [(lang, count) for lang, count in lang_rows]

    cat_facet_ids = q_before_category.with_entities(Channel.id).scalar_subquery()
    cat_rows = db.session.query(Channel.category, db.func.count(Channel.id))\
        .filter(Channel.id.in_(cat_facet_ids), Channel.category != None)\
        .group_by(Channel.category)\
        .order_by(Channel.category).all()
    categories = [(cat, count) for cat, count in cat_rows]
    missing_category_count = db.session.query(db.func.count(Channel.id))\
        .filter(Channel.id.in_(cat_facet_ids), Channel.category == None)\
        .scalar() or 0

    country_facet_ids = q_before_country.with_entities(Channel.id).scalar_subquery()
    country_rows = db.session.query(Channel.country, db.func.count(Channel.id))\
        .filter(Channel.id.in_(country_facet_ids), Channel.country != None, Channel.country != '')\
        .group_by(Channel.country)\
        .order_by(Channel.country).all()
    countries = [(c, cnt) for c, cnt in country_rows]

    page_names = {(ch.name or '').strip() for ch in channels.items if (ch.name or '').strip()}
    page_ids   = {ch.id for ch in channels.items}
    duplicate_names = exact_duplicate_names & page_names
    possible_duplicate_names = possible_duplicate_names & page_names
    gn_duplicate_page_ids = gn_duplicate_ids & page_ids
    duplicate_group_keys = {ch.id: _canonical_duplicate_name(ch.name or '') for ch in channels.items}
    chnum_map = _page_default_feed_chnum_map(channels.items)

    # Pinned-number conflict detection for the current page.
    from sqlalchemy import func as _func
    _conflict_numbers = {
        row[0]
        for row in db.session.query(Channel.number)
        .filter(Channel.number_pinned == True, Channel.number.isnot(None))
        .group_by(Channel.number)
        .having(_func.count(Channel.id) > 1)
        .all()
    }
    chnum_conflicts = {
        ch.id for ch in channels.items
        if ch.number_pinned and ch.number in _conflict_numbers
    }

    # Compute which page channels appear in at least one non-default feed.
    in_any_feed_ids: set[int] = set()
    for feed in feeds:
        if feed.slug == 'default':
            continue
        f_filters = feed.filters or {}
        pinned = set(f_filters.get('pinned_channel_ids') or [])
        in_any_feed_ids.update(pinned & page_ids)
        q_filters = feed_to_query_filters(f_filters)
        matched = (
            _build_channel_query(q_filters)
            .filter(Channel.id.in_(page_ids))
            .with_entities(Channel.id)
            .all()
        )
        in_any_feed_ids.update(r[0] for r in matched)

    from urllib.parse import urlencode
    filter_qs = urlencode({k: v for k, v in {
        'feed': feed_filter, 'source': source_filter, 'search': search,
        'enabled': enabled_filter, 'presence': presence_filter, 'drm': drm_filter,
        'language': language_filter, 'country': country_filter,
        'gracenote': gracenote_filter, 'gracenote_mode': gracenote_mode_filter,
        'category': category_filter, 'duplicates': duplicates_filter,
        'new': new_filter,
        'epg': epg_filter,
        'resolution': resolution_filter,
        'sort': sort_by, 'dir': sort_dir,
    }.items() if v})

    return render_template('admin/channels.html',
                           channels=channels, all_channel_ids=all_channel_ids,
                           sources=sources, feeds=feeds,
                           feed_filter=feed_filter, selected_feed=selected_feed,
                           source_filter=source_filter, search=search,
                           enabled_filter=enabled_filter, drm_filter=drm_filter,
                           presence_filter=presence_filter,
                           gracenote_filter=gracenote_filter,
                           gracenote_mode_filter=gracenote_mode_filter,
                           language_filter=language_filter, languages=languages,
                           country_filter=country_filter, countries=countries,
                           category_filter=category_filter, categories=categories,
                           missing_category_count=missing_category_count,
                           duplicates_filter=duplicates_filter,
                           new_filter=new_filter,
                           epg_filter=epg_filter,
                           resolution_filter=resolution_filter,
                           duplicate_names=duplicate_names,
                           possible_duplicate_names=possible_duplicate_names,
                           gn_duplicate_ids=gn_duplicate_page_ids,
                           duplicate_group_keys=duplicate_group_keys,
                           sort_by=sort_by, sort_dir=sort_dir,
                           chnum_map=chnum_map,
                           chnum_conflicts=chnum_conflicts,
                           in_any_feed_ids=in_any_feed_ids,
                           filter_qs=filter_qs)


def _guide_sort_letter(name):
    """First letter a channel sorts under in the alphabetical guide view.

    Mirrors the SQL ``guide_sort_name`` ordering: lowercase and strip a leading
    ``the ``/``an ``/``a `` article so the rail's jump targets match the actual
    row order. Non-alphabetic leading characters bucket under ``#``.
    """
    s = (name or '').strip().lower()
    for article in ('the ', 'an ', 'a '):
        if s.startswith(article):
            s = s[len(article):].lstrip()
            break
    if not s:
        return '#'
    return s[0].upper() if s[0].isalpha() else '#'


@admin_bp.route('/guide')
def guide():
    from zoneinfo import ZoneInfo

    now_utc = datetime.now(timezone.utc)
    app_settings = AppSettings.get()
    tz = ZoneInfo(app_settings.effective_timezone_name())

    offset_hours     = max(-48, min(48, request.args.get('offset', type=int, default=0)))
    offset           = timedelta(hours=offset_hours)
    # Start the window on the previous half-hour boundary (local time) so the
    # guide's natural left edge lands on a clean :00/:30. Program titles begin
    # on those boundaries, so this keeps them visible with no client-side
    # scroll snapping — which never worked reliably on iOS.
    anchor_local     = (now_utc + offset).astimezone(tz).replace(second=0, microsecond=0)
    anchor_local    -= timedelta(minutes=anchor_local.minute % 30)
    window_start_utc = anchor_local.astimezone(timezone.utc)
    window_end_utc   = window_start_utc + timedelta(hours=5)
    window_seconds   = (window_end_utc - window_start_utc).total_seconds()

    window_start_local = window_start_utc.astimezone(tz)
    window_end_local   = window_end_utc.astimezone(tz)
    now_local          = now_utc.astimezone(tz)

    source_id = request.args.get('source_id', type=int)
    feed_id   = request.args.get('feed_id',   type=int)
    category  = request.args.get('category',  type=str, default='').strip()
    search    = request.args.get('search',    type=str, default='').strip()

    sources    = Source.query.filter_by(is_enabled=True).order_by(Source.display_name).all()
    feeds      = Feed.query.filter_by(is_enabled=True).order_by(Feed.name).all()
    categories = [r[0] for r in (
        db.session.query(Channel.category)
        .filter(Channel.is_active == True, Channel.is_enabled == True, Channel.category != None)
        .distinct().order_by(Channel.category).all()
    )]

    q = Channel.query.options(load_only(
        Channel.id, Channel.name, Channel.logo_url, Channel.number, Channel.source_id,
    )).filter_by(is_active=True, is_enabled=True)
    if source_id:
        q = q.filter(Channel.source_id == source_id)
    if feed_id:
        selected_feed = Feed.query.filter_by(id=feed_id, is_enabled=True).first()
        if selected_feed:
            q = q.join(Source, Channel.source_id == Source.id)
            q = _apply_admin_feed_membership_filters(q, selected_feed)
    if category:
        q = q.filter(Channel.category == category)
    if search:
        # Match channels whose own name matches, OR that air a program whose
        # title matches within the visible guide window. This way searching
        # "Star Trek" surfaces dedicated channels even when none of their
        # currently-airing programs happen to mention the term.
        matching_ch_ids = (
            db.session.query(Program.channel_id)
            .filter(
                Program.title.ilike(f'%{search}%'),
                Program.end_time   > window_start_utc,
                Program.start_time < window_end_utc,
            )
            .distinct()
            .scalar_subquery()
        )
        q = q.filter(db.or_(
            Channel.name.ilike(f'%{search}%'),
            Channel.id.in_(matching_ch_ids),
        ))
    guide_sort_name = case(
        (db.func.lower(Channel.name).like('the %'), db.func.lower(db.func.substr(Channel.name, 5))),
        (db.func.lower(Channel.name).like('an %'),  db.func.lower(db.func.substr(Channel.name, 4))),
        (db.func.lower(Channel.name).like('a %'),   db.func.lower(db.func.substr(Channel.name, 3))),
        else_=db.func.lower(Channel.name),
    )
    if source_id:
        channels = q.order_by(
            db.func.coalesce(Channel.number, 99999).asc(),
            Channel.name,
        ).all()
    else:
        channels = q.order_by(guide_sort_name).all()

    channel_ids = [c.id for c in channels]

    program_count = (
        Program.query.filter(
            Program.channel_id.in_(channel_ids),
            Program.end_time   > window_start_utc,
            Program.start_time < window_end_utc,
        ).count()
        if channel_ids else 0
    )

    guide_rows = [
        {'channel': c, 'sort_letter': _guide_sort_letter(c.name)} for c in channels
    ]

    def time_pct(dt_local):
        return (dt_local - window_start_local).total_seconds() / window_seconds * 100

    # 30-minute tick marks aligned to clock boundaries
    ticks = []
    t = window_start_local.replace(second=0, microsecond=0)
    remainder = t.minute % 30
    if remainder:
        t += timedelta(minutes=30 - remainder)
    while t <= window_end_local:
        ticks.append({'label': t.strftime('%-I:%M %p'), 'left': round(time_pct(t), 4)})
        t += timedelta(minutes=30)

    now_pct = round(time_pct(now_local), 4)

    # The window now begins exactly on the half-hour boundary, so the guide's
    # natural left edge already sits there (snap offset 0). The AJAX program
    # fetch is pinned to this same anchor so its bar positions stay aligned.
    snap_pct = 0.0
    window_anchor_epoch = int(window_start_utc.timestamp())

    return render_template(
        'admin/guide.html',
        guide_rows=guide_rows,
        ticks=ticks,
        now_pct=now_pct,
        snap_pct=snap_pct,
        window_anchor_epoch=window_anchor_epoch,
        sources=sources,
        feeds=feeds,
        categories=categories,
        source_id=source_id,
        feed_id=feed_id,
        category=category,
        search=search,
        offset_hours=offset_hours,
        channel_count=len(channels),
        program_count=program_count,
        app_timezone_name=app_settings.effective_timezone_name(),
    )


@admin_bp.route('/guide/programs')
def guide_programs():
    from zoneinfo import ZoneInfo
    raw_ids = (request.args.get('channel_ids') or '').strip()
    if not raw_ids:
        return jsonify({})
    try:
        channel_ids = [int(x) for x in raw_ids.split(',') if x.strip()]
    except ValueError:
        return jsonify({}), 400
    if not channel_ids:
        return jsonify({})

    offset_hours     = max(-48, min(48, request.args.get('offset', type=int, default=0)))
    now_utc          = datetime.now(timezone.utc)
    offset           = timedelta(hours=offset_hours)

    app_settings        = AppSettings.get()
    tz                  = ZoneInfo(app_settings.effective_timezone_name())

    # Prefer the anchor the page computed so program bars line up exactly with
    # the ticks/now-line; fall back to flooring locally (matches the page route)
    # if it's missing or malformed.
    anchor = request.args.get('anchor', type=int)
    if anchor:
        window_start_utc = datetime.fromtimestamp(anchor, tz=timezone.utc)
    else:
        anchor_local     = (now_utc + offset).astimezone(tz).replace(second=0, microsecond=0)
        anchor_local    -= timedelta(minutes=anchor_local.minute % 30)
        window_start_utc = anchor_local.astimezone(timezone.utc)
    window_end_utc   = window_start_utc + timedelta(hours=5)
    window_seconds   = (window_end_utc - window_start_utc).total_seconds()

    window_start_local  = window_start_utc.astimezone(tz)
    window_end_local    = window_end_utc.astimezone(tz)

    programs = (
        Program.query
        .options(load_only(
            Program.channel_id, Program.start_time, Program.end_time,
            Program.title, Program.description, Program.category,
            Program.rating, Program.poster_url,
        ))
        .filter(
            Program.channel_id.in_(channel_ids),
            Program.end_time   > window_start_utc,
            Program.start_time < window_end_utc,
        )
        .order_by(Program.channel_id, Program.start_time)
        .all()
    )

    def _as_utc(dt):
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

    def time_pct(dt_local):
        return (dt_local - window_start_local).total_seconds() / window_seconds * 100

    result = {cid: [] for cid in channel_ids}
    for p in programs:
        start_utc_p   = _as_utc(p.start_time)
        end_utc_p     = _as_utc(p.end_time)
        start_local   = start_utc_p.astimezone(tz)
        end_local     = end_utc_p.astimezone(tz)
        clamped_start = max(start_local, window_start_local)
        clamped_end   = min(end_local,   window_end_local)
        left  = time_pct(clamped_start)
        width = (clamped_end - clamped_start).total_seconds() / window_seconds * 100
        result[p.channel_id].append({
            'title':   p.title,
            'desc':    (p.description or '')[:300],
            'cat':     p.category or '',
            'start':   start_local.strftime('%-I:%M %p'),
            'end':     end_local.strftime('%-I:%M %p'),
            'left':    round(left, 4),
            'width':   round(width, 4),
            'current': start_utc_p <= now_utc < end_utc_p,
            'rating':  p.rating or '',
            'poster':  p.poster_url or '',
        })
    return jsonify(result)


@admin_bp.route('/channels/chnum-map')
def channels_chnum_map():
    raw_ids = (request.args.get('ids') or '').strip()
    if not raw_ids:
        return jsonify({'chnum_map': {}})
    ids: list[int] = []
    for part in raw_ids.split(','):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            continue
    if not ids:
        return jsonify({'chnum_map': {}})
    page_items = (
        Channel.query
        .join(Source)
        .filter(Channel.id.in_(ids))
        .all()
    )
    chnum_map = _page_default_feed_chnum_map(page_items)

    # Pin state for each requested channel.
    pinned = {ch.id: bool(ch.number_pinned) for ch in page_items}

    # Conflict detection: find pinned numbers used by more than one channel.
    from sqlalchemy import func as _func
    conflict_numbers = {
        row[0]
        for row in db.session.query(Channel.number)
        .filter(Channel.number_pinned == True, Channel.number.isnot(None))
        .group_by(Channel.number)
        .having(_func.count(Channel.id) > 1)
        .all()
    }
    conflict_ids = {
        ch.id for ch in page_items
        if ch.number_pinned and ch.number in conflict_numbers
    }

    return jsonify({
        'chnum_map':    chnum_map,
        'pinned':       pinned,
        'conflict_ids': list(conflict_ids),
    })


@admin_bp.route('/feeds')
def feeds():
    app_settings = AppSettings.get()
    sources    = Source.query.filter_by(is_enabled=True).order_by(Source.display_name).all()
    feeds      = Feed.query.order_by(Feed.name).all()
    cats = db.session.query(Channel.category)\
        .filter(Channel.is_active == True, Channel.category != None)\
        .distinct().order_by(Channel.category).all()
    categories = [c[0] for c in cats]
    langs = db.session.query(Channel.language)\
        .filter(Channel.is_active == True, Channel.language != None)\
        .distinct().order_by(Channel.language).all()
    languages  = [{'code': r[0], 'label': r[0]} for r in langs]
    country_rows = db.session.query(Channel.country)\
        .filter(Channel.is_active == True, Channel.country != None, Channel.country != '')\
        .distinct().order_by(Channel.country).all()
    countries = [r[0] for r in country_rows]
    base_url   = public_base_url()
    default_feed = next((f for f in feeds if f.slug == 'default'), None)
    # chnum_start is now the single source of truth for all feeds including default.
    # Show the auto-assigned namespace as placeholder for feeds without an explicit value.
    feed_chnum_placeholder = {}
    for feed in feeds:
        if feed.chnum_start is None and feed.slug != 'default':
            feed_chnum_placeholder[feed.id] = feed_namespace_start(feed, gracenote=False)
    feed_split_counts = {
        feed.id: _feed_split_counts(feed)
        for feed in feeds
    }
    default_split = feed_split_counts.get(default_feed.id, {}) if default_feed else {}
    feed_summary = {
        'total_feeds':   len(feeds),
        'enabled_feeds': sum(1 for f in feeds if f.is_enabled),
        'custom_feeds':  sum(1 for f in feeds if f.slug != 'default'),
        'all_channels':  default_feed.channel_count() if default_feed else 0,
        'gracenote':     default_split.get('gracenote_count', 0),
        'sources':       len(sources),
    }
    return render_template('admin/feeds.html',
                           feeds=feeds, sources=sources,
                           categories=categories, languages=languages, countries=countries,
                           base_url=base_url,
                           feed_summary=feed_summary,
                           feed_split_counts=feed_split_counts,
                           feed_chnum_placeholder=feed_chnum_placeholder,
                           default_chnum_from_env=default_feed and default_feed.chnum_start is None and app_settings.env_global_chnum_start() is not None)


@admin_bp.route('/settings')
def settings():
    app_settings = AppSettings.get()
    request_base_url = request.host_url.rstrip('/')
    settings_needs_config = []
    _eff_url, _url_source, _url_needs_config = public_base_url_config(app_settings)
    if _url_needs_config:
        settings_needs_config.append({
            'key': 'public_base_url',
            'label': 'FastChannels Server URL',
            'anchor': 'settings-card-public-base-url',
        })
    if not (app_settings.channels_dvr_url or '').strip() and app_settings.env_channels_dvr_url() is None:
        settings_needs_config.append({
            'key': 'channels_dvr_url',
            'label': 'Channels DVR',
            'anchor': 'settings-card-channels-dvr',
        })
    if not (app_settings.timezone_name or '').strip():
        settings_needs_config.append({
            'key': 'timezone_name',
            'label': 'Time Zone',
            'anchor': 'settings-card-timezone',
        })
    _url_from_env = _url_source in {'FASTCHANNELS_SERVER_URL', 'PUBLIC_BASE_URL'}
    _no_port_warning = False
    if _eff_url and not _url_from_env:
        from urllib.parse import urlsplit as _urlsplit
        try:
            _p = _urlsplit(_eff_url if '://' in _eff_url else f'http://{_eff_url}')
            _no_port_warning = not bool(_p.port)
        except Exception:
            pass
    return render_template('admin/settings.html',
                           channels_dvr_url=app_settings.effective_channels_dvr_url() or '',
                           public_base_url=_eff_url,
                           timezone_name=app_settings.effective_timezone_name(),
                           timezone_name_from_db=(app_settings.timezone_name or '').strip(),
                           timezone_choices=timezone_choices(),
                           channels_dvr_url_from_env=(not (app_settings.channels_dvr_url or '').strip()) and app_settings.env_channels_dvr_url() is not None,
                           public_base_url_from_env=_url_from_env,
                           public_base_url_env_name=_url_source if _url_from_env else None,
                           public_base_url_needs_config=_url_needs_config,
                           public_base_url_no_port_warning=_no_port_warning,
                           settings_needs_config=settings_needs_config,
                           request_base_url=request_base_url,
                           detected_base_url=detected_base_url(),
                           gracenote_auto_fill=app_settings.gracenote_auto_fill if app_settings.gracenote_auto_fill is not None else True,
                           dvr_epg_auto_refresh=app_settings.dvr_epg_auto_refresh if app_settings.dvr_epg_auto_refresh is not None else True,
                           image_proxy_enabled=app_settings.image_proxy_enabled if app_settings.image_proxy_enabled is not None else True,
                           gracenote_contribution_url=app_settings.gracenote_contribution_url or '')


@admin_bp.route('/logs')
def logs():
    return render_template('admin/logs.html')


@admin_bp.route('/reports/channel-changes')
def channel_changes_report():
    from ..timezone_utils import current_zoneinfo
    settings = AppSettings.get()
    local_tz = current_zoneinfo(settings.effective_timezone_name())
    now = datetime.now(local_tz)
    window_options = {
        '1d': 1,
        '3d': 3,
        '7d': 7,
        '30d': 30,
    }
    selected_window = (request.args.get('window') or '1d').strip().lower()
    if selected_window not in window_options:
        selected_window = '1d'
    window_days = window_options[selected_window]
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    window_start = today_start - timedelta(days=window_days - 1)

    new_rows = (
        db.session.query(Channel, Source)
        .join(Source, Source.id == Channel.source_id)
        .filter(Channel.created_at >= window_start)
        .order_by(Channel.created_at.desc(), Source.display_name.asc(), Channel.name.asc())
        .all()
    )

    inferred_lost_rows = (
        db.session.query(Channel, Source)
        .join(Source, Source.id == Channel.source_id)
        .filter(
            Channel.is_active == False,
            Channel.went_inactive_at >= window_start,
        )
        .order_by(Channel.went_inactive_at.desc(), Source.display_name.asc(), Channel.name.asc())
        .all()
    )

    at_risk_rows = (
        db.session.query(Channel, Source)
        .join(Source, Source.id == Channel.source_id)
        .filter(
            Channel.is_active == True,
            Channel.missed_scrapes > 0,
        )
        .order_by(Channel.missed_scrapes.desc(), Channel.last_seen_at.asc(), Source.display_name.asc(), Channel.name.asc())
        .all()
    )

    returned_rows = (
        db.session.query(Channel, Source)
        .join(Source, Source.id == Channel.source_id)
        .filter(
            Channel.is_active == True,
            Channel.returned_at >= window_start,
        )
        .order_by(Channel.returned_at.desc(), Source.display_name.asc(), Channel.name.asc())
        .all()
    )

    # Pending-review queue: enabled slots whose upstream content swapped (e.g. a
    # Vizio FEATURED slot rotating to a different show under the same channelId).
    # identity_changed_at is cleared the moment the user edits Gracenote, so this
    # naturally drains as swaps are reviewed — it's a "needs attention" list, not
    # a complete history.
    content_swap_rows = (
        db.session.query(Channel, Source)
        .join(Source, Source.id == Channel.source_id)
        .filter(Channel.identity_changed_at >= window_start)
        .order_by(Channel.identity_changed_at.desc(), Source.display_name.asc(), Channel.name.asc())
        .all()
    )

    def _group_counts(rows):
        counts: dict[str, int] = defaultdict(int)
        for _channel, source in rows:
            counts[source.display_name] += 1
        return sorted(counts.items(), key=lambda item: (-item[1], item[0].casefold()))

    def _daily_counts(rows, attr_name: str):
        counts: dict[str, int] = defaultdict(int)
        for channel, _source in rows:
            dt = getattr(channel, attr_name, None)
            if not dt:
                continue
            counts[dt.date().isoformat()] += 1
        return sorted(counts.items(), key=lambda item: item[0], reverse=True)

    # Per-source health summary
    all_sources = Source.query.filter(Source.is_enabled == True).order_by(Source.display_name).all()
    source_health = []
    for src in all_sources:
        active = Channel.query.filter_by(source_id=src.id, is_active=True).count()
        if not active:
            continue
        at_risk = Channel.query.filter_by(source_id=src.id, is_active=True).filter(Channel.missed_scrapes > 0).count()
        source_health.append({
            'display_name': src.display_name,
            'active': active,
            'at_risk': at_risk,
            'last_scraped_at': src.last_scraped_at,
            'scrape_interval': src.scrape_interval,
        })

    net_change = len(new_rows) - len(inferred_lost_rows)

    return render_template(
        'admin/channel_changes_report.html',
        now=now,
        today_start=today_start,
        window_start=window_start,
        selected_window=selected_window,
        window_days=window_days,
        window_options=window_options,
        new_rows=new_rows,
        new_counts=_group_counts(new_rows),
        new_daily_counts=_daily_counts(new_rows, 'created_at'),
        inferred_lost_rows=inferred_lost_rows,
        inferred_lost_counts=_group_counts(inferred_lost_rows),
        inferred_lost_daily_counts=_daily_counts(inferred_lost_rows, 'went_inactive_at'),
        at_risk_rows=at_risk_rows,
        at_risk_counts=_group_counts(at_risk_rows),
        returned_rows=returned_rows,
        returned_counts=_group_counts(returned_rows),
        content_swap_rows=content_swap_rows,
        content_swap_counts=_group_counts(content_swap_rows),
        content_swap_daily_counts=_daily_counts(content_swap_rows, 'identity_changed_at'),
        source_health=source_health,
        net_change=net_change,
    )


@admin_bp.route('/help')
def help():
    return render_template('admin/help.html')

"""Admin API for user-created playlist sources (an M3U URL, optionally paired
with an XMLTV guide). Each playlist is its own Source row named `m3u_<slug>`;
see app/scrapers/playlist.py."""
import logging
import os
import tempfile
from datetime import datetime, timezone
from collections import Counter

import requests
from flask import Blueprint, abort, jsonify, request

from ..extensions import db
from ..event_lifecycle import delete_event
from ..config_store import load_source_cache
from ..models import Source, Channel, Feed, Program
from ..scrapers.m3u_import import slugify
from ..scrapers.playlist import (
    DEFAULT_MAX_CHANNELS, MAX_SLUG_LEN, SOURCE_PREFIX, PlaylistError, PlaylistScraper,
    SNAPSHOT_CACHE_KEY, _download, fetch_playlist, is_playlist_source, playlist_summary, validate_url,
)
from ..scrapers.xmltv_import import iter_xmltv
from .api_shared import _invalidate_and_refresh_xml
from .tasks import trigger_playlist_delete, trigger_scrape

logger = logging.getLogger(__name__)

playlists_bp = Blueprint('api_playlists', __name__)

# Below this many channels the dialog pre-selects "enable all now"; above it,
# "send to review". The user can pick either.
ENABLE_NOW_THRESHOLD = 100

# The preview only samples a guide to report how many channels it covers, so it
# gives up on anything big rather than hold the request open.
_PREVIEW_GUIDE_BYTES = 64 * 1024 * 1024


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({'User-Agent': 'Mozilla/5.0 (compatible; FastChannels/1.0)'})
    return s


def _playlist_source_or_404(source_id: int) -> Source:
    source = Source.query.get_or_404(source_id)
    if not is_playlist_source(source.name):
        # Built-in sources and Custom Channels are never managed (or deleted)
        # through this API.
        abort(404)
    return source


def _clean_groups(value) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(g) for g in value]


def _clean_group_lifecycle(value) -> dict:
    if not isinstance(value, dict):
        raise PlaylistError('Group settings must be an object')
    cleaned = {}
    for group, policy in value.items():
        if not isinstance(group, str) or not isinstance(policy, dict):
            raise PlaylistError('Invalid group setting')
        if len(group) > 256 or policy.get('type') not in ('event', 'linear'):
            raise PlaylistError('Group type must be Event or Linear')
        if policy['type'] == 'event':
            limit = policy.get('delete_after_misses')
            if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 30:
                raise PlaylistError('Event deletion limit must be 1 to 30 missed imports')
            cleaned[group] = {'type': 'event', 'delete_after_misses': limit}
        else:
            limit = policy.get('disable_after_misses')
            if isinstance(limit, bool) or not isinstance(limit, int) or not 0 <= limit <= 30:
                raise PlaylistError('Linear disable limit must be 0 to 30 missed imports')
            if limit:
                cleaned[group] = {'type': 'linear', 'disable_after_misses': limit}
    return cleaned


def _guide_channel_ids(session: requests.Session, epg_url: str, user_agent: str | None):
    """The channel ids (and their display names) a guide has at least one
    programme for, or None if it is too big to read from a request. A
    <channel> entry with no programmes doesn't count — it gives no guide."""
    fd, path = tempfile.mkstemp(prefix='fc-xmltv-preview-', suffix='.tmp')
    try:
        with os.fdopen(fd, 'wb') as fp:
            try:
                _download(session, epg_url, 'Guide', _PREVIEW_GUIDE_BYTES, user_agent, dest=fp,
                          retry_rate_limit=False)
            except PlaylistError as exc:
                if 'larger than' in str(exc):
                    return None
                raise
        display_names: dict[str, list[str]] = {}
        ids = set()
        for tag, el in iter_xmltv(path):
            if tag == 'channel':
                display_names[(el.get('id') or '').casefold()] = [
                    dn.text.strip().casefold() for dn in el.findall('display-name') if dn.text
                ]
            else:
                ids.add((el.get('channel') or '').casefold())
        names = {name for cid in ids for name in display_names.get(cid, ())}
        return ids, names
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def _imported_guide_stats(source: Source) -> dict:
    """Guide coverage from what the last import stored, instead of
    downloading the guide again."""
    active = db.session.query(Channel.id).filter(
        Channel.source_id == source.id, Channel.is_active == True)
    with_programs = (db.session.query(db.func.count(db.distinct(Program.channel_id)))
                     .filter(Program.channel_id.in_(active.scalar_subquery()),
                             Program.end_time > datetime.now(timezone.utc)).scalar())
    return {'checked': True, 'matched': with_programs or 0, 'total': active.count(),
            'from_import': True, 'error': None, 'url_detected': ''}


def _preview_response(summary: dict, guide: dict, saved_groups, checked_at: str | None = None):
    return jsonify({
        **{k: v for k, v in summary.items() if k not in ('guide_url_detected', 'm3u_url', 'checked_at')},
        'saved_groups':   saved_groups,
        'guide':          guide,
        'checked_at':     checked_at,
        'max_channels':   DEFAULT_MAX_CHANNELS,
        'enable_now_threshold': ENABLE_NOW_THRESHOLD,
    })


@playlists_bp.route('/playlists/preview', methods=['POST'])
def preview_playlist():
    """Drives the add/edit dialog: group checkboxes, counts, and the guide
    match rate. Saves nothing.

    stored=true (edit mode) answers from what the last scrape recorded and
    contacts nothing; 404 when there is no record yet. A live check of a saved
    playlist reuses the imported guide coverage unless the guide URL changed,
    so it costs one playlist download rather than two."""
    data = request.get_json() or {}
    m3u_url = (data.get('m3u_url') or '').strip()
    epg_url = (data.get('epg_url') or '').strip()
    user_agent = None
    saved_groups = None
    source = None

    # Edit mode: fall back to what's saved, so the dialog never has to be sent
    # (or re-send) the stored URLs.
    if data.get('source_id'):
        source = _playlist_source_or_404(int(data['source_id']))
        cfg = source.config or {}
        m3u_url = m3u_url or cfg.get('m3u_url') or ''
        if 'epg_url' not in data:
            epg_url = cfg.get('epg_url') or ''
        user_agent = (cfg.get('user_agent') or '').strip() or None
        saved_groups = cfg.get('groups_include') or []
        if data.get('stored'):
            snapshot = load_source_cache(source.id).get(SNAPSHOT_CACHE_KEY)
            if not snapshot or snapshot.get('m3u_url') != cfg.get('m3u_url'):
                return jsonify({'error': 'No stored playlist data yet'}), 404
            guide = (_imported_guide_stats(source) if cfg.get('epg_url')
                     else {'checked': False, 'matched': None, 'error': None, 'url_detected': ''})
            return _preview_response(snapshot, guide, saved_groups, snapshot.get('checked_at'))

    session = _session()
    try:
        playlist = fetch_playlist(session, m3u_url, user_agent, retry_rate_limit=False)
        entries = playlist.entries
        summary = playlist_summary(playlist)

        guide_url = epg_url or playlist.guide_url or ''
        guide = {'url_detected': playlist.guide_url or '', 'checked': False, 'matched': None, 'error': None}
        if source is not None and guide_url and guide_url == (source.config or {}).get('epg_url'):
            guide = {**_imported_guide_stats(source), 'url_detected': playlist.guide_url or ''}
        elif guide_url:
            try:
                declared = _guide_channel_ids(session, validate_url(guide_url, 'Guide URL'), user_agent)
                if declared is not None:
                    ids, names = declared
                    guide['checked'] = True
                    guide['matched'] = sum(
                        1 for e in entries
                        if (e.attr('tvg-id').casefold() in ids if e.attr('tvg-id')
                            else e.name.casefold() in names)
                    )
            except PlaylistError as exc:
                guide['error'] = str(exc)
            except Exception as exc:
                guide['error'] = f'Guide could not be read: {type(exc).__name__}'
    except PlaylistError as exc:
        return jsonify({'error': str(exc)}), 422

    return _preview_response(summary, guide, saved_groups)


def _unique_source_name(display_name: str) -> str:
    slug = slugify(display_name, MAX_SLUG_LEN).replace('.', '-') or 'playlist'
    # '.' separates source from channel in a tvg-id, so it can't be in the name.
    candidate = SOURCE_PREFIX + slug
    n = 2
    while Source.query.filter_by(name=candidate).first():
        candidate = f'{SOURCE_PREFIX}{slug}-{n}'
        n += 1
    return candidate


def _apply_dialog_fields(config: dict, data: dict) -> dict:
    """Validate and copy the fields the playlist dialog owns into `config`."""
    config = dict(config)
    if 'm3u_url' in data:
        config['m3u_url'] = validate_url(data.get('m3u_url'), 'Playlist URL')
    if 'epg_url' in data:
        epg_url = (data.get('epg_url') or '').strip()
        if epg_url:
            config['epg_url'] = validate_url(epg_url, 'Guide URL')
        else:
            config.pop('epg_url', None)
    if 'fill_missing_epg' in data:
        if not isinstance(data['fill_missing_epg'], bool):
            raise PlaylistError('Fill missing guide data must be true or false')
        config['fill_missing_epg'] = data['fill_missing_epg']
    if 'groups_include' in data:
        groups = _clean_groups(data.get('groups_include'))
        if groups:
            config['groups_include'] = groups
        else:
            config.pop('groups_include', None)
    if 'group_lifecycle' in data:
        policies = _clean_group_lifecycle(data['group_lifecycle'])
        if policies:
            config['group_lifecycle'] = policies
        else:
            config.pop('group_lifecycle', None)
    if 'default_language' in data:
        lang = (data.get('default_language') or 'en').strip().lower()
        if not (2 <= len(lang) <= 3 and lang.isalpha()):
            raise PlaylistError('Language must be a 2- or 3-letter code, e.g. en')
        config['default_language'] = lang
    if 'default_country' in data:
        country = (data.get('default_country') or 'US').strip().upper()
        if not (len(country) == 2 and country.isalpha()):
            raise PlaylistError('Country must be a 2-letter code, e.g. US')
        config['default_country'] = country
    return config


def _stored_groups(source: Source) -> list[dict]:
    """Groups this playlist's stored channels belong to, with channel counts.

    The dialog lists groups from a live check of the playlist, so a group that
    has dropped out upstream would vanish from it while its channels are still
    here. These let the dialog keep showing it, so a rule can still be set."""
    counts: Counter = Counter()
    active: Counter = Counter()
    rows = (db.session.query(Channel.playlist_group, Channel.tags, Channel.is_active)
            .filter(Channel.source_id == source.id).all())
    for group, tags, is_active in rows:
        if group is None:
            group = (tags or '').split(',', 1)[0]
        counts[group] += 1
        if is_active:
            active[group] += 1
    return [{'name': name, 'count': n, 'active': active[name]} for name, n in sorted(counts.items())]


def _playlist_dict(source: Source, include_stored_groups: bool = False) -> dict:
    cfg = source.config or {}
    d = source.to_dict()
    d.update({
        'm3u_url':          cfg.get('m3u_url') or '',
        'epg_url':          cfg.get('epg_url') or '',
        'fill_missing_epg': cfg.get('fill_missing_epg', True),
        'groups_include':   cfg.get('groups_include') or [],
        'group_lifecycle':  cfg.get('group_lifecycle') or {},
        'default_language': cfg.get('default_language') or 'en',
        'default_country':  cfg.get('default_country') or 'US',
    })
    if include_stored_groups:
        d['stored_groups'] = _stored_groups(source)
    return d


@playlists_bp.route('/playlists', methods=['POST'])
def create_playlist():
    data = request.get_json() or {}
    display_name = (data.get('name') or '').strip()
    if not display_name:
        return jsonify({'error': 'name is required'}), 400
    if len(display_name) > 100:
        return jsonify({'error': 'name is too long'}), 400
    try:
        config = _apply_dialog_fields({}, {
            'm3u_url':          data.get('m3u_url'),
            'epg_url':          data.get('epg_url'),
            'fill_missing_epg': data.get('fill_missing_epg', True),
            'groups_include':   data.get('groups_include'),
            'group_lifecycle':  data.get('group_lifecycle', {}),
            'default_language': data.get('default_language'),
            'default_country':  data.get('default_country'),
        })
    except PlaylistError as exc:
        return jsonify({'error': str(exc)}), 422

    enable_now = bool(data.get('enable_now'))
    if enable_now:
        # One-shot: the worker clears this and flips the source to 'review'
        # once the first import has committed channels.
        config['first_import_pending'] = True

    source = Source(
        name=_unique_source_name(display_name),
        display_name=display_name,
        scrape_interval=PlaylistScraper.scrape_interval,
        config=config,
        epg_only=False,
        is_enabled=True,
        new_channel_policy='enabled' if enable_now else 'review',
    )
    db.session.add(source)
    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        if 'database is locked' in str(exc).lower():
            return jsonify({'error': 'Database busy — a scrape job is running. Try again in a moment.'}), 503
        raise
    logger.info('[playlists] created %s (%s)', source.name, display_name)
    trigger_scrape(source.name, force_full=True)
    return jsonify(_playlist_dict(source)), 201


@playlists_bp.route('/playlists/<int:source_id>', methods=['GET'])
def get_playlist(source_id):
    return jsonify(_playlist_dict(_playlist_source_or_404(source_id), include_stored_groups=True))


def _remove_groups(source: Source, config: dict, groups: list[str]) -> int:
    """Forget groups the user removed in the dialog: drop their selection and
    rule from `config` (in place) and delete their stored channels. Returns the
    number of channels deleted. A group that later reappears upstream imports
    as new channels."""
    gone = {g.casefold() for g in groups}
    if not gone:
        return 0
    if config.get('groups_include'):
        kept = [g for g in config['groups_include'] if g.casefold() not in gone]
        if kept:
            config['groups_include'] = kept
        else:
            config.pop('groups_include', None)
    if config.get('group_lifecycle'):
        kept = {g: p for g, p in config['group_lifecycle'].items() if g.casefold() not in gone}
        if kept:
            config['group_lifecycle'] = kept
        else:
            config.pop('group_lifecycle', None)

    deleted_ids = set()
    for ch in Channel.query.filter_by(source_id=source.id).all():
        group = ch.playlist_group
        if group is None:
            group = (ch.tags or '').split(',', 1)[0]
        if group.casefold() in gone:
            deleted_ids.add(ch.id)
            delete_event(ch)
    if deleted_ids:
        # Feeds can hold channel ids directly; SQLite may reuse a deleted id.
        for feed in Feed.query.all():
            filters = dict(feed.filters or {})
            changed = False
            for key in ('channel_ids', 'excluded_channel_ids', 'pinned_channel_ids'):
                ids = filters.get(key)
                if isinstance(ids, list) and deleted_ids.intersection(ids):
                    filters[key] = [cid for cid in ids if cid not in deleted_ids]
                    changed = True
            if changed:
                feed.filters = filters
        logger.info('[%s] removed groups %s: deleted %d channels',
                    source.name, sorted(groups), len(deleted_ids))
    return len(deleted_ids)


@playlists_bp.route('/playlists/<int:source_id>', methods=['PUT'])
def update_playlist(source_id):
    source = _playlist_source_or_404(source_id)
    data = request.get_json() or {}
    old = dict(source.config or {})
    try:
        config = _apply_dialog_fields(old, data)
    except PlaylistError as exc:
        return jsonify({'error': str(exc)}), 422

    if 'name' in data:
        display_name = (data.get('name') or '').strip()
        if not display_name or len(display_name) > 100:
            return jsonify({'error': 'name must be 1-100 characters'}), 400
        # display_name only. Source.name is permanent — it is in every tvg-id
        # and play URL already handed to clients.
        source.display_name = display_name

    # A changed group rule starts its own miss count. Otherwise an already
    # missing channel could hit a new disable or deletion limit immediately.
    old_policies = old.get('group_lifecycle') or {}
    new_policies = config.get('group_lifecycle') or {}
    changed_groups = {
        group.casefold() for group, policy in new_policies.items()
        if policy != old_policies.get(group)
    }
    if changed_groups:
        for ch in Channel.query.filter_by(source_id=source.id).all():
            group = ch.playlist_group
            if group is None:
                group = (ch.tags or '').split(',', 1)[0]
            if group.casefold() in changed_groups:
                ch.missed_scrapes = 0

    removed = _remove_groups(source, config, _clean_groups(data.get('remove_groups')))

    source.config = config
    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        if 'database is locked' in str(exc).lower():
            return jsonify({'error': 'Database busy — a scrape job is running. Try again in a moment.'}), 503
        raise

    rescrape = source.is_enabled and any(
        old.get(key) != config.get(key)
        for key in ('m3u_url', 'epg_url', 'groups_include', 'group_lifecycle', 'default_language', 'default_country')
    )
    if rescrape:
        trigger_scrape(source.name, force_full=True)
    elif old.get('fill_missing_epg', True) != config.get('fill_missing_epg', True):
        _invalidate_and_refresh_xml()
    if removed:
        _invalidate_and_refresh_xml()
    d = _playlist_dict(source)
    d['scrape_queued'] = rescrape
    d['removed_channels'] = removed
    return jsonify(d)


@playlists_bp.route('/playlists/<int:source_id>', methods=['DELETE'])
def delete_playlist(source_id):
    source = _playlist_source_or_404(source_id)
    trigger_playlist_delete(source.id)
    return jsonify({'status': 'queued'}), 202

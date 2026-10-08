"""Admin API for user-created playlist sources (an M3U URL, optionally paired
with an XMLTV guide). Each playlist is its own Source row named `m3u_<slug>`;
see app/scrapers/playlist.py."""
import logging
import os
import tempfile
from collections import Counter

import requests
from flask import Blueprint, abort, jsonify, request

from ..extensions import db
from ..models import Source
from ..scrapers.m3u_import import slugify
from ..scrapers.playlist import (
    DEFAULT_MAX_CHANNELS, MAX_SLUG_LEN, SOURCE_PREFIX, PlaylistError, PlaylistScraper,
    _download, fetch_playlist, is_playlist_source, validate_url,
)
from ..scrapers.xmltv_import import iter_xmltv
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


def _guide_channel_ids(session: requests.Session, epg_url: str, user_agent: str | None):
    """The channel ids (and their display names) a guide has at least one
    programme for, or None if it is too big to read from a request. A
    <channel> entry with no programmes doesn't count — it gives no guide."""
    fd, path = tempfile.mkstemp(prefix='fc-xmltv-preview-', suffix='.tmp')
    try:
        with os.fdopen(fd, 'wb') as fp:
            try:
                _download(session, epg_url, 'Guide', _PREVIEW_GUIDE_BYTES, user_agent, dest=fp)
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


@playlists_bp.route('/playlists/preview', methods=['POST'])
def preview_playlist():
    """Fetch and parse a playlist without saving anything. Drives the add/edit
    dialog: group checkboxes, counts, and the guide match rate."""
    data = request.get_json() or {}
    m3u_url = (data.get('m3u_url') or '').strip()
    epg_url = (data.get('epg_url') or '').strip()
    user_agent = None
    saved_groups = None

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

    session = _session()
    try:
        playlist = fetch_playlist(session, m3u_url, user_agent)
        entries = playlist.entries

        groups = Counter(e.group for e in entries)
        group_rows = [
            {'name': name, 'count': count}
            for name, count in sorted(groups.items(), key=lambda kv: (kv[0] == '', kv[0].casefold()))
        ]

        guide_url = epg_url or playlist.guide_url or ''
        guide = {'url_detected': playlist.guide_url or '', 'checked': False, 'matched': None, 'error': None}
        if guide_url:
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

    return jsonify({
        'channel_count':  len(entries),
        'skipped':        playlist.skipped,
        'groups':         group_rows,
        'saved_groups':   saved_groups,
        'with_tvg_id':    sum(1 for e in entries if e.attr('tvg-id')),
        'with_gracenote': sum(1 for e in entries if e.attr('tvc-guide-stationid')),
        'with_headers':   sum(1 for e in entries if e.headers),
        'with_drm':       sum(1 for e in entries if e.has_drm_props),
        'guide':          guide,
        'max_channels':   DEFAULT_MAX_CHANNELS,
        'enable_now_threshold': ENABLE_NOW_THRESHOLD,
    })


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
    if 'groups_include' in data:
        groups = _clean_groups(data.get('groups_include'))
        if groups:
            config['groups_include'] = groups
        else:
            config.pop('groups_include', None)
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


def _playlist_dict(source: Source) -> dict:
    cfg = source.config or {}
    d = source.to_dict()
    d.update({
        'm3u_url':          cfg.get('m3u_url') or '',
        'epg_url':          cfg.get('epg_url') or '',
        'groups_include':   cfg.get('groups_include') or [],
        'default_language': cfg.get('default_language') or 'en',
        'default_country':  cfg.get('default_country') or 'US',
    })
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
            'groups_include':   data.get('groups_include'),
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
    return jsonify(_playlist_dict(_playlist_source_or_404(source_id)))


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
        for key in ('m3u_url', 'epg_url', 'groups_include', 'default_language', 'default_country')
    )
    if rescrape:
        trigger_scrape(source.name, force_full=True)
    d = _playlist_dict(source)
    d['scrape_queued'] = rescrape
    return jsonify(d)


@playlists_bp.route('/playlists/<int:source_id>', methods=['DELETE'])
def delete_playlist(source_id):
    source = _playlist_source_or_404(source_id)
    trigger_playlist_delete(source.id)
    return jsonify({'status': 'queued'}), 202

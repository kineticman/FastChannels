import logging
import time as _time

logger = logging.getLogger(__name__)
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request
from sqlalchemy.exc import OperationalError
from ..extensions import db
from ..models import Source, Channel
from .tasks import (
    trigger_bulk_channel_update,
    trigger_bulk_channel_review,
)
from .admin import _default_feed_chnum_map_full
from .api_shared import _apply_channel_filters, _apply_gracenote_update, _invalidate_and_refresh_xml

channels_bp = Blueprint('api_channels', __name__)


@channels_bp.route('/channels')
def list_channels():
    page     = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    q        = Channel.query.join(Source)
    if request.args.get('feed_eligible') in ('1', 'true', 'yes'):
        q = q.filter(
            Channel.is_active == True,
            Channel.is_enabled == True,
            Source.is_enabled == True,
            Source.epg_only == False,
            Channel.stream_url != None,
        )
    if s := request.args.get('source'):
        q = q.filter(Source.name == s)
    if c := request.args.get('category'):
        q = q.filter(Channel.category.ilike(f'%{c}%'))
    if search := request.args.get('search'):
        q = q.filter(Channel.name.ilike(f'%{search}%'))
    pag = q.order_by(Channel.name).paginate(page=page, per_page=per_page, error_out=False)
    if request.args.get('slim') in ('1', 'true'):
        items = [{'id': ch.id, 'name': ch.name, 'source_name': ch.source.name,
                  'category': ch.category, 'language': ch.language,
                  'country': ch.country, 'gracenote_id': ch.gracenote_id}
                 for ch in pag.items]
    else:
        items = [ch.to_dict() for ch in pag.items]
    return jsonify({
        'channels': items,
        'total': pag.total, 'page': page, 'pages': pag.pages,
    })


@channels_bp.route('/channels/bulk', methods=['POST'])
def bulk_update_channels():
    data    = request.get_json() or {}
    action  = data.get('action')
    filters = data.get('filters') or {}

    if action not in ('enable', 'disable', 'mark_reviewed'):
        return jsonify({'error': 'action must be enable, disable, or mark_reviewed'}), 400

    q = _apply_channel_filters(Channel.query.join(Source), filters)
    matched = q.count()
    if matched:
        if action == 'mark_reviewed':
            trigger_bulk_channel_review(filters)
        else:
            trigger_bulk_channel_update(filters, action == 'enable')
    return jsonify({'status': 'queued' if matched else 'idle', 'updated': matched})


@channels_bp.route('/channels/gracenote-bulk', methods=['POST'])
def bulk_update_channel_gracenote():
    data = request.get_json(force=True) or {}
    action = (data.get('action') or '').strip()
    ids = [int(v) for v in (data.get('ids') or []) if str(v).isdigit()]
    filters = data.get('filters') or {}

    if action not in ('set_auto', 'set_manual', 'set_off', 'clear_ids'):
        return jsonify({'error': 'Invalid action.'}), 400

    if ids:
        channels = Channel.query.filter(Channel.id.in_(ids)).all()
    else:
        channels = _apply_channel_filters(Channel.query.join(Source), filters).all()
    if not channels:
        return jsonify({'updated': 0})

    for ch in channels:
        current_id = (ch.gracenote_id or '').strip() or None
        current_mode = getattr(ch, 'gracenote_mode', None) or ('manual' if getattr(ch, 'gracenote_locked', False) and current_id else 'auto')
        if action == 'set_auto':
            _apply_gracenote_update(ch, current_id, 'auto')
        elif action == 'set_manual':
            # Lock whatever ID the channel already has; channels with no ID stay as-is (auto)
            if current_id:
                _apply_gracenote_update(ch, current_id, 'manual')
        elif action == 'set_off':
            _apply_gracenote_update(ch, None, 'off')
        elif action == 'clear_ids':
            # The only path that actually destroys a stored ID (mode 'off' now
            # preserves it). Keeps the prior mode-result behaviour: off stays
            # off, anything else resets to auto.
            ch.gracenote_id = None
            ch.gracenote_locked = False
            ch.gracenote_mode = 'off' if current_mode == 'off' else 'auto'

    db.session.commit()
    _invalidate_and_refresh_xml()
    return jsonify({'updated': len(channels)})


@channels_bp.route('/channels/<int:channel_id>', methods=['PATCH'])
def update_channel(channel_id):
    ch   = Channel.query.get_or_404(channel_id)
    data = request.get_json()

    def _apply_changes():
        """Apply all field mutations to ch. Re-runnable after a rollback."""
        # Resolve the requested lock target from CURRENT (pre-mutation) state,
        # and validate it before touching ch, so the check reflects true prior
        # DB state rather than an in-flight change the map-building algorithm
        # could itself silently resolve out from under us (it always assigns
        # unique numbers, bumping a "losing" contender to a fresh one instead
        # of ever producing a literal duplicate — so checking the resolved map
        # *after* mutating ch could miss a real collision that got auto-healed
        # away in the same computation).
        target_number = data['number'] if 'number' in data else ch.number
        target_pinned = data['number_pinned'] if 'number_pinned' in data else ch.number_pinned
        # Setting a number without explicitly managing the pin auto-pins it.
        if 'number' in data and data['number'] is not None and 'number_pinned' not in data:
            target_pinned = True
        if ('number' in data or 'number_pinned' in data) and target_pinned and target_number is not None:
            # Reject locking to a number another channel already has locked —
            # a direct contradiction between two explicit locks.
            conflict = Channel.query.filter(
                Channel.id != ch.id,
                Channel.number_pinned == True,
                Channel.number == target_number,
            ).first()
            if conflict:
                raise ValueError(
                    f'Channel number {target_number} is already locked by "{conflict.name}". '
                    'Unlock it first or choose a different number.'
                )
            # Also reject locking to a number that's simply someone else's
            # current auto-assigned slot in the Default feed — locking would
            # silently bump that channel to a different number with no
            # indication anywhere that it happened.
            holder_id = next(
                (cid for cid, num in _default_feed_chnum_map_full().items()
                 if num == target_number and cid != ch.id),
                None,
            )
            if holder_id is not None:
                holder = Channel.query.get(holder_id)
                raise ValueError(
                    f'Channel number {target_number} is currently in use by '
                    f'"{holder.name if holder else holder_id}" in the Default feed. '
                    'Choose a different number.'
                )

        for field in ('name', 'logo_url', 'logo_url_pinned', 'category', 'category_override', 'language', 'language_override', 'is_active', 'is_enabled', 'scrape_pinned', 'number', 'number_pinned', 'disable_reason', 'is_duplicate', 'user_note'):
            if field in data:
                setattr(ch, field, data[field])
        if target_pinned:
            ch.number_pinned = True
        # Any explicit enable/disable counts as reviewing a new channel — clear the
        # 'pending' marker so it leaves the "Needs review" filter.
        if 'is_enabled' in data:
            ch.review_state = 'approved'
        if data.get('review_action') == 'keep_disabled':
            ch.review_state = 'approved'
        if data.get('is_enabled') is True and 'is_active' not in data:
            ch.is_active = True
            if ch.disable_reason in ('Dead', 'VOD', 'NotAuthorized') or (ch.disable_reason or '').startswith('DRM'):
                ch.disable_reason = None
            ch.last_seen_at = datetime.now(timezone.utc)
            ch.missed_scrapes = 0
        if data.get('scrape_pinned') is True and not ch.is_active:
            ch.is_active = True
            ch.last_seen_at = datetime.now(timezone.utc)
        if 'gracenote_id' in data or 'gracenote_mode' in data:
            _apply_gracenote_update(ch, data.get('gracenote_id'), data.get('gracenote_mode'))

    # Retry commit up to 3× (1s apart) if SQLite is briefly locked by a worker.
    # A failed flush poisons the session, so we must rollback and re-apply the
    # mutations on each attempt — a bare re-commit would raise PendingRollbackError.
    for _attempt in range(3):
        try:
            _apply_changes()
        except ValueError as exc:
            db.session.rollback()
            return jsonify({'error': str(exc)}), 422
        try:
            db.session.commit()
            break
        except OperationalError as _oe:
            db.session.rollback()
            if 'database is locked' not in str(_oe) or _attempt == 2:
                raise
            _time.sleep(1)
    _invalidate_and_refresh_xml()
    return jsonify(ch.to_dict())


@channels_bp.route('/channels/<int:channel_id>', methods=['DELETE'])
def delete_channel(channel_id):
    """Permanently delete a channel that has been marked inactive or pinned due to missed scrapes."""
    ch = Channel.query.get_or_404(channel_id)
    if ch.is_active and not ch.scrape_pinned:
        return jsonify({'error': 'Cannot delete an active channel'}), 409
    if (ch.missed_scrapes or 0) < 3:
        return jsonify({'error': 'Channel has not exceeded the missed-scrape threshold'}), 409
    db.session.delete(ch)
    db.session.commit()
    _invalidate_and_refresh_xml()
    return jsonify({'status': 'deleted', 'id': channel_id})


@channels_bp.route('/channels/<int:channel_id>/category-explain', methods=['GET'])
def channel_category_explain(channel_id):
    from ..scrapers.category_utils import explain_category, CANONICAL_CATEGORIES, category_for_channel
    ch = Channel.query.get_or_404(channel_id)
    if ch.category_override:
        explanation = {
            'source': 'user_override',
            'rule': 'user_override',
            'detail': f'Manually set to "{ch.category_override}" by a user — overrides all automatic logic.',
        }
    else:
        # explain_category works on the auto-resolved category (before override)
        _src_name = ch.source.name if ch.source else None
        auto_cat = category_for_channel(ch.name, ch.category, _src_name)
        explanation = explain_category(ch.name, auto_cat, _src_name)
    return jsonify({
        'channel_id': ch.id,
        'channel_name': ch.name,
        'category': ch.category,
        'category_override': ch.category_override,
        'canonical_categories': list(CANONICAL_CATEGORIES),
        **explanation,
    })


@channels_bp.route('/channels/<int:channel_id>/language-explain', methods=['GET'])
def channel_language_explain(channel_id):
    ch = Channel.query.get_or_404(channel_id)
    common_languages = [
        ('en', 'English'), ('es', 'Spanish'), ('fr', 'French'), ('de', 'German'),
        ('pt', 'Portuguese'), ('it', 'Italian'), ('zh', 'Chinese'), ('ja', 'Japanese'),
        ('ko', 'Korean'), ('ar', 'Arabic'), ('hi', 'Hindi'), ('ru', 'Russian'),
        ('pl', 'Polish'), ('nl', 'Dutch'), ('sv', 'Swedish'), ('tr', 'Turkish'),
    ]
    return jsonify({
        'channel_id': ch.id,
        'channel_name': ch.name,
        'language': ch.language or 'en',
        'language_override': ch.language_override,
        'common_languages': common_languages,
    })



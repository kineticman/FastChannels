"""'Sign in again' notices for TV-provider sign-ins.

A saved sign-in can lapse in a way only a person can fix: the provider has
no scripted sign-in (Spectrum/Cox, YouTube TV, Sling …), or it's a phone
sign-in, which can't renew itself. Wherever that's certain, the code calls
mark_signin_needed(); the network's row on Settings → TV Everywhere and a
dashboard banner then say so, until a newer sign-in supersedes it.

Stored in the TVE account's `tve_last_error[key]` (the same record
_record_tve_login_error writes, keyed the same way — requestor id for
A+E/Warner, 'nbc'/'fox'/'amcn'/'discovery', plus 'foxone' and 'espn' for
the Premium sources) with `needs_signin: True`. tve_network_status() only
shows a record newer than the row's last successful sign-in, so a sign-in
clears it with no extra code. Never call this for a network error, 429 or
5xx — those say nothing about the sign-in.
"""
from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

# Plays retry a lot; don't rewrite the same notice on every one.
_REWRITE_AFTER_SECONDS = 30 * 60


def mark_signin_needed(key: str | None, message: str) -> None:
    if not key:
        return
    try:
        from flask import has_app_context
        if not has_app_context():
            return
        from ..extensions import db
        from ..models import TVEAccount

        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if not account:
            return
        cfg = dict(account.config or {})
        errors = dict(cfg.get('tve_last_error') or {})
        prev = errors.get(key) or {}
        now = int(time.time())
        if (prev.get('needs_signin') and prev.get('message') == message[:300]
                and now - int(prev.get('at') or 0) < _REWRITE_AFTER_SECONDS):
            return
        errors[key] = {'message': message[:300], 'at': now, 'needs_signin': True}
        cfg['tve_last_error'] = errors
        account.config = cfg
        db.session.commit()
        logger.info('[tve] %s needs signing in again: %s', key, message[:200])
    except Exception as exc:  # noqa: BLE001 — a notice must never break the caller
        logger.debug('[tve] could not record sign-in notice for %s: %s', key, exc)


def pending_signins() -> list[dict]:
    """Sign-ins that need redoing, for the dashboard banner:
    [{'label', 'message', 'at', 'href'}]."""
    from ..models import Source, TVEAccount
    from .status import tve_network_status

    account = TVEAccount.query.filter_by(provider_id='mvpd').first()
    if not account:
        return []
    out = [
        {'label': n['label'], 'message': n.get('last_error_message'), 'at': n.get('last_error_at'),
         'href': '/admin/settings#settings-card-tve'}
        for n in tve_network_status(account) if n.get('needs_signin')
    ]
    errors = (account.config or {}).get('tve_last_error') or {}
    # Premium sources with their own sign-in: superseded by their own latest one.
    for key, source_name, label, success_key in (
        ('foxone', 'fox_one', 'FOX One', 'access_token_captured_at'),
        ('espn', 'espn', 'ESPN (TV provider)', 'adobe_signed_in_at'),
    ):
        err = errors.get(key) or {}
        if not err.get('needs_signin'):
            continue
        source = Source.query.filter_by(name=source_name).first()
        if not source or not source.is_enabled:
            continue
        signed_in_at = int((source.config or {}).get(success_key) or 0)
        if int(err.get('at') or 0) > signed_in_at:
            out.append({'label': label, 'message': err.get('message'), 'at': err.get('at'),
                        'href': '/admin/sources'})
    return out

"""Optional username/password login for the admin UI and its /api (#75).

Off until a password is saved under Settings > Advanced. Once on, every route
requires a signed-in session EXCEPT what DVRs, IPTV players and the bridges
fetch on their own — playlists, guides, the play/stream proxies, images and the
/watch page PrismCast's Chrome loads — since none of those clients can sign in.
So this keeps strangers out of the admin UI and its API (DB backup/restore,
source credentials, sign-ins); it does not make the playlists or streams private.

Recovery: FC_AUTH_DISABLED=1 in the container's environment bypasses the login
so a forgotten password can be changed or cleared from Settings.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
from datetime import timedelta
from urllib.parse import urlsplit

from flask import jsonify, redirect, request, session, url_for

logger = logging.getLogger(__name__)

SESSION_LIFETIME = timedelta(days=30)

# Fetched by clients that can't sign in (Channels DVR, Plex/Threadfin, TiviMate,
# PrismCast's Chrome, the FastChannels Player APK, ah4c's bmitune.sh), so they
# stay open. Everything not listed here — including any route added later —
# requires a session.
_PUBLIC_PREFIXES = (
    '/m3u',          # master playlists (/m3u, /m3u/gracenote, /m3u/fc-player/...)
    '/epg.xml',
    '/feeds/',       # per-feed playlists/guides; the feeds admin API is /api/feeds
    '/play/',        # stream resolve + manifest/segment/key/license proxies
    '/watch/',       # PrismCast's Chrome loads this page to capture DRM channels
    '/logos/',
    '/posters/',
    '/images/',
    '/static/',
    '/login',
    '/logout',
)
_PUBLIC_PATHS = frozenset({
    '/favicon.ico',
    # /watch's idle-stop ping (templates/watch.html) — the page is public, so its
    # heartbeat has to be too.
    '/api/fc-player/heartbeat',
})


def env_disabled() -> bool:
    return (os.environ.get('FC_AUTH_DISABLED') or '').strip().lower() in {'1', 'true', 'yes', 'on'}


def is_public_path(path: str) -> bool:
    return path in _PUBLIC_PATHS or path.startswith(_PUBLIC_PREFIXES)


def _credentials() -> tuple[str, str] | None:
    """(username, password_hash) when a login is configured, else None."""
    from .models import AppSettings
    row = AppSettings.get()
    pw_hash = (row.admin_password_hash or '').strip()
    if not pw_hash:
        return None
    return (row.admin_username or 'admin').strip() or 'admin', pw_hash


def _session_token(pw_hash: str) -> str:
    # Tied to the current password hash, so changing or clearing the password
    # signs every other browser out.
    from flask import current_app
    key = str(current_app.config['SECRET_KEY']).encode()
    return hmac.new(key, pw_hash.encode(), hashlib.sha256).hexdigest()


def auth_required() -> bool:
    return not env_disabled() and _credentials() is not None


def is_signed_in() -> bool:
    creds = _credentials()
    if creds is None:
        return False
    token = session.get('admin_auth') or ''
    return hmac.compare_digest(token, _session_token(creds[1]))


def sign_in(pw_hash: str) -> None:
    session.clear()
    session.permanent = True
    session['admin_auth'] = _session_token(pw_hash)


def check_login(username: str, password: str) -> str | None:
    """Returns the stored hash when username/password match, else None."""
    from werkzeug.security import check_password_hash
    creds = _credentials()
    if creds is None:
        return None
    stored_user, pw_hash = creds
    user_ok = hmac.compare_digest((username or '').strip().casefold(), stored_user.casefold())
    # Always run the hash check so a wrong username takes as long as a wrong password.
    pw_ok = check_password_hash(pw_hash, password or '')
    return pw_hash if (user_ok and pw_ok) else None


def safe_next(target: str | None) -> str:
    """Only same-site relative paths, so ?next= can't bounce to another host."""
    target = (target or '').strip()
    parts = urlsplit(target)
    if not target.startswith('/') or target.startswith('//') or parts.scheme or parts.netloc or '\\' in target:
        return '/admin/'
    return target


def enforce():
    """before_request hook: None lets the request through."""
    path = request.path or '/'
    if request.method == 'OPTIONS' or is_public_path(path):
        return None
    try:
        if not auth_required() or is_signed_in():
            return None
    except Exception:
        # Can't tell whether a login is configured (DB locked/unreadable): fail
        # closed. The columns are added by ensure_runtime_schema() before gunicorn
        # starts, so an upgrade alone can't land here.
        logger.exception('[admin-auth] could not read login settings; refusing %s', path)
        return jsonify({'error': 'Could not check sign-in; try again'}), 503
    if path.startswith('/api/') or request.method != 'GET':
        resp = jsonify({'error': 'Sign-in required', 'login_url': url_for('admin_login')})
        # templates/admin/_theme_head.html redirects open pages to /login on this.
        resp.headers['X-FastChannels-Auth'] = 'required'
        return resp, 401
    full = request.full_path if request.query_string else path
    return redirect(url_for('admin_login', next=full))


def register(app) -> None:
    from flask import render_template

    # Assigned, not setdefault: Flask's default config already has every one of
    # these keys. SameSite=Lax is the CSRF guard — other sites' form POSTs to
    # /api arrive without the session cookie.
    app.config['PERMANENT_SESSION_LIFETIME'] = SESSION_LIFETIME
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.config['SESSION_COOKIE_NAME'] = 'fastchannels_session'
    app.before_request(enforce)

    if env_disabled():
        logger.warning('[admin-auth] FC_AUTH_DISABLED is set — the admin login is bypassed')

    @app.route('/login', methods=['GET', 'POST'], endpoint='admin_login')
    def admin_login():
        next_url = safe_next(request.values.get('next'))
        if not auth_required() or is_signed_in():
            return redirect(next_url)
        error = None
        username = ''
        if request.method == 'POST':
            username = (request.form.get('username') or '').strip()
            pw_hash = check_login(username, request.form.get('password') or '')
            if pw_hash:
                sign_in(pw_hash)
                return redirect(next_url)
            logger.warning('[admin-auth] failed sign-in for %r from %s', username[:64], request.remote_addr)
            error = 'Incorrect username or password.'
        return render_template('login.html', error=error, username=username, next_url=next_url), (401 if error else 200)

    @app.route('/logout', methods=['GET', 'POST'], endpoint='admin_logout')
    def admin_logout():
        session.clear()
        return redirect(url_for('admin_login'))

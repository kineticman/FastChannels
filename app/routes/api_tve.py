import json
import logging
import os as _os
import time as _time

logger = logging.getLogger(__name__)
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request, current_app
from ..extensions import db
from ..models import Source, TVEAccount, SourceCache
from ..tve.adobe_pass import TVEAuthError, verify_mvpd_history
from ..tve.providers import ytdlp_adobe_mso_providers

tve_bp = Blueprint('api_tve', __name__)


def _get_tve_account(provider_id: str, display_name: str) -> TVEAccount:
    account = TVEAccount.query.filter_by(provider_id=provider_id).first()
    if account:
        return account
    account = TVEAccount(provider_id=provider_id, display_name=display_name, is_enabled=False, config={})
    db.session.add(account)
    db.session.flush()
    return account


@tve_bp.route('/settings/tve/mvpd', methods=['GET', 'POST'])
def tve_mvpd_settings():
    account = _get_tve_account('mvpd', 'TV Provider')
    if request.method == 'POST':
        data = request.get_json(force=True) or {}
        provider_choices = {p['id']: p for p in ytdlp_adobe_mso_providers()}
        selected_mso_id = (data.get('provider_id') or data.get('selected_mso_id') or 'Cox').strip()
        if selected_mso_id not in provider_choices:
            return jsonify({'error': 'Unsupported TVE provider.'}), 400
        selected_provider = provider_choices[selected_mso_id]
        if 'is_enabled' in data:
            account.is_enabled = bool(data['is_enabled'])
        if 'username' in data:
            account.username = (data.get('username') or '').strip() or None
        if data.get('clear_password'):
            account.password = None
        elif 'password' in data and (data.get('password') or ''):
            account.password = data.get('password')

        cfg = dict(account.config or {})
        cfg['selected_mso_id'] = selected_mso_id
        cfg['selected_mso_name'] = selected_provider['name']
        if 'auth_backend' in data:
            auth_backend = (data.get('auth_backend') or 'native').strip()
            if auth_backend not in {'native', 'yt_dlp'}:
                return jsonify({'error': 'Unsupported TVE auth backend.'}), 400
            cfg['auth_backend'] = auth_backend
        if 'adobe_mso_id' in data:
            cfg['adobe_mso_id'] = (data.get('adobe_mso_id') or selected_mso_id).strip() or selected_mso_id
        else:
            cfg.setdefault('adobe_mso_id', selected_mso_id)
        if 'yt_dlp_mso_id' in data:
            cfg['yt_dlp_mso_id'] = (data.get('yt_dlp_mso_id') or selected_mso_id).strip() or selected_mso_id
        else:
            cfg['yt_dlp_mso_id'] = selected_mso_id
        if data.get('clear_software_statement'):
            cfg.pop('software_statement', None)
        elif 'software_statement' in data:
            statement = (data.get('software_statement') or '').strip()
            if statement:
                cfg['software_statement'] = statement
        # Shared across every TVE source that needs a home market — currently
        # only fox_one (regional entitlement scoping), but this is where a
        # future zip-driven local-affiliate lookup (e.g. for nbc_tve) would
        # read from too, instead of each source collecting its own copy.
        if 'home_zip_code' in data:
            cfg['home_zip_code'] = (data.get('home_zip_code') or '').strip()
        account.config = cfg
        db.session.commit()
    return jsonify(account.to_safe_dict())


@tve_bp.route('/settings/tve/status')
def tve_network_status_route():
    from ..tve.status import tve_network_status
    account = TVEAccount.query.filter_by(provider_id='mvpd').first()
    return jsonify({'networks': tve_network_status(account)})


# Sources whose auth identity is TVE/Adobe-Pass-tied. Kept as one list so
# reset and any future TVE-wide admin action stay in sync automatically.
_TVE_SOURCE_NAMES = ('aenetworks_tve', 'fox_tve', 'fox_one', 'discovery_tve', 'amcn_tve', 'warner_tve', 'nbc_tve')


@tve_bp.route('/settings/tve/reset', methods=['POST'])
def tve_reset():
    """Wipe every saved TVE credential and cached sign-in so the next test
    starts genuinely cold — same four things a manual "test like a new user"
    reset needs: the account row (username/password + cached Adobe Pass
    tokens), each TVE source's own device-identity cache, the credential-tied
    SourceCache rows (adobe_auth:*, discovery_tve_session, nbc_entitlements/
    playback), and the on-disk Camoufox profile holding real Adobe SSO
    cookies. Deliberately narrower than a full wipe — leaves non-auth scraper
    cache (bootstrap configs, manifests, audit results) alone since that
    isn't a credential and just repopulates on the next scrape regardless of
    sign-in state.
    """
    import shutil

    account = TVEAccount.query.filter_by(provider_id='mvpd').first()
    # home_zip_code is a user preference (shared across every TVE source that
    # needs a home market — see /api/settings/tve/mvpd), not a credential or
    # cached sign-in artifact — a reset shouldn't make the user re-enter it
    # (code review, 2026-08-10, originally scoped to fox_one's own config
    # before home_zip_code moved onto the shared account).
    preserved_zip = ((account.config or {}).get('home_zip_code') or '').strip() if account else ''
    if account:
        db.session.delete(account)
        db.session.flush()
    if preserved_zip:
        db.session.add(TVEAccount(
            provider_id='mvpd', display_name='TV Provider', is_enabled=False,
            config={'home_zip_code': preserved_zip},
        ))

    sources = Source.query.filter(Source.name.in_(_TVE_SOURCE_NAMES)).all()
    source_ids = [s.id for s in sources]
    for s in sources:
        s.config = {}

    if source_ids:
        SourceCache.query.filter(
            SourceCache.source_id.in_(source_ids),
            db.or_(
                SourceCache.cache_key.like('adobe_auth:%'),
                SourceCache.cache_key.in_(('discovery_tve_session', 'nbc_entitlements', 'nbc_playback')),
            ),
        ).delete(synchronize_session=False)

    db.session.commit()

    profile_dir = '/data/browser_profiles/mvpd_tve'
    removed_profile = False
    try:
        if _os.path.isdir(profile_dir):
            shutil.rmtree(profile_dir)
            removed_profile = True
    except Exception as exc:  # noqa: BLE001
        logger.warning('[tve-reset] could not remove browser profile dir: %s', exc)

    try:
        import redis as _redis
        r = _redis.from_url(current_app.config['REDIS_URL'])
        keys = []
        for pattern in ('mvpd:browser-login:*', 'nbc-mvpd:browser-login:*', 'fox-mvpd:browser-login:*'):
            keys.extend(r.keys(pattern))
        if keys:
            r.delete(*keys)
    except Exception as exc:  # noqa: BLE001
        logger.warning('[tve-reset] could not flush redis keys: %s', exc)

    logger.info('[tve-reset] cleared TVE account, %d source configs, browser profile removed=%s', len(sources), removed_profile)
    return jsonify({'ok': True, 'sources_reset': len(sources), 'browser_profile_removed': removed_profile})


@tve_bp.route('/settings/tve/mvpd/test', methods=['POST'])
def test_tve_mvpd_settings():
    account = _get_tve_account('mvpd', 'TV Provider')
    if not account.has_credentials():
        return jsonify({'error': 'TVE username and password are required.'}), 400
    cfg = account.config or {}
    selected_mso_name = (cfg.get('selected_mso_name') or cfg.get('selected_mso_id') or 'Cox').strip()
    try:
        result = verify_mvpd_history(
            account,
            software_statement=(account.config or {}).get('software_statement'),
        )
        account.last_auth_status = 'ok'
        account.last_auth_message = f'{selected_mso_name} authorized History via Adobe Pass.'
        account.last_auth_at = datetime.now(timezone.utc)
        db.session.commit()
        return jsonify({'ok': True, 'account': account.to_safe_dict(), 'result': result})
    except TVEAuthError as exc:
        account.last_auth_status = 'error'
        account.last_auth_message = str(exc)[:1000]
        account.last_auth_at = datetime.now(timezone.utc)
        db.session.commit()
        return jsonify({'ok': False, 'error': str(exc), 'account': account.to_safe_dict()}), 502


@tve_bp.route('/settings/tve/foxone/signin', methods=['POST'])
def foxone_signin():
    """FOX One authenticates natively (a scripted Adobe Pass MVPD OAuth
    dance, see FoxOneScraper._authenticate_via_mvpd) rather than through
    Adobe Pass "second screen" pairing like every other TVE network — no
    browser needed, so unlike the rest of the "Sign in" buttons this is a
    plain synchronous request/response, not the streamed-screenshot modal.
    Only Cox has a scripted login wired up right now — other MVPDs fail
    with a clear error instead of silently trying Cox's login form.

    Deliberately calls _authenticate_via_mvpd directly instead of going
    through _ensure_access_token's cache-first path — a "Sign in" click
    should always exercise a live MVPD login, not silently short-circuit on
    a still-valid cached token (which would report success without actually
    testing anything, undermining the whole point of a manual sign-in).
    """
    from ..models import Source
    from ..scrapers.fox_one import FoxOneScraper
    from ..config_store import persist_source_config_updates

    account = _get_tve_account('mvpd', 'TV Provider')
    if not account.has_credentials():
        return jsonify({'error': 'TV provider username and password are required.'}), 400

    source = Source.query.filter_by(name='fox_one').first()
    scraper = FoxOneScraper(config=dict((source.config if source else {}) or {}))
    mso_id = scraper._account_mso_id(account)
    try:
        access_token, expires_at = scraper._authenticate_via_mvpd(
            mso_id, account.username, account.password, (account.config or {}).get('xfinity_cookie_jar'),
        )
    except Exception as exc:  # noqa: BLE001
        account.last_auth_status = 'error'
        account.last_auth_message = f'FOX One {mso_id} MVPD auth failed: {exc}'[:500]
        account.last_auth_at = datetime.now(timezone.utc)
        # Same 'tve_last_error' shape app.worker._record_tve_login_error writes
        # for the other networks — app.tve.status.tve_network_status() reads
        # this so a network that's never succeeded shows why, not just "Never".
        cfg = dict(account.config or {})
        errors = dict(cfg.get('tve_last_error') or {})
        errors['foxone'] = {'message': str(exc)[:300], 'at': int(_time.time())}
        cfg['tve_last_error'] = errors
        account.config = cfg
        db.session.commit()
        if source and scraper._pending_config_updates:
            persist_source_config_updates(source.id, scraper._pending_config_updates)
        return jsonify({'ok': False, 'error': str(exc)}), 502

    scraper._update_config('access_token', access_token)
    scraper._update_config('access_expires_at', expires_at)
    scraper._update_config('access_token_captured_at', int(_time.time()))
    if source:
        persist_source_config_updates(source.id, scraper._pending_config_updates)
    account.last_auth_status = 'ok'
    account.last_auth_message = f'FOX One access token obtained through {mso_id} MVPD.'
    account.last_auth_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({'ok': True})


# ── MVPD interactive browser sign-in (Adobe Pass "second screen" pairing) ──
# For MSOs whose login page blocks scripted clients outright (e.g. Sling's
# identity.sling.com returns HTTP 417 even to a browser-impersonating HTTP
# client — see app/tve/ytdlp_mvpd.py), a real, human-operated Camoufox tab
# completes the MSO's actual login page while this process polls Adobe for
# the resulting authn_token using the same reg_code — see the docstring on
# app.worker.run_mvpd_browser_login for the full mechanism. Same streamed-
# screenshot/forwarded-input pattern as the Sling FAST sign-in above, just
# not tied to a Source (the TVE account isn't one).

@tve_bp.route('/settings/tve/browser-login/requestors')
def mvpd_browser_login_requestors():
    from ..tve.mvpd_targets import REQUESTOR_CHOICES
    return jsonify({'requestors': REQUESTOR_CHOICES})


@tve_bp.route('/settings/tve/browser-login/start', methods=['POST'])
def mvpd_browser_login_start():
    from ..tve.mvpd_targets import resolve_requestor_target
    from .tasks import trigger_mvpd_browser_login

    account = _get_tve_account('mvpd', 'TV Provider')
    if not account.is_enabled:
        return jsonify({'error': 'Enable and save the TVE account first.'}), 400
    data = request.get_json(force=True) or {}
    requestor_id = (data.get('requestor_id') or '').strip().upper()
    if not requestor_id:
        return jsonify({'error': 'requestor_id is required.'}), 400
    cfg = account.config or {}
    mso_id = (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or 'Cox').strip()
    try:
        target = resolve_requestor_target(requestor_id)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    # Use target['requestor_id'] (the wire-protocol value Adobe actually
    # expects), not the raw admin-UI key — they differ for Warner's truTV
    # (see resolve_requestor_target's docstring). This keeps every use of
    # requestor_id inside run_mvpd_browser_login (AdobePassCoxClient, cached
    # authn_token storage, display labels) consistent with authorize_mvpd()'s
    # play-time cache lookup.
    started = trigger_mvpd_browser_login(
        target['requestor_id'], target['resource'], target['software_statement'], target['redirect_url'], mso_id,
    )
    return jsonify({'status': 'started' if started else 'already_running'})


@tve_bp.route('/settings/tve/browser-login/state')
def mvpd_browser_login_state():
    import base64
    import redis as _redis

    r = _redis.from_url(current_app.config['REDIS_URL'])
    raw_status = r.get('mvpd:browser-login:status')
    result = json.loads(raw_status) if raw_status else {'state': 'idle'}
    shot = r.get('mvpd:browser-login:screenshot')
    if shot:
        result['screenshot'] = base64.b64encode(shot).decode('ascii')
    hint = r.get('mvpd:browser-login:hint')
    if hint:
        result['hint'] = hint.decode('utf-8', 'replace') if isinstance(hint, bytes) else hint
    log_lines = r.lrange('tve:browser-login:log', -40, -1)
    if log_lines:
        result['activity_log'] = [l.decode('utf-8', 'replace') if isinstance(l, bytes) else l for l in log_lines]
    return jsonify(result)


@tve_bp.route('/settings/tve/browser-login/input', methods=['POST'])
def mvpd_browser_login_input():
    import redis as _redis

    data = request.get_json() or {}
    kind = data.get('type')
    if kind in ('click', 'mousemove', 'mousedown', 'mouseup'):
        try:
            payload = {'type': kind, 'x': float(data['x']), 'y': float(data['y'])}
        except (KeyError, TypeError, ValueError):
            return jsonify({'error': f'{kind} requires numeric x/y'}), 400
    elif kind == 'key':
        key = str(data.get('key') or '')
        if not key:
            return jsonify({'error': 'key requires a non-empty key'}), 400
        payload = {'type': 'key', 'key': key}
    else:
        return jsonify({'error': 'invalid input type'}), 400
    r = _redis.from_url(current_app.config['REDIS_URL'])
    r.rpush('mvpd:browser-login:input', json.dumps(payload))
    r.expire('mvpd:browser-login:input', 60)
    return jsonify({'status': 'ok'})


@tve_bp.route('/settings/tve/browser-login/stop', methods=['POST'])
def mvpd_browser_login_stop():
    from .tasks import stop_mvpd_browser_login
    stop_mvpd_browser_login()
    return jsonify({'status': 'stopping'})


# AMC Networks TVE / Discovery TVE standalone sign-in — neither has a
# dedicated Adobe Pass client to register up front, so these reuse the
# legacy flow's redis keys/job (see app.worker._run_amcn_or_discovery_
# standalone_login). Only /start is a distinct route; /state, /input and
# /stop are the SAME view functions as the legacy routes above, just
# registered under these prefixes too — the frontend's MVPD_LOGIN_FAMILIES
# builds state/input/stop URLs from each family's own `base`, so those need
# to resolve even though the underlying redis keys are shared.

@tve_bp.route('/settings/tve/amcn/browser-login/start', methods=['POST'])
def amcn_browser_login_start():
    from .tasks import trigger_amcn_browser_login

    account = _get_tve_account('mvpd', 'TV Provider')
    if not account.is_enabled:
        return jsonify({'error': 'Enable and save the TVE account first.'}), 400
    cfg = account.config or {}
    mso_id = (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or 'Cox').strip()
    started = trigger_amcn_browser_login(mso_id)
    return jsonify({'status': 'started' if started else 'already_running'})


@tve_bp.route('/settings/tve/discovery/browser-login/start', methods=['POST'])
def discovery_browser_login_start():
    from .tasks import trigger_discovery_browser_login

    account = _get_tve_account('mvpd', 'TV Provider')
    if not account.is_enabled:
        return jsonify({'error': 'Enable and save the TVE account first.'}), 400
    cfg = account.config or {}
    mso_id = (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or 'Cox').strip()
    started = trigger_discovery_browser_login(mso_id)
    return jsonify({'status': 'started' if started else 'already_running'})


tve_bp.add_url_rule('/settings/tve/amcn/browser-login/state', 'amcn_browser_login_state', mvpd_browser_login_state)
tve_bp.add_url_rule('/settings/tve/amcn/browser-login/input', 'amcn_browser_login_input', mvpd_browser_login_input, methods=['POST'])
tve_bp.add_url_rule('/settings/tve/amcn/browser-login/stop', 'amcn_browser_login_stop', mvpd_browser_login_stop, methods=['POST'])
tve_bp.add_url_rule('/settings/tve/discovery/browser-login/state', 'discovery_browser_login_state', mvpd_browser_login_state)
tve_bp.add_url_rule('/settings/tve/discovery/browser-login/input', 'discovery_browser_login_input', mvpd_browser_login_input, methods=['POST'])
tve_bp.add_url_rule('/settings/tve/discovery/browser-login/stop', 'discovery_browser_login_stop', mvpd_browser_login_stop, methods=['POST'])


# FOX One's "Sign in" button uses the plain synchronous /foxone/signin route
# above for Cox (unchanged — fast, no browser needed); this streamed-modal
# flow is only reached for other MSOs (YouTubeTV, Sling), which can't
# complete without a human — see app.worker.run_foxone_browser_login.
@tve_bp.route('/settings/tve/foxone/browser-login/start', methods=['POST'])
def foxone_browser_login_start():
    from .tasks import trigger_foxone_browser_login

    account = _get_tve_account('mvpd', 'TV Provider')
    if not account.is_enabled:
        return jsonify({'error': 'Enable and save the TVE account first.'}), 400
    cfg = account.config or {}
    mso_id = (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or 'Cox').strip()
    started = trigger_foxone_browser_login(mso_id)
    return jsonify({'status': 'started' if started else 'already_running'})


tve_bp.add_url_rule('/settings/tve/foxone/browser-login/state', 'foxone_browser_login_state', mvpd_browser_login_state)
tve_bp.add_url_rule('/settings/tve/foxone/browser-login/input', 'foxone_browser_login_input', mvpd_browser_login_input, methods=['POST'])
tve_bp.add_url_rule('/settings/tve/foxone/browser-login/stop', 'foxone_browser_login_stop', mvpd_browser_login_stop, methods=['POST'])


# Standalone "Sign in with Google" — captures a Google master_token directly
# against Google's own embedded Android device-setup page, independent of
# any specific TVE network's Adobe Pass SAML flow. See
# app.worker.run_google_signin's docstring for why this exists as its own
# dedicated flow. Its own redis-key namespace (google-signin:browser-login:*)
# rather than reusing the shared legacy one, so it can run/be watched
# independent of any specific network's own sign-in attempt.
@tve_bp.route('/settings/tve/google/browser-login/start', methods=['POST'])
def google_signin_start():
    from .tasks import trigger_google_signin

    account = _get_tve_account('mvpd', 'TV Provider')
    if not account.is_enabled:
        return jsonify({'error': 'Enable and save the TVE account first.'}), 400
    started = trigger_google_signin()
    return jsonify({'status': 'started' if started else 'already_running'})


@tve_bp.route('/settings/tve/google/browser-login/state')
def google_signin_state():
    import base64
    import redis as _redis

    r = _redis.from_url(current_app.config['REDIS_URL'])
    raw_status = r.get('google-signin:browser-login:status')
    result = json.loads(raw_status) if raw_status else {'state': 'idle'}
    shot = r.get('google-signin:browser-login:screenshot')
    if shot:
        result['screenshot'] = base64.b64encode(shot).decode('ascii')
    hint = r.get('google-signin:browser-login:hint')
    if hint:
        result['hint'] = hint.decode('utf-8', 'replace') if isinstance(hint, bytes) else hint
    log_lines = r.lrange('tve:browser-login:log', -40, -1)
    if log_lines:
        result['activity_log'] = [l.decode('utf-8', 'replace') if isinstance(l, bytes) else l for l in log_lines]
    return jsonify(result)


@tve_bp.route('/settings/tve/google/browser-login/input', methods=['POST'])
def google_signin_input():
    import redis as _redis

    data = request.get_json() or {}
    kind = data.get('type')
    if kind in ('click', 'mousemove', 'mousedown', 'mouseup'):
        try:
            payload = {'type': kind, 'x': float(data['x']), 'y': float(data['y'])}
        except (KeyError, TypeError, ValueError):
            return jsonify({'error': f'{kind} requires numeric x/y'}), 400
    elif kind == 'key':
        key = str(data.get('key') or '')
        if not key:
            return jsonify({'error': 'key requires a non-empty key'}), 400
        payload = {'type': 'key', 'key': key}
    else:
        return jsonify({'error': 'invalid input type'}), 400
    r = _redis.from_url(current_app.config['REDIS_URL'])
    r.rpush('google-signin:browser-login:input', json.dumps(payload))
    r.expire('google-signin:browser-login:input', 60)
    return jsonify({'status': 'ok'})


@tve_bp.route('/settings/tve/google/browser-login/stop', methods=['POST'])
def google_signin_stop():
    from .tasks import stop_google_signin
    stop_google_signin()
    return jsonify({'status': 'stopping'})


# ── NBC TVE browser sign-in (Adobe Pass v2 "second screen" pairing) ────────
# NBC TVE uses a different Adobe Pass generation (v2 JSON REST) than Warner/
# A+E's legacy XML protocol — see app.worker.run_nbc_browser_login and
# app/scrapers/nbc_tve.py's AdobePassV2Client. Separate Redis-key
# namespace (nbc-mvpd:*) so it doesn't collide with the legacy job above.

@tve_bp.route('/settings/tve/nbc/browser-login/start', methods=['POST'])
def nbc_browser_login_start():
    from .tasks import trigger_nbc_browser_login

    account = _get_tve_account('mvpd', 'TV Provider')
    if not account.is_enabled:
        return jsonify({'error': 'Enable and save the TVE account first.'}), 400
    cfg = account.config or {}
    mso_id = (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or 'Cox').strip()
    started = trigger_nbc_browser_login(mso_id)
    return jsonify({'status': 'started' if started else 'already_running'})


@tve_bp.route('/settings/tve/nbc/browser-login/state')
def nbc_browser_login_state():
    import base64
    import redis as _redis

    r = _redis.from_url(current_app.config['REDIS_URL'])
    raw_status = r.get('nbc-mvpd:browser-login:status')
    result = json.loads(raw_status) if raw_status else {'state': 'idle'}
    shot = r.get('nbc-mvpd:browser-login:screenshot')
    if shot:
        result['screenshot'] = base64.b64encode(shot).decode('ascii')
    hint = r.get('nbc-mvpd:browser-login:hint')
    if hint:
        result['hint'] = hint.decode('utf-8', 'replace') if isinstance(hint, bytes) else hint
    log_lines = r.lrange('tve:browser-login:log', -40, -1)
    if log_lines:
        result['activity_log'] = [l.decode('utf-8', 'replace') if isinstance(l, bytes) else l for l in log_lines]
    return jsonify(result)


@tve_bp.route('/settings/tve/nbc/browser-login/input', methods=['POST'])
def nbc_browser_login_input():
    import redis as _redis

    data = request.get_json() or {}
    kind = data.get('type')
    if kind in ('click', 'mousemove', 'mousedown', 'mouseup'):
        try:
            payload = {'type': kind, 'x': float(data['x']), 'y': float(data['y'])}
        except (KeyError, TypeError, ValueError):
            return jsonify({'error': f'{kind} requires numeric x/y'}), 400
    elif kind == 'key':
        key = str(data.get('key') or '')
        if not key:
            return jsonify({'error': 'key requires a non-empty key'}), 400
        payload = {'type': 'key', 'key': key}
    else:
        return jsonify({'error': 'invalid input type'}), 400
    r = _redis.from_url(current_app.config['REDIS_URL'])
    r.rpush('nbc-mvpd:browser-login:input', json.dumps(payload))
    r.expire('nbc-mvpd:browser-login:input', 60)
    return jsonify({'status': 'ok'})


@tve_bp.route('/settings/tve/nbc/browser-login/stop', methods=['POST'])
def nbc_browser_login_stop():
    from .tasks import stop_nbc_browser_login
    stop_nbc_browser_login()
    return jsonify({'status': 'stopping'})


# ── FOX Sports TVE browser sign-in ──────────────────────────────────────────
# api3.fox.com's own REST flow (app.worker.run_fox_browser_login /
# app/scrapers/fox_tve.py's _fox_sports_mvpd_token) — a third distinct Adobe
# Pass integration style. Separate Redis-key namespace (fox-mvpd:*).

@tve_bp.route('/settings/tve/fox/browser-login/start', methods=['POST'])
def fox_browser_login_start():
    from .tasks import trigger_fox_browser_login

    account = _get_tve_account('mvpd', 'TV Provider')
    if not account.is_enabled:
        return jsonify({'error': 'Enable and save the TVE account first.'}), 400
    cfg = account.config or {}
    mso_id = (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or 'Cox').strip()
    started = trigger_fox_browser_login(mso_id)
    return jsonify({'status': 'started' if started else 'already_running'})


@tve_bp.route('/settings/tve/fox/browser-login/state')
def fox_browser_login_state():
    import base64
    import redis as _redis

    r = _redis.from_url(current_app.config['REDIS_URL'])
    raw_status = r.get('fox-mvpd:browser-login:status')
    result = json.loads(raw_status) if raw_status else {'state': 'idle'}
    shot = r.get('fox-mvpd:browser-login:screenshot')
    if shot:
        result['screenshot'] = base64.b64encode(shot).decode('ascii')
    hint = r.get('fox-mvpd:browser-login:hint')
    if hint:
        result['hint'] = hint.decode('utf-8', 'replace') if isinstance(hint, bytes) else hint
    log_lines = r.lrange('tve:browser-login:log', -40, -1)
    if log_lines:
        result['activity_log'] = [l.decode('utf-8', 'replace') if isinstance(l, bytes) else l for l in log_lines]
    return jsonify(result)


@tve_bp.route('/settings/tve/fox/browser-login/input', methods=['POST'])
def fox_browser_login_input():
    import redis as _redis

    data = request.get_json() or {}
    kind = data.get('type')
    if kind in ('click', 'mousemove', 'mousedown', 'mouseup'):
        try:
            payload = {'type': kind, 'x': float(data['x']), 'y': float(data['y'])}
        except (KeyError, TypeError, ValueError):
            return jsonify({'error': f'{kind} requires numeric x/y'}), 400
    elif kind == 'key':
        key = str(data.get('key') or '')
        if not key:
            return jsonify({'error': 'key requires a non-empty key'}), 400
        payload = {'type': 'key', 'key': key}
    else:
        return jsonify({'error': 'invalid input type'}), 400
    r = _redis.from_url(current_app.config['REDIS_URL'])
    r.rpush('fox-mvpd:browser-login:input', json.dumps(payload))
    r.expire('fox-mvpd:browser-login:input', 60)
    return jsonify({'status': 'ok'})


@tve_bp.route('/settings/tve/fox/browser-login/stop', methods=['POST'])
def fox_browser_login_stop():
    from .tasks import stop_fox_browser_login
    stop_fox_browser_login()
    return jsonify({'status': 'stopping'})



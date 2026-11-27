"""
hbomax_login.py — headless Camoufox sign-in for the HBO Max source.

The web password login (auth.hbomax.com) is Arkose-gated, but a real Camoufox
browser clears Arkose transparently (the raw server-side POST does not — it
returns arkose.required, and the OTP/device-code endpoints are HMAC-walled with a
withheld key). So this drives the actual web sign-in form headlessly and harvests
the resulting cookie jar (the `st` session JWT, ~decade-long), which the scraper
then replays — the same cookie-jar pattern as the Xfinity TVE source.

Proven 2026-11-17; see dev/hbomax/BUILD_PLAN.md and dev/hbomax/camoufox_login_v4.py.

Flow: auth.hbomax.com -> dismiss the recurring "Legal Terms" consent modal (JS
click; its Agree button is type=submit inside #layer-root-modal and intercepts
pointer events) -> email step -> /auth/login?flow=login password step -> submit
each with Enter on the focused field -> poll until the `st` cookie is
non-anonymous.
"""
from __future__ import annotations

import base64
import json
import logging
import shutil
import time
import uuid

logger = logging.getLogger(__name__)

HBOMAX_LOGIN_STATUS_KEY = 'hbomax:login:status'
_STATUS_TTL = 420
_PROFILE_BASE = '/data/browser_profiles'
SIGNIN_URL = 'https://auth.hbomax.com/'
_LOGIN_TIMEOUT = 90.0


def _login_status(state: str, message: str = '') -> dict:
    return {'state': state, 'message': message, 'at': int(time.time())}


def _decode_anonymous(token: str):
    try:
        p = token.split('.')[1]
        p += '=' * (-len(p) % 4)
        return json.loads(base64.urlsafe_b64decode(p)).get('anonymous')
    except Exception:  # noqa: BLE001
        return None


def _capture_session(ctx) -> dict | None:
    """Return {'cookies': [...], 'device_id': ...} once the st cookie is a real
    (non-anonymous) user session, else None."""
    cookies = ctx.cookies()
    st = next((c for c in cookies if c['name'] == 'st'), None)
    if not st or _decode_anonymous(st['value']) is not False:
        return None
    device_id = next((c['value'] for c in cookies if c['name'] == 'GI_WEB_SDK_SONIC_DEVICE_ID'), None)
    return {
        'cookies': [{'name': c['name'], 'value': c['value']} for c in cookies],
        'device_id': device_id or str(uuid.uuid4()),
        'captured_at': int(time.time()),
    }


def _drive_login(page, ctx, email: str, password: str) -> dict | None:
    """Run the web sign-in form; return a captured session dict or None."""

    def kill_modal(appear_wait=6.0, clear_wait=8.0):
        end = time.time() + appear_wait
        while time.time() < end:
            if page.evaluate("""()=>!![...document.querySelectorAll('button')]"""
                             """.find(x=>x.textContent.trim()==='Agree')"""):
                break
            time.sleep(0.4)
        end = time.time() + clear_wait
        while time.time() < end:
            state = page.evaluate("""()=>{
                const b=[...document.querySelectorAll('button')].find(x=>x.textContent.trim()==='Agree');
                if(b){b.click();return 'clicked'}
                const r=document.querySelector('#layer-root-modal');
                return (r&&r.children.length)?'still':'clear';
            }""")
            if state == 'clicked':
                time.sleep(1.0)
            elif state == 'clear':
                return
            else:
                time.sleep(0.4)

    def type_verify(selector, value):
        el = page.wait_for_selector(selector, timeout=15000, state='visible')
        for _ in range(3):
            try:
                el.fill('')
                el.fill(value)
            except Exception:  # noqa: BLE001
                el.click()
                page.keyboard.press('Control+A')
                page.keyboard.press('Delete')
                page.keyboard.type(value, delay=45)
            try:
                if el.input_value() == value:
                    return el
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.6)
        return None

    page.goto(SIGNIN_URL, timeout=45000, wait_until='domcontentloaded')
    kill_modal()
    email_el = type_verify(
        'input[type=email],input[type=text],input[type=tel],input[inputmode=email]', email)
    if not email_el:
        raise RuntimeError('could not enter the email address')
    email_el.press('Enter')
    time.sleep(4)
    kill_modal()
    pw_el = type_verify('input[type=password]', password)
    if not pw_el:
        raise RuntimeError('could not reach the password step')
    pw_el.press('Enter')

    deadline = time.time() + _LOGIN_TIMEOUT
    while time.time() < deadline:
        time.sleep(2)
        try:
            session = _capture_session(ctx)
        except Exception:  # noqa: BLE001
            session = None
        if session:
            return session
    return None


def run_hbomax_login(source_id: int) -> None:
    """RQ job: sign in to HBO Max with Camoufox, store the cookie jar on the
    source, and rescrape. Status is published to HBOMAX_LOGIN_STATUS_KEY."""
    import redis

    from app.worker import flask_app
    from ..config_store import persist_source_config_updates
    from ..models import Source

    with flask_app.app_context():
        r = redis.from_url(flask_app.config['REDIS_URL'])

        def set_status(state: str, message: str = '') -> None:
            r.setex(HBOMAX_LOGIN_STATUS_KEY, _STATUS_TTL, json.dumps(_login_status(state, message)))

        src = Source.query.filter_by(name='hbomax').first()
        if not src:
            set_status('error', 'HBO Max source no longer exists.')
            return
        cfg = dict(src.config or {})
        email = (cfg.get('email') or '').strip()
        password = cfg.get('password') or ''
        if not email or not password:
            set_status('error', 'Add your HBO Max email and password first.')
            return

        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            set_status('error', 'Camoufox is not installed on this container.')
            return

        import os
        profile_dir = os.path.join(_PROFILE_BASE, f'hbomax-{uuid.uuid4().hex[:8]}')
        os.makedirs(profile_dir, exist_ok=True)
        set_status('signing-in', 'Opening the HBO Max sign-in…')
        session = None
        try:
            with Camoufox(headless='virtual', os='windows', humanize=True, window=(1280, 800),
                          persistent_context=True, user_data_dir=profile_dir) as ctx:
                page = ctx.pages[0] if ctx.pages else ctx.new_page()
                session = _drive_login(page, ctx, email, password)
        except Exception as exc:  # noqa: BLE001
            # Playwright's message can quote what was being typed: its type only.
            why = type(exc).__name__ if type(exc).__module__.startswith('playwright') else str(exc)[:300]
            logger.warning('[hbomax] sign-in failed: %s', why)
            set_status('error', why)
            return
        finally:
            shutil.rmtree(profile_dir, ignore_errors=True)

        if not session:
            set_status('error', 'Sign-in did not complete — check the email/password, or paste the st cookie instead.')
            return

        persist_source_config_updates(src.id, {
            'cookie_jar': session,
            'cookie_jar_captured_at': session['captured_at'],
            'signin_error': None,
        })
        logger.info('[hbomax] signed in; cookie jar captured (%d cookies)', len(session['cookies']))
        set_status('success', 'Signed in to HBO Max.')
        if src.is_enabled:
            from ..routes.tasks import trigger_scrape
            trigger_scrape('hbomax', force_full=True)


def save_manual_st(source_id: int, st_value: str, device_id: str | None = None) -> dict:
    """Fallback: store a user-pasted `st` cookie as the session jar. Returns a
    status dict. Validates the token is a non-anonymous JWT before saving."""
    from ..config_store import persist_source_config_updates
    from ..models import Source

    st_value = (st_value or '').strip()
    if _decode_anonymous(st_value) is not False:
        return {'ok': False, 'error': 'That is not a signed-in st token (it is missing or anonymous).'}
    src = Source.query.filter_by(name='hbomax').first()
    if not src:
        return {'ok': False, 'error': 'HBO Max source not found.'}
    session = {
        'cookies': [{'name': 'st', 'value': st_value}],
        'device_id': device_id or str(uuid.uuid4()),
        'captured_at': int(time.time()),
    }
    persist_source_config_updates(src.id, {
        'cookie_jar': session, 'cookie_jar_captured_at': session['captured_at'], 'signin_error': None,
    })
    if src.is_enabled:
        from ..routes.tasks import trigger_scrape
        trigger_scrape('hbomax', force_full=True)
    return {'ok': True}

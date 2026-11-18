"""Hulu (Hulu + Live TV) sign-in — one of the pluggable MVPD login backends
dispatched by app/tve/mvpd/__init__.py's login_to_mvpd().

Hulu's own login page is reCAPTCHA-gated, so a scripted password sign-in is
out. Instead the account is linked once with Hulu's device code (the user
enters a code at hulu.com/activate), which gives a long-lived Hulu device
token. From then on every Adobe Pass sign-in through Hulu is scripted:

  device_token  --/v2/device/device_token/authenticate-->  user_token (12h)
  user_token as the `_hulu_session` cookie
      --GET auth.hulu.com/v1/oauth2/authorize?<Adobe's OAuth params>-->  {code, state}
  Adobe's redirect_uri?code=&state=  --follow-->  the network's own callback

This is what Hulu's own login page does in the browser once it sees a
signed-in session (hoth-ui OAuth2ClientHandler: authn_entitlements, then
oauth2/authorize, both with `Cookie: _hulu_session=<token>`). All of it was
verified live 2026-11-17 against Discovery TVE (dev/hulu/FINDINGS.md):

  - /v1/device/code/authenticate answers 404 (errorCode 1004) until the code
    is entered, then the normal auth response.
  - The device token ROTATES on every refresh; the reply's token must be
    saved. A token that has already been rotated still refreshes, so two
    workers refreshing at once don't lock the account out.
  - The Adobe callback needs no cookies from the earlier authenticate step;
    a fresh session follows it to the network's code URL.
  - oauth2/authorize hands out a code even for a plan without Live TV;
    Discovery then fails with a misleading "activation code has expired".
    So the entitlement check runs first and gives the real reason.
"""
from __future__ import annotations

import html
import logging
import re
import secrets
import time
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit

import requests

from ..adobe_pass import TVEAuthError, TVENotAuthorizedError

logger = logging.getLogger(__name__)

AUTH_BASE = 'https://auth.hulu.com'
ACTIVATE_URL = 'https://hulu.com/activate'  # → auth.hulu.com/activate
ACCOUNT_KEY = 'hulu_device'
# Identity the Hulu Android app presents. device_id 166 is the app's fixed id
# for Android; the serial is ours, made once per link and kept with it.
_APP_VERSION = '6.34.0+18552420-google'
_DEVICE_ID = '166'
_MODEL = 'Pixel 7'
# Refresh the user token once less than this much of its life is left.
_USER_TOKEN_MARGIN = 30 * 60


class HuluNotLinked(TVEAuthError):
    """The account has no linked Hulu device (or Hulu revoked it); only a new
    "Sign in on my phone" can fix it."""


def _headers(serial: str) -> dict[str, str]:
    return {
        'User-Agent': f'Hulu/{_APP_VERSION} (Android 13; en_US; {_MODEL}; Build/TQ3A.230901.001;)',
        'X-Hulu-User-Agent': f'androidv4/{_APP_VERSION}/{serial};OS_33,MODEL_{_MODEL}',
    }


def _device_fields(serial: str) -> dict[str, str]:
    return {
        'serial_number': serial, 'friendly_name': _MODEL, 'device_id': _DEVICE_ID,
        'device_platform': 'Android', 'time_zone': 'America/New_York',
    }


def _auth_data(reply: dict) -> dict:
    # Device-code replies are wrapped in "data"; refresh replies are not.
    data = reply.get('data') if isinstance(reply.get('data'), dict) else reply
    return data or {}


def _link_from_auth(serial: str, data: dict, previous: dict | None = None) -> dict:
    link = dict(previous or {})
    link['serial'] = serial
    if data.get('device_token'):
        link['device_token'] = data['device_token']
    if data.get('user_token'):
        link['user_token'] = data['user_token']
        link['user_token_expires_at'] = int(time.time()) + int(data.get('expires_in') or 0)
    link.setdefault('linked_at', int(time.time()))
    link['refreshed_at'] = int(time.time())
    return link


# ── device-code linking (used by the phone sign-in, app/tve/link_login.py) ──

def new_serial() -> str:
    return secrets.token_hex(8)


def register_device_code(serial: str) -> str:
    """Starts a link; returns the code the user enters at ACTIVATE_URL."""
    r = requests.post(f'{AUTH_BASE}/v1/device/code/register', headers=_headers(serial), timeout=20,
                      data={'serial_number': serial, 'friendly_name': _MODEL})
    if r.status_code != 200:
        raise TVEAuthError(f'Hulu did not hand out a sign-in code (HTTP {r.status_code}).')
    code = ((r.json() or {}).get('code') or '').strip()
    if not code:
        raise TVEAuthError('Hulu did not hand out a sign-in code.')
    return code


def poll_device_code(code: str, serial: str) -> dict | None:
    """None until the code has been entered; then the new link dict."""
    r = requests.post(f'{AUTH_BASE}/v1/device/code/authenticate', headers=_headers(serial), timeout=20,
                      data={'code': code, **_device_fields(serial)})
    if r.status_code == 404:
        return None  # errorCode 1004: not entered yet
    if r.status_code != 200:
        raise TVEAuthError(f'Hulu sign-in check returned HTTP {r.status_code}: {r.text[:200]}')
    data = _auth_data(r.json() or {})
    if not data.get('device_token'):
        raise TVEAuthError('Hulu sign-in finished without a device token.')
    return _link_from_auth(serial, data)


# ── the saved link ──────────────────────────────────────────────────────────

def hulu_link(account) -> dict | None:
    link = ((account.config or {}).get(ACCOUNT_KEY) if account else None) or None
    if isinstance(link, dict) and link.get('device_token') and link.get('serial'):
        return link
    return None


def save_hulu_link(account, link: dict | None) -> None:
    """Writes the link onto the account row (None removes it). Re-reads the
    row first so a concurrent write to another config key isn't lost."""
    from ...extensions import db
    try:
        db.session.refresh(account)
    except Exception:  # noqa: BLE001 - a detached/new row: write what we have
        pass
    cfg = dict(account.config or {})
    if link:
        cfg[ACCOUNT_KEY] = link
    else:
        cfg.pop(ACCOUNT_KEY, None)
    account.config = cfg
    db.session.commit()


def refresh_link(link: dict, *, relink_hint: str = 'Use "Sign in on my phone" to link Hulu again.',
                 timeout=20) -> dict:
    """Renews a link: a new user token and (usually) a rotated device token.
    Saves nothing; the caller must store the returned link right away, since
    the old device token is superseded. Raises HuluNotLinked when Hulu refuses
    the device token, TVEAuthError for anything worth retrying."""
    serial = link['serial']
    try:
        r = requests.post(f'{AUTH_BASE}/v2/device/device_token/authenticate', headers=_headers(serial),
                          timeout=timeout, data={'device_token': link['device_token'], 'action': 'token_refresh',
                                                 **_device_fields(serial)})
    except requests.RequestException as exc:
        raise TVEAuthError(f'Could not reach Hulu to renew the sign-in; will retry: {exc}') from exc
    if r.status_code in (400, 401, 403):
        logger.info('[hulu] device token refused (HTTP %s): %s', r.status_code, r.text[:200])
        raise HuluNotLinked(f'Hulu no longer accepts this FastChannels sign-in. {relink_hint}')
    if r.status_code != 200:
        raise TVEAuthError(f'Hulu returned HTTP {r.status_code} renewing the sign-in; will retry.')
    data = _auth_data(r.json() or {})
    if not data.get('user_token'):
        raise TVEAuthError('Hulu renewed the sign-in without a user token; will retry.')
    new_link = _link_from_auth(serial, data, previous=link)
    logger.info('[hulu] renewed the Hulu sign-in (device token rotated: %s)',
                new_link['device_token'] != link['device_token'])
    return new_link


def user_token_fresh(link: dict | None) -> bool:
    return bool(link and link.get('user_token')
                and int(link.get('user_token_expires_at') or 0) - time.time() > _USER_TOKEN_MARGIN)


def api_headers(link: dict) -> dict[str, str]:
    """Headers for Hulu's app APIs (guide, playlist, users/self) as the
    linked Android device, with its current user token."""
    return {**_headers(link['serial']), 'Authorization': f"Bearer {link['user_token']}",
            'Accept': 'application/json'}


LIVE_TV_PACKAGES = {'16', '38'}  # Live TV; 38 is the Spanish Live TV bundle


def user_info(link: dict, timeout=20) -> dict:
    """The signed-in Hulu user with its subscription (package_ids, features)."""
    r = requests.get('https://home.hulu.com/v3/users/self', headers=api_headers(link), timeout=timeout,
                     params={'action': 'login', 'expand': 'subscriptions'})
    r.raise_for_status()
    return (r.json() or {}).get('data') or {}


def _refresh(account, link: dict) -> dict:
    new_link = refresh_link(link)
    save_hulu_link(account, new_link)
    return new_link


def user_token(account, *, force_refresh: bool = False) -> str:
    """A Hulu user token with some life left, renewing (and saving the
    rotated device token) when needed."""
    link = hulu_link(account)
    if not link:
        raise HuluNotLinked('Hulu isn\'t linked to FastChannels yet. Use "Sign in on my phone" to link it.')
    if user_token_fresh(link) and not force_refresh:
        return link['user_token']
    return _refresh(account, link)['user_token']


# ── the MVPD login itself ───────────────────────────────────────────────────

def _oauth_params(page_url: str) -> dict[str, str]:
    parts = urlsplit(page_url or '')
    if parts.netloc != 'auth.hulu.com' or not parts.path.startswith('/oauth2/login'):
        raise TVEAuthError(f'Unexpected Hulu login page: {parts.netloc}{parts.path}')
    params = {k: v[0] for k, v in parse_qs(parts.query).items()}
    for required in ('client_id', 'redirect_uri', 'state'):
        if not params.get(required):
            raise TVEAuthError(f'Hulu login page is missing {required}.')
    return params


def _get(path: str, params: dict, token: str) -> requests.Response:
    return requests.get(f'{AUTH_BASE}{path}', params=params, timeout=20, headers={
        'User-Agent': _headers('0')['User-Agent'], 'Accept': 'application/json',
        'Cookie': f'_hulu_session={token}',
    })


def hulu_oauth_login(page_url: str, account) -> str:
    """Completes the Hulu step of an Adobe Pass sign-in from Adobe's redirect
    to auth.hulu.com/oauth2/login, and returns the URL the network's callback
    finally lands on (login_to_mvpd()'s contract)."""
    params = _oauth_params(page_url)
    token = user_token(account)
    for attempt in (1, 2):
        r = _get('/v1/user/authn_entitlements', params, token)
        if r.status_code in (401, 403) and attempt == 1:
            token = user_token(account, force_refresh=True)  # Hulu dropped the user token early
            continue
        break
    if r.status_code != 200:
        raise TVEAuthError(f'Hulu entitlement check returned HTTP {r.status_code}: {r.text[:200]}')
    resources = (r.json() or {}).get('resources') or [{}]
    if not resources[0].get('authorized'):
        raise TVENotAuthorizedError(
            'Your Hulu plan doesn\'t include Live TV, so Hulu can\'t sign in to TV Everywhere networks.')

    r = _get('/v1/oauth2/authorize', params, token)
    if r.status_code != 200 or not (r.json() or {}).get('code'):
        raise TVEAuthError(f'Hulu sign-in returned HTTP {r.status_code}: {r.text[:200]}')
    reply = r.json()
    callback = f"{params['redirect_uri']}?{urlencode({'code': reply['code'], 'state': reply.get('state') or params['state']})}"
    landed = _follow_callback(callback)
    logger.info('[hulu] signed in through Hulu for %s', params.get('requestor_id') or params['client_id'])
    return landed


def _follow_callback(url: str) -> str:
    """Hands Hulu's code to Adobe and follows the redirects to wherever the
    network's callback lands. The sign-in is recorded once Adobe has taken
    the code (the first hop), so a network landing page that errors or times
    out afterwards isn't a failure: the URL it was sent to (which carries any
    code the network needs, e.g. Discovery's gauth-sync?code=) is returned."""
    session = requests.Session()
    headers = {'User-Agent': _headers('0')['User-Agent'], 'Accept': 'text/html,*/*'}
    for hop in range(10):
        try:
            r = session.get(url, headers=headers, allow_redirects=False, timeout=30)
        except requests.RequestException as exc:
            if hop == 0:
                raise TVEAuthError(f'Could not finish the Hulu sign-in with Adobe: {exc}') from exc
            return url
        location = r.headers.get('location')
        if r.status_code not in (301, 302, 303, 307, 308) or not location:
            if hop == 0 and r.status_code >= 400:
                raise TVEAuthError(f'Adobe refused the Hulu sign-in (HTTP {r.status_code}): {r.text[:200]}')
            return url
        url = urljoin(url, location)
    return url


def _find_hulu_login(url: str) -> str | None:
    """Follows a network's sign-in link (the one the phone sign-in would show
    the user) to Hulu's login page, or None if it never gets there, e.g. a
    page that wants a code typed in."""
    session = requests.Session()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                             '(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36',
               'Accept': 'text/html,*/*'}
    for _ in range(10):
        parts = urlsplit(url)
        if parts.netloc == 'auth.hulu.com' and parts.path.startswith('/oauth2/login'):
            return url
        try:
            r = session.get(url, headers=headers, allow_redirects=False, timeout=30)
        except requests.RequestException:
            return None
        location = r.headers.get('location')
        if r.status_code in (301, 302, 303, 307, 308) and location:
            url = urljoin(url, location)
            continue
        # A page that sends the browser on with a meta refresh or script.
        m = re.search(r'https://auth\.hulu\.com/oauth2/login\?[^"\'<>\s]+', html.unescape(r.text or ''))
        return m.group(0) if m else None
    return None


def complete_signin_url(url: str, account) -> bool:
    """Finishes a network's sign-in link through the linked Hulu device
    instead of the user opening it. False when the link doesn't lead to
    Hulu's login page (the caller shows it to the user as before). Raises
    TVENotAuthorizedError when Hulu says the plan doesn't cover it."""
    login_url = _find_hulu_login(url)
    if not login_url:
        return False
    hulu_oauth_login(login_url, account)
    return True


def new_link_serial(account) -> str:
    """Reuse the account's serial when relinking, so Hulu keeps showing one
    FastChannels device instead of a new one per link."""
    link = ((account.config or {}).get(ACCOUNT_KEY) if account else None) or {}
    return link.get('serial') or new_serial()

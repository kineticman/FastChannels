"""DirecTV code sign-in: the device-code grant DirecTV's Android TV app uses.

A second way to sign the DirecTV source in, next to the email/password web
login in directv.py. The user opens a link at directv.com/tvsigninv2, approves
the code in their own browser, and DirecTV hands back the same three tokens the
web login captures (bearer, refresh, DRM activation). No password is stored,
and the session renews from its refresh token instead of logging in again.

Protocol worked out by mackid1993 (github.com/mackid1993/FastChannels-mackid1993):
  start:   POST {grant}/v2/devicecode   {clientId, deviceClassID}
  poll:    GET  {grant}/tokens          ?clientID=&deviceCode=
  refresh: POST authn-refreshgo/v3/refresh?clientID=   (form: refresh_token,
           reqParams=ACTIVATIONTOKEN — that is what re-mints the activation token)

Only the session differs from the web login; channel lookups and playback are
unchanged. Every function here returns the same result dict shape as
capture_directv_auth_cffi, with auth_method='device_code'.
"""
from __future__ import annotations

import logging
import time
import uuid

import requests

logger = logging.getLogger(__name__)

AUTH_METHOD = 'device_code'

_CLIENT_ID = 'UNIFIED_Android_TV_02'
_GRANT_BASE = 'https://api.cld.dtvce.com/account/device/grant'
_DEVICECODE_URL = f'{_GRANT_BASE}/v2/devicecode'
_TOKENS_URL = f'{_GRANT_BASE}/tokens'
_REFRESH_URL = 'https://api.cld.dtvce.com/authn-refreshgo/v3/refresh'
_VERIFY_FALLBACK_URL = 'https://directv.com/tvsigninv2'

# The Android TV app's User-Agent shape (APP_PROJECT_NAME is literal, and there
# are two spaces before PureRN). One fixed device, so sign-in, refresh and DRM
# calls all present the same client whether or not a bridge device is involved.
APP_USER_AGENT = (
    'APP_PROJECT_NAME/5.0.136.2002113867 (Android 12; Chromecast; sabrina)  PureRN/0.79.5'
)

_DEFAULT_CODE_TTL = 600
_MIN_POLL_INTERVAL = 5


class DeviceAuthError(RuntimeError):
    pass


def is_device_session(config: dict | None) -> bool:
    cfg = config or {}
    return cfg.get('auth_method') == AUTH_METHOD and bool(cfg.get('refresh_token'))


def app_headers() -> dict:
    return {'Accept': 'application/json, text/plain, */*', 'User-Agent': APP_USER_AGENT}


def _int(value, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def start_grant() -> dict:
    """Ask DirecTV for a sign-in code. Returns what the UI shows (user_code, url)
    and what poll_grant needs (device_code, interval, expires_at)."""
    r = requests.post(
        _DEVICECODE_URL,
        json={'clientId': _CLIENT_ID, 'deviceClassID': str(uuid.uuid4())},
        headers=app_headers(), timeout=20,
    )
    if not r.ok:
        logger.warning('[directv-code] devicecode HTTP %s: %s', r.status_code, (r.text or '')[:300])
        raise DeviceAuthError(f'DirecTV would not issue a sign-in code (HTTP {r.status_code})')
    try:
        data = r.json()
    except ValueError:
        data = {}
    device_code = (data.get('deviceCode') or '').strip()
    user_code = (data.get('userCode') or '').strip()
    if not device_code or not user_code:
        logger.warning('[directv-code] devicecode response missing fields: %s', sorted(data))
        raise DeviceAuthError('DirecTV returned no sign-in code')
    url = (data.get('url') or data.get('displayURL') or _VERIFY_FALLBACK_URL).strip()
    if not url.startswith(('http://', 'https://')):
        url = f'https://{url}'
    return {
        'device_code': device_code,
        'user_code': user_code,
        'url': url,
        'interval': max(_MIN_POLL_INTERVAL, _int(data.get('pollingInterval'), _MIN_POLL_INTERVAL)),
        'expires_at': time.time() + _int(data.get('expiresIn'), _DEFAULT_CODE_TTL),
    }


def poll_grant(device_code: str) -> dict | None:
    """One poll of the grant. None while the code is still unapproved, else the
    login result. The endpoint is keyed on the device code alone."""
    r = requests.get(
        _TOKENS_URL, params={'clientID': _CLIENT_ID, 'deviceCode': device_code},
        headers=app_headers(), timeout=20,
    )
    try:
        data = r.json()
    except ValueError:
        return None
    if not isinstance(data, dict) or not (data.get('access_token') or data.get('accessToken')):
        return None
    return _result(data)


def refresh_session(refresh_token: str) -> dict:
    """Renew a code sign-in session from its refresh token. DirecTV rotates the
    refresh token, so the caller must persist the result."""
    r = requests.post(
        _REFRESH_URL, params={'clientID': _CLIENT_ID},
        # clientMake/clientModel are sent exactly as mackid1993 validated them live.
        data=[('clientMake', 'Google'), ('clientModel', 'Chrome'),
              ('refresh_token', refresh_token), ('reqParams', 'ACTIVATIONTOKEN')],
        headers={**app_headers(),
                 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'},
        timeout=30,
    )
    if not r.ok:
        logger.warning('[directv-code] refresh HTTP %s: %s', r.status_code, (r.text or '')[:300])
        raise DeviceAuthError(f'DirecTV token refresh failed (HTTP {r.status_code})')
    try:
        data = r.json()
    except ValueError as exc:
        raise DeviceAuthError('DirecTV token refresh returned no JSON') from exc
    result = _result(data)
    result['refresh_token'] = result['refresh_token'] or refresh_token
    return result


def _result(token_data: dict) -> dict:
    from .directv import _normalize_activation_token  # lazy: directv imports this module

    bearer = (token_data.get('access_token') or token_data.get('accessToken') or '').strip()
    if not bearer:
        raise DeviceAuthError('DirecTV returned no access token')
    value_pairs = token_data.get('valuePairs')
    value_pairs = value_pairs if isinstance(value_pairs, dict) else {}
    return {
        'bearer_token': bearer,
        'refresh_token': (token_data.get('refresh_token') or token_data.get('refreshToken') or '').strip(),
        'activation_token': _normalize_activation_token((value_pairs.get('activationToken') or '').strip()),
        'cookies': [],
        'captured_at': time.time(),
        'auth_method': AUTH_METHOD,
    }

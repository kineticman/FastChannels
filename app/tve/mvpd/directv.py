"""DIRECTV (mso_id='DTV') sign-in — one of the pluggable MVPD login backends
dispatched by app/tve/mvpd/__init__.py's login_to_mvpd().
"""
from __future__ import annotations

import json
from urllib.parse import parse_qs, urlsplit

from ..adobe_pass import TVEAuthError, _hidden_form, throttle_directv_login

_IDENTITY_AUTH_URL = 'https://identity.directv.com/am/IdPwdAuth'
_IDENTITY_AUTHORIZE_URL = 'https://identity.directv.com/authorize'
# Distinct from app/scrapers/directv.py's own 'fr_web_02' (the native
# stream.directv.com app's OAuth client) — this is the client ForgeRock
# actually issues callbacks under when the flow originates from Adobe Pass's
# idp.dtvce.com SAMLRequest gateway rather than DIRECTV's own app. Confirmed
# live 2026-08-17: the /weblogin/authorize page's own query string names it
# `fr_client_id`; sending it back to identity.directv.com as `client_id`
# (the REST param name, confirmed against the native scraper's equivalent
# call) is what /authorize actually expects.
_MVPD_FORGEROCK_CLIENT_ID = 'fr_idp_02'
_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'
)


def _fill_auth_callback(payload: dict, value: str) -> dict:
    data = json.loads(json.dumps(payload))
    for callback in data.get('callbacks') or []:
        if callback.get('type') not in ('NameCallback', 'PasswordCallback', 'ChoiceCallback'):
            continue
        inputs = callback.get('input') or []
        if inputs:
            inputs[0]['value'] = value
            break
    return data


def _json_or_error(response, label: str) -> dict:
    try:
        data = response.json()
    except Exception as exc:  # noqa: BLE001
        raise TVEAuthError(
            f'{label} returned non-JSON HTTP {response.status_code}: {response.text[:160]}'
        ) from exc
    if response.status_code < 200 or response.status_code >= 300:
        msg = data.get('message') or data.get('errorDescription') or data.get('reason') or data
        raise TVEAuthError(f'{label} failed HTTP {response.status_code}: {msg}')
    return data


def directv_login(start_html: str, start_url: str, username: str, password: str) -> str:
    """Login to DIRECTV (mso_id='DTV') via its real ForgeRock/OpenAM identity
    provider, driven as a JSON callback API — not form-scraping. Adobe Pass's
    own authenticate call (however each family reaches it) doesn't redirect
    for DIRECTV at all; it returns a 200 HTML page whose body is an
    auto-submit SAMLRequest form posting to idp.dtvce.com — that's exactly
    `start_html`/`start_url` as every caller passes them (the raw response
    from their own family-specific authenticate call). Submitting that form
    (done here, first thing) is what actually lands on
    `identity.directv.com/weblogin/authorize?code_challenge=...
    &fr_client_id=fr_idp_02&redirect_uri=...&response_type=code&state=...`
    — a client-side React SPA, not a server-rendered form (confirmed live
    2026-08-17: zero `<form>` tags in its HTML), so everything from there on
    talks to its backend REST API directly instead.

    Two real obstacles here, both confirmed live 2026-08-17:
      1. identity.directv.com sits behind Akamai Bot Manager on the JSON
         callback POSTs — a plain `requests` client gets a 403 Access Denied;
         curl_cffi Chrome impersonation sails straight through, same fix
         already proven for Xfinity (xfinity.py) and for this same identity
         provider's native-app login (app/scrapers/directv.py's
         capture_directv_auth_cffi).
      2. The SPA itself can't be scraped, but its backend can be driven
         directly — the same ForgeRock JSON callback API (NameCallback /
         PasswordCallback -> tokenId) app/scrapers/directv.py already
         reverse-engineered for the native app's own client
         ('fr_web_02'), works identically here under 'fr_idp_02' (the
         client ForgeRock issues callbacks under for this Adobe-Pass-
         initiated flow) — same endpoint (identity.directv.com/am/IdPwdAuth),
         same three-step shape.

    After that JSON login, completing the PKCE code exchange needs the REAL
    REST endpoint (identity.directv.com/authorize) — NOT the SPA route
    (/weblogin/authorize) `start_url` itself points at, which just always
    re-serves the same static shell regardless of auth state. The
    code_challenge/redirect_uri/state embedded in `start_url`'s query string
    were minted server-side by idp.dtvce.com when it first built this flow
    from Adobe's SAMLRequest, so DIRECTV's own backend — not us — holds the
    matching code_verifier; hitting /authorize with those same params (now
    carrying our freshly authenticated ForgeRock session cookie) is enough
    to get a real `code` back, no verifier of our own needed. That code
    redirects to `redirect_uri` (DIRECTV's own api.cld.dtvce.com/idp/login/
    v3/login — the SAME URL regardless of which content-owner's SAMLRequest
    started the flow, confirmed live against both the legacy protocol's
    History request and FOX One's own v2 REST request), whose response is
    itself one more auto-submit form — this time a SAMLResponse posting to
    Adobe's own https://sp.auth.adobe.com/sp/saml/SAMLAssertionConsumer,
    completing the round trip exactly like Cox's SAML flow ends (see
    _cox_saml_login in fox_tve.py/discovery_tve.py/amcn_tve.py) — DIRECTV
    just gets there via ForgeRock+PKCE instead of Okta.

    Confirmed live 2026-08-17 end-to-end against real DIRECTV Stream
    credentials: produces a real Adobe authnToken/session (fetch_session_token
    succeeds) for both the legacy protocol (HISTORY) and a v2 REST family
    (via FOX One's own authenticate call) — the mechanism itself works.
    Entitlement is a separate question: this particular account came back
    "0033-The customer is not authorized for the content requested" for both
    HISTORY and TNT, a clean, definitive Adobe business-rule answer (surfaces
    as TVENotAuthorizedError to callers), not a mechanism failure.

    Returns the final landed URL once login completes — same contract as
    xfinity_cookie_jar_login() — for callers (e.g. Discovery TVE) that need
    to pull a `code` query param out of wherever their own redirect_url
    chain lands; callers that only need the session's cookies/binding state
    can ignore the return value.
    """
    from curl_cffi import requests as curl_requests

    throttle_directv_login()

    session = curl_requests.Session(impersonate='chrome')
    session.headers.update({'User-Agent': _UA, 'Accept-Language': 'en-US,en;q=0.9'})

    # `start_html`/`start_url` is Adobe's OWN authenticate-call response —
    # the SAMLRequest auto-submit form posting to idp.dtvce.com (see this
    # function's docstring) — not yet the actual identity.directv.com login
    # page. POSTing it (impersonated — idp.dtvce.com sits behind the same
    # Akamai wall) is what lands on the real
    # identity.directv.com/weblogin/authorize?code_challenge=... URL the
    # rest of this function needs.
    action, fields = _hidden_form(start_html, start_url)
    try:
        r0 = session.post(
            action, data=fields,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30, allow_redirects=True,
        )
    except Exception as exc:  # noqa: BLE001
        raise TVEAuthError(str(exc)) from exc
    if r0.status_code >= 400:
        raise TVEAuthError(f'DIRECTV sign-in did not reach a login page: HTTP {r0.status_code}.')
    start_url = str(r0.url)

    auth_headers = {
        'Content-Type': 'application/json',
        'Accept-API-Version': 'resource=2.0, protocol=1.0',
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'client_id': _MVPD_FORGEROCK_CLIENT_ID,
        'Origin': 'https://identity.directv.com',
        'Referer': start_url,
        'Accept': 'application/json, text/plain, */*',
    }

    try:
        data = _json_or_error(
            session.post(_IDENTITY_AUTH_URL, headers=auth_headers, data='{}', timeout=30),
            'DIRECTV username challenge',
        )
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, TVEAuthError):
            raise
        raise TVEAuthError(str(exc)) from exc
    if not any(c.get('type') == 'NameCallback' for c in data.get('callbacks') or []):
        raise TVEAuthError('DIRECTV username challenge did not return NameCallback')

    try:
        data = _json_or_error(
            session.post(_IDENTITY_AUTH_URL, headers=auth_headers, json=_fill_auth_callback(data, username), timeout=30),
            'DIRECTV password challenge',
        )
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, TVEAuthError):
            raise
        raise TVEAuthError(str(exc)) from exc
    if not any(c.get('type') == 'PasswordCallback' for c in data.get('callbacks') or []):
        raise TVEAuthError('DIRECTV password challenge did not return PasswordCallback')

    try:
        data = _json_or_error(
            session.post(_IDENTITY_AUTH_URL, headers=auth_headers, json=_fill_auth_callback(data, password), timeout=30),
            'DIRECTV password submit',
        )
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, TVEAuthError):
            raise
        raise TVEAuthError(str(exc)) from exc
    if not data.get('tokenId'):
        msg = data.get('message') or data.get('reason') or 'missing tokenId'
        raise TVEAuthError(f'DIRECTV login failed: {msg}')

    q = parse_qs(urlsplit(start_url).query)
    authorize_params = {
        'scope': 'read',
        'response_type': (q.get('response_type') or ['code'])[0],
        'client_id': (q.get('fr_client_id') or [_MVPD_FORGEROCK_CLIENT_ID])[0],
        'redirect_uri': (q.get('redirect_uri') or [''])[0],
        'code_challenge': (q.get('code_challenge') or [''])[0],
        'code_challenge_method': (q.get('code_challenge_method') or ['S256'])[0],
        'state': (q.get('state') or [''])[0],
    }
    if not authorize_params['redirect_uri'] or not authorize_params['code_challenge']:
        raise TVEAuthError('DIRECTV sign-in: start_url is missing code_challenge/redirect_uri to complete PKCE.')

    try:
        r = session.get(
            _IDENTITY_AUTHORIZE_URL, params=authorize_params,
            headers={'Referer': start_url}, allow_redirects=False, timeout=30,
        )
    except Exception as exc:  # noqa: BLE001
        raise TVEAuthError(str(exc)) from exc
    location = r.headers.get('location') or ''
    if r.status_code not in (301, 302, 303, 307, 308) or not location:
        raise TVEAuthError(f'DIRECTV sign-in did not receive an authorization code: HTTP {r.status_code}.')

    try:
        r2 = session.get(location, allow_redirects=True, timeout=30)
    except Exception as exc:  # noqa: BLE001
        raise TVEAuthError(str(exc)) from exc
    if r2.status_code >= 400:
        raise TVEAuthError(f'DIRECTV redirect_uri completion failed: HTTP {r2.status_code}.')

    # DIRECTV's backend hands back one more auto-submit form here — a
    # SAMLResponse posting to Adobe's own SP consumer endpoint. Nothing
    # auto-submits it for us; without this hop Adobe never actually
    # receives the completed login (confirmed live: fetch_session_token()
    # reports no session yet until this POST happens).
    action, fields = _hidden_form(r2.text, str(r2.url))
    try:
        r3 = session.post(
            action, data=fields,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30, allow_redirects=True,
        )
    except Exception as exc:  # noqa: BLE001
        raise TVEAuthError(str(exc)) from exc
    if r3.status_code >= 400:
        raise TVEAuthError(f'DIRECTV sign-in confirmation step failed: HTTP {r3.status_code}.')
    return str(r3.url)

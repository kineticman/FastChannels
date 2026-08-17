"""Comcast_SSO (Xfinity) sign-in — one of the pluggable MVPD login backends
dispatched by app/tve/mvpd/__init__.py's login_to_mvpd().
"""
from __future__ import annotations

import re

from ..adobe_pass import TVEAuthError, _hidden_form


def xfinity_cookie_jar_login(auth_url: str, username: str, password: str, cookie_jar: dict) -> str:
    """Login to Comcast_SSO (login.xfinity.com) using a transplanted cookie
    jar harvested from a real authenticated browser session, instead of a
    browser. MSO-protocol-agnostic — takes any MSO-login URL that ultimately
    lands on login.xfinity.com, regardless of which Adobe Pass "family"
    generated it (the legacy XML protocol's authenticate/saml URL, NBC's own
    v2 REST /sessions redirect, FOX's own REST flow, etc.) — the actual
    xfinity.com login form/wall is the same one every family redirects to.

    Confirmed live 2026-08-14: login.xfinity.com is protected by Akamai Bot
    Manager on the credential-submission POST specifically (the page-load
    GET is unprotected for any client — a bare curl_cffi request sails
    through). A bare HTTP client's own freshly-issued _abck/ak_bmsc/bm_sz
    cookies are NOT sufficient on their own — confirmed live, a fresh
    curl_cffi session carrying its own server-issued cookies into the same
    session's next POST still gets 403. Akamai only trusts a cookie's value
    once it's been "matured" through real JS sensor execution in a real
    browser. Transplanting a jar already matured by a real session
    (harvested in app/worker.py's run_mvpd_browser_login/run_nbc_browser_login/
    run_fox_browser_login after a successful Camoufox pairing) gets straight
    through.

    Often the jar's own Xfinity SESSION cookie is ALSO still a valid
    already-authenticated identity, in which case Xfinity skips straight to
    a "You're automatically signed in" interstitial with an embedded
    continue URL and no password is ever needed — that path is tried at
    every step (it can appear on the very first GET, before any login form
    ever renders, or only after a username POST — confirmed live both ways
    with the SAME cookie jar against different requestor_ids/client_ids). If
    a real password field appears instead (SESSION expired but the Akamai
    cookies are still matured), falls through to a normal identifier-first
    username+password submission.

    Uses its own dedicated curl_cffi session (impersonation matters here —
    plain `requests` doesn't produce a convincing TLS fingerprint) — does
    NOT touch or require anything from whatever session/client called this,
    since Adobe binds the completed login server-side to auth_url's own
    embedded state (reg_code, or NBC/FOX's own session identifier) rather
    than to any particular local HTTP session — the caller can safely poll/
    continue with a completely different session afterward, same as the
    existing browser-assisted pairing's cross-session polling already
    relies on.

    Returns the final landed URL (as a string) once login completes —
    needed by callers like Discovery TVE whose own completion mechanism
    extracts a `code` query param from wherever the login flow's own
    redirect_url lands, rather than polling independently server-side like
    the legacy/NBC/FOX families do. Those callers can just ignore the
    return value.
    """
    from curl_cffi import requests as curl_requests

    def _follow_interstitial_if_present(session, html_text: str) -> str | None:
        m = re.search(r'continue:\s*"([^"]+)"', html_text)
        if not m:
            return None
        continue_url = m.group(1).encode().decode('unicode_escape')
        try:
            r3 = session.get(continue_url, timeout=30, allow_redirects=True)
        except Exception as exc:  # noqa: BLE001
            raise TVEAuthError(str(exc)) from exc
        # Deliberately NOT raising on a non-2xx/3xx status here — this hop
        # lands on the CALLER's own redirect_url (e.g. a TVE network's own
        # /live page), which is irrelevant to whether Adobe's own
        # server-side login binding succeeded. Confirmed live 2026-08-14:
        # AMC Networks' configured live_url (www.amc.com/live) is itself a
        # genuine 404 on AMC's own site — nothing to do with Xfinity or
        # auth — yet the login had already completed successfully by this
        # point (the caller's subsequent /profiles/code/{code} poll
        # confirmed real entitlement). Callers that need this URL to
        # actually resolve (e.g. Discovery TVE, extracting a `code` query
        # param) check its content themselves; callers that don't (legacy/
        # NBC/AMCN, which verify completion via an independent poll) can
        # safely ignore the status entirely.
        return str(r3.url)

    xfinity_session = curl_requests.Session(impersonate='chrome')
    for name, meta in cookie_jar.items():
        xfinity_session.cookies.set(
            name, meta.get('value', ''),
            domain=meta.get('domain') or 'login.xfinity.com',
            path=meta.get('path') or '/',
        )

    try:
        r = xfinity_session.get(auth_url, timeout=30, allow_redirects=True)
    except Exception as exc:  # noqa: BLE001
        raise TVEAuthError(str(exc)) from exc
    if r.status_code >= 400 or 'login.xfinity.com' not in str(r.url):
        raise TVEAuthError(f'Xfinity cookie-jar sign-in did not reach the login page: HTTP {r.status_code}.')

    landed = _follow_interstitial_if_present(xfinity_session, r.text)
    if landed:
        return landed

    login_url = str(r.url)
    action, fields = _hidden_form(r.text, login_url)
    fields['user'] = username
    fields['flowStep'] = 'username'
    try:
        r2 = xfinity_session.post(
            action, data=fields,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30, allow_redirects=True,
        )
    except Exception as exc:  # noqa: BLE001
        raise TVEAuthError(str(exc)) from exc
    if r2.status_code >= 400:
        raise TVEAuthError(
            f'Xfinity cookie-jar sign-in blocked at username step: HTTP {r2.status_code} '
            '(cookie jar likely stale — needs a fresh browser pairing).'
        )

    landed = _follow_interstitial_if_present(xfinity_session, r2.text)
    if landed:
        return landed

    if 'passwd' not in r2.text.lower() and 'type="password"' not in r2.text.lower():
        raise TVEAuthError(
            'Xfinity cookie-jar sign-in: neither an auto-signin interstitial nor '
            'a password field appeared.'
        )

    action2, fields2 = _hidden_form(r2.text, str(r2.url))
    fields2['passwd'] = password
    fields2['flowStep'] = 'password'
    try:
        r3 = xfinity_session.post(
            action2, data=fields2,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30, allow_redirects=True,
        )
    except Exception as exc:  # noqa: BLE001
        raise TVEAuthError(str(exc)) from exc
    if r3.status_code >= 400:
        raise TVEAuthError(f'Xfinity cookie-jar sign-in blocked at password step: HTTP {r3.status_code}.')
    return str(r3.url)

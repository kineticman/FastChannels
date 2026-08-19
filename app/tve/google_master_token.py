"""Google master_token capture and renewal for YouTubeTV-as-MVPD Adobe Pass
pairing.

The technique (borrowed from a competitor's YouTube TV Android-impersonation
implementation, dev/youtube/Youtube): a Google `master_token`, captured once
from a real interactive login, is the exact private credential a real Android
device uses to stay signed in indefinitely. From it, `gpsoauth` (the same
private API Android's own account-setup flow uses) can mint either a fresh
API bearer token or a full set of real Google browser session cookies
(SID/HSID/etc) at any time, entirely offline — no browser, no re-login,
no password.

Why this exists: app/worker.py's YouTubeTV/Google MVPD browser-logins share
one persistent Camoufox profile (/data/browser_profiles/mvpd_tve), which was
assumed to carry a "warm" Google session across runs. Confirmed live
2026-08-19 that it does not — that profile has never held a real SID-family
login cookie, only generic NID/OTZ/__Host-GAPS ones — so every fresh
Adobe/YouTubeTV authorization fell back to the full interactive human-relay
flow. A master_token, captured once and stored server-side
(adobe_pass.save_google_master_token), lets app.worker prime a Camoufox
context with a real signed-in session before it ever navigates, without
depending on that profile's cookie jar at all.

See docs/... (none yet) for the manual verification: real interactive login
via Camoufox (plain Playwright Chromium got an instant "this browser or app
may not be secure" from Google — Camoufox's anti-detect patches were
required) captured a master_token; mint_browser_cookies() from it alone (zero
browser) came up fully signed in on a brand-new, never-logged-in profile.
"""
import logging
import time
import uuid

import gpsoauth
import requests

logger = logging.getLogger(__name__)

# The exact scope/app/signature combination the recovered competitor
# implementation uses to verify a master_token against the real YouTube TV
# Android app identity — not needed for the MVPD-pairing use case itself
# (we only need real Google browser cookies), but exchange_oauth_token()
# fails fast if the master_token gpsoauth just minted can't even do this,
# which is a much clearer signal than a mysterious later Adobe failure.
_YTTV_SCOPE = (
    'oauth2:https://www.googleapis.com/auth/accounts.reauth '
    'https://www.googleapis.com/auth/youtube.force-ssl '
    'https://www.googleapis.com/auth/youtube '
    'https://www.googleapis.com/auth/identity.lateimpersonation'
)
_YTTV_APP = 'com.google.android.apps.youtube.unplugged'
_YTTV_CLIENT_SIG = '3a82b5ee26bc46bf68113d920e610cd090198d4a'

# The GMS (Google Play Services) app identity + "weblogin" service string is
# the documented gpsoauth pattern for minting an "uberauth" token scoped to
# produce real browser cookies via MergeSession, as opposed to _YTTV_SCOPE's
# API-only bearer token.
_UBERAUTH_SERVICE = (
    'weblogin:continue=https://www.google.com/accounts/OAuthLogin'
    '?source=ChromiumBrowser%26issueuberauth=1'
)
_GMS_APP = 'com.google.android.gms'
_GMS_CLIENT_SIG = '38918a453d07199354f8b19af05ec6562ced5788'


def exchange_oauth_token(oauth_token: str) -> dict | None:
    """Exchange a web oauth_token — harvested from the `oauth_token` cookie
    Google's embedded Android device-setup page
    (accounts.google.com/embedded/setup/v2/android) sets once its login
    completes — for a permanent master_token.

    Returns {'email', 'master_token', 'android_id', 'captured_at'}, ready for
    adobe_pass.save_google_master_token(), or None on any failure. Always
    best-effort: this rides on top of a login that already succeeded by some
    other means, so a failure here should never surface as the pairing
    attempt's own error.
    """
    android_id = uuid.uuid4().hex[:16]
    try:
        result = gpsoauth.exchange_token('placeholder@gmail.com', oauth_token, android_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning('[google-master-token] exchange_token failed: %s', exc)
        return None
    master_token = result.get('Token')
    if not master_token:
        logger.warning('[google-master-token] exchange_token rejected: %s', result.get('Error', result))
        return None
    email = result.get('Email', '')
    try:
        verify = gpsoauth.perform_oauth(
            email, master_token, android_id,
            service=_YTTV_SCOPE, app=_YTTV_APP, client_sig=_YTTV_CLIENT_SIG,
        )
        if 'Auth' not in verify:
            logger.warning('[google-master-token] minted master_token failed YouTube TV verification: %s', verify.get('Error', verify))
            return None
    except Exception as exc:  # noqa: BLE001
        logger.warning('[google-master-token] YouTube TV verification call failed: %s', exc)
        return None
    return {'email': email, 'master_token': master_token, 'android_id': android_id, 'captured_at': int(time.time())}


def mint_browser_cookies(saved: dict) -> list[dict] | None:
    """Mint a fresh set of real Google session cookies (SID, HSID, SAPISID,
    __Secure-1PSID, etc.) from a saved master_token — no browser, no
    interactive login. Returns cookies shaped for Playwright/Camoufox's
    `context.add_cookies()`, or None on any failure (revoked token, password
    changed since capture, network error). Always best-effort — callers fall
    back to the ordinary interactive browser flow on None.
    """
    try:
        result = gpsoauth.perform_oauth(
            saved['email'], saved['master_token'], saved['android_id'],
            service=_UBERAUTH_SERVICE, app=_GMS_APP, client_sig=_GMS_CLIENT_SIG,
        )
        merge_url = result.get('Auth')
        if not merge_url:
            logger.warning('[google-master-token] uberauth mint failed: %s', result.get('Error', result))
            return None
        session = requests.Session()
        session.get(merge_url, timeout=15)
    except Exception as exc:  # noqa: BLE001
        logger.warning('[google-master-token] cookie mint failed: %s', exc)
        return None
    cookies = []
    for c in session.cookies:
        domain = c.domain if c.domain.startswith('.') else '.' + c.domain
        cookies.append({'name': c.name, 'value': c.value, 'domain': domain, 'path': c.path or '/'})
    return cookies or None

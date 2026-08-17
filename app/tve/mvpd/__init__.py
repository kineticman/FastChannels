"""Pluggable MVPD (TV-provider) sign-in backends for TVE scrapers.

Every TVE scraper (nbc_tve, fox_tve, fox_one, amcn_tve, discovery_tve, and
the legacy Cox-native client in app/tve/adobe_pass.py) needs to sign the
shared TVEAccount into whichever MVPD it's configured for once its own
family-specific REST call has reached the login step. Cox has a fast native
login (still hand-rolled per file — see each scraper's own _cox_saml_login;
deliberately NOT centralized here, since those are already proven in
production with small per-family differences and unifying them risks
regressing something that currently works for no real benefit). Every other
MVPD's actual login mechanics are protocol/vendor-specific but otherwise
interchangeable from a caller's point of view, so they're centralized here
behind one dispatcher — add a new provider by writing one function in its
own module and registering it below; no scraper needs to change.
"""
from __future__ import annotations

from urllib.parse import urlsplit

from ..adobe_pass import TVEAuthError, TVENotAuthorizedError, directv_login_cooldown, _mark_directv_login_failed

# host substring each MSO's login page/redirect must actually live on,
# checked before any credentials go out — catches a caller passing the wrong
# page (e.g. a mismatched mso_id/redirect pairing upstream) instead of
# silently attempting a provider's login flow against an unrelated host.
# Providers with no fixed host (DIRECTV never redirects to begin with — see
# directv.py's directv_login() docstring) simply have no entry here.
_EXPECTED_HOST_SUBSTRING = {
    'Comcast_SSO': 'xfinity.com',
}


def login_to_mvpd(
    mso_id: str, page_html: str, page_url: str, username: str, password: str,
    *, cookie_jar: dict | None = None,
) -> str:
    """Sign in to `mso_id` starting from the login page Adobe Pass's own
    authenticate call already produced (`page_html`/`page_url` — the actual
    login-page body for MSOs that respond 200 directly, e.g. DIRECTV, or an
    intermediate redirect target's page for MSOs that 3xx instead, e.g.
    Xfinity). Returns the final landed URL once login completes, so every
    caller gets the same contract regardless of MSO-specific completion
    mechanics — callers that only need the session-binding side effect
    (most v2 REST families) can ignore the return value; callers that need
    to extract a `code` query param from wherever their own redirect chain
    lands (Discovery TVE) use it directly.

    Raises TVEAuthError for any MSO with no backend registered here yet
    (Cox is intentionally excluded — see module docstring).
    """
    expected_host = _EXPECTED_HOST_SUBSTRING.get(mso_id)
    if expected_host and expected_host not in page_url:
        raise TVEAuthError(f'Unexpected {mso_id} login host: {urlsplit(page_url).netloc}.')

    if mso_id == 'Comcast_SSO':
        if not cookie_jar:
            raise TVEAuthError(
                'Comcast_SSO needs a saved Xfinity cookie jar — needs a browser-assisted sign-in to harvest one first.'
            )
        from .xfinity import xfinity_cookie_jar_login
        return xfinity_cookie_jar_login(page_url, username, password, cookie_jar)

    if mso_id == 'DTV':
        # Circuit breaker, not just spacing (directv.py's own
        # throttle_directv_login() already spaces individual attempts by
        # 20s) — confirmed live 2026-08-17 that Akamai's block on
        # identity.directv.com outlasts a 90s gap. Every caller here
        # (this legacy client and 5 v2 REST scrapers) calls login_to_mvpd()
        # once per brand/channel with no memory of the others, so a single
        # audit pass across a multi-brand scraper (Warner's TNT/TBS/truTV,
        # A+E's four networks) would otherwise burn one live attempt per
        # brand against an identity provider already known to be blocked —
        # each one just re-triggering (and likely extending) the same
        # cooldown. Short-circuits instead.
        cooldown = directv_login_cooldown()
        if cooldown:
            raise TVEAuthError(
                f'DIRECTV sign-in is cooling down for {cooldown["remaining"]}s after a recent failure '
                f'({cooldown["reason"]}) — not retrying yet.'
            )
        from .directv import directv_login
        try:
            return directv_login(page_html, page_url, username, password)
        except TVENotAuthorizedError:
            raise  # definitive answer from Adobe — retrying won't change it, not a cooldown case
        except TVEAuthError as exc:
            _mark_directv_login_failed(str(exc))
            raise

    raise TVEAuthError(f'{mso_id}: no scripted sign-in is wired up for this provider yet.')

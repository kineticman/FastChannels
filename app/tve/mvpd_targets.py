"""Known TVE requestor_id targets for MVPD browser-assisted pairing.

Centralizes how to get (resource, software_statement, redirect_url) for each
network covered by the generic yt-dlp/browser MVPD path (Warner TVE, A+E
Networks TVE) — used by both the pairing API route and the post-pairing
sibling auto-pair step in app/worker.py. Scraper classes are imported lazily
(function-local) to avoid an import cycle, since those scrapers import from
app.tve.adobe_pass.
"""
from __future__ import annotations

# requestor_id -> display name, for the admin picker.
REQUESTOR_CHOICES: list[dict] = [
    {'requestor_id': 'HISTORY', 'name': 'History (A+E Networks TVE)'},
    {'requestor_id': 'AETV', 'name': 'A&E (A+E Networks TVE)'},
    {'requestor_id': 'LIFETIME', 'name': 'Lifetime (A+E Networks TVE)'},
    {'requestor_id': 'FYI', 'name': 'FYI (A+E Networks TVE)'},
    {'requestor_id': 'TNT', 'name': 'TNT (Warner TVE)'},
    {'requestor_id': 'TBS', 'name': 'TBS (Warner TVE)'},
    {'requestor_id': 'TRUTV', 'name': 'truTV (Warner TVE)'},
]

_WARNER_BRAND_KEYS = {'TNT': 'tnt', 'TBS': 'tbs', 'TRUTV': 'trutv'}


def resolve_requestor_target(requestor_id: str) -> dict:
    """Returns {'requestor_id', 'resource', 'software_statement', 'redirect_url'}
    for a requestor_id.

    The input (and REQUESTOR_CHOICES / all_requestor_ids()) uses a fixed
    uppercase admin-UI convention ('TRUTV', 'TNT', ...) purely as a stable
    lookup/display key. The returned 'requestor_id' is the value Adobe Pass
    actually expects on the wire, which for most networks equals the input
    but for Warner's truTV is genuinely cased `truTV` — confirmed via a real
    sign-in HAR (dev/tnt/tru.har, 2026-08-06) and truTV's own live page
    config (top2.auth.authBrand). Sending the wrong case there makes Adobe's
    regcode/authenticate calls behave as if truTV+Cox weren't paired at all
    (an immediate bounce back to redirect_url, misread as "not a
    participating provider" — TNT/TBS never surfaced this because their
    authBrand happens to already match the uppercase admin key). Callers
    building an AdobePassCoxClient, or caching/looking up a captured
    authn_token, must use this 'requestor_id' — not the raw input — so the
    admin browser-login flow stays consistent with authorize_mvpd()'s
    play-time cache lookup, which always uses the scraper's own correctly-
    cased value (see warner_tve.py's resolve()).
    """
    requestor_id = (requestor_id or '').strip().upper()

    from ..scrapers.aenetworks_tve import _NETWORKS
    from .adobe_pass import discover_aenetworks_software_statement
    aenetworks_by_requestor = {n.requestor_id: n for n in _NETWORKS.values()}
    if requestor_id in aenetworks_by_requestor:
        network = aenetworks_by_requestor[requestor_id]
        return {
            'requestor_id': network.requestor_id,
            'resource': network.resource,
            'software_statement': discover_aenetworks_software_statement(network.brand),
            'redirect_url': network.redirect_url,
        }

    if requestor_id in _WARNER_BRAND_KEYS:
        from ..scrapers import registry
        from ..scrapers.warner_tve import BRAND_SITES
        scraper_cls = registry.get('warner_tve')
        if not scraper_cls:
            raise ValueError('Warner TVE scraper is not registered')
        scraper = scraper_cls(config={})
        brand_key = _WARNER_BRAND_KEYS[requestor_id]
        brand_cfg = scraper._brand_config(brand_key)
        site = BRAND_SITES[brand_key]
        return {
            'requestor_id': brand_cfg['requestor_id'],
            'resource': brand_cfg['requestor_id'],
            'software_statement': brand_cfg['software_statement'],
            'redirect_url': site.url,
        }

    raise ValueError(f'Unknown TVE requestor_id: {requestor_id}')


def all_requestor_ids() -> list[str]:
    return [c['requestor_id'] for c in REQUESTOR_CHOICES]

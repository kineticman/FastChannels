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
    """Returns {'resource', 'software_statement', 'redirect_url'} for a requestor_id."""
    requestor_id = (requestor_id or '').strip().upper()

    from ..scrapers.aenetworks_tve import _NETWORKS
    from .adobe_pass import discover_aenetworks_software_statement
    aenetworks_by_requestor = {n.requestor_id: n for n in _NETWORKS.values()}
    if requestor_id in aenetworks_by_requestor:
        network = aenetworks_by_requestor[requestor_id]
        return {
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
            'resource': brand_cfg['requestor_id'],
            'software_statement': brand_cfg['software_statement'],
            'redirect_url': site.url,
        }

    raise ValueError(f'Unknown TVE requestor_id: {requestor_id}')


def all_requestor_ids() -> list[str]:
    return [c['requestor_id'] for c in REQUESTOR_CHOICES]

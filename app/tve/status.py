"""Per-network TVE sign-in status, for the admin settings page.

Reports *when* each network last had a sign-in succeed (browser-assisted or
scripted), not whether it's currently valid — cached credentials have been
observed to expire unpredictably (see app/worker.py's cascade functions), so
a "signed in" badge implying current truth would be misleading in exactly
the way the old "Test" button already was. A timestamp is honest; resolve()
still surfaces a real error if a cached credential has gone stale, and the
account's "Sign in (browser)" flow re-establishes it.
"""
from __future__ import annotations


def tve_network_status(account) -> list[dict]:
    from .mvpd_targets import REQUESTOR_CHOICES

    cfg = (account.config or {}) if account else {}
    entries: list[dict] = []

    # 'family' + 'requestor_id' tell the admin UI which sign-in endpoint (if
    # any) a row's "Sign in (browser)" button should drive:
    #   'legacy' -> POST /api/settings/tve/browser-login/start {requestor_id}
    #   'nbc'    -> POST /api/settings/tve/nbc/browser-login/start (fixed target)
    #   'fox'    -> POST /api/settings/tve/fox/browser-login/start (fixed target)
    #   None     -> no standalone trigger exists; only reachable via another
    #               row's cascade (see app.worker._silent_pair_amcn/_discovery)
    mvpd_authn = cfg.get('mvpd_authn') or {}
    for choice in REQUESTOR_CHOICES:
        cached = mvpd_authn.get(choice['requestor_id']) or {}
        entries.append({
            'label': choice['name'],
            'last_signed_in_at': cached.get('captured_at'),
            'note': None,
            'family': 'legacy',
            'requestor_id': choice['requestor_id'],
        })

    nbc = cfg.get('nbc_mvpd_auth') or {}
    entries.append({
        'label': 'NBC TVE',
        'last_signed_in_at': nbc.get('captured_at'),
        'note': None,
        'family': 'nbc',
        'requestor_id': None,
    })

    entries.append({
        'label': 'FOX Sports TVE',
        'last_signed_in_at': cfg.get('fox_sports_access_token_captured_at'),
        'note': None,
        'family': 'fox',
        'requestor_id': None,
    })

    entries.append({
        'label': 'AMC Networks TVE',
        'last_signed_in_at': None,
        'note': "Only native Cox sign-in is cached today — signs in fresh on every play regardless. Signed in automatically when you sign into another network above.",
        'family': None,
        'requestor_id': None,
    })

    disco_cached_at = None
    try:
        from ..config_store import load_source_cache_by_name
        disco_cache = load_source_cache_by_name('discovery_tve', keys=['discovery_tve_session'])
        disco_cached_at = (disco_cache.get('discovery_tve_session') or {}).get('cached_at')
    except Exception:  # noqa: BLE001
        pass
    entries.append({
        'label': 'Discovery TVE',
        'last_signed_in_at': disco_cached_at,
        'note': "Signed in automatically when you sign into another network above." if not disco_cached_at else None,
        'family': None,
        'requestor_id': None,
    })

    return entries

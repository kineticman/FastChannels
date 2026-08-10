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

_AMCN_REQUESTOR_IDS = ('AMC', 'BBCA', 'IFC', 'WETV')


def tve_network_status(account) -> list[dict]:
    from .mvpd_targets import REQUESTOR_CHOICES, resolve_requestor_target

    cfg = (account.config or {}) if account else {}
    entries: list[dict] = []

    # 'family' + 'requestor_id' tell the admin UI which sign-in endpoint a
    # row's "Sign in (browser)" button should drive:
    #   'legacy'    -> POST /api/settings/tve/browser-login/start {requestor_id}
    #   'nbc'       -> POST /api/settings/tve/nbc/browser-login/start (fixed target)
    #   'fox'       -> POST /api/settings/tve/fox/browser-login/start (fixed target)
    #   'amcn'      -> POST /api/settings/tve/amcn/browser-login/start (fixed target)
    #   'discovery' -> POST /api/settings/tve/discovery/browser-login/start (fixed target)
    # AMC Networks TVE and Discovery TVE are ALSO still swept for free by the
    # legacy family's "sign in once" cascade (app.worker._silent_pair_amcn/
    # _discovery, triggered by "Sign in to all") — these standalone buttons
    # just mean you no longer have to run that whole sweep only to reach them.
    mvpd_authn = cfg.get('mvpd_authn') or {}
    for choice in REQUESTOR_CHOICES:
        # Cached tokens are stored under the wire-protocol requestor_id
        # (resolve_requestor_target's 'requestor_id'), which for Warner's
        # truTV differs in case from this raw admin-UI key — see
        # resolve_requestor_target's docstring. Falls back to the raw key on
        # any resolution error so a transient failure just shows "Never"
        # instead of breaking the whole status list.
        try:
            cache_key = resolve_requestor_target(choice['requestor_id'])['requestor_id']
        except Exception:  # noqa: BLE001
            cache_key = choice['requestor_id']
        cached = mvpd_authn.get(cache_key) or {}
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

    # FOX One authenticates natively (username/password OAuth against Cox's
    # own identityhydra endpoints, not a browser-pairing flow), so there's no
    # dedicated sign-in button — it reuses the Cox login above automatically
    # the first time a channel needs a token. The timestamp lives on the
    # fox_one source's own config, not the shared account config the other
    # entries in this function read from.
    fox_one_captured_at = None
    try:
        from ..models import Source
        fox_one_source = Source.query.filter_by(name='fox_one').first()
        if fox_one_source:
            fox_one_captured_at = (fox_one_source.config or {}).get('access_token_captured_at')
    except Exception:  # noqa: BLE001
        pass
    entries.append({
        'label': 'FOX One',
        'last_signed_in_at': fox_one_captured_at,
        'note': 'Uses the Cox login above — authenticates automatically, no separate sign-in.',
        'family': None,
        'requestor_id': None,
    })

    amcn_cached_at = None
    try:
        from ..config_store import load_source_cache_by_name
        amcn_keys = [f'adobe_auth:{rid}' for rid in _AMCN_REQUESTOR_IDS]
        amcn_cache = load_source_cache_by_name('amcn_tve', keys=amcn_keys)
        stamps = [v.get('cached_at') for v in amcn_cache.values() if isinstance(v, dict) and v.get('cached_at')]
        amcn_cached_at = max(stamps) if stamps else None
    except Exception:  # noqa: BLE001
        pass
    entries.append({
        'label': 'AMC Networks TVE',
        'last_signed_in_at': amcn_cached_at,
        'note': None,
        'family': 'amcn',
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
        'note': None,
        'family': 'discovery',
        'requestor_id': None,
    })

    return entries

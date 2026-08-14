"""Per-network TVE sign-in status, for the admin settings page.

Reports *when* each network last had a sign-in succeed (browser-assisted or
scripted), not whether it's currently valid — cached credentials have been
observed to expire unpredictably, so a "signed in" badge implying current
truth would be misleading in exactly the way the old "Test" button already
was. A timestamp is honest; resolve()
still surfaces a real error if a cached credential has gone stale, and the
account's "Sign in (browser)" flow re-establishes it.
"""
from __future__ import annotations

_AMCN_REQUESTOR_IDS = ('AMC', 'BBCA', 'IFC', 'WETV')


def tve_network_status(account) -> list[dict]:
    from .mvpd_targets import REQUESTOR_CHOICES, resolve_requestor_target

    cfg = (account.config or {}) if account else {}
    entries: list[dict] = []
    errors = cfg.get('tve_last_error') or {}

    def _last_error(key: str, last_signed_in_at) -> tuple[str | None, int | None]:
        """A network that's never signed in successfully just shows "Never"
        with no indication why (confirmed live 2026-08-11: FYI came back
        "not entitled" while its A+E siblings all succeeded, and there was
        no way to tell that from the admin page). Only surfaces an error
        that's NEWER than the last success — a later successful sign-in
        naturally supersedes an older failure, no explicit clearing needed.
        """
        err = errors.get(key) or {}
        at = err.get('at')
        if not at or (last_signed_in_at and at <= last_signed_in_at):
            return None, None
        return err.get('message'), at

    # 'family' + 'requestor_id' tell the admin UI which sign-in endpoint a
    # row's "Sign in (browser)" button should drive:
    #   'legacy'    -> POST /api/settings/tve/browser-login/start {requestor_id}
    #   'nbc'       -> POST /api/settings/tve/nbc/browser-login/start (fixed target)
    #   'fox'       -> POST /api/settings/tve/fox/browser-login/start (fixed target)
    #   'amcn'      -> POST /api/settings/tve/amcn/browser-login/start (fixed target)
    #   'discovery' -> POST /api/settings/tve/discovery/browser-login/start (fixed target)
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
        last_signed_in_at = cached.get('captured_at')
        error_message, error_at = _last_error(cache_key, last_signed_in_at)
        entries.append({
            'label': choice['name'],
            'last_signed_in_at': last_signed_in_at,
            'note': None,
            'family': 'legacy',
            'requestor_id': choice['requestor_id'],
            'last_error_message': error_message,
            'last_error_at': error_at,
        })

    nbc = cfg.get('nbc_mvpd_auth') or {}
    nbc_last_signed_in_at = nbc.get('captured_at')
    nbc_error_message, nbc_error_at = _last_error('nbc', nbc_last_signed_in_at)
    entries.append({
        'label': 'NBC TVE',
        'last_signed_in_at': nbc_last_signed_in_at,
        'note': None,
        'family': 'nbc',
        'requestor_id': None,
        'last_error_message': nbc_error_message,
        'last_error_at': nbc_error_at,
    })

    fox_last_signed_in_at = cfg.get('fox_sports_access_token_captured_at')
    fox_error_message, fox_error_at = _last_error('fox', fox_last_signed_in_at)
    entries.append({
        'label': 'FOX TVE',
        'last_signed_in_at': fox_last_signed_in_at,
        'note': None,
        'family': 'fox',
        'requestor_id': None,
        'last_error_message': fox_error_message,
        'last_error_at': fox_error_at,
    })

    # FOX One authenticates natively (username/password OAuth against Cox's
    # own identityhydra endpoints, not Adobe Pass) — it also happens
    # unprompted the first time a channel needs a token, but since it's a
    # fast scripted request/response (no browser), it doubles as a good,
    # low-friction way to check the raw Cox credentials are actually still
    # valid — see api.foxone_signin. family='foxone' routes its "Sign in"
    # button to that plain endpoint instead of the streamed-screenshot modal
    # every other family uses. The timestamp lives on the fox_one source's
    # own config, not the shared account config the other entries read from.
    fox_one_captured_at = None
    try:
        from ..models import Source
        fox_one_source = Source.query.filter_by(name='fox_one').first()
        if fox_one_source:
            fox_one_captured_at = (fox_one_source.config or {}).get('access_token_captured_at')
    except Exception:  # noqa: BLE001
        pass
    foxone_error_message, foxone_error_at = _last_error('foxone', fox_one_captured_at)
    entries.append({
        'label': 'FOX One',
        'last_signed_in_at': fox_one_captured_at,
        'note': None,
        'family': 'foxone',
        'requestor_id': None,
        'last_error_message': foxone_error_message,
        'last_error_at': foxone_error_at,
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
    amcn_error_message, amcn_error_at = _last_error('amcn', amcn_cached_at)
    entries.append({
        'label': 'AMC Networks TVE',
        'last_signed_in_at': amcn_cached_at,
        'note': None,
        'family': 'amcn',
        'requestor_id': None,
        'last_error_message': amcn_error_message,
        'last_error_at': amcn_error_at,
    })

    disco_cached_at = None
    try:
        from ..config_store import load_source_cache_by_name
        disco_cache = load_source_cache_by_name('discovery_tve', keys=['discovery_tve_session'])
        disco_cached_at = (disco_cache.get('discovery_tve_session') or {}).get('cached_at')
    except Exception:  # noqa: BLE001
        pass
    disco_error_message, disco_error_at = _last_error('discovery', disco_cached_at)
    entries.append({
        'label': 'Discovery TVE',
        'last_signed_in_at': disco_cached_at,
        'note': None,
        'family': 'discovery',
        'requestor_id': None,
        'last_error_message': disco_error_message,
        'last_error_at': disco_error_at,
    })

    return entries

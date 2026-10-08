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

    from .accounts import source_for_network_key, tve_account_for, uses_separate_signin
    from .providers import tve_account_mso_id, unsupported_network_reason

    cfg = (account.config or {}) if account else {}
    entries: list[dict] = []
    errors = cfg.get('tve_last_error') or {}

    # A source with a separate sign-in keeps its tokens, sign-in times and
    # errors on its own account (see app/tve/accounts.py), so each row reads
    # the account its source actually signs in with.
    source_accounts: dict = {}

    def _account(source_name):
        if source_name not in source_accounts:
            source_accounts[source_name] = (
                tve_account_for(source_name) if uses_separate_signin(source_name) else account)
        return source_accounts[source_name]

    def _cfg(source_name) -> dict:
        acct = _account(source_name)
        return (acct.config or {}) if acct else {}

    def _errors(source_name) -> dict:
        return _cfg(source_name).get('tve_last_error') or {}

    def _needs_signin(key: str, last_signed_in_at, errors=errors) -> bool:
        """A newer-than-last-success error that says only a person can fix it
        (see app/tve/signin_notice.py)."""
        err = errors.get(key) or {}
        at = err.get('at')
        return bool(err.get('needs_signin') and at and not (last_signed_in_at and at <= last_signed_in_at))

    def _last_error(key: str, last_signed_in_at, errors=errors) -> tuple[str | None, int | None]:
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
    for choice in REQUESTOR_CHOICES:
        source_name = source_for_network_key(choice['requestor_id'])
        mvpd_authn = _cfg(source_name).get('mvpd_authn') or {}
        source_errors = _errors(source_name)
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
        error_message, error_at = _last_error(cache_key, last_signed_in_at, source_errors)
        entries.append({
            'source': source_name,
            'label': choice['name'],
            'last_signed_in_at': last_signed_in_at,
            'note': None,
            'family': 'legacy',
            'requestor_id': choice['requestor_id'],
            'last_error_message': error_message,
            'last_error_at': error_at,
            'needs_signin': _needs_signin(cache_key, last_signed_in_at, source_errors),
        })

    nbc = _cfg('nbc_tve').get('nbc_mvpd_auth') or {}
    nbc_last_signed_in_at = nbc.get('captured_at')
    nbc_error_message, nbc_error_at = _last_error('nbc', nbc_last_signed_in_at, _errors('nbc_tve'))
    entries.append({
        'source': 'nbc_tve',
        'label': 'NBC TVE',
        'last_signed_in_at': nbc_last_signed_in_at,
        'note': None,
        'family': 'nbc',
        'requestor_id': None,
        'last_error_message': nbc_error_message,
        'last_error_at': nbc_error_at,
        'needs_signin': _needs_signin('nbc', nbc_last_signed_in_at, _errors('nbc_tve')),
    })

    tcm = _cfg('warner_tve').get('tcm_mvpd_auth') or {}
    tcm_last_signed_in_at = tcm.get('captured_at')
    tcm_error_message, tcm_error_at = _last_error('tcm', tcm_last_signed_in_at, _errors('warner_tve'))
    # Older link-login runs reported a definite entitlement denial as a
    # generic save failure. Show the useful Adobe result for those records too.
    old_prefix = 'TCM: save failed: '
    if tcm_error_message and tcm_error_message.startswith(old_prefix) and 'not entitled' in tcm_error_message.lower():
        tcm_error_message = tcm_error_message[len(old_prefix):]
    entries.append({
        'source': 'warner_tve',
        'label': 'TCM (Warner TVE)',
        'last_signed_in_at': tcm_last_signed_in_at,
        'note': 'Sign in with a link on your phone or computer.',
        'family': 'tcm',
        'requestor_id': None,
        'last_error_message': tcm_error_message,
        'last_error_at': tcm_error_at,
        'needs_signin': _needs_signin('tcm', tcm_last_signed_in_at, _errors('warner_tve')),
    })

    fox_last_signed_in_at = _cfg('fox_tve').get('fox_sports_access_token_captured_at')
    fox_error_message, fox_error_at = _last_error('fox', fox_last_signed_in_at, _errors('fox_tve'))
    entries.append({
        'source': 'fox_tve',
        'label': 'FOX TVE',
        'last_signed_in_at': fox_last_signed_in_at,
        'note': None,
        'family': 'fox',
        'requestor_id': None,
        'last_error_message': fox_error_message,
        'last_error_at': fox_error_at,
        'needs_signin': _needs_signin('fox', fox_last_signed_in_at, _errors('fox_tve')),
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
    amcn_error_message, amcn_error_at = _last_error('amcn', amcn_cached_at, _errors('amcn_tve'))
    entries.append({
        'source': 'amcn_tve',
        'label': 'AMC Networks TVE',
        'last_signed_in_at': amcn_cached_at,
        'note': None,
        'family': 'amcn',
        'requestor_id': None,
        'last_error_message': amcn_error_message,
        'last_error_at': amcn_error_at,
        'needs_signin': _needs_signin('amcn', amcn_cached_at, _errors('amcn_tve')),
    })

    disco_cached_at = None
    try:
        from ..config_store import load_source_cache_by_name
        disco_cache = load_source_cache_by_name('discovery_tve', keys=['discovery_tve_session'])
        disco_cached_at = (disco_cache.get('discovery_tve_session') or {}).get('cached_at')
    except Exception:  # noqa: BLE001
        pass
    disco_error_message, disco_error_at = _last_error('discovery', disco_cached_at, _errors('discovery_tve'))
    entries.append({
        'source': 'discovery_tve',
        'label': 'Discovery TVE',
        'last_signed_in_at': disco_cached_at,
        'note': None,
        'family': 'discovery',
        'requestor_id': None,
        'last_error_message': disco_error_message,
        'last_error_at': disco_error_at,
        'needs_signin': _needs_signin('discovery', disco_cached_at, _errors('discovery_tve')),
    })

    for entry in entries:
        acct = _account(entry['source'])
        separate = acct is not account
        mso_id = tve_account_mso_id(acct)
        entry['unsupported'] = unsupported_network_reason(entry.get('family') or '', mso_id)
        # Provider name when this network signs in with its own login
        # instead of the shared one...
        entry['separate_provider'] = (
            (((acct.config or {}).get('selected_mso_name') or mso_id) if acct else mso_id) if separate else None)
        # ...which only signs in by phone link, whatever the page-wide choice.
        entry['signin_method'] = 'phone' if separate else None
    return entries

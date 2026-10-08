"""Which TV-provider account a TVE source signs in with.

Every lookup of a TVEAccount row goes through here — nothing else should
query the table by provider_id. Today there is one row, the shared account
under Settings → TV Everywhere, and every source resolves to it. Routing
each caller through tve_account_for(<its source>) is what lets a source be
given a separate sign-in later by changing only this module.

Callers that genuinely mean the Settings account itself (the Settings page
and its API, the Google sign-in, FOX One's "use the shared login" path) use
shared_tve_account() and are unaffected by any per-source choice.
"""
from __future__ import annotations

SHARED_PROVIDER_ID = 'mvpd'
SHARED_DISPLAY_NAME = 'TV Provider'

# The keys sign-in status and errors are recorded under (tve_last_error,
# see app/tve/signin_notice.py) -> the source that network belongs to.
# Adobe requestor ids are matched case-insensitively (Warner's is 'truTV').
_NETWORK_KEY_SOURCES = {
    'nbc': 'nbc_tve',
    'fox': 'fox_tve',
    'amcn': 'amcn_tve',
    'discovery': 'discovery_tve',
    'foxone': 'fox_one',
    'espn': 'espn',
    'tcm': 'warner_tve',
    'tnt': 'warner_tve',
    'tbs': 'warner_tve',
    'trutv': 'warner_tve',
    'history': 'aenetworks_tve',
    'aetv': 'aenetworks_tve',
    'lifetime': 'aenetworks_tve',
    'fyi': 'aenetworks_tve',
    'amc': 'amcn_tve',
    'bbca': 'amcn_tve',
    'ifc': 'amcn_tve',
    'wetv': 'amcn_tve',
}


def shared_tve_account():
    """The account under Settings → TV Everywhere, or None if never saved."""
    from ..models import TVEAccount

    return TVEAccount.query.filter_by(provider_id=SHARED_PROVIDER_ID).first()


def get_or_create_shared_tve_account():
    from ..extensions import db
    from ..models import TVEAccount

    account = shared_tve_account()
    if account:
        return account
    account = TVEAccount(
        provider_id=SHARED_PROVIDER_ID, display_name=SHARED_DISPLAY_NAME, is_enabled=False, config={})
    db.session.add(account)
    db.session.flush()
    return account


def tve_account_for(source_name: str | None):
    """The account `source_name` (a Source.name, e.g. 'nbc_tve') signs in
    with. Always the shared one for now. None means the caller has no TVE
    source of its own (FOX One's shared-login path, the Spectrum source's
    sign-in, the Settings-level Google sign-in) and gets the shared one."""
    return shared_tve_account()


def source_for_network_key(key: str | None) -> str | None:
    """The source a status/error key or Adobe requestor id belongs to."""
    return _NETWORK_KEY_SOURCES.get((key or '').strip().lower())


def tve_account_for_network(key: str | None):
    """tve_account_for(), for callers that only know the network's
    status/error key or requestor id."""
    return tve_account_for(source_for_network_key(key))

"""Which TV-provider account a TVE source signs in with.

Every lookup of a TVEAccount row goes through here — nothing else should
query the table by provider_id. There is one shared row (Settings → TV
Everywhere) that every source uses by default, plus an optional row per
source in SEPARATE_SIGNIN_SOURCES for a source set to sign in with a
different TV provider login (provider_id 'mvpd:<source name>'; in use while
its is_enabled is set). A separate row holds its own provider and sign-in
status. It is signed in only by phone link (app/tve/link_login.py), started
from the source's card, so it stores no username/password and never uses a
browser profile.

Callers that genuinely mean the Settings account itself (the Settings page
and its API, the Google sign-in, FOX One's "use the shared login" path) use
shared_tve_account() and are unaffected by any per-source choice.
"""
from __future__ import annotations

SHARED_PROVIDER_ID = 'mvpd'
SHARED_DISPLAY_NAME = 'TV Provider'

# Sources that can be given a separate sign-in. Add a source here only once
# its phone-link sign-in, status row and scripted renewal all follow its
# account.
SEPARATE_SIGNIN_SOURCES = frozenset({'discovery_tve'})
# source -> its 'family' in app/tve/providers.py's unsupported-provider table.
_SOURCE_FAMILIES = {'discovery_tve': 'discovery'}
# source -> SourceCache keys holding its signed-in session, dropped when the
# login it signs in with changes.
SIGNIN_CACHE_KEYS = {'discovery_tve': ('discovery_tve_session',)}

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


def separate_tve_account(source_name: str | None):
    """The source's own account row whether or not it's in use, or None."""
    if source_name not in SEPARATE_SIGNIN_SOURCES:
        return None
    from ..models import TVEAccount

    return TVEAccount.query.filter_by(provider_id=f'{SHARED_PROVIDER_ID}:{source_name}').first()


def get_or_create_separate_tve_account(source_name: str):
    from ..extensions import db
    from ..models import TVEAccount

    if source_name not in SEPARATE_SIGNIN_SOURCES:
        raise ValueError(f'{source_name} cannot have a separate TV provider sign-in')
    account = separate_tve_account(source_name)
    if account:
        return account
    account = TVEAccount(
        provider_id=f'{SHARED_PROVIDER_ID}:{source_name}', display_name=SHARED_DISPLAY_NAME,
        is_enabled=False, config={})
    db.session.add(account)
    db.session.flush()
    return account


def uses_separate_signin(source_name: str | None) -> bool:
    account = separate_tve_account(source_name)
    return bool(account and account.is_enabled)


def any_separate_signin() -> bool:
    return any(uses_separate_signin(name) for name in SEPARATE_SIGNIN_SOURCES)


def tve_account_for(source_name: str | None):
    """The account `source_name` (a Source.name, e.g. 'nbc_tve') signs in
    with: its own when it's set to a separate sign-in, else the shared one.
    None means the caller has no TVE source of its own (FOX One's
    shared-login path, the Spectrum source's sign-in, the Settings-level
    Google sign-in) and gets the shared one."""
    own = separate_tve_account(source_name)
    if own and own.is_enabled:
        return own
    return shared_tve_account()


def source_family(source_name: str) -> str:
    return _SOURCE_FAMILIES.get(source_name, '')


def separate_signin_info(source_name: str) -> dict | None:
    """What a source's card needs to show its sign-in choice, or None when
    the source can't have a separate sign-in. Never includes a password."""
    if source_name not in SEPARATE_SIGNIN_SOURCES:
        return None
    from .providers import UNSUPPORTED_NETWORK_PROVIDERS, tve_account_mso_id, ytdlp_adobe_mso_providers

    own = separate_tve_account(source_name)
    own_cfg = (own.config or {}) if own else {}
    shared = shared_tve_account()
    shared_ready = bool(shared and shared.is_enabled and shared.has_credentials())
    return {
        'mode': 'separate' if own and own.is_enabled else 'shared',
        'provider_id': tve_account_mso_id(own) if own_cfg.get('selected_mso_id') else '',
        # The phone-link sign-in to start for this source (/api/settings/tve/link-login).
        'family': source_family(source_name),
        'providers': [{'id': p['id'], 'name': p['name']} for p in ytdlp_adobe_mso_providers()],
        'unsupported_providers': dict(UNSUPPORTED_NETWORK_PROVIDERS.get(source_family(source_name)) or {}),
        'shared_provider': (
            ((shared.config or {}).get('selected_mso_name') or tve_account_mso_id(shared)) if shared_ready else ''),
    }


def source_for_network_key(key: str | None) -> str | None:
    """The source a status/error key or Adobe requestor id belongs to."""
    return _NETWORK_KEY_SOURCES.get((key or '').strip().lower())


def tve_account_for_network(key: str | None):
    """tve_account_for(), for callers that only know the network's
    status/error key or requestor id."""
    return tve_account_for(source_for_network_key(key))

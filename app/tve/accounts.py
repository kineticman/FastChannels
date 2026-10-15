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
# its phone-link sign-in, status rows and scripted renewal all follow its
# account.
SEPARATE_SIGNIN_SOURCES = frozenset({
    'nbc_tve', 'fox_tve', 'warner_tve', 'aenetworks_tve', 'amcn_tve', 'discovery_tve',
})

# What a source's sign-in leaves behind outside its account row. All of it is
# dropped when the login the source signs in with changes, so nothing from the
# previous login can keep it playing or make it look signed in:
#   cache / cache_like — SourceCache keys (exact / SQL LIKE) holding sessions
#   config             — Source.config keys holding the device identity the
#                        sign-in was bound to
#   account            — keys on the account being left that record a
#                        sign-in made with that device identity
SIGNIN_STATE = {
    'nbc_tve': {'cache': ('nbc_entitlements', 'nbc_playback'), 'cache_like': ('adobe_auth:%',),
                'config': ('device_fingerprint',), 'account': ('nbc_mvpd_auth',)},
    'warner_tve': {'cache': ('warner_manifest',), 'config': ('tcm_device_fingerprint',),
                   'account': ('tcm_mvpd_auth',)},
    'amcn_tve': {'cache_like': ('adobe_auth:%', 'adobe_session:%'), 'config': ('device_id',)},
    'discovery_tve': {'cache': ('discovery_tve_session',)},
    # FOX and A+E keep everything on the account row itself.
    'fox_tve': {},
    'aenetworks_tve': {},
}

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


def separate_signin_info(source_name: str) -> dict | None:
    """What a source's card needs to show its sign-in choice, or None when
    the source can't have a separate sign-in. Never includes a password."""
    if source_name not in SEPARATE_SIGNIN_SOURCES:
        return None
    from .providers import tve_account_mso_id, ytdlp_adobe_mso_providers

    own = separate_tve_account(source_name)
    own_cfg = (own.config or {}) if own else {}
    shared = shared_tve_account()
    shared_ready = bool(shared and shared.is_enabled and shared.has_credentials())
    return {
        'mode': 'separate' if own and own.is_enabled else 'shared',
        'provider_id': tve_account_mso_id(own) if own_cfg.get('selected_mso_id') else '',
        'providers': [{'id': p['id'], 'name': p['name']} for p in ytdlp_adobe_mso_providers()],
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


def clear_signin_state(source, leaving_account) -> None:
    """Drop what `source` (a Source row) kept from signing in with
    `leaving_account` — see SIGNIN_STATE. Doesn't commit."""
    from ..extensions import db
    from ..models import SourceCache

    state = SIGNIN_STATE.get(source.name) or {}
    conds = [SourceCache.cache_key.like(pattern) for pattern in state.get('cache_like', ())]
    if state.get('cache'):
        conds.append(SourceCache.cache_key.in_(state['cache']))
    if conds:
        SourceCache.query.filter(SourceCache.source_id == source.id, db.or_(*conds)).delete(synchronize_session=False)
    if state.get('config'):
        source.config = {k: v for k, v in (source.config or {}).items() if k not in state['config']}
    if leaving_account is not None and state.get('account'):
        leaving_account.config = {
            k: v for k, v in (leaving_account.config or {}).items() if k not in state['account']}


# Network key (lowercase) -> the channels it covers, as a LIKE pattern on
# Channel.source_channel_id, for sources whose networks sign in separately.
# A key that isn't here covers its whole source.
_NETWORK_CHANNEL_PATTERNS = {
    'tnt': 'tnt-%', 'tbs': 'tbs-%', 'trutv': 'tru-%', 'tcm': 'tcm-%',
    'history': 'history', 'aetv': 'aetv', 'lifetime': 'lifetime', 'fyi': 'fyi',
}


def reenable_not_authorized_channels(network_key: str | None) -> int:
    """Bring back the channels of one network that were switched off as
    "not authorized", once a sign-in for it has just come back authorized
    (e.g. after its source moved to a TV provider that carries it). Only
    then: with a working sign-in a channel the account still isn't entitled
    to is switched off again by its next play or audit, whereas re-enabling
    without one would leave it enabled and unplayable. Commits; returns how
    many channels came back."""
    from datetime import datetime, timezone

    from ..extensions import db
    from ..models import Channel, Source

    source = Source.query.filter_by(name=source_for_network_key(network_key) or '').first()
    if not source:
        return 0
    query = Channel.query.filter_by(source_id=source.id, disable_reason='NotAuthorized')
    pattern = _NETWORK_CHANNEL_PATTERNS.get((network_key or '').strip().lower())
    if pattern:
        query = query.filter(Channel.source_channel_id.like(pattern))
    revived = query.all()
    for ch in revived:
        # The same fields a manual re-enable sets (app/routes/api_channels.py).
        ch.disable_reason = None
        ch.is_active = True
        ch.is_enabled = True
        ch.last_seen_at = datetime.now(timezone.utc)
        ch.missed_scrapes = 0
    if revived:
        db.session.commit()
    return len(revived)

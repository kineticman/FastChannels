from .mvpd_catalog import accepted_providers

# Adobe's display names, where ours read better. Adobe has no MVPD called
# "Cox Spectrum": a Cox account migrated to Spectrum is told to pick it
# (IDLI-4213, confirmed live 2026-09-24), and Adobe's "Cox" is what now hands
# off to Spectrum's login page, so it's labelled so users can find it.
_LABELS = {
    'Cox': 'Cox / Cox Spectrum',
    'DTV': 'DIRECTV',
    'ATTOTT': 'DIRECTV STREAM',
}


def _friendly_sort_key(provider: dict) -> tuple[int, str]:
    preferred = {
        'Cox': 0,
        'Comcast_SSO': 1,
        'Spectrum': 2,
        'DTV': 3,
        'ATTOTT': 4,
        'ATT': 5,
        'Verizon': 6,
        'Cablevision': 7,
        'Philo': 8,
        'Fubo': 9,
        'slingtv': 10,
        'YouTubeTV': 11,
    }
    return preferred.get(provider['id'], 1000), provider['name'].casefold()


def tve_provider_choices(keep: dict[str, str] | None = None) -> list[dict]:
    """The TV providers a user can pick: every provider at least one of our
    networks accepts (app/tve/mvpd_catalog.py), most common first.

    `keep` is {id: name} for providers already saved on an account. One no
    longer on any network's list stays offered, marked `retired`, so its
    dropdown keeps showing it instead of silently falling back to another
    provider and its form can still be saved."""
    accepted = accepted_providers()
    providers = [{'id': i, 'name': _LABELS.get(i, name)} for i, name in accepted.items()]
    for mvpd_id, name in (keep or {}).items():
        if mvpd_id and mvpd_id not in accepted:
            providers.append({'id': mvpd_id, 'name': f'{name or mvpd_id} (no longer listed)', 'retired': True})
    return sorted(providers, key=_friendly_sort_key)


def saved_providers(*accounts) -> dict[str, str]:
    """{id: name} of the providers saved on these TVE accounts, for
    tve_provider_choices(keep=...)."""
    saved = {}
    for account in accounts:
        cfg = (account.config or {}) if account else {}
        mvpd_id = (cfg.get('selected_mso_id') or '').strip()
        if mvpd_id:
            saved[mvpd_id] = (cfg.get('selected_mso_name') or mvpd_id).strip()
    return saved


# TVE networks that can't work with a given TV provider, keyed by the admin
# UI's sign-in family (see app/tve/status.py) then mso_id -> user-facing
# reason. The settings page shows the reason in place of that network's
# "Sign in" button, "Sign in to all" skips it, and its start route refuses.
UNSUPPORTED_NETWORK_PROVIDERS: dict[str, dict[str, str]] = {
    # (empty) FOX One + Cox was listed here until 2026-09-25; lifted once a
    # real browser capture showed FOX One signing in through "Cox / Cox
    # Spectrum" on fox.com.
}


def tve_account_mso_id(account) -> str:
    """The MVPD id the TVE sign-in start routes actually use (same order)."""
    cfg = (account.config or {}) if account else {}
    return (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or 'Cox').strip()


def unsupported_network_reason(family: str, mso_id: str) -> str | None:
    return (UNSUPPORTED_NETWORK_PROVIDERS.get(family) or {}).get(mso_id)


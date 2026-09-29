import json
import re
import subprocess
from functools import lru_cache
from pathlib import Path


_SUPPLEMENTAL_PROVIDERS_PATH = Path(__file__).resolve().parents[2] / 'docs' / 'tve_provider_availability.json'

_FALLBACK_ADOBE_MSO_PROVIDERS = [
    {"id": "Cox", "name": "Cox"},
    {"id": "Comcast_SSO", "name": "Comcast XFINITY"},
    {"id": "Spectrum", "name": "Spectrum"},
    {"id": "DTV", "name": "DIRECTV"},
    {"id": "ATT", "name": "AT&T U-verse"},
    {"id": "Verizon", "name": "Verizon FiOS"},
    {"id": "Cablevision", "name": "Optimum/Cablevision"},
    {"id": "Philo", "name": "Philo"},
    {"id": "Fubo", "name": "Fubo"},
    {"id": "slingtv", "name": "Sling TV"},
    {"id": "YouTubeTV", "name": "YouTube TV"},
]

# Legacy Charter/TWC MVPDs still in yt-dlp's bundled list, but no network we
# support accepts them (Adobe's per-requestor MVPD config + Discovery's partner
# list, checked 2026-09-25); Spectrum customers must use 'Spectrum'.
_RETIRED_MSO_IDS = frozenset({'TWC', 'Charter_Direct'})

_PROVIDER_LINE_RE = re.compile(r'^(?P<id>\S+)\s+(?P<name>.+?)\s*$')


def _supplemental_providers() -> list[dict]:
    # yt-dlp's --ap-list-mso is a static list bundled with yt-dlp and lags
    # Adobe Pass's real MVPD roster -- smaller/regional providers (e.g. Blue
    # Stream Fiber, id 'tpc010') are confirmed live, participating MVPDs (see
    # docs/tve_provider_availability.json, scraped from getchannels.com,
    # which uses the same mso_id namespace as yt-dlp -- e.g. both list Cox
    # as 'Cox' and Xfinity as 'Comcast_SSO') but are missing from yt-dlp's
    # bundled list entirely. Merge them in so the dropdown isn't limited to
    # yt-dlp's coverage.
    try:
        with open(_SUPPLEMENTAL_PROVIDERS_PATH) as f:
            data = json.load(f)
    except Exception:
        return []
    providers = []
    for provider_id, info in data.get('providers', {}).items():
        label = (info or {}).get('label')
        if not provider_id or not label:
            continue
        providers.append({'id': provider_id, 'name': label})
    return providers


def _friendly_sort_key(provider: dict) -> tuple[int, str]:
    preferred = {
        'Cox': 0,
        'Comcast_SSO': 1,
        'Spectrum': 2,
        'DTV': 3,
        'ATT': 4,
        'Verizon': 5,
        'Cablevision': 6,
        'Philo': 7,
        'Fubo': 8,
        'slingtv': 9,
        'YouTubeTV': 10,
    }
    return preferred.get(provider['id'], 1000), provider['name'].casefold()


@lru_cache(maxsize=1)
def ytdlp_adobe_mso_providers() -> list[dict]:
    providers: list[dict] = []
    seen: set[str] = set()
    try:
        proc = subprocess.run(
            ['yt-dlp', '--ap-list-mso'],
            check=True,
            capture_output=True,
            text=True,
            timeout=8,
        )
        for line in proc.stdout.splitlines():
            line = line.strip()
            if not line or line.startswith('Supported TV Providers') or line.startswith('mso '):
                continue
            match = _PROVIDER_LINE_RE.match(line)
            if not match:
                continue
            provider_id = match.group('id').strip()
            name = match.group('name').strip()
            if not provider_id or provider_id in seen:
                continue
            providers.append({'id': provider_id, 'name': name})
            seen.add(provider_id)
    except Exception:
        pass

    if not providers:
        providers = list(_FALLBACK_ADOBE_MSO_PROVIDERS)
        seen = {p['id'] for p in providers}

    for provider in _supplemental_providers():
        if provider['id'] in seen:
            continue
        providers.append(provider)
        seen.add(provider['id'])

    if 'Cox' not in seen:
        providers.append({'id': 'Cox', 'name': 'Cox'})
    # Spectrum's own error for a Cox account migrated to Spectrum tells the
    # user to pick "Cox Spectrum" (IDLI-4213, confirmed live 2026-09-24).
    # Adobe has no MVPD by that name — it's Adobe's "Cox", which now hands
    # off to Spectrum's login page — so label it so users can find it.
    for provider in providers:
        if provider['id'] == 'Cox':
            provider['name'] = 'Cox / Cox Spectrum'
    # yt-dlp has no Google/OAuth MSO support at all (confirmed 2026-06-15 —
    # its --ap-list-mso output never includes YouTubeTV, and its adobepass
    # extractor is a credential-POST login with no code path to consume a
    # browser session), so YouTubeTV can never come from the probe above —
    # only the browser-assisted "second screen" pairing
    # (app.worker.run_mvpd_browser_login/run_nbc_browser_login) can complete
    # this MSO's login. Force it into the list regardless, same as the 'Cox'
    # backstop just above.
    if 'YouTubeTV' not in seen:
        providers.append({'id': 'YouTubeTV', 'name': 'YouTube TV'})
    providers = [p for p in providers if p['id'] not in _RETIRED_MSO_IDS]
    return sorted(providers, key=_friendly_sort_key)


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


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
    {"id": "TWC", "name": "Time Warner Cable | Spectrum"},
    {"id": "Charter_Direct", "name": "Charter Spectrum"},
    {"id": "DTV", "name": "DIRECTV"},
    {"id": "ATT", "name": "AT&T U-verse"},
    {"id": "Verizon", "name": "Verizon FiOS"},
    {"id": "Cablevision", "name": "Optimum/Cablevision"},
    {"id": "Philo", "name": "Philo"},
    {"id": "Fubo", "name": "Fubo"},
    {"id": "slingtv", "name": "Sling TV"},
    {"id": "YouTubeTV", "name": "YouTube TV"},
]

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
        'TWC': 3,
        'Charter_Direct': 4,
        'DTV': 5,
        'ATT': 6,
        'Verizon': 7,
        'Cablevision': 8,
        'Philo': 9,
        'Fubo': 10,
        'slingtv': 11,
        'YouTubeTV': 12,
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
    return sorted(providers, key=_friendly_sort_key)

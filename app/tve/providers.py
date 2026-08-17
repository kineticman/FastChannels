import re
import subprocess
from functools import lru_cache


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
    try:
        proc = subprocess.run(
            ['yt-dlp', '--ap-list-mso'],
            check=True,
            capture_output=True,
            text=True,
            timeout=8,
        )
    except Exception:
        return list(_FALLBACK_ADOBE_MSO_PROVIDERS)

    providers: list[dict] = []
    seen: set[str] = set()
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

    if not providers:
        return list(_FALLBACK_ADOBE_MSO_PROVIDERS)
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

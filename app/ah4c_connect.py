"""Set up an existing ah4c for FastChannels through ah4c's own settings API.

FastChannels never installs ah4c. It reads GET /api/config, shows what it would
change, and on confirmation writes the FastChannels script set, its own address,
and the tuner list with PUT /api/config, then restarts ah4c if a changed setting
needs it. Only /api/* is called: on old bnhf images GET /config exits the process.

ah4c rules this relies on (checked against v2026.09.30.1515):
  - PUT is refused (409) until /opt/config, /opt/captions and /opt/preroll are all
    mounted; ah4c reports what's missing as missingPersistentMounts.
  - A setting with a non-empty value in the container environment is locked
    ("owned by the environment"); empty ones stay editable.
  - Vars left out of a PUT are kept. A tuners list replaces the old one, so each
    tuner's cmd/teecmd/powerCycle is sent back. Extras left out are deleted, so the
    unlocked ones are always sent back.
  - POST /api/config/restart refuses (409) while a tune is active, and in Docker
    it just exits; the container's restart policy brings it back.
"""
from __future__ import annotations

import ipaddress
import time
from urllib.parse import urlsplit

import requests

FASTCHANNELS_STREAMER_APP = 'scripts/firetv/fastchannels'

_TIMEOUT = 8
_RESTART_WAIT_SECONDS = 75

# The ah4c settings Connect manages, in display order.
_MANAGED_KEYS = ('STREAMER_APP', 'FASTCHANNELS_URL', 'IPADDRESS', 'CHANNELSIP', 'UPDATE_SCRIPTS')


class Ah4cConnectError(Exception):
    def __init__(self, message: str, status: int = 502, **detail):
        super().__init__(message)
        self.message = message
        self.status = status
        self.detail = detail


def host_port(url: str | None, default_port: int | None = None) -> str:
    """'http://host:port/x' -> 'host:port', the bare form ah4c's address settings use."""
    raw = (url or '').strip()
    if not raw:
        return ''
    if '://' not in raw:
        raw = 'http://' + raw
    parts = urlsplit(raw)
    host = parts.hostname or ''
    if not host:
        return ''
    if ':' in host:
        host = f'[{host}]'
    port = parts.port or default_port
    return f'{host}:{port}' if port else host


_UNREACHABLE_HOSTS = {'localhost', 'host.docker.internal', 'gateway.docker.internal'}


def lan_reachable(address: str) -> bool:
    """Whether a 'host:port' could plausibly be reached from another computer or
    container. FastChannels may reach ah4c or Channels DVR by a name only it can
    resolve: a Docker service name ('ah4c'), localhost, or host.docker.internal."""
    raw = (address or '').strip()
    if raw.startswith('['):
        host = raw[1:].split(']', 1)[0]
    else:
        host = raw.rsplit(':', 1)[0] if raw.count(':') == 1 else raw
    host = host.lower().rstrip('.')
    if not host or host in _UNREACHABLE_HOSTS or host.endswith(('.internal', '.localhost')):
        return False
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return '.' in host  # a single-label name is most likely a Docker service name
    return not (ip.is_loopback or ip.is_unspecified or ip.is_link_local)


def _request(method: str, base: str, path: str, **kwargs) -> requests.Response:
    try:
        return requests.request(method, f'{base}{path}', timeout=_TIMEOUT, **kwargs)
    except requests.RequestException as e:
        raise Ah4cConnectError(f"Couldn't reach ah4c at {base}: {e}") from e


def _json(resp: requests.Response) -> dict:
    try:
        data = resp.json()
    except ValueError:
        data = None
    if not isinstance(data, dict):
        raise Ah4cConnectError(f'ah4c returned an unexpected reply (HTTP {resp.status_code}).')
    return data


def _get_config(base: str) -> dict:
    resp = _request('GET', base, '/api/config')
    if resp.status_code == 404:
        raise Ah4cConnectError(
            'This ah4c has no settings API, so FastChannels can’t set it up. Update ah4c '
            '(bnhf/ah4c from late September 2026 or newer), or set it up by hand.', status=409)
    if resp.status_code != 200:
        raise Ah4cConnectError(f'ah4c refused to share its settings (HTTP {resp.status_code}).')
    data = _json(resp)
    if not isinstance(data.get('catalog'), list) or not isinstance(data.get('tuners'), dict):
        raise Ah4cConnectError('ah4c’s settings reply is missing expected fields; it may be too old or too new for this version of FastChannels.')
    return data


def _version(base: str) -> str:
    try:
        resp = requests.get(f'{base}/api/version', timeout=_TIMEOUT)
        return str(_json(resp).get('version') or '')
    except (requests.RequestException, Ah4cConnectError):
        return ''


def _active_tunes(base: str) -> int:
    try:
        resp = requests.get(f'{base}/api/status', timeout=_TIMEOUT)
        readers = _json(resp).get('Readers') or []
        return len(readers) if isinstance(readers, list) else 0
    except (requests.RequestException, Ah4cConnectError):
        return 0


def _catalog(cfg: dict) -> dict[str, dict]:
    return {item.get('key'): item for item in cfg['catalog'] if isinstance(item, dict) and item.get('key')}


# The address settings the modal lets the user edit. Both are used across the
# network, so neither may be an address that only works from FastChannels:
#   IPADDRESS  - ah4c's own address, written into its M3U for Channels DVR (required)
#   CHANNELSIP - Channels DVR's address, which ah4c calls (optional, as in ah4c)
ADDRESS_KEYS = ('IPADDRESS', 'CHANNELSIP')
_REQUIRED_ADDRESSES = {'IPADDRESS'}
_ADDRESS_ERRORS = {
    'IPADDRESS': 'Channels DVR can’t reach ah4c at “{value}”. Enter the address it uses on your '
                 'network, such as 192.168.1.30:7654.',
    'CHANNELSIP': 'ah4c can’t reach Channels DVR at “{value}”. Enter Channels DVR’s address on your '
                  'network, such as 192.168.1.20:8089, or leave it blank.',
}


def _address_guess(key: str, base: str, channels_dvr_url: str | None) -> str:
    """The address FastChannels itself uses for that service."""
    return host_port(base) if key == 'IPADDRESS' else host_port(channels_dvr_url, 8089)


def _address(key: str, base: str, channels_dvr_url: str | None, current: str, override: str | None) -> str:
    """The user's entry wins, then what ah4c has if it works from elsewhere, then
    the address FastChannels uses if that does. Blank means none (for IPADDRESS,
    that the user has to enter one). An unusable address ah4c already has is
    replaced by a usable guess when there is one; otherwise an optional one is
    left as it is rather than blocking the save, and flagged in the preview."""
    optional = key not in _REQUIRED_ADDRESSES
    if override is not None:
        if not override.strip() and optional:
            return ''
        if optional and override.strip() == current:
            return current  # untouched; not Connect's to refuse
        value = host_port(override, urlsplit(base).port if key == 'IPADDRESS' else 8089)
        if not value or not lan_reachable(value):
            raise Ah4cConnectError(_ADDRESS_ERRORS[key].format(value=override.strip() or '(blank)'), status=400)
        return value
    if current and lan_reachable(current):
        return current
    guess = _address_guess(key, base, channels_dvr_url)
    if lan_reachable(guess):
        return guess
    return current if optional else ''


def _desired(base: str, cfg: dict, fastchannels_url: str, channels_dvr_url: str | None,
             addresses: dict[str, str] | None = None) -> dict[str, str]:
    """What Connect wants each managed setting to be. The ah4c and Channels DVR
    addresses are only filled when blank, so an address the user chose stays."""
    catalog = _catalog(cfg)
    current = {key: str((catalog.get(key) or {}).get('value') or '') for key in _MANAGED_KEYS}
    addresses = addresses or {}
    desired = {
        'STREAMER_APP': FASTCHANNELS_STREAMER_APP,
        'FASTCHANNELS_URL': fastchannels_url.rstrip('/'),
        'UPDATE_SCRIPTS': 'true',
    }
    for key in ADDRESS_KEYS:
        # A locked address is the compose file's; don't propose or check an entry for it.
        desired[key] = (current[key] if (catalog.get(key) or {}).get('locked')
                        else _address(key, base, channels_dvr_url, current[key], addresses.get(key)))
    return {key: value for key, value in desired.items() if key in catalog}


def _plan_rows(cfg: dict, desired: dict[str, str]) -> list[dict]:
    catalog = _catalog(cfg)
    rows = []
    for key in _MANAGED_KEYS:
        item = catalog.get(key)
        if not item:
            continue
        current = str(item.get('value') or '')
        want = desired.get(key, current)
        locked = bool(item.get('locked'))
        if key in _REQUIRED_ADDRESSES and not want and not locked:
            action = 'needed'  # no usable guess; the user enters it
        elif current == want:
            action = 'ok'
        elif locked:
            action = 'locked'
        else:
            action = 'set'
        rows.append({
            'key': key,
            'label': item.get('label') or key,
            'current': current,
            'proposed': want,
            'locked': locked,
            'action': action,
            'applies': item.get('applies') or '',
        })
    return rows


def _tuner_rows(cfg: dict) -> list[dict]:
    rows = []
    for item in cfg['tuners'].get('list') or []:
        if not isinstance(item, dict):
            continue
        locks = item.get('locked') if isinstance(item.get('locked'), dict) else {}
        rows.append({
            'tunerIP': str(item.get('tunerIP') or ''),
            'encoderURL': str(item.get('encoderURL') or ''),
            'cmd': str(item.get('cmd') or ''),
            'teecmd': str(item.get('teecmd') or ''),
            'powerCycle': str(item.get('powerCycle') or ''),
            'locked': {k: bool(v) for k, v in locks.items()},
        })
    return rows


def _address_notes(base: str, channels_dvr_url: str | None, rows: list[dict]) -> dict[str, dict]:
    """Per editable address: one ah4c already has that only works from
    FastChannels' side ('current'), or the one FastChannels uses when it
    couldn't be offered as a guess ('guess')."""
    notes = {}
    for r in rows:
        if r['key'] not in ADDRESS_KEYS or r['locked']:
            continue
        guess = _address_guess(r['key'], base, channels_dvr_url)
        notes[r['key']] = {
            'current': r['current'] if r['current'] and not lan_reachable(r['current']) else '',
            'guess': guess if guess and not lan_reachable(guess) else '',
        }
    return notes


def inspect(base: str, fastchannels_url: str, channels_dvr_url: str | None) -> dict:
    """Read-only: what ah4c has now and what Connect would change."""
    cfg = _get_config(base)
    desired = _desired(base, cfg, fastchannels_url, channels_dvr_url)
    rows = _plan_rows(cfg, desired)
    streamer = next((r for r in rows if r['key'] == 'STREAMER_APP'), None)
    tuners = cfg['tuners']
    return {
        'version': _version(base),
        'missing_mounts': [m.get('path') for m in cfg.get('missingPersistentMounts') or [] if isinstance(m, dict)],
        'docker_managed': bool(cfg.get('dockerManaged')),
        'settings': rows,
        'locked_settings': [r for r in rows if r['action'] == 'locked'],
        # Switching away from another app's scripts breaks that app's channels in ah4c.
        # (A locked one can't be switched from here; it's listed under locked_settings.)
        'replaces_streamer_app': (streamer['current'] if streamer and streamer['action'] == 'set'
                                  and streamer['current'] else ''),
        'tuners': _tuner_rows(cfg),
        'address_notes': _address_notes(base, channels_dvr_url, rows),
        'tuners_locked': bool(tuners.get('locked') or tuners.get('countLocked')),
        'active_tunes': _active_tunes(base),
        # Saved in ah4c but not running until it restarts (an earlier Connect whose
        # restart was refused, or a change in ah4c's own Settings page). What ah4c
        # shows above may not be what it's running; an empty saved tuner list, for
        # one, reads back as the running list until the restart.
        'restart_pending': bool(cfg.get('restartNeeded')),
        'restart_pending_for': [str(x) for x in cfg.get('restartRequired') or [] if x],
    }


def _normalize_tuner_address(value: str) -> str:
    value = (value or '').strip()
    if not value:
        return ''
    if '://' in value:
        value = host_port(value)
    host = value.rsplit(']', 1)[-1]
    if ':' not in host:
        value = f'{value}:5555'
    return value


_BLANK_TUNER = {'tunerIP': '', 'encoderURL': '', 'cmd': '', 'teecmd': '', 'powerCycle': '', 'locked': {}}


def _merge_tuners(cfg: dict, requested: list[dict], clear_tuners: bool = False) -> list[dict]:
    """The list to PUT: the user's addresses and encoder URLs, with every other
    field ah4c holds for that tuner sent back unchanged, and locked fields kept
    exactly as the environment has them (ah4c rejects anything else).

    Each requested row carries 'index', the position it had in ah4c's list when
    the form was built (absent for a row the user added). Rows are matched to
    ah4c's tuners by that, not by where they sit now, so removing a row doesn't
    hand the next tuner's cmd/teecmd/powerCycle to its neighbour."""
    if cfg['tuners'].get('locked') or cfg['tuners'].get('countLocked'):
        raise Ah4cConnectError(
            'ah4c’s tuner list is set in its compose file or environment (NUMBER_TUNERS), '
            'so it has to be changed there.', status=409)
    existing = _tuner_rows(cfg)
    if not requested and existing and not clear_tuners:
        raise Ah4cConnectError('That would remove every tuner from ah4c. Confirm removing them all first.',
                               status=400, clears_tuners=True)
    used: set[int] = set()
    out = []
    for position, req in enumerate(requested):
        index = req.get('index')
        old = _BLANK_TUNER
        if isinstance(index, int) and not isinstance(index, bool) and 0 <= index < len(existing):
            if index in used:
                raise Ah4cConnectError(f'Tuner {index + 1} is listed twice.', status=400)
            used.add(index)
            old = existing[index]
        locks = old['locked']
        if any(locks.values()) and index != position:
            # ah4c ties environment-set tuner fields (TUNERn_IP, ENCODERn_URL) to a position.
            raise Ah4cConnectError(
                f'Tuner {index + 1} is set in ah4c’s compose file, so the tuners before it '
                'can’t be removed here. Change them in the compose file.', status=409)
        address = _normalize_tuner_address(str(req.get('tunerIP') or ''))
        encoder = str(req.get('encoderURL') or '').strip()
        if not address:
            raise Ah4cConnectError(f'Tuner {position + 1} needs a device address.', status=400)
        if not encoder and not old['cmd']:
            raise Ah4cConnectError(f'Tuner {position + 1} needs an encoder URL.', status=400)
        if encoder and urlsplit(encoder).scheme not in ('http', 'https'):
            raise Ah4cConnectError(f'Tuner {position + 1}’s encoder URL must start with http:// or https://.', status=400)
        tuner = {
            'tunerIP': old['tunerIP'] if locks.get('tunerIP') else address,
            'encoderURL': old['encoderURL'] if locks.get('encoderURL') else encoder,
            'cmd': old['cmd'],
            'teecmd': old['teecmd'],
            'powerCycle': old['powerCycle'],
        }
        out.append(tuner)
    for index, old in enumerate(existing):
        if index not in used and any(old['locked'].values()):
            raise Ah4cConnectError(
                f'Tuner {index + 1} is set in ah4c’s compose file, so it can’t be removed here.', status=409)
    return out


def _wait_for_restart(base: str) -> bool:
    """True once a fresh ah4c answers with nothing left to restart for."""
    time.sleep(2)  # ah4c exits ~0.5s after accepting the restart
    deadline = time.monotonic() + _RESTART_WAIT_SECONDS
    while time.monotonic() < deadline:
        try:
            resp = requests.get(f'{base}/api/config', timeout=3)
            data = resp.json() if resp.status_code == 200 else None
            if isinstance(data, dict) and data.get('restartNeeded') is False:
                return True
        except (requests.RequestException, ValueError):
            pass
        time.sleep(1.5)
    return False


def apply(base: str, fastchannels_url: str, channels_dvr_url: str | None,
          tuners: list[dict] | None, allow_streamer_switch: bool, clear_tuners: bool = False,
          addresses: dict[str, str] | None = None) -> dict:
    """Write the settings, restart ah4c when needed, and report what happened.
    Re-reads ah4c first, so a change made in ah4c since inspect() is respected."""
    cfg = _get_config(base)
    missing = [m.get('path') for m in cfg.get('missingPersistentMounts') or [] if isinstance(m, dict)]
    if missing:
        raise Ah4cConnectError(
            'ah4c can’t save settings until these folders are mounted into its container: '
            + ', '.join(missing) + '.', status=409, missing_mounts=missing)

    desired = _desired(base, cfg, fastchannels_url, channels_dvr_url, addresses)
    rows = _plan_rows(cfg, desired)
    if any(r['action'] == 'needed' for r in rows):
        raise Ah4cConnectError(
            'Enter the address Channels DVR uses to reach ah4c, such as 192.168.1.30:7654. '
            f'FastChannels reaches it at {host_port(base)}, which only works from here.', status=400)
    streamer = next((r for r in rows if r['key'] == 'STREAMER_APP'), None)
    if (streamer and streamer['action'] == 'set' and streamer['current']
            and not allow_streamer_switch):
        raise Ah4cConnectError(
            f'ah4c is set up for {streamer["current"]}. Confirm switching it to FastChannels first.',
            status=409, replaces_streamer_app=streamer['current'])

    body: dict = {'vars': {r['key']: r['proposed'] for r in rows if r['action'] == 'set'}}
    if tuners is not None:
        merged = _merge_tuners(cfg, tuners, clear_tuners)
        if merged != [{k: v for k, v in t.items() if k != 'locked'} for t in _tuner_rows(cfg)]:
            body['tuners'] = merged
    changed: list[str] = []
    restart_needed = bool(cfg.get('restartNeeded'))
    if body['vars'] or 'tuners' in body:
        body['extra'] = {k: v for k, v in (cfg.get('extra') or {}).items()
                         if not (cfg.get('extraLocked') or {}).get(k)}
        resp = _request('PUT', base, '/api/config', json=body)
        data = _json(resp)
        if resp.status_code == 409 and data.get('locked'):
            raise Ah4cConnectError(
                'ah4c says these are set in its compose file or environment: '
                + ', '.join(data['locked']) + '.', status=409, locked=data['locked'])
        if resp.status_code != 200:
            raise Ah4cConnectError(f'ah4c didn’t save the settings: {data.get("error") or resp.status_code}',
                                   status=409 if resp.status_code == 409 else 502)
        changed = [str(c) for c in data.get('changed') or []]
        restart_needed = bool(data.get('restartNeeded'))

    restart = 'not_needed'
    if restart_needed:
        resp = _request('POST', base, '/api/config/restart')
        if resp.status_code == 409:
            restart = 'busy'
        elif resp.status_code != 200:
            restart = 'failed'
        else:
            restart = 'done' if _wait_for_restart(base) else 'no_return'

    # The saved STREAMER_APP (applied now, or at ah4c's next restart). Known from
    # the plan: an unlocked one was just saved (or already matched), a locked one
    # stays as the environment has it. Confirmed by a re-read when ah4c answers,
    # but a restart that hasn't come back yet doesn't turn it into ''.
    streamer_app = ''
    if streamer:
        streamer_app = streamer['current'] if streamer['action'] == 'locked' else streamer['proposed']
    try:
        after = _catalog(_get_config(base))
        streamer_app = str((after.get('STREAMER_APP') or {}).get('value') or '')
    except Ah4cConnectError:
        pass
    uses_scripts = streamer_app == FASTCHANNELS_STREAMER_APP
    return {
        'changed': changed,
        'restart': restart,
        'locked_settings': [r for r in rows if r['action'] == 'locked'],
        'streamer_app': streamer_app,
        'uses_fastchannels_scripts': uses_scripts,
        # Tunes only work once ah4c is actually running the FastChannels scripts:
        # saved, and either applied by a restart that came back or needing none.
        'capture_ready': uses_scripts and restart in ('done', 'not_needed'),
    }


def compose_snippet(timezone_name: str | None) -> str:
    """A minimal ah4c compose file that Connect can fully set up: every folder
    ah4c requires before it saves settings, a restart policy so its settings
    restart brings it back, and nothing in environment that would lock a setting."""
    tz = (timezone_name or 'America/New_York').strip() or 'America/New_York'
    return f"""services:
  ah4c:
    image: bnhf/ah4c:latest
    container_name: ah4c
    ports:
      - 7654:7654
    environment:
      - TZ={tz}
    volumes:
      - ./ah4c/config:/opt/config
      - ./ah4c/scripts:/opt/scripts
      - ./ah4c/m3u:/opt/m3u
      - ./ah4c/adb:/root/.android
      - ./ah4c/captions:/opt/captions
      - ./ah4c/preroll:/opt/preroll
    restart: unless-stopped
"""

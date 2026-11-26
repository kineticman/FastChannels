"""Built-in capture add-on (beta): Google Chrome inside the FastChannels container
plays our own /watch/<id>?capture=1 page, and FFmpeg captures the virtual display
and audio sink to HLS.

Opt-in. Nothing here runs or downloads until someone clicks Install on the Bridge
page. The add-on lives entirely under ADDON_DIR on the /data volume:

    config.json   what the user chose (enabled, tuner count, quality)
    status.json   install state, written only by the daemon's installer
    debs/         cached .deb files (Chrome from Google, PulseAudio/FFmpeg from
                  Debian), so a recreated container reinstalls without re-downloading
    profiles/     one persistent Chrome profile per tuner (keeps Widevine registered)

The daemon (python -m app.capture.daemon, started by entrypoint.sh) owns the
install, the tuner pool and the capture sessions. It listens on 127.0.0.1 only;
FastChannels talks to it through daemon_request() and serves the HLS segments
itself, straight from SESSION_ROOT, so long-running streams never pin a gunicorn
worker (workers recycle every 250 requests).

Loading /watch from http://127.0.0.1 is what makes this work without HTTPS:
loopback is a secure context, so Widevine/EME is allowed. Google ships Chrome for
Linux on amd64 only, so the add-on is unavailable on other architectures.
"""
from __future__ import annotations

import json
import os
import platform
import tempfile
import time

ADDON_DIR = os.environ.get('FC_CAPTURE_DIR', '/data/addons/capture')
CONFIG_PATH = os.path.join(ADDON_DIR, 'config.json')
STATUS_PATH = os.path.join(ADDON_DIR, 'status.json')
RESTRICTED_PATH = os.path.join(ADDON_DIR, 'restricted.json')
DEBS_DIR = os.path.join(ADDON_DIR, 'debs')
PROFILES_DIR = os.path.join(ADDON_DIR, 'profiles')
HOME_DIR = os.path.join(ADDON_DIR, 'home')

# Live HLS output; tmpfs-backed /tmp is fine, it's a few seconds of video per tuner.
SESSION_ROOT = os.environ.get('FC_CAPTURE_SESSION_ROOT', '/tmp/fc-capture')

DAEMON_HOST = '127.0.0.1'
DAEMON_PORT = int(os.environ.get('FC_CAPTURE_PORT', '5591'))

# FastChannels as the tuners' Chrome sees it: loopback, so EME needs no HTTPS.
WATCH_BASE = os.environ.get('FC_CAPTURE_WATCH_BASE', 'http://127.0.0.1:5523')

CHROME_BIN = '/usr/bin/google-chrome-stable'

# Sources whose license servers refuse Linux Chrome. Empty since 2026-11-21: all three that
# the 2026-11-19/20 spike found were our request, not Linux Chrome.
#   dazn_tve — HTTP 403 code 10802 "Device not allowed to receive a license". The license
#              server checks that the request's user agent platform matches the CDM's own
#              (client_info platform_name); we always presented a Mac. dazn_tve.py now sends
#              the browser's platform, and capture plays at 720p.
# hbomax was excluded here until 2026-11-21. Its license server refuses any request that
# lists key ids above a software (L3) CDM's tier (the manifest's PSSH names six; HBO's own
# player asks for the two SD/audio ones on Linux Chrome), answering HTTP 400
# vmp_platform_not_verified. fc_player.js now trims the key list on desktop Linux, so capture
# works, at SD (576p): that is all a software CDM is allowed. cnn_tve (same Warner license
# stack, same six key ids) had the identical failure and the identical fix, verified 2026-11-21.
# Also verified working that day: philo, roku, versant_tve, vix, tennis_tve.
INCOMPATIBLE_SOURCES = frozenset()

# Single channels whose license keys come back 'output-restricted' (the content owner
# requires a hardware-protected player), within an otherwise working source. Fubo was
# first excluded wholesale on one such channel (ABC News Live); 18 of 18 random Fubo
# channels then played (2026-11-20). The daemon records these as it meets them
# (mark_restricted) and capture playlists skip them; entries expire so a policy change
# is picked up again.
RESTRICTED_TTL_SECONDS = 30 * 24 * 3600

QUALITY_PRESETS = {
    # name: (width, height, fps, video kbps). fps is captured as NTSC (x1000/1001)
    # to match broadcast sources; a 59.94 source grabbed at exactly 60 repeats a frame
    # every ~17 s.
    '720p30': (1280, 720, 30, 4000),
    '720p60': (1280, 720, 60, 6000),
    '1080p30': (1920, 1080, 30, 7000),
    '1080p60': (1920, 1080, 60, 10000),
}

DEFAULT_CONFIG = {
    'enabled': True,
    'max_tuners': 2,
    'quality': '720p60',
    'encoder': 'auto',      # 'auto' = GPU (VA-API/NVENC) when it works, else x264; 'cpu' = x264 only
    # Warm: each tuner's Chrome stays open between tunes (~0.6 GB each, ~2 s to
    # playing). Cold: Chrome closes after COLD_IDLE_SECONDS without a tune and the
    # next tune starts it first.
    'keep_warm': True,
}
COLD_IDLE_SECONDS = int(os.environ.get('FC_CAPTURE_COLD_IDLE_SECONDS', '300'))  # env: testing only
ENCODERS = ('auto', 'cpu')

# VA-API H.264 needs ~1.5x x264-veryfast's bitrate for the same quality (2026-11-19
# A/B on Intel N95 Quick Sync vs a lossless reference: 6M 41.8 dB, 9M ~44 dB, x264 6M 42.8 dB).
GPU_BITRATE_FACTOR = 1.5


def render_node() -> str | None:
    """The first DRM render node passed into the container, if any. Docker only
    exposes one when the compose file maps /dev/dri, so this is the user's opt-in."""
    try:
        nodes = sorted(n for n in os.listdir('/dev/dri') if n.startswith('renderD'))
    except OSError:
        return None
    return f'/dev/dri/{nodes[0]}' if nodes else None

# Docker on Windows (WSL2) has no real /dev/dri GPU: the render node there is
# vgem, and the GPU is reached through /dev/dxg plus Microsoft's D3D12 libraries
# in /usr/lib/wsl/lib, using Mesa's d3d12 VA-API driver. Verified 2026-11-21 on a
# Ryzen 5 PRO 5650U with Docker Desktop: needs GALLIUM_DRIVER=d3d12 to initialise
# and Mesa 26 (trixie-backports) to encode; Mesa 25.0.7 segfaults on the first
# frame. Low-power mode isn't offered.
WSL_LIB_DIR = '/usr/lib/wsl/lib'
WSL_VAAPI_ENV = {'GALLIUM_DRIVER': 'd3d12', 'LIBVA_DRIVER_NAME': 'd3d12', 'LD_LIBRARY_PATH': WSL_LIB_DIR}
WSL_MESA_MIN_MAJOR = 26


def wsl_gpu() -> bool:
    """The container was given the Windows GPU bridge (/dev/dxg)."""
    return os.path.exists('/dev/dxg')


def wsl_libs_present() -> bool:
    return os.path.exists(os.path.join(WSL_LIB_DIR, 'libd3d12.so'))

MAX_TUNERS_LIMIT = 8


_PCI_VENDORS = {'0x8086': 'Intel', '0x1002': 'AMD', '0x10de': 'NVIDIA'}


def gpu_info() -> dict:
    """Which GPU the container can see, before anything is installed or tested:
    {'vendor': 'Intel'|'AMD'|'NVIDIA'|'', 'via': 'dri'|'wsl'|'nvidia'|''}. Only a
    hint for the Bridge panel; the daemon's test encode decides what's actually used."""
    node = render_node()
    if node and wsl_gpu():
        # Windows hides the vendor behind D3D12 (the node's sysfs device is vgem).
        return {'vendor': 'GPU', 'via': 'wsl'}
    if node:
        try:
            with open(f'/sys/class/drm/{os.path.basename(node)}/device/vendor') as fp:
                vendor = _PCI_VENDORS.get(fp.read().strip().lower(), '')
        except OSError:
            vendor = ''
        if vendor != 'NVIDIA':
            return {'vendor': vendor or 'GPU', 'via': 'dri'}
    if os.path.exists('/dev/nvidiactl') or (os.environ.get('NVIDIA_VISIBLE_DEVICES') or 'void') not in ('void', 'none', ''):
        return {'vendor': 'NVIDIA', 'via': 'nvidia'}
    return {'vendor': '', 'via': ''}


def gpu_choices() -> list[dict]:
    """The GPUs the container could encode on, {'vendor', 'node'}: each real
    /dev/dri render node, plus NVIDIA handed over by the toolkit without one
    (node None). Virtual display adapters (VMs) aren't GPUs and are left out.
    Empty on Windows, where the vendor is hidden behind D3D12."""
    if wsl_gpu():
        return []
    try:
        nodes = sorted(n for n in os.listdir('/dev/dri') if n.startswith('renderD'))
    except OSError:
        nodes = []
    out = []
    for name in nodes:
        try:
            with open(f'/sys/class/drm/{name}/device/vendor') as fp:
                vendor = _PCI_VENDORS.get(fp.read().strip().lower())
        except OSError:
            vendor = None
        if vendor:
            out.append({'vendor': vendor, 'node': f'/dev/dri/{name}'})
    toolkit = os.path.exists('/dev/nvidiactl') or (os.environ.get('NVIDIA_VISIBLE_DEVICES') or 'void') not in ('void', 'none', '')
    if toolkit and not any(g['vendor'] == 'NVIDIA' for g in out):
        out.append({'vendor': 'NVIDIA', 'node': None})
    return out


def supported_platform() -> bool:
    return platform.machine().lower() in ('x86_64', 'amd64')


def _read_json(path: str) -> dict:
    try:
        with open(path, encoding='utf-8') as fp:
            data = json.load(fp)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix='.tmp-')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as fp:
            json.dump(data, fp, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def read_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    cfg.update({k: v for k, v in _read_json(CONFIG_PATH).items() if k in DEFAULT_CONFIG})
    return normalize_config(cfg)


def normalize_config(cfg: dict) -> dict:
    out = dict(DEFAULT_CONFIG)
    out['enabled'] = bool(cfg.get('enabled', out['enabled']))
    try:
        tuners = int(cfg.get('max_tuners', out['max_tuners']))
    except (TypeError, ValueError):
        tuners = out['max_tuners']
    out['max_tuners'] = max(1, min(MAX_TUNERS_LIMIT, tuners))
    quality = cfg.get('quality')
    out['quality'] = quality if quality in QUALITY_PRESETS else out['quality']
    encoder = cfg.get('encoder')
    out['encoder'] = encoder if encoder in ENCODERS else out['encoder']
    out['keep_warm'] = bool(cfg.get('keep_warm', out['keep_warm']))
    return out


def write_config(cfg: dict) -> dict:
    cfg = normalize_config(cfg)
    _write_json(CONFIG_PATH, cfg)
    return cfg


def read_status() -> dict:
    status = _read_json(STATUS_PATH)
    status.setdefault('state', 'not_installed')
    return status


def write_status(state: str, message: str = '', **extra) -> dict:
    status = {'state': state, 'message': message, 'updated_at': int(time.time())}
    status.update(extra)
    _write_json(STATUS_PATH, status)
    return status


def restricted_channels() -> dict[int, dict]:
    """{channel_id: {reason, at}} for channels recorded as output-restricted within
    RESTRICTED_TTL_SECONDS."""
    now = time.time()
    out = {}
    for key, entry in _read_json(RESTRICTED_PATH).items():
        try:
            if isinstance(entry, dict) and now - float(entry.get('at', 0)) < RESTRICTED_TTL_SECONDS:
                out[int(key)] = entry
        except (TypeError, ValueError):
            continue
    return out


def restricted_channel_ids() -> frozenset:
    return frozenset(restricted_channels())


def mark_restricted(channel_id: int, reason: str) -> None:
    """Record a channel whose keys the license server restricts to hardware players.
    Written only by the capture daemon (one process), so no lock is needed."""
    entries = {str(k): v for k, v in restricted_channels().items()}
    entries[str(int(channel_id))] = {'reason': reason[:200], 'at': int(time.time())}
    _write_json(RESTRICTED_PATH, entries)


def is_restricted_error(message: str) -> bool:
    """A page error meaning this channel's keys are restricted for software Widevine
    (Shaka 4012 with output-restricted key status), as opposed to a transient failure."""
    return 'output-restricted' in (message or '')


def is_installed() -> bool:
    return read_status().get('state') == 'installed'


def is_active() -> bool:
    """Switched on and installed, or being (re)installed: the capture playlists and
    /play/capture routes serve, and DRM channels count it as a bridge.

    'installing' counts so an Update Chrome or the reinstall after a container
    update doesn't read as the add-on going away: that would re-run DRM bridge
    eligibility and, where Built-in Capture is the only method, disable every DRM
    channel for a minute. Tunes during it get "tuners are starting".
    """
    return (read_status().get('state') in ('installed', 'installing')
            and read_config().get('enabled', False))


def daemon_request(method: str, path: str, *, timeout: float = 5.0, **kwargs):
    """JSON call to the local capture daemon. Returns (status_code, dict); (0, {...})
    when the daemon isn't reachable (old image without the entrypoint hook, or it's
    restarting)."""
    import requests
    url = f'http://{DAEMON_HOST}:{DAEMON_PORT}{path}'
    try:
        r = requests.request(method, url, timeout=timeout, **kwargs)
    except requests.RequestException as e:
        return 0, {'error': f'capture service unreachable ({type(e).__name__})'}
    try:
        body = r.json()
    except ValueError:
        body = {'error': r.text[:300]}
    return r.status_code, body if isinstance(body, dict) else {'data': body}

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

# Sources whose license servers refuse Linux Chrome, found in the 2026-11-19 spike:
#   cnn_tve — HTTP 400 vmp_platform_not_verified (Warner Bros. Discovery enforces VMP)
#   fubo    — license granted, but every key is output-restricted for software Widevine
# hbomax shares CNN's WBD license stack, so it's excluded until someone shows otherwise.
INCOMPATIBLE_SOURCES = frozenset({'cnn_tve', 'fubo', 'hbomax'})

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
    'max_tuners': 1,
    'quality': '720p60',
    'encoder': 'auto',      # 'auto' = GPU (VA-API) when /dev/dri works, else x264; 'cpu' = x264 only
}
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

MAX_TUNERS_LIMIT = 4


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


def is_installed() -> bool:
    return read_status().get('state') == 'installed'


def is_active() -> bool:
    """Installed and switched on: the capture playlist and /play/capture routes serve."""
    return is_installed() and read_config().get('enabled', False)


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

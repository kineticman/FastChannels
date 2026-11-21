"""Capture add-on daemon: python -m app.capture.daemon (started by entrypoint.sh).

Always running, mostly idle. When the add-on is installed and enabled it keeps a
pool of warm tuners; each is one Xvfb display, one PulseAudio null sink and one
Google Chrome (persistent profile, so Widevine is registered before the first
tune). A capture session drives a tuner's Chrome to /watch/<id>?capture=1 over
the DevTools protocol, waits for the page to report that video is playing, then
runs FFmpeg (x11grab + pulse -> H.264/AAC HLS) into SESSION_ROOT/<session>/.

FastChannels serves that HLS itself (app/routes/capture.py), so this process
only answers short control calls on 127.0.0.1:
    GET  /health             install state, config, tuners, sessions
    POST /sessions           {"channel_id": N} -> start or keep alive a session
    POST /install            install / reinstall the packages
    POST /update-chrome      re-download Chrome and reinstall
    POST /uninstall          stop everything and remove the add-on

A session ends when nothing has polled it for SESSION_IDLE_SECONDS (Channels DVR
re-fetches the playlist every couple of seconds while a client is watching).
Tuners, Chrome and FFmpeg all run as the unprivileged 'fccapture' user at lower
CPU priority than FastChannels itself.
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

from . import (CHROME_BIN, DAEMON_HOST, DAEMON_PORT, PROFILES_DIR, HOME_DIR,
               INCOMPATIBLE_SOURCES, QUALITY_PRESETS, SESSION_ROOT, WATCH_BASE,
               GPU_BITRATE_FACTOR, is_restricted_error, mark_restricted, read_config, read_status,
               render_node, supported_platform, COLD_IDLE_SECONDS,
               WSL_MESA_MIN_MAJOR, WSL_VAAPI_ENV, wsl_gpu, wsl_libs_present)
from . import installer

logger = logging.getLogger('app.capture')

RUN_DIR = '/tmp/fc-capture-run'
PULSE_SOCKET = os.path.join(RUN_DIR, 'pulse.sock')
DISPLAY_BASE = 70          # tuner n uses X display :70+n (Camoufox sign-ins use xvfb-run's own)
CDP_PORT_BASE = 9470       # tuner n's DevTools port, bound to 127.0.0.1
WIDEVINE_SETTLE_SECONDS = 6
TUNE_TIMEOUT_SECONDS = 40
SESSION_IDLE_SECONDS = 45
TUNER_RETRY_SECONDS = 30
PREEMPT_IDLE_SECONDS = 6   # unpolled this long + another channel wants the tuner = client is gone
STALL_RELOAD_SECONDS = 20
GPU_STALL_SECONDS = 15      # GPU ffmpeg alive but no frame out this long = driver hang
FAILED_SESSION_TTL = 60
NICE_CHROME = 5
NICE_FFMPEG = 10


# ── DevTools protocol (just enough: navigate + evaluate) ──────────────────────

def _cdp_page_ws(port: int) -> str:
    targets = requests.get(f'http://127.0.0.1:{port}/json/list', timeout=3).json()
    pages = [t for t in targets if t.get('type') == 'page' and t.get('webSocketDebuggerUrl')]
    if not pages:
        # Kiosk window lost its tab somehow: open a fresh one.
        t = requests.put(f'http://127.0.0.1:{port}/json/new?about:blank', timeout=3).json()
        return t['webSocketDebuggerUrl']
    return pages[0]['webSocketDebuggerUrl']


def _cdp_call(port: int, method: str, params: dict | None = None, timeout: float = 10.0):
    from websockets.sync.client import connect
    ws_url = _cdp_page_ws(port)
    with connect(ws_url, max_size=None, open_timeout=timeout, close_timeout=2) as ws:
        ws.send(json.dumps({'id': 1, 'method': method, 'params': params or {}}))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = json.loads(ws.recv(timeout=max(0.1, deadline - time.monotonic())))
            if msg.get('id') == 1:
                if 'error' in msg:
                    raise RuntimeError(f"CDP {method}: {msg['error'].get('message')}")
                return msg.get('result') or {}
    raise TimeoutError(f'CDP {method} timed out')


def _navigate(port: int, url: str) -> None:
    _cdp_call(port, 'Page.navigate', {'url': url})


def _page_state(port: int) -> dict | None:
    res = _cdp_call(port, 'Runtime.evaluate', {
        'expression': 'JSON.stringify(window.__fcCapture || null)',
        'returnByValue': True,
    }, timeout=5)
    value = (res.get('result') or {}).get('value')
    try:
        return json.loads(value) if value else None
    except ValueError:
        return None


# ── process helpers ───────────────────────────────────────────────────────────

def _spawn(cmd: list[str], *, uid: int, gid: int, env: dict, nice: int = 0,
           stdout=subprocess.DEVNULL, log_name: str = '', groups=()) -> subprocess.Popen:
    def _preexec():
        os.setsid()
        if nice:
            os.nice(nice)
    stderr = subprocess.DEVNULL
    if log_name:
        stderr = open(os.path.join(RUN_DIR, f'{log_name}.log'), 'ab')
    proc = subprocess.Popen(cmd, env=env, user=uid, group=gid, extra_groups=list(groups),
                            stdout=stdout, stderr=stderr, preexec_fn=_preexec, close_fds=True)
    if log_name:
        stderr.close()
    return proc


def _kill(proc: subprocess.Popen | None, grace: float = 3.0) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except OSError:
        pass
    try:
        proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            pass
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass


def _base_env(uid_home: str) -> dict:
    return {
        'PATH': '/usr/local/bin:/usr/bin:/bin',
        'HOME': uid_home,
        'XDG_RUNTIME_DIR': RUN_DIR,
        'XDG_CONFIG_HOME': os.path.join(uid_home, '.config'),
        'PULSE_SERVER': f'unix:{PULSE_SOCKET}',
        'LANG': 'C.UTF-8',
    }


# ── encoder ───────────────────────────────────────────────────────────────────

# What FFmpeg encodes with; decided when the pool starts (detect_encoder) and read
# by every session. 'vaapi' = GPU H.264 through the render node, else 'x264'.
ENCODER: dict = {'kind': 'x264', 'reason': 'not checked yet'}


def nvidia_present() -> bool:
    """An NVIDIA GPU handed to the container by the NVIDIA Container Toolkit
    (runtime: nvidia / --gpus), which creates /dev/nvidiactl and mounts the host
    driver's libraries (libnvidia-encode needs NVIDIA_DRIVER_CAPABILITIES to
    include 'video')."""
    return os.path.exists('/dev/nvidiactl') or (os.environ.get('NVIDIA_VISIBLE_DEVICES') or 'void') not in ('void', 'none', '')


def _nvidia_gids() -> list[int]:
    # /dev/nvidia* are normally world-writable; pass their group in case a host isn't.
    gids = set()
    for dev in ('/dev/nvidiactl', '/dev/nvidia0', '/dev/nvidia-uvm'):
        try:
            gids.add(os.stat(dev).st_gid)
        except OSError:
            pass
    return sorted(gids)


def _try_encoder(uid: int, gid: int, groups: list[int], video_args: list[str],
                 extra_env: dict | None = None) -> tuple[bool, str]:
    """Test-encode a few frames as the capture user. (ok, ffmpeg's stderr)."""
    cmd = ['ffmpeg', '-hide_banner', '-loglevel', 'verbose', '-nostdin',
           '-f', 'lavfi', '-i', 'testsrc2=size=1280x720:rate=30', '-frames:v', '10',
           *video_args, '-f', 'null', '-']
    try:
        proc = subprocess.run(cmd, env={**_base_env(HOME_DIR), **(extra_env or {})},
                              user=uid, group=gid, extra_groups=groups,
                              capture_output=True, text=True, timeout=30)
    except Exception as e:
        return False, str(e)
    return proc.returncode == 0, proc.stderr


def _last_line(stderr: str) -> str:
    return ((stderr or '').strip().splitlines() or ['unknown error'])[-1][:200]


def _nvenc_reason(stderr: str) -> str:
    """Why the NVENC test failed, in words a user can act on. FFmpeg's last line is
    a generic "Conversion failed!"; the cause is earlier."""
    lines = (stderr or '').splitlines()
    if any('Cannot load lib' in l for l in lines):
        return ("NVIDIA driver libraries aren't available in the container (is the NVIDIA "
                "Container Toolkit set up, with NVIDIA_DRIVER_CAPABILITIES including compute,video?)")
    for key in ('No capable devices', 'OpenEncodeSessionEx', 'not supported', 'out of memory', 'Error'):
        hit = next((l.strip() for l in lines if key in l), None)
        if hit:
            return hit[:200]
    return _last_line(stderr)


def detect_encoder(uid: int, gid: int, setting: str) -> dict:
    """GPU encoding when a GPU was passed into the container and it really works for
    the capture user, else x264 with the reason shown on the Bridge page:

      1. Intel/AMD through VA-API (/dev/dri): the low-power fixed-function encoder
         (Intel VDEnc) first, then the standard entrypoint (AMD, older Intel).
         On Docker for Windows (/dev/dxg) it's Mesa's d3d12 VA-API driver instead
         (_detect_wsl_vaapi).
      2. NVIDIA through NVENC (NVIDIA Container Toolkit). EXPERIMENTAL: written
         without NVIDIA hardware to test on, so it's only trusted after the same
         test encode, and an early failure in a real session falls back to x264.
    """
    if setting == 'cpu':
        return {'kind': 'x264', 'reason': 'CPU only (set on the Bridge page)'}
    reasons = []
    device = render_node()
    if device and not installer.gpu_drivers_present():
        reasons.append('GPU drivers not installed yet (Reinstall the add-on)')
    elif device and wsl_gpu():
        enc, why = _detect_wsl_vaapi(uid, gid, device)
        if enc:
            return enc
        reasons.append(why)
    elif device:
        dgid = os.stat(device).st_gid
        for low_power in (True, False):
            ok, err = _try_encoder(uid, gid, [dgid], [
                '-vaapi_device', device, '-vf', 'format=nv12,hwupload', '-c:v', 'h264_vaapi',
                *(['-low_power', '1'] if low_power else []), '-b:v', '4000k'])
            if ok:
                driver = next((l.split('driver:', 1)[1].strip() for l in err.splitlines()
                               if 'VAAPI driver:' in l or 'VA-API driver:' in l), '')
                enc = {'kind': 'vaapi', 'device': device, 'gid': dgid, 'low_power': low_power,
                       'driver': driver or 'VA-API', 'reason': ''}
                logger.info('[capture] GPU encoding on %s (%s%s)', device, enc['driver'],
                            ', low-power' if low_power else '')
                return enc
        reasons.append(f'GPU encode test failed: {_last_line(err)}')
        logger.warning('[capture] VA-API encode test failed on %s: %s', device, _last_line(err))
    if nvidia_present():
        groups = _nvidia_gids()
        ok, err = _try_encoder(uid, gid, groups, ['-pix_fmt', 'bgr0', '-c:v', 'h264_nvenc', '-b:v', '4000k'])
        if ok:
            logger.info('[capture] GPU encoding on NVIDIA (NVENC, experimental)')
            return {'kind': 'nvenc', 'driver': 'NVIDIA NVENC', 'groups': groups, 'reason': ''}
        reasons.append(f'NVIDIA encode test failed: {_nvenc_reason(err)}')
        logger.warning('[capture] NVENC encode test failed, using CPU: %s', _nvenc_reason(err))
    if not reasons:
        reasons.append('no GPU passed to the container')
    return {'kind': 'x264', 'reason': '; '.join(reasons)}


# x11grab delivers bgr0; this is how sessions upload it and convert on the GPU.
_VAAPI_GPU_CONVERT = 'hwupload,scale_vaapi=format=nv12:out_color_matrix=bt709:out_range=tv'


def _detect_wsl_vaapi(uid: int, gid: int, device: str) -> tuple[dict | None, str]:
    """(encoder, '') when the Windows GPU encodes through Mesa's d3d12 driver, else
    (None, reason). The test mirrors the session filter chain (bgr0 upload + GPU
    colour conversion); if d3d12 can't do that, it retries with the conversion on
    the CPU (format=nv12 before the upload), which costs more CPU but still beats x264."""
    if not wsl_libs_present():
        return None, ('Windows GPU found, but /usr/lib/wsl isn\'t mounted into the container '
                      '(add "- /usr/lib/wsl:/usr/lib/wsl:ro" under volumes)')
    if installer.mesa_va_major() < WSL_MESA_MIN_MAJOR:
        return None, (f'Windows GPU needs Mesa {WSL_MESA_MIN_MAJOR} or newer, found '
                      f'{installer.mesa_va_major() or "none"} (Reinstall the add-on)')
    gids = sorted({os.stat(p).st_gid for p in (device, '/dev/dxg')})
    err = ''
    for upload in ('gpu', 'cpu'):
        vf = f'format=bgr0,{_VAAPI_GPU_CONVERT}' if upload == 'gpu' else 'format=nv12,hwupload'
        ok, err = _try_encoder(uid, gid, gids, ['-vaapi_device', device, '-vf', vf,
                                                '-c:v', 'h264_vaapi', '-b:v', '4000k'],
                               extra_env=WSL_VAAPI_ENV)
        if ok:
            driver = next((l.split('driver:', 1)[1].strip() for l in err.splitlines()
                           if 'VAAPI driver:' in l or 'VA-API driver:' in l), '')
            logger.info('[capture] GPU encoding on Windows GPU via D3D12 (%s, %s colour conversion)',
                        driver or 'Mesa d3d12', upload.upper())
            return {'kind': 'vaapi', 'device': device, 'gid': gids[0], 'gids': gids,
                    'low_power': False, 'upload': upload, 'env': dict(WSL_VAAPI_ENV),
                    'driver': driver or 'Mesa D3D12', 'reason': ''}, ''
    logger.warning('[capture] D3D12 VA-API encode test failed on %s: %s', device, _last_line(err))
    return None, f'Windows GPU encode test failed: {_last_line(err)}'


def _encoder_groups(enc: dict) -> list[int]:
    """Supplementary groups the capture user needs for this encoder's device nodes."""
    if enc.get('kind') == 'vaapi':
        return list(enc.get('gids') or [enc['gid']])
    if enc.get('kind') == 'nvenc':
        return list(enc.get('groups') or [])
    return []


def _video_encoder_args(t: 'Tuner', enc: dict) -> tuple[list[str], list[str]]:
    """(global args before the inputs, video output args) for this tuner's preset."""
    gop = t.fps * 2
    gop_args = ['-g', str(gop), '-keyint_min', str(gop)]
    # x11grab tags its frames as RGB, and the GPU encoders carry that into the
    # H.264 VUI (matrix "gbr"). Channels DVR copies that value into a
    # `-colorspace gbr` transcode option, which FFmpeg's parser rejects, so every
    # remote/transcoded play failed. Tag what we actually emit: BT.709 limited.
    color_args = ['-colorspace', 'bt709', '-color_primaries', 'bt709',
                  '-color_trc', 'bt709', '-color_range', 'tv']
    if enc.get('kind') == 'nvenc':
        # x11grab's bgr0 goes straight in: NVENC converts on the GPU. Same bitrate
        # headroom as VA-API until someone measures NVENC against x264 (experimental).
        kbps = int(t.kbps * GPU_BITRATE_FACTOR)
        return ([], ['-c:v', 'h264_nvenc', '-preset', 'p5', '-tune', 'hq', '-profile:v', 'high',
                     '-rc', 'vbr', '-spatial-aq', '1',
                     '-b:v', f'{kbps}k', '-maxrate', f'{kbps}k', '-bufsize', f'{kbps * 2}k',
                     '-forced-idr', '1', *gop_args, *color_args])
    if enc.get('kind') == 'vaapi':
        kbps = int(t.kbps * GPU_BITRATE_FACTOR)
        vf = _VAAPI_GPU_CONVERT
        if enc.get('upload') == 'cpu':
            # D3D12 that can't upload/convert bgr0 itself (see _detect_wsl_vaapi).
            vf = 'scale=out_color_matrix=bt709:out_range=tv,format=nv12,hwupload'
        if t.height >= 1080 and t.fps == 60:
            # radeonsi (Mesa 25.0, Radeon 680M) hangs on the first frame at 1080p59.94
            # whenever the SPS carries a colour description: any primaries/trc/matrix,
            # BT.709 or BT.601. 1080p30, 1080p at exactly 60 and 900p are fine. Tag the
            # range only, and strip x11grab's RGB matrix so "gbr" can't come back;
            # players treat an untagged HD stream as BT.709, which is what we emit.
            vf += ',setparams=colorspace=unknown:color_primaries=unknown:color_trc=unknown:range=tv'
            color_args = ['-color_range', 'tv']
        return (['-vaapi_device', enc['device']],
                # Colour conversion on the GPU too: CPU-side format=nv12 cost ~2x the CPU.
                ['-vf', vf,
                 '-c:v', 'h264_vaapi', *(['-low_power', '1'] if enc.get('low_power') else []),
                 '-b:v', f'{kbps}k', '-maxrate', f'{kbps}k', '-bufsize', f'{kbps * 2}k',
                 *gop_args, *color_args])
    # No -tune zerolatency: it drops B-frames and lookahead (~0.6 dB at the same
    # bitrate in the 2026-11-19 A/B) and buys nothing with 2 s HLS segments.
    return ([], ['-c:v', 'libx264', '-preset', 'veryfast', '-pix_fmt', 'yuv420p',
                 '-b:v', f'{t.kbps}k', '-maxrate', f'{t.kbps}k', '-bufsize', f'{t.kbps * 2}k',
                 *gop_args, '-sc_threshold', '0', *color_args])


# ── tuners ────────────────────────────────────────────────────────────────────

class Tuner:
    def __init__(self, index: int, quality: str, uid: int, gid: int):
        self.index = index
        self.quality = quality
        self.width, self.height, self.fps, self.kbps = QUALITY_PRESETS[quality]
        self.uid, self.gid = uid, gid
        self.display = DISPLAY_BASE + index
        self.cdp_port = CDP_PORT_BASE + index
        self.sink = f'fct{index}'
        self.profile = os.path.join(PROFILES_DIR, f't{index}')
        self.xvfb: subprocess.Popen | None = None
        self.chrome: subprocess.Popen | None = None
        self.state = 'stopped'      # stopped | starting | idle | busy | error | asleep
        self.error = ''
        self.idle_since = time.monotonic()   # for cold mode (keep_warm off)
        self.session: 'Session | None' = None

    @property
    def env(self) -> dict:
        env = _base_env(HOME_DIR)
        env.update(DISPLAY=f':{self.display}', PULSE_SINK=self.sink)
        return env

    def start(self) -> None:
        self.state, self.error = 'starting', ''
        os.makedirs(self.profile, exist_ok=True)
        os.chown(self.profile, self.uid, self.gid)
        # Drop the HTTP/code caches: the profile persists for Widevine, and a cached
        # copy of fc_player.js from an older FastChannels must never drive a capture.
        for cache in ('Cache', 'Code Cache', 'GPUCache'):
            shutil.rmtree(os.path.join(self.profile, 'Default', cache), ignore_errors=True)
        # Chrome refuses to start if a previous run's lock survived a hard kill.
        for name in ('SingletonLock', 'SingletonSocket', 'SingletonCookie'):
            try:
                os.unlink(os.path.join(self.profile, name))
            except OSError:
                pass
        sock = f'/tmp/.X11-unix/X{self.display}'
        # A hard-killed Xvfb leaves its socket and lock behind; /tmp survives a
        # container restart, and Xvfb refuses a display whose lock file exists.
        for stale in (sock, f'/tmp/.X{self.display}-lock'):
            try:
                os.unlink(stale)
            except OSError:
                pass
        # One pixel larger than the capture: with no window manager, Chrome shrinks a
        # window that exactly fills the screen to (w-1)x(h-1), so every frame would be
        # resampled (soft picture, 1 px black edge). A w x h window on a (w+1)x(h+1)
        # screen gets an exact w x h viewport at 0,0, which is all x11grab reads.
        self.xvfb = _spawn(['Xvfb', f':{self.display}', '-screen', '0',
                            f'{self.width + 1}x{self.height + 1}x24', '-nolisten', 'tcp', '-ac'],
                           uid=self.uid, gid=self.gid, env=self.env, log_name=f'xvfb{self.index}')
        for _ in range(50):
            if os.path.exists(sock):
                break
            time.sleep(0.1)
        else:
            raise RuntimeError(f'Xvfb :{self.display} did not start')
        self.chrome = _spawn([
            CHROME_BIN,
            '--no-sandbox',                     # no user namespaces inside Docker
            '--disable-dev-shm-usage',          # Docker's default /dev/shm is 64 MB
            '--no-first-run', '--no-default-browser-check', '--password-store=basic',
            f'--user-data-dir={self.profile}',
            '--autoplay-policy=no-user-gesture-required',
            '--kiosk', '--window-position=0,0', f'--window-size={self.width},{self.height}',
            '--disable-background-media-suspend', '--disable-renderer-backgrounding',
            '--disable-backgrounding-occluded-windows', '--disable-background-timer-throttling',
            '--disable-features=Translate,MediaRouter,GlobalMediaControls',
            '--hide-scrollbars', '--disable-notifications',
            f'--remote-debugging-port={self.cdp_port}', '--remote-debugging-address=127.0.0.1',
            'about:blank',
        ], uid=self.uid, gid=self.gid, env=self.env, nice=NICE_CHROME, log_name=f'chrome{self.index}')
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.chrome.poll() is not None:
                raise RuntimeError(f'Chrome exited during startup (code {self.chrome.returncode}); '
                                   f'see {RUN_DIR}/chrome{self.index}.log')
            try:
                requests.get(f'http://127.0.0.1:{self.cdp_port}/json/version', timeout=1)
                break
            except requests.RequestException:
                time.sleep(0.3)
        else:
            raise RuntimeError('Chrome DevTools endpoint did not come up')
        # Widevine registers asynchronously after launch; a /watch load before that
        # fails Shaka's support check ("browser-unsupported").
        time.sleep(WIDEVINE_SETTLE_SECONDS)
        self.idle_since = time.monotonic()
        self.state = 'idle'
        logger.info('[capture] tuner %d ready (:%d, %s)', self.index, self.display, self.quality)

    def alive(self) -> bool:
        return (self.xvfb is not None and self.xvfb.poll() is None
                and self.chrome is not None and self.chrome.poll() is None)

    def stop(self) -> None:
        _kill(self.chrome)
        _kill(self.xvfb)
        self.chrome = self.xvfb = None
        self.state = 'stopped'

    def sleep(self) -> None:
        """Cold mode: close Chrome and the screen; the next tune starts them."""
        _kill(self.chrome)
        _kill(self.xvfb)
        self.chrome = self.xvfb = None
        self.state = 'asleep'

    def blank(self) -> None:
        try:
            _navigate(self.cdp_port, 'about:blank')
        except Exception as e:
            logger.warning('[capture] tuner %d: could not blank page (%s)', self.index, e)

    def describe(self) -> dict:
        s = self.session
        return {'index': self.index, 'state': self.state, 'error': self.error,
                'channel_id': s.channel_id if s else None, 'session': s.id if s else None}


# ── sessions ──────────────────────────────────────────────────────────────────

class Session:
    def __init__(self, channel_id: int, tuner: Tuner):
        self.id = secrets.token_hex(8)
        self.channel_id = channel_id
        self.tuner = tuner
        self.dir = os.path.join(SESSION_ROOT, self.id)
        self.state = 'tuning'          # tuning | live | failed | ended
        self.error = ''
        self.created = time.time()
        self.live_at: float | None = None
        self.last_touch = time.monotonic()
        self.ended_at: float | None = None
        self.ffmpeg: subprocess.Popen | None = None
        self.reloads = 0
        self.video_size = ''
        self.encoder: dict = {}
        self.ffmpeg_started = 0.0
        self._stop = threading.Event()
        # Health samples for the Bridge page: ffmpeg -progress blocks (every 2 s)
        # and the page's frame counters (every _watch poll, 5 s).
        self._ff_samples: deque = deque(maxlen=8)
        self._page_samples: deque = deque(maxlen=4)
        self._page_last: dict = {}
        self._last_reload_at = 0.0
        self._encoder_proven = False

    def touch(self) -> None:
        self.last_touch = time.monotonic()

    @property
    def watch_url(self) -> str:
        # h= caps the variant the page picks at what the capture keeps (see watch.html).
        return f'{WATCH_BASE}/watch/{self.channel_id}?capture=1&h={self.tuner.height}&_v=3'

    def describe(self) -> dict:
        return {'session': self.id, 'channel_id': self.channel_id, 'state': self.state,
                'error': self.error, 'tuner': self.tuner.index, 'dir': self.dir,
                'video_size': self.video_size, 'reloads': self.reloads,
                'encoder': self.encoder.get('kind', ''),
                'age_s': int(time.time() - self.created),
                'live_s': int(time.time() - self.live_at) if self.live_at else 0,
                'idle_s': int(time.monotonic() - self.last_touch),
                'stats': self.stats()}

    def stats(self) -> dict:
        """Is the picture smooth? Two independent views, each a rate over the last
        ~15 s: what the page decodes (upstream / Chrome) and how many real frames
        ffmpeg grabs (the capture itself; duplicates mean the grab fell behind)."""
        target = self.tuner.fps * 1000 / 1001
        out = {'target_fps': round(target, 2), 'health': 'starting', 'why': ''}
        page = self._page_last
        out['source'] = page.get('source')
        out['buffer_s'] = page.get('buffer_s')
        ff = list(self._ff_samples)
        if len(ff) >= 2 and ff[-1]['t'] > ff[0]['t']:
            a, b = ff[0], ff[-1]
            dt = b['t'] - a['t']
            out['capture_fps'] = round(((b['frame'] - b['dup']) - (a['frame'] - a['dup'])) / dt, 1)
            out['dup_pct'] = round(100 * (b['dup'] - a['dup']) / max(1, b['frame'] - a['frame']), 1)
            out['out_kbps'] = b.get('kbps') or self._segment_kbps()
            out['speed'] = b.get('speed')
        pg = list(self._page_samples)
        if len(pg) >= 2 and pg[-1]['t'] > pg[0]['t']:
            a, b = pg[0], pg[-1]
            dt = b['t'] - a['t']
            if a['decoded'] is not None and b['decoded'] is not None and b['decoded'] >= a['decoded']:
                out['decoded_fps'] = round((b['decoded'] - a['decoded']) / dt, 1)
                out['dropped_per_min'] = round(60 * max(0, (b['dropped'] or 0) - (a['dropped'] or 0)) / dt, 1)
            prev = pg[-2]['ct']
            out['advancing'] = b['ct'] is not None and prev is not None and b['ct'] > prev
        out['reloads'] = self.reloads
        if self.state != 'live' or 'capture_fps' not in out or 'decoded_fps' not in out:
            return out
        src_fps = (out['source'] or {}).get('fps') or 0
        expect_decode = min(src_fps or target, target)
        if not out['advancing'] or time.monotonic() - self._last_reload_at < 30:
            out['health'], out['why'] = 'buffering', 'video paused or reloading'
        elif out['capture_fps'] < 0.9 * target:
            out['health'], out['why'] = 'choppy', 'capture is missing frames'
        elif out['decoded_fps'] < 0.9 * expect_decode or out['dropped_per_min'] > 60:
            out['health'], out['why'] = 'choppy', 'Chrome is not keeping up with the video'
        else:
            out['health'] = 'smooth'
        return out

    def _segment_kbps(self) -> int | None:
        """Output bitrate from the newest HLS segments (the hls muxer reports N/A)."""
        try:
            with open(os.path.join(self.dir, 'index.m3u8')) as fp:
                lines = fp.read().splitlines()
            secs = size = 0.0
            pairs = [(lines[i], lines[i + 1]) for i in range(len(lines) - 1) if lines[i].startswith('#EXTINF:')]
            for inf, name in pairs[-4:]:
                size += os.path.getsize(os.path.join(self.dir, name.strip()))
                secs += float(inf[8:].split(',')[0])
            return round(size * 8 / secs / 1000) if secs else None
        except (OSError, ValueError):
            return None

    def _read_progress(self, proc: subprocess.Popen) -> None:
        """Drain ffmpeg -progress key=value blocks from stdout into samples."""
        block: dict = {}
        try:
            for raw in proc.stdout:
                line = raw.decode('utf-8', 'replace').strip()
                if '=' not in line:
                    continue
                k, v = line.split('=', 1)
                if k != 'progress':
                    block[k] = v
                    continue
                try:
                    kbps = block.get('bitrate', '').replace('kbits/s', '').strip()
                    self._ff_samples.append({
                        't': time.monotonic(),
                        'frame': int(block.get('frame') or 0),
                        'dup': int(block.get('dup_frames') or 0),
                        'drop': int(block.get('drop_frames') or 0),
                        'kbps': round(float(kbps)) if kbps and kbps != 'N/A' else None,
                        'speed': (block.get('speed') or '').strip().rstrip('x') or None,
                    })
                except ValueError:
                    pass
                block = {}
        except (OSError, ValueError):
            pass
        finally:
            try:
                proc.stdout.close()
            except OSError:
                pass

    def _note_page(self, st: dict) -> None:
        if not st:
            return
        self._page_last = st
        self._page_samples.append({'t': time.monotonic(), 'decoded': st.get('decoded'),
                                   'dropped': st.get('dropped'), 'ct': st.get('current_time')})

    # run in its own thread
    def run(self) -> None:
        t = self.tuner
        url = self.watch_url
        try:
            os.makedirs(self.dir, exist_ok=True)
            os.chown(self.dir, t.uid, t.gid)
            _navigate(t.cdp_port, url)
            state = self._wait_playing()
            self.video_size = state.get('video_size') or ''
            self._start_ffmpeg()
            self.state, self.live_at = 'live', time.time()
            logger.info('[capture] session %s channel %s live on tuner %d (source video %s)',
                        self.id, self.channel_id, t.index, self.video_size)
            self._watch()
        except Exception as e:
            if not self._stop.is_set():
                self.state, self.error = 'failed', str(e)[:300]
                logger.warning('[capture] session %s channel %s failed: %s', self.id, self.channel_id, self.error)
                if is_restricted_error(str(e)):
                    # Deterministic: the license server only lets hardware players show
                    # this channel. Remember it so the capture playlists skip it.
                    try:
                        mark_restricted(self.channel_id, 'license keys restricted to hardware players')
                        logger.info('[capture] channel %s marked restricted (hardware players only)',
                                    self.channel_id)
                    except Exception:
                        logger.exception('[capture] could not record restricted channel %s', self.channel_id)
        finally:
            self._teardown()

    def _wait_playing(self) -> dict:
        deadline = time.monotonic() + TUNE_TIMEOUT_SECONDS
        last = None
        while time.monotonic() < deadline and not self._stop.is_set():
            try:
                st = _page_state(self.tuner.cdp_port)
            except Exception:
                st = None
            if st:
                last = st
                if st.get('state') == 'error':
                    raise RuntimeError(f"page error: {st.get('error') or 'unknown'}")
                if st.get('state') == 'playing' and (st.get('decoded') or 0) > 0:
                    return st
            time.sleep(0.5)
        if self._stop.is_set():
            raise RuntimeError('stopped')
        raise RuntimeError(f'video did not start within {TUNE_TIMEOUT_SECONDS}s '
                           f'(last page state: {(last or {}).get("state", "none")})')

    def _start_ffmpeg(self, enc: dict | None = None) -> None:
        t = self.tuner
        self.encoder = enc = dict(enc or ENCODER)
        pre, video = _video_encoder_args(t, enc)
        cmd = [
            'ffmpeg', '-hide_banner', '-loglevel', 'warning', '-nostdin', *pre,
            '-thread_queue_size', '1024', '-f', 'x11grab', '-draw_mouse', '0',
            '-framerate', f'{t.fps * 1000}/1001', '-video_size', f'{t.width}x{t.height}', '-i', f':{t.display}',
            # -isync 0: ffmpeg zeroes each input's timestamps separately, and x11grab
            # takes its first frame while probing, before pulse is even opened. If
            # pulse opens >~100 ms later (busy box mid-tune), the offset sticks and
            # ffmpeg 7's scheduler starves x11grab for the whole session (4-12 fps
            # grabbed, the rest duplicated). Both inputs are wallclock-stamped, so
            # isync lines audio up with video and the offset can't happen.
            '-thread_queue_size', '1024', '-isync', '0', '-f', 'pulse', '-i', f'{t.sink}.monitor',
            '-map', '0:v', '-map', '1:a', *video,
            '-c:a', 'aac', '-b:a', '160k', '-ar', '48000', '-ac', '2',
            '-progress', 'pipe:1', '-stats_period', '2',
            '-f', 'hls', '-hls_time', '2', '-hls_list_size', '8',
            '-hls_flags', 'delete_segments+independent_segments+omit_endlist',
            '-hls_segment_filename', os.path.join(self.dir, 'seg_%06d.ts'),
            os.path.join(self.dir, 'index.m3u8'),
        ]
        self.ffmpeg_started = time.monotonic()
        self._ff_samples.clear()
        self._encoder_proven = False
        # enc['env']: Docker on Windows needs Mesa's d3d12 driver selected for ffmpeg.
        self.ffmpeg = _spawn(cmd, uid=t.uid, gid=t.gid, env={**t.env, **(enc.get('env') or {})},
                             nice=NICE_FFMPEG,
                             log_name=f'ffmpeg{t.index}', stdout=subprocess.PIPE,
                             groups=_encoder_groups(enc))
        threading.Thread(target=self._read_progress, args=(self.ffmpeg,), daemon=True).start()

    def _watch(self) -> None:
        """Keep the session healthy until it's stopped or goes idle."""
        last_time, last_progress, last_reload = None, time.monotonic(), 0.0
        while not self._stop.wait(5):
            on_gpu = self.encoder.get('kind') in ('vaapi', 'nvenc')
            if self.ffmpeg and self.ffmpeg.poll() is not None:
                if on_gpu and time.monotonic() - self.ffmpeg_started < 30:
                    # The pool's test encode passed but the real one died at once:
                    # stop using the GPU until the next pool start, keep this session.
                    self._fall_back_to_cpu(f'GPU encoder failed during capture (exit {self.ffmpeg.returncode})')
                    continue
                raise RuntimeError(f'ffmpeg exited (code {self.ffmpeg.returncode}); '
                                   f'see {RUN_DIR}/ffmpeg{self.tuner.index}.log')
            if (on_gpu and not self._encoder_proven
                    and time.monotonic() - self.ffmpeg_started > GPU_STALL_SECONDS):
                if any(x['frame'] > 0 for x in self._ff_samples):
                    self._encoder_proven = True
                else:
                    # Alive but no frame out: a GPU driver hang (radeonsi at 1080p59.94
                    # with a colour description sat at 100% CPU forever, 2026-11-21).
                    self._fall_back_to_cpu(f'GPU encoder produced no video in {GPU_STALL_SECONDS}s')
                    continue
            if not self.tuner.alive():
                raise RuntimeError('tuner browser exited')
            try:
                st = _page_state(self.tuner.cdp_port) or {}
            except Exception:
                st = {}
            self._note_page(st)
            ct = st.get('current_time')
            now = time.monotonic()
            if ct is not None and ct != last_time:
                last_time, last_progress = ct, now
            stalled = now - last_progress > STALL_RELOAD_SECONDS
            if (st.get('state') == 'error' or stalled) and now - last_reload > 60:
                self.reloads += 1
                last_reload = last_progress = self._last_reload_at = now
                self._page_samples.clear()
                logger.warning('[capture] session %s channel %s %s — reloading page (reload #%d)',
                               self.id, self.channel_id,
                               'page error: ' + str(st.get('error')) if st.get('state') == 'error' else 'video stalled',
                               self.reloads)
                try:
                    _navigate(self.tuner.cdp_port, self.watch_url)
                except Exception as e:
                    logger.warning('[capture] reload failed: %s', e)

    def _fall_back_to_cpu(self, why: str) -> None:
        """Switch every new capture to x264 until the next pool start and restart
        this session's ffmpeg on it, keeping the session (and its tuner) alive."""
        logger.warning('[capture] %s on session %s — switching to CPU encoding', why, self.id)
        _kill(self.ffmpeg)
        ENCODER.clear()
        ENCODER.update(kind='x264', reason=f'{why}; see {RUN_DIR}/ffmpeg{self.tuner.index}.log')
        shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir, exist_ok=True)
        os.chown(self.dir, self.tuner.uid, self.tuner.gid)
        self._start_ffmpeg(dict(ENCODER))

    def stop(self) -> None:
        self._stop.set()

    def _teardown(self) -> None:
        _kill(self.ffmpeg)
        self.ffmpeg = None
        self.tuner.blank()
        if self.state != 'failed':
            self.state = 'ended'
        self.ended_at = time.monotonic()
        shutil.rmtree(self.dir, ignore_errors=True)


# ── pool / daemon state ───────────────────────────────────────────────────────

class CaptureService:
    def __init__(self):
        self.lock = threading.RLock()
        self.tuners: list[Tuner] = []
        self.sessions: dict[str, Session] = {}
        self.pulse: subprocess.Popen | None = None
        self.pool_key: tuple | None = None
        self.pool_error = ''
        self.busy_action = ''          # 'install' | 'uninstall' | 'update-chrome' | 'reinstall'
        self.uid = self.gid = None

    # ---- install actions ----
    def start_action(self, action: str) -> tuple[bool, str]:
        with self.lock:
            if self.busy_action:
                return False, f'{self.busy_action} already running'
            if action in ('install', 'update-chrome') and not supported_platform():
                return False, 'Google Chrome for Linux is only available on amd64 (x86_64).'
            self.busy_action = action
        threading.Thread(target=self._run_action, args=(action,), daemon=True).start()
        return True, 'started'

    def _run_action(self, action: str) -> None:
        try:
            if action in ('uninstall', 'update-chrome', 'install'):
                self.stop_pool()
            if action == 'install':
                installer.install()
            elif action == 'update-chrome':
                installer.install(refresh_chrome=True)
            elif action == 'reinstall':
                installer.reinstall()
            elif action == 'uninstall':
                installer.uninstall()
        finally:
            with self.lock:
                self.busy_action = ''
            self.uid = self.gid = None

    # ---- pool ----
    def _desired_pool(self) -> tuple | None:
        if read_status().get('state') != 'installed':
            return None
        cfg = read_config()
        if not cfg['enabled']:
            return None
        return (cfg['max_tuners'], cfg['quality'], cfg['encoder'])

    def reconcile(self) -> None:
        if self.busy_action:
            return
        status = read_status()
        if status.get('state') == 'installed' and not installer.packages_present():
            self.start_action('reinstall')
            return
        desired = self._desired_pool()
        with self.lock:
            if desired == self.pool_key and all(t.alive() or t.state in ('starting', 'error', 'asleep')
                                                for t in self.tuners):
                self._restart_dead_tuners()
                self._apply_warmth()
                return
            if self.sessions and desired is not None and self.pool_key is not None:
                # Settings changed while someone is watching: apply when the pool is idle.
                if any(s.state in ('tuning', 'live') for s in self.sessions.values()):
                    return
        self.stop_pool()
        if desired is not None:
            self.start_pool(desired)

    def _restart_dead_tuners(self) -> None:
        now = time.monotonic()
        for t in self.tuners:
            if t.state == 'error' and now - getattr(t, 'failed_at', 0) > TUNER_RETRY_SECONDS:
                logger.info('[capture] retrying tuner %d after error: %s', t.index, t.error)
                t.state = 'starting'
                threading.Thread(target=self._start_tuner, args=(t,), daemon=True).start()
                continue
            if t.state in ('idle', 'busy') and not t.alive():
                logger.warning('[capture] tuner %d died — restarting', t.index)
                if t.session:
                    t.session.stop()
                t.stop()
                threading.Thread(target=self._start_tuner, args=(t,), daemon=True).start()

    def _apply_warmth(self) -> None:
        """keep_warm off: close tuners idle for COLD_IDLE_SECONDS. On: start any that
        cold mode closed (the setting was switched back)."""
        keep_warm = read_config().get('keep_warm', True)
        now = time.monotonic()
        for t in self.tuners:
            if keep_warm and t.state == 'asleep':
                logger.info('[capture] keeping tuners warm again: starting tuner %d', t.index)
                t.state = 'starting'
                threading.Thread(target=self._start_tuner, args=(t,), daemon=True).start()
            elif (not keep_warm and t.state == 'idle' and t.session is None
                  and now - t.idle_since > COLD_IDLE_SECONDS):
                logger.info('[capture] tuner %d idle %d min — closing Chrome until the next tune',
                            t.index, int((now - t.idle_since) // 60))
                t.sleep()

    def _start_tuner(self, t: Tuner) -> None:
        try:
            t.start()
        except Exception as e:
            t.state, t.error = 'error', str(e)[:300]
            logger.error('[capture] tuner %d failed to start: %s', t.index, e)
            t.stop()
            t.state = 'error'
            t.failed_at = time.monotonic()

    def start_pool(self, key: tuple) -> None:
        tuners, quality, encoder_setting = key
        try:
            self.uid, self.gid = installer.ensure_capture_user()
            os.makedirs(RUN_DIR, exist_ok=True)
            os.chown(RUN_DIR, self.uid, self.gid)
            os.chmod(RUN_DIR, 0o700)
            os.makedirs('/tmp/.X11-unix', exist_ok=True)
            os.chmod('/tmp/.X11-unix', 0o1777)
            os.makedirs(SESSION_ROOT, exist_ok=True)
            self._start_pulse(tuners)
            enc = detect_encoder(self.uid, self.gid, encoder_setting)
            ENCODER.clear()
            ENCODER.update(enc)
        except Exception as e:
            self.pool_error = f'could not start audio: {e}'
            logger.error('[capture] %s', self.pool_error)
            self.pool_key = key
            return
        keep_warm = read_config().get('keep_warm', True)
        with self.lock:
            self.pool_error = ''
            self.tuners = [Tuner(i, quality, self.uid, self.gid) for i in range(tuners)]
            self.pool_key = key
        if not keep_warm:
            # Cold: nothing runs until the first tune.
            for t in self.tuners:
                t.state = 'asleep'
            logger.info('[capture] %d tuner(s) at %s, started on demand (keep warm is off)', tuners, quality)
            return
        for t in self.tuners:
            threading.Thread(target=self._start_tuner, args=(t,), daemon=True).start()
        logger.info('[capture] starting %d tuner(s) at %s', tuners, quality)

    def _start_pulse(self, tuners: int) -> None:
        _kill(self.pulse)
        try:
            os.unlink(PULSE_SOCKET)
        except OSError:
            pass
        cmd = ['pulseaudio', '--daemonize=no', '--exit-idle-time=-1', '--disallow-exit=yes',
               '--use-pid-file=no', '--system=no', '-n', '--log-target=stderr',
               '-L', f'module-native-protocol-unix socket={PULSE_SOCKET} auth-anonymous=1']
        for i in range(tuners):
            cmd += ['-L', f'module-null-sink sink_name=fct{i} rate=48000 channels=2']
        self.pulse = _spawn(cmd, uid=self.uid, gid=self.gid, env=_base_env(HOME_DIR), log_name='pulse')
        for _ in range(50):
            if os.path.exists(PULSE_SOCKET):
                return
            if self.pulse.poll() is not None:
                break
            time.sleep(0.1)
        raise RuntimeError(f'pulseaudio did not start; see {RUN_DIR}/pulse.log')

    def stop_pool(self) -> None:
        with self.lock:
            sessions = list(self.sessions.values())
            tuners = list(self.tuners)
        for s in sessions:
            s.stop()
        for t in tuners:
            t.stop()
        _kill(self.pulse)
        with self.lock:
            self.pulse = None
            self.tuners = []
            self.sessions = {}
            self.pool_key = None

    # ---- sessions ----
    def open_session(self, channel_id: int) -> tuple[int, dict]:
        """Start or keep alive this channel's session. With every tuner busy, a
        session nobody has polled for PREEMPT_IDLE_SECONDS gives up its tuner:
        that's a client that changed channel or stopped, and making the next tune
        wait out the full idle timeout would make channel surfing on one tuner
        impossible."""
        code, body, victim = self._open_session_once(channel_id)
        if victim is None:
            return code, body
        logger.info('[capture] channel %s takes tuner %d from abandoned session %s (channel %s, idle %ds)',
                    channel_id, victim.tuner.index, victim.id, victim.channel_id,
                    int(time.monotonic() - victim.last_touch))
        victim.stop()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and victim.tuner.state != 'idle':
            time.sleep(0.2)
        code, body, _ = self._open_session_once(channel_id, allow_preempt=False)
        return code, body

    def _open_session_once(self, channel_id: int, allow_preempt: bool = True):
        with self.lock:
            for s in self.sessions.values():
                if s.channel_id == channel_id and s.state in ('tuning', 'live'):
                    s.touch()
                    return 200, s.describe(), None
            # A failed tune stays visible briefly so the caller polling it sees why.
            for s in self.sessions.values():
                if s.channel_id == channel_id and s.state == 'failed' and s.ended_at \
                        and time.monotonic() - s.ended_at < 5:
                    return 200, s.describe(), None
            if not self.tuners:
                why = self.pool_error or ('add-on is not installed or not enabled'
                                          if self._desired_pool() is None else 'tuners are starting')
                return 503, {'error': f'capture unavailable: {why}'}, None
            idle = next((t for t in self.tuners if t.state == 'idle'), None)
            if idle is None:
                asleep = next((t for t in self.tuners if t.state == 'asleep'), None)
                if asleep is not None and not any(t.state == 'starting' for t in self.tuners):
                    logger.info('[capture] waking tuner %d for channel %s', asleep.index, channel_id)
                    asleep.state = 'starting'
                    threading.Thread(target=self._start_tuner, args=(asleep,), daemon=True).start()
                if any(t.state == 'starting' for t in self.tuners):
                    return 503, {'error': 'capture tuners are still starting', 'retry_after': 5, 'reason': 'starting'}, None
                if all(t.state == 'error' for t in self.tuners):
                    return 503, {'error': 'capture tuner failed to start: ' + self.tuners[0].error}, None
                now = time.monotonic()
                abandoned = [x for x in self.sessions.values()
                             if x.state in ('tuning', 'live') and now - x.last_touch > PREEMPT_IDLE_SECONDS]
                if allow_preempt and abandoned:
                    return 0, {}, max(abandoned, key=lambda x: now - x.last_touch)
                return 503, {'error': f'all {len(self.tuners)} capture tuner(s) are busy', 'retry_after': 10, 'reason': 'busy'}, None
            s = Session(channel_id, idle)
            idle.state, idle.session = 'busy', s
            self.sessions[s.id] = s
        threading.Thread(target=self._run_session, args=(s,), daemon=True).start()
        return 200, s.describe(), None

    def _run_session(self, s: Session) -> None:
        try:
            s.run()
        finally:
            with self.lock:
                if s.tuner.session is s:
                    s.tuner.session = None
                    if s.tuner.state == 'busy':
                        s.tuner.state = 'idle'
                        s.tuner.idle_since = time.monotonic()

    def stop_session(self, session_id: str) -> bool:
        with self.lock:
            s = self.sessions.get(session_id)
        if not s or s.state not in ('tuning', 'live'):
            return False
        logger.info('[capture] session %s channel %s stopped from the Bridge page', s.id, s.channel_id)
        s.stop()
        return True

    def reap(self) -> None:
        now = time.monotonic()
        with self.lock:
            for sid, s in list(self.sessions.items()):
                if s.state in ('tuning', 'live') and now - s.last_touch > SESSION_IDLE_SECONDS:
                    logger.info('[capture] session %s channel %s idle %ds — stopping',
                                s.id, s.channel_id, int(now - s.last_touch))
                    s.stop()
                elif s.state in ('ended', 'failed') and s.ended_at and now - s.ended_at > FAILED_SESSION_TTL:
                    del self.sessions[sid]

    def health(self) -> dict:
        status = read_status()
        with self.lock:
            return {
                'platform_supported': supported_platform(),
                'install': status,
                'action': self.busy_action,
                'config': read_config(),
                'pool_error': self.pool_error,
                'encoder': {k: v for k, v in ENCODER.items() if k not in ('gid', 'gids', 'groups', 'env')},
                'tuners': [t.describe() for t in self.tuners],
                'sessions': [s.describe() for s in self.sessions.values()],
                'incompatible_sources': sorted(INCOMPATIBLE_SOURCES),
                'memory': capture_memory(self.uid, self.gid) if self.tuners else None,
            }


# Real memory (PSS: shared pages split between processes; RSS would count Chrome's
# shared memory once per process, ~2x too high). Only the processes' own user may
# read their smaps here (Docker drops CAP_SYS_PTRACE), so it runs as fccapture.
_MEM_READER = r'''
import os
tot = n = 0
uid = os.getuid()
for pid in os.listdir('/proc'):
    if not pid.isdigit():
        continue
    try:
        if os.stat('/proc/' + pid).st_uid != uid:
            continue
        with open('/proc/%s/smaps_rollup' % pid) as fp:
            for line in fp:
                if line.startswith('Pss:'):
                    tot += int(line.split()[1]) * 1024
                    n += 1
                    break
    except OSError:
        pass
print(tot, n)
'''
_mem_cache = {'at': 0.0, 'value': None}


def capture_memory(uid: int | None, gid: int | None) -> dict | None:
    """{'bytes', 'processes'} for everything running as the capture user, cached 30 s."""
    if uid is None:
        return None
    now = time.monotonic()
    if now - _mem_cache['at'] < 30:
        return _mem_cache['value']
    value = None
    try:
        out = subprocess.run([sys.executable, '-I', '-c', _MEM_READER], user=uid, group=gid,
                             capture_output=True, text=True, timeout=10).stdout.split()
        value = {'bytes': int(out[0]), 'processes': int(out[1])}
    except Exception as e:
        logger.debug('[capture] memory read failed: %s', e)
    _mem_cache.update(at=now, value=value)
    return value


SERVICE = CaptureService()


class Handler(BaseHTTPRequestHandler):
    server_version = 'fc-capture/1'

    def log_message(self, fmt, *args):  # quiet: FastChannels logs the user-facing side
        pass

    def _json(self, code: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict:
        try:
            n = int(self.headers.get('Content-Length') or 0)
            return json.loads(self.rfile.read(n) or b'{}') if n else {}
        except (ValueError, OSError):
            return {}

    def do_GET(self):
        if self.path == '/health':
            return self._json(200, SERVICE.health())
        return self._json(404, {'error': 'not found'})

    def do_POST(self):
        if self.path == '/sessions':
            try:
                channel_id = int(self._body().get('channel_id'))
            except (TypeError, ValueError):
                return self._json(400, {'error': 'channel_id required'})
            code, body = SERVICE.open_session(channel_id)
            return self._json(code, body)
        if self.path.startswith('/sessions/') and self.path.endswith('/stop'):
            sid = self.path[len('/sessions/'):-len('/stop')]
            ok = SERVICE.stop_session(sid)
            return self._json(200 if ok else 404, {'ok': ok})
        if self.path in ('/install', '/uninstall', '/update-chrome'):
            ok, msg = SERVICE.start_action(self.path.strip('/'))
            return self._json(202 if ok else 409, {'ok': ok, 'message': msg})
        if self.path == '/reconcile':
            threading.Thread(target=SERVICE.reconcile, daemon=True).start()
            return self._json(202, {'ok': True})
        return self._json(404, {'error': 'not found'})


def _loop() -> None:
    while True:
        try:
            SERVICE.reconcile()
            SERVICE.reap()
        except Exception:
            logger.exception('[capture] reconcile loop error')
        time.sleep(5)


def main() -> None:
    # Same handlers/format as gunicorn and the RQ workers: local-time timestamps,
    # and the shared log file, so capture lines show on the Logs page.
    from ..logfile import setup as _setup_logfile
    _setup_logfile()
    shutil.rmtree(SESSION_ROOT, ignore_errors=True)
    threading.Thread(target=_loop, daemon=True).start()
    server = ThreadingHTTPServer((DAEMON_HOST, DAEMON_PORT), Handler)
    server.daemon_threads = True

    def _shutdown(*_):
        SERVICE.stop_pool()
        os._exit(0)
    signal.signal(signal.SIGTERM, _shutdown)
    logger.info('[capture] service listening on %s:%d', DAEMON_HOST, DAEMON_PORT)
    server.serve_forever()


if __name__ == '__main__':
    main()

"""Kodi/Fire TV HDMI-encoder DRM bridge — JSON-RPC + adb orchestration.

Owns all direct communication with the Kodi/Fire TV device: triggering playback via
the fc_bridge resolver addon (dev/kodi/plugin.video.fc_bridge/), health checks,
wake/relaunch recovery, and post-tune playback confirmation.

Design principle (dev/kodi/IMPLEMENTATION_PLAN.md): speed over per-request certainty.
trigger_channel() returns as soon as Kodi acknowledges the request — it does not wait
for real decode to start. confirm_playback() is a separate, slower call meant to run
asynchronously (the watchdog), never inline with a user-facing request.

Full validated groundwork (sleep/wake fix, confirmed-working sources, Cox/Warner root
cause) lives in dev/kodi/README.md.
"""
import logging
import subprocess
import time
from urllib.parse import urlencode

import requests

from .models import AppSettings

logger = logging.getLogger(__name__)

_JSONRPC_TIMEOUT = 3
_DEFAULT_LICENSE_TYPE = 'com.widevine.alpha'

# Sources confirmed (dev/kodi/README.md, real Fire TV Stick 4K hardware, 2026-08-16) to
# actually decrypt through Kodi/inputstream.adaptive. Cox and Warner TVE hit a confirmed,
# non-fixable inputstream.adaptive same-KID session-splitting wall and stay excluded;
# Roku is pending re-verification (rate-limited mid-check) — excluded until actually
# confirmed. Philo confirmed 2026-08-18 (real Widevine key exchange + sustained
# fluctuating decode bitrate on-device via BBC News). See IMPLEMENTATION_PLAN.md section 5.
# Shared by app/routes/play.py (play_kodi_bridge) and app/generators/m3u.py
# (generate_kodi_bridge_m3u) — keep both in sync via this single source of truth.
KODI_BRIDGE_TRUSTED_SOURCES = frozenset({
    'sling', 'nbc_tve', 'pbs', 'amazon_prime_free', 'directv', 'vidaa', 'philo',
})


class KodiBridgeNotConfigured(RuntimeError):
    """Raised when required kodi_bridge settings aren't configured."""


def is_configured() -> bool:
    settings = AppSettings.get()
    return bool(
        settings.kodi_bridge_enabled
        and settings.effective_kodi_bridge_device_url()
        and settings.effective_kodi_bridge_adb_address()
    )


def keepalive_enabled() -> bool:
    """Whether the scheduler watchdog should actively ping/relaunch the device — the
    bridge can be enabled (trigger_channel usable) with this off, e.g. while testing."""
    return bool(AppSettings.get().kodi_bridge_keepalive_enabled) and is_configured()


def _device_url() -> str:
    url = AppSettings.get().effective_kodi_bridge_device_url()
    if not url:
        raise KodiBridgeNotConfigured('kodi_bridge_device_url is not configured')
    return url


def _adb_address() -> str:
    address = AppSettings.get().effective_kodi_bridge_adb_address()
    if not address:
        raise KodiBridgeNotConfigured('kodi_bridge_adb_address is not configured')
    return address


def _jsonrpc(method: str, params: dict | None = None, timeout: int = _JSONRPC_TIMEOUT) -> dict:
    payload = {'jsonrpc': '2.0', 'method': method, 'id': 1}
    if params is not None:
        payload['params'] = params
    resp = requests.post(f'{_device_url()}/jsonrpc', json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def is_alive(timeout: int = 2) -> bool:
    """Fast JSON-RPC ping. False on any failure (device asleep, network down, Kodi
    crashed/backgrounded) — never raises except for missing configuration."""
    try:
        result = _jsonrpc('JSONRPC.Ping', timeout=timeout)
    except KodiBridgeNotConfigured:
        raise
    except Exception:
        return False
    return result.get('result') == 'pong'


def wake_and_relaunch(max_wait_s: int = 30) -> bool:
    """Recovery path: adb-connect, force-relaunch Kodi, wait for JSON-RPC to answer.

    Covers two confirmed real gaps (dev/kodi/README.md): Kodi getting backgrounded (it
    has no launcher role, so anything that returns to the Fire TV home screen strands
    it) and Kodi's BOOT_COMPLETED receiver existing but not actually relaunching the
    app after a power-cycle/reboot.
    """
    address = _adb_address()
    try:
        subprocess.run(
            ['adb', 'connect', address],
            capture_output=True, timeout=10, check=False,
        )
        subprocess.run(
            ['adb', '-s', address, 'shell', 'am', 'start', '-n', 'org.xbmc.kodi/.Splash'],
            capture_output=True, timeout=10, check=False,
        )
    except Exception as e:
        logger.warning('[kodi-bridge] wake_and_relaunch adb call failed: %s', e)
        return False

    deadline = time.monotonic() + max_wait_s
    while time.monotonic() < deadline:
        if is_alive(timeout=2):
            return True
        time.sleep(2)
    logger.warning('[kodi-bridge] wake_and_relaunch: Kodi did not come back within %ss', max_wait_s)
    return False


def stop_playback() -> None:
    """Best-effort stop of whatever's currently playing. Not required before
    trigger_channel() — Player.Open on a new item replaces the current one on its
    own — but useful for an explicit "tune away" (e.g. watchdog idling the device)."""
    try:
        players = (_jsonrpc('Player.GetActivePlayers').get('result')) or []
        for player in players:
            _jsonrpc('Player.Stop', {'playerid': player['playerid']})
    except Exception as e:
        logger.warning('[kodi-bridge] stop_playback failed (continuing): %s', e)


def trigger_channel(
    manifest_url: str,
    license_url: str | None = None,
    *,
    name: str = 'FastChannels',
    license_type: str = _DEFAULT_LICENSE_TYPE,
) -> bool:
    """Tell Kodi to play this manifest via the fc_bridge resolver addon.

    Returns True if Kodi *acknowledged* the request — not proof playback actually
    started. Pair with confirm_playback(), called asynchronously, never inline with
    the redirect (dev/kodi/IMPLEMENTATION_PLAN.md section 2).
    """
    params = {'url': manifest_url, 'name': name}
    if license_url:
        params['license'] = license_url
        params['license_type'] = license_type
    plugin_url = 'plugin://plugin.video.fc_bridge/?' + urlencode(params)
    try:
        result = _jsonrpc('Player.Open', {'item': {'file': plugin_url}}, timeout=5)
    except Exception as e:
        logger.warning('[kodi-bridge] trigger_channel failed: %s', e)
        return False
    return result.get('result') == 'OK'


def confirm_playback(timeout_s: int = 5) -> dict:
    """Poll for real playback confirmation after a trigger — meant to run async
    (watchdog), never blocking the user-facing redirect.

    Returns {"playing": bool, "reason": str}.
    """
    deadline = time.monotonic() + timeout_s
    reason = 'no active player'
    while time.monotonic() < deadline:
        try:
            players = (_jsonrpc('Player.GetActivePlayers').get('result')) or []
        except Exception as e:
            reason = f'jsonrpc error: {e}'
            time.sleep(1)
            continue
        if not players:
            time.sleep(1)
            continue
        try:
            props = _jsonrpc('Player.GetProperties', {
                'playerid': players[0]['playerid'],
                'properties': ['speed', 'time'],
            }).get('result') or {}
        except Exception as e:
            reason = f'jsonrpc error: {e}'
            time.sleep(1)
            continue
        if props.get('speed') == 1:
            return {'playing': True, 'reason': 'ok'}
        reason = f'speed={props.get("speed")!r}'
        time.sleep(1)
    return {'playing': False, 'reason': reason}

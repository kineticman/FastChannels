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
import re
import subprocess
import time
from urllib.parse import urlencode

import redis
import requests
from flask import current_app

from .models import AppSettings

logger = logging.getLogger(__name__)

_JSONRPC_TIMEOUT = 3
_DEFAULT_LICENSE_TYPE = 'com.widevine.alpha'

# How long to wait after stopping the current player before opening the next one, on
# a genuine channel change. Confirmed live 2026-08-21: Player.Open alone does not wait
# for the previous secure MediaCodec instance to release before the new one tries to
# open, and racing it hits CDVDVideoCodecAndroidMediaCodec::Open - InstanceGuard locked
# (this MediaTek SoC allows only one secure decoder instance at a time) — Kodi then
# silently falls back to a non-DRM-capable codec and playback freezes with no visible
# error (switching Ax Men -> Duck Dynasty reproduced this). Kept as short as possible
# since this runs inline with the user-facing redirect (dev/kodi/IMPLEMENTATION_PLAN.md
# speed-first design) — 400ms is comfortably above the ~85ms Kodi's own internal retry
# took to self-recover on the one case observed where it didn't just get stuck.
_CODEC_RELEASE_GRACE_S = 0.4

# How long the DVR-side encoder stream (kodi_bridge_encoder_url) must show no active
# puller before we tell Kodi to stop — confirmed live 2026-08-19: a PBS Kids session
# kept decrypting/streaming for 10+ hours after the actual downstream viewer (whatever
# was consuming the Channels DVR capture-engine stream) had stopped, since Kodi has no
# way to know the DVR side ended. 5 minutes mirrors the stale-session grace a mature
# third-party Channels DVR monitor (ChannelWatch) uses for the same "no explicit stop
# event" gap.
_ENCODER_IDLE_GRACE_S = 5 * 60
_encoder_idle_since: float | None = None

# Set by trigger_channel() (something asked Kodi to play), cleared once
# check_idle_and_stop() actually stops it. Gates the whole idle check so it
# doesn't poll the DVR and log "stopping Kodi playback" every ~5min forever
# whenever the device just sits idle between uses (reported live 2026-08-19:
# the 45s watchdog ticked all night with nothing ever triggered, each idle
# window re-firing the same stop + log line since dvr_encoder_active() just
# keeps reporting not-active with nothing to distinguish "was playing, now
# isn't" from "was never asked to play anything").
#
# Lives in Redis, not a module global: trigger_channel() runs in a gunicorn
# web worker (app/routes/play.py) but check_idle_and_stop() runs in the
# separate scheduler process (app/worker.py's BackgroundScheduler) — a plain
# global set in one process is invisible in the other. The 24h TTL is just a
# safety net against a stuck-active flag surviving forever if some future
# code path fails to clear it; it plays no role in the normal fire/clear
# cycle above (that's every ~5min, always long before this would expire).
_BELIEVED_ACTIVE_KEY = 'fc:kodi-bridge:believed-active'
_BELIEVED_ACTIVE_TTL_S = 24 * 60 * 60

# Which channel (f'{source_name}:{channel_id}') trigger_channel() last successfully
# opened. Confirmed live 2026-08-21: some downstream consumer of the bridge's fixed
# encoder URL re-hits /play/kodi-bridge/<source>/<id>.m3u8 for the SAME channel every
# ~90-150s (channel_id and source unchanged across retriggers, confirmed via distinct
# request_ids in the access log — not a caching/polling artifact and not us testing).
# Every one of those redundant retriggers calls Player.Open again, which on this
# device's MediaTek secure decoder races the still-releasing instance from the PRIOR
# open and hits the same InstanceGuard wall documented above — freezing playback that
# was otherwise fine. There's no legitimate reason to reopen a channel that's already
# the active one, so trigger_channel() short-circuits to a no-op (still returns True)
# when channel_key matches. A real channel change (different key) always retriggers
# normally.
#
# Deliberately a MUCH shorter TTL than _BELIEVED_ACTIVE_KEY's 24h safety net, not the
# same one: this key gets set as soon as Kodi's Player.Open *acknowledges* the request,
# which happens before the fc_bridge addon actually fetches/resolves the underlying
# manifest — so a channel whose resolution fails afterward (confirmed live 2026-08-21,
# DirecTV BBC News mid-token-expiry) gets stuck permanently marked "active" with
# nothing actually playing, silently deduping every real retry attempt until it's
# cleared. A short TTL bounds that to a self-healing few minutes instead of requiring
# a manual `redis-cli DEL`, while still comfortably covering the ~90-150s external
# re-poll interval this key exists to dedupe against.
_ACTIVE_CHANNEL_KEY = 'fc:kodi-bridge:active-channel'
_ACTIVE_CHANNEL_TTL_S = 5 * 60


def _get_active_channel() -> str | None:
    try:
        val = redis.from_url(current_app.config['REDIS_URL']).get(_ACTIVE_CHANNEL_KEY)
        return val.decode() if val else None
    except Exception as e:
        logger.warning('[kodi-bridge] _get_active_channel failed: %s', e)
        return None


def _set_active_channel(channel_key: str | None) -> None:
    try:
        r = redis.from_url(current_app.config['REDIS_URL'])
        if channel_key:
            r.setex(_ACTIVE_CHANNEL_KEY, _ACTIVE_CHANNEL_TTL_S, channel_key)
        else:
            r.delete(_ACTIVE_CHANNEL_KEY)
    except Exception as e:
        logger.warning('[kodi-bridge] _set_active_channel(%s) failed: %s', channel_key, e)


def _set_believed_active(active: bool) -> None:
    try:
        r = redis.from_url(current_app.config['REDIS_URL'])
        if active:
            r.setex(_BELIEVED_ACTIVE_KEY, _BELIEVED_ACTIVE_TTL_S, '1')
        else:
            r.delete(_BELIEVED_ACTIVE_KEY)
    except Exception as e:
        logger.warning('[kodi-bridge] _set_believed_active(%s) failed: %s', active, e)


def _is_believed_active() -> bool:
    try:
        return bool(redis.from_url(current_app.config['REDIS_URL']).exists(_BELIEVED_ACTIVE_KEY))
    except Exception as e:
        logger.warning('[kodi-bridge] _is_believed_active check failed: %s', e)
        return True  # fail open to the pre-existing always-check behavior

# Sources confirmed (dev/kodi/README.md, real Fire TV Stick 4K hardware, 2026-08-16) to
# actually decrypt through Kodi/inputstream.adaptive. Cox and Warner TVE hit a confirmed,
# non-fixable inputstream.adaptive same-KID session-splitting wall and stay excluded.
# Philo confirmed 2026-08-18 (real Widevine key exchange + sustained fluctuating decode
# bitrate on-device via BBC News). Roku confirmed 2026-08-20 (AFTMA08C15/Android 11, CNN
# Headlines: 2 license POSTs both 200, sustained speed=1 real-time decode over an 18s
# poll) — the earlier 403 rate-limit hit mid-check was transient upstream throttling, not
# reproduced on retest. See IMPLEMENTATION_PLAN.md section 5.
# Shared by app/routes/play.py (play_kodi_bridge) and app/generators/m3u.py
# (generate_kodi_bridge_m3u) — keep both in sync via this single source of truth.
KODI_BRIDGE_TRUSTED_SOURCES = frozenset({
    'sling', 'nbc_tve', 'pbs', 'amazon_prime_free', 'directv', 'vidaa', 'philo', 'roku',
})


def drm_bridge_mode_for(source_name: str) -> bool:
    """Whether a DRM channel on this source should be kept active + bridged rather than
    disabled — true if EITHER bridge can plausibly serve it: the PrismCast browser/EME
    bridge (global drm_bridge_enabled toggle, any license_url-capable source) or this
    Kodi/HDMI-encoder bridge (global kodi_bridge_enabled toggle, restricted to
    KODI_BRIDGE_TRUSTED_SOURCES). Callers still separately gate on the scraper actually
    having license_url handling."""
    settings = AppSettings.get()
    if bool(settings.drm_bridge_enabled):
        return True
    return bool(settings.kodi_bridge_enabled) and source_name in KODI_BRIDGE_TRUSTED_SOURCES


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


def set_captions_enabled(enabled: bool) -> bool:
    """Push Kodi's "Enable parsing for closed captions" setting (subtitles.parsecaptions).

    Off by default in Kodi, and required for CEA-608/708 captions embedded in a DRM
    source's video track to be extracted and rendered at all — confirmed 2026-08-18
    (CBS News Texas / Sling, CBS News 24/7 / Philo): with it off, Player.GetProperties
    reports zero subtitle tracks despite the manifest's <Accessibility> signaling; with
    it on, Kodi surfaces a "cc" track and auto-selects it. Since this bridge captures
    Kodi's raw HDMI output, whatever it renders (including this overlay) is what the
    capture card sees — there's no separate/per-viewer caption toggle downstream, this
    is the only switch. Takes effect on already-playing content, no relaunch needed.

    Returns True on a confirmed write, False on any failure (device unreachable, etc.)
    — best-effort, never raises, mirrors stop_playback()'s tolerance.
    """
    try:
        result = _jsonrpc('Settings.SetSettingValue', {
            'setting': 'subtitles.parsecaptions',
            'value': bool(enabled),
        })
    except Exception as e:
        logger.warning('[kodi-bridge] set_captions_enabled(%s) failed: %s', enabled, e)
        return False
    return bool(result.get('result'))


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


def dvr_encoder_active(timeout: int = 5) -> bool | None:
    """Whether Channels DVR currently has an active puller on kodi_bridge_encoder_url
    — i.e. whether anyone downstream is actually watching, confirmed via the DVR
    server's own (undocumented but stable — relied on by third-party monitors like
    ChannelWatch) /dvr status endpoint, which reports live-viewing activity as
    {"session-id": "Watching ch<N> <name> from <ip>: ..."}.

    Returns None — never treated as "stop it" by the caller — when this can't be
    determined: channels_dvr_url isn't configured, kodi_bridge_encoder_url isn't a
    Channels-DVR-capture-engine style URL with a /channels/<id>/ segment (e.g. a
    standalone hardware encoder we have no DVR-side visibility into), or the DVR
    didn't respond.
    """
    settings = AppSettings.get()
    dvr_url = (settings.effective_channels_dvr_url() or '').strip().rstrip('/')
    encoder_url = settings.effective_kodi_bridge_encoder_url() or ''
    if not dvr_url or not encoder_url:
        return None
    match = re.search(r'/channels/([^/]+)/', encoder_url)
    if not match:
        return None
    channel = match.group(1)
    try:
        resp = requests.get(f'{dvr_url}/dvr', timeout=timeout)
        resp.raise_for_status()
        activity = resp.json().get('activity') or {}
    except Exception as e:
        logger.warning('[kodi-bridge] dvr_encoder_active check failed: %s', e)
        return None
    needle = f'Watching ch{channel} '
    return any(str(v).startswith(needle) for v in activity.values())


def check_idle_and_stop() -> None:
    """Called by the watchdog on each tick (dev/kodi — see _ENCODER_IDLE_GRACE_S).

    If the DVR is actively pulling the encoder stream, resets the idle timer. If not,
    and it's stayed that way for _ENCODER_IDLE_GRACE_S, stops whatever Kodi is
    playing — nobody's on the other end of the encoder stream, no reason to keep
    decrypting/streaming. No-ops when dvr_encoder_active() returns None (can't
    determine activity) so this never fires on a guess.

    Also no-ops entirely, without even polling the DVR, when nothing has asked
    Kodi to play a channel since the last time this already stopped one — see
    _BELIEVED_ACTIVE_KEY's docstring.
    """
    global _encoder_idle_since
    if not _is_believed_active():
        _encoder_idle_since = None
        return
    active = dvr_encoder_active()
    if active is None or active:
        _encoder_idle_since = None
        return
    now = time.time()
    if _encoder_idle_since is None:
        _encoder_idle_since = now
        return
    if now - _encoder_idle_since >= _ENCODER_IDLE_GRACE_S:
        logger.info('[kodi-bridge] watchdog: encoder stream idle >= %ss, stopping Kodi playback',
                     _ENCODER_IDLE_GRACE_S)
        stop_playback()
        _encoder_idle_since = None
        _set_believed_active(False)
        _set_active_channel(None)


def trigger_channel(
    manifest_url: str,
    license_url: str | None = None,
    *,
    name: str = 'FastChannels',
    license_type: str = _DEFAULT_LICENSE_TYPE,
    channel_key: str | None = None,
) -> bool:
    """Tell Kodi to play this manifest via the fc_bridge resolver addon.

    Returns True if Kodi *acknowledged* the request — not proof playback actually
    started. Pair with confirm_playback(), called asynchronously, never inline with
    the redirect (dev/kodi/IMPLEMENTATION_PLAN.md section 2).

    channel_key (typically f'{source_name}:{channel_id}') lets repeated requests for
    the SAME already-active channel short-circuit instead of reopening — see
    _ACTIVE_CHANNEL_KEY's docstring for why that reopen is actively harmful on this
    device. Pass None to always trigger unconditionally (e.g. from a manual/debug path).
    """
    if channel_key and channel_key == _get_active_channel():
        logger.info('[kodi-bridge] trigger_channel: %s already active, skipping reopen', channel_key)
        return True

    try:
        players = (_jsonrpc('Player.GetActivePlayers', timeout=2).get('result')) or []
        if players:
            for player in players:
                _jsonrpc('Player.Stop', {'playerid': player['playerid']}, timeout=2)
            time.sleep(_CODEC_RELEASE_GRACE_S)
    except Exception as e:
        logger.warning('[kodi-bridge] trigger_channel pre-stop failed (continuing): %s', e)

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
    ok = result.get('result') == 'OK'
    if ok:
        _set_believed_active(True)
        _set_active_channel(channel_key)
    return ok


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

"""FastChannels Player remote-play trigger — adb orchestration.

For the FastChannels-owned player app (app/fc_player/) — a single-Activity
Media3/ExoPlayer app with no UI of its own to speak of; everything is driven from
here over adb. There is no StreamVault (or any other third-party app) involved: an
earlier design routed playback through StreamVault-IPTV's own UI, but once it was
clear this device needs no on-device browsing at all, that whole integration (and
the fragility that came with it) was dropped in favor of this much smaller app.

Android has no in-process way to launch this: a plain context.startActivity() from
a background service is blocked by the background-activity-start restriction —
confirmed live 2026-08-24 — and the standard workaround (a full-screen-intent
notification) is ALSO blocked on this device by Fire OS's own notification allowlist
("AmazonNotificationService: Package ... is not in allow list"). adb-shell-privileged
launches are exempt from both restrictions, so this triggers playback via a plain
`adb shell am start -n <package>/.PlaybackActivity` with simple string/boolean extras
(no Serializable involved — PlaybackActivity reads plain Intent extras).

Live-validated 2026-08-24: real secure Widevine DASH decode (MediaTek secure hardware
decoder) via a single adb am start call, ~2s from call to first rendered frame.
"""
import json
import logging
import os
import re
import shlex
import subprocess
import time

import redis
import requests
from flask import current_app

from .models import AppSettings

logger = logging.getLogger(__name__)

_ADB_TIMEOUT = 10
# Fixed — this is our own player app's package (app/fc_player/), not a setting; it never
# varies per install.
_PLAYER_COMPONENT = 'com.fastchannels.player/.PlaybackActivity'

# Where the Dockerfile bundles the latest release APK, when one exists — see its
# comment for why this is fetched from GitHub Releases at image-build time rather
# than committed to git.
_BUNDLED_APK_PATH = '/app/fc_player_release.apk'

# Android's largest accepted timeout. Together with stay_on_while_plugged_in it
# is the closest portable “never” setting for Fire TV / Android TV devices.
_NEVER_TIMEOUT_MS = 2_147_483_647


def bundled_apk_path() -> str | None:
    """The bundled release APK's path, or None if the release asset was unavailable
    when this image was built — the settings page's Install button uses this to know
    whether it has anything to install."""
    return _BUNDLED_APK_PATH if os.path.isfile(_BUNDLED_APK_PATH) else None

# How long a bridged channel can sit with no confirmed viewer (neither Channels DVR
# activity nor a /watch heartbeat) before we stop it. Matches the old Kodi bridge's
# _ENCODER_IDLE_GRACE_S, which borrowed the same window from a mature third-party
# Channels DVR monitor's own idle-detection grace period.
_IDLE_GRACE_S = 5 * 60

# How often to retry the (heavier) DVR /devices guide-number lookup for a channel
# that hasn't resolved to any guide number yet — bounds how often that bigger call
# happens rather than letting it fire on every single watchdog tick forever.
_DVR_LOOKUP_RETRY_S = 5 * 60

_DVR_POLL_TIMEOUT = 5

# All Redis keys below exist only while idle-stop is enabled and something has been
# triggered; a disabled toggle means trigger_channel() never touches Redis at all for
# any of this, so there's zero added cost for installs that don't opt in.
_BELIEVED_ACTIVE_KEY = 'fc:fc-player:believed-active'
_BELIEVED_ACTIVE_TTL_S = 24 * 60 * 60
_CHANNEL_KEY_KEY = 'fc:fc-player:channel-key'
_TRACKED_CHANNELS_KEY = 'fc:fc-player:tracked-channels'
_DVR_LOOKUP_COOLDOWN_KEY = 'fc:fc-player:dvr-lookup-cooldown'
_IDLE_SINCE_KEY = 'fc:fc-player:idle-since'
_WEB_HEARTBEAT_PREFIX = 'fc:fc-player:web-heartbeat:'
_WEB_HEARTBEAT_TTL_S = 45


def _redis():
    return redis.from_url(current_app.config['REDIS_URL'])


def idle_stop_enabled() -> bool:
    """Whether the idle-stop watchdog should be doing anything at all. Cheap gate
    checked first everywhere below, so a disabled toggle costs nothing."""
    settings = AppSettings.get()
    return bool(settings.fc_player_bridge_idle_stop_enabled and is_configured())


def captions_enabled() -> bool:
    """Whether the device should render an English subtitle/CC track when the stream
    advertises one. Applied per-trigger (an Intent extra passed at am start time), not
    a live mid-stream toggle — the app has no channel to re-check settings once a
    stream is already playing."""
    return bool(AppSettings.get().fc_player_bridge_captions_enabled)


class FcPlayerNotConfigured(RuntimeError):
    """Raised when required fc_player_bridge settings aren't configured."""


def is_configured() -> bool:
    settings = AppSettings.get()
    return bool(
        settings.fc_player_bridge_enabled
        and settings.effective_fc_player_bridge_adb_address()
    )


def _adb_address() -> str:
    address = AppSettings.get().effective_fc_player_bridge_adb_address()
    if not address:
        raise FcPlayerNotConfigured('fc_player_bridge_adb_address is not configured')
    return address


def test_connection() -> tuple[bool, str]:
    """adb-reachability check for the settings page's Test Connection button.

    Distinguishes "device unreachable" from "reachable but the player app isn't
    installed yet" — the latter is exactly what the (not yet built) install flow
    would need to detect too.
    """
    address = _adb_address()
    try:
        subprocess.run(
            ['adb', 'connect', address],
            capture_output=True, timeout=_ADB_TIMEOUT, check=False,
        )
        state = subprocess.run(
            ['adb', '-s', address, 'get-state'],
            capture_output=True, timeout=_ADB_TIMEOUT, check=False, text=True,
        )
    except Exception as e:
        return False, f'adb error: {e}'

    if state.returncode != 0 or 'device' not in (state.stdout or ''):
        return False, ("Couldn't reach the device over adb — check the IP and that ADB "
                        "debugging is enabled (Settings > My Fire TV > Developer options).")

    try:
        packages = subprocess.run(
            ['adb', '-s', address, 'shell', 'pm', 'list', 'packages', 'com.fastchannels.player'],
            capture_output=True, timeout=_ADB_TIMEOUT, check=False, text=True,
        )
    except Exception:
        return True, 'Device reachable over adb.'

    if 'com.fastchannels.player' in (packages.stdout or ''):
        return True, 'Device reachable — FastChannels Player is installed.'
    return True, 'Device reachable, but FastChannels Player is not installed yet.'


def install_app(apk_path: str, timeout: int = 90) -> tuple[bool, str]:
    """adb-installs the FastChannels Player APK onto the configured device.

    Works without ever touching the device's "Apps from Unknown Sources" toggle —
    that setting only gates on-device tap-to-install of a downloaded APK file (the
    package installer UI flow); `adb install` goes through the privileged shell/
    package-manager path directly and was never subject to that gate. Live-verified
    2026-08-25: installed cleanly onto a device with that toggle in its default
    (never touched) state.

    Passing `-r` (replace existing) means this also works as the update path once
    something's already installed, as long as it's signed with the same key — see
    project memory on release signing for why that matters.
    """
    address = _adb_address()
    try:
        subprocess.run(
            ['adb', 'connect', address],
            capture_output=True, timeout=_ADB_TIMEOUT, check=False,
        )
        result = subprocess.run(
            ['adb', '-s', address, 'install', '-r', apk_path],
            capture_output=True, timeout=timeout, check=False, text=True,
        )
    except Exception as e:
        return False, f'adb error: {e}'

    output = ((result.stdout or '') + (result.stderr or '')).strip()
    if result.returncode == 0 and 'Success' in output:
        return True, 'Installed successfully.'
    return False, output or f'adb install failed (rc={result.returncode})'


def _adb_shell(address: str, *command: str, timeout: int = _ADB_TIMEOUT) -> tuple[bool, str]:
    """Run a small adb shell command and return its combined text safely."""
    try:
        result = subprocess.run(
            ['adb', '-s', address, 'shell', *command],
            capture_output=True, timeout=timeout, check=False, text=True,
        )
    except Exception as e:
        return False, f'adb error: {e}'
    text = ((result.stdout or '') + (result.stderr or '')).strip()
    return result.returncode == 0, text


def _device_connected() -> tuple[bool, str, str | None]:
    """Connect to the configured device and return its adb address if usable."""
    try:
        address = _adb_address()
    except FcPlayerNotConfigured:
        return False, 'Set a Fire TV / Android TV IP address first.', None
    try:
        subprocess.run(['adb', 'connect', address], capture_output=True,
                       timeout=_ADB_TIMEOUT, check=False)
        state = subprocess.run(['adb', '-s', address, 'get-state'], capture_output=True,
                               timeout=_ADB_TIMEOUT, check=False, text=True)
    except Exception as e:
        return False, f'adb error: {e}', None
    if state.returncode != 0 or 'device' not in (state.stdout or ''):
        return False, 'Could not reach the device over adb. Check its IP and ADB authorization.', None
    return True, '', address


def _setting_number(address: str, namespace: str, name: str) -> int | None:
    ok, value = _adb_shell(address, 'settings', 'get', namespace, name)
    if not ok:
        return None
    try:
        return int(value.strip())
    except (TypeError, ValueError):
        return None


def device_controls_status() -> dict:
    """Return lightweight, user-facing diagnostics for the Device Controls modal.

    This deliberately uses only standard adb shell commands: it works for both
    Fire OS and Android TV, and does not require the bridge feature toggle itself
    to be enabled.
    """
    connected, message, address = _device_connected()
    if not connected:
        return {'ok': False, 'message': message}

    _, model = _adb_shell(address, 'getprop', 'ro.product.model')
    _, release = _adb_shell(address, 'getprop', 'ro.build.version.release')
    _, power = _adb_shell(address, 'dumpsys', 'power')
    _, focus = _adb_shell(address, 'dumpsys', 'window')
    _, package_info = _adb_shell(address, 'dumpsys', 'package', 'com.fastchannels.player')
    _, sessions = _adb_shell(address, 'dumpsys', 'media_session')

    wake_match = re.search(r'mWakefulness=(\w+)', power)
    display_match = re.search(r'Display Power:\s*state=(\w+)', power)
    version_name = re.search(r'\bversionName=([^\s]+)', package_info)
    version_code = re.search(r'\bversionCode=(\d+)', package_info)
    focus_match = re.search(r'mCurrentFocus=([^\r\n]+)', focus)
    player_session = re.search(
        r'package=com\.fastchannels\.player(?:(?!\n\s*package=).){0,1200}?'
        r'state=PlaybackState \{state=(\d+)', sessions, re.S,
    )

    return {
        'ok': True,
        'address': address,
        'model': model.strip() or 'Android TV device',
        'android_version': release.strip() or None,
        'awake': (wake_match.group(1).lower() == 'awake') if wake_match else None,
        'wakefulness': wake_match.group(1) if wake_match else 'Unknown',
        'display_power': display_match.group(1) if display_match else 'Unknown',
        'focus': focus_match.group(1).strip() if focus_match else None,
        'player_installed': bool(version_name),
        'player_version': version_name.group(1) if version_name else None,
        'player_version_code': int(version_code.group(1)) if version_code else None,
        'player_playing': player_session.group(1) == '3' if player_session else False,
        'stay_on_while_powered': _setting_number(address, 'global', 'stay_on_while_plugged_in') or 0,
        'screen_off_timeout': _setting_number(address, 'system', 'screen_off_timeout'),
        'sleep_timeout': _setting_number(address, 'secure', 'sleep_timeout'),
    }


def wake_device() -> tuple[bool, str]:
    connected, message, address = _device_connected()
    if not connected:
        return False, message
    ok, output = _adb_shell(address, 'input', 'keyevent', 'KEYCODE_WAKEUP')
    return (True, 'Wake command sent.') if ok else (False, output or 'Could not wake the device.')


def set_device_power_settings(*, stay_awake: bool, screen_off_timeout: int,
                              sleep_timeout: int) -> tuple[bool, str, dict | None]:
    """Apply explicit display settings and return the values they replaced."""
    if (not isinstance(screen_off_timeout, int) or not isinstance(sleep_timeout, int)
            or screen_off_timeout < 0 or sleep_timeout < 0
            or screen_off_timeout > _NEVER_TIMEOUT_MS or sleep_timeout > _NEVER_TIMEOUT_MS):
        return False, 'Invalid display timeout.', None
    connected, message, address = _device_connected()
    if not connected:
        return False, message, None
    previous = {
        'stay_on_while_powered': _setting_number(address, 'global', 'stay_on_while_plugged_in'),
        'screen_off_timeout': _setting_number(address, 'system', 'screen_off_timeout'),
        'sleep_timeout': _setting_number(address, 'secure', 'sleep_timeout'),
    }
    commands = (
        ('global', 'stay_on_while_plugged_in', '3' if stay_awake else '0'),
        ('system', 'screen_off_timeout', str(screen_off_timeout)),
        ('secure', 'sleep_timeout', str(sleep_timeout)),
    )
    for namespace, name, value in commands:
        ok, output = _adb_shell(address, 'settings', 'put', namespace, name, value)
        if not ok:
            return False, output or f'Could not set {name}.', None
    return True, 'Device power settings saved.', previous


def headless_power_settings() -> tuple[bool, str, dict | None]:
    """Apply the safe headless preset: stay awake on power plus max timeouts."""
    return set_device_power_settings(
        stay_awake=True,
        screen_off_timeout=_NEVER_TIMEOUT_MS,
        sleep_timeout=_NEVER_TIMEOUT_MS,
    )


def restore_device_power_settings(previous: dict) -> tuple[bool, str]:
    """Restore the exact settings snapshot saved before a headless preset."""
    if not isinstance(previous, dict):
        return False, 'No saved device settings are available to restore.'
    mapping = (
        ('global', 'stay_on_while_plugged_in', previous.get('stay_on_while_powered')),
        ('system', 'screen_off_timeout', previous.get('screen_off_timeout')),
        ('secure', 'sleep_timeout', previous.get('sleep_timeout')),
    )
    connected, message, address = _device_connected()
    if not connected:
        return False, message
    for namespace, name, value in mapping:
        if value is not None and (not isinstance(value, int) or value < 0 or value > _NEVER_TIMEOUT_MS):
            return False, 'Saved device settings are invalid.'
        command = ('settings', 'delete', namespace, name) if value is None else (
            'settings', 'put', namespace, name, str(value))
        ok, output = _adb_shell(address, *command)
        if not ok:
            return False, output or f'Could not restore {name}.'
    return True, 'Previous device power settings restored.'


def _dvr_activity_channel_numbers(timeout: int = _DVR_POLL_TIMEOUT) -> set[str] | None:
    """The set of channel numbers Channels DVR currently reports as being watched
    (its `/dvr` status endpoint's `activity` dict, values shaped like
    "Watching ch<N> from <client> (...): ..." — the same undocumented-but-stable
    endpoint third-party monitors like ChannelWatch rely on, and what the old Kodi
    bridge's dvr_encoder_active() used).

    Returns None when this can't be determined (channels_dvr_url not configured, or
    the DVR didn't respond) — never treated as "nothing's active" by callers, since a
    momentary DVR hiccup must never look like an idle stream.
    """
    dvr_url = (AppSettings.get().effective_channels_dvr_url() or '').strip().rstrip('/')
    if not dvr_url:
        return None
    try:
        resp = requests.get(f'{dvr_url}/dvr', timeout=timeout)
        resp.raise_for_status()
        activity = resp.json().get('activity') or {}
    except Exception as e:
        logger.warning('[fc-player] DVR activity check failed: %s', e)
        return None
    numbers = set()
    for value in activity.values():
        m = re.match(r'Watching ch(\S+) ', str(value))
        if m:
            numbers.add(m.group(1))
    return numbers


def _dvr_guide_numbers_for_channel(channel_key: str, timeout: int = _DVR_POLL_TIMEOUT) -> set[str] | None:
    """Every DVR guide number, across all of Channels DVR's configured sources, whose
    channel `ID` matches this channel — found by scanning the `/devices` endpoint for
    an ID shaped like our own generated M3U `channel-id` attribute
    (`f'{source_name}.{source_channel_id}'`, from _tvg_id() in app/generators/m3u.py —
    confirmed live 2026-08-25 that Channels DVR echoes this back verbatim as each
    channel's `ID` field when it imports a custom M3U source).

    Superseded a before/after DVR-activity diff approach (see git history) that
    assumed a trigger always precedes DVR marking the channel "watching" — confirmed
    live 2026-08-25, over a real ~2-hour Channels-DVR-driven viewing session, that
    this assumption doesn't hold: DVR appears to mark a channel active as soon as the
    client's tune request comes in, at or before it calls our own play route, so a
    "what's new since the trigger" diff can permanently see nothing new. Looking up
    the guide number(s) directly sidesteps this timing question entirely — it doesn't
    matter when DVR marked the channel active, only whether it's active now.

    Returns an empty set (not None) when DVR was reachable but nothing matched — e.g.
    the channel was never pushed to any DVR source, or was added some other way that
    didn't preserve our channel-id format; idle-stop then relies solely on the /watch
    heartbeat for this channel. Returns None only when DVR itself couldn't be reached.
    """
    dvr_url = (AppSettings.get().effective_channels_dvr_url() or '').strip().rstrip('/')
    if not dvr_url or ':' not in channel_key:
        return None
    source_name, source_channel_id = channel_key.split(':', 1)
    target_id = f'{source_name}.{source_channel_id}'
    try:
        resp = requests.get(f'{dvr_url}/devices', timeout=timeout)
        resp.raise_for_status()
        devices = resp.json() or []
    except Exception as e:
        logger.warning('[fc-player] DVR /devices lookup failed: %s', e)
        return None
    numbers = set()
    for device in devices:
        for ch in (device.get('Channels') or []):
            if ch.get('ID') == target_id and ch.get('GuideNumber'):
                numbers.add(str(ch['GuideNumber']))
    return numbers


def note_trigger(channel_key: str) -> None:
    """Called at the start of trigger_channel() when idle-stop is enabled: just marks
    that something was triggered and which channel it was. The (heavier) DVR guide-
    number lookup happens later, off the critical path, on the watchdog's own tick —
    see check_idle_and_stop() — so this stays fast enough to never delay the actual
    adb trigger.

    Best-effort and non-blocking: any failure here must never delay or break the
    actual adb trigger, which stays the speed-first critical path.
    """
    try:
        r = _redis()
        r.setex(_BELIEVED_ACTIVE_KEY, _BELIEVED_ACTIVE_TTL_S, '1')
        r.setex(_CHANNEL_KEY_KEY, _BELIEVED_ACTIVE_TTL_S, channel_key)
        r.delete(_TRACKED_CHANNELS_KEY)
        r.delete(_DVR_LOOKUP_COOLDOWN_KEY)
        r.delete(_IDLE_SINCE_KEY)
    except Exception as e:
        logger.warning('[fc-player] note_trigger failed (idle-stop tracking skipped): %s', e)


def note_web_heartbeat(channel_key: str) -> None:
    """Called by the /watch page's periodic ping while it's actively displaying a
    channel that fell through to the fc-player bridge path — an independent "still in
    use" signal alongside DVR activity, since Channels DVR has zero visibility into
    someone watching through our own browser page instead."""
    try:
        _redis().setex(_WEB_HEARTBEAT_PREFIX + channel_key, _WEB_HEARTBEAT_TTL_S, '1')
    except Exception as e:
        logger.warning('[fc-player] note_web_heartbeat failed: %s', e)


def _recent_web_heartbeat(channel_key: str) -> bool:
    try:
        return bool(_redis().exists(_WEB_HEARTBEAT_PREFIX + channel_key))
    except Exception as e:
        logger.warning('[fc-player] web heartbeat check failed: %s', e)
        return False


def _stop_playback() -> bool:
    """Force-stops the player app entirely — blunt, but already proven reliable this
    session for clearing stale DRM sessions. No JSON-RPC-style control channel exists
    to ask it to stop gracefully (unlike the old Kodi bridge's Player.Stop)."""
    try:
        address = _adb_address()
    except FcPlayerNotConfigured:
        return False
    try:
        result = subprocess.run(
            ['adb', '-s', address, 'shell', 'am', 'force-stop', 'com.fastchannels.player'],
            capture_output=True, timeout=_ADB_TIMEOUT, check=False, text=True,
        )
    except Exception as e:
        logger.warning('[fc-player] idle-stop: force-stop failed: %s', e)
        return False
    return result.returncode == 0


def check_idle_and_stop() -> None:
    """Watchdog tick (app.worker's scheduled job). Resolves the triggered channel to
    its DVR guide number(s) via _dvr_guide_numbers_for_channel() (once, then cached),
    and treats it as "in use" if either that guide number shows up in DVR's live
    activity, or a recent /watch heartbeat exists for the same channel.
    """
    if not idle_stop_enabled():
        return
    try:
        r = _redis()
        if not r.exists(_BELIEVED_ACTIVE_KEY):
            return

        channel_key = (r.get(_CHANNEL_KEY_KEY) or b'').decode() or None
        tracked_raw = r.get(_TRACKED_CHANNELS_KEY)
        tracked = set(json.loads(tracked_raw)) if tracked_raw is not None else None

        if tracked is None and channel_key and not r.exists(_DVR_LOOKUP_COOLDOWN_KEY):
            numbers = _dvr_guide_numbers_for_channel(channel_key)
            if numbers is not None:
                # Only cool down after a real answer (even an empty one) — a transient
                # DVR outage shouldn't block retrying again on the very next tick.
                r.setex(_DVR_LOOKUP_COOLDOWN_KEY, _DVR_LOOKUP_RETRY_S, '1')
                if numbers:
                    # Cache a real result for the rest of this trigger's lifetime — the
                    # mapping won't change mid-session.
                    tracked = numbers
                    r.setex(_TRACKED_CHANNELS_KEY, _BELIEVED_ACTIVE_TTL_S, json.dumps(sorted(numbers)))
                    logger.info('[fc-player] idle-stop: resolved DVR guide number(s) %s for %s',
                                sorted(numbers), channel_key)
                else:
                    # Don't cache "nothing found" — leave _TRACKED_CHANNELS_KEY unset so
                    # this stays retryable (on the cooldown above) rather than a
                    # permanent dead end, in case the channel gets pushed to a DVR
                    # source later in the same viewing session.
                    logger.info('[fc-player] idle-stop: no DVR guide number found for %s '
                                '(relying on /watch heartbeat only for now)', channel_key)

        active = False
        if tracked:
            now_numbers = _dvr_activity_channel_numbers()
            if now_numbers is not None and (tracked & now_numbers):
                active = True
        if not active and channel_key and _recent_web_heartbeat(channel_key):
            active = True

        if active:
            r.delete(_IDLE_SINCE_KEY)
            return

        idle_since_raw = r.get(_IDLE_SINCE_KEY)
        now = time.time()
        if idle_since_raw is None:
            r.set(_IDLE_SINCE_KEY, str(now))
            return
        idle_since = float(idle_since_raw)
        if now - idle_since >= _IDLE_GRACE_S:
            logger.info('[fc-player] idle-stop: %s idle >= %ss, stopping playback', channel_key, _IDLE_GRACE_S)
            _stop_playback()
            r.delete(_BELIEVED_ACTIVE_KEY)
            r.delete(_CHANNEL_KEY_KEY)
            r.delete(_TRACKED_CHANNELS_KEY)
            r.delete(_DVR_LOOKUP_COOLDOWN_KEY)
            r.delete(_IDLE_SINCE_KEY)
    except Exception as e:
        logger.warning('[fc-player] idle-stop watchdog tick failed: %s', e)


def trigger_channel(manifest_url: str, license_url: str | None = None, *, name: str = 'FastChannels',
                     channel_key: str | None = None, adb_address: str | None = None) -> bool:
    """Tell the FastChannels Player app to start playing this stream right now.

    manifest_url should already be the fully-resolved play URL (same shape
    _get_playback_info() hands to the watch page — the DRM dash.mpd proxy route for DRM
    channels, the generic play-proxy .m3u8 URL otherwise). Returns True if adb
    acknowledged the am start call — not proof playback actually started; there is no
    separate confirm_playback() here since there's no remote-control channel to poll.

    channel_key (f'{source_name}:{channel_id}') is only used when the idle-stop
    watchdog is enabled — see note_trigger().

    adb_address overrides the single configured device for this one trigger — ah4c
    supplies it (per allocated tuner) when it's fronting more than one streaming stick.
    When set, idle-stop tracking is skipped: it's single-device global state, and the
    ah4c stop script already force-stops each stick on its own disconnect.
    """
    address = adb_address or _adb_address()
    drm = bool(license_url)

    if channel_key and adb_address is None and idle_stop_enabled():
        note_trigger(channel_key)

    try:
        subprocess.run(
            ['adb', 'connect', address],
            capture_output=True, timeout=_ADB_TIMEOUT, check=False,
        )
        # `adb shell` reconstructs everything after "shell" into a single string that gets
        # handed to the DEVICE's own shell for interpretation — passing each arg as a separate
        # Python list element does NOT preserve argv boundaries the way a local subprocess.run
        # call would. Confirmed live 2026-08-24: an unescaped "$100000 Pyramid" title lost its
        # "$1" to the remote shell's own variable expansion and the trailing word bled into the
        # next flag, corrupting --ez drm too. Build one explicitly shell-quoted command string
        # instead so the remote shell sees exactly the literal values intended.
        remote_cmd = ' '.join([
            'am', 'start',
            '-n', shlex.quote(_PLAYER_COMPONENT),
            '--es', 'stream_url', shlex.quote(manifest_url),
            '--es', 'title', shlex.quote(name),
            '--ez', 'drm', 'true' if drm else 'false',
            '--ez', 'captions', 'true' if captions_enabled() else 'false',
        ])
        if drm:
            remote_cmd += ' --es license_url ' + shlex.quote(license_url)
        result = subprocess.run(
            ['adb', '-s', address, 'shell', remote_cmd],
            capture_output=True, timeout=_ADB_TIMEOUT, check=False, text=True,
        )
    except Exception as e:
        logger.warning('[fc-player] trigger_channel adb call failed: %s', e)
        return False

    ok = result.returncode == 0 and 'Error' not in (result.stdout or '') and 'Exception' not in (result.stdout or '')
    if not ok:
        logger.warning('[fc-player] trigger_channel am start failed: rc=%s stdout=%s stderr=%s',
                        result.returncode, (result.stdout or '').strip(), (result.stderr or '').strip())
    return ok

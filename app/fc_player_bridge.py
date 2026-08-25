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

# How long a bridged channel can sit with no confirmed viewer (neither Channels DVR
# activity nor a /watch heartbeat) before we stop it. Matches the old Kodi bridge's
# _ENCODER_IDLE_GRACE_S, which borrowed the same window from a mature third-party
# Channels DVR monitor's own idle-detection grace period.
_IDLE_GRACE_S = 5 * 60

# How long to keep retrying to correlate a fresh trigger to a DVR activity session
# before giving up on idle-tracking that particular trigger. Giving up just means we
# never learn this trigger is idle — the stream keeps running — not a wrong stop.
_CORRELATE_GIVEUP_S = 2 * 60

_DVR_POLL_TIMEOUT = 5

# All Redis keys below exist only while idle-stop is enabled and something has been
# triggered; a disabled toggle means trigger_channel() never touches Redis at all for
# any of this, so there's zero added cost for installs that don't opt in.
_BELIEVED_ACTIVE_KEY = 'fc:fc-player:believed-active'
_BELIEVED_ACTIVE_TTL_S = 24 * 60 * 60
_CHANNEL_KEY_KEY = 'fc:fc-player:channel-key'
_PRE_TRIGGER_ACTIVITY_KEY = 'fc:fc-player:pre-trigger-activity'
_PRE_TRIGGER_ACTIVITY_TTL_S = _CORRELATE_GIVEUP_S + 30
_TRACKED_CHANNEL_KEY = 'fc:fc-player:tracked-channel'
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


def _capture_card_channel_number() -> str | None:
    """The channel number embedded in fc_player_bridge_encoder_url's
    /channels/<N>/ segment — the capture card's own internal pass-through channel,
    not something any client actually tunes to. See its one caller for why this needs
    excluding from the trigger-correlation diff."""
    encoder_url = AppSettings.get().effective_fc_player_bridge_encoder_url() or ''
    m = re.search(r'/channels/([^/]+)/', encoder_url)
    return m.group(1) if m else None


def note_trigger(channel_key: str) -> None:
    """Called at the start of trigger_channel() when idle-stop is enabled: snapshots
    which DVR channel numbers are already active *before* this trigger, so the
    watchdog can later diff against a follow-up snapshot to find the one this trigger
    caused (Channels DVR assigns its own guide number per channel on import — not the
    fixed number in fc_player_bridge_encoder_url — so there's no fixed number to match
    against up front; confirmed live 2026-08-25 against a real multi-channel custom
    M3U source).

    Best-effort and non-blocking: any failure here must never delay or break the
    actual adb trigger, which stays the speed-first critical path.
    """
    try:
        numbers = _dvr_activity_channel_numbers()
        r = _redis()
        r.setex(_BELIEVED_ACTIVE_KEY, _BELIEVED_ACTIVE_TTL_S, '1')
        r.setex(_CHANNEL_KEY_KEY, _BELIEVED_ACTIVE_TTL_S, channel_key)
        r.delete(_TRACKED_CHANNEL_KEY)
        r.delete(_IDLE_SINCE_KEY)
        if numbers is not None:
            r.setex(_PRE_TRIGGER_ACTIVITY_KEY, _PRE_TRIGGER_ACTIVITY_TTL_S, json.dumps(sorted(numbers)))
        else:
            r.delete(_PRE_TRIGGER_ACTIVITY_KEY)
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
    """Watchdog tick (app.worker's scheduled job). See note_trigger()'s docstring for
    why this can't just match a fixed channel number up front — it instead correlates
    a trigger to whichever DVR channel number newly appeared afterward, then tracks
    that specific number (or a /watch heartbeat for the same channel) going idle.
    """
    if not idle_stop_enabled():
        return
    try:
        r = _redis()
        if not r.exists(_BELIEVED_ACTIVE_KEY):
            return

        channel_key = (r.get(_CHANNEL_KEY_KEY) or b'').decode() or None
        tracked = (r.get(_TRACKED_CHANNEL_KEY) or b'').decode() or None

        if not tracked:
            pre_raw = r.get(_PRE_TRIGGER_ACTIVITY_KEY)
            if pre_raw is None:
                # Either already gave up correlating this trigger, or note_trigger()
                # couldn't reach the DVR at all — nothing left to do for it.
                return
            pre = set(json.loads(pre_raw))
            now_numbers = _dvr_activity_channel_numbers()
            if now_numbers is None:
                return  # DVR unreachable this tick; try again next tick
            new_numbers = now_numbers - pre
            # The capture card's own pass-through channel (the number embedded in
            # fc_player_bridge_encoder_url, when that URL has a parseable
            # /channels/<N>/ segment) briefly shows as separately "active" alongside
            # the real client-facing channel right when a tune starts — confirmed live
            # 2026-08-25 (both ch70108 and ch70092 showed up in the same activity
            # snapshot for one tick, settling to just ch70108 shortly after). Exclude
            # it when we can identify it, but don't rely on that alone — the encoder
            # URL doesn't always have that shape (confirmed live the same day: a
            # .../channels.m3u playlist URL with no specific channel segment at all).
            capture_ch = _capture_card_channel_number()
            if capture_ch:
                new_numbers.discard(capture_ch)
            if len(new_numbers) == 1:
                tracked = next(iter(new_numbers))
                r.setex(_TRACKED_CHANNEL_KEY, _BELIEVED_ACTIVE_TTL_S, tracked)
                r.delete(_PRE_TRIGGER_ACTIVITY_KEY)
                logger.info('[fc-player] idle-stop: correlated trigger to DVR ch%s', tracked)
            elif len(new_numbers) > 1:
                # Don't give up immediately — this same transient double-registration
                # (not just the known capture-card case above) tends to settle to a
                # single channel within a tick or two. Keep the original pre-trigger
                # snapshot and retry; _PRE_TRIGGER_ACTIVITY_KEY's own TTL bounds how
                # long we keep at it before truly giving up.
                logger.info('[fc-player] idle-stop: ambiguous DVR activity diff (%d new channels), retrying next tick', len(new_numbers))
            # else: zero new channels so far — same TTL-bounded retry.
            return

        active = False
        now_numbers = _dvr_activity_channel_numbers()
        if now_numbers is not None and tracked in now_numbers:
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
            logger.info('[fc-player] idle-stop: ch%s idle >= %ss, stopping playback', tracked, _IDLE_GRACE_S)
            _stop_playback()
            r.delete(_BELIEVED_ACTIVE_KEY)
            r.delete(_CHANNEL_KEY_KEY)
            r.delete(_TRACKED_CHANNEL_KEY)
            r.delete(_IDLE_SINCE_KEY)
    except Exception as e:
        logger.warning('[fc-player] idle-stop watchdog tick failed: %s', e)


def trigger_channel(manifest_url: str, license_url: str | None = None, *, name: str = 'FastChannels',
                     channel_key: str | None = None) -> bool:
    """Tell the FastChannels Player app to start playing this stream right now.

    manifest_url should already be the fully-resolved play URL (same shape
    _get_playback_info() hands to the watch page — the DRM dash.mpd proxy route for DRM
    channels, the generic play-proxy .m3u8 URL otherwise). Returns True if adb
    acknowledged the am start call — not proof playback actually started; there is no
    separate confirm_playback() here since there's no remote-control channel to poll.

    channel_key (f'{source_name}:{channel_id}') is only used when the idle-stop
    watchdog is enabled — see note_trigger().
    """
    address = _adb_address()
    drm = bool(license_url)

    if channel_key and idle_stop_enabled():
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

"""FastChannels Player remote-play trigger — adb orchestration.

FastChannels' analog of kodi_bridge.trigger_channel(), for the FastChannels-owned
player app (app/fc_player/) — a single-Activity Media3/ExoPlayer app with no UI of
its own to speak of; everything is driven from here over adb. There is no
StreamVault (or any other third-party app) involved: an earlier design routed
playback through StreamVault-IPTV's own UI, but once it was clear this device needs
no on-device browsing at all, that whole integration (and the fragility that came
with it) was dropped in favor of this much smaller app.

Android has no in-process way to launch this: a plain context.startActivity() from
a background service is blocked by the background-activity-start restriction —
confirmed live 2026-08-24 — and the standard workaround (a full-screen-intent
notification) is ALSO blocked on this device by Fire OS's own notification allowlist
("AmazonNotificationService: Package ... is not in allow list"). adb-shell-privileged
launches are exempt from both restrictions — the same reason kodi_bridge.py's
wake_and_relaunch() uses `adb shell am start -n org.xbmc.kodi/.Splash` rather than any
in-process API — so this triggers playback the same way: a plain
`adb shell am start -n <package>/.PlaybackActivity` with simple string/boolean extras
(no Serializable involved — PlaybackActivity reads plain Intent extras).

Live-validated 2026-08-24: real secure Widevine DASH decode (MediaTek secure hardware
decoder) via a single adb am start call, ~2s from call to first rendered frame.
"""
import logging
import shlex
import subprocess

from .models import AppSettings

logger = logging.getLogger(__name__)

_ADB_TIMEOUT = 10
# Fixed — this is our own player app's package (app/fc_player/), not a setting; it never
# varies per install.
_PLAYER_COMPONENT = 'com.fastchannels.player/.PlaybackActivity'


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


def trigger_channel(manifest_url: str, license_url: str | None = None, *, name: str = 'FastChannels') -> bool:
    """Tell the FastChannels Player app to start playing this stream right now.

    manifest_url should already be the fully-resolved play URL (same shape
    _get_playback_info() hands to the watch page / kodi_bridge — the DRM dash.mpd proxy
    route for DRM channels, the generic play-proxy .m3u8 URL otherwise). Returns True if
    adb acknowledged the am start call — not proof playback actually started; there is no
    separate confirm_playback() here since there's no remote-control channel to poll,
    unlike Kodi's JSON-RPC Player.GetActivePlayers.
    """
    address = _adb_address()
    drm = bool(license_url)

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

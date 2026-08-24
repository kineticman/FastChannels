"""FastChannels Bridge background service.

Runs continuously once Kodi starts (registered as a second extension point in
this same addon, alongside the request/reply resolver in default.py).
Complements app/kodi_bridge.py's run_instanceguard_watch (which tails adb
logcat on the FastChannels side and can only react to two specific known
failure signatures matched against raw log text) with two faster,
event-driven signals, both POSTing the same heartbeat straight back to
FastChannels:

1. `onPlaybackError` — Kodi's own player engine giving up. Definitive, but
   Kodi doesn't declare this until its own internal stall timeout (~15s of
   AddPacketsRenderer/OutputPicture timeouts) elapses, so this can't be any
   faster than that ceiling.
2. A local stall watchdog polling `xbmc.Player().getTime()` directly —
   in-process, no adb/JSON-RPC/network round-trip needed at all, unlike
   run_instanceguard_watch's external periodic poll (app/kodi_bridge.py's
   _force_reopen_if_stalled, called via a container->adb->device hop). Since
   it's just a local Python attribute read, it can sample far more often and
   confirm a real stall (position genuinely not advancing, not paused) in
   roughly _STALL_POLL_S * (_STALL_STREAK_THRESHOLD - 1) seconds — single
   digits, well under both the external poll's interval and Kodi's own
   onPlaybackError ceiling. Added 2026-08-24 after a live stall (no
   InstanceGuard/non-secure-codec log line, no onPlaybackError either) took
   ~15-20s to recover because the external poll was the only thing watching.

Self-configuring: derives FastChannels' host from whatever URL is currently
playing rather than needing a separate addon settings screen. The playing
file is either the bare resolved manifest URL (e.g.
http://<fc-host>/play/.../dash.mpd) or, if Kodi still reports the outer
plugin:// invocation used to open it, a URL with an embedded
`url=<urlencoded manifest>` query param (see default.py) — both forms are
hosted on FastChannels itself, so either one yields the right host.
"""
import json
import re
import threading
import time
import urllib.request
from urllib.parse import parse_qs, unquote, urlparse

import xbmc

_HEARTBEAT_PATH = '/play/kodi-bridge/heartbeat'
_HEARTBEAT_TIMEOUT_S = 3
_HEARTBEAT_DEBOUNCE_S = 5  # local debounce; FastChannels also has its own short cooldown
_STALL_POLL_S = 2
_STALL_STREAK_THRESHOLD = 3  # ~(threshold-1)*_STALL_POLL_S seconds of confirmed no movement

_URL_RE = re.compile(r'https?://[^\s&]+')


def _fastchannels_heartbeat_url(playing_file: str | None) -> str | None:
    if not playing_file:
        return None
    candidate = playing_file
    if playing_file.startswith('plugin://'):
        embedded = parse_qs(urlparse(playing_file).query).get('url', [None])[0]
        if not embedded:
            return None
        candidate = unquote(embedded)
    m = _URL_RE.match(candidate)
    if not m:
        return None
    parsed = urlparse(m.group(0))
    if not parsed.scheme or not parsed.netloc:
        return None
    return f'{parsed.scheme}://{parsed.netloc}{_HEARTBEAT_PATH}'


def _send_heartbeat(url: str) -> None:
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps({'event': 'playback_error'}).encode('utf-8'),
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        urllib.request.urlopen(req, timeout=_HEARTBEAT_TIMEOUT_S)
    except Exception as e:
        xbmc.log(f'[fc_bridge service] heartbeat POST to {url} failed: {e}', xbmc.LOGWARNING)


class _MonitorPlayer(xbmc.Player):
    """Kodi's Player callbacks are system-wide, not scoped to whichever addon
    started playback — this fires for content the resolver plugin (default.py,
    a separate invocation) opened, same as any other add-on subclassing
    xbmc.Player would see."""

    def __init__(self):
        super().__init__()
        self._last_file: str | None = None
        self._last_sent = 0.0
        self._playing = False
        self._paused = False
        self._last_time_value = None
        self._stall_streak = 0

    def onAVStarted(self) -> None:
        self._playing = True
        self._paused = False
        self._last_time_value = None
        self._stall_streak = 0
        try:
            self._last_file = self.getPlayingFile()
        except Exception:
            pass

    def onPlayBackPaused(self) -> None:
        self._paused = True

    def onPlayBackResumed(self) -> None:
        self._paused = False
        self._last_time_value = None
        self._stall_streak = 0

    def onPlayBackStopped(self) -> None:
        self._playing = False

    def onPlayBackEnded(self) -> None:
        self._playing = False

    def onPlaybackError(self) -> None:
        self._fire_heartbeat('onPlaybackError')

    def check_stall(self) -> None:
        """Called every _STALL_POLL_S by the service loop below. In-process
        equivalent of app/kodi_bridge.py's _force_reopen_if_stalled, but
        local (no adb/JSON-RPC hop) so it can sample far more often — see
        module docstring for why this exists alongside onPlaybackError."""
        if not self._playing or self._paused:
            self._stall_streak = 0
            self._last_time_value = None
            return
        try:
            if not self.isPlaying():
                return
            current = self.getTime()
        except Exception:
            return
        if self._last_time_value is not None and current == self._last_time_value:
            self._stall_streak += 1
        else:
            self._stall_streak = 0
        self._last_time_value = current
        if self._stall_streak >= _STALL_STREAK_THRESHOLD:
            self._stall_streak = 0  # re-accumulate before firing again
            self._fire_heartbeat('stall watchdog')

    def _fire_heartbeat(self, trigger: str) -> None:
        now = time.monotonic()
        if now - self._last_sent < _HEARTBEAT_DEBOUNCE_S:
            return
        playing_file = self._last_file
        if not playing_file:
            try:
                playing_file = self.getPlayingFile()
            except Exception:
                playing_file = None
        url = _fastchannels_heartbeat_url(playing_file)
        if not url:
            xbmc.log(
                f'[fc_bridge service] {trigger} fired but no FastChannels '
                f'host could be determined from {playing_file!r}, skipping heartbeat',
                xbmc.LOGWARNING,
            )
            return
        self._last_sent = now
        xbmc.log(f'[fc_bridge service] {trigger} -> heartbeat {url}', xbmc.LOGWARNING)
        threading.Thread(target=_send_heartbeat, args=(url,), daemon=True).start()


def run() -> None:
    player = _MonitorPlayer()
    monitor = xbmc.Monitor()
    while not monitor.abortRequested():
        player.check_stall()
        if monitor.waitForAbort(_STALL_POLL_S):
            break


if __name__ == '__main__':
    run()

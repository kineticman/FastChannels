"""Shared DRM-bridge eligibility logic — which sources can be kept active/bridged
rather than disabled when a channel needs DRM decryption, and which of them are
trusted to actually decrypt through an adb-triggered device bridge (as opposed to the
PrismCast browser/EME bridge, which works for a broader set of DRM sources).

Split out of the old Kodi HDMI-encoder bridge module (removed 2026-08-25, see
dev/kodi/removed-from-app/) — both symbols here are load-bearing for FastChannels
Player (app/fc_player_bridge.py) and PrismCast, not Kodi-specific despite originating
there.
"""
from .models import AppSettings

# Sources confirmed (dev/kodi/README.md; also re-validated live against FastChannels
# Player 2026-08-25) to actually decrypt through an adb-triggered device bridge. Cox and
# Warner TVE hit a confirmed, non-fixable inputstream.adaptive same-KID session-splitting
# wall and stay excluded.
#
# Re-tested 2026-08-28: hypothesized this was Kodi/inputstream.adaptive-specific (it
# opens a second CDM session per stream even on shared default_KID) and might not
# reproduce under FastChannels Player's Media3/ExoPlayer + platform MediaDrm stack.
# It does reproduce — confirmed live against two channels (A&E, CNN) via real
# fc_player_bridge.trigger_channel() on the Fire TV Stick: Media3 opens a single CDM
# session (no splitting), but every cox-mds license POST still returns HTTP 403 even
# after the server-side session-refresh retry (app/routes/play.py) exhausts its
# attempts, ending in DefaultDrmSession/MediaDrmCallbackException. So the 403 isn't
# session-count-dependent — it's Cox-side rejection of this device/out-of-home
# context regardless of client stack. Stays excluded.
# Shared by app/routes/play.py, app/routes/admin.py, app/routes/api_dvr.py,
# app/generators/m3u.py, and app/worker.py — keep them all in sync via this single
# source of truth.
DRM_BRIDGE_TRUSTED_SOURCES = frozenset({
    'sling', 'nbc_tve', 'pbs', 'amazon_prime_free', 'directv', 'vidaa', 'philo', 'roku',
    'fubo',
})


def drm_bridge_mode_for(source_name: str) -> bool:
    """Whether a DRM channel on this source should be kept active + bridged rather than
    disabled — true if ANY bridge can plausibly serve it: the PrismCast browser/EME
    bridge (global drm_bridge_enabled toggle, any license_url-capable source), or the
    FastChannels Player bridge (app/fc_player_bridge.py), gated on its own enabled
    toggle and restricted to DRM_BRIDGE_TRUSTED_SOURCES. Callers still separately gate
    on the scraper actually having license_url handling."""
    settings = AppSettings.get()
    if bool(settings.drm_bridge_enabled):
        return True
    if source_name not in DRM_BRIDGE_TRUSTED_SOURCES:
        return False
    return bool(settings.fc_player_bridge_enabled)


def active_bridge_label(source_name: str) -> str:
    """Human-readable name(s) of the bridge(s) actually available for this source right
    now, for log/report messages — so "DRM→bridge" lines don't say "PrismCast" when the
    channel is really only reachable (or also reachable) via FastChannels Player, or
    vice versa. Callers should only call this once drm_bridge_mode_for() is already True;
    it returns 'bridge' as a neutral fallback if neither is actually on (shouldn't happen
    in practice, but a label bug here must never look like a bridging decision)."""
    settings = AppSettings.get()
    prismcast = bool(settings.drm_bridge_enabled)
    fc_player = source_name in DRM_BRIDGE_TRUSTED_SOURCES and bool(settings.fc_player_bridge_enabled)
    if prismcast and fc_player:
        return 'PrismCast + FastChannels Player'
    if fc_player:
        return 'FastChannels Player'
    if prismcast:
        return 'PrismCast'
    return 'bridge'

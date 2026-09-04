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
# Player 2026-08-25) to actually decrypt through an adb-triggered device bridge.
#
# Cox and Warner TVE are excluded — confirmed root cause (2026-08-28, full trail in
# dev/comcast/COX_FC_PLAYER_BRIDGE_INVESTIGATION.md): Cox's TVE license server rejects
# every native-Android Widevine client by app identity, not session count, privacy
# mode, security level, or device/platform class. Ruled out each of those in turn via
# live testing (Fire TV Stick and a genuinely Cox-supported Galaxy phone both failed
# identically). Chrome/Shaka works because it's a Cox-recognized browser client, not
# because of anything about "browser" vs "Android" as a platform per se — the wall is
# specifically "not Cox's own signed, registered Contour app." Not fixable from a
# native DRM client on our side; only a screen-capture-style bridge (PrismCast
# running real Chrome) could ever reach Cox TVE outside the browser.
# Shared by app/routes/play.py, app/routes/admin.py, app/routes/api_dvr.py,
# app/generators/m3u.py, and app/worker.py — keep them all in sync via this single
# source of truth.
DRM_BRIDGE_TRUSTED_SOURCES = frozenset({
    'sling', 'nbc_tve', 'pbs', 'amazon_prime_free', 'directv', 'vidaa', 'philo', 'roku',
    'fubo',
})


def drm_bridge_mode_for(source_name: str) -> bool:
    """Whether a DRM channel on this source should be kept active + bridged rather than
    disabled — true only when the global Bridge policy is on and at least one enabled
    method can serve it: PrismCast works for any license_url-capable source, while
    FastChannels Player is restricted to DRM_BRIDGE_TRUSTED_SOURCES. Callers still
    separately gate on the scraper actually having license_url handling."""
    settings = AppSettings.get()
    if not bool(settings.bridge_enabled):
        return False
    if settings.prismcast_enabled:
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
    prismcast = settings.prismcast_bridge_active()
    fc_player = (bool(settings.bridge_enabled) and source_name in DRM_BRIDGE_TRUSTED_SOURCES
                 and bool(settings.fc_player_bridge_enabled))
    if prismcast and fc_player:
        return 'PrismCast + FastChannels Player'
    if fc_player:
        return 'FastChannels Player'
    if prismcast:
        return 'PrismCast'
    return 'bridge'

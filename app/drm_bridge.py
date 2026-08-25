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
# Shared by app/routes/play.py, app/routes/admin.py, app/routes/api.py,
# app/generators/m3u.py, and app/worker.py — keep them all in sync via this single
# source of truth.
DRM_BRIDGE_TRUSTED_SOURCES = frozenset({
    'sling', 'nbc_tve', 'pbs', 'amazon_prime_free', 'directv', 'vidaa', 'philo', 'roku',
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

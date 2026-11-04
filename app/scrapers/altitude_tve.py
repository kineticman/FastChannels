"""
altitude_tve.py — Altitude Sports (Denver / Rocky Mountain) with a TV provider sign-in.

altitudeplus.com is a ViewLift site; the flow is viewlift_tve.py's. Scoped
2026-10-27 from a signed-in HAR (dev/hitlist/viewlift_rsn_scoping_2026-10-27.md):

- Requestor and resource `Altitude`. One channel, guide from Gracenote 65596 (ALTSPHD).
- The account is email-only (/v2/identity/account), seen working in the HAR:
  identitySignInTve linked the Spectrum sign-in to it.
- Market: 403 contentGeoRestricted for an Ohio Spectrum account, so in-market
  providers only, like NESN. Altitude also sells its own subscription
  (Altitude+), which this source doesn't use.
"""
from __future__ import annotations

from .base import BaseScraper, ConfigField
from .viewlift_tve import ViewLiftTVEScraper

API_BASE = 'https://altitude.api.viewlift.com'


class AltitudeTVEScraper(ViewLiftTVEScraper, BaseScraper):
    """Altitude Sports via ViewLift + Adobe Pass v2 (Rocky Mountain providers only)."""

    source_name = 'altitude_tve'
    display_name = 'Altitude TVE'
    license_url = f'{API_BASE}/v1/license/widevine/acquire'

    key = 'altitude'
    brand = 'Altitude'
    site = 'altitude'
    live_page_url = 'https://www.altitudeplus.com/live-24x7'
    origin = 'https://www.altitudeplus.com'
    domain_name = 'altitudeplus.com'
    redirect_url = 'https://www.altitudeplus.com/checkout?isTveUser=Authenticated'
    requestor_id = 'Altitude'
    default_resource = 'Altitude'
    api_base = API_BASE
    scheme = 'altitude-tve://'
    guide_prefix = 'ALTITUDE'
    channels = {
        'de0a3981-03fa-4104-bf95-87735ebe4a8a': ('altitude', 'Altitude Sports'),
    }
    account_signup = 'email'
    otp_enabled = True
    market_note = 'Altitude Sports is only available to TV provider accounts in its Rocky Mountain market'

    config_schema = [
        ConfigField('email', 'Altitude account email', placeholder='you@example.com',
                    help_text='The free Altitude account the TV provider sign-in is linked to. '
                              'Any address works: Altitude doesn\'t check it.'),
    ]

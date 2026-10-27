"""
nesn_tve.py — NESN and NESN Plus with a TV provider sign-in.

watch.nesn.com is a ViewLift site; the flow is viewlift_tve.py's. Scoped
2026-10-21 from a signed-in HAR — dev/nesn/SCOPED.md. NESN-specific facts:

- Requestor `NESNGO`, resource `NESN`.
- The NESN account is email → code (identityAuthOtpInitiate); NESN never
  offered the email-only signup the later ViewLift sites use.
- Market: New England providers only (403 contentGeoRestricted elsewhere).
"""
from __future__ import annotations

from .base import BaseScraper, ConfigField
from .viewlift_tve import ViewLiftTVEScraper

SCHEME = 'nesn-tve://'
API_BASE = 'https://nesn.api.viewlift.com'


class NesnTVEScraper(ViewLiftTVEScraper, BaseScraper):
    """NESN and NESN Plus via ViewLift + Adobe Pass v2 (New England providers only)."""

    source_name = 'nesn_tve'
    display_name = 'NESN TVE'
    license_url = f'{API_BASE}/v1/license/widevine/acquire'

    key = 'nesn'
    brand = 'NESN'
    site = 'nesn'
    live_page_url = 'https://watch.nesn.com/live'
    origin = 'https://watch.nesn.com'
    domain_name = 'watch.nesn.com'
    redirect_url = 'https://watch.nesn.com/checkout?isTveUser=Authenticated'
    requestor_id = 'NESNGO'
    default_resource = 'NESN'
    api_base = API_BASE
    scheme = SCHEME
    guide_prefix = 'NESN'
    channels = {
        'nesn': ('nesn', 'NESN'),
        'nesn plus': ('nesn-plus', 'NESN+'),
    }
    account_signup = 'otp'
    market_note = 'NESN is only available to TV provider accounts in its New England market'

    # The NESN account is passwordless (an emailed code — see request_login_code
    # and the "Sign in to NESN" panel on the source). The login is kept in
    # config.nesn_account; the email field only labels which account it is.
    config_schema = [
        ConfigField('email', 'NESN account email', placeholder='you@example.com',
                    help_text='The free NESN account the TV provider sign-in is linked to '
                              '(passwordless — NESN emails you a code).'),
    ]

"""
myoutdoortv_tve.py — MyOutdoorTV's four 24/7 channels (Outdoor Sportsman Group)
with a TV provider sign-in.

app.myoutdoortv.com is a ViewLift site; the flow is viewlift_tve.py's. Scoped
2026-10-27 from a signed-in HAR (dev/hitlist/viewlift_rsn_scoping_2026-10-27.md):

- Requestor `MyOutdoorTV`, resources OC, SC, WFN (Outdoor Channel, Sportsman
  Channel, World Fishing Network): the TV provider has to carry one of them.
  Spectrum's Ohio package doesn't, so the HAR's sign-in got
  preauthorization_denied_by_mvpd. That's a package check, not a market lock.
- The four channels are Amagi playouts (guides on Amagi's CloudFront), on the
  same Amagi account as Game & Fish TV, which Free Networks plays from a
  public URL. These four have no public playout URL that could be found
  (not in any HAR or iptv-org), so they go through entitlement.
- The account is email-only (/v2/identity/account); the site has no emailed
  code (otp_enabled false), so an address that already has an account can't
  be reused. MyOutdoorTV also sells its own subscription, which this source
  doesn't use.
"""
from __future__ import annotations

from .base import BaseScraper, ConfigField
from .viewlift_tve import SHARED_PUBLIC_KEY_URL, ViewLiftTVEScraper

API_BASE = 'https://myoutdoortv.api.viewlift.com'


class MyOutdoorTVTVEScraper(ViewLiftTVEScraper, BaseScraper):
    """MyOutdoorTV's live channels via ViewLift + Adobe Pass v2."""

    source_name = 'myoutdoortv_tve'
    display_name = 'MyOutdoorTV TVE'
    license_url = f'{API_BASE}/v1/license/widevine/acquire'

    key = 'motv'
    brand = 'MyOutdoorTV'
    site = 'myoutdoortv'
    live_page_url = 'https://app.myoutdoortv.com/motv-channel'
    origin = 'https://app.myoutdoortv.com'
    domain_name = 'app.myoutdoortv.com'
    redirect_url = 'https://app.myoutdoortv.com/checkout?isTveUser=Authenticated'
    requestor_id = 'MyOutdoorTV'
    default_resource = 'OC'
    api_base = API_BASE
    # The page names the GCP mirror of the shared key; both serve the same file.
    public_key_url = SHARED_PUBLIC_KEY_URL
    scheme = 'motv-tve://'
    guide_prefix = 'MOTV'
    channel_category = 'Outdoors'
    channels = {
        '5bbe7ace-d18a-4a16-81ad-00b2e101cdf5': ('whitetail', 'MOTV WhitetailStream'),
        '59761bc3-b675-449a-acb1-ffe06bbe465a': ('seasons', 'MOTV Seasons'),
        'f3ecb756-e3e9-4599-a844-cc61897111bb': ('huntstream', 'MOTV HuntStream'),
        'f785f400-aebc-4adc-819a-a3e0cc2fe388': ('bass', 'The Bass Channel featuring MLF'),
    }
    account_signup = 'email'
    otp_enabled = False
    market_note = 'MyOutdoorTV is not available to your TV provider account in your area'

    config_schema = [
        ConfigField('email', 'MyOutdoorTV account email', placeholder='you@example.com',
                    help_text='The free MyOutdoorTV account the TV provider sign-in is linked to. '
                              'Any address works: MyOutdoorTV doesn\'t check it.'),
    ]

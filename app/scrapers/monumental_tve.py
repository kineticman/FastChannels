"""
monumental_tve.py — Monumental Sports Network and Monumental Sports Network 2
(Washington DC / Maryland / Virginia) with a TV provider sign-in.

monumentalsportsnetwork.com is a ViewLift site; the flow is viewlift_tve.py's.
Scoped 2026-10-27 from a signed-in HAR (dev/hitlist/viewlift_rsn_scoping_2026-10-27.md):

- Requestor and resource `CSNMidAtlantic` (from its Comcast SportsNet days).
- Its own API host and x-api-key, and its own public key for the account
  email (appcms.monumentalsportsnetwork.com) — the shared ViewLift one gets 500.
- The page's first channel is titled "LIVE NOW", so channels are matched by
  their ViewLift id. Guides are Gracenote stations 10271 (MNMT) and 58633 (MNMT2).
- Market: the Adobe profile's ZIP must be in the DMV market (403
  contentGeoRestricted with tveZip/deviceZip otherwise).
"""
from __future__ import annotations

from .base import BaseScraper, ConfigField
from .viewlift_tve import ViewLiftTVEScraper

API_BASE = 'https://api.monumentalsportsnetwork.com'


class MonumentalTVEScraper(ViewLiftTVEScraper, BaseScraper):
    """Monumental Sports Network 1 and 2 via ViewLift + Adobe Pass v2 (DMV providers only)."""

    source_name = 'monumental_tve'
    display_name = 'Monumental Sports TVE'
    license_url = f'{API_BASE}/v1/license/widevine/acquire'

    key = 'monumental'
    brand = 'Monumental'
    site = 'monumental-network'
    live_page_url = 'https://www.monumentalsportsnetwork.com/live-schedule'
    origin = 'https://www.monumentalsportsnetwork.com'
    domain_name = 'monumentalsportsnetwork.com'
    redirect_url = 'https://www.monumentalsportsnetwork.com/checkout?isTveUser=Authenticated'
    requestor_id = 'CSNMidAtlantic'
    default_resource = 'CSNMidAtlantic'
    api_base = API_BASE
    api_key = 'eDDgMPynXA8W2YeB5b0qe9KtnsuKP96W8aBWQCEF'
    public_key_url = 'https://appcms.monumentalsportsnetwork.com/certificates/public_key_prod.pem'
    scheme = 'monumental-tve://'
    guide_prefix = 'MONUMENTAL'
    channels = {
        '586fe783-6e1f-4492-90a6-b9bf7b5fb5c2': ('msn', 'Monumental Sports Network'),
        'c1a092ba-fe3c-4837-ba9b-d621d7d7fa1e': ('msn2', 'Monumental Sports Network 2'),
    }
    account_signup = 'email'
    otp_enabled = True
    market_note = ('Monumental Sports Network is only available to TV provider accounts in its '
                   'Washington DC / Maryland / Virginia market')

    config_schema = [
        ConfigField('email', 'Monumental account email', placeholder='you@example.com',
                    help_text='The free Monumental account the TV provider sign-in is linked to. '
                              'Any address works: Monumental doesn\'t check it.'),
    ]

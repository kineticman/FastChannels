"""
nbc_tve.py — NBCUniversal cable/digital networks via TV Everywhere: NBC News
NOW, NBC Sports Now, Bravo (East/West), Bravo Vault, Real Housewives Vault,
Telemundo Al Día, Telemundo Deportes Ahora, Noticias Telemundo Ahora, NBC
Universo (East/West).

DRM: Widevine CENC DASH via Adobe Pass Cox + NBCUniversal's own KeyOS-backed
drm-proxy. Same generic license-proxy bridge pattern as app/scrapers/
warner_tve.py and app/scrapers/directv.py. Full investigation: dev/nbc/
(HAR capture, reversed JS bundles, and a working standalone repro script).

Key facts established by probing (mostly NOT visible from a casual read of
the HAR — the capture truncated mid-file and the DRM secret is delivered
encrypted; the algorithm below was reconstructed from NBC's own JS and
verified byte-for-byte against a real captured license request):

- NBC.com uses **Adobe Pass API v2** (`sp.auth.adobe.com/api/v2/...`), a JSON
  REST flavor distinct from the legacy XML flow in app/tve/adobe_pass.py's
  AdobePassCoxClient. The underlying OAuth2 client-credentials exchange
  (`/o/client/register` + `/o/client/token`) and the actual Cox SAML login
  (`login.cox.com/api/v1/authn` + hidden-form POST) are identical between v1
  and v2, so this file reuses `_cox_saml_login()` from fox_tve.py for the
  login step and only implements the v2-specific session/profile/preauthorize
  calls itself.
- The v2 API's `software_statement` (needed to register a client) is a
  static JWT baked into NBC's own JS bundle — not fetched from any API. It
  lives in whichever `/generetic/generated/generetic.<hash>.js` bundle
  (referenced via `<link rel="preload" as="script">` on any nbc.com page)
  contains a `"v2":{"softwareStatement":"..."}` object literal. Bundle
  hashes change on every NBC deploy, so this is discovered dynamically
  (fetch the page, follow the preload links, regex the literal) and cached.
- Playback (manifest + Widevine license) is NOT server-side gated by Adobe
  Pass in practice — lemonade.nbc.com and drmproxy.digitalsvc.apps.nbcuni.com
  both accepted requests with no MVPD token at all in the captured HAR.
  Adobe Pass preauthorize is still run in resolve() to (a) confirm the
  configured Cox account actually works and (b) skip channels the account's
  package doesn't include — same validate-then-gate pattern as
  warner_tve.py's audit_resolve. A resourceId Adobe's preauthorize response
  doesn't even mention (observed for the two free/ad-supported NBC News
  NOW / NBC Sports Now resources) is treated as unrestricted rather than
  blocked, since Adobe silently omitting a resource is how it behaves for
  content that isn't MVPD-gated at all.
- The Widevine license proxy URL is *signed*, not a static constant:
    secretValue = f"{timestamp_ms}widevine"
    hash = HMAC-SHA256(drmProxySecret, secretValue).hexdigest()
    url  = f"{drmProxyUrl}/widevine?time={timestamp_ms}&hash={hash}&device=web&keyId={streamAccessName}"
  `drmProxySecret` itself never appears in plaintext on the wire — it ships
  as an AES-256-GCM blob (`"oc":"<base64>"` embedded in the server-rendered
  page state of any nbc.com page) where the 12-byte IV, the 32-byte AES-256
  key, AND the ciphertext all travel together in the SAME blob (last 4 bytes
  are a big-endian uint32 "compat version", must equal 1). That's integrity
  protection, not confidentiality — the client already holds every piece it
  needs to decrypt it, which is exactly what NBC's own JS does. Verified:
  decrypting a real captured "oc" value yielded a drmProxySecret whose HMAC
  reproduced the real `hash=` query param from a captured license request,
  byte-for-byte.
- Channel discovery + EPG come from ONE call: friendship.nbc.com/v3/graphql
  persisted query `componentsForPlaceholders_cached` (sha256Hash below) with
  a `componentConfigs` entry that base64-decodes to
  `{"type":"TvGuide","implementation":"liveGuideTvGuide","name":"","app":""}`
  (a fixed, non-user-specific constant). It returns a `Guide` component whose
  `schedules[]` covers every linear network nbc.com exposes on its `/live`
  grid — each with `channelId`, `streamAccessName` (the drm-proxy `keyId`
  AND the geolocation `channelName`), `resourceId` (the Adobe Pass resource
  to preauthorize), and a `stationId` (what `lemonade.nbc.com/v2/linear/
  <stationId>` needs to hand back a DASH manifest) — plus roughly 12h of
  forward program listings per channel. No separate EPG call needed.
- That guide grid mixes national feeds with regional/single-market ones: the
  "nbc" row is the caller's LOCAL NBC affiliate (WCMH/PK22 in the captured
  HAR), `necn` is New England Cable News, and four `rsn-*` rows are regional
  sports networks. This scraper keeps the local affiliate and NECN but skips
  the RSN rows (near-certain blackout restrictions outside their home
  market) — see _INCLUDED_BRANDS.
- The `name` GraphQL variable is NOT the fixed literal "live" it first
  appeared to be — the HAR that pattern was copied from happened to be a
  ?brand=nbc-sports-now page load, where NBC's own client also sends
  name="live". A real browser hitting *plain* nbc.com/live (no ?brand=)
  sends name=<local-affiliate-slug> instead (name="wcmh" here) — and that
  distinction actually matters: sending name="live" makes ONLY the local
  "nbc" row's `programs`/`stationId` come back null ("Program Information
  Unavailable" placeholder), while every other national/vault row is
  unaffected by `name` either way. So this scraper always sets `name` equal
  to `nbcAffiliateName`, matching the real unbranded-page request shape.
- USA, Syfy, MSNBC, CNBC, E!, and Oxygen are NOT part of nbc.com's TVE
  product despite being NBCUniversal-owned — confirmed two independent ways
  on 2026-08-03: (1) a real headless-browser page load of
  nbc.com/live?brand=syfy&callsign=syfy_east (syfy.com/live's own redirect
  target) falls back to the same generic placeholder content, and (2) the
  `page` GraphQL query (the one that resolves nbc-sports-now's real content
  by callSign) was retried directly for syfy/oxygen/usa/e/msnbc/cnbc with
  `name` set to each brand's own slug (ruling out the `name`-value bug
  above) and still only returns the generic NBC News NOW fallback for all
  of them — they just don't have working entries in NBC's backend. USA
  Network does have a real, working live guide, but on usanetwork.com,
  powered by an entirely different backend
  (usanetwork-cached.api.viewlift.com/graphql) — a separate platform, would
  need its own scraper. MSNBC redirects to ms.now; CNBC and E! are on their
  own domains too.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import struct
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import quote, urlsplit

import requests

from .base import BaseScraper, ChannelData, ConfigField, ProgramData
from .fox_tve import _cox_saml_login
from ..gracenote_map import resolve_gracenote
from ..models import TVEAccount
from ..tve.adobe_pass import (
    MvpdCooldownMixin,
    TVEAuthError,
    TVENotAuthorizedError,
    load_cached_adobe_client_creds,
    save_adobe_client_creds,
)

try:
    from Cryptodome.Cipher import AES as _AES
except ImportError:  # pragma: no cover
    from Crypto.Cipher import AES as _AES  # type: ignore[no-redef]

SCHEME = 'nbc-tve://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'

LIVE_PAGE_URL = 'https://www.nbc.com/live'
GEOLOCATION_URL = 'https://geolocation.digitalsvc.apps.nbcuni.com/geolocation/live/'
GRAPHQL_URL = 'https://friendship.nbc.com/v3/graphql'
# Apollo persisted-query hash for the componentsForPlaceholders(_cached) guide
# query. NBC rotates this on deploy — the API returns a 200 with
# {"errors":[{"code":"PERSISTED_QUERY_NOT_FOUND"}]} rather than a 4xx when it's
# stale, which _fetch_guide() below can't distinguish from "no channels in the
# response" on its own. Re-discovered 2026-08-28 via a real headless-browser
# capture of nbc.com/live's network traffic (the value lives in a webpack
# chunk, module id varies per bundle build — search a fresh generetic.*.js
# bundle for the literal 'componentsForPlaceholders' export mapping to find it
# again if this goes stale).
GUIDE_QUERY_HASH = 'ce452fadd8c890b99a3bced155078c5d07db87f5f39be430ef3e5be5d4bb2e33'
# base64 of {"type":"TvGuide","implementation":"liveGuideTvGuide","name":"","app":""} — fixed constant.
GUIDE_COMPONENT_CONFIG_B64 = 'eyJ0eXBlIjoiVHZHdWlkZSIsImltcGxlbWVudGF0aW9uIjoibGl2ZUd1aWRlVHZHdWlkZSIsIm5hbWUiOiIiLCJhcHAiOiIifQ=='
LEMONADE_LINEAR_URL = 'https://lemonade.nbc.com/v2/linear/'
DRM_PROXY_LICENSE_URL = 'https://drmproxy.digitalsvc.apps.nbcuni.com/drm-proxy/license/widevine'

ADOBE_BASE = 'https://sp.auth.adobe.com'
REQUESTOR_ID = 'nbcentertainment'
DEFAULT_REDIRECT_URL = 'https://www.nbc.com/mvpd-complete'

# National networks + the caller's local NBC affiliate + NECN — see module
# docstring. The 4 RSN rows (Bay Area, Boston, California, Philadelphia) are
# still deliberately excluded: single-market regional sports, almost always
# blackout-restricted to that market's own MVPD footprint.
_INCLUDED_BRANDS: dict[str, tuple[str, str]] = {  # machineName -> (display name, category)
    'nbc': ('NBC', 'Entertainment'),
    'nbc-news': ('NBC News NOW', 'News'),
    'nbc-sports-now': ('NBC Sports Now', 'Sports'),
    'bravo': ('Bravo', 'Entertainment'),
    'bravo-vault': ('Bravo Vault', 'Entertainment'),
    'real-housewives-vault': ('Real Housewives Vault', 'Entertainment'),
    'telemundo-al-dia': ('Telemundo Al Día', 'News'),
    'telemundo-deportes-ahora': ('Telemundo Deportes Ahora', 'Sports'),
    'noticias-telemundo-ahora': ('Noticias Telemundo Ahora', 'News'),
    'nbc-universo': ('NBC Universo', 'Entertainment'),
    'necn': ('NECN', 'News'),
}

# Reverse of _INCLUDED_BRANDS' display names, for recovering the machine brand
# name from a DB Channel row (source_channel_id is stream_access_name, an
# opaque per-install value — see the comment in fetch_channels() — so the
# community CSV key has to be rebuilt from the display name instead).
_DISPLAY_NAME_TO_BRAND = {display: brand for brand, (display, _category) in _INCLUDED_BRANDS.items()}
_EAST_WEST_BRANDS = {'bravo', 'nbc-universo'}

# Page config (software statement / drm proxy secret) is baked into a versioned,
# deploy-tied JS bundle — cache well past a single scrape/play cycle.
_PAGE_CONFIG_TTL = 12 * 3600
# Guide response carries station IDs + ~12h of EPG per channel.
_GUIDE_TTL = 15 * 60
# Cox Adobe Pass client token observed with a 6h exp; refresh a bit early.
_ENTITLEMENT_TTL = 3 * 3600
# Local-affiliate geolocation result — this install's outbound IP isn't
# going anywhere, no need to re-check often.
_GEO_TTL = 12 * 3600


def _lookup_geo_channel(brand_key: str, auth_key: str) -> str | None:
    """Ask NBC's own geolocation service which local affiliate it would hand
    a browser making this exact call — same IP-based lookup nbc.com/live does
    itself when no zip override is set client-side. See dev/nbc/NOTES.md: the
    real site's `serviceZip` override is an opaque encrypted blob (AES scheme
    not reversed), so there's no plaintext zip param to send here; this always
    reflects wherever the scraper's own outbound requests originate."""
    try:
        r = requests.post(
            f'{GEOLOCATION_URL}{brand_key}',
            json={'adobeMvpdId': None, 'device': 'web'},
            headers={
                'User-Agent': UA,
                'Accept': 'application/media.geo-v2+json',
                'Content-Type': 'application/json',
                'Origin': 'https://www.nbc.com',
                'Referer': 'https://www.nbc.com/',
                'client': 'oneapp',
                'authorization': f'NBC-Basic key="{auth_key}", version="3.0", type="cpc"',
            },
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError):
        return None
    geo_channel = ((data.get('localizedChannelInfo') or {}).get('geoChannel') or '').strip().lower()
    return geo_channel or None


def _decrypt_obfuscated_config(html: str) -> str | None:
    """Extract and decrypt the `"oc":"<base64>"` blob embedded in any nbc.com
    page's server-rendered client state. See module docstring for the format:
    12-byte IV + 32-byte AES-256 key + ciphertext(+16-byte GCM tag) + 4-byte
    big-endian compat-version uint32 (must be 1) — key and ciphertext travel
    together, so this is integrity protection, not real confidentiality."""
    m = re.search(r'"oc":"([^"]+)"', html)
    if not m:
        return None
    try:
        payload = base64.b64decode(m.group(1))
        if len(payload) <= 12 + 32 + 4:
            return None
        iv, key, ct_and_tag = payload[:12], payload[12:44], payload[44:-4]
        (version,) = struct.unpack('>I', payload[-4:])
        if version != 1:
            return None
        ciphertext, tag = ct_and_tag[:-16], ct_and_tag[-16:]
        cipher = _AES.new(key, _AES.MODE_GCM, nonce=iv)
        plaintext = cipher.decrypt_and_verify(ciphertext, tag)
        data = json.loads(plaintext)
        secret = (data.get('coreVideo') or {}).get('drmProxySecret')
        return secret if isinstance(secret, str) and secret else None
    except Exception:
        return None


def _find_software_statement(html: str, session: requests.Session) -> str | None:
    bundle_paths = re.findall(r'href="(/generetic/generated/generetic\.[a-f0-9]+\.js)"', html)
    for path in dict.fromkeys(bundle_paths):  # de-dupe, keep order
        try:
            r = session.get(f'https://www.nbc.com{path}', timeout=20)
        except requests.RequestException:
            continue
        if not r.ok:
            continue
        m = re.search(r'"v2":\{"softwareStatement":"([^"]+)"', r.text)
        if m:
            return m.group(1)
    return None


def _parse_iso(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


class AdobePassV2Client:
    """Minimal client for Adobe Pass's newer JSON REST API (`/api/v2/...`),
    as used by nbc.com — distinct from the XML-based flow in
    app/tve/adobe_pass.py's AdobePassCoxClient. See module docstring."""

    def __init__(
        self, requestor_id: str, software_statement: str, redirect_url: str, device_fingerprint: str,
        client_creds: dict | None = None,
    ) -> None:
        self.requestor_id = requestor_id
        self.software_statement = software_statement
        self.redirect_url = redirect_url
        self._client_creds = client_creds
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': UA,
            'Origin': 'https://www.nbc.com',
            'Referer': 'https://www.nbc.com/',
            # Required by /sessions et al ("invalid_header_device_identifier" otherwise) —
            # confirmed via a live 400 against the real API; format reverse-engineered
            # from a full (unfiltered) HAR header dump: "fingerprint " + base64(uuid).
            'ap-device-identifier': 'fingerprint ' + base64.b64encode(device_fingerprint.encode()).decode(),
        })
        self.client_id: str | None = None
        self.client_secret: str | None = None
        self.access_token: str | None = None

    def _post(self, url: str, **kwargs) -> requests.Response:
        try:
            r = self.session.post(url, timeout=20, **kwargs)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            raise TVEAuthError(str(exc)) from exc

    def _bearer_headers(self) -> dict[str, str]:
        return {'Authorization': f'Bearer {self.access_token}'}

    def _register_client(self) -> None:
        # A fresh client_creds means a caller already has a still-fresh
        # (client_id, client_secret, access_token) from a previous
        # registration for this requestor_id — reuse it instead of minting a
        # brand-new OAuth client with Adobe on every single attempt. See
        # app.tve.adobe_pass.load_cached_adobe_client_creds()'s docstring.
        cached = self._client_creds
        if cached and cached.get('client_id') and cached.get('access_token'):
            self.client_id = cached['client_id']
            self.client_secret = cached.get('client_secret')
            self.access_token = cached['access_token']
            return

        r = self._post(
            f'{ADOBE_BASE}/o/client/register',
            json={'software_statement': self.software_statement},
        )
        data = r.json()
        client_id, client_secret = data.get('client_id'), data.get('client_secret')
        if not client_id or not client_secret:
            raise TVEAuthError('Adobe Pass v2: client registration did not return credentials.')
        self.client_id, self.client_secret = client_id, client_secret

        r = self._post(
            f'{ADOBE_BASE}/o/client/token',
            data={'grant_type': 'client_credentials', 'client_id': client_id, 'client_secret': client_secret},
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
        )
        access_token = r.json().get('access_token')
        if not access_token:
            raise TVEAuthError('Adobe Pass v2: client token exchange did not return an access_token.')
        self.access_token = access_token

    def authorize(self, mso_id: str, username: str, password: str, cookie_jar: dict | None = None) -> dict:
        self._register_client()

        r = self._post(
            f'{ADOBE_BASE}/api/v2/{self.requestor_id}/sessions',
            data={'mvpd': mso_id, 'redirectUrl': self.redirect_url, 'domainName': 'nbc.com'},
            headers={**self._bearer_headers(), 'Content-Type': 'application/x-www-form-urlencoded'},
        )
        if not r.ok:
            try:
                detail = r.json().get('message') or r.text[:300]
            except ValueError:
                detail = r.text[:300]
            raise TVEAuthError(f'Adobe Pass v2: sessions request failed for MVPD {mso_id}: {detail}')
        session_data = r.json()
        auth_path = session_data.get('url')
        if not auth_path:
            raise TVEAuthError('Adobe Pass v2: sessions call did not return an authenticate url.')

        try:
            r = self.session.get(
                f'{ADOBE_BASE}{auth_path}', headers=self._bearer_headers(),
                allow_redirects=False, timeout=20,
            )
        except requests.RequestException as exc:
            raise TVEAuthError(str(exc)) from exc
        mso_login_url = r.headers.get('location') or ''
        # DIRECTV doesn't redirect here at all (see app/tve/mvpd/directv.py's
        # directv_login() docstring) — login_to_mvpd() below works from this
        # response's body directly, so it's exempt from the "no redirect"
        # check every other MSO needs.
        if not mso_login_url and mso_id != 'DTV':
            raise TVEAuthError('Adobe Pass v2: sessions authenticate call did not return an MVPD login redirect.')

        if mso_id == 'Cox':
            if 'login.cox.com' not in mso_login_url:
                raise TVEAuthError(f'Adobe Pass v2: unexpected authenticate redirect host {urlsplit(mso_login_url).netloc!r}.')
            try:
                _cox_saml_login(self.session, mso_login_url, username, password)
            except ValueError as exc:
                raise TVENotAuthorizedError(str(exc)) from exc
            except requests.RequestException as exc:
                raise TVEAuthError(str(exc)) from exc
        else:
            # Every other MVPD's actual sign-in mechanics live in
            # app/tve/mvpd/ — add one there (not here) to support a new
            # provider everywhere at once. Uses its own dedicated session
            # internally, entirely separate from self.session — Adobe binds
            # the completed login server-side to THIS session's
            # access_token/device fingerprint (embedded in mso_login_url via
            # the /sessions call above) rather than to any particular HTTP
            # session, same as the existing browser-assisted pairing's
            # cross-session polling already relies on.
            from ..tve.mvpd import login_to_mvpd
            page_html, page_url = (r.text, str(r.url)) if not mso_login_url else ('', mso_login_url)
            login_to_mvpd(mso_id, page_html, page_url, username, password, cookie_jar=cookie_jar)

        r = self._get(f'{ADOBE_BASE}/api/v2/{self.requestor_id}/profiles/{mso_id}', headers=self._bearer_headers())
        profile = ((r.json() or {}).get('profiles') or {}).get(mso_id)
        if not profile:
            raise TVENotAuthorizedError(f'Adobe Pass v2: no {mso_id} profile after login — MVPD did not authorize this account.')
        return profile

    def _get(self, url: str, **kwargs) -> requests.Response:
        try:
            r = self.session.get(url, timeout=20, **kwargs)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            raise TVEAuthError(str(exc)) from exc

    def preauthorize(self, mso_id: str, resource_ids: list[str]) -> dict[str, bool]:
        r = self._post(
            f'{ADOBE_BASE}/api/v2/{self.requestor_id}/decisions/preauthorize/{mso_id}',
            json={'resources': resource_ids},
            headers={**self._bearer_headers(), 'Content-Type': 'application/json'},
        )
        decisions: dict[str, bool] = {}
        for entry in (r.json() or {}).get('decisions') or []:
            resource = entry.get('resource')
            if resource:
                decisions[resource] = bool(entry.get('authorized'))
        return decisions


@dataclass(frozen=True)
class NbcGuideEntry:
    stream_access_name: str  # drm-proxy keyId / geolocation channelName — used as source_channel_id
    channel_id: str
    brand: str
    resource_id: str
    station_id: str
    logo_url: str | None
    name: str
    category: str
    programs: list[dict]

    @property
    def stream_url(self) -> str:
        return f'{SCHEME}{self.stream_access_name}'


class NbcTveScraper(MvpdCooldownMixin, BaseScraper):
    """NBCUniversal cable networks resolved via Adobe Pass Cox + NBCUniversal's
    own lemonade/drm-proxy playback stack. See module docstring."""

    source_name = 'nbc_tve'
    display_name = 'NBCUniversal TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 240
    stream_audit_enabled = True
    license_url = DRM_PROXY_LICENSE_URL  # class-level presence only: signals DRM-capable
                                          # to registry.drm_capable_source_names(); the real
                                          # per-request signed URL comes from get_license_url().
    all_channels_require_drm_bridge = True
    config_schema = [
        ConfigField(
            'local_nbc_affiliate',
            'Local NBC affiliate override',
            placeholder='wjar',
            help_text='Optional. The "NBC" channel is your market\'s local affiliate, auto-detected '
                       'by IP geolocation the same way nbc.com/live does — set this only if that '
                       'guess is wrong (e.g. this server is hosted somewhere other than where you '
                       'watch). Use the station\'s call letters, lowercase, no "W"/"K" prefix stripped '
                       '(e.g. wjar for Providence, knbc for Los Angeles).',
        ),
        ConfigField(
            'local_telemundo_affiliate',
            'Local Telemundo affiliate override',
            placeholder='wdem',
            help_text='Optional. Same as the NBC affiliate override above, for the local Telemundo station.',
        ),
    ]

    # ── channel + EPG discovery ──────────────────────────────────────────────

    def _local_affiliates(self) -> tuple[str, str]:
        nbc_override = (self.config.get('local_nbc_affiliate') or '').strip().lower()
        telemundo_override = (self.config.get('local_telemundo_affiliate') or '').strip().lower()
        if nbc_override and telemundo_override:
            return nbc_override, telemundo_override

        cached = self.cache.get('nbc_local_affiliate') or {}
        if not (cached and (time.time() - float(cached.get('cached_at', 0))) < _GEO_TTL):
            cached = {
                'nbc': _lookup_geo_channel('nbc', 'nbc_live'),
                'telemundo': _lookup_geo_channel('telemundo', 'telemundo_live'),
                'cached_at': time.time(),
            }
            self._update_cache('nbc_local_affiliate', cached)

        # 'wcmh'/'wdem' (Columbus, OH) are the last-resort fallback if geolocation
        # is unreachable AND there's no cached result yet — matches the value this
        # scraper hardcoded everywhere before per-install detection existed.
        nbc_affiliate = nbc_override or cached.get('nbc') or 'wcmh'
        telemundo_affiliate = telemundo_override or cached.get('telemundo') or 'wdem'
        return nbc_affiliate, telemundo_affiliate

    def _graphql_user_id(self) -> str:
        user_id = self.config.get('graphql_user_id')
        if not user_id:
            user_id = str(-(uuid.uuid4().int % 10**19))
            self._update_config('graphql_user_id', user_id)
        return user_id

    def _fetch_guide(self) -> dict[str, NbcGuideEntry]:
        cached = self.cache.get('nbc_guide') or {}
        if cached.get('entries') and (time.time() - float(cached.get('cached_at', 0))) < _GUIDE_TTL:
            return {k: NbcGuideEntry(**v) for k, v in cached['entries'].items()}

        # `name` must match the caller's local NBC affiliate slug (same value as
        # `nbcAffiliateName`) — NOT the literal string "live". Confirmed live:
        # a real browser hitting plain nbc.com/live sends name=nbcAffiliateName
        # and gets a populated local-affiliate row; the brand-specific capture
        # this was first copied from (?brand=nbc-sports-now) had name="live",
        # which silently breaks ONLY the local "nbc" row (returns a
        # stationId:null "Program Information Unavailable" placeholder) while
        # every other national row is unaffected either way.
        #
        # The affiliate itself is per-install, not a fixed constant — see
        # _local_affiliates(). `callSign` is sent empty: confirmed live
        # (2026-08-12) that varying it (including blank) doesn't change the
        # returned local-affiliate row at all, so there's nothing real to put
        # there.
        nbc_affiliate, telemundo_affiliate = self._local_affiliates()
        variables = {
            'userId': self._graphql_user_id(),
            'device': 'web',
            'platform': 'web',
            'language': 'en',
            'authorized': False,
            'isDayZero': True,
            'name': nbc_affiliate,
            'type': 'STREAM',
            'subType': 'home',
            'timeZone': 'America/New_York',
            'nbcAffiliateName': nbc_affiliate,
            'telemundoAffiliateName': telemundo_affiliate,
            'nationalBroadcastType': 'eastCoast',
            'callSign': '',
            'app': 'nbc',
            'appVersion': 1254001,
            'componentConfigs': [GUIDE_COMPONENT_CONFIG_B64],
            'queryName': 'componentsForPlaceholders_cached',
        }
        extensions = {'persistedQuery': {'version': 1, 'sha256Hash': GUIDE_QUERY_HASH}}
        r = self.session.get(
            GRAPHQL_URL,
            params={'variables': json.dumps(variables, separators=(',', ':')),
                    'extensions': json.dumps(extensions, separators=(',', ':'))},
            headers={'Referer': 'https://www.nbc.com/', 'User-Agent': UA},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        components = (((data.get('data') or {}).get('componentsForPlaceholders') or {}).get('components')) or []
        guide = next((c.get('data') or {} for c in components if c.get('component') == 'Guide'), {})
        schedules = guide.get('schedules') or []

        entries: dict[str, NbcGuideEntry] = {}
        for schedule in schedules:
            meta = schedule.get('meta') or {}
            brand = meta.get('brand') or ''
            branding = _INCLUDED_BRANDS.get(brand)
            if not branding:
                continue
            programs = ((schedule.get('data') or {}).get('programs')) or []
            first = next((p.get('guideProgramData') or {} for p in programs if isinstance(p, dict)), {})
            stream_access_name = first.get('streamAccessName')
            station_id = first.get('stationId')
            if not stream_access_name or not station_id:
                continue
            name, category = branding
            entries[stream_access_name] = NbcGuideEntry(
                stream_access_name=stream_access_name,
                channel_id=first.get('channelId') or stream_access_name,
                brand=brand,
                resource_id=first.get('resourceId') or brand,
                station_id=station_id,
                logo_url=first.get('whiteBrandLogo'),
                name=name,
                category=category,
                programs=programs,
            )

        if not entries:
            if cached.get('entries'):
                return {k: NbcGuideEntry(**v) for k, v in cached['entries'].items()}
            raise RuntimeError('NBC TVE: guide fetch returned no recognized national channels.')

        self._update_cache('nbc_guide', {
            'entries': {k: v.__dict__ for k, v in entries.items()},
            'cached_at': time.time(),
        })
        return entries

    def fetch_channels(self) -> list[ChannelData]:
        guide = self._fetch_guide()
        east_west = {'bravo', 'nbc-universo'}
        channels = []
        for entry in guide.values():
            suffix = ''
            # entry.stream_access_name is an opaque, per-install value straight
            # from NBC's live API — not stable/predictable enough to key a
            # shared Gracenote map by. entry.brand is the fixed machine name
            # from _INCLUDED_BRANDS, so key the lookup off that (plus the same
            # east/west split used for the display name) instead.
            gracenote_key = entry.brand
            if entry.brand in east_west:
                is_east = entry.stream_access_name.endswith('_east')
                suffix = ' (East)' if is_east else ' (West)'
                gracenote_key = f"{entry.brand}_{'east' if is_east else 'west'}"
            channels.append(ChannelData(
                source_channel_id=entry.stream_access_name,
                name=f'{entry.name}{suffix}',
                slug=f'nbc-tve-{entry.stream_access_name}',
                logo_url=entry.logo_url,
                stream_url=entry.stream_url,
                stream_type='dash',
                category=entry.category,
                language='es' if entry.brand.startswith('telemundo') or entry.brand == 'nbc-universo' else 'en',
                country='US',
                guide_key=f'NBCTVE:{entry.stream_access_name.upper()}',
                description=None,
                gracenote_id=resolve_gracenote('nbc_tve', lookup_key=gracenote_key),
            ))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {ch.source_channel_id for ch in channels}
        guide = self._fetch_guide()
        programs: list[ProgramData] = []
        for stream_access_name, entry in guide.items():
            if stream_access_name not in wanted:
                continue
            for program in entry.programs:
                if not isinstance(program, dict):
                    continue
                gp = program.get('guideProgramData') or {}
                start = _parse_iso(gp.get('startTime'))
                end = _parse_iso(gp.get('endTime'))
                if not start or not end or end <= start:
                    continue
                title = (gp.get('programTitle') or '').strip() or entry.name
                episode_title = (gp.get('episodeTitle') or '').strip() or None
                if episode_title == title:
                    episode_title = None
                season_val = gp.get('seasonNumber')
                episode_val = gp.get('episodeNumber')
                season_raw = str(season_val) if season_val is not None else ''
                episode_raw = str(episode_val) if episode_val is not None else ''
                programs.append(ProgramData(
                    source_channel_id=stream_access_name,
                    title=title,
                    description=(gp.get('programDescription') or '').strip() or None,
                    start_time=start,
                    end_time=end,
                    poster_url=gp.get('image'),
                    category=entry.category,
                    rating=gp.get('ratingWithAdvisories') or None,
                    episode_title=episode_title,
                    season=int(season_raw) if season_raw.isdigit() else None,
                    episode=int(episode_raw) if episode_raw.isdigit() else None,
                    episode_id=gp.get('tmsId') or None,
                    is_live=True,
                ))
        return programs

    # ── Adobe Pass Cox authorization ─────────────────────────────────────────

    def _discover_page_config(self) -> dict:
        cached = self.cache.get('nbc_page_config') or {}
        if cached and (time.time() - float(cached.get('cached_at', 0))) < _PAGE_CONFIG_TTL:
            return cached

        try:
            r = self.session.get(LIVE_PAGE_URL, headers={'User-Agent': UA}, timeout=20)
            r.raise_for_status()
        except requests.RequestException:
            if cached:
                return cached
            raise
        html = r.text

        drm_proxy_secret = _decrypt_obfuscated_config(html)
        software_statement = _find_software_statement(html, self.session)
        if not drm_proxy_secret or not software_statement:
            if cached:
                return cached  # stale beats nothing
            raise RuntimeError('NBC TVE: could not discover page config (software statement / DRM proxy secret).')

        config = {
            'software_statement': software_statement,
            'drm_proxy_secret': drm_proxy_secret,
            'cached_at': time.time(),
        }
        self._update_cache('nbc_page_config', config)
        return config

    def _mvpd_account(self) -> TVEAccount | None:
        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if account and account.is_enabled and account.has_credentials():
            return account
        return None

    def _ensure_device_fingerprint(self) -> str:
        fingerprint = self.config.get('device_fingerprint')
        if not fingerprint:
            fingerprint = str(uuid.uuid4())
            self._update_config('device_fingerprint', fingerprint)
        return fingerprint

    def _ensure_entitled(self, resource_id: str) -> None:
        from .. import db

        cached = self.cache.get('nbc_entitlements') or {}
        decisions = cached.get('decisions') or {}
        checked = set(cached.get('checked') or ())
        fresh = decisions and (time.time() - float(cached.get('cached_at', 0))) < _ENTITLEMENT_TTL
        # Adobe silently omitting a resource it WAS ASKED ABOUT (rather than
        # returning authorized:false) means that resource isn't MVPD-gated at all
        # — see module docstring. That's a different thing from "we've simply
        # never asked Adobe about this resource yet" (e.g. it was just added to
        # _INCLUDED_BRANDS after the cache was last populated) — `checked` tracks
        # which resource_ids were actually part of the last preauthorize request,
        # so a not-yet-asked-about resource always gets a real check instead of
        # silently inheriting "permitted" from decisions.get(..., True).
        if fresh and resource_id in checked and decisions.get(resource_id, True):
            return

        account = self._mvpd_account()
        if not account:
            raise TVEAuthError('TVE credentials are not configured in Settings.')
        cfg = account.config or {}
        mso_id = (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or cfg.get('adobe_mso_id') or 'Cox').strip()

        page_config = self._discover_page_config()

        # A human may have already completed a one-time browser-assisted sign-in
        # for this MVPD (app.worker.run_nbc_browser_login), for MSOs whose login
        # page blocks scripted clients. Reuse that access_token+device
        # fingerprint instead of re-registering a client and requiring the
        # human again, until Adobe actually stops recognizing it.
        cached_auth = cfg.get('nbc_mvpd_auth') or {}
        if mso_id != 'Cox' and cached_auth.get('mso_id') == mso_id and cached_auth.get('access_token'):
            cached_client = AdobePassV2Client(
                REQUESTOR_ID, page_config['software_statement'], DEFAULT_REDIRECT_URL,
                cached_auth.get('device_fingerprint') or self._ensure_device_fingerprint(),
            )
            cached_client.access_token = cached_auth['access_token']
            try:
                r = cached_client._get(f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/profiles/{mso_id}', headers=cached_client._bearer_headers())
                profile = ((r.json() or {}).get('profiles') or {}).get(mso_id)
            except TVEAuthError:
                profile = None
            if profile:
                resource_ids = sorted({e.resource_id for e in self._fetch_guide().values()} | {resource_id})
                decisions = cached_client.preauthorize(mso_id, resource_ids)
                self._update_cache('nbc_entitlements', {
                    'decisions': decisions, 'checked': sorted(resource_ids), 'cached_at': time.time(),
                })
                if not decisions.get(resource_id, True):
                    raise TVENotAuthorizedError(f'NBC TVE: {mso_id} account is not entitled to {resource_id}.')
                return

        if mso_id == 'YouTubeTV':
            # Only reachable when the cached_auth check above didn't already
            # return (no token saved yet, or it stopped being recognized) --
            # client.authorize() below would fall into login_to_mvpd()'s
            # generic "no scripted sign-in is wired up for this provider
            # yet" TVEAuthError, which reads as a maybe-someday-supported
            # gap and — being a plain TVEAuthError — play.py treats as
            # possibly transient (503 forever, never disables). Confirmed
            # definitively 2026-08-17 via a real browser-assisted login:
            # Adobe's own decision service denies this account for
            # nbcentertainment on YouTube TV, a clean permanent answer, not
            # a missing implementation. Skip the doomed scripted attempt
            # entirely and raise that directly.
            raise TVENotAuthorizedError(f'NBC TVE: {mso_id} is not entitled for {resource_id} (confirmed via Adobe Pass).')

        client_creds = load_cached_adobe_client_creds(account, REQUESTOR_ID)
        client = AdobePassV2Client(
            REQUESTOR_ID, page_config['software_statement'], DEFAULT_REDIRECT_URL,
            self._ensure_device_fingerprint(),
            client_creds=client_creds,
        )
        try:
            client.authorize(mso_id, account.username or '', account.password or '', cfg.get('xfinity_cookie_jar'))
            if not client_creds:
                save_adobe_client_creds(account, REQUESTOR_ID, client.client_id, client.client_secret, client.access_token)
            resource_ids = sorted({e.resource_id for e in self._fetch_guide().values()} | {resource_id})
            decisions = client.preauthorize(mso_id, resource_ids)
            account.last_auth_status = 'ok'
            account.last_auth_message = f'NBC TVE access token obtained through {mso_id} MVPD.'
            account.last_auth_at = datetime.now(timezone.utc)
            # Also stamp nbc_mvpd_auth's captured_at, same as the browser-
            # assisted flow's _save_nbc_mvpd_auth — the admin settings page's
            # "last signed in" column reads this, not last_auth_at (which is
            # shared across every TVE network and would show the wrong
            # timestamp for NBC specifically). For mso_id != 'Cox' this also
            # doubles as the actual reusable cached credential (see the
            # cached_auth check above); for Cox it's status-display-only,
            # since Cox always re-authorizes fresh here rather than reading it
            # back. Confirmed missing live 2026-08-11: a successful scripted
            # Cox sign-in from the "Sign in" button still showed "Never".
            new_cfg = dict(account.config or {})
            new_cfg['nbc_mvpd_auth'] = {
                'mso_id': mso_id,
                'access_token': client.access_token,
                'device_fingerprint': self._ensure_device_fingerprint(),
                'captured_at': int(time.time()),
            }
            account.config = new_cfg
            db.session.commit()
        except TVENotAuthorizedError as exc:
            account.last_auth_status = 'error'
            account.last_auth_message = f'NBC TVE: {mso_id} is not authorized: {exc}'[:500]
            account.last_auth_at = datetime.now(timezone.utc)
            db.session.commit()
            raise TVENotAuthorizedError(f'NBC TVE: {mso_id} is not authorized: {exc}') from exc
        except TVEAuthError as exc:
            account.last_auth_status = 'error'
            account.last_auth_message = f'NBC TVE: Adobe Pass auth failed: {exc}'[:500]
            account.last_auth_at = datetime.now(timezone.utc)
            db.session.commit()
            raise TVEAuthError(f'NBC TVE: Adobe Pass auth failed: {exc}') from exc

        self._update_cache('nbc_entitlements', {
            'decisions': decisions, 'checked': sorted(resource_ids), 'cached_at': time.time(),
        })
        if not decisions.get(resource_id, True):
            raise TVENotAuthorizedError(f'NBC TVE: {mso_id} account is not entitled to {resource_id}.')

    # ── playback resolve ──────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        # Every channel here is Widevine CENC DASH even though the manifest looks
        # like ordinary content to the generic audit — validate entitlement via
        # resolve(), then return the opaque URL sentinel so the audit skips
        # manifest inspection entirely. Same pattern as warner_tve.py.
        if raw_url.startswith(SCHEME):
            self.resolve(raw_url)
            return raw_url
        return raw_url

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported NBC TVE stream URL: {raw_url}')
        stream_access_name = raw_url[len(SCHEME):]

        guide = self._fetch_guide()
        entry = guide.get(stream_access_name)
        if not entry:
            raise RuntimeError(f'NBC TVE: channel {stream_access_name} not found in the current guide.')

        self._ensure_entitled(entry.resource_id)

        r = self.session.get(
            f'{LEMONADE_LINEAR_URL}{entry.station_id}',
            params={'platform': 'web', 'browser': 'other'},
            headers={'Referer': 'https://www.nbc.com/', 'User-Agent': UA},
            timeout=20,
        )
        r.raise_for_status()
        urls = (r.json() or {}).get('playbackUrls') or []
        manifest_url = next((u.get('url') for u in urls if u.get('cdn') == 'CLOUDFRONT'), None)
        if not manifest_url and urls:
            manifest_url = urls[0].get('url')
        if not manifest_url:
            raise RuntimeError(f'NBC TVE: no playback URL returned for {stream_access_name}.')

        # The drm-proxy `keyId` param selects WHICH stream's keys the license
        # server issues, and it must be the CDN channel token out of the playback
        # URL (".../Live/channel(<token>)/master.mpd") — confirmed against a real
        # HAR, where nbc.com's own player sent keyId=nbcsportspeacock for
        # channel(nbcsportspeacock). It is NOT our source_channel_id: those only
        # coincide for national feeds. The local NBC affiliate is the case that
        # exposes it — source_channel_id 'nbc' but channel(wcmh) on the CDN — so
        # sending 'nbc' got keys for the wrong stream, and every sample then
        # decrypted to garbage (the first audio packet died in the decoder with
        # PIPELINE_ERROR_DECODE, and no video frame ever decoded either).
        cdn_channel = None
        match = re.search(r'/channel\(([^)]+)\)/', manifest_url)
        if match:
            cdn_channel = match.group(1)
            playback = dict(self.cache.get('nbc_playback') or {})
            if playback.get(stream_access_name, {}).get('cdn_channel') != cdn_channel:
                playback[stream_access_name] = {'cdn_channel': cdn_channel, 'cached_at': time.time()}
                self._update_cache('nbc_playback', playback)
        return manifest_url

    # ── DRM license proxy wiring ─────────────────────────────────────────────

    @classmethod
    def license_request_headers(cls, config: dict) -> dict:
        return {'Content-Type': 'application/octet-stream'}

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        page_config = config.get('nbc_page_config') or {}
        drm_proxy_secret = page_config.get('drm_proxy_secret')
        if not drm_proxy_secret or not channel_id:
            return cls.license_url
        # keyId must be the CDN channel token resolve() cached, not our
        # source_channel_id — see the note in resolve(). Falling back to
        # channel_id keeps national feeds working if the cache is cold.
        key_id = ((config.get('nbc_playback') or {}).get(channel_id) or {}).get('cdn_channel') or channel_id
        timestamp_ms = int(time.time() * 1000)
        secret_value = f'{timestamp_ms}widevine'
        signature = hmac.new(drm_proxy_secret.encode(), secret_value.encode(), hashlib.sha256).hexdigest()
        return (
            f'{DRM_PROXY_LICENSE_URL}?time={timestamp_ms}&hash={signature}'
            f'&device=web&keyId={quote(key_id, safe="")}'
        )

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        return challenge, cls.license_request_headers(config)

    @classmethod
    def community_map_keys(cls, channel) -> list[str]:
        name = channel.name or ''
        is_east = name.endswith(' (East)')
        is_west = name.endswith(' (West)')
        base_name = name[:-len(' (East)')] if (is_east or is_west) else name
        brand = _DISPLAY_NAME_TO_BRAND.get(base_name)
        if not brand:
            return []
        if brand in _EAST_WEST_BRANDS and (is_east or is_west):
            return [f"{brand}_{'east' if is_east else 'west'}"]
        return [brand]

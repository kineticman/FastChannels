"""
viewlift_tve.py — the shared ViewLift ("AppCMS") TV-provider scraper.

NESN, Monumental Sports Network, Altitude+ and MyOutdoorTV are all ViewLift
tenants running the same web app (builds 77xx), the same platform as
usanetwork.com (see versant_tve.py). Each site is a small subclass that sets
the class attributes below; the flow is the same for all of them. NESN was
built first (scoped 2026-10-21, dev/nesn/SCOPED.md); the other three were
scoped from HARs on 2026-10-27 (dev/hitlist/viewlift_rsn_scoping_2026-10-27.md).

Key facts established by probing:

- The live page is server-rendered. `window.app_data` carries the API base,
  x-api-key and Adobe Pass settings (requestor, resource(s), a v2 software
  statement). `window.page_data` has one LinearchannelStandaloneModule whose
  `channels[]` are the site's channels, each with a public XMLTV guide URL
  whose `meta` names the upstream guide (Gracenote station, Amagi, ViewLift).
  No login needed.
- Adobe Pass v2 (phone-link sign-in, link_login.py's _ViewLift). The pages
  name api.auth.adobe.com; sp.auth.adobe.com answers the same calls, so the
  shared AdobePassV2Client works unchanged.
- A TV-provider sign-in links only to a registered ViewLift account:
  `identitySignInTve` under an anonymous user answers INVALID_CONFIG. Two
  ways to get one, both passwordless:
    * email → code: GraphQL `identityAuthOtpInitiate` emails a code,
      `identityAuthOtpValidate` returns a 30-day user token and a refresh
      token (NESN).
    * email only: `POST /v2/identity/account` with the email RSA-OAEP
      encrypted under the site's public key (app_data.appcmsMain.publicKey)
      creates the account outright — no code, no password, and the address
      is never checked (Altitude, MyOutdoorTV, Monumental). An address that
      already has an account (`/v2/identity/auth/verify` → iae) falls back
      to the code where the site offers one.
  Either way GET /identity/refresh/<token> renews it (the refresh token stays
  the same). `identitySignInTve` with the account's token then attaches the
  Adobe sign-in to the account and returns an empty token: the account's own
  token is the one playback uses.
- `/v3/entitlement/linearchannel` is market-locked by the TV provider's
  service ZIP, not the viewer's IP: outside the market it answers 403
  `contentGeoRestricted` with `tveZip` (from the Adobe profile) next to
  `deviceZip`. So a VPN doesn't help; the account must be billed in-market.
- No tenant has been seen past that check, so the stream format and DRM are
  unconfirmed. Versant on the same platform is Widevine CENC DASH, so this
  assumes the same (bridge only). The web player (in every tenant's bundle)
  is Axinom DRM like Versant's: it posts the challenge to the answer's
  `videoAssets.widevine.licenseUrl` with `X-AxDRM-Message: licenseToken`, so
  the license proxy uses the URL from the last answer (get_license_url), not
  a fixed one. With no Widevine asset it falls back to a clear DASH, then HLS,
  asset, and logs which asset types the answer had.

Every config, cache and Redis key is `<key>_…`, so NESN (key 'nesn') keeps
the keys it had before this module was split out of nesn_tve.py.
"""
from __future__ import annotations

import base64
import json
import logging
import re
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import requests

from .base import BaseScraper, ChannelData, ProgramData, null_placeholder_season_episode
from ..gracenote_map import resolve_gracenote
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import MvpdCooldownMixin, TVEAuthError, TVENotAuthorizedError, commit_token_cache

logger = logging.getLogger(__name__)

UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'
ADOBE_BASE = 'https://sp.auth.adobe.com'
# The ViewLift-wide web key; a tenant with its own (Monumental) sets api_key.
SHARED_API_KEY = 'WX41iaJiOw7hJW8sNbDP5JpVwmjaH6t6y3xbQUsc'
SHARED_PUBLIC_KEY_URL = 'https://appcms.asset.viewlift.com/certificates/public_key_shared-prod.pem'

_PAGE_TTL = 12 * 3600
# Entitlement answers are reused this long (Versant's license token lasts ~12h).
_PLAYBACK_TTL = 3600
# Mint a new ViewLift token this long before the current one expires.
_SESSION_MARGIN = 24 * 3600
# Refresh the account token (30 days) this long before it expires.
_ACCOUNT_MARGIN = 3 * 86400


def _jwt_payload(token: str) -> dict:
    try:
        part = token.split('.')[1]
        return json.loads(base64.urlsafe_b64decode(part + '=' * (-len(part) % 4)))
    except (IndexError, ValueError, AttributeError):
        return {}


def _page_blob(html: str, name: str) -> dict:
    i = html.index(f'window.{name}')
    return json.JSONDecoder().raw_decode(html[html.index('{', i):])[0]


def _xmltv_time(value: str | None) -> datetime | None:
    try:
        return datetime.strptime(value or '', '%Y%m%d%H%M%S %z').astimezone(timezone.utc)
    except ValueError:
        return None


def _guide_gracenote(epg_url: str) -> str | None:
    """The Gracenote station a guide URL's `meta` says it is built from, if any."""
    try:
        meta = parse_qs(urlsplit(epg_url).query).get('meta', [''])[0]
        info = json.loads(base64.b64decode(meta + '=' * (-len(meta) % 4)))
    except (ValueError, TypeError):
        return None
    if info.get('format') != 'GRACENOTE_JSON':
        return None
    m = re.search(r'/stations/(\d+)/', info.get('sourceUrl') or '')
    return m.group(1) if m else None


class ViewLiftTVEScraper(MvpdCooldownMixin, BaseScraper):
    """A ViewLift tenant's live channels with a TV provider sign-in. Not a
    source itself (no source_name); each site subclasses it, listing
    BaseScraper again so the registry discovers it."""

    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True
    # Assumed from Versant on the same platform — see module docstring.
    all_channels_require_drm_bridge = True
    # Never played past the market check — see module docstring.
    is_beta = True

    # ── per site ──
    # Prefix for config/cache/Redis keys, the link-login family and the
    # sign-in status key (app/tve/status.py, signin_notice).
    key = ''
    brand = ''              # 'NESN' — used in messages and the account panel
    site = ''               # ViewLift site id
    live_page_url = ''
    origin = ''
    domain_name = ''        # Adobe /sessions domainName
    redirect_url = ''
    requestor_id = ''
    default_resource = ''
    api_base = ''           # fallback when the page can't be read
    api_key = SHARED_API_KEY
    public_key_url = SHARED_PUBLIC_KEY_URL
    scheme = ''             # opaque stream_url prefix, e.g. 'nesn-tve://'
    guide_prefix = ''       # guide_key = f'{guide_prefix}:{CID}'
    channel_category = 'Sports'
    # ViewLift channel id or lower-cased title -> (our id, display name).
    channels: dict[str, tuple[str, str]] = {}
    # 'otp' (email → code) or 'email' (email only, code as fallback).
    account_signup = 'otp'
    # Whether the site can email a code (the 'email' signup's fallback).
    otp_enabled = True
    # Said when the entitlement answers contentGeoRestricted.
    market_note = ''

    # ── page config / channel discovery ──────────────────────────────────────

    def _page_config(self) -> dict:
        cache_key = f'{self.key}_page'
        cached = self.cache.get(cache_key) or {}
        if cached and time.time() - float(cached.get('cached_at', 0)) < _PAGE_TTL:
            return cached
        try:
            r = self.session.get(self.live_page_url, headers={'User-Agent': UA}, timeout=20)
            r.raise_for_status()
            app_data = _page_blob(r.text, 'app_data')
            page_data = _page_blob(r.text, 'page_data')
            tve = app_data['appcmsPlatform']['tveSettings']
            api = (app_data.get('appcmsMain') or {}).get('apiConfig') or {}
            module = next(m for m in page_data['page']['modules']
                          if m.get('moduleType') == 'LinearchannelStandaloneModule')
            linear = module['contentData'][0]
            m = re.search(r'window\.xApiKey="([^"]+)"', r.text)
            channels = {}
            for ch in linear.get('channels') or []:
                known = (self.channels.get(ch.get('id') or '')
                         or self.channels.get((ch.get('title') or '').strip().lower()))
                if not known:
                    logger.warning('[%s] skipping unrecognised channel %r', self.source_name, ch.get('title'))
                    continue
                channels[known[0]] = {
                    'vl_id': ch['id'], 'title': ch.get('title'), 'logo': ch.get('thumbnail'),
                    'epg_url': (ch.get('epgInfo') or {}).get('vlUrl') or '',
                }
            config = {
                'requestor_id': tve['requestorID'],
                'software_statement': tve.get('softwareStatementV2') or tve['softwareStatement'],
                'resources': tve.get('resourceIds') or [tve.get('resourceID') or self.default_resource],
                'api_base': api.get('apiBaseUrl') or self.api_base,
                'api_key': m.group(1) if m else self.api_key,
                'linear_id': linear['id'],
                'channels': channels,
                'cached_at': time.time(),
            }
            if config['requestor_id'] != self.requestor_id or not config['software_statement'] or not channels:
                raise ValueError(f'unexpected {self.domain_name} page config')
        except (requests.RequestException, ValueError, KeyError, TypeError, StopIteration, IndexError) as exc:
            if cached:
                logger.warning('[%s] could not refresh page config, using the saved one: %s', self.source_name, exc)
                return cached
            raise RuntimeError(f'{self.display_name}: could not read {self.live_page_url}: {exc}') from exc
        self._update_cache(cache_key, config)
        return config

    def fetch_channels(self) -> list[ChannelData]:
        names = dict(self.channels.values())
        return [
            ChannelData(
                source_channel_id=cid,
                name=names[cid],
                slug=f'{self.scheme.removesuffix("://")}-{cid}',
                logo_url=ch.get('logo'),
                stream_url=f'{self.scheme}{cid}',
                stream_type='dash',
                category=self.channel_category,
                language='en',
                country='US',
                guide_key=f'{self.guide_prefix}:{cid.upper()}',
                description=None,
                gracenote_id=resolve_gracenote(self.source_name, upstream_id=_guide_gracenote(ch.get('epg_url') or ''),
                                               lookup_key=cid),
            )
            for cid, ch in self._page_config()['channels'].items()
        ]

    # ── guide ────────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        page = self._page_config()['channels']
        programs: list[ProgramData] = []
        for channel in channels:
            ch = page.get(channel.source_channel_id) or {}
            parsed = self._fetch_guide(channel.source_channel_id, ch.get('epg_url')) if ch.get('epg_url') else []
            programs.extend(parsed or self._placeholder_epg(channel))
        null_placeholder_season_episode(programs)
        return programs

    def _fetch_guide(self, cid: str, url: str) -> list[ProgramData]:
        try:
            r = self.session.get(url, headers={'User-Agent': UA}, timeout=30)
            r.raise_for_status()
            # The feed starts with a UTF-8 BOM, which ElementTree accepts from bytes.
            root = ET.fromstring(r.content)
        except (requests.RequestException, ET.ParseError) as exc:
            logger.warning('[%s] guide fetch failed for %s: %s', self.source_name, cid, exc)
            return []
        programs = []
        for p in root.findall('programme'):
            start, end = _xmltv_time(p.get('start')), _xmltv_time(p.get('stop'))
            title = (p.findtext('title') or '').strip()
            if not start or not end or end <= start or not title:
                continue
            sub_type = (p.findtext('sub-type') or '').strip().upper()
            icon = p.find('icon')
            programs.append(ProgramData(
                source_channel_id=cid,
                title=title,
                start_time=start,
                end_time=end,
                description=(p.findtext('desc') or '').strip() or None,
                poster_url=icon.get('src') if icon is not None else None,
                category=self.channel_category,
                episode_id=p.get('id') or None,
                # ViewLift marks airings (R)/(NEW)/(LIVE) in <sub-type>.
                is_live='LIVE' in sub_type,
            ))
        programs.sort(key=lambda prog: prog.start_time)
        return programs

    @staticmethod
    def _placeholder_epg(channel: ChannelData) -> list[ProgramData]:
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        return [
            ProgramData(
                source_channel_id=channel.source_channel_id,
                title=f'{channel.name} Live',
                description=f'Live {channel.name} TV Everywhere programming.',
                start_time=now + timedelta(hours=i),
                end_time=now + timedelta(hours=i + 1),
                category=channel.category,
                is_live=True,
            )
            for i in range(24)
        ]

    # ── ViewLift account (passwordless) ──────────────────────────────────────

    @classmethod
    def _vl_headers(cls, api_key: str | None = None) -> dict:
        return {'User-Agent': UA, 'Origin': cls.origin, 'Referer': cls.origin + '/',
                'x-api-key': api_key or cls.api_key}

    @classmethod
    def _anonymous_token(cls, device_id: str) -> str:
        anon = requests.get(f'{cls.api_base}/identity/anonymous-token', headers=cls._vl_headers(),
                            params={'site': cls.site, 'platform': 'web_browser', 'deviceId': device_id}, timeout=20)
        anon.raise_for_status()
        return anon.json()['authorizationToken']

    @classmethod
    def _encrypt(cls, value: str) -> str:
        """RSA-OAEP (SHA-1, as the web app's node-forge does) under the site's public key."""
        from Cryptodome.Cipher import PKCS1_OAEP
        from Cryptodome.PublicKey import RSA
        r = requests.get(cls.public_key_url, headers={'User-Agent': UA}, timeout=20)
        r.raise_for_status()
        return base64.b64encode(PKCS1_OAEP.new(RSA.import_key(r.text)).encrypt(value.encode())).decode()

    @classmethod
    def begin_account_login(cls, email: str) -> dict:
        """Start the account sign-in. Returns {'status': 'signed_in', 'login': …}
        when the account is ready, or {'status': 'code', 'ctx': …} when the site
        emailed a code for verify_login_code(). Raises ValueError with a
        user-facing message."""
        email = (email or '').strip()
        if not email or '@' not in email:
            raise ValueError(f'Enter an email address for your {cls.brand} account.')
        if cls.account_signup == 'otp':
            return {'status': 'code', 'ctx': cls.request_login_code(email)}
        device_id = f'browser-{uuid.uuid4()}'
        try:
            anon_token = cls._anonymous_token(device_id)
            headers = {**cls._vl_headers(), 'authorization': anon_token}
            encrypted = cls._encrypt(email)
            r = requests.get(f'{cls.api_base}/v2/identity/auth/verify', headers=headers,
                             params={'e': encrypted}, timeout=20)
            existing = bool(r.ok and (r.json() or {}).get('iae'))
            if not existing:
                r = requests.post(f'{cls.api_base}/v2/identity/account', headers=headers,
                                  params={'deviceId': device_id}, timeout=30,
                                  json={'e': encrypted, 'p': '', 'name': '', 'dob': '', 'userZipcode': '',
                                        'emailConsent': False})
                body = r.json() if r.content else {}
        except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
            raise ValueError(f'Could not reach {cls.brand}: {exc}') from exc
        if existing:
            if not cls.otp_enabled:
                raise ValueError(f'That email already has a {cls.brand} account. Use a different email: '
                                 f'{cls.brand} only needs one to link your TV provider to, and doesn\'t check it.')
            return {'status': 'code', 'ctx': cls.request_login_code(email)}
        if not body.get('authorizationToken') or not body.get('refreshToken'):
            raise ValueError(f'{cls.brand} did not create the account (HTTP {r.status_code}: '
                             f'{body.get("errorMessage") or body.get("error") or "no token returned"}).')
        return {'status': 'signed_in', 'login': {
            'email': email, 'user_id': body.get('userId'), 'device_id': device_id,
            'token': body['authorizationToken'], 'refresh_token': body['refreshToken'],
            'signed_in_at': int(time.time())}}

    @classmethod
    def request_login_code(cls, email: str) -> dict:
        """The site emails a sign-in code to `email`. Returns the context
        verify_login_code() needs. Raises ValueError with a user-facing message."""
        email = (email or '').strip()
        if not email or '@' not in email:
            raise ValueError(f'Enter the email address of your {cls.brand} account.')
        device_id = f'browser-{uuid.uuid4()}'
        query = ('mutation ($site: String!, $device: EntitlementDevice!, $email: String!, $emailConsent: Boolean!, '
                 '$campaign: String!, $campaignSource: String!, $campaignMedium: String!, $deviceName: String!) '
                 '{ identityAuthOtpInitiate(site: $site, device: $device, input: {email: $email, '
                 'emailConsent: $emailConsent, deviceName: $deviceName, campaign: $campaign, '
                 'campaignSource: $campaignSource, campaignMedium: $campaignMedium}) { key } }')
        try:
            anon_token = cls._anonymous_token(device_id)
            r = requests.post(f'{cls.api_base}/graphql', timeout=30,
                              headers={**cls._vl_headers(), 'authorization': anon_token},
                              json={'query': query, 'variables': {
                                  'site': cls.site, 'device': 'web_browser', 'email': email, 'emailConsent': False,
                                  'campaign': '', 'campaignSource': '', 'campaignMedium': '', 'deviceName': 'WEB'}})
            body = r.json()
        except (requests.RequestException, ValueError, KeyError) as exc:
            raise ValueError(f'Could not reach {cls.brand}: {exc}') from exc
        key = ((body.get('data') or {}).get('identityAuthOtpInitiate') or {}).get('key')
        if not key:
            message = ((body.get('errors') or [{}])[0] or {}).get('message')
            raise ValueError(f'{cls.brand} did not send a code ({message or "no key returned"}).')
        return {'email': email, 'key': key, 'anon_token': anon_token, 'device_id': device_id}

    @classmethod
    def verify_login_code(cls, ctx: dict, code: str) -> dict:
        """The emailed code → the account login to keep in config.<key>_account.
        Raises ValueError with a user-facing message."""
        code = (code or '').strip().replace(' ', '')
        if not code:
            raise ValueError(f'Enter the code {cls.brand} emailed you.')
        if not (ctx and ctx.get('key')):
            raise ValueError('Sign-in session expired — request a new code.')
        query = ('mutation ($site: String!, $key: String, $otp: String) { identityAuthOtpValidate(site: $site, '
                 'key: $key, otp: $otp) { userId refreshToken authorizationToken } }')
        try:
            r = requests.post(f'{cls.api_base}/graphql', timeout=30,
                              headers={**cls._vl_headers(), 'authorization': ctx['anon_token']},
                              json={'query': query, 'variables': {'site': cls.site, 'key': ctx['key'], 'otp': code}})
            body = r.json()
        except (requests.RequestException, ValueError) as exc:
            raise ValueError(f'Could not reach {cls.brand}: {exc}') from exc
        result = (body.get('data') or {}).get('identityAuthOtpValidate') or {}
        if not result.get('authorizationToken') or not result.get('refreshToken'):
            raise ValueError('That code was incorrect or expired.')
        return {'email': ctx['email'], 'user_id': result.get('userId'), 'device_id': ctx['device_id'],
                'token': result['authorizationToken'], 'refresh_token': result['refreshToken'],
                'signed_in_at': int(time.time())}

    def _account_token(self) -> tuple[str, dict]:
        """The account's user token (refreshed when it nears its 30-day expiry;
        the refreshed one lives in the source cache) and the login."""
        login = self.config.get(f'{self.key}_account') or {}
        if not login.get('refresh_token'):
            raise self._sign_in_needed(
                f'{self.display_name}: sign in to your {self.brand} account first '
                f'(Sources → {self.display_name} → Sign in to {self.brand}).')
        cache_key = f'{self.key}_account_token'
        cached = self.cache.get(cache_key) or {}
        if cached.get('signed_in_at') != login.get('signed_in_at'):
            cached = {'token': login['token'], 'signed_in_at': login.get('signed_in_at')}
        exp = _jwt_payload(cached.get('token') or '').get('exp') or 0
        if exp - time.time() > _ACCOUNT_MARGIN:
            return cached['token'], login
        try:
            r = requests.get(f'{self.api_base}/identity/refresh/{login["refresh_token"]}',
                             headers=self._vl_headers(), timeout=20)
        except requests.RequestException as exc:
            raise TVEAuthError(f'{self.display_name}: could not reach {self.brand}: {exc}') from exc
        token = (r.json() or {}).get('authorizationToken') if r.ok else None
        if not token:
            if exp > time.time():
                return cached['token'], login
            raise self._sign_in_needed(
                f'{self.display_name}: your {self.brand} account sign-in expired (HTTP {r.status_code}) — '
                f'sign in to {self.brand} again under Sources → {self.display_name}.')
        self._update_cache(cache_key, {'token': token, 'signed_in_at': login.get('signed_in_at')})
        return token, login

    # ── sign-in ──────────────────────────────────────────────────────────────

    def _account(self):
        account = tve_account_for(self.source_name)
        if not account or not account.is_usable():
            raise TVEAuthError('TVE credentials are not configured in Settings.')
        return account

    @classmethod
    def adobe_client(cls, statement: str, auth: dict):
        """The Adobe v2 client a saved sign-in belongs to (same client id and
        device fingerprint — Adobe ties the sign-in to both)."""
        from .nbc_tve import AdobePassV2Client
        client = AdobePassV2Client(cls.requestor_id, statement, cls.redirect_url, auth['device_fingerprint'],
                                   client_creds=auth if auth.get('client_id') else None, origin=cls.origin)
        client.client_id = auth.get('client_id')
        client.client_secret = auth.get('client_secret')
        client.access_token = auth.get('access_token')
        return client

    def _adobe_profile(self, client, mso_id: str, account) -> dict | None:
        """Adobe's profile for the saved sign-in; None if Adobe says it's gone.
        A 401 is usually just an expired access token, so mint a new one for the
        same client first. Network trouble raises — it says nothing about the sign-in."""
        from .. import db
        url = f'{ADOBE_BASE}/api/v2/{self.requestor_id}/profiles/{mso_id}'

        def fetch():
            try:
                r = client.session.get(url, headers=client._bearer_headers(), timeout=20)
            except requests.RequestException as exc:
                raise TVEAuthError(f'{self.display_name}: could not reach Adobe: {exc}') from exc
            if r.status_code == 429 or r.status_code >= 500:
                raise TVEAuthError(f'{self.display_name}: Adobe returned HTTP {r.status_code}; will retry.')
            return r

        r = fetch()
        if r.status_code == 401 and client.client_id and client.client_secret:
            client.refresh_access_token()
            auth_key = f'{self.key}_mvpd_auth'
            cfg = dict(account.config or {})
            cfg[auth_key] = {**(cfg.get(auth_key) or {}), 'access_token': client.access_token}
            account.config = cfg
            commit_token_cache(f'{self.key} Adobe access token')
            r = fetch()
        if not r.ok:
            return None
        try:
            return ((r.json() or {}).get('profiles') or {}).get(mso_id) or None
        except ValueError:
            return None

    def _sign_in_needed(self, message: str) -> TVEAuthError:
        from ..tve.signin_notice import mark_signin_needed
        mark_signin_needed(self.key, message)
        return TVEAuthError(message)

    def _viewlift_session(self, force: bool = False) -> dict:
        """A ViewLift user token that carries the TV-provider sign-in."""
        account = self._account()
        auth = (account.config or {}).get(f'{self.key}_mvpd_auth') or {}
        if not auth.get('client_id') or not auth.get('device_fingerprint'):
            raise self._sign_in_needed(
                f'{self.display_name}: not signed in yet — use "Sign in" for {self.brand} under '
                'Settings → TV Everywhere.')
        cache_key = f'{self.key}_session'
        cached = self.cache.get(cache_key) or {}
        if (not force and cached.get('token') and cached.get('adobe_client_id') == auth['client_id']
                and cached.get('account_signed_in_at') == (self.config.get(f'{self.key}_account') or {}).get('signed_in_at')
                and float(cached.get('exp') or 0) - time.time() > _SESSION_MARGIN):
            return cached

        page = self._page_config()
        mso_id = auth['mso_id']
        client = self.adobe_client(page['software_statement'], auth)
        profile = self._adobe_profile(client, mso_id, account)
        if not profile:
            raise self._sign_in_needed(
                f'{self.display_name}: the {mso_id} sign-in has expired — sign in again under '
                'Settings → TV Everywhere.')
        session = self.sign_in_viewlift(client, mso_id, profile, page)
        session['adobe_client_id'] = auth['client_id']
        self._update_cache(cache_key, session)
        return session

    def sign_in_viewlift(self, client, mso_id: str, profile: dict, page: dict) -> dict:
        """Preauthorize with Adobe, then link the Adobe sign-in to the ViewLift
        account the way the site's own web client does."""
        r = client._post(
            f'{ADOBE_BASE}/api/v2/{self.requestor_id}/decisions/preauthorize/{mso_id}',
            json={'resources': page['resources']},
            headers={**client._bearer_headers(), 'Content-Type': 'application/json'},
        )
        decisions = {d.get('resource'): bool(d.get('authorized')) for d in (r.json() or {}).get('decisions') or []}
        if decisions and not any(decisions.values()):
            raise TVENotAuthorizedError(f'{self.display_name}: {mso_id} doesn\'t include {self.brand}.')

        api, headers = page['api_base'], self._vl_headers(page['api_key'])
        account_token, login = self._account_token()
        try:
            attrs = profile.get('attributes') or {}

            def attr(name):
                value = attrs.get(name)
                return (value.get('value') if isinstance(value, dict) else value) or ''

            variables = {'site': self.site, 'input': {
                'mvpdProvider': mso_id, 'provider': mso_id, 'idpName': mso_id,
                'userId': login.get('user_id') or _jwt_payload(account_token).get('id'), 'email': login.get('email') or '',
                'platform': 'web_browser', 'device': 'web_browser', 'deviceName': 'WEB',
                'deviceId': login.get('device_id') or f'browser-{uuid.uuid4()}',
                'tveUserId': attr('userID'),
                'adbAccessToken': client.access_token,
                'adbDeviceId': client.session.headers['ap-device-identifier'],
                'adobeMetadata': {'zip': None, 'encryptedZip': attr('zip')},
            }}
            query = ('mutation ($site: String!, $input: SignInTveInput!) { identitySignInTve(site: $site, '
                     'input: $input) { userId tveUserId provider authorizationToken refreshToken } }')
            r = requests.post(f'{api}/graphql', json={'query': query, 'variables': variables},
                              headers={**headers, 'authorization': account_token}, timeout=30)
            body = r.json()
        except (requests.RequestException, ValueError, KeyError) as exc:
            raise TVEAuthError(f'{self.display_name}: ViewLift sign-in failed: {exc}') from exc
        signin = (body.get('data') or {}).get('identitySignInTve')
        if body.get('errors') or not signin:
            raise TVEAuthError(f'{self.display_name}: {self.brand} refused to link the TV provider: '
                               f'{str(body.get("errors"))[:200]}')
        # Linked to the account; an empty token here means keep the account's own.
        token = signin.get('authorizationToken') or account_token
        return {'token': token, 'exp': _jwt_payload(token).get('exp') or time.time() + 7 * 86400,
                'decisions': decisions, 'mso_id': mso_id, 'account_signed_in_at': login.get('signed_in_at'),
                'cached_at': time.time()}

    # ── playback ─────────────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        # Validate entitlement, then hand back the opaque URL so the audit
        # skips manifest inspection — same pattern as versant_tve.py.
        if raw_url.startswith(self.scheme):
            self.resolve(raw_url)
        return raw_url

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(self.scheme):
            raise ValueError(f'Unsupported {self.display_name} stream URL: {raw_url}')
        cid = raw_url[len(self.scheme):]
        page = self._page_config()
        ch = page['channels'].get(cid)
        if not ch:
            raise RuntimeError(f'{self.display_name}: channel {cid} is not on {self.live_page_url} any more.')

        playback_key = f'{self.key}_playback'
        playback = self.cache.get(playback_key) or {}
        cached = playback.get(cid) or {}
        if (cached.get('url') and time.time() - float(cached.get('cached_at', 0)) < _PLAYBACK_TTL
                and float(cached.get('license_exp') or 0) - time.time() > 2 * 3600):
            return cached['url']

        session = self._viewlift_session()
        status, body = self._entitlement(page, ch, session['token'])
        if status in (401, 403) and body.get('errorCode') != 'contentGeoRestricted':
            # A token ViewLift no longer honours, or a package change since it
            # was minted: re-mint it from the Adobe sign-in once.
            logger.info('[%s] entitlement HTTP %s (%s) for %s; renewing the ViewLift sign-in',
                        self.source_name, status, body.get('errorCode'), cid)
            session = self._viewlift_session(force=True)
            status, body = self._entitlement(page, ch, session['token'])
        if status == 403 and body.get('errorCode') == 'contentGeoRestricted':
            raise TVENotAuthorizedError(
                f'{self.display_name}: {self.market_note} '
                f'(your {session.get("mso_id")} account is registered to ZIP {body.get("tveZip") or "unknown"}).')
        if status == 403 and body.get('errorCode') == 'TVE_SUBSCRIPTION_NOT_FOUND':
            raise TVENotAuthorizedError(
                f'{self.display_name}: not entitled to {ch["title"]} ({body.get("errorMessage")}).')
        if status != 200 or not body.get('playable', True):
            raise RuntimeError(f'{self.display_name}: entitlement for {cid} failed: '
                               f'HTTP {status} {body.get("errorCode") or ""}')

        assets = ((body.get('linearchannel') or {}).get('streamingInfo') or {}).get('videoAssets') or {}
        logger.info('[%s] entitlement for %s returned assets: %s', self.source_name, cid, sorted(assets))
        widevine = assets.get('widevine') or {}
        license_url = None
        if widevine.get('url') and widevine.get('licenseToken'):
            url, token = widevine['url'], widevine['licenseToken']
            license_url = widevine.get('licenseUrl')
            license_exp = _jwt_payload(token.split('|', 1)[-1]).get('exp') or time.time() + 6 * 3600
        else:
            # Unconfirmed: no DRM asset, so try a clear rendition (DASH first,
            # since the channels are registered as DASH).
            url = None
            for kind in ('dash', 'hls'):
                asset = assets.get(kind)
                url = asset.get('url') if isinstance(asset, dict) else asset
                if url:
                    break
            token, license_exp = None, time.time() + 24 * 3600
            if not url:
                raise RuntimeError(f'{self.display_name}: no playable stream returned for {cid} '
                                   f'(assets: {sorted(assets)}).')
        playback = dict(playback)
        playback[cid] = {'url': url, 'license_token': token, 'license_url': license_url,
                         'license_exp': license_exp, 'cached_at': time.time()}
        self._update_cache(playback_key, playback)
        return url

    def _entitlement(self, page: dict, ch: dict, token: str) -> tuple[int, dict]:
        try:
            r = self.session.get(
                f'{page["api_base"]}/v3/entitlement/linearchannel',
                params={'id': page['linear_id'], 'channelId': ch['vl_id'], 'deviceType': 'web_browser',
                        'contentConsumption': 'web', 'ssaiDisable': 'false'},
                headers={'Authorization': token, 'x-api-key': page['api_key'], 'hdcp': 'HDCP-1.4',
                         'sl': 'SW_SECURE_CRYPTO', 'User-Agent': UA, 'Origin': self.origin,
                         'Referer': self.origin + '/'},
                timeout=20,
            )
        except requests.RequestException as exc:
            raise TVEAuthError(f'{self.display_name}: could not reach ViewLift: {exc}') from exc
        try:
            return r.status_code, r.json() or {}
        except ValueError:
            return r.status_code, {}

    # ── DRM license proxy wiring ─────────────────────────────────────────────

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        # The license proxy merges source_cache into config, so the URL from
        # this channel's last entitlement answer is here once resolve() ran.
        cached = ((config.get(f'{cls.key}_playback') or {}).get(channel_id or '') or {}).get('license_url')
        return cached or cls.license_url

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        # The per-channel token from the last entitlement answer (resolve()
        # caches it; the license proxy merges source_cache into config).
        headers = {'Content-Type': 'application/octet-stream', 'Origin': cls.origin, 'Referer': cls.origin + '/'}
        token = ((config.get(f'{cls.key}_playback') or {}).get(channel_id or '') or {}).get('license_token')
        if token:
            headers['X-AxDRM-Message'] = token
        return challenge, headers


# Every ViewLift source, for the wiring that lists sources by name.
VIEWLIFT_SOURCES = ('nesn_tve', 'monumental_tve', 'altitude_tve', 'myoutdoortv_tve')

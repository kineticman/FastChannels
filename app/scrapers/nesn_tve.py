"""
nesn_tve.py — NESN and NESN Plus with a TV provider sign-in.

watch.nesn.com is a ViewLift ("AppCMS") site, the same platform as
usanetwork.com (see versant_tve.py), so the flow is Versant's with NESN's
values. Scoped 2026-10-21 from a signed-in HAR — dev/nesn/SCOPED.md.

Key facts established by probing:

- watch.nesn.com/live is server-rendered. `window.app_data` carries the API
  base, x-api-key (the same ViewLift-wide key as Versant) and Adobe Pass
  settings: requestor `NESNGO`, resource `NESN`, a v2 software statement.
  `window.page_data` has one LINEARCHANNEL whose `channels[]` are NESN and
  NESN Plus, each with a public XMLTV guide URL. No login needed.
- Adobe Pass v2 (phone-link sign-in, link_login.py's _Nesn). The page names
  api.auth.adobe.com; sp.auth.adobe.com answers the same calls, so the shared
  AdobePassV2Client works unchanged.
- Unlike Versant, NESN's ViewLift tenant links a TV-provider sign-in only to
  a registered NESN account: `identitySignInTve` under an anonymous user
  answers INVALID_CONFIG. NESN accounts are passwordless — GraphQL
  `identityAuthOtpInitiate` emails a code, `identityAuthOtpValidate` returns a
  30-day user token and a refresh token (GET /identity/refresh/<token>; the
  refresh token stays the same). So sign-in is two steps: the NESN account
  (email → code, on the source's settings), then the TV provider (phone link).
  `identitySignInTve` with the account's token then attaches the Adobe
  sign-in to the account and returns an empty token: the account's own token
  is the one playback uses.
- `/v3/entitlement/linearchannel` is market-locked by the TV provider's
  service ZIP, not the viewer's IP: outside the NESN market it answers 403
  `contentGeoRestricted` with `tveZip` (from the Adobe profile) next to
  `deviceZip`. So a VPN doesn't help; the account must be billed in-market.
- Never seen past that 403, so the stream format and DRM are unconfirmed.
  Versant on the same platform is Widevine CENC DASH, so this assumes the
  same (bridge only). NESN's web player (checked in its bundle) is Axinom
  DRM like Versant's: it posts the challenge to the answer's
  `videoAssets.widevine.licenseUrl` with `X-AxDRM-Message: licenseToken`, so
  the license proxy uses the URL from the last answer (get_license_url), not
  a fixed one. With no Widevine asset it falls back to a clear DASH, then HLS,
  asset, and logs which asset types the answer had.
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

import requests

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, null_placeholder_season_episode
from ..gracenote_map import resolve_gracenote
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import MvpdCooldownMixin, TVEAuthError, TVENotAuthorizedError

logger = logging.getLogger(__name__)

SCHEME = 'nesn-tve://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'
LIVE_PAGE_URL = 'https://watch.nesn.com/live'
ORIGIN = 'https://watch.nesn.com'
ADOBE_BASE = 'https://sp.auth.adobe.com'
REQUESTOR_ID = 'NESNGO'
REDIRECT_URL = 'https://watch.nesn.com/checkout?isTveUser=Authenticated'
DOMAIN_NAME = 'watch.nesn.com'
SITE = 'nesn'
# Fallbacks for when the page can't be read; the page's own values win.
DEFAULT_API_BASE = 'https://nesn.api.viewlift.com'
DEFAULT_API_KEY = 'WX41iaJiOw7hJW8sNbDP5JpVwmjaH6t6y3xbQUsc'
DEFAULT_RESOURCE = 'NESN'
WIDEVINE_LICENSE_URL = f'{DEFAULT_API_BASE}/v1/license/widevine/acquire'
# Where the sign-in status/errors are kept (app/tve/status.py, signin_notice).
NETWORK_KEY = 'nesn'

# ViewLift channel title -> (our id, display name).
_CHANNELS = {
    'nesn': ('nesn', 'NESN'),
    'nesn plus': ('nesn-plus', 'NESN+'),
}

_PAGE_TTL = 12 * 3600
# Entitlement answers are reused this long (Versant's license token lasts ~12h).
_PLAYBACK_TTL = 3600
# Mint a new ViewLift token this long before the current one expires.
_SESSION_MARGIN = 24 * 3600
# Refresh the NESN account token (30 days) this long before it expires.
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


class NesnTVEScraper(MvpdCooldownMixin, BaseScraper):
    """NESN and NESN Plus via ViewLift + Adobe Pass v2 (New England providers only)."""

    source_name = 'nesn_tve'
    display_name = 'NESN TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True
    license_url = WIDEVINE_LICENSE_URL
    # Assumed from Versant on the same platform — see module docstring.
    all_channels_require_drm_bridge = True
    # Never played past the market check — see module docstring.
    is_beta = True

    # The NESN account is passwordless (an emailed code — see request_login_code
    # and the "Sign in to NESN" panel on the source). The login is kept in
    # config.nesn_account; the email field only labels which account it is.
    config_schema = [
        ConfigField('email', 'NESN account email', placeholder='you@example.com',
                    help_text='The free NESN account the TV provider sign-in is linked to '
                              '(passwordless — NESN emails you a code).'),
    ]

    # ── page config / channel discovery ──────────────────────────────────────

    def _page_config(self) -> dict:
        cached = self.cache.get('nesn_page') or {}
        if cached and time.time() - float(cached.get('cached_at', 0)) < _PAGE_TTL:
            return cached
        try:
            r = self.session.get(LIVE_PAGE_URL, headers={'User-Agent': UA}, timeout=20)
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
                known = _CHANNELS.get((ch.get('title') or '').strip().lower())
                if not known:
                    logger.warning('[nesn_tve] skipping unrecognised channel %r', ch.get('title'))
                    continue
                channels[known[0]] = {
                    'vl_id': ch['id'], 'title': ch.get('title'), 'logo': ch.get('thumbnail'),
                    'epg_url': (ch.get('epgInfo') or {}).get('vlUrl') or '',
                }
            config = {
                'requestor_id': tve['requestorID'],
                'software_statement': tve.get('softwareStatementV2') or tve['softwareStatement'],
                'resources': tve.get('resourceIds') or [tve.get('resourceID') or DEFAULT_RESOURCE],
                'api_base': api.get('apiBaseUrl') or DEFAULT_API_BASE,
                'api_key': m.group(1) if m else DEFAULT_API_KEY,
                'linear_id': linear['id'],
                'channels': channels,
                'cached_at': time.time(),
            }
            if config['requestor_id'] != REQUESTOR_ID or not config['software_statement'] or not channels:
                raise ValueError('unexpected watch.nesn.com page config')
        except (requests.RequestException, ValueError, KeyError, TypeError, StopIteration, IndexError) as exc:
            if cached:
                logger.warning('[nesn_tve] could not refresh page config, using the saved one: %s', exc)
                return cached
            raise RuntimeError(f'NESN TVE: could not read watch.nesn.com/live: {exc}') from exc
        self._update_cache('nesn_page', config)
        return config

    def fetch_channels(self) -> list[ChannelData]:
        names = dict(_CHANNELS.values())
        return [
            ChannelData(
                source_channel_id=cid,
                name=names[cid],
                slug=f'nesn-tve-{cid}',
                logo_url=ch.get('logo'),
                stream_url=f'{SCHEME}{cid}',
                stream_type='dash',
                category='Sports',
                language='en',
                country='US',
                guide_key=f'NESN:{cid.upper()}',
                description=None,
                gracenote_id=resolve_gracenote('nesn_tve', lookup_key=cid),
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
            logger.warning('[nesn_tve] guide fetch failed for %s: %s', cid, exc)
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
                category='Sports',
                episode_id=p.get('id') or None,
                # NESN marks airings (R)/(NEW)/(LIVE) in <sub-type>.
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

    # ── NESN account (passwordless: email → code) ────────────────────────────

    @staticmethod
    def _vl_headers(api_key: str = DEFAULT_API_KEY) -> dict:
        return {'User-Agent': UA, 'Origin': ORIGIN, 'Referer': ORIGIN + '/', 'x-api-key': api_key}

    @classmethod
    def request_login_code(cls, email: str) -> dict:
        """Step 1 — NESN emails a sign-in code to `email`. Returns the context
        verify_login_code() needs. Raises ValueError with a user-facing message."""
        email = (email or '').strip()
        if not email or '@' not in email:
            raise ValueError('Enter the email address of your NESN account.')
        device_id = f'browser-{uuid.uuid4()}'
        query = ('mutation ($site: String!, $device: EntitlementDevice!, $email: String!, $emailConsent: Boolean!, '
                 '$campaign: String!, $campaignSource: String!, $campaignMedium: String!, $deviceName: String!) '
                 '{ identityAuthOtpInitiate(site: $site, device: $device, input: {email: $email, '
                 'emailConsent: $emailConsent, deviceName: $deviceName, campaign: $campaign, '
                 'campaignSource: $campaignSource, campaignMedium: $campaignMedium}) { key } }')
        try:
            anon = requests.get(f'{DEFAULT_API_BASE}/identity/anonymous-token', headers=cls._vl_headers(),
                                params={'site': SITE, 'platform': 'web_browser', 'deviceId': device_id}, timeout=20)
            anon.raise_for_status()
            anon_token = anon.json()['authorizationToken']
            r = requests.post(f'{DEFAULT_API_BASE}/graphql', timeout=30,
                              headers={**cls._vl_headers(), 'authorization': anon_token},
                              json={'query': query, 'variables': {
                                  'site': SITE, 'device': 'web_browser', 'email': email, 'emailConsent': False,
                                  'campaign': '', 'campaignSource': '', 'campaignMedium': '', 'deviceName': 'WEB'}})
            body = r.json()
        except (requests.RequestException, ValueError, KeyError) as exc:
            raise ValueError(f'Could not reach NESN: {exc}') from exc
        key = ((body.get('data') or {}).get('identityAuthOtpInitiate') or {}).get('key')
        if not key:
            message = ((body.get('errors') or [{}])[0] or {}).get('message')
            raise ValueError(f'NESN did not send a code ({message or "no key returned"}).')
        return {'email': email, 'key': key, 'anon_token': anon_token, 'device_id': device_id}

    @classmethod
    def verify_login_code(cls, ctx: dict, code: str) -> dict:
        """Step 2 — the emailed code → the account login to keep in
        config.nesn_account. Raises ValueError with a user-facing message."""
        code = (code or '').strip().replace(' ', '')
        if not code:
            raise ValueError('Enter the code NESN emailed you.')
        if not (ctx and ctx.get('key')):
            raise ValueError('Sign-in session expired — request a new code.')
        query = ('mutation ($site: String!, $key: String, $otp: String) { identityAuthOtpValidate(site: $site, '
                 'key: $key, otp: $otp) { userId refreshToken authorizationToken } }')
        try:
            r = requests.post(f'{DEFAULT_API_BASE}/graphql', timeout=30,
                              headers={**cls._vl_headers(), 'authorization': ctx['anon_token']},
                              json={'query': query, 'variables': {'site': SITE, 'key': ctx['key'], 'otp': code}})
            body = r.json()
        except (requests.RequestException, ValueError) as exc:
            raise ValueError(f'Could not reach NESN: {exc}') from exc
        result = (body.get('data') or {}).get('identityAuthOtpValidate') or {}
        if not result.get('authorizationToken') or not result.get('refreshToken'):
            raise ValueError('That code was incorrect or expired.')
        return {'email': ctx['email'], 'user_id': result.get('userId'), 'device_id': ctx['device_id'],
                'token': result['authorizationToken'], 'refresh_token': result['refreshToken'],
                'signed_in_at': int(time.time())}

    def _account_token(self) -> tuple[str, dict]:
        """The NESN account's user token (refreshed when it nears its 30-day
        expiry; the refreshed one lives in the source cache) and the login."""
        login = self.config.get('nesn_account') or {}
        if not login.get('refresh_token'):
            raise self._sign_in_needed(
                'NESN TVE: sign in to your NESN account first (Sources → NESN TVE → Sign in to NESN).')
        cached = self.cache.get('nesn_account_token') or {}
        if cached.get('signed_in_at') != login.get('signed_in_at'):
            cached = {'token': login['token'], 'signed_in_at': login.get('signed_in_at')}
        exp = _jwt_payload(cached.get('token') or '').get('exp') or 0
        if exp - time.time() > _ACCOUNT_MARGIN:
            return cached['token'], login
        try:
            r = requests.get(f'{DEFAULT_API_BASE}/identity/refresh/{login["refresh_token"]}',
                             headers=self._vl_headers(), timeout=20)
        except requests.RequestException as exc:
            raise TVEAuthError(f'NESN TVE: could not reach NESN: {exc}') from exc
        token = (r.json() or {}).get('authorizationToken') if r.ok else None
        if not token:
            if exp > time.time():
                return cached['token'], login
            raise self._sign_in_needed(
                f'NESN TVE: your NESN account sign-in expired (HTTP {r.status_code}) — '
                'sign in to NESN again under Sources → NESN TVE.')
        self._update_cache('nesn_account_token', {'token': token, 'signed_in_at': login.get('signed_in_at')})
        return token, login

    # ── sign-in ──────────────────────────────────────────────────────────────

    def _account(self):
        account = tve_account_for(self.source_name)
        if not account or not account.is_usable():
            raise TVEAuthError('TVE credentials are not configured in Settings.')
        return account

    @staticmethod
    def adobe_client(statement: str, auth: dict):
        """The Adobe v2 client a saved sign-in belongs to (same client id and
        device fingerprint — Adobe ties the sign-in to both)."""
        from .nbc_tve import AdobePassV2Client
        client = AdobePassV2Client(REQUESTOR_ID, statement, REDIRECT_URL, auth['device_fingerprint'],
                                   client_creds=auth if auth.get('client_id') else None, origin=ORIGIN)
        client.client_id = auth.get('client_id')
        client.client_secret = auth.get('client_secret')
        client.access_token = auth.get('access_token')
        return client

    def _adobe_profile(self, client, mso_id: str, account) -> dict | None:
        """Adobe's profile for the saved sign-in; None if Adobe says it's gone.
        A 401 is usually just an expired access token, so mint a new one for the
        same client first. Network trouble raises — it says nothing about the sign-in."""
        from .. import db
        url = f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/profiles/{mso_id}'

        def fetch():
            try:
                r = client.session.get(url, headers=client._bearer_headers(), timeout=20)
            except requests.RequestException as exc:
                raise TVEAuthError(f'NESN TVE: could not reach Adobe: {exc}') from exc
            if r.status_code == 429 or r.status_code >= 500:
                raise TVEAuthError(f'NESN TVE: Adobe returned HTTP {r.status_code}; will retry.')
            return r

        r = fetch()
        if r.status_code == 401 and client.client_id and client.client_secret:
            client.refresh_access_token()
            cfg = dict(account.config or {})
            cfg['nesn_mvpd_auth'] = {**(cfg.get('nesn_mvpd_auth') or {}), 'access_token': client.access_token}
            account.config = cfg
            db.session.commit()
            r = fetch()
        if not r.ok:
            return None
        try:
            return ((r.json() or {}).get('profiles') or {}).get(mso_id) or None
        except ValueError:
            return None

    @staticmethod
    def _sign_in_needed(message: str) -> TVEAuthError:
        from ..tve.signin_notice import mark_signin_needed
        mark_signin_needed(NETWORK_KEY, message)
        return TVEAuthError(message)

    def _viewlift_session(self, force: bool = False) -> dict:
        """A ViewLift user token that carries the TV-provider sign-in."""
        account = self._account()
        auth = (account.config or {}).get('nesn_mvpd_auth') or {}
        if not auth.get('client_id') or not auth.get('device_fingerprint'):
            raise self._sign_in_needed(
                'NESN TVE: not signed in yet — use "Sign in" for NESN under Settings → TV Everywhere.')
        cached = self.cache.get('nesn_session') or {}
        if (not force and cached.get('token') and cached.get('adobe_client_id') == auth['client_id']
                and cached.get('account_signed_in_at') == (self.config.get('nesn_account') or {}).get('signed_in_at')
                and float(cached.get('exp') or 0) - time.time() > _SESSION_MARGIN):
            return cached

        page = self._page_config()
        mso_id = auth['mso_id']
        client = self.adobe_client(page['software_statement'], auth)
        profile = self._adobe_profile(client, mso_id, account)
        if not profile:
            raise self._sign_in_needed(
                f'NESN TVE: the {mso_id} sign-in has expired — sign in again under Settings → TV Everywhere.')
        session = self.sign_in_viewlift(client, mso_id, profile, page)
        session['adobe_client_id'] = auth['client_id']
        self._update_cache('nesn_session', session)
        return session

    def sign_in_viewlift(self, client, mso_id: str, profile: dict, page: dict) -> dict:
        """Preauthorize NESN with Adobe, then link the Adobe sign-in to the NESN
        account the way watch.nesn.com's own client does."""
        r = client._post(
            f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/decisions/preauthorize/{mso_id}',
            json={'resources': page['resources']},
            headers={**client._bearer_headers(), 'Content-Type': 'application/json'},
        )
        decisions = {d.get('resource'): bool(d.get('authorized')) for d in (r.json() or {}).get('decisions') or []}
        if decisions and not any(decisions.values()):
            raise TVENotAuthorizedError(f'NESN TVE: {mso_id} doesn\'t include NESN.')

        api, headers = page['api_base'], self._vl_headers(page['api_key'])
        account_token, login = self._account_token()
        try:
            attrs = profile.get('attributes') or {}

            def attr(name):
                value = attrs.get(name)
                return (value.get('value') if isinstance(value, dict) else value) or ''

            variables = {'site': SITE, 'input': {
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
            raise TVEAuthError(f'NESN TVE: ViewLift sign-in failed: {exc}') from exc
        signin = (body.get('data') or {}).get('identitySignInTve')
        if body.get('errors') or not signin:
            raise TVEAuthError(f'NESN TVE: NESN refused to link the TV provider: {str(body.get("errors"))[:200]}')
        # Linked to the account; an empty token here means keep the account's own.
        token = signin.get('authorizationToken') or account_token
        return {'token': token, 'exp': _jwt_payload(token).get('exp') or time.time() + 7 * 86400,
                'decisions': decisions, 'mso_id': mso_id, 'account_signed_in_at': login.get('signed_in_at'),
                'cached_at': time.time()}

    # ── playback ─────────────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        # Validate entitlement, then hand back the opaque URL so the audit
        # skips manifest inspection — same pattern as versant_tve.py.
        if raw_url.startswith(SCHEME):
            self.resolve(raw_url)
        return raw_url

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported NESN TVE stream URL: {raw_url}')
        cid = raw_url[len(SCHEME):]
        page = self._page_config()
        ch = page['channels'].get(cid)
        if not ch:
            raise RuntimeError(f'NESN TVE: channel {cid} is not on watch.nesn.com/live any more.')

        playback = self.cache.get('nesn_playback') or {}
        cached = playback.get(cid) or {}
        if (cached.get('url') and time.time() - float(cached.get('cached_at', 0)) < _PLAYBACK_TTL
                and float(cached.get('license_exp') or 0) - time.time() > 2 * 3600):
            return cached['url']

        session = self._viewlift_session()
        status, body = self._entitlement(page, ch, session['token'])
        if status in (401, 403) and body.get('errorCode') != 'contentGeoRestricted':
            # A token ViewLift no longer honours, or a package change since it
            # was minted: re-mint it from the Adobe sign-in once.
            logger.info('[nesn_tve] entitlement HTTP %s (%s) for %s; renewing the ViewLift sign-in',
                        status, body.get('errorCode'), cid)
            session = self._viewlift_session(force=True)
            status, body = self._entitlement(page, ch, session['token'])
        if status == 403 and body.get('errorCode') == 'contentGeoRestricted':
            raise TVENotAuthorizedError(
                f'NESN TVE: NESN is only available to TV provider accounts in its New England market '
                f'(your {session.get("mso_id")} account is registered to ZIP {body.get("tveZip") or "unknown"}).')
        if status == 403 and body.get('errorCode') == 'TVE_SUBSCRIPTION_NOT_FOUND':
            raise TVENotAuthorizedError(f'NESN TVE: not entitled to {ch["title"]} ({body.get("errorMessage")}).')
        if status != 200 or not body.get('playable', True):
            raise RuntimeError(f'NESN TVE: entitlement for {cid} failed: HTTP {status} {body.get("errorCode") or ""}')

        assets = ((body.get('linearchannel') or {}).get('streamingInfo') or {}).get('videoAssets') or {}
        logger.info('[nesn_tve] entitlement for %s returned assets: %s', cid, sorted(assets))
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
                raise RuntimeError(f'NESN TVE: no playable stream returned for {cid} (assets: {sorted(assets)}).')
        playback = dict(playback)
        playback[cid] = {'url': url, 'license_token': token, 'license_url': license_url,
                         'license_exp': license_exp, 'cached_at': time.time()}
        self._update_cache('nesn_playback', playback)
        return url

    def _entitlement(self, page: dict, ch: dict, token: str) -> tuple[int, dict]:
        try:
            r = self.session.get(
                f'{page["api_base"]}/v3/entitlement/linearchannel',
                params={'id': page['linear_id'], 'channelId': ch['vl_id'], 'deviceType': 'web_browser',
                        'contentConsumption': 'web', 'ssaiDisable': 'false'},
                headers={'Authorization': token, 'x-api-key': page['api_key'], 'hdcp': 'HDCP-1.4',
                         'sl': 'SW_SECURE_CRYPTO', 'User-Agent': UA, 'Origin': ORIGIN, 'Referer': ORIGIN + '/'},
                timeout=20,
            )
        except requests.RequestException as exc:
            raise TVEAuthError(f'NESN TVE: could not reach ViewLift: {exc}') from exc
        try:
            return r.status_code, r.json() or {}
        except ValueError:
            return r.status_code, {}

    # ── DRM license proxy wiring ─────────────────────────────────────────────

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        # The license proxy merges source_cache into config, so the URL from
        # this channel's last entitlement answer is here once resolve() ran.
        cached = ((config.get('nesn_playback') or {}).get(channel_id or '') or {}).get('license_url')
        return cached or cls.license_url

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        # The per-channel token from the last entitlement answer (resolve()
        # caches it; the license proxy merges source_cache into config).
        headers = {'Content-Type': 'application/octet-stream', 'Origin': ORIGIN, 'Referer': ORIGIN + '/'}
        token = ((config.get('nesn_playback') or {}).get(channel_id or '') or {}).get('license_token')
        if token:
            headers['X-AxDRM-Message'] = token
        return challenge, headers

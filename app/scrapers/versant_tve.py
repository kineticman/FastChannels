"""
versant_tve.py — Versant's cable networks (the NBCUniversal spin-off): USA,
E!, Syfy and Oxygen (East + West), CNBC, MS NOW and Golf Channel.

None of these are in nbc.com's TVE backend (see nbc_tve.py). They all play
from usanetwork.com, a ViewLift ("AppCMS") site; syfy.com/oxygen.com/
eonline.com are aliases of the same ViewLift tenant and cnbc.com embeds its
player. Scoped 2026-10-17 — dev/versant/SCOPED.md has the full trace.

Key facts established by probing:

- usanetwork.com/live is server-rendered. `window.app_data` carries the
  tenant's API base, x-api-key and Adobe Pass settings (`tveSettings`):
  requestor `sparkmedia`, a v2 software statement, and one resource per
  brand (usa, e, syfy, oxygen, cnbc, msnbc, golf). `window.page_data` has one
  LINEARCHANNEL whose `channels[]` are all 11 feeds, each with its ViewLift
  id and a public XMLTV guide URL. No login is needed for any of it.
- `sparkmedia` is Adobe Pass v2 only — the legacy regcode API 401s — so the
  sign-in is the phone-link one (app/tve/link_login.py's _Versant), using the
  same AdobePassV2Client as NBC/TCM. One sign-in covers all 11 channels.
- ViewLift doesn't take an Adobe media token. Its web client trades the
  Adobe client's access token + device fingerprint + the authorized
  resources for a ViewLift user token (GraphQL `identitySignInTve`), and
  ViewLift checks the sign-in with Adobe itself. That token lasts 30 days,
  and can be re-minted from the saved Adobe sign-in at any time without the
  user, until the Adobe sign-in itself lapses (~90 days).
- `/v3/entitlement/linearchannel` returns, per channel, AWS MediaTailor URLs:
  FairPlay HLS, and Widevine/PlayReady CENC DASH (PSSH in the init segment).
  Every device type gets the same DRM assets — there is no clear rendition.
  The URLs carry no token; the license request does.
- License: POST usanetwork.api.viewlift.com/v1/license/widevine/acquire with
  `X-AxDRM-Message: <videoAssets.widevine.licenseToken>` (an HS256 JWT,
  ~12h, per channel). Keys rotate during playback, so the player keeps
  asking; prepare_license_request() adds the cached token every time.
- The manifest URL as returned redirects to one whose ad macros
  (ads.prof=VIEWLIFT_PROF, ...) are unfilled, and MediaTailor then passes the
  network's own feed straight through its breaks — everything stays
  encrypted, so a bridge never sees a clear/encrypted switch. (With the
  macros filled, MediaTailor splices in clear ad pods.)
- Guide: each channel's own XMLTV feed (Gracenote data, ~15 days). The West
  feeds are separate Gracenote stations — mostly a 3-hour delay, but live
  sports air at the same time on both — so each feed uses its own guide.
  The station id in that feed is the channel's Gracenote id.
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
from ..tve.adobe_pass import MvpdCooldownMixin, TVEAuthError, TVENotAuthorizedError

logger = logging.getLogger(__name__)

SCHEME = 'versant-tve://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'
LIVE_PAGE_URL = 'https://www.usanetwork.com/live'
ORIGIN = 'https://www.usanetwork.com'
ADOBE_BASE = 'https://sp.auth.adobe.com'
REQUESTOR_ID = 'sparkmedia'
# The redirect URI Adobe registered for this statement's client.
REDIRECT_URL = 'https://usanetwork.web.viewlift.com'
DOMAIN_NAME = 'usanetwork.web.viewlift.com'
SITE = 'usanetwork'
# Fallbacks for when the page can't be read; the page's own values win.
DEFAULT_API_BASE = 'https://usanetwork.api.viewlift.com'
DEFAULT_API_KEY = 'WX41iaJiOw7hJW8sNbDP5JpVwmjaH6t6y3xbQUsc'
WIDEVINE_LICENSE_URL = f'{DEFAULT_API_BASE}/v1/license/widevine/acquire'
# Where the sign-in status/errors are kept (app/tve/status.py, signin_notice).
NETWORK_KEY = 'versant'

# ViewLift channel title prefix -> (our id, display name, Adobe resource, category).
_BRANDS = {
    'usa': ('usa', 'USA Network', 'usa', 'Entertainment'),
    'e!': ('e', 'E!', 'e', 'Entertainment'),
    'syfy': ('syfy', 'Syfy', 'syfy', 'Entertainment'),
    'oxygen': ('oxygen', 'Oxygen True Crime', 'oxygen', 'Entertainment'),
    'cnbc': ('cnbc', 'CNBC', 'cnbc', 'News'),
    'msnow': ('msnow', 'MS NOW', 'msnbc', 'News'),
    'golf': ('golf', 'Golf Channel', 'golf', 'Sports'),
}

_PAGE_TTL = 12 * 3600
# Entitlement answers are reused this long; the license token in one lasts ~12h.
_PLAYBACK_TTL = 3600
# Mint a new ViewLift token this long before the current one expires.
_SESSION_MARGIN = 24 * 3600


def _channel_key(title: str) -> tuple[str, tuple] | None:
    """'USA-East' -> ('usa-east', brand), 'MSNOW' -> ('msnow', brand)."""
    base, _, feed = (title or '').strip().partition('-')
    brand = _BRANDS.get(base.strip().lower())
    if not brand:
        return None
    feed = feed.strip().lower()
    if feed in ('east', 'west'):
        return f'{brand[0]}-{feed}', brand
    return (brand[0], brand) if not feed else None


def _station_id(epg_url: str) -> str | None:
    """The Gracenote station id inside a ViewLift guide URL's `meta` param."""
    try:
        meta = parse_qs(urlsplit(epg_url).query)['meta'][0]
        source_url = json.loads(base64.b64decode(meta + '=' * (-len(meta) % 4)))['sourceUrl']
    except (KeyError, IndexError, ValueError, TypeError):
        return None
    m = re.search(r'/stations/(\d+)/', source_url)
    return m.group(1) if m else None


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


def _int(value) -> int | None:
    return int(value) if value and str(value).isdigit() else None


class VersantTVEScraper(MvpdCooldownMixin, BaseScraper):
    """USA, E!, Syfy, Oxygen, CNBC, MS NOW and Golf via ViewLift + Adobe Pass v2."""

    source_name = 'versant_tve'
    display_name = 'Versant TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True
    license_url = WIDEVINE_LICENSE_URL
    # Every channel is Widevine/PlayReady CENC DASH — see module docstring.
    all_channels_require_drm_bridge = True

    # ── page config / channel discovery ──────────────────────────────────────

    def _page_config(self) -> dict:
        cached = self.cache.get('versant_page') or {}
        if cached and time.time() - float(cached.get('cached_at', 0)) < _PAGE_TTL:
            return cached
        try:
            # Its origin throws the odd 503; self.session retries those.
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
                key = _channel_key(ch.get('title'))
                if not key:
                    logger.warning('[versant_tve] skipping unrecognised channel %r', ch.get('title'))
                    continue
                epg_url = (ch.get('epgInfo') or {}).get('vlUrl') or ''
                channels[key[0]] = {
                    'vl_id': ch['id'], 'title': ch.get('title'), 'logo': ch.get('thumbnail'),
                    'epg_url': epg_url, 'station_id': _station_id(epg_url),
                }
            config = {
                'requestor_id': tve['requestorID'],
                'software_statement': tve.get('softwareStatementV2') or tve['softwareStatement'],
                'resources': tve.get('resourceIds') or [],
                'api_base': api.get('apiBaseUrl') or DEFAULT_API_BASE,
                'api_key': m.group(1) if m else DEFAULT_API_KEY,
                'linear_id': linear['id'],
                'channels': channels,
                'cached_at': time.time(),
            }
            if config['requestor_id'] != REQUESTOR_ID or not config['software_statement'] or not channels:
                raise ValueError('unexpected usanetwork.com page config')
        except (requests.RequestException, ValueError, KeyError, TypeError, StopIteration, IndexError) as exc:
            if cached:
                logger.warning('[versant_tve] could not refresh page config, using the saved one: %s', exc)
                return cached
            raise RuntimeError(f'Versant TVE: could not read usanetwork.com/live: {exc}') from exc
        self._update_cache('versant_page', config)
        return config

    def fetch_channels(self) -> list[ChannelData]:
        out = []
        for cid, ch in self._page_config()['channels'].items():
            key = _channel_key(ch['title'])
            if not key:
                continue
            _, name, _, category = key[1]
            if cid.endswith('-east'):
                name += ' (East)'
            elif cid.endswith('-west'):
                name += ' (West)'
            out.append(ChannelData(
                source_channel_id=cid,
                name=name,
                slug=f'versant-{cid}',
                logo_url=ch.get('logo'),
                stream_url=f'{SCHEME}{cid}',
                stream_type='dash',
                category=category,
                language='en',
                country='US',
                guide_key=f'VERSANT:{cid.upper()}',
                description=None,
                gracenote_id=resolve_gracenote('versant_tve', upstream_id=ch.get('station_id'), lookup_key=cid),
            ))
        return out

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
            root = ET.fromstring(r.content)
        except (requests.RequestException, ET.ParseError) as exc:
            logger.warning('[versant_tve] guide fetch failed for %s: %s', cid, exc)
            return []
        programs = []
        for p in root.findall('programme'):
            start, end = _xmltv_time(p.get('start')), _xmltv_time(p.get('stop'))
            title = (p.findtext('title') or '').strip()
            if not start or not end or end <= start or not title:
                continue
            nums = {e.get('system'): (e.text or '').strip() for e in p.findall('episode-num')}
            sub_type = (p.findtext('sub-type') or '').strip().lower()
            episode_title = (p.findtext('sub-title') or '').strip() or None
            icon = p.find('icon')
            program_id = p.get('id') or ''
            programs.append(ProgramData(
                source_channel_id=cid,
                title=title,
                start_time=start,
                end_time=end,
                description=(p.findtext('desc') or '').strip() or None,
                poster_url=icon.get('src') if icon is not None else None,
                category=';'.join(c.text.strip() for c in p.findall('category') if c.text and c.text.strip()) or None,
                rating=(p.findtext('rating/value') or '').strip() or None,
                episode_title=episode_title if episode_title != title else None,
                season=_int(nums.get('season')),
                episode=_int(nums.get('episode')),
                program_type='movie' if program_id.startswith('MV') or sub_type == 'feature film'
                else ('episode' if program_id.startswith('EP') else None),
                # Gracenote TMS ids: EP/SH + 8-digit series root + 4 digits.
                series_id=program_id[2:10] if program_id[:2] in ('EP', 'SH') and len(program_id) >= 10 else None,
                episode_id=program_id or None,
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
                raise TVEAuthError(f'Versant TVE: could not reach Adobe: {exc}') from exc
            if r.status_code == 429 or r.status_code >= 500:
                raise TVEAuthError(f'Versant TVE: Adobe returned HTTP {r.status_code}; will retry.')
            return r

        r = fetch()
        if r.status_code == 401 and client.client_id and client.client_secret:
            client.refresh_access_token()
            cfg = dict(account.config or {})
            cfg['versant_mvpd_auth'] = {**(cfg.get('versant_mvpd_auth') or {}), 'access_token': client.access_token}
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
        auth = (account.config or {}).get('versant_mvpd_auth') or {}
        if not auth.get('client_id') or not auth.get('device_fingerprint'):
            raise self._sign_in_needed(
                'Versant TVE: not signed in yet — use "Sign in" for Versant under Settings → TV Everywhere.')
        cached = self.cache.get('versant_session') or {}
        if (not force and cached.get('token') and cached.get('adobe_client_id') == auth['client_id']
                and float(cached.get('exp') or 0) - time.time() > _SESSION_MARGIN):
            return cached

        page = self._page_config()
        mso_id = auth['mso_id']
        client = self.adobe_client(page['software_statement'], auth)
        profile = self._adobe_profile(client, mso_id, account)
        if not profile:
            raise self._sign_in_needed(
                f'Versant TVE: the {mso_id} sign-in has expired — sign in again under Settings → TV Everywhere.')
        session = self.sign_in_viewlift(client, mso_id, profile, page)
        session['adobe_client_id'] = auth['client_id']
        self._update_cache('versant_session', session)
        return session

    @staticmethod
    def sign_in_viewlift(client, mso_id: str, profile: dict, page: dict) -> dict:
        """Preauthorize every brand with Adobe, then trade the Adobe client
        token for a ViewLift user token the way usanetwork.com's own client does."""
        r = client._post(
            f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/decisions/preauthorize/{mso_id}',
            json={'resources': page['resources']},
            headers={**client._bearer_headers(), 'Content-Type': 'application/json'},
        )
        decisions = {d.get('resource'): bool(d.get('authorized')) for d in (r.json() or {}).get('decisions') or []}
        authorized = [res for res, ok in decisions.items() if ok]
        if not authorized:
            raise TVENotAuthorizedError(f'Versant TVE: {mso_id} doesn\'t include any Versant network.')

        api, headers = page['api_base'], {'User-Agent': UA, 'Origin': ORIGIN, 'Referer': ORIGIN + '/',
                                          'x-api-key': page['api_key']}
        device_id = f'browser-{uuid.uuid4()}'
        try:
            anon = requests.get(f'{api}/identity/anonymous-token', headers=headers, timeout=20,
                                params={'site': SITE, 'platform': 'web_browser', 'deviceId': device_id})
            anon.raise_for_status()
            anon_token = anon.json()['authorizationToken']
            attrs = profile.get('attributes') or {}

            def attr(name):
                value = attrs.get(name)
                return (value.get('value') if isinstance(value, dict) else value) or ''

            variables = {'site': SITE, 'input': {
                'mvpdProvider': mso_id, 'provider': mso_id, 'idpName': mso_id,
                'userId': _jwt_payload(anon_token).get('id'), 'email': '',
                'platform': 'web_browser', 'device': 'web_browser', 'deviceName': 'WEB', 'deviceId': device_id,
                'channelIds': authorized, 'tveUserId': attr('userID'),
                'adbAccessToken': client.access_token,
                'adbDeviceId': client.session.headers['ap-device-identifier'],
                'adobeMetadata': {'zip': None, 'encryptedZip': attr('zip')},
            }}
            query = ('mutation ($site: String!, $input: SignInTveInput!) { identitySignInTve(site: $site, '
                     'input: $input) { userId tveUserId provider authorizationToken refreshToken } }')
            r = requests.post(f'{api}/graphql', json={'query': query, 'variables': variables},
                              headers={**headers, 'authorization': anon_token}, timeout=30)
            body = r.json()
        except (requests.RequestException, ValueError, KeyError) as exc:
            raise TVEAuthError(f'Versant TVE: ViewLift sign-in failed: {exc}') from exc
        signin = (body.get('data') or {}).get('identitySignInTve') or {}
        token = signin.get('authorizationToken')
        if not token:
            raise TVEAuthError(f'Versant TVE: ViewLift refused the sign-in: {str(body.get("errors"))[:200]}')
        return {'token': token, 'exp': _jwt_payload(token).get('exp') or time.time() + 7 * 86400,
                'decisions': decisions, 'mso_id': mso_id, 'cached_at': time.time()}

    # ── playback ─────────────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        # Every channel is DRM; the CENC markers are in the DASH init segments,
        # not anything the generic audit reads. Validate entitlement, then hand
        # back the opaque URL so the audit skips manifest inspection — same
        # pattern as warner_tve.py / nbc_tve.py.
        if raw_url.startswith(SCHEME):
            self.resolve(raw_url)
        return raw_url

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported Versant TVE stream URL: {raw_url}')
        cid = raw_url[len(SCHEME):]
        page = self._page_config()
        ch = page['channels'].get(cid)
        key = _channel_key((ch or {}).get('title') or '')
        if not ch or not key:
            raise RuntimeError(f'Versant TVE: channel {cid} is not on usanetwork.com/live any more.')
        resource = key[1][2]

        playback = self.cache.get('versant_playback') or {}
        cached = playback.get(cid) or {}
        if (cached.get('url') and time.time() - float(cached.get('cached_at', 0)) < _PLAYBACK_TTL
                and float(cached.get('license_exp') or 0) - time.time() > 2 * 3600):
            return cached['url']

        session = self._viewlift_session()
        if session.get('decisions', {}).get(resource) is False:
            raise TVENotAuthorizedError(f'Versant TVE: {session.get("mso_id")} doesn\'t include {key[1][1]}.')
        status, body = self._entitlement(page, ch, session['token'])
        if status in (401, 403):
            # A token ViewLift no longer honours, or a package change since it
            # was minted: re-mint it from the Adobe sign-in once.
            logger.info('[versant_tve] entitlement HTTP %s (%s) for %s; renewing the ViewLift sign-in',
                        status, body.get('errorCode'), cid)
            session = self._viewlift_session(force=True)
            if session.get('decisions', {}).get(resource) is False:
                raise TVENotAuthorizedError(f'Versant TVE: {session.get("mso_id")} doesn\'t include {key[1][1]}.')
            status, body = self._entitlement(page, ch, session['token'])
        if status == 403 and body.get('errorCode') == 'TVE_SUBSCRIPTION_NOT_FOUND':
            raise TVENotAuthorizedError(f'Versant TVE: not entitled to {key[1][1]} ({body.get("errorMessage")}).')
        if status != 200 or not body.get('playable', True):
            raise RuntimeError(f'Versant TVE: entitlement for {cid} failed: HTTP {status} {body.get("errorCode") or ""}')

        assets = ((body.get('linearchannel') or {}).get('streamingInfo') or {}).get('videoAssets') or {}
        widevine = assets.get('widevine') or {}
        if not widevine.get('url') or not widevine.get('licenseToken'):
            raise RuntimeError(f'Versant TVE: no Widevine stream returned for {cid}.')
        token = widevine['licenseToken']
        playback = dict(playback)
        playback[cid] = {
            'url': widevine['url'],
            'license_token': token,
            'license_exp': _jwt_payload(token.split('|', 1)[-1]).get('exp') or time.time() + 6 * 3600,
            'cached_at': time.time(),
        }
        self._update_cache('versant_playback', playback)
        return widevine['url']

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
            raise TVEAuthError(f'Versant TVE: could not reach ViewLift: {exc}') from exc
        try:
            return r.status_code, r.json() or {}
        except ValueError:
            return r.status_code, {}

    # ── DRM license proxy wiring ─────────────────────────────────────────────

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        # The per-channel token from the last entitlement answer (resolve()
        # caches it; the license proxy merges source_cache into config).
        headers = {'Content-Type': 'application/octet-stream', 'Origin': ORIGIN, 'Referer': ORIGIN + '/'}
        token = ((config.get('versant_playback') or {}).get(channel_id or '') or {}).get('license_token')
        if token:
            headers['X-AxDRM-Message'] = token
        return challenge, headers

from __future__ import annotations

import base64
import json
import secrets
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlsplit

import requests

from .base import BaseScraper, ChannelData, ProgramData
from ..models import TVEAccount
from ..tve.adobe_pass import TVEAuthError, TVENotAuthorizedError


SCHEME = 'discovery-tve://'
API_BASE = 'https://us1-prod-direct.watch.hgtv.com'
CALLBACK_BASE = 'https://us1-prod.disco-api.com'
AUTH_HOST = 'https://auth.watch.hgtv.com'
WATCH_BASE = 'https://watch.hgtv.com'
BRAND_ID = '5af07ab86b66d16f0e095063'
PARTNER_ID = '55e9d01a6b66d1244474bbe5'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'
SESSION_CACHE_KEY = 'discovery_tve_session'
SESSION_TTL_SECONDS = 6 * 60 * 60


@dataclass(frozen=True)
class DiscoveryTVEChannel:
    channel_id: str
    code: str
    slug: str
    name: str
    package: str
    logo_url: str
    category: str = 'Lifestyle'

    @property
    def stream_url(self) -> str:
        return f'{SCHEME}{self.channel_id}'


CHANNELS: dict[str, DiscoveryTVEChannel] = {
    '161': DiscoveryTVEChannel('161', 'TLC', 'tlc', 'TLC', 'Tlc', 'https://us1-prod-images.disco-api.com/2020/08/06/0222b622-66b4-32f1-ab8b-0d2894a527d6.png'),
    '162': DiscoveryTVEChannel('162', 'FOOD', 'food-network', 'Food Network', 'FoodNetwork', 'https://us1-prod-images.disco-api.com/2020/08/06/6455539f-2683-30ed-9106-611f7f19d73d.png', 'Food'),
    '163': DiscoveryTVEChannel('163', 'DSC', 'discovery', 'Discovery', 'Discovery', 'https://us1-prod-images.disco-api.com/2020/08/06/1b0657af-8f5f-3aa1-9311-abf6c1daa616.png', 'Documentary'),
    '164': DiscoveryTVEChannel('164', 'DLF', 'discovery-life', 'Discovery Life', 'DiscoveryLife', 'https://us1-prod-images.disco-api.com/2022/03/01/4ee66450-e834-3f40-914f-972dd610e6da.png'),
    '165': DiscoveryTVEChannel('165', 'IDS', 'investigation-discovery', 'Investigation Discovery', 'InvestigationDiscovery', 'https://us1-prod-images.disco-api.com/2020/08/06/02a255a8-8590-3e46-827e-b1e56023aa14.png', 'Crime'),
    '166': DiscoveryTVEChannel('166', 'SCI', 'science', 'Science', 'Science', 'https://us1-prod-images.disco-api.com/2021/01/05/0f248bf0-5098-3b41-b449-d827a39a55c2.png', 'Science'),
    '167': DiscoveryTVEChannel('167', 'APL', 'animal-planet', 'Animal Planet', 'AnimalPlanet', 'https://us1-prod-images.disco-api.com/2020/08/06/9fb1c51d-66f4-3f99-a56c-82ff8a22950b.png', 'Animals'),
    '168': DiscoveryTVEChannel('168', 'OWN', 'own', 'OWN', 'OprahWinfrey', 'https://us1-prod-images.disco-api.com/2021/06/28/01252a56-9a48-3958-96db-bda2a0b24624.png', 'Entertainment'),
    '169': DiscoveryTVEChannel('169', 'DAM', 'destination-america', 'Destination America', 'DestinationAmerica', 'https://us1-prod-images.disco-api.com/2021/06/28/5d5761ac-7d32-35a1-bf21-8f5dbeea7329.png', 'Travel'),
    '170': DiscoveryTVEChannel('170', 'DTURBO', 'motortrend', 'Discovery Turbo', 'MotorTrend', 'https://us1-prod-images.disco-api.com/2026/01/22/ff592432-1c75-4270-8293-dd3ed42184b2.png', 'Auto'),
    '171': DiscoveryTVEChannel('171', 'AHC', 'ahc', 'AHC', 'AmericanHeroes', 'https://us1-prod-images.disco-api.com/2020/12/09/d1ff93db-ed4c-3111-b606-6a741154de51.png', 'Documentary'),
    '172': DiscoveryTVEChannel('172', 'HGTV', 'hgtv', 'HGTV', 'HomeAndGarden', 'https://us1-prod-images.disco-api.com/2020/08/06/859ddd5d-7e3d-3767-ab68-7a33cd99feed.png'),
    '174': DiscoveryTVEChannel('174', 'COOK', 'cooking-channel', 'Cooking Channel', 'Cooking', 'https://us1-prod-images.disco-api.com/2021/06/28/938e1261-cf65-3b6f-98bc-361f781032c5.png', 'Food'),
    '175': DiscoveryTVEChannel('175', 'TRAV', 'travel-channel', 'Travel Channel', 'Travel', 'https://us1-prod-images.disco-api.com/2020/11/04/90ab20ee-fa65-3983-ac62-f5590e9bf5d2.png', 'Travel'),
    '222': DiscoveryTVEChannel('222', 'MAG', 'magnolia-network-preview-atve-us', 'Magnolia Network', 'MagnoliaNetPreview', 'https://us1-prod-images.disco-api.com/2020/12/16/d3f28f6f-7991-3810-a5ea-754297661fe2.png'),
}


class _FormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_form = False
        self.action = ''
        self.inputs: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs) -> None:
        attrs = dict(attrs)
        if tag.lower() == 'form' and not self.in_form:
            self.in_form = True
            self.action = attrs.get('action') or ''
            return
        if self.in_form and tag.lower() == 'input':
            name = attrs.get('name')
            if name:
                self.inputs[name] = attrs.get('value') or ''

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == 'form' and self.in_form:
            self.in_form = False


def _hidden_form(document: str, base_url: str) -> tuple[str, dict[str, str]]:
    parser = _FormParser()
    parser.feed(document)
    if not parser.action and not parser.inputs:
        raise TVEAuthError('Expected Cox SAML form but none was found.')
    return urljoin(base_url, parser.action or base_url), parser.inputs


def channel_for_url(raw_url: str) -> DiscoveryTVEChannel | None:
    parsed = urlparse(raw_url or '')
    channel_id = parsed.netloc or parsed.path.lstrip('/').split('/')[0]
    if not raw_url.startswith(SCHEME):
        return None
    return CHANNELS.get(channel_id)


def _auth_device_info(device_id: str) -> str:
    return f'hgtv/4.5.0 (desktop/desktop; Windows/NT 10.0; {device_id}/6ed92d9d8e3518b46e90)'


def _playback_device_info(device_id: str) -> str:
    return f'hgtv/27.43.0 (desktop/desktop; unknown/unknown; {device_id})'


def _browser_headers() -> dict[str, str]:
    return {
        'User-Agent': UA,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': WATCH_BASE,
        'Referer': WATCH_BASE + '/',
        'x-disco-client': 'WEB:UNKNOWN:hgtv:27.43.0',
        'x-disco-params': 'realm=go,siteLookupKey=hgtv',
    }


def _auth_widget_headers(device_id: str, *, referer: str = AUTH_HOST + '/') -> dict[str, str]:
    return {
        'User-Agent': UA,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': AUTH_HOST,
        'Referer': referer,
        'x-disco-client': 'WEB:10:WEB_AUTH:0.37.1',
        'x-disco-params': 'realm=go,siteLookupKey=hgtv',
        'x-device-info': _auth_device_info(device_id),
    }


def _gauth_headers(device_id: str, *, referer: str = AUTH_HOST + '/') -> dict[str, str]:
    return {
        'User-Agent': UA,
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': AUTH_HOST,
        'Referer': referer,
        'x-device-info': _auth_device_info(device_id),
    }


def _jwt_exp(token: str) -> int | None:
    try:
        payload = token.split('.')[1]
        payload += '=' * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload.encode()).decode())
        return int(data.get('exp')) if data.get('exp') else None
    except Exception:
        return None


def _cookie_dict(session: requests.Session) -> dict[str, str]:
    return session.cookies.get_dict()


def _restore_cookies(session: requests.Session, cookies: dict[str, str]) -> None:
    for name, value in (cookies or {}).items():
        session.cookies.set(name, value, domain='.watch.hgtv.com', path='/')


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith('Z'):
            value = value[:-1] + '+00:00'
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _image_lookup(items: list[dict]) -> dict[str, str]:
    images: dict[str, str] = {}
    for item in items:
        if item.get('type') != 'image':
            continue
        attrs = item.get('attributes') or {}
        src = (attrs.get('src') or '').strip()
        if src:
            images[str(item.get('id'))] = src
    return images


def _first_image_url(item: dict, images: dict[str, str]) -> str | None:
    rel_images = (((item.get('relationships') or {}).get('images') or {}).get('data') or [])
    for rel in rel_images:
        url = images.get(str(rel.get('id')))
        if url:
            return url
    return None


def _cox_saml_login(session: requests.Session, cox_saml_url: str, username: str, password: str) -> str:
    session.get(cox_saml_url, allow_redirects=True, timeout=30)
    login_user = username.split('@', 1)[0] if username.lower().endswith('@cox.net') else username
    r = session.post(
        'https://login.cox.com/api/v1/authn',
        json={
            'username': login_user,
            'password': password,
            'options': {'warnBeforePasswordExpired': True, 'multiOptionalFactorEnroll': True},
        },
        headers={
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Origin': 'https://login.cox.com',
            'Referer': cox_saml_url,
            'x-okta-user-agent-extended': 'okta-signin-widget-3.8.2',
            'User-Agent': UA,
        },
        timeout=30,
    )
    r.raise_for_status()
    auth = r.json()
    if auth.get('status') != 'SUCCESS' or not auth.get('sessionToken'):
        raise TVEAuthError(f'Cox authn did not succeed: {auth.get("status") or "unknown"}')

    redirect_url = 'https://login.cox.com/login/sessionCookieRedirect?' + urlencode({
        'checkAccountSetupComplete': 'true',
        'token': auth['sessionToken'],
        'redirectUrl': cox_saml_url,
    })
    r = session.get(redirect_url, allow_redirects=True, timeout=30)
    r.raise_for_status()
    action, form = _hidden_form(r.text, str(r.url))
    if 'SAMLResponse' not in form:
        raise TVEAuthError('Cox SAML page did not include SAMLResponse')

    r = session.post(
        action,
        data=form,
        headers={
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://login.cox.com',
            'Referer': 'https://login.cox.com/',
            'User-Agent': UA,
        },
        allow_redirects=False,
        timeout=30,
    )
    if r.status_code not in {200, 301, 302, 303, 307, 308}:
        raise TVEAuthError(f'Adobe SAML consumer returned HTTP {r.status_code}')
    return r.headers.get('location') or str(r.url)


class DiscoveryTVEScraper(BaseScraper):
    source_name = 'discovery_tve'
    display_name = 'Discovery TVE'
    source_category = 'tve'
    under_development = True
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True

    def _session(self) -> requests.Session:
        session = requests.Session()
        self._configure_session(session)
        session.headers.update(_browser_headers())
        return session

    def _cached_session(self) -> requests.Session | None:
        cached = self.cache.get(SESSION_CACHE_KEY) or {}
        if not isinstance(cached, dict):
            return None
        if int(cached.get('expires_at') or 0) <= int(time.time()) + 300:
            return None
        cookies = cached.get('cookies') or {}
        if not isinstance(cookies, dict) or not cookies.get('st'):
            return None
        session = self._session()
        _restore_cookies(session, cookies)
        try:
            r = session.get(f'{API_BASE}/users/me', headers=_browser_headers(), timeout=15)
            if r.status_code == 200 and not (((r.json().get('data') or {}).get('attributes') or {}).get('anonymous')):
                return session
        except Exception:
            return None
        return None

    def _authenticate(self) -> requests.Session:
        account = TVEAccount.query.filter_by(provider_id='cox').first()
        if not account or not account.is_enabled or not account.has_credentials():
            raise TVEAuthError('Cox TVE credentials are not configured in Settings.')

        session = self._session()
        device_id = self.config.get('device_id') or str(uuid.uuid4())
        if not self.config.get('device_id'):
            self._update_config('device_id', device_id)

        # This anonymous token must be minted with x-device-info. Without it,
        # /login succeeds but the upgraded st later fails live playback with
        # access.denied.by.partner / "error missing device info".
        r = session.get(
            f'{API_BASE}/token',
            params={'realm': 'go', 'deviceId': device_id, 'shortlived': 'true'},
            headers=_auth_widget_headers(device_id),
            timeout=30,
        )
        r.raise_for_status()

        redirect_url = f'{AUTH_HOST}/gauth-sync'
        r = session.get(
            f'{API_BASE}/v1/gauth/authorize',
            params={
                'brand_id': BRAND_ID,
                'no_redirect': '1',
                'partner_id': PARTNER_ID,
                'redirect_url': redirect_url,
            },
            headers=_gauth_headers(device_id),
            timeout=30,
        )
        r.raise_for_status()
        target_url = (r.json().get('target_url') or '').strip()
        if not target_url:
            raise TVEAuthError('Discovery gauth authorize did not return target_url.')

        r = session.get(target_url, headers={'User-Agent': UA, 'Accept': 'text/html,*/*'}, allow_redirects=False, timeout=30)
        if r.status_code in {301, 302, 303, 307, 308}:
            r = session.get(r.headers.get('location') or target_url, headers={'User-Agent': UA, 'Accept': 'text/html,*/*'}, allow_redirects=False, timeout=30)
        cox_saml_url = r.headers.get('location') or ''
        if 'login.cox.com' not in cox_saml_url:
            raise TVEAuthError(f'Unexpected Adobe authenticate redirect host: {urlsplit(cox_saml_url).netloc}.')

        callback_url = _cox_saml_login(session, cox_saml_url, account.username or '', account.password or '')
        if CALLBACK_BASE not in callback_url:
            raise TVEAuthError(f'Unexpected Discovery callback host: {urlsplit(callback_url).netloc}.')
        r = session.get(callback_url, headers={'User-Agent': UA, 'Accept': 'text/html,*/*'}, allow_redirects=False, timeout=30)
        if r.status_code not in {301, 302, 303, 307, 308}:
            raise TVEAuthError(f'Discovery callback returned HTTP {r.status_code}.')
        code_url = r.headers.get('location') or ''
        code = (parse_qs(urlsplit(code_url).query).get('code') or [''])[0]
        if not code:
            raise TVEAuthError('Discovery callback did not return a gauth code.')

        r = session.get(
            f'{API_BASE}/v1/gauth/token',
            params={'code': code},
            headers=_gauth_headers(device_id, referer=f'{AUTH_HOST}/gauth-sync'),
            timeout=30,
        )
        r.raise_for_status()
        gauth_token = (r.json().get('token') or '').strip()
        if not gauth_token:
            raise TVEAuthError('Discovery gauth token exchange did not return a token.')

        r = session.post(
            f'{API_BASE}/login',
            json={'credentials': {'gauthToken': gauth_token, 'provider': 'gauth'}},
            headers={**_auth_widget_headers(device_id), 'Content-Type': 'application/json'},
            timeout=30,
        )
        if r.status_code >= 400:
            raise TVEAuthError(f'Discovery login returned HTTP {r.status_code}: {r.text[:300]}')
        r.raise_for_status()
        login_token = (((r.json().get('data') or {}).get('attributes') or {}).get('token') or '').strip()

        r = session.get(
            f'{API_BASE}/v1/gauth/authorized',
            headers={**_gauth_headers(device_id), 'Authorization': f'Bearer {gauth_token}'},
            timeout=20,
        )
        if r.status_code >= 400:
            raise TVEAuthError(f'Discovery entitlement check returned HTTP {r.status_code}: {r.text[:300]}')
        expires_at = _jwt_exp(login_token) or int(time.time()) + SESSION_TTL_SECONDS
        self._update_cache(SESSION_CACHE_KEY, {
            'cookies': _cookie_dict(session),
            'expires_at': min(expires_at, int(time.time()) + SESSION_TTL_SECONDS),
            'cached_at': int(time.time()),
        })
        return session

    def _authorized_session(self) -> requests.Session:
        return self._cached_session() or self._authenticate()

    def _refresh_short_token(self, session: requests.Session) -> None:
        playback_device_id = self.config.get('playback_device_id') or secrets.token_hex(16)
        if not self.config.get('playback_device_id'):
            self._update_config('playback_device_id', playback_device_id)
        r = session.get(
            f'{API_BASE}/token',
            params={'realm': 'go', 'deviceId': playback_device_id, 'shortlived': 'true'},
            headers={**_browser_headers(), 'x-device-info': _playback_device_info(playback_device_id)},
            timeout=20,
        )
        r.raise_for_status()

    def _prime_playback_context(self, session: requests.Session, channel: DiscoveryTVEChannel) -> None:
        for url, params in (
            (f'{API_BASE}/users/me', None),
            (f'{API_BASE}/cms/routes/channel/{channel.slug}', {'include': 'default', 'decorators': 'viewingHistory,isFavorite,playbackAllowed'}),
            (f'{API_BASE}/content/serverTime', None),
            (f'{API_BASE}/content/channels/{channel.channel_id}', {'include': 'images'}),
        ):
            r = session.get(url, params=params, headers=_browser_headers(), timeout=20)
            if r.status_code >= 500:
                r.raise_for_status()

    def fetch_channels(self):
        return [
            ChannelData(
                source_channel_id=channel.channel_id,
                name=channel.name,
                slug=f'{channel.slug}-tve',
                logo_url=channel.logo_url,
                stream_url=channel.stream_url,
                stream_type='hls',
                category=channel.category,
                language='en',
                country='US',
                guide_key=channel.code,
                description='Discovery-family TV Everywhere live stream authorized through the configured Cox account.',
            )
            for channel in CHANNELS.values()
        ]

    def _fetch_channel_listings(self, session: requests.Session, channel: DiscoveryTVEChannel) -> list[ProgramData]:
        r = session.get(
            f'{API_BASE}/cms/routes/channel/{channel.slug}',
            params={'include': 'default', 'decorators': 'viewingHistory,isFavorite,playbackAllowed'},
            headers=_browser_headers(),
            timeout=30,
        )
        r.raise_for_status()
        payload = r.json()
        included = payload.get('included') or []
        images = _image_lookup(included)
        programs: list[ProgramData] = []
        seen: set[tuple[datetime, datetime, str]] = set()
        for item in included:
            if item.get('type') != 'video':
                continue
            attrs = item.get('attributes') or {}
            if attrs.get('videoType') != 'LISTING':
                continue
            start = _parse_datetime(attrs.get('scheduleStart'))
            end = _parse_datetime(attrs.get('scheduleEnd'))
            if not start or not end or end <= start:
                continue
            title = (attrs.get('description') or attrs.get('name') or channel.name).strip()
            subtitle = (attrs.get('name') or '').strip()
            description = title
            if subtitle and subtitle != title:
                description = f'{title}: {subtitle}'
            key = (start, end, title + '\0' + subtitle)
            if key in seen:
                continue
            seen.add(key)
            rating = None
            ratings = attrs.get('contentRatings') or []
            if ratings and isinstance(ratings[0], dict):
                rating = ratings[0].get('code')
            programs.append(ProgramData(
                source_channel_id=channel.channel_id,
                title=title,
                description=description,
                start_time=start,
                end_time=end,
                category=channel.category,
                poster_url=_first_image_url(item, images),
                rating=rating,
                episode_title=subtitle or None,
                is_live=True,
            ))
        programs.sort(key=lambda p: p.start_time)
        return programs

    def fetch_epg(self, channels, **kwargs):
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        wanted = {ch.source_channel_id for ch in channels}
        session = self._authorized_session()
        programs: list[ProgramData] = []
        for channel_id, channel in CHANNELS.items():
            if channel_id not in wanted:
                continue
            try:
                listings = self._fetch_channel_listings(session, channel)
            except Exception:
                listings = []
            if listings:
                programs.extend(listings)
                continue
            programs.extend(
                ProgramData(
                    source_channel_id=channel.channel_id,
                    title=f'{channel.name} Live',
                    description=f'Live {channel.name} TV Everywhere programming.',
                    start_time=now + timedelta(hours=i),
                    end_time=now + timedelta(hours=i + 1),
                    category=channel.category,
                    is_live=True,
                )
                for i in range(24)
            )
        return programs

    def resolve(self, raw_url: str) -> str:
        channel = channel_for_url(raw_url)
        if not channel:
            raise ValueError(f'Unsupported Discovery TVE stream URL: {raw_url}')

        session = self._authorized_session()
        self._prime_playback_context(session, channel)
        self._refresh_short_token(session)
        device_id = self.config.get('device_id') or str(uuid.uuid4())
        payload = {
            'channelId': channel.channel_id,
            'deviceInfo': {
                'adBlocker': False,
                'drmSupported': True,
                'hdrCapabilities': ['SDR'],
                'hwDecodingCapabilities': [],
                'player': {'width': 1760, 'height': 1262},
                'screen': {'width': 1760, 'height': 1262},
                'soundCapabilities': ['STEREO'],
            },
            'wisteriaProperties': {
                'advertiser': {
                    'adId': '',
                    'firstPlay': 0,
                    'fwCcpa': '1YNN',
                    'fwDid': '',
                    'fwIsLat': 0,
                    'fwNielsenAppId': 'P0198F357-9506-45DD-839D-46195AFB42E3',
                    'gpaln': '',
                    'interactiveCapabilities': ['brightline'],
                },
                'appBundle': '',
                'device': {
                    'browser': {'name': 'chrome', 'version': '150.0.0.0'},
                    'language': 'en',
                    'make': '',
                    'model': '',
                    'name': 'Chrome',
                    'os': 'Windows',
                    'osVersion': 'NT 10.0',
                    'type': 'desktop',
                    'id': device_id,
                    'player': {'name': 'Discovery Player Web', 'version': '27.43.1'},
                },
                'gdpr': 0,
                'siteId': 'hgtv',
                'platform': 'desktop',
                'playbackId': str(uuid.uuid4()),
                'product': 'hgtv',
                'sessionId': str(uuid.uuid4()),
                'streamProvider': {'suspendBeaconing': 1, 'hlsVersion': 7, 'pingConfig': 1, 'version': '1.0.0'},
            },
        }
        r = session.post(
            f'{API_BASE}/playback/v3/channelPlaybackInfo',
            data=json.dumps(payload, separators=(',', ':')),
            headers={**_browser_headers(), 'Content-Type': 'application/json'},
            timeout=30,
        )
        if r.status_code in {401, 403}:
            self._update_cache(SESSION_CACHE_KEY, {})
            raise TVENotAuthorizedError(f'{channel.name}: Discovery denied playback entitlement HTTP {r.status_code}: {r.text[:300]}')
        r.raise_for_status()
        data = r.json()
        streams = (((data.get('data') or {}).get('attributes') or {}).get('streaming') or [])
        for stream in streams:
            if not isinstance(stream, dict):
                continue
            protection = stream.get('protection') or {}
            if protection.get('drmEnabled'):
                raise TVENotAuthorizedError(f'{channel.name}: Discovery returned a DRM-enabled stream.')
            url = (stream.get('url') or '').strip()
            if stream.get('type') == 'hls' and '.m3u8' in url:
                return url
        raise RuntimeError(f'Discovery playback response did not include HLS for {channel.name}.')

from __future__ import annotations

import base64
import html
import json
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, urljoin, urlparse, urlsplit
from zoneinfo import ZoneInfo

import requests

from .base import BaseScraper, ChannelData, ProgramData, null_placeholder_season_episode
from ..gracenote_map import resolve_gracenote
from ..models import TVEAccount
from ..tve.adobe_pass import (
    ADOBE_BASE,
    AdobePassCoxClient,
    MvpdCooldownMixin,
    TVEAuthError,
    TVENotAuthorizedError,
    _hidden_form,
    throttle_cox_login,
)


# AMC Networks TVE scraper.
#
# Validated direct-HLS family members: AMC, BBC America, IFC, and WE tv.
# Sundance TV is intentionally excluded: the current web app uses DICE/IMG and
# returns DRM-protected HLS, so it needs a separate EPG/DRM-bridge integration.

SCHEME = 'amcn-tve://'
API_BASE = 'https://gw.cds.amcn.com'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'
APP_VERSION = '3.79.1'
TENANT = 'amcn'
PLATFORM = 'web'
COUNTRY = 'us'
LANGUAGE = 'en-us'
MVPD = 'Cox'
TOKEN_SKEW_SECONDS = 300

AMCN_SOFTWARE_STATEMENTS = {
    'AMC': (
        'eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJkNmYxMDBkNi0zMjRjLTQxNjUtYWY2MS1kM2UxN2VjMjlhZDQiLCJuYmYiOjE3NjU4MjY0OTEsImlzcyI6ImF1dGguYWRvYmUuY29tIiwiaWF0IjoxNzY1ODI2NDkxfQ.'
        'd-7JK8FNYevE3rLp_grlKAaYw6rQIqlg11Et6RFNm17E8hhjdk6F58H_ETfToCM1w5nZJH-C5sj-pEas0xN5CjounNt6yFKjHDF20i1JaHYO81rbVqR5DJwDsQ-l0LCNZDUDy6mPp1cP01EdlUHpj4LPvqApQbER1iGsMgQnd-GZ-FKNFJ5q4QRPT9xrKuaqHApn06rbgmvDh7PF8lQz0z8XAg7C23h5qyIiYJtVV1owH4FSo9QXsEcozsdoSl51O4KSRfql0B5uQq9cPlTuW6-pbJwgSTz3Yoac-85Gmfjo8zsvacVzEDTX0l1Td5CK0FDV6YqD0R2pUJiwM9rJpA'
    ),
    'BBCA': (
        'eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJmZWI1YzM1OC1lOWE3LTQ5MmYtOWE4Yi02MWJlMWQ1MmNkM2QiLCJuYmYiOjE3NjU4MjY3MjYsImlzcyI6ImF1dGguYWRvYmUuY29tIiwiaWF0IjoxNzY1ODI2NzI2fQ.'
        'Ty_Q4uEGJwxC1tRXPjGTp4rIizJ8JYvEElxtpu2k1hNb06VgHwGDD2b0h6AH2syQVk_ddmuD3E2jkrJdGZU2JxF1No2s8NMHgCibIbw2WEpz3QwYKoD_5ZqgD6UBHXOgCaE7VJ63aqk-_801jOCuAgMuU4i2qzuUaX97I0bBxFRWW1wwWjbYqOYMyXcxFqq8MT4mbrmbNScjtffVf050pWH1ccNyfq6Eb7bK2SrlQaeL350pSBNDsi0ZJ871hxmROLfLvRz4LZV5wc_swsornu__S3WiTL0pNTbspreuz_fLjuVlnYxFDudDEV4dDiTx1_2JHrm5PD1xWGNqfgBvZw'
    ),
    'IFC': (
        'eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIyNWUwYTBlOS04ZTg3LTQ5ZjAtYjJjNC1kNGZiNGU0NjllMmMiLCJuYmYiOjE3NjU4MjY2ODgsImlzcyI6ImF1dGguYWRvYmUuY29tIiwiaWF0IjoxNzY1ODI2Njg4fQ.'
        'g7Dz1xokA6xR5JSxiZiuNBDUZyhrjztihfBI_RfuE4-164IS3b-Tf2PQXU9xVOOSUfW64ERkvchOAMLMItMmXJwJJ-kFc8AVmP8_iyPNGqVZiZbaNdhHYAEKh6-gmfDk-TZHfmdeIXpBZdvPMizKf0zPlBTe3tvM5hX0i3selA2XykES86CKeIctnUe9ID8eikQgmnWmP_nhdiIuhhgYbbqTuRSuN5xyz33x6SMlxaUquliDYkGfNtGI7KXY3HesRrS8wBOwL7-Y5CFe1ywg4eC6fPA_IQZOXMvQ3fDtKucLUyNqMjlB9ETqnOMlnwIKWZKAl0EYSFaOsplHQniCpA'
    ),
    'WETV': (
        'eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI5ZDM5NjkwYS0yNzVlLTRhMWQtOGE2OS1hY2VmY2QyNjBlZmEiLCJuYmYiOjE3NjU4MjY3NjYsImlzcyI6ImF1dGguYWRvYmUuY29tIiwiaWF0IjoxNzY1ODI2NzY2fQ.'
        'PJZafwmm4Uc8jqwAaKWufiV9SBqY7Xp_4o_6Jidp1aOuGlERepsomoVggvhllQm-iX1NfwBX4PS5uTyGrn8YVr0Cj6SQ-qvGSl96hr5qO-Cz6HlsbIV95FDsD3uSmtl3sdfxb6L41Mp-nGyeT3OGAivzTPtjNop1eOYN8UvDpdDdv_g0l6sVSiJsQd4O6t7DBdf8nl3he83CRjS_jh7j7Gv49UyzMSxIpuo59rRHhwTaIf36-514jR7VjBZuho-ZefuFxI_S8WbnI67a8d_QQUq9Cf-91HwuvEBitGBx4MRZPGABzmmSKtfA2aDuhyGRVsrZKNsJFek-hVQrl307sQ'
    ),
}
AMCN_SOFTWARE_STATEMENT = AMCN_SOFTWARE_STATEMENTS['AMC']

X_DEVICE_INFO = base64.b64encode(
    b'platform=Windows;mobile=false;brands=Not;A=Brand/8|Chromium/150|Google Chrome/150;'
    b'lang=en-US;dpr=1;screen=1760x1262;cores=12;touch=10'
).decode()


@dataclass(frozen=True)
class AMCNChannel:
    channel_id: str
    name: str
    slug: str
    requestor_id: str
    domain: str
    service_group_id: str
    livestream_id: str
    logo_url: str
    category: str = 'Entertainment'
    enabled: bool = True

    @property
    def stream_url(self) -> str:
        return f'{SCHEME}{self.channel_id}'

    @property
    def page_origin(self) -> str:
        return f'https://www.{self.domain}'

    @property
    def live_url(self) -> str:
        return f'{self.page_origin}/live'

    @property
    def schedule_url(self) -> str:
        return f'{self.page_origin}/schedule/'


CHANNELS: dict[str, AMCNChannel] = {
    'amc': AMCNChannel(
        channel_id='amc',
        name='AMC',
        slug='amc',
        requestor_id='AMC',
        domain='amc.com',
        service_group_id='1',
        livestream_id='6398764467112',
        logo_url='https://cdn.amcnetworks.com/amc/theme/web/amc_logo_bk_bg.png',
    ),
    'bbca': AMCNChannel(
        channel_id='bbca',
        name='BBC America',
        slug='bbca',
        requestor_id='BBCA',
        domain='bbcamerica.com',
        service_group_id='6',
        livestream_id='6398760324112',
        logo_url='https://cdn.amcnetworks.com/default-imgs/bbca/bbca-square.jpg',
    ),
    'ifc': AMCNChannel(
        channel_id='ifc',
        name='IFC',
        slug='ifc',
        requestor_id='IFC',
        domain='ifc.com',
        service_group_id='7',
        livestream_id='6398760311112',
        logo_url='https://cdn.amcnetworks.com/default-imgs/ifc/ifc-square.jpg',
    ),
    'wetv': AMCNChannel(
        channel_id='wetv',
        name='WE tv',
        slug='wetv',
        requestor_id='WETV',
        domain='wetv.com',
        service_group_id='9',
        livestream_id='6398685705112',
        logo_url='https://cdn.amcnetworks.com/default-imgs/wetv/wetv-square-2024.jpg',
    ),
}


def channel_for_url(raw_url: str) -> AMCNChannel | None:
    parsed = urlparse(raw_url or '')
    channel_id = parsed.netloc or parsed.path.lstrip('/').split('/')[0]
    if not raw_url.startswith(SCHEME):
        return None
    return CHANNELS.get(channel_id)


def _b64(value: str) -> str:
    return base64.b64encode(value.encode()).decode()


def _jwt_payload(token: str) -> dict:
    try:
        payload = token.split('.')[1]
        payload += '=' * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload.encode()).decode())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _jwt_exp(token: str) -> int | None:
    exp = _jwt_payload(token).get('exp')
    try:
        return int(exp)
    except (TypeError, ValueError):
        return None


def _cache_hash(channel: AMCNChannel, entitlement: str, feature_flags: str | None = None) -> str:
    value = f't={TENANT};n={channel.slug};p={PLATFORM};e={entitlement};c={COUNTRY};l={LANGUAGE};av={APP_VERSION}'
    if feature_flags:
        value += f';ff={feature_flags}'
    return _b64(value)


def _user_cache_hash(device_id: str, *, adobe_id: str = '', feature_flags: str | None = None) -> str:
    parts = []
    if adobe_id:
        parts.append(f'a={adobe_id}')
    parts.extend([f'd={device_id}', f'c={COUNTRY}', f'l={LANGUAGE}', f'av={APP_VERSION}'])
    if feature_flags:
        parts.append(f'ff={feature_flags}')
    return _b64(';'.join(parts))


def _amcn_headers(
    channel: AMCNChannel,
    *,
    token: str = '',
    device_id: str,
    entitlement: str = 'unauth',
    adobe_id: str = '',
    mvpd: str = '',
    feature_flags: str | None = None,
    cache_hash: str | None = None,
    user_cache_hash: str | None = None,
) -> dict[str, str]:
    headers = {
        'User-Agent': UA,
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Content-Type': 'application/json',
        'Origin': channel.page_origin,
        'Referer': channel.page_origin + '/',
        'x-amcn-adobe-id': adobe_id,
        'x-amcn-app-version': APP_VERSION,
        'x-amcn-country': COUNTRY,
        'x-amcn-device-id': device_id,
        'x-amcn-device-ad-id': device_id,
        'x-amcn-language': LANGUAGE,
        'x-amcn-mvpd': mvpd,
        'x-amcn-network': channel.slug,
        'x-amcn-platform': PLATFORM,
        'x-amcn-service-group-id': channel.service_group_id,
        'x-amcn-service-id': channel.slug,
        'x-amcn-tenant': TENANT,
        'x-amcn-audience-id': '',
        'x-ccpa-do-not-sell': 'default',
        'x-amcn-cache-hash': cache_hash or _cache_hash(channel, entitlement, feature_flags),
        'x-amcn-user-cache-hash': user_cache_hash or _user_cache_hash(device_id, adobe_id=adobe_id, feature_flags=feature_flags),
    }
    if token:
        headers['Authorization'] = f'Bearer {token}'
    return headers


def _adobe_bearer_headers(bearer: str, channel: AMCNChannel, device_id: str) -> dict[str, str]:
    return {
        'User-Agent': UA,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Authorization': f'Bearer {bearer}',
        'Origin': channel.page_origin,
        'Referer': channel.page_origin + '/',
        'ap-device-identifier': 'fingerprint ' + _b64(device_id),
        'x-device-info': X_DEVICE_INFO,
    }


def _adobe_headers(client: AdobePassCoxClient, channel: AMCNChannel, device_id: str) -> dict[str, str]:
    return _adobe_bearer_headers(client.ctx.access_token, channel, device_id)


def _clean(text: str | None) -> str | None:
    if not text:
        return None
    text = html.unescape(str(text)).replace('\xa0', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    return text or None


def _parse_local_time(date_value: str | None, time_value: str | None) -> datetime | None:
    if not date_value or not time_value:
        return None
    value = f'{date_value} {time_value}'.strip().lower()
    for fmt in ('%Y-%m-%d %I:%M%p', '%Y-%m-%d %I%p'):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=ZoneInfo('America/New_York')).astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def _season_episode(value: str | None) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    m = re.search(r'S\s*(\d+)\s+E\s*(\d+)', value, re.I)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def _find_nodes_by_type(root, node_type: str) -> list[dict]:
    found: list[dict] = []
    if isinstance(root, dict):
        if root.get('type') == node_type:
            found.append(root)
        for value in root.values():
            found.extend(_find_nodes_by_type(value, node_type))
    elif isinstance(root, list):
        for value in root:
            found.extend(_find_nodes_by_type(value, node_type))
    return found


def _parse_schedule_payload(channel: AMCNChannel, payload: dict) -> list[ProgramData]:
    programs: list[ProgramData] = []
    seen: set[tuple[datetime, datetime, str]] = set()
    for node in _find_nodes_by_type(payload, 'tv-listings'):
        props = node.get('properties') or {}
        for item in props.get('scheduleItems') or []:
            text = ((item.get('properties') or {}).get('text') or {}) if isinstance(item, dict) else {}
            if not isinstance(text, dict):
                continue
            start = _parse_local_time(text.get('startDate'), text.get('startTime'))
            end = _parse_local_time(text.get('endDate'), text.get('endTime'))
            if not start or not end or end <= start:
                continue
            title = _clean(text.get('showTitle')) or channel.name
            episode_title = _clean(text.get('episodeTitle'))
            season, episode = _season_episode(_clean(text.get('episodeSeasonNumber')))
            key = (start, end, title + '\0' + (episode_title or ''))
            if key in seen:
                continue
            seen.add(key)
            programs.append(ProgramData(
                source_channel_id=channel.channel_id,
                title=title,
                description=_clean(text.get('description')),
                start_time=start,
                end_time=end,
                category=channel.category,
                episode_title=episode_title if episode_title and episode_title != title else None,
                season=season,
                episode=episode,
                is_live=True,
            ))
    programs.sort(key=lambda p: p.start_time)
    return programs


def _extract_schedule_payload(html_text: str) -> dict | None:
    idx = html_text.find('{"type":"tv-listings","properties":')
    if idx < 0:
        return None
    try:
        node, _ = json.JSONDecoder().raw_decode(html_text[idx:])
    except ValueError:
        return None
    return {'data': node}


def _find_hls_source(root) -> str | None:
    if isinstance(root, dict):
        url = (root.get('src') or root.get('url') or root.get('href') or '').strip()
        source_type = (root.get('type') or root.get('mimeType') or root.get('mime_type') or '').lower()
        if '.m3u8' in url and ('mpegurl' in source_type or 'm3u8' in source_type or not source_type):
            return url
        for key in ('sources', 'playbackJsonData', 'video', 'data', 'items'):
            found = _find_hls_source(root.get(key))
            if found:
                return found
        for value in root.values():
            found = _find_hls_source(value)
            if found:
                return found
    elif isinstance(root, list):
        for item in root:
            found = _find_hls_source(item)
            if found:
                return found
    return None


def _placeholder_epg(channel: AMCNChannel) -> list[ProgramData]:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return [
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
    ]


def _extract_user_token(response: requests.Response) -> str:
    cookie_token = response.cookies.get('user_token') or ''
    if cookie_token:
        return cookie_token
    patterns = [
        r'user_token=([^";\s]+)',
        r'"user_token":"([^"]+)"',
    ]
    for pattern in patterns:
        m = re.search(pattern, response.text)
        if m:
            return html.unescape(m.group(1))
    return ''


def _cox_saml_login(session: requests.Session, cox_saml_url: str, username: str, password: str) -> str:
    session.get(cox_saml_url, allow_redirects=True, timeout=30)
    throttle_cox_login()
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
            'x-okta-user-agent-extended': 'okta-signin-widget-5.16.1',
            'User-Agent': UA,
        },
        timeout=30,
    )
    r.raise_for_status()
    auth = r.json()
    if auth.get('status') != 'SUCCESS' or not auth.get('sessionToken'):
        raise TVEAuthError(f'Cox authn did not succeed: {auth.get("status") or "unknown"}.')

    redirect_url = 'https://login.cox.com/login/sessionCookieRedirect?' + urlencode({
        'checkAccountSetupComplete': 'true',
        'token': auth['sessionToken'],
        'redirectUrl': cox_saml_url,
    })
    r = session.get(redirect_url, allow_redirects=True, timeout=30)
    r.raise_for_status()
    action, form = _hidden_form(r.text, str(r.url))
    if 'SAMLResponse' not in form:
        raise TVEAuthError('Cox SAML page did not include SAMLResponse.')

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
        raise TVEAuthError(f'Adobe SAML consumer returned HTTP {r.status_code}.')
    return r.headers.get('location') or str(r.url)


class AMCNetworksTVEScraper(MvpdCooldownMixin, BaseScraper):
    source_name = 'amcn_tve'
    source_aliases = ('amc_tve', 'amcnetworks_tve')
    display_name = 'AMC Networks TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True

    def fetch_channels(self):
        return [
            ChannelData(
                source_channel_id=channel.channel_id,
                name=channel.name,
                slug=f'{channel.channel_id}-tve',
                logo_url=channel.logo_url,
                stream_url=channel.stream_url,
                stream_type='hls',
                category=channel.category,
                language='en',
                country='US',
                guide_key=channel.requestor_id,
                description=None,
                gracenote_id=resolve_gracenote('amcn_tve', lookup_key=channel.channel_id),
            )
            for channel in CHANNELS.values() if channel.enabled
        ]

    def _session(self) -> requests.Session:
        session = requests.Session()
        self._configure_session(session)
        session.headers.update({
            'User-Agent': UA,
            'Accept': 'application/json,text/html,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        return session

    def _device_id(self) -> str:
        device_id = (self.config.get('device_id') or '').strip()
        if not device_id:
            device_id = str(uuid.uuid4())
            self._update_config('device_id', device_id)
        return device_id

    def _bootstrap_token(self, session: requests.Session, channel: AMCNChannel) -> tuple[str, str, str | None]:
        cached = self.cache.get(f'bootstrap:{channel.channel_id}') or {}
        if isinstance(cached, dict):
            token = (cached.get('token') or '').strip()
            exp = int(cached.get('exp') or 0)
            if token and exp > int(time.time()) + TOKEN_SKEW_SECONDS:
                return token, (cached.get('device_id') or self._device_id()).strip(), cached.get('feature_flags')

        r = session.get(channel.schedule_url, params={'tz': 'ET'}, headers={'Referer': channel.page_origin + '/'}, timeout=30)
        r.raise_for_status()
        token = _extract_user_token(r)
        token_data = {}
        if token:
            payload = _jwt_payload(token)
            token_data = {
                'token': token,
                'device_id': (payload.get('device-id') or self._device_id()).strip(),
                'feature_flags': payload.get('feature_flags'),
                'exp': _jwt_exp(token) or int(time.time()) + 3600,
            }
        else:
            device_id = self._device_id()
            unauth = session.post(
                f'{API_BASE}/auth-orchestration-id/api/v1/unauth',
                json={},
                headers=_amcn_headers(channel, device_id=device_id),
                timeout=30,
            )
            unauth.raise_for_status()
            data = unauth.json().get('data') or {}
            token = (data.get('access_token') or '').strip()
            if not token:
                raise TVEAuthError(f'{channel.name}: AMCN unauth did not return a bearer token.')
            payload = _jwt_payload(token)
            token_data = {
                'token': token,
                'refresh_token': (data.get('refresh_token') or '').strip(),
                'cache_hash': data.get('cache_hash'),
                'user_cache_hash': data.get('user_cache_hash'),
                'device_id': (payload.get('device-id') or device_id).strip(),
                'feature_flags': payload.get('feature_flags'),
                'exp': _jwt_exp(token) or int(time.time()) + 3600,
            }
        self._update_cache(f'bootstrap:{channel.channel_id}', token_data)
        return token_data['token'], token_data['device_id'], token_data.get('feature_flags')

    def _fetch_schedule_day(self, session: requests.Session, channel: AMCNChannel, day) -> list[ProgramData]:
        page = session.get(
            channel.schedule_url,
            params={'tz': 'ET', 'from': day.isoformat()},
            headers={'Referer': channel.page_origin + '/'},
            timeout=30,
        )
        page.raise_for_status()
        payload = _extract_schedule_payload(page.text)
        if payload:
            parsed = _parse_schedule_payload(channel, payload)
            if parsed:
                return parsed

        token, device_id, _feature_flags = self._bootstrap_token(session, channel)
        r = session.get(
            f'{API_BASE}/content-compiler-cr/api/v1/content/{TENANT}/{channel.slug}/path/schedule/',
            params={'tz': 'ET', 'from': day.isoformat()},
            headers=_amcn_headers(channel, token=token, device_id=device_id, entitlement='unauth'),
            timeout=30,
        )
        if r.status_code == 401:
            self._update_cache(f'bootstrap:{channel.channel_id}', {})
        r.raise_for_status()
        return _parse_schedule_payload(channel, r.json())

    def fetch_epg(self, channels, **kwargs):
        wanted = {ch.source_channel_id for ch in channels}
        total = sum(1 for channel_id in CHANNELS if channel_id in wanted)
        done = 0
        local_today = datetime.now(ZoneInfo('America/New_York')).date()
        session = self._session()
        programs: list[ProgramData] = []
        for channel_id, channel in CHANNELS.items():
            if channel_id not in wanted:
                continue
            parsed: list[ProgramData] = []
            for offset in range(0, 3):
                try:
                    parsed.extend(self._fetch_schedule_day(session, channel, local_today + timedelta(days=offset)))
                except Exception:
                    continue
            programs.extend(parsed or _placeholder_epg(channel))
            done += 1
            if self._progress_cb:
                self._progress_cb('epg', done, total)
        null_placeholder_season_episode(programs)
        return programs

    def _amcn_software_statement(self, channel: AMCNChannel, account: TVEAccount) -> str:
        cfg = account.config or {}
        statements = cfg.get('amcn_software_statements') if isinstance(cfg.get('amcn_software_statements'), dict) else {}
        return (
            cfg.get(f'amcn_software_statement_{channel.requestor_id.lower()}')
            or statements.get(channel.requestor_id)
            or cfg.get('amcn_software_statement')
            or AMCN_SOFTWARE_STATEMENTS.get(channel.requestor_id)
            or AMCN_SOFTWARE_STATEMENT
        ).strip()

    def _adobe_session_redirect(
        self, channel: AMCNChannel, software_statement: str, device_id: str, mso_id: str,
    ) -> tuple[AdobePassCoxClient, str, str, dict[str, str], requests.Response]:
        """Registers an Adobe Pass v2 client and starts a session for `mso_id`.

        Returns (client, code, mso_login_url, auth_headers, page_response)
        without completing any login — this is the scripted part that's
        never blocked by an MSO's bot defense. `page_response` is the same
        response `mso_login_url` was derived from — for MSOs that get a real
        redirect it's just the raw 3xx response (unused), but DIRECTV's own
        backend needs its body directly since DIRECTV never redirects here
        at all (see app/tve/mvpd/directv.py's directv_login() docstring).
        """
        client = AdobePassCoxClient(
            requestor_id=channel.requestor_id,
            resource=channel.requestor_id,
            software_statement=software_statement,
            redirect_url=channel.live_url,
        )
        client.setup_client()
        auth_headers = _adobe_headers(client, channel, device_id)

        r = client.session.post(
            f'{ADOBE_BASE}/api/v2/{channel.requestor_id}/sessions',
            data={
                'mvpd': mso_id,
                'domainName': f'https://{channel.domain}',
                'redirectUrl': channel.live_url,
            },
            headers={**auth_headers, 'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30,
        )
        if not r.ok:
            try:
                detail = r.json().get('message') or r.text[:300]
            except ValueError:
                detail = r.text[:300]
            # "integration...does not exist or is disabled" is Adobe's own
            # definitive rejection (confirmed live 2026-08-05: Sling isn't
            # registered for AMC Networks at all) — not a transient failure,
            # so the audit should disable these instead of treating it as a
            # possibly-temporary error. Any other session-request failure
            # (network blip, Adobe outage) stays generic/non-disabling.
            if 'does not exist or is disabled' in detail.lower():
                raise TVENotAuthorizedError(f'{channel.name}: {mso_id} is not a participating provider for AMC Networks: {detail}')
            raise TVEAuthError(f'{channel.name}: Adobe session request failed for MVPD {mso_id}: {detail}')
        session_data = r.json()
        code = (session_data.get('code') or '').strip()
        auth_path = (session_data.get('url') or '').strip()
        if not code or not auth_path:
            raise TVEAuthError(f'{channel.name}: Adobe session did not return an auth code.')

        r = client.session.get(
            urljoin(ADOBE_BASE, auth_path),
            headers={**auth_headers, 'Accept': 'text/html,*/*'},
            allow_redirects=False,
            timeout=30,
        )
        if r.status_code in {301, 302, 303, 307, 308}:
            mso_login_url = r.headers.get('location') or ''
        else:
            r.raise_for_status()
            mso_login_url = r.url
        if not mso_login_url:
            raise TVEAuthError(f'{channel.name}: Adobe did not return an MVPD login redirect.')
        return client, code, mso_login_url, auth_headers, r

    def _adobe_decision_finish(
        self, session: requests.Session, channel: AMCNChannel, code: str, mso_id: str, auth_headers: dict[str, str],
    ) -> tuple[str, str, int | None]:
        """The poll-by-code + entitlement-decision tail. Cheap (two small JSON
        calls, no MSO round trip) — safe to call repeatedly against the same
        `code`/bearer once the MSO login itself has completed (see
        _adobe_decision_token's cheap-redecision path)."""
        profile = session.get(
            f'{ADOBE_BASE}/api/v2/{channel.requestor_id}/profiles/code/{code}',
            headers={**auth_headers, 'Content-Type': 'application/json'},
            timeout=30,
        )
        profile.raise_for_status()
        mso_profile = ((profile.json().get('profiles') or {}).get(mso_id) or {})
        attrs = mso_profile.get('attributes') or {}
        adobe_id = (((attrs.get('userID') or {}).get('value')) or '').strip()
        if not adobe_id:
            raise TVEAuthError(f'{channel.name}: Adobe profile did not include a {mso_id} userID.')

        decision = session.post(
            f'{ADOBE_BASE}/api/v2/{channel.requestor_id}/decisions/authorize/{mso_id}',
            json={'resources': [channel.requestor_id]},
            headers={**auth_headers, 'Content-Type': 'application/json'},
            timeout=30,
        )
        decision.raise_for_status()
        decisions = decision.json().get('decisions') or []
        authorized = next((item for item in decisions if item.get('authorized') is True), None)
        token_obj = (authorized or {}).get('token') or {}
        serialized = token_obj.get('serializedToken')
        if not serialized:
            raise TVENotAuthorizedError(f'{channel.name}: Adobe did not authorize {channel.requestor_id} for {mso_id}.')
        return serialized, adobe_id, token_obj.get('notAfter')

    def _adobe_auth_cache_key(self, channel: AMCNChannel) -> str:
        return f'adobe_auth:{channel.requestor_id}'

    def _cached_adobe_auth(self, channel: AMCNChannel, mso_id: str) -> tuple[str, str] | None:
        cached = self.cache.get(self._adobe_auth_cache_key(channel))
        if not isinstance(cached, dict):
            return None
        if cached.get('mso_id') != mso_id or not cached.get('adobe_token') or not cached.get('adobe_id'):
            return None
        if int(cached.get('expires_at') or 0) <= int(time.time()) + TOKEN_SKEW_SECONDS:
            return None
        return cached['adobe_token'], cached['adobe_id']

    def _save_adobe_auth_cache(
        self, channel: AMCNChannel, mso_id: str, adobe_token: str, adobe_id: str, notafter_ms: int | None = None,
    ) -> None:
        # The "adobeShortMediaToken" is genuinely short-lived — Adobe's own
        # decision response embeds a real notAfter (confirmed live 2026-08-14:
        # exactly 300s after issuance, "<ttl>300000</ttl>" inside the token's
        # own serialized XML). It is NOT a JWT, so _jwt_exp() always returned
        # None for it and this used to silently fall back to a guessed 24h
        # TTL — meaning every resolve() more than ~5 minutes after the last
        # would pass this bogus-still-valid cached token to AMCN's playback
        # API, get a fast HTTP 400 "TOKEN_EXPIRED", and only THEN fall
        # through to a full MVPD re-login inside the request (~13-19s for
        # Xfinity's cookie-jar login / Cox SAML) — long enough that real
        # players gave up before the 302 ever arrived, even though resolve()
        # itself eventually succeeded. Pass the decision response's own
        # notAfter (ms) so the cache reflects the token's real ~5min life;
        # _adobe_decision_token's cheap-redecision path (below) is what
        # actually keeps resolve() fast despite that short life.
        now = int(time.time())
        if notafter_ms:
            expires_at = int(notafter_ms) // 1000
        else:
            expires_at = _jwt_exp(adobe_token) or (now + 86400)
        self._update_cache(self._adobe_auth_cache_key(channel), {
            'mso_id': mso_id, 'adobe_token': adobe_token, 'adobe_id': adobe_id,
            'cached_at': now, 'expires_at': expires_at,
        })

    def _adobe_session_cache_key(self, channel: AMCNChannel) -> str:
        return f'adobe_session:{channel.requestor_id}'

    def _cached_adobe_session(self, channel: AMCNChannel, mso_id: str) -> dict | None:
        cached = self.cache.get(self._adobe_session_cache_key(channel))
        if not isinstance(cached, dict):
            return None
        if cached.get('mso_id') != mso_id or not cached.get('code') or not cached.get('client_bearer'):
            return None
        if int(cached.get('expires_at') or 0) <= int(time.time()) + TOKEN_SKEW_SECONDS:
            return None
        return cached

    def _save_adobe_session_cache(self, channel: AMCNChannel, mso_id: str, code: str, client_bearer: str) -> None:
        # client_bearer (the Adobe "api:client:v2" client token minted by
        # setup_client(), distinct from the short-lived playback token above)
        # is a real JWT good for ~6h (confirmed live 2026-08-14). Reusing it
        # with the same `code` re-runs just the two-call profile+decision
        # tail (~0.1-1.2s, confirmed live) instead of repeating the MSO login,
        # so resolve() stays fast for the life of this cached session instead
        # of paying a full re-login on every play request more than ~5min apart.
        self._update_cache(self._adobe_session_cache_key(channel), {
            'mso_id': mso_id, 'code': code, 'client_bearer': client_bearer,
            'expires_at': _jwt_exp(client_bearer) or (int(time.time()) + 3600),
        })

    def _adobe_decision_token(
        self, channel: AMCNChannel, account: TVEAccount, device_id: str, *, force: bool = False,
    ) -> tuple[str, str]:
        cfg = account.config or {}
        # AMCN's own v2 REST API, not the legacy XML protocol yt-dlp's generic
        # MVPD login flows speak — so unlike warner_tve.py/aenetworks_tve.py,
        # non-Cox MSOs here can't fall back to authorize_mvpd()/yt-dlp. Native
        # scripted login exists for Cox, Comcast_SSO, and DIRECTV (below); any
        # other mso_id raises below instead of silently misfiring.
        mso_id = (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or cfg.get('adobe_mso_id') or 'Cox').strip()

        # force=True skips a still-valid cached token — used by the admin
        # "Sign in" button (app.worker.run_amcn_browser_login), which must
        # always exercise a live Cox login so the attempt is real and
        # last_signed_in_at actually advances, not silently return an
        # untested cached token (same reasoning as FOX's and FOX One's own
        # "Sign in" buttons — see run_fox_browser_login's docstring, code
        # review 2026-08-12: this button was reporting "paired" and logging
        # a success message off a pure cache hit, with no Cox request made
        # and no timestamp movement to show for it).
        if not force:
            cached = self._cached_adobe_auth(channel, mso_id)
            if cached:
                return cached

            # Cheap path: reuse an already-logged-in Adobe client session
            # (code + client bearer, ~6h life) to mint a fresh Short Media
            # Token via just the two-call profile+decision tail — confirmed
            # live 2026-08-14 this works with a brand-new plain session and
            # no MSO cookies at all, in ~0.1-1.2s. Falls through to the full
            # MSO login below only if the cached session itself has actually
            # gone stale/been revoked.
            session_cached = self._cached_adobe_session(channel, mso_id)
            if session_cached:
                try:
                    redo_session = requests.Session()
                    self._configure_session(redo_session)
                    headers = _adobe_bearer_headers(session_cached['client_bearer'], channel, device_id)
                    adobe_token, adobe_id, notafter_ms = self._adobe_decision_finish(
                        redo_session, channel, session_cached['code'], mso_id, headers,
                    )
                    self._save_adobe_auth_cache(channel, mso_id, adobe_token, adobe_id, notafter_ms)
                    return adobe_token, adobe_id
                except Exception:
                    self._update_cache(self._adobe_session_cache_key(channel), {})

        statement = self._amcn_software_statement(channel, account)
        client, code, mso_login_url, auth_headers, mso_login_response = self._adobe_session_redirect(
            channel, statement, device_id, mso_id,
        )

        if mso_id == 'Cox':
            if 'login.cox.com' not in mso_login_url:
                raise TVEAuthError(f'{channel.name}: unexpected Adobe redirect host {urlsplit(mso_login_url).netloc}.')
            _cox_saml_login(client.session, mso_login_url, account.username or '', account.password or '')
        elif mso_id == 'YouTubeTV':
            # Not login_to_mvpd()'s generic "no scripted sign-in is wired up
            # for this provider yet" (which wrongly implies this could just
            # be built later, and — being a plain TVEAuthError — gets
            # treated as possibly transient, so play.py kept 503ing forever
            # instead of disabling the channel; reported live 2026-08-17).
            # Confirmed definitively 2026-08-17 via real browser-assisted
            # logins for all 4 AMCN channels: Adobe's own decision service
            # denies every one of them for YouTubeTV ("Adobe did not
            # authorize AMC/BBCA/IFC/WETV for YouTubeTV") — a clean,
            # reproducible, permanent entitlement answer, not a missing
            # scripted-login implementation. Skip straight to that instead
            # of a generic, misleading, retry-forever message.
            raise TVENotAuthorizedError(f'{channel.name}: not entitled for YouTube TV (confirmed via Adobe Pass).')
        else:
            # Every other MVPD's actual sign-in mechanics live in
            # app/tve/mvpd/ — add one there (not here) to support a new
            # provider everywhere at once. Uses its own dedicated session
            # internally (not client.session, which carries the
            # Bearer/auth_headers needed by _adobe_decision_finish's
            # /profiles/code/{code} poll below) — Adobe binds the completed
            # login server-side to `code` regardless of which HTTP session
            # did the MVPD login, same pattern already proven for NBC TVE.
            from ..tve.mvpd import login_to_mvpd
            cookie_jar = cfg.get('xfinity_cookie_jar')
            page_html, page_url = (
                (mso_login_response.text, str(mso_login_response.url))
                if not mso_login_url else ('', mso_login_url)
            )
            try:
                login_to_mvpd(
                    mso_id, page_html, page_url, account.username or '', account.password or '',
                    cookie_jar=cookie_jar,
                )
            except TVEAuthError as exc:
                raise TVEAuthError(f'{channel.name}: {exc}') from exc

        adobe_token, adobe_id, notafter_ms = self._adobe_decision_finish(client.session, channel, code, mso_id, auth_headers)
        self._save_adobe_session_cache(channel, mso_id, code, client.ctx.access_token)
        self._save_adobe_auth_cache(channel, mso_id, adobe_token, adobe_id, notafter_ms)
        return adobe_token, adobe_id

    def _mvpd_access_token(
        self,
        session: requests.Session,
        channel: AMCNChannel,
        *,
        adobe_id: str,
        device_id: str,
        mso_id: str = MVPD,
    ) -> tuple[str, str | None]:
        # Use the device_id the bootstrap token is actually bound to (its own
        # JWT device-id claim when present), not the caller-supplied one —
        # the cookie-token path (channel.schedule_url response with no
        # device_id in the request) can mint a token bound to a DIFFERENT
        # device-id than self._device_id(), and sending a mismatched header
        # here risks rejection by a backend that validates identity against
        # the token's own claim. _fetch_schedule_day already does this right.
        bootstrap, bootstrap_device_id, _feature_flags = self._bootstrap_token(session, channel)
        device_id = bootstrap_device_id or device_id
        cached = self.cache.get(f'bootstrap:{channel.channel_id}') or {}
        refresh_bearer = (cached.get('refresh_token') or bootstrap) if isinstance(cached, dict) else bootstrap
        headers = _amcn_headers(
            channel,
            token=refresh_bearer,
            device_id=device_id,
            entitlement='unauth',
            adobe_id=adobe_id,
            mvpd=mso_id,
            cache_hash=cached.get('cache_hash') if isinstance(cached, dict) else None,
            user_cache_hash=cached.get('user_cache_hash') if isinstance(cached, dict) else None,
        )
        refreshed = session.post(
            f'{API_BASE}/auth-orchestration-id/api/v1/refresh',
            json={},
            headers=headers,
            timeout=30,
        )
        if refreshed.status_code == 401:
            self._update_cache(f'bootstrap:{channel.channel_id}', {})
        refreshed.raise_for_status()
        refresh_data = refreshed.json().get('data') or {}
        unauth_token = (refresh_data.get('access_token') or '').strip()
        feature_flags = _jwt_payload(unauth_token).get('feature_flags')
        if not unauth_token:
            raise TVEAuthError(f'{channel.name}: AMCN refresh did not return a bearer token.')

        auth = session.post(
            f'{API_BASE}/auth-orchestration-id/api/v1/mvpd-auth',
            json={},
            headers=_amcn_headers(
                channel,
                token=unauth_token,
                device_id=device_id,
                entitlement='unauth',
                adobe_id=adobe_id,
                mvpd=mso_id,
                feature_flags=feature_flags,
            ),
            timeout=30,
        )
        if auth.status_code == 401:
            self._update_cache(f'bootstrap:{channel.channel_id}', {})
        auth.raise_for_status()
        auth_data = auth.json().get('data') or {}
        token = (auth_data.get('access_token') or '').strip()
        if not token:
            raise TVEAuthError(f'{channel.name}: AMCN mvpd-auth did not return a bearer token.')
        return token, _jwt_payload(token).get('feature_flags')

    def resolve(self, raw_url: str) -> str:
        channel = channel_for_url(raw_url)
        if not channel:
            raise ValueError(f'Unsupported AMC Networks TVE stream URL: {raw_url}')

        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if not account or not account.is_enabled or not account.has_credentials():
            raise TVEAuthError('TVE credentials are not configured in Settings.')

        cfg = account.config or {}
        mso_id = (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or cfg.get('adobe_mso_id') or 'Cox').strip()

        session = self._session()
        device_id = self._device_id()

        def _attempt(adobe_token: str, adobe_id: str) -> requests.Response:
            amcn_token, feature_flags = self._mvpd_access_token(
                session, channel, adobe_id=adobe_id, device_id=device_id, mso_id=mso_id,
            )
            return session.post(
                f'{API_BASE}/playback-id/api/v1/livestream/{channel.livestream_id}',
                json={
                    'adobeShortMediaToken': adobe_token,
                    'hba': False,
                    'adtags': {
                        'lat': 0,
                        'url': channel.live_url,
                        'playerWidth': 1920,
                        'playerHeight': 1080,
                    },
                },
                headers=_amcn_headers(
                    channel,
                    token=amcn_token,
                    device_id=device_id,
                    entitlement='mvpd-auth',
                    adobe_id=adobe_id,
                    mvpd=mso_id,
                    feature_flags=feature_flags,
                ),
                timeout=30,
            )

        adobe_token, adobe_id = self._adobe_decision_token(channel, account, device_id)
        try:
            playback = _attempt(adobe_token, adobe_id)
            playback.raise_for_status()
        except (requests.HTTPError, TVEAuthError):
            # Cached token rejected (expired server-side before our TTL guess
            # expected, or Adobe revoked it) — clear it and fall through to a
            # fresh decision token (Cox: re-logs in; others: raises the clear
            # "no cached sign-in" error) rather than silently looping forever
            # on a dead cached token.
            self._update_cache(self._adobe_auth_cache_key(channel), {})
            adobe_token, adobe_id = self._adobe_decision_token(channel, account, device_id)
            playback = _attempt(adobe_token, adobe_id)
        playback.raise_for_status()
        data = playback.json()
        hls_url = _find_hls_source(data)
        if hls_url:
            return hls_url
        raise RuntimeError(f'AMCN playback response did not include HLS for {channel.name}.')

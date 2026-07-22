"""
Cox Contour / Comcast CloudTV metadata scraper.

This source uses the same CloudTV backend family as Xfinity Stream, but the
validated login path here is Cox Contour:

  Cox Okta -> Xerxes OAuth fragment -> CloudTV SAT token

The SAT token is enough for catalog and guide APIs. TVE playback uses the
CloudTV Web XACT/XSCT flow plus Cox's XCal Widevine license server. Channelmap
still labels TVE streams as HLS, but the matching `.mpd?trred=false` endpoint
returns DASH/CENC manifests that FastChannels can serve through Shaka or a
PrismCast bridge.
"""
from __future__ import annotations

import json
import logging
import random
import re
import time
import uuid
import base64
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import parse_qs, parse_qsl, quote, urlencode, urlsplit, urlunsplit

try:
    from curl_cffi import requests as _cffi_requests
    _CFFI_IMPERSONATE = 'chrome'
except ImportError:  # pragma: no cover - deployment dependency guard
    _cffi_requests = None
    _CFFI_IMPERSONATE = None

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, ScrapeSkipError
from .category_utils import category_for_channel, infer_category_from_name

logger = logging.getLogger(__name__)

AUTH_URL = (
    'https://xerxes-sub.xerxessecure.com/xerxes-ctrl/oauth/authorize?'
    'response_type=token&client_id=stream-login-cox&partner_id=cox&'
    'redirect_uri=https%3A%2F%2Fwatchtv.cox.com%2Fsat-token.html&partner_id_hint=openid'
)
CLOUDTV_BASE = 'https://xtvapi.cloudtv.comcast.net'
LICENSE_URL = 'https://cox-mds.az.cox.comcast.net/license'
APP_VERSION = '7.14.1'
UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'
)
CLIENT_PLATFORM = (
    'web;linear-tve;local-tve;espnott;est;i18n-ratings;beta-channels;'
    'standard-sports-images;session;livod;local-tve-nbc-v3;rdvr;record-tve'
)
TOKENISH = re.compile(r'(token|auth|session|cookie|jwt|assert|code|state|nonce|ticket|password|secret)', re.I)
AUDIO_ONLY_PREFIXES = ('music choice', 'stingray')
DEFAULT_EPG_HOURS = 24
DEFAULT_BATCH_SIZE = 75
TOKEN_REFRESH_SKEW = 300
XACT_REFRESH_SKEW = 7200
XSCT_REFRESH_SKEW = 3600
PLAYBACK_CACHE_TTL = 45 * 60


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _int_value(*values: Any) -> int | None:
    for value in values:
        try:
            if value is not None and str(value).strip():
                return int(float(str(value).strip()))
        except (TypeError, ValueError):
            continue
    return None


def _bool_config(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def _positive_int(value: Any, default: int, *, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        parsed = default
    parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def _datetime_from_millis(value: Any) -> datetime | None:
    if value in (None, ''):
        return None
    try:
        number = int(float(str(value)))
    except (TypeError, ValueError):
        return None
    if number > 10_000_000_000:
        number = number // 1000
    return datetime.fromtimestamp(number, timezone.utc)


def _sanitize_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlsplit(url)
    safe_qs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        safe_qs.append((key, '<redacted>' if TOKENISH.search(key) else value))
    fragment = '<redacted>' if TOKENISH.search(parsed.fragment) else parsed.fragment
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(safe_qs), fragment))


def _js_unescape(value: str) -> str:
    return value.encode('utf-8').decode('unicode_escape')


def _extract_login_values(html: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for key in ('stateToken', 'redirectUri', 'fromUri', 'relayState', 'baseUrl'):
        match = re.search(rf'"{key}"\s*:\s*"((?:\\.|[^"\\])*)"', html)
        if match:
            values[key] = _js_unescape(match.group(1))
    return values


def _fingerprint() -> str:
    return f'cloudtv_web_polymer_{APP_VERSION}_prod_{int(time.time() * 1000)}_{random.randint(10000, 99999)}'


def _client_info() -> str:
    return (
        'app_name="Contour"; '
        f'app_version="{APP_VERSION}"; '
        'os_name="Web"; '
        f'web_user_agent="{quote(UA, safe="")}"'
    )


def _base_headers() -> dict[str, str]:
    return {
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': UA,
        'sec-ch-ua': '"Chromium";v="145", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }


def _chunks(items: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(items), size):
        yield items[index:index + size]


def _concrete_logo_url(url: str | None) -> str | None:
    if not url:
        return None
    url = url.replace('{&width,height,gravity,rule,ratio,quality,outputFormat}', '&width=360&height=270')
    url = url.replace('{&width,height,gravity,extent,rule,ratio,quality,outputFormat}', '&width=360&height=270')
    return url


def _display_channel_name(item: dict[str, Any]) -> str | None:
    call_sign = _first_text(item.get('callSign'), item.get('callsign'))
    affiliate = _first_text(item.get('branchOf/company/callSign'))
    voice = _first_text(item.get('callSignVoiceOverHint'))
    if affiliate and call_sign and affiliate.lower() != call_sign.lower():
        return f'{affiliate} ({call_sign})'
    if voice and call_sign and voice.lower() != call_sign.lower():
        return f'{voice} ({call_sign})'
    return _first_text(affiliate, voice, call_sign)


_COX_CATEGORY_BY_BRAND = {
    'a&e': 'Entertainment',
    'abc': 'Broadcast',
    'acc network': 'Sports',
    'amc': 'Movies',
    'american heroes': 'History',
    'animal planet': 'Nature',
    'antenna tv': 'Classic TV',
    'bbc america': 'Entertainment',
    'bet': 'Entertainment',
    'big ten network': 'Sports',
    'bounce tv': 'Broadcast',
    'bouncetv': 'Broadcast',
    'bravo': 'Reality TV',
    'byutv': 'Faith',
    'c-span': 'News',
    'cbs': 'Broadcast',
    'charge!': 'Action & Adventure',
    'cnbc': 'News',
    'comet': 'Sci-Fi',
    'cowboy channel': 'Outdoors',
    'cozi tv': 'Classic TV',
    'dabl': 'Lifestyle',
    'daystar': 'Faith',
    'destination america': 'Travel',
    'discovery channel': 'Documentary',
    'discovery life': 'Lifestyle',
    'disney channel': 'Kids',
    'e!': 'Entertainment',
    'entertainment studios': 'Entertainment',
    'estrella tv': 'Latino',
    'ewtn': 'Faith',
    'fetv': 'Classic TV',
    'fox': 'Broadcast',
    'fox business': 'News',
    'freeform': 'Entertainment',
    'fx': 'Entertainment',
    'fxx': 'Comedy',
    'galavisión': 'Latino',
    'gettv': 'Classic TV',
    'grit': 'Westerns',
    'h&i': 'Classic TV',
    'hgtv': 'Home & DIY',
    'hln': 'News',
    'impact': 'Faith',
    'independent': 'Broadcast',
    'insp': 'Faith',
    'investigation discovery': 'True Crime',
    'ion': 'Broadcast',
    'jewelrytv': 'Shopping',
    'jltv': 'Faith',
    'lifetime': 'Drama',
    'local government': 'Broadcast',
    'local programming': 'Broadcast',
    'magnolia network': 'Home & DIY',
    'metv': 'Classic TV',
    'metv+': 'Classic TV',
    'ms now': 'News',
    'mynetworktv': 'Broadcast',
    'nat geo': 'Documentary',
    'nickelodeon': 'Kids',
    'outlaw': 'Westerns',
    'own': 'Lifestyle',
    'paramount network': 'Entertainment',
    'pop': 'Entertainment',
    'public access (peg)': 'Broadcast',
    'pursuit channel': 'Outdoors',
    'quest': 'Documentary',
    'rewind tv': 'Classic TV',
    'sec': 'Sports',
    'start': 'Drama',
    'starz': 'Movies',
    'sundance tv': 'Movies',
    'sundancetv': 'Movies',
    'syfy': 'Sci-Fi',
    'tbs': 'Comedy',
    'tcm': 'Movies',
    'telexitos': 'Latino',
    'thegrio': 'News',
    'tlc': 'Reality TV',
    'tnt': 'Drama',
    'trutv': 'Comedy',
    'tudn': 'Sports',
    'tv land': 'Classic TV',
    'unimás': 'Latino',
    'usa': 'Entertainment',
    'vh1': 'Music',
    'vice': 'Documentary',
    'we tv': 'Reality TV',
    'yurview': 'Local News',
}


def _category_for_channel_item(item: dict[str, Any], name: str) -> str | None:
    raw_brand = _first_text(item.get('branchOf/company/callSign'), item.get('callSignVoiceOverHint'))
    brand_key = (raw_brand or '').strip().lower()
    raw_category = _COX_CATEGORY_BY_BRAND.get(brand_key)
    return category_for_channel(name, raw_category, 'cox') or infer_category_from_name(name)


def _content_rating(item: dict[str, Any]) -> str | None:
    detailed = item.get('contentRating/detailed')
    if isinstance(detailed, dict):
        rating = _first_text(detailed.get('name'), detailed.get('value'))
        if rating:
            return rating
    return _first_text(item.get('rating'), item.get('contentRating'))


def _listing_channel_id(item: dict[str, Any]) -> str | None:
    links = item.get('_links') if isinstance(item.get('_links'), dict) else {}
    self_link = links.get('self') if isinstance(links.get('self'), dict) else {}
    href = _first_text(self_link.get('href'))
    if not href:
        return None
    parsed = urlsplit(href)
    values = parse_qs(parsed.query).get('channelId')
    return values[0] if values else None


def _channel_items(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
        return
    if not isinstance(payload, dict):
        return
    value = payload.get('channels')
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                yield item
    embedded = payload.get('_embedded')
    if isinstance(embedded, dict):
        yield from _channel_items(embedded)


def _priority_rank(item: dict[str, Any]) -> int:
    entitled = item.get('entitled') is True
    is_tve = item.get('isTve') is True
    is_hd = item.get('isHD') is True
    # Browser playback needs the TVE/DDTVE stream family. Entitled in-home cable
    # rows can look better on paper, but their TVILHD metadata gets 401 from MDS
    # outside Cox's native in-home playback context.
    if entitled and is_tve:
        return 0 if is_hd else 1
    if entitled and not is_tve and is_hd:
        return 2
    if entitled and not is_tve:
        return 3
    if item.get('entitled') is not False:
        return 4
    return 5


def _stream_links(item: dict[str, Any]) -> tuple[str | None, str | None]:
    streams = item.get('_embedded', {}).get('stream') if isinstance(item.get('_embedded'), dict) else None
    if not isinstance(streams, list):
        return None, None
    content_url = None
    hd_url = None
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        links = stream.get('_links') if isinstance(stream.get('_links'), dict) else {}
        content = links.get('contentUrl') if isinstance(links.get('contentUrl'), dict) else {}
        hd = links.get('hdContentUrl') if isinstance(links.get('hdContentUrl'), dict) else {}
        content_url = content_url or _first_text(content.get('href'))
        hd_url = hd_url or _first_text(hd.get('href'))
    return content_url, hd_url


def _stream_object(item: dict[str, Any]) -> dict[str, Any]:
    streams = item.get('_embedded', {}).get('stream') if isinstance(item.get('_embedded'), dict) else None
    if not isinstance(streams, list):
        return {}
    for stream in streams:
        if isinstance(stream, dict):
            return stream
    return {}


def _hls_to_mpd_url(url: str | None) -> str | None:
    if not url:
        return None
    url = url.replace('http://', 'https://', 1)
    if '.m3u8' in url:
        url = url.split('.m3u8', 1)[0] + '.mpd?trred=false'
    return url


def _extract_content_metadata(manifest: str) -> str | None:
    match = re.search(r'DRMAGNOSTIC="([^"]+)"', manifest or '')
    return match.group(1) if match else None


def _logo_url(item: dict[str, Any]) -> str | None:
    links = item.get('_links') if isinstance(item.get('_links'), dict) else {}
    logo = links.get('logo') if isinstance(links.get('logo'), dict) else None
    if logo:
        href = _first_text(logo.get('href'))
        if href:
            return _concrete_logo_url(href)
    return None


def _language_code(item: dict[str, Any], name: str) -> str:
    raw = (_first_text(item.get('language')) or '').strip().lower()
    if raw in {'spa', 'es', 'spanish', 'espanol', 'español'}:
        return 'es'
    if raw in {'eng', 'en', 'english'}:
        return 'en'
    if any(marker in name.lower() for marker in ('univision', 'telemundo', 'estrella', 'galavision')):
        return 'es'
    return 'en'


def _opaque_url(channel_id: str, payload: dict[str, Any]) -> str:
    encoded = quote(json.dumps(payload, separators=(',', ':'), sort_keys=True), safe='')
    return f'cox://channel/{channel_id}?data={encoded}'


def _decode_opaque(raw_url: str) -> dict[str, Any]:
    if not raw_url.startswith('cox://channel/'):
        raise ValueError('unsupported Cox URL')
    data = parse_qs(urlsplit(raw_url).query).get('data')
    if not data:
        return {}
    decoded = json.loads(data[0])
    return decoded if isinstance(decoded, dict) else {}


class CoxScraper(BaseScraper):
    source_name = 'cox'
    source_aliases = ('cox_contour', 'contour')
    display_name = 'Cox Contour'
    scrape_interval = 720
    min_scrape_interval = 60
    config_required = True
    is_premium = True
    source_category = 'premium'
    license_url = LICENSE_URL
    stream_audit_enabled = False
    audit_requires_config = ['username', 'password']
    epg_quality = 'full'
    phase_timeouts = {
        'init': 30,
        'bootstrap': 90,
        'channels': 180,
        'epg': 900,
    }

    config_schema = [
        ConfigField('username', 'Cox Username', required=True,
                    placeholder='you@cox.net',
                    help_text='Your Cox Contour login. Cox email usernames are submitted to Okta without the @cox.net suffix.'),
        ConfigField('password', 'Password', field_type='password', required=True,
                    secret=True,
                    help_text='Your Cox Contour password.'),
        ConfigField('include_inhome_cable', 'Include In-Home Cable Rows', field_type='toggle', default=False,
                    help_text='Experimental. Includes non-TVE cable rows that usually require Cox in-home playback context.'),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session = self._new_cffi_session()
        self.sat_access_token = self.config.get('sat_access_token')
        self.id_token = self.config.get('id_token')
        self.features_token = self.config.get('features_token')

    def _new_cffi_session(self):
        if _cffi_requests is None:
            raise ScrapeSkipError('Cox Contour requires curl-cffi to authenticate.')
        return _cffi_requests.Session(impersonate=_CFFI_IMPERSONATE)

    def html_headers(self) -> dict[str, str]:
        return {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', **_base_headers()}

    def okta_headers(self, referer: str) -> dict[str, str]:
        return {
            'accept': 'application/json',
            'content-type': 'application/json',
            'origin': 'https://login.cox.com',
            'referer': referer,
            'x-okta-user-agent-extended': 'okta-signin-widget-5.16.1',
            **_base_headers(),
        }

    def cloudtv_headers(self, *, include_features: bool = False) -> dict[str, str]:
        self._ensure_auth()
        headers = {
            **_base_headers(),
            'accept': 'application/comcast+hal+json, application/json, */*; q=0.01',
            'authorization': f'SAT {self.sat_access_token}',
            'referer': 'https://watchtv.cox.com/',
            'client-platform': CLIENT_PLATFORM,
            'fingerprint': _fingerprint(),
            'x-finity-accept-language': 'en-US',
            'x-finity-client-info': _client_info(),
            'x-hypergard': '6.0.0',
        }
        if include_features and self.features_token:
            headers['x-finity-features'] = self.features_token
        return headers

    def pre_run_setup(self) -> None:
        self._ensure_auth()
        self._fetch_features()

    def _token_fresh(self) -> bool:
        if not self.sat_access_token:
            return False
        try:
            expires_at = float(self.config.get('sat_expires_at') or 0)
        except (TypeError, ValueError):
            return False
        return expires_at > time.time() + TOKEN_REFRESH_SKEW

    def _ensure_auth(self) -> None:
        if self._token_fresh():
            return
        username = (self.config.get('username') or '').strip()
        password = self.config.get('password') or ''
        if not username or not password:
            raise ScrapeSkipError('Cox Contour username and password are required.')
        self._authenticate(username, password)

    def _authenticate(self, username: str, password: str) -> None:
        login = self.session.get(AUTH_URL, headers=self.html_headers(), timeout=30)
        login.raise_for_status()
        values = _extract_login_values(login.text)
        if 'stateToken' not in values:
            logger.debug('[cox] login URL after failed stateToken parse: %s', _sanitize_url(str(login.url)))
            raise ScrapeSkipError('Cox login page did not expose stateToken.')

        okta_headers = self.okta_headers(str(login.url))
        introspect = self.session.post(
            'https://login.cox.com/api/v1/authn/introspect',
            headers=okta_headers,
            json={'stateToken': values['stateToken']},
            timeout=30,
        )
        introspect.raise_for_status()
        body = introspect.json()
        values['stateToken'] = body.get('stateToken', values['stateToken'])

        login_user = username.split('@', 1)[0] if username.lower().endswith('@cox.net') else username
        authn = self.session.post(
            'https://login.cox.com/api/v1/authn',
            headers=okta_headers,
            json={
                'username': login_user,
                'password': password,
                'stateToken': values['stateToken'],
                'options': {'warnBeforePasswordExpired': True, 'multiOptionalFactorEnroll': True},
            },
            timeout=30,
        )
        authn.raise_for_status()
        auth_body = authn.json()
        if auth_body.get('status') != 'SUCCESS':
            raise ScrapeSkipError(f'Cox Okta auth did not return SUCCESS: {auth_body.get("status")}')
        next_href = auth_body.get('_links', {}).get('next', {}).get('href')
        if not next_href:
            raise ScrapeSkipError('Cox Okta auth response did not include next redirect.')

        final_url = self._follow_redirects(next_href)
        fragment = parse_qs(urlsplit(final_url).fragment)
        token = (fragment.get('access_token') or [None])[0]
        id_token = (fragment.get('id_token') or [None])[0]
        if not token:
            raise ScrapeSkipError('Cox Xerxes redirect did not include an access_token fragment.')

        try:
            expires_in = int((fragment.get('expires_in') or [3600])[0])
        except (TypeError, ValueError):
            expires_in = 3600
        self.sat_access_token = token
        self.id_token = id_token
        self._update_config('sat_access_token', token)
        self._update_config('sat_expires_at', time.time() + expires_in)
        if id_token:
            self._update_config('id_token', id_token)
            self._update_config('id_token_expires_at', time.time() + expires_in)
        logger.info('[cox] refreshed SAT token; expires in %ss', expires_in)

    def _follow_redirects(self, url: str) -> str:
        current = url
        for _ in range(8):
            response = self.session.get(current, headers=self.html_headers(), timeout=30, allow_redirects=False)
            location = response.headers.get('location') or response.headers.get('Location')
            if not location or response.status_code not in {301, 302, 303, 307, 308}:
                return str(response.url)
            current = location
        raise ScrapeSkipError('Cox login redirect chain exceeded limit.')

    def _fetch_features(self) -> dict[str, Any]:
        response = self.session.get(f'{CLOUDTV_BASE}/features/', headers=self.cloudtv_headers(), timeout=30)
        response.raise_for_status()
        data = response.json()
        features_token = data.get('featuresToken') if isinstance(data.get('featuresToken'), str) else None
        if features_token and features_token != self.features_token:
            self.features_token = features_token
            self._update_config('features_token', features_token)
        return data

    def _token_expiry_fresh(self, key: str, skew: int) -> bool:
        try:
            return float(self.config.get(key) or 0) > time.time() + skew
        except (TypeError, ValueError):
            return False

    def _ensure_xact(self) -> str:
        xact = self.config.get('xact')
        if xact and self._token_expiry_fresh('xact_expires_at', XACT_REFRESH_SKEW):
            return xact
        self._ensure_auth()
        if not self.id_token or not self._token_expiry_fresh('id_token_expires_at', TOKEN_REFRESH_SKEW):
            self.sat_access_token = None
            self.config['sat_expires_at'] = 0
            self._ensure_auth()
        if not self.id_token:
            raise ScrapeSkipError('Cox DRM provisioning requires a Xerxes id_token.')
        response = self.session.post(
            f'{CLOUDTV_BASE}/partner/device/provisionWeb/',
            headers={**self.cloudtv_headers(include_features=False), 'content-type': 'application/x-www-form-urlencoded'},
            data={'identityToken': self.id_token},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        xact = data.get('xact')
        if not xact:
            raise ScrapeSkipError('Cox provisionWeb did not return an XACT token.')
        expires = data.get('expires')
        try:
            expires_at = float(expires)
        except (TypeError, ValueError):
            expires_at = time.time() + 86400
        if expires_at < 10_000_000_000:
            expires_at *= 1000
        # CloudTV returns epoch seconds; store seconds for local comparisons.
        if expires_at > 10_000_000_000:
            expires_at /= 1000
        self._update_config('xact', xact)
        self._update_config('xact_expires_at', expires_at)
        return xact

    def _ensure_xsct(self) -> str:
        xsct = self.config.get('xsct')
        if xsct and self._token_expiry_fresh('xsct_expires_at', XSCT_REFRESH_SKEW):
            return xsct
        xact = self._ensure_xact()
        response = self.session.post(
            f'{CLOUDTV_BASE}/partner/drm/create-session/',
            headers={**self.cloudtv_headers(include_features=False), 'content-type': 'application/x-www-form-urlencoded'},
            data={'xact': xact, 'partnerId': 'cox', 'hardAcquisition': 'false'},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        xsct = data.get('xsct')
        if not xsct:
            raise ScrapeSkipError('Cox create-session did not return an XSCT token.')
        token_summary = data.get('tokenSummary') if isinstance(data.get('tokenSummary'), dict) else {}
        expires_at = token_summary.get('notOnOrAfter') or (time.time() + 7200)
        try:
            expires_at = float(expires_at)
        except (TypeError, ValueError):
            expires_at = time.time() + 7200
        if expires_at > 10_000_000_000:
            expires_at /= 1000
        self._update_config('xsct', xsct)
        self._update_config('xsct_expires_at', expires_at)
        if data.get('serviceAccessToken'):
            self._update_config('service_access_token', data.get('serviceAccessToken'))
        return xsct

    def _fetch_channelmap(self) -> dict[str, Any]:
        response = self.session.get(
            f'{CLOUDTV_BASE}/channelmap/?freetome=off',
            headers=self.cloudtv_headers(include_features=True),
            timeout=45,
        )
        response.raise_for_status()
        return response.json()

    def _fetch_tvgrid(self, channel_ids: list[str], *, hours: int, batch_size: int) -> dict[str, Any]:
        now_ms = int(time.time() // 3600 * 3600 * 1000)
        merged: dict[str, Any] = {'_embedded': {'channels': []}, 'startTime': str(now_ms), 'hours': hours}
        batches = list(_chunks(channel_ids, batch_size))
        for index, batch in enumerate(batches, start=1):
            params = urlencode({'startTime': now_ms, 'hours': hours, 'channelIds': ','.join(batch)})
            response = self.session.get(
                f'{CLOUDTV_BASE}/tvgrid/chunks/?{params}',
                headers=self.cloudtv_headers(include_features=True),
                timeout=60,
            )
            response.raise_for_status()
            data = response.json()
            rows = data.get('_embedded', {}).get('channels', []) if isinstance(data, dict) else []
            if isinstance(rows, list):
                merged['_embedded']['channels'].extend(rows)
            if index < len(batches):
                time.sleep(0.15)
        return merged

    def fetch_channels(self) -> list[ChannelData]:
        self._ensure_auth()
        self._fetch_features()
        payload = self._fetch_channelmap()
        selected: dict[str, tuple[int, dict[str, Any]]] = {}
        for item in _channel_items(payload):
            if item.get('entitled') is not True:
                continue
            if item.get('isTve') is not True and not _bool_config(self.config.get('include_inhome_cable')):
                continue
            name = _display_channel_name(item)
            if not name or name.lower().startswith(AUDIO_ONLY_PREFIXES):
                continue
            call_sign = _first_text(item.get('callSign'), item.get('callsign'))
            dedupe = (call_sign or _first_text(item.get('stationId'), item.get('channelId')) or name).lower()
            rank = _priority_rank(item)
            previous = selected.get(dedupe)
            if previous is None or rank < previous[0]:
                selected[dedupe] = (rank, item)

        channels: list[ChannelData] = []
        for rank, item in selected.values():
            channel = self._channel_from_item(item, rank)
            if channel:
                channels.append(channel)
        channels.sort(key=lambda ch: ((ch.number if ch.number is not None else 999999), ch.name, ch.source_channel_id))
        return channels

    def _channel_from_item(self, item: dict[str, Any], rank: int) -> ChannelData | None:
        channel_id = _first_text(item.get('channelId'), item.get('stationId'), item.get('callSign'))
        name = _display_channel_name(item)
        if not channel_id or not name:
            return None
        station_id = _first_text(item.get('stationId'))
        call_sign = _first_text(item.get('callSign'), item.get('callsign'))
        affiliate = _first_text(item.get('branchOf/company/callSign'))
        content_url, hd_url = _stream_links(item)
        if not content_url and not hd_url:
            return None
        stream_obj = _stream_object(item)
        restrictions = [str(v) for v in item.get('restrictStreaming', [])] if isinstance(item.get('restrictStreaming'), list) else []
        tags = ['Cox Contour']
        tags.append('TVE' if item.get('isTve') is True else 'Cable')
        if restrictions:
            tags.extend(restrictions)
        opaque_payload = {
            'channel_id': channel_id,
            'station_id': station_id,
            'stream_id': _first_text(item.get('streamId')),
            'call_sign': call_sign,
            'content_url': content_url,
            'hd_content_url': hd_url,
            'media_id': _first_text(stream_obj.get('mediaId')),
            'encoding_format': _first_text(stream_obj.get('encodingFormat')),
            'stream_location_required': stream_obj.get('locationRequired') is True,
            'geofenced': stream_obj.get('geofenced') is True,
            'is_tve': item.get('isTve') is True,
            'is_hd': item.get('isHD') is True,
            'restrict_streaming': restrictions,
            'rank': rank,
        }
        return ChannelData(
            source_channel_id=channel_id,
            name=name,
            stream_url=_opaque_url(channel_id, opaque_payload),
            logo_url=_logo_url(item),
            category=_category_for_channel_item(item, name),
            language=_language_code(item, name),
            country='US',
            stream_type='dash' if (content_url or hd_url) else 'hls',
            number=_int_value(item.get('number')),
            guide_key=station_id,
            tags=tags,
            description=affiliate or call_sign,
        )

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        if not channels:
            return []
        self._ensure_auth()
        self._fetch_features()
        hours = _positive_int(kwargs.get('hours') or self.config.get('epg_hours'), DEFAULT_EPG_HOURS, minimum=1, maximum=72)
        batch_size = _positive_int(self.config.get('batch_size'), DEFAULT_BATCH_SIZE, minimum=10, maximum=150)
        channel_ids = [str(channel.source_channel_id) for channel in channels if channel.source_channel_id]
        known_channel_ids = set(channel_ids)
        station_to_channel = {str(channel.guide_key): str(channel.source_channel_id) for channel in channels if channel.guide_key}
        tvgrid = self._fetch_tvgrid(channel_ids, hours=hours, batch_size=batch_size)
        return self._programs_from_tvgrid(tvgrid, station_to_channel, known_channel_ids)

    def _programs_from_tvgrid(
        self,
        tvgrid: dict[str, Any],
        station_to_channel: dict[str, str],
        known_channel_ids: set[str],
    ) -> list[ProgramData]:
        programs: list[ProgramData] = []
        rows = tvgrid.get('_embedded', {}).get('channels', []) if isinstance(tvgrid, dict) else []
        if not isinstance(rows, list):
            return programs
        for channel_row in rows:
            if not isinstance(channel_row, dict):
                continue
            station_id = _first_text(channel_row.get('stationId'))
            fallback_channel_id = station_to_channel.get(station_id or '', station_id or '')
            listings = channel_row.get('_embedded', {}).get('listings') if isinstance(channel_row.get('_embedded'), dict) else []
            if not isinstance(listings, list):
                continue
            for item in listings:
                if not isinstance(item, dict):
                    continue
                program = self._program_from_listing(item, fallback_channel_id, known_channel_ids)
                if program:
                    programs.append(program)
        programs.sort(key=lambda program: (program.source_channel_id, program.start_time, program.title))
        return programs

    def _program_from_listing(
        self,
        item: dict[str, Any],
        fallback_channel_id: str,
        known_channel_ids: set[str],
    ) -> ProgramData | None:
        listing_channel_id = _listing_channel_id(item)
        source_channel_id = listing_channel_id if listing_channel_id in known_channel_ids else fallback_channel_id
        title = _first_text(item.get('title'), item.get('name'))
        start = _datetime_from_millis(item.get('startTime'))
        end = _datetime_from_millis(item.get('endTime'))
        if not source_channel_id or not title or not start or not end:
            return None
        program_type = (_first_text(item.get('type')) or '').lower() or None
        if program_type not in {'movie', 'episode'}:
            program_type = None
        return ProgramData(
            source_channel_id=source_channel_id,
            title=title,
            start_time=start,
            end_time=end,
            description=_first_text(item.get('description'), item.get('shortDescription'), item.get('longDescription')),
            poster_url=_concrete_logo_url(_first_text(item.get('imageUrl'), item.get('posterUrl'))),
            category=_first_text(item.get('genre'), item.get('category')),
            rating=_content_rating(item),
            episode_title=_first_text(item.get('episodeTitle'), item.get('subtitle')),
            is_live=(_first_text(item.get('airingType')) or '').upper() == 'LIVE',
            program_type=program_type,
            series_id=_first_text(item.get('seriesId')),
            episode_id=_first_text(item.get('entityId'), item.get('listingId')),
        )

    def resolve(self, raw_url: str) -> str:
        data = _decode_opaque(raw_url)
        channel_id = _first_text(data.get('channel_id'))
        if data.get('is_tve') is True:
            # HAR2's successful browser path used the red-tve URL, not red-tve-hd.
            # The HD host can return TVILHD contentMetadata, which MDS rejects for
            # this proxy path with a 401 on the content-license request.
            hls_url = _first_text(data.get('content_url'), data.get('hd_content_url'))
        else:
            hls_url = _first_text(data.get('hd_content_url'), data.get('content_url'))
        if _bool_config(self.config.get('allow_experimental_direct_hls')):
            if not hls_url:
                raise ScrapeSkipError('Cox Contour channel did not include a CloudTV content URL.')
            return hls_url.replace('http://', 'https://', 1)
        if not channel_id or not hls_url:
            raise ScrapeSkipError('Cox Contour channel did not include TVE playback metadata.')

        cached = (self.cache.get('cox_playback') or {}).get(channel_id)
        if cached and time.time() - float(cached.get('cached_at') or 0) < PLAYBACK_CACHE_TTL:
            mpd_url = cached.get('mpd_url')
            if mpd_url:
                self._ensure_xsct()
                return mpd_url

        xsct = self._ensure_xsct()
        hls_url = hls_url.replace('http://', 'https://', 1)
        hls_response = self.session.get(
            hls_url,
            headers={'origin': 'https://watchtv.cox.com', 'referer': 'https://watchtv.cox.com/', 'accept': '*/*'},
            timeout=30,
        )
        hls_response.raise_for_status()
        content_metadata = _extract_content_metadata(hls_response.text)

        mpd_probe = self.session.get(
            _hls_to_mpd_url(hls_url),
            headers={'origin': 'https://watchtv.cox.com', 'referer': 'https://watchtv.cox.com/', 'accept': '*/*'},
            timeout=30,
        )
        mpd_probe.raise_for_status()
        mpd_url = str(mpd_probe.url)
        content_type = (mpd_probe.headers.get('content-type') or '').lower()
        if 'json' in content_type:
            locations = mpd_probe.json().get('locations') or []
            if locations:
                mpd_url = locations[0]
        if not mpd_url:
            raise ScrapeSkipError('Cox Contour did not return a DASH manifest URL.')

        playback = dict(self.cache.get('cox_playback') or {})
        playback[channel_id] = {
            'mpd_url': mpd_url,
            'hls_url': hls_url,
            'content_metadata': content_metadata,
            'media_id': data.get('media_id'),
            'geofenced': data.get('geofenced') is True,
            'xsct_cached_at': time.time() if xsct else 0,
            'cached_at': time.time(),
        }
        self._update_cache('cox_playback', playback)
        return mpd_url

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        return cls.license_url

    @classmethod
    def license_request_headers(cls, config: dict) -> dict:
        trace_id = str(uuid.uuid4())
        return {
            'Accept': 'application/vnd.xcal.mds.licenseResponse+json; version=1',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/vnd.xcal.mds.licenseRequest+json; version=1',
            'Origin': 'https://watchtv.cox.com',
            'Pragma': 'no-cache',
            'Referer': 'https://watchtv.cox.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'User-Agent': UA,
            'X-MoneyTrace': f'trace-id={trace_id};parent-id={random.getrandbits(63)};span-id={random.getrandbits(63)};',
            'sec-ch-ua': '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        playback_map = config.get('cox_playback') or {}
        playback = playback_map.get(channel_id or '') if channel_id else None
        if not playback and not channel_id and playback_map:
            playback = max(
                playback_map.values(),
                key=lambda item: item.get('cached_at') or item.get('xsct_cached_at') or 0,
            )
        content_metadata = (playback or {}).get('content_metadata')
        xsct = config.get('xsct') or (playback or {}).get('xsct')
        if not content_metadata:
            raise RuntimeError('Cox license request is missing per-channel content metadata; resolve the channel first.')
        if not xsct:
            raise RuntimeError('Cox license request is missing XSCT; refresh Cox playback session first.')
        body = json.dumps({
            'keySystem': 'widevine',
            'licenseRequest': base64.b64encode(challenge).decode('ascii'),
            'contentMetadata': content_metadata,
            'mediaUsage': 'stream',
            'accessToken': xsct,
            'accessAttributes': {'content:xcal:streamType': 'Geofenced'},
        }).encode('utf-8')
        return body, cls.license_request_headers(config)

    @classmethod
    def post_license_request(cls, url: str, body: bytes, headers: dict, timeout: int = 15):
        if _cffi_requests is not None:
            return _cffi_requests.post(
                url,
                data=body,
                headers=headers,
                timeout=timeout,
                impersonate=_CFFI_IMPERSONATE,
            )
        import requests as _requests_fallback
        return _requests_fallback.post(url, data=body, headers=headers, timeout=timeout)

    @classmethod
    def process_license_response(cls, response_bytes: bytes) -> bytes:
        stripped = (response_bytes or b'').lstrip()
        if not stripped:
            return response_bytes
        # Cox MDS returns JSON-wrapped service-certificate responses, but some
        # license exchanges may already be raw Widevine protobuf bytes. Hand
        # non-JSON responses through without noisy tracebacks.
        if stripped[:1] not in (b'{', b'['):
            return response_bytes
        try:
            data = json.loads(response_bytes)
            encoded = data.get('license')
            if encoded:
                return base64.b64decode(encoded + '==')
        except Exception:
            logger.warning('[cox] JSON license response could not be unwrapped', exc_info=True)
        return response_bytes

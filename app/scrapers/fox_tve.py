from __future__ import annotations

import base64
import json
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, urlparse, urlsplit

import requests

from .base import BaseScraper, ChannelData, ProgramData

SCHEME = 'fox-tve://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
HLS_RE = re.compile(r'https?://[^\s"\'<>]+?\.m3u8(?:\?[^\s"\'<>]*)?', re.I)
FOX_SPORTS_API_KEY = 'cf289e299efdfa39fb6316f259d1de93'
FOX_SPORTS_PRODUCT_API = 'https://api.fox.com/fs/product/curated/v1/sporting/keystone/detail/by_filters'
FOX_SPORTS_PREVIEW_MVPD = 'TempPass_fbcfox_60min'
FOX_SPORTS_DVP_PLAYBACK_API = 'https://prod.api.digitalvideoplatform.com/sports/v3.0/partner-playback/live'
FOX_NEWS_BUSINESS_SCHEDULE_FEEDS = {
    'fox_news': 'https://apps.foxnews.com/schedule_new/feed/fox-news.jn',
    'fox_business': 'https://apps.foxnews.com/schedule_new/feed/fox-business.jn',
}
FOX_SIGNED_HLS_RE = re.compile(r'https://247\.(?:foxnews|foxbusiness)\.com/[^\s\"\'<>]+?master\.m3u8\?hdne[at]=[^\s\"\'<>]+', re.I)
FOX_HLS_TOKEN_EXP_RE = re.compile(r'hdne[at]=exp=(\d+)~', re.I)
FOX_ADOBE_API = 'https://sp.auth.adobe.com'
FOX_TOKEN_GENERATOR_API = 'https://tve-auth.foxnews.com/atk/token/generator'
FOX_FNFB_TEMP_PASS_MVPD = 'FNFB_tempPass_1'
FOX_NEWS_BUSINESS_ADOBE: dict[str, dict[str, str]] = {
    'fox_news': {
        'requestor': 'foxnews',
        'domain': 'foxnews.com',
        'redirect_url': 'https://www.foxnews.com/live/channel/callback',
        'software_statement': 'eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIyMDczMDdjMi1hNDNiLTRiNDAtOGUzZS0wNjIzNTdmNTlmODciLCJuYmYiOjE3Njg5NDAxMzIsImlzcyI6ImF1dGguYWRvYmUuY29tIiwiaWF0IjoxNzY4OTQwMTMyfQ.YWJ8i2nLMDUX8tXTpyZCFrDbWg6pSMLRQ6N9KbjHIyIdtUX718NAigbnwpf8N16N2qEadzYoM90HvkU-aIvjInQAWNC7pPgFbxoo_AslKgMs-eD3eGmz4o_MHAFkePW5APkDSl2sl_yPJsCEghzkpB_mNHqAP7DbpDg0J9xsswEywDfDNqScen9tBLcYlUjRir9-sxRBPUOHvdheT7tUurf7cPfy9hNAI3KqA3hHTSYOLBS-Pef-_1eAaaazJ1_Dy-zzca4U7OgcVTBsEzBlvL2wAu5Uc8hCcihc4DNOih16MnQfhz1HGGFRdIhuoxLzqSERCpRm5-XMSPdTW3GeXw',
        'resource': '<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/"><channel><title>Fox News Channel</title><item><title>Fox News Channel</title><guid>2056</guid><media:rating scheme="urn:v-chip">TV-G</media:rating></item></channel></rss>',
    },
    'fox_business': {
        'requestor': 'foxbusiness',
        'domain': 'foxbusiness.com',
        'redirect_url': 'https://www.foxbusiness.com/live/channel/callback',
        'software_statement': 'eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJkMWNiMzgwMC1mMWNhLTRmYzItYjE4Yi1iNWFiMzE2ZmYxMjUiLCJuYmYiOjE3NjkwMDY1NzgsImlzcyI6ImF1dGguYWRvYmUuY29tIiwiaWF0IjoxNzY5MDA2NTc4fQ.qSe5yCQZ_5FfkkkBH0bHnST0ZN5Dos6FOdtQAAsahjURVac2jBXlgPE78OYE4tBkOouIy24mSy9E7VZjH4Y8Dm0vg5hh1532T26m1QUr_PCwpWAt8_QeNfxVg7IrhI-gSY6OR-dtYcS1nlEh9S_vQEvoIWJ1-b0wDxSTVXNfM5Gnt9NUvYbSCfnKItLWJXwKt3NUVf7cqThd-Gvk_TANRUSpmVB9JGiENxXDZG1zzDpbJv2evUIz9yC_7y_bXXuthW3wTNIy8nBXTBmgfha6GTjNX3Nv1d_YckuXmQ0UGwcdvCgY7_R-sd-wIM2rGATKkwUKW2eNEwiEiW186frpdg',
        'resource': '<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/"><channel><title>Fox Business Network</title><item><title>Fox Business Network</title><guid>2176</guid><media:rating scheme="urn:v-chip">TV-G</media:rating></item></channel></rss>',
    },
}
_SIGNED_HLS_CACHE: dict[str, tuple[str, int]] = {}
SIGNED_HLS_CACHE_KEY_PREFIX = 'signed_hls:'


@dataclass(frozen=True)
class FoxTVEChannel:
    channel_id: str
    name: str
    page_url: str
    fallback_stream_url: str | None
    logo_url: str
    category: str
    description: str
    guide_title: str
    call_signs: tuple[str, ...] = ()
    playable: bool = True
    signed_page_hls: bool = False

    @property
    def stream_url(self) -> str:
        return f'{SCHEME}{self.channel_id}/live-v1'


CHANNELS: dict[str, FoxTVEChannel] = {
    'fox_news': FoxTVEChannel(
        channel_id='fox_news',
        name='Fox News Channel',
        page_url='https://www.foxnews.com/video/5614615980001',
        fallback_stream_url='https://247.foxnews.com/hls/live/2003586/FNCHLSv3/master.m3u8',
        logo_url='https://static.foxnews.com/static/orion/styles/img/fox-news/og/og-fox-news.png',
        category='News',
        description='FOX News TVE live stream. Playback resolves through the official FOX page to obtain a short-lived Akamai-signed HLS URL.',
        guide_title='Fox News Channel',
        signed_page_hls=True,
    ),
    'fox_business': FoxTVEChannel(
        channel_id='fox_business',
        name='Fox Business Network',
        page_url='https://www.foxbusiness.com/video/5640669329001',
        fallback_stream_url='https://247.foxbusiness.com/hls/live/2003756/FBNHLSv3/master.m3u8',
        logo_url='https://static.foxnews.com/static/orion/styles/img/fox-business/og/og-fox-business.png',
        category='Business',
        description='FOX Business TVE live stream. Playback resolves through the official FOX Business page to obtain a short-lived Akamai-signed HLS URL.',
        guide_title='Fox Business Network',
        signed_page_hls=True,
    ),
    'fox_weather': FoxTVEChannel(
        channel_id='fox_weather',
        name='Fox Weather',
        page_url='https://www.foxweather.com/live',
        fallback_stream_url='https://content.uplynk.com/db88cebb80ea46269a1b2691871f3660.m3u8?rays=g',
        logo_url='https://static.foxnews.com/static/orion/styles/img/fox-weather/og/og-fox-weather.png',
        category='News',
        description='Live FOX Weather stream discovered from the FOX Weather live page.',
        guide_title='Fox Weather Live',
        playable=True,
    ),
    'fox_sports_fs1': FoxTVEChannel(
        channel_id='fox_sports_fs1',
        name='FS1',
        page_url='https://www.foxsports.com/live/fs1',
        fallback_stream_url=None,
        logo_url='https://assets.platform.foxsports.com/stations_v1/FS1/7bd193561008c5a.png',
        category='Sports',
        description='FOX Sports TVE prototype channel. EPG is sourced from the FOX Product API; playback resolves through FOX Sports DVP with Cox MVPD auth when configured, falling back to Fox PreviewPass while prototyping.',
        guide_title='FS1',
        call_signs=('FS1', 'FS1-DIGITAL'),
    ),
    'fox_sports_fs2': FoxTVEChannel(
        channel_id='fox_sports_fs2',
        name='FS2',
        page_url='https://www.foxsports.com/live/fs2',
        fallback_stream_url=None,
        logo_url='https://assets.foxdcg.com/dpp-uploaded/images/stations/fs2.png',
        category='Sports',
        description='FOX Sports TVE prototype channel. EPG is sourced from the FOX Product API; playback resolves through FOX Sports DVP with Cox MVPD auth when configured, falling back to Fox PreviewPass while prototyping.',
        guide_title='FS2',
        call_signs=('FS2', 'FS2-DIGITAL'),
    ),
    'fox_sports_fox': FoxTVEChannel(
        channel_id='fox_sports_fox',
        name='FOX Sports',
        page_url='https://www.foxsports.com/live/fox',
        fallback_stream_url=None,
        logo_url='https://assets.platform.foxsports.com/stations_v1/FOX-DIGITAL/7c0393e220035d0.png',
        category='Sports',
        description='FOX Sports TVE prototype for national FOX/digital sports listings. Playback resolves through FOX Sports DVP with Cox MVPD auth when configured, falling back to Fox PreviewPass while prototyping.',
        guide_title='FOX Sports',
        call_signs=('FOX', 'FOX-DIGITAL'),
    ),
    'fox_sports_btn': FoxTVEChannel(
        channel_id='fox_sports_btn',
        name='Big Ten Network',
        page_url='https://www.foxsports.com/live/btn',
        fallback_stream_url=None,
        logo_url='https://assets.foxdcg.com/dpp-uploaded/images/stations/btnv2.png',
        category='Sports',
        description='FOX Sports TVE prototype channel. EPG is sourced from the FOX Product API; playback resolves through FOX Sports DVP with Cox MVPD auth when configured, falling back to Fox PreviewPass while prototyping.',
        guide_title='Big Ten Network',
        call_signs=('BTN',),
    ),
    'fox_deportes': FoxTVEChannel(
        channel_id='fox_deportes',
        name='FOX Deportes',
        page_url='https://www.foxsports.com/live/foxdep',
        fallback_stream_url=None,
        logo_url='https://assets.foxdcg.com/dpp-uploaded/images/stations/foxdep.png',
        category='Sports',
        description='FOX Deportes TVE prototype channel. EPG is sourced from the FOX Product API; playback resolves through FOX Sports DVP with Cox MVPD auth when configured, falling back to Fox PreviewPass while prototyping.',
        guide_title='FOX Deportes',
        call_signs=('FOXDEP',),
    ),
    'fox_soccer_plus': FoxTVEChannel(
        channel_id='fox_soccer_plus',
        name='FOX Soccer Plus',
        page_url='https://www.foxsports.com/live/fsp',
        fallback_stream_url=None,
        logo_url='https://assets.foxdcg.com/dpp-uploaded/images/stations/fsp-v2.png',
        category='Sports',
        description='FOX Soccer Plus TVE prototype channel. EPG is sourced from the FOX Product API; playback resolves through FOX Sports DVP with Cox MVPD auth when configured, falling back to Fox PreviewPass while prototyping.',
        guide_title='FOX Soccer Plus',
        call_signs=('FSP',),
    ),
}


def channel_for_url(raw_url: str) -> FoxTVEChannel | None:
    parsed = urlparse(raw_url or '')
    channel_id = parsed.netloc or parsed.path.lstrip('/').split('/')[0]
    if not raw_url.startswith(SCHEME):
        return None
    return CHANNELS.get(channel_id)


def find_live_hls(page_url: str) -> str | None:
    try:
        r = requests.get(page_url, headers={'User-Agent': UA, 'Accept': 'text/html,*/*'}, timeout=20)
        r.raise_for_status()
    except requests.RequestException:
        return None

    # Prefer structured VideoObject contentUrl when present. The page currently
    # exposes a short Uplynk rolling window as JSON-LD rather than as app state.
    for match in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', r.text, re.S | re.I):
        try:
            data = json.loads(match.group(1))
        except ValueError:
            continue
        stack = [data]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                url = item.get('contentUrl')
                if isinstance(url, str) and '.m3u8' in url:
                    return url.replace('\\/', '/')
                stack.extend(item.values())
            elif isinstance(item, list):
                stack.extend(item)

    for match in HLS_RE.finditer(r.text):
        url = match.group(0).replace('\\/', '/')
        if 'uplynk.com' in url or 'fox' in url:
            return url
    return None



def _signed_hls_exp(url: str) -> int:
    match = FOX_HLS_TOKEN_EXP_RE.search(url)
    if not match:
        return int(time.time()) + 300
    try:
        return int(match.group(1))
    except ValueError:
        return int(time.time()) + 300


def _fox_browser_session():
    try:
        from curl_cffi import requests as curl_requests

        return curl_requests.Session(impersonate='chrome')
    except Exception:
        return requests.Session()


def _fox_static_headers(domain: str = 'foxnews.com') -> dict[str, str]:
    return {
        'User-Agent': UA,
        'Accept': 'application/json',
        'Origin': 'https://static.foxnews.com',
        'Referer': f'https://static.foxnews.com/static/orion/html/video/iframe/vod.html?domain={domain}',
    }


def _fox_device_info(requestor: str) -> str:
    payload = {
        'version': '1.0.0',
        'deviceType': 'web',
        'appName': requestor,
        'appVersion': 'web',
        'model': 'Desktop',
        'osName': 'Windows',
        'osVersion': '10',
        'primaryHardwareType': 'Desktop',
        'connectionType': '4g',
    }
    raw = json.dumps(payload, separators=(',', ':')).encode()
    return base64.b64encode(raw).decode()


def resolve_fox_http_signed_hls(channel: FoxTVEChannel) -> str:
    now = int(time.time())
    cached = _SIGNED_HLS_CACHE.get(channel.channel_id)
    if cached and cached[1] > now + 120:
        return cached[0]

    cfg = FOX_NEWS_BUSINESS_ADOBE.get(channel.channel_id)
    if not cfg or not channel.fallback_stream_url:
        raise ValueError(f'FOX HTTP signed HLS is not configured for {channel.name}')

    session = _fox_browser_session()
    headers = _fox_static_headers(cfg['domain'])
    register = session.post(
        f'{FOX_ADOBE_API}/o/client/register',
        json={'software_statement': cfg['software_statement']},
        headers={**headers, 'Content-Type': 'application/json'},
        timeout=30,
    )
    register.raise_for_status()
    client = register.json()
    client_id = client.get('client_id')
    client_secret = client.get('client_secret')
    if not client_id or not client_secret:
        raise ValueError(f'FOX Adobe register response missing client credentials for {channel.name}')

    token = session.post(
        f'{FOX_ADOBE_API}/o/client/token?{urlencode({"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"})}',
        headers=headers,
        timeout=30,
    )
    token.raise_for_status()
    access_token = token.json().get('access_token')
    if not access_token:
        raise ValueError(f'FOX Adobe client token response missing access token for {channel.name}')

    auth_headers = {
        **headers,
        'Authorization': f'Bearer {access_token}',
        'AP-Device-Identifier': 'fingerprint ZmFzdGNoYW5uZWxz',
        'X-Device-Info': _fox_device_info(cfg['requestor']),
    }
    session_resp = session.post(
        f'{FOX_ADOBE_API}/api/v2/{cfg["requestor"]}/sessions',
        data={
            'redirectUrl': cfg['redirect_url'],
            'domainName': 'fox.com',
            'mvpd': FOX_FNFB_TEMP_PASS_MVPD,
        },
        headers={**auth_headers, 'Content-Type': 'application/x-www-form-urlencoded'},
        timeout=30,
    )
    session_resp.raise_for_status()

    profile = session.get(
        f'{FOX_ADOBE_API}/api/v2/{cfg["requestor"]}/profiles/{FOX_FNFB_TEMP_PASS_MVPD}',
        headers={**auth_headers, 'Content-Type': 'application/json'},
        timeout=30,
    )
    profile.raise_for_status()

    decision = session.post(
        f'{FOX_ADOBE_API}/api/v2/{cfg["requestor"]}/decisions/authorize/{FOX_FNFB_TEMP_PASS_MVPD}',
        json={'resources': [cfg['resource']]},
        headers={**auth_headers, 'Content-Type': 'application/json'},
        timeout=30,
    )
    decision.raise_for_status()
    decisions = (decision.json().get('decisions') or [])
    authorized = next((item for item in decisions if item.get('authorized') is True), None)
    serialized = ((authorized or {}).get('token') or {}).get('serializedToken')
    if not serialized:
        raise ValueError(f'FOX Adobe authorization did not return a serialized token for {channel.name}')

    media_token = base64.b64decode(serialized).decode('utf-8', 'replace')
    generated = session.post(
        FOX_TOKEN_GENERATOR_API,
        json={'mediaToken': media_token},
        headers={**headers, 'Content-Type': 'application/json'},
        timeout=30,
    )
    generated.raise_for_status()
    hls_token = generated.json().get('token') or ''
    if not hls_token.startswith(('hdnea=', 'hdnts=')):
        raise ValueError(f'FOX token generator did not return an HLS token for {channel.name}')

    signed_url = f'{channel.fallback_stream_url}?{hls_token}'
    check = session.get(
        signed_url,
        headers={'User-Agent': UA, 'Accept': 'application/vnd.apple.mpegurl,*/*', 'Referer': channel.page_url},
        timeout=20,
    )
    check.raise_for_status()
    if '#EXTM3U' not in check.text[:256]:
        raise ValueError(f'FOX signed HLS validation did not return an HLS manifest for {channel.name}')

    exp = _signed_hls_exp(signed_url)
    _SIGNED_HLS_CACHE[channel.channel_id] = (signed_url, exp)
    return signed_url


def resolve_fox_page_signed_hls(channel: FoxTVEChannel) -> str:
    now = int(time.time())
    cached = _SIGNED_HLS_CACHE.get(channel.channel_id)
    if cached and cached[1] > now + 120:
        return cached[0]

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise ValueError('Playwright is not available for FOX signed HLS resolution') from exc

    matches: list[str] = []
    expected_host = urlsplit(channel.fallback_stream_url or '').netloc.lower()

    def _maybe_capture(url: str) -> None:
        if 'master.m3u8' not in url or 'hdne' not in url:
            return
        host = urlsplit(url).netloc.lower()
        if expected_host and host != expected_host:
            return
        if FOX_SIGNED_HLS_RE.search(url) and url not in matches:
            matches.append(url)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=UA,
                viewport={'width': 1365, 'height': 900},
            )
            page = context.new_page()
            page.on('request', lambda req: _maybe_capture(req.url))
            page.on('response', lambda resp: _maybe_capture(resp.url))
            page.goto(channel.page_url, wait_until='domcontentloaded', timeout=60000)
            deadline = time.time() + 18
            while time.time() < deadline and not matches:
                page.wait_for_timeout(250)
            browser.close()
    except Exception as exc:
        raise ValueError(f'FOX page did not expose signed HLS for {channel.name}: {exc}') from exc

    if not matches:
        raise ValueError(f'FOX page did not expose signed HLS for {channel.name}')

    signed_url = matches[0]
    exp = _signed_hls_exp(signed_url)
    _SIGNED_HLS_CACHE[channel.channel_id] = (signed_url, exp)
    return signed_url

def _parse_fox_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _first_text(value) -> str | None:
    if value is None or value == {}:
        return None
    if isinstance(value, str):
        return value or None
    if isinstance(value, dict):
        for key in ('#text', 'value', 'url', 'name'):
            text = _first_text(value.get(key))
            if text:
                return text
        return None
    if isinstance(value, list):
        for item in value:
            text = _first_text(item)
            if text:
                return text
    return str(value) if value else None


def _fox_schedule_hosts(show: dict) -> str | None:
    hosts = show.get('hosts')
    if isinstance(hosts, dict):
        hosts = hosts.get('name')
    names = [_first_text(host) for host in _as_list(hosts)]
    names = [name for name in names if name]
    return ', '.join(names) if names else None


def fetch_fox_news_business_schedule(channel_id: str) -> list[dict]:
    url = FOX_NEWS_BUSINESS_SCHEDULE_FEEDS.get(channel_id)
    if not url:
        return []
    r = requests.get(url, headers={'User-Agent': UA, 'Accept': 'application/json,*/*'}, timeout=20)
    r.raise_for_status()
    payload = r.json()
    shows: list[dict] = []
    for day in _as_list(payload.get('day')):
        if not isinstance(day, dict):
            continue
        for show in _as_list(day.get('show')):
            if isinstance(show, dict):
                shows.append(show)
    return shows


def _fox_news_business_program(channel: FoxTVEChannel, show: dict) -> ProgramData | None:
    start_time = _parse_fox_time(show.get('start-utc') or show.get('start'))
    end_time = _parse_fox_time(show.get('end-utc') or show.get('end'))
    if not start_time or not end_time or end_time <= start_time:
        return None

    title = _first_text(show.get('title')) or channel.guide_title
    description = (
        _first_text(show.get('long-description'))
        or _first_text(show.get('description'))
        or _first_text(show.get('short-description'))
    )
    hosts = _fox_schedule_hosts(show)
    if hosts and description and hosts not in description:
        description = f'{description} Hosts: {hosts}.'
    elif hosts and not description:
        description = f'Hosts: {hosts}.'

    poster_url = (
        _first_text(show.get('image'))
        or _first_text(show.get('feature-image'))
        or channel.logo_url
    )
    fox_id = _first_text(show.get('foxipedia-id')) or _first_text(show.get('machine-title'))
    return ProgramData(
        source_channel_id=channel.channel_id,
        title=title,
        description=description,
        start_time=start_time,
        end_time=end_time,
        poster_url=poster_url,
        category=channel.category,
        is_live=True,
        series_id=fox_id,
        episode_id=f'{channel.channel_id}:{fox_id or title}:{start_time.isoformat()}',
    )


def fetch_fox_sports_listings(start: datetime, hours: int = 24) -> list[dict]:
    call_signs = sorted({
        call_sign
        for channel in CHANNELS.values()
        for call_sign in channel.call_signs
    })
    if not call_signs:
        return []

    params = {
        'callsign': ','.join(call_signs),
        'end_date': str(int((start + timedelta(hours=hours)).timestamp())),
        'size': '50',
        'start_date': str(int(start.timestamp())),
        'video_type': 'listing',
    }
    headers = {
        'User-Agent': UA,
        'Accept': 'application/json',
        'content-type': 'application/json',
        'x-fox-apikey': FOX_SPORTS_API_KEY,
        'x-fox-request-id': '',
    }

    listings: list[dict] = []
    url = f'{FOX_SPORTS_PRODUCT_API}?{urlencode(params)}'
    for _ in range(8):
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        payload = r.json()
        data = payload.get('data') or {}
        listing_data = data.get('listings') or {}
        items = listing_data.get('items') or []
        listings.extend(item for item in items if isinstance(item, dict))
        next_url = (data.get('scroll') or {}).get('next')
        if not next_url:
            break
        url = 'https://api.fox.com/fs' + next_url if next_url.startswith('/product/') else next_url
    return listings


def _current_fox_sports_listing(channel: FoxTVEChannel, at: datetime | None = None) -> dict | None:
    at = at or datetime.now(timezone.utc)
    window_start = (at - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    call_signs = {value.upper() for value in channel.call_signs}
    try:
        listings = fetch_fox_sports_listings(window_start, hours=8)
    except requests.RequestException:
        return None

    candidates: list[tuple[int, datetime, dict]] = []
    for item in listings:
        if str(item.get('call_sign') or '').upper() not in call_signs:
            continue
        start_time = _parse_fox_time(item.get('start_time'))
        end_time = _parse_fox_time(item.get('end_time'))
        if not start_time or not end_time:
            continue
        if start_time <= at < end_time:
            live_rank = 0 if item.get('airing_type') == 'live' else 1
            candidates.append((live_rank, start_time, item))
    if not candidates:
        return None
    candidates.sort(key=lambda row: (row[0], row[1]))
    return candidates[0][2]


def _fox_sports_preview_token(session: requests.Session, device_id: str) -> str:
    headers = {
        'User-Agent': UA,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'x-api-key': FOX_SPORTS_API_KEY,
    }
    r = session.get(
        f'https://api3.fox.com/v2.0/previewpassmvpd?device_id={device_id}&mvpd_id={FOX_SPORTS_PREVIEW_MVPD}',
        headers=headers,
        timeout=20,
    )
    r.raise_for_status()
    return r.json()['accessToken']


def _jwt_payload(token: str) -> dict | None:
    try:
        part = token.split('.')[1]
        part += '=' * (-len(part) % 4)
        payload = json.loads(base64.urlsafe_b64decode(part.encode()).decode())
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _jwt_exp(token: str) -> int | None:
    payload = _jwt_payload(token) or {}
    exp = payload.get('exp')
    try:
        return int(exp) if exp else None
    except (TypeError, ValueError):
        return None


def _fox_json_headers(token: str | None = None) -> dict:
    headers = {
        'User-Agent': UA,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'x-api-key': FOX_SPORTS_API_KEY,
        'X-Delegated-Auth-Flow': 'true',
    }
    if token:
        headers['Authorization'] = f'Bearer {token}'
    return headers


def _cox_saml_login(session: requests.Session, cox_saml_url: str, username: str, password: str) -> None:
    from ..tve.adobe_pass import _hidden_form

    login_user = username.split('@', 1)[0] if username.lower().endswith('@cox.net') else username
    session.get(cox_saml_url, allow_redirects=True, timeout=30)
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
        },
        timeout=30,
    )
    r.raise_for_status()
    auth = r.json()
    if auth.get('status') != 'SUCCESS' or not auth.get('sessionToken'):
        raise ValueError(f'Cox authn did not succeed: {auth.get("status") or "unknown"}')

    redirect_url = 'https://login.cox.com/login/sessionCookieRedirect?' + urlencode({
        'checkAccountSetupComplete': 'true',
        'token': auth['sessionToken'],
        'redirectUrl': cox_saml_url,
    })
    r = session.get(redirect_url, allow_redirects=True, timeout=30)
    r.raise_for_status()
    action, form = _hidden_form(r.text, str(r.url))
    if 'SAMLResponse' not in form:
        raise ValueError('Cox SAML page did not include SAMLResponse')

    r = session.post(
        action,
        data=form,
        headers={'Content-Type': 'application/x-www-form-urlencoded', 'User-Agent': UA},
        allow_redirects=True,
        timeout=30,
    )
    r.raise_for_status()


def _fox_sports_cox_token(session: requests.Session, device_id: str, username: str, password: str) -> str:
    anon = session.post(
        'https://api3.fox.com/v2.0/login',
        headers=_fox_json_headers(),
        json={'deviceId': device_id},
        timeout=30,
    )
    anon.raise_for_status()
    anon_token = anon.json()['accessToken']
    headers = _fox_json_headers(anon_token)

    reg = session.post(
        'https://api3.fox.com/v2.0/accountregcode/v2',
        headers=headers,
        json={'deviceId': device_id, 'isRegister': False, 'isMvpd': True, 'selectedMvpdId': 'Cox'},
        timeout=30,
    )
    reg.raise_for_status()
    code = reg.json()['code']

    mvpd = session.post(
        f'https://api3.fox.com/v2.0/accountregcode/{code}/mvpdlogin',
        headers=headers,
        json={'mvpdId': 'Cox', 'redirectUrl': 'https://www.foxsports.com/live/fs1'},
        timeout=30,
    )
    mvpd.raise_for_status()
    auth_url = mvpd.json()['authenticateUrl']

    r = session.get(
        auth_url,
        headers={'Accept': 'text/html,application/json', 'User-Agent': UA},
        allow_redirects=False,
        timeout=30,
    )
    r.raise_for_status()
    cox_saml_url = r.headers.get('location') or ''
    if 'login.cox.com' not in cox_saml_url:
        raise ValueError(f'Unexpected FOX Adobe redirect host: {urlsplit(cox_saml_url).netloc}')

    _cox_saml_login(session, cox_saml_url, username, password)
    check = session.get(
        'https://api3.fox.com/v2.0/checkadobeauthn/v2',
        headers=headers,
        params={'device_id': device_id, 'requestor': 'fbc-fox'},
        timeout=30,
    )
    check.raise_for_status()
    token = check.json()['accessToken']
    if 'Cox' not in token:
        # JWT is opaque to callers; this cheap text check only catches obviously
        # wrong anonymous/PreviewPass responses before handing it to DVP.
        exp = _jwt_exp(token)
        if not exp:
            raise ValueError('FOX checkadobeauthn returned an invalid token')
    return token


def _fox_sports_access_token(session: requests.Session, device_id: str) -> str:
    from .. import db
    from ..models import TVEAccount

    account = TVEAccount.query.filter_by(provider_id='cox').first()
    now = int(datetime.now(timezone.utc).timestamp())
    if account and account.is_enabled and account.has_credentials():
        cfg = dict(account.config or {})
        cached_token = cfg.get('fox_sports_access_token') or ''
        cached_exp = int(cfg.get('fox_sports_access_token_exp') or 0)
        if cached_token and cached_exp > now + 300:
            return cached_token
        try:
            token = _fox_sports_cox_token(session, device_id, account.username or '', account.password or '')
            exp = _jwt_exp(token) or (now + 3600)
            cfg['fox_sports_access_token'] = token
            cfg['fox_sports_access_token_exp'] = exp
            account.config = cfg
            account.last_auth_status = 'ok'
            account.last_auth_message = 'FOX Sports MVPD token obtained through Cox.'
            account.last_auth_at = datetime.now(timezone.utc)
            db.session.commit()
            return token
        except Exception as exc:
            account.last_auth_status = 'error'
            account.last_auth_message = f'FOX Sports Cox auth failed: {exc}'[:500]
            account.last_auth_at = datetime.now(timezone.utc)
            db.session.commit()

    return _fox_sports_preview_token(session, device_id)


def resolve_fox_sports_hls(channel: FoxTVEChannel) -> str:
    listing = _current_fox_sports_listing(channel)
    if not listing:
        raise ValueError(f'No current FOX Sports listing found for {channel.name}')

    stream_id = listing.get('entity_id')
    if not stream_id:
        raise ValueError(f'Current FOX Sports listing has no entity_id for {channel.name}')

    session = requests.Session()
    device_id = str(uuid.uuid4())
    token = _fox_sports_access_token(session, device_id)
    headers = {
        'User-Agent': UA,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'x-api-key': FOX_SPORTS_API_KEY,
        'x-access-token': f'Bearer {token}',
    }
    payload = {
        'streamId': stream_id,
        'streamType': 'live',
        'capabilities': ['drm/widevine', 'fsdk/yo'],
        'deviceWidth': 1280,
        'deviceHeight': 720,
        'maxRes': '720p',
        'os': 'macos',
        'osv': '',
        'provider': {
            'freewheel': {'did': device_id},
            'vdms': {'rays': ''},
            'dmp': {'kuid': '', 'seg': ''},
        },
        'privacy': {'us': '1---'},
        'siteSection': '',
    }
    r = session.post(FOX_SPORTS_DVP_PLAYBACK_API, headers=headers, json=payload, timeout=20)
    r.raise_for_status()
    playback_url = r.json().get('playbackUrl')
    if not playback_url:
        raise ValueError(f'FOX Sports DVP response had no playbackUrl for {channel.name}')
    return playback_url


class FoxTVEScraper(BaseScraper):
    source_name = 'fox_tve'
    display_name = 'FOX TVE'
    source_category = 'tve'
    under_development = True
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = False

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
                guide_key=channel.channel_id.upper(),
                description=channel.description,
            )
            for channel in CHANNELS.values()
        ]

    def fetch_epg(self, channels, **kwargs):
        wanted = {ch.source_channel_id for ch in channels}
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        programs: list[ProgramData] = []

        for channel_id in ('fox_news', 'fox_business'):
            if channel_id not in wanted:
                continue
            channel = CHANNELS[channel_id]
            try:
                shows = fetch_fox_news_business_schedule(channel_id)
            except requests.RequestException:
                shows = []
            for show in shows:
                program = _fox_news_business_program(channel, show)
                if program:
                    programs.append(program)

        for channel_id, channel in CHANNELS.items():
            if channel_id != 'fox_weather' or channel_id not in wanted:
                continue
            programs.extend(
                ProgramData(
                    source_channel_id=channel.channel_id,
                    title=channel.guide_title,
                    description=channel.description,
                    start_time=now + timedelta(hours=i),
                    end_time=now + timedelta(hours=i + 1),
                    category=channel.category,
                    is_live=True,
                )
                for i in range(24)
            )

        call_sign_to_channel_id = {
            call_sign.upper(): channel.channel_id
            for channel in CHANNELS.values()
            if channel.channel_id in wanted
            for call_sign in channel.call_signs
        }
        if call_sign_to_channel_id:
            try:
                listings = fetch_fox_sports_listings(now, hours=24)
            except requests.RequestException:
                listings = []
            for item in listings:
                channel_id = call_sign_to_channel_id.get(str(item.get('call_sign') or '').upper())
                if not channel_id:
                    continue
                start_time = _parse_fox_time(item.get('start_time'))
                end_time = _parse_fox_time(item.get('end_time'))
                if not start_time or not end_time or end_time <= start_time:
                    continue
                images = item.get('images') or {}
                poster_url = (
                    images.get('still')
                    or images.get('video_list')
                    or images.get('series_list')
                    or images.get('series_detail')
                )
                genres = item.get('genres') or []
                programs.append(ProgramData(
                    source_channel_id=channel_id,
                    title=item.get('title') or CHANNELS[channel_id].guide_title,
                    description=item.get('description'),
                    start_time=start_time,
                    end_time=end_time,
                    poster_url=poster_url,
                    category=genres[0] if genres else CHANNELS[channel_id].category,
                    is_live=item.get('airing_type') == 'live',
                    series_id=item.get('series_id') or item.get('showcode'),
                    episode_id=item.get('entity_id') or item.get('tms_id') or item.get('foxipedia_id'),
                ))
        return programs

    def resolve(self, raw_url: str) -> str:
        channel = channel_for_url(raw_url)
        if not channel:
            raise ValueError(f'Unsupported FOX TVE stream URL: {raw_url}')
        if channel.channel_id == 'fox_weather':
            return find_live_hls(channel.page_url) or channel.fallback_stream_url
        if channel.signed_page_hls:
            now = int(time.time())
            cache_key = f'{SIGNED_HLS_CACHE_KEY_PREFIX}{channel.channel_id}'
            cached = self.cache.get(cache_key) or {}
            cached_url = cached.get('url') if isinstance(cached, dict) else None
            cached_exp = int(cached.get('exp') or 0) if isinstance(cached, dict) else 0
            if cached_url and cached_exp > now + 120:
                _SIGNED_HLS_CACHE[channel.channel_id] = (cached_url, cached_exp)
                return cached_url
            try:
                signed_url = resolve_fox_http_signed_hls(channel)
            except Exception:
                signed_url = resolve_fox_page_signed_hls(channel)
            self._update_cache(cache_key, {
                'url': signed_url,
                'exp': _signed_hls_exp(signed_url),
                'cached_at': now,
            })
            return signed_url
        return resolve_fox_sports_hls(channel)

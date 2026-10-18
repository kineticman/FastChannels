"""
tennis_tve.py — tennischannel.com's 24/7 channels: Tennis Channel (with a
TV-provider sign-in) plus the free Tennis Channel 2 and Pickleball TV. Scoped 2026-10-18 from a logged-out HAR plus the site's
own bundles; dev/tennis/SCOPED.md has the trace.

Key facts established by probing:

- tennischannel.com is a Sportradar (Agamo/"avvpl") OTT site. Its config
  (/api/v3/config/settings) names the TV-provider stream in
  additionalSettings.mvpdPage: content 1641153 "Tennis Channel TV Broadcast"
  (status Live, drmProtectionEnabled) and EPG channel 893.
- TV-provider access is Adobe Pass REST v2, requestor and resource both
  `tceverywhere`. The site registers the Adobe client for us:
  POST /api/v3/mvpd/register?with_device_identifier=true → a 6-hour client
  access token plus a deviceIdentifier the sign-in is bound to (sent as
  "AP-Device-Identifier: fingerprint <id>"). Re-registering without the flag
  renews the token for the same device.
- Playback: Adobe decisions/authorize → serializedToken, sent as X-Mvpd-Token
  to POST /api/v3/contents/<id>/check-access {type: mvpd} → {data: <code>};
  then POST /api/v3/contents/<id>/access/dash?authorization_code=<code> →
  {data: {stream, drmToken}} (the avvpl player's stream-access call).
  Widevine through BuyDRM KeyOS, so the channel is bridged through
  FastChannels Player.
- The free channels (content payment "Free": Tennis Channel 2, Pickleball TV)
  skip Adobe and check-access: POST access/dash with no code returns the
  same {stream, drmToken}.
- Channel list + guide: /api/v3/epg-channels/ (one per 24/7 feed, each with
  its currentlyPlaying content) and /api/v3/epg-channels/<id>/assets, public.

Not verified for Tennis Channel itself: the test account's provider doesn't
carry it, so its check-access response shape comes from the site's code.
"""
from __future__ import annotations

import base64
import logging
import re
import time
from datetime import datetime, timedelta, timezone

import requests

from .base import BaseScraper, ChannelData, ProgramData
from ..gracenote_map import resolve_gracenote
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import MvpdCooldownMixin, TVEAuthError, TVENotAuthorizedError

logger = logging.getLogger(__name__)

SCHEME = 'tennis-tve://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
SITE = 'https://www.tennischannel.com'
ADOBE_BASE = 'https://sp.auth.adobe.com'
REQUESTOR_ID = 'tceverywhere'
RESOURCE = 'tceverywhere'
NETWORK_KEY = 'tennis'
# What the site sends Adobe as domainName / redirectUrl.
DOMAIN_NAME = 'www.tennischannel.com'
REDIRECT_URL = f'{SITE}/en-us/page/home?fromMvpd=1'
_LICENSE_URL = 'https://widevine.keyos.com/api/v4/getLicense'
# Fallbacks for when the site config can't be read.
_DEFAULT_CONTENT = '1641153'
_DEFAULT_EPG_CHANNEL = '893'
_LOGO = f'{SITE}/image/120x120/4bc747a8-93ed-4078-82b9-f2bc83adc28f.png'
# EPG channel id → our name; the site calls two of them "GraceNote".
_NAMES = {'893': 'Tennis Channel', '930': 'Tennis Channel 2', '931': 'Pickleball TV'}

_GUIDE_DAYS = 7
# The stream URL's Akamai token (hdnea) lasts about a minute; the first
# manifest fetch trades it for a long one, so only reuse a URL that's fresh.
_STREAM_MIN_LIFE = 20
_TOKEN_MARGIN = 3600  # the site renews an hour before its 6-hour expiry


def device_info() -> str:
    """X-Device-Info: base64 of "<environment>_<user agent>", as the site sends it."""
    return base64.b64encode(f'production_{UA}'.encode()).decode()


def register_client(device_identifier: str | None = None) -> dict:
    """The site's Adobe client registration → {accessToken, expiresIn,
    createdAt, deviceIdentifier}. With a device identifier it only renews the
    token; without one the site issues a new identifier too."""
    url = f'{SITE}/api/v3/mvpd/register' + ('' if device_identifier else '?with_device_identifier=true')
    r = requests.post(url, json={}, timeout=20, headers={
        'User-Agent': UA, 'Origin': SITE, 'Referer': SITE + '/', 'Content-Type': 'application/json',
        'X-Device-Info': device_info()})
    try:
        data = r.json()
    except ValueError:
        data = {}
    if not r.ok or not data.get('accessToken'):
        raise TVEAuthError(f'Tennis Channel: Adobe client registration failed: HTTP {r.status_code}')
    if device_identifier:
        data['deviceIdentifier'] = device_identifier
    return data


def adobe_headers(token: str, device_identifier: str) -> dict:
    return {'User-Agent': UA, 'Authorization': f'Bearer {token}', 'X-Device-Info': device_info(),
            'AP-Device-Identifier': f'fingerprint {device_identifier}', 'Accept': 'application/json'}


class TennisTVEScraper(MvpdCooldownMixin, BaseScraper):
    """Tennis Channel (24/7 TV feed) with a TV-provider sign-in."""

    source_name = 'tennis_tve'
    display_name = 'Tennis Channel TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True
    license_url = _LICENSE_URL
    all_channels_require_drm_bridge = True

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({'User-Agent': UA, 'Origin': SITE, 'Referer': SITE + '/'})

    # ── channels + guide ─────────────────────────────────────────────────────

    def _mvpd_page(self) -> dict:
        """{content, epg_channel} for the TV-provider stream, from the site config."""
        try:
            r = self.session.get(f'{SITE}/api/v3/config/settings', params={'filter[deviceCategory]': 1}, timeout=20)
            r.raise_for_status()
            page = (((r.json() or {}).get('data') or {}).get('additionalSettings') or {}).get('mvpdPage') or {}
        except (requests.RequestException, ValueError) as exc:
            logger.warning('[tennis_tve] could not read the site config, using the known ids: %s', exc)
            page = {}
        return {'content': str(page.get('contentId') or _DEFAULT_CONTENT),
                'epg_channel': str(page.get('channelId') or _DEFAULT_EPG_CHANNEL)}

    def _feeds(self) -> list[dict]:
        """[{epg, content, free, name}] — one per 24/7 feed; the TV-provider
        one first, even if the EPG list can't be read."""
        page = self._mvpd_page()
        feeds = [{'epg': page['epg_channel'], 'content': page['content'], 'free': False}]
        try:
            r = self.session.get(f'{SITE}/api/v3/epg-channels/', timeout=20)
            r.raise_for_status()
            rows = (r.json() or {}).get('data') or []
        except (requests.RequestException, ValueError) as exc:
            logger.warning('[tennis_tve] could not read the channel list: %s', exc)
            rows = []
        for row in rows:
            epg = str(row.get('id'))
            content = ((row.get('currentlyPlaying') or row.get('nextPlaying') or {}).get('content') or {})
            if epg == page['epg_channel'] or not content.get('id'):
                continue
            try:
                c = self.session.get(f'{SITE}/api/v3/contents/{content["id"]}', timeout=20)
                c.raise_for_status()
                free = (((c.json() or {}).get('data') or {}).get('payment') or {}).get('name') == 'Free'
            except (requests.RequestException, ValueError):
                continue
            if not free:
                logger.info('[tennis_tve] skipping paid feed %s (not the TV-provider one)', epg)
                continue
            title = (((content.get('editorial') or {}).get('translations') or {}).get('en') or {}).get('title') or ''
            feeds.append({'epg': epg, 'content': str(content['id']), 'free': True,
                          'name': title.replace(' Broadcast', '').strip()})
        return feeds

    def fetch_channels(self) -> list[ChannelData]:
        out = []
        for feed in self._feeds():
            name = _NAMES.get(feed['epg']) or feed.get('name') or f'Tennis Channel {feed["epg"]}'
            out.append(ChannelData(
                source_channel_id=feed['epg'],
                name=name,
                slug='tennis-tve-' + name.lower().replace(' ', '-'),
                logo_url=_LOGO,
                stream_url=f'{SCHEME}{feed["epg"]}/{feed["content"]}' + ('?free=1' if feed['free'] else ''),
                stream_type='dash',
                category='Sports',
                language='en',
                country='US',
                guide_key=f'TENNIS:{feed["epg"]}',
                gracenote_id=resolve_gracenote('tennis_tve', lookup_key=feed['epg']),
            ))
        logger.info('[tennis_tve] %d channels', len(out))
        return out

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        programs = []
        for ch in channels:
            programs.extend(self._guide(ch.source_channel_id))
        logger.info('[tennis_tve] %d guide entries', len(programs))
        return programs

    def _guide(self, epg_channel: str) -> list[ProgramData]:
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        params = {'filter[dateTimeFrom]': (now - timedelta(hours=6)).isoformat(),
                  'filter[dateTimeTo]': (now + timedelta(days=_GUIDE_DAYS)).isoformat(), 'limit': 50}
        programs, seen, page = [], set(), 1
        while page <= 20:
            try:
                r = self.session.get(f'{SITE}/api/v3/epg-channels/{epg_channel}/assets',
                                     params={**params, 'page': page}, timeout=20)
                r.raise_for_status()
                body = r.json() or {}
            except (requests.RequestException, ValueError) as exc:
                logger.warning('[tennis_tve] guide page %d failed: %s', page, exc)
                break
            for row in body.get('data') or []:
                prog = _program(epg_channel, row)
                if prog and row.get('id') not in seen:
                    seen.add(row.get('id'))
                    programs.append(prog)
            if page >= int((body.get('meta') or {}).get('last_page') or 1):
                break
            page += 1
        programs.sort(key=lambda p: p.start_time)
        return programs

    # ── playback ─────────────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        # Validate entitlement, then hand back the opaque URL: the stream is
        # Widevine either way, and its token is single-use per access call.
        self.resolve(raw_url)
        return raw_url

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported Tennis Channel stream URL: {raw_url}')
        path, _, query = raw_url[len(SCHEME):].partition('?')
        epg, _, content = path.partition('/')
        if not epg or not content:
            raise ValueError(f'Unsupported Tennis Channel stream URL: {raw_url}')
        streams = dict(self.cache.get('tennis_streams') or {})
        cached = streams.get(epg)
        if cached and cached.get('content') == content and float(cached.get('exp') or 0) - time.time() > _STREAM_MIN_LIFE:
            return cached['stream']
        # The free feeds need no code; an empty body keeps Akamai from 400ing the POST.
        params = {} if 'free=1' in query else {'authorization_code': self._access_code(content, self._media_token())}
        r = self.session.post(f'{SITE}/api/v3/contents/{content}/access/dash', params=params, data=b'',
                              timeout=20, headers={'Accept': 'application/json'})
        data = _json(r).get('data') or {}
        stream = data.get('stream')
        drm_token = data.get('drmToken') or (data.get('multiDrm') or {}).get('token')
        if not r.ok or not stream:
            raise RuntimeError(f'Tennis Channel: stream access refused: HTTP {r.status_code} {_reason(r)}')
        m = re.search(r'hdnea=exp=(\d+)', stream)
        streams[epg] = {'content': content, 'stream': stream, 'drm_token': drm_token,
                        'exp': int(m.group(1)) if m else time.time() + 60}
        self._update_cache('tennis_streams', streams)
        return stream

    def _access_code(self, content: str, media_token: str) -> str:
        r = self.session.post(f'{SITE}/api/v3/contents/{content}/check-access', json={'type': 'mvpd'}, timeout=20,
                              headers={'Accept': 'application/json', 'X-Mvpd-Token': media_token})
        code = _json(r).get('data')
        if isinstance(code, dict):
            code = code.get('token') or code.get('accessToken')
        if r.status_code == 401 or not code:
            raise TVENotAuthorizedError(f'Tennis Channel: the site refused the TV-provider token '
                                        f'(HTTP {r.status_code} {_reason(r)}).')
        return code

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        return cls.license_url

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        headers = {'Content-Type': 'application/octet-stream', 'User-Agent': UA,
                   'Origin': SITE, 'Referer': SITE + '/'}
        token = ((config.get('tennis_streams') or {}).get(channel_id or '') or {}).get('drm_token')
        if token:
            # KeyOS v4 reads the token from x-keyos-authorization; older
            # KeyOS setups read the same value as customdata.
            headers['x-keyos-authorization'] = token
            headers['customdata'] = token
        return challenge, headers

    # ── Adobe ────────────────────────────────────────────────────────────────

    def _auth(self) -> dict:
        account = tve_account_for(self.source_name)
        auth = ((account.config or {}).get('tennis_mvpd_auth') or {}) if account else {}
        if not auth.get('device_identifier') or not auth.get('mso_id'):
            raise self._sign_in_needed(
                'Tennis Channel: not signed in yet — use "Sign in" for Tennis Channel under Settings → TV Everywhere.')
        return auth

    def _client_token(self, device_identifier: str, force: bool = False) -> str:
        cached = self.cache.get('tennis_client') or {}
        if (not force and cached.get('device') == device_identifier
                and float(cached.get('exp') or 0) - time.time() > _TOKEN_MARGIN):
            return cached['token']
        data = register_client(device_identifier)
        cached = {'token': data['accessToken'], 'device': device_identifier,
                  'exp': time.time() + float(data.get('expiresIn') or 21600)}
        self._update_cache('tennis_client', cached)
        return cached['token']

    def _media_token(self) -> str:
        """Adobe's short media token for tceverywhere."""
        auth = self._auth()
        url = f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/decisions/authorize/{auth["mso_id"]}'
        for attempt in range(2):
            token = self._client_token(auth['device_identifier'], force=attempt > 0)
            r = self.session.post(url, json={'resources': [RESOURCE]}, timeout=20, headers={
                **adobe_headers(token, auth['device_identifier']), 'Content-Type': 'application/json'})
            if r.status_code != 401:
                break
        decision = next(iter(_json(r).get('decisions') or []), {})
        media_token = (decision.get('token') or {}).get('serializedToken')
        if decision.get('authorized') and media_token:
            return media_token
        if r.status_code == 401 or (decision.get('error') or {}).get('code') in ('authenticated_profile_missing',
                                                                                  'authenticated_profile_expired'):
            raise self._sign_in_needed(
                f'Tennis Channel: the {auth["mso_id"]} sign-in has expired — sign in again under Settings → TV Everywhere.')
        raise TVENotAuthorizedError(f'Tennis Channel: your TV provider ({auth["mso_id"]}) doesn\'t include Tennis Channel.')

    @staticmethod
    def _sign_in_needed(message: str) -> TVEAuthError:
        from ..tve.signin_notice import mark_signin_needed
        mark_signin_needed(NETWORK_KEY, message)
        return TVEAuthError(message)


def _json(r: requests.Response) -> dict:
    try:
        body = r.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _reason(r: requests.Response) -> str:
    body = _json(r)
    return str(body.get('reason') or body.get('message') or '')[:120]


def _program(source_channel_id: str, row: dict) -> ProgramData | None:
    try:
        start = datetime.fromisoformat(row['start'])
        end = datetime.fromisoformat(row['end'])
    except (KeyError, TypeError, ValueError):
        return None
    editorial = (((row.get('content') or {}).get('editorial') or {}).get('translations') or {}).get('en') or {}
    title = (row.get('alternativeTitle') or editorial.get('alternativeText') or editorial.get('title') or '').strip()
    if not title or end <= start:
        return None
    description = row.get('alternativeDescription') or None
    return ProgramData(
        source_channel_id=source_channel_id,
        title=title,
        start_time=start,
        end_time=end,
        description=description,
        category='Sports',
    )

"""
disney_tve.py — Disney's TV Everywhere channels: your local ABC station,
Freeform, FX, FXX, FXM, Nat Geo, Nat Geo Wild, Disney Channel, Disney Junior,
Disney XD (East/West where Disney streams both), plus the free ABC News Live
and ABC-owned local news streams. Scoped 2026-10-18 — dev/disney_tve/SCOPED.md
has the full trace.

Key facts established by probing:

- Every brand plays through Disney Media Platform (BAM, the same stack as
  espn.py): an anonymous BAM device token, then /v7/playback/dtc-tve/
  ctr-regular with a tveAuth block (Adobe client access token + device
  fingerprint + an MRSS naming the Adobe resource). Widevine CENC (CTR) HLS,
  so every channel is bridged through FastChannels Player. The old Uplynk
  entitlement service the catalog still lists (drms NONE) is retired (503).
- The anonymous token must be minted with the BRAND's public browser key —
  with another brand's key the playback service answers 403 "blackout",
  which looks like a location block but isn't. ABC's key also covers
  Freeform; DisneyNOW has its own.
- Two Adobe Pass v2 sign-ins: requestor `dtci` (abc.com) authorizes ABC,
  Freeform, FX and Nat Geo; requestor `DisneyChannels` (disneynow.com)
  authorizes the Disney channels. A dtci token on a DisneyNOW channel → 401.
- Free channels (accesslevel 0) mint anonymously through /v7/playback/tve/.
- Channel list: abc.com's channelguide modules (cable nets + the local
  station; ABC News Live + local news) and DisneyNOW's videoplayer modules
  carry each channel's playbackId (`dmpPlaybackToken`). The local station's
  playbackId only appears when DATG-MVPD names a provider the station takes;
  entitlement is still checked when the stream is minted.
- Guide: the schedule module takes `channel=<brand>` and a multi-day window,
  with Gracenote TMS ids. It ignores the Pacific locale, so West feeds are
  the East schedule three hours later.
"""
from __future__ import annotations

import base64
import json
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin
from xml.sax.saxutils import escape

import requests

from .base import BaseScraper, ChannelData, ConfigField, ProgramData
from ..gracenote_map import resolve_gracenote
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import (MvpdCooldownMixin, TVEAuthError, TVENotAuthorizedError,
                              refresh_adobe_client_token)

logger = logging.getLogger(__name__)

SCHEME = 'disney-tve://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
ADOBE_BASE = 'https://sp.auth.adobe.com'
NETWORK_KEY = 'disney'
# Adobe requestor → the page whose bundle carries its software statement,
# and what that site sends Adobe as redirectUrl / domainName.
REQUESTORS = {
    'dtci': {'label': 'ABC, Freeform, FX, Nat Geo', 'page': 'https://abc.com/watch-live',
             'redirect': 'https://abc.com/', 'domain': 'abc.com'},
    'DisneyChannels': {'label': 'Disney Channel, Disney Junior, Disney XD', 'page': 'https://disneynow.com/watch-live',
                       'redirect': 'https://disneynow.com/', 'domain': 'disneynow.com'},
}

_GATEKEEPER = 'https://prod.gatekeeper.us-abc.symphony.edgedatg.com'
_PLUTO = f'{_GATEKEEPER}/api/ws/pluto/v1'
_GEO_URL = f'{_GATEKEEPER}/vp2/ws/utils/2021/geo/video/geolocation/001/001/gt/-1.jsonp'
# abc.com's live-page guide modules: cable nets + the viewer's station, and
# ABC News Live + the ABC-owned stations' free local news streams.
_GUIDE_MODULES = ('4209284', '4304137')
_GUIDE_LAYOUT = '4034190'
_SCHEDULE = f'{_PLUTO}/module/schedule/63241'
_SCHEDULE_LAYOUT = '2803949'
# DisneyNOW channels: brand → (videoplayer module, layout).
_DISNEYNOW_PLAYERS = {'004': ('553', '554'), '008': ('645', '647'), '009': ('682', '683')}
_DISNEYNOW_CHANNELS = 'https://api.contents.watchabc.go.com/vp2/ws/s/contents/3000/channels/{brand}/001/-1.json'
# Providers tried in DATG-MVPD to have the guide list the local station (after
# the viewer's own): which ones a station takes varies by market.
_STATION_MVPDS = ('Comcast_SSO', 'Spectrum', 'Charter_Direct', 'DTV', 'Cox', 'Verizon')

_BAM_GRAPH = 'https://disney-entertainment.api.edge.bamgrid.com/graph/v1/device/graphql'
_PLAYBACK = 'https://disney-entertainment.playback.edge.bamgrid.com/v7/playback'
_LICENSE_URL = 'https://disney-entertainment.playback.edge.bamgrid.com/widevine/v1/channel/obtain-license'
_BAM_HEADERS = {
    'User-Agent': UA,
    'Origin': 'https://abc.com',
    'Referer': 'https://abc.com/',
    'x-application-version': '0.0.1',
    'x-bamsdk-client-id': 'disney-entertainment-a03f82b0',
    'x-bamsdk-platform': 'javascript/windows/chrome',
    'x-bamsdk-version': '35.3',
}
# The sites' public browser keys (abc.com BRANDS_API_KEYS_PROD; DisneyNOW's bundle).
_BAM_KEYS = {
    'abc': 'YWJjJmJyb3dzZXImMS4wLjA.B1avqvrcbTb6GRneixWJGLgLCyVkVzOulkgaeD75Bys',
    'fx': 'Znhub3cmYnJvd3NlciYxLjAuMA.LAVIDYBTSmSiJQbcaW6iUEdI7pUDdFBMoiVC9tv3dgs',
    'natgeo': 'bmF0X2dlb190diZicm93c2VyJjEuMC4w.-QupTkjq1nczqTek6efei5xUORLGqU3tBOF6GvrYy8A',
    'disneynow': 'ZGlzbmV5bm93JmJyb3dzZXImMS4wLjA.A_LRL4CHG6rLxiliwQ6ZKygxM0S9gCErCNB7meYj5Ws',
}
# Disney brand code → BAM key, Adobe requestor, Adobe resource, category, logo.
_DELTA = 'https://assets-cdn.watchdisneyfe.com/delta/assets'
_BRANDS = {
    '001': ('abc', 'dtci', 'ABC', 'Entertainment', None),
    '002': ('abc', 'dtci', 'ABCFamily', 'Entertainment',
            'https://cdn1.edgedatg.com/tml/assets/content/freeform/networklogo_center_freeform.png'),
    '003': ('abc', None, None, 'News', f'{_DELTA}/abc/abcnewslive.png'),
    '200': ('abc', None, None, 'Local News',
            'https://cdn1.edgedatg.com/tml/assets/content/abc/networklogo_center_abc.png'),
    '020': ('fx', 'dtci', 'fx', 'Drama', f'{_DELTA}/fx/fx.png'),
    '021': ('fx', 'dtci', 'fxx', 'Comedy', f'{_DELTA}/fx/fx.png'),
    '022': ('fx', 'dtci', 'fxm', 'Movies', f'{_DELTA}/fx/fx.png'),
    '023': ('natgeo', 'dtci', 'ngc', 'Documentary', f'{_DELTA}/natgeo/nat-geo.png'),
    '024': ('natgeo', 'dtci', 'ngw', 'Nature', f'{_DELTA}/natgeo/nat-geo.png'),
    '004': ('disneynow', 'DisneyChannels', 'Disney', 'Kids', None),
    '008': ('disneynow', 'DisneyChannels', 'DisneyJunior', 'Kids', None),
    '009': ('disneynow', 'DisneyChannels', 'DisneyXD', 'Kids', None),
}
_NAMES = {'004': 'Disney Channel', '008': 'Disney Junior', '009': 'Disney XD'}
_LABELS = {**_NAMES, '001': 'your ABC station', '002': 'Freeform', '003': 'ABC News Live', '200': 'ABC local news',
           '020': 'FX', '021': 'FXX', '022': 'FXM', '023': 'Nat Geo', '024': 'Nat Geo Wild'}

_GUIDE_DAYS = 3
_STREAM_CACHE_TTL = 10 * 60
_BAM_TOKEN_TTL = 4 * 3600
_ADOBE_TOKEN_MAX_AGE = 5 * 3600     # client tokens last ~6h; the sign-ins 90 days / a year
_TOKEN_MARGIN = 300


class DisneyTVEScraper(MvpdCooldownMixin, BaseScraper):
    """ABC (local), Freeform, FX/FXX/FXM, Nat Geo/Wild, Disney Channel/Junior/XD
    (+ free ABC News Live and ABC local news)."""

    source_name = 'disney_tve'
    display_name = 'Disney TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 360
    stream_audit_enabled = True
    license_url = _LICENSE_URL
    # Widevine CENC on every channel — the HLS master reads as clear to the
    # generic audit, so bridge from the first scrape.
    all_channels_require_drm_bridge = True

    config_schema = [
        ConfigField('affiliate', 'Local ABC station', placeholder='e.g. WABC — blank = by your location',
                    help_text='Call sign of the ABC station to list. Blank uses the one Disney picks for this '
                              "server's location; Disney only plays a station inside its own market."),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({'User-Agent': UA, 'Origin': 'https://abc.com', 'Referer': 'https://abc.com/',
                                     'appversion': '9.32.0'})

    # ── channels ─────────────────────────────────────────────────────────────

    def _affiliate(self) -> str | None:
        configured = (self.config.get('affiliate') or '').strip().upper()
        if configured:
            return configured
        try:
            r = self.session.get(_GEO_URL, timeout=20)
            r.raise_for_status()
            text = r.text.strip()
            data = json.loads(text[text.find('{'):text.rfind('}') + 1])
            found = ((data.get('affiliates') or {}).get('affiliate') or [{}])[0].get('name')
        except (requests.RequestException, ValueError, IndexError) as exc:
            logger.warning('[disney_tve] could not look up the local ABC station: %s', exc)
            found = None
        return (found or '').upper() or None

    def _guide_module(self, module: str, affiliate: str | None, mvpd: str | None) -> list[dict]:
        # The guide only takes US offsets (-0400 … -1000); it lists what's on now either way.
        now = datetime.now(timezone.utc) - timedelta(hours=5)
        params = {'brand': '001', 'device': '001', 'authlevel': '0', 'layout': _GUIDE_LAYOUT, 'offset': '-0500',
                  'starttime': now.strftime('%Y%m%d-%H00'), 'endtime': (now + timedelta(hours=4)).strftime('%Y%m%d-%H00')}
        if affiliate:
            params['affiliate'] = affiliate
        r = self.session.get(f'{_PLUTO}/module/channelguide/{module}', params=params, timeout=20,
                             headers={'DATG-MVPD': mvpd} if mvpd else None)
        r.raise_for_status()
        return (r.json() or {}).get('channels') or []

    def _abc_channels(self, affiliate: str | None) -> list[dict]:
        """abc.com's guide rows, with the local station's playbackId filled in
        by trying providers until one the station takes is named."""
        auth = self._mvpd_auth().get('dtci') or {}
        mvpds = [m for m in (auth.get('mso_id'), *_STATION_MVPDS) if m]
        rows = self._guide_module(_GUIDE_MODULES[0], affiliate, mvpds[0])
        station = next((c for c in rows if c.get('brand') == '001'), None)
        for mvpd in mvpds[1:]:
            if not station or station.get('dmpPlaybackToken'):
                break
            station = next((c for c in self._guide_module(_GUIDE_MODULES[0], affiliate, mvpd)
                            if c.get('brand') == '001'), None)
        if station and not station.get('dmpPlaybackToken'):
            logger.warning('[disney_tve] no provider unlocked %s in the guide; leaving it out', station.get('title'))
        rows = [c for c in rows if c.get('brand') != '001'] + ([station] if station else [])
        rows += self._guide_module(_GUIDE_MODULES[1], affiliate, None)
        return rows

    def _disneynow_ids(self) -> dict[str, list[str]]:
        """brand → channel ids. disneynow.com/watch-live lists East and West;
        the contents API only the East feeds, so it's the fallback."""
        ids: dict[str, list[str]] = {}
        try:
            html = self.session.get(REQUESTORS['DisneyChannels']['page'], timeout=30).text
            start = html.find('"channels":[{"brand"')
            if start >= 0:
                rows, _ = json.JSONDecoder().raw_decode(html[start + len('"channels":'):])
                for c in rows:
                    if c.get('brand') in _DISNEYNOW_PLAYERS and c.get('id'):
                        ids.setdefault(c['brand'], []).append(c['id'])
        except (requests.RequestException, ValueError) as exc:
            logger.warning('[disney_tve] disneynow.com live page failed: %s', exc)
        for brand in _DISNEYNOW_PLAYERS:
            if ids.get(brand):
                continue
            try:
                r = self.session.get(_DISNEYNOW_CHANNELS.format(brand=brand), timeout=20)
                r.raise_for_status()
                ids[brand] = [c['id'] for c in (r.json() or {}).get('channel') or [] if c.get('id')]
            except (requests.RequestException, ValueError, KeyError) as exc:
                logger.warning('[disney_tve] DisneyNOW %s channel list failed: %s', brand, exc)
        return ids

    def _disneynow_channels(self) -> list[dict]:
        rows = []
        for brand, ids in self._disneynow_ids().items():
            module, layout = _DISNEYNOW_PLAYERS[brand]
            for vid in ids:
                try:
                    r = self.session.get(f'{_PLUTO}/module/videoplayer/{module}', timeout=20, params={
                        'brand': brand, 'device': '001', 'authlevel': '0', 'layout': layout, 'video': vid})
                    r.raise_for_status()
                    video = (r.json() or {}).get('video') or {}
                except (requests.RequestException, ValueError) as exc:
                    logger.warning('[disney_tve] DisneyNOW %s failed: %s', vid, exc)
                    continue
                if video.get('dmpPlaybackToken'):
                    rows.append({'id': vid, 'brand': brand, 'title': video.get('title'), 'accesslevel': '1',
                                 'dmpPlaybackToken': video['dmpPlaybackToken']})
        return rows

    def fetch_channels(self) -> list[ChannelData]:
        affiliate = self._affiliate()
        rows = self._abc_channels(affiliate) + self._disneynow_channels()
        out, seen = [], set()
        for c in rows:
            vid, brand, token = c.get('id'), c.get('brand'), c.get('dmpPlaybackToken')
            if not vid or not token or vid in seen or brand not in _BRANDS:
                continue
            try:
                channel_id = json.loads(base64.b64decode(token))['channelId']
            except (ValueError, KeyError, TypeError):
                continue
            seen.add(vid)
            name = _channel_name(c)
            call = (c.get('aff') or '').upper() if brand == '001' else ''
            logo = _BRANDS[brand][4]
            if brand == '001' and call:
                logo = f'https://cdn1.edgedatg.com/assets/media/m/fep/images/aff/{call.lower()}/{call}_Aria.png'
            out.append(ChannelData(
                source_channel_id=vid,
                name=name,
                slug=f'disney-{re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")}',
                logo_url=logo,
                stream_url=f'{SCHEME}{brand}/{vid}/{channel_id}' + ('?free=1' if str(c.get('accesslevel')) == '0' else ''),
                category=_BRANDS[brand][3],
                language='en',
                country='US',
                guide_key=f'DISNEY:{vid}',
                gracenote_id=resolve_gracenote('disney_tve', lookup_key=call or vid),
            ))
        self._update_cache('disney_channels', {ch.source_channel_id: {
            'brand': ch.stream_url[len(SCHEME):].split('/')[0], 'west': _is_west(ch.name),
            'affiliate': affiliate} for ch in out})
        logger.info('[disney_tve] %d channels (station %s)', len(out), affiliate or 'none')
        return out

    # ── guide ────────────────────────────────────────────────────────────────

    def _schedule(self, brand: str, affiliate: str | None, video: str | None = None) -> list[dict]:
        # Times are local to `offset`, which must be a US one (ABC News Live
        # answers nothing for +0000).
        now = (datetime.now(timezone.utc) - timedelta(hours=5)).replace(minute=0, second=0, microsecond=0)
        params = {
            'brand': '001', 'device': '001', 'layout': _SCHEDULE_LAYOUT, 'channel': brand,
            'starttime': (now - timedelta(hours=3)).strftime('%Y%m%d-%H%M'),
            'endtime': (now + timedelta(days=_GUIDE_DAYS)).strftime('%Y%m%d-%H%M'),
            # With an affiliate the module answers that station's lineup whatever the channel.
            'offset': '-0500', 'affiliate': (affiliate or '') if brand == '001' else '', 'locale': 'US/Eastern'}
        if video:
            params['video'] = video  # the local news streams (brand 200) each have their own
        r = self.session.get(_SCHEDULE, timeout=30, params=params)
        r.raise_for_status()
        return (r.json() or {}).get('videos') or []

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        meta = self.cache.get('disney_channels') or {}
        affiliate = next((m.get('affiliate') for m in meta.values() if m.get('affiliate')), None)
        schedules: dict[str, list[dict]] = {}
        programs = []
        for ch in channels:
            brand = ch.stream_url[len(SCHEME):].split('/')[0]
            # Each local news stream has its own schedule; the other brands share one per brand.
            key = ch.source_channel_id if brand == '200' else brand
            if key not in schedules:
                try:
                    schedules[key] = self._schedule(brand, affiliate,
                                                    video=ch.source_channel_id if brand == '200' else None)
                except (requests.RequestException, ValueError) as exc:
                    logger.warning('[disney_tve] guide for %s failed: %s', key, exc)
                    schedules[key] = []
            shift = timedelta(hours=3) if _is_west(ch.name) else timedelta(0)
            for v in schedules[key]:
                prog = _program(ch.source_channel_id, v, shift)
                if prog:
                    programs.append(prog)
        programs.sort(key=lambda p: (p.source_channel_id, p.start_time))
        logger.info('[disney_tve] %d guide entries', len(programs))
        return programs

    # ── playback ─────────────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        # Validate entitlement, then hand back the opaque URL so the audit skips
        # manifest inspection (the CENC markers live in the variant playlists).
        self.resolve(raw_url)
        return raw_url

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported Disney TVE stream URL: {raw_url}')
        path, _, query = raw_url[len(SCHEME):].partition('?')
        brand, vid, channel_id = (path.split('/') + ['', ''])[:3]
        if brand not in _BRANDS or not vid or not channel_id:
            raise ValueError(f'Unsupported Disney TVE stream URL: {raw_url}')
        streams = dict(self.cache.get('disney_streams') or {})
        cached = streams.get(vid)
        if cached and time.time() - float(cached.get('cached_at', 0)) < _STREAM_CACHE_TTL:
            return cached['manifest_url']
        streams[vid] = self._mint(brand, channel_id, free='free=1' in query)
        self._update_cache('disney_streams', streams)
        return streams[vid]['manifest_url']

    def _mint(self, brand: str, channel_id: str, free: bool) -> dict:
        key_name, requestor, resource = _BRANDS[brand][:3]
        token = self._bam_token(key_name)
        playback_id = base64.b64encode(json.dumps(
            {'channelId': channel_id, 'contentType': 'linear', 'sourceId': 'disney-entertainment-static'},
            separators=(',', ':')).encode()).decode()
        body = _playback_body(playback_id)
        if free or not requestor:
            r = self._post_playback(f'{_PLAYBACK}/tve/ctr-regular', body, token)
            return self._parse_playback(r, brand, key_name)
        auth = self._mvpd_auth().get(requestor) or {}
        if not auth.get('client_id'):
            raise self._sign_in_needed(
                'Disney TVE: not signed in yet — use "Sign in" for Disney under Settings → TV Everywhere.')
        mrss = ("<rss version='2.0' xmlns:media='http://search.yahoo.com/mrss/'><channel>"
                f"<title>{escape(resource)}</title></channel></rss>")
        fingerprint = base64.b64encode(auth['device_fingerprint'].encode()).decode()
        for attempt in range(2):
            body['tveAuth'] = {'tokenType': 'ADOBE', 'resource': mrss,
                               'accessToken': self._adobe_token(requestor, auth, force=attempt > 0),
                               'deviceIdentifier': 'fingerprint ' + fingerprint, 'mvpd': auth['mso_id']}
            r = self._post_playback(f'{_PLAYBACK}/dtc-tve/ctr-regular', body, token)
            # 401 adobe-pass-unauthorized: usually just a stale client token.
            if not (r.status_code == 401 and _error(r).get('code') == 'adobe-pass-unauthorized'):
                break
        else:
            raise self._sign_in_needed(
                f'Disney TVE: the {REQUESTORS[requestor]["label"]} sign-in was rejected — sign in again '
                'under Settings → TV Everywhere.')
        return self._parse_playback(r, brand, key_name)

    def _post_playback(self, url: str, body: dict, token: str) -> requests.Response:
        return self.session.post(url, json=body, timeout=20, headers={
            **_BAM_HEADERS, 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json',
            'Accept': 'application/vnd.media-service+json', 'x-dss-edge-accept': 'vnd.dss.edge+json; version=2',
            'x-dss-feature-filtering': 'true', 'x-request-id': str(uuid.uuid4())})

    @staticmethod
    def _parse_playback(r: requests.Response, brand: str, key_name: str) -> dict:
        try:
            data = r.json() if r.content else {}
        except ValueError:
            data = {}
        stream = data.get('stream') or {}
        sources = sorted(stream.get('sources') or [], key=lambda s: s.get('priority') or 99)
        manifest_url = next((((s.get('slide') or s.get('complete')) or {}).get('url')
                             for s in sources if (s.get('slide') or s.get('complete'))), None)
        ctx = (stream.get('playbackRights') or {}).get('playbackRightsContext')
        err = (data.get('errors') or [{}])[0]
        name = _LABELS.get(brand, brand)
        if r.status_code == 403 and err.get('code') in ('not-entitled', 'adobe-pass-failed-authorization'):
            raise TVENotAuthorizedError(f'Disney TVE: your TV provider doesn\'t include {name}.')
        if not r.ok or not manifest_url or not ctx:
            raise RuntimeError(f'Disney TVE playback ({name}): HTTP {r.status_code} '
                               f'{err.get("code", "")} {err.get("description", "")}'.strip())
        # The license must carry the same brand's BAM token the stream was minted with.
        return {'manifest_url': manifest_url, 'rights_ctx': ctx, 'key': key_name, 'cached_at': time.time()}

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        return cls.license_url

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        # No "Bearer " prefix on the license call, unlike the other BAM requests.
        headers = {**_BAM_HEADERS, 'Content-Type': 'application/octet-stream'}
        entry = (config.get('disney_streams') or {}).get(channel_id or '') or {}
        token = ((config.get('disney_bam_tokens') or {}).get(entry.get('key') or '') or {}).get('token')
        if token:
            headers['Authorization'] = token
        if entry.get('rights_ctx'):
            headers['x-playback-rights-authorization'] = entry['rights_ctx']
        return challenge, headers

    # ── tokens ───────────────────────────────────────────────────────────────

    def _bam_token(self, key_name: str) -> str:
        """Anonymous BAM token minted with the brand's own key."""
        tokens = dict(self.cache.get('disney_bam_tokens') or {})
        cached = tokens.get(key_name) or {}
        if cached.get('token') and float(cached.get('exp') or 0) - time.time() > _TOKEN_MARGIN:
            return cached['token']
        auth = 'Bearer ' + _BAM_KEYS[key_name]
        data = self._bam_gql(
            'mutation registerDevice($input: RegisterDeviceInput!) { registerDevice(registerDevice: $input) '
            '{ grant { grantType assertion } } }',
            {'input': {'deviceFamily': 'browser', 'applicationRuntime': 'chrome', 'deviceProfile': 'windows',
                       'deviceLanguage': 'en-US', 'devicePlatformId': 'browser',
                       'attributes': {'osDeviceIds': [], 'manufacturer': 'microsoft', 'model': None,
                                      'operatingSystem': 'windows', 'operatingSystemVersion': '10.0',
                                      'browserName': 'chrome', 'browserVersion': '140.0.0', 'brand': 'web'}}}, auth)
        grant = data['data']['registerDevice']['grant']['assertion']
        data = self._bam_gql(
            'mutation exchangeDeviceGrantForAccessToken($input: ExchangeDeviceGrantForAccessTokenInput!) '
            '{ exchangeDeviceGrantForAccessToken(exchangeDeviceGrantForAccessToken: $input) { accepted } }',
            {'input': {'deviceGrant': grant}}, auth)
        token = data['extensions']['sdk']['token']
        tokens[key_name] = {'token': token['accessToken'],
                            'exp': time.time() + float(token.get('expiresIn') or _BAM_TOKEN_TTL)}
        self._update_cache('disney_bam_tokens', tokens)
        return tokens[key_name]['token']

    def _bam_gql(self, query: str, variables: dict, auth: str) -> dict:
        r = self.session.post(_BAM_GRAPH, json={'query': query, 'variables': variables}, timeout=20,
                              headers={**_BAM_HEADERS, 'Authorization': auth, 'Content-Type': 'application/json'})
        try:
            data = r.json()
        except ValueError:
            raise RuntimeError(f'Disney TVE: device registration HTTP {r.status_code}') from None
        if data.get('errors'):
            raise RuntimeError(f'Disney TVE: device registration failed: {data["errors"][0]}')
        return data

    def _mvpd_auth(self) -> dict:
        """{requestor: saved Adobe client} from the TV-provider account."""
        account = tve_account_for(self.source_name)
        if not account:
            return {}
        return (account.config or {}).get('disney_mvpd_auth') or {}

    def _adobe_token(self, requestor: str, auth: dict, force: bool = False) -> str:
        """Access token for the Adobe client the sign-in belongs to, refreshed
        for that same client — a new client wouldn't carry the sign-in."""
        tokens = dict(self.cache.get('disney_adobe_tokens') or {})
        cached = tokens.get(requestor) or {}
        if (not force and cached.get('client_id') == auth['client_id']
                and time.time() - float(cached.get('at') or 0) < _ADOBE_TOKEN_MAX_AGE):
            return cached['token']
        try:
            token = refresh_adobe_client_token(auth['client_id'], auth['client_secret'])
        except TVEAuthError as exc:
            raise TVEAuthError(f'Disney TVE: could not renew the TV-provider sign-in: {exc}') from exc
        tokens[requestor] = {'token': token, 'at': time.time(), 'client_id': auth['client_id']}
        self._update_cache('disney_adobe_tokens', tokens)
        return token

    @staticmethod
    def _sign_in_needed(message: str) -> TVEAuthError:
        from ..tve.signin_notice import mark_signin_needed
        mark_signin_needed(NETWORK_KEY, message)
        return TVEAuthError(message)


# ── sign-in helpers (used by app/tve/link_login.py) ──────────────────────────

def software_statements(requestor: str) -> list[str]:
    """Adobe software statements on the requestor's site, in page order. The
    pages carry several (one per brand); the caller registers with each
    until Adobe accepts one for this requestor."""
    s = requests.Session()
    s.headers.update({'User-Agent': UA})
    page = REQUESTORS[requestor]['page']
    html = s.get(page, timeout=30).text
    found: list[str] = []
    texts = [html]
    for src in re.findall(r'src="([^"]+\.js[^"]*)"', html):
        try:
            texts.append(s.get(urljoin(page, src), timeout=20).text)
        except requests.RequestException:
            continue
    for text in texts:
        for jwt in re.findall(r'eyJhbGciOiJSUzI1NiJ9\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+', text):
            if jwt not in found and _jwt_claims(jwt).get('iss') == 'auth.adobe.com':
                found.append(jwt)
    return found


def _jwt_claims(token: str) -> dict:
    try:
        part = token.split('.')[1]
        return json.loads(base64.urlsafe_b64decode(part + '=' * (-len(part) % 4)))
    except (IndexError, ValueError):
        return {}


# ── helpers ──────────────────────────────────────────────────────────────────

def _playback_body(playback_id: str) -> dict:
    return {
        'playbackId': playback_id,
        'playback': {
            'attributes': {
                'resolution': {'max': ['1920x1080']}, 'protocol': 'HTTPS',
                'assetInsertionStrategies': {'point': 'SGAI', 'range': 'SGAI'},
                'playbackInitiationContext': 'ONLINE', 'frameRates': [60],
                'videoSegmentTypes': ['FMP4'], 'encryption': ['ctr'],
                'codecs': {'video': ['h.264'], 'audio': [{'name': 'aac', 'muxed': False}]},
                'maxSlideDuration': '4_HOUR', 'supportsPlaylistFiltering': True, 'iframe': False,
            },
            'adTracking': {'limitAdTrackingEnabled': 'NOT_SUPPORTED',
                           'deviceAdId': '00000000-0000-0000-0000-000000000000',
                           'privacyOptOut': 'NO', 'additionalConsent': ''},
            'tracking': {'playbackSessionId': str(uuid.uuid4())},
        },
        'allowedCreatives': [],
        'targeting': {'device': {'deviceOsName': 'windows', 'playerFrameworkName': 'HiVE-DMP'}},
    }


def _error(r: requests.Response) -> dict:
    try:
        return ((r.json() or {}).get('errors') or [{}])[0]
    except (ValueError, AttributeError):
        return {}


def _is_west(title: str | None) -> bool:
    return bool(title) and bool(re.search(r'\bWest\b', title))


def _channel_name(c: dict) -> str:
    brand, title = c.get('brand'), (c.get('title') or '').strip()
    if brand == '001':
        call = (c.get('aff') or title).upper()
        return f'ABC ({call})'
    if brand in _NAMES:
        return _NAMES[brand] + (' West' if _is_west(title) else '')
    # "Nat Geo East" → "Nat Geo"; keep "West" so both feeds stay apart.
    return re.sub(r'\s+(East|National)$', '', title)


def _parse_airtime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, '%a, %d %b %Y %H:%M:%S %z').astimezone(timezone.utc)
    except ValueError:
        return None


def _program(source_channel_id: str, v: dict, shift: timedelta) -> ProgramData | None:
    start = _parse_airtime(v.get('airtime'))
    duration = int(v.get('duration') or 0)
    if not start or duration <= 0:
        return None
    start += shift
    show = v.get('show') or {}
    show_title = (show.get('title') or '').strip()
    episode = (v.get('title') or '').strip()
    title = show_title or episode or 'Disney'
    images = sorted((i for i in v.get('images') or [] if i.get('value')),
                    key=lambda i: -(i.get('width') or 0))
    season, number = v.get('seasonNumber'), v.get('episodeNumber')
    tmsid = v.get('tmsid') or ''
    return ProgramData(
        source_channel_id=source_channel_id,
        title=title,
        start_time=start,
        end_time=start + timedelta(milliseconds=duration),
        description=(v.get('description') or '').strip() or None,
        episode_title=episode if episode and episode != title else None,
        season=int(season) if str(season or '').isdigit() else None,
        episode=int(number) if str(number or '').isdigit() else None,
        rating=v.get('tvrating') or None,
        category=(show.get('genre') or '').title() or None,
        poster_url=images[0]['value'] if images else None,
        program_type='movie' if tmsid.startswith('MV') else ('episode' if tmsid.startswith('EP') else None),
        series_id=show.get('id') or None,
        episode_id=tmsid or None,
    )

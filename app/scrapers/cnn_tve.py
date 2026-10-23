"""
cnn_tve.py — CNN, CNN International and HLN with a TV-provider sign-in.
Scoped and verified 2026-10-19 (Spectrum); dev/cnn/fresh_look_2026-10-19.md
has the trace (from dev/hitlist/hars/cnn.har).

Key facts established by probing:

- cnn.com/videos/live signs in with Adobe Pass REST v1 (Turner's TOP auth):
  requestor and resource `CNN`, the page's TOP_AUTH_SOFTWARE_STATEMENT. v2
  answers 401 for these Turner statements, so this uses the regcode flow
  (AdobePassCoxClient), like C-SPAN and Warner.
- Playback is WBD's Bolt API (default.any-any.prd.api.bolt.cnn.com), not
  NGTV: no CNN media id exists on medium.ngtv.io any more.
  1. An anonymous Bolt token: POST /refreshToken/exchange?realm=bolt (~30 days).
  2. POST /proxy-api/v1/tokenExchange with Adobe's short media token, the
     page's TOP_AUTH_SERVICE_APP_ID and the channel's `live…` media id. It
     answers 204 with an x-wbd-session-state `capabilities:` entry, which is
     merged into the session-state header the later calls send.
  3. POST /any/playback/v1/playbackInfo with the channel's Bolt edit id →
     a Widevine DASH manifest (dash-cmaf-cenc) and a pre-signed license URL.
  Skipping step 2 gives 403 access.denied.missingpackage — what a probe
  without a sign-in got in August 2026.
- One CNN authorization opens all three channels.
- Bolt checks the media token's signature and, after a grace period, its
  expiry (code 2506), so every tune gets a fresh short token.
- The manifest URL carries its own CDN token in the path and the manifest
  has no BaseURL, so segment URLs are relative to it. Clients that resolve
  them against the URL they asked for rather than the 302's target (ffmpeg's
  dash demuxer, some IPTV players) send them to us, so the play route serves
  the manifest itself with those paths made absolute (cnn_manifest_response
  in routes/play.py). A tune's URL is reused for a few minutes so the manifest
  refreshes that come back to us don't each run a token exchange. Manifest
  and segments send ACAO *. The license needs no headers beyond the challenge.
- Guide: HLN from the same wme-digital schedule widget warner_tve uses
  (it has no CNN or CNN International); CNN and CNN International from
  the tvtv cache under their Gracenote IDs.
"""
from __future__ import annotations

import base64
import json
import logging
import re
import time
import uuid
from types import SimpleNamespace

from .base import BaseScraper, ChannelData, ProgramData
from ..gracenote_map import resolve_gracenote
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import (
    AdobePassCoxClient, MvpdCooldownMixin, TVEAuthError, TVENotAuthorizedError, refresh_adobe_client_token,
)

logger = logging.getLogger(__name__)

SCHEME = 'cnn-tve://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
SITE = 'https://www.cnn.com'
LIVE_PAGE = f'{SITE}/videos/live'
REQUESTOR_ID = 'CNN'
NETWORK_KEY = 'cnn'
BOLT = 'https://default.any-any.prd.api.bolt.cnn.com'
# Any Bolt license URL; each tune's real, pre-signed one comes from
# playbackInfo and is what get_license_url() hands the license proxy.
LICENSE_BASE = 'https://busy.any-any.prd.api.discomax.com/drm-proxy/any/drm-proxy/drm/license/widevine'
_CLIENT_VERSION = '4.70.0'
# How long a tune's manifest URL is reused, and how close to its CDN token's
# Expires= it stops being reused.
_STREAM_REUSE_SECONDS = 600
_STREAM_EXPIRY_MARGIN = 300
_EXPIRES_RE = re.compile(r'Expires=(\d+)')

# source_channel_id -> channel. media_id and edit_id are the tve-asset ids
# on cnn.com/videos/live.
CHANNELS = {
    'cnn': {'name': 'CNN', 'media_id': 'livedee0fd3a7f81d264969c7f6f911079c3bd715cd1',
            'edit_id': '2b8da9d4-f838-5ab7-87ef-be29d1fceb40', 'gracenote': '58646',
            'logo': '/static/logos/cnn/cnn.png'},
    'cnni': {'name': 'CNN International', 'media_id': 'live4d61573b7a0615fa6cfeca73f1df9560efc93803',
             'edit_id': '29e834c6-0a32-536a-a815-de67b879eddb', 'gracenote': '10146',
             'logo': '/static/logos/cnn/cnni.png'},
    'hln': {'name': 'HLN', 'media_id': 'livecd0ee653fc42a51546445d02a6c38769c47ab454',
            'edit_id': '97936a20-6c8f-55ee-b942-aac242e07554', 'gracenote': '64549',
            'logo': '/static/logos/cnn/hln.png', 'widget': 'HLN'},
}

# The live page's TOP_AUTH_* values as of 2026-10-19, for when the page can't
# be read. Fresh copies replace them from the page once a day.
_SOFTWARE_STATEMENT = (
    'eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIyY2QwZTZiZC01ZjFlLTRhMjItYTRhMC01Njg3YzNjOWI3NTEiLCJuYmYiOjE1MzcxOTA'
    '3NTcsImlzcyI6ImF1dGguYWRvYmUuY29tIiwiaWF0IjoxNTM3MTkwNzU3fQ.tBxO0aQhj8sy6RPiDMeThvvZgBkYRNVr1VseVCV3'
    'soJZdQJO7dWCcjeNghS8Qg2pc4u7vy6MQNtABcMU25BnCEBH8xKBf4HWb49NaFQLnmdXQULpfc1Uts5_CY0ALAtMgmfEdI_lzB9a'
    '80FuEiZ4VZcGxSpy7QTgZZivBqaq9hk71Yynhik9nsCv8pcHUKBkdq5W4lMyMGbDVGlCcHepmjj3yohzyc-'
    '4_gsfqtkaJHQBBAXSSqYVTKkg6bM-1GmKm2nBhjDBTHngM3vyA0YjpZ5dVsrGkRpGdfXLnCYB_9T91h-'
    'dYV8tle_V0HiLAn_8EVOmuQmKl7BzBJlERwo8JA'
)
_APP_ID = (
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuZXR3b3JrIjoiY25uIiwicHJvZHVjdCI6ImNubiIsInBsYXRmb3JtIjoid2Vi'
    'LXRvcDIiLCJhcHBJZCI6ImNubi1jbm4td2ViLXRvcDItOWowYnI2In0.TbUdtroeG7T1gfSTUfdobssbI8vPsAX6tFEX5KI8hcA'
)

_SITE_CONFIG_TTL = 86400
_BOLT_TOKEN_MARGIN = 86400    # anonymous Bolt tokens last ~30 days
_CLIENT_TOKEN_TTL = 5 * 3600  # Adobe client tokens last ~6h
_AUTHZ_TTL = 23 * 3600        # Adobe authz tokens last 24h

_SOFTSTAT_RE = re.compile(r'"TOP_AUTH_SOFTWARE_STATEMENT":"([^"]+)"')
_APP_ID_RE = re.compile(r'"TOP_AUTH_SERVICE_APP_ID":"([^"]+)"')


def adobe_client(software_statement: str, auth: dict | None = None) -> AdobePassCoxClient:
    """A v1 client for CNN; with `auth` (a saved sign-in) it reuses that
    sign-in's Adobe client and device, which the sign-in is bound to."""
    auth = auth or {}
    creds = None
    if auth.get('client_id') and auth.get('access_token'):
        creds = {k: auth.get(k) for k in ('client_id', 'client_secret', 'access_token')}
    return AdobePassCoxClient(
        requestor_id=REQUESTOR_ID, resource=REQUESTOR_ID, software_statement=software_statement,
        redirect_url=LIVE_PAGE, device_fingerprint=auth.get('fingerprint') or uuid.uuid4().hex,
        client_creds=creds,
    )


class CNNTVEScraper(MvpdCooldownMixin, BaseScraper):
    """CNN, CNN International and HLN with a TV-provider sign-in."""

    source_name = 'cnn_tve'
    display_name = 'CNN TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 360
    stream_audit_enabled = True
    license_url = LICENSE_BASE
    all_channels_require_drm_bridge = True

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({'User-Agent': UA})

    # ── channels + guide ─────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        self._site_config(refresh=True)
        out = [
            ChannelData(
                source_channel_id=cid,
                name=c['name'],
                slug=f'cnn-tve-{cid}',
                logo_url=c['logo'],
                stream_url=f'{SCHEME}{cid}',
                stream_type='dash',
                category='News',
                language='en',
                country='US',
                gracenote_id=resolve_gracenote('cnn_tve', upstream_id=c['gracenote'], lookup_key=cid),
            )
            for cid, c in CHANNELS.items()
        ]
        logger.info('[cnn_tve] %d channels', len(out))
        return out

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        from .free_networks import FreeNetworksScraper
        from .warner_tve import WarnerTVEScraper, _fetch_widget_schedule

        programs = []
        for ch in channels:
            c = CHANNELS.get(ch.source_channel_id)
            if not c:
                continue
            # warner_tve's widget parser and placeholder only read these.
            like = SimpleNamespace(channel_id=ch.source_channel_id, name=c['name'], category='News')
            rows = []
            if c.get('widget'):
                grouped = _fetch_widget_schedule(c['widget'])
                rows = WarnerTVEScraper._parse_widget_schedule(like, grouped) if grouped else []
            if not rows and ch.gracenote_id:
                rows = FreeNetworksScraper._tvtv_programs(self, ch.source_channel_id, ch.gracenote_id)
            programs.extend(rows or WarnerTVEScraper._placeholder_epg(like))
        logger.info('[cnn_tve] %d guide entries', len(programs))
        return programs

    # ── playback ─────────────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        # Widevine everywhere; confirm the play flow works and hand back the
        # opaque URL so the audit skips manifest inspection (as vix does).
        self.resolve(raw_url)
        return raw_url

    def resolve(self, raw_url: str) -> str:
        cid = raw_url[len(SCHEME):] if raw_url.startswith(SCHEME) else ''
        if cid not in CHANNELS:
            raise ValueError(f'Unsupported CNN TVE stream URL: {raw_url}')
        cached = (self.cache.get('cnn_streams') or {}).get(cid) or {}
        if cached.get('url') and time.time() - float(cached.get('at') or 0) < _STREAM_REUSE_SECONDS:
            expires = _EXPIRES_RE.search(cached['url'])
            if not expires or int(expires.group(1)) - time.time() > _STREAM_EXPIRY_MARGIN:
                return cached['url']
        media_token = self._media_token(cid)
        for attempt in (0, 1):
            bolt = self._bolt(force=bool(attempt))
            r = self._bolt_post(bolt, '/session-context/headwaiter/v1/bootstrap', {})
            if _bolt_token_rejected(r) and not attempt:
                continue
            state = _merge_state('', r.headers.get('x-wbd-session-state'))
            r = self._bolt_post(bolt, '/proxy-api/v1/tokenExchange', {
                'accessTokenType': 'adobe', 'accessToken': media_token,
                'appId': self._site_config()['app_id'], 'mediaId': CHANNELS[cid]['media_id'],
                'params': {'realm': 'bolt', 'deviceId': bolt['device_id']},
            }, state)
            if _bolt_token_rejected(r) and not attempt:
                continue
            if not r.ok:
                raise TVEAuthError(f'CNN TVE: CNN refused the TV-provider token: HTTP {r.status_code} {_bolt_error(r)}')
            state = _merge_state(state, r.headers.get('x-wbd-session-state'))
            r = self._bolt_post(bolt, '/any/playback/v1/playbackInfo', self._playback_body(bolt, cid), state)
            if _bolt_token_rejected(r) and not attempt:
                continue
            break
        body = _json(r)
        if r.status_code == 403 and 'missingpackage' in _bolt_error(r):
            raise TVENotAuthorizedError(f'CNN TVE: your TV provider doesn\'t include {CHANNELS[cid]["name"]}.')
        url = (body.get('manifest') or {}).get('url')
        if not r.ok or not url:
            raise RuntimeError(f'CNN TVE: no stream for {CHANNELS[cid]["name"]}: HTTP {r.status_code} {_bolt_error(r)}')
        license_url = ((((body.get('drm') or {}).get('schemes') or {}).get('widevine')) or {}).get('licenseUrl')
        streams = dict(self.cache.get('cnn_streams') or {})
        streams[cid] = {'license': license_url, 'url': url, 'at': time.time()}
        self._update_cache('cnn_streams', streams)
        return url

    def _playback_body(self, bolt: dict, cid: str) -> dict:
        return {
            'appBundle': 'cnn', 'consumptionType': 'streaming', 'editId': CHANNELS[cid]['edit_id'],
            'deviceInfo': {
                'deviceId': bolt['device_id'], 'browser': {'name': 'Chrome', 'version': '141.0.0.0'},
                'make': 'desktop', 'model': 'desktop', 'os': {'name': 'Windows', 'version': '10'},
                'platform': 'WEB', 'deviceType': 'web',
                'player': {'sdk': {'name': 'Beam Player Desktop', 'version': _CLIENT_VERSION},
                           'mediaEngine': {'name': 'GLUON_BROWSER', 'version': '11.0.0'},
                           'playerView': {'height': 1080, 'width': 1920}},
            },
            'capabilities': {
                'manifests': {'formats': {'dash': {}}},
                'contentProtection': {'contentDecryptionModules': [
                    {'drmKeySystem': 'widevine', 'maxSecurityLevel': 'L3'}]},
            },
            'gdpr': False, 'firstPlay': False, 'userPreferences': {},
            'playbackSessionId': str(uuid.uuid4()), 'applicationSessionId': str(uuid.uuid4()),
        }

    # ── DRM license proxy wiring ─────────────────────────────────────────────

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        # The license proxy merges source_cache into config, so a tune's
        # pre-signed URL is here once resolve() has run for it.
        cached = ((config.get('cnn_streams') or {}).get(channel_id or '') or {}).get('license')
        return cached or cls.license_url

    @classmethod
    def prepare_license_request(cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs):
        return challenge, {'Content-Type': 'application/octet-stream', 'User-Agent': UA,
                           'Origin': SITE, 'Referer': SITE + '/'}

    # ── Bolt ─────────────────────────────────────────────────────────────────

    def _bolt_headers(self, device_id: str) -> dict:
        return {
            'Origin': SITE, 'Referer': SITE + '/',
            'x-device-info': f'cnn/{_CLIENT_VERSION} (desktop/desktop; Windows/10; {device_id})',
            'x-disco-client': f'WEB:10:cnn:{_CLIENT_VERSION}',
            'x-disco-params': 'realm=bolt,bid=cnn,features=ar',
            'x-wbd-preferred-language': 'en-US,en',
        }

    def _bolt(self, force: bool = False) -> dict:
        """The anonymous Bolt token and its device id, kept in the source cache."""
        bolt = self.cache.get('cnn_bolt') or {}
        if not force and bolt.get('token') and float(bolt.get('exp') or 0) - time.time() > _BOLT_TOKEN_MARGIN:
            return bolt
        device_id = str(uuid.uuid4())
        r = self.session.post(f'{BOLT}/refreshToken/exchange', params={'realm': 'bolt'},
                              headers=self._bolt_headers(device_id), timeout=20)
        token = ((_json(r).get('data') or {}).get('attributes') or {}).get('token')
        if not token:
            raise RuntimeError(f'CNN TVE: could not start a CNN session: HTTP {r.status_code} {_bolt_error(r)}')
        bolt = {'token': token, 'device_id': device_id, 'exp': _jwt_exp(token) or time.time() + 7 * 86400}
        self._update_cache('cnn_bolt', bolt)
        return bolt

    def _bolt_post(self, bolt: dict, path: str, body: dict, state: str = ''):
        headers = {**self._bolt_headers(bolt['device_id']), 'Authorization': f'Bearer {bolt["token"]}'}
        if state:
            headers['x-wbd-session-state'] = state
        return self.session.post(BOLT + path, json=body, headers=headers, timeout=20)

    def _site_config(self, refresh: bool = False) -> dict:
        """The live page's software statement and app id, refreshed at most
        daily (from fetch_channels); the built-in copies if it can't be read."""
        cached = self.cache.get('cnn_site_config') or {}
        if cached.get('app_id') and (not refresh or time.time() - float(cached.get('at') or 0) < _SITE_CONFIG_TTL):
            return cached
        if refresh:
            try:
                r = self.session.get(LIVE_PAGE, timeout=30)
                softstat, app_id = _SOFTSTAT_RE.search(r.text), _APP_ID_RE.search(r.text)
                if softstat and app_id:
                    cached = {'software_statement': softstat.group(1), 'app_id': app_id.group(1), 'at': time.time()}
                    self._update_cache('cnn_site_config', cached)
                    return cached
                logger.warning('[cnn_tve] the live page has no TOP auth config (HTTP %s)', r.status_code)
            except Exception as exc:
                logger.warning('[cnn_tve] could not read the live page: %s', exc)
        return {'software_statement': cached.get('software_statement') or _SOFTWARE_STATEMENT,
                'app_id': cached.get('app_id') or _APP_ID}

    def software_statement(self) -> str:
        return self._site_config()['software_statement']

    # ── Adobe ────────────────────────────────────────────────────────────────

    def _auth(self) -> tuple:
        account = tve_account_for(self.source_name)
        auth = ((account.config or {}).get('cnn_mvpd_auth') or {}) if account else {}
        if not auth.get('authn_token') or not auth.get('client_id'):
            raise self._sign_in_needed(
                'CNN TVE: not signed in yet — use "Sign in" for CNN under Settings → TV Everywhere.')
        return account, auth

    def _client(self, auth: dict) -> AdobePassCoxClient:
        """The sign-in's own Adobe client, with a current access token."""
        cached = self.cache.get('cnn_tve_client') or {}
        token = cached.get('token') if cached.get('client_id') == auth['client_id'] else None
        if not token or time.time() - float(cached.get('at') or 0) > _CLIENT_TOKEN_TTL:
            token = refresh_adobe_client_token(auth['client_id'], auth.get('client_secret') or '')
            self._update_cache('cnn_tve_client', {'client_id': auth['client_id'], 'token': token, 'at': time.time()})
        client = adobe_client(self.software_statement(), {**auth, 'access_token': token})
        client.setup_client()
        client.register_device()
        client.ctx.authn_token = auth['authn_token']
        return client

    def _media_token(self, cid: str) -> str:
        """Adobe's short media token (one CNN authorization covers all three
        channels, so the authz is cached under one key)."""
        _, auth = self._auth()
        client = self._client(auth)
        entry = self.cache.get('cnn_tve_authz') or {}
        try:
            if entry.get('authz_token') and time.time() - float(entry.get('at') or 0) < _AUTHZ_TTL:
                try:
                    return client.authorize_with_cached_authz(entry['authz_token'])
                except TVENotAuthorizedError:
                    raise
                except TVEAuthError as exc:
                    logger.info('[cnn_tve] cached authz rejected, authorizing again: %s', exc)
            token = client.authorize()
        except TVENotAuthorizedError as exc:
            raise TVENotAuthorizedError(
                f'CNN TVE: your TV provider ({auth.get("mso_id")}) doesn\'t include {CHANNELS[cid]["name"]}.') from exc
        except TVEAuthError as exc:
            if 'pendingLogout' in str(exc):
                raise self._sign_in_needed(
                    f'CNN TVE: the {auth.get("mso_id")} sign-in has expired — sign in again under '
                    'Settings → TV Everywhere.') from exc
            raise
        self._update_cache('cnn_tve_authz', {'authz_token': client.ctx.authz_token, 'at': time.time()})
        return token

    @staticmethod
    def _sign_in_needed(message: str) -> TVEAuthError:
        from ..tve.signin_notice import mark_signin_needed
        mark_signin_needed(NETWORK_KEY, message)
        return TVEAuthError(message)


def _merge_state(state: str, update: str | None) -> str:
    """Merge Bolt's x-wbd-session-state entries ("name:value;" pairs), the way
    its client does: a later entry replaces an earlier one of the same name."""
    parts = {}
    for chunk in (state, update or ''):
        for part in chunk.split(';'):
            name, sep, value = part.partition(':')
            if sep and name.strip():
                parts[name.strip()] = value
    return ''.join(f'{k}:{v};' for k, v in parts.items())


def _bolt_token_rejected(r) -> bool:
    return r.status_code == 401 or (r.status_code == 400 and 'invalid.token' in _bolt_error(r))


def _bolt_error(r) -> str:
    body = _json(r)
    errors = body.get('errors') if isinstance(body, dict) else None
    if errors and isinstance(errors[0], dict):
        return errors[0].get('code') or errors[0].get('detail') or ''
    auth_error = ((body.get('auth') or {}).get('error') or {}) if isinstance(body, dict) else {}
    return auth_error.get('message') or auth_error.get('msg') or ''


def _json(r):
    try:
        body = r.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _jwt_exp(token: str) -> float | None:
    try:
        part = token.split('.')[1]
        return float(json.loads(base64.urlsafe_b64decode(part + '=' * (-len(part) % 4))).get('exp'))
    except (IndexError, ValueError, TypeError):
        return None

"""
newsmax_tve.py — the main Newsmax channel with a TV-provider sign-in. Scoped
2026-11-13 from a signed-in HAR (dev/hitlist/hars/www.newsmaxtv.com3.har,
Spectrum account signing in as Cox).

Key facts established by probing:

- newsmaxtv.com is Adobe Pass REST v2, requestor and resource both `NEWSMAX`.
  Its 162-provider list includes Spectrum and Cox.
- The site registers the Adobe client for us:
  GET /CMSPages/NewsmaxTV/Handlers/TVEClientRegistration.ashx → a fresh
  {client_id, client_secret} each call. The page then trades them for a
  6-hour access token at /o/client/token and sends
  "AP-Device-Identifier: fingerprint <base64(client_id)>". Adobe binds the
  sign-in to that client, so the sign-in keeps its client_id/secret and
  renews the token for the same client (refresh_adobe_client_token) instead
  of registering again.
- After the sign-in the page calls decisions/authorize {resources:
  [NEWSMAX]} (a 2-hour authorization) and plays
  n1ctsota.akamaized.net/hls/live/2113880/Live_1/index.m3u8: clear HLS
  (no EXT-X-KEY), 1280x720 H.264/AAC with CC1, no token. The CDN doesn't
  check the authorization; the page does. resolve() does the same check, so
  a provider that drops Newsmax stops playback here.
- Guide: api.newsmaxtv.com/api/wideorbittimeline/1011/schedule, public, the
  same WideOrbit rows Free Networks reads for Newsmax2 (ShowDateTime naive
  Eastern, ShowDuration seconds), about a week.
"""
from __future__ import annotations

import base64
import json
import logging
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

from .base import BaseScraper, ChannelData, ProgramData
from ..gracenote_map import resolve_gracenote
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import MvpdCooldownMixin, TVEAuthError, TVENotAuthorizedError, refresh_adobe_client_token

logger = logging.getLogger(__name__)

SCHEME = 'newsmax-tve://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
SITE = 'https://www.newsmaxtv.com'
ADOBE_BASE = 'https://sp.auth.adobe.com'
REQUESTOR_ID = 'NEWSMAX'
RESOURCE = 'NEWSMAX'
NETWORK_KEY = 'newsmax'
# What the site sends Adobe as domainName / redirectUrl.
DOMAIN_NAME = 'newsmaxtv.com'
REDIRECT_URL = 'https://newsmax.com/mvpd-redirect?redirectURL=https%253A%252F%252Fwww.newsmaxtv.com%252F'
REGISTRATION_URL = f'{SITE}/CMSPages/NewsmaxTV/Handlers/TVEClientRegistration.ashx'
STREAM_URL = 'https://n1ctsota.akamaized.net/hls/live/2113880/Live_1/index.m3u8'
SCHEDULE_URL = 'https://api.newsmaxtv.com/api/wideorbittimeline/1011/schedule'
CHANNEL_ID = 'newsmax'
_GRACENOTE = '97163'  # Newsmax TV HD, as DirecTV and Spectrum carry it
_LOGO = 'https://tmsimg.fancybits.co/assets/s97163_ll_h15_aa.png?w=360&h=270'
_ET = ZoneInfo('America/New_York')

_CLIENT_TOKEN_MARGIN = 3600  # renew the 6-hour client token an hour early
_AUTHZ_MARGIN = 600          # re-authorize this long before the 2-hour authorization ends


def register_client() -> dict:
    """The site's Adobe client registration → {client_id, client_secret}.

    www.newsmaxtv.com (Akamai) leaves plain HTTP clients hanging (a 60s
    timeout on 2026-11-13), so the handler is called from the site's own
    page in Camoufox, as its jQuery.adobePass.js does (about 4s). Only a
    sign-in needs this; playback talks to Adobe alone."""
    try:
        from camoufox.sync_api import Camoufox
        with Camoufox(headless='virtual', os='windows', window=(1280, 800)) as browser:
            page = browser.new_page()
            page.goto(f'{SITE}/mvpd-picker', wait_until='domcontentloaded', timeout=60000)
            out = page.evaluate("""async (url) => {
                const r = await fetch(url, {headers: {'Accept': '*/*', 'Content-Type': 'application/json',
                                                      'X-Requested-With': 'XMLHttpRequest'}});
                let body = null;
                try { body = await r.json(); } catch (e) {}
                return {status: r.status, body: body};
            }""", REGISTRATION_URL)
    except Exception as exc:
        raise TVEAuthError(f'Newsmax: could not reach newsmaxtv.com to register the sign-in: {exc}') from exc
    data = out.get('body') or {}
    if not data.get('client_id') or not data.get('client_secret'):
        raise TVEAuthError(f'Newsmax: Adobe client registration failed (HTTP {out.get("status")}).')
    return {'client_id': data['client_id'], 'client_secret': data['client_secret']}


def device_identifier(client_id: str) -> str:
    return base64.b64encode(client_id.encode()).decode()


def adobe_headers(token: str, client_id: str) -> dict:
    """Headers the site's Adobe v2 calls carry."""
    info = base64.b64encode(json.dumps({'model': 'PC', 'osName': 'Windows', 'osVersion': '10.0',
                                        'version': '2.0', 'connectionType': '4G'}).encode()).decode()
    return {'User-Agent': UA, 'Accept': 'application/json', 'Authorization': f'Bearer {token}',
            'AP-Device-Identifier': f'fingerprint {device_identifier(client_id)}', 'X-Device-Info': info,
            'Origin': SITE, 'Referer': SITE + '/'}


class NewsmaxTVEScraper(MvpdCooldownMixin, BaseScraper):
    """Newsmax (the main channel) with a TV-provider sign-in."""

    source_name = 'newsmax_tve'
    display_name = 'Newsmax TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({'User-Agent': UA})

    # ── channels + guide ─────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        return [ChannelData(
            source_channel_id=CHANNEL_ID,
            name='Newsmax',
            slug='newsmax-tve-newsmax',
            logo_url=_LOGO,
            stream_url=f'{SCHEME}{CHANNEL_ID}',
            stream_type='hls',
            category='News',
            language='en',
            country='US',
            gracenote_id=resolve_gracenote('newsmax_tve', lookup_key=CHANNEL_ID) or _GRACENOTE,
        )]

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        try:
            r = self.session.get(SCHEDULE_URL, timeout=30, headers={'Origin': SITE, 'Referer': SITE + '/'})
            r.raise_for_status()
            rows = r.json() or []
        except (requests.RequestException, ValueError) as exc:
            logger.warning('[newsmax_tve] guide fetch failed: %s', exc)
            return []
        programs, seen = [], set()
        for row in rows if isinstance(rows, list) else []:
            try:
                start = datetime.fromisoformat(row['ShowDateTime']).replace(tzinfo=_ET)
                secs = int(row.get('ShowDuration') or 0)
            except (KeyError, TypeError, ValueError):
                continue
            title = (row.get('Title') or row.get('Program') or '').strip()
            if secs <= 0 or not title or start in seen:
                continue
            seen.add(start)
            episode = (row.get('Episode') or '').strip()
            for ch in channels:
                programs.append(ProgramData(
                    source_channel_id=ch.source_channel_id,
                    title=title,
                    start_time=start,
                    end_time=start + timedelta(seconds=secs),
                    episode_title=episode if episode and episode != title else None,
                    description=row.get('Description') or None,
                    category='News',
                ))
        logger.info('[newsmax_tve] %d guide entries', len(programs))
        return programs

    # ── playback ─────────────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported Newsmax TVE stream URL: {raw_url}')
        self._authorize()
        return STREAM_URL

    def audit_preflight(self) -> None:
        # One check up front instead of the audit failing the channel with a
        # vague error when nobody has signed in.
        self._auth()

    # ── Adobe ────────────────────────────────────────────────────────────────

    def _auth(self) -> dict:
        account = tve_account_for(self.source_name)
        auth = ((account.config or {}).get('newsmax_mvpd_auth') or {}) if account else {}
        if not auth.get('client_id') or not auth.get('client_secret') or not auth.get('mso_id'):
            raise self._sign_in_needed(
                'Newsmax: not signed in yet — use "Sign in" for Newsmax under Settings → TV Everywhere.')
        return auth

    def _client_token(self, auth: dict, force: bool = False) -> str:
        cached = self.cache.get('newsmax_client') or {}
        if (not force and cached.get('client_id') == auth['client_id']
                and float(cached.get('exp') or 0) - time.time() > _CLIENT_TOKEN_MARGIN):
            return cached['token']
        token = refresh_adobe_client_token(auth['client_id'], auth['client_secret'])
        self._update_cache('newsmax_client', {'token': token, 'client_id': auth['client_id'],
                                              'exp': time.time() + 6 * 3600})
        return token

    def _authorize(self) -> None:
        """Adobe's authorization for NEWSMAX, reused until shortly before it ends."""
        auth = self._auth()
        cached = self.cache.get('newsmax_authz') or {}
        if (cached.get('mso_id') == auth['mso_id'] and cached.get('client_id') == auth['client_id']
                and float(cached.get('exp') or 0) - time.time() > _AUTHZ_MARGIN):
            return
        url = f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/decisions/authorize/{auth["mso_id"]}'
        for attempt in range(2):
            token = self._client_token(auth, force=attempt > 0)
            r = self.session.post(url, json={'resources': [RESOURCE]}, timeout=20, headers={
                **adobe_headers(token, auth['client_id']), 'Content-Type': 'application/json; charset=UTF-8'})
            if r.status_code != 401:
                break
        decision = next(iter(_json(r).get('decisions') or []), {})
        if decision.get('authorized'):
            not_after = float(decision.get('notAfter') or 0) / 1000 or time.time() + 3600
            self._update_cache('newsmax_authz', {'mso_id': auth['mso_id'], 'client_id': auth['client_id'],
                                                 'exp': not_after})
            return
        code = (decision.get('error') or {}).get('code')
        if r.status_code == 401 or code in ('authenticated_profile_missing', 'authenticated_profile_expired'):
            raise self._sign_in_needed(
                f'Newsmax: the {auth["mso_id"]} sign-in has expired — sign in again under Settings → TV Everywhere.')
        if r.status_code >= 500 or (not decision and not r.ok):
            raise RuntimeError(f'Newsmax: Adobe authorization failed: HTTP {r.status_code}')
        raise TVENotAuthorizedError(f'Newsmax: your TV provider ({auth["mso_id"]}) doesn\'t include Newsmax.')

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

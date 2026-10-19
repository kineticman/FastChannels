"""
cspan_tve.py — the 24/7 C-SPAN, C-SPAN2 and C-SPAN3 networks with a
TV-provider sign-in. Scoped and verified 2026-10-19 (Spectrum);
dev/cspan/tve_fresh_look_2026-10-19.md has the trace.

The free event-driven channels (House/Senate floor, overflow) stay in
cspan.py; this source shares its guide API and its www.c-span.org fetch
coordination (Redis cache, WAF backoff, fleet-wide spacing).

Key facts established by probing:

- c-span.org/networks/?channel=c-span|c-span-2|c-span-3 embeds a Brightcove
  player gated by Adobe Pass REST v1 (AccessEnabler): requestor `C-SPAN`, the
  software statement in data-apsoftstat. v2 /sessions answers 401, so this
  uses the regcode flow (AdobePassCoxClient), like A+E and Warner.
- The Adobe resource is NOT the plain data-resourceid. Brightcove checks the
  media token against the video's custom_fields.bcadobepassresourceid, an MRSS
  string naming the network and its Brightcove video id (_resource below,
  mirroring the site's getAPResourceId). A token for plain "C-SPAN" is
  accepted by Adobe but Brightcove answers SOURCES_RESTRICTED / TVE_AUTH.
- Brightcove playback API: videos/<bcid>?tveToken=<short media token> with the
  player's policy key → sources[0].src, a fixed Akamai URL per network on
  cspan{1,2,3}-msl4.c-spanvideo.org.
- The manifest needs ?hdnts=<token> from the page's data-akamaitoken (2h,
  acl=/*). One page's token opens all three networks, so only the C-SPAN page
  is fetched. Without it the manifest is 403; with it, clear HLS (no
  EXT-X-KEY), 1024x576 H.264/AAC with CC1. Variant paths carry their own
  hdntl and segments need no Referer, so playback is a plain 302.
- www.c-span.org 202-challenges (AWS WAF) after a handful of quick fetches,
  so the page is fetched through cspan.py's coordinated _fetch_page and
  cached until shortly before its token expires.
"""
from __future__ import annotations

import logging
import re
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import quote

from .base import BaseScraper, ChannelData, ProgramData
from .cspan import (
    CSpanScraper, _LOGO_CSPAN, _LOGO_CSPAN2, _LOGO_CSPAN3, _await_fetch_slot, _cache_get, _cache_set,
    _fetch_page,
)
from ..gracenote_map import resolve_gracenote
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import (
    AdobePassCoxClient, MvpdCooldownMixin, TVEAuthError, TVENotAuthorizedError, refresh_adobe_client_token,
)

logger = logging.getLogger(__name__)

SCHEME = 'cspan-tve://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
SITE = 'https://www.c-span.org'
REQUESTOR_ID = 'C-SPAN'
NETWORK_KEY = 'cspan'
REDIRECT_URL = f'{SITE}/networks/?channel=c-span'
PAGE_URL = f'{SITE}/networks/?channel=c-span'
BC_ACCOUNT = '3162030207001'
BC_PLAYER_JS = f'https://players.brightcove.net/{BC_ACCOUNT}/2B2qWQJYYM_default/index.min.js'
BC_PLAYBACK = f'https://edge.api.brightcove.com/playback/v1/accounts/{BC_ACCOUNT}/videos/'

# source_channel_id -> network. `resource` and `title` are the page's
# data-resourceid and data-bctitle; `network` is the guide API's id.
NETWORKS = {
    'cspan': {'name': 'C-SPAN', 'resource': 'C-SPAN', 'title': 'C-SPAN 1', 'bcid': '6306075540112',
              'network': 1, 'logo': _LOGO_CSPAN},
    'cspan2': {'name': 'C-SPAN2', 'resource': 'C-SPAN2', 'title': 'C-SPAN 2', 'bcid': '6306074571112',
               'network': 2, 'logo': _LOGO_CSPAN2},
    'cspan3': {'name': 'C-SPAN3', 'resource': 'C-SPAN3', 'title': 'C-SPAN 3', 'bcid': '6306076431112',
               'network': 3, 'logo': _LOGO_CSPAN3},
}

# The page's data-apsoftstat as of 2026-10-19, for when the page can't be read
# (WAF) while a sign-in is starting. Fresh copies replace it from the page.
_SOFTWARE_STATEMENT = (
    'eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIyMzE0N2QxOS01ZTZhLTRiNjUtODhlMS1hOTlhNjA2N2E5MDciLCJuYmYiOjE1ODg3OTM'
    'wMDksImlzcyI6ImF1dGguYWRvYmUuY29tIiwiaWF0IjoxNTg4NzkzMDA5fQ.X9ElQnfrekQicD7kaSEvffXQoaG5o-z2x0yUuKMm'
    'ayXLfVimmyYx8JrPfuCAAIPpdwW6IjgKp4VEulOoA-_ArF5By9Ha8iNvkdKgr6MzTjY-KF_iQmPH2BZumXq_cHDvXHVpXeApGijr'
    'Qj3wzDpilK5oHrqLbj5ozuOhroQLisU8t88--sbbtXSBkBP3Xa1x4j5utHsQuT3Q5TUeJw-90eQpjgho58B_F6AY00gy68o2gAlT'
    'J5moKBldXesIUX5oTMvH8eE791xqVsMJzGSv9j0rv0fXR3w9h4yWXahBGbO16oJBqEG9P3gp96i3zX--Ug6zbQvhT2XpvOYHj83r'
    'EA'
)

_PAGE_CACHE_KEY = 'tve:page'  # in cspan.py's Redis namespace, shared by both workers
_TOKEN_MARGIN = 600           # refetch the page this long before its token expires
_POLICY_KEY_TTL = 7 * 86400
_CLIENT_TOKEN_TTL = 5 * 3600  # Adobe client tokens last ~6h
_AUTHZ_TTL = 23 * 3600        # Adobe authz tokens last 24h

_AKAMAI_RE = re.compile(r'data-akamaitoken="([^"]+)"')
_SOFTSTAT_RE = re.compile(r'data-apsoftstat="([^"]+)"')
_EXP_RE = re.compile(r'exp=(\d+)')
_POLICY_KEY_RE = re.compile(r'policyKey\s*:\s*"([^"]+)"')


def _resource(cid: str) -> str:
    """The MRSS resource Brightcove checks the media token against (see the
    site's getAPResourceId)."""
    n = NETWORKS[cid]
    return ("<rss version='2.0' xmlns:media='http://search.yahoo.com/mrss/'><channel>"
            f"<title>{n['resource']}</title><item><title>{n['title']}</title><guid>{n['bcid']}</guid>"
            "<media:rating scheme='urn:v-chip'>UR</media:rating></item></channel></rss>")


def adobe_client(software_statement: str, auth: dict | None = None) -> AdobePassCoxClient:
    """A v1 client for C-SPAN; with `auth` (a saved sign-in) it reuses that
    sign-in's Adobe client and device, which the sign-in is bound to."""
    auth = auth or {}
    creds = None
    if auth.get('client_id') and auth.get('access_token'):
        creds = {k: auth.get(k) for k in ('client_id', 'client_secret', 'access_token')}
    return AdobePassCoxClient(
        requestor_id=REQUESTOR_ID, resource=_resource('cspan'), software_statement=software_statement,
        redirect_url=REDIRECT_URL, device_fingerprint=auth.get('fingerprint') or uuid.uuid4().hex,
        client_creds=creds,
    )


class CSpanTVEScraper(MvpdCooldownMixin, BaseScraper):
    """C-SPAN, C-SPAN2 and C-SPAN3 (24/7) with a TV-provider sign-in."""

    source_name = 'cspan_tve'
    display_name = 'C-SPAN TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 180  # the guide API only looks ~24-38h ahead
    stream_audit_enabled = True

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({'User-Agent': UA})

    # ── channels + guide ─────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        out = [
            ChannelData(
                source_channel_id=cid,
                name=n['name'],
                slug=f'cspan-tve-{cid}',
                logo_url=n['logo'],
                stream_url=f'{SCHEME}{cid}',
                stream_type='hls',
                category='News',
                language='en',
                country='US',
                guide_key=f'CSPAN:{n["network"]}',
                gracenote_id=resolve_gracenote('cspan_tve', lookup_key=cid),
            )
            for cid, n in NETWORKS.items()
        ]
        logger.info('[cspan_tve] %d channels', len(out))
        return out

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        programs = []
        for ch in channels:
            n = NETWORKS.get(ch.source_channel_id)
            if not n:
                continue
            # cspan.py's guide fetch only needs a requests session on self.
            rows = CSpanScraper._epg_rows_from_schedule(ch, CSpanScraper._fetch_network_schedule(self, n['network']))
            programs.extend(rows or [CSpanScraper._neutral_block(ch, datetime.now(timezone.utc))])
        logger.info('[cspan_tve] %d guide entries', len(programs))
        return programs

    # ── playback ─────────────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        cid = raw_url[len(SCHEME):] if raw_url.startswith(SCHEME) else ''
        if cid not in NETWORKS:
            raise ValueError(f'Unsupported C-SPAN TVE stream URL: {raw_url}')
        src = self._brightcove_src(cid, self._media_token(cid))
        page = self._page()
        if not page.get('token'):
            raise RuntimeError("C-SPAN TVE: c-span.org is rate-limiting us right now; try again in a couple of minutes.")
        return src + ('&' if '?' in src else '?') + 'hdnts=' + quote(page['token'], safe='')

    def _brightcove_src(self, cid: str, media_token: str) -> str:
        r = self.session.get(BC_PLAYBACK + NETWORKS[cid]['bcid'], params={'tveToken': media_token}, timeout=20,
                             headers={'Accept': f'application/json;pk={self._policy_key()}',
                                      'Origin': SITE, 'Referer': SITE + '/'})
        body = _json(r)
        sources = body.get('sources') if isinstance(body, dict) else None
        src = next((s.get('src') for s in sources or [] if '.m3u8' in (s.get('src') or '')), None)
        if src:
            return src
        # A restricted video is a 200 with sources [] and an errors list; a
        # bad request is a bare list of errors.
        errors = body if isinstance(body, list) else body.get('errors') or []
        error = errors[0] if errors and isinstance(errors[0], dict) else {}
        if error.get('error_subcode') == 'TVE_AUTH':
            raise TVEAuthError(f'C-SPAN TVE: Brightcove refused the {NETWORKS[cid]["name"]} TV-provider token.')
        raise RuntimeError(f'C-SPAN TVE: no stream for {NETWORKS[cid]["name"]}: HTTP {r.status_code} '
                           f'{error.get("error_code") or ""}'.strip())

    def _policy_key(self) -> str:
        cached = self.cache.get('cspan_tve_policy_key') or {}
        if cached.get('key') and time.time() - float(cached.get('at') or 0) < _POLICY_KEY_TTL:
            return cached['key']
        r = self.session.get(BC_PLAYER_JS, timeout=20)
        m = _POLICY_KEY_RE.search(r.text if r.ok else '')
        if not m:
            if cached.get('key'):
                return cached['key']
            raise RuntimeError(f'C-SPAN TVE: could not read the Brightcove player key (HTTP {r.status_code}).')
        self._update_cache('cspan_tve_policy_key', {'key': m.group(1), 'at': time.time()})
        return m.group(1)

    def _page(self) -> dict:
        """{token, exp, software_statement} from the C-SPAN networks page,
        shared across workers; {} when the page can't be read right now."""
        cached = _cache_get(_PAGE_CACHE_KEY)
        page = cached[1] if cached else {}
        if page.get('token') and float(page.get('exp') or 0) - time.time() > _TOKEN_MARGIN:
            return page
        _await_fetch_slot()
        html = _fetch_page(self, PAGE_URL)
        token = _AKAMAI_RE.search(html or '')
        if not token:
            if html is not None:
                logger.warning('[cspan_tve] the networks page has no Akamai token')
            return page if float(page.get('exp') or 0) > time.time() else {}
        exp = _EXP_RE.search(token.group(1))
        softstat = _SOFTSTAT_RE.search(html)
        page = {'token': token.group(1), 'exp': int(exp.group(1)) if exp else time.time() + 3600,
                'software_statement': softstat.group(1) if softstat else page.get('software_statement')}
        _cache_set(_PAGE_CACHE_KEY, time.time(), page)
        return page

    def software_statement(self) -> str:
        return self._page().get('software_statement') or _SOFTWARE_STATEMENT

    # ── Adobe ────────────────────────────────────────────────────────────────

    def _auth(self) -> tuple:
        account = tve_account_for(self.source_name)
        auth = ((account.config or {}).get('cspan_mvpd_auth') or {}) if account else {}
        if not auth.get('authn_token') or not auth.get('client_id'):
            raise self._sign_in_needed(
                'C-SPAN TVE: not signed in yet — use "Sign in" for C-SPAN under Settings → TV Everywhere.')
        return account, auth

    def _client(self, auth: dict) -> AdobePassCoxClient:
        """The sign-in's own Adobe client, with a current access token."""
        cached = self.cache.get('cspan_tve_client') or {}
        token = cached.get('token') if cached.get('client_id') == auth['client_id'] else None
        if not token or time.time() - float(cached.get('at') or 0) > _CLIENT_TOKEN_TTL:
            token = refresh_adobe_client_token(auth['client_id'], auth.get('client_secret') or '')
            self._update_cache('cspan_tve_client', {'client_id': auth['client_id'], 'token': token, 'at': time.time()})
        client = adobe_client(self.software_statement(), {**auth, 'access_token': token})
        client.setup_client()
        client.register_device()
        client.ctx.authn_token = auth['authn_token']
        return client

    def _media_token(self, cid: str) -> str:
        """Adobe's short media token for one network."""
        _, auth = self._auth()
        client = self._client(auth)
        client.resource = _resource(cid)
        authz = dict(self.cache.get('cspan_tve_authz') or {})
        entry = authz.get(cid) or {}
        try:
            if entry.get('authz_token') and time.time() - float(entry.get('at') or 0) < _AUTHZ_TTL:
                try:
                    return client.authorize_with_cached_authz(entry['authz_token'])
                except TVENotAuthorizedError:
                    raise
                except TVEAuthError as exc:
                    logger.info('[cspan_tve] cached authz for %s rejected, authorizing again: %s', cid, exc)
            token = client.authorize()
        except TVENotAuthorizedError as exc:
            raise TVENotAuthorizedError(
                f'C-SPAN TVE: your TV provider ({auth.get("mso_id")}) doesn\'t include {NETWORKS[cid]["name"]}.') from exc
        except TVEAuthError as exc:
            if 'pendingLogout' in str(exc):
                raise self._sign_in_needed(
                    f'C-SPAN TVE: the {auth.get("mso_id")} sign-in has expired — sign in again under '
                    'Settings → TV Everywhere.') from exc
            raise
        authz[cid] = {'authz_token': client.ctx.authz_token, 'at': time.time()}
        self._update_cache('cspan_tve_authz', authz)
        return token

    @staticmethod
    def _sign_in_needed(message: str) -> TVEAuthError:
        from ..tve.signin_notice import mark_signin_needed
        mark_signin_needed(NETWORK_KEY, message)
        return TVEAuthError(message)


def _json(r):
    try:
        body = r.json()
    except ValueError:
        return {}
    return body if isinstance(body, (dict, list)) else {}

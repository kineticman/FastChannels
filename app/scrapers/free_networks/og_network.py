"""OG Network's live channels (play.theognetwork.net/#/live): 84 small FAST
channels (CaribVision, Nigbati TV, ToonOvation, Colorized.TV, ZAZ Gamers, ...),
most carried by no other source.

The web app is a static site on Sotal Cloud's SDP. The channel list comes from
the site's own catalog.json, which it regenerates daily from SDP. Playback
follows the web app: an anonymous client_credentials token for its public
client (sent as x-auth-token), the channel asset's 'hls-live' resource, then a
playback session whose playback_url is clear HLS (up to 1080p) on noisypeak.
The session ticket in that URL lasts ~10 minutes, so it's created per tune.
Some channels' variant playlists carry no segments at all (a dozen on
2026-10-27, FearStream and CaribVision among them), which players fail on
with opaque errors, so resolve() checks for segments before handing it over.

Guide: the web app shows none, but SDP serves the one OG's phone apps show
to the same anonymous token. /epg-events?$expand=channel is every channel's
schedule in one ~10 MB response (filters are ignored), keyed by an EPG id that
/assets?$expand=epg maps back to each channel's asset id. Checked 2026-10-27:
83 of 84 channels carry rows (about a day and a half ahead), and the titles
airing matched the playback URL's ads.content_title on all 10 sampled.
"""
from __future__ import annotations

import threading
import time
import uuid
from urllib.parse import urljoin

from ._common import EmptyPlaylistError, Network, NetChannel, logger, parse_iso, program

_CATALOG = 'https://play.theognetwork.net/catalog.json'
_SDP = 'https://discovery-ams.sotalcloud.com/sdp/v2'
# Sotal tenant: web client id cloud:<tenant>_ottweb_device, and the web app's host.
OG_TENANT = ('ognetwork', 'play.theognetwork.net')
# Tokens carry no expiry; take a new one now and then, and on any 401.
_TOKEN_MAX_AGE = 6 * 3600
# Checked 2026-10-26 by matching the playback URL's ads.content_title against
# tvtv's now-playing; fills holes in OG's own guide. Other name matches (Dark
# Matter TV, Ghost Dimension, Viajar, Terra, The Family Channel, ...) air
# different schedules.
_GRACENOTE = {'471446': '49411'}  # CaribVision
# Tagged Hispanic/LATAM but in English.
_ENGLISH = {'The TRUMP Channel', 'New America Channel', 'GALLI Lucha Libre'}

_token_lock = threading.Lock()
_tokens: dict[str, tuple[str, float]] = {}


def headers(tenant: tuple[str, str], token: str | None = None) -> dict:
    h = {'Origin': f'https://{tenant[1]}', 'Referer': f'https://{tenant[1]}/'}
    if token:
        h['x-auth-token'] = token
    return h


def catalog(s) -> list[dict]:
    """OG's catalog.json channels (shared with the partner-site network, which
    skips anything OG already lists)."""
    def fetch():
        r = s.get(_CATALOG, params={'t': int(time.time() * 1000)}, headers=headers(OG_TENANT))
        return [ch for ch in ((r.json() or {}).get('channels') or []) if ch.get('type') == 'channel'] if r else []
    return s.memo('ognetwork_catalog', fetch)


# Sotal partner channels on AWS MediaTailor answer their playlists only with
# the site's Origin/Referer (504 otherwise; segments play without), which a
# client following a 302 doesn't send. Those streams go through the Free
# Networks playlist proxy with the tenant's headers; everything else still
# gets a plain redirect (Network.proxies).
_MEDIATAILOR_HOSTS = ('mediatailor.us-west-2.amazonaws.com', 'mediatailor.us-east-1.amazonaws.com',
                      'mediatailor.us-east-2.amazonaws.com')


class SotalProxy:
    """Mixin for Sotal tenants: proxy MediaTailor streams with the tenant's headers."""
    tenant: tuple[str, str]
    proxied = True
    proxy_hosts = _MEDIATAILOR_HOSTS

    def proxies(self, url: str) -> bool:
        return self.proxy_allows(url)

    def proxy_headers(self) -> dict:
        return headers(self.tenant)


class OGNetwork(SotalProxy, Network):
    key = 'ognetwork'
    name = 'OG Network'
    tenant = OG_TENANT
    # Channels the last programs() run had no current rows for, so they get
    # tvtv or placeholder blocks.
    _unguided: frozenset[str] = frozenset()

    def has_guide(self, channel_id: str) -> bool:
        return channel_id not in self._unguided

    def channels(self, s):
        out = []
        for ch in catalog(s):
            asset_id, title = ch.get('assetId'), (ch.get('title') or '').strip()
            if not asset_id or not title:
                continue
            genres = [g for g in ch.get('genres') or [] if g != 'Live TV']
            out.append(NetChannel(
                str(asset_id), title, str(asset_id),
                logo=ch.get('posterImg') or None,
                category=genres[0] if genres else None,
                language='es' if 'Hispanic' in genres and title not in _ENGLISH else 'en',
                gracenote_id=_GRACENOTE.get(str(asset_id)),
                description=_description(ch.get('synopsis')),
            ))
        return out

    def programs(self, s, channels):
        out = tenant_programs(s, OG_TENANT, channels, str)
        self._unguided = frozenset({ch.id for ch in channels} - {p.source_channel_id for p in out})
        return out

    def resolve(self, s, ref):
        return playback_url(s, OG_TENANT, ref)


def with_token(s, tenant: tuple[str, str], fn):
    """fn(token), retried once with a fresh token if SDP answers 401."""
    try:
        return fn(auth_token(s, tenant))
    except PermissionError:
        return fn(auth_token(s, tenant, fresh=True))


def _get(s, tenant, token, path, params=None, timeout=30):
    r = s.session.get(f'{_SDP}/{path}', params=params, headers=headers(tenant, token), timeout=timeout)
    if r.status_code == 401:
        raise PermissionError(f'Sotal {tenant[0]} token rejected')
    r.raise_for_status()
    return r.json() or {}


def tenant_assets(s, tenant: tuple[str, str]) -> list[dict]:
    """Every asset of a tenant with its EPG ref and genres (movies and episodes
    too; about 0.5-2 MB), memoized per scrape run."""
    return s.memo(f'sotal_assets_{tenant[0]}', lambda: with_token(
        s, tenant, lambda tok: _get(s, tenant, tok, 'assets', {'$expand': 'epg,genres'}).get('data') or []))


def tenant_programs(s, tenant: tuple[str, str], channels, channel_id_for) -> list:
    """Guide rows for these channels from the tenant's /epg-events.
    channel_id_for(asset_id) maps an asset id to the NetChannel id."""
    by_epg = {}
    for asset in tenant_assets(s, tenant):
        epg_id = (asset.get('epg') or {}).get('id')
        if epg_id and asset.get('id'):
            by_epg[epg_id] = channel_id_for(asset['id'])
    data = with_token(s, tenant, lambda tok: _get(s, tenant, tok, 'epg-events', {'$expand': 'channel'}, timeout=60))
    wanted = {ch.id for ch in channels}
    rows: dict[tuple, dict] = {}
    for row in data.get('data') or []:
        cid = by_epg.get((row.get('channel') or {}).get('id'))
        start = parse_iso(row.get('start_time'))
        if cid not in wanted or not start:
            continue
        # A few new channels (FearStream, WonderViz, MI Healthy Mind, ...)
        # carry two syncs' schedules interleaved; on a shared start the
        # newer sync (higher id) wins.
        prev = rows.get((cid, start))
        if not prev or (row.get('id') or 0) > (prev.get('id') or 0):
            rows[(cid, start)] = row
    now = time.time()
    out = []
    ordered = sorted(rows.items())
    for i, ((cid, start), row) in enumerate(ordered):
        end = parse_iso(row.get('end_time'))
        # ...and otherwise overlap, so each entry ends by the next one's start.
        if end and i + 1 < len(ordered) and ordered[i + 1][0][0] == cid:
            end = min(end, ordered[i + 1][0][1])
        if not end or end.timestamp() < now:
            continue
        p = program(cid, row.get('name'), start, end,
                    description=_description(row.get('description') or row.get('episode_description')),
                    episode_title=row.get('episode_name'))
        if p:
            out.append(p)
    return out


def playback_url(s, tenant: tuple[str, str], asset_id: str) -> str:
    return with_token(s, tenant, lambda tok: _playback_url(s, tenant, asset_id, tok))


def _playback_url(s, tenant, asset_id: str, token: str) -> str:
    resources = (_get(s, tenant, token, f'assets/{asset_id}', {'$expand': 'resources'}, timeout=20)
                 .get('data') or {}).get('resources') or []
    live = next((res for res in resources if res.get('type') == 'hls-live'), None)
    if not live:
        raise RuntimeError(f'Sotal {tenant[0]} channel {asset_id} has no live stream')
    r = s.session.post(f'{_SDP}/assets/{asset_id}/resources/{live["id"]}/playback-sessions',
                       json={}, headers=headers(tenant, token), timeout=20)
    if r.status_code == 401:
        raise PermissionError(f'Sotal {tenant[0]} token rejected')
    r.raise_for_status()
    url = ((r.json() or {}).get('data') or {}).get('playback_url')
    if not url:
        raise RuntimeError(f'Sotal {tenant[0]} gave no playback URL for channel {asset_id}')
    problem = _stream_problem(s, tenant, url)
    if problem:
        raise EmptyPlaylistError(f'Sotal {tenant[0]} channel {asset_id} {problem}')
    return url


def _description(text: str | None) -> str | None:
    # Nigbati TV fills it with just 'Movie' or 'Show'.
    return None if not text or text.strip() in ('Movie', 'Show', 'TBD') else text


def _stream_problem(s, tenant, url: str) -> str | None:
    """Why the stream Sotal handed out can't play, or None. Only a definite
    answer counts: the master or first variant answering 404/410/5xx (some
    partner channels point at a dead MediaTailor or CDN origin and 504 every
    time), or a variant that lists no segments. A timeout or connection error
    lets the URL through for the player to try (some origins are just slow).
    resolve() reports a problem as EmptyPlaylistError: playback gets a quick
    503 instead of a redirect to a dead stream, and the audit (see
    FreeNetworksScraper.audit_resolve) marks the channel dead after repeated
    tries."""
    def dead(r) -> bool:
        return r.status_code in (404, 410) or r.status_code >= 500
    try:
        r = s.session.get(url, headers=headers(tenant), timeout=15)
        if dead(r):
            return f'stream answered HTTP {r.status_code}'
        if not r.ok:
            return None
        variant = next((ln.strip() for ln in r.text.splitlines() if ln.strip() and not ln.startswith('#')), None)
        if not variant or '#EXT-X-STREAM-INF' not in r.text:
            return None  # not a master playlist: nothing further to probe
        v = s.session.get(urljoin(r.url, variant), headers=headers(tenant), timeout=15)
        if dead(v):
            return f'stream variant answered HTTP {v.status_code}'
        if v.ok and '#EXTINF' not in v.text:
            return 'is serving an empty playlist (no segments)'
        return None
    except Exception:
        return None


def auth_token(s, tenant: tuple[str, str], fresh: bool = False) -> str:
    """The tenant web client's anonymous SDP token, shared across tunes."""
    with _token_lock:
        cached = _tokens.get(tenant[0])
        if not fresh and cached and time.time() - cached[1] < _TOKEN_MAX_AGE:
            return cached[0]
        r = s.session.post(f'{_SDP}/authorize', json={}, headers=headers(tenant), timeout=20, params={
            'client_id': f'cloud:{tenant[0]}_ottweb_device', 'grant_type': 'client_credentials',
            'device_id': uuid.uuid4().hex[:8]})
        r.raise_for_status()
        token = (r.json() or {}).get('data')
        if not token:
            raise RuntimeError(f'Sotal {tenant[0]} authorize returned no token')
        logger.debug('[free_networks] Sotal %s: new SDP token', tenant[0])
        _tokens[tenant[0]] = (token, time.time())
        return token

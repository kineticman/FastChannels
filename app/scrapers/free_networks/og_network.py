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

from ._common import Network, NetChannel, logger, parse_iso, program

_CATALOG = 'https://play.theognetwork.net/catalog.json'
_SDP = 'https://discovery-ams.sotalcloud.com/sdp/v2'
_CLIENT_ID = 'cloud:ognetwork_ottweb_device'
_HEADERS = {'Origin': 'https://play.theognetwork.net', 'Referer': 'https://play.theognetwork.net/'}
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
_token: tuple[str, float] | None = None


class OGNetwork(Network):
    key = 'ognetwork'
    name = 'OG Network'
    # Channels the last programs() run had no current rows for, so they get
    # tvtv or placeholder blocks.
    _unguided: frozenset[str] = frozenset()

    def has_guide(self, channel_id: str) -> bool:
        return channel_id not in self._unguided

    def channels(self, s):
        r = s.get(_CATALOG, params={'t': int(time.time() * 1000)}, headers=_HEADERS)
        if not r:
            return []
        out = []
        for ch in (r.json() or {}).get('channels') or []:
            asset_id, title = ch.get('assetId'), (ch.get('title') or '').strip()
            if not asset_id or not title or ch.get('type') != 'channel':
                continue
            genres = [g for g in ch.get('genres') or [] if g != 'Live TV']
            out.append(NetChannel(
                str(asset_id), title, str(asset_id),
                logo=ch.get('posterImg') or None,
                category=genres[0] if genres else None,
                language='es' if 'Hispanic' in genres and title not in _ENGLISH else 'en',
                gracenote_id=_GRACENOTE.get(str(asset_id)),
            ))
        return out

    def programs(self, s, channels):
        try:
            return self._programs(s, channels, _auth_token(s))
        except PermissionError:
            return self._programs(s, channels, _auth_token(s, fresh=True))

    def _programs(self, s, channels, token):
        headers = {**_HEADERS, 'x-auth-token': token}
        r = s.session.get(f'{_SDP}/assets', params={'$expand': 'epg'}, headers=headers, timeout=30)
        if r.status_code == 401:
            raise PermissionError('OG Network token rejected')
        r.raise_for_status()
        by_epg = {}
        for asset in (r.json() or {}).get('data') or []:
            epg_id = (asset.get('epg') or {}).get('id')
            if epg_id and asset.get('id'):
                by_epg[epg_id] = str(asset['id'])
        r = s.session.get(f'{_SDP}/epg-events', params={'$expand': 'channel'}, headers=headers, timeout=60)
        if r.status_code == 401:
            raise PermissionError('OG Network token rejected')
        r.raise_for_status()
        wanted = {ch.id for ch in channels}
        rows: dict[tuple, dict] = {}
        for row in (r.json() or {}).get('data') or []:
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
                        description=_description(row),
                        episode_title=row.get('episode_name'))
            if p:
                out.append(p)
        self._unguided = frozenset(wanted - {p.source_channel_id for p in out})
        return out

    def resolve(self, s, ref):
        try:
            return self._playback_url(s, ref, _auth_token(s))
        except PermissionError:
            return self._playback_url(s, ref, _auth_token(s, fresh=True))

    def _playback_url(self, s, asset_id: str, token: str) -> str:
        headers = {**_HEADERS, 'x-auth-token': token}
        r = s.session.get(f'{_SDP}/assets/{asset_id}', params={'$expand': 'resources'}, headers=headers, timeout=20)
        if r.status_code == 401:
            raise PermissionError('OG Network token rejected')
        r.raise_for_status()
        resources = ((r.json() or {}).get('data') or {}).get('resources') or []
        live = next((res for res in resources if res.get('type') == 'hls-live'), None)
        if not live:
            raise RuntimeError(f'OG Network channel {asset_id} has no live stream')
        r = s.session.post(f'{_SDP}/assets/{asset_id}/resources/{live["id"]}/playback-sessions',
                           json={}, headers=headers, timeout=20)
        if r.status_code == 401:
            raise PermissionError('OG Network token rejected')
        r.raise_for_status()
        url = ((r.json() or {}).get('data') or {}).get('playback_url')
        if not url:
            raise RuntimeError(f'OG Network gave no playback URL for channel {asset_id}')
        if not _has_segments(s, url):
            raise RuntimeError(f'OG Network channel {asset_id} is serving an empty playlist (no segments)')
        return url


def _description(row: dict) -> str | None:
    # Nigbati TV fills it with just 'Movie' or 'Show'.
    text = row.get('description') or row.get('episode_description')
    return None if not text or text.strip() in ('Movie', 'Show', 'TBD') else text


def _has_segments(s, url: str) -> bool:
    """False only when the first variant definitely lists no segments; any
    probe failure lets the URL through for the player to try."""
    try:
        r = s.session.get(url, headers=_HEADERS, timeout=15)
        if not r.ok:
            return True
        variant = next((ln.strip() for ln in r.text.splitlines() if ln.strip() and not ln.startswith('#')), None)
        if not variant:
            return True
        v = s.session.get(urljoin(r.url, variant), headers=_HEADERS, timeout=15)
        return not v.ok or '#EXTINF' in v.text
    except Exception:
        return True


def _auth_token(s, fresh: bool = False) -> str:
    """The web client's anonymous SDP token, shared across tunes."""
    global _token
    with _token_lock:
        if not fresh and _token and time.time() - _token[1] < _TOKEN_MAX_AGE:
            return _token[0]
        r = s.session.post(f'{_SDP}/authorize', json={}, headers=_HEADERS, timeout=20, params={
            'client_id': _CLIENT_ID, 'grant_type': 'client_credentials', 'device_id': uuid.uuid4().hex[:8]})
        r.raise_for_status()
        token = (r.json() or {}).get('data')
        if not token:
            raise RuntimeError('OG Network authorize returned no token')
        logger.debug('[free_networks] OG Network: new SDP token')
        _token = (token, time.time())
        return token

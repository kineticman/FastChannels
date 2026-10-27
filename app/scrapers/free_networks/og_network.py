"""OG Network's live channels (play.theognetwork.net/#/live): 84 small FAST
channels (CaribVision, Nigbati TV, ToonOvation, Colorized.TV, ZAZ Gamers, ...),
most carried by no other source.

The web app is a static site on Sotal Cloud's SDP. The channel list comes from
the site's own catalog.json, which it regenerates daily from SDP. Playback
follows the web app: an anonymous client_credentials token for its public
client (sent as x-auth-token), the channel asset's 'hls-live' resource, then a
playback session whose playback_url is clear HLS (up to 1080p) on noisypeak.
The session ticket in that URL lasts ~10 minutes, so it's created per tune.

No guide: the web app shows none and SDP exposes none to this client, so the
channels get tvtv (by Gracenote ID; only CaribVision's checks out) or
placeholder blocks.
"""
from __future__ import annotations

import threading
import time
import uuid

from ._common import Network, NetChannel, logger

_CATALOG = 'https://play.theognetwork.net/catalog.json'
_SDP = 'https://discovery-ams.sotalcloud.com/sdp/v2'
_CLIENT_ID = 'cloud:ognetwork_ottweb_device'
_HEADERS = {'Origin': 'https://play.theognetwork.net', 'Referer': 'https://play.theognetwork.net/'}
# Tokens carry no expiry; take a new one now and then, and on any 401.
_TOKEN_MAX_AGE = 6 * 3600
# Checked 2026-10-26 by matching the playback URL's ads.content_title against
# tvtv's now-playing. Other name matches (Dark Matter TV, Ghost Dimension,
# Viajar, Terra, The Family Channel, ...) air different schedules.
_GRACENOTE = {'471446': '49411'}  # CaribVision
# Tagged Hispanic/LATAM but in English.
_ENGLISH = {'The TRUMP Channel', 'New America Channel', 'GALLI Lucha Libre'}

_token_lock = threading.Lock()
_token: tuple[str, float] | None = None


class OGNetwork(Network):
    key = 'ognetwork'
    name = 'OG Network'

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
        return url


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

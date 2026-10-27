"""KSL (Salt Lake City): KSL On Air, Happening Now, Sports Zone, the high
school sports channels, NewsRadio, Weather, Roof Cam, BEONDTV and Utah TV.

Channels: the KSL app's public JW Player playlist "Live Channels (Dynamic)",
whose items carry the stream (fuelmedia / MediaTailor / Amagi HLS) and a
scheduleUrl. Guide: those bonneville.com/feeds/<name>.json schedules, about two
weeks of {title, startTime, endTime (UTC), chapterPointCustomProperties}.
They're behind Cloudflare: they need a Chrome TLS fingerprint and the app's
Referer/Origin, and even with them Cloudflare can serve a cached copy days
old (BEOND's ran out of guide), so each fetch adds a cache-busting query.
"""
from __future__ import annotations

import time

from ._common import Network, NetChannel, logger, parse_iso, program

_PLAYLIST = 'https://cdn.jwplayer.com/v2/playlists/RtIGzIhL'
_APP = 'https://app.ksltv.com'
# Items with no schedule feed (Specials and Utah TV have none; Sports Live 6's
# feed 404s). They get placeholder blocks.
_NO_GUIDE = {'3ahVLEws', 'MKg1R1CV', '9U3YgnFK'}
_NAMES = {'Happening Now': 'KSL Happening Now', 'BEONDTV': 'BEOND TV'}
# KSL On Air's poster is a studio photo; the others' posters are title cards.
_LOGOS = {'4vsaoghj': 'https://tmsimg.fancybits.co/assets/GNLZZGG002QZZCY.png?w=360&h=270'}  # Gracenote's KSL TV


def _category(title: str) -> str:
    t = title.lower()
    if 'sports' in t:
        return 'Sports'
    if title == 'Utah TV':
        return 'Travel'
    if title == 'BEONDTV':
        return 'Entertainment'
    return 'Local News'


class KSL(Network):
    key = 'ksl'
    name = 'KSL'

    def _items(self, s) -> list[dict]:
        def fetch():
            r = s.get(_PLAYLIST, params={'page_limit': 50})
            return ((r.json() or {}).get('playlist') or []) if r else []
        return s.memo('ksl_playlist', fetch)

    def channels(self, s):
        out = []
        for it in self._items(s):
            stream = next((src.get('file') for src in it.get('sources') or []
                           if '.m3u8' in (src.get('file') or '')), None)
            title = (it.get('title') or '').strip()
            if not it.get('mediaid') or not stream or not title:
                continue
            name = _NAMES.get(title) or (title[:-5] if title.endswith(' Live') and 'Sports Live' not in title
                                         else title)
            out.append(NetChannel(it['mediaid'], name, stream, logo=_LOGOS.get(it['mediaid']) or it.get('image'), category=_category(title)))
        return out

    def has_guide(self, channel_id):
        return channel_id not in _NO_GUIDE

    def programs(self, s, channels):
        wanted = {ch.id for ch in channels}
        out = []
        bust = {'_': int(time.time())}
        for it in self._items(s):
            cid, url = it.get('mediaid'), it.get('scheduleUrl')
            if cid not in wanted or cid in _NO_GUIDE or not url:
                continue
            try:
                r = s.impersonated().get(url, params=bust, headers={'Referer': f'{_APP}/', 'Origin': _APP}, timeout=20)
                rows = r.json() if r.status_code == 200 else None
            except Exception as exc:
                logger.warning('[free_networks] KSL schedule %s failed: %s', url, exc)
                continue
            if not isinstance(rows, list):
                logger.warning('[free_networks] KSL schedule %s: HTTP %s', url, r.status_code)
                continue
            for row in rows:
                props = {p.get('key'): p.get('value') for p in row.get('chapterPointCustomProperties') or []}
                p = program(cid, row.get('title'), parse_iso(row.get('startTime')), parse_iso(row.get('endTime')),
                            description=props.get('description') or None, poster_url=props.get('image') or None)
                if p:
                    out.append(p)
        return out

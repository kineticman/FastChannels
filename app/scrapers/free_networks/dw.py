"""DW (Deutsche Welle) English, Spanish and Arabic.

GET www.dw.com/graph-api/<lang>/livestream/<name> returns the channel's HLS URL
and its next ~12 hours of time slots (UTC), so the guide stays short and is
topped up each scrape. No German feed found at the matching de/ paths.
"""
from __future__ import annotations

from ._common import Network, NetChannel, parse_iso, program

_API = 'https://www.dw.com/graph-api/{path}'
_LOGO = 'https://www.dw.com/images/icons/favicon-180x180.png'
_FEEDS = [
    ('english', 'DW News', 'en/livestream/english', 'en', 'DE'),
    ('spanish', 'DW Español', 'es/livestream/spanish', 'es', 'DE'),
    ('arabic', 'DW Arabic', 'ar/livestream/arabic', 'ar', 'DE'),
]


class DW(Network):
    key = 'dw'
    name = 'DW'

    def _feed(self, s, path):
        def fetch():
            r = s.get(_API.format(path=path))
            chans = (r.json().get('data') or {}).get('livestreamChannels') if r else None
            return chans[0] if chans else None
        return s.memo(('dw', path), fetch)

    def channels(self, s):
        out = []
        for cid, name, path, lang, country in _FEEDS:
            feed = self._feed(s, path)
            if feed and feed.get('hlsVideoSrc'):
                out.append(NetChannel(cid, name, feed['hlsVideoSrc'], logo=_LOGO, category='News',
                                      language=lang, country=country))
        return out

    def programs(self, s, channels):
        paths = {f[0]: f[2] for f in _FEEDS}
        out = []
        for ch in channels:
            feed = self._feed(s, paths.get(ch.id, ''))
            for slot in (feed or {}).get('nextTimeSlots') or []:
                prog = slot.get('program') or {}
                element = (slot.get('programElement') or {}).get('name')
                p = program(ch.id, prog.get('name'), parse_iso(slot.get('startDate')),
                            parse_iso(slot.get('endDate')), poster_url=prog.get('posterImageUrl'),
                            episode_title=element if element and element != prog.get('name') else None)
                if p:
                    out.append(p)
        return out

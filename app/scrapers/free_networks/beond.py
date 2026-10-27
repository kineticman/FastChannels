"""BeOND TV (beond.tv/page/66a031f27e7d0a0024ac304c): Coastline Partners' FAST
lineup (Real Crime Uncovered, Travel Escapes, Beyond Paranormal, Health Hero
Network, Adrenaline TV+, Martha Stewart, USA TODAY, ...).

The site runs on Univtec's Insight platform: one public call,
insight-api-shared.univtec.com/interface/epg with the tenant header, lists
every channel with its HLS URL (Wurl / Amagi, clear, up to 1080p; the
ad-macro query string isn't needed) and ~24 h of schedule in
epgObject.events. Rows flagged noDisplay are the gaps before the schedule
starts.
"""
from __future__ import annotations

from ._common import Network, NetChannel, parse_iso, program

_EPG = 'https://insight-api-shared.univtec.com/interface/epg'
_HEADERS = {'x-tenant-id': 'beondtv', 'platform': 'web', 'Origin': 'https://www.beond.tv',
            'Accept': 'application/json'}
# Carried in Free Networks already from the network's own feed (BEONDTV via
# KSL, LatiNation = Cineverse's Latino Alternative TV stream).
_ELSEWHERE = {"80's sitcom flashback", 'comercio tv', 'tastemade', 'young hollywood', 'beondtv', 'latination'}
_SPANISH = {'Novelas De Siempre', 'TeleOnce America'}


class BeOND(Network):
    key = 'beond'
    name = 'BeOND TV'

    def _feed(self, s) -> list[dict]:
        def fetch():
            r = s.get(_EPG, headers=_HEADERS)
            data = r.json() if r else []
            return data if isinstance(data, list) else []
        return s.memo('beond_feed', fetch)

    def channels(self, s):
        out = []
        for ch in self._feed(s):
            cid, title, url = ch.get('id'), (ch.get('title') or '').strip(), ch.get('videoUrl')
            if not cid or not title or not url or title.lower() in _ELSEWHERE:
                continue
            # BEOND Travel's URL is a DAI request that needs its query.
            stream = url if 'doubleclick.net' in url else url.split('?', 1)[0]
            out.append(NetChannel(cid, title, stream, logo=ch.get('image') or ch.get('poster') or None,
                                  language='es' if title in _SPANISH else 'en'))
        return out

    def programs(self, s, channels):
        events = {ch.get('id'): (ch.get('epgObject') or {}).get('events') or [] for ch in self._feed(s)}
        out = []
        for ch in channels:
            for ev in events.get(ch.id) or []:
                if ev.get('noDisplay'):
                    continue
                p = program(ch.id, ev.get('title'), parse_iso(ev.get('start')), parse_iso(ev.get('end')),
                            description=ev.get('description'))
                if p:
                    out.append(p)
        return out

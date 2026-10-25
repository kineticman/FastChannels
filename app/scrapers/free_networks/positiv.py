"""PosiTV (TBN's family channel).

Stream: JW Player live broadcast FoWynVnP. Guide: the Trilogy app backend the
watch.positiv.tv player uses, ~2 days of entries with UTC start/end in
`extensions`.
"""
from __future__ import annotations

from ._common import Network, NetChannel, parse_iso, program

_MEDIA = 'FoWynVnP'
_STREAM = f'https://cdn.jwplayer.com/live/broadcast/{_MEDIA}.m3u8'
_EPG = 'https://positivdsp-prod.trilogyapps.com/v1/epg'
_LOGO = 'https://tmsimg.fancybits.co/assets/s34169_ll_h15_ad.png?w=360&h=270'  # Gracenote's


class PosiTV(Network):
    key = 'positiv'
    name = 'PosiTV'

    def channels(self, s):
        return [NetChannel('positiv', 'PosiTV', _STREAM, logo=_LOGO, category='Faith')]

    def programs(self, s, channels):
        if not channels:
            return []
        r = s.get(_EPG, params={'channel': 'POSITIV', 'mediaid': _MEDIA})
        out = []
        for row in ((r.json() or {}).get('entry') or []) if r else []:
            ext = row.get('extensions') or {}
            p = program('positiv', row.get('title'), parse_iso(ext.get('start_time')),
                        parse_iso(ext.get('end_time')), description=row.get('summary'),
                        poster_url=ext.get('imgLivePageDesktop16x9') or ext.get('image'))
            if p:
                out.append(p)
        return out

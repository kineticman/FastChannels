"""JTV (Jewelry Television): JTV, JTV2 and JTV Español.

Each jtv.com live page embeds a JW Player for one media id; the public
cdn.jwplayer.com/v2/media/<id> feed gives that media's live HLS URL (JW
live/broadcast for JTV and JTV2, MediaPackage for Español; clear, 1080p, no
login). resolve() asks the feed each tune so a re-pointed broadcast follows.
JTV Extra is a Bambuser live-shopping widget, not a channel.

Guide: tvtv's, by Gracenote ID (JTV and JTV Español are their own stations;
JTV2 has none).
"""
from __future__ import annotations

from ._common import Network, NetChannel

_MEDIA = 'https://cdn.jwplayer.com/v2/media/{}'
_TMS_LOGO = 'https://tmsimg.fancybits.co/assets/s{}_ll_h15_aa.png?w=360&h=270'
_CHANNELS = [
    NetChannel('jtv', 'JTV', 'id2Frx0C', logo=_TMS_LOGO.format('16604'), category='Shopping',
               gracenote_id='16604'),
    NetChannel('jtv2', 'JTV2', 'zuWADEhu', logo='https://images.jtv.com/media/jtv-site/page-images/jtv2/tab-logo-jtv2.png',
               category='Shopping'),
    NetChannel('espanol', 'JTV Español', 'fgFWCwZ7', logo=_TMS_LOGO.format('163702'), category='Shopping',
               language='es', gracenote_id='163702'),
]


class JTV(Network):
    key = 'jtv'
    name = 'JTV'

    def channels(self, s):
        return list(_CHANNELS)

    def resolve(self, s, ref: str) -> str:
        r = s.session.get(_MEDIA.format(ref), timeout=15)
        r.raise_for_status()
        playlist = r.json().get('playlist') or []
        sources = (playlist[0].get('sources') if playlist else None) or []
        url = next((src.get('file') for src in sources if '.m3u8' in (src.get('file') or '')), None)
        if not url:
            raise RuntimeError(f'JW Player gave no HLS source for JTV media {ref}')
        return url

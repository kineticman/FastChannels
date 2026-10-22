"""QVC, QVC2, QVC3 and the fourth QVC feed.

Streams: QVC Group's Akamai live (lsqvc1us..lsqvc4us), as loaded by
qvc.com/content/shop-live-tv.html. QVC's own program guide is behind a bot
wall (HTTP 418) and HSN's guide doesn't include QVC, so the guide comes from
the tvtv cache where a Gracenote ID is known.
"""
from __future__ import annotations

from ._common import Network, NetChannel

_STREAM = 'https://qvc-amd-live.akamaized.net/hls/live/2034113/lsqvc{}us/master.m3u8'
_LOGO = 'https://tmsimg.fancybits.co/assets/s{}_ll_h15_aa.png?w=360&h=270'  # Gracenote station logos
# id, name, stream n, gracenote, logo station
_CHANNELS = [
    ('qvc', 'QVC', 1, '60222', '60222'),
    ('qvc2', 'QVC2', 2, '177681', '82682'),
    ('qvc3', 'QVC3', 3, '101260', '101260'),
    ('qvc4', 'QVC 4', 4, None, '60222'),
]


class QVC(Network):
    key = 'qvc'
    name = 'QVC'

    def channels(self, s):
        return [NetChannel(cid, name, _STREAM.format(n), logo=_LOGO.format(logo), category='Shopping',
                           gracenote_id=gn)
                for cid, name, n, gn, logo in _CHANNELS]

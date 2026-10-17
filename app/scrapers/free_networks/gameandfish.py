"""Game & Fish TV (Outdoor Sportsman Group's free channel).

Stream: the Amagi playout outdoorchannel.com's watch page plays. Guide: the
XMLTV file that page loads (~9 days).
"""
from __future__ import annotations

from ._common import Network, NetChannel, xmltv_programs

_STREAM = ('https://amg00436-amg00436c7-osgviewlift-us-6738.playouts.now.amagi.tv/playlist/'
           'amg00436-ksemotvholdingsfast-gameandfishtv-osgviewliftus/playlist.m3u8')
_EPG = 'https://d3bd0tgyk368z1.cloudfront.net/feeds/epg/osgam_osgviewliftus/AMG00436C7.xml'
_LOGO = 'https://tmsimg.fancybits.co/assets/s168912_ll_h15_aa.png?w=360&h=270'


class GameAndFish(Network):
    key = 'gameandfish'
    name = 'Game & Fish TV'

    def channels(self, s):
        return [NetChannel('gftv', 'Game & Fish TV', _STREAM, logo=_LOGO, category='Outdoors')]

    def programs(self, s, channels):
        if not channels:
            return []
        r = s.get(_EPG)
        return xmltv_programs('gftv', r.content) if r else None

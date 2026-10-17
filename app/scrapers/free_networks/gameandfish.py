"""Game & Fish TV (Outdoor Sportsman Group's free channel).

Stream: the Amagi playout outdoorchannel.com's watch page plays. Guide: the
XMLTV file that page loads (~9 days), start/stop like
2026-10-16T10:00:00.000+0000.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime

from ._common import Network, NetChannel, program

_STREAM = ('https://amg00436-amg00436c7-osgviewlift-us-6738.playouts.now.amagi.tv/playlist/'
           'amg00436-ksemotvholdingsfast-gameandfishtv-osgviewliftus/playlist.m3u8')
_EPG = 'https://d3bd0tgyk368z1.cloudfront.net/feeds/epg/osgam_osgviewliftus/AMG00436C7.xml'
_LOGO = 'https://tmsimg.fancybits.co/assets/s168912_ll_h15_aa.png?w=360&h=270'


def _time(val):
    try:
        return datetime.strptime(val or '', '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError:
        return None


class GameAndFish(Network):
    key = 'gameandfish'
    name = 'Game & Fish TV'

    def channels(self, s):
        return [NetChannel('gftv', 'Game & Fish TV', _STREAM, logo=_LOGO, category='Outdoors')]

    def programs(self, s, channels):
        if not channels:
            return []
        r = s.get(_EPG)
        if not r:
            return None
        out = []
        for el in ET.fromstring(r.content).iter('programme'):
            sub = el.findtext('sub-title')
            icons = [i.get('src') for i in el.findall('icon') if i.get('src')]
            season = episode = None
            ns = next((e.text for e in el.findall('episode-num') if e.get('system') == 'xmltv_ns'), None)
            if ns and ns.count('.') == 2:
                try:
                    season, episode = int(ns.split('.')[0]) + 1, int(ns.split('.')[1]) + 1
                except ValueError:
                    pass
            p = program('gftv', el.findtext('title'), _time(el.get('start')), _time(el.get('stop')),
                        episode_title=sub, description=el.findtext('desc'), category=el.findtext('category'),
                        poster_url=icons[0] if icons else None, season=season, episode=episode)
            if p:
                out.append(p)
        return out

"""Free Speech TV.

Stream: the Zype linear channel the freespeech.org player loads. Guide:
freespeech.org/wp-json/zype/v1/epg, a Zype "XMLTV" export (~2 weeks) with
epoch-millisecond start/stop attributes and <description>/<subtitle> tags in
place of the standard <desc>/<sub-title>.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET

from ._common import Network, NetChannel, from_epoch, program

_STREAM = ('https://na.linear.zype.com/f2f02a72-71d0-45f3-829f-4ae51f721102/'
           '86ef0177-697d-41c4-be57-c4d1a2b83cad-hls4/live.m3u8')
_EPG = 'https://freespeech.org/wp-json/zype/v1/epg'
_LOGO = 'https://freespeech.org/wp-content/uploads/2026/05/FSTV-logo_OG-social-image_1200%C3%97628px.png'


class FreeSpeechTV(Network):
    key = 'fstv'
    name = 'Free Speech TV'

    def channels(self, s):
        return [NetChannel('fstv', 'Free Speech TV', _STREAM, logo=_LOGO, category='News')]

    def programs(self, s, channels):
        if not channels:
            return []
        r = s.get(_EPG)
        if not r:
            return []
        root = ET.fromstring(r.content)
        out = []
        for el in root.iter('programme'):
            def text(*tags):
                for tag in tags:
                    node = el.find(tag)
                    if node is not None and (node.text or '').strip():
                        return node.text
                return None
            title = text('title')
            sub = text('sub-title', 'subtitle')
            icon = el.find('icon')
            poster = icon.get('src') if icon is not None else None
            p = program('fstv', title, from_epoch(el.get('start')), from_epoch(el.get('stop')),
                        episode_title=sub if sub and sub != title else None,
                        description=text('desc', 'description'),
                        poster_url=None if poster and 'guide-default' in poster else poster)
            if p:
                out.append(p)
        return out

"""Outside TV and its four JW Player channels (Climbing, Mixtape, Pinkbike TV,
Warren Miller TV).

Outside TV itself is an Amagi stream whose guide comes from tvtv (Gracenote
114313). The other four are JW Player SSAI streams; each JW media item's
scheduleUrl is an XMLTV file on epg.fast.jwp.services, about a day ahead.
"""
from __future__ import annotations

from ._common import Network, NetChannel, logger, xmltv_programs

_JW = 'https://cdn.jwplayer.com/v2/sites/f902XiZc/media/{}/ssai.m3u8?ad_config_id={}'
_EPG = 'https://epg.fast.jwp.services/0_Cz2comTD_{}/epg.xml'
_LOGO = 'https://wp.watch.outsideonline.com/wp-content/uploads/2024/04/Outside-TV-Web-Snippet-v3.png'
# id, name, JW media id, ad config
_JW_CHANNELS = [
    ('climbing', 'Climbing', 'Z61LZzEf', 'wMGfd1m7'),
    ('mixtape', 'Outside Mixtape', '8lqjEAz5', '6MkBJaRl'),
    ('pinkbike', 'Pinkbike TV', 'kfFzR2PG', 'wMGfd1m7'),
    ('warrenmiller', 'Warren Miller TV', 'gxAjbUJX', 'wMGfd1m7'),
]
_MEDIA = {cid: media for cid, _, media, _ in _JW_CHANNELS}


class OutsideTV(Network):
    key = 'outside'
    name = 'Outside TV'

    def channels(self, s):
        return [NetChannel('outsidetv', 'Outside TV',
                           'https://cdn-ue1-prod.tsv2.amagi.tv/linear/outsidetv-outsidetvplus-oando/playlist.m3u8',
                           logo=_LOGO, category='Outdoors', gracenote_id='114313')] + [
            NetChannel(cid, name, _JW.format(media, ad), logo=_LOGO, category='Outdoors')
            for cid, name, media, ad in _JW_CHANNELS]

    def has_guide(self, channel_id):
        return channel_id in _MEDIA

    def programs(self, s, channels):
        out = []
        for ch in channels:
            media = _MEDIA.get(ch.id)
            if not media:
                continue
            r = s.get(_EPG.format(media))
            if not r:
                logger.warning('[free_networks] Outside TV %s: no EPG', ch.id)
                continue
            out.extend(xmltv_programs(ch.id, r.content))
        return out

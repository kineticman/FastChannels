"""Networks that are just fixed stream URLs with no guide of their own.

Their guide, when they have one, comes from the tvtv cache via a Gracenote ID.
IDs here were checked against tvtv's program titles on 2026-10-17; others can
be added through the community CSV (provider 'free_networks').
"""
from __future__ import annotations

from ._common import Network, NetChannel


class StaticNetwork(Network):
    def __init__(self, key, name, channels):
        self.key, self.name, self._channels = key, name, channels

    def channels(self, s):
        return list(self._channels)


_JW = 'https://cdn.jwplayer.com/v2/sites/f902XiZc/media/{}/ssai.m3u8?ad_config_id={}'
_TMS_LOGO = 'https://tmsimg.fancybits.co/assets/s{}_ll_h15_aa.png?w=360&h=270'  # Gracenote station logos
_CMC_LOGO = 'https://cmctv.com/cmc/images/CMC_Logo.png'
_OUTSIDE_LOGO = 'https://wp.watch.outsideonline.com/wp-content/uploads/2024/04/Outside-TV-Web-Snippet-v3.png'

NETWORKS = [
    StaticNetwork('sky', 'Sky News', [
        NetChannel('news', 'Sky News',
                   'https://linear901-oo-hls0-prd-gtm.delivery.skycdp.com/17501/sde-fast-skynews/master.m3u8',
                   logo=_TMS_LOGO.format('101290'), category='News', country='GB'),
        NetChannel('arabia', 'Sky News Arabia',
                   'https://live-stream.skynewsarabia.com/c-horizontal-channel/horizontal-stream/index.m3u8',
                   logo='https://www.skynewsarabia.com/icon-192x192.png', category='News', language='ar', country='AE',
                   gracenote_id='99085'),
    ]),
    StaticNetwork('cheddar', 'Cheddar', [
        NetChannel('news', 'Cheddar News', 'https://hls.livecdn.io/cheddar.com/cheddar/playlist.m3u8',
                   logo=_TMS_LOGO.format('107241'),
                   category='News', gracenote_id='107241'),
    ]),
    StaticNetwork('buzzr', 'BUZZR', [
        NetChannel('buzzr', 'BUZZR', 'https://buzzrota-ono.amagi.tv/playlist720p.m3u8',
                   logo=_TMS_LOGO.format('93430'), category='Game Shows', gracenote_id='113452'),
    ]),
    StaticNetwork('accuweather', 'AccuWeather', [
        NetChannel('now', 'AccuWeather NOW',
                   'https://amg00684-accuweatherinc-accuweather-ono-8kh8p.amagi.tv/playlist/'
                   'amg00684-accuweatherinc-accuweather-ono/playlist.m3u8',
                   logo=_TMS_LOGO.format('91994'), category='News', gracenote_id='91994'),
    ]),
    StaticNetwork('courttv', 'Court TV', [
        # Yospace master; each request opens its own ad session.
        NetChannel('courttv', 'Court TV',
                   'https://csm-e-ceshared1lusordlive-103306026.bln1.yospace.com/csm/extlive/lawnewzprd01,court-tv.m3u8',
                   logo=_TMS_LOGO.format('111043'), category='True Crime', gracenote_id='123605'),
    ]),
    StaticNetwork('cmc', 'CMC', [
        NetChannel('cmc', 'CMC California Music Channel',
                   'https://cmc-ono.amagi.tv/hls/amagi_hls_data_cmcAAAAAA-cmc-ono/CDN/playlist.m3u8', logo=_CMC_LOGO,
                   category='Music', gracenote_id='91404'),
        NetChannel('usa', 'CMC-USA Country Music Channel',
                   'https://hwlive.streamingmediahosting.com/14215-live/0_obd393sh/chunklist.m3u8', logo=_CMC_LOGO,
                   category='Music', gracenote_id='106796'),
    ]),
    StaticNetwork('outside', 'Outside TV', [
        NetChannel('outsidetv', 'Outside TV',
                   'https://cdn-ue1-prod.tsv2.amagi.tv/linear/outsidetv-outsidetvplus-oando/playlist.m3u8',
                   logo=_OUTSIDE_LOGO, category='Outdoors', gracenote_id='114313'),
        NetChannel('climbing', 'Climbing', _JW.format('Z61LZzEf', 'wMGfd1m7'), logo=_OUTSIDE_LOGO, category='Outdoors'),
        NetChannel('mixtape', 'Outside Mixtape', _JW.format('8lqjEAz5', '6MkBJaRl'), logo=_OUTSIDE_LOGO,
                   category='Outdoors'),
        NetChannel('pinkbike', 'Pinkbike TV', _JW.format('kfFzR2PG', 'wMGfd1m7'), logo=_OUTSIDE_LOGO,
                   category='Outdoors'),
        NetChannel('warrenmiller', 'Warren Miller TV', _JW.format('gxAjbUJX', 'wMGfd1m7'), logo=_OUTSIDE_LOGO,
                   category='Outdoors'),
    ]),
]

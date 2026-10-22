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


_DAI = 'https://dai.google.com/linear/hls/event/{}/master.m3u8'
_GLOBAL_LOGO = 'https://assets.news.corusappservices.com/upload/news/{}/thumbnail-450x300-GlobalLiveStreamThumbnails_{}.png'
# Global News 24/7 streams: globalnews.ca/live/<city>/ -> its video-entry's
# daiSetting.assetKey. Gracenote IDs are Gracenote's own "Global News <city>"
# streaming stations; the other six cities have none.
# city id, name, DAI asset key, logo path, logo name, gracenote
_GLOBAL_NEWS = [
    ('national', 'Global News National', 'cQOVg6jKSvusQAfYpTrUoQ', '95g2xvkn8m-xyl67b6954', 'National', '114494'),
    ('bc', 'Global News BC', 'eIIBH8jkR-SWGaD5E4MesA', 'z6u7o3dlys-7gqb07ct2x', 'bc', '114495'),
    ('toronto', 'Global News Toronto', 'BblZeSsBRSSCtvkKRZ8XJQ', 'd5nl1u9qep-8sduvnp8na', 'Toronto', '114492'),
    ('edmonton', 'Global News Edmonton', 'K8ZZN35nQA2N_hskrUB2oQ', '94jexai5uv-rkcaq5vdmj', 'Edmonton', '114496'),
    ('calgary', 'Global News Calgary', 'AYQ1ssB7RcOX9D78FtdoQQ', '00qbku0v3r-4e0bqqux6n', 'Calgary', '114493'),
    ('halifax', 'Global News Halifax', 'ljv6O6ZiSiKudWk7qwWbEw', 'tfa0och9w4-fm64e81svp', 'Halifax', '146047'),
    ('winnipeg', 'Global News Winnipeg', 'R5XZJcTAQPuyvhsVWIKRhw', 'byv3d2lbz2-9z4ww0myek', 'Winnipeg', '146046'),
    ('montreal', 'Global News Montreal', 'wI13hMWhRSKbyZ8Q1DQR3Q', 'yj4j7xi9bs-3ejgrgzown', 'Montreal', '146045'),
    ('okanagan', 'Global News Okanagan', 'PLPlPfeDQCCe4GaxAWpi4g', 'pbb0n53f0s-fgi2153mdd', 'Okanagan', None),
    ('saskatoon', 'Global News Saskatoon', 'cAeO9FUXSRatnrvaksoJ9A', 'jti7jyu2h9-xitlqhwx2d', 'Saskatoon', None),
    ('regina', 'Global News Regina', 'A69Xh6OaTfOrRf2oHVLaPQ', 'rzlinuvfxj-5ouxs6lq01', 'Regina', None),
    ('kingston', 'Global News Kingston', 'poxVVQNQSMqFBnT6qR-Lwg', 'vu3i9sm33r-n6nfvjzzlf', 'Kingston', None),
    ('peterborough', 'Global News Peterborough', 'CQ82MHxxS7qwe7Zvs4_sUA', 'bap0x876hf-ilbx4p7kkd', 'peterborough', None),
    ('lethbridge', 'Global News Lethbridge', 'hYQh95EXRwaF0IOhUrJoEQ', '9ew2c3c60k-rak3q85q2q', 'Lethbridge', None),
]
_TMS_LOGO = 'https://tmsimg.fancybits.co/assets/s{}_ll_h15_aa.png?w=360&h=270'  # Gracenote station logos
_CMC_LOGO = 'https://cmctv.com/cmc/images/CMC_Logo.png'

NETWORKS = [
    StaticNetwork('sky', 'Sky News', [
        NetChannel('news', 'Sky News',
                   'https://linear901-oo-hls0-prd-gtm.delivery.skycdp.com/17501/sde-fast-skynews/master.m3u8',
                   logo=_TMS_LOGO.format('101290'), category='News', country='GB', gracenote_id='122885'),
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
    StaticNetwork('globalnews', 'Global News (Canada)', [
        NetChannel(cid, name, _DAI.format(dai), logo=_GLOBAL_LOGO.format(path, logo),
                   category='News' if cid == 'national' else 'Local News', country='CA', gracenote_id=gn)
        for cid, name, dai, path, logo, gn in _GLOBAL_NEWS
    ]),
    StaticNetwork('lx', 'NBC LX', [
        NetChannel('lx', 'NBC LX Home', 'https://nbculocallive.akamaized.net/hls/live/2037096/lx/use1.m3u8',
                   logo=_TMS_LOGO.format('126864'), category='Lifestyle', gracenote_id='126864'),
    ]),
    StaticNetwork('nrb', 'NRB TV', [
        NetChannel('nrbtv', 'NRB TV', 'https://tgn2.bozztv.com/nrbtv/nrbtv-live/index.m3u8',
                   logo='https://nrbtv.org/wp-content/uploads/2020/09/nrbtv-social1.png', category='Faith', gracenote_id='49097'),
    ]),
    StaticNetwork('thedove', 'theDove', [
        # theDove is KDOV-LD Medford, Oregon (its TitanTV widget); checked against
        # the stream on 2026-10-17 (both airing Stand in the Gap).
        NetChannel('thedove', 'theDove TV', 'https://thedovetv.bozztv.com/thedovetv/thedovetv/index.m3u8',
                   logo='https://thedove.us/wp-content/uploads/2026/04/doveLive.png', category='Faith',
                   gracenote_id='65731'),
    ]),
    StaticNetwork('eplustv6', 'EPlusTV 6', [
        # Bundled PNG rendered from eplustv6.com/assets/svgs/tv6-logo.svg: the
        # logo cache can't decode SVG and Channels DVR apps can't display it.
        NetChannel('eplustv6', 'EPlusTV 6 Jackson',
                   'https://livestream.telvue.com/jacksonenergy1/f7b44cfafd5c52223d5498196c8a2e7b.sdp/playlist.m3u8',
                   logo='/static/logos/eplustv6/eplustv6.png', category='Local News', gracenote_id='44861'),
    ]),
]

"""Networks that are just fixed stream URLs with no guide of their own.

Their guide, when they have one, comes from the tvtv cache via a Gracenote ID.
IDs here were checked against tvtv's program titles on 2026-10-17; others can
be added through the community CSV (provider 'free_networks').
"""
from __future__ import annotations

from ._common import Network, NetChannel


class StaticNetwork(Network):
    def __init__(self, key, name, channels, browser_relay_hosts=()):
        self.key, self.name, self._channels = key, name, channels
        self.browser_relay_hosts = tuple(browser_relay_hosts)

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
# 3ABN's 24/7 networks: 3abnplus.tv's Live TV programs (Uscreen) play these
# fixed Tulix/Nimble URLs (clear 720p, no login). Gracenote IDs only for the
# national networks whose station name matches exactly; the main 3ABN
# station showed "To Be Announced" on tvtv, so it keeps placeholders.
# Logos are the programs' 3abnplus.tv cards. Checked 2026-11-17.
_3ABN = 'https://3abn.tulixcdn.com/{}.m3u8'
_3ABN_LOGO = 'https://alpha.uscreencdn.com/1110xnull/images/programs/{}?auto=webp'
# id, name, stream path, logo path, language, country, gracenote
_3ABN_CHANNELS = [
    ('3abn', '3ABN', '3abn2/3abn_live/smil:3abn_live.smil/playlist',
     '526675/horizontal/big_3ABN--1480x840.1659535621.jpg', 'en', 'US', None),
    ('proclaim', '3ABN Proclaim!', '3abn2/Pro_live/smil:Pro_live.smil/playlist',
     '527372/horizontal/big_Proclaim-1480x840.1659535750.jpg', 'en', 'US', '68985'),
    ('d2d', '3ABN Dare to Dream', '3abn2/d2d_live/smil:d2d_live.smil/playlist',
     '530203/horizontal/big_D2D-1480x840.1659535818.jpg', 'en', 'US', '78540'),
    ('international', '3ABN International', '3abn2/Int_live/smil:Int_live.smil/playlist',
     '530218/horizontal/big_International1480x840.1659537368.jpg', 'en', 'US', None),
    ('canada', '3ABN Canada', '3abncanada/3ABN/master',
     '3084114/horizontal/big_1569f16b-e4a7-4797-b745-f9d50b5ffe13.jpg', 'en', 'CA', None),
    ('praisehim', '3ABN Praise Him Music', '3abn1/PraiseHim/smil:PraiseHim.smil/playlist',
     '531102/horizontal/big_Praise-Him1480x840.1659537466.jpg', 'en', 'US', None),
    ('kids', '3ABN Kids', '3abn2/Kids_live/smil:Kids_live.smil/playlist',
     '532608/horizontal/big_Kids1480x840.1659537556.jpg', 'en', 'US', '99867'),
    ('latino', '3ABN Latino', '3abn2/Lat_live/smil:Lat_live.smil/playlist',
     '534693/horizontal/big_Latino1480x840.1659537624.jpg', 'es', 'US', '49987'),
    ('russia', '3ABN Russia', '3abn2/Rus_live/smil:Rus_live.smil/playlist',
     '739766/horizontal/big_Russia-1480x840.1659538945.jpg', 'ru', 'RU', None),
    ('francais', '3ABN Français', '3abn2/Fre_live/smil:Fre_live.smil/playlist',
     '739696/horizontal/big_French-1480x840.1659539108.jpg', 'fr', 'US', None),
]

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
    ],
        # Amagi's beacon segments redirect to S3/CloudFront .ts files sent with
        # no CORS header, which the admin player can't load from our page.
        browser_relay_hosts=('cmc-ono.amagi.tv',)),
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
    StaticNetwork('bowlingtv', 'Bowling TV', [
        # PBA's 24/7 channel: watchbowling.tv/live frames tripleb.tv/bowling-tv,
        # whose hls.js player loads this fixed Ottera URL (clear, 1080p, no
        # token). Gracenote is the FAST station Sling carries; tvtv lists it.
        # Checked 2026-11-10.
        NetChannel('bowlingtv', 'Bowling TV', 'https://bbb-bowl-streamstak.otteravision.com/bbb/bowl/bowl.m3u8',
                   logo='https://static1.squarespace.com/static/6a0f1667e14e7d311009eebc/t/6a1463af54041973688f1081/'
                        '1779721135229/Bowling+TV_16by9.png?format=500w',
                   category='Sports', gracenote_id='200290',
                   description="The PBA's free 24/7 bowling channel."),
    ]),
    StaticNetwork('aew', 'Watch AEW', [
        # All Elite Wrestling's free 24/7 channel. myaew.com plays it through
        # Kiswe (JWT sessions); the channel itself is this Amagi playout,
        # clear 1080p with no token (LG Channels lists it with ad macros, which
        # it doesn't need). Gracenote is the FAST station Tubi carries.
        # Checked 2026-11-10.
        NetChannel('watchaew', 'Watch AEW',
                   'https://amg16221-amg16221c1-amgplt0023.playout.now3.amagi.tv/ts-us-e2-n1/playlist/'
                   'amg16221-amg16221c1-amgplt0023/playlist.m3u8',
                   logo='https://preview-amagicloud-aew-automation-aew.cloudport.amagi.tv/Images/S3/WATCH-AEW-400x200.png',
                   category='Sports', gracenote_id='206205',
                   description="All Elite Wrestling's free 24/7 channel: matches, shows and replays."),
    ]),
    StaticNetwork('sportsgrid', 'SportsGrid', [
        # sportsgrid.com/live's Amagi playout, without the page's ad macros
        # (clear 720p). Gracenote is the FAST station Sling and Fubo carry;
        # tvtv's now-playing matched the stream's title on 2026-11-17.
        NetChannel('sportsgrid', 'SportsGrid',
                   'https://amg00315-sportsgridfast-amg00315c1-sportsgrid-us-1271.playouts.now.amagi.tv/playlist/'
                   'amg00315-sportsgridfast-sportsgrid-sportsgridus/playlist.m3u8',
                   logo=_TMS_LOGO.format('112732'), category='Sports', gracenote_id='112732',
                   description='Sports betting and fantasy news, analysis and live shows.'),
    ]),
    StaticNetwork('speedsport1', 'SPEED SPORT 1', [
        # speedsport1.com/live-broadcast's HLS Player plugin config (base64 in
        # the page; the site answers non-browsers 429/403, the stream doesn't):
        # Brightcove Live, clear 1080p. Gracenote is the FAST station Fubo
        # carries. Checked 2026-11-17.
        NetChannel('speedsport1', 'SPEED SPORT 1',
                   'https://speedsport1.global.transmit.live/hls/695306edec6a381683f12f29/dist/ss1/599/hls/'
                   'master/playlist.m3u8',
                   logo=_TMS_LOGO.format('138647'), category='Sports', gracenote_id='138647',
                   description='Motorsports: dirt track, sprint cars, short track and more.'),
    ]),
    StaticNetwork('shoplc', 'Shop LC', [
        # shoplc.com/pages/live-tv's Shaka player loads this Akamai HLS (and a
        # DASH twin), clear 1080p. Checked 2026-11-17.
        NetChannel('shoplc', 'Shop LC',
                   'https://cdn-shop-lc-01.akamaized.net/Content/HLS_HLS/Live/channel(ott)/index.m3u8',
                   logo=_TMS_LOGO.format('93564'), category='Shopping', gracenote_id='93564'),
    ]),
    StaticNetwork('3abn', '3ABN', [
        NetChannel(cid, name, _3ABN.format(path), logo=_3ABN_LOGO.format(logo), category='Faith',
                   language=lang, country=country, gracenote_id=gn)
        for cid, name, path, logo, lang, country, gn in _3ABN_CHANNELS
    ]),
    StaticNetwork('eplustv6', 'EPlusTV 6', [
        # Bundled PNG rendered from eplustv6.com/assets/svgs/tv6-logo.svg: the
        # logo cache can't decode SVG and Channels DVR apps can't display it.
        NetChannel('eplustv6', 'EPlusTV 6 Jackson',
                   'https://livestream.telvue.com/jacksonenergy1/f7b44cfafd5c52223d5498196c8a2e7b.sdp/playlist.m3u8',
                   logo='/static/logos/eplustv6/eplustv6.png', category='Local News', gracenote_id='44861'),
    ]),
]

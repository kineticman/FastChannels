"""CBS News 24/7, the CBS News local streams, and CBS's other free channels
(Sports HQ, Inside Edition, ET, 48 Hours, 60 Minutes, and from cbs.com's live
TV: Judge Judy, Hot Bench, Car Chase).

Channel list: www.cbsnews.com/video/xhr/collection/component/live-channels/
(JSON; needs a browser TLS fingerprint, plain requests get 406). Each entry's
first item has a stable Google DAI master URL. CBS publishes no forward guide,
so the guide comes from the tvtv cache via Gracenote IDs — the ones below were
matched to tvtv by their newscast titles (WJZ = Baltimore, KDKA = Pittsburgh,
...) on 2026-10-17. The static list is the fallback when the endpoint fails.
"""
from __future__ import annotations

from ._common import Network, NetChannel, logger

_LIST = 'https://www.cbsnews.com/video/xhr/collection/component/live-channels/'
_DAI = 'https://dai.google.com/linear/hls/event/{}/master.m3u8'
_LOGO = ('https://assets2.cbsnewsstatic.com/hub/i/r/2024/04/16/0fb75ad2-a909-44bb-87dc-86b9d51cbeb2/'
         'thumbnail/640x360/1ccc32a8059d6432a82e11e3aca70c23/247-key-channelthumbnail-1920x1080.jpg')

# slug -> (name, DAI event, gracenote, category)
_KNOWN = {
    'cbsnews':        ('CBS News 24/7', 'Sid4xiTQTkCT1SLu6rjUSQ', '104846', 'News'),
    'baltimore':      ('CBS News Baltimore', 'TMUt418sQMeu1QtneYJ9ag', '123127', 'Local News'),
    'sanfrancisco':   ('CBS News Bay Area', 'laLJwH1nS2KQAUQTF8z1Cg', '123121', 'Local News'),
    'boston':         ('CBS News Boston', 'dHA193vxQ_WqZNVoC-W5MA', '123130', 'Local News'),
    'chicago':        ('CBS News Chicago', '_xQBtL7ZSEClkNebaXzn8w', '123122', 'Local News'),
    'colorado':       ('CBS News Colorado', 'E9CkyheTSKeGiOiICU_JBQ', '123120', 'Local News'),
    'detroit':        ('CBS News Detroit', 'xnyAMOsfSV2J1b-A3nROFQ', '126738', 'Local News'),
    'losangeles':     ('CBS News Los Angeles', 'K_2aA7OVRTacJJli1oSi8w', '123126', 'Local News'),
    'miami':          ('CBS News Miami', '_yODrBHESGSzPe3dqW4gGg', '123380', 'Local News'),
    'minnesota':      ('CBS News Minnesota', 'c0XucO32RNKG8oeQ351a4Q', '123119', 'Local News'),
    'newyork':        ('CBS News New York', 'jeTDC610T-OSPdiWhcl0Pg', '123125', 'Local News'),
    'philadelphia':   ('CBS News Philadelphia', 'R2Kw0uvnQIW7fUmOseurzA', '123128', 'Local News'),
    'pittsburgh':     ('CBS News Pittsburgh', 'mir-uZYjTu6-8JLMHBTKhQ', '123129', 'Local News'),
    'sacramento':     ('CBS News Sacramento', 'e7mp4YVGS7O_e7rqRNjKow', '123124', 'Local News'),
    'texas':          ('CBS News Texas', 'ogzLhRCkRbG3lFHHJwqaNw', '123123', 'Local News'),
    'cbs-sports-hq':  ('CBS Sports HQ', '9Lq0ERvoSR-z9AwvFS-xYA', '108919', 'Sports'),
    'inside-edition': ('Inside Edition 24/7', 'Y2DMgfclT3S2nNxANeIawA', '130385', 'Entertainment'),
    'mixible':        ('ET Live', 'xrVrJYTmTfitfXBQfeZByQ', '110291', 'Entertainment'),
    '48-hours':       ('48 Hours', 'JUr94WL2QAiVpGNHY5n5dA', '160400', 'True Crime'),
    '60-minutes':     ('60 Minutes', 'nrb-l85CR7SIr-rGXqz4zA', '161735', 'News'),
    # On cbs.com/live-tv but not cbsnews.com's list (DAI events from the
    # cbs.com channel pages, 2026-10-27). Gracenote IDs are Pluto's; tvtv's
    # slots matched cbs.com's on-now times the same day.
    'judge-judy':     ('Judge Judy', 'bRE69gQJQm2_cW4qnf6okg', '201079', 'Reality TV'),
    'hot-bench':      ('Hot Bench', 'ZU0VFVOpRB29p8Lhj_z1CA', '202596', 'Reality TV'),
    'car-chase':      ('Car Chase', 'RjvF1cd0Qx-65Dz14THpPA', '148202', 'Reality TV'),
}
_CBS_LOGO = 'https://wwwimage-tve.cbsstatic.com/thumbnails/photos/w370-q80/channel/{}.png'
_LOGOS = {
    'judge-judy': _CBS_LOGO.format('78/49/95/3af01ed5-da60-477e-a5fe-c40193cfbf3c'),
    'hot-bench':  _CBS_LOGO.format('59/93/30/890c4d26-c818-4b84-9233-004c9b653b4b'),
    'car-chase':  _CBS_LOGO.format('88/01/96/ce80dc2e-ebf8-489e-a730-6ad58284f62e'),
}


class CBSNews(Network):
    key = 'cbsnews'
    name = 'CBS News'

    def channels(self, s):
        live = {}
        try:
            r = s.impersonated().get(_LIST, timeout=20)
            r.raise_for_status()
            for item in r.json().get('items') or []:
                sub = (item.get('items') or [{}])[0]
                if item.get('slug') and sub.get('video'):
                    live[item['slug']] = (sub.get('title') or item['slug'], sub['video'])
        except Exception as exc:
            logger.warning('[free_networks] CBS News channel list failed (%s), using built-in list', exc)
        out = []
        for slug, (name, event, gracenote, category) in _KNOWN.items():
            url = live.get(slug, (None, _DAI.format(event)))[1]
            out.append(NetChannel(slug, name, url, logo=_LOGOS.get(slug, _LOGO), category=category,
                                  gracenote_id=gracenote))
        for slug, (title, url) in live.items():
            if slug not in _KNOWN:
                logger.info('[free_networks] CBS News: new channel %r (%s) not in the built-in list', title, slug)
                out.append(NetChannel(slug, title, url, logo=_LOGO, category='News'))
        return out

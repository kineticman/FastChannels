"""NewsNation's live stream (newsnationnow.com/news-nation-live/).

The site's Lura (Anvato) player plays it with no TV-provider sign-in. The web
page counts down a 5-10 minute preview before asking for one, but that limit is
the player's own: the stream kept playing for 30+ minutes here (2026-10-19). Its
config call, tkx.mp.lura.live/rest/v2/mcp/video/<id>?anvack=<the page's access
key>, answers JSONP whose published_urls[0].embed_url is a session-tokened
master playlist: clear HLS (720p, CC1, no EXT-X-KEY). So the video ID is
stored and resolved to a fresh URL at play time, like CBN.

The access key and video ID come from the live page as loaded in a browser on
2026-10-19 (its providerSettings.anvato.anvack, a Nexstar WordPress video
plugin setting). newsnationnow.com and the Nexstar station sites are all behind
PerimeterX, so plain requests can't re-read it. Each scrape checks the key, and
a rotated one ("Invalid Anvack", code 101) is logged as such: read the new one
off that page in a real browser (Camoufox gets through).

Guide: Gracenote 91096 (the NewsNation HD that Spectrum and DirecTV carry), via
the tvtv cache.
"""
from __future__ import annotations

import json
import re

from ._common import Network, NetChannel, logger

_CONFIG = 'https://tkx.mp.lura.live/rest/v2/mcp/video/{}'
_ACCESS_KEY = 'GNLYj4WPEPgKWUvG6mTVNFejokMyVq3x'
_VIDEO_ID = 'adstP5LOdMYydk9w'
_LOGO = 'https://tmsimg.fancybits.co/assets/s91096_ll_h15_ad.png?w=360&h=270'
_JSONP_RE = re.compile(r'^[^(]*\((.*)\)\s*;?\s*$', re.S)


class NewsNation(Network):
    key = 'newsnation'
    name = 'NewsNation'

    def channels(self, s):
        # The scrape's check that the access key still works (resolve() raising
        # here would only be logged, and the channel is listed regardless).
        try:
            self.resolve(s, _VIDEO_ID)
        except Exception as exc:
            logger.warning('[free_networks] %s', exc)
        return [NetChannel('live', 'NewsNation', _VIDEO_ID, logo=_LOGO, category='News', gracenote_id='91096')]

    def resolve(self, s, ref):
        r = s.session.get(_CONFIG.format(ref), params={'anvack': _ACCESS_KEY}, timeout=15)
        m = _JSONP_RE.search(r.text)
        try:
            data = json.loads(m.group(1) if m else r.text)
        except ValueError:
            data = {}
        if not isinstance(data, dict):
            data = {}
        exception = data.get('exception')
        if not isinstance(exception, dict):
            exception = {}
        if str(exception.get('code')) == '101':
            raise RuntimeError('NewsNation: Lura no longer accepts the access key (Invalid Anvack) — NewsNation '
                               'has rotated it; the new one is on newsnationnow.com/news-nation-live/')
        url = next((p.get('embed_url') for p in data.get('published_urls') or []
                    if '.m3u8' in (p.get('embed_url') or '')), None)
        if not url:
            desc = exception.get('desc') or ''
            raise RuntimeError(f'NewsNation: Lura returned no stream for {ref} (HTTP {r.status_code}) {desc}'.strip())
        return url

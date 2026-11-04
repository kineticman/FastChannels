"""i24NEWS English, French, Hebrew and Arabic (video.i24news.tv).

The site runs on Univtec's Insight platform, like BeOND, but under tenant
i24israel each language is its own "region" (all = English, french, hebrew,
arabic) with its own pages, and the shared /interface/epg call returns [] for
it. Each region's Live page (/interface/pages/<id> with the regioncode
header) lists its one channel with a clear HLS URL
(i24news<lang>-cdn.encoders.immergo.tv/master.m3u8, up to 1080p, no login)
and ~36 h of schedule in epgObject.events (checked 2026-11-02).

Page ids are hard-coded because finding them takes a config call per region;
if a page stops listing a channel, that region's pages from
/interface/customers/config are scanned instead.
"""
from __future__ import annotations

import logging

from ._common import Network, NetChannel, parse_iso, program

logger = logging.getLogger(__name__)

_API = 'https://insight-api-shared.univtec.com/interface/'
_HEADERS = {'x-tenant-id': 'i24israel', 'platform': 'web', 'x-device-type': 'web',
            'Origin': 'https://video.i24news.tv', 'Accept': 'application/json'}
# region code -> (channel id, channel name, language, Live page id)
_REGIONS = {
    'all': ('english', 'i24NEWS English', 'en', '6a15e51fd7dd65c210d30d48'),
    'french': ('french', 'i24NEWS French', 'fr', '6a15e5646b920109d9fa6b31'),
    'hebrew': ('hebrew', 'i24NEWS Hebrew', 'he', '6a15e4c06b920109d9fa6a12'),
    'arabic': ('arabic', 'i24NEWS Arabic', 'ar', '6a15e5a7d7dd65c210d30e4a'),
}
# Programs without their own artwork point at a generic .../<LANG>/9999.png.
_GENERIC_POSTER = '/9999.png'


def _channel_items(page: dict) -> list[dict]:
    return [item for section in (page.get('sections') or []) for item in (section.get('items') or [])
            if isinstance(item, dict) and item.get('type') == 'channel' and item.get('videoUrl')]


class I24News(Network):
    key = 'i24news'
    name = 'i24NEWS'

    def _page(self, s, region: str, page_id: str) -> dict:
        r = s.get(_API + 'pages/' + page_id, headers={**_HEADERS, 'regioncode': region})
        data = r.json() if r else {}
        return data if isinstance(data, dict) else {}

    def _region_channel(self, s, region: str, page_id: str) -> dict | None:
        items = _channel_items(self._page(s, region, page_id))
        if items:
            return items[0]
        r = s.get(_API + 'customers/config', headers={**_HEADERS, 'regioncode': region})
        pages = ((r.json() if r else {}) or {}).get('config', {}).get('pages') or []
        for p in pages:
            if p.get('_id') and p['_id'] != page_id and p.get('regionCode') == region:
                items = _channel_items(self._page(s, region, p['_id']))
                if items:
                    logger.info('[free_networks] i24NEWS %s Live page moved to %s', region, p['_id'])
                    return items[0]
        return None

    def _feed(self, s) -> dict[str, dict]:
        def fetch():
            out = {}
            for region, (cid, _, _, page_id) in _REGIONS.items():
                try:
                    item = self._region_channel(s, region, page_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning('[free_networks] i24NEWS %s unavailable: %s', region, exc)
                    continue
                if item:
                    out[cid] = item
            return out
        return s.memo('i24news_feed', fetch)

    def channels(self, s):
        out = []
        meta = {cid: (name, lang) for cid, name, lang, _ in _REGIONS.values()}
        for cid, item in self._feed(s).items():
            name, lang = meta[cid]
            out.append(NetChannel(cid, name, item['videoUrl'].split('?', 1)[0],
                                  logo=item.get('image') or item.get('poster') or None,
                                  category='News', language=lang, country='IL'))
        return out

    def programs(self, s, channels):
        feed = self._feed(s)
        out = []
        for ch in channels:
            events = ((feed.get(ch.id) or {}).get('epgObject') or {}).get('events') or []
            for ev in events:
                if ev.get('noDisplay'):
                    continue
                custom = ev.get('custom') or {}
                poster = ((custom.get('poster') or custom.get('image') or {}).get('downloadUrl')) or None
                if poster and poster.endswith(_GENERIC_POSTER):
                    poster = None
                p = program(ch.id, ev.get('title'), parse_iso(ev.get('start')), parse_iso(ev.get('end')),
                            description=ev.get('description'), category=custom.get('category') or 'News',
                            poster_url=poster)
                if p:
                    out.append(p)
        return out

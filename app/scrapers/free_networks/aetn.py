"""A+E Networks' free FAST channels (Ax Men, The First 48, Dance Moms, ...).

The play.<brand> sites list them on /on-now, in the Next.js flight payload
(self.__next_f chunks): objects with id, slug, channelTitle, logoUrl,
streamUrl / fastStreamUrl (public Wurl or Amagi HLS, no login) and
streamType 'fast'. The one 'linear' entry per brand is the cable channel,
which is the A+E TVE source's.

Guide: A+E only exposes the current program (/tve-play/api/now-playing), so
the guide comes from tvtv via Gracenote IDs. IDs below were checked on
2026-10-17 by matching A+E's now-playing start time and episode against tvtv
(First 48, Live PD Presents and Tiny House Nation match on slot times, but
tvtv's episode titles are generic). The rest get placeholder blocks.
"""
from __future__ import annotations

import json
import re

from ._common import Network, NetChannel, logger

_BRANDS = [
    ('history', 'https://play.history.com/on-now', 'History'),
    ('aetv', 'https://play.aetv.com/on-now', 'Entertainment'),
    ('lifetime', 'https://play.mylifetime.com/on-now', 'Entertainment'),
]
_GRACENOTE = {
    'ax-men': '123425',
    'ice-road-truckers': '123426',
    'unxplained-zone': '123431',
    'deal-zone': '123432',
    'modern-marvels': '123433',
    'torque': '123423',
    'military-heroes': '126439',
    'cold-case-files': '129137',
    'crime-cults-killers': '123430',
    'duck-dynasty': '131429',
    'home-made-nation': '113673',
    'live-pd-presents': '176608',
    'storage-wars': '172207',
    'the-first-48': '181303',
    'tiny-house-nation': '118456',
    'dance-moms': '126369',
    'movie-favorites-by-lifetime': '121308',
    'matched-married-meet': '129047',
    'perform': '123424',
}
_CATEGORY = {
    'the-first-48': 'True Crime', 'live-pd-presents': 'True Crime', 'crime-360': 'True Crime',
    'crime-cults-killers': 'True Crime', 'cold-case-files': 'True Crime', 'chaos-cam-by-ae': 'True Crime',
    'home-made-nation': 'Lifestyle', 'tiny-house-nation': 'Lifestyle',
    'deal-zone': 'Entertainment', 'torque': 'Automotive', 'classic-car-auctions': 'Automotive',
    'xtreme-outdoors': 'Outdoors', 'modern-marvels': 'Documentary', 'unxplained-zone': 'Documentary',
    'lifetime-movies-love-and-drama': 'Movies', 'movie-favorites-by-lifetime': 'Movies',
}
_FLIGHT_RE = re.compile(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)</script>', re.S)
_CHANNEL_RE = re.compile(r'\{"id":"[^"]+","slug":')


class AETN(Network):
    key = 'aetn'
    name = 'A+E Networks FAST'

    def channels(self, s):
        out, seen = [], set()
        for brand, url, default_category in _BRANDS:
            r = s.get(url)
            if not r:
                continue
            found = 0
            for row in _flight_channels(r.text):
                slug = row.get('slug')
                stream = row.get('fastStreamUrl') or row.get('streamUrl')
                if row.get('streamType') != 'fast' or not slug or not stream or slug in seen:
                    continue
                seen.add(slug)
                found += 1
                out.append(NetChannel(slug, row.get('channelTitle') or slug, stream, logo=row.get('logoUrl'),
                                      category=_CATEGORY.get(slug, default_category),
                                      gracenote_id=_GRACENOTE.get(slug)))
            if not found:
                logger.warning('[free_networks] A+E %s: no FAST channels in %s', brand, url)
        return out


def _flight_channels(page: str) -> list[dict]:
    flight = ''
    for chunk in _FLIGHT_RE.findall(page):
        try:
            flight += json.loads(f'"{chunk}"')
        except ValueError:
            continue
    dec, out = json.JSONDecoder(), []
    for m in _CHANNEL_RE.finditer(flight):
        try:
            obj, _ = dec.raw_decode(flight, m.start())
        except ValueError:
            continue
        if 'streamType' in obj:
            out.append(obj)
    return out

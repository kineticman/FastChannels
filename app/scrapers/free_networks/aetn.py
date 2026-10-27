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
tvtv's episode titles are generic). Chaos Cam and Xtreme Outdoors get
placeholder blocks: their tvtv stations (171976; 116101 / 120962) carry the
same shows but at different times, so they're other playouts.
"""
from __future__ import annotations

import json
import re
import time

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
    'classic-car-auctions': '158083',
    'crime-360': '113780',
    'lifetime-movies-love-and-drama': '177720',
}
_CATEGORY = {
    'the-first-48': 'True Crime', 'live-pd-presents': 'True Crime', 'crime-360': 'True Crime',
    'crime-cults-killers': 'True Crime', 'cold-case-files': 'True Crime', 'chaos-cam-by-ae': 'True Crime',
    'home-made-nation': 'Lifestyle', 'tiny-house-nation': 'Lifestyle',
    'deal-zone': 'Shopping', 'torque': 'Automotive', 'classic-car-auctions': 'Automotive',
    'xtreme-outdoors': 'Outdoors', 'modern-marvels': 'Documentary', 'unxplained-zone': 'Documentary',
    'lifetime-movies-love-and-drama': 'Movies', 'movie-favorites-by-lifetime': 'Movies',
    'ax-men': 'Reality TV', 'duck-dynasty': 'Reality TV', 'dance-moms': 'Reality TV',
    'matched-married-meet': 'Reality TV', 'storage-wars': 'Reality TV',
}
# Since 2026-10-26 play.history.com's server render often fails to load its
# channel data and sends the page with loading skeletons instead (no fixed
# pattern: from 2 in 3 fetches to none for minutes); aetv and lifetime render
# every time. Retry the page a few times, spaced out, then fall back
# to the brand's last good list: the network-wide fallback in __init__ never
# kicks in while the other brands still list channels, and reconcile would
# deactivate History's channels after three missed scrapes.
_PAGE_ATTEMPTS = 6
_RETRY_DELAY_SECS = 3
_LAST_GOOD_CACHE_KEY = 'free_networks_aetn_last_good'
_LAST_GOOD_MAX_AGE = 3 * 86400
_FLIGHT_RE = re.compile(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)</script>', re.S)
_CHANNEL_RE = re.compile(r'\{"id":"[^"]+","slug":')


class AETN(Network):
    key = 'aetn'
    name = 'A+E Networks FAST'

    def channels(self, s):
        out, seen = [], set()
        last_good = dict(s.cache.get(_LAST_GOOD_CACHE_KEY) or {})
        for brand, url, default_category in _BRANDS:
            rows = []
            for attempt in range(_PAGE_ATTEMPTS):
                if attempt:
                    time.sleep(_RETRY_DELAY_SECS)
                r = s.get(url)
                if r:
                    rows = [row for row in _flight_channels(r.text)
                            if row.get('streamType') == 'fast' and row.get('slug')
                            and (row.get('fastStreamUrl') or row.get('streamUrl'))]
                if rows:
                    break
            if rows:
                last_good[brand] = {'at': time.time(), 'rows': rows}
                s._update_cache(_LAST_GOOD_CACHE_KEY, last_good)
            else:
                kept = last_good.get(brand) or {}
                if kept.get('rows') and time.time() - float(kept.get('at') or 0) < _LAST_GOOD_MAX_AGE:
                    rows = kept['rows']
                    logger.info('[free_networks] A+E %s: no FAST channels in %s after %d tries — '
                                'keeping its last %d', brand, url, _PAGE_ATTEMPTS, len(rows))
                else:
                    logger.warning('[free_networks] A+E %s: no FAST channels in %s', brand, url)
            for row in rows:
                slug = row['slug']
                if slug in seen:
                    continue
                seen.add(slug)
                out.append(NetChannel(slug, row.get('channelTitle') or slug,
                                      row.get('fastStreamUrl') or row.get('streamUrl'), logo=row.get('logoUrl'),
                                      category=_CATEGORY.get(slug, default_category),
                                      gracenote_id=_GRACENOTE.get(slug)))
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

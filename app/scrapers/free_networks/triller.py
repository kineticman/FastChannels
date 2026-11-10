"""TrillerTV's free 24/7 channels: Tennis TV 24/7 (ATP), Willow Sports
(cricket) and TrillerTV 24/7 (boxing, MMA, kickboxing). No account needed.

Stream: the embed player (trillertv.com/embed/v1/<id>/) asks
/embed/play/<id>.m3u8?site=fite&format=json and gets a CloudFront-signed
master URL, good for about 30 minutes, so it's fetched per tune. We swap
its 24-hour DVR playlist (playlist_dvr.m3u8) for the live one beside it
(playlist.m3u8, ~8-segment variants). The master
lists its variants as bare relative paths, and they (and the segments and the
AES-128 hls.key under them) are authorized by CloudFront cookies the master
sets, which a client following our 302 never has. The same Policy/Signature/
Key-Pair-Id work as a query string, so the channels go through the Free
Networks playlist proxy, which copies the master's query onto every child URL
(Network.child_url's default) and re-signs when it expires.

Guide: /api/_v2/videos/<vvid>/epg/?from_dt=&to_dt= (the watch page's EPG
widget). Willow lists ~3 days, TrillerTV 24/7 ~a day, Tennis TV nothing; each
is followed by hourly blocks named after the channel. Checked 2026-11-10.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from ._common import Network, NetChannel, parse_iso, program

_SITE = 'https://www.trillertv.com'
_FILL_DAYS = 3
# key, embed id, video id (guide), name, category, logo
_CHANNELS = [
    ('tennistv', '2phhm', 4548298, 'Tennis TV 24/7', 'Sports',
     f'{_SITE}/thumbs/vf/2phhm/tennistvclassics247_freechannel_2_1920x1080/atp-tennistv-classics-free-channel-1600x900fit.jpg',
     "The ATP's free channel: match highlights, classic replays and Challenger Tour streams."),
    ('willow', '2pfqj', 4546027, 'Willow Sports', 'Sports',
     f'{_SITE}/thumbs/vf/2pfqj/willowsports_1920x1080/willow-sports-cricket-1600x900fit.jpg',
     'Free linear cricket: highlights and matches from around the world.'),
    ('trillertv', '2p6mu', 4534230, 'TrillerTV 24/7', 'Sports',
     f'{_SITE}/thumbs/vf/2p6mu/trillertv_24-7_1920x1080/fite-24-7-1600x900fit.jpg',
     'Boxing, MMA, kickboxing and pro wrestling around the clock.'),
]
_VVID = {key: vvid for key, _eid, vvid, *_ in _CHANNELS}
_HEADERS = {'Accept': 'application/json', 'Referer': f'{_SITE}/'}


class Triller(Network):
    key = 'triller'
    name = 'TrillerTV'
    proxied = True
    proxy_hosts = ('cdn-cf.trillertv.com',)

    def channels(self, s):
        return [NetChannel(key, name, eid, logo=logo, category=cat, description=desc)
                for key, eid, _vvid, name, cat, logo, desc in _CHANNELS]

    def programs(self, s, channels):
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        end_at = now + timedelta(days=_FILL_DAYS)
        stamp = lambda d: d.strftime('%Y-%m-%dT%H:%M:%S.000Z')  # noqa: E731
        out = []
        for ch in channels:
            vvid = _VVID.get(ch.id)
            rows = []
            if vvid:
                try:
                    r = s.session.get(f'{_SITE}/api/_v2/videos/{vvid}/epg/', headers=_HEADERS, timeout=30,
                                      params={'from_dt': stamp(now - timedelta(hours=3)), 'to_dt': stamp(end_at)})
                    r.raise_for_status()
                    rows = (r.json() or {}).get('programs') or []
                except Exception:  # noqa: BLE001 — one channel's guide failing leaves it on blocks
                    rows = []
            mine = []
            for row in rows:
                image = (((row.get('links') or {}).get('image') or {}).get('href') or '')
                image = image.replace('{width}', '640').replace('{height}', '360') or None
                p = program(ch.id, row.get('title'), parse_iso(row.get('start_time')), parse_iso(row.get('end_time')),
                            description=row.get('description'), poster_url=image, category=ch.category)
                if p:
                    mine.append(p)
            out.extend(mine)
            # Past the listed schedule (all of it, for Tennis TV): hourly blocks.
            t = max([p.end_time for p in mine], default=now)
            while t < end_at:
                nxt = (t + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                out.append(program(ch.id, ch.name, t, nxt, category=ch.category, description=ch.description))
                t = nxt
        return [p for p in out if p]

    def resolve(self, s, ref):
        r = s.session.get(f'{_SITE}/embed/play/{ref}.m3u8', timeout=20,
                          headers={**_HEADERS, 'Referer': f'{_SITE}/embed/v1/{ref}/'},
                          params={'site': 'fite', 'ts': str(int(time.time() * 1000)), 'format': 'json'})
        r.raise_for_status()
        url = (r.json() or {}).get('url')
        if not url:
            raise RuntimeError(f'TrillerTV gave no stream for {ref}')
        # The player gets the 24-hour DVR playlist (~14,400 segments, ~10 MB
        # per refresh once every URI carries the signature); the live one
        # beside it (~8 segments) is covered by the same signature.
        return url.replace('/playlist_dvr.m3u8', '/playlist.m3u8', 1)

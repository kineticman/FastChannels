"""NHK World-Japan.

Stream: media-tyo.hls.nhkworld.jp/hls/w/live/master.m3u8 (plain; the web
player itself uses signed per-variant paths under the same host). Guide:
masterpl.hls.nhkworld.jp/epg/w/YYYYMMDD.json, one file per JST day, ~11 days
ahead, with ISO start/end times that carry their own +09:00 offset.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ._common import Network, NetChannel, parse_iso, program

_STREAM = 'https://media-tyo.hls.nhkworld.jp/hls/w/live/master.m3u8'
_EPG = 'https://masterpl.hls.nhkworld.jp/epg/w/{day}.json'
_LOGO = 'https://www3.nhk.or.jp/nhkworld/common/sns_images/nhkworld_1200x630.png'
_JST = ZoneInfo('Asia/Tokyo')
_DAYS = 7


class NHK(Network):
    key = 'nhk'
    name = 'NHK World'

    def channels(self, s):
        return [NetChannel('world', 'NHK World-Japan', _STREAM, logo=_LOGO, category='News', country='JP')]

    def programs(self, s, channels):
        if not channels:
            return []
        today = datetime.now(_JST).date()
        out, seen = [], set()
        for i in range(-1, _DAYS):
            r = s.get(_EPG.format(day=(today + timedelta(days=i)).strftime('%Y%m%d')))
            for row in ((r.json() or {}).get('data') or []) if r else []:
                start = parse_iso(row.get('startTime'))
                if not start or start in seen:
                    continue
                seen.add(start)
                p = program('world', row.get('title'), start, parse_iso(row.get('endTime')),
                            episode_title=row.get('episodeTitle'), description=row.get('description'),
                            poster_url=row.get('thumbnail'))
                if p:
                    out.append(p)
        return out

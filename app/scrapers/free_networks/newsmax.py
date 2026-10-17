"""Newsmax2, Newsmax's free channel (the main Newsmax feed needs a TV login).

Stream from the newsmaxtv.com player. Guide: api.newsmaxtv.com's WideOrbit
timeline for station "n2": rows with ShowDateTime (naive Eastern),
ShowDuration (seconds), Title, Episode, Description; about a week.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from ._common import ET, Network, NetChannel, program

_STREAM = 'https://n2ctsota.akamaized.net/hls/live/2113130/Live_1/index.m3u8'
_SCHEDULE = 'https://api.newsmaxtv.com/api/wideorbittimeline/1011/schedule/n2'
# Gracenote's Newsmax 2 station logo; newsmaxtv.com itself times out to non-browser clients.
_LOGO = 'https://tmsimg.fancybits.co/assets/s121162_ll_h15_aa.png?w=360&h=270'


class Newsmax(Network):
    key = 'newsmax'
    name = 'Newsmax2'

    def channels(self, s):
        return [NetChannel('n2', 'Newsmax2', _STREAM, logo=_LOGO, category='News')]

    def programs(self, s, channels):
        if not channels:
            return []
        r = s.get(_SCHEDULE, headers={'Origin': 'https://www.newsmaxtv.com', 'Referer': 'https://www.newsmaxtv.com/'})
        out, seen = [], set()
        for row in (r.json() or []) if r else []:
            try:
                start = datetime.fromisoformat(row['ShowDateTime']).replace(tzinfo=ET)
                secs = int(row.get('ShowDuration') or 0)
            except (KeyError, ValueError, TypeError):
                continue
            if secs <= 0 or start in seen:
                continue
            seen.add(start)
            title = row.get('Title') or row.get('Program')
            episode = row.get('Episode')
            p = program('n2', title, start, start + timedelta(seconds=secs),
                        episode_title=episode if episode and episode != title else None,
                        description=row.get('Description'))
            if p:
                out.append(p)
        return out

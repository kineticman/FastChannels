"""Newsmax's free channels: Newsmax2 and World at War (the main Newsmax and
Newsmax en Español feeds are behind newsmaxtv.com's Newsmax+ paywall).

Streams from the newsmaxtv.com player (pages with enablePaywall = false).
Guide: api.newsmaxtv.com's WideOrbit timeline per station ("n2",
"WorldAtWar"): rows with ShowDateTime (naive Eastern), ShowDuration
(seconds), Title, Episode, Description; about a week.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from ._common import ET, Network, NetChannel, program

_SCHEDULE = 'https://api.newsmaxtv.com/api/wideorbittimeline/1011/schedule/{station}'
# id → (name, stream, WideOrbit station, logo, category). Newsmax2's logo is
# Gracenote's. World at War's is self-hosted (cropped from its newsmaxtv.com page
# art): newsmaxtv.com hangs for most user agents, ours and browsers' included.
_WAW_LOGO = '/static/logos/newsmax/worldatwar.png'
_CHANNELS = {
    'n2': ('Newsmax2', 'https://n2ctsota.akamaized.net/hls/live/2113130/Live_1/index.m3u8', 'n2',
           'https://tmsimg.fancybits.co/assets/s121162_ll_h15_aa.png?w=360&h=270', 'News'),
    'worldatwar': ('World at War', 'https://linear-927.frequency.stream/dist/newsmax/927/hls/master/playlist.m3u8',
                   'WorldAtWar', _WAW_LOGO, 'History'),
}


class Newsmax(Network):
    key = 'newsmax'
    name = 'Newsmax'

    def channels(self, s):
        return [NetChannel(cid, name, stream, logo=logo, category=category)
                for cid, (name, stream, _, logo, category) in _CHANNELS.items()]

    def programs(self, s, channels):
        out = []
        for ch in channels:
            station = _CHANNELS[ch.id][2]
            r = s.get(_SCHEDULE.format(station=station),
                      headers={'Origin': 'https://www.newsmaxtv.com', 'Referer': 'https://www.newsmaxtv.com/'})
            seen = set()
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
                if episode in ('Feature Film', 'Special'):  # World at War's labels, not titles
                    episode = None
                p = program(ch.id, title, start, start + timedelta(seconds=secs),
                            episode_title=episode if episode and episode != title else None,
                            description=row.get('Description'))
                if p:
                    out.append(p)
        return out

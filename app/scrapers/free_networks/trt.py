"""TRT World and TRT Arabi.

Streams are fixed (tv-<name>.medya.trt.com.tr/master.m3u8, taken from
trtworld.com's liveStream data). Guide: TRT's e-television schedule API, one
call per day (it returns 204 without a date): planDateTime (UTC),
planDurationMilisecond, programName, synopsis, programMedias[].link.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from ._common import UTC, Network, NetChannel, parse_iso, program

_API = 'https://etelevizyon.trt.net.tr/TRT.etelevizyon/api/Schedule/GetPlannedSchedulesWithActualDate'
_CHANNELS = [
    # id, name, stream, schedule channelId, language, logo
    ('world', 'TRT World', 'https://tv-trtworld.medya.trt.com.tr/master.m3u8', 31, 'en',
     'https://www.trtworld.com/favicon/favicon-world-48.png'),
    ('arabi', 'TRT Arabi', 'https://tv-trtarabi.medya.trt.com.tr/master.m3u8', 6, 'ar',
     'https://www.trtarabi.com/favicon/favicon-trt-48.png'),
]
_DAYS = 7


class TRT(Network):
    key = 'trt'
    name = 'TRT'

    def channels(self, s):
        return [NetChannel(cid, name, url, logo=logo, category='News', language=lang, country='TR')
                for cid, name, url, _, lang, logo in _CHANNELS]

    def programs(self, s, channels):
        wanted = {ch.id for ch in channels}
        today = datetime.now(UTC).date()
        out = []
        for cid, _, _, sched_id, _, _ in _CHANNELS:
            if cid not in wanted:
                continue
            seen = set()
            for i in range(-1, _DAYS):
                r = s.get(_API, params={'channelId': sched_id, 'date': (today + timedelta(days=i)).isoformat()})
                if not r or r.status_code != 200:
                    continue
                for row in r.json() or []:
                    start = parse_iso(row.get('planDateTime'))
                    ms = row.get('planDurationMilisecond') or 0
                    if not start or not ms or start in seen:
                        continue
                    seen.add(start)
                    media = row.get('programMedias') or []
                    p = program(cid, (row.get('programName') or '').title(), start,
                                start + timedelta(milliseconds=ms), description=row.get('synopsis'),
                                poster_url=media[0].get('link') if media else None)
                    if p:
                        out.append(p)
        return out

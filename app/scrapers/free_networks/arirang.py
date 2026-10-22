"""Arirang TV (Korea's English-language international channel).

Stream: amdlive-ch01-g-ctnd-com.akamaized.net/arirang_1gch (the "World" feed
the website plays). Guide: arirang.com's own proxy to their broadcast system,
POST /v1.0/open/external/proxy with listScheduleV3.do for one broadcast day:
dsSchWeek[{broadYmd, broadHm "HHMM", broadRun (minutes), displayNm, episodeNm}].
Times are KST: shifted to UTC they line up title-for-title (90/95) with tvtv's
listing for Arirang's international feed (Gracenote 142652), checked
2026-10-17. displayNm is all caps with in-house tags ("[R]" reruns, Korean
notes like "(B-30' 자체A)"), which _title() strips.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ._common import Network, NetChannel, program

_STREAM = 'https://amdlive-ch01-g-ctnd-com.akamaized.net/arirang_1gch/smil:arirang_1gch.smil/playlist.m3u8'
_PROXY = 'https://www.arirang.com/v1.0/open/external/proxy'
_SCHEDULE = 'https://script.arirang.com/api/v1/bis/listScheduleV3.do'
_LOGO = 'https://www.arirang.com/images/logo192.png'
_KST = ZoneInfo('Asia/Seoul')
_DAYS = 7
_TAGS_RE = re.compile(r'\s*\[[^\]]*\]|\s*\([^)]*[가-힣][^)]*\)')  # [R], (…Korean…)


def _title(name):
    name = _TAGS_RE.sub('', name or '').strip()
    return name.title() if name.isupper() else name


class Arirang(Network):
    key = 'arirang'
    name = 'Arirang'

    def channels(self, s):
        return [NetChannel('world', 'Arirang TV', _STREAM, logo=_LOGO, category='News', country='KR')]

    def programs(self, s, channels):
        if not channels:
            return []
        today = datetime.now(_KST).date()
        out, seen = [], set()
        for i in range(-1, _DAYS):
            day = (today + timedelta(days=i)).strftime('%Y%m%d')
            body = {'address': _SCHEDULE, 'method': 'POST', 'headers': {},
                    'body': {'data': {'dmParam': {'chanId': 'CH_W', 'broadYmd': day, 'planNo': '1'}}}}
            try:
                r = s.session.post(_PROXY, json=body, timeout=30,
                                   headers={'Origin': 'https://www.arirang.com', 'Referer': 'https://www.arirang.com/'})
                r.raise_for_status()
                rows = (r.json().get('responseBody') or {}).get('dsSchWeek') or []
            except Exception:
                continue
            for row in rows:
                try:
                    d, hm = row['broadYmd'], row['broadHm']
                    start = datetime(int(d[:4]), int(d[4:6]), int(d[6:8]), int(hm[:2]) % 24, int(hm[2:4]),
                                     tzinfo=_KST) + timedelta(days=int(hm[:2]) // 24)
                    minutes = int(row.get('broadRun') or 0)
                except (KeyError, ValueError, TypeError):
                    continue
                if start in seen or minutes <= 0:
                    continue
                seen.add(start)
                title = _title(row.get('displayNm'))
                episode = row.get('episodeNm')
                p = program('world', title, start, start + timedelta(minutes=minutes),
                            episode_title=episode if episode and episode != row.get('displayNm') else None)
                if p:
                    out.append(p)
        return out

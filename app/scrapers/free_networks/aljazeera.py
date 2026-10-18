"""Al Jazeera English, Arabic, Arabic 2, Mubasher and Documentary.

Streams: the /video/live pages carry a base64 __APOLLO_STATE__ whose Post has
liveBroadcast / liveBroadcastPlayers[].bcPlaybackUrl (clear HLS on thehlive.com).
The English page lists AJE; the Arabic page lists AJA, AJA2, AJM and AJD.

Guide: the /schedule page's Post.schedule[] — a week of {showDay, showTimeslot
"HH:MM", duration "H:M:S", startDate (epoch of that day's 00:00 UTC), showName,
showDescription}. English times are UTC (the page converts to local in JS);
Arabic says "all times Makkah time" (UTC+3). Mubasher and Documentary have no
schedule on the site; their guide comes from the tvtv cache via Gracenote IDs
(checked against tvtv's Arabic listings 2026-10-17). Arabic 2 has neither.
"""
from __future__ import annotations

import base64
import json
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ._common import UTC, Network, NetChannel, from_epoch, program

_SITES = {'en': 'https://www.aljazeera.com', 'ar': 'https://www.aljazeera.net'}
_STATE_RE = re.compile(r'__APOLLO_STATE__="([^"]+)"')
_PATH_RE = re.compile(r'/([A-Z0-9]+)/index\.m3u8')

# stream path key -> (channel id, name, language, schedule site, schedule tz)
_KNOWN = {
    'AJE':  ('aje', 'Al Jazeera English', 'en', 'en', UTC),
    'AJA':  ('aja', 'Al Jazeera Arabic', 'ar', 'ar', ZoneInfo('Asia/Riyadh')),
    'AJA2': ('aja2', 'Al Jazeera Arabic 2', 'ar', None, None),
    'AJM':  ('ajm', 'Al Jazeera Mubasher', 'ar', None, None),
    'AJD':  ('ajd', 'Al Jazeera Documentary', 'ar', None, None),
}
_GRACENOTE = {'ajm': '143340', 'ajd': '143339'}
_LOGOS = {
    'en': 'https://www.aljazeera.com/images/logo_aje_social.png',
    'ar': 'https://www.aljazeera.net/images/logo_aja_social.png',
}


def _state(s, url):
    r = s.get(url)
    m = _STATE_RE.search(r.text) if r else None
    if not m:
        return {}
    try:
        return json.loads(base64.b64decode(m.group(1)))
    except Exception:
        return {}


def _posts(state):
    return [v for v in state.values() if isinstance(v, dict) and v.get('__typename') == 'Post']


class AlJazeera(Network):
    key = 'aljazeera'
    name = 'Al Jazeera'

    def channels(self, s):
        found = {}
        for lang, site in _SITES.items():
            for post in _posts(_state(s, site + '/video/live')):
                players = [post.get('liveBroadcast') or {}] + list(post.get('liveBroadcastPlayers') or [])
                for p in players:
                    url = (p or {}).get('bcPlaybackUrl')
                    m = _PATH_RE.search(url or '')
                    if m and m.group(1) in _KNOWN and m.group(1) not in found:
                        found[m.group(1)] = url
        out = []
        for path_key, (cid, name, lang, _, _) in _KNOWN.items():
            if path_key in found:
                out.append(NetChannel(cid, name, found[path_key], logo=_LOGOS[lang],
                                      category='Documentary' if cid == 'ajd' else 'News',
                                      language=lang, country='QA', gracenote_id=_GRACENOTE.get(cid)))
        return out

    def has_guide(self, channel_id):
        return any(cid == channel_id and site for cid, _, _, site, _ in _KNOWN.values())

    def programs(self, s, channels):
        wanted = {ch.id for ch in channels}
        out = []
        for cid, _, _, site_lang, tz in _KNOWN.values():
            if cid not in wanted or not site_lang:
                continue
            for post in _posts(_state(s, _SITES[site_lang] + '/schedule')):
                for row in post.get('schedule') or []:
                    start, end = self._times(row, tz)
                    p = program(cid, row.get('showName'), start, end,
                                description=row.get('showDescription'))
                    if p:
                        out.append(p)
        return out

    @staticmethod
    def _times(row, tz):
        day = from_epoch(row.get('startDate'))
        try:
            hh, mm = (int(x) for x in (row.get('showTimeslot') or '').split(':')[:2])
            h, m, sec = (int(x or 0) for x in ((row.get('duration') or '') + '::').split(':')[:3])
        except ValueError:
            return None, None
        if not day:
            return None, None
        start = datetime(day.year, day.month, day.day, hh, mm, tzinfo=tz)
        return start, start + timedelta(hours=h, minutes=m, seconds=sec)

"""Scripps News.

Guide and stream both come from Scripps' scheduler API (the one their site's
schedule widget reads): schedules[] of {title, description, *_image, timezone
'US/Eastern', date_start, time_start, time_end}, ~17 days of single airings.
A time_end of 00:00 means midnight after. extras[0].stream_url is the live
uplynk channel; the live page's own uplynk ID is the fallback.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ._common import ET, Network, NetChannel, program

_API = 'https://feedmachine.ewscloud.com/scheduler/api/schedules/com.scrippsnews/config/'
_FALLBACK_STREAM = 'https://content.uplynk.com/channel/4bb4901b934c4e029fd4c1abfc766c37.m3u8'
_LOGO = 'https://ewscripps.brightspotcdn.com/6a/67/0907d04f41ee8cbb00aec3a5ca9f/main-logo.png'
# The container's tz database has no legacy US/* links.
_LEGACY_ZONES = {'US/Eastern': 'America/New_York', 'US/Central': 'America/Chicago',
                 'US/Mountain': 'America/Denver', 'US/Pacific': 'America/Los_Angeles'}


def _zone(name):
    try:
        return ZoneInfo(_LEGACY_ZONES.get(name or '', name or 'America/New_York'))
    except Exception:
        return ET


class Scripps(Network):
    key = 'scripps'
    name = 'Scripps News'

    def _config(self, s):
        def fetch():
            r = s.get(_API, params={'type': 'web', 'channel': 'snews-main-channel'})
            return r.json() if r else {}
        return s.memo('scripps', fetch)

    def channels(self, s):
        extras = self._config(s).get('extras') or [{}]
        stream = (extras[0] or {}).get('stream_url') or _FALLBACK_STREAM
        return [NetChannel('news', 'Scripps News', stream, logo=_LOGO, category='News', gracenote_id='120010')]

    def programs(self, s, channels):
        if not channels:
            return []
        out = []
        for row in self._config(s).get('schedules') or []:
            tz = _zone(row.get('timezone'))
            try:
                day = datetime.strptime(row['date_start'], '%Y-%m-%d')
                t0 = datetime.strptime(row['time_start'], '%H:%M:%S').time()
                t1 = datetime.strptime(row['time_end'], '%H:%M:%S').time()
            except (KeyError, ValueError, TypeError):
                continue
            start = datetime.combine(day.date(), t0, tzinfo=tz)
            end = datetime.combine(day.date(), t1, tzinfo=tz)
            if end <= start:
                end += timedelta(days=1)
            p = program('news', row.get('title'), start, end, description=row.get('description'),
                        poster_url=row.get('web_image') or row.get('ott_image'))
            if p:
                out.append(p)
        return out

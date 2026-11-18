"""CGTN (China Global Television Network): English, Español, Français,
Arabic, Russian and Documentary.

Channels: api.cgtn.com/website/api/live/channel/list, the list behind
cgtn.com/tv, gives each channel's id, name, logo and a fixed clear HLS URL
(english-livetx.cgtn.com/hls/<key>/playlist.m3u8, up to 1080p, no login).
The older news.cgtn.com/resource/live/... masters still answer but point at
a host that's gone.

Guide: .../live/channel/epg/list?channelId=<id>&startTime=&endTime= (epoch
ms) with the same ids. It rejects ranges past one day ("date check fail"), so
it is asked one UTC day at a time; it covers ~5 days ahead (checked
2026-11-17). Rows carry only a title.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from ._common import Network, NetChannel, from_epoch, program

logger = logging.getLogger(__name__)

_API = 'https://api.cgtn.com/website/api/live/channel/'
_DAYS = 5
# id -> (channel id, name, language). Names here, not the API's (which are in
# each channel's own script: "CGTN العربية", "CGTN Pусский").
_CHANNELS = {
    '1': ('english', 'CGTN', 'en'),
    '2': ('espanol', 'CGTN Español', 'es'),
    '3': ('francais', 'CGTN Français', 'fr'),
    '4': ('arabic', 'CGTN Arabic', 'ar'),
    '5': ('russian', 'CGTN Russian', 'ru'),
    '12': ('documentary', 'CGTN Documentary', 'en'),
}
_API_ID = {cid: api_id for api_id, (cid, _, _) in _CHANNELS.items()}


class CGTN(Network):
    key = 'cgtn'
    name = 'CGTN'

    def channels(self, s):
        r = s.get(_API + 'list')
        rows = ((r.json() if r else {}) or {}).get('data') or []
        out = []
        for row in rows:
            meta = _CHANNELS.get(str(row.get('id')))
            video = ((row.get('coverVideo') or {}).get('video') or {})
            url = video.get('url') or video.get('backupUrl')
            if not meta or not url:
                continue
            cid, name, lang = meta
            out.append(NetChannel(cid, name, url, logo=(row.get('shareBody') or {}).get('iconUrl'),
                                  category='Documentary' if cid == 'documentary' else 'News',
                                  language=lang, country='CN'))
        return out

    def programs(self, s, channels):
        day0 = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        out = []
        for ch in channels:
            api_id = _API_ID.get(ch.id)
            if not api_id:
                continue
            seen = set()
            for i in range(_DAYS):
                start = int((day0 + timedelta(days=i)).timestamp() * 1000)
                r = s.get(_API + 'epg/list', params={'channelId': api_id, 'startTime': start,
                                                     'endTime': start + 86_399_000})
                for row in ((r.json() if r else {}) or {}).get('data') or []:
                    key = row.get('epgId') or (row.get('startTime'), row.get('name'))
                    if key in seen:
                        continue
                    seen.add(key)
                    p = program(ch.id, row.get('name'), from_epoch(row.get('startTime')),
                                from_epoch(row.get('endTime')),
                                category='Documentary' if ch.id == 'documentary' else 'News')
                    if p:
                        out.append(p)
        return out

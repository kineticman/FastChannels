"""EWTN's regional TV feeds and Adoration.

One call — www.ewtn.com/api/content/live/epg/d/en — returns every feed (TV,
radio, adoration) with its mediaUrl (Wowza HLS) and ~3.5 days of programs with
UTC start/end. Radio feeds are audio-only and left out.
"""
from __future__ import annotations

from ._common import Network, NetChannel, parse_iso, program

_API = 'https://www.ewtn.com/api/content/live/epg/d/en'
_LOGO = 'https://www.ewtn.com/images/ewtn-stream-catholic-share.jpg'
# channelId -> (name, language, country)
_NAMES = {
    'united-states': ('EWTN', 'en', 'US'),
    'europe': ('EWTN Europe', 'en', 'GB'),
    'canada': ('EWTN Canada', 'en', 'CA'),
    'asia-pacific': ('EWTN Asia Pacific', 'en', 'AU'),
    'africa-asia': ('EWTN Africa/Asia', 'en', 'US'),
    'estados-unidos': ('EWTN Español', 'es', 'US'),
    'es-latam': ('EWTN Latinoamérica', 'es', 'US'),
    'adoration': ('EWTN Adoration', 'en', 'US'),
}


class EWTN(Network):
    key = 'ewtn'
    name = 'EWTN'

    def _feeds(self, s):
        def fetch():
            r = s.get(_API)
            feeds = {}
            for group in (r.json() if r else []) or []:
                for ch in group.get('channels') or []:
                    if ch.get('medium') == 'video' and ch.get('mediaUrl') and ch.get('channelId'):
                        feeds[ch['channelId']] = ch
            return feeds
        return s.memo('ewtn', fetch)

    def channels(self, s):
        out = []
        for cid, feed in self._feeds(s).items():
            name, lang, country = _NAMES.get(cid, (f"EWTN {feed.get('name') or cid}", 'en', 'US'))
            out.append(NetChannel(cid, name, feed['mediaUrl'], logo=_LOGO, category='Faith',
                                  language=lang, country=country))
        return out

    def programs(self, s, channels):
        feeds = self._feeds(s)
        out = []
        for ch in channels:
            for row in (feeds.get(ch.id) or {}).get('programs') or []:
                images = row.get('images') or {}
                title = row.get('title')
                sub = row.get('subtitle')
                p = program(ch.id, title, parse_iso((row.get('startDate') or {}).get('iso')),
                            parse_iso((row.get('endDate') or {}).get('iso')),
                            episode_title=sub if sub and sub != title else None,
                            description=row.get('description'),
                            poster_url=images.get('fhd') or images.get('uhd') or row.get('programBackdropUrl'))
                if p:
                    out.append(p)
        return out

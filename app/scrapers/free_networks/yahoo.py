"""Yahoo Finance and Yahoo Sports Network (Yahoo's free 24/7 channels).

Both play through Yahoo's video API: GET video-api.yql.yahoo.com/v1/video/
sapi/streams/<uuid> returns the channel's mediaObj with one HLS stream
(Brightcove Live behind yahoo2.global.transmit.live, clear, Finance up to
720p and Sports up to 1080p, no login). The playlist path is signed per
request, so the channel stores the uuid and resolve() asks for a fresh URL.

Guide: Yahoo Finance's live schedule.json (s.yimg.com/cv/apiv2/finance/
portal/live/schedule.json, ~3 days ahead, half-hour shows) also names the
channel's current uuid. Yahoo Sports Network's watch page lists shows but no
times, so it takes tvtv's guide for its Gracenote ID (the FAST station Fubo
and DirecTV carry; tvtv's now-playing matched the page on 2026-11-17).
"""
from __future__ import annotations

import logging

from ._common import Network, NetChannel, from_epoch, program

logger = logging.getLogger(__name__)

_STREAMS = 'https://video-api.yql.yahoo.com/v1/video/sapi/streams/{}'
_SCHEDULE = 'https://s.yimg.com/cv/apiv2/finance/portal/live/schedule.json'
# Fallback uuids, for when schedule.json is down (Finance) and always (Sports:
# the watch page's HeroVideoUUID, "Yahoo Sports Network").
_FINANCE_UUID = '5a59a610-41d0-315f-925d-1491376872b9'
_SPORTS_UUID = 'cfcda0ce-0f84-3eb5-8320-804cbe53506e'
_FINANCE_LOGO = 'https://s.yimg.com/cv/apiv2/cv/apiv2/social/images/yahoo-finance-default-logo.png'
_SPORTS_LOGO = 'https://s.yimg.com/cv/apiv2/watch/logo/tv_share_1000_524.jpg'
_SITE = {'finance': 'finance', 'sports': 'sports'}


class Yahoo(Network):
    key = 'yahoo'
    name = 'Yahoo'

    def _schedule(self, s) -> dict:
        def fetch():
            r = s.get(_SCHEDULE, params={'lang': 'en-US', 'region': 'US'})
            body = (r.json() if r else {}) or {}
            return body.get('body') if isinstance(body.get('body'), dict) else {}
        return s.memo('yahoo_schedule', fetch)

    def channels(self, s):
        finance_uuid = self._schedule(s).get('uuid') or _FINANCE_UUID
        return [
            NetChannel('finance', 'Yahoo Finance', f'finance:{finance_uuid}', logo=_FINANCE_LOGO,
                       category='News', gracenote_id='147894',
                       description="Yahoo Finance's live market news and analysis."),
            NetChannel('sports', 'Yahoo Sports Network', f'sports:{_SPORTS_UUID}', logo=_SPORTS_LOGO,
                       category='Sports', gracenote_id='187516',
                       description="Yahoo Sports' 24/7 channel: live shows and highlights."),
        ]

    def has_guide(self, channel_id: str) -> bool:
        return channel_id == 'finance'

    def programs(self, s, channels):
        if not any(ch.id == 'finance' for ch in channels):
            return []
        rows = self._schedule(s).get('schedule')
        if not rows:
            return None
        out = []
        for row in rows:
            start = from_epoch(row.get('startTime'))
            try:
                end = from_epoch(float(row.get('startTime')) / 1000 + float(row.get('duration')))
            except (TypeError, ValueError):
                continue
            p = program('finance', row.get('showTitle'), start, end, description=row.get('description') or None,
                        category='News')
            if p:
                out.append(p)
        return out

    def resolve(self, s, ref: str) -> str:
        site, _, uuid = ref.partition(':')
        r = s.session.get(_STREAMS.format(uuid), timeout=15, params={
            'protocol': 'http', 'format': 'm3u8', 'rt': 'html', 'region': 'US',
            'site': _SITE.get(site, site), 'lang': 'en-US'})
        r.raise_for_status()
        objs = ((r.json().get('query') or {}).get('results') or {}).get('mediaObj') or []
        streams = (objs[0].get('streams') if objs else None) or []
        hls = [st for st in streams if 'mpegurl' in (st.get('mime_type') or '').lower()] or streams
        if not hls or not hls[0].get('host'):
            status = (objs[0].get('status') or {}).get('msg') if objs else None
            raise RuntimeError(f'Yahoo returned no stream for {site} ({status or "no media"})')
        return hls[0]['host'] + (hls[0].get('path') or '')

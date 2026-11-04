"""NWSL+ 24/7 (plus.nwslsoccer.com/live/275408): the National Women's Soccer
League's free around-the-clock channel. NWSL+'s matches need an account; this
channel doesn't.

NWSL+ runs on DICE (realm dce.nwsl), like Shout! TV and SportsNet Pittsburgh:
a guest token from /v2/login/guest/checkin with the web app's shared x-api-key
is GRANTED the event, and /v5/manifest/event/275408 returns clear HLS (7
renditions, METHOD=NONE) on dice-live-us.akamaized.net. The URL carries a
short-lived hdnea token, so it's fetched per tune. The 24/7 event isn't in
the realm's live list, so its id is fixed.

Guide: the home page's "NWSL 24-7" row is an EPG item (channelId 1140), and
/v4/epg/content/programmes?channelId=1140 gives its schedule, but only ~7
hours ahead (3 rows: match replays and the like). Those are used, followed by
hourly blocks named after the channel for 3 days, so a DVR's guide doesn't run
dry between the 6-hourly refreshes. No Gracenote ID. Checked 2026-11-02.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone

from ._common import Network, NetChannel, parse_iso, program

_API = 'https://dce-frontoffice.imggaming.com/api'
_SITE = 'https://plus.nwslsoccer.com'
_HEADERS = {'x-api-key': '857a1e5d-e35e-4fdf-805b-a87b6f8364bf', 'Realm': 'dce.nwsl', 'app': 'dice',
            'x-app-var': '6.60.0', 'Origin': _SITE, 'Referer': _SITE + '/',
            'Content-Type': 'application/json', 'Accept': 'application/json'}
_EVENT = '275408'
_GUIDE_CHANNEL = '1140'
_LOGO = 'https://img.dge-prod.dicelaboratory.com/original/2024/11/22101220-tgwkrv9kdmvdqo2o.png'
_FILL_DAYS = 3
# Guest tokens last longer than this; take a new one now and then, and on any 401.
_TOKEN_MAX_AGE = 3600

_token_lock = threading.Lock()
_token: tuple[str, float] | None = None


def _guest_token(s, fresh: bool = False) -> str:
    global _token
    with _token_lock:
        if not fresh and _token and time.time() - _token[1] < _TOKEN_MAX_AGE:
            return _token[0]
        r = s.session.post(f'{_API}/v2/login/guest/checkin', json={}, headers=_HEADERS, timeout=20)
        r.raise_for_status()
        token = (r.json() or {}).get('authorisationToken')
        if not token:
            raise RuntimeError('NWSL+ guest check-in returned no token')
        _token = (token, time.time())
        return token


class NWSL(Network):
    key = 'nwsl'
    name = 'NWSL+ 24/7'

    def channels(self, s):
        return [NetChannel('nwsl247', 'NWSL+ 24/7', _EVENT, logo=_LOGO, category='Sports',
                           description="The National Women's Soccer League's free 24/7 channel.")]

    def programs(self, s, channels):
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        stamp = lambda d: d.strftime('%Y-%m-%dT%H:%M:%S.000Z')  # noqa: E731
        r = s.session.get(f'{_API}/v4/epg/content/programmes', timeout=30,
                          headers={**_HEADERS, 'Authorization': f'Bearer {_guest_token(s)}'},
                          params={'channelId': _GUIDE_CHANNEL, 'from': stamp(now - timedelta(hours=3)),
                                  'to': stamp(now + timedelta(days=_FILL_DAYS))})
        r.raise_for_status()
        out = []
        for ch in channels:
            for row in ((r.json() or {}).get('channels') or {}).get(_GUIDE_CHANNEL) or []:
                p = program(ch.id, row.get('episode') or row.get('title'), parse_iso(row.get('startDate')),
                            parse_iso(row.get('endDate')), description=row.get('description'),
                            poster_url=row.get('thumbnailUrl') or None, category='Sports')
                if p:
                    out.append(p)
            # The schedule only reaches a few hours ahead: hourly blocks after it.
            t = max([p.end_time for p in out if p.source_channel_id == ch.id], default=now)
            while t < now + timedelta(days=_FILL_DAYS):
                nxt = (t + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                out.append(program(ch.id, ch.name, t, nxt, category='Sports'))
                t = nxt
        return [p for p in out if p]

    def resolve(self, s, ref):
        for fresh in (False, True):
            r = s.session.post(f'{_API}/v5/manifest/event/{ref}', timeout=20,
                               params={'includePlaybackDetails': 'URL', 'displayGeoblocked': 'HIDE'},
                               headers={**_HEADERS, 'Authorization': f'Bearer {_guest_token(s, fresh)}'},
                               json={'mediaCapabilities': [{'protocols': ['HLS'], 'audioCodecs': ['aac'],
                                                            'videoCodecs': ['h264']}]})
            if r.status_code == 401 and not fresh:
                continue
            r.raise_for_status()
            body = r.json() or {}
            url = next((st['url'] for st in body.get('streams') or []
                        if st.get('url') and (st.get('protocol') or '').upper() == 'HLS'), None)
            if not url:
                raise RuntimeError(f'NWSL+ gave no HLS stream (access {body.get("accessLevel")})')
            return url
        raise RuntimeError('NWSL+ refused the guest token')

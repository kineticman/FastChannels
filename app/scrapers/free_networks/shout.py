"""Shout! TV (watch.shout-tv.com): Shout! Studios' free channels: Shout! TV,
MST3K, Johnny Carson TV, The Carol Burnett Show, ALF, Farscape and The Bold
and the Beautiful. No account.

Shout! TV runs on DICE (realm dce.shout), like NWSL+ 24/7 (nwsl.py) and with
the same shared web x-api-key: a guest token from /v2/login/guest/checkin,
/v2/event/live?p=1&rpp=25 for the channel list (rpp=100 answers 400), and
/v5/manifest/event/<id> for the stream (Amagi or Frequency, clear, 1080p, no
token in the URL). Each event's programmingInfo.channelId is its guide
channel for /v4/epg/content/programmes?channelId=<id>&from&to (up to 7 days
per call).

Two channels, Scream Factory TV and Hong Kong Fight Club, are
GRANTED_ON_SIGN_IN (a free Shout! account); guests get no stream for them, so
they're left out until there's a sign-in. Checked 2026-11-10.
"""
from __future__ import annotations

import html
import threading
import time
from datetime import datetime, timedelta, timezone

from ._common import Network, NetChannel, parse_iso, program

_API = 'https://dce-frontoffice.imggaming.com/api'
_SITE = 'https://watch.shout-tv.com'
_HEADERS = {'x-api-key': '857a1e5d-e35e-4fdf-805b-a87b6f8364bf', 'Realm': 'dce.shout', 'app': 'dice',
            'x-app-var': '6.60.0', 'Origin': _SITE, 'Referer': _SITE + '/',
            'Content-Type': 'application/json', 'Accept': 'application/json'}
_GUIDE_DAYS = 3
_TOKEN_MAX_AGE = 3600
_CATEGORIES = {'mst3k': 'Comedy', 'alf': 'Classic TV', 'farscape': 'Sci-Fi', 'the carol burnett show': 'Classic TV',
               'johnny carson tv': 'Classic TV', 'shout! tv': 'Classic TV', 'the bold & the beautiful': 'Drama'}

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
            raise RuntimeError('Shout! TV guest check-in returned no token')
        _token = (token, time.time())
        return token


def _get(s, path: str, params: dict | None = None) -> dict:
    for fresh in (False, True):
        r = s.session.get(f'{_API}{path}', params=params, timeout=30,
                          headers={**_HEADERS, 'Authorization': f'Bearer {_guest_token(s, fresh)}'})
        if r.status_code == 401 and not fresh:
            continue
        r.raise_for_status()
        return r.json() or {}
    raise RuntimeError(f'Shout! TV refused the guest token for {path}')


class Shout(Network):
    key = 'shout'
    name = 'Shout! TV'

    def channels(self, s):
        out = []
        for ev in _get(s, '/v2/event/live', {'p': 1, 'rpp': 25}).get('events') or []:
            if ev.get('accessLevel') != 'GRANTED' or not ev.get('id'):
                continue  # GRANTED_ON_SIGN_IN needs a Shout! account
            name = html.unescape(ev.get('title') or '').strip()
            guide = (ev.get('programmingInfo') or {}).get('channelId')
            if not name:
                continue
            # The guide channel rides along in the ref: "<event id>:<guide channel id>".
            out.append(NetChannel(str(ev['id']), name, f'{ev["id"]}:{guide or ""}',
                                  logo=ev.get('thumbnailUrl') or None,
                                  category=_CATEGORIES.get(name.lower(), 'Entertainment')))
        return out

    def programs(self, s, channels):
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        stamp = lambda d: d.strftime('%Y-%m-%dT%H:%M:%S.000Z')  # noqa: E731
        out = []
        for ch in channels:
            guide = ch.stream.partition(':')[2]
            if not guide:
                continue
            body = _get(s, '/v4/epg/content/programmes', {
                'channelId': guide, 'from': stamp(now - timedelta(hours=3)),
                'to': stamp(now + timedelta(days=_GUIDE_DAYS))})
            for row in (body.get('channels') or {}).get(guide) or []:
                show = html.unescape(row.get('title') or '').strip() or ch.name
                episode = html.unescape(row.get('episode') or '').strip()
                # "MST3K: The Human Duplicators", "The Bold And The Beautiful: Episode 230"
                head, sep, rest = episode.partition(': ')
                if sep and head.lower().replace(' and ', ' & ') == show.lower().replace(' and ', ' & '):
                    episode = rest.strip()
                if episode.lower() == show.lower():
                    episode = ''
                desc = next((html.unescape(row[k]).strip() for k in ('description', 'longDescription',
                             'shortDescription', 'synopsis') if isinstance(row.get(k), str) and row[k].strip()), None)
                rating = (row.get('contentRating') or {}).get('rating')
                p = program(ch.id, show, parse_iso(row.get('startDate')), parse_iso(row.get('endDate')),
                            episode_title=episode or None, description=desc,
                            poster_url=row.get('thumbnailUrl') or None, category=ch.category,
                            rating=rating or None)
                if p:
                    out.append(p)
        return out

    def resolve(self, s, ref):
        event = ref.partition(':')[0]
        for fresh in (False, True):
            r = s.session.post(f'{_API}/v5/manifest/event/{event}', timeout=20,
                               params={'includePlaybackDetails': 'URL', 'displayGeoblocked': 'HIDE'},
                               headers={**_HEADERS, 'Authorization': f'Bearer {_guest_token(s, fresh)}'},
                               json={'mediaCapabilities': [{'protocols': ['HLS'], 'audioCodecs': ['aac'],
                                                            'videoCodecs': ['h264']}]})
            if r.status_code == 401 and not fresh:
                continue
            r.raise_for_status()
            body = r.json() or {}
            url = next((st['url'] for st in body.get('streams') or []
                        if st.get('url') and (st.get('protocol') or 'HLS').upper() == 'HLS'), None)
            if not url:
                raise RuntimeError(f'Shout! TV gave no HLS stream (access {body.get("accessLevel")})')
            return url
        raise RuntimeError('Shout! TV refused the guest token')

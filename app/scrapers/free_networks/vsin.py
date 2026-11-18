"""VSiN (Vegas Stats & Information Network), free 24/7 channel.

The VSIN Watch web app (video.vsin.com) is a MAZ app: it POSTs its public
app id and API key (shipped in video.vsin.com/configuration.js) to
api.maz.tv/policy, which returns a CloudFront signature for the app's feed
on cloud.maz.tv. The feed's "Guide" menu holds a LiveChannel whose
contentArray is the schedule (~10 days, every show with a summary). Its
contentUrl for every programme is the same fixed Zype linear URL, so the
stream stays static and there's nothing to resolve at play time. No login.

Gracenote 108970 is VSiN's streaming station; tvtv's slots match the MAZ
guide (2026-11-18) but have no descriptions and ~2 days of coverage, so
tvtv only fills holes in this guide.
"""
from __future__ import annotations

from datetime import datetime

from ._common import UTC, Network, NetChannel, program

_APP_ID = 733
_API_KEY = 'de42517043d93d091c9cef33acc78e56'
_ORIGIN = 'https://video.vsin.com'
_CHANNEL = NetChannel(
    'vsin', 'VSiN',
    'https://na.linear.zype.com/987593e6-5894-4f92-bf21-7282da9084f4/'
    '2514774e-d092-4253-b02d-c2bf3127c934-hls4/live.m3u8',
    logo='https://assets.maz.tv/yatso91lgu4fy5buhuv2aqobunw7.png', category='Sports',
    gracenote_id='108970',
    description='The Vegas Stats & Information Network: sports betting news and analysis.')


def _when(val) -> datetime | None:
    # "2026-11-17 04:00:00 UTC"
    try:
        return datetime.strptime(str(val), '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=UTC)
    except (TypeError, ValueError):
        return None


def _live_channels(node):
    if isinstance(node, dict):
        if node.get('catalog') == 'LiveChannel':
            yield node
        for v in node.values():
            yield from _live_channels(v)
    elif isinstance(node, list):
        for v in node:
            yield from _live_channels(v)


class VSiN(Network):
    key = 'vsin'
    name = 'VSiN'

    def channels(self, s):
        return [_CHANNEL]

    def programs(self, s, channels):
        if not any(ch.id == 'vsin' for ch in channels):
            return []
        headers = {'Origin': _ORIGIN, 'Referer': f'{_ORIGIN}/', 'Accept': 'application/json'}
        r = s.session.post('https://api.maz.tv/policy', timeout=20, headers=headers,
                           json={'app_id': _APP_ID, 'key': _API_KEY, 'language': 'en'})
        policy = r.json() if r is not None and r.ok else {}
        if not policy.get('locale_id') or not policy.get('signature'):
            return None
        r = s.get(f"https://cloud.maz.tv/{_APP_ID}/{policy['locale_id']}/en/feeds/v2/tv_app_feed"
                  f"{policy['signature']}", headers=headers)
        if not r:
            return None
        lives = list(_live_channels(r.json()))
        live = next((c for c in lives if c.get('slug') == 'vsin'), lives[0] if lives else None)
        if not live:
            return None
        out = []
        for row in live.get('contentArray') or []:
            p = program('vsin', row.get('title'), _when(row.get('epgStartTime')), _when(row.get('epgEndTime')),
                        description=row.get('summary'))
            if p:
                out.append(p)
        return out or None

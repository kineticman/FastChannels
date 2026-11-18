"""Olympic Channel TV (OCTV), olympics.com's 24/7 linear channel.

No login. The master (ott-dai-oc.akamaized.net/OC1/master.m3u8, Google DAI
on Akamai, clear AES-128, 1080p25) needs an hdnts token from olympics.com's
tokenGenerator; the site sits behind Akamai Bot Manager, so the token and guide
calls go through the Chrome-fingerprinted session. The token lasts ~5 minutes
and only gates the first request: the master 302s to a session URL (hdnea, then
hdntl on the variants) that carries on from there. The site's sign-up wall is
enforced in its own player only.

Akamai refuses ffmpeg's default User-Agent (Lavf/...) at every level, and the
session is bound to the UA that opened it, so a client must fetch the whole
chain with one non-Lavf UA. Browsers, Channels DVR, FastChannels Player, VLC and
ffmpeg with -user_agent all play; bare ffmpeg does not.

Guide: /api/v1/live/video/<date>/octv/epglist, one call per day (each returns
roughly the day before through the day after, UTC, with descriptions and art);
about 2-3 days ahead.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from ._common import UTC, Network, NetChannel, logger, parse_iso, program

_SITE = 'https://www.olympics.com'
_MASTER = 'https://ott-dai-oc.akamaized.net/OC1/master.m3u8'
_TOKEN = (f'{_SITE}/tokenGenerator?url={_MASTER}&domain=https://ott-dai-oc.akamaized.net')
_EPG = _SITE + '/en/api/v1/live/video/{}/octv/epglist?channelid=OCTV'
_HEADERS = {'Referer': _SITE + '/en/live/'}
_IMG = 'https://img.olympics.com/images/image/private/{}/primary/{}'
_CHANNEL = NetChannel(
    'octv', 'Olympic Channel', 'octv',
    logo=_IMG.format('t_1-1_320/f_auto', 'suxpkzf7zbrw8nrjbst3'),
    category='Sports',
    description='The Olympic Channel\'s 24/7 stream: replays of Olympic events, documentaries and athlete stories.',
)


class Olympics(Network):
    key = 'olympics'
    name = 'Olympic Channel'

    def channels(self, s):
        return [_CHANNEL]

    def resolve(self, s, ref: str) -> str:
        r = s.impersonated().get(_TOKEN, headers=_HEADERS, timeout=15)
        url = r.json() if r is not None and r.status_code == 200 else None
        if not isinstance(url, str) or not url.startswith(_MASTER):
            raise RuntimeError(f'Olympic Channel token request failed ({getattr(r, "status_code", "no response")})')
        return url

    def programs(self, s, channels):
        if not channels:
            return []
        today = datetime.now(UTC).date()
        rows = {}
        fetched = False
        for d in range(4):
            r = s.impersonated().get(_EPG.format(today + timedelta(days=d)), headers=_HEADERS, timeout=20)
            if r is None or r.status_code != 200:
                continue
            try:
                items = r.json()['modules'][0]['content']
            except (ValueError, KeyError, IndexError, TypeError):
                continue
            fetched = True
            if not items:
                break
            for it in items:
                rows[it.get('startTime')] = it
        if not fetched:
            logger.warning('[free_networks] Olympic Channel: no guide day answered')
            return None
        out = []
        for it in rows.values():
            thumb = it.get('thumb')
            p = program('octv', it.get('title'), parse_iso(it.get('startTime')), parse_iso(it.get('endTime')),
                        description=it.get('description'), category='Sports',
                        poster_url=thumb.replace('{formatInstructions}', 't_16-9_1280/f_auto') if thumb else None)
            if p:
                out.append(p)
        return out

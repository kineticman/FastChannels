"""SHIFT TV (Croatian motorsport channel) and its four BEATMAG music channels.

ott.shift.tv/livetv plays SHIFT TV from a fixed Nimble URL
(hls.shift.tv/live/shifttv/playlist.m3u8, clear 1080p) and lists the BEATMAG
channels, whose watch pages give their own fixed hls.beatmag.tv URLs
base64-encoded in the player config (720p for 1, 1080p for 2-4). No login.
BeOND also carries SHIFT TV, through an Amagi playout.

Guide: only SHIFT TV has one, the "Schedule" list on /livetv (~12 hours of
start times as data-ts epochs; each show ends at the next). The BEATMAG
channels get placeholder blocks.
"""
from __future__ import annotations

import html
import re

from ._common import Network, NetChannel, chain_ends, from_epoch, program

_LIVETV = 'https://ott.shift.tv/livetv'
_LOGO = 'https://ott.shift.tv/upload/TV%20CHANNEL%20LOGO/{}.jpg'
_CHANNELS = [
    NetChannel('shifttv', 'SHIFT TV', 'https://hls.shift.tv/live/shifttv/playlist.m3u8',
               logo='https://ott.shift.tv/upload/app-images/shift-tv-web-logo.png', category='Automotive',
               language='hr', country='HR',
               description='Motorsport, car culture and tuning shows from Croatia.'),
    NetChannel('beatmag1', 'BEATMAG TV 1', 'https://hls.beatmag.tv/live/beatmagch1_abr/playlist.m3u8',
               logo=_LOGO.format('BeatmagTV1'), category='Music', country='HR'),
    NetChannel('beatmag2', 'BEATMAG TV 2', 'https://hls.beatmag.tv/live/beatmagch2/playlist.m3u8',
               logo=_LOGO.format('BeatmagCH1'), category='Music', country='HR'),
    NetChannel('beatmag3', 'BEATMAG TV 3', 'https://hls.beatmag.tv/live/beatmagch3/playlist.m3u8',
               logo=_LOGO.format('BeatmagCH2'), category='Music', country='HR'),
    NetChannel('beatmag4', 'BEATMAG TV 4', 'https://hls.beatmag.tv/live/beatmagch4/playlist.m3u8',
               logo=_LOGO.format('BeatmagCH3'), category='Music', country='HR'),
]
_ROW_RE = re.compile(r'<li[^>]*>\s*<time data-ts="(\d+)">.*?<b>([^<]+)</b>'
                     r'(?:<span class="v3-epg-sub">\s*[–-]?\s*([^<]*)</span>)?', re.S)


class Shift(Network):
    key = 'shift'
    name = 'SHIFT TV'
    # hls.beatmag.tv answers CORS for ott.shift.tv only; hls.shift.tv sends *.
    browser_relay_hosts = ('hls.beatmag.tv',)

    def channels(self, s):
        return list(_CHANNELS)

    def has_guide(self, channel_id: str) -> bool:
        return channel_id == 'shifttv'

    def programs(self, s, channels):
        if not any(ch.id == 'shifttv' for ch in channels):
            return []
        r = s.get(_LIVETV)
        if not r:
            return None
        block = r.text.split('v3-epg-list', 1)[-1].split('</ol>', 1)[0]
        rows = []
        for ts, title, sub in _ROW_RE.findall(block):
            start = from_epoch(ts)
            if start:
                rows.append((start, {'title': html.unescape(title), 'sub': html.unescape(sub or '').strip()}))
        out = []
        for start, end, row in chain_ends(rows, max_gap_hours=6):
            p = program('shifttv', row['title'], start, end,
                        episode_title=row['sub'] if row['sub'] and row['sub'] != row['title'] else None)
            if p:
                out.append(p)
        return out

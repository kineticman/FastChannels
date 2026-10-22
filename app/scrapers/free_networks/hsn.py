"""HSN and HSN2.

Streams: QVC Group's Akamai live (lshsn1us / lshsn2us). Guide: the
server-rendered hsn.com/watch/program-guide?date=MM/DD/YYYY&network=N table
(network 1 = HSN, 4 = HSN2), one hourly-ish row per show with
data-startdate in Eastern time; hosts and guests make the description. Rows
only carry a start, so each ends at the next.
"""
from __future__ import annotations

import html
import re
from datetime import datetime, timedelta

from ._common import ET, Network, NetChannel, chain_ends, program

_GUIDE = 'https://www.hsn.com/watch/program-guide'
_STREAM = 'https://qvc-amd-live.akamaized.net/hls/live/2034113/lshsn{}us/master.m3u8'
_LOGO = 'https://img.hsni.com/images/prod/2014/images/iosHSNFlatWhite_120.jpg'
_CHANNELS = [('hsn', 'HSN', 1, 1, None), ('hsn2', 'HSN2', 2, 4, None)]  # id, name, stream n, guide network, gracenote
_DAYS = 7

_ROW_RE = re.compile(r'<tr>(.*?)</tr>', re.S)
_SHOW_RE = re.compile(r'class="show-details"[^>]*data-startdate="([^"]+)"[^>]*>([^<]+)<')
_HOST_RE = re.compile(r'<a class="show-host"[^>]*>([^<]+)</a>')
_GUEST_RE = re.compile(r'<div class="show-guest">([^<]+)</div>')


class HSN(Network):
    key = 'hsn'
    name = 'HSN'

    def channels(self, s):
        return [NetChannel(cid, name, _STREAM.format(n), logo=_LOGO, category='Shopping', gracenote_id=gn)
                for cid, name, n, _, gn in _CHANNELS]

    def programs(self, s, channels):
        wanted = {ch.id for ch in channels}
        today = datetime.now(ET).date()
        out = []
        for cid, _, _, network, _ in _CHANNELS:
            if cid not in wanted:
                continue
            rows = {}
            for i in range(_DAYS):
                day = today + timedelta(days=i)
                r = s.get(_GUIDE, params={'date': day.strftime('%m/%d/%Y'), 'network': network})
                for block in _ROW_RE.findall(r.text) if r else []:
                    m = _SHOW_RE.search(block)
                    if not m:
                        continue
                    try:
                        start = datetime.strptime(m.group(1), '%m/%d/%Y %I:%M:%S %p').replace(tzinfo=ET)
                    except ValueError:
                        continue
                    hosts = [html.unescape(h).strip() for h in _HOST_RE.findall(block)]
                    guests = [html.unescape(g).strip() for g in _GUEST_RE.findall(block)]
                    desc = '; '.join(filter(None, [
                        f"Host: {', '.join(hosts)}" if hosts else '',
                        f"Guest: {', '.join(guests)}" if guests else '',
                    ]))
                    rows[start] = {'title': html.unescape(m.group(2)), 'desc': desc or None}
            for start, end, row in chain_ends(list(rows.items()), max_gap_hours=3):
                p = program(cid, row['title'], start, end, description=row['desc'])
                if p:
                    out.append(p)
        return out

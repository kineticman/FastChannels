"""Sinclair's free diginets: Comet, Charge!, TheNest, ROAR.

Each has its own WordPress site on the same theme. The watch-live page embeds
clear HLS at fast-channels.sinclairstoryline.com/<CODE>/index.m3u8; the code is
re-read each scrape, and a network whose page shows the "TBD" placeholder
(ROAR as of 2026-10-17) is skipped until it has a real stream.

Guide: /wp-json/comet/v1/fetch-all-schedule-days?days=YYYY-MM-DD,... returns
{"html": {day: rendered rows}}. Times read "8 PM Eastern Pacific"; the stream
follows the Eastern clock (checked 2026-10-17 against live captions). Each day
is a broadcast day, so after-midnight rows belong to the next date, and slots
already aired today are left out. Rows carry only a start time.
"""
from __future__ import annotations

import html
import re
from datetime import datetime, timedelta

from ._common import ET, Network, NetChannel, chain_ends, logger, program

_STREAM = 'https://fast-channels.sinclairstoryline.com/{code}/index.m3u8'
_CODE_RE = re.compile(r'fast-channels\.sinclairstoryline\.com/([A-Za-z0-9_-]+)/index\.m3u8')
_PLACEHOLDERS = {'TBD'}

_ROW_SPLIT = '<div class="row single-show'
_TIME_RE = re.compile(r'<div>\s*(\d{1,2})(?::(\d{2}))?\s*([AP]M)\s+Eastern', re.I)
_TITLE_RE = re.compile(r'<h2 class="title">\s*(?:<a[^>]*>)?\s*([^<]+?)\s*<', re.S)
_EPISODE_RE = re.compile(r'class="episode-title">\s*(?:<a[^>]*>)?\s*([^<]+?)\s*<', re.S)
_DESC_RE = re.compile(r'class="description">\s*(.*?)\s*</div>', re.S)
_IMG_RE = re.compile(r'<img[^>]*\bsrc="([^"]+)"')

# "code" is the fallback when the watch-live page can't be read.
_NETS = [
    ('comet', 'Comet', 'https://comettv.com', 'COMET', 'Sci-Fi',
     'https://d17su2xjlj6zyp.cloudfront.net/uploads/2021/06/cropped-comet-logo-square-270x270.png'),
    ('charge', 'Charge!', 'https://watchcharge.com', 'CHARGE', 'Action & Adventure',
     'https://d12lhx8qhim5rg.cloudfront.net/uploads/2021/07/Charge_Yellow-1.png'),
    ('thenest', 'TheNest', 'https://watchthenest.com', 'THENEST', 'Reality TV',
     'https://d3etz0zhgardfq.cloudfront.net/uploads/2023/10/06.The_Nest_Logo_White.png'),
    ('roar', 'ROAR', 'https://watchroar.com', None, 'Comedy',
     'https://dsk4yqs0338lt.cloudfront.net/uploads/2025/04/ROAR_WHITE-scaled.png'),
]
_SITES = {n[0]: n[2] for n in _NETS}


class Sinclair(Network):
    key = 'sinclair'
    name = 'Sinclair (Comet, Charge!, TheNest)'

    def channels(self, s):
        out = []
        for cid, name, site, fallback, category, logo in _NETS:
            code = self._live_code(s, name, site, fallback)
            if code:
                out.append(NetChannel(cid, name, code, logo=logo, category=category))
        return out

    def resolve(self, s, ref):
        return _STREAM.format(code=ref)

    def programs(self, s, channels):
        today = datetime.now(ET).date()
        # Yesterday's broadcast day still covers the hours just after midnight.
        days = [(today + timedelta(days=i)).isoformat() for i in range(-1, 7)]
        out = []
        for ch in channels:
            site = _SITES.get(ch.id)
            if not site:
                continue
            r = s.get(site + '/wp-json/comet/v1/fetch-all-schedule-days',
                      params={'days': ','.join(days), 'timezone': 'America/New_York'})
            data = r.json().get('html') if r else None
            if not isinstance(data, dict):  # empty comes back as a list
                continue
            for start, end, row in chain_ends(self._parse_days(data)):
                p = program(ch.id, row['title'], start, end, episode_title=row['episode'],
                            description=row['desc'], poster_url=row['img'])
                if p:
                    out.append(p)
        return out

    @staticmethod
    def _live_code(s, name, site, fallback):
        r = s.get(site + '/watch-live/')
        if not r:
            return fallback
        # Charge's page also has a commented-out uplynk URL; the first
        # sinclairstoryline match is the live one on every page so far.
        codes = [c for c in _CODE_RE.findall(r.text) if c not in _PLACEHOLDERS]
        if codes:
            if fallback and codes[0] != fallback:
                logger.warning('[free_networks] %s stream code changed upstream: %s -> %s',
                               name, fallback, codes[0])
            return codes[0]
        if _CODE_RE.search(r.text):
            logger.info('[free_networks] %s has no live stream yet (placeholder), skipping', name)
            return None
        return fallback

    @staticmethod
    def _parse_days(data):
        rows = {}
        for day, body in data.items():
            try:
                base = datetime.strptime(day, '%Y-%m-%d').date()
            except ValueError:
                continue
            prev = None
            for block in (body or '').split(_ROW_SPLIT)[1:]:
                m, t = _TIME_RE.search(block), _TITLE_RE.search(block)
                if not m or not t:
                    continue
                hour = int(m.group(1)) % 12 + (12 if m.group(3).upper() == 'PM' else 0)
                start = datetime(base.year, base.month, base.day, hour, int(m.group(2) or 0), tzinfo=ET)
                while prev and start < prev:  # rows run past midnight
                    start += timedelta(days=1)
                prev = start
                ep, desc, img = _EPISODE_RE.search(block), _DESC_RE.search(block), _IMG_RE.search(block)
                rows[start] = {
                    'title': html.unescape(t.group(1)),
                    'episode': html.unescape(ep.group(1)) if ep else None,
                    'desc': desc.group(1) if desc else None,
                    'img': img.group(1) if img else None,
                }
        return list(rows.items())

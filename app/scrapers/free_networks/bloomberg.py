"""Bloomberg TV (US, BTV+ US, Europe, Asia, Australia, Originals) plus the live
event, Politics, QuickTake and Radio feeds.

Streams play from plain requests: Google DAI events for the TV feeds, and
www.bloomberg.com/media-manifest/streams/<name>.m3u8 for the rest. The event
IDs were matched to /live/<page> from browser HARs (2026-10-17).

Guide: GET www.bloomberg.com/live/schedule/<region> with header RSC: 1 returns
a Next.js RSC payload of LiveSchedule objects (~a week, UTC). All of
bloomberg.com/live* is behind PerimeterX: it passes with a curl_cffi Chrome
fingerprint, but plain requests get a 403 and a burst of them seems to earn
the IP a temporary block. So: impersonated requests only, at most once a day
(sooner only if there's no stored guide left to keep, e.g. after the channels
were deleted), and on any failure the existing guide is simply kept (programs
are replaced per channel, so returning nothing leaves it in place).

Radio: the radio-national-video stream is the national radio simulcast (its
bug rotates through the NY, Boston, DC and SiriusXM stations). Its guide is
www.bloombergradio.com/schedule/'s WBBR grid: plain HTML, not behind
PerimeterX, a repeating Sunday-Friday week in ET (the dates on the page are
stale; Saturday isn't listed and gets a plain "Bloomberg Radio" block). Spot
checked 2026-10-19 against one weekday frame only, so it's best effort.
"""
from __future__ import annotations

import html
import json
import re
import time
from datetime import datetime, timedelta, timezone

from ._common import ET, Network, NetChannel, logger, parse_iso, program

_DAI = 'https://dai.google.com/linear/hls/event/{}/master.m3u8'
_MANIFEST = 'https://www.bloomberg.com/media-manifest/streams/{}.m3u8'
_SCHEDULE = 'https://www.bloomberg.com/live/schedule/{}'
# Gracenote's logos; bloomberg.com only has a favicon.
_TMS_LOGO = 'https://tmsimg.fancybits.co/assets/{}.png?w=360&h=270'
_LOGO = _TMS_LOGO.format('s53168_ll_h15_ad')  # "Bloomberg"
_LOGOS = {
    'us': _TMS_LOGO.format('s71799_ll_h15_aa'),  # "Bloomberg TV"
    'us-plus': _TMS_LOGO.format('s175659_ll_h15_aa'),  # "Bloomberg TV+"
    'europe': _TMS_LOGO.format('s14755_ll_h15_ab'),  # "Bloomberg Television"
    'asia': _TMS_LOGO.format('s14755_ll_h15_ab'),
    'australia': _TMS_LOGO.format('s14755_ll_h15_ab'),
    'originals': _TMS_LOGO.format('GNLZZGG003PQG7H'),
}
_RADIO_SCHEDULE = 'https://www.bloombergradio.com/schedule/'
_RADIO_BLOCK_RE = re.compile(
    r'class="show-block[^"]*day_(\w+) station_WBBR_RADIO">\s*<div class="col-md-2">(\d\d):(\d\d)</div>'
    r'.*?<h2>(.*?)</h2>(?:\s*<p class="textbox">(.*?)</p>)?', re.S)
_WEEKDAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# id, name, stream, schedule region
_CHANNELS = [
    ('us', 'Bloomberg TV US', _DAI.format('6JVA-Zs8Rbuj0qoA5Gso1Q'), 'us-btv'),
    ('us-plus', 'Bloomberg TV+ US', _DAI.format('k-VHR5unRdusBDqoXAuB0Q'), 'us'),
    ('europe', 'Bloomberg TV Europe', _DAI.format('TE-eb6hOS1GBMm6Ofp1r3g'), 'europe'),
    ('asia', 'Bloomberg TV Asia', _DAI.format('FvrjI2MQSs6_bNTURP2CaA'), 'asia'),
    ('australia', 'Bloomberg TV Australia', _DAI.format('eqieA1HDSue0bSrHM5Rj6w'), 'australia'),
    ('originals', 'Bloomberg Originals', _DAI.format('jKwfd4apQcuxQtyRI9Q6-Q'), 'originals'),
    ('us-event', 'Bloomberg U.S. Live Event', _MANIFEST.format('us-event'), None),
    ('asia-event', 'Bloomberg Asia Live Event', _MANIFEST.format('asia-event'), None),
    ('emea-event', 'Bloomberg EMEA Live Event', _MANIFEST.format('eu-event'), None),
    ('politics', 'Bloomberg Politics', _MANIFEST.format('politics'), None),
    ('quicktake', 'Bloomberg QuickTake', _MANIFEST.format('qt'), None),
    ('radio', 'Bloomberg Radio', _MANIFEST.format('radio-national-video'), None),
]
_GUIDED = {cid for cid, _, _, region in _CHANNELS if region} | {'radio'}
_CACHE_KEY = 'bloomberg_schedule_fetched_at'
_REFRESH_SECONDS = 20 * 3600


class Bloomberg(Network):
    key = 'bloomberg'
    name = 'Bloomberg'

    def channels(self, s):
        return [NetChannel(cid, name, url, logo=_LOGOS.get(cid, _LOGO), category='News') for cid, name, url, _ in _CHANNELS]

    def has_guide(self, channel_id):
        return channel_id in _GUIDED

    def programs(self, s, channels):
        wanted = {ch.id for ch in channels}
        last = float(s.cache.get(_CACHE_KEY) or 0)
        if time.time() - last < _REFRESH_SECONDS:
            # Fetched recently: keep what's stored, as long as there is a stored
            # guide that still runs a day ahead. Its channels may have been
            # deleted since (source or network switched off and on again).
            stored_end = s.stored_guide_end(self.key, wanted & _GUIDED)
            if stored_end and stored_end > datetime.now(timezone.utc) + timedelta(days=1):
                return None
            logger.info('[free_networks] Bloomberg: no stored guide to keep, fetching despite the daily limit')
        out, ok = [], 0
        for cid, _, _, region in _CHANNELS:
            if not region or cid not in wanted:
                continue
            try:
                r = s.impersonated().get(_SCHEDULE.format(region), headers={'RSC': '1'}, timeout=20)
            except Exception as exc:
                logger.warning('[free_networks] Bloomberg schedule %s failed: %s', region, exc)
                continue
            if r.status_code != 200:
                logger.warning('[free_networks] Bloomberg schedule %s: HTTP %s (bot check?) — keeping existing guide',
                               region, r.status_code)
                break  # don't keep knocking after a block
            ok += 1
            out.extend(self._parse(cid, r.text))
        if 'radio' in wanted:
            radio = self._radio(s)
            ok += bool(radio)
            out.extend(radio)
        if not ok:
            return None
        s._update_cache(_CACHE_KEY, time.time())
        return out

    @staticmethod
    def _radio(s):
        """A week of the WBBR grid from today (ET); [] if the page can't be read."""
        try:
            r = s.session.get(_RADIO_SCHEDULE, timeout=20)
        except Exception as exc:
            logger.warning('[free_networks] Bloomberg Radio schedule failed: %s', exc)
            return []
        week = {}
        for day, hh, mm, title, desc in _RADIO_BLOCK_RE.findall(r.text if r.ok else ''):
            week.setdefault(day, []).append((int(hh), int(mm), html.unescape(title).strip(),
                                             html.unescape(desc or '').strip() or None))
        if not week:
            logger.warning('[free_networks] Bloomberg Radio schedule: no WBBR grid (HTTP %s)', r.status_code)
            return []
        today = datetime.now(ET).date()
        slots = []
        for offset in range(-1, 8):
            day = today + timedelta(days=offset)
            midnight = datetime(day.year, day.month, day.day, tzinfo=ET)
            rows = week.get(_WEEKDAYS[day.weekday()]) or [(0, 0, 'Bloomberg Radio', None)]
            for hh, mm, title, desc in rows:
                slots.append((midnight.replace(hour=hh, minute=mm), title, desc))
        slots.sort(key=lambda x: x[0])
        out = []
        for (start, title, desc), (end, _, _) in zip(slots, slots[1:]):
            p = program('radio', title, start, end, description=desc)
            if p:
                out.append(p)
        return out

    @staticmethod
    def _parse(cid, text):
        out, seen = [], set()
        dec = json.JSONDecoder()
        i = text.find('{"showInfo":')
        while i != -1:
            try:
                obj, end = dec.raw_decode(text, i)
            except ValueError:
                end = i + 1
                obj = None
            if isinstance(obj, dict):
                ep = obj.get('episodeInfo') or {}
                show = obj.get('showInfo') or {}
                start = parse_iso(ep.get('episodeStartTime'))
                if start and start not in seen:
                    seen.add(start)
                    title = show.get('showTitle') or ep.get('episodeTitle')
                    p = program(cid, title, start, parse_iso(ep.get('episodeEndTime')),
                                episode_title=ep.get('episodeTitle') if ep.get('episodeTitle') != title else None,
                                description=ep.get('episodeDescription'),
                                poster_url=(ep.get('thumbnail') or {}).get('url'))
                    if p:
                        out.append(p)
            i = text.find('{"showInfo":', end)
        return out

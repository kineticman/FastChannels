"""sports.tv: Local Now's free sports site, plus a batch of local news stations.

No login. Its own catalog on Local Now's DSP platform (myspotlight.tv), not the
localnow.com lineup. Any /channels/<slug> page server-renders every channel in
__NEXT_DATA__ (props.pageProps.page.channels: name, slug, videoId, logo,
genres) with ~7 days of guide each (title, description, starts/ends epochs), so
one fetch gives the lineup and the whole guide.

Playback: api.myspotlight.tv/dotplayer/video/<company>/<videoId> answers with
the channel's live block: a MediaTailor endpoint (the ad-inserted stream the
site plays) and the raw origin behind it (Amagi, noisypeak, fuelmedia). Both are
clear HLS with CORS for any Origin. We play the origin: in a probe of all 65
channels (2026-11-18) it played every live one, while MediaTailor's playlists
on about ten (the Montana/Washington stations, KHQ, Fight Network, Cowboy+
Sports) kept listing segments its CDN had already dropped, scattered through the
playlist and changing minute to minute, so a spot check can't catch them.
MediaTailor is the fallback when a channel has no origin URL.
"""
from __future__ import annotations

import json
import re
import threading
import time

from ._common import UA, Network, NetChannel, from_epoch, logger, program

_SITE = 'https://www.sports.tv'
_PAGE = _SITE + '/channels/epg-hbcu-go'
_COMPANY = '5ef4a47fc27512184b7d2b62'
_PLAYER = ('https://api.myspotlight.tv/dotplayer/video/' + _COMPANY +
           '/{}?width=1920&height=1080&page_url=https%3A%2F%2Fwww.sports.tv%2F')
_HEADERS = {'Origin': _SITE, 'Referer': _SITE + '/', 'User-Agent': UA}
_NEXT_RE = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)
_CALL_SIGN_RE = re.compile(r'^(?:[KW][A-Z]{2,3}\b|\d+&\d+ News)')
_PAGE_TTL = 900          # channels() and programs() share one page fetch per run

_page_cache: tuple[float, list] | None = None
_page_lock = threading.Lock()


def _channels_payload(s) -> list | None:
    global _page_cache
    with _page_lock:
        if _page_cache and time.monotonic() - _page_cache[0] < _PAGE_TTL:
            return _page_cache[1]
        r = s.get(_PAGE)
        if not r:
            return None
        m = _NEXT_RE.search(r.text)
        try:
            chans = json.loads(m.group(1))['props']['pageProps']['page']['channels'] if m else None
        except (ValueError, KeyError, TypeError):
            chans = None
        if not isinstance(chans, list):
            logger.warning('[free_networks] sports.tv: no channel list in the page')
            return None
        _page_cache = (time.monotonic(), chans)
        return chans


def _category(ch: dict) -> str:
    if _CALL_SIGN_RE.match(ch.get('name') or '') or 'News' in (ch.get('genres') or [])[:1]:
        return 'Local News'
    return 'Sports'



class SportsTV(Network):
    key = 'sportstv'
    name = 'sports.tv'

    def channels(self, s):
        chans = _channels_payload(s)
        if chans is None:
            return []
        out = []
        for ch in chans:
            vid, name = ch.get('videoId'), (ch.get('name') or '').strip()
            if not vid or not name:
                continue
            out.append(NetChannel(vid, name, vid, logo=ch.get('logo') or None, category=_category(ch)))
        return out

    def programs(self, s, channels):
        wanted = {ch.id for ch in channels}
        if not wanted:
            return []
        chans = _channels_payload(s)
        if chans is None:
            return None
        out = []
        for ch in chans:
            vid = ch.get('videoId')
            if vid not in wanted:
                continue
            for row in ch.get('program') or []:
                p = program(vid, row.get('title'), from_epoch(row.get('starts')), from_epoch(row.get('ends')),
                            description=row.get('description'))
                if p:
                    out.append(p)
        return out

    def resolve(self, s, ref: str) -> str:
        r = s.session.get(_PLAYER.format(ref), headers=_HEADERS, timeout=(5, 10))
        r.raise_for_status()
        live = (r.json().get('video') or {}).get('live') or {}
        raw = live.get('url')
        mt = (live['mediatailor_endpoint'] + (live.get('mediatailor_filename') or 'playlist.m3u8')
              if live.get('mediatailor_endpoint') else None)
        if raw or mt:
            return raw or mt
        raise RuntimeError(f'sports.tv {ref}: no live stream in the player response')

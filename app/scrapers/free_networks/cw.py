"""The CW's free channels (cwtv.com/channels): CW Forever, CW Gold, WWE NXT,
Penn & Teller plus the third-party FAST nets CW carries (ION, FilmRise,
Lionsgate, Shout! ...).

One public feed has everything: data.cwtv.com's EPG feed (the one cwtv.com's
fast_index.bundle.js loads) lists each channel with a direct Amagi HLS URL
(clear, no token) and ~24 h of programs. It's paged by channel_count /
page_size; all 75 fit on page 1 today, the rest are fetched if that changes.
"""
from __future__ import annotations

import time

from ._common import Network, NetChannel, from_epoch, logger, program

_FEED = ('https://data.cwtv.com/feed/app-2/landing/epg/page_{page}/pagesize_75/'
         'device_web/apiversion_24/cacheversion_{cache}')
# Carried in Free Networks already from the network's own feed.
_ELSEWHERE = {'court-tv', 'movie-favorites-by-lifetime', 'ae-crime-360'}
# Where CW's own label disagrees with the other sources carrying the channel.
_CATEGORY = {
    'the-asylum-movie-channel': 'Movies',
    'cw-untold-stories-of-the-er': 'True Crime',
    'americas-funniest-home-videos': 'Comedy',
    'bounce-xl': 'Entertainment',
    'jack-hanna': 'Nature',
}
# Gracenote's schedule for these is a stale loop that doesn't match the stream
# (ACC Digital Network 124806 lists 2022 lacrosse replays during live football),
# so they keep the ID for matching but get placeholders, not tvtv's guide.
_NO_TVTV = {'acc-digital-network'}


def _category(ch: dict) -> str | None:
    if ch.get('slug') in _CATEGORY:
        return _CATEGORY[ch['slug']]
    raw = (ch.get('content_category') or ch.get('genre') or '').split(',')[0].strip()
    return raw or None


def _is_tba(p: dict) -> bool:
    return (p.get('title') or '').strip().lower() == 'to be announced' or \
        str(p.get('asset_id') or '').endswith('_TBA')


class CW(Network):
    key = 'cw'
    name = 'The CW'
    # Slugs whose feed was all "To Be Announced" on the last guide fetch.
    _tba: set[str] = set()

    def has_guide(self, channel_id):
        return channel_id not in self._tba

    def borrows_tvtv(self, channel_id):
        return channel_id not in _NO_TVTV

    def _feed(self, s) -> list[dict]:
        def fetch():
            cache = int(time.time())
            channels, page, pages = [], 1, 1
            while page <= pages:
                r = s.get(_FEED.format(page=page, cache=cache))
                data = (r.json() or {}) if r else {}
                channels.extend(data.get('channels') or [])
                size, count = data.get('page_size') or 0, data.get('channel_count') or 0
                if page == 1 and size and count > size:
                    pages = -(-count // size)
                page += 1
            return channels
        return s.memo('cw_feed', fetch)

    def channels(self, s):
        out = []
        for ch in self._feed(s):
            slug, url = ch.get('slug'), ch.get('stream_url')
            if not slug or not url or slug in _ELSEWHERE:
                continue
            if ch.get('has_drm') or (ch.get('stream_type') or 'hls') != 'hls':
                logger.info('[free_networks] CW %s skipped: DRM or non-HLS', slug)
                continue
            out.append(NetChannel(slug, (ch.get('title') or slug).strip(), url,
                                  logo=ch.get('thumbnail_url') or ch.get('icon_unfocused_url'),
                                  category=_category(ch)))
        return out

    def programs(self, s, channels):
        wanted = {c.id for c in channels}
        tba, out = set(), []
        for ch in self._feed(s):
            slug = ch.get('slug')
            if slug not in wanted:
                continue
            rows = ch.get('programs') or []
            # A feed that's nothing but "To Be Announced" (ACC Digital Network) is
            # no guide; leave the channel uncovered so it gets named placeholders.
            if rows and all(_is_tba(p) for p in rows):
                tba.add(slug)
                continue
            for p in rows:
                prog = program(slug, p.get('title'), from_epoch(p.get('program_start_ts')),
                               from_epoch(p.get('program_end_ts')),
                               episode_title=p.get('subtitle'), description=p.get('description'),
                               category=p.get('category'), rating=p.get('ratings') or None,
                               poster_url=p.get('details_image_url') or None)
                if prog:
                    out.append(prog)
        type(self)._tba = tba
        return out

"""Mansa TV (mansa.com): Black-owned FAST lineup (The Black Effect, Maverick
Black Cinema, Stash TV, HBCU, Manifest TV, Stingray music channels, ...).

Runs on Kanku: POST prod.kankuapi.com/mansa/playlist/page lists the channels
with public HLS URLs (Wurl MediaTailor, Amagi, Ottera; clear, up to 1080p).
'VIRTUAL' channels are on-demand video playlists, not linear, and are left
out. The guide is the public epgsyncservice feed (~1 day), which keys rows by
its own channel ids, so it's matched to the list by channel name.
"""
from __future__ import annotations

import time

from ._common import Network, NetChannel, from_epoch, program

_PLAYLISTS = 'https://prod.kankuapi.com/mansa/playlist/page'
_EPG = 'https://prod.epgsyncservice.kankuapi.com/api/epgs'
_HEADERS = {'Origin': 'https://www.mansa.com', 'Referer': 'https://www.mansa.com/'}
_WARM_ATTEMPTS = 3
_WARM_DELAY_SECS = 1.5
# Carried in Free Networks already from The CW's feed.
_ELSEWHERE = {'electric now'}


class Mansa(Network):
    key = 'mansa'
    name = 'Mansa TV'

    def channels(self, s):
        r = s.session.post(_PLAYLISTS, json={'size': 200}, headers=_HEADERS, timeout=30)
        out = []
        for ch in ((r.json() or {}).get('channels') if r is not None and r.ok else None) or []:
            cid, title, url = ch.get('id'), (ch.get('title') or '').strip(), ch.get('streamUrl')
            if ch.get('channelType') != 'FAST' or not cid or not title or not url \
                    or title.lower() in _ELSEWHERE:
                continue
            # Some rows' tags hold a URL instead of a genre.
            tags = [t for t in ch.get('tags') or [] if t and not t.startswith('http')]
            out.append(NetChannel(str(cid), title, url,
                                  logo=ch.get('unSelectedStateImage') or ch.get('selectedStateImage') or None,
                                  category=tags[0] if tags else None))
        return out

    # Channels the last programs() run had no current rows for (some
    # schedules stopped updating long ago), so they get placeholder blocks.
    _unguided: frozenset[str] = frozenset()

    def has_guide(self, channel_id: str) -> bool:
        return channel_id not in self._unguided

    def resolve(self, s, ref):
        # Ottera's ad stitcher (the Stingray channels) answers a channel's
        # first request after it has sat idle with HTTP 400, then 200 a moment
        # later; the stream audit marked three Dead on that first 400. Warm the
        # channel before handing the URL over.
        if 'ottera.tv' not in ref:
            return ref
        for attempt in range(_WARM_ATTEMPTS):
            if attempt:
                time.sleep(_WARM_DELAY_SECS)
            try:
                if s.session.get(ref, timeout=15).status_code != 400:
                    break
            except Exception:
                break
        return ref

    def programs(self, s, channels):
        r = s.get(_EPG, headers=_HEADERS)
        by_name = {ch.name.lower(): ch.id for ch in channels}
        since = (time.time() - 3600) * 1000
        out = []
        for row in ((r.json() or {}).get('items') if r else None) or []:
            cid = by_name.get((row.get('channelName') or '').strip().lower())
            if not cid or (row.get('endTimeMillis') or 0) < since:
                continue
            images = row.get('images') or []
            p = program(cid, row.get('title'), from_epoch(row.get('startTimeMillis')),
                        from_epoch(row.get('endTimeMillis')), description=row.get('description'),
                        poster_url=images[0].get('path') if images else None)
            if p:
                out.append(p)
        if r:
            self._unguided = frozenset(ch.id for ch in channels) - {p.source_channel_id for p in out}
        return out

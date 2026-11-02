"""FreeTV (freetv.com): Olympusat's free Spanish-language channels (FreeTV
Acción, Banda, Clásico, Drama, Estelar, Familia, Hits, Live, Sureño, Terror,
Asia, Popcorn Central). All Spanish, including Popcorn Central's dubbed
English-language films (its guide is Spanish whatever language is asked for).

The site runs on OTTera (api-ott.freetv.com). Every call carries the web
app's static key (ottera-cs-auth plus ottera-referrer), the same in every
capture and without which the API answers 401 'Signature invalid':
- getobjects?object_type=linear_channel lists all 16 channels in one call.
  Four (Ultra Familia, Ultra Clásico, Ultra Film, Cine Mexicano) have
  required_permissions PR, the paid FreeTV Premium tier, whose player answers
  "You must upgrade to access this video" (code 599), so they're skipped.
- The stream is stream.ads.ottera.tv/playlist.m3u8 for the channel's
  network_id, a number that isn't on the channel object: it's in the
  embedded player the site loads (embeddedVideoPlayer?id=<video id>, the
  channel's current video, video://<channel id + 1> in the guide). It's read
  at channel fetch; the bare URL (content_livestream=1&network_id=N, no ad
  macros) plays clear HLS up to 1080p.
- getvideosegments?linear_channel_id=<id>&timestamp=<now>&max=N is the guide,
  from now (without timestamp it returns a stale page); ~3 weeks are
  available, so a few days' worth is asked for.
Checked 2026-11-02.
"""
from __future__ import annotations

import re
import time

from ._common import Network, NetChannel, from_epoch, logger, program

_API = 'https://api-ott.freetv.com/'
_HEADERS = {'ottera-cs-auth': '2Ecto78sUJlGg89jCgaz9F0fOWZ', 'ottera-referrer': 'freetv.com',
            'Origin': 'https://www.freetv.com', 'Referer': 'https://www.freetv.com/'}
_PARAMS = {'platform': 'web', 'device_type': 'desktop', 'partner': 'internal', 'language': 'es',
           'version': '15.0', 'connection': 'wifi'}
_STREAM = 'https://stream.ads.ottera.tv/playlist.m3u8?content_livestream=1&network_id={}'
_NETWORK_RE = re.compile(r"stream\.ads\.ottera\.tv/playlist\.m3u8\?[^'\"]*?network_id=(\d+)")
_VIDEO_RE = re.compile(r'video://(\d+)')
_GUIDE_ROWS = 200


class FreeTV(Network):
    key = 'freetv'
    name = 'FreeTV'

    def _get(self, s, endpoint: str, **params):
        return s.get(_API + endpoint, params={**_PARAMS, **params}, headers=_HEADERS)

    def _segments(self, s, channel_id: str) -> list[dict]:
        def fetch():
            r = self._get(s, 'getvideosegments', linear_channel_id=channel_id, parent_id=channel_id,
                          parent_type='linear_channel', timestamp=int(time.time()), timezone='+0000',
                          max=_GUIDE_ROWS, image_format='widescreen', image_width=366)
            return ((r.json() or {}).get('objects') or []) if r else []
        return s.memo(f'freetv_segments_{channel_id}', fetch)

    def _network_id(self, s, channel_id: str) -> str | None:
        video_id = next((m.group(1) for o in self._segments(s, channel_id)
                         for m in [_VIDEO_RE.match(o.get('link') or '')] if m), None)
        r = self._get(s, 'embeddedVideoPlayer', id=video_id or int(channel_id) + 1,
                      max_bitrate=10000, max_res=1080)
        m = _NETWORK_RE.search(r.text) if r else None
        return m.group(1) if m else None

    def channels(self, s):
        r = self._get(s, 'getobjects', object_type='linear_channel', max=100)
        out = []
        for ch in ((r.json() or {}).get('objects') or []) if r else []:
            cid, name = str(ch.get('id') or ''), (ch.get('name') or '').strip()
            if not cid or not name or ch.get('type') != 'linear_channel':
                continue
            if ch.get('required_permissions'):
                continue  # FreeTV Premium (paid)
            try:
                network_id = self._network_id(s, cid)
            except Exception as exc:  # noqa: BLE001
                logger.warning('[free_networks] FreeTV %s player unavailable: %s', name, exc)
                continue
            if not network_id:
                logger.info('[free_networks] FreeTV %s: no stream in its player, skipped', name)
                continue
            out.append(NetChannel(cid, name, _STREAM.format(network_id), logo=ch.get('logo') or None,
                                  language='es',
                                  description=(ch.get('long_description') or ch.get('short_description') or None)))
        return out

    def programs(self, s, channels):
        out = []
        for ch in channels:
            for seg in self._segments(s, ch.id):
                p = program(ch.id, seg.get('name'), from_epoch(seg.get('segment_start_time')),
                            from_epoch(seg.get('segment_end_time')),
                            description=seg.get('long_description') or seg.get('short_description'))
                if p:
                    out.append(p)
        return out

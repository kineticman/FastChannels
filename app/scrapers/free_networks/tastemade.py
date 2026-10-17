"""Tastemade, Tastemade Home, Tastemade Travel, Tastemade en Español.

app.tastemade.com runs on the backstage-api.com platform, which answers with
just the web app's fixed application/service IDs plus any device ID:
/media/channels lists the channels, /media/channels/<id>/editions +
/stream?editionId=... gives the Amagi HLS URL (stored without its ad-macro
query string, which plays fine), and /epg?from&to&channels=... returns ~5 days
of broadcasts with descriptions and art.
"""
from __future__ import annotations

import time
import uuid

from ._common import Network, NetChannel, from_epoch, logger, program

_API = 'https://backstage-api.com'
_HEADERS = {
    'x-application-id': '13cb82a2-53bd-4268-aa70-a77b612f385a',
    'x-service-id': 'ea9a9af0-df79-11ed-9146-a5b9ee2529e3',
    'x-device-type': 'web',
    'Origin': 'https://app.tastemade.com',
    'Referer': 'https://app.tastemade.com/',
    'Accept': 'application/json',
}
_CATEGORIES = {'Tastemade Home': 'Home & DIY', 'Tastemade Travel': 'Travel'}
_DAYS = 6


def _first_image(images, *kinds):
    for kind in kinds:
        for img in (images or {}).get(kind) or []:
            if img.get('url'):
                return img['url']
    return None


class Tastemade(Network):
    key = 'tastemade'
    name = 'Tastemade'

    def _get(self, s, path, **params):
        headers = {**_HEADERS, 'x-device-id': s.memo('tastemade_device', lambda: str(uuid.uuid4()))}
        r = s.get(_API + path, params=params or None, headers=headers)
        return r.json() if r else None

    def channels(self, s):
        out = []
        for ch in (self._get(s, '/media/channels') or {}).get('items') or []:
            cid, label = ch.get('id'), ch.get('label')
            if not cid or not label:
                continue
            url = self._stream(s, cid)
            if not url:
                logger.warning('[free_networks] Tastemade: no stream for %s', label)
                continue
            out.append(NetChannel(
                cid, label, url,
                logo=_first_image(ch.get('images'), 'logo', 'square', 'poster'),
                category=_CATEGORIES.get(label, 'Food'),
                language='es' if 'Español' in label else 'en',
            ))
        return out

    def _stream(self, s, cid):
        editions = self._get(s, f'/media/channels/{cid}/editions') or []
        if isinstance(editions, dict):
            editions = editions.get('items') or []
        for ed in editions:
            data = self._get(s, f'/media/channels/{cid}/stream', editionId=ed.get('id'), type='hls', drm='unencrypted')
            url = (data or {}).get('url')
            if url:
                return url.split('?', 1)[0]
        return None

    def programs(self, s, channels):
        if not channels:
            return []
        now = int(time.time())
        data = self._get(s, '/epg', **{'from': now - 3600, 'to': now + _DAYS * 86400,
                                       'channels': ','.join(ch.id for ch in channels)}) or {}
        out = []
        for cid, rows in data.items() if isinstance(data, dict) else []:
            for row in rows or []:
                p = program(cid, row.get('label'), from_epoch(row.get('startsAt')), from_epoch(row.get('endsAt')),
                            description=row.get('description') or row.get('shortDescription'),
                            poster_url=_first_image(row.get('images'), 'still', 'poster', 'background'))
                if p:
                    out.append(p)
        return out

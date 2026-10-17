"""CBN: CBN Family, CBN News Channel and CBN Español.

Brightcove live videos on CBN's account. The Playback API (with the policy key
from CBN's own player page) returns JWT-signed HLS sources, so the video ID is
stored and resolved to a fresh URL at play time. No DRM.

Guide: only CBN News Channel has a Gracenote station (109977); checked against
the stream on 2026-10-17 (both airing Superbook). The others get placeholders.
"""
from __future__ import annotations

from ._common import Network, NetChannel

_PLAYBACK = 'https://edge.api.brightcove.com/playback/v1/accounts/734546207001/videos/{}'
_POLICY_KEY = ('BCpkADawqM0sDkrHa5QVTX2eEWcGBzTjq81hkP-Ic9lxCbneX4I_3zgSKAqBJLmjqmpjKNb8ZLo3-Rvg'
               'e9temMDI2VbyD2UZotI3ZllY02THXz1JW4LU1MUs730')
_LOGO = 'https://www.cbn.com/partners/video/img/cbn-partners-og.jpg?v=1'
# id, name, Brightcove video id, language, gracenote
_CHANNELS = [
    ('family', 'CBN Family', '5721815535001', 'en', None),
    ('news', 'CBN News Channel', '5721888200001', 'en', '109977'),
    ('espanol', 'CBN Español', '5721890224001', 'es', None),
]


class CBN(Network):
    key = 'cbn'
    name = 'CBN'

    def channels(self, s):
        return [NetChannel(cid, name, vid, logo=_LOGO, category='Faith', language=lang, gracenote_id=gn)
                for cid, name, vid, lang, gn in _CHANNELS]

    def resolve(self, s, ref):
        r = s.session.get(_PLAYBACK.format(ref), headers={'Accept': '*/*', 'BCOV-Policy': _POLICY_KEY}, timeout=15)
        r.raise_for_status()
        sources = r.json().get('sources') or []
        url = next((src['src'] for src in sources
                    if (src.get('src') or '').startswith('https') and '.m3u8' in src['src']
                    and not src.get('key_systems')), None)
        if not url:
            raise RuntimeError(f'CBN: no clear HLS source for video {ref}')
        return url

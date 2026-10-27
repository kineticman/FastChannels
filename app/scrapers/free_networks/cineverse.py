"""Cineverse's free live channels: streaming.cineverse.com/livetv (AsianCrush,
MidnightPulp, Screambox, Bob Ross, Dove Channel, Lone Star ...), plus the single
channels on retrocrush.tv and joysauce.tv.

All three sites run on Matchpoint (JunctionTV). Its metax API answers only with
an Authorization header the sites add server side, so the guide is fetched the
way the sites' own pages do it: GET <site>/api/get with the metax URL
CryptoJS-AES-encrypted into an mp-tag header (passphrase from the sites' JS
bundle) and a cauth header (MD5 of the app tag). The answer is the metax JSON,
AES-256-CTR encrypted (IV first) with another bundle key. No session, cookie or
request signature is needed. The livefeed lists every channel with its HLS URL
and its schedule from a day back to a day ahead.

Streams are clear HLS up to 1080p on Amagi / g-mana, stored without their
ad-macro query string, which plays fine; indicue URLs 400 without it.

If the sites change their bundle keys, decryption fails, the network lists no
channels and Free Networks keeps the last good list (up to 3 days); the warning
below says where to look.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import time
from urllib.parse import urlsplit

try:
    from Cryptodome.Cipher import AES
    from Cryptodome.Util.Padding import pad
except ImportError:  # pragma: no cover
    from Crypto.Cipher import AES  # type: ignore[no-redef]
    from Crypto.Util.Padding import pad  # type: ignore[no-redef]

from ._common import Network, NetChannel, from_epoch, logger, program

# From the sites' _app bundle (shared by all three).
_URL_PASSPHRASE = b'd0e5a4cf1b791825734aadfe874b3ccf'
_RESPONSE_KEY = base64.b64encode(hashlib.sha256(b'e85c33e2d7333ad30dce716633d3b775').digest())[:32]
_CAUTH = hashlib.md5(b'Cinedigm-India-Cineverse').hexdigest()

_LIVEFEED = 'https://metax.api.matchpoint.tv/metax/6.0/c_{brand}/itemEpg/livefeed?isSchedule=true&jloc=us'
# Single-channel sites first: their copy of RetroCrush plays without the query
# string, Cineverse's (indicue) doesn't.
_SITES = (
    ('retrocrush', 'https://www.retrocrush.tv'),
    ('joysauce', 'https://www.joysauce.tv'),
    ('cineverse', 'https://streaming.cineverse.com'),
)
# Carried in Free Networks already from the network's own feed, or from
# BeOND's (its EntrepreneurTV plays; Cineverse's g-mana copy doesn't).
_ELSEWHERE = {'cmc california music channel', 'tastemade us', 'tastemade home', 'tastemade travel',
              'entrepreneurtv'}
# Hosts that need the stream URL's query string.
_KEEP_QUERY = {'cnvslinear.telos.indicue.com'}
# Names category_utils can't place.
_CATEGORY = {
    'midnightpulp': 'Movies',
    'real madrid': 'Sports',
    'infamous tv': 'True Crime',
    'young hollywood': 'Entertainment',
    'entrepreneurtv': 'Lifestyle',
    'so... real': 'Reality TV',
    'latino alternative tv': 'Latino',
}


def _encrypt_url(url: str) -> str:
    """CryptoJS.AES.encrypt(url, passphrase): OpenSSL "Salted__" format with
    an MD5 EVP_BytesToKey key and IV."""
    salt = os.urandom(8)
    derived, block = b'', b''
    while len(derived) < 48:
        block = hashlib.md5(block + _URL_PASSPHRASE + salt).digest()
        derived += block
    cipher = AES.new(derived[:32], AES.MODE_CBC, derived[32:48])
    return base64.b64encode(b'Salted__' + salt + cipher.encrypt(pad(url.encode(), 16))).decode()


def _decrypt_response(data: list[int]) -> str:
    raw = bytes(data)
    cipher = AES.new(_RESPONSE_KEY, AES.MODE_CTR, nonce=b'', initial_value=raw[:16])
    return cipher.decrypt(raw[16:]).decode('utf-8')


def _slug(title: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')


def _stream_url(url: str) -> str:
    return url if urlsplit(url).hostname in _KEEP_QUERY else url.split('?', 1)[0]


class Cineverse(Network):
    key = 'cineverse'
    name = 'Cineverse'
    # Channels the last programs() run had no current rows for (Young
    # Hollywood and Infamous TV list no schedule; Nolly Africa's can stop a day
    # short), so they get tvtv or placeholder blocks instead of an empty row.
    _unguided: frozenset[str] = frozenset()

    def has_guide(self, channel_id: str) -> bool:
        return channel_id not in self._unguided

    def _livefeed(self, s, brand: str, site: str) -> list[dict]:
        r = s.get(site + '/api/get', headers={
            'cauth': _CAUTH,
            'mp-tag': _encrypt_url(_LIVEFEED.format(brand=brand)),
            'Origin': site,
            'Referer': site + '/livetv',
        })
        if not r:
            return []
        try:
            feed = json.loads(_decrypt_response(r.json()['data']['data']))
        except (ValueError, KeyError, TypeError, UnicodeDecodeError) as exc:
            logger.warning('[free_networks] Cineverse: %s livefeed did not decrypt (%s) — '
                           'the site bundle keys in cineverse.py may have changed', brand, exc)
            return []
        return feed.get('liveFeeds') or []

    def _feeds(self, s) -> dict[str, dict]:
        """Channel slug -> livefeed entry, across the three sites; the first
        site to list a channel wins."""
        def fetch():
            out: dict[str, dict] = {}
            for brand, site in _SITES:
                for feed in self._livefeed(s, brand, site):
                    title = (feed.get('title') or '').strip()
                    url = (feed.get('content') or {}).get('hls_url')
                    if title and url and title.lower() not in _ELSEWHERE:
                        out.setdefault(_slug(title), feed)
            return out
        return s.memo('cineverse_feeds', fetch)

    def channels(self, s):
        return [
            NetChannel(slug, feed['title'].strip(), _stream_url(feed['content']['hls_url']),
                       logo=feed.get('thumbnail') or None,
                       category=_CATEGORY.get(feed['title'].strip().lower()))
            for slug, feed in self._feeds(s).items()
        ]

    def programs(self, s, channels):
        feeds = self._feeds(s)
        # The schedule runs from a day back to a day ahead.
        since = (time.time() - 3600) * 1000
        out = []
        for ch in channels:
            for row in (feeds.get(ch.id) or {}).get('content', {}).get('schedule') or []:
                if row.get('type') == 'dummy' or (row.get('schedule_end_timestamp') or 0) < since:
                    continue
                series = (row.get('series_title') or '').strip()
                episode = row.get('episode_no')
                rating = (row.get('rating') or {}).get('value') if isinstance(row.get('rating'), dict) else None
                p = program(ch.id, series or row.get('title'),
                            from_epoch(row.get('schedule_timestamp')),
                            from_epoch(row.get('schedule_end_timestamp')),
                            episode_title=row.get('title') if series else None,
                            # Outside a series episode_no is an internal ID, not an episode.
                            episode=episode if series and isinstance(episode, int) and 0 < episode < 1000 else None,
                            description=row.get('description') or row.get('description_large')
                            or row.get('description_small'),
                            poster_url=row.get('thumbnail_land') or row.get('thumbnail_port') or None,
                            rating=rating or None,
                            program_type='movie' if row.get('type') in ('movie', 'vod') else None)
                if p:
                    out.append(p)
        self._unguided = frozenset(ch.id for ch in channels) - {p.source_channel_id for p in out}
        return out

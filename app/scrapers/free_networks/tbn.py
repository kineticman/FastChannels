"""TBN's live channels from TBN+ (tbnplus.com): TBN, Inspire, its FAST
channels (Women of Faith, Gaither Gospel Hour, ...) and the international
feeds (TBN UK, Enlace, Nejat, ...).

Streams: the TBN+ app's public "Live Channels" JW Player playlist, which lists
each channel's clear HLS URL. TBN+ shows most of them behind a free account,
but the streams themselves aren't gated. Positiv Movies is skipped; it's the
same feed as the PosiTV network.

Guide: TBN, Inspire and TBN UK from the Trilogy app backend's /v1/epg (about
two days). The JW "pop-up" FAST channels have an XMLTV file on
epg.fast.jwp.services named after their stream's channel id (about a day).
Enlace comes from tvtv (Gracenote 33527); the rest get placeholders.
"""
from __future__ import annotations

import re

from ._common import Network, NetChannel, logger, parse_iso, program, xmltv_programs

_PLAYLIST = 'https://cdn.jwplayer.com/v2/playlists/wUwJ86KF'
_EPG = 'https://tbndsp-prod.trilogyapps.com/v1/epg'
_JW_EPG = 'https://epg.fast.jwp.services/{}/epg.xml'
_JW_CHANNEL = re.compile(r'cdn\.fast\.jwp\.services/v1/channel/([\w-]+)/')
_SKIP = {'75en6TR2'}  # Positiv Movies
# JW media id → (channel id, category, language, country, Trilogy EPG channel, gracenote)
_CHANNELS = {
    'yHJBlrke': ('tbn', 'Faith', 'en', 'US', 'TBN', None),
    'LjTiLONi': ('teaching', 'Faith', 'en', 'US', None, None),
    'SSTTcJZP': ('womenoffaith', 'Faith', 'en', 'US', None, None),
    '70zJzfER': ('menoffaith', 'Faith', 'en', 'US', None, None),
    'HuHLCD6h': ('drivethruhistory', 'Faith', 'en', 'US', None, None),
    'Rh0OeBFM': ('bettertogether', 'Faith', 'en', 'US', None, None),
    'OLPS65LV': ('gaither', 'Faith', 'en', 'US', None, None),
    'ggpOYSEC': ('inspire', 'Faith', 'en', 'US', 'INSPIRE', None),
    'Zog5LNPx': ('yippee', 'Kids', 'en', 'US', None, None),
    'uhdo8RnF': ('uk', 'Faith', 'en', 'GB', 'TBNUK', None),
    'OKlLtovE': ('enlace', 'Faith', 'es', 'US', None, '33527'),
    'QUtyrd9v': ('africa', 'Faith', 'en', 'US', None, None),
    '6RqNnde2': ('ejtv', 'Faith', 'en', 'US', None, None),
    'xzP825Cv': ('alhorreya', 'Faith', 'ar', 'US', None, None),
    'qFHlWfBl': ('nejat', 'Faith', 'fa', 'US', None, None),
    '9G7uKMZf': ('tci', 'Faith', 'it', 'IT', None, None),
}
_TRILOGY = {v[0]: v[4] for v in _CHANNELS.values() if v[4]}
# Logos in place of the playlist's posters, which for these are photos with the
# brand small in the middle (Gracenote's, or TBN+'s own circle). The rest keep
# their posters: they're already title cards, or there's nothing better.
_TMS_LOGO = 'https://tmsimg.fancybits.co/assets/{}.png?w=360&h=270'
_LOGOS = {
    'tbn': _TMS_LOGO.format('GNLZZGG0028Y4QD'),
    'inspire': _TMS_LOGO.format('s122276_ll_h15_aa'),
    'womenoffaith': _TMS_LOGO.format('s165133_ll_h15_aa'),
    'teaching': 'https://cdn.mediabackstage.com/assets/tbn/thumbnails/imglivefeedcircletandw-1788470129575.png',
    'yippee': _TMS_LOGO.format('s146780_ll_h15_aa'),
    'uk': _TMS_LOGO.format('s97332_ll_h15_ac'),
    'enlace': _TMS_LOGO.format('s20286_ll_h15_ab'),
}
# JW pop-up FAST channels, whose guide is found from their stream URL.
_JW_IDS = {'teaching', 'womenoffaith', 'menoffaith', 'drivethruhistory', 'bettertogether', 'gaither'}


def _hls(item) -> str | None:
    return next((src['file'] for src in item.get('sources') or []
                 if 'mpegurl' in (src.get('type') or '') and src.get('file')), None)


class TBN(Network):
    key = 'tbn'
    name = 'TBN'

    def channels(self, s):
        r = s.get(_PLAYLIST)
        out = []
        for item in ((r.json() or {}).get('playlist') or []) if r else []:
            media = item.get('mediaid')
            stream = _hls(item)
            if not media or media in _SKIP or not stream:
                continue
            cid, category, lang, country, _, gracenote = _CHANNELS.get(
                media, (media.lower(), 'Faith', 'en', 'US', None, None))
            out.append(NetChannel(cid, item.get('title') or cid, stream, logo=_LOGOS.get(cid) or item.get('image'),
                                  category=category, language=lang, country=country,
                                  gracenote_id=gracenote))
        if r and not out:
            logger.warning('[free_networks] TBN: live playlist had no channels')
        return out

    def has_guide(self, channel_id):
        return channel_id in _TRILOGY or channel_id in _JW_IDS

    def programs(self, s, channels):
        out = []
        for ch in channels:
            if ch.id in _TRILOGY:
                r = s.get(_EPG, params={'channel': _TRILOGY[ch.id]})
                for row in ((r.json() or {}).get('entry') or []) if r else []:
                    ext = row.get('extensions') or {}
                    p = program(ch.id, row.get('title'), parse_iso(ext.get('start_time')),
                                parse_iso(ext.get('end_time')), description=row.get('summary'))
                    if p:
                        out.append(p)
                continue
            m = _JW_CHANNEL.search(ch.stream or '')
            if not m:
                continue
            r = s.get(_JW_EPG.format(m.group(1)))
            if not r:
                logger.warning('[free_networks] TBN %s: no EPG', ch.id)
                continue
            for p in xmltv_programs(ch.id, r.content):
                # Titles are "<episode> | <show>"
                episode, sep, show = p.title.rpartition(' | ')
                if sep and episode and show:
                    p.title, p.episode_title = show, p.episode_title or episode
                out.append(p)
        return out

"""Red Bull TV's nine linear channels: World of Red Bull plus Padel, Bike,
Adventure, Motorsports, Surfing, Skateboarding, Winter and Action Reel.

No login. Each channel is a video-channels rrn; Red Bull's DMS turns it into a
302 to the live master (play.redbull.com/streams/v1/rbtv/<name>/ts, 720p; World
of Red Bull on freqsyndlin.redbull.com, 1080p). Clear TS, CORS *, no token, so
we store the DMS URL and let clients follow the redirect.

Guide: redbull.tv/en/epg server-renders every channel's schedule into its
Next.js flight payload as "channelRails": ~8 days per channel, back to back,
with descriptions and art. The page is ~16 MB (~1.1 MB gzipped); one fetch
covers all nine channels.
"""
from __future__ import annotations

import json
import re

from ._common import Network, NetChannel, logger, parse_iso, program

_EPG = 'https://www.redbull.tv/en/epg'
_DMS = 'https://dms.redbull.tv/v5/destination/rbtv/{}/personal_computer/http/us/en_US/playlist.m3u8'
_RRN = 'rrn:content:video-channels:'
# The channels' own "logo" art is a photo; every channel uses the Red Bull TV mark.
_LOGO = 'https://res.cloudinary.com/hhhkasdw4/image/upload/f_png,w_400/streaming-platform/smarttv/rbtv/logos/logo.svg'

# (channel id = rrn uuid, name, description)
_LINEUP = [
    ('c81f8686-ab67-4965-ba04-5f6658bb96cc', 'World of Red Bull',
     'Red Bull TV\'s main channel: action sports, adventure films and series.'),
    ('e0e6dee0-8c39-4de1-9488-72828468efe0', 'Red Bull TV Padel', 'Premier Padel highlights and padel stories.'),
    ('ee30c528-32b1-4604-8976-e3bcee4ae7f0', 'Red Bull TV Bike', 'Mountain bike, BMX and cycling films and events.'),
    ('870bcfa8-62b1-4e84-9c85-39f083df368a', 'Red Bull TV Adventure', 'Climbing, flying, diving and expedition films.'),
    ('fd4ed3c9-1800-477b-9909-53255da06632', 'Red Bull TV Motorsports', 'Racing, rally, motocross and motorsport series.'),
    ('2f6afaec-7ade-4fb8-961a-a51aa8279a99', 'Red Bull TV Surfing', 'Surf films, series and contest highlights.'),
    ('5021f46c-6f34-4f51-ba1f-967f2885ac97', 'Red Bull TV Skateboarding', 'Skate parts, series and contests.'),
    ('f4aa4fe4-5ce6-4b1c-a60b-abc6f21f16d0', 'Red Bull TV Winter', 'Snowboard, ski and winter sports films.'),
    ('69a66f02-21fd-42a1-be5b-6965541cfe6a', 'Red Bull TV Action Reel', 'Short action sports clips, non-stop.'),
]
_CHANNELS = [
    NetChannel(uid, name, _DMS.format(_RRN + uid),
               logo=_LOGO,
               category='Sports', description=desc)
    for uid, name, desc in _LINEUP
]
_FLIGHT_RE = re.compile(r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)', re.S)


def _card_art(card) -> str | None:
    url = ((card.get('media_resources') or {}).get('rbtv_display_art_landscape') or {}).get('url')
    return url.replace('{im}', 'im:i:w_1280') if url else None


class RedBull(Network):
    key = 'redbull'
    name = 'Red Bull TV'

    def channels(self, s):
        return list(_CHANNELS)

    def programs(self, s, channels):
        wanted = {ch.id for ch in channels}
        if not wanted:
            return []
        r = s.get(_EPG)
        if not r:
            return None
        try:
            flight = ''.join(json.loads(f'"{c}"') for c in _FLIGHT_RE.findall(r.text))
            at = flight.index('"channelRails":') + len('"channelRails":')
            rails, _ = json.JSONDecoder().raw_decode(flight, at)
        except ValueError:
            logger.warning('[free_networks] Red Bull TV: no channelRails in the EPG page')
            return None
        out = []
        for rail in rails:
            cid = str(rail.get('id') or '').removeprefix(_RRN)
            if cid not in wanted:
                continue
            for card in rail.get('cards') or []:
                kind = card.get('content_type')
                p = program(cid, card.get('title'), parse_iso(card.get('start_time')), parse_iso(card.get('end_time')),
                            episode_title=card.get('subheading') if kind == 'episode' else None,
                            description=card.get('long_description') or card.get('short_description'),
                            poster_url=_card_art(card), category='Sports',
                            program_type='movie' if kind == 'film' else None)
                if p:
                    out.append(p)
        return out

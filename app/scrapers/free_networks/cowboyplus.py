"""Cowboy+ Sports, the free rodeo/western sports channel on cowboyplus.com.

cowboyplus.com is a DICE/Vesper site: Cowboy+ Sports is GRANTED_ON_SIGN_IN
(any free account), and its manifest call hands back a fixed Amagi playout that
plays without the sign-in. Clear 1080p, CORS *. The Cowboy Channel itself is
DENIED to free accounts (paid), so it isn't here.

Guide: Teton Ridge's broadcast-schedule API (lasso.tetonridge.io), public JSON,
~7 days back to back with descriptions, episode numbers and art.
"""
from __future__ import annotations

from datetime import date

from ._common import Network, NetChannel, logger, parse_iso, program

_STREAM = ('https://amg17292-amg17292c1-amgplt0814.playout.now3.amagi.tv/ts-us-e2-n2/'
           'playlist/amg17292-amg17292c1-amgplt0814/playlist.m3u8')
_SCHEDULE = 'https://lasso.tetonridge.io/api/v1/schedule/cowboy-plus-sports'
_CHANNEL = NetChannel(
    'cowboyplussports', 'Cowboy+ Sports', _STREAM,
    # Gracenote has no logo image for 198426; the site's own channel thumbnail.
    logo='https://img.dge-prod.dicelaboratory.com/thumbnails/315823/416x234/latest.jpg',
    category='Sports', gracenote_id='198426',
    description='Rodeo, bull riding and western sports, live and replayed.',
)


def _episode(code):
    """xmltv_ns 'season.episode.part' (zero-based) to (season, episode)."""
    try:
        season, episode = (str(code).split('.') + ['', ''])[:2]
        return (int(season) + 1 if season.isdigit() else None,
                int(episode) + 1 if episode.isdigit() else None)
    except (TypeError, ValueError):
        return None, None


def _air_date(val):
    try:
        return date.fromisoformat(str(val)[:10]) if val else None
    except ValueError:
        return None


class CowboyPlus(Network):
    key = 'cowboyplus'
    name = 'Cowboy+ Sports'

    def channels(self, s):
        return [_CHANNEL]

    def programs(self, s, channels):
        if not channels:
            return []
        r = s.get(_SCHEDULE)
        if not r:
            return None
        try:
            rows = r.json()
        except ValueError:
            logger.warning('[free_networks] Cowboy+ Sports: schedule was not JSON')
            return None
        out = []
        for row in rows if isinstance(rows, list) else []:
            season, episode = _episode(row.get('season_episode_xmltv'))
            cats = ['sports' if c == 'sport' else c for c in row.get('categories') or [] if isinstance(c, str)]
            p = program('cowboyplussports', row.get('title'), parse_iso(row.get('start_utc')),
                        parse_iso(row.get('stop_utc')),
                        episode_title=row.get('sub_title'), description=row.get('description'),
                        poster_url=row.get('image_thumb16x9'), rating=row.get('rating'),
                        category=';'.join(dict.fromkeys(c.title() for c in cats)) or 'Sports',
                        season=season, episode=episode, is_live=bool(row.get('is_live')) or None,
                        original_air_date=_air_date(row.get('original_air_date')))
            if p:
                out.append(p)
        return out

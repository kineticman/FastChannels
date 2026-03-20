"""
Pluto TV scraper for FastChannels.
Config (set via admin UI):
  - username / password  (optional — anonymous works fine)
  - country_codes        (comma-separated, default: us_east)
"""
from __future__ import annotations

import re
import threading
import uuid
from datetime import datetime, timedelta
from typing import Optional

import pytz
import requests

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata
from ..gracenote_map import resolve_gracenote

import logging
logger = logging.getLogger(__name__)

STREAM_POOL_SIZE = 10
STITCHER = "https://cfd-v4-service-channel-stitcher-use1-1.prd.pluto.tv"

ALLOWED_COUNTRY_CODES = ['local', 'us_east', 'us_west', 'ca', 'uk', 'fr', 'de']

X_FORWARD = {
    "local":   {"X-Forwarded-For": ""},
    "uk":      {"X-Forwarded-For": "178.238.11.6"},
    "ca":      {"X-Forwarded-For": "192.206.151.131"},
    "fr":      {"X-Forwarded-For": "193.169.64.141"},
    "de":      {"X-Forwarded-For": "81.173.176.155"},
    "us_east": {"X-Forwarded-For": "108.82.206.181"},
    "us_west": {"X-Forwarded-For": "76.81.9.69"},
}

BOOT_HEADERS = {
    'authority': 'boot.pluto.tv',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'origin': 'https://pluto.tv',
    'referer': 'https://pluto.tv/',
    'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
}

BOOT_PARAMS_BASE = {
    'appName': 'web',
    'appVersion': '8.0.0-111b2b9dc00bd0bea9030b30662159ed9e7c8bc6',
    'deviceVersion': '122.0.0',
    'deviceModel': 'web',
    'deviceMake': 'chrome',
    'deviceType': 'web',
    'clientModelNumber': '1.0.0',
    'serverSideAds': 'false',
    'drmCapabilities': '',
    'blockingMode': '',
    'notificationVersion': '1',
    'appLaunchCount': '',
    'lastAppLaunchDate': '',
}

_SERIES_GENRES = {
    ("Animated",): ["Family Animation", "Cartoons"],
    ("Educational",): ["Education & Guidance", "Instructional & Educational"],
    ("News",): ["News and Information", "General News", "News + Opinion", "General News"],
    ("History",): ["History & Social Studies"],
    ("Politics",): ["Politics"],
    ("Action",): [
        "Action & Adventure", "Action Classics", "Martial Arts", "Crime Action",
        "Family Adventures", "Action Sci-Fi & Fantasy", "Action Thrillers", "African-American Action",
    ],
    ("Adventure",): ["Action & Adventure", "Adventures", "Sci-Fi Adventure"],
    ("Reality",): ["Reality", "Reality Drama", "Courtroom Reality", "Occupational Reality", "Celebrity Reality"],
    ("Documentary",): [
        "Documentaries", "Social & Cultural Documentaries", "Science and Nature Documentaries",
        "Miscellaneous Documentaries", "Crime Documentaries", "Travel & Adventure Documentaries",
        "Sports Documentaries", "Military Documentaries", "Political Documentaries", "Foreign Documentaries",
        "Religion & Mythology Documentaries", "Historical Documentaries", "Biographical Documentaries",
        "Faith & Spirituality Documentaries",
    ],
    ("Biography",): ["Biographical Documentaries", "Inspirational Biographies"],
    ("Science Fiction",): ["Sci-Fi Thrillers", "Sci-Fi Adventure", "Action Sci-Fi & Fantasy"],
    ("Thriller",): ["Sci-Fi Thrillers", "Thrillers", "Crime Thrillers"],
    ("Talk",): ["Talk & Variety", "Talk Show"],
    ("Variety",): ["Sketch Comedies"],
    ("Home Improvement",): ["Art & Design", "DIY & How To", "Home Improvement"],
    ("House/garden",): ["Home & Garden"],
    ("Cooking",): ["Cooking Instruction", "Food & Wine", "Food Stories"],
    ("Travel",): ["Travel & Adventure Documentaries", "Travel"],
    ("Western",): ["Westerns", "Classic Westerns"],
    ("LGBTQ",): ["Gay & Lesbian", "Gay & Lesbian Dramas", "Gay"],
    ("Game show",): ["Game Show"],
    ("Military",): ["Classic War Stories"],
    ("Comedy",): [
        "Cult Comedies", "Spoofs and Satire", "Slapstick", "Classic Comedies", "Stand-Up",
        "Sports Comedies", "African-American Comedies", "Showbiz Comedies", "Sketch Comedies",
        "Teen Comedies", "Latino Comedies", "Family Comedies",
    ],
    ("Crime",): ["Crime Action", "Crime Drama", "Crime Documentaries"],
    ("Sports",): ["Sports", "Sports & Sports Highlights", "Sports Documentaries", "Poker & Gambling"],
    ("Poker & Gambling",): ["Poker & Gambling"],
    ("Crime drama",): ["Crime Drama"],
    ("Drama",): ["Classic Dramas", "Family Drama", "Indie Drama", "Romantic Drama", "Crime Drama"],
    ("Children",): ["Kids", "Children & Family", "Kids' TV", "Cartoons", "Animals", "Family Animation", "Ages 2-4", "Ages 11-12"],
}


class _StreamSession:
    """One virtual device — own clientID, session, and per-country token cache."""

    def __init__(self, username=None, password=None):
        self.client_id   = str(uuid.uuid4())
        self.session     = requests.Session()
        self.username    = username
        self.password    = password
        self._resp_cache: dict = {}
        self._cached_at:  dict = {}

    def boot(self, country_code: str) -> tuple[Optional[dict], Optional[str]]:
        now    = datetime.now(pytz.utc)
        cached = self._resp_cache.get(country_code)
        if cached and (now - self._cached_at.get(country_code, datetime.min.replace(tzinfo=pytz.utc))) < timedelta(hours=4):
            return cached, None

        params = {**BOOT_PARAMS_BASE, 'clientID': self.client_id}
        if self.username and self.password:
            params['username'] = self.username
            params['password'] = self.password

        headers = {**BOOT_HEADERS}
        if country_code in X_FORWARD:
            headers.update(X_FORWARD[country_code])

        try:
            r = self.session.get('https://boot.pluto.tv/v4/start', headers=headers, params=params, timeout=15)
        except Exception as e:
            return None, f"boot request failed: {e}"

        if not (200 <= r.status_code <= 201):
            return None, f"boot HTTP {r.status_code}"

        resp = r.json()
        self._resp_cache[country_code] = resp
        self._cached_at[country_code]  = now
        logger.debug("[pluto] slot %s new token for %s", self.client_id[:8], country_code)
        return resp, None


class PlutoScraper(BaseScraper):
    source_name     = "pluto"
    display_name    = "Pluto TV"
    stream_audit_enabled = True
    scrape_interval = 360

    config_schema = [
        ConfigField(
            key='username', label='Pluto TV Username',
            field_type='text', secret=False,
            placeholder='email@example.com',
            help_text='Optional. Log in to access your favourites and additional content.',
        ),
        ConfigField(
            key='password', label='Pluto TV Password',
            field_type='password', secret=True,
            help_text='Optional. Leave blank for anonymous access.',
        ),
        ConfigField(
            key='country_codes', label='Country/Region Feeds',
            field_type='text', default='us_east',
            placeholder='us_east,us_west,ca,uk,fr,de',
            help_text=f'Comma-separated list. Available: {", ".join(ALLOWED_COUNTRY_CODES)}',
        ),
        ConfigField(
            key='pool_size', label='Session Pool Size',
            field_type='number', default=str(STREAM_POOL_SIZE),
            placeholder=str(STREAM_POOL_SIZE),
            help_text='Number of concurrent Pluto TV sessions for stream resolution. Default is 10. Increase if you have many concurrent viewers.',
        ),
    ]

    def __init__(self, config: dict = None):
        super().__init__(config)
        username = self.config.get('username') or None
        password = self.config.get('password') or None

        raw_codes = self.config.get('country_codes', 'us_east')
        self.country_codes = [
            c.strip() for c in raw_codes.split(',')
            if c.strip() in ALLOWED_COUNTRY_CODES
        ] or ['us_east']

        try:
            pool_size = max(1, int(self.config.get('pool_size') or STREAM_POOL_SIZE))
        except (ValueError, TypeError):
            pool_size = STREAM_POOL_SIZE

        self._pool      = [_StreamSession(username, password) for _ in range(pool_size)]
        self._pool_size = pool_size
        self._pool_idx  = 0
        self._pool_lock = threading.Lock()
        self._meta_slot = self._pool[0]

    def _next_slot(self) -> _StreamSession:
        with self._pool_lock:
            slot = self._pool[self._pool_idx % self._pool_size]
            self._pool_idx += 1
        return slot

    def _meta_token(self, country_code: str) -> tuple[Optional[str], Optional[dict], Optional[str]]:
        resp, err = self._meta_slot.boot(country_code)
        if err:
            return None, None, err
        token = resp.get('sessionToken')
        if not token:
            return None, None, "no sessionToken in boot response"
        return token, resp, None

    def fetch_channels(self) -> list[ChannelData]:
        all_channels: list[ChannelData] = []
        seen_ids: set[str] = set()
        for country_code in self.country_codes:
            for ch in self._fetch_country_channels(country_code):
                if ch.source_channel_id not in seen_ids:
                    seen_ids.add(ch.source_channel_id)
                    all_channels.append(ch)
        logger.info("[pluto] total %d channels across %s", len(all_channels), self.country_codes)
        return all_channels

    def _fetch_country_channels(self, country_code: str) -> list[ChannelData]:
        token, _, err = self._meta_token(country_code)
        if err:
            logger.error("[pluto] boot failed for %s: %s", country_code, err)
            return []

        headers = {
            'accept': '*/*', 'accept-language': 'en-US,en;q=0.9',
            'authorization': f'Bearer {token}',
            'origin': 'https://pluto.tv', 'referer': 'https://pluto.tv/',
        }
        if country_code in X_FORWARD:
            headers.update(X_FORWARD[country_code])

        try:
            r = self.session.get(
                'https://service-channels.clusters.pluto.tv/v2/guide/channels',
                params={'channelIds': '', 'offset': '0', 'limit': '1000', 'sort': 'number:asc'},
                headers=headers, timeout=30,
            )
            r.raise_for_status()
            channel_list = r.json().get('data', [])
        except Exception as e:
            logger.error("[pluto] channel fetch failed for %s: %s", country_code, e)
            return []

        cat_map: dict[str, str] = {}
        try:
            r2 = self.session.get(
                'https://service-channels.clusters.pluto.tv/v2/guide/categories',
                params={'offset': '0', 'limit': '1000'},
                headers=headers, timeout=30,
            )
            r2.raise_for_status()
            for cat in r2.json().get('data', []):
                for cid in cat.get('channelIDs', []):
                    cat_map[cid] = cat.get('name')
        except Exception as e:
            logger.warning("[pluto] category fetch failed for %s: %s", country_code, e)

        channels: list[ChannelData] = []
        seen_numbers: set[int] = set()

        for elem in channel_list:
            ch_id    = elem.get('id')
            name     = elem.get('name') or elem.get('call_sign', '')
            number   = elem.get('number')
            if number is not None:
                while number in seen_numbers:
                    number += 1
                seen_numbers.add(number)
            logo = next(
                (img['url'] for img in elem.get('images', []) if img.get('type') == 'colorLogoPNG'),
                None
            )
            channels.append(ChannelData(
                source_channel_id = ch_id,
                name              = name,
                # Resolve by stable Pluto channel ID rather than slug. Slugs can
                # drift or be repointed, while source_channel_id is what the M3U
                # and XMLTV already key on.
                stream_url        = f"pluto://{country_code}/{ch_id}",
                stream_type       = 'hls',
                logo_url          = logo,
                slug              = elem.get('slug') or name.lower().replace(' ', '-'),
                category          = cat_map.get(ch_id),
                language          = infer_language_from_metadata(name, cat_map.get(ch_id)),
                country           = country_code,
                number            = number,
                gracenote_id      = resolve_gracenote('pluto', upstream_id=elem.get('tmsid'), lookup_key=ch_id),
            ))

        logger.info("[pluto] %s: %d channels", country_code, len(channels))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        programs: list[ProgramData] = []
        by_country: dict[str, list[ChannelData]] = {}
        for ch in channels:
            if ch.stream_url and ch.stream_url.startswith('pluto://'):
                parts   = ch.stream_url[len('pluto://'):].split('/', 1)
                country = parts[0] if parts else 'us_east'
            else:
                country = ch.country or 'us_east'
            by_country.setdefault(country, []).append(ch)
        for country_code, chs in by_country.items():
            programs.extend(self._fetch_country_epg(country_code, chs))
        return programs

    def _fetch_country_epg(self, country_code: str, channels: list[ChannelData]) -> list[ProgramData]:
        token, _, err = self._meta_token(country_code)
        if err:
            logger.warning("[pluto] EPG boot failed for %s: %s", country_code, err)
            return []

        headers = {
            'accept': '*/*', 'accept-language': 'en-US,en;q=0.9',
            'authorization': f'Bearer {token}',
            'origin': 'https://pluto.tv', 'referer': 'https://pluto.tv/',
        }
        if country_code in X_FORWARD:
            headers.update(X_FORWARD[country_code])

        start_time = datetime.now(pytz.utc).strftime("%Y-%m-%dT%H:00:00.000Z")
        all_ids    = [ch.source_channel_id for ch in channels]
        programs: list[ProgramData] = []
        end_time   = start_time

        n_batches = (len(all_ids) + 99) // 100

        for window in range(3):
            if window > 0:
                start_time = end_time
            for i in range(0, len(all_ids), 100):
                batch     = all_ids[i:i+100]
                batch_num = i // 100 + 1
                try:
                    r = self.session.get(
                        'https://service-channels.clusters.pluto.tv/v2/guide/timelines',
                        params={'start': start_time, 'channelIds': ','.join(batch), 'duration': '720'},
                        headers=headers, timeout=30,
                    )
                    r.raise_for_status()
                    data     = r.json()
                    new_prgs = self._parse_timelines(data.get('data', []))
                    programs.extend(new_prgs)
                    logger.debug("[pluto] %s: EPG w%d b%d/%d → %d entries",
                                 country_code, window + 1, batch_num, n_batches, len(new_prgs))
                    meta_end = data.get('meta', {}).get('endDateTime')
                    if meta_end:
                        end_time = (
                            datetime.strptime(meta_end, "%Y-%m-%dT%H:%M:%S.%fZ")
                            .replace(tzinfo=pytz.utc)
                            .strftime("%Y-%m-%dT%H:00:00.000Z")
                        )
                except Exception as e:
                    logger.warning("[pluto] EPG fetch failed %s window=%d batch=%d: %s",
                                   country_code, window + 1, batch_num, e)

        # Deduplicate within the fetch — programs near window boundaries can
        # appear in both adjacent window responses.
        seen: set[tuple] = set()
        unique: list[ProgramData] = []
        for p in programs:
            key = (p.source_channel_id, p.start_time)
            if key not in seen:
                seen.add(key)
                unique.append(p)

        logger.info("[pluto] %s: %d EPG entries total (%d dupes dropped)",
                    country_code, len(unique), len(programs) - len(unique))
        return unique

    def _parse_timelines(self, data: list) -> list[ProgramData]:
        programs = []
        illegal  = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')

        _filler = {'no info available', 'n/a'}

        def clean(s):
            return illegal.sub('', s or '').replace('&quot;', '"')

        def clean_meta(s):
            """Return None for Pluto filler placeholder strings."""
            v = clean(s)
            return None if v.lower() in _filler else (v or None)

        def mapped_categories(value):
            if not value or value.lower() in _filler:
                return []
            for labels, raw_values in _SERIES_GENRES.items():
                if value in raw_values:
                    return list(labels)
            return [value]

        def parse_original_air_date(value):
            if not value:
                return None
            try:
                parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00')).date()
            except Exception:
                return None
            if parsed.year <= 1970:
                return None
            return parsed

        for entry in data:
            channel_id = entry.get('channelId')
            for tl in entry.get('timelines', []):
                try:
                    start = datetime.strptime(tl['start'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
                    end   = datetime.strptime(tl['stop'],  "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
                except (KeyError, ValueError):
                    continue
                ep       = tl.get('episode', {})
                series   = ep.get('series', {})
                clip     = ep.get('clip', {}) if isinstance(ep.get('clip'), dict) else {}
                title    = clean(tl.get('title', ''))
                ep_title = clean(ep.get('name', ''))
                categories = []
                # Skip Pluto filler slots — no real program data
                if not title or title.lower() in _filler:
                    continue
                categories.extend(mapped_categories(ep.get('genre')))
                series_type = series.get('type', '')
                if series_type == 'tv':
                    categories.append('Series')
                elif series_type == 'film':
                    categories.append('Movie')
                categories.extend(mapped_categories(ep.get('subGenre')))
                unique_categories = list(dict.fromkeys(cat for cat in categories if cat))
                programs.append(ProgramData(
                    source_channel_id = channel_id,
                    title             = title,
                    description       = clean_meta(ep.get('description', '')),
                    start_time        = start,
                    end_time          = end,
                    poster_url        = series.get('tile', {}).get('path') or None,
                    category          = ';'.join(unique_categories) or None,
                    season            = ep.get('season'),
                    episode           = ep.get('number'),
                    episode_title     = ep_title if ep_title.lower() != title.lower() else None,
                    original_air_date = parse_original_air_date(clip.get('originalReleaseDate')),
                ))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith('pluto://'):
            return raw_url
        remainder    = raw_url[len('pluto://'):]
        country_code, watch_id = remainder.split('/', 1) if '/' in remainder else ('us_east', remainder)
        slot = self._next_slot()
        resp, err = slot.boot(country_code)
        if err:
            logger.error("[pluto] resolve boot failed for %s: %s", country_code, err)
            return raw_url
        token           = resp.get('sessionToken', '')
        stitcher_params = resp.get('stitcherParams', '')
        return (
            f"{STITCHER}/v2/stitch/hls/channel/{watch_id}/master.m3u8"
            f"?{stitcher_params}&jwt={token}&masterJWTPassthrough=true&includeExtendedEvents=true"
        )

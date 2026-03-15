# app/scrapers/roku.py
#
# The Roku Channel — FAST live TV scraper
#
# Auth flow (fully headless, no browser):
#   1. GET /                     → session cookies
#   2. GET /api/v1/csrf          → csrf token
#   3. GET content proxy         → playId + linearSchedule (now/next EPG)
#   4. POST /api/v3/playback     → JWT-signed osm.sr.roku.com stream URL
#
# stream_url stored as: roku://{station_id}
# resolve() boots a fresh session on demand and calls /api/v3/playback
# Token caching: csrf + cookies cached for 55 minutes (they last ~1hr)

from __future__ import annotations

import logging
import re
import base64
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import parse_qs, quote, urlparse

from .base import BaseScraper, ChannelData, ProgramData, StreamDeadError, ScrapeSkipError, infer_language_from_metadata, is_transient_network_error

logger = logging.getLogger(__name__)


def _join_categories(values: list[str] | tuple[str, ...] | None) -> str | None:
    if not values:
        return None
    normalized = []
    for value in values:
        if not value:
            continue
        clean = value.strip()
        if not clean:
            continue
        label = clean[0].upper() + clean[1:]
        if label not in normalized:
            normalized.append(label)
    return ';'.join(normalized) or None


def _language_from_metadata(*values: str | None) -> str:
    return infer_language_from_metadata(*values)

# ── Constants ──────────────────────────────────────────────────────────────────

_BASE        = "https://therokuchannel.roku.com"
_HOME        = f"{_BASE}/"
_LIVE_TV     = f"{_BASE}/live-tv"
_CSRF_URL    = f"{_BASE}/api/v1/csrf"
_PLAYBACK    = f"{_BASE}/api/v3/playback"
_CONTENT_TPL = "https://content.sr.roku.com/content/v1/roku-trc/{sid}"
_PROXY_BASE  = f"{_BASE}/api/v2/homescreen/content/"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

_EPG_URL = f"{_BASE}/api/v2/epg"

# Tags in the EPG station object → human-readable category (checked in order)
_TAG_CATEGORY_PRIORITY = [
    ('news',            'News'),
    ('spanish-language','Spanish'),
    ('music',           'Music'),
    ('kids_music',      'Kids'),
    ('kids_linear',     'Kids'),
    ('ages_1-3',        'Kids'),
    ('ages_4-6',        'Kids'),
    ('ages_7-9',        'Kids'),
    ('ages_10plus',     'Kids'),
    ('educational',     'Kids'),
    ('preschool_specials', 'Kids'),
]

_SESSION_TTL = 55 * 60  # seconds before we refresh cookies + csrf
_SESSION_HARD_TTL = 12 * 60 * 60  # discard persisted session state after 12h
_PLAY_ID_TTL = 6 * 60 * 60  # reuse playIds for a few hours to reduce tune-time content lookups
_STREAM_URL_TTL = 5 * 60 * 60  # reuse exact HLS URLs; Roku traceId URLs stay valid ~6h
_OSM_BASE = "https://osm.sr.roku.com"
# OSM stream URL pattern: /osm/v1/hls/master/{selector_uuid}/{session_token}/index.m3u8
_OSM_STREAM_RE = re.compile(r"/osm/v1/hls/master/([0-9a-f-]{36})/([0-9a-f]+)/index\.m3u8")
_SELECTOR_UUID_RE = re.compile(r"/v1/([0-9a-f-]{36})$")
_LIVE_TV_403_RETRIES = 3
_CACHE_WARM_RETRY_WORKERS = 4
_EPG_WORKERS = 5
_DESC_WORKERS = 5
_ROKU_403_COOLDOWN = 5 * 60
_DESC_CACHE_TTL = 14 * 24 * 60 * 60  # keep descriptions for 14 days; content doesn't change

# Name-based category keywords — checked in order, first match wins.
# Each entry: (set-of-substrings, category).  All comparisons are lowercase.
_NAME_CATEGORY_RULES: list[tuple[set[str], str]] = [
    # Sports — before News so "CBS Sports" beats generic fallback
    ({
        'sport', 'deportes',
        'nfl', 'nba', 'nhl', 'mlb', 'nascar', 'nhra', 'pga tour',
        'ufc', 'mma', 'tennis', 'golf', 'wrestling', 'boxing', 'ringside',
        'billiard', 'pickleball', 'bassmaster', 'x games', 'pbr:',
        'motocross', 'f1 channel', 'espn', 'fubo', 'fanduel tv',
        'draftkings', 'sportsgrid', 'speed sport', 'swerve combat',
        'swerve women', 'hbo boxing', 'one championship', 'pfl mma',
        'dazn', 'top rank', 'lucha plus', 'big 12 studios', 'acc digital',
        'red bull tv', 'outside tv', 'myoutdoortv', 'racer select',
        'racing america', 'top barça', 'uefa', 'fifa+', 'pursuitup',
        'rig tv', 'monster jam', 'hong kong fight', 'hi-yah',
        'american ninja', 'american gladiator', 'meateater',
        'nesn', 'overtime', 'fuel tv', 'team usa tv', 'fear factor',
        'jim rome',
    }, 'Sports'),
    # Music — iHeart, Vevo, Stingray ambient, music radio
    ({
        'iheart', 'vevo', 'stingray', 'tiktok radio', 'revolt mixtape',
        'circle country', 'electric now', 'mvstv', 'lamusica', 'lamúsica',
        'musica tv', 'música tv', 'fuse +',
        'bet pluto', 'mtv pluto',
    }, 'Music'),
    # News / Weather — national brands + local stations
    ({
        'news', 'noticias', 'weather', 'cnn', 'fox local',
        'usa today', 'the hill', 'tyt-go', 'newsmax', 'oan plus',
        'liveno', 'scripps', 'rcn noticias', 'telemundo al día',
        'telemundo ahora', 'fuerza informativa', 'telediario',
        'abc7', 'abc13', 'abc30', 'abc6 ', 'abc11',
        'kiro 7', 'wpxi', 'wsb ', 'wsoc', 'wftv', 'wapa+',
        "arizona's family", "america's voice", 'first alert',
        'abc localish', 'inside edition',
    }, 'News'),
    # True Crime & Mystery
    ({
        'crime', 'mystery', 'court tv', 'cold case', 'first 48', 'cops',
        'jail', 'law & crime', 'forensic files', 'dateline', 'live pd',
        'to catch a', 'american crimes', 'trublu', 'total crime',
        'unsolved', 'i (almost)', 'living with evil', 'dr. g:',
        'chaos on cam', 'untold stories of the er',
        'murder she wrote', 'mysteria', 'mysterious', 'caught in providence',
        'confess by nosey', 'paternity court', 'ghost hunter',
        '48 hours', '20/20',
    }, 'True Crime'),
    # Horror
    ({
        'horror', 'scary', 'screambox', 'haunt', 'fear zone', 'dark fears',
        'cine de horror', 'scares by shudder', 'universal monsters',
        'z nation', 'unxplained', 'ghosts are real', 'survive or die',
    }, 'Horror'),
    # Sci-Fi
    ({
        'sci-fi', 'star trek', 'stargate', 'outersphere', 'space & beyond',
        'alien nation', 'sci fi', 'doctor who', 'pluto tv fantastic',
    }, 'Sci-Fi'),
    # Anime
    ({
        'anime', 'crunchyroll', 'retrocrush', 'retro crush', 'yu-gi-oh',
    }, 'Anime'),
    # Food & Cooking
    ({
        'food network', 'tastemade', 'cooking', 'kitchen', 'chef',
        'emeril', 'jamie oliver', 'bon appetit', 'pbs food',
        "america's test kitchen", 'bobby flay', 'martha stewart',
        'great british baking', 'bbc food', 'delicious eats',
    }, 'Food'),
    # Nature & Wildlife
    ({
        'nature', 'wildlife', 'wildearth', 'love nature', 'jack hanna',
        'naturaleza', 'national geographic', 'wicked tuna', 'life below zero',
        'dog whisperer', 'incredible dr. pol', 'paws & claws',
        'magellan', 'curiosity', 'earthday', 'love the planet',
        'bbc earth', 'real disaster', 'pet collective',
    }, 'Nature'),
    # Home & DIY
    ({
        'this old house', 'home & diy', 'home crashers', 'homeful',
        'chip & jo', 'gardening', 'tiny house', 'home improvement',
        'powernation', 'inside outside', 'at home with', 'rustic retreat',
        'home.made', 'ultimate builds', 'bbc home & garden', 'repair shop',
    }, 'Home & DIY'),
    # Reality TV
    ({
        'real housewives', 'bravo vault', 'bridezillas', 'braxton family',
        'dance moms', 'jersey shore', 'love & hip hop', 'love after lockup',
        'million dollar listing', 'project runway', 'say yes to the dress',
        'storage wars', 'teen mom', 'bad girls club', 'growing up hip hop',
        'all reality', 'reality rocks', 'pawn stars', 'duck dynasty',
        'survivor', 'the challenge', 'shark tank', 'deal or no deal',
        'supermarket sweep', 'supernanny', 'the masked singer',
        'extreme makeover', 'extreme jobs', 'bachelor nation',
        "dallas cowboys cheerleader", 'world of love island',
        'matched married', 'ax men', 'ice road trucker', 'dog the bounty',
        'the amazing race', 'e! keeping up', 'cheaters',
        'divorce court', 'judge nosey', 'the judge judy channel',
        'judge judy', 'dr. phil', 'the doctors',
        'caso cerrado', 'ellen channel', 'nosey',
    }, 'Reality TV'),
    # Game Shows
    ({
        'game show', 'price is right', 'family feud', 'buzzr',
        "let's make a deal", 'who wants to be a millionaire',
        'celebrity name game', 'deal or no deal',
    }, 'Game Shows'),
    # Comedy
    ({
        'comedy', 'laugh', 'lol network', 'just for laughs', 'sitcom',
        'snl vault', 'portlandia', 'get comedy', 'laff',
        'funniest home video', 'mst3k', 'failarmy', "wild 'n out",
        'national lampoon', 'pink panther', 'johnny carson',
        'carol burnett', 'anger management',
        'cheers + frasier', 'cougar town', 'according to jim',
        'are we there yet', 'saved by the bell', 'my wife and kids',
        'the conners', 'bernie mac', 'dick van dyke', 'life with derek',
        'blossom', 'seinfeld', 'the goldbergs', 'leave it to beaver',
        'ed sullivan', 'the red green channel',
    }, 'Comedy'),
    # Kids & Family
    ({
        'dino', 'animation+', 'animation +',
    }, 'Kids'),
    # Drama & Soaps
    ({
        'drama', 'primetime soaps', 'lifetime love', 'lifetime movie',
        'hallmark', 'tv land drama', 'tv amor', 'kanal d drama',
        'novela', 'supernatural drama', 'general hospital',
        'law & order', 'nypd blue', 'csi', 'the practice',
        'the walking dead', 'silent witness', 'midsomer', 'felicity',
        'degrassi', 'baywatch', 'beverly hills 90210', 'xena',
        'nash bridges', 'bull ', 'heartland classic', 'acorn tv',
        'britbox', 'sundance now',
        'cw forever', 'cw gold', 'allblk', 'alfred hitchcock',
        'tyler perry', 'in the heat of the night', 'tribeca',
        'shout factory',
    }, 'Drama'),
    # Movies
    ({
        'movies', 'movie', 'cinema', 'film', 'cinevault', 'miramax',
        'mgm', 'filmrise', 'samuel goldwyn', 'gravitas', 'asylum',
        'lionsgate', 'paramount movie', 'universal action', 'universal crime',
        'universal westerns', 'xumo free', 'just movies', 'cine',
        'filmex', 'great american rom', 'my time movie', 'cinépolis',
        'maverick black cinema', 'pam grier',
        'amc+', 'kino lorber', 'blackpix', 'shades of black',
    }, 'Movies'),
    # Westerns
    ({
        'western', 'gunsmoke', 'wild west', 'lone ranger', 'virginian',
        'classic movie western',
    }, 'Westerns'),
    # Faith & Inspiration
    ({
        'dove channel', 'osteen', 'up faith', 'aspire', 'highway to heaven',
        'little house',
    }, 'Faith'),
    # Travel & Adventure
    ({
        'travel', 'adventure', 'exploration', 'xplore', 'places & spaces',
        'no reservations', 'bizarre foods', 'highway thru hell',
        'locked up abroad',
    }, 'Travel'),
    # Science, History & Documentary
    ({
        'science', 'mythbusters', 'history', 'smithsonian', 'ancient aliens',
        'modern marvels', 'science is amazing', 'science quest',
        'military heroes', 'classic car auction', 'modern innovations',
        'docu', 'docurama', 'magellan tv', 'pbs genealogy',
        'antiques roadshow', 'get factual',
    }, 'Science'),
    # Gaming & Esports
    ({
        'gaming', 'esports', 'league of legends', 'fgteev', 'unspeakable',
        'mrbeast', 'mythical', 'team liquid',
    }, 'Gaming'),
    # Automotive
    ({
        'top gear', 'torque tv', 'mecum', 'discovery turbo',
        'in the garage', 'car chase', 'motortrend', 'velocity',
        'roadkill channel', 'hot rod',
    }, 'Automotive'),
    # Spanish — name-based fallback for channels without the spanish-language tag
    ({
        'flixlatino', 'vix ', 'vix+', 'canela.tv', 'canela tv',
        'venevisión', 'novelísima', 'novelisima',
        'remezcla', 'en español', 'atresplayer', 'pitufo',
        'mi raza', 'sobreviví', 'sobrevivi', 'c4 en alerta',
        'telemundo acción', 'telemundo accion', 'telemundo puerto',
        'emoción atres', 'emocion atres', 'única tv', 'unica tv',
        'cine exclusivo', 'azteca', 'univision', 'canal estrellas',
        'imagen tv', 'tvnotas', 'bandamax', 'ritmoson',
    }, 'Spanish'),
    # Shopping
    ({
        'qvc', 'hsn', 'jewelry television', 'deal zone',
    }, 'Shopping'),
]


def _category_from_name(title: str) -> str | None:
    """Infer category from channel name keywords. Returns None if no match."""
    tl = title.lower()
    for keywords, label in _NAME_CATEGORY_RULES:
        if any(kw in tl for kw in keywords):
            return label
    return None


# ── Category helpers ───────────────────────────────────────────────────────────

def _category_from_station(station: dict) -> str:
    """Derive a human-readable category from an EPG station object."""
    if station.get("kidsDirected"):
        return "Kids"
    tags = set(station.get("tags", []))
    for tag, label in _TAG_CATEGORY_PRIORITY:
        if tag in tags:
            return label
    # channelcode_* tags hint at genre
    for tag in tags:
        tl = tag.lower()
        if "reality" in tl or "wedding" in tl:
            return "Reality TV"
        if "thriller" in tl or "movie" in tl or "film" in tl or "ifc" in tl:
            return "Movies"
        if "comedy" in tl:
            return "Comedy"
        if "drama" in tl or "stories" in tl:
            return "Drama"
    # Fall back to name-based keyword matching
    return _category_from_name(station.get("title") or "") or "Live TV"


def _cat_id_to_label(cat_id: str) -> str:
    """Convert a Roku cat-* ID to a friendly label (best effort)."""
    _MAP = {
        "cat-news": "News", "cat-national-news": "News", "cat-epg-news-opinion": "News",
        "cat-sports": "Sports", "cat-sports-general": "Sports",
        "cat-movies": "Movies", "cat-movie": "Movies",
        "cat-comedy": "Comedy", "cat-drama": "Drama",
        "cat-reality": "Reality TV", "cat-lifestyle": "Lifestyle",
        "cat-food": "Food", "cat-music": "Music",
        "cat-kids": "Kids", "cat-family": "Kids",
    }
    return _MAP.get(cat_id, "Live TV")


# ── Scraper ────────────────────────────────────────────────────────────────────

class RokuScraper(BaseScraper):

    source_name           = "roku"
    display_name          = "The Roku Channel"
    scrape_interval       = 60    # EPG refreshed every hour
    channel_refresh_hours = 6     # refresh channel metadata several times a day to warm Roku caches
    stream_audit_enabled  = True

    # No config needed — fully anonymous, no credentials
    config_schema = []

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update({
            "User-Agent":      _UA,
            "Accept-Language": "en-US,en;q=0.9",
        })

        # Session state — refreshed when expired
        self._csrf_token:    Optional[str]   = None
        self._session_born:  Optional[float] = None   # epoch seconds
        self._play_id_cache: dict[str, dict[str, object]] = {}
        self._selector_url_cache: dict[str, dict[str, object]] = {}
        self._stream_url_cache: dict[str, dict[str, object]] = {}
        self._osm_session: tuple[str, str] | None = None  # (session_token, traceId) from latest cached OSM URL
        self._last_epg_ok: float = 0.0  # epoch of last successful EPG API response
        self._cooldown_until: float | None = None
        self._cooldown_reason: str | None = None
        self._description_cache: dict[str, str] = {}       # content_id → description text
        self._description_cache_times: dict[str, float] = {}  # content_id → epoch when cached
        self._load_cached_session()
        self._load_play_id_cache()
        self._load_selector_url_cache()
        self._load_stream_url_cache()
        self._load_403_cooldown()
        self._load_description_cache()

    # ── Session management ─────────────────────────────────────────────────────

    def _session_is_fresh(self) -> bool:
        if not self._csrf_token or not self._session_born:
            return False
        age = time.time() - self._session_born
        return age < _SESSION_HARD_TTL and bool(self.session.cookies)

    def _load_cached_session(self) -> None:
        csrf = (self.config.get("csrf_token") or "").strip()
        born = self.config.get("session_born")
        cookies = self.config.get("session_cookies") or {}
        if not csrf or not isinstance(born, (int, float)) or not isinstance(cookies, dict):
            return
        age = time.time() - float(born)
        if age >= _SESSION_HARD_TTL:
            return
        self._csrf_token = csrf
        self._session_born = float(born)
        self.session.cookies.update(cookies)

    def _persist_session(self) -> None:
        self._update_config("csrf_token", self._csrf_token)
        self._update_config("session_born", self._session_born)
        self._update_config("session_cookies", self.session.cookies.get_dict())

    def _load_403_cooldown(self) -> None:
        until = self.config.get("roku_403_cooldown_until")
        reason = self.config.get("roku_403_cooldown_reason")
        if isinstance(until, (int, float)) and float(until) > time.time():
            self._cooldown_until = float(until)
            self._cooldown_reason = str(reason or "Roku returned 403")

    def _persist_403_cooldown(self) -> None:
        self._update_config("roku_403_cooldown_until", self._cooldown_until)
        self._update_config("roku_403_cooldown_reason", self._cooldown_reason)

    def _cooldown_active(self) -> bool:
        if not self._cooldown_until:
            return False
        if time.time() >= self._cooldown_until:
            self._cooldown_until = None
            self._cooldown_reason = None
            self._persist_403_cooldown()
            return False
        return True

    def _cooldown_remaining(self) -> int:
        if not self._cooldown_until:
            return 0
        return max(0, int(self._cooldown_until - time.time()))

    def _set_403_cooldown(self, reason: str) -> None:
        self._cooldown_until = time.time() + _ROKU_403_COOLDOWN
        self._cooldown_reason = reason
        self._persist_403_cooldown()
        logger.warning("[roku] entering %ds cooldown after 403 (%s)", _ROKU_403_COOLDOWN, reason)

    def _clear_403_cooldown(self) -> None:
        if self._cooldown_until or self._cooldown_reason:
            self._cooldown_until = None
            self._cooldown_reason = None
            self._persist_403_cooldown()

    def _load_play_id_cache(self) -> None:
        raw = self.config.get("play_id_cache") or {}
        if not isinstance(raw, dict):
            return
        now = time.time()
        for station_id, entry in raw.items():
            if not isinstance(station_id, str) or not isinstance(entry, dict):
                continue
            play_id = entry.get("play_id")
            cached_at = entry.get("cached_at")
            if not play_id or not isinstance(cached_at, (int, float)):
                continue
            if (now - float(cached_at)) >= _PLAY_ID_TTL:
                continue
            self._play_id_cache[station_id] = {
                "play_id": play_id,
                "cached_at": float(cached_at),
            }

    def _persist_play_id_cache(self) -> None:
        self._update_config("play_id_cache", self._play_id_cache)

    def _cache_play_id(self, station_id: str, play_id: str | None) -> None:
        if not station_id or not play_id:
            return
        self._play_id_cache[station_id] = {
            "play_id": play_id,
            "cached_at": time.time(),
        }
        self._persist_play_id_cache()

    def _cached_play_id(self, station_id: str) -> str | None:
        entry = self._play_id_cache.get(station_id)
        if not entry:
            return None
        play_id = entry.get("play_id")
        cached_at = entry.get("cached_at")
        if not play_id or not isinstance(cached_at, (int, float)):
            return None
        if (time.time() - float(cached_at)) >= _PLAY_ID_TTL:
            self._play_id_cache.pop(station_id, None)
            self._persist_play_id_cache()
            return None
        return str(play_id)

    def _invalidate_play_id(self, station_id: str) -> None:
        if station_id in self._play_id_cache:
            self._play_id_cache.pop(station_id, None)
            self._persist_play_id_cache()

    def _load_selector_url_cache(self) -> None:
        raw = self.config.get("selector_url_cache") or {}
        if not isinstance(raw, dict):
            return
        now = time.time()
        for station_id, entry in raw.items():
            if not isinstance(station_id, str) or not isinstance(entry, dict):
                continue
            selector_url = entry.get("selector_url")
            cached_at = entry.get("cached_at")
            if not selector_url or not isinstance(cached_at, (int, float)):
                continue
            if (now - float(cached_at)) >= _PLAY_ID_TTL:
                continue
            self._selector_url_cache[station_id] = {
                "selector_url": selector_url,
                "cached_at": float(cached_at),
            }

    def _persist_selector_url_cache(self) -> None:
        self._update_config("selector_url_cache", self._selector_url_cache)

    def _cache_selector_url(self, station_id: str, selector_url: str | None) -> None:
        if not station_id or not selector_url:
            return
        self._selector_url_cache[station_id] = {
            "selector_url": selector_url,
            "cached_at": time.time(),
        }
        self._persist_selector_url_cache()

    def _cached_selector_url(self, station_id: str) -> str | None:
        entry = self._selector_url_cache.get(station_id)
        if not entry:
            return None
        selector_url = entry.get("selector_url")
        cached_at = entry.get("cached_at")
        if not selector_url or not isinstance(cached_at, (int, float)):
            return None
        if (time.time() - float(cached_at)) >= _PLAY_ID_TTL:
            self._selector_url_cache.pop(station_id, None)
            self._persist_selector_url_cache()
            return None
        return str(selector_url)

    def _invalidate_selector_url(self, station_id: str) -> None:
        if station_id in self._selector_url_cache:
            self._selector_url_cache.pop(station_id, None)
            self._persist_selector_url_cache()

    @staticmethod
    def _extract_selector_url(view_opts: list[dict] | None) -> str | None:
        media = (view_opts[0].get("media") or {}) if view_opts else {}
        videos = media.get("videos") or []
        return next(
            (
                video.get("url")
                for video in videos
                if isinstance(video, dict)
                and str(video.get("videoType", "")).upper() == "HLS"
                and video.get("url")
            ),
            None,
        )

    def _load_stream_url_cache(self) -> None:
        raw = self.config.get("stream_url_cache") or {}
        if not isinstance(raw, dict):
            return
        now = time.time()
        best_cached_at = 0.0
        for station_id, entry in raw.items():
            if not isinstance(station_id, str) or not isinstance(entry, dict):
                continue
            stream_url = entry.get("stream_url")
            cached_at = entry.get("cached_at")
            if not stream_url or not isinstance(cached_at, (int, float)):
                continue
            if (now - float(cached_at)) >= _STREAM_URL_TTL:
                continue
            self._stream_url_cache[station_id] = {
                "stream_url": stream_url,
                "cached_at": float(cached_at),
            }
            if float(cached_at) > best_cached_at:
                self._try_update_osm_session(str(stream_url))
                if self._osm_session:
                    best_cached_at = float(cached_at)

    def _persist_stream_url_cache(self) -> None:
        self._update_config("stream_url_cache", self._stream_url_cache)

    def _try_update_osm_session(self, stream_url: str) -> None:
        """Extract session_token+traceId from an OSM stream URL and update _osm_session."""
        m = _OSM_STREAM_RE.search(stream_url)
        if m:
            trace = (parse_qs(urlparse(stream_url).query).get("traceId") or [""])[0]
            self._osm_session = (m.group(2), trace)

    def _cache_stream_url(self, station_id: str, stream_url: str | None) -> None:
        if not station_id or not stream_url:
            return
        self._stream_url_cache[station_id] = {
            "stream_url": stream_url,
            "cached_at": time.time(),
        }
        self._persist_stream_url_cache()
        self._try_update_osm_session(stream_url)

    def _cached_stream_url(self, station_id: str) -> str | None:
        entry = self._stream_url_cache.get(station_id)
        if not entry:
            return None
        stream_url = entry.get("stream_url")
        cached_at = entry.get("cached_at")
        if not stream_url or not isinstance(cached_at, (int, float)):
            return None
        if (time.time() - float(cached_at)) >= _STREAM_URL_TTL:
            self._stream_url_cache.pop(station_id, None)
            self._persist_stream_url_cache()
            return None
        return str(stream_url)

    def _invalidate_stream_url(self, station_id: str) -> None:
        if station_id in self._stream_url_cache:
            self._stream_url_cache.pop(station_id, None)
            self._persist_stream_url_cache()

    def _load_description_cache(self) -> None:
        raw = self.config.get("description_cache") or {}
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                return
        if not isinstance(raw, dict):
            return
        now = time.time()
        for cid, entry in raw.items():
            if not isinstance(entry, dict):
                continue
            desc = entry.get("d")
            t = entry.get("t")
            if not desc or not isinstance(t, (int, float)):
                continue
            if (now - float(t)) >= _DESC_CACHE_TTL:
                continue
            self._description_cache[cid] = str(desc)
            self._description_cache_times[cid] = float(t)
        if len(self._description_cache) > self._DESC_CACHE_MAX_ENTRIES:
            keep = sorted(self._description_cache, key=lambda c: self._description_cache_times.get(c, 0), reverse=True)[:self._DESC_CACHE_MAX_ENTRIES]
            self._description_cache = {c: self._description_cache[c] for c in keep}
            self._description_cache_times = {c: self._description_cache_times[c] for c in keep}

    _DESC_CACHE_MAX_ENTRIES = 5000  # cap to ~1.5MB worst-case; evict oldest first

    def _persist_description_cache(self) -> None:
        cache = self._description_cache
        times = self._description_cache_times
        if len(cache) > self._DESC_CACHE_MAX_ENTRIES:
            keep = sorted(cache, key=lambda c: times.get(c, 0), reverse=True)[:self._DESC_CACHE_MAX_ENTRIES]
            cache = {c: cache[c] for c in keep}
            times = {c: times.get(c, 0) for c in keep}
        serialized = {
            cid: {"d": desc, "t": times.get(cid, time.time())}
            for cid, desc in cache.items()
        }
        # Store as a JSON string, not a dict — merge_config_updates replaces strings
        # but recursively merges dicts, which would prevent expired entries from ever
        # being pruned from Source.config.
        self._update_config("description_cache", json.dumps(serialized))

    def _cache_descriptions(self, new_descs: dict[str, str]) -> None:
        if not new_descs:
            return
        now = time.time()
        for cid, desc in new_descs.items():
            self._description_cache[cid] = desc
            self._description_cache_times[cid] = now
        self._persist_description_cache()

    def _warm_missing_metadata(
        self,
        missing_channels: list[ChannelData],
        headers_snapshot: dict,
        cookies_snapshot: dict,
    ) -> tuple[int, int]:
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if not missing_channels:
            return 0, 0

        thread_local = threading.local()
        warmed_play = 0
        warmed_selector = 0
        lock = threading.Lock()

        def warm_one(ch: ChannelData) -> tuple[str, str | None, str | None]:
            sess = getattr(thread_local, "session", None)
            if sess is None:
                sess = self.new_session(headers=headers_snapshot, cookies=cookies_snapshot)
                thread_local.session = sess
            sess.cookies.update(cookies_snapshot)
            sid = ch.source_channel_id
            qs = "?featureInclude=linearSchedule"
            content_url = _CONTENT_TPL.format(sid=sid) + qs
            proxy_url = _PROXY_BASE + quote(content_url, safe="")
            try:
                response = sess.get(proxy_url, timeout=10)
                if response.status_code != 200:
                    return sid, None, None
                data = response.json()
                view_opts = data.get("viewOptions") or [{}]
                play_id = view_opts[0].get("playId") if view_opts else None
                selector_url = self._extract_selector_url(view_opts)
                return sid, play_id, selector_url
            except Exception:
                return sid, None, None

        with ThreadPoolExecutor(max_workers=_CACHE_WARM_RETRY_WORKERS) as executor:
            futures = {executor.submit(warm_one, ch): ch.source_channel_id for ch in missing_channels}
            for future in as_completed(futures):
                sid, play_id, selector_url = future.result()
                with lock:
                    if play_id:
                        self._cache_play_id(sid, play_id)
                        warmed_play += 1
                    if selector_url:
                        self._cache_selector_url(sid, selector_url)
                        warmed_selector += 1
        return warmed_play, warmed_selector

    def _cached_osm_session(self) -> tuple[str, str] | None:
        """Return (session_token, traceId) from the most recently cached OSM stream URL, or None.

        Updated eagerly on every _cache_stream_url call and on load, so this is O(1).
        """
        return self._osm_session

    def _synthetic_osm_url(self, selector_url: str, session_token: str, trace_id: str) -> str | None:
        """Build a synthetic OSM stream URL by combining selector UUID with a reusable session token."""
        m = _SELECTOR_UUID_RE.search(urlparse(selector_url).path)
        if not m:
            return None
        selector_uuid = m.group(1)
        url = f"{_OSM_BASE}/osm/v1/hls/master/{selector_uuid}/{session_token}/index.m3u8"
        if trace_id:
            url += f"?traceId={trace_id}"
        return url

    def _seed_osm_session(self, channels: list[ChannelData]) -> bool:
        """Call the playback API for one channel to seed _osm_session.

        Used when _osm_session is None (cold start / cache flush) so prewarm
        can proceed without waiting for a user tune.  Makes exactly one
        playback API call.  Returns True if the session was seeded.
        """
        if self._cooldown_active():
            logger.debug("[roku] prewarm seed skipped — cooldown active")
            return False
        if not self._ensure_session():
            logger.debug("[roku] prewarm seed skipped — could not obtain session")
            return False

        session_id = self.session.cookies.get("_usn", "roku-scraper")
        for ch in channels:
            sid = ch.source_channel_id
            play_id = self._cached_play_id(sid)
            selector_url = self._cached_selector_url(sid)
            if not play_id or not selector_url:
                continue
            try:
                decoded = base64.b64decode(play_id.split(".", 1)[1]).decode()
                media_format = "mpeg-dash" if "dash" in decoded.lower() else "m3u"
            except Exception:
                media_format = "m3u"
            if media_format != "m3u":
                continue
            body = {
                "rokuId":      sid,
                "playId":      play_id,
                "mediaFormat": media_format,
                "drmType":     "widevine",
                "quality":     "fhd",
                "bifUrl":      None,
                "adPolicyId":  "",
                "providerId":  "rokuavod",
                "playbackContextParams": (
                    f"sessionId={session_id}"
                    "&pageId=trc-us-live-ml-page-en-current"
                    "&isNewSession=0&idType=roku-trc"
                ),
            }
            r = self._api_post(_PLAYBACK, json_body=body, timeout=10, label=f"prewarm seed for {sid}")
            if r and r.status_code == 200:
                stream_url = r.json().get("url", "")
                if stream_url:
                    self._cache_stream_url(sid, stream_url)
                    logger.info("[roku] prewarm seed: seeded osm_session from %s", sid)
                    return True
            # If this channel failed (404, 401, etc.) try the next one
        logger.warning("[roku] prewarm seed: could not seed osm_session — no channel succeeded")
        return False

    def _validate_stream_url(self, stream_url: str) -> bool:
        try:
            response = self.session.get(stream_url, timeout=8, stream=True)
            if response.status_code != 200:
                logger.debug("[roku] validate_stream_url %s → %d", stream_url, response.status_code)
                response.close()
                return False
            chunk = next(response.iter_content(chunk_size=4096), b"")
            response.close()
            return b"#EXTM3U" in chunk
        except Exception:
            return False

    def _clear_cached_session(self) -> None:
        self._csrf_token = None
        self._session_born = None
        self.session.cookies.clear()
        self._update_config("csrf_token", None)
        self._update_config("session_born", None)
        self._update_config("session_cookies", {})

    @staticmethod
    def _live_tv_headers() -> dict:
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Cache-Control": "max-age=0",
            "Pragma": "no-cache",
            "Referer": _HOME,
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }

    def _refresh_session(self) -> bool:
        """Boot a fresh Roku browser session. Returns True on success."""
        if self._cooldown_active():
            logger.warning(
                "[roku] bootstrap cooldown active for %ds (%s)",
                self._cooldown_remaining(),
                self._cooldown_reason or "Roku returned 403",
            )
            return False
        try:
            self._clear_cached_session()
            # Step 1: hit home page to collect cookies. /live-tv is intermittently
            # blocked by CloudFront, but the root page yields the same anonymous
            # cookies and works for csrf + API bootstrap.
            r1 = None
            for attempt in range(_LIVE_TV_403_RETRIES + 1):
                r1 = self.session.get(_HOME, headers=self._live_tv_headers(), timeout=15)
                if r1.status_code == 200:
                    self._clear_403_cooldown()
                    break
                if r1.status_code == 403:
                    self._set_403_cooldown("bootstrap")
                    self._log_bootstrap_403(r1)
                logger.error("[roku] home bootstrap returned %d", r1.status_code)
                return False

            # Step 2: fetch csrf token (retry up to 4 times)
            csrf = None
            for attempt in range(5):
                r2 = self.session.get(_CSRF_URL, timeout=10)
                if r2.status_code == 200:
                    csrf = r2.json().get("csrf")
                    break
                wait = 2 ** attempt
                logger.warning("[roku] csrf attempt %d returned %d, retry in %ds",
                               attempt + 1, r2.status_code, wait)
                time.sleep(wait)

            if not csrf:
                logger.error("[roku] could not obtain csrf token")
                return False

            self._csrf_token   = csrf
            self._session_born = time.time()
            self._persist_session()
            logger.debug("[roku] session refreshed, csrf=%s…", csrf[:12])
            return True

        except Exception as exc:
            if is_transient_network_error(exc):
                raise
            logger.error("[roku] session refresh failed: %s", exc)
            return False

    def _ensure_session(self) -> bool:
        if not self._session_is_fresh():
            return self._refresh_session()
        if self._session_born and (time.time() - self._session_born) >= _SESSION_TTL:
            logger.debug("[roku] reusing cached session older than soft TTL; will refresh only if Roku rejects it")
        return True

    def _api_get(self, url: str, *, timeout: int, label: str) -> Optional[object]:
        for attempt in range(2):
            headers = self._api_headers()
            response = self.session.get(url, headers=headers, timeout=timeout)
            if response.status_code == 403:
                self._set_403_cooldown(label)
                return response
            if response.status_code not in (401, 403) or attempt == 1:
                return response
            logger.warning("[roku] %s returned %d, refreshing session and retrying once",
                           label, response.status_code)
            if not self._refresh_session():
                return response
        return None

    def _api_post(self, url: str, *, json_body: dict, timeout: int, label: str):
        for attempt in range(2):
            headers = self._api_headers()
            response = self.session.post(url, headers=headers, json=json_body, timeout=timeout)
            if response.status_code == 403:
                self._set_403_cooldown(label)
                return response
            if response.status_code not in (401, 403) or attempt == 1:
                return response
            logger.warning("[roku] %s returned %d, refreshing session and retrying once",
                           label, response.status_code)
            if not self._refresh_session():
                return response
        return None

    @staticmethod
    def _log_bootstrap_403(response) -> None:
        body = ""
        try:
            body = (response.text or "").strip().replace("\n", " ").replace("\r", " ")
        except Exception:
            body = ""
        if len(body) > 160:
            body = body[:160] + "..."
        logger.warning(
            "[roku] bootstrap 403 details: cf_pop=%s x_cache=%s server=%s content_type=%s body=%r",
            response.headers.get("x-amz-cf-pop"),
            response.headers.get("x-cache"),
            response.headers.get("server"),
            response.headers.get("content-type"),
            body,
        )

    def _api_headers(self) -> dict:
        return {
            "csrf-token":                         self._csrf_token or "",
            "origin":                             _BASE,
            "referer":                            _HOME,
            "content-type":                       "application/json",
            "x-roku-reserved-amoeba-ids":         "",
            "x-roku-reserved-experiment-configs": "e30=",
            "x-roku-reserved-experiment-state":   "W10=",
            "x-roku-reserved-lat":                "0",
        }

    # ── Content proxy helper ───────────────────────────────────────────────────

    def _fetch_content(self, station_id: str, feature_include: str = "", _raise_on_404: bool = False) -> Optional[dict]:
        """Call the therokuchannel content proxy for a given station_id."""
        qs = f"?featureInclude={feature_include}" if feature_include else ""
        content_url = _CONTENT_TPL.format(sid=station_id) + qs
        proxy_url   = _PROXY_BASE + quote(content_url, safe="")
        try:
            r = self._api_get(proxy_url, timeout=10, label=f"content proxy for {station_id}")
            if r.status_code == 200:
                return r.json()
            if _raise_on_404 and r.status_code == 404:
                raise StreamDeadError(f"[roku] channel not found (404): {station_id}")
        except StreamDeadError:
            raise
        except Exception as exc:
            logger.warning("[roku] content fetch error for %s: %s", station_id, exc)
        return None

    # ── fetch_channels ─────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        if not self._ensure_session():
            raise ScrapeSkipError("[roku] session bootstrap failed; keeping previous channel data")

        channels: list[ChannelData] = []
        seen: set[str] = set()

        # ── Phase 1: /api/v2/epg — returns all ~795 live channels ─────────────
        # Each collection item has features.station with full channel metadata.
        try:
            r = self._api_get(_EPG_URL, timeout=20, label="epg")
            if r.status_code == 200:
                self._last_epg_ok = time.time()
                for col in r.json().get("collections", []):
                    station = col.get("features", {}).get("station")
                    if not station:
                        continue
                    sid = station.get("meta", {}).get("id")
                    if not sid or sid in seen:
                        continue
                    self._add_channel_from_station(channels, seen, sid, station)
            else:
                logger.warning("[roku] EPG returned %d", r.status_code)
        except Exception as exc:
            logger.warning("[roku] EPG fetch failed: %s", exc)

        # ── Phase 2: billboard (hero channels, fills any EPG gaps) ────────────
        try:
            r2 = self.session.get(
                f"{_BASE}/api/v1/billboard/landing/trc-us-live-ml-page-en-current",
                headers=self._api_headers(),
                timeout=10,
            )
            if r2.status_code == 200:
                for item in r2.json():
                    sid = (item.get("meta") or {}).get("id")
                    if not sid or sid in seen:
                        continue
                    self._add_channel_from_content(channels, seen, sid, item)
        except Exception as exc:
            logger.warning("[roku] billboard fetch failed: %s", exc)

        if not channels:
            logger.error("[roku] fetch_channels returned 0 channels — session may be bad")
            raise ScrapeSkipError("[roku] channel fetch returned 0 channels; keeping previous channel data")

        logger.info("[roku] %d channels fetched", len(channels))

        return channels

    def _add_channel_from_station(
        self,
        channels: list[ChannelData],
        seen: set[str],
        station_id: str,
        station: dict,
    ) -> None:
        """Add a channel parsed from an EPG features.station object."""
        seen.add(station_id)

        title  = station.get("title") or station.get("shortName") or "Unknown"
        number = station.get("displayNumber")

        # Logo: prefer gridEpg → epgLogo → liveHudLogo
        logo = None
        image_map = station.get("imageMap") or {}
        for key in ("gridEpg", "epgLogo", "liveHudLogo", "epgLogoDark"):
            img = image_map.get(key)
            if img and img.get("path"):
                logo = img["path"]
                break

        # Category from kidsDirected flag or tags
        category = _category_from_station(station)
        tags_str = ' '.join(station.get('tags') or [])

        channels.append(ChannelData(
            source_channel_id = station_id,
            name              = title,
            stream_url        = f"roku://{station_id}",
            logo_url          = logo,
            category          = category,
            language          = _language_from_metadata(title, category, tags_str),
            country           = "US",
            stream_type       = "hls",
            number            = number,
            slug              = f"|",  # playId resolved on demand; no gracenoteId from EPG
        ))

    def _add_channel_from_content(
        self,
        channels: list[ChannelData],
        seen: set[str],
        station_id: str,
        item: dict,
    ) -> None:
        """Add a channel parsed from a content-proxy / billboard item."""
        seen.add(station_id)

        title = item.get("title", "Unknown")

        # Logo: prefer grid thumbnail
        logo = None
        image_map = item.get("imageMap") or {}
        for key in ("grid", "gridEpg", "detailBackground", "detailPoster"):
            img = image_map.get(key)
            if img and img.get("path"):
                logo = img["path"]
                break

        # Category from categories list
        cats = item.get("categories") or []
        category = _cat_id_to_label(cats[0]) if cats else None

        # playId from viewOptions
        view_opts = item.get("viewOptions") or [{}]
        play_id   = view_opts[0].get("playId") if view_opts else None
        selector_url = self._extract_selector_url(view_opts)
        self._cache_play_id(station_id, play_id)
        self._cache_selector_url(station_id, selector_url)

        # Gracenote station ID (numeric stationId in the EPG schedule)
        gracenote_id = item.get("gracenoteStationId") or item.get("stationId") or ""
        if gracenote_id and not str(gracenote_id).isdigit():
            gracenote_id = ""

        channels.append(ChannelData(
            source_channel_id = station_id,
            name              = title,
            stream_url        = f"roku://{station_id}",
            logo_url          = logo,
            category          = category,
            language          = _language_from_metadata(title, category),
            country           = "US",
            stream_type       = "hls",
            slug              = f"{play_id or ''}|{gracenote_id}",
        ))

    # ── fetch_epg ──────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], skip_ids: set[str] | None = None) -> list[ProgramData]:
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if not self._ensure_session():
            raise ScrapeSkipError("[roku] session bootstrap failed before EPG fetch; keeping previous EPG data")

        # Validate the cached session against a real Roku API before starting
        # the threaded content-proxy fanout. Otherwise an upstream-expired
        # session can yield a misleading "0 programs" success on EPG-only runs.
        # Skip probe if fetch_channels() already confirmed the session within 60s.
        if (time.time() - self._last_epg_ok) > 60:
            epg_probe = self._api_get(_EPG_URL, timeout=20, label="epg")
            if not epg_probe or epg_probe.status_code != 200:
                logger.warning("[roku] EPG validation returned %s before threaded fetch",
                               getattr(epg_probe, "status_code", "no response"))
                raise ScrapeSkipError("[roku] session rejected before EPG fetch; keeping previous EPG data")
            self._last_epg_ok = time.time()

        total = len(channels)
        skipped = len(skip_ids & {ch.source_channel_id for ch in channels}) if skip_ids else 0
        if skipped:
            logger.info("[roku] EPG skip: %d/%d channels have fresh programs, skipping content proxy", skipped, total)
        # Snapshot merged headers (session defaults + API-specific) and cookies
        # so each worker thread can reuse its own independent session without
        # mutating the shared scraper session or opening a fresh pool per task.
        headers_snapshot = {**self.session.headers, **self._api_headers()}
        cookies_snapshot  = self.session.cookies.get_dict()

        programs: list[ProgramData] = []
        # Map content_id → programs within 48h that need a description backfill
        cid_to_progs: dict[str, list[ProgramData]] = {}
        lock = threading.Lock()
        thread_local = threading.local()
        done = [0]

        cutoff_48h = datetime.now(timezone.utc) + timedelta(hours=48)

        def fetch_one(ch: ChannelData) -> tuple[list[ProgramData], dict, str | None, str | None]:
            sess = getattr(thread_local, "session", None)
            if sess is None:
                sess = self.new_session(headers=headers_snapshot, cookies=cookies_snapshot)
                thread_local.session = sess
            sess.cookies.update(cookies_snapshot)
            sid = ch.source_channel_id
            if skip_ids and sid in skip_ids:
                return [], {}, self._cached_play_id(sid), self._cached_selector_url(sid)
            try:
                qs = "?featureInclude=linearSchedule"
                content_url = _CONTENT_TPL.format(sid=sid) + qs
                proxy_url   = _PROXY_BASE + quote(content_url, safe="")
                r = sess.get(proxy_url, timeout=10)
                if r.status_code != 200:
                    logger.debug("[roku] content proxy returned %d for %s", r.status_code, sid)
                    return [], {}, None, None
                data = r.json()
                view_opts = data.get("viewOptions") or [{}]
                play_id = view_opts[0].get("playId") if view_opts else None
                selector_url = self._extract_selector_url(view_opts)
                schedule = data.get("features", {}).get("linearSchedule", [])
                result = []
                local_cid_map: dict[str, list[ProgramData]] = {}
                for entry in schedule:
                    prog = self._parse_program(sid, entry)
                    if not prog:
                        continue
                    result.append(prog)
                    # Track content_id for programs in the 48h window so we
                    # can backfill descriptions in a second pass.
                    if prog.start_time <= cutoff_48h:
                        cid = (entry.get("content") or {}).get("meta", {}).get("id")
                        if cid:
                            local_cid_map.setdefault(cid, []).append(prog)
                return result, local_cid_map, play_id, selector_url
            except Exception as exc:
                if is_transient_network_error(exc):
                    raise
                logger.warning("[roku] EPG error for %s (%s): %s", ch.name, sid, exc)
                return [], {}, None, None

        with ThreadPoolExecutor(max_workers=_EPG_WORKERS) as executor:
            futures = {executor.submit(fetch_one, ch): ch for ch in channels}
            for future in as_completed(futures):
                exc = future.exception()
                if exc and type(exc).__name__ == 'JobTimeoutException':
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise exc
                if exc and is_transient_network_error(exc):
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise exc
                result, local_cid_map, play_id, selector_url = future.result() if not exc else ([], {}, None, None)
                with lock:
                    programs.extend(result)
                    for cid, progs in local_cid_map.items():
                        cid_to_progs.setdefault(cid, []).extend(progs)
                    self._cache_play_id(futures[future].source_channel_id, play_id)
                    self._cache_selector_url(futures[future].source_channel_id, selector_url)
                    done[0] += 1
                    if self._progress_cb:
                        self._progress_cb('epg', done[0], total)

        missing_channels = [
            ch for ch in channels
            if not self._cached_play_id(ch.source_channel_id) or not self._cached_selector_url(ch.source_channel_id)
        ]
        retried_play = 0
        retried_selector = 0
        if missing_channels:
            retried_play, retried_selector = self._warm_missing_metadata(
                missing_channels,
                headers_snapshot,
                cookies_snapshot,
            )

        # ── Description backfill for 48h window ───────────────────────────────
        if cid_to_progs:
            uncached_cids = [cid for cid in cid_to_progs if cid not in self._description_cache]
            if uncached_cids:
                new_descs = self._fetch_descriptions(uncached_cids, headers_snapshot, cookies_snapshot)
                self._cache_descriptions(new_descs)
            filled = 0
            for cid, progs in cid_to_progs.items():
                desc = self._description_cache.get(cid)
                if desc:
                    for prog in progs:
                        if not prog.description:
                            prog.description = desc
                            filled += 1
            logger.info(
                "[roku] description backfill: %d unique IDs (%d cached, %d fetched) → %d programs filled",
                len(cid_to_progs),
                len(cid_to_progs) - len(uncached_cids),
                len(uncached_cids),
                filled,
            )

        programs.sort(key=lambda p: (p.source_channel_id, p.start_time))
        play_cached = selector_cached = stream_cached = 0
        for ch in channels:
            sid = ch.source_channel_id
            if self._cached_play_id(sid):
                play_cached += 1
            if self._cached_selector_url(sid):
                selector_cached += 1
            if self._cached_stream_url(sid):
                stream_cached += 1
        if not self._cached_osm_session():
            self._seed_osm_session(channels)
        logger.info(
            "[roku] cache warm summary: play_id=%d/%d selector=%d/%d stream_url=%d/%d retry_play=%d retry_selector=%d",
            play_cached,
            total,
            selector_cached,
            total,
            stream_cached,
            total,
            retried_play,
            retried_selector,
        )
        logger.info("[roku] %d EPG entries fetched for %d channels", len(programs), total)
        return programs

    def _fetch_descriptions(
        self,
        content_ids: list[str],
        headers_snapshot: dict,
        cookies_snapshot: dict,
    ) -> dict[str, str]:
        """Fetch program descriptions in parallel via the content proxy."""
        import requests as _req
        from concurrent.futures import ThreadPoolExecutor, as_completed

        desc_map: dict[str, str] = {}
        lock = __import__('threading').Lock()

        def fetch_desc(cid: str):
            sess = _req.Session()
            sess.headers.update(headers_snapshot)
            sess.cookies.update(cookies_snapshot)
            prog_url  = f"https://content.sr.roku.com/content/v1/roku-trc/{cid}"
            proxy_url = _PROXY_BASE + quote(prog_url, safe="")
            try:
                r = sess.get(proxy_url, timeout=10)
                if r.status_code == 200:
                    d = r.json()
                    descs = d.get("descriptions") or {}
                    desc = None
                    for key in ("250", "100", "60"):
                        entry = descs.get(key)
                        if entry:
                            desc = entry.get("text") if isinstance(entry, dict) else entry
                            break
                    if not desc:
                        desc = d.get("description")
                    if desc:
                        return cid, str(desc)
            except Exception:
                pass
            return cid, None

        with ThreadPoolExecutor(max_workers=_DESC_WORKERS) as executor:
            for cid, desc in executor.map(fetch_desc, content_ids):
                if desc:
                    with lock:
                        desc_map[cid] = desc

        return desc_map

    def _parse_program(self, station_id: str, entry: dict) -> Optional[ProgramData]:
        try:
            start_str = entry.get("date", "")
            start = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            duration = entry.get("duration", 0)
            end = start + timedelta(seconds=duration)
        except (ValueError, TypeError):
            return None

        c       = entry.get("content", {})
        series  = c.get("series", {})
        ep_title = c.get("title", "")
        series_title = series.get("title", "")
        title   = series_title or ep_title or "Unknown"

        # Description
        descs = c.get("descriptions") or {}
        description = (
            descs.get("250") or descs.get("60") or descs.get("40") or c.get("description")
        )

        # Artwork — prefer gridEpg, fall back to grid
        image_map = c.get("imageMap") or {}
        poster = (
            (image_map.get("gridEpg") or {}).get("path")
            or (image_map.get("grid") or {}).get("path")
        )

        # Rating
        ratings = c.get("parentalRatings") or []
        rating  = ratings[0].get("code") if ratings else None

        # Season / Episode
        season  = c.get("seasonNumber")
        episode = c.get("episodeNumber")
        try:
            season  = int(season)  if season  else None
            episode = int(episode) if episode else None
        except (ValueError, TypeError):
            season = episode = None

        # Category from genres
        genres = c.get("genres") or []
        category = _join_categories(genres)

        return ProgramData(
            source_channel_id = station_id,
            title             = title,
            start_time        = start,
            end_time          = end,
            description       = description,
            poster_url        = poster,
            category          = category,
            rating            = rating,
            episode_title     = ep_title if series_title and ep_title != series_title else None,
            season            = season,
            episode           = episode,
        )

    # ── resolve ────────────────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        """
        raw_url format: roku://{station_id}
        Returns a live osm.sr.roku.com HLS/DASH stream URL.
        Calls /api/v3/playback with a fresh session each time.
        The JWT in the stream URL is short-lived so we always fetch fresh.
        """
        if not raw_url.startswith("roku://"):
            return raw_url

        station_id = raw_url[len("roku://"):]
        had_play_id = False
        had_selector_url = False
        need_content_details = False
        failure_stage = "cache"

        try:
            cached_stream_url = self._cached_stream_url(station_id)
            if cached_stream_url:
                logger.info("[roku] resolve %s via stream_url cache", station_id)
                return cached_stream_url

            if self._cooldown_active():
                raise RuntimeError(
                    f"[roku] resolve blocked by temporary 403 cooldown for {self._cooldown_remaining()}s"
                )

            failure_stage = "bootstrap"
            if not self._ensure_session():
                raise RuntimeError(f"[roku] resolve failed — could not obtain session for {station_id}")

            # Step 1: prefer cached playId to avoid content lookups on tune.
            failure_stage = "content"
            content_data = None
            play_id = self._cached_play_id(station_id)
            selector_url = self._cached_selector_url(station_id)
            had_play_id = bool(play_id)
            had_selector_url = bool(selector_url)
            need_content_details = not play_id
            if need_content_details:
                content_data = self._fetch_content(station_id, _raise_on_404=True)
                if content_data:
                    view_opts = content_data.get("viewOptions") or [{}]
                    play_id = view_opts[0].get("playId") if view_opts else None
                    self._cache_play_id(station_id, play_id)
                    selector_url = self._extract_selector_url(view_opts)
                    self._cache_selector_url(station_id, selector_url)

            if not play_id:
                # Try regex fallback from raw response
                content_url = _CONTENT_TPL.format(sid=station_id)
                proxy_url   = _PROXY_BASE + quote(content_url, safe="")
                try:
                    r = self._api_get(proxy_url, timeout=10, label=f"content fallback for {station_id}")
                    pids = re.findall(r's-[a-z0-9_]+\.[A-Za-z0-9+/=]+', r.text)
                    play_id = pids[0] if pids else None
                    self._cache_play_id(station_id, play_id)
                except Exception:
                    pass

            if not play_id:
                logger.warning("[roku] no playId found for %s", station_id)
                raise RuntimeError(f"[roku] no playId found for {station_id}")

            # Decode to determine media format
            try:
                decoded = base64.b64decode(play_id.split(".", 1)[1]).decode()
                media_format = "mpeg-dash" if "dash" in decoded.lower() else "m3u"
            except Exception:
                media_format = "m3u"

            # Step 2: try synthesizing from a cached OSM session token (no API call)
            if media_format == "m3u" and selector_url:
                osm_session = self._cached_osm_session()
                if osm_session:
                    session_token, _ = osm_session
                    synthetic = self._synthetic_osm_url(selector_url, session_token, str(uuid.uuid4()))
                    if synthetic and self._validate_stream_url(synthetic):
                        self._cache_play_id(station_id, play_id)
                        self._cache_selector_url(station_id, selector_url)
                        self._cache_stream_url(station_id, synthetic)
                        logger.info(
                            "[roku] resolve %s via synthetic_osm play_id_cache=%s selector_cache=%s content_lookup=%s",
                            station_id, had_play_id, had_selector_url, need_content_details,
                        )
                        return synthetic

            # Step 3: call /api/v3/playback
            failure_stage = "playback"
            session_id = self.session.cookies.get("_usn", "roku-scraper")
            body = {
                "rokuId":      station_id,
                "playId":      play_id,
                "mediaFormat": media_format,
                "drmType":     "widevine",
                "quality":     "fhd",
                "bifUrl":      None,
                "adPolicyId":  "",
                "providerId":  "rokuavod",
                "playbackContextParams": (
                    f"sessionId={session_id}"
                    "&pageId=trc-us-live-ml-page-en-current"
                    "&isNewSession=0&idType=roku-trc"
                ),
            }
            r2 = self._api_post(_PLAYBACK, json_body=body, timeout=10, label=f"playback for {station_id}")
            if r2.status_code == 200:
                stream_url = r2.json().get("url", "")
                if stream_url:
                    self._persist_session()
                    self._cache_play_id(station_id, play_id)
                    self._cache_selector_url(station_id, selector_url)
                    self._cache_stream_url(station_id, stream_url)
                    logger.info(
                        "[roku] resolve %s via playback_api play_id_cache=%s selector_cache=%s content_lookup=%s",
                        station_id,
                        had_play_id,
                        had_selector_url,
                        need_content_details,
                    )
                    return stream_url
            if r2.status_code in (401, 403, 404):
                self._invalidate_play_id(station_id)
                self._invalidate_selector_url(station_id)
                self._invalidate_stream_url(station_id)
            raise RuntimeError(f"[roku] playback returned {r2.status_code} for {station_id}")
        except RuntimeError:
            logger.warning(
                "[roku] resolve %s failed stage=%s play_id_cache=%s selector_cache=%s content_lookup=%s",
                station_id,
                failure_stage,
                had_play_id,
                had_selector_url,
                need_content_details,
            )
            raise
        except Exception as exc:
            self._invalidate_stream_url(station_id)
            logger.warning(
                "[roku] resolve %s failed stage=%s play_id_cache=%s selector_cache=%s content_lookup=%s",
                station_id,
                failure_stage,
                had_play_id,
                had_selector_url,
                need_content_details,
            )
            raise RuntimeError(f"[roku] playback request failed for {station_id}: {exc}") from exc

    # ── M3U extras ─────────────────────────────────────────────────────────────
    # FastChannels calls generate_m3u() which uses ChannelData fields.
    # We stuffed "playId|gracenoteId" into slug. Override the M3U line builder
    # to emit tvc-guide-stationid for channels that have a Gracenote ID.
    # BaseScraper's generate_m3u() calls channel_m3u_tags() if it exists.

    def channel_m3u_tags(self, ch: ChannelData) -> dict[str, str]:
        """
        Return extra M3U tags for this channel.
        Called by BaseScraper.generate_m3u() if the method exists.
        """
        tags: dict[str, str] = {}

        # Unpack gracenoteId from slug field (format: "playId|gracenoteId")
        if ch.slug and "|" in ch.slug:
            _, gracenote_id = ch.slug.split("|", 1)
            if gracenote_id and gracenote_id.isdigit():
                # This tells Channels DVR to pull full guide data from Gracenote
                tags["tvc-guide-stationid"] = gracenote_id

        return tags

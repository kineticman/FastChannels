"""
DistroTV scraper for FastChannels.
No config fields required — anonymous public API.
"""
from __future__ import annotations

import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from .base import BaseScraper, ChannelData, ProgramData

logger = logging.getLogger(__name__)

FEED_URL = "https://tv.jsrdn.com/tv_v5/getfeed.php?type=live"
EPG_URL  = "https://tv.jsrdn.com/epg/query.php"

ANDROID_UA = "Dalvik/2.1.0 (Linux; U; Android 9; AFTT Build/STT9.221129.002) GTV/AFTT DistroTV/2.0.9"
BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

HLS_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Origin":     "https://distro.tv",
    "Referer":    "https://distro.tv/",
}

SESSION_CDN_HOSTS = {
    "d3s7x6kmqcnb6b.cloudfront.net",   # session-token CDN
    "d35j504z0x2vu2.cloudfront.net",   # requires Origin/Referer headers — don't pre-fetch
}

MACRO_RE = re.compile(r"__[^_].*?__")

MACRO_REPLACEMENTS = {
    "__CACHE_BUSTER__":           lambda: str(int(time.time() * 1000)),
    "__DEVICE_ID__":              lambda: str(uuid.uuid4()),
    "__LIMIT_AD_TRACKING__":      lambda: "0",
    "__IS_GDPR__":                lambda: "0",
    "__IS_CCPA__":                lambda: "0",
    "__GEO_COUNTRY__":            lambda: "US",
    "__LATITUDE__":               lambda: "",
    "__LONGITUDE__":              lambda: "",
    "__GEO_DMA__":                lambda: "",
    "__GEO_TYPE__":               lambda: "",
    "__PAGEURL_ESC__":            lambda: "https%3A%2F%2Fdistro.tv%2F",
    "__STORE_URL__":              lambda: "https%3A%2F%2Fdistro.tv%2F",
    "__APP_BUNDLE__":             lambda: "distro.tv",
    "__APP_VERSION__":            lambda: "0",
    "__APP_CATEGORY__":           lambda: "",
    "__WIDTH__":                  lambda: "1920",
    "__HEIGHT__":                 lambda: "1080",
    "__DEVICE__":                 lambda: "Linux",
    "__DEVICE_ID_TYPE__":         lambda: "uuid",
    "__DEVICE_CONNECTION_TYPE__": lambda: "",
    "__DEVICE_CATEGORY__":        lambda: "desktop",
    "__env.i__":                  lambda: "web",
    "__env.u__":                  lambda: "web",
    "__PALN__":                   lambda: "",
    "__GDPR_CONSENT__":           lambda: "",
    "__ADVERTISING_ID__":         lambda: "",
    "__CLIENT_IP__":              lambda: "",
}

# Tags that indicate language/region rather than content genre.
# These are split out into the channel's language field, not the category.
_LANG_TAGS = frozenset({
    'English', 'Spanish', 'Asian', 'African', 'Arabic', 'Middle Eastern',
    'French', 'Portuguese', 'Hindi', 'Urdu', 'Korean', 'Japanese',
    'Chinese', 'Tagalog', 'Vietnamese', 'Russian',
})

# Map Distro region labels → ISO 639-1 language codes where unambiguous.
# 'Asian' and 'African' are regional, not a single language — stored as-is.
_LANG_CODE = {
    'English':        'en',
    'Spanish':        'es',
    'French':         'fr',
    'Portuguese':     'pt',
    'Hindi':          'hi',
    'Urdu':           'ur',
    'Korean':         'ko',
    'Japanese':       'ja',
    'Chinese':        'zh',
    'Tagalog':        'tl',
    'Vietnamese':     'vi',
    'Russian':        'ru',
    'Arabic':         'ar',
}


_DISTRO_CATEGORY_MAP = {
    # Top-level tag → normalized label
    'News':          'News',
    'Sports':        'Sports',
    'Music':         'Music',
    'Lifestyle':     'Lifestyle',
    'Documentary':   'Documentary',
    'Education':     'Science',
    'Travel':        'Travel',
    'Finance':       'Business',
    'Business':      'Business',
    'Fun & Games':   'Gaming',
}

# When top-level is "Entertainment", use the second tag to refine
_DISTRO_ENTERTAINMENT_MAP = {
    'Movies':            'Movies',
    'Classic Movies':    'Movies',
    'Drama':             'Drama',
    'Comedy':            'Comedy',
    'Horror':            'Horror',
    'Thriller':          'Horror',
    'Action/Adventure':  'Action',
    'Animation & Anime': 'Anime',
    'True Crime':        'True Crime',
    'Western':           'Westerns',
    'Reality TV':        'Reality TV',
    'Talk Show':         'Reality TV',
    'Bollywood':         'Bollywood',
    'Hindi GEC':         'Drama',
    'Circus':            'Entertainment',
    'Pop Culture':       'Entertainment',
    'Infotainment':      'Entertainment',
    'Food':              'Food',
    'Fashion':           'Lifestyle',
    'Family/Children':   'Kids',
}


def _parse_distro_tags(raw: str) -> tuple[Optional[str], str]:
    """
    Parse Distro's comma-joined tag string into (category, language).

    Distro stores everything in one field, e.g.:
      'News,Current Affairs,Politics,Asian'
      'Entertainment,Classic Movies,English'
      'Music,Music Video,Contemporary Hits/Pop/Top 40,Hip Hop Music,Spanish'

    Returns:
      category — normalized single-label category
      language — ISO 639-1 code from the first recognised language tag,
                 or the raw label if no mapping exists (e.g. 'Asian'),
                 defaulting to 'en' if none found.
    """
    if not raw:
        return None, 'en'

    tags = [t.strip() for t in raw.split(',') if t.strip()]

    genre_tags = []
    lang       = 'en'
    lang_found = False

    for tag in tags:
        if tag in _LANG_TAGS:
            if not lang_found:
                lang       = _LANG_CODE.get(tag, tag.lower())
                lang_found = True
        else:
            genre_tags.append(tag)

    if not genre_tags:
        return None, lang

    primary = genre_tags[0]
    secondary = genre_tags[1] if len(genre_tags) > 1 else None

    if primary == 'Entertainment' and secondary:
        category = _DISTRO_ENTERTAINMENT_MAP.get(secondary, 'Entertainment')
    else:
        category = _DISTRO_CATEGORY_MAP.get(primary, primary)

    return category, lang


def _sanitize_url(url: str) -> str:
    parts = urlsplit(url)
    q = parse_qsl(parts.query, keep_blank_values=True)
    sanitized = []
    for k, v in q:
        if v in MACRO_REPLACEMENTS:
            v = MACRO_REPLACEMENTS[v]()
        elif MACRO_RE.search(v or ""):
            v = ""
        sanitized.append((k, v))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(sanitized, doseq=True), ""))


def _pick_best_variant(master_text: str, master_url: str) -> Optional[str]:
    lines   = [ln.strip() for ln in master_text.splitlines() if ln.strip()]
    best_bw = -1
    best_uri: Optional[str] = None
    for i, ln in enumerate(lines):
        if not ln.startswith("#EXT-X-STREAM-INF:"):
            continue
        bw = -1
        for part in (ln.split(":", 1)[1] if ":" in ln else "").split(","):
            if part.startswith("BANDWIDTH="):
                try:
                    bw = int(part.split("=", 1)[1])
                except Exception:
                    pass
                break
        j = i + 1
        while j < len(lines) and lines[j].startswith("#"):
            j += 1
        if j >= len(lines):
            continue
        abs_uri = urljoin(master_url, lines[j])
        if bw > best_bw:
            best_bw  = bw
            best_uri = abs_uri
    return best_uri


def _iter_shows(feed: object):
    if isinstance(feed, dict):
        shows = feed.get("shows")
        if isinstance(shows, dict):
            yield from (s for s in shows.values() if isinstance(s, dict))
            return
        if isinstance(shows, list):
            yield from (s for s in shows if isinstance(s, dict))
            return
        for key in ("data", "items", "results"):
            v = feed.get(key)
            if isinstance(v, list):
                yield from (s for s in v if isinstance(s, dict))
                return
        if "type" in feed and "title" in feed:
            yield feed
            return
    if isinstance(feed, list):
        yield from (s for s in feed if isinstance(s, dict))


class DistroScraper(BaseScraper):
    source_name     = "distro"
    display_name    = "Distro TV"
    stream_audit_enabled = True
    scrape_interval = 720
    config_schema   = []

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update({
            "User-Agent": ANDROID_UA,
            "Accept":     "application/json,*/*",
        })

    def fetch_channels(self) -> list[ChannelData]:
        r = self.get(FEED_URL)
        if not r:
            return []
        try:
            feed = r.json()
        except Exception as e:
            logger.error("[distro] feed JSON decode failed: %s", e)
            return []

        channels = []
        for show in _iter_shows(feed):
            if show.get("type") != "live":
                continue
            name     = (show.get("title") or "").strip()
            logo     = (show.get("img_logo") or "").strip()
            seasons  = show.get("seasons") or []
            if not seasons or not isinstance(seasons[0], dict):
                continue
            episodes = seasons[0].get("episodes") or []
            if not episodes or not isinstance(episodes[0], dict):
                continue
            ep           = episodes[0]
            tvg_id       = ep.get("id")
            content      = ep.get("content") or {}
            upstream_url = content.get("url")
            if not name or not tvg_id or not upstream_url:
                continue

            raw_genre        = (show.get("genre") or "").strip()
            category, lang   = _parse_distro_tags(raw_genre)

            channels.append(ChannelData(
                source_channel_id = str(tvg_id),
                name              = name,
                stream_url        = upstream_url,
                stream_type       = "hls",
                logo_url          = logo or None,
                category          = category,
                language          = lang,
                country           = "US",
            ))

        logger.info("[distro] parsed %d channels", len(channels))
        return channels

    def fetch_epg(self, channels: list[ChannelData]) -> list[ProgramData]:
        if not channels:
            return []
        all_ids = ",".join(ch.source_channel_id for ch in channels)
        r = self.get(
            f"{EPG_URL}?id={all_ids}&range=now,24h",
            headers={"User-Agent": ANDROID_UA, "Accept": "application/json,*/*"},
        )
        if not r:
            return []
        try:
            raw_epg = r.json().get("epg") or {}
        except Exception as e:
            logger.warning("[distro] EPG JSON parse failed: %s", e)
            return []

        programs = []
        for ch_id, ch_epg in raw_epg.items():
            for slot in (ch_epg.get("slots") or []):
                try:
                    start = datetime.strptime(slot["start"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    end   = datetime.strptime(slot["end"],   "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except (KeyError, ValueError):
                    continue
                programs.append(ProgramData(
                    source_channel_id = ch_id,
                    title             = (slot.get("title") or "").strip() or "Unknown",
                    description       = (slot.get("description") or "").strip() or None,
                    start_time        = start,
                    end_time          = end,
                    poster_url        = slot.get("img_thumbh") or None,
                ))
        logger.info("[distro] parsed %d EPG entries", len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        sanitized = _sanitize_url(raw_url)
        host = urlsplit(sanitized).netloc
        if host in SESSION_CDN_HOSTS:
            return sanitized
        try:
            r = self.session.get(sanitized, headers=HLS_HEADERS, timeout=15)
            r.raise_for_status()
            text = r.text or ""
            if "#EXT-X-STREAM-INF" in text:
                variant = _pick_best_variant(text, sanitized)
                return variant or sanitized
            return sanitized
        except Exception as e:
            logger.warning("[distro] resolve failed, serving sanitized URL: %s", e)
            return sanitized

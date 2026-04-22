"""
DistroTV scraper for FastChannels.
No config fields required — anonymous public API.
"""
from __future__ import annotations

import html as _html
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, StreamDeadError

logger = logging.getLogger(__name__)

_DISTRO_SXE_DASH_RE = re.compile(
    r"^(?P<series>.+?)\s+S(?P<season>\d+)\s*E(?P<episode>\d+)\s*[-–]\s*(?P<episode_title>.+?)\s*$",
    re.IGNORECASE,
)
_DISTRO_EPISODE_ONLY_RE = re.compile(
    r"^(?P<series>.+?)(?::\s*|,\s*)Episode\s+(?P<episode>\d+)\s*$",
    re.IGNORECASE,
)
_DISTRO_EPISODE_WITH_SUBTITLE_RE = re.compile(
    r"^(?P<series>.+?)(?::\s*|,\s*)Episode\s+(?P<episode>\d+)\s*[-:]\s*(?P<episode_title>.+?)\s*$",
    re.IGNORECASE,
)


def _unescape(text: str) -> str:
    """Fully unescape HTML entities, handling multiply-encoded upstream data."""
    prev = None
    while prev != text:
        prev = text
        text = _html.unescape(text)
    return text


def _parse_distro_title(raw: str | None) -> tuple[str | None, int | None, int | None, str | None]:
    if not raw:
        return raw, None, None, None

    title = _unescape(raw.strip()) or raw

    match = _DISTRO_SXE_DASH_RE.match(title)
    if match:
        return (
            match.group("series").strip(),
            int(match.group("season")),
            int(match.group("episode")),
            match.group("episode_title").strip() or None,
        )

    match = _DISTRO_EPISODE_WITH_SUBTITLE_RE.match(title)
    if match:
        return (
            match.group("series").strip(),
            None,
            int(match.group("episode")),
            match.group("episode_title").strip() or None,
        )

    match = _DISTRO_EPISODE_ONLY_RE.match(title)
    if match:
        episode = int(match.group("episode"))
        return match.group("series").strip(), None, episode, f"Episode {episode}"

    return title, None, None, None

CHANNEL_SCHEME = "distro://channel/"

# Module-level feed cache keyed by geo code: geo -> (fetched_at, {raw_id: stream_url})
# Shared across scraper instances so a burst of play requests doesn't hammer the API.
_feed_cache: dict[str, tuple[float, dict[str, str]]] = {}
_FEED_CACHE_TTL = 1800  # 30 minutes

FEED_URL = "https://tv.jsrdn.com/tv_v5/getfeed.php?type=live"
EPG_URL  = "https://tv.jsrdn.com/epg/query.php"

ANDROID_UA = "Dalvik/2.1.0 (Linux; U; Android 9; AFTT Build/STT9.221129.002) GTV/AFTT DistroTV/2.0.9"
BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

HLS_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Origin":     "https://distro.tv",
    "Referer":    "https://distro.tv/",
}

# Hosts that require a manifest proxy in play.py (Origin/Referer restricted or session-token CDN).
SESSION_CDN_HOSTS = {
    "d3s7x6kmqcnb6b.cloudfront.net",   # session-token CDN (proxy required)
    "d35j504z0x2vu2.cloudfront.net",   # requires Origin/Referer headers (proxy required)
}

# Superset: hosts where resolve() should skip the server-side pre-fetch.
# Broadpeak is included here (direct redirect works for clients) but NOT in
# SESSION_CDN_HOSTS because it does NOT need the manifest proxy.
NO_PREFETCH_HOSTS = SESSION_CDN_HOSTS | {
    "streamdot.broadpeak.io",           # session-based; server-side fetches return intermittent 404
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

DEFAULT_GEO = "US"

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


def _normalize_geo(value: str | None) -> str:
    geo = (value or DEFAULT_GEO).strip().upper()
    return geo or DEFAULT_GEO


def _qualified_channel_id(geo: str, channel_id: str) -> str:
    return f"{_normalize_geo(geo)}:{channel_id}"


def _split_qualified_channel_id(source_channel_id: str) -> tuple[str, str]:
    raw = (source_channel_id or "").strip()
    if ":" not in raw:
        return DEFAULT_GEO, raw
    geo, channel_id = raw.split(":", 1)
    return _normalize_geo(geo), channel_id.strip()


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


def _resolve_from_feed(scraper: "DistroScraper", geo: str, raw_id: str) -> str | None:
    """Return a fresh stream URL for a Distro channel, using a cached feed if available."""
    now = time.time()
    cached = _feed_cache.get(geo)
    if cached and (now - cached[0]) < _FEED_CACHE_TTL:
        return cached[1].get(raw_id)

    r = scraper.get(scraper._feed_url(geo))
    if not r:
        return None
    try:
        feed = r.json()
    except Exception:
        return None

    url_map: dict[str, str] = {}
    for show in _iter_shows(feed):
        if show.get("type") != "live":
            continue
        seasons = show.get("seasons") or []
        if not seasons or not isinstance(seasons[0], dict):
            continue
        episodes = seasons[0].get("episodes") or []
        if not episodes or not isinstance(episodes[0], dict):
            continue
        ep = episodes[0]
        tvg_id = ep.get("id")
        content = ep.get("content") or {}
        upstream_url = content.get("url")
        if tvg_id and upstream_url:
            # The jsrdn feed embeds ad-targeting query params in stream URLs.
            # For the d3s7x6kmqcnb6b CDN (/d/distro001a/ paths) these params
            # cause the CDN to return a broken master (200 with dead variants)
            # instead of redirecting to the working /hls/... path — strip them.
            # For the d35j504z0x2vu2 CDN (/v1/master/ paths) the params are
            # required — the CDN returns 404 without them, so keep as-is.
            parsed = urlsplit(upstream_url)
            if parsed.path.startswith('/d/distro001a/'):
                upstream_url = urlunsplit(parsed._replace(query=''))
            url_map[str(tvg_id)] = upstream_url

    _feed_cache[geo] = (now, url_map)
    logger.debug("[distro] feed cache refreshed for geo=%s (%d channels)", geo, len(url_map))
    return url_map.get(raw_id)


class DistroScraper(BaseScraper):
    source_name        = "distro"
    display_name       = "Distro TV"
    stream_audit_enabled = True
    scrape_interval    = 720
    session_cdn_hosts  = SESSION_CDN_HOSTS | {"streamdot.broadpeak.io"}  # inspect verifies via feed for these
    config_schema   = [
        ConfigField(
            "geo",
            "Geo Feeds",
            field_type="text",
            required=False,
            default="QQ",
            placeholder="QQ",
            help_text="One or more Distro geo codes separated by commas. Examples: US, CA, MX, AR, XE, QQ. QQ is a worldwide feed with 26 extra channels not in the US feed.",
        ),
    ]

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update({
            "User-Agent": ANDROID_UA,
            "Accept":     "application/json,*/*",
        })

    def _geos(self) -> list[str]:
        raw = self.config.get("geo") or DEFAULT_GEO
        codes = [_normalize_geo(part) for part in re.split(r"[,|/\s]+", raw) if part.strip()]
        return codes or [DEFAULT_GEO]

    def _feed_url(self, geo: str) -> str:
        geo = _normalize_geo(geo)
        return FEED_URL if geo == DEFAULT_GEO else f"{FEED_URL}&geo={geo}"

    def fetch_channels(self) -> list[ChannelData]:
        channels: list[ChannelData] = []
        seen_ids: set[str] = set()
        geos = self._geos()

        for geo in geos:
            r = self.get(self._feed_url(geo))
            if not r:
                continue
            try:
                feed = r.json()
            except Exception as e:
                logger.error("[distro] feed JSON decode failed for geo=%s: %s", geo, e)
                continue

            region_count = 0
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

                source_channel_id = _qualified_channel_id(geo, str(tvg_id))
                if source_channel_id in seen_ids:
                    continue
                seen_ids.add(source_channel_id)

                raw_genre      = (show.get("genre") or "").strip()
                category, lang = _parse_distro_tags(raw_genre)

                description = (
                    show.get("summary") or show.get("description")
                    or show.get("short_description") or show.get("long_description")
                    or None
                )
                if description:
                    description = str(description).strip() or None

                channels.append(ChannelData(
                    source_channel_id = source_channel_id,
                    name              = name,
                    stream_url        = f"{CHANNEL_SCHEME}{source_channel_id}",
                    stream_type       = "hls",
                    logo_url          = logo or None,
                    category          = category,
                    language          = lang,
                    country           = _normalize_geo(geo),
                    description       = description,
                ))
                region_count += 1

            logger.info("[distro] parsed %d channels for geo=%s", region_count, geo)

        logger.info("[distro] parsed %d channels across %d geo feed(s)", len(channels), len(geos))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        if not channels:
            return []
        ids_by_raw: dict[str, list[str]] = {}
        for ch in channels:
            _, raw_id = _split_qualified_channel_id(ch.source_channel_id)
            if not raw_id:
                continue
            ids_by_raw.setdefault(raw_id, []).append(ch.source_channel_id)
        all_ids = ",".join(ids_by_raw)
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
            target_ids = ids_by_raw.get(ch_id) or []
            if not target_ids:
                continue
            for slot in (ch_epg.get("slots") or []):
                try:
                    start = datetime.strptime(slot["start"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    end   = datetime.strptime(slot["end"],   "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except (KeyError, ValueError):
                    continue
                title, season, episode, episode_title = _parse_distro_title(slot.get("title"))
                for qualified_id in target_ids:
                    programs.append(ProgramData(
                        source_channel_id = qualified_id,
                        title             = title or "Unknown",
                        description       = _unescape((slot.get("description") or "").strip()) or None,
                        start_time        = start,
                        end_time          = end,
                        poster_url        = slot.get("img_thumbh") or None,
                        episode_title     = episode_title,
                        season            = season,
                        episode           = episode,
                    ))
        logger.info("[distro] parsed %d EPG entries", len(programs))
        return programs

    # Broadpeak's session-based CDN returns intermittent 404s when fetched
    # server-side — a 404 would trigger a false-positive disable in the audit.
    # Channels on these hosts skip the manifest fetch; feed presence is enough.
    _AUDIT_SKIP_HOSTS = frozenset({"streamdot.broadpeak.io"})

    def audit_resolve(self, raw_url: str) -> str:
        """
        Audit-time resolve: confirm the channel is in the feed, then return
        either a real HTTP URL (for full manifest verification) or the opaque
        distro:// URL (to skip manifest fetch) for hosts that cause false positives.
        """
        if not raw_url.startswith(CHANNEL_SCHEME):
            return raw_url
        source_channel_id = raw_url[len(CHANNEL_SCHEME):]
        geo, raw_id = _split_qualified_channel_id(source_channel_id)
        upstream_url = _resolve_from_feed(self, geo, raw_id)
        if not upstream_url:
            raise StreamDeadError(f"Channel {source_channel_id} not found in Distro feed")
        sanitized = _sanitize_url(upstream_url)
        host = urlsplit(sanitized).netloc
        if host in self._AUDIT_SKIP_HOSTS:
            return raw_url  # opaque URL → audit runner skips manifest fetch
        return sanitized   # real URL → audit does full manifest verification

    def resolve(self, raw_url: str) -> str:
        if raw_url.startswith(CHANNEL_SCHEME):
            source_channel_id = raw_url[len(CHANNEL_SCHEME):]
            geo, raw_id = _split_qualified_channel_id(source_channel_id)
            upstream_url = _resolve_from_feed(self, geo, raw_id)
            if not upstream_url:
                logger.warning("[distro] could not resolve fresh URL for %s", source_channel_id)
                raise StreamDeadError(f"Channel {source_channel_id} not found in Distro feed")
            raw_url = upstream_url

        sanitized = _sanitize_url(raw_url)
        host = urlsplit(sanitized).netloc
        if host in NO_PREFETCH_HOSTS:
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

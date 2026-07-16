"""
C-SPAN scraper for FastChannels.

C-SPAN publishes its congressional and public-affairs coverage as free,
unauthenticated HLS event streams — the same feeds the public web player uses.
(The 24/7 linear networks, C-SPAN 1/2/3, are a separate Brightcove pipeline
gated behind a TV-provider login and are NOT scraped here.)

Everything C-SPAN covers — floor sessions, committee hearings, Washington
Journal, White House and campaign events — is exposed the same way: an
`event.<id>` manifest on an open CDN, discovered from a public web page.

Two-host CDN with different rules (reverse-engineered from the web player):

  * m3u8-l.c-spanvideo.org   — master + variant manifests; fully open, no auth.
  * m3u8-l2.c-spanvideo.org  — media segments; 403 unless the request carries
                               `Referer: https://www.c-span.org/` (plain hotlink
                               protection, not authentication).
  (Segments occasionally appear on m3u8-l too and are gated identically, so the
   proxy keys off the shared c-spanvideo.org suffix rather than a single host.)

Channels
--------
  * House Floor / Senate Floor — discovered from /congress/?chamber=<x>, which
    embeds the live event's manifest URL directly. The schedule page is
    C-SPAN-1-centric and does not reliably surface the Senate floor, so the
    floors use their own authoritative page.
  * Washington Journal — the daily 7-10am ET call-in show; discovered from the
    public /schedule/ page (live only during its airing window).
  * C-SPAN Live Event (optional, config toggle) — a rotating channel that plays
    whatever hearing / briefing / White House event is currently live on the
    /schedule/ page. Best-effort: the schedule view is C-SPAN-1-centric, so it
    catches marquee events but may miss a C-SPAN-3-only hearing.

The event ID is not published ahead of air — it appears only once the event
goes live, and the manifest opens with a short "starting soon" bumper. So
discovery happens at play time and returns None (channel dark) when nothing is
live. Discovery pages sit behind an AWS WAF that issues a JS challenge (HTTP
202) if hit too hard, so results are cached per page with stale-fallback.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urljoin

from .base import BaseScraper, ChannelData, ConfigField, ProgramData

logger = logging.getLogger(__name__)

CHANNEL_SCHEME = "cspan://"

# Manifest URL built from an event ID (used for schedule-discovered events).
MANIFEST_TMPL = "https://m3u8-l.c-spanvideo.org/event/event.{event_id}.tsc.m3u8"

CONGRESS_URL = "https://www.c-span.org/congress/?chamber={chamber}"
SCHEDULE_URL = "https://www.c-span.org/schedule/"

# Self-hosted logos (official C-SPAN network wordmarks, rasterized from the open
# static CDN's SVGs). Served locally so clients don't need the Referer the C-SPAN
# image CDN requires. See app/static/logos/cspan/.
_LOGO_BASE = "/static/logos/cspan"
_LOGO_CSPAN = f"{_LOGO_BASE}/cspan.png"    # C-SPAN 1  (House, Washington Journal)
_LOGO_CSPAN2 = f"{_LOGO_BASE}/cspan2.png"  # C-SPAN 2  (Senate)
_LOGO_CSPAN3 = f"{_LOGO_BASE}/cspan3.png"  # C-SPAN 3  (hearings / live events)

# Floor channels — discovered from their own /congress/ page.
FLOOR_CHANNELS = {
    "house":  {"name": "C-SPAN House Floor",  "chamber": "house",  "logo": _LOGO_CSPAN},
    "senate": {"name": "C-SPAN Senate Floor", "chamber": "senate", "logo": _LOGO_CSPAN2},
}

# Schedule-discovered channels (fixed opaque ids).
WJ_CHANNEL_ID = "washington-journal"
LIVE_EVENT_CHANNEL_ID = "live-event"

# Schedule slugs that belong to a dedicated channel above, so the rotating
# Live Event channel never doubles up on them.
_DEDICATED_SLUGS = frozenset({
    "us-house-of-representatives",
    "us-senate",
    "washington-journal",
})

# Segment CDN requires the Referer; both manifest and segment hosts live under
# this suffix, so the proxy routes any segment on it through the Referer proxy.
CDN_HOST_SUFFIX = "c-spanvideo.org"
REFERER = "https://www.c-span.org/"

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# Headers a real document navigation sends — the AWS WAF in front of
# www.c-span.org is more tolerant of a request that looks like one.
PAGE_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Headers the segment CDN requires (hotlink protection).
CDN_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Referer": REFERER,
    "Origin": "https://www.c-span.org",
}

# Discovery is cached so a burst of play requests — or the scrape and play paths
# together — never hammer the WAF-protected pages. An event ID is stable for the
# whole session (hours), so a long TTL is safe.
_DISCOVERY_TTL = 600  # 10 minutes
# When the play proxy detects a dead/ended event manifest (a floor session rolled
# to a new "Part", so the cached event 403s or carries ENDLIST), it forces a fresh
# discovery to pick up the new event id. This cooldown bounds how often a forced
# refetch can hit the WAF-protected page during a genuine recess (no new event),
# where every poll would otherwise see the dead manifest and refetch.
_FORCE_COOLDOWN = 20  # seconds
_discovery_lock = threading.Lock()
# chamber -> (fetched_at, floor_info | None)
_floor_cache: dict[str, tuple[float, Optional[dict]]] = {}
# single key -> (fetched_at, [event dict, ...])
_schedule_cache: tuple[float, list[dict]] | None = None
# discovery key -> monotonic-ish wall time of the last FORCED refetch
_last_forced: dict[str, float] = {}

_VIDEOFILE_RE = re.compile(r"data-videofile='([^']+\.m3u8)'")
_VIDEOID_RE = re.compile(r"data-videoid='([^']+)'")
_TITLE_RE = re.compile(r"id='chronicle-dashboard-video-title'>([^<]{0,200})")
_DESC_RE = re.compile(r"id='chronicle-dashboard-video-description'>([^<]{0,600})")

# Schedule anchors: /event/<slug>/<sub>/<id> ... </a>; a currently-airing event
# carries <em class='live'>Live</em> inside the anchor.
_SCHED_ANCHOR_RE = re.compile(
    r'href="//www\.c-span\.org/event/([a-z0-9\-]+)/[a-z0-9\-]+/(\d+)"[^>]*>(.*?)</a>',
    re.S,
)
_TAG_RE = re.compile(r"<[^>]+>")


def _truthy(value) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def _parse_floor(html: str) -> Optional[dict]:
    """Extract the live event's manifest URL + metadata from a /congress/ page."""
    m = _VIDEOFILE_RE.search(html)
    if not m:
        return None
    manifest_url = m.group(1).strip()
    if not manifest_url.startswith("http"):
        return None
    vid = _VIDEOID_RE.search(html)
    title = _TITLE_RE.search(html)
    desc = _DESC_RE.search(html)
    return {
        "manifest_url": manifest_url,
        "video_id": vid.group(1).strip() if vid else None,
        "title": title.group(1).strip() if title else None,
        "description": desc.group(1).strip() if desc else None,
    }


def _parse_schedule(html: str) -> list[dict]:
    """Return currently-live events from the /schedule/ page.

    Each entry: {slug, event_id, title}. Only anchors marked with the inline
    <em class='live'>Live</em> badge are returned, so this is 'what is airing
    right now', not the full day's grid.
    """
    live: list[dict] = []
    for m in _SCHED_ANCHOR_RE.finditer(html):
        slug, event_id, inner = m.group(1), m.group(2), m.group(3)
        if "class='live'" not in inner and 'class="live"' not in inner:
            continue
        title = _TAG_RE.sub(" ", inner)
        # Drop a trailing "Live" and the date fragment for a cleaner title.
        title = re.sub(r"\s+", " ", title).strip()
        title = re.sub(r"\s*\d{2}/\d{2}/\d{4}\s*Live\s*$", "", title).strip()
        title = re.sub(r"\s*Live\s*$", "", title).strip()
        title = title.strip(":").strip()
        live.append({"slug": slug, "event_id": event_id, "title": title or None})
    return live


def _fetch_page(scraper: "CSpanScraper", url: str) -> Optional[str]:
    """GET a discovery page, returning HTML or None. A 202 is the AWS WAF JS
    challenge (we're being rate-limited); the caller falls back to cache."""
    try:
        r = scraper.session.get(url, headers=PAGE_HEADERS, timeout=15)
    except Exception as e:
        logger.warning("[cspan] page fetch failed for %s: %s", url, e)
        return None
    if r.status_code == 202:
        logger.warning("[cspan] WAF challenge (202) for %s", url)
        return None
    if r.status_code != 200:
        logger.warning("[cspan] page HTTP %s for %s", r.status_code, url)
        return None
    return r.text or ""


def _should_fetch(key: str, cached_at: Optional[float], force: bool) -> bool:
    """Decide whether to refetch a discovery page.

    Normal path: refetch once the cache is older than the TTL. Forced path (the
    play proxy saw a dead/ended manifest): refetch even within the TTL, but no
    more than once per _FORCE_COOLDOWN so a recess can't hammer the WAF.
    """
    now = time.time()
    fresh = cached_at is not None and (now - cached_at) < _DISCOVERY_TTL
    if force and (now - _last_forced.get(key, 0.0)) >= _FORCE_COOLDOWN:
        _last_forced[key] = now
        return True
    return not fresh


def discover_floor(scraper: "CSpanScraper", chamber: str, force: bool = False) -> Optional[dict]:
    """Current live floor event for a chamber (cached, stale-fallback).

    `force` re-fetches even within the TTL (rate-limited) so the play proxy can
    recover immediately when a session rolls to a new Part.
    """
    with _discovery_lock:
        cached = _floor_cache.get(chamber)
    if not _should_fetch(f"floor:{chamber}", cached[0] if cached else None, force):
        return cached[1] if cached else None
    now = time.time()

    html = _fetch_page(scraper, CONGRESS_URL.format(chamber=chamber))
    if html is None:
        return cached[1] if cached else None

    info = _parse_floor(html)
    with _discovery_lock:
        _floor_cache[chamber] = (now, info)
    if info:
        logger.info("[cspan] %s floor live: event %s (%s)",
                    chamber, info.get("video_id"), info.get("title") or "")
    else:
        logger.info("[cspan] %s floor not in session", chamber)
    return info


def discover_schedule(scraper: "CSpanScraper", force: bool = False) -> list[dict]:
    """Currently-live schedule events (cached, stale-fallback)."""
    global _schedule_cache
    with _discovery_lock:
        cached = _schedule_cache
    if not _should_fetch("schedule", cached[0] if cached else None, force):
        return cached[1] if cached else []
    now = time.time()

    html = _fetch_page(scraper, SCHEDULE_URL)
    if html is None:
        return cached[1] if cached else []

    events = _parse_schedule(html)
    with _discovery_lock:
        _schedule_cache = (now, events)
    logger.info("[cspan] schedule: %d live event(s): %s",
                len(events), ", ".join(e["slug"] for e in events) or "none")
    return events


def rewrite_media_playlist(media_text: str, variant_url: str, rewrite_segment) -> str:
    """Rewrite segment URLs through `rewrite_segment`, keeping every tag intact —
    including #EXT-X-ENDLIST. Used to serve an ENDED floor session as a finite VOD
    (from segment 0, so the player shows the recorded session and its opening
    'coming up today' slate) rather than failing. Keeping ENDLIST is what stops it
    stalling: the player treats it as finite instead of waiting for live segments."""
    out = []
    for raw in media_text.splitlines():
        s = raw.strip()
        if s and not s.startswith("#"):
            out.append(rewrite_segment(urljoin(variant_url, s)))
        else:
            out.append(raw)
    return "\n".join(out) + "\n"


def pick_best_variant(master_text: str, master_url: str) -> Optional[str]:
    """Return the highest-bandwidth variant URL from an HLS master playlist,
    or None if `master_text` is not a master (has no #EXT-X-STREAM-INF)."""
    lines = [ln.strip() for ln in master_text.splitlines() if ln.strip()]
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
                except ValueError:
                    pass
                break
        j = i + 1
        while j < len(lines) and lines[j].startswith("#"):
            j += 1
        if j >= len(lines):
            continue
        if bw > best_bw:
            best_bw = bw
            best_uri = urljoin(master_url, lines[j])
    return best_uri


# Per-segment tags that travel with the segment URL (order-preserving); global
# headers (VERSION/TARGETDURATION/MEDIA-SEQUENCE/PLAYLIST-TYPE) are regenerated.
_SEGMENT_TAG_PREFIXES = (
    "#EXTINF",
    "#EXT-X-DISCONTINUITY",
    "#EXT-X-PROGRAM-DATE-TIME",
    "#EXT-X-KEY",
    "#EXT-X-MAP",
    "#EXT-X-BYTERANGE",
)
# How many trailing segments to expose as the live window (~6.4s each).
LIVE_WINDOW_SIZE = 10


def build_live_window(media_text: str, variant_url: str, rewrite_segment,
                      window_size: int = LIVE_WINDOW_SIZE) -> str:
    """Trim a growing EVENT media playlist to a sliding live window.

    C-SPAN serves EVENT playlists that keep every segment from the session start
    with MEDIA-SEQUENCE:0, so a player starts hours behind the live edge. This
    re-emits only the last `window_size` segments as a standard live media
    playlist (correct MEDIA-SEQUENCE / DISCONTINUITY-SEQUENCE, no EVENT type) so
    playback begins at the live edge and advances monotonically across refreshes.

    `rewrite_segment(abs_url)` maps an absolute segment URL to its served form
    (e.g. the Referer-adding segment proxy).
    """
    target_duration = "7"
    segments: list[dict] = []
    pending: list[str] = []
    for raw in media_text.splitlines():
        s = raw.strip()
        if not s:
            continue
        if s.startswith("#"):
            if s.startswith("#EXT-X-TARGETDURATION:"):
                target_duration = s.split(":", 1)[1].strip() or target_duration
            elif s.startswith(_SEGMENT_TAG_PREFIXES):
                pending.append(s)
            # other global headers are dropped and regenerated
        else:
            segments.append({"tags": pending, "url": urljoin(variant_url, s)})
            pending = []

    if not segments:
        return media_text  # nothing to trim; let caller handle

    window = segments[-window_size:]
    dropped = segments[:-window_size] if len(segments) > window_size else []
    media_seq = len(dropped)
    disc_seq = sum(
        1 for seg in dropped
        if any(t.startswith("#EXT-X-DISCONTINUITY") and not t.startswith("#EXT-X-DISCONTINUITY-SEQUENCE")
               for t in seg["tags"])
    )
    # A discontinuity attached to the first kept segment belongs to the boundary
    # we cut at — account for it in the sequence and strip it so the window opens
    # cleanly rather than with a leading discontinuity.
    first_tags = window[0]["tags"]
    if any(t == "#EXT-X-DISCONTINUITY" for t in first_tags):
        disc_seq += 1
        window[0]["tags"] = [t for t in first_tags if t != "#EXT-X-DISCONTINUITY"]

    out = [
        "#EXTM3U",
        "#EXT-X-VERSION:6",
        f"#EXT-X-TARGETDURATION:{target_duration}",
        f"#EXT-X-MEDIA-SEQUENCE:{media_seq}",
    ]
    if disc_seq:
        out.append(f"#EXT-X-DISCONTINUITY-SEQUENCE:{disc_seq}")
    for seg in window:
        out.extend(seg["tags"])
        out.append(rewrite_segment(seg["url"]))
    return "\n".join(out) + "\n"


class CSpanScraper(BaseScraper):
    source_name  = "cspan"
    display_name = "C-SPAN"
    source_category = "specialty"
    # Live-only event feeds (dark between sessions), so DO NOT let the audit
    # resolve them — a recess would look like a dead stream. resolve() also
    # returns unavailable (not dead) with no live event, so nothing auto-disables
    # the channels during downtime.
    stream_audit_enabled = False
    scrape_interval = 180  # channels + EPG are static; scrape only rolls the EPG window forward

    config_schema = [
        ConfigField(
            "include_live_events",
            'Rotating "Live Event" Channel',
            field_type="toggle",
            default=True,
            help_text=(
                "Add a channel that plays whatever hearing, briefing, or White "
                "House event C-SPAN currently has live (discovered from the public "
                "schedule; best-effort — may miss a C-SPAN-3-only hearing)."
            ),
        ),
    ]

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update(PAGE_HEADERS)

    def _live_events_enabled(self) -> bool:
        return _truthy(self.config.get("include_live_events", True))

    def fetch_channels(self) -> list[ChannelData]:
        defs = [
            (cid, meta["name"], meta["logo"]) for cid, meta in FLOOR_CHANNELS.items()
        ]
        defs.append((WJ_CHANNEL_ID, "C-SPAN Washington Journal", _LOGO_CSPAN))
        if self._live_events_enabled():
            defs.append((LIVE_EVENT_CHANNEL_ID, "C-SPAN Live Event", _LOGO_CSPAN3))

        channels = [
            ChannelData(
                source_channel_id=cid,
                name=name,
                stream_url=f"{CHANNEL_SCHEME}{cid}",
                stream_type="hls",
                logo_url=logo,
                category="News",
                language="en",
                country="US",
            )
            for cid, name, logo in defs
        ]
        logger.info("[cspan] published %d channels", len(channels))
        return channels

    # ---- discovery dispatch --------------------------------------------------

    def _resolve_info(self, cid: str, force: bool = False) -> Optional[dict]:
        """Return {manifest_url, title, description} for a channel id, or None
        when nothing is live for it. `force` re-discovers past the cache (used by
        the play proxy when the cached event's manifest has gone dead)."""
        if cid in FLOOR_CHANNELS:
            info = discover_floor(self, FLOOR_CHANNELS[cid]["chamber"], force=force)
            return info  # already has manifest_url/title/description or None

        if cid == WJ_CHANNEL_ID:
            for ev in discover_schedule(self, force=force):
                if ev["slug"] == "washington-journal":
                    return self._event_info(ev)
            return None

        if cid == LIVE_EVENT_CHANNEL_ID:
            for ev in discover_schedule(self, force=force):
                if ev["slug"] not in _DEDICATED_SLUGS:
                    return self._event_info(ev)
            return None

        logger.warning("[cspan] unknown channel id %s", cid)
        return None

    @staticmethod
    def _event_info(ev: dict) -> dict:
        return {
            "manifest_url": MANIFEST_TMPL.format(event_id=ev["event_id"]),
            "video_id": ev["event_id"],
            "title": ev.get("title"),
            "description": None,
        }

    def resolve(self, raw_url: str, force: bool = False) -> Optional[str]:
        """Return the current live master-manifest URL for the channel, or None
        when nothing is live. Never raises StreamDeadError — an off-air channel
        is a normal transient absence, not a dead stream.

        `force` re-discovers past the cache; the play proxy sets it after seeing
        a dead/ended manifest so a floor session's Part-rollover recovers at once.
        """
        if not raw_url.startswith(CHANNEL_SCHEME):
            return raw_url
        cid = raw_url[len(CHANNEL_SCHEME):]
        info = self._resolve_info(cid, force=force)
        if not info:
            logger.info("[cspan] nothing live for %s", cid)
            return None
        return info["manifest_url"]

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        """Minimal guide data: a neutral, always-present block per channel.

        These are live event channels with no machine-readable schedule, and their
        live/off-air state flips far faster than the scrape interval. The EPG
        deliberately does NOT assert live vs. off-air: a scrape-time discovery
        probe is stale within minutes and — because the scraper and the play path
        run in separate processes with separate caches — can flatly contradict
        playback (e.g. label a channel "off air" while it streams fine). It would
        also add WAF load on every scrape. So the guide just labels the channel;
        actual availability is handled at play time (503 when nothing is live).
        Regenerated every scrape_interval (< the block duration) so consecutive
        blocks overlap and leave no gap.
        """
        programs: list[ProgramData] = []
        now = datetime.now(timezone.utc)
        start = now.replace(minute=0, second=0, microsecond=0)
        end = start + timedelta(hours=6)
        for ch in channels:
            programs.append(ProgramData(
                source_channel_id=ch.source_channel_id,
                title=ch.name,
                start_time=start,
                end_time=end,
            ))
        logger.info("[cspan] built %d EPG blocks", len(programs))
        return programs

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
  * Washington Journal / Overflow — Hearings / Overflow — Events — older builds
    attempted to discover these from the public /schedule/ page. C-SPAN's current
    schedule HTML is a TV grid and no longer exposes reliable live event manifest
    IDs, so these channels are hidden by default unless a user explicitly opts
    into the best-effort discovery path. The two "Overflow" channels are NOT the
    real 24/7 linear C-SPAN 3 / C-SPAN 1 networks — each just plays whatever is
    currently live-badged on its schedule view (Network 3's view for Hearings,
    the default C-SPAN-1-centric view for Events) and goes dark between events.
    They were originally named "C-SPAN 3" / "C-SPAN Live Event", which read as
    an always-on channel and misled users when it went dark; renamed + given an
    on-channel description note instead.

The event ID is not published ahead of air — it appears only once the event
goes live, and the manifest opens with a short "starting soon" bumper. So
discovery happens at play time and returns None (channel dark) when nothing is
live. Discovery pages sit behind an AWS WAF that issues a JS challenge (HTTP
202) if hit too hard, so results are cached per page with stale-fallback.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urljoin

import requests

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
CSPAN3_CHANNEL_ID = "cspan3"
CSPAN3_NETWORK = 3  # /schedule/?channel=3 — the per-network C-SPAN 3 view

# Display names for the two rotating overflow channels. Neither is the real
# 24/7 linear network (see the module docstring) — each is just "whatever is
# currently live-badged" on its schedule view, dark the rest of the time. The
# old names ("C-SPAN 3", "C-SPAN Live Event") read like an always-on channel
# and misled users when it went dark between events; "Overflow" plus the
# on-channel description (_OVERFLOW_NOTE_*) make the intermittent nature explicit.
_CSPAN3_NAME = "C-SPAN Overflow — Hearings"
_LIVE_EVENT_NAME = "C-SPAN Overflow — Events"
_OVERFLOW_NOTE_CSPAN3 = (
    "Best-effort feed of whatever C-SPAN currently has live on its Network 3 "
    "schedule (committee hearings, etc). Not a 24/7 channel — dark between events."
)
# The Live Event channel's note reuses _OVERFLOW_DESC (below) — same "dark between
# events" framing already used for its EPG filler blocks, kept as one string.

# ---- C-SPAN Now app API (EPG only) ------------------------------------------
# api.c-spanarchives.org is the C-SPAN Now app's backend (AWS API Gateway). Its
# schedule/{networkId} endpoint (networkId 1/2/3) returns a real, titled, gapless
# ~24-38h forward guide per network — far richer than a neutral block. Used ONLY
# for EPG; live playback still comes from the public web player (discover_floor /
# discover_schedule above), so nothing here resolves a stream.
#
# The endpoint requires an app-wide X-Api-Key — a shared secret baked into the
# client, NOT a per-user login. A bundled default works out of the box, while the
# CSPAN_API_KEY env var still allows rotation/override without editing code. If
# the key rotates and 403s, EPG transparently falls back to the neutral block
# below; nothing else is affected.
API_BASE = "https://api.c-spanarchives.org/3.0"
API_KEY = os.environ.get("CSPAN_API_KEY", "3TTDHrdFlraPQ6g2p4goq6HPPNi3cIzi3CwUFLQF")
API_HEADERS = {"X-Api-Key": API_KEY, "Accept": "application/json"}

# Which C-SPAN network each channel draws its guide from. House and Washington
# Journal both air on C-SPAN 1, so they intentionally share the net-1 grid — the
# guide reflects the source network, not a per-program slice.
CHANNEL_NETWORK = {
    "house":               1,
    "senate":              2,
    CSPAN3_CHANNEL_ID:     3,
    WJ_CHANNEL_ID:         1,
    LIVE_EVENT_CHANNEL_ID: 3,
}

# Single-content channels show only their own program(s) from the network grid
# and mark every other slot "Off Air", instead of mirroring the whole network
# lineup. The app API exposes useful metadata (`live`, exact title, and the
# gavel-to-gavel floor description), so avoid broad substring matching: network 2
# regularly airs clips titled "U.S. Senate: ..." that are not the Senate floor.
# Network / rotating channels (cspan3, live-event) are intentionally absent: they
# have no single program to key off, so they get the OVERFLOW_CONFIG treatment
# below instead (generic blocks, not the raw network grid).
CHANNEL_PROGRAM_FILTER = {
    "house":  "house_floor",
    "senate": "senate_floor",
    WJ_CHANNEL_ID: "washington_journal",
}
_OFF_AIR_TITLE = "Off Air"

_OVERFLOW_DESC = (
    "Whatever hearing, briefing, or event C-SPAN currently has live. "
    "Best-effort — the channel may be unavailable between events."
)

# Both rotating channels show the real network-3 schedule grid's TITLES only
# rarely: the network-3 API grid is the actual 24/7 linear feed for that network
# (hearings when Congress is in session, archival/library reruns otherwise — see
# fetch_epg's EXCEPTION note), which is routinely NOT what these channels are
# actually playing at play time. So every block gets a generic overflow label,
# except the slot covering "now", which is spliced with the real title IF
# discovery has actually confirmed something live for that specific channel
# (mirrors each channel's own resolve() preference in _resolve_info, so the
# spliced title matches what play would actually load).
_OVERFLOW_CONFIG = {
    CSPAN3_CHANNEL_ID: {
        "description": _OVERFLOW_NOTE_CSPAN3,
        "cache_keys": (f"schedule:{CSPAN3_NETWORK}",),
        "exclude_dedicated": False,
    },
    LIVE_EVENT_CHANNEL_ID: {
        "description": _OVERFLOW_DESC,
        "cache_keys": ("schedule:default", f"schedule:{CSPAN3_NETWORK}"),
        "exclude_dedicated": True,
    },
}

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

# Discovery is cached so play requests never hammer the WAF-protected pages. A live
# event ID is stable for the whole session (hours), so a long TTL is safe AND is our
# main defense against the WAF: fewer distinct page fetches = far less chance of the
# AWS WAF flagging our IP and JS-challenging us (which a plain HTTP client can't
# solve). Staleness self-heals at play time — when a cached event's manifest goes
# dead/ended the play proxy calls resolve(force=True), which re-discovers immediately
# (rate-limited by _FORCE_COOLDOWN), so the long TTL never strands a channel.
_DISCOVERY_TTL = 1800  # 30 minutes
# When the play proxy detects a dead/ended event manifest (a floor session rolled
# to a new "Part", so the cached event 403s or carries ENDLIST), it forces a fresh
# discovery to pick up the new event id. This cooldown bounds how often a forced
# refetch can hit the WAF-protected page during a genuine recess (no new event),
# where every poll would otherwise see the dead manifest and refetch.
_FORCE_COOLDOWN = 20  # seconds
# Discovery coordination state is SHARED ACROSS ALL WORKERS via Redis (the app's
# existing store), so the 2 gunicorn web workers keep ONE cache / WAF-backoff /
# fetch-gate / pre-warm cooldown instead of each holding its own in-process copy.
# Per-worker copies were the WAF's undoing: a play landing on a cold worker re-ran
# discovery, and the uncoordinated combined rate across workers tripped the
# challenge. Every Redis helper below degrades to "no coordination" if Redis is
# unreachable (if it is, the whole app is down anyway).
#
#   cspan:disc:<key>   JSON {"at": fetched_at, "val": floor|schedule}  (ex=_CACHE_TTL)
#   cspan:waf_backoff  presence = backing off; TTL = seconds left       (ex=_WAF_BACKOFF)
#   cspan:fetch_gate   short-lived NX lock enforcing _MIN_FETCH_GAP spacing
#   cspan:prewarm      NX cooldown so only one worker pre-warms per window
_REDIS_PREFIX = "cspan:"
_CACHE_TTL = 3600        # Redis key lifetime; freshness within it judged by _DISCOVERY_TTL
_WAF_BACKOFF = 120       # seconds to pause ALL discovery fetches after a 202
_MIN_FETCH_GAP = 2.5     # seconds between discovery page fetches, fleet-wide
_PREWARM_COOLDOWN = 300  # seconds between background pre-warms, fleet-wide

# In-process only: a cheap first-line single-flight within one worker, plus the
# force-cooldown map (force is rare and already rate-limited, so per-worker is fine).
_discovery_lock = threading.Lock()
_fetch_locks: dict[str, threading.Lock] = {}
_last_forced: dict[str, float] = {}
_redis_client = None

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


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Parse an API timestamp like '2026-07-16T21:00:00Z' to aware UTC, or None."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _clean_title(title: Optional[str]) -> Optional[str]:
    """Tidy schedule titles. C-SPAN doubles many up as 'X: X' (e.g.
    'U.S. House of Representatives: U.S. House of Representatives') — collapse those
    to a single 'X'. Genuinely two-part titles ('The Hearing Room: Labor Secretary
    Nominee Testifies...') are left intact."""
    if not title:
        return None
    t = re.sub(r"\s+", " ", title).strip()
    if ":" in t:
        left, right = (p.strip() for p in t.split(":", 1))
        if left and left == right:
            return left
    return t or None


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


# ---- shared coordination via Redis ------------------------------------------

def _redis():
    """Shared Redis handle (lazy, cached). Returns None if Redis can't be reached,
    so callers degrade gracefully instead of raising in the play path."""
    global _redis_client
    if _redis_client is None:
        try:
            import redis
            _redis_client = redis.from_url(
                os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
                decode_responses=True, socket_timeout=2,
            )
        except Exception as e:
            logger.warning("[cspan] Redis unavailable — discovery coordination degraded: %s", e)
            return None
    return _redis_client


def _cache_get(key: str):
    """Return (fetched_at, value) for a discovery view from the shared cache, or
    None when absent / Redis is down. `value` is the parsed floor dict / schedule
    list, JSON round-tripped."""
    r = _redis()
    if r is None:
        return None
    try:
        raw = r.get(_REDIS_PREFIX + "disc:" + key)
        if not raw:
            return None
        obj = json.loads(raw)
        return (obj["at"], obj["val"])
    except Exception:
        return None


def _cache_set(key: str, at: float, val) -> None:
    r = _redis()
    if r is None:
        return
    try:
        r.set(_REDIS_PREFIX + "disc:" + key,
              json.dumps({"at": at, "val": val}), ex=_CACHE_TTL)
    except Exception:
        pass


def _waf_backoff_left() -> Optional[float]:
    """Seconds left on the fleet-wide WAF backoff, or None if not active / Redis down."""
    r = _redis()
    if r is None:
        return None
    try:
        ttl = r.ttl(_REDIS_PREFIX + "waf_backoff")
        return float(ttl) if ttl and ttl > 0 else None
    except Exception:
        return None


def _arm_waf_backoff() -> None:
    r = _redis()
    if r is None:
        return
    try:
        r.set(_REDIS_PREFIX + "waf_backoff", "1", ex=_WAF_BACKOFF)
    except Exception:
        pass


def _fetch_page(scraper: "CSpanScraper", url: str) -> Optional[str]:
    """GET a discovery page -> HTML or None. Honors the shared WAF backoff (skips
    the request entirely while active, so we stop agitating the WAF) and arms it
    fleet-wide on a fresh 202. Spacing + caching are handled by the _discover flow."""
    left = _waf_backoff_left()
    if left is not None:
        logger.info("[cspan] WAF backoff active (%.0fs left) — skipping %s", left, url)
        return None
    try:
        r = scraper.session.get(url, headers=PAGE_HEADERS, timeout=15)
    except Exception as e:
        logger.warning("[cspan] page fetch failed for %s: %s", url, e)
        return None
    if r.status_code == 202:
        _arm_waf_backoff()
        logger.warning("[cspan] WAF challenge (202) for %s — backing off %ds",
                       url, _WAF_BACKOFF)
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


def _fetch_lock_for(key: str) -> threading.Lock:
    """Return the per-view single-flight lock for `key`, creating it on demand.
    Held around the actual page fetch so only one caller stampedes a cold view."""
    with _discovery_lock:
        lock = _fetch_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _fetch_locks[key] = lock
        return lock


def _await_fetch_slot() -> None:
    """Block until it's this caller's turn to fetch, keeping discovery page fetches
    >= _MIN_FETCH_GAP apart FLEET-WIDE via a short-lived Redis NX lock, so the two
    workers can't fetch at the same instant. Degrades to no spacing if Redis is down.
    Bounded wait so a stuck gate can never hang a request forever."""
    r = _redis()
    if r is None:
        return
    gate = _REDIS_PREFIX + "fetch_gate"
    gap_ms = int(_MIN_FETCH_GAP * 1000)
    for _ in range(240):  # safety cap (~ minutes worst case); normally 1-2 iterations
        try:
            if r.set(gate, "1", nx=True, px=gap_ms):
                return  # we own this slot; the key auto-expires after the gap
            ttl_ms = r.pttl(gate)
        except Exception:
            return
        time.sleep(ttl_ms / 1000.0 if ttl_ms and ttl_ms > 0 else 0.1)


def _maybe_prewarm(scraper: "CSpanScraper") -> None:
    """Warm every discovery view in the background so the rest of a viewing session
    hits (shared) cache. Fires only from a play (never on a timer), and at most once
    per _PREWARM_COOLDOWN FLEET-WIDE (a Redis NX key), so the two workers don't each
    launch their own warm. Each discover_* call fetches only if its view is cold, is
    spaced by the fleet-wide gate, and reuses the shared cache — so this adds no load
    when caches are already warm. No-op if Redis is down (on-demand still works)."""
    r = _redis()
    if r is None:
        return
    try:
        if not r.set(_REDIS_PREFIX + "prewarm", "1", nx=True, ex=_PREWARM_COOLDOWN):
            return  # another worker pre-warmed within the cooldown
    except Exception:
        return

    def _run() -> None:
        try:
            discover_floor(scraper, "house")
            discover_floor(scraper, "senate")
            discover_schedule(scraper, None)
            discover_schedule(scraper, CSPAN3_NETWORK)
        except Exception as e:  # background best-effort; never surface
            logger.debug("[cspan] prewarm error: %s", e)

    threading.Thread(target=_run, name="cspan-prewarm", daemon=True).start()


def _discover(scraper, *, key, url, parse, empty, force, log):
    """Shared discovery flow: Redis cache -> in-worker single-flight -> fleet-wide
    WAF-backoff check -> fleet-wide fetch spacing -> re-check cache -> fetch -> cache.

    `parse(html)` -> value; `empty` is the no-live default (None for floors, [] for
    schedules); `log(value)` emits the result line. Returns the cached/fresh value,
    or the stale value / `empty` when a fetch is skipped or fails."""
    cached = _cache_get(key)
    if not _should_fetch(key, cached[0] if cached else None, force):
        return cached[1] if cached is not None else empty

    prev_at = cached[0] if cached else 0.0
    with _fetch_lock_for(key):                       # cheap in-worker single-flight
        cached = _cache_get(key)
        if cached is not None and cached[0] > prev_at:
            return cached[1]                         # refreshed while we waited (this worker)

        left = _waf_backoff_left()
        if left is not None:
            logger.info("[cspan] WAF backoff active (%.0fs left) — skipping %s", left, url)
            return cached[1] if cached is not None else empty

        _await_fetch_slot()                          # fleet-wide inter-fetch spacing
        cached = _cache_get(key)
        if cached is not None and cached[0] > prev_at:
            return cached[1]                         # another WORKER refreshed it while we waited

        html = _fetch_page(scraper, url)
        if html is None:
            return cached[1] if cached is not None else empty

        val = parse(html)
        _cache_set(key, time.time(), val)
        log(val)
        return val


def discover_floor(scraper: "CSpanScraper", chamber: str, force: bool = False) -> Optional[dict]:
    """Current live floor event for a chamber (shared cache, stale-fallback).

    `force` re-fetches even within the TTL (rate-limited) so the play proxy can
    recover immediately when a session rolls to a new Part.
    """
    def _log(info):
        if info:
            logger.info("[cspan] %s floor live: event %s (%s)",
                        chamber, info.get("video_id"), info.get("title") or "")
        else:
            logger.info("[cspan] %s floor not in session", chamber)

    return _discover(scraper, key=f"floor:{chamber}",
                     url=CONGRESS_URL.format(chamber=chamber),
                     parse=_parse_floor, empty=None, force=force, log=_log)


def discover_schedule(scraper: "CSpanScraper", channel: Optional[int] = None,
                      force: bool = False) -> list[dict]:
    """Currently-live events on the /schedule/ page, shared-cached per network view.

    channel=None uses the default (C-SPAN-1-centric) view — fine for Washington
    Journal and marquee events. channel=3 uses the per-network C-SPAN 3 view, the
    reliable source for committee hearings (the default view misses them).
    """
    view = str(channel) if channel is not None else "default"
    url = SCHEDULE_URL if channel is None else f"{SCHEDULE_URL}?channel={channel}"

    def _log(events):
        logger.info("[cspan] schedule[%s]: %d live event(s): %s",
                    view, len(events), ", ".join(e["slug"] for e in events) or "none")

    return _discover(scraper, key=f"schedule:{view}", url=url,
                     parse=_parse_schedule, empty=[], force=force, log=_log)


# How stale an ENDED session's own embedded timestamp can be before the
# manifest-proxy VOD fallback (see rewrite_media_playlist below) refuses to
# serve it. The fallback exists for the few minutes right after a floor
# session's Part actually just wrapped, so the player shows the recording
# instead of failing outright — NOT for indefinitely replaying whatever the
# last video happens to be. /congress/?chamber=<x> has no live/ended signal of
# its own (_parse_floor just grabs data-videofile unconditionally) and keeps
# pointing at the last-aired event's manifest even a day+ after it ended, so
# without this check a dead/quiet chamber replays yesterday's clip from the top
# on every play — which is exactly what looks like "stuck on repeat" to a user.
STALE_VOD_MINUTES = 30

_PROGRAM_DATE_TIME_RE = re.compile(r"^#EXT-X-PROGRAM-DATE-TIME:(\S+)", re.M)


def latest_program_date_time(media_text: str) -> Optional[datetime]:
    """Return the latest #EXT-X-PROGRAM-DATE-TIME timestamp in a media playlist
    (aware UTC), or None if it has none/none parse. Used to tell "this session
    ended a minute ago" apart from "this session ended over a day ago"."""
    latest: Optional[datetime] = None
    for m in _PROGRAM_DATE_TIME_RE.finditer(media_text):
        dt = _parse_iso(m.group(1))
        if dt and (latest is None or dt > latest):
            latest = dt
    return latest


# Lazy, process-shared session carrying CDN_HEADERS (Referer) for manifest
# freshness checks — separate from a scraper's self.session (PAGE_HEADERS, for
# the WAF-protected HTML discovery pages) and from play.py's own persistent
# proxy session, since fetch_epg runs in the scraper worker, not the web workers.
_floor_check_session: Optional["requests.Session"] = None


def _floor_check_http() -> "requests.Session":
    global _floor_check_session
    if _floor_check_session is None:
        s = requests.Session()
        s.headers.update(CDN_HEADERS)
        _floor_check_session = s
    return _floor_check_session


def floor_manifest_is_fresh(manifest_url: str) -> bool:
    """Fetch a floor event's manifest from the OPEN CDN — never the WAF-protected
    /congress/ or /schedule/ pages, so this is safe to call from fetch_epg on
    every scrape without risking a WAF challenge — and report whether it's
    currently playable: genuinely still live, or ended recently enough for the
    play proxy's own VOD fallback (STALE_VOD_MINUTES).

    Exists because /congress/?chamber=<x> has no live/ended signal of its own
    (_parse_floor just grabs data-videofile unconditionally) and keeps pointing
    at the last-aired event's manifest long after it ended. Without this check,
    fetch_epg's floor-title splice (_current_floor_title) would keep claiming a
    session is airing right now — on the EPG's own generous multi-day filler
    block, no less — even after the play proxy has already started refusing to
    serve that same stale recording. Fails closed (False) on any error, so a
    network hiccup here degrades to the honest "Off Air" filler, never a false
    claim of live content.
    """
    try:
        session = _floor_check_http()
        m_r = session.get(manifest_url, timeout=8)
        if m_r.status_code != 200:
            return False
        text, base_url = m_r.text, m_r.url
        v_url = pick_best_variant(text, base_url)
        if v_url:
            v_r = session.get(v_url, timeout=8)
            if v_r.status_code != 200:
                return False
            text = v_r.text
        elif "#EXTINF" not in text:
            return False  # neither a master nor a media playlist
        if "#EXT-X-ENDLIST" not in text:
            return True  # still growing — genuinely live
        last = latest_program_date_time(text)
        if last is None:
            return False
        return (datetime.now(timezone.utc) - last) <= timedelta(minutes=STALE_VOD_MINUTES)
    except Exception as e:
        logger.warning("[cspan] floor manifest freshness check failed for %s: %s", manifest_url, e)
        return False


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
    channel_miss_threshold = 1
    # The rotating Live Event channel is a config-driven static entry, not a
    # discovered one — fetch_channels() only omits it if include_live_events (or
    # its include_schedule_channels prerequisite) is off. With threshold=1 that
    # means ANY transient config-read hiccup deactivates it on the very next
    # scrape with zero grace, same as the other cspan channels are meant to have
    # for a genuine toggle-off. Being dark is this channel's normal state (see
    # stream_audit_enabled above) — exempt it from the miss threshold entirely
    # rather than raising the threshold for the whole source.
    pinned_channel_ids = frozenset({LIVE_EVENT_CHANNEL_ID})
    scrape_interval = 180  # channels + EPG are static; scrape only rolls the EPG window forward

    config_schema = [
        ConfigField(
            "include_schedule_channels",
            "Experimental Schedule Channels",
            field_type="toggle",
            default=False,
            help_text=(
                "Add C-SPAN Overflow — Hearings, Washington Journal, and the "
                "rotating Overflow — Events channel. These depend on C-SPAN's "
                "public schedule HTML and may be unavailable when that page does "
                "not expose a live event ID."
            ),
        ),
        ConfigField(
            "include_live_events",
            '"Overflow — Events" Channel',
            field_type="toggle",
            default=False,
            help_text=(
                "Add a channel that plays whatever hearing, briefing, or White "
                "House event C-SPAN currently has live (discovered from the public "
                "schedule; best-effort — may miss a hearing that only aired on the "
                "Overflow — Hearings feed)."
            ),
        ),
    ]

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update(PAGE_HEADERS)

    def _live_events_enabled(self) -> bool:
        return _truthy(self.config.get("include_live_events", False))

    def _schedule_channels_enabled(self) -> bool:
        return _truthy(self.config.get("include_schedule_channels", False))

    def fetch_channels(self) -> list[ChannelData]:
        defs = [
            (cid, meta["name"], meta["logo"], None, None) for cid, meta in FLOOR_CHANNELS.items()
        ]
        if self._schedule_channels_enabled():
            defs.append((CSPAN3_CHANNEL_ID, _CSPAN3_NAME, _LOGO_CSPAN3,
                        "cspan-overflow-hearings", _OVERFLOW_NOTE_CSPAN3))
            defs.append((WJ_CHANNEL_ID, "C-SPAN Washington Journal", _LOGO_CSPAN, None, None))
            if self._live_events_enabled():
                defs.append((LIVE_EVENT_CHANNEL_ID, _LIVE_EVENT_NAME, _LOGO_CSPAN3,
                            "cspan-overflow-events", _OVERFLOW_DESC))

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
                slug=slug,
                description=desc,
            )
            for cid, name, logo, slug, desc in defs
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

        if cid == CSPAN3_CHANNEL_ID:
            # The C-SPAN 3 network view lists only C-SPAN 3's own programming, so
            # the currently-live entry is whatever hearing/event is on the network.
            for ev in discover_schedule(self, channel=CSPAN3_NETWORK, force=force):
                return self._event_info(ev)
            return None

        if cid == WJ_CHANNEL_ID:
            for ev in discover_schedule(self, force=force):
                if ev["slug"] == "washington-journal":
                    return self._event_info(ev)
            return None

        if cid == LIVE_EVENT_CHANNEL_ID:
            # 1) Default (C-SPAN-1-centric) view first — the marquee White House
            #    events / briefings this channel is named for live on C-SPAN 1.
            for ev in discover_schedule(self, force=force):
                if ev["slug"] not in _DEDICATED_SLUGS:
                    return self._event_info(ev)
            # 2) Fall back to the C-SPAN 3 network view, which surfaces committee
            #    hearings the C-SPAN-1-centric default view misses. This may mirror
            #    whatever the dedicated cspan3 channel is showing — acceptable for a
            #    best-effort rotating channel. The channel=3 view is shared-cached
            #    (cspan3 already populates it), so this adds ~no WAF load.
            for ev in discover_schedule(self, channel=CSPAN3_NETWORK, force=force):
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
        # Someone's watching — warm the other channels' discovery in the background
        # (spaced, cooldown-guarded) so their next channel-changes hit cache rather
        # than firing a fresh page each. No-op if caches are already warm.
        _maybe_prewarm(self)
        if not info:
            logger.info("[cspan] nothing live for %s", cid)
            return None
        return info["manifest_url"]

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        """Real guide data from the C-SPAN Now app's schedule API.

        Each channel draws its guide from its source C-SPAN network (1/2/3) via
        schedule/{networkId}: a gapless, titled, ~24-38h forward grid. House and
        Washington Journal both air on C-SPAN 1, so they intentionally share the
        net-1 grid (the guide reflects the source network, not a per-program slice).

        GUIDE ONLY — it deliberately does not assert live vs. off-air. The API's
        `live` flag means 'first-run', not 'airing now' (a network routinely reports
        several `live=True` blocks at once), and scrape-time state would contradict
        the play path anyway (separate processes/caches; see resolve()). Actual
        availability stays a play-time concern (503 when nothing is live). If the
        API key is unset/rotated or a network is unreachable, that channel falls
        back to one neutral 6h block so the guide never empties — the original
        behaviour. Regenerated every scrape_interval.

        EXCEPTION — the two rotating overflow channels (cspan3, live-event) get
        generic overflow blocks, not the raw network-3 titles: their real content
        is picked at play time from schedule discovery (resolve()'s per-channel
        preference; see _resolve_info), which routinely disagrees with this grid —
        the network-3 API grid is C-SPAN's actual 24/7 linear feed (hearings when
        Congress is in session, archival/library reruns the rest of the time), so
        a specific-sounding title here ("Bryan Burrough, The Gunfighters") would
        routinely name a program the channel isn't actually showing — and when
        nothing's genuinely live the channel won't even load. A vague label that's
        honestly vague beats a precise one that's routinely wrong. The one
        exception is the current slot: _splice_overflow_title overwrites it with
        the real title when discovery has actually confirmed something live for
        that specific channel.
        """
        now = datetime.now(timezone.utc)
        schedules: dict[int, Optional[list[dict]]] = {}  # network -> items | None
        programs: list[ProgramData] = []

        for ch in channels:
            net = CHANNEL_NETWORK.get(ch.source_channel_id)
            items: Optional[list[dict]] = None
            if net is not None:
                if net not in schedules:
                    schedules[net] = self._fetch_network_schedule(net)
                items = schedules[net]

            overflow_cfg = _OVERFLOW_CONFIG.get(ch.source_channel_id)
            if overflow_cfg:
                rows = self._overflow_rows(ch, items, overflow_cfg["description"])
                self._splice_overflow_title(
                    rows, now, overflow_cfg["cache_keys"], overflow_cfg["exclude_dedicated"],
                )
            else:
                program_filter = CHANNEL_PROGRAM_FILTER.get(ch.source_channel_id)
                if program_filter:
                    rows = self._filtered_epg_rows(ch, items, program_filter)
                    if ch.source_channel_id in FLOOR_CHANNELS:
                        # network-1/2's schedule grid only itemizes a full floor
                        # session (title == "U.S. House/Senate..."); a brief
                        # recess pro-forma session shows up there only as the
                        # generic umbrella block, so _filtered_epg_rows falls
                        # back to "Off Air" for the whole day even while the
                        # chamber is genuinely live. discover_floor() (what
                        # resolve() actually plays) catches those — splice its
                        # title in for "now" when it has confirmed live.
                        self._splice_floor_title(rows, now, ch.source_channel_id)
                else:
                    rows = self._epg_rows_from_schedule(ch, items)
            if rows:
                programs.extend(rows)
            else:
                programs.append(self._neutral_block(ch, now))

        live_nets = sum(1 for it in schedules.values() if it)
        logger.info("[cspan] built %d EPG entries for %d channels (%d/%d networks live)",
                    len(programs), len(channels), live_nets, len(schedules))
        return programs

    def _fetch_network_schedule(self, network: int) -> Optional[list[dict]]:
        """schedule/{network} items from the app API, or None on any failure (the
        caller falls back to a neutral block so EPG generation never breaks)."""
        if not API_KEY:
            return None
        try:
            r = self.session.get(f"{API_BASE}/schedule/{network}",
                                 headers=API_HEADERS, timeout=15)
        except Exception as e:
            logger.warning("[cspan] schedule/%s fetch failed: %s", network, e)
            return None
        if r.status_code != 200:
            logger.warning("[cspan] schedule/%s HTTP %s", network, r.status_code)
            return None
        try:
            return (r.json() or {}).get("scheduleItems") or []
        except ValueError:
            logger.warning("[cspan] schedule/%s non-JSON body", network)
            return None

    @staticmethod
    def _epg_rows_from_schedule(ch: ChannelData,
                                items: Optional[list[dict]]) -> list[ProgramData]:
        """Map schedule items -> ProgramData rows for one channel (empty if no data)."""
        if not items:
            return []
        rows: list[ProgramData] = []
        for it in items:
            start = _parse_iso(it.get("beginTime"))
            end = _parse_iso(it.get("endTime"))
            if not start or not end or end <= start:
                continue
            rows.append(ProgramData(
                source_channel_id=ch.source_channel_id,
                title=_clean_title(it.get("title")) or ch.name,
                start_time=start,
                end_time=end,
                description=(it.get("description") or "").strip() or None,
            ))
        return rows

    @staticmethod
    def _filtered_epg_rows(ch: ChannelData, items: Optional[list[dict]],
                           program_filter: str) -> list[ProgramData]:
        """EPG for a single-program channel: keep only rows that match that
        channel's app-API identity, and fill every other slot with one merged
        Off Air block. Gapless, because the network grid is contiguous."""
        if not items:
            return []
        parsed = []
        for it in items:
            start = _parse_iso(it.get("beginTime"))
            end = _parse_iso(it.get("endTime"))
            if start and end and end > start:
                parsed.append((start, end, it))

        rows: list[ProgramData] = []
        fill_start = fill_end = None

        def _flush_filler() -> None:
            nonlocal fill_start, fill_end
            if fill_start and fill_end and fill_end > fill_start:
                rows.append(ProgramData(
                    source_channel_id=ch.source_channel_id,
                    title=_OFF_AIR_TITLE, start_time=fill_start, end_time=fill_end,
                ))
            fill_start = fill_end = None

        for start, end, it in sorted(parsed, key=lambda x: x[0]):
            title = _clean_title(it.get("title")) or ""
            if CSpanScraper._matches_single_program(it, program_filter):
                _flush_filler()
                rows.append(ProgramData(
                    source_channel_id=ch.source_channel_id,
                    title=title, start_time=start, end_time=end,
                    description=(it.get("description") or "").strip() or None,
                ))
            elif fill_start is None:
                fill_start, fill_end = start, end
            elif start <= fill_end:            # contiguous grid → extend the filler
                fill_end = max(fill_end, end)
            else:
                _flush_filler()
                fill_start, fill_end = start, end
        _flush_filler()
        return rows

    @staticmethod
    def _matches_single_program(item: dict, program_filter: str) -> bool:
        title = (_clean_title(item.get("title")) or "").strip().lower()
        desc = (item.get("description") or "").strip().lower()
        live = bool(item.get("live"))

        if program_filter == "house_floor":
            return (
                title == "u.s. house of representatives"
                and (live or "gavel-to-gavel coverage of the united states house" in desc)
            )
        if program_filter == "senate_floor":
            return (
                title == "u.s. senate"
                and (live or "gavel-to-gavel coverage of the united states senate" in desc)
            )
        if program_filter == "washington_journal":
            return title.startswith("washington journal") and live
        return False

    @staticmethod
    def _overflow_rows(ch: ChannelData, items: Optional[list[dict]],
                       description: str) -> list[ProgramData]:
        """Generic 'something might be on, might be dark' guide for a rotating
        overflow channel. Reuses the network-3 schedule purely for its time
        boundaries (a reasonable proxy for grid granularity) — every title is
        replaced with the channel's own name and description with the given
        overflow note, rather than showing the network-3 program actually airing
        there, since that program is very often NOT what this channel is really
        showing (see fetch_epg's EXCEPTION note)."""
        rows = CSpanScraper._epg_rows_from_schedule(ch, items)
        for row in rows:
            row.title = ch.name
            row.description = description
        return rows

    @staticmethod
    def _current_overflow_title(cache_keys: tuple, exclude_dedicated: bool) -> Optional[str]:
        """Title of whatever a rotating overflow channel is actually airing right
        now, read from the shared discovery cache — NEVER fetches. fetch_epg runs
        on a timer regardless of viewership, and only real plays / the
        play-triggered prewarm are allowed to hit the WAF-protected schedule
        pages, so this only reads whatever those already populated. `cache_keys`
        and `exclude_dedicated` mirror the calling channel's own preference order
        in _resolve_info, so the spliced title matches what resolve() would
        actually pick for THAT channel. Returns None if nothing is cached, the
        cache is stale, or (when exclude_dedicated) nothing live is outside the
        dedicated (house/senate/WJ) slugs."""
        for key in cache_keys:
            cached = _cache_get(key)
            if not cached or (time.time() - cached[0]) >= _DISCOVERY_TTL:
                continue
            for ev in cached[1]:
                if exclude_dedicated and ev["slug"] in _DEDICATED_SLUGS:
                    continue
                if ev.get("title"):
                    return ev["title"]
        return None

    @classmethod
    def _splice_overflow_title(cls, rows: list[ProgramData], now: datetime,
                               cache_keys: tuple, exclude_dedicated: bool) -> None:
        """Override the generic block covering `now` with the real live title, in
        place, IF discovery has actually confirmed something live for this channel
        right now. Future slots stay generic (best-effort from the network-3 grid
        boundaries only)."""
        title = cls._current_overflow_title(cache_keys, exclude_dedicated)
        if not title:
            return
        for row in rows:
            if row.start_time <= now < row.end_time:
                row.title = title
                row.description = None
                return

    @staticmethod
    def _current_floor_title(chamber: str) -> Optional[str]:
        """Real title of a floor chamber's live event right now, read from the
        shared discover_floor cache (never re-fetches the WAF-protected
        /congress/ page itself) but VERIFIED against the actual open-CDN
        manifest via floor_manifest_is_fresh before trusting it — the cached
        discovery info alone doesn't know whether that event has since gone
        stale (see floor_manifest_is_fresh's docstring). Returns None if nothing
        is cached, the cache is stale, or the manifest isn't actually playable
        right now."""
        cached = _cache_get(f"floor:{chamber}")
        if not cached or (time.time() - cached[0]) >= _DISCOVERY_TTL:
            return None
        info = cached[1]
        if not info or not info.get("manifest_url"):
            return None
        if not floor_manifest_is_fresh(info["manifest_url"]):
            return None
        return info.get("title")

    @classmethod
    def _splice_floor_title(cls, rows: list[ProgramData], now: datetime, chamber: str) -> None:
        """Override the block covering `now` with the real floor session title
        when discover_floor confirms the chamber is actually live — see the call
        site in fetch_epg for why the network schedule API alone misses this."""
        title = cls._current_floor_title(chamber)
        if not title:
            return
        for row in rows:
            if row.start_time <= now < row.end_time:
                row.title = title
                row.description = None
                return

    @staticmethod
    def _neutral_block(ch: ChannelData, now: datetime) -> ProgramData:
        """The original always-present 6h block — a per-channel fallback when the
        schedule API is unavailable, so the guide never goes empty."""
        start = now.replace(minute=0, second=0, microsecond=0)
        return ProgramData(
            source_channel_id=ch.source_channel_id,
            title=ch.name,
            start_time=start,
            end_time=start + timedelta(hours=6),
        )

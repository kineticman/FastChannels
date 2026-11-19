"""
gracenote_listings.py

Per-station schedules from Gracenote's public TV listings site
(tvlistings.gracenote.com, the old zap2it backend) via its /api/sslgrid
endpoint, the "station schedule" view behind the site's grid.

One POST returns 14 days of airings for one Gracenote station id
(prgsvcid), FAST stations included (Paramount+ 1218xx, Samsung, Pluto,
Xumo, ...). The lineup/zip in the body only has to be a valid pair; the
station need not be in that lineup. Each airing carries its TMS id plus
episode title, season/episode, description, rating and release year.

The endpoint is undocumented: keep request rates low and stop on refusals
(see guide_cache.refresh). Scoping notes: dev/gracenote/FINDINGS.md.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import requests

log = logging.getLogger(__name__)

SSLGRID_URL = "https://tvlistings.gracenote.com/api/sslgrid"
IMAGE_BASE = "https://dtil.tmsimg.com/assets/"
# Full-size assets run ~3 MB; a guide thumbnail size keeps them ~100-200 KB.
IMAGE_SIZE = "?w=480&h=360"
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
# Any valid lineup/zip pair works; the station doesn't have to be in it.
_ANCHOR = {"lineupId": "USA-NY31695-DEFAULT", "headendId": "NY31695", "postalCode": "13302"}
# sslgrid only accepts a 336-hour span starting on a UTC day boundary.
_TIMESPAN_HOURS = 336


class GracenoteRefused(Exception):
    """Gracenote answered with a refusal or server error (403/429/5xx).
    retry_after: seconds from a Retry-After header, when it sent one."""

    def __init__(self, message: str, retry_after: int | None = None):
        super().__init__(message)
        self.retry_after = retry_after


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": _UA,
        "Accept": "application/json",
        "Referer": "https://tvlistings.gracenote.com/grid-affiliates.html?aid=orbebb",
    })
    return s


def _nth_sunday(year: int, month: int, n: int) -> date:
    d = date(year, month, 1)
    d += timedelta(days=(6 - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)


def _body(station_id: str, day_start: datetime) -> dict:
    # The DST fields mirror what the site's own page sends; the response's
    # epoch times don't depend on them.
    year = day_start.year
    return {
        **_ANCHOR,
        "IsSSLinkNavigation": "true",
        "timespan": str(_TIMESPAN_HOURS),
        "timestamp": str(int(day_start.timestamp())),
        "prgsvcid": str(station_id),
        "countryCode": "USA",
        "device": "X",
        "userId": "-",
        "aid": "orbebb",
        "DSTUTCOffset": "-240",
        "STDUTCOffset": "-300",
        "DSTStart": f"{_nth_sunday(year, 3, 2).isoformat()}T07:00Z",
        "DSTEnd": f"{_nth_sunday(year, 11, 1).isoformat()}T06:00Z",
        "languagecode": "en-us",
    }


def _int(value) -> int | None:
    try:
        n = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _text(value) -> str | None:
    s = str(value).strip() if value is not None else ""
    return s or None


def parse_airing(item: dict) -> dict | None:
    """One sslgrid airing → guide_cache's provider-neutral airing dict."""
    prog = item.get("program") or {}
    try:
        start = datetime.fromtimestamp(int(item["startTime"]), tz=timezone.utc)
        end = datetime.fromtimestamp(int(item["endTime"]), tz=timezone.utc)
    except (KeyError, TypeError, ValueError):
        return None
    title = _text(prog.get("title"))
    if not title or end <= start:
        return None
    tms_id = _text(prog.get("tmsId")) or _text(prog.get("id"))
    season, episode = _int(prog.get("season")), _int(prog.get("episode"))
    thumb = _text(item.get("thumbnail"))
    episode_title = _text(prog.get("episodeTitle"))
    return {
        "program_id": tms_id,
        "title": title,
        "episode_title": episode_title if episode_title != title else None,
        "start": start,
        "end": end,
        "season": season if season and episode else None,
        "episode": episode if season and episode else None,
        "description": _text(prog.get("shortDesc")),
        "rating": _text(item.get("rating")),
        "release_year": _int(prog.get("releaseYear")),
        "series_id": _text(prog.get("seriesId")) or _text(item.get("seriesId")),
        "program_type": "movie" if (tms_id or "").startswith("MV") else None,
        "image": IMAGE_BASE + thumb + ".jpg" + IMAGE_SIZE if thumb else None,
    }


def fetch_station(session: requests.Session, station_id: str,
                  now: datetime | None = None) -> list[dict]:
    """About 14 days of airings for a Gracenote station, from the start of the
    current UTC day. [] when Gracenote has no schedule for the station.
    Raises GracenoteRefused on 403/429/5xx, other exceptions on network or
    format errors."""
    now = now or datetime.now(timezone.utc)
    day_start = now.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    r = session.post(SSLGRID_URL, json=_body(station_id, day_start), timeout=30)
    if r.status_code in (403, 429) or r.status_code >= 500:
        retry = r.headers.get("Retry-After", "")
        raise GracenoteRefused(f"HTTP {r.status_code}", int(retry) if retry.isdigit() else None)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict) or "error" in data:
        raise ValueError(f"unexpected sslgrid response: {str(data)[:120]}")
    out, seen = [], set()
    for items in data.values():
        for item in items or []:
            airing = parse_airing(item)
            if airing and airing["start"] not in seen:
                seen.add(airing["start"])
                out.append(airing)
    out.sort(key=lambda a: a["start"])
    return out

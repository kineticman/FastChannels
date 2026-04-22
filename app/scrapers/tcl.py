"""
TCL TV+ scraper for FastChannels.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata
from .category_utils import category_for_channel, infer_category_from_name

_SPANISH_CAT_NAMES = frozenset({'en español', 'noticias'})

# Normalise TCL's inconsistent rating strings to standard US TV/MPAA values.
# Strip sub-rating descriptors (e.g. "TV-14 L,V" → "TV-14") then map variants.
_RATING_NORM: dict[str, str] = {
    'TVY':   'TV-Y',  'TV Y':  'TV-Y',
    'TVY7':  'TV-Y7', 'TV Y7': 'TV-Y7',
    'TVG':   'TV-G',  'TV G':  'TV-G',
    'TVPG':  'TV-PG', 'TV PG': 'TV-PG',
    'TV14':  'TV-14', 'TV 14': 'TV-14',
    'TVMA':  'TV-MA', 'TV MA': 'TV-MA',
    'TVNR':  'TV-NR', 'TV NR': 'TV-NR',
    'NR':    'TV-NR', 'NA':    'TV-NR', 'UNRATED': 'TV-NR',
}
_VALID_RATINGS = frozenset({
    'TV-Y', 'TV-Y7', 'TV-Y7-FV', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA', 'TV-NR',
    'G', 'PG', 'PG-13', 'R', 'NC-17', 'NR',
})


import re as _re

# "Bones S06: Twisted Bones In The Melted Truck 608"
# "Law & Order: SVU S23: People vs. Richard Wheatley 2309"
_TCL_COLON_RE = _re.compile(r'^(.+?)\s+S(\d+):\s+(.+)$', _re.IGNORECASE)
_TCL_TRAILING_CODE = _re.compile(r'\s+\d+$')

# "Show S1 - \"Ep Title\"" / "Show S2 E4" / "Show S1"
_TCL_DASH_RE = _re.compile(
    r'^(.+?)\s+S(\d+)(?:\s+E(\d+))?(?:\s*[-–]\s*"?(.+?)"?\s*)?$',
    _re.IGNORECASE,
)

# "The Rifleman  - A Matter of Faith" (no season marker; 1–2 spaces before dash)
_TCL_PLAIN_DASH_RE = _re.compile(r'^(.+?)\s{1,2}-\s+(.+)$')


def _parse_tcl_title(
    raw: str | None,
    api_season: int | None,
    api_episode: int | None,
) -> tuple[str | None, int | None, int | None, str | None]:
    """Parse a TCL composite title into (series_title, season, episode, episode_title)."""
    if not raw:
        return raw, api_season, api_episode, None
    s = raw.strip()

    # Pattern: "Series S06: Episode Title 608"
    m = _TCL_COLON_RE.match(s)
    if m:
        series   = m.group(1).strip()
        season   = int(m.group(2))
        ep_title = _TCL_TRAILING_CODE.sub('', m.group(3)).strip() or None
        return series, season, api_episode, ep_title

    # Pattern: "Series S1 - \"Ep Title\"" / "Series S2 E4" / "Series S1"
    m = _TCL_DASH_RE.match(s)
    if m:
        series   = m.group(1).strip()
        season   = int(m.group(2)) if m.group(2) else api_season
        episode  = int(m.group(3)) if m.group(3) else api_episode
        ep_title = m.group(4).strip().strip('"') if m.group(4) else None
        return series, season, episode, ep_title

    # Pattern: "The Rifleman  - A Matter of Faith" (no season in title, API has none either)
    if api_season is None and api_episode is None:
        m = _TCL_PLAIN_DASH_RE.match(s)
        if m:
            return m.group(1).strip(), None, None, m.group(2).strip() or None

    return s, api_season, api_episode, None


def _normalize_rating(raw: str | None) -> str | None:
    if not raw:
        return None
    # Strip sub-rating descriptors: "TV-14 D,L,V" → "TV-14"
    base = raw.strip().split()[0].upper()
    normed = _RATING_NORM.get(base, base)
    return normed if normed in _VALID_RATINGS else None


logger = logging.getLogger(__name__)


class TCLScraper(BaseScraper):
    source_name = "tcl"
    display_name = "TCL TV+"
    scrape_interval = 720
    stream_audit_enabled = True

    BASE = "https://gateway-prod.ideonow.com"
    IMAGE_BASE = "https://tcl-channel-cdn.ideonow.com"
    ORIGIN = "https://tcltv.plus"

    _DEVICE_ID = '1776786148042-4c4uc'

    config_schema = [
        ConfigField(
            key='country_code', label='Country',
            field_type='select', default='US',
            help_text='Only US and CA are supported.',
            options=[
                {'value': 'US', 'label': 'United States'},
                {'value': 'CA', 'label': 'Canada'},
            ],
        ),
    ]

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.country_code = self.config.get('country_code', 'US')
        self.state_code = 'OH'
        self.user_id = self._DEVICE_ID
        self.timeout = 20

        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Origin": self.ORIGIN,
            "Referer": f"{self.ORIGIN}/",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/147.0.0.0 Safari/537.36"
            ),
        })

    def _common_params(self) -> dict:
        return {
            "userId": self.user_id,
            "device_type": "web",
            "device_model": "web",
            "device_id": self.user_id,
            "app_version": "1.0",
            "country_code": self.country_code,
            "state_code": self.state_code,
        }

    def _get_json(self, path: str, params: Optional[dict] = None) -> dict:
        url = f"{self.BASE}{path}"
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _post_json(self, path: str, params: Optional[dict] = None, payload: Optional[dict] = None) -> dict:
        url = f"{self.BASE}{path}"
        resp = self.session.post(
            url,
            params=params,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def _fix_url(self, url: str | None) -> str | None:
        if not url:
            return None
        if url.startswith('/'):
            return f"{self.IMAGE_BASE}{url}"
        return url

    def fetch_channels(self) -> List[ChannelData]:
        livetab = self._get_json("/api/metadata/v2/livetab", params=self._common_params())
        categories = livetab.get("lines", [])
        deduped: Dict[str, ChannelData] = {}

        for cat in categories:
            cat_id = cat["id"]
            cat_name = cat.get("name")
            
            params = self._common_params()
            params["category_id"] = cat_id
            try:
                payload = self._get_json("/api/metadata/v1/epg/programlist/by/category", params=params)
            except Exception as e:
                logger.warning(f"Failed to fetch category {cat_name} ({cat_id}): {e}")
                continue

            is_spanish_cat = (cat_name or "").lower() in _SPANISH_CAT_NAMES

            for ch in payload.get("channels", []):
                bundle_id = str(ch.get("bundle_id") or ch.get("id"))
                name = ch.get("name", "")

                category = category_for_channel(name, cat_name) or infer_category_from_name(name)
                language = 'es' if is_spanish_cat else infer_language_from_metadata(name)

                if bundle_id not in deduped:
                    logo_url = ch.get("logo_color") or ch.get("logo_white")
                    source_tag = ch.get("source") or ""
                    media_url = ch.get("media") or ""
                    stream_url = "tcl://" + bundle_id + "?" + urlencode({
                        "source": source_tag,
                        "media": media_url,
                    })

                    deduped[bundle_id] = ChannelData(
                        source_channel_id=bundle_id,
                        name=name,
                        stream_url=stream_url,
                        logo_url=self._fix_url(logo_url),
                        category=category,
                        language=language,
                        description=ch.get("description"),
                    )
                else:
                    if category and not deduped[bundle_id].category:
                        deduped[bundle_id].category = category
                    if language == 'es' and deduped[bundle_id].language != 'es':
                        deduped[bundle_id].language = 'es'
        
        # Final fallback for anything still missing
        for ch in deduped.values():
            if not ch.category:
                ch.category = "Entertainment"
        
        return list(deduped.values())

    def fetch_epg(self, channels: List[ChannelData], **kwargs) -> List[ProgramData]:
        all_programs: List[ProgramData] = []
        
        livetab = self._get_json("/api/metadata/v2/livetab", params=self._common_params())
        categories = livetab.get("lines", [])

        seen_programs: set = set()
        # raw stub list: (bundle_id, prog_id, start_iso, end_iso, ch_poster_url)
        stubs: List[tuple] = []

        now = datetime.now(timezone.utc)
        range_params = {
            "start": (now - timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": (now + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        for cat in categories:
            cat_id = cat["id"]
            params = self._common_params()
            params["category_id"] = cat_id
            params.update(range_params)
            try:
                payload = self._get_json("/api/metadata/v1/epg/programlist/by/category", params=params)
            except Exception:
                continue

            for ch in payload.get("channels", []):
                bundle_id = str(ch.get("bundle_id") or ch.get("id"))
                ch_poster = ch.get("poster_h_small") or ch.get("poster_h_medium") or ch.get("poster_v_small")
                ch_poster_url = self._fix_url(ch_poster)

                for prog in (ch.get("programs") or []):
                    prog_id = prog.get("id")
                    if not prog_id:
                        continue
                    unique_key = f"{bundle_id}:{prog_id}:{prog.get('start')}"
                    if unique_key in seen_programs:
                        continue
                    seen_programs.add(unique_key)
                    stubs.append((bundle_id, prog_id, prog.get("start"), prog.get("end"), ch_poster_url, prog.get("title", "")))

        # Batch-fetch program details (desc, rating, season, episode, poster)
        details = self._fetch_program_details([s[1] for s in stubs])

        for bundle_id, prog_id, start_iso, end_iso, ch_poster_url, list_title in stubs:
            try:
                start_time = datetime.fromisoformat(start_iso.replace('Z', '+00:00'))
                end_time   = datetime.fromisoformat(end_iso.replace('Z', '+00:00'))
            except (ValueError, TypeError, AttributeError):
                continue

            d = details.get(self._detail_lookup_id(str(prog_id)), {})
            poster_url = self._fix_url(
                d.get("poster_h_large") or d.get("poster_h_medium") or
                d.get("poster_v_large") or d.get("poster_v_medium") or ch_poster_url
            )
            series = d.get("series") or {}
            raw_title = d.get("title") or list_title or "No Title"
            title, season, episode, ep_title = _parse_tcl_title(
                raw_title, series.get("season"), series.get("episode")
            )

            all_programs.append(ProgramData(
                source_channel_id=bundle_id,
                title=title or "No Title",
                start_time=start_time,
                end_time=end_time,
                description=d.get("desc"),
                poster_url=poster_url,
                rating=_normalize_rating(d.get("rating")),
                season=season,
                episode=episode,
                episode_title=ep_title,
            ))

        return all_programs

    @staticmethod
    def _detail_lookup_id(prog_id: str) -> str:
        """Extract the content_id the detail API expects from a compound prog_id.

        The EPG list returns composite IDs in the form bundle_id:content_id:slot_id.
        The /epg/program/detail endpoint only accepts the content_id (middle part).
        Simple (non-compound) IDs are returned unchanged.
        """
        parts = prog_id.split(":")
        return parts[1] if len(parts) == 3 else prog_id

    def _fetch_program_details(self, prog_ids: List[str]) -> Dict[str, dict]:
        """Batch-fetch /epg/program/detail for *prog_ids*, returning content_id→detail map.

        Compound prog_ids (bundle:content:slot) are resolved to their content_id
        for the API call; results are keyed by content_id so callers use
        _detail_lookup_id(prog_id) to look up.
        """
        # Dedupe by content_id — multiple slots may share the same content
        unique_lookup_ids = list(dict.fromkeys(self._detail_lookup_id(pid) for pid in prog_ids))

        details: Dict[str, dict] = {}
        batch_size = 25
        base_params = self._common_params()
        total = len(unique_lookup_ids)

        for i in range(0, total, batch_size):
            batch = unique_lookup_ids[i:i + batch_size]
            qs = urlencode(list(base_params.items()) + [("ids", lid) for lid in batch])
            url = f"{self.BASE}/api/metadata/v1/epg/program/detail?{qs}"
            try:
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                for item in resp.json():
                    lid = str(item.get("id") or "")
                    if lid:
                        details[lid] = item
            except Exception as e:
                logger.warning("[tcl] program detail batch %d failed: %s", i // batch_size, e)
            if self._progress_cb:
                self._progress_cb('epg', min(i + batch_size, total), total)

        logger.info("[tcl] program details fetched: %d/%d", len(details), len(unique_lookup_ids))
        return details

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("tcl://"):
            return raw_url

        parsed = urlparse(raw_url)
        bundle_id = parsed.netloc
        qs = parse_qs(parsed.query)
        source = (qs.get("source") or [""])[0] or None
        media = (qs.get("media") or [""])[0]

        payload = {
            "type": "channel",
            "bundle_id": bundle_id,
            "device_id": self.user_id,
            "source": source,
            "stream_url": media,
        }
        req_params = {
            "country_code": self.country_code,
            "app_version": "3.2.7",
        }

        try:
            data = self._post_json("/api/metadata/v1/format-stream-url", params=req_params, payload=payload)
            return data.get("stream_url") or media
        except Exception as e:
            logger.error("[tcl] Failed to resolve stream for %s: %s", bundle_id, e)
            return media

"""
TCL TV+ scraper for FastChannels.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata
from .category_utils import category_for_channel, infer_category_from_name

_SPANISH_CAT_NAMES = frozenset({'en español', 'noticias'})

logger = logging.getLogger(__name__)


class TCLScraper(BaseScraper):
    source_name = "tcl"
    display_name = "TCL TV+"
    scrape_interval = 360
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
        
        seen_programs = set()

        for cat in categories:
            cat_id = cat["id"]
            params = self._common_params()
            params["category_id"] = cat_id
            try:
                payload = self._get_json("/api/metadata/v1/epg/programlist/by/category", params=params)
            except Exception:
                continue

            for ch in payload.get("channels", []):
                bundle_id = str(ch.get("bundle_id") or ch.get("id"))
                
                ch_poster = ch.get("poster_h_small") or ch.get("poster_h_medium") or ch.get("poster_v_small")
                ch_poster_url = self._fix_url(ch_poster)

                raw_programs = ch.get("programs") or []
                for prog in raw_programs:
                    prog_id = prog.get("id")
                    if not prog_id:
                        continue
                        
                    unique_key = f"{bundle_id}:{prog_id}:{prog.get('start')}"
                    if unique_key in seen_programs:
                        continue
                    seen_programs.add(unique_key)

                    try:
                        start_time = datetime.fromisoformat(prog.get("start").replace('Z', '+00:00'))
                        end_time = datetime.fromisoformat(prog.get("end").replace('Z', '+00:00'))
                    except (ValueError, TypeError):
                        continue
                    
                    poster_url = prog.get("poster_h") or prog.get("poster_v") or ch_poster_url

                    all_programs.append(ProgramData(
                        source_channel_id=bundle_id,
                        title=prog.get("title") or "No Title",
                        start_time=start_time,
                        end_time=end_time,
                        description=prog.get("desc"),
                        poster_url=self._fix_url(poster_url),
                    ))
        
        return all_programs

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

from __future__ import annotations

import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from .base import BaseScraper, ChannelData, ConfigField, ProgramData

logger = logging.getLogger(__name__)


class LGChannelsScraper(BaseScraper):
    """
    FastChannels scraper for LG Channels.

    Notes:
    - Uses the public LG Channels web API discovered from the HAR/probe.
    - schedulelist currently provides both channel metadata and program listings,
      so fetch_epg() reuses the same endpoint and flattens embedded programs.
    - stream_url is stored as the upstream mediaStaticUrl template with LG-style
      macros intact; resolve() expands them at playback time and resolves HLS
      masters to a concrete media playlist URL when possible.
    """

    source_name = "lg-channels"
    display_name = "LG Channels"
    scrape_interval = 720  # lineup looks fairly stable
    stream_audit_enabled = True

    config_schema = [
        ConfigField(
            key="country",
            label="Country",
            field_type="text",
            required=False,
            default="US",
            placeholder="US",
            help_text="Country code for channel/EPG filtering. Default: US.",
        ),
        ConfigField(
            key="probe_best_variant",
            label="Resolve best HLS variant",
            field_type="toggle",
            required=False,
            default=True,
            help_text="If enabled, resolve master playlists to the highest-bandwidth media playlist during playback.",
        ),
    ]

    API_BASE = "https://api.lgchannels.com/api/v1.0"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._schedulelist_cache: dict | None = None

        self.country          = (self.config.get("country") or "US").strip() or "US"
        self.language         = "en"
        self.device_type      = "WEB"
        self.play_device_type = "Personal Computer"
        self.app_name         = "lgchannels_web"
        self.probe_best_variant = bool(self.config.get("probe_best_variant", True))

        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://lgchannels.com",
                "Referer": "https://lgchannels.com/",
                "x-device-country": self.country,
                "x-device-language": self.language,
                "x-device-type": self.device_type,
            }
        )

    # ── Required ─────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        payload = self._fetch_schedulelist()
        if not payload:
            return []

        channels: list[ChannelData] = []
        seen_ids: set[str] = set()

        for category in payload.get("categories", []):
            category_name = self._clean_str(category.get("categoryName"))
            for ch in category.get("channels", []):
                source_channel_id = self._clean_str(ch.get("channelId"))
                name = self._clean_str(ch.get("channelName"))
                stream_url = self._clean_str(ch.get("mediaStaticUrl"))

                if not source_channel_id or not name or not stream_url:
                    continue
                if source_channel_id in seen_ids:
                    continue
                seen_ids.add(source_channel_id)

                number = self._parse_int(ch.get("channelNumber"))
                provider_id = self._clean_str(ch.get("providerId"))
                channel_genre = self._clean_str(ch.get("channelGenreName"))

                category_value = category_name or channel_genre or provider_id or None

                channels.append(
                    ChannelData(
                        source_channel_id=source_channel_id,
                        name=name,
                        stream_url=stream_url,
                        logo_url=self._clean_str(ch.get("channelLogoUrl")),
                        category=category_value,
                        language=self._normalize_language(self.language),
                        country=self.country.upper(),
                        stream_type="hls",
                        number=number,
                    )
                )

        logger.info("[lg-channels] %d channels fetched", len(channels))
        return channels

    # ── Optional ─────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        payload = self._fetch_schedulelist()
        if not payload:
            return []

        valid_channel_ids = {ch.source_channel_id for ch in channels}
        programs: list[ProgramData] = []

        for category in payload.get("categories", []):
            default_category = self._clean_str(category.get("categoryName"))
            for ch in category.get("channels", []):
                source_channel_id = self._clean_str(ch.get("channelId"))
                if not source_channel_id or source_channel_id not in valid_channel_ids:
                    continue

                for prog in ch.get("programs", []):
                    title = self._clean_str(prog.get("programTitle"))
                    start_time = self._parse_dt(prog.get("startDateTime"))
                    end_time = self._parse_dt(prog.get("endDateTime"))

                    if not title or not start_time or not end_time or end_time <= start_time:
                        continue

                    primary_genre   = self._clean_str(prog.get("engGenreName"))
                    secondary_genre = self._clean_str(prog.get("engSecondGenreName"))
                    base_category   = (
                        primary_genre
                        or self._clean_str(ch.get("channelGenreName"))
                        or default_category
                    )
                    if secondary_genre and secondary_genre != base_category:
                        category_value = f"{base_category};{secondary_genre}"
                    else:
                        category_value = base_category

                    programs.append(
                        ProgramData(
                            source_channel_id=source_channel_id,
                            title=title,
                            start_time=start_time,
                            end_time=end_time,
                            description=self._clean_str(prog.get("description")),
                            poster_url=(
                                self._clean_str(prog.get("thumbnailUrl"))
                                or self._clean_str(prog.get("imageUrl"))
                                or self._clean_str(prog.get("previewImgUrl"))
                            ),
                            category=category_value,
                            rating=self._clean_str(prog.get("ratingCode")),
                            episode_title=None,
                            season=None,
                            episode=None,
                        )
                    )

        logger.info("[lg-channels] %d EPG entries fetched", len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url:
            return raw_url

        resolved = self._expand_stream_macros(raw_url)

        if not self.probe_best_variant:
            return resolved

        try:
            r = self.session.get(resolved, timeout=10)
            r.raise_for_status()
            text = r.text or ""

            if "#EXT-X-STREAM-INF" not in text:
                return resolved

            best_uri = None
            best_bw = -1
            lines = text.splitlines()

            for idx, line in enumerate(lines):
                if not line.startswith("#EXT-X-STREAM-INF"):
                    continue

                match = re.search(r"BANDWIDTH=(\d+)", line)
                bw = int(match.group(1)) if match else -1

                j = idx + 1
                while j < len(lines) and lines[j].startswith("#"):
                    j += 1
                if j >= len(lines):
                    continue

                candidate = urljoin(resolved, lines[j].strip())
                if bw > best_bw:
                    best_bw = bw
                    best_uri = candidate

            return best_uri or resolved

        except Exception as exc:
            logger.warning("[lg-channels] resolve fallback to raw expanded URL: %s", exc)
            return resolved

    # ── Internal helpers ─────────────────────────────────────

    def _fetch_schedulelist(self) -> dict[str, Any] | None:
        if self._schedulelist_cache is not None:
            return self._schedulelist_cache

        r = self.get(f"{self.API_BASE}/schedulelist")
        if not r:
            logger.error("[lg-channels] schedulelist request failed")
            return None
        try:
            payload = r.json()
        except Exception as exc:
            logger.error("[lg-channels] schedulelist JSON parse failed: %s", exc)
            return None

        if not isinstance(payload, dict):
            logger.error("[lg-channels] schedulelist payload was not an object")
            return None

        self._schedulelist_cache = payload
        return payload

    def _expand_stream_macros(self, url: str) -> str:
        nonce = str(int(time.time()))
        device_id = str(uuid.uuid4())
        ua = self.session.headers.get("User-Agent", "Mozilla/5.0")
        replacements = {
            "[DEVICE_ID]": device_id,
            "[IFA]": "",
            "[IFA_TYPE]": "",
            "[LMT]": "0",
            "[DNS]": "0",
            "[UA]": ua,
            "[IP]": "0.0.0.0",
            "[GDPR]": "",
            "[GDPR_CONSENT]": "",
            "[COUNTRY]": self.country.upper(),
            "[US_PRIVACY]": "",
            "[APP_STOREURL]": "",
            "[APP_BUNDLE]": "",
            "[APP_NAME]": self.app_name,
            "[APP_VERSION]": "",
            "[DEVICE_TYPE]": self.play_device_type,
            "[DEVICE_MAKE]": "",
            "[DEVICE_MODEL]": "",
            "[TARGETAD_ALLOWED]": "",
            "[FCK]": "",
            "[VIEWSIZE]": "1920x1080",
            "[NONCE]": nonce,
            "[HOTELTYPE]": "",
        }

        expanded = url
        for key, value in replacements.items():
            expanded = expanded.replace(key, value)
        return expanded

    @staticmethod
    def _parse_dt(value: Any) -> datetime | None:
        if not value or not isinstance(value, str):
            return None
        value = value.strip()
        try:
            if value.endswith("Z"):
                return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
            dt = datetime.fromisoformat(value)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _clean_str(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _parse_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_language(value: str) -> str:
        value = (value or "en").strip()
        if not value:
            return "en"
        # XMLTV/M3U users generally expect two-letter language where possible.
        if len(value) == 2:
            return value.lower()
        if "-" in value:
            return value.split("-", 1)[0].lower()
        return value.lower()

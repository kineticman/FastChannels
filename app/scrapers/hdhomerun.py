# app/scrapers/hdhomerun.py
"""
HDHomeRun (SiliconDust) local-network tuner integration.

Unlike the cloud FAST scrapers, an HDHomeRun lives on the user's LAN and is
addressed by a local IP (e.g. http://192.168.86.93).  This scraper therefore
only works when the FastChannels instance can reach that IP — i.e. a
self-hosted install on the same network as the tuner.

Data flow:
  - GET {base}/discover.json   → device identity + DeviceAuth + tuner count
  - GET {base}/lineup.json     → the channel lineup (GuideNumber, GuideName, …)
  - GET api.hdhomerun.com/api/xmltv?DeviceAuth=… → XMLTV guide (Gracenote-sourced;
    2 days for all users, 14 days with DVR subscription)

Streams are raw MPEG-TS over HTTP at {host}:5004/auto/v{GuideNumber}.  They are
NOT HLS — they play in VLC/ffmpeg/most TV clients but not HLS-only web players.
We store an opaque hdhr://<GuideNumber> URL and rebuild the live :5004 URL in
resolve() so a DHCP IP change only requires updating the source config, not a
re-scrape.
"""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urlencode, urlsplit

from .base import BaseScraper, ChannelData, ConfigField, ProgramData

logger = logging.getLogger(__name__)

_EPISODE_RE = re.compile(r'S(\d+)E(\d+)', re.IGNORECASE)

# Gracenote station ID from the XMLTV channel id, e.g. "US32639.hdhomerun.com" → "32639".
_XMLTV_CHANNEL_ID_RE = re.compile(r'^[A-Z]{2}(\d+)\.', re.IGNORECASE)

# OTA diginet Affiliate brand → canonical category.  Matched by prefix against
# the lowercased Affiliate string (so "Dabl Network", "PBS Kids HD", "METVN"
# all match); ordered, first hit wins, so specific keys precede general ones.
# Anything unmatched falls back to the genre-neutral 'Broadcast' category.
_AFFILIATE_CATEGORY_RULES: tuple[tuple[str, str], ...] = (
    ('pbs kids',   'Kids'),
    ('ion plus',   'Drama'),
    ('true crime', 'True Crime'),
    ('court tv',   'True Crime'),
    ('busted',     'True Crime'),
    ('crime',      'True Crime'),
    ('metv',       'Classic TV'),
    ('antenna',    'Classic TV'),
    ('retro',      'Classic TV'),
    ('cozi',       'Classic TV'),
    ('heroes',     'Classic TV'),
    ('start tv',   'Classic TV'),
    ('get tv',     'Classic TV'),
    ('gettv',      'Classic TV'),
    ('great tv',   'Classic TV'),
    ('greattv',    'Classic TV'),
    ('laff',       'Comedy'),
    ('catchy',     'Comedy'),
    ('comedy',     'Comedy'),
    ('dabl',       'Lifestyle'),
    ('the nest',   'Lifestyle'),
    ('create',     'Lifestyle'),
    ('quest',      'Documentary'),
    ('grit',       'Action & Adventure'),
    ('charge',     'Action & Adventure'),
    ('defy',       'Action & Adventure'),
    ('comet',      'Sci-Fi'),
    ('ion',        'Drama'),
    ('bounce',     'Entertainment'),
    ('movie',      'Movies'),
    ('buzzr',      'Game Shows'),
)

# Fallback keyword match against the channel NAME, for independents whose
# Affiliate field is blank (e.g. Rev'n, Retro TV).
_NAME_CATEGORY_RULES: tuple[tuple[str, str], ...] = (
    ('retro', 'Classic TV'),
    ('revn',  'Automotive'),
    ("rev'n", 'Automotive'),
)

_DEFAULT_OTA_CATEGORY = 'Broadcast'

# XMLTV <category> values that are too generic to use as programme category.
_XMLTV_SKIP_CATEGORIES = frozenset({'Series', 'Movie'})

# Regex for pure channel-number strings like "4.1" or "10" — not a real affiliate.
_LCN_RE = re.compile(r'^\d+(\.\d+)?$')


class HDHomeRunScraper(BaseScraper):
    source_name = "hdhomerun"
    display_name = "HDHomeRun"
    source_category = "specialty"
    scrape_interval = 360
    config_required = True
    epg_quality = "full"
    # The LAN device serves raw MPEG-TS, not HLS — the generic play-proxy DRM
    # probe (which parses HLS manifests) doesn't apply, and there's no upstream
    # catalogue to audit against. Leave the stream audit off.
    stream_audit_enabled = False

    STREAM_PORT = 5004
    XMLTV_URL = "https://api.hdhomerun.com/api/xmltv"

    config_schema = [
        ConfigField(
            'device_url',
            'Device address',
            field_type='text',
            required=True,
            placeholder='http://192.168.86.93',
            help_text=(
                'LAN address of your HDHomeRun (the BaseURL from its '
                'discover.json). FastChannels must be on the same network to '
                'reach it. Find it at http://my.hdhomerun.com or on the device.'
            ),
        ),
        ConfigField(
            'transcode_profile',
            'Transcode profile',
            field_type='select',
            default='',
            options=[
                {'value': '', 'label': 'Native (no transcoding)'},
                {'value': 'heavy', 'label': 'Heavy'},
                {'value': 'mobile', 'label': 'Mobile'},
                {'value': 'internet540', 'label': 'Internet 540p'},
                {'value': 'internet480', 'label': 'Internet 480p'},
                {'value': 'internet360', 'label': 'Internet 360p'},
                {'value': 'internet240', 'label': 'Internet 240p'},
            ],
            help_text=(
                'Optional HDHomeRun EXTEND hardware-transcode profile. Other '
                'models ignore or reject this setting; leave Native selected.'
            ),
        ),
    ]

    def __init__(self, config: dict = None):
        super().__init__(config)
        self._discover_cache: dict | None = None
        self._xmltv_cache: tuple[dict, list] | None = None

    # ── helpers ──────────────────────────────────────────────

    def _device_base(self) -> str | None:
        """Normalised scheme://host base for the configured device (no port)."""
        raw = (self.config.get('device_url') or '').strip()
        if not raw:
            return None
        if '://' not in raw:
            raw = 'http://' + raw
        parsed = urlsplit(raw)
        if not parsed.hostname:
            return None
        host = f"[{parsed.hostname}]" if ':' in parsed.hostname else parsed.hostname
        return f"{parsed.scheme or 'http'}://{host}"

    def _discover(self) -> dict:
        if self._discover_cache is not None:
            return self._discover_cache
        base = self._device_base()
        if not base:
            self._discover_cache = {}
            return self._discover_cache
        r = self.get(f"{base}/discover.json")
        self._discover_cache = (r.json() if r else {}) or {}
        return self._discover_cache

    def _stream_url_for(self, guide_number: str) -> str | None:
        base = self._device_base()
        if not base:
            return None
        parsed = urlsplit(base)
        host = f"[{parsed.hostname}]" if ':' in parsed.hostname else parsed.hostname
        url = f"{parsed.scheme}://{host}:{self.STREAM_PORT}/auto/v{guide_number}"
        transcode_profile = (self.config.get('transcode_profile') or '').strip()
        if transcode_profile:
            url = f"{url}?{urlencode({'transcode': transcode_profile})}"
        return url

    @staticmethod
    def _lineup_tags(row: dict) -> list[str]:
        """Normalize lineup.json's comma-separated Tags field."""
        raw = row.get("Tags")
        if isinstance(raw, str):
            values = raw.split(',')
        elif isinstance(raw, (list, tuple, set)):
            values = raw
        else:
            values = []
        return [str(value).strip().lower() for value in values if str(value).strip()]

    @staticmethod
    def _category_for(affiliate: str | None, name: str | None) -> str:
        """Canonical category from the OTA affiliate (preferred) or channel name.

        Falls back to 'Broadcast' — a genre-neutral bucket for full broadcast
        stations (major-network affiliates) that don't map to a single genre.
        """
        aff = (affiliate or '').strip().lower()
        if aff:
            for keyword, category in _AFFILIATE_CATEGORY_RULES:
                if aff.startswith(keyword):
                    return category
        nm = (name or '').strip().lower()
        for keyword, category in _NAME_CATEGORY_RULES:
            if keyword in nm:
                return category
        return _DEFAULT_OTA_CATEGORY

    # ── channels ─────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        discover = self._discover()
        base = self._device_base()
        if not base:
            logger.warning("[hdhomerun] no device_url configured")
            return []

        lineup_url = discover.get("LineupURL") or f"{base}/lineup.json"
        r = self.get(lineup_url)
        if not r:
            logger.warning("[hdhomerun] lineup fetch failed from %s", lineup_url)
            return []
        try:
            rows = r.json()
        except Exception as exc:
            logger.error("[hdhomerun] invalid lineup JSON: %s", exc)
            return []

        # Channel-level metadata (logo, affiliate, gracenote_id) from XMLTV.
        channel_meta, _ = self._fetch_xmltv(discover)

        channels: list[ChannelData] = []
        for row in rows:
            guide_number = str(row.get("GuideNumber") or "").strip()
            if not guide_number:
                continue
            lineup_tags = self._lineup_tags(row)
            # Encrypted (CableCARD) channels can't be streamed — skip them.
            if row.get("DRM") or "drm" in lineup_tags:
                continue

            name = (row.get("GuideName") or f"Channel {guide_number}").strip()
            meta = channel_meta.get(guide_number, {})
            affiliate = (meta.get("affiliate") or "").strip()
            tags = list(lineup_tags)
            if affiliate and affiliate.lower() not in tags:
                tags.append(affiliate)

            channels.append(
                ChannelData(
                    source_channel_id=guide_number,
                    name=name,
                    stream_url=f"hdhr://{guide_number}",
                    logo_url=meta.get("logo_url"),
                    category=self._category_for(affiliate, name),
                    number=None,  # GuideNumber ("4.1") isn't an int; let the app assign
                    # Store the Gracenote station ID but default to 'off' so the
                    # channel stays in the standard M3U using the imported HDHR
                    # EPG. Users opt a channel into Gracenote routing via admin.
                    gracenote_id=meta.get("gracenote_id"),
                    gracenote_mode='off',
                    guide_key=guide_number,
                    country="US",
                    language="en",
                    stream_type="mpegts",
                    tags=tags,
                )
            )

        logger.info(
            "[hdhomerun] %d channels from %s (%d tuners)",
            len(channels),
            discover.get("FriendlyName") or base,
            discover.get("TunerCount") or 0,
        )
        return channels

    # ── EPG ──────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        _, programs = self._fetch_xmltv(self._discover())
        return programs

    # ── XMLTV guide ──────────────────────────────────────────

    def _fetch_xmltv(self, discover: dict) -> tuple[dict, list[ProgramData]]:
        """Download and parse the SiliconDust XMLTV guide. Cached per run.

        Returns (channel_meta, programs) where channel_meta maps guide number
        (LCN, e.g. "4.1") to {logo_url, affiliate, gracenote_id}.
        """
        if self._xmltv_cache is not None:
            return self._xmltv_cache

        auth = discover.get("DeviceAuth")
        if not auth:
            logger.info("[hdhomerun] no DeviceAuth — skipping cloud EPG")
            self._xmltv_cache = ({}, [])
            return self._xmltv_cache

        r = self.get(self.XMLTV_URL, params={"DeviceAuth": auth})
        if not r:
            self._xmltv_cache = ({}, [])
            return self._xmltv_cache

        try:
            root = ET.fromstring(r.content)
        except ET.ParseError as exc:
            logger.error("[hdhomerun] XMLTV parse error: %s", exc)
            self._xmltv_cache = ({}, [])
            return self._xmltv_cache

        # Parse <channel> elements: xmltv_channel_id → {lcn, logo_url, affiliate, gracenote_id}
        xmltv_channels: dict[str, dict] = {}
        for ch in root.iter("channel"):
            ch_id = ch.get("id") or ""
            lcn = (ch.findtext("lcn") or "").strip()
            if not ch_id or not lcn:
                continue
            icon_el = ch.find("icon")
            logo_url = icon_el.get("src") if icon_el is not None else None
            # Last display-name is the affiliate/network brand; skip bare LCN strings.
            display_names = [el.text.strip() for el in ch.findall("display-name") if el.text]
            affiliate = next(
                (dn for dn in reversed(display_names) if not _LCN_RE.match(dn)),
                None,
            )
            m = _XMLTV_CHANNEL_ID_RE.match(ch_id)
            gracenote_id = m.group(1) if m else None
            xmltv_channels[ch_id] = {
                "lcn": lcn,
                "logo_url": logo_url,
                "affiliate": affiliate,
                "gracenote_id": gracenote_id,
            }

        # LCN-keyed map for fetch_channels lookups.
        channel_meta: dict[str, dict] = {
            meta["lcn"]: meta for meta in xmltv_channels.values()
        }

        # Parse <programme> elements.
        programs: list[ProgramData] = []
        for prog_el in root.iter("programme"):
            ch_id = prog_el.get("channel") or ""
            ch = xmltv_channels.get(ch_id)
            if not ch:
                continue
            pd = self._program_from_xmltv(ch["lcn"], prog_el)
            if pd:
                programs.append(pd)

        logger.info("[hdhomerun] XMLTV: %d channels, %d programs", len(channel_meta), len(programs))
        self._xmltv_cache = (channel_meta, programs)
        return self._xmltv_cache

    def _program_from_xmltv(self, guide_number: str, prog: ET.Element) -> ProgramData | None:
        title = prog.findtext("title")
        start_str = prog.get("start") or ""
        stop_str = prog.get("stop") or ""
        if not title or not start_str or not stop_str:
            return None

        try:
            start = datetime.strptime(start_str, "%Y%m%d%H%M%S %z")
            end = datetime.strptime(stop_str, "%Y%m%d%H%M%S %z")
        except ValueError:
            return None

        # Season/episode from <episode-num system="onscreen"> (e.g. "S01E06").
        season = episode = None
        for ep_el in prog.findall("episode-num"):
            if ep_el.get("system") == "onscreen":
                m = _EPISODE_RE.search(ep_el.text or "")
                if m:
                    season, episode = int(m.group(1)), int(m.group(2))
                break

        # Series ID from <series-id system="cseries">.
        series_id = None
        for sid_el in prog.findall("series-id"):
            if sid_el.get("system") == "cseries":
                series_id = (sid_el.text or "").strip() or None
                break

        # Per-episode ID from <episode-num system="dd_progid"> (TMS program ID).
        episode_id = None
        for ep_el in prog.findall("episode-num"):
            if ep_el.get("system") == "dd_progid":
                episode_id = (ep_el.text or "").strip() or None
                break

        if series_id and series_id.startswith("MV"):
            program_type = "movie"
        elif season is not None or episode is not None:
            program_type = "episode"
        else:
            program_type = None

        # First specific category (skip generic "Series"/"Movie" labels).
        categories = [el.text.strip() for el in prog.findall("category") if el.text]
        category = next(
            (c for c in categories if c not in _XMLTV_SKIP_CATEGORIES),
            categories[0] if categories else None,
        )

        date_str = (prog.findtext("date") or "").strip()
        original_air_date = None
        if date_str:
            try:
                original_air_date = datetime.strptime(date_str[:8], "%Y%m%d").date()
            except ValueError:
                pass

        icon_el = prog.find("icon")
        poster_url = icon_el.get("src") if icon_el is not None else None

        return ProgramData(
            source_channel_id=guide_number,
            title=title,
            start_time=start,
            end_time=end,
            description=prog.findtext("desc"),
            poster_url=poster_url,
            category=category,
            episode_title=prog.findtext("sub-title") or None,
            season=season,
            episode=episode,
            original_air_date=original_air_date,
            program_type=program_type,
            series_id=series_id,
            episode_id=episode_id,
        )

    # ── resolve ──────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("hdhr://"):
            return raw_url
        guide_number = raw_url[len("hdhr://"):]
        return self._stream_url_for(guide_number) or raw_url

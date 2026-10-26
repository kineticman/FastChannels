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

Several devices can be listed in device_url (comma/space separated).  The
primary device keeps the bare GuideNumber IDs a single-device install has
always used; every other device's channels are keyed "<DeviceID>:<GuideNumber>"
and stream from hdhr://<DeviceID>/<GuideNumber>, which resolve() maps back to
an address through each device's discover.json.  Each device's guide comes
from its own DeviceAuth.

The primary is pinned by DeviceID in config['primary_device_id'] (set to the
first listed device on the first scrape), so reordering the list never moves
the bare IDs to a different device.
"""
from __future__ import annotations

import logging
import re
import threading
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlencode, urlsplit

from .base import BaseScraper, ChannelData, ConfigField, ProgramData
from ..gracenote_map import normalize_gracenote_id
from .xmltv_import import program_from_xmltv

logger = logging.getLogger(__name__)

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

# Regex for pure channel-number strings like "4.1" or "10" — not a real affiliate.
_LCN_RE = re.compile(r'^\d+(\.\d+)?$')

# device_url may list several addresses separated by commas, semicolons or whitespace.
_DEVICE_SPLIT_RE = re.compile(r'[\s,;]+')

# Play-time cache of base URL → (DeviceID, fetched_at) so resolve() for a
# secondary device doesn't re-query every discover.json on each tune.
_DEVICE_ID_TTL = 600
_device_id_cache: dict[str, tuple[str, float]] = {}
_device_id_lock = threading.Lock()


class HDHomeRunScraper(BaseScraper):
    source_name = "hdhomerun"
    display_name = "HDHomeRun"
    source_category = 'local'
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
            'Device addresses',
            field_type='list',
            required=True,
            placeholder='http://192.168.86.93',
            add_label='Add another HDHomeRun',
            help_text=(
                'LAN address of each HDHomeRun (the BaseURL from its '
                'discover.json). FastChannels must be on the same network to '
                'reach them. Find them at http://my.hdhomerun.com or on the '
                'device. Your original HDHomeRun keeps the channel IDs it '
                'already has, whatever order the list is in.'
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
        self._discover_cache: dict[str, dict] = {}
        self._xmltv_cache: dict[str, tuple[dict, list]] = {}

    # ── helpers ──────────────────────────────────────────────

    @staticmethod
    def _normalize_base(raw: str) -> str | None:
        """Normalised scheme://host base for one device address (no port)."""
        raw = (raw or '').strip()
        if not raw:
            return None
        if '://' not in raw:
            raw = 'http://' + raw
        parsed = urlsplit(raw)
        if not parsed.hostname:
            return None
        host = f"[{parsed.hostname}]" if ':' in parsed.hostname else parsed.hostname
        return f"{parsed.scheme or 'http'}://{host}"

    def _device_bases(self) -> list[str]:
        """Configured device bases in order, de-duplicated."""
        raw = self.config.get('device_url') or ''
        bases: list[str] = []
        for part in _DEVICE_SPLIT_RE.split(raw):
            base = self._normalize_base(part)
            if base and base not in bases:
                bases.append(base)
        return bases

    def _pinned_device_id(self) -> str:
        return str(self.config.get('primary_device_id') or '').strip().upper()

    def _device_base(self) -> str | None:
        """Address of the primary device — the one whose channels keep bare IDs."""
        bases = self._device_bases()
        if len(bases) <= 1:
            return bases[0] if bases else None
        pinned = self._pinned_device_id()
        if not pinned:
            return bases[0]
        return self._base_for_device_id(pinned)

    def _discover(self, base: str | None = None) -> dict:
        base = base or self._device_base()
        if not base:
            return {}
        if base in self._discover_cache:
            return self._discover_cache[base]
        r = self.get(f"{base}/discover.json")
        try:
            data = (r.json() if r else {}) or {}
        except Exception:
            data = {}
        self._discover_cache[base] = data
        device_id = str(data.get("DeviceID") or "").strip().upper()
        if device_id:
            with _device_id_lock:
                _device_id_cache[base] = (device_id, time.monotonic())
        return data

    def _base_for_device_id(self, device_id: str) -> str | None:
        """Find which configured address currently answers as device_id."""
        device_id = device_id.upper()
        bases = self._device_bases()
        now = time.monotonic()
        with _device_id_lock:
            cached = {b: v for b, v in _device_id_cache.items() if b in bases}
        for base in bases:
            hit = cached.get(base)
            if hit and hit[0] == device_id and now - hit[1] < _DEVICE_ID_TTL:
                return base
        # Stale or unknown — ask the devices themselves.
        for base in bases:
            self._discover_cache.pop(base, None)
            found = str(self._discover(base).get("DeviceID") or "").strip().upper()
            if found == device_id:
                return base
        return None

    def _stream_url_for(self, guide_number: str, base: str | None = None) -> str | None:
        base = base or self._device_base()
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

    def _primary_base(self, bases: list[str]) -> str | None:
        """Pick the device that keeps bare channel IDs, pinning it if needed.

        The pinned DeviceID wins wherever it sits in the list.  With nothing
        pinned yet (an upgraded or new install) the first listed device is
        pinned.  If the pinned device is gone and it was the only one
        configured, the replacement takes over its IDs, as a single-device
        install always has.  If it's gone with several configured, no device
        gets bare IDs, so an offline primary never hands its IDs to another.
        """
        if not bases:
            return None
        pinned = self._pinned_device_id()
        if pinned:
            for base in bases:
                if str(self._discover(base).get("DeviceID") or "").strip().upper() == pinned:
                    return base
            if len(bases) > 1:
                logger.warning("[hdhomerun] primary device %s not found — its channels "
                               "are skipped this run", pinned)
                return None
        first_id = str(self._discover(bases[0]).get("DeviceID") or "").strip().upper()
        if first_id and first_id != pinned:
            logger.info("[hdhomerun] pinning %s (%s) as the primary device", first_id, bases[0])
            self._update_config('primary_device_id', first_id)
        return bases[0]

    def _devices(self) -> list[tuple[str, dict, str]]:
        """(base, discover, id_prefix) per reachable device.

        The primary device gets an empty prefix so a single-device install's
        channel IDs never change; the rest get "<DeviceID>:".  A secondary
        device without a DeviceID can't be addressed at play time, so it's
        skipped.  The same physical device listed twice is used once.
        """
        bases = self._device_bases()
        primary = self._primary_base(bases)
        devices: list[tuple[str, dict, str]] = []
        seen_ids: set[str] = set()
        for base in bases:
            discover = self._discover(base)
            device_id = str(discover.get("DeviceID") or "").strip().upper()
            if device_id and device_id in seen_ids:
                logger.info("[hdhomerun] %s is device %s again — skipping", base, device_id)
                continue
            if base == primary:
                prefix = ""
            elif device_id:
                prefix = f"{device_id}:"
            else:
                logger.warning("[hdhomerun] %s: unreachable or no DeviceID in discover.json — skipping", base)
                continue
            if device_id:
                seen_ids.add(device_id)
            devices.append((base, discover, prefix))
        return devices

    def fetch_channels(self) -> list[ChannelData]:
        devices = self._devices()
        if not devices:
            logger.warning("[hdhomerun] no device_url configured")
            return []
        multi = len(devices) > 1
        channels: list[ChannelData] = []
        for base, discover, prefix in devices:
            channels.extend(self._fetch_device_channels(base, discover, prefix, multi))
        return channels

    def _fetch_device_channels(self, base: str, discover: dict, prefix: str,
                               multi: bool) -> list[ChannelData]:
        lineup_url = discover.get("LineupURL") or f"{base}/lineup.json"
        r = self.get(lineup_url)
        if not r:
            logger.warning("[hdhomerun] lineup fetch failed from %s", lineup_url)
            return []
        try:
            rows = r.json()
        except Exception as exc:
            logger.error("[hdhomerun] invalid lineup JSON from %s: %s", lineup_url, exc)
            return []

        # Channel-level metadata (logo, affiliate, gracenote_id) from XMLTV.
        channel_meta, _ = self._fetch_xmltv(base, discover, prefix)

        device_id = str(discover.get("DeviceID") or "").strip().upper()
        # With several devices, tag each channel with its device so feeds can
        # filter by tuner and duplicates are easy to tell apart.
        device_tag = f"hdhr-{device_id.lower()}" if multi and device_id else None
        stream_prefix = f"{device_id}/" if prefix else ""

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
            if device_tag:
                tags.append(device_tag)

            channel_id = f"{prefix}{guide_number}"
            channels.append(
                ChannelData(
                    source_channel_id=channel_id,
                    name=name,
                    stream_url=f"hdhr://{stream_prefix}{guide_number}",
                    logo_url=meta.get("logo_url"),
                    category=self._category_for(affiliate, name),
                    number=None,  # GuideNumber ("4.1") isn't an int; let the app assign
                    # Store the Gracenote station ID but default to 'off' so the
                    # channel stays in the standard M3U using the imported HDHR
                    # EPG. Users opt a channel into Gracenote routing via admin.
                    gracenote_id=meta.get("gracenote_id"),
                    gracenote_mode='off',
                    guide_key=channel_id,
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
        programs: list[ProgramData] = []
        for base, discover, prefix in self._devices():
            programs.extend(self._fetch_xmltv(base, discover, prefix)[1])
        return programs

    # ── XMLTV guide ──────────────────────────────────────────

    def _fetch_xmltv(self, base: str, discover: dict,
                     prefix: str = "") -> tuple[dict, list[ProgramData]]:
        """Download and parse one device's SiliconDust XMLTV guide. Cached per run.

        Returns (channel_meta, programs) where channel_meta maps guide number
        (LCN, e.g. "4.1") to {logo_url, affiliate, gracenote_id}, and each
        program is keyed to the channel ID prefix + LCN.
        """
        if base in self._xmltv_cache:
            return self._xmltv_cache[base]

        auth = discover.get("DeviceAuth")
        if not auth:
            logger.info("[hdhomerun] %s: no DeviceAuth — skipping cloud EPG", base)
            self._xmltv_cache[base] = ({}, [])
            return self._xmltv_cache[base]

        r = self.get(self.XMLTV_URL, params={"DeviceAuth": auth})
        if not r:
            self._xmltv_cache[base] = ({}, [])
            return self._xmltv_cache[base]

        try:
            root = ET.fromstring(r.content)
        except ET.ParseError as exc:
            logger.error("[hdhomerun] %s: XMLTV parse error: %s", base, exc)
            self._xmltv_cache[base] = ({}, [])
            return self._xmltv_cache[base]

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
            gracenote_id = normalize_gracenote_id(m.group(1)) if m else None
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
            pd = program_from_xmltv(f"{prefix}{ch['lcn']}", prog_el, live_flag=False,
                                    movie_from_program_id=False)
            if pd:
                programs.append(pd)

        logger.info("[hdhomerun] XMLTV %s: %d channels, %d programs",
                    discover.get("FriendlyName") or base, len(channel_meta), len(programs))
        self._xmltv_cache[base] = (channel_meta, programs)
        return self._xmltv_cache[base]

    # ── resolve ──────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("hdhr://"):
            return raw_url
        rest = raw_url[len("hdhr://"):]
        if "/" in rest:
            # Secondary device: hdhr://<DeviceID>/<GuideNumber>
            device_id, guide_number = rest.split("/", 1)
            base = self._base_for_device_id(device_id)
            if not base:
                logger.warning("[hdhomerun] device %s not found at any configured address", device_id)
                return raw_url
            return self._stream_url_for(guide_number, base) or raw_url
        base = self._device_base()
        if not base:
            logger.warning("[hdhomerun] primary device %s not found at any configured address",
                           self._pinned_device_id())
            return raw_url
        return self._stream_url_for(rest, base) or raw_url

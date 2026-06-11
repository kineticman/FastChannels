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
  - GET api.hdhomerun.com/api/guide.php?DeviceAuth=… → rich Gracenote-sourced EPG
    (titles, synopses, S/E numbers, original air dates, series IDs, poster art)

Streams are raw MPEG-TS over HTTP at {host}:5004/auto/v{GuideNumber}.  They are
NOT HLS — they play in VLC/ffmpeg/most TV clients but not HLS-only web players.
We store an opaque hdhr://<GuideNumber> URL and rebuild the live :5004 URL in
resolve() so a DHCP IP change only requires updating the source config, not a
re-scrape.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from urllib.parse import urlencode, urlsplit

from .base import BaseScraper, ChannelData, ConfigField, ProgramData

logger = logging.getLogger(__name__)

_EPISODE_RE = re.compile(r'S(\d+)E(\d+)', re.IGNORECASE)

# Gracenote station ID embedded in the cloud-guide channel logo URL,
# e.g. https://img.hdhomerun.com/channels/US32639.png → "32639".  This is the
# Gracenote station ID, emitted as tvc-guide-stationid in the Gracenote M3U.
_STATION_ID_RE = re.compile(r'/channels/[A-Z]{2}(\d+)\.', re.IGNORECASE)

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
    CLOUD_GUIDE_URL = "https://api.hdhomerun.com/api/guide.php"
    # Each page advances ~4h (the least-covered channel's window); 12 pages
    # covers ~36-48h of gap-free guide. Each page is one fast HTTP call.
    GUIDE_MAX_PAGES = 12

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
        self._guide_cache: list[dict] | None = None

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

    @staticmethod
    def _station_id_from_image(url: str | None) -> str | None:
        """Extract the Gracenote station ID from the cloud-guide logo URL."""
        if not url:
            return None
        m = _STATION_ID_RE.search(url)
        return m.group(1) if m else None

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

        # Channel-level metadata (logo, affiliate) only comes from the cloud guide.
        guide_meta = self._guide_channel_meta(discover)

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
            meta = guide_meta.get(guide_number, {})
            affiliate = (meta.get("Affiliate") or "").strip()
            tags = list(lineup_tags)
            if affiliate and affiliate.lower() not in tags:
                tags.append(affiliate)

            channels.append(
                ChannelData(
                    source_channel_id=guide_number,
                    name=name,
                    stream_url=f"hdhr://{guide_number}",
                    logo_url=meta.get("ImageURL"),
                    category=self._category_for(affiliate, name),
                    number=None,  # GuideNumber ("4.1") isn't an int; let the app assign
                    # Store the Gracenote station ID but default to 'off' so the
                    # channel stays in the standard M3U using the imported HDHR
                    # EPG. Users opt a channel into Gracenote routing via admin.
                    gracenote_id=self._station_id_from_image(meta.get("ImageURL")),
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
        guide = self._cloud_guide(self._discover())
        if not guide:
            return []

        programs: list[ProgramData] = []
        for ch_entry in guide:
            guide_number = str(ch_entry.get("GuideNumber") or "").strip()
            if not guide_number:
                continue
            for prog in (ch_entry.get("Guide") or []):
                pd = self._program_from_guide(guide_number, prog)
                if pd:
                    programs.append(pd)

        logger.info("[hdhomerun] %d programs across %d channels", len(programs), len(guide))
        return programs

    def _program_from_guide(self, guide_number: str, prog: dict) -> ProgramData | None:
        title = prog.get("Title")
        start = self._unix_to_dt(prog.get("StartTime"))
        end = self._unix_to_dt(prog.get("EndTime"))
        if not title or not start or not end:
            return None

        season = episode = None
        m = _EPISODE_RE.search(prog.get("EpisodeNumber") or "")
        if m:
            season, episode = int(m.group(1)), int(m.group(2))

        series_id = prog.get("SeriesID") or None
        if series_id and series_id.startswith("MV"):
            program_type = "movie"
        elif prog.get("EpisodeNumber"):
            program_type = "episode"
        else:
            program_type = None

        category = None
        filt = prog.get("Filter")
        if isinstance(filt, list) and filt:
            category = filt[0]

        return ProgramData(
            source_channel_id=guide_number,
            title=title,
            start_time=start,
            end_time=end,
            description=prog.get("Synopsis"),
            poster_url=prog.get("PosterURL") or prog.get("ImageURL"),
            category=category,
            episode_title=prog.get("EpisodeTitle") or None,
            season=season,
            episode=episode,
            original_air_date=self._unix_to_date(prog.get("OriginalAirdate")),
            program_type=program_type,
            series_id=series_id,
            episode_id=series_id,  # HDHR has no stable per-episode id; series is closest
        )

    # ── cloud guide ──────────────────────────────────────────

    def _cloud_guide(self, discover: dict) -> list[dict]:
        """Fetch the SiliconDust cloud guide, paginating forward in time.

        Returns a list of {GuideNumber, GuideName, Affiliate, ImageURL, Guide:[…]}
        with each channel's Guide arrays merged across pages.  Cached per run.
        """
        if self._guide_cache is not None:
            return self._guide_cache

        auth = discover.get("DeviceAuth")
        if not auth:
            logger.info("[hdhomerun] no DeviceAuth — skipping cloud EPG")
            self._guide_cache = []
            return self._guide_cache

        # Each guide.php call returns a ~4-7h window per channel, but the window
        # end varies per channel within a page. Advancing the next Start to the
        # GLOBAL max end would skip the tail of every channel that ended earlier,
        # leaving same-time gaps on most channels. Instead advance to the MINIMUM
        # per-channel frontier so no channel is skipped, and dedup the resulting
        # overlap by (channel, StartTime).
        merged: dict[str, dict] = {}
        seen: dict[str, set] = {}
        start: int | None = None
        for _ in range(self.GUIDE_MAX_PAGES):
            params = {"DeviceAuth": auth}
            if start is not None:
                params["Start"] = start
            r = self.get(self.CLOUD_GUIDE_URL, params=params)
            if not r:
                break
            try:
                page = r.json()
            except Exception as exc:
                logger.debug("[hdhomerun] guide page parse error: %s", exc)
                break
            if not page:
                break

            channel_ends: list[int] = []  # last EndTime seen per channel this page
            for ch_entry in page:
                gn = str(ch_entry.get("GuideNumber") or "").strip()
                if not gn:
                    continue
                slot = merged.setdefault(gn, {**ch_entry, "Guide": []})
                starts = seen.setdefault(gn, set())
                ch_end = 0
                for prog in (ch_entry.get("Guide") or []):
                    st = prog.get("StartTime")
                    ch_end = max(ch_end, prog.get("EndTime") or 0)
                    if st in starts:
                        continue  # already merged from an overlapping page
                    starts.add(st)
                    slot["Guide"].append(prog)
                if ch_end:
                    channel_ends.append(ch_end)

            # Frontier = the least-covered channel's end this page. Channels that
            # returned no data (genuine upstream gap) don't drag the frontier.
            if not channel_ends:
                break
            next_start = min(channel_ends)
            if start is not None and next_start <= start:
                break
            start = next_start

        self._guide_cache = list(merged.values())
        return self._guide_cache

    def _guide_channel_meta(self, discover: dict) -> dict[str, dict]:
        """Map GuideNumber → channel-level guide metadata (logo, affiliate)."""
        return {
            str(c.get("GuideNumber") or "").strip(): c
            for c in self._cloud_guide(discover)
            if c.get("GuideNumber")
        }

    # ── resolve ──────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("hdhr://"):
            return raw_url
        guide_number = raw_url[len("hdhr://"):]
        return self._stream_url_for(guide_number) or raw_url

    # ── datetime helpers ─────────────────────────────────────

    @staticmethod
    def _unix_to_dt(val) -> datetime | None:
        try:
            return datetime.fromtimestamp(int(val), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            return None

    @classmethod
    def _unix_to_date(cls, val):
        dt = cls._unix_to_dt(val)
        if not dt or dt.year <= 1970:
            return None
        return dt.date()

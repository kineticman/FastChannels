
from __future__ import annotations

import html as _html
import json
import logging
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata
from .category_utils import infer_category_from_name

logger = logging.getLogger(__name__)


class AmazonPrimeFreeScraper(BaseScraper):
    """
    FastChannels scraper for Amazon Prime Video free linear (FAST) channels.

    - Scrapes channel metadata from the Live TV page and paginated collection API.
    - Builds near-term EPG from the station.schedule arrays embedded in those responses.
    - Resolves live DASH stream URLs via direct HTTP calls to Amazon's PRS endpoint
      (no browser required). All channels are bulk-resolved during scrape in ~30s using
      parallel requests. URLs are cached ~1.5 h in source.config.
    - Streams are CENC-encrypted DASH (Widevine + PlayReady); DRM-capable clients only
      (e.g. Kodi + inputstream.adaptive).
    """

    source_name = "amazon_prime_free"
    source_aliases = ("amazon-prime-free",)
    display_name = "Amazon Prime Free Channels"
    scrape_interval = 100  # minutes — keep well under the 2-hour DASH URL TTL

    stream_audit_enabled = True
    audit_requires_config = ['cookie_header']
    license_url = 'https://atv-ps.amazon.com/playback/drm-linear/GetWidevineLicense'
    kodi_props = {
        'inputstream': 'inputstream.adaptive',
        'inputstream.adaptive.manifest_type': 'mpd',
        'inputstream.adaptive.license_type': 'com.widevine.alpha',
    }

    phase_timeouts = {
        "init":      30,
        "bootstrap": 60,
        "channels":  180,   # bulk PRS resolution: ~30s typical, 180s generous ceiling
        "epg":       300,
    }

    config_schema = [
        ConfigField(
            "amazon_email",
            "Amazon Email",
            field_type="text",
            placeholder="you@example.com",
            help_text="Your Amazon account email. Used with Auto-Login to obtain session cookies automatically.",
        ),
        ConfigField(
            "amazon_password",
            "Amazon Password",
            field_type="password",
            secret=True,
            help_text="Your Amazon account password. Stored securely alongside other source credentials.",
        ),
        ConfigField(
            "cookie_header",
            "Amazon Cookie Header",
            field_type="password",
            secret=True,
            help_text="Active session cookies. Populated automatically by Auto-Login, or paste manually from browser DevTools.",
        ),
    ]

    LIVE_TV_URL = "https://www.amazon.com/gp/video/livetv"
    PAGINATE_URL = "https://www.amazon.com/gp/video/api/paginateCollection"

    DEFAULT_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.amazon.com/gp/video/storefront/",
    }

    PAGINATE_HEADERS = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.amazon.com/gp/video/livetv",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest",
        "X-Amzn-Client-Ttl-Seconds": "15",
    }

    # Observed in the analyzed HAR. Keep these centralized so they are easy to adjust.
    # The full dynamicFeatures list (matching what browsers send) is required for
    # hasMoreItems=True — a shorter list causes Amazon to truncate pagination at ~10 items.
    PAGINATE_DEFAULT_PARAMS: dict[str, Any] = {
        "pageType": "home",
        "pageId": "live",
        "collectionType": "Container",
        "actionScheme": "default",
        "payloadScheme": "default",
        "decorationScheme": "web-decoration-asin-v4",
        "featureScheme": "web-features-v6",
        "widgetScheme": "web-explore-v33",
        "variant": "desktopWindows",
        "journeyIngressContext": "28|CgVQcmltZQoLZnJlZXdpdGhhZHM=",
        "dynamicFeatures": [
            "integration",
            "CLIENT_DECORATION_ENABLE_DAAPI",
            "ENABLE_DRAPER_CONTENT",
            "HorizontalPagination",
            "CleanSlate",
            "EpgContainerPagination",
            "ENABLE_GPCI",
            "SupportsImageTextLinkTextInStandardHero",
            "Remaster",
            "SupportsChannelWidget",
            "PromotionalBannerSupported",
            "HERO_IMAGE_OPTIONAL",
            "RemoveFromContinueWatching",
            "ENABLE_CSIR",
            "SearchChannelBundles",
            "LinearStationsInHero",
            "LinearStationInAllCarousels",
            "SupportChannelItemDecoration",
            "TvodMovieBundles",
        ],
    }

    _STATION_NEEDLE = '"station":{'

    # Direct HTTP stream URL resolution (no browser required)
    _PRS_URL = "https://atv-ps.amazon.com/cdp/catalog/GetPlaybackResources"
    _STREAM_URL_TTL = 5400   # 1.5 hours — well under Amazon's 2-hour TTL
    _PRS_WORKERS = 20        # parallel HTTP workers for bulk resolution
    _PRS_TIMEOUT = 10        # per-request timeout (seconds)

    def __init__(self, config: dict | None = None):
        super().__init__(config)

        self._cookie_header = (self.config.get("cookie_header") or "").strip()
        self._marketplace_id = "ATVPDKIKX0DER"
        self._ux_locale = "en_US"
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
        )

        self.session.headers.update(self.DEFAULT_HEADERS)
        self.session.headers.update({"User-Agent": user_agent})
        if self._cookie_header:
            self.session.headers["Cookie"] = self._cookie_header

        # Stable device UUID for PRS calls — Amazon associates the deviceID with auth state.
        # Generate once and persist so we reuse the same identity across scrapes.
        self._prs_device_id: str = (
            self.config.get("prs_device_id") or str(uuid.uuid4())
        )
        if not self.config.get("prs_device_id"):
            self._pending_config_updates["prs_device_id"] = self._prs_device_id

        # PRS request headers (shared across all parallel workers)
        at_main = self._extract_cookie("at-main") or ""
        self._prs_headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Authorization": f"Bearer {at_main}",
            "Cookie": self._cookie_header,
            "Origin": "https://www.amazon.com",
            "Referer": "https://www.amazon.com/",
            "User-Agent": user_agent,
        }

        # fetch_channels() populates this; fetch_epg() reads from it.
        self._station_cache: dict[str, dict[str, Any]] = {}

        # Stream URL cache: {station_id: {"url": str, "expires_at": float}}
        # Persisted in source.config["stream_url_cache"] across scrapes.
        raw_cache = self.config.get("stream_url_cache") or {}
        self._stream_url_cache: dict[str, dict[str, Any]] = (
            raw_cache if isinstance(raw_cache, dict) else {}
        )

        # Per-channel playback envelopes harvested from the livetv carousel during scrape:
        # {station_gti: {"pe": str, "expiry": ms, "asin": str}}. Each channel's PE is bound to
        # THAT channel's content — using one channel's PE for another returns the wrong stream.
        # The PE TTL is ~3 h and the scrape interval is well under that, so a scraped PE is
        # normally still valid at play time; if expired we re-mint via enrichItemMetadata (ASIN).
        raw_cpe = self.config.get("channel_pe") or {}
        self._channel_pe: dict[str, dict[str, Any]] = (
            raw_cpe if isinstance(raw_cpe, dict) else {}
        )

    # ------------------------------------------------------------------
    # DRM / license support
    # ------------------------------------------------------------------

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        from urllib.parse import urlencode
        import uuid as _uuid
        params = {
            'deviceID': config.get('prs_device_id', '') or '',
            'deviceTypeID': 'AOAGZA014O5RE',
            'gascEnabled': 'false',
            'marketplaceID': 'ATVPDKIKX0DER',
            'uxLocale': 'en_US',
            'firmware': '1',
            'nerid': _uuid.uuid4().hex[:18] + '00',
        }
        if channel_id:
            params['titleId'] = channel_id
        return f'{cls.license_url}?{urlencode(params)}'

    # The player's GetLivePlaybackResources returns a *coherent* session: manifest URLs,
    # sessionHandoffToken, and the Widevine service certificate all keyed to the same content
    # keys. The Widevine license server only entitles keys from this endpoint, so both stream
    # resolution (resolve()) and SHT acquisition use it. The catalog GetPlaybackResources
    # endpoint returns a manifest with a DIFFERENT, unentitled KID — licensing that KID against
    # this session's SHT returns 403 Denied. Captured from the real web player; see
    # dev/lab_notes_amazon_drm.md.
    _LIVE_PRS_URL = "https://atv-ps.amazon.com/playback/prs/GetLivePlaybackResources"

    @classmethod
    def _channel_pe_for(cls, config: dict, title_id: str) -> str | None:
        """Return the playbackEnvelope bound to `title_id`: the value scraped from the livetv
        carousel if still valid, else a freshly minted one via enrichItemMetadata (cookies
        only, no browser)."""
        import time as _time
        entry = (config.get('channel_pe') or {}).get(title_id) or {}
        pe = entry.get('pe')
        # expiry is epoch-ms; refresh a minute early to avoid races
        if pe and float(entry.get('expiry', 0)) / 1000.0 > _time.time() + 60:
            return pe
        fresh = cls._enrich_pe(config, entry.get('asin'))
        return fresh or pe or None

    @classmethod
    def _enrich_pe(cls, config: dict, asin: str | None) -> str | None:
        """Mint a fresh per-channel playbackEnvelope for `asin` via the web enrichItemMetadata
        API (uses session cookies; no playbackEnvelope or browser required)."""
        if not asin:
            return None
        import requests as _req
        cookie = config.get('cookie_header', '') or ''
        ua = ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36')
        data = {
            'metadataToEnrich': json.dumps({'placement': 'HOVER', 'playback': True}),
            'titleIDsToEnrich': json.dumps([asin]),
            'currentUrl': 'https://www.amazon.com/gp/video/livetv',
        }
        try:
            r = _req.post('https://www.amazon.com/gp/video/api/enrichItemMetadata', data=data,
                          headers={'Content-Type': 'application/x-www-form-urlencoded',
                                   'X-Requested-With': 'XMLHttpRequest', 'Cookie': cookie,
                                   'Referer': 'https://www.amazon.com/gp/video/livetv',
                                   'User-Agent': ua}, timeout=12)
            node = (r.json().get('enrichments') or {}).get(asin) or {}
            for pa in (node.get('playbackActions') or []):
                pe = (pa.get('playbackExperienceMetadata') or {}).get('playbackEnvelope')
                if pe:
                    logger.info('[amazon] minted fresh PE for %s via enrich (%d chars)', asin, len(pe))
                    return pe
        except Exception as exc:
            logger.warning('[amazon] enrichItemMetadata failed for %s: %s', asin, exc)
        return None

    @classmethod
    def _get_live_playback_resources(cls, config: dict, title_id: str,
                                     pe: str | None = None) -> dict | None:
        """POST GetLivePlaybackResources with the full multi-resource body and return the
        parsed response (livePlaybackUrls + sessionization + widevineServiceCertificate), or
        None on error. `pe` must be the per-channel playbackEnvelope for `title_id` — the
        endpoint returns whatever content the PE is bound to, ignoring title_id otherwise."""
        import uuid as _uuid
        import requests as _req
        pe = pe or cls._channel_pe_for(config, title_id)
        if not pe:
            return None
        cookie = config.get('cookie_header', '') or ''
        device_id = config.get('prs_device_id', '') or ''
        ua = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36')
        params = {
            'deviceID': device_id, 'deviceTypeID': 'AOAGZA014O5RE', 'gascEnabled': 'false',
            'marketplaceID': 'ATVPDKIKX0DER', 'uxLocale': 'en_US', 'firmware': '1',
            'titleId': title_id, 'nerid': _uuid.uuid4().hex[:18] + '00',
        }
        body = {
            'globalParameters': {
                'deviceCapabilityFamily': 'WebPlayer',
                'playbackEnvelope': pe,
                'capabilityDiscriminators': {
                    'operatingSystem': {'name': 'Windows', 'version': 'unknown'},
                    'middleware': {'name': 'Chrome', 'version': '146.0.0.0'},
                    'nativeApplication': {'name': 'Chrome', 'version': '146.0.0.0'},
                    'hfrControlMode': 'Legacy',
                    'displayResolution': {'height': 1080, 'width': 1920},
                },
                'sessionTrackingMode': 'WITH_SESSION_HANDOFF',
                'userWatchSessionId': str(_uuid.uuid4()),
            },
            'widevineServiceCertificateRequest': {},
            'playbackDataRequest': {},
            # maxVideoResolution '480p' makes Amazon issue an SD-policy key (single KID, policy
            # marker "SD", reps up to 576p), which releases to L3 software CDMs (desktop
            # Chrome/Shaka, Linux Kodi). A higher cap yields an HD-policy key that L3 cannot
            # license — Amazon then returns 403 {"code":"Downgrade.Sd"}. Real TV clients with L1
            # hardware are unaffected by the cap beyond the 576p ceiling.
            'livePlaybackUrlsRequest': {
                'device': {
                    'firmwareVersion': 'UNKNOWN', 'hdcpLevel': '1.4',
                    'liveManifestTypes': ['Accumulating', 'Live'],
                    'maxVideoResolution': '480p', 'operatingSystem': 'Windows',
                    'supportedStreamingTechnologies': ['DASH'],
                    'streamingTechnologies': {'DASH': {
                        'bitrateAdaptations': ['CBR', 'CVBR'], 'codecs': ['H264'],
                        'drmKeyScheme': 'DualKey', 'drmType': 'Widevine',
                        'dynamicRangeFormats': ['None'],
                        'edgeDeliveryAuthorizationSchemes': ['PVExchangeV1', 'Transparent'],
                        'fragmentRepresentations': ['ByteOffsetRange', 'SeparateFile'],
                        'frameRates': ['Standard'],
                    }},
                    'thumbnailRepresentations': ['None'],
                },
                'playbackSettingsRequest': {
                    'firmware': 'UNKNOWN', 'playerType': 'xp',
                    'responseFormatVersion': '1.0.0', 'titleId': title_id,
                },
            },
        }
        try:
            r = _req.post(cls._LIVE_PRS_URL, params=params, json=body, headers={
                'Content-Type': 'application/json', 'Cookie': cookie,
                'Origin': 'https://www.amazon.com', 'Referer': 'https://www.amazon.com/',
                'User-Agent': ua,
            }, timeout=12)
            data = r.json()
        except Exception as exc:
            logger.warning('[amazon] GetLivePlaybackResources failed for %s: %s', title_id[:40], exc)
            return None
        if data.get('globalError'):
            logger.warning('[amazon] GetLivePlaybackResources globalError for %s: %s',
                           title_id[:40], data['globalError'])
            return None
        return data

    @classmethod
    def _get_session_handoff_token(cls, config: dict, channel_id: str) -> str | None:
        """Return a sessionHandoffToken coherent with the manifest resolve() serves: use the
        browser-cached value if fresh (< 4 min), else fetch a fresh one from the same
        GetLivePlaybackResources endpoint using this channel's playbackEnvelope."""
        import time as _time
        pe = cls._channel_pe_for(config, channel_id)
        if not pe:
            return None
        shtc = config.get('sht_cache') or {}
        cached = shtc.get(channel_id) if channel_id else None
        if cached and isinstance(cached, dict):
            age = _time.time() - float(cached.get('ts', 0))
            if age < 240 and cached.get('token'):
                logger.debug('[amazon] using cached sessionHandoffToken for %s (age=%.0fs)', channel_id, age)
                return cached['token']
        data = cls._get_live_playback_resources(config, channel_id, pe)
        if not data:
            return None
        token = (data.get('sessionization') or {}).get('sessionHandoffToken')
        if token:
            logger.debug('[amazon] fresh SHT via GetLivePlaybackResources for %s (%d chars)',
                        channel_id, len(token))
            return token
        logger.warning('[amazon] GetLivePlaybackResources missing sessionHandoffToken for %s', channel_id)
        return None

    @classmethod
    def prepare_license_request(cls, challenge: bytes, config: dict, channel_id: str | None = None) -> tuple[bytes, dict]:
        import base64 as _b64
        cookie = config.get('cookie_header', '') or ''
        # The license body must carry THIS channel's playbackEnvelope — the one the manifest
        # and SHT were derived from — or Amazon returns 403 Denied.
        playback_envelope = cls._channel_pe_for(config, channel_id) if channel_id else ''

        session_handoff_token = None
        if playback_envelope and channel_id:
            session_handoff_token = cls._get_session_handoff_token(config, channel_id)

        body_dict: dict = {
            'includeHdcpTestKey': True,
            'licenseChallenge': _b64.b64encode(challenge).decode('ascii'),
        }
        if playback_envelope:
            body_dict['playbackEnvelope'] = playback_envelope
        if session_handoff_token:
            body_dict['sessionHandoffToken'] = session_handoff_token

        body = json.dumps(body_dict).encode('utf-8')
        headers = {
            'Cookie': cookie,
            'Content-Type': 'text/plain',
            'Origin': 'https://www.amazon.com',
            'Referer': 'https://www.amazon.com/',
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36'
            ),
        }
        return body, headers

    @classmethod
    def process_license_response(cls, response_bytes: bytes) -> bytes:
        """Amazon returns JSON with base64-encoded license at widevineLicense.license."""
        try:
            import base64 as _b64
            data = json.loads(response_bytes)
            wv = data.get('widevineLicense') or {}
            encoded = (wv.get('license') if isinstance(wv, dict) else '') or ''
            if encoded:
                return _b64.b64decode(encoded + '==')
        except Exception:
            pass
        return response_bytes

    @classmethod
    def get_kodi_props_for_channel(cls, base_url: str, source_channel_id: str) -> dict[str, str]:
        props = dict(cls.kodi_props)
        props['inputstream.adaptive.license_key'] = (
            f'{base_url}/play/amazon_prime_free/license?channel_id={source_channel_id}||R{{SSM}}|'
        )
        return props

    def fetch_channels(self) -> list[ChannelData]:
        self._station_cache = {}

        page = self.get(self.LIVE_TV_URL)
        if not page:
            logger.error("[%s] failed to load Live TV page", self.source_name)
            return []

        html = page.text
        stations = self._extract_initial_stations(html)
        self._harvest_pe_from_html(html)

        seed = self._extract_pagination_seed(html)
        if seed:
            paged = self._paginate_stations(seed)
            for station in paged.values():
                station_id = self._station_id(station)
                if station_id and station_id not in stations:
                    stations[station_id] = station
        else:
            logger.warning("[%s] could not find pagination seed in Live TV HTML", self.source_name)

        channels: list[ChannelData] = []
        for station_id, station in stations.items():
            channel = self._channel_from_station(station_id, station)
            if channel:
                channels.append(channel)
                self._station_cache[station_id] = station

        channels.sort(key=lambda c: c.name.lower())
        logger.info("[%s] %d channels", self.source_name, len(channels))

        # Persist the per-channel playback envelopes harvested from the carousel. Prune entries
        # for channels no longer present so the map doesn't grow unbounded.
        if self._channel_pe:
            live_ids = {c.source_channel_id for c in channels}
            self._channel_pe = {k: v for k, v in self._channel_pe.items() if k in live_ids}
            self._pending_config_updates["channel_pe"] = dict(self._channel_pe)
            logger.info("[%s] harvested playback envelopes for %d/%d channels",
                        self.source_name, len(self._channel_pe), len(channels))

        # Stream URLs are resolved lazily at play time in resolve() via GetLivePlaybackResources
        # (the only endpoint whose manifest KID the Widevine license path entitles). No bulk
        # pre-warm: that requires a valid playbackEnvelope per channel and the catalog URLs it
        # produced were unplayable (wrong KID → 403 Denied at license time).
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        programs: list[ProgramData] = []
        seen: set[tuple[str, int, int, str]] = set()

        for channel in channels:
            station = self._station_cache.get(channel.source_channel_id)
            if not station:
                continue

            for airing in station.get("schedule", []):
                program = self._program_from_schedule(channel.source_channel_id, airing)
                if not program:
                    continue

                dedupe_key = (
                    program.source_channel_id,
                    int(program.start_time.timestamp()),
                    int(program.end_time.timestamp()),
                    program.title,
                )
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                programs.append(program)

        programs.sort(key=lambda p: (p.source_channel_id, p.start_time, p.title))
        logger.info("[%s] %d EPG entries", self.source_name, len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        """Resolve a playable, DRM-entitled DASH manifest at play time via
        GetLivePlaybackResources, using THIS channel's playbackEnvelope. The manifest's KID is
        entitled by the license proxy's SHT session (also fetched with this channel's PE)."""
        if not raw_url.startswith("primefree://"):
            return raw_url

        station_id = raw_url[len("primefree://"):]
        cached = self._stream_url_cache.get(station_id)
        if cached and cached.get("expires_at", 0) > time.time():
            logger.debug("[%s] resolve cache hit for %s", self.source_name, station_id[:40])
            return cached["url"]

        if not self._cookie_header:
            logger.warning("[%s] no cookie_header — cannot resolve stream URL for %s",
                           self.source_name, station_id[:40])
            return raw_url
        pe = self._channel_pe_for(self.config, station_id)
        if not pe:
            logger.warning("[%s] no playback envelope for %s — cannot resolve entitled stream "
                           "(re-scrape to refresh)", self.source_name, station_id[:40])
            return None

        logger.debug("[%s] resolving live stream for %s", self.source_name, station_id[:40])
        url = self._resolve_live(station_id, pe)
        if url:
            self._stream_url_cache[station_id] = {
                "url": url,
                "expires_at": time.time() + self._STREAM_URL_TTL,
            }
            updated = dict(self.config.get("stream_url_cache") or {})
            updated[station_id] = self._stream_url_cache[station_id]
            self._pending_config_updates["stream_url_cache"] = updated
            return url

        logger.warning("[%s] could not resolve live stream for %s", self.source_name, station_id[:40])
        return None

    def audit_resolve(self, raw_url: str) -> str:
        """Liveness-only resolution for the stream audit. Uses the catalog endpoint
        (cookies only, no playbackEnvelope) since the audit just checks the manifest is
        reachable — it does not exercise DRM. Keeps audits independent of PE freshness."""
        if not raw_url.startswith("primefree://"):
            return raw_url
        station_id = raw_url[len("primefree://"):]
        if not self._cookie_header:
            return raw_url
        url = self._resolve_channels([station_id]).get(station_id, "")
        return url if (url and not url.startswith("prs_error:")) else None

    def _resolve_live(self, title_id: str, pe: str | None = None) -> str | None:
        """Resolve the live DASH manifest URL from a GetLivePlaybackResources session."""
        data = self._get_live_playback_resources(self.config, title_id, pe)
        if not data:
            return None
        url_sets = data.get("livePlaybackUrls", {}).get("urlSets", {})
        default_id = data.get("livePlaybackUrls", {}).get("defaultUrlSetId", "")
        url = self._pick_manifest_from_urlsets(url_sets, default_id)
        if url:
            return url
        # Fallback: scan the response for a manifest URL (prefer a Qwilt host for clean URLs).
        blob = json.dumps(data)
        mpds = re.findall(r'https://[^"\\]+?\.mpd[^"\\]*', blob)
        if not mpds:
            return None
        return next((u for u in mpds if "qw" in u.lower()), mpds[0])

    @staticmethod
    def _pick_manifest_from_urlsets(url_sets: dict, default_id: str = "") -> str:
        """Pick a manifest URL from a PRS urlSets dict: prefer Qwilt CDN (clean URL, no
        obfuscating auth tokens), then the default set, then the first available."""
        for sdata in url_sets.values():
            m = sdata.get("urls", {}).get("manifest", {})
            if m.get("cdn") == "Qwilt" and m.get("url"):
                return m["url"]
        if default_id:
            u = url_sets.get(default_id, {}).get("urls", {}).get("manifest", {}).get("url", "")
            if u:
                return u
        for sdata in url_sets.values():
            u = sdata.get("urls", {}).get("manifest", {}).get("url", "")
            if u:
                return u
        return ""

    # ------------------------------------------------------------------
    # Direct HTTP stream URL resolution (Amazon PRS endpoint)
    # ------------------------------------------------------------------

    def _extract_cookie(self, name: str) -> str | None:
        m = re.search(rf'(?:^|;\s*){re.escape(name)}=([^;]+)', self._cookie_header)
        return m.group(1).strip() if m else None

    def _resolve_channels(self, station_ids: list[str]) -> dict[str, str]:
        """
        Resolves DASH manifest URLs for the given station GIPs via direct HTTP calls
        to Amazon's GetPlaybackResources PRS endpoint.  Uses a thread pool for parallel
        resolution — typical throughput is 20+ channels/second.

        Returns {station_id: dash_url} for all successfully resolved channels.
        Streams are CENC-encrypted (Widevine + PlayReady); DRM-capable clients only.
        """
        if not self._cookie_header:
            return {}

        results: dict[str, str] = {}

        import requests as _requests  # local import to avoid shadowing module-level name

        def _resolve_one(gip: str) -> tuple[str, str]:
            try:
                r = _requests.get(
                    self._PRS_URL,
                    params={
                        "deviceID": self._prs_device_id,
                        "deviceTypeID": "AOAGZA014O5RE",
                        "gascEnabled": "false",
                        "marketplaceID": self._marketplace_id,
                        "uxLocale": self._ux_locale,
                        "firmware": "1",
                        "playerType": "xp",
                        "operatingSystemName": "Windows",
                        "operatingSystemVersion": "10.0",
                        "deviceApplicationName": "Chrome",
                        "asin": gip,
                        "consumptionType": "Streaming",
                        "desiredResources": "PlaybackUrls,PlaybackSettings",
                        "resourceUsage": "CacheResources",
                        "videoMaterialType": "LiveStreaming",
                        "userWatchSessionId": str(uuid.uuid4()),
                        "displayWidth": "1920",
                        "displayHeight": "1080",
                        "deviceStreamingTechnologyOverride": "DASH",
                        "deviceDrmOverride": "CENC",
                        "deviceAdInsertionTypeOverride": "SSAI",
                        "deviceVideoCodecOverride": "H264",
                        # SD (not HD): Amazon labels the Widevine key with an HD/SD policy
                        # marker (PSSH protobuf field 5). HD keys are released only to L1
                        # hardware CDMs — desktop Chrome/Shaka and Linux Kodi run L3 software
                        # CDMs and get 403 {"code":"Denied"}. SD-policy keys release to L3,
                        # so SD plays on every client (capped ~576p vs 1080p). See
                        # dev/lab_notes_amazon_drm.md.
                        "deviceVideoQualityOverride": "SD",
                        "liveManifestType": "accumulating,live",
                        "playerAttributes": json.dumps({
                            "middlewareName": "Chrome",
                            "middlewareVersion": "146.0.0.0",
                            "nativeApplicationName": "Chrome",
                            "nativeApplicationVersion": "146.0.0.0",
                            "supportedAudioCodecs": "AAC",
                            "frameRate": "HFR",
                            "H264.codecLevel": "4.2",
                            "H265.codecLevel": "0.0",
                            "AV1.codecLevel": "0.0",
                        }, separators=(",", ":")),
                    },
                    headers=self._prs_headers,
                    timeout=self._PRS_TIMEOUT,
                )
                body = r.json()
            except Exception as exc:
                logger.debug("[%s] PRS request failed for %s: %s", self.source_name, gip[:40], exc)
                return gip, ""

            url_sets = body.get("playbackUrls", {}).get("urlSets", {})
            if not url_sets:
                top_err = body.get("error", {}).get("errorCode", "")
                res_err = body.get("errorsByResource", {}).get("PlaybackUrls", {}).get("errorCode", "")
                err = top_err or res_err
                if err:
                    logger.warning("[%s] PRS error for %s: %s", self.source_name, gip[:40], err)
                    return gip, f"prs_error:{err}"
                return gip, ""

            # Prefer Qwilt CDN (clean URL, no obfuscating auth tokens in path)
            for sdata in url_sets.values():
                m = sdata.get("urls", {}).get("manifest", {})
                if m.get("cdn") == "Qwilt" and m.get("url"):
                    return gip, m["url"]
            # Fall back to default or first available
            default_id = body.get("playbackUrls", {}).get("defaultUrlSetId", "")
            manifest = (
                url_sets.get(default_id, {}).get("urls", {}).get("manifest", {}).get("url", "")
                or next(
                    (s.get("urls", {}).get("manifest", {}).get("url", "")
                     for s in url_sets.values()
                     if s.get("urls", {}).get("manifest", {}).get("url")),
                    "",
                )
            )
            return gip, manifest

        logger.info("[%s] resolving %d stream URLs via PRS (%d workers)...",
                    self.source_name, len(station_ids), self._PRS_WORKERS)
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=self._PRS_WORKERS) as pool:
            for gip, url in pool.map(_resolve_one, station_ids):
                if url and not url.startswith("prs_error:"):
                    results[gip] = url
        elapsed = time.time() - t0
        logger.info("[%s] PRS resolved %d/%d stream URLs in %.1fs",
                    self.source_name, len(results), len(station_ids), elapsed)
        return results

    # ------------------------------------------------------------------
    # HTML / JSON extraction helpers
    # ------------------------------------------------------------------

    def _extract_initial_stations(self, html: str) -> dict[str, dict[str, Any]]:
        stations: dict[str, dict[str, Any]] = {}
        start = 0

        while True:
            idx = html.find(self._STATION_NEEDLE, start)
            if idx == -1:
                break

            obj_start = idx + len('"station":')
            blob = self._extract_balanced_json(html, obj_start)
            start = obj_start + 1
            if not blob:
                continue

            try:
                station = json.loads(blob)
            except json.JSONDecodeError:
                continue

            station_id = self._station_id(station)
            if station_id:
                stations[station_id] = station

        logger.info("[%s] extracted %d stations from initial HTML", self.source_name, len(stations))
        return stations

    # ------------------------------------------------------------------
    # Per-channel playbackEnvelope harvesting
    # ------------------------------------------------------------------

    @staticmethod
    def _asin_from_fallback(url: str) -> str:
        m = re.search(r'/gp/video/detail/([A-Za-z0-9]{8,})', url or '')
        return m.group(1) if m else ''

    def _record_pe(self, gti: str, meta: dict, fallback_url: str = '') -> None:
        pe = (meta or {}).get('playbackEnvelope')
        if gti and pe:
            self._channel_pe[str(gti)] = {
                'pe': pe,
                'expiry': (meta or {}).get('expiryTime', 0),
                'asin': self._asin_from_fallback(fallback_url),
            }

    def _harvest_pe_from_entity(self, entity: dict) -> None:
        """Capture a channel's playbackEnvelope from a paginate carousel entity."""
        for pa in (entity.get('playbackActions') or []):
            meta = pa.get('playbackExperienceMetadata') or {}
            gti = pa.get('channelId') or meta.get('channelId')
            if gti and meta.get('playbackEnvelope'):
                self._record_pe(gti, meta, pa.get('fallbackUrl', ''))
                return

    def _harvest_pe_from_html(self, html: str) -> None:
        """Capture playbackEnvelopes for the initial-page channels embedded in the livetv HTML.
        In a playbackAction object `channelId` and `fallbackUrl` precede the nested
        `playbackExperienceMetadata.playbackEnvelope`, so associate each envelope with the
        nearest preceding channelId in the same object."""
        for m in re.finditer(r'"playbackEnvelope":"([^"]+)"', html):
            pe = m.group(1)
            back = html[max(0, m.start() - 1500):m.start()]
            cids = re.findall(r'"channelId":"(amzn1\.dv\.gti\.[0-9a-f-]+)"', back)
            if not cids:
                continue
            gti = cids[-1]
            if gti in self._channel_pe:
                continue
            exp = re.search(r'"expiryTime":(\d+)', html[m.start():m.start() + 400])
            fb = re.findall(r'"fallbackUrl":"([^"]+)"', back)
            self._record_pe(gti, {
                'playbackEnvelope': pe,
                'expiryTime': int(exp.group(1)) if exp else 0,
            }, fb[-1] if fb else '')

    def _extract_pagination_seed(self, html: str) -> dict[str, Any] | None:
        # All three fields live in the same JSON object in the page HTML.
        # There may be multiple pagination blocks (e.g. "Live events" and "Your stations").
        # We want the one whose EpgGroup entities contain station objects.
        pattern = re.compile(
            r'"paginationServiceToken":"(?P<token>[^"]+)"'
            r'[^}]{0,300}?"paginationStartIndex":(?P<start>\d+)'
            r'[^}]{0,300}?"paginationTargetId":"(?P<target>[^"]+)"',
            re.DOTALL,
        )
        best = None
        for m in pattern.finditer(html):
            # Check if station objects appear in the 5000 chars following this block —
            # that indicates this is the linear-station carousel, not a content carousel.
            window = html[m.start(): m.start() + 5000]
            if '"station":{' in window:
                best = m
                break  # take the first block that has station entities after it

        if best is None:
            # Fallback: use the last pagination block found (stations tend to be last)
            matches = list(pattern.finditer(html))
            if not matches:
                return None
            best = matches[-1]

        return {
            "start_index": int(best.group("start")),
            "pagination_target_id": best.group("target"),
            "service_token": best.group("token"),
        }

    def _paginate_stations(self, seed: dict[str, Any]) -> dict[str, dict[str, Any]]:
        stations: dict[str, dict[str, Any]] = {}
        start_index = int(seed["start_index"])
        pagination_target_id = seed["pagination_target_id"]
        service_token = seed["service_token"]
        has_more = True
        page_no = 0

        while has_more and page_no < 200:
            params = dict(self.PAGINATE_DEFAULT_PARAMS)
            params.update(
                {
                    "paginationTargetId": pagination_target_id,
                    "serviceToken": service_token,
                    "startIndex": str(start_index),
                }
            )

            response = self.get(self.PAGINATE_URL, params=params, headers=self.PAGINATE_HEADERS)
            if not response:
                break

            try:
                payload = response.json()
            except ValueError:
                if self._cookie_header:
                    logger.warning("[%s] non-JSON paginateCollection response at startIndex=%s — cookies may be expired", self.source_name, start_index)
                else:
                    logger.info("[%s] pagination requires auth (no cookie configured) — using %d channels from initial page only", self.source_name, len(stations))
                break

            entities = payload.get("entities", []) or []
            logger.debug("[%s] page startIndex=%s: %d entities, hasMoreItems=%s",
                         self.source_name, start_index, len(entities), payload.get("hasMoreItems"))
            for entity in entities:
                station = entity.get("station") or {}
                station_id = self._station_id(station)
                if station_id:
                    stations[station_id] = station
                    self._harvest_pe_from_entity(entity)

            has_more = bool(payload.get("hasMoreItems"))

            # Amazon returns an updated serviceToken in each response — must use it
            # for the next request or subsequent pages return empty results.
            pagination = payload.get("pagination") or {}
            next_token = pagination.get("serviceToken") or pagination.get("token")
            if next_token:
                service_token = next_token

            next_index = payload.get("startIndex")
            if has_more:
                if isinstance(next_index, int) and next_index > start_index:
                    start_index = next_index
                else:
                    start_index += len(entities)
                    if not entities:
                        break
            page_no += 1

        logger.info("[%s] extracted %d stations from pagination (%d pages)", self.source_name, len(stations), page_no)
        return stations

    @staticmethod
    def _extract_balanced_json(text: str, start_idx: int) -> str | None:
        depth = 0
        in_str = False
        esc = False
        started = False

        for i in range(start_idx, len(text)):
            ch = text[i]
            if not started:
                if ch == "{":
                    started = True
                    depth = 1
                else:
                    continue
                continue

            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue

            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start_idx : i + 1]

        return None

    # ------------------------------------------------------------------
    # Station / EPG mapping helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _station_id(station: dict[str, Any]) -> str | None:
        station_id = station.get("id")
        if not station_id:
            return None
        return str(station_id)

    def _channel_from_station(self, station_id: str, station: dict[str, Any]) -> ChannelData | None:
        name = _html.unescape((station.get("name") or "").strip())
        if not name:
            return None

        if station.get("isOnLinearNewsPage") or station.get("genre") == "news":
            category = "News"
        elif station.get("genre"):
            category = station["genre"].title()
        else:
            category = infer_category_from_name(name) or "Entertainment"

        return ChannelData(
            source_channel_id=station_id,
            name=name,
            stream_url=f"primefree://{station_id}",
            logo_url=station.get("logo"),
            category=category,
            language=infer_language_from_metadata(name, category),
            stream_type='dash',
        )

    def _program_from_schedule(self, source_channel_id: str, airing: dict[str, Any]) -> ProgramData | None:
        try:
            start_ms = int(airing["start"])
            end_ms = int(airing["end"])
        except (KeyError, TypeError, ValueError):
            return None

        if end_ms <= start_ms:
            return None

        metadata = airing.get("metadata") or {}
        title = (metadata.get("title") or "").strip() or "Unknown"
        synopsis = metadata.get("synopsis")
        poster = self._pick_image_url(metadata)
        rating = self._pick_rating(metadata)
        episode_title = metadata.get("episodeTitle") or None
        release_year = metadata.get("releaseYear")

        description = synopsis
        if release_year:
            description = f"{synopsis} ({release_year})" if synopsis else str(release_year)

        return ProgramData(
            source_channel_id=source_channel_id,
            title=title,
            start_time=datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc),
            end_time=datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc),
            description=description,
            poster_url=poster,
            category=self._guess_program_category(metadata),
            rating=rating,
            episode_title=episode_title,
        )

    @staticmethod
    def _pick_image_url(metadata: dict[str, Any]) -> str | None:
        for key in ("image", "modalImage"):
            image = metadata.get(key) or {}
            url = image.get("url")
            if url:
                return str(url)
        return None

    @staticmethod
    def _pick_rating(metadata: dict[str, Any]) -> str | None:
        rating = metadata.get("contentMaturityRating") or {}
        value = rating.get("rating")
        return str(value) if value else None

    @staticmethod
    def _guess_program_category(metadata: dict[str, Any]) -> str | None:
        badge = (metadata.get("linearBadge") or {}).get("label")
        if badge == "LIVE":
            return "Live"
        if badge == "ON NOW":
            return "Current"
        return None

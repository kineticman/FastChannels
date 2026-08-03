from __future__ import annotations

import base64
import hashlib
import json
import logging
import time
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, parse_qsl, unquote, urlencode, urlparse, urlunparse

from .base import (
    BaseScraper, ChannelData, ProgramData, ScrapeSkipError,
    infer_language_from_metadata,
)
from .category_utils import category_for_channel, infer_category_from_name

logger = logging.getLogger(__name__)

# ── In-place upgrade to the VIDAA 2.0 backend ────────────────────────────────
# The old vtvapp-ovp/tvch `catalogue-search` stack this scraper used to target is
# orphaned/dying (its `epg/grid` service 500s indefinitely). The real production
# app — Hisense Channels (net.vidaatv.hisense.android.tv) — talks to a completely
# different signed backend, reverse-engineered from the decompiled APK and
# verified live. We keep source_name="vidaa" so this replaces the existing source
# in place: the 2.0 channels arrive with new numeric ids and the old 1.0 channels
# (UUID ids) fall off inactive via normal reconcile once they stop being listed.
# Full writeup, including how each constant/endpoint was derived:
# dev/vidaa/MIGRATION_RESEARCH.md.

_APP_KEY             = "1204099470"
_APP_SECRET          = "64nprh5fhk2syebs6qlkmmpt3l7s3ljg"  # MD5 request-signing secret
_OAUTH_CLIENT_SECRET = "28B3E8943D6FADFB28071C303EF3F26AAC634B4BA1F9937FFD725F0ED516CA1952237D39DC2460219C46E0B7170577AD"
_OAUTH_URL           = "https://partner.vidaahub.com/ns/account/oauth2.0/access_token"
_LAYOUT_UI           = "https://partner-layout-ui.vidaahub.com"
_DETAIL_UI           = "https://partner-detail-ui.vidaahub.com"

_LIVE_TYPE_CODE      = "600007"   # tile/media typeCode for linear channels (VOD tiles are 600001)
_MEDIAS_BATCH_SIZE   = 5          # server hard-caps mediasInfo: 6+ ids -> "Exceeds the limit of quantity"
_EPG_STEP_HOURS      = 12         # relatedDate stride; each call returns ~13h so this keeps coverage gap-free
_EPG_HORIZON_DAYS    = 5          # confirmed live: schedule data stops between +5 and +6 days out
_EPG_CIRCUIT_THRESHOLD = 5        # consecutive failed EPG batches before giving up without more requests
_SPANISH_COLUMN_TITLE = "In Spanish"  # a language grouping, not a genre — forces language='es' instead of category

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

# deviceId isn't a capability gate (auth is accessToken + request signature) —
# it's an analytics/context field the backend expects present. Reusing a fixed
# captured value is fine; it doesn't need to be unique per install.
_DEVICE_ID = "002003059003001007000128pngito97hw8ktrcmhh4njsaof802ig"

_COMMON_PARAMS = {
    "appPackageName":      "net.vidaatv.hisense.android.tv",
    "appVersion":          "os_t.atvchannels.1.00.0.0.Q030800",
    "language":            "eng",
    "locale":              "USA",
    "country":             "USA",
    "brand":               "his",
    "capabilityCode":      "2026031101",
    "playerFamilyName":    "VDAndroid_TV",
    "playerFamilyVersion": "006110000",
    "deviceType":          "1",
    "appOwnership":        "oem",
    "personalizedRec":     "1",
    "localeLanguage":      "eng",
}


def _sign_query(params: dict[str, str]) -> str:
    keys = sorted(k for k, v in params.items() if v not in (None, ""))
    sign_data = "&".join(f"{k}={params[k]}" for k in keys)
    return base64.b64encode(hashlib.md5((sign_data + _APP_SECRET).encode()).digest()).decode()


def _sign_body(body: str) -> str:
    return base64.b64encode(hashlib.md5((body + _APP_SECRET).encode()).digest()).decode()


def _clean_stream_url(url: str) -> str:
    """Strip unfilled ad-macro placeholders (`[PLATFORM]`, `{ADS.DEVICE_COUNTRY}`)
    from the streamingParam manifest URL — real values (e.g. `ads.vauth=...`)
    are left untouched, only bracket/brace-delimited placeholders are dropped."""
    parsed = urlparse(url)
    params = parse_qsl(parsed.query, keep_blank_values=True)
    clean = [(k, v) for k, v in params
             if not ((v.startswith('[') and v.endswith(']')) or (v.startswith('{') and v.endswith('}')))]
    return urlunparse(parsed._replace(query=urlencode(clean)))


def _parse_mini_player(url: str) -> dict | None:
    """Unwrap a DRM tile's `streamingParam` — for encrypted channels it isn't a
    manifest, it's a `mini-player/V1.0/index-mini-player.html` wrapper page meant
    for a webview. It carries the real DASH manifest URL, the multi-DRM license
    URL, and a `stream_id` (a distinct asset id, not the Vidaa channel id) as
    encoded query params. Returns None if `url` isn't a recognizable wrapper."""
    if not url or 'mini-player' not in url:
        return None
    q = parse_qs(urlparse(url).query)
    streaming_url = unquote((q.get('streaming_url') or [''])[0])
    drm_url = unquote((q.get('drm_url') or [''])[0])
    stream_id = (q.get('stream_id') or [''])[0]
    if not streaming_url or not drm_url or not stream_id:
        return None
    return {
        'streaming_url': _clean_stream_url(streaming_url),
        'drm_url': drm_url,
        'stream_id': stream_id,
    }


def _chunk(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


class VidaaScraper(BaseScraper):
    source_name           = "vidaa"
    display_name          = "Vidaa Free TV"
    scrape_interval        = 720   # 12 hours
    stream_audit_enabled   = True
    channel_miss_threshold = 5

    # Truthy marker only — enables the /play/vidaa/license proxy route and the
    # audit's DRM-bridge branch. get_license_url() below always re-resolves the
    # real per-channel URL live rather than reusing this constant.
    license_url = "https://drm-vss.vidaahub.com/multi-drm/vss/license"

    # Channel discovery: 1 category-list call + ~15 paginated columnData calls +
    # ~55 batched mediasInfo POSTs (257 channels / 5 per batch). EPG: same batches
    # x 10 relatedDate steps (5-day horizon / 12h stride) — several hundred POSTs.
    phase_timeouts = {
        "init":      30,
        "bootstrap": 60,
        "channels":  180,
        "epg":       1800,
    }

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({"User-Agent": _UA})
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0

    # ── auth ─────────────────────────────────────────────────────────────────

    def _ensure_token(self) -> None:
        if self._access_token and time.monotonic() < self._token_expires_at:
            return
        r = self.session.get(_OAUTH_URL, params={
            "client_id": _APP_KEY,
            "client_secret": _OAUTH_CLIENT_SECRET,
            "grant_type": "client_credentials",
        }, timeout=20)
        r.raise_for_status()
        data = r.json()
        token = data.get("access_token")
        if not token:
            raise RuntimeError(f"Vidaa OAuth token missing in response: {data}")
        self._access_token = token
        expires_in = int(data.get("expires_in") or 3600)
        self._token_expires_at = time.monotonic() + max(expires_in - 120, 60)

    def _base_params(self, **extra: str) -> dict[str, str]:
        self._ensure_token()
        params = dict(_COMMON_PARAMS)
        params["deviceId"] = _DEVICE_ID
        params["accessToken"] = self._access_token
        params["commonRandomId"] = uuid.uuid4().hex
        params.update(extra)
        return params

    def _get(self, host: str, path: str, params: dict[str, str]) -> dict:
        signature = _sign_query(params)
        r = self.session.get(f"{host}{path}", params=params,
                              headers={"appKey": _APP_KEY, "x-sign-for": signature}, timeout=30)
        r.raise_for_status()
        return r.json()

    def _post(self, host: str, path: str, params: dict[str, str], body: dict) -> dict:
        body_str = json.dumps(body, separators=(",", ":"))
        signature = _sign_body(body_str)
        r = self.session.post(f"{host}{path}", params=params, data=body_str.encode(), headers={
            "appKey": _APP_KEY, "x-sign-for": signature, "Content-Type": "application/json",
        }, timeout=30)
        r.raise_for_status()
        return r.json()

    def _fetch_medias_info(self, channel_ids: list[int], related_date: int) -> dict:
        params = self._base_params(sceneCode="ottChannel", relatedDate=str(related_date))
        body = {"medias": [{"id": cid, "typeCode": _LIVE_TYPE_CODE} for cid in channel_ids]}
        return self._post(_DETAIL_UI, "/api/v1.0.0/detailApi/mediasInfo", params, body)

    # ── fetch_channels ───────────────────────────────────────────────────────

    def _discover_columns(self) -> list[dict]:
        # scene=osPagingChannel + resourceType=1, no columnId -> the Live TV tab's
        # category list (Movies/Entertainment/News & Opinion/Sports/Kids/Music/In
        # Spanish, each with a real column id). Column ids are catalogue data, not
        # a fixed schema — always discovered fresh, never hardcoded.
        params = self._base_params(scene="osPagingChannel", resourceType="1")
        data = self._get(_LAYOUT_UI, "/api/v1.0.0/layoutApi/activityResources", params)
        columns = data.get("columns") or []
        if not columns:
            raise RuntimeError(f"Vidaa category discovery returned no columns: {data}")
        return columns

    def _fetch_column_tiles(self, column_id) -> list[dict]:
        tiles_by_id: dict[int, dict] = {}
        page = 1
        while page <= 20:  # safety cap; real columns top out well under this
            params = self._base_params(columnId=str(column_id), scene="osPagingChannel", ratio="16:9")
            if page > 1:
                params["metaInfo"] = f"page={page},oneMoreTile=1"
            data = self._get(_LAYOUT_UI, "/api/v1.0.0/layoutApi/columnData", params)
            tiles = (data.get("column") or {}).get("tiles") or []
            if not tiles:
                break
            for tile in tiles:
                if tile.get("typeCode") == _LIVE_TYPE_CODE and tile.get("id") is not None:
                    tiles_by_id[tile["id"]] = tile
            page += 1
        return list(tiles_by_id.values())

    def _build_channel(self, media: dict, column_title: str | None) -> ChannelData | None:
        cid = media.get("id")
        show = media.get("showInfo") or {}
        chan = media.get("channelInfo") or {}
        name = (show.get("title") or "").strip()
        stream_url = chan.get("streamingParam")
        if not cid or not name or not stream_url:
            return None
        drm = bool((chan.get("streamingDetailParam") or {}).get("encryption"))
        if drm:
            # Encrypted tiles: streamingParam is a mini-player wrapper page, not a
            # manifest — unwrap it to the real DASH URL. If the shape doesn't match
            # what we've reverse-engineered, skip the tile rather than store a
            # wrapper URL that will never resolve to playable content.
            wrapped = _parse_mini_player(stream_url)
            if not wrapped:
                logger.warning("[vidaa] DRM tile %s (%s) has an unrecognized streamingParam shape — skipping", cid, name)
                return None
            stream_url = wrapped['streaming_url']
        else:
            stream_url = _clean_stream_url(stream_url)
        is_spanish = column_title == _SPANISH_COLUMN_TITLE
        category = category_for_channel(name, column_title) or infer_category_from_name(name)
        language = "es" if is_spanish else infer_language_from_metadata(name)
        return ChannelData(
            source_channel_id = str(cid),
            name              = name,
            stream_url        = stream_url,
            logo_url          = show.get("frontPic") or show.get("appIcon") or None,
            category          = category,
            language          = language,
            country           = "US",
            stream_type       = "dash" if drm else "hls",
        )

    def fetch_channels(self) -> list[ChannelData]:
        columns = self._discover_columns()

        tile_ids_by_column: dict[int, list[int]] = {}
        column_title_by_channel_id: dict[int, str] = {}
        for col in columns:
            col_id = col.get("id")
            col_title = col.get("title") or ""
            if col_id is None:
                continue
            tiles = self._fetch_column_tiles(col_id)
            ids = [t["id"] for t in tiles]
            tile_ids_by_column[col_id] = ids
            for cid in ids:
                column_title_by_channel_id.setdefault(cid, col_title)
            logger.info("[vidaa] %s: %d channels", col_title, len(ids))

        all_ids = list(column_title_by_channel_id.keys())
        if not all_ids:
            raise RuntimeError("Vidaa: no live channels discovered across any category")

        now_ts = int(time.time())
        channels: list[ChannelData] = []
        for batch in _chunk(all_ids, _MEDIAS_BATCH_SIZE):
            detail = self._fetch_medias_info(batch, now_ts)
            for media in detail.get("medias") or []:
                cid = media.get("id")
                if cid is None:
                    continue
                channel = self._build_channel(media, column_title_by_channel_id.get(cid))
                if channel:
                    channels.append(channel)

        logger.info("[vidaa] %d channels fetched across %d categories", len(channels), len(columns))
        return channels

    # ── fetch_epg ────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        if not channels:
            return []

        ids = [int(ch.source_channel_id) for ch in channels]
        now = int(time.time())
        steps = (_EPG_HORIZON_DAYS * 24) // _EPG_STEP_HOURS
        related_dates = [now + step * _EPG_STEP_HOURS * 3600 for step in range(steps)]
        batches = list(_chunk(ids, _MEDIAS_BATCH_SIZE))
        total = len(batches) * len(related_dates)

        programs_by_key: dict[tuple[str, int], ProgramData] = {}
        seen_channel_ids: set[str] = set()
        done = 0
        consecutive_failures = 0
        circuit_open = False
        for related_date in related_dates:
            for batch in batches:
                done += 1
                if circuit_open:
                    continue
                try:
                    data = self._fetch_medias_info(batch, related_date)
                    consecutive_failures = 0
                except Exception as exc:
                    consecutive_failures += 1
                    logger.warning("[vidaa] EPG batch failed (relatedDate=%s, ids=%s): %s",
                                    related_date, batch, exc)
                    if consecutive_failures >= _EPG_CIRCUIT_THRESHOLD:
                        circuit_open = True
                        logger.error("[vidaa] %d consecutive EPG batch failures — "
                                     "aborting remaining requests", consecutive_failures)
                    if self._progress_cb:
                        self._progress_cb('epg', min(done, total), total)
                    continue

                for media in data.get("medias") or []:
                    cid = media.get("id")
                    if cid is None:
                        continue
                    seen_channel_ids.add(str(cid))
                    for sched in (media.get("channelInfo") or {}).get("scheduleList") or []:
                        title = (sched.get("scheduleName") or "").strip()
                        start_ms, end_ms = sched.get("startTime"), sched.get("endTime")
                        if not title or not start_ms or not end_ms:
                            continue
                        start_dt = datetime.fromtimestamp(start_ms / 1000, tz=UTC)
                        end_dt   = datetime.fromtimestamp(end_ms / 1000, tz=UTC)
                        if end_dt <= start_dt:
                            continue
                        key = (str(cid), sched.get("scheduleId") or start_ms)
                        programs_by_key[key] = ProgramData(
                            source_channel_id = str(cid),
                            title             = title,
                            start_time        = start_dt,
                            end_time          = end_dt,
                            description       = (sched.get("summary") or "").strip() or None,
                            poster_url        = sched.get("recommendPic"),
                        )
                if self._progress_cb:
                    self._progress_cb('epg', min(done, total), total)

        programs = list(programs_by_key.values())
        if not programs and (circuit_open or consecutive_failures):
            raise ScrapeSkipError("Vidaa EPG unavailable: all attempted batches failed")

        # Some tiles (confirmed on the DRM "FAST Channel <Show>" single-title loops)
        # publish no schedule at all — scheduleList comes back genuinely empty on
        # every relatedDate, not a fetch failure. Fill those with a placeholder
        # using the channel's own name so the guide isn't blank, but only for
        # channels we actually queried successfully with zero results — if Vidaa
        # starts publishing a real schedule for one later, this naturally stops
        # firing for it instead of needing a source list to maintain.
        covered_ids = {cid for cid, _ in programs_by_key}
        name_by_id = {ch.source_channel_id: ch.name for ch in channels}
        placeholder_channels = 0
        for source_channel_id in seen_channel_ids - covered_ids:
            name = name_by_id.get(source_channel_id)
            if not name:
                continue
            for step in range(_EPG_HORIZON_DAYS * 24):
                block_start = datetime.fromtimestamp(now + step * 3600, tz=UTC)
                block_end = block_start + timedelta(hours=1)
                programs_by_key[(source_channel_id, -1 - step)] = ProgramData(
                    source_channel_id = source_channel_id,
                    title             = name,
                    start_time        = block_start,
                    end_time          = block_end,
                    description       = "Live schedule not published for this channel by the source.",
                    poster_url        = None,
                )
            placeholder_channels += 1
        if placeholder_channels:
            logger.info("[vidaa] %d channel(s) had no published schedule — filled with placeholder blocks",
                        placeholder_channels)

        programs = list(programs_by_key.values())
        logger.info("[vidaa] %d EPG programs fetched across %d channels", len(programs), len(channels))
        return programs

    # ── DRM license bridge (Widevine CENC, DASH tiles only) ─────────────────────
    # The mini-player's own player config (captured from its JS bundle) builds two
    # things per play: the license server URL `{drm_url}?streamId={stream_id}&
    # reqFrom=v_tvch_m`, and a `playback-request` header. That header looks like an
    # entitlement ticket but isn't one — it's just base64(JSON) of self-asserted
    # client metadata, computed locally in the JS with no signature and no
    # server round-trip. Confirmed live: a plain L3 Widevine challenge posted with
    # this header returns a real license (content key matched the manifest's KID).
    # `stream_id` is a distinct per-tile asset id (not the Vidaa channel id) that
    # only appears inside the wrapper URL, so both methods below re-fetch it live
    # via mediasInfo rather than guessing — same reasoning as PBS's get_license_url.

    @classmethod
    def _lookup_drm_params(cls, config: dict, channel_id: str) -> dict | None:
        try:
            scraper = cls(config=config)
            detail = scraper._fetch_medias_info([int(channel_id)], int(time.time()))
        except Exception as exc:
            logger.warning("[vidaa] DRM param lookup failed for %s: %s", channel_id, exc)
            return None
        medias = detail.get("medias") or []
        if not medias:
            return None
        raw = (medias[0].get("channelInfo") or {}).get("streamingParam") or ""
        return _parse_mini_player(raw)

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        if not channel_id:
            return None
        params = cls._lookup_drm_params(config, channel_id)
        if not params:
            return None
        return f"{params['drm_url']}?streamId={params['stream_id']}&reqFrom=v_tvch_m"

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs
    ) -> tuple[bytes, dict]:
        params = cls._lookup_drm_params(config, channel_id) if channel_id else None
        stream_id = params['stream_id'] if params else (channel_id or "")
        playback_request = base64.b64encode(json.dumps({
            "assetId": stream_id,
            "clientMetadata": {
                "name": "vod",
                "version": "1.0.0",
                "os": {"name": "Linux", "version": "5.15.148"},
                "device": {"type": "vidaaOS"},
                "supportedDrms": [{"type": "widevine"}, {"type": "playready"}],
            },
        }, separators=(",", ":")).encode()).decode()
        return challenge, {"playback-request": playback_request}

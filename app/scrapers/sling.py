from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import secrets
import string
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, quote, urlparse

import requests

try:
    from .base import BaseScraper, ChannelData, ConfigField, ProgramData, StreamDeadError, ScrapeSkipError, infer_language_from_metadata
except ImportError:  # pragma: no cover - local staging outside FastChannels package
    from app.scrapers.base import BaseScraper, ChannelData, ConfigField, ProgramData, StreamDeadError, ScrapeSkipError, infer_language_from_metadata

logger = logging.getLogger(__name__)


def _join_categories(values: list[str] | tuple[str, ...] | None) -> str | None:
    if not values:
        return None
    unique = list(dict.fromkeys(v.strip() for v in values if v and v.strip()))
    return ';'.join(unique) or None


class SlingScraper(BaseScraper):
    """
    Best-effort FastChannels scraper for Sling Freestream.

    Current status:
    - Channel inventory: working against Sling's player ribbon API.
    - EPG: working from per-channel schedule.qvt windows.
    - Playback: resolve() returns the current DASH MPD URL when available.
    - DRM: expected for many channels; this scraper does not acquire licenses.

    Important limitation:
    - Playback remains DRM-protected for many channels. This scraper only
      captures metadata/EPG and the current manifest URL.
    """

    source_name = "sling"
    display_name = "Sling Freestream"
    scrape_interval = 360
    stream_audit_enabled = True
    channel_refresh_hours = 12

    CMW_FAST = "https://p-cmwnext-fast.movetv.com"
    CMS = "https://cbd46b77.cdn.cms.movetv.com"

    DEFAULT_FOCUS_CHANNEL_ID = "21ec280634b247cfa0688744fb7a7e8a"

    config_schema = [
        ConfigField(
            "bearer_jwt",
            "Bearer JWT",
            field_type="password",
            secret=True,
            help_text="Optional. Paste a fresh Sling Bearer token to skip OAuth bootstrap.",
        ),
        ConfigField(
            "consumer_key",
            "OAuth Consumer Key",
            field_type="text",
            required=False,
            help_text="Needed if bearer_jwt is not supplied.",
        ),
        ConfigField(
            "consumer_secret",
            "OAuth Consumer Secret",
            field_type="password",
            required=False,
            secret=True,
            help_text="Needed if bearer_jwt is not supplied.",
        ),
        ConfigField(
            "access_token",
            "OAuth Access Token",
            field_type="password",
            required=False,
            secret=True,
            help_text="Sling browser accessToken from the session bootstrap response.",
        ),
        ConfigField(
            "access_secret",
            "OAuth Access Secret",
            field_type="password",
            required=False,
            secret=True,
            help_text="Sling browser accessSecret from the session bootstrap response.",
        ),
        ConfigField(
            "device_guid",
            "Device GUID",
            field_type="text",
            required=False,
            help_text="HardwareDeviceGUID from Sling browser localStorage.",
        ),
        ConfigField(
            "profile_guid",
            "Profile GUID",
            field_type="text",
            required=False,
            help_text="Profile/user GUID used in Sling's /cmw/v1/client/jwt request.",
        ),
        ConfigField(
            "profile_type",
            "Profile Type",
            field_type="text",
            required=False,
            default="Admin",
        ),
        ConfigField(
            "account_status",
            "Account Status",
            field_type="text",
            required=False,
            default="prospect",
        ),
        ConfigField(
            "client_version",
            "Client Version",
            field_type="text",
            required=False,
            default="7.1.32",
        ),
        ConfigField(
            "player_version",
            "Player Version",
            field_type="text",
            required=False,
            default="9.1.0",
        ),
        ConfigField(
            "device_model",
            "Device Model",
            field_type="text",
            required=False,
            default="Chrome",
        ),
        ConfigField(
            "client_config",
            "Client Config",
            field_type="text",
            required=False,
            default="rn-client-config",
        ),
        ConfigField(
            "response_config",
            "Response Config",
            field_type="text",
            required=False,
            default="ar_browser_1_1",
        ),
        ConfigField(
            "dma",
            "DMA",
            field_type="text",
            required=False,
            default="535",
        ),
        ConfigField(
            "geo_zipcode",
            "Geo Zipcode",
            field_type="text",
            required=False,
            default="43017",
        ),
        ConfigField(
            "time_zone_id",
            "Time Zone ID",
            field_type="text",
            required=False,
            default="America/New_York",
        ),
        ConfigField(
            "timezone_offset",
            "Timezone Offset",
            field_type="text",
            required=False,
            default="-0500",
        ),
        ConfigField(
            "focus_channel_id",
            "Focus Channel ID",
            field_type="text",
            required=False,
            default=DEFAULT_FOCUS_CHANNEL_ID,
            help_text="Seed channel GUID for the paginated player_all_channels ribbon.",
        ),
        ConfigField(
            "max_channel_pages",
            "Max Channel Pages",
            field_type="number",
            required=False,
            default=100,
        ),
        ConfigField(
            "epg_windows_per_channel",
            "EPG Windows Per Channel",
            field_type="number",
            required=False,
            default=4,
        ),
        ConfigField(
            "epg_channel_limit",
            "EPG Channel Limit",
            field_type="number",
            required=False,
            default=0,
            help_text="0 means all channels; useful for testing.",
        ),
        ConfigField(
            "epg_workers",
            "EPG Parallel Workers",
            field_type="number",
            required=False,
            default=20,
            help_text="Number of parallel threads for EPG fetching. Higher = faster but more load.",
        ),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update(
            {
                "accept": "application/json, text/plain, */*",
                "origin": "https://watch.sling.com",
                "referer": "https://watch.sling.com/",
                "client-config": self._cfg("client_config", "rn-client-config"),
                "client-version": self._cfg("client_version", "7.1.32"),
                "device-model": self._cfg("device_model", "Chrome"),
                "player-version": self._cfg("player_version", "9.1.0"),
                "response-config": self._cfg("response_config", "ar_browser_1_1"),
                "dma": self._cfg("dma", "535"),
                "geo-zipcode": self._cfg("geo_zipcode", "43017"),
                "time-zone-id": self._cfg("time_zone_id", "America/New_York"),
                "timezone": self._cfg("timezone_offset", "-0500"),
                "features": "enable_ad_tracking,web_browser",
            }
        )
        self._bearer_jwt = (self.config.get("bearer_jwt") or "").strip()

    def pre_run_setup(self) -> None:
        try:
            self._ensure_bearer()
        except RuntimeError as exc:
            raise ScrapeSkipError(
                "Sling auth is not available yet. Configure bearer_jwt or OAuth creds if you want Sling EPG data."
            ) from exc

    def fetch_channels(self) -> list[ChannelData]:
        self._ensure_bearer()

        focus_channel_id = self._cfg("focus_channel_id", self.DEFAULT_FOCUS_CHANNEL_ID)
        max_pages = int(self._cfg("max_channel_pages", 100))
        start_url = (
            f"{self.CMW_FAST}/pres/player_screen/player_all_channels"
            f"?focus_channel_id={quote(focus_channel_id)}"
        )

        queue = [start_url]
        seen_urls: set[str] = set()
        channels: dict[str, ChannelData] = {}

        while queue and len(seen_urls) < max_pages:
            url = queue.pop(0)
            if url in seen_urls:
                continue
            seen_urls.add(url)

            payload = self._get_json(url)
            for link_key in ("href", "next", "prev"):
                link = payload.get(link_key)
                if isinstance(link, str) and "player_all_channels" in link and link not in seen_urls:
                    queue.append(link)

            for tile in payload.get("tiles", []):
                channel = self._channel_from_tile(tile)
                if channel is not None:
                    channels[channel.source_channel_id] = channel

        result = sorted(channels.values(), key=lambda c: (c.name or "", c.source_channel_id))
        logger.info("[%s] %d channels", self.source_name, len(result))
        return result

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        self._ensure_bearer()

        limit       = int(self._cfg("epg_channel_limit", 0))
        max_windows = int(self._cfg("epg_windows_per_channel", 4))
        max_workers = int(self._cfg("epg_workers", 20))
        selected    = channels if limit <= 0 else channels[:limit]
        total       = len(selected)

        # Snapshot the current headers (including auth) once; each worker thread
        # reuses its own Session to avoid shared mutation and repeated fresh
        # connection pools for every channel task.
        headers_snapshot = dict(self.session.headers)

        programs: list[ProgramData] = []
        lock     = threading.Lock()
        thread_local = threading.local()
        done     = [0]   # mutable counter accessible from threads

        def fetch_one(channel_id: str) -> None:
            sess = getattr(thread_local, "session", None)
            if sess is None:
                sess = self.new_session(headers=headers_snapshot)
                thread_local.session = sess
            try:
                result = self._fetch_epg_for_channel_with_session(channel_id, max_windows, sess)
            except Exception as exc:  # noqa: BLE001
                resp = getattr(exc, 'response', None)
                if resp is not None and resp.status_code == 404:
                    logger.debug("[%s] no EPG for %s", self.source_name, channel_id)
                else:
                    logger.warning("[%s] EPG fetch failed for %s: %s", self.source_name, channel_id, exc)
                result = []
            with lock:
                programs.extend(result)
                done[0] += 1
                if self._progress_cb:
                    self._progress_cb('epg', done[0], total)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_one, ch.source_channel_id) for ch in selected}
            for future in as_completed(futures):
                exc = future.exception()
                if exc and type(exc).__name__ == 'JobTimeoutException':
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise exc

        programs.sort(key=lambda p: (p.source_channel_id, p.start_time, p.title))
        logger.info("[%s] %d EPG entries", self.source_name, len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("sling://"):
            return raw_url

        self._ensure_bearer()
        channel_guid = raw_url.split("sling://", 1)[1].strip()
        if not channel_guid:
            return raw_url

        try:
            payload = self._get_json(self._channel_schedule_url(channel_guid))
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                raise StreamDeadError(f"[sling] channel not found (404): {channel_guid}") from exc
            raise

        playback = payload.get("playback_info") or {}
        for key in ("dash_manifest_url", "live_m3u8_url_template", "m3u8_url_template"):
            url = (playback.get(key) or "").strip()
            if url and "{" not in url and url.startswith("http"):
                return url
        raise RuntimeError(f"No playable URL found for sling channel {channel_guid}")

    def _cfg(self, key: str, default: Any = None) -> Any:
        value = self.config.get(key)
        return default if value in (None, "") else value

    def _ensure_bearer(self) -> None:
        if self._bearer_jwt:
            self.session.headers["authorization"] = f"Bearer {self._bearer_jwt}"
            return

        try:
            self._bearer_jwt = self._mint_bearer_jwt()
        except RuntimeError:
            logger.info("[%s] OAuth creds not configured, falling back to browser bootstrap", self.source_name)
            self._bearer_jwt = self._browser_bootstrap()

        self._update_config("bearer_jwt", self._bearer_jwt)
        self.session.headers["authorization"] = f"Bearer {self._bearer_jwt}"

    def _mint_bearer_jwt(self) -> str:
        required = {
            "consumer_key": self._cfg("consumer_key"),
            "consumer_secret": self._cfg("consumer_secret"),
            "access_token": self._cfg("access_token"),
            "access_secret": self._cfg("access_secret"),
            "device_guid": self._cfg("device_guid"),
            "profile_guid": self._cfg("profile_guid"),
        }
        missing = [key for key, value in required.items() if not value]
        if missing:
            raise RuntimeError(
                "Sling bootstrap requires config for: "
                + ", ".join(sorted(missing))
                + ". Alternatively provide bearer_jwt directly."
            )

        url = f"{self.CMW_FAST}/cmw/v1/client/jwt"
        payload = {
            "device_guid": required["device_guid"],
            "platform": "browser",
            "product": "sling",
            "profile_guid": required["profile_guid"],
            "profile_type": self._cfg("profile_type", "Admin"),
            "account_status": self._cfg("account_status", "prospect"),
        }

        headers = {
            "content-type": "application/json; charset=UTF-8",
            "authorization": self._oauth1_header(
                method="POST",
                url=url,
                consumer_key=required["consumer_key"],
                consumer_secret=required["consumer_secret"],
                token=required["access_token"],
                token_secret=required["access_secret"],
            ),
        }
        resp = self.session.post(url, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        token = data.get("jwt")
        if not token:
            raise RuntimeError(f"Sling /client/jwt response missing jwt: {data}")
        return token

    def _browser_bootstrap(self) -> str:
        """Launch a headless Chromium browser, load watch.sling.com, and capture
        the Bearer JWT from outbound requests to movetv.com.  Requires the
        playwright package and Chromium to be installed."""
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            raise RuntimeError(
                "playwright is not installed. "
                "Run: pip install playwright && playwright install chromium"
            )

        captured: list[str] = []
        bootstrap_data: dict[str, str] = {}

        def on_request(request):
            if captured:
                return
            auth = request.headers.get("authorization", "")
            if auth.startswith("Bearer ") and "movetv.com" in request.url:
                captured.append(auth[7:])

        def on_response(response):
            url = response.url
            try:
                if "/cmw/v1/client/jwt" in url:
                    data = response.json()
                    token = (data.get("jwt") or "").strip()
                    if token and not captured:
                        captured.append(token)
                elif "/user/prospect" in url:
                    data = response.json()
                    # Persist bootstrap values when Sling exposes them so future
                    # runs can mint a bearer without Playwright.
                    mapping = {
                        "accessToken": "access_token",
                        "accessSecret": "access_secret",
                        "consumerKey": "consumer_key",
                        "consumerSecret": "consumer_secret",
                        "profileGuid": "profile_guid",
                        "userGuid": "profile_guid",
                        "accountStatus": "account_status",
                        "profileType": "profile_type",
                    }
                    for source_key, dest_key in mapping.items():
                        value = (data.get(source_key) or "").strip() if isinstance(data.get(source_key), str) else data.get(source_key)
                        if value:
                            bootstrap_data[dest_key] = value
            except Exception:
                # Token capture is best-effort; ignore parsing issues from
                # unrelated responses and keep waiting for a direct auth header.
                return

        logger.info("[%s] Starting browser bootstrap to capture Bearer JWT", self.source_name)
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                ctx = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    )
                )
                page = ctx.new_page()
                page.on("request", on_request)
                page.on("response", on_response)
                try:
                    page.goto("https://watch.sling.com/", wait_until="domcontentloaded", timeout=60_000)
                except PWTimeout:
                    pass

                # The Sling SPA keeps loading data after initial HTML arrives.
                # Wait explicitly for the auth/bootstrap requests that yield the
                # anonymous Freestream bearer token.
                for _ in range(30):
                    if captured:
                        break
                    page.wait_for_timeout(1000)

                try:
                    device_guid = page.evaluate("window.localStorage.getItem('HardwareDeviceGUID') || ''")
                    if device_guid:
                        bootstrap_data["device_guid"] = device_guid.strip()
                except Exception:
                    pass
            finally:
                browser.close()

        if not captured:
            raise RuntimeError(
                "Browser bootstrap failed: no Bearer JWT captured from watch.sling.com. "
                "The page may have changed — try providing bearer_jwt manually."
            )

        for key, value in bootstrap_data.items():
            self._update_config(key, value)

        logger.info("[%s] Browser bootstrap succeeded", self.source_name)
        return captured[0]

    def _oauth1_header(
        self,
        *,
        method: str,
        url: str,
        consumer_key: str,
        consumer_secret: str,
        token: str | None = None,
        token_secret: str | None = None,
    ) -> str:
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

        oauth_params = {
            "oauth_consumer_key": consumer_key,
            "oauth_nonce": self._oauth_nonce(),
            "oauth_signature_method": "HMAC-SHA1",
            "oauth_timestamp": str(int(datetime.now(timezone.utc).timestamp())),
            "oauth_version": "1.0",
        }
        if token:
            oauth_params["oauth_token"] = token

        all_params = []
        all_params.extend(parse_qsl(parsed.query, keep_blank_values=True))
        all_params.extend(oauth_params.items())
        normalized = "&".join(
            f"{quote(str(k), safe='~')}={quote(str(v), safe='~')}"
            for k, v in sorted((str(k), str(v)) for k, v in all_params)
        )

        base_string = "&".join(
            [
                method.upper(),
                quote(base_url, safe="~"),
                quote(normalized, safe="~"),
            ]
        )
        signing_key = "&".join(
            [
                quote(consumer_secret, safe="~"),
                quote(token_secret or "", safe="~"),
            ]
        )
        digest = hmac.new(
            signing_key.encode("utf-8"),
            base_string.encode("utf-8"),
            hashlib.sha1,
        ).digest()
        oauth_params["oauth_signature"] = base64.b64encode(digest).decode("ascii")

        return "OAuth " + ",".join(
            f'{quote(k, safe="~")}="{quote(str(v), safe="~")}"'
            for k, v in sorted(oauth_params.items())
        )

    def _oauth_nonce(self, length: int = 32) -> str:
        alphabet = string.ascii_letters + string.digits
        return "".join(secrets.choice(alphabet) for _ in range(length))

    def _get_json(self, url: str) -> dict[str, Any]:
        resp = self.session.get(url, timeout=30)
        if resp.status_code == 401 and self._bearer_jwt:
            logger.info("[%s] bearer token expired; attempting one refresh", self.source_name)
            self._bearer_jwt = ""
            self._ensure_bearer()
            resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _channel_schedule_url(self, channel_guid: str) -> str:
        return f"{self.CMS}/playermetadata/sling/v1/api/channels/{channel_guid}/current/schedule.qvt"

    def _fetch_epg_for_channel(self, channel_guid: str, max_windows: int) -> list[ProgramData]:
        return self._fetch_epg_for_channel_with_session(channel_guid, max_windows, self.session)

    def _fetch_epg_for_channel_with_session(self, channel_guid: str, max_windows: int, session) -> list[ProgramData]:
        url = self._channel_schedule_url(channel_guid)
        seen_urls: set[str] = set()
        programs: dict[tuple[str, str], ProgramData] = {}

        for _ in range(max_windows):
            if not url or url in seen_urls:
                break
            seen_urls.add(url)

            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
            playback = payload.get("playback_info") or {}
            asset = playback.get("asset") or {}
            program = self._program_from_asset(channel_guid, asset, payload)
            if program is not None:
                key = (program.source_channel_id, program.start_time.isoformat())
                programs[key] = program
            url = payload.get("_next")

        return sorted(programs.values(), key=lambda p: p.start_time)

    def _program_from_asset(
        self,
        channel_guid: str,
        asset: dict[str, Any],
        payload: dict[str, Any],
    ) -> ProgramData | None:
        start = self._parse_dt(asset.get("schedule_start"))
        end = self._parse_dt(asset.get("schedule_end"))
        title = asset.get("title") or asset.get("franchise_title")
        if not start or not end or not title:
            return None

        thumbnail = None
        shows = payload.get("shows") or []
        if shows:
            thumbnail = ((shows[0].get("thumbnail") or {}).get("url"))

        genre = asset.get("genre") or asset.get("channel_genre")
        if isinstance(genre, list):
            genre = _join_categories(genre)

        rating = asset.get("rating")
        if isinstance(rating, list):
            rating = ", ".join(x for x in rating if x)

        return ProgramData(
            source_channel_id=channel_guid,
            title=title,
            start_time=start,
            end_time=end,
            description=None,
            poster_url=thumbnail,
            category=genre,
            rating=rating,
            episode_title=asset.get("episode_title"),
            season=self._to_int(asset.get("season_number")),
            episode=self._to_int(asset.get("episode_number")),
        )

    def _channel_from_tile(self, tile: dict[str, Any]) -> ChannelData | None:
        actions = tile.get("actions") or {}
        play_action = actions.get("PLAY_CONTENT") or {}
        detail_action = actions.get("DETAIL_VIEW") or {}
        playback = play_action.get("playback_info") or {}
        channel_guid = playback.get("channel_guid") or ((tile.get("analytics") or {}).get("item_id"))
        if not channel_guid:
            return None

        name = self._best_channel_name(tile, playback, play_action, detail_action)
        if not name:
            return None

        logo_url = ((playback.get("channel_logo") or {}).get("url"))
        category = self._infer_group(tile, name)

        return ChannelData(
            source_channel_id=channel_guid,
            name=name,
            stream_url=f"sling://{channel_guid}",
            logo_url=logo_url,
            slug=self._slugify(name),
            category=category,
            language=infer_language_from_metadata(name, category),
            country="US",
            stream_type="dash",
        )

    def _best_channel_name(
        self,
        tile: dict[str, Any],
        playback: dict[str, Any],
        play_action: dict[str, Any],
        detail_action: dict[str, Any],
    ) -> str | None:
        for candidate in (
            ((play_action.get("adobe") or {}).get("ChannelName")),
            ((detail_action.get("adobe") or {}).get("ChannelName")),
        ):
            if candidate:
                return candidate.strip()
        for attr in tile.get("attributes", []):
            value = (attr.get("str_value") or "").strip()
            if value and value != (tile.get("title") or "").strip():
                return value
        call_sign = (playback.get("call_sign") or "").strip()
        return call_sign or None

    @staticmethod
    def _category_from_name(name: str) -> str:
        n = name.lower()
        if any(x in n for x in ("news", "noticias", "cheddar", "scripps", "newsmax", "bloomberg", "euronews", "france 24", "al jazeera", "al arabiya", "al hadath", "al araby")):
            return "News"
        if any(x in n for x in ("sport", " nba ", "nhl network", "pickleball", "espn", "nbc sports", "draftkings", "fanduel", "bowling", "billiard", "poker", "surf league", "baseball tv", "fubo sport", "beinsport", "bein sport", "sportsnet", "sportsgrid", "willow sport")):
            return "Sports"
        if any(x in n for x in ("vevo", " music", "mtv ", "concerts by stingray", "country network", "b4u music", "yrf music", "raj mus", "saga music", "aghani")):
            return "Music"
        if any(x in n for x in ("kids", "junior", "jr.", "cartoon", "barney", "dinos 24", "my little pony", "strawberry shortcake", "power rangers", "kartoon", "pocket.watch", "baby")):
            return "Kids"
        if any(x in n for x in ("food", "cook", "kitchen", "tastemade", "bon appetit", "emeril", "chef", "gusto", "jamie oliver", "martha stewart", "america's test", "foodxp")):
            return "Food"
        if any(x in n for x in ("crime", "murder", "killer", "forensic files", "court tv", "chasing criminal", "unsolved", "bounty hunter", "cold case", "true crime")):
            return "True Crime"
        if any(x in n for x in ("documentary", "curiosity stream", "pbs ", "nhk", "earth touch", "love nature", "wildearth", "wild ocean", "terra mater", "history & warfare", "combat war", "magellan", "science is")):
            return "Documentary"
        if any(x in n for x in ("qvc", "hsn", "shopping", "deal zone")):
            return "Shopping"
        if any(x in n for x in ("anime", "hidive", "crunchyroll", "retrocrush")):
            return "Anime"
        if any(x in n for x in ("tbn", "osteen", "quran", "bhajan", "dharm", "faith & family", "christian", "padre pio", "noursat", "aastha")):
            return "Religion"
        if any(x in n for x in ("travel", "voyage", "travelxp", "go traveler", "places & spaces", "tv5monde voyage")):
            return "Travel"
        if any(x in n for x in ("comedy", "funny", "laugh", "gags", "always funny", "comedy central")):
            return "Comedy"
        if any(x in n for x in ("outdoor", "meateater", "waypoint tv", "wired2fish", "xtreme outdoor", "outside tv")):
            return "Outdoors"
        return "Entertainment"

    def _infer_group(self, tile: dict[str, Any], name: str = "") -> str | None:
        # Name-based channel category inference
        if name:
            return self._category_from_name(name)
        return None

    def _slugify(self, value: str) -> str:
        cleaned = []
        last_dash = False
        for char in value.lower():
            if char.isalnum():
                cleaned.append(char)
                last_dash = False
            elif not last_dash:
                cleaned.append("-")
                last_dash = True
        return "".join(cleaned).strip("-") or "sling"

    def _parse_dt(self, value: str | None) -> datetime | None:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _to_int(self, value: Any) -> int | None:
        try:
            if value in (None, ""):
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

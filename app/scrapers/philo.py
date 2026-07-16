# app/scrapers/philo.py
#
# Philo — premium live TV (DRM bridge)
#
# Philo delivers CENC DASH + Widevine (castLabs DRMtoday).  The channels can't
# play on a normal IPTV client, but they decrypt on a software (L3) CDM, so they
# ride the existing /watch → PrismCast bridge exactly like Roku/Amazon DRM.
#
#   stream_url stored as:  philo://<channelId>
#
# Play-time resolve (per tune):
#   1. playbackSessionPresentation({id: channelId, broadcastAt: now})
#        → current Broadcast id
#   2. createPlaybackSessionV2({id: broadcastId, playerId})
#        → dashURL (a real DASH MPD) + drmProvider.authToken (x-dt-auth-token)
#   3. The MPD is fetched/proxied by /play/philo/<id>/dash.mpd (CORS is philo-only);
#      segments are on prod.cdn-*.philo.com with CORS '*' so Shaka fetches them direct.
#   4. The Widevine license is relayed by /play/philo/license to DRMtoday with the
#      per-session authToken; the response is JSON {"status":"OK","license":<b64>}
#      which process_license_response() unwraps to the raw license bytes.
#
# Auth is a cookie session (login sets cookies; GraphQL uses Apollo persisted-query
# hashes, no bearer).  The session cookies + playerId are persisted in Source.config.

from __future__ import annotations

import base64
import json
import logging
import time
from datetime import datetime, timedelta, timezone

from .base import (BaseScraper, ChannelData, ProgramData, ConfigField,
                   ScrapeSkipError, StreamDeadError)
from .category_utils import category_for_channel

logger = logging.getLogger(__name__)

_GQL = "https://www.philo.com/graphql"
_LICENSE = "https://lic.drmtoday.com/license-proxy-widevine/cenc/"
_ORIGIN = "https://www.philo.com"
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36")

# Passwordless sign-in endpoints (confirmed live). Philo emails/SMSs a 6-digit
# code; the resulting device session is valid ~1 year.
_AUTH_INFO = f"{_ORIGIN}/auth/info/login_code"       # → {init, action, poll}
_LOGIN_BOOTSTRAP = f"{_ORIGIN}/login/authenticate"   # sets anon session cookies

# The session cookies that matter for an authenticated Philo session.
_AUTH_COOKIE_NAMES = ("_session_id", "hashed_session_id", "ott_customer", "ajs_user_id")

# Apollo persisted-query sha256 hashes (from the web client).  If Philo rotates
# these, the server replies PersistedQueryNotFound and we surface a clear skip.
_HASH = {
    "sessionStatus":               "6b3d0f3dbb1ef4870d38442103b1cc73a11ba3915fd93457503d6494ca6ebe34",
    "userSubscription":            "990a8b451f87cf3cca1ce78ae3895ebc12c87f5082f48d61249ab086e2b68c10",
    "page":                        "5a3975c00b8c834d29af0bd311f7c7c5b7983bc92cedf0ba8e08598c6dac3c01",
    "playbackSessionPresentation": "a19362d932245454f6ff4dfde89066c47e18f17574b1a92e5ab279cdf1e74bf1",
    "createPlaybackSessionV2":     "92f9d45fe8ed36e660bd6f9365dca1ddcef1b5e3b0d651c3988ebd44a0305e34",
    "registerPlayerV2":            "8312f5c234270aa2de0f199e79667ca23e41ac6e23d8f4bc31d3007702fbe9a9",
}

# Guide page() variable template — only endCursor/firstGroups/initialTiles change.
_GUIDE_VARS = {
    "pageType": "GUIDE", "typeId": None, "filterId": None, "filter": None,
    "sorterId": None, "endCursor": None, "startCursor": None,
    "firstGroups": 60, "initialTiles": 1, "lastGroups": None,
    "numSparseGroups": 400, "includeTileDescription": False,
    "includeTileChannel": True, "iconFormat": "SVG",
    "capabilities": ["COLLECTION_TILE_GROUPS", "HERO_PROMOTION", "MOVIE_SHOWINGS",
                     "GUIDE_FILTERS", "SEARCH_PAGE_RECS",
                     "UNIFIED_SHOWS_MOVIES_SEARCH_RESULTS", "EXTERNAL_CONTENT",
                     "COLLECTION_GROUPS", "OUT_OF_PLAN_CONTENT",
                     "CHANNEL_TILE_GROUPS_V2"],
    "startTime": None, "endTime": None,
}

# Philo is passwordless: a device session is authenticated once (email/SMS code)
# and its cookies (_session_id / hashed_session_id / ott_customer) are valid for a
# YEAR and re-extended by the server on activity. So we don't re-auth on a schedule
# — we trust the seeded/persisted cookies until the server actually rejects them
# (401/403), then surface a re-bootstrap prompt. TTL here is only a soft "very old
# session, log a warning" bound; it does not force a re-login (there's no password
# to log in with).
_SESSION_TTL = 300 * 24 * 60 * 60   # ~10 months
_DASH_TTL = 4 * 60             # per-session dashURL/token reuse window (seconds)
_GUIDE_MAX_PAGES = 12
# Airings per channel to pull for the guide. 48 half-hour slots ≈ 24–34h of EPG;
# the grid supports more (hasNextPage) but this is a sensible per-scrape window.
_GUIDE_EPG_TILES = 48

# Descriptions: the guide grid ships no synopses, so we backfill them from the
# per-broadcast PRESENTATION page (data.page.tile.longDescription), batched. Only
# programs starting within this near-term window are fetched each scrape — enough
# to cover a typical scrape interval — and results are cached (keyed by episode/
# movie id) so later scrapes only fetch what's newly entered the window.
_DESC_WINDOW_HOURS = 6
_DESC_BATCH = 8   # Philo's /graphql rejects request bodies over ~3.5KB (HTTP 413)
_DESC_CACHE_TTL = 14 * 24 * 60 * 60   # keep synopses 14 days (content is static)
_DESC_CACHE_MAX = 8000


class PhiloScraper(BaseScraper):

    source_name          = "philo"
    display_name         = "Philo"
    scrape_interval      = 60
    is_premium           = True
    source_category      = "premium"
    config_required      = True
    stream_audit_enabled = True
    # Multi-hour forward schedule with episode/ratings, but Philo's grid ships no
    # synopses, so not "full" (which implies descriptions) — see fetch_epg.
    epg_quality          = "basic"
    # Presence of license_url marks the source DRM-capable → enables the
    # /play/philo/license proxy and the PrismCast bridge model.
    license_url          = _LICENSE
    # The EPG-only description cache is loaded lazily in fetch_epg, never on the
    # play/resolve hot path (which only touches the small dash_cache).
    LAZY_CACHE_KEYS      = frozenset({"description_cache"})

    # Philo has NO password — sign-in is the in-app email/SMS one-time code flow
    # (see request_login_code / verify_login_code + the "Sign in to Philo" panel
    # on the source). That mints a ~1-year device session stored in
    # config.session_cookies; there are no credentials to type. The email field
    # is just an informational label for which account this source is bound to.
    config_schema = [
        ConfigField("email", "Account email", placeholder="you@example.com",
                    help_text="The Philo account this source signs in to "
                              "(passwordless — you'll get a code by email/text)."),
    ]

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update({
            "User-Agent": _UA,
            "Origin": _ORIGIN,
            "Referer": f"{_ORIGIN}/",
            "Content-Type": "application/json",
            "apollographql-client-name": "web",
            "Accept": "*/*",
        })
        self._player_id: str | None = self.config.get("player_id")
        self._session_born: float | None = None
        self._load_cached_session()
        self._dash_cache: dict[str, dict] = {}
        self._load_dash_cache()
        # {desc_key: {"d": text, "t": epoch}} — loaded lazily at fetch_epg start.
        self._desc_cache: dict[str, dict] = {}

    # ── Session / auth ──────────────────────────────────────────────────────

    def _load_cached_session(self) -> None:
        cookies = self.config.get("session_cookies") or {}
        # The admin field is free text, so accept a pasted JSON string too.
        if isinstance(cookies, str):
            try:
                cookies = json.loads(cookies)
            except Exception:
                cookies = {}
        born = self.config.get("session_born")
        if isinstance(cookies, dict) and cookies:
            self.session.cookies.update(cookies)
        if isinstance(born, (int, float)):
            self._session_born = float(born)

    def _persist_session(self) -> None:
        self._update_config("session_cookies", self.session.cookies.get_dict())
        self._update_config("session_born", self._session_born or time.time())
        if self._player_id:
            self._update_config("player_id", self._player_id)

    def _session_is_fresh(self) -> bool:
        if not self.session.cookies:
            return False
        if not self._session_born:
            return True   # externally-seeded cookies with no timestamp — trust once
        return (time.time() - self._session_born) < _SESSION_TTL

    def _ensure_session(self) -> bool:
        """Philo is passwordless — there is no credential login to fall back on.
        Either we have valid session cookies (from the in-app sign-in flow, then
        auto-refreshed on use) or we can't proceed and the user must sign in again."""
        if self.session.cookies:
            if self._session_born and (time.time() - self._session_born) >= _SESSION_TTL:
                logger.warning("[philo] session cookies are very old (>%d days); "
                               "sign in again if calls start failing.",
                               _SESSION_TTL // 86400)
            return True
        raise ScrapeSkipError(
            "[philo] no session — Philo is passwordless; use the source's "
            "\"Sign in to Philo\" flow (email → code) to authenticate.")

    # ── Passwordless sign-in (in-app, two-step: send code → verify code) ────
    #
    # Philo has no password. The user enters their email/phone; Philo sends a
    # 6-digit code; the user enters it and we get a ~1-year device session.
    # These are staticmethods so the admin route can drive sign-in before the
    # source is fully configured.  request_login_code() returns a small opaque
    # context (anon cookies + device_ident) that must be handed back to
    # verify_login_code() to complete the exchange.

    @staticmethod
    def _auth_session() -> "requests.Session":
        import requests as _r
        s = _r.Session()
        s.headers.update({
            "User-Agent": _UA, "Origin": _ORIGIN,
            "Referer": _LOGIN_BOOTSTRAP, "Content-Type": "application/json",
            "Accept": "application/json",
        })
        return s

    @staticmethod
    def _cookie_jar_dict(session) -> dict:
        """Flatten a cookie jar to {name: value}, preferring www.philo.com-domain
        cookies (the jar can carry duplicate names across domains)."""
        out = {}
        for c in session.cookies:
            if c.name in out and "philo.com" not in (c.domain or ""):
                continue
            out[c.name] = c.value
        return out

    @classmethod
    def request_login_code(cls, ident: str, *, voice: bool = False) -> dict:
        """Step 1 — trigger Philo to send a sign-in code to `ident` (email or phone).

        Returns an opaque context {ident, device_ident, cookies} to pass to
        verify_login_code().  Raises ValueError with a user-facing message on failure.
        """
        import uuid
        ident = (ident or "").strip()
        if not ident:
            raise ValueError("Enter an email address or mobile number.")
        s = cls._auth_session()
        try:
            s.get(_LOGIN_BOOTSTRAP, timeout=20)
            info = s.get(_AUTH_INFO, timeout=15).json()
            init_url = info.get("init")
            if not init_url:
                raise ValueError("Philo sign-in is unavailable right now (no init URL).")
            device_ident = "web-" + str(uuid.uuid4())
            payload = {
                "device_ident": device_ident, "device": "web", "ident": ident,
                "include_login_link": False, "send_confirm_link": False,
                "send_token": True, "location": None,
            }
            if voice:
                payload["resend"] = "true"
                payload["voice"] = True
            r = s.post(init_url, data=json.dumps(payload), timeout=20)
            data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Could not reach Philo sign-in: {e}")
        if data.get("status") != "SUCCESS":
            msg = data.get("description") or f"Philo did not send a code (status={data.get('status')})."
            raise ValueError(msg)
        return {
            "ident": ident,
            "device_ident": device_ident,
            "cookies": cls._cookie_jar_dict(s),
            "can_resend": bool(data.get("can_resend", True)),
        }

    @classmethod
    def verify_login_code(cls, ctx: dict, code: str) -> dict:
        """Step 2 — submit the 6-digit code from request_login_code()'s context.

        On success returns {session_cookies, player_id, user_id} to persist into
        the source config.  Raises ValueError with a user-facing message otherwise.
        """
        code = (code or "").strip().replace(" ", "")
        if not code:
            raise ValueError("Enter the code Philo sent you.")
        if not (ctx and ctx.get("cookies")):
            raise ValueError("Sign-in session expired — request a new code.")
        s = cls._auth_session()
        s.cookies.update(ctx["cookies"])
        try:
            info = s.get(_AUTH_INFO, timeout=15).json()
            action_url = info.get("action")
            if not action_url:
                raise ValueError("Philo sign-in is unavailable right now (no action URL).")
            r = s.post(action_url, data=json.dumps({"token": code}), timeout=20)
            data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Could not verify the code: {e}")
        status = data.get("status")
        if status != "SUCCESS":
            if status in ("FAIL", "POLL") or data.get("error_code"):
                raise ValueError(data.get("description") or "That code was incorrect or expired.")
            raise ValueError(f"Unexpected sign-in response (status={status}).")
        cookies = cls._cookie_jar_dict(s)
        return {
            "session_cookies": cookies,
            "user_id": (data.get("analytics") or {}).get("userId"),
        }

    def _ensure_player(self) -> str | None:
        """Return a Philo playerId, registering one via registerPlayerV2 if needed.

        The player is keyed by a per-source device UUID (persisted in config), so
        re-registering returns the same player rather than spawning duplicates."""
        import uuid
        if self._player_id:
            return self._player_id
        device_ident = self.config.get("player_device_ident")
        if not device_ident:
            device_ident = str(uuid.uuid4())
            self._update_config("player_device_ident", device_ident)
        variables = {
            "captionsEnabled": False, "deviceIcon": "PHONE",
            "deviceName": "FastChannels", "deviceType": "WEB",
            "deviceModel": _UA, "deviceManufacturer": "",
            "deviceIdent": device_ident, "volume": 0.75,
            "applicationVersion": "2026.7.10-1356118", "osVersion": "150.0.0",
            "properties": [{"name": "supportsChunkLoading", "value": "true"}],
        }
        parts = self._gql("registerPlayerV2", variables)
        for part in parts:
            reg = ((part or {}).get("data") or {}).get("registerPlayerV2")
            if reg and reg.get("player", {}).get("id"):
                self._player_id = reg["player"]["id"]
                return self._player_id
        return None

    # ── GraphQL ─────────────────────────────────────────────────────────────

    def _gql(self, op: str, variables: dict) -> list:
        """POST a persisted-query op (as a 1-element batch) and return the raw
        response list.  Raises ScrapeSkipError on auth/persisted-query failure."""
        body = [{
            "operationName": op,
            "variables": variables,
            "extensions": {"persistedQuery": {"version": 1, "sha256Hash": _HASH[op]}},
        }]
        r = self.session.post(_GQL, data=json.dumps(body), timeout=30)
        if r.status_code == 401 or r.status_code == 403:
            raise ScrapeSkipError(f"[philo] {op} unauthorized (HTTP {r.status_code}) "
                                  "— session expired; keeping previous data.")
        r.raise_for_status()
        payload = r.json()
        parts = payload if isinstance(payload, list) else [payload]
        for part in parts:
            for err in (part.get("errors") or []):
                code = ((err.get("extensions") or {}).get("code") or "")
                if code == "PERSISTED_QUERY_NOT_FOUND":
                    raise ScrapeSkipError(
                        f"[philo] persisted-query hash for '{op}' is stale — "
                        "Philo rotated their client; refresh _HASH.")
        return parts

    # ── fetch_channels ──────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        if not self._ensure_session():
            raise ScrapeSkipError("[philo] no session; keeping previous channel data")

        channels: list[ChannelData] = []
        seen: set[str] = set()
        cursor = None
        for _ in range(_GUIDE_MAX_PAGES):
            variables = dict(_GUIDE_VARS)
            variables["endCursor"] = cursor
            parts = self._gql("page", variables)
            groups = (((parts[0].get("data") or {}).get("page") or {}).get("groups") or {})
            edges = groups.get("edges") or []
            for e in edges:
                ch = (e.get("node") or {}).get("channel") or {}
                cid = ch.get("channelId")
                if not cid or cid in seen:
                    continue
                seen.add(cid)
                self._add_channel(channels, cid, ch)
            page_info = groups.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        if not channels:
            raise ScrapeSkipError("[philo] guide returned 0 channels; keeping previous data")

        self._persist_session()
        logger.info("[philo] %d channels fetched", len(channels))
        return channels

    def _add_channel(self, channels: list[ChannelData], cid: str, ch: dict) -> None:
        name = ch.get("displayName") or "Unknown"
        logo = None
        white = ch.get("whiteLogo") or {}
        dark = ch.get("darkLogo") or {}
        for src in (dark.get("largeD") if isinstance(dark, dict) else None,
                    white.get("largeWhite") if isinstance(white, dict) else None,
                    white.get("smallWhite") if isinstance(white, dict) else None):
            if src:
                logo = src
                break
        category = category_for_channel(name, None)
        channels.append(ChannelData(
            source_channel_id=cid,
            name=name,
            stream_url=f"philo://{cid}",
            logo_url=logo,
            category=category,
            country="US",
            stream_type="dash",   # DRM bridge — DASH+Widevine, browser/EME only
        ))

    # ── fetch_epg (forward schedule from the guide grid) ────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        """Build a multi-hour EPG from the guide grid.

        One paginated page(GUIDE) traversal returns every channel's forward
        schedule inline (each tile = one airing with start/end/title/episode/
        ratings), so — unlike the old now-playing-only path — no per-channel
        fanout is needed. The grid ships no synopses, so descriptions are then
        backfilled (near-term window, batched, cached) from the per-broadcast
        PRESENTATION page."""
        enabled_ids = kwargs.get("enabled_ids")
        if not self._ensure_session():
            raise ScrapeSkipError("[philo] no session; keeping previous EPG data")
        self._load_description_cache()

        # entries: (prog, desc_key, broadcast_id) — kept so the description
        # backfill can map synopses back onto the airings that need them.
        entries: list[tuple] = []
        cursor = None
        for _ in range(_GUIDE_MAX_PAGES):
            variables = dict(_GUIDE_VARS)
            variables["initialTiles"] = _GUIDE_EPG_TILES
            variables["endCursor"] = cursor
            parts = self._gql("page", variables)
            groups = (((parts[0].get("data") or {}).get("page") or {}).get("groups") or {})
            edges = groups.get("edges") or []
            for e in edges:
                node = e.get("node") or {}
                cid = ((node.get("channel") or {}).get("channelId"))
                if not cid or (enabled_ids is not None and cid not in enabled_ids):
                    continue
                for tile_edge in ((node.get("tiles") or {}).get("edges") or []):
                    tile = tile_edge.get("node") or {}
                    prog = self._parse_tile(cid, tile)
                    if prog:
                        entries.append((prog, self._desc_key(tile), tile.get("playableAssetId")))
            page_info = groups.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if self._progress_cb:
                self._progress_cb("epg", len(entries), len(entries))

        self._backfill_descriptions(entries)
        programs = [prog for (prog, _k, _b) in entries]
        programs.sort(key=lambda p: (p.source_channel_id, p.start_time))
        n_desc = sum(1 for p in programs if p.description)
        logger.info("[philo] %d EPG entries across %d channels (%d with descriptions)",
                    len(programs), len({p.source_channel_id for p in programs}), n_desc)
        return programs

    # ── Description backfill (per-broadcast PRESENTATION page, batched+cached) ─

    @staticmethod
    def _desc_key(tile: dict) -> str | None:
        """Stable key for a synopsis: per-episode where possible (each episode has
        its own), else the show/movie id."""
        ep = tile.get("episode") or {}
        show = tile.get("show") or {}
        if ep.get("episodeId"):
            return f"ep:{ep['episodeId']}"
        if show.get("showId"):
            return f"sh:{show['showId']}"
        return None

    def _load_description_cache(self) -> None:
        raw = self.load_lazy_cache_key("description_cache") or {}
        if not isinstance(raw, dict):
            return
        now = time.time()
        for key, entry in raw.items():
            if isinstance(entry, dict) and entry.get("d") and isinstance(entry.get("t"), (int, float)):
                if (now - float(entry["t"])) < _DESC_CACHE_TTL:
                    self._desc_cache[key] = entry

    def _persist_description_cache(self) -> None:
        cache = self._desc_cache
        if len(cache) > _DESC_CACHE_MAX:   # evict oldest
            keep = sorted(cache, key=lambda k: cache[k].get("t", 0), reverse=True)[:_DESC_CACHE_MAX]
            cache = {k: cache[k] for k in keep}
            self._desc_cache = cache
        self._update_cache("description_cache", cache)

    def _backfill_descriptions(self, entries: list[tuple]) -> None:
        """Fetch synopses for airings starting within the near-term window whose
        key isn't cached yet, then apply cached synopses to every matching airing."""
        cutoff = datetime.now(timezone.utc) + timedelta(hours=_DESC_WINDOW_HOURS)
        # unique desc_key → a representative broadcast id, near-term & uncached only
        to_fetch: dict[str, str] = {}
        for prog, key, bid in entries:
            if not key or not bid or key in self._desc_cache:
                continue
            start = prog.start_time
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            if start <= cutoff:
                to_fetch.setdefault(key, bid)

        if to_fetch:
            items = list(to_fetch.items())
            fetched = 0
            for i in range(0, len(items), _DESC_BATCH):
                chunk = items[i:i + _DESC_BATCH]
                for key, desc in self._fetch_descriptions([b for _k, b in chunk],
                                                          [k for k, _b in chunk]).items():
                    self._desc_cache[key] = {"d": desc, "t": time.time()}
                    fetched += 1
                if self._progress_cb:
                    self._progress_cb("epg-desc", min(i + _DESC_BATCH, len(items)), len(items))
            if fetched:
                self._persist_description_cache()
            logger.debug("[philo] description backfill: %d fetched / %d needed (window %dh)",
                         fetched, len(to_fetch), _DESC_WINDOW_HOURS)

        # Apply every cached synopsis (incl. ones cached on prior scrapes) to airings.
        for prog, key, _bid in entries:
            if key and not prog.description:
                entry = self._desc_cache.get(key)
                if entry:
                    prog.description = entry["d"]

    def _fetch_descriptions(self, broadcast_ids: list[str], keys: list[str]) -> dict:
        """Batch one PRESENTATION query per broadcast id; return {key: longDescription}."""
        # Trim variables to only the query's required set — Philo caps the request
        # body at ~3.5KB, so smaller ops let us batch more per POST.
        batch = []
        for bid in broadcast_ids:
            variables = {"pageType": "PRESENTATION", "typeId": bid,
                         "includeTileDescription": True, "includeTileChannel": False,
                         "iconFormat": "SVG"}
            batch.append({"operationName": "page", "variables": variables,
                          "extensions": {"persistedQuery": {"version": 1,
                                         "sha256Hash": _HASH["page"]}}})
        out: dict[str, str] = {}
        try:
            r = self.session.post(_GQL, data=json.dumps(batch), timeout=40)
            if r.status_code != 200:
                return out
            for key, part in zip(keys, r.json()):
                tile = (((part.get("data") or {}).get("page") or {}).get("tile") or {})
                desc = tile.get("longDescription") or tile.get("description")
                if desc:
                    out[key] = desc
        except Exception as exc:
            logger.debug("[philo] description batch failed: %s", exc)
        return out

    @staticmethod
    def _img_url(value) -> str | None:
        if isinstance(value, dict):
            return value.get("url")
        return value if isinstance(value, str) else None

    @classmethod
    def _parse_tile(cls, cid: str, tile: dict) -> ProgramData | None:
        title = tile.get("title")
        starts = tile.get("availabilityStartsAt") or tile.get("airingTime")
        ends = tile.get("availabilityEndsAt")
        if not (title and starts):
            return None
        try:
            start = datetime.fromisoformat(starts.replace("Z", "+00:00"))
            if ends:
                end = datetime.fromisoformat(ends.replace("Z", "+00:00"))
            elif tile.get("durationInSeconds"):
                end = start + timedelta(seconds=int(tile["durationInSeconds"]))
            else:
                return None
        except Exception:
            return None
        show = tile.get("show") or {}
        ep = tile.get("episode") or {}
        # V-Chip / TV rating, if present
        rating = None
        for r in (tile.get("ratings") or []):
            if (r.get("classification") or "").upper() == "VCHIP" and r.get("value"):
                rating = r["value"]
                break
        poster = (cls._img_url(show.get("posterImage"))
                  or cls._img_url(ep.get("horizontalIconicImage"))
                  or cls._img_url(show.get("horizontalIconicImage")))
        is_movie = bool(show.get("movieReleaseYear"))
        # original_air_date column is db.Date → needs a Python date, not Philo's ISO string.
        oad = None
        oad_raw = ep.get("originalAirDate") or show.get("originalAirDate")
        if oad_raw:
            try:
                oad = datetime.fromisoformat(str(oad_raw).replace("Z", "+00:00")).date()
            except Exception:
                oad = None
        return ProgramData(
            source_channel_id=cid,
            title=title,
            start_time=start,
            end_time=end,
            poster_url=poster,
            episode_title=tile.get("subtitle"),
            season=ep.get("seasonNum"),
            episode=ep.get("episodeNum"),
            rating=rating,
            original_air_date=oad,
            program_type=("movie" if is_movie else ("episode" if ep else None)),
            series_id=show.get("showId"),
            episode_id=ep.get("episodeId"),
        )

    # ── Play-time resolve (DASH + Widevine) ─────────────────────────────────

    def _load_dash_cache(self) -> None:
        raw = self.cache.get("dash_cache") or {}
        if not isinstance(raw, dict):
            return
        now = time.time()
        for cid, entry in raw.items():
            if not isinstance(entry, dict):
                continue
            cached_at = entry.get("cached_at")
            if not entry.get("dash_url") or not isinstance(cached_at, (int, float)):
                continue
            if (now - float(cached_at)) >= _DASH_TTL:
                continue
            self._dash_cache[cid] = entry

    def _cache_dash(self, cid: str, dash_url: str, auth_token: str | None) -> None:
        self._dash_cache[cid] = {
            "dash_url": dash_url,
            "auth_token": auth_token,
            "cached_at": time.time(),
        }
        self._update_cache("dash_cache", self._dash_cache)

    def _cached_dash(self, cid: str) -> dict | None:
        entry = self._dash_cache.get(cid)
        if not entry:
            return None
        cached_at = entry.get("cached_at")
        if not isinstance(cached_at, (int, float)) or (time.time() - float(cached_at)) >= _DASH_TTL:
            self._dash_cache.pop(cid, None)
            return None
        return entry

    def resolve(self, raw_url: str) -> str:
        """philo://<channelId> → a live DASH MPD URL.

        Also caches the per-session Widevine authToken (in source_cache) so the
        license proxy — a separate request — can attach it to the DRMtoday call.
        """
        if not raw_url.startswith("philo://"):
            return raw_url
        cid = raw_url[len("philo://"):]

        cached = self._cached_dash(cid)
        if cached:
            return cached["dash_url"]

        if not self._ensure_session():
            raise RuntimeError(f"[philo] resolve — no session for {cid}")

        # 1. current broadcast for this channel
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        pres = self._gql("playbackSessionPresentation", {"id": cid, "broadcastAt": now})
        edges = ((((pres[0].get("data") or {}).get("node") or {})
                  .get("broadcasts") or {}).get("edges") or [])
        if not edges:
            raise StreamDeadError(f"[philo] no current broadcast for {cid}")
        broadcast_id = (edges[0].get("node") or {}).get("id")
        if not broadcast_id:
            raise RuntimeError(f"[philo] no broadcast id for {cid}")

        # 2. playback session → dashURL + drmProvider.authToken
        player_id = self._ensure_player()
        if not player_id:
            raise RuntimeError(f"[philo] could not obtain playerId for {cid}")
        variables = {"id": broadcast_id, "playerId": player_id, "idfa": None,
                     "lat": None, "givn": None, "tileGroupId": None,
                     "broadcastAt": None, "startAtOverride": None, "isPreload": False}
        cps = self._gql("createPlaybackSessionV2", variables)
        sess = None
        for part in cps:
            node = ((part.get("data") or {}).get("createPlaybackSessionV2"))
            if node:
                sess = node
                break
        if not sess or not sess.get("dashURL"):
            raise RuntimeError(f"[philo] no dashURL for {cid}")
        dash_url = sess["dashURL"]
        auth_token = ((sess.get("drmProvider") or {}).get("authToken"))

        self._cache_dash(cid, dash_url, auth_token)
        self._persist_session()
        logger.info("[philo] resolve %s → dashURL (license=%s)",
                    cid, "yes" if auth_token else "no")
        return dash_url

    def audit_resolve(self, raw_url: str) -> str | None:
        """Liveness-only resolution for the stream audit.

        Deliberately does NOT go through the full resolve():
          * resolve() raises StreamDeadError on an empty-broadcast response, and
            the audit's Dead handler permanently disables the channel with no
            grace (is_enabled cleared → not restored by a later rescrape). An
            empty broadcast is a normal transient (schedule boundary, brief API
            blip), not a confirmed-dead stream, so we return None (a soft audit
            error) instead of ever raising Dead here.
          * resolve() also mints a real createPlaybackSessionV2 (the same call a
            live tune makes) per channel — auditing the whole lineup would open a
            burst of real sessions against the account. This checks only whether a
            current broadcast exists (playbackSessionPresentation), never opening a
            playback session.

        Returns the opaque philo:// URL as an "alive, skip manifest fetch"
        sentinel, or None if liveness could not be confirmed (never Dead).
        """
        if not raw_url.startswith("philo://"):
            return raw_url
        cid = raw_url[len("philo://"):]

        # A live DASH URL cached from a recent tune/resolve proves the channel is up.
        if self._cached_dash(cid):
            return raw_url
        # No session to check with → can't audit, don't penalize (mirrors amazon).
        if not self._ensure_session():
            return raw_url

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        pres = self._gql("playbackSessionPresentation", {"id": cid, "broadcastAt": now})
        edges = ((((pres[0].get("data") or {}).get("node") or {})
                  .get("broadcasts") or {}).get("edges") or [])
        return raw_url if edges else None

    # ── Widevine license relay (DRMtoday) ───────────────────────────────────

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        # DRMtoday authenticates via the x-dt-auth-token header (attached in
        # prepare_license_request), so the URL is the same constant for every
        # channel; presence just flags the source DRM-capable.
        return cls.license_url

    @classmethod
    def prepare_license_request(cls, challenge: bytes, config: dict,
                                channel_id: str | None = None, **kwargs):
        """Attach the per-session x-dt-auth-token captured during resolve()."""
        headers = {
            "Origin": _ORIGIN,
            "Referer": f"{_ORIGIN}/",
            "User-Agent": _UA,
        }
        token = None
        if channel_id:
            entry = (config.get("dash_cache") or {}).get(channel_id)
            if isinstance(entry, dict):
                token = entry.get("auth_token")
        if token:
            headers["x-dt-auth-token"] = token
        return challenge, headers

    @classmethod
    def process_license_response(cls, response_bytes: bytes) -> bytes:
        """DRMtoday returns JSON {"status":"OK","license":"<base64>"}; Shaka needs
        the raw license bytes."""
        try:
            data = json.loads(response_bytes.decode("utf-8"))
        except Exception:
            return response_bytes   # already raw (or an error we pass through)
        lic = data.get("license")
        if lic:
            try:
                return base64.b64decode(lic)
            except Exception:
                return response_bytes
        return response_bytes

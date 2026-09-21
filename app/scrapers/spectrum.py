"""
Spectrum scraper for FastChannels.

Login is entirely browser-driven (Camoufox, reCAPTCHA Enterprise + ThreatMetrix
gated) via the "Sign in to Spectrum" button on this source's config panel —
see app/tve/browser_login/spectrum.py. There is no scripted credential POST:
this scraper only ever reads the access_token/refresh_token/client_device_id
that flow already saved into config, and raises ScrapeSkipError with a clear
pointer back to that button when there's nothing usable.

DRM: DASH + Widevine/PlayReady CENC. The Widevine license server needs BOTH a
Bearer access_token AND a second AST-Authorization header — the latter comes
back as the x-set-ast response header on the per-channel stream/live/v6 mint
call, not from any request/response body (confirmed live 2026-09-17).
It's cached per-channel (stream_cache), same shape as Fubo's dash_cache,
since it's minted per stream, not once per account.

Spectrum caps concurrent stream sessions ("aegis" sessions) at 3 per account
regardless of client. resolve() never releases the channel it's currently
serving (a real concurrent viewer on another device might still be using
it), but every distinct channel change mints a brand-new one — so
channel-surfing alone can exhaust the cap well inside the 5min cache TTL
(confirmed live 2026-09-17). _evict_lru_sessions keeps at most
_MAX_TRACKED_SESSIONS other channels' sessions open, releasing the
least-recently-minted ones first; _evict_cache_entry also releases a
channel's own session the moment its cache entry expires, rather than
silently minting a second one on the next resolve() for the same channel.

The ~12h access token is refreshed silently via the OAuth refresh_token grant
(_refresh_session, confirmed live 2026-09-17) whenever it's within
_TOKEN_REFRESH_BUFFER of expiring — no browser/recaptcha needed for that. The
refresh_token itself has its own absolute ceiling tied to the original login
(refresh_ceiling_at, read from validateSession's refreshTokenMaxTTL — a
countdown, not a sliding window that resets per refresh); once that's within
_RELOGIN_BUFFER, app.worker's spectrum_relogin_watchdog job fires an
unattended Camoufox re-login (check_relogin_due) using saved credentials
against the same trusted persistent profile the manual button uses.
_refresh_session also fires that same re-login REACTIVELY the instant a
refresh comes back 401 — confirmed live 2026-09-18 the refresh_token can be
invalidated well before refresh_ceiling_at's predicted deadline (e.g. a
separate login attempt against the same account, even an abandoned one, can
knock it out early), so waiting on the proactive watchdog alone isn't
enough. Only if neither path can complete on its own — e.g. credentials
changed — does a scrape ever fall back to ScrapeSkipError pointing a human
back to the button.
"""
from __future__ import annotations

import logging
import fcntl
import re
import time
import uuid
import random
import urllib.parse
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager

import requests

from .base import (BaseScraper, ChannelData, ConfigField, ProgramData,
                    ScrapeSkipError, StreamDeadError, infer_language_from_metadata)
from .category_utils import category_for_channel, infer_category_from_name
from ..tve.adobe_pass import TVENotAuthorizedError

logger = logging.getLogger(__name__)

_API_BASE = 'https://apis-vid.spectrum.net'
_AUTH_BASE = 'https://apis.spectrum.net'
_IMG_BASE = 'https://cdnimg.spectrum.net'
_LICENSE_BASE = 'https://apis-drm.spectrum.net/drm/licenseServer/widevine/v2'
_CLIENT_ID = 'stva-ovp'
_CLIENT_VERSION = '17.32.0.289483649'
_USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
               '(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36')
_TOKEN_REFRESH_BUFFER = 10 * 60  # refresh 10min before actual expiry, not exactly at it
_PROACTIVE_REFRESH_WINDOW = 2 * 60 * 60  # refresh the access token once it has under 2h left (watchdog runs every 20min)
_RELOGIN_BUFFER = 3 * 60 * 60   # trigger an unattended re-login with 3h of runway left on the refresh_token's ceiling
_RELOGIN_COOLDOWN = 45 * 60      # don't re-trigger more than once per 45min if a prior attempt is still in flight or failed

_MC_NAME_RE = re.compile(r'^~mc(\d+):?$')

# Spectrum's own channels/v3 networkName is genuinely garbage for every Music
# Choice channel — literally "~MC05:", nothing after the colon — confirmed
# live 2026-09-17 by checking the raw API response directly (not a scraper
# bug). Recovered by cross-referencing each channel's own EPG: every program
# on a Music Choice channel IS just the channel's real name (they're 24/7
# audio channels with no actual programming to speak of). Music Choice's
# lineup is a stable, standardized national one (same across MVPDs), so this
# is hardcoded rather than looked up per-scrape. #42 corrects an apparent
# upstream typo ("Musical Choice" -> "Music Choice") for consistency with
# every other entry.
_MUSIC_CHOICE_NAMES: dict[int, str] = {
    1: "Music Choice Today's Hits", 2: 'Music Choice Trending Hits',
    3: 'Music Choice Feel-Good Favorites', 4: 'Music Choice Pop Energy',
    5: 'Music Choice Hip Hop and R&B', 6: 'Music Choice Dance',
    7: 'Music Choice Hip-Hop Classics', 8: 'Music Choice Throwback Jams',
    9: 'Music Choice R&B Classics', 10: "Music Choice Today's R&B",
    11: 'Music Choice Gospel', 12: 'Music Choice Contemporary Christian',
    13: 'Music Choice Rock', 14: 'Music Choice Yacht Rock',
    15: "Music Choice '60s & '70s Mellow Hits", 16: 'Music Choice Adult Alternative',
    17: 'Music Choice Alt & Rock Favorites', 18: 'Music Choice Classic Rock',
    19: 'Music Choice Soft Rock', 20: 'Music Choice Happy Hits',
    21: 'Music Choice Pop Hits', 22: "Music Choice Today's Latin Hits",
    23: 'Music Choice Tropicales', 24: 'Music Choice Romantic Latin Pop',
    25: "Music Choice '70s & '80s Favorites", 26: "Music Choice '90s",
    27: "Music Choice '80s", 28: "Music Choice '70s",
    29: "Music Choice '60s Generation", 30: 'Music Choice Solid Gold Oldies',
    31: 'Music Choice Pop & Country', 32: "Music Choice Today's Country",
    33: 'Music Choice Country Favorites', 34: 'Music Choice Classic Country',
    35: 'Music Choice Country Rock', 36: 'Music Choice Sleep Sounds',
    37: 'Music Choice Relaxing Vibes', 38: 'Music Choice Calming Classical',
    39: 'Music Choice Joyful Instrumentals', 40: 'Music Choice Pop Instrumentals',
    41: 'Music Choice Light Classical', 42: 'Music Choice Classical Masterpieces',
    43: 'Music Choice Soundscapes', 44: 'Music Choice Smooth Jazz',
    45: 'Music Choice Jazz', 46: 'Music Choice Blues',
    47: 'Music Choice Singers & Swing', 48: 'Music Choice Easy Listening',
    49: 'Music Choice Classic Christmas', 50: 'Music Choice Sounds of the Seasons',
}

_EPG_HOURS_PER_CALL = 6
_EPG_BLOCKS = 8  # 8 * 6h = 48h of guide data per scrape
_STREAM_CACHE_TTL = 5 * 60  # aegisTokenRefreshSeconds was 300 on every mint seen live — match it
_MAX_TRACKED_SESSIONS = 2  # self-imposed, one under Spectrum's real 3-session AegisTooManySessions cap


class SpectrumScraper(BaseScraper):
    source_name = 'spectrum'
    display_name = 'Spectrum'
    is_premium = True
    source_category = 'premium'
    config_required = True
    license_url = _LICENSE_BASE
    stream_audit_enabled = True
    # Every channel is DASH+CENC, no exceptions observed — same shape as
    # amazon_prime_free/nbc_tve/sling/directv/warner_tve. Declared statically
    # rather than left to the audit's live per-fetch Widevine/PlayReady marker
    # detection, which can false-negative on an SSAI ad-break splice landing
    # genuinely clear content mid-otherwise-encrypted-stream (see warner_tve).
    all_channels_require_drm_bridge = True
    config_schema = [
        ConfigField('username', 'Username', placeholder='your Spectrum username',
                    help_text='Used by the "Sign in to Spectrum" button to auto-fill the sign-in form.'),
        ConfigField('password', 'Password', field_type='password', secret=True,
                    help_text='Used by the "Sign in to Spectrum" button to auto-fill the sign-in form.'),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._stream_cache: dict = dict(self.cache.get('stream_cache') or {})

    # ── Auth ─────────────────────────────────────────────────────────────────

    def _ensure_session(self) -> None:
        if not self.config.get('access_token') or not self.config.get('client_device_id'):
            raise ScrapeSkipError(
                '[spectrum] no session — use the source\'s "Sign in to Spectrum" '
                'button (Camoufox, reCAPTCHA-gated) to authenticate.')
        expires_at = self.config.get('token_expires_at')
        # Older browser captures omitted expiry. Refresh those once instead of
        # treating an unknown expiry as an indefinitely valid access token.
        if not expires_at or time.time() > float(expires_at) - _TOKEN_REFRESH_BUFFER:
            if not self._refresh_session():
                raise ScrapeSkipError(
                    '[spectrum] saved session has expired and could not be refreshed — '
                    'sign in again via the "Sign in to Spectrum" button.')

    def _refresh_session(self) -> bool:
        """Silent OAuth refresh_token grant — confirmed live 2026-09-17 this
        extends the access token's 12h life with no browser/recaptcha needed.
        The refresh token itself has its own absolute ceiling tied to the
        original login (refreshTokenMaxTTL, observed ~24h from login, not a
        sliding window that resets per refresh) — once that's passed this
        will start failing and _ensure_session falls back to asking for a
        fresh browser sign-in."""
        refresh_token = self.config.get('refresh_token')
        if not refresh_token:
            return False
        try:
            r = self.session.post(
                f'{_AUTH_BASE}/auth/oauth/v2/token',
                data={
                    'client_id': _CLIENT_ID, 'grant_type': 'refresh_token',
                    'refresh_token': refresh_token,
                    'client_device_id': self.config.get('client_device_id', ''),
                },
                headers={
                    'accept': 'application/json, text/plain, */*',
                    'origin': 'https://watch.spectrum.net',
                    'referer': 'https://watch.spectrum.net/',
                    'user-agent': _USER_AGENT,
                },
                timeout=15,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning('[spectrum] token refresh request failed: %s', exc)
            return False
        if not r.ok:
            logger.warning('[spectrum] token refresh rejected: HTTP %d', r.status_code)
            if r.status_code == 401:
                # A 401 here means the refresh_token itself was rejected
                # outright, not just "not due for renewal yet" — confirmed
                # live 2026-09-18 this can happen well before
                # refresh_ceiling_at's predicted deadline (a separate login
                # attempt against the same account, even an abandoned one,
                # can invalidate the existing refresh_token early). Don't
                # just wait for the proactive ceiling watchdog to notice —
                # fire the same unattended re-login right away so the NEXT
                # resolve()/scrape() call has a fresh session instead of a
                # human needing to notice a 503 and hit the button.
                if self._fire_unattended_relogin():
                    logger.info('[spectrum] refresh_token was rejected outright — triggered unattended re-login')
            return False
        try:
            data = r.json()
        except ValueError:
            logger.warning('[spectrum] token refresh response was not JSON')
            return False
        access_token = data.get('access_token')
        if not access_token:
            logger.warning('[spectrum] token refresh response had no access_token: %s', data)
            return False
        self._update_config('access_token', access_token)
        self._update_config('refresh_token', data.get('refresh_token') or refresh_token)
        self._update_config('token_captured_at', int(time.time()))
        self._update_config('token_expires_at', int(time.time()) + int(data.get('expires_in') or 0))
        logger.info('[spectrum] refreshed access token, valid for another %ds', int(data.get('expires_in') or 0))
        ceiling_ttl = _fetch_refresh_ceiling(
            self.config.get('access_token'), self.config.get('refresh_token'),
            self.config.get('client_device_id'))
        if ceiling_ttl is not None:
            self._update_config('refresh_ceiling_at', int(time.time()) + ceiling_ttl)
        return True

    def refresh_if_due(self) -> bool:
        """Called on a timer by app.worker's spectrum_relogin_watchdog job.
        Refreshes the access token while it still has _PROACTIVE_REFRESH_WINDOW
        of life left. Confirmed live 2026-09-21 the refresh grant succeeds on a
        still-valid token but was rejected (401) all 3 times it was attempted,
        which was always ~2min AFTER expiry because the 6h scrape cadence lines
        up with the 12h token life — that forced a full browser re-login every
        12h and skipped a scrape each time. Refreshing ahead of expiry avoids
        both. No-op unless a session is actually saved."""
        if not (self.config.get('access_token') and self.config.get('refresh_token')
                and self.config.get('client_device_id')):
            return False
        expires_at = self.config.get('token_expires_at')
        if not expires_at:
            return False  # unknown expiry — _ensure_session refreshes these on the next scrape/resolve
        if float(expires_at) - time.time() > _PROACTIVE_REFRESH_WINDOW:
            return False
        return self._refresh_session()

    def check_relogin_due(self) -> bool:
        """Called on a timer by app.worker's spectrum_relogin_watchdog job, NOT
        from any normal scrape/resolve path. _refresh_session can extend the
        access token indefinitely but only up to the refresh_token's own
        absolute ceiling (refresh_ceiling_at) — this decides whether that
        ceiling is close enough to warrant firing an unattended re-login via
        _fire_unattended_relogin. This is the PROACTIVE half; _refresh_session
        also fires the same trigger REACTIVELY the moment a refresh comes
        back 401, since confirmed live 2026-09-18 the refresh_token can be
        invalidated well before this ceiling predicts."""
        ceiling_at = self.config.get('refresh_ceiling_at')
        if not ceiling_at:
            return False
        remaining = float(ceiling_at) - time.time()
        if remaining > _RELOGIN_BUFFER:
            return False
        fired = self._fire_unattended_relogin()
        if fired:
            logger.info('[spectrum] refresh_token has %.1fh left before its ceiling — triggered unattended re-login', remaining / 3600)
        return fired

    def _fire_unattended_relogin(self) -> bool:
        """Shared trigger-with-cooldown logic behind both check_relogin_due
        (proactive, ceiling-based) and _refresh_session's reactive 401
        handling. Fires the same Camoufox flow the "Sign in to Spectrum"
        button uses, using saved credentials against the persistent trusted
        profile — device trust built up there is what let the original
        human-driven login past Spectrum's reCAPTCHA Enterprise + ThreatMetrix
        gate (a genuinely fresh profile was rejected outright), so an
        unattended run against that same profile is expected to usually
        complete without a human. If it can't — changed credentials, a new
        verification step — it simply times out like the manual button
        would; _RELOGIN_COOLDOWN keeps either caller from re-firing a browser
        login on every single resolve() call during an outage, and
        ScrapeSkipError's pointer back to the button remains the ultimate
        fallback everywhere else in this scraper."""
        username = (self.config.get('username') or '').strip()
        password = (self.config.get('password') or '').strip()
        if not username or not password:
            return False
        last_attempt = float(self.config.get('auto_relogin_last_attempt_at') or 0)
        if time.time() - last_attempt < _RELOGIN_COOLDOWN:
            return False
        from ..routes.tasks import trigger_spectrum_signin
        if not trigger_spectrum_signin():
            return False  # shared browser profile busy with another MVPD login — retry next tick
        self._update_config('auto_relogin_last_attempt_at', int(time.time()))
        return True

    def _headers(self, extra: dict | None = None) -> dict:
        headers = {
            'accept': 'application/json, text/plain, */*',
            'authorization': f"Bearer {self.config.get('access_token', '')}",
            'device_id': self.config.get('client_device_id', ''),
            'x-client-id': _CLIENT_ID,
            'x-client-version': _CLIENT_VERSION,
            'origin': 'https://watch.spectrum.net',
            'referer': 'https://watch.spectrum.net/',
            'user-agent': _USER_AGENT,
        }
        if extra:
            headers.update(extra)
        return headers

    @staticmethod
    def _raise_for_auth(r) -> None:
        """Spectrum's API gateway returns a plain 400 (not 401/403) for an
        expired/invalid token — {"resultCode":"2076","resultMessage":"Bad
        Request"} — confirmed live 2026-09-17. Surface that as a clear
        "sign in again" skip rather than a raw HTTPError traceback; anything
        else still raises normally."""
        if r.status_code in (400, 401, 403):
            raise ScrapeSkipError(
                f'[spectrum] API rejected the saved session (HTTP {r.status_code}) — '
                'sign in again via the "Sign in to Spectrum" button.')
        r.raise_for_status()

    # ── Channels ─────────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        self._ensure_session()
        r = self.session.get(
            f'{_API_BASE}/lantern/lrs/api/smarttv/channels/v3',
            params={'streamVersion': 5}, headers=self._headers(), timeout=30,
        )
        self._raise_for_auth(r)
        rows = r.json()

        channels: list[ChannelData] = []
        for row in rows:
            # Excludes VOD/non-linear catalog rows (e.g. "Video On Demand") —
            # every real tunable channel in a live sample had online=true.
            if not row.get('online'):
                continue
            entitlement_id = row.get('entitlementId')
            tms_guide_id = row.get('tmsGuideId')
            # ~6% of raw names carry stray leading/trailing whitespace straight
            # from Spectrum (e.g. " MeTV (KMEE) HD", "CW (KAZT) ") — confirmed
            # live 2026-09-17 across 30/502 channels.
            name = (row.get('networkName') or row.get('callSign') or '').strip()
            mc_match = _MC_NAME_RE.match(name.lower())
            if mc_match:
                name = _MUSIC_CHOICE_NAMES.get(int(mc_match.group(1)), name)
            if not entitlement_id or not tms_guide_id or not name:
                continue
            numbers = row.get('channelNumbers') or []
            logo_uri = row.get('logoUri')
            # ~8% of channels carry more than one raw genre (e.g. AMC HD West:
            # ['Entertainment', 'Movies']) — try each individually in order
            # rather than joining them, which would never match any alias.
            category = None
            for raw_genre in (row.get('genres') or [None]):
                category = category_for_channel(name, raw_genre, source_name='spectrum')
                if category:
                    break
            category = category or infer_category_from_name(name)
            channels.append(ChannelData(
                source_channel_id=str(entitlement_id),
                name=name,
                stream_url=f'spectrum://{entitlement_id}',
                logo_url=f'{_IMG_BASE}{logo_uri}' if logo_uri else None,
                category=category,
                language=infer_language_from_metadata(name),
                country='US',
                stream_type='dash',
                number=numbers[0] if numbers else None,
                # tmsGuideId, not entitlementId — the EPG grid endpoint is keyed
                # by this, not by the playback/entitlement id. Read back in
                # fetch_epg() via each ChannelData's own .guide_key.
                guide_key=tms_guide_id,
            ))
        logger.info('[spectrum] %d channels fetched', len(channels))
        return channels

    # ── EPG ──────────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        self._ensure_session()
        tms_to_entitlement = {ch.guide_key: ch.source_channel_id for ch in channels if ch.guide_key}
        if not tms_to_entitlement:
            return []
        tms_ids = ','.join(tms_to_entitlement.keys())

        now = datetime.now(timezone.utc)
        block_start = now.replace(minute=0, second=0, microsecond=0)
        # One block before "now" too, matching what the real app fetches —
        # covers the currently-airing program even when "now" isn't exactly
        # on an hour boundary.
        block_start -= timedelta(hours=block_start.hour % _EPG_HOURS_PER_CALL)

        programs: list[ProgramData] = []
        for i in range(_EPG_BLOCKS):
            start = block_start + timedelta(hours=_EPG_HOURS_PER_CALL * i)
            r = self.session.get(
                f'https://stva-epgs-cf-v4.ipvideo.prd.spectrum.net/epgs/api/smarttv/guide/v4/twctv/grid',
                params={
                    'hours': _EPG_HOURS_PER_CALL,
                    'startDateTime': int(start.timestamp()),
                    'tmsIds': tms_ids,
                },
                headers=self._headers(), timeout=60,
            )
            if i == 0:
                self._raise_for_auth(r)  # bail out fast — every block will fail the same way
            elif not r.ok:
                logger.warning('[spectrum] EPG block %d (start=%s) failed: HTTP %d', i, start.isoformat(), r.status_code)
                continue
            grid = r.json()
            for tms_id, entries in grid.items():
                entitlement_id = tms_to_entitlement.get(tms_id)
                if not entitlement_id:
                    continue
                for entry in entries or []:
                    program = self._program_from_entry(entitlement_id, entry)
                    if program:
                        programs.append(program)
        logger.info('[spectrum] %d EPG entries fetched across %d blocks', len(programs), _EPG_BLOCKS)
        return programs

    @staticmethod
    def _program_from_entry(entitlement_id: str, entry: dict) -> ProgramData | None:
        start_sec = entry.get('startTimeSec')
        duration_min = entry.get('durationMinutes')
        title = entry.get('title')
        if start_sec is None or duration_min is None or not title:
            return None
        start_time = datetime.fromtimestamp(start_sec, tz=timezone.utc)
        end_time = start_time + timedelta(minutes=duration_min)
        metadata = entry.get('metadata') or {}
        genres = entry.get('genres') or []
        is_movie = 'movie' in (g.lower() for g in genres) or (entry.get('programType') or '').lower() == 'movie'
        program_type = 'movie' if is_movie else ('episode' if metadata.get('type') == 'episode' else None)
        image_uri = entry.get('imageUrl')
        return ProgramData(
            source_channel_id=entitlement_id,
            title=title,
            start_time=start_time,
            end_time=end_time,
            description=entry.get('shortDesc') or None,
            poster_url=f'{_IMG_BASE}{image_uri}' if image_uri else None,
            category='; '.join(genres) or None,
            rating=entry.get('rating') or None,
            episode_title=metadata.get('title') if metadata.get('type') == 'episode' else None,
            season=metadata.get('season'),
            episode=metadata.get('episode'),
            program_type=program_type,
            series_id=metadata.get('tmsSeriesId') or entry.get('vodTmsSeriesId') or None,
            episode_id=entry.get('tmsProgramId') or None,
        )

    # ── Playback ─────────────────────────────────────────────────────────────

    @contextmanager
    def _stream_cache_transaction(self):
        """Serialize session mint/eviction and commit before returning a manifest.

        A separate process-shared lock covers the upstream calls as well as the
        cache write. The config-store lock only serializes writes and cannot
        protect snapshots loaded by concurrent scraper instances. Polling flock
        keeps gevent workers cooperative while another request is minting.
        """
        from ..config_store import load_source_cache_by_name, persist_source_cache_updates
        from ..models import Source

        with open('/tmp/fastchannels-spectrum-stream.lock', 'a+') as lock_file:
            deadline = time.monotonic() + 90
            while True:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError('[spectrum] timed out waiting for stream session lock')
                    time.sleep(0.1)
            try:
                source = Source.query.filter_by(name=self.source_name).first()
                if source is None:
                    raise RuntimeError('[spectrum] source no longer exists')
                self._stream_cache = dict(load_source_cache_by_name(
                    self.source_name, keys=['stream_cache']).get('stream_cache') or {})
                self._pending_cache_updates.pop('stream_cache', None)
                try:
                    yield
                finally:
                    # Do not leave a snapshot for the caller's later persistence:
                    # that would overwrite another transaction after we unlock.
                    updates = self._pending_cache_updates.pop('stream_cache', None)
                    if updates is not None and not persist_source_cache_updates(
                        source.id, {'stream_cache': updates}
                    ):
                        raise RuntimeError('[spectrum] could not save stream session credentials')
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def resolve(self, raw_url: str) -> str:
        self._ensure_session()
        cid = raw_url.removeprefix('spectrum://')
        with self._stream_cache_transaction():
            cached = self._cached_stream(cid)
            if cached:
                return cached['manifest_url']
            self._evict_lru_sessions(cid)
            manifest_url, ast, stream_session_id, aegis_token = self._mint_stream(cid)
            self._cache_stream(cid, manifest_url, ast, stream_session_id, aegis_token)
            return manifest_url

    def audit_resolve(self, raw_url: str) -> str:
        """Stream Audit checks all ~500 channels back-to-back, one right after
        another — unlike real playback, which holds one session open for as
        long as someone's actually watching. Spectrum caps concurrent stream
        sessions at 3 per account ("AegisTooManySessions", confirmed live
        2026-09-17: every mint that doesn't release its predecessor eats one
        of only 3 slots, so the 4th+ channel in an audit run starts failing
        with 429 and the whole audit aborts around ~20 consecutive errors).
        Release the session immediately after minting since the audit only
        needs the manifest once — this is audit-only (see run_stream_audit's
        audit_resolve preference); ordinary resolve()/play() only ever
        releases a session opportunistically, via _evict_lru_sessions'
        least-recently-minted eviction, never the channel actually being
        resolved for the current request."""
        self._ensure_session()
        cid = raw_url.removeprefix('spectrum://')
        with self._stream_cache_transaction():
            # Reuse an active playback session without replacing its credentials
            # or releasing it. Audit-only sessions need no persisted license data.
            cached = self._cached_stream(cid)
            if cached:
                return cached['manifest_url']
            manifest_url, ast, stream_session_id, aegis_token = self._mint_stream(cid)
            if aegis_token:
                self._release_aegis(aegis_token)
            return manifest_url

    def _release_aegis(self, aegis_token: str) -> None:
        try:
            self.session.delete(
                f'{_API_BASE}/ipvs/api/smarttv/aegis/v1',
                params={'aegis': aegis_token},
                headers=self._headers(), timeout=10,
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug('[spectrum] aegis release failed (non-fatal): %s', exc)

    def _cached_stream(self, cid: str) -> dict | None:
        entry = self._stream_cache.get(cid)
        if not entry or not entry.get('manifest_url'):
            return None
        if (time.time() - float(entry.get('cached_at', 0))) >= _STREAM_CACHE_TTL:
            self._evict_cache_entry(cid, entry)
            return None
        return entry

    def _evict_cache_entry(self, cid: str, entry: dict) -> None:
        """Pops a cache entry and, if it's still holding an aegis session
        Spectrum thinks is open, releases it. Without this, a channel that's
        genuinely still being watched past the 5min cache TTL (matches
        aegisTokenRefreshSeconds, confirmed live 2026-09-17) would silently
        mint a second session on the next resolve() call instead of reusing
        one — eating a slot in the account's hard 3-concurrent cap for the
        exact same channel."""
        self._stream_cache.pop(cid, None)
        self._update_cache('stream_cache', self._stream_cache)
        aegis_token = entry.get('aegis_token')
        if aegis_token:
            self._release_aegis(aegis_token)

    def _evict_lru_sessions(self, exclude_cid: str) -> None:
        """Channel-surfing mints a brand-new aegis session on every distinct
        channel — resolve() deliberately never releases the PREVIOUS
        channel's session on its own (it might still have a real concurrent
        viewer on another device), but confirmed live 2026-09-17 that alone
        is enough to exhaust the account's hard 3-session cap within a
        handful of channel changes, well inside the 5min TTL that would
        otherwise self-heal it (see _evict_cache_entry). Keep at most
        _MAX_TRACKED_SESSIONS other channels' sessions open, releasing the
        least-recently-minted ones first, before minting one more.
        Self-imposed and one below Spectrum's real cap on purpose — leaves
        headroom instead of racing it exactly."""
        open_entries = [
            (cid, e) for cid, e in self._stream_cache.items()
            if cid != exclude_cid and e.get('aegis_token')
        ]
        if len(open_entries) < _MAX_TRACKED_SESSIONS:
            return
        open_entries.sort(key=lambda kv: kv[1].get('cached_at', 0))
        for stale_cid, entry in open_entries[:len(open_entries) - _MAX_TRACKED_SESSIONS + 1]:
            self._evict_cache_entry(stale_cid, entry)

    def _cache_stream(self, cid: str, manifest_url: str, ast: str | None,
                       stream_session_id: str, aegis_token: str | None = None) -> None:
        self._stream_cache[cid] = {
            'manifest_url': manifest_url,
            'ast': ast,
            'stream_session_id': stream_session_id,
            'aegis_token': aegis_token,
            'cached_at': time.time(),
        }
        self._update_cache('stream_cache', self._stream_cache)

    def _mint_stream(self, cid: str) -> tuple[str, str | None, str, str | None]:
        device_id = self.config.get('client_device_id', '')
        r = self.session.post(
            f'{_API_BASE}/lantern/foc-ipvs/api/smarttv/stream/live/v6/{cid}',
            params={
                'csid': 'stva_ovp_pc_live', 'dai-supported': 'true', 'drm-supported': 'true',
                'vast-supported': 'true', 'adID': device_id, 'secureTransport': 'true',
                'use_token': 'true', 'OTT': 'false', 'parentalControlsEnabled': 'false',
            },
            json={'deviceCapabilities': {
                'packaging': 'dash', 'drm': 'cenc',
                'videoCodecs': ['avc'], 'audioCodecs': ['aac', 'ac3', 'eac3'],
            }},
            headers=self._headers({'content-type': 'application/json'}), timeout=20,
        )
        if r.status_code == 403:
            # A 403 here isn't necessarily a dead session — Spectrum overloads it
            # for at least two distinct per-channel reasons, both confirmed live
            # 2026-09-17, neither of which "sign in again" fixes:
            #   {"context":{"unentitled":true,...}}  — account package doesn't
            #     include this channel. Permanent account-tier fact → StreamDeadError,
            #     same handling play.py already gives Philo's "channel not in
            #     subscription" case.
            #   {"context":{"blockedOOH":true,"dmaMismatch":...,"initLocation":
            #     {"inMarket":false,...}},...}  — local-affiliate retransmission
            #     rules blocking this SPECIFIC channel because the resolving IP
            #     isn't recognized as in-market. Not an account-tier fact the way
            #     "unentitled" is (a real subscriber's own home connection would
            #     likely show inMarket:true for their own locals — this could
            #     resolve differently on a different network), but the channel is
            #     just as unplayable from THIS deployment right now, so it's
            #     disabled the same way — TVENotAuthorizedError rather than
            #     StreamDeadError so the admin UI can tell "not authorized from
            #     here" apart from "stream is actually broken". A future Stream
            #     Audit re-checks NotAuthorized channels automatically and
            #     re-enables this the moment it stops happening (e.g. the server
            #     moves to the account's real home network) — confirmed live
            #     2026-09-17 this doesn't self-resolve by waiting, only by
            #     resolving from a different network.
            try:
                context = r.json().get('context') or {}
            except ValueError:
                context = {}
            if context.get('unentitled') or context.get('blockedByPCChannel') or context.get('blockedDRM'):
                reason = next((k for k, v in context.items() if k.startswith(('unentitled', 'blocked')) and v), 'blocked')
                raise StreamDeadError(f'[spectrum] channel {cid} not entitled under this account ({reason})')
            if context:
                blocked_reason = next(
                    (k for k, v in context.items() if v is True and k not in ('inUS', 'inUsOrTerritory')), None,
                )
                if blocked_reason or context.get('dmaMismatch') or context.get('streamProperties', {}).get('availableOutOfMarket') is False:
                    raise TVENotAuthorizedError(
                        f'[spectrum] channel {cid} blocked for this location/market '
                        f'({blocked_reason or "dmaMismatch"}) — not a session problem, '
                        'may resolve differently from the account\'s actual home network.')
        if r.status_code == 429:
            # Not generic rate-limiting — Spectrum caps concurrent stream sessions
            # at 3 per account ("AegisTooManySessions"/networkLimits.sessionLimit,
            # confirmed live 2026-09-17). Only actually fixable by releasing
            # sessions promptly (see audit_resolve/_release_aegis); reported here
            # as an accurate skip rather than a generic error so it doesn't count
            # toward the audit's consecutive-error abort budget.
            try:
                failure = r.json().get('failure')
            except ValueError:
                failure = None
            if failure == 'AegisTooManySessions':
                raise ScrapeSkipError(
                    f'[spectrum] channel {cid}: account at its concurrent stream session '
                    'limit — not a dead session, sessions should free up shortly.')
        self._raise_for_auth(r)
        data = r.json()
        ast = r.headers.get('x-set-ast')
        aegis_token = (data.get('aegis') or {}).get('aegisToken')
        stream_url = data.get('streamUrl')
        if not stream_url:
            raise RuntimeError(f'[spectrum] stream/live/v6 returned no streamUrl for {cid}: {data}')

        stream_session_id = (
            time.strftime('%Y%m%d%H%M') + 'V-' + str(uuid.uuid4()) + '|'
            + format(int(time.time() * 1000), 'x') + '|0'
        )
        manifest_url = stream_url
        if urllib.parse.urlsplit(stream_url).netloc == 'edge-mm.spectrum.net':
            # DAI-eligible channels route through this ad-decisioning redirector,
            # which needs Nielsen/ad-tracking query params appended or it 404s
            # ("UNKNOWN-ID") — confirmed live these are self-generated, not
            # signed/validated.
            init_location = data.get('initLocation') or {}
            base, _, qs = stream_url.partition('?')
            params = dict(urllib.parse.parse_qsl(qs))
            params.update({
                'lat': '0', 'vcid': str(uuid.uuid4()), 'mapTEnabled': 'false',
                'blockDataSharing': 'false',
                'bz5': init_location.get('geoZip', ''), 'z5': init_location.get('serviceZip', ''),
                'pvrn': str(random.randint(10 ** 19, 10 ** 20 - 1)),
                'vprn': str(random.randint(10 ** 19, 10 ** 20 - 1)),
                'adId': device_id, 'csid': 'stva_ovp_pc_live',
                'altContent': data.get('altContent', ''),
                'behindOwnModem': 'false', 'stateAbbr': init_location.get('stateAbbr', ''),
                'inMarket': 'false', 'OTT': 'false', 'parentalControlsEnabled': 'false',
                'streamSessionId': stream_session_id,
            })
            manifest_url = base + '?' + urllib.parse.urlencode(params)
        return manifest_url, ast, stream_session_id, aegis_token

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        if not channel_id:
            return _LICENSE_BASE
        entry = (config.get('stream_cache') or {}).get(channel_id)
        session_id = entry.get('stream_session_id') if isinstance(entry, dict) else None
        if not session_id:
            return _LICENSE_BASE
        return f'{_LICENSE_BASE}?contentType=LINEAR&streamSessionId={session_id}'

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs
    ) -> tuple[bytes, dict]:
        headers = {
            'Origin': 'https://watch.spectrum.net',
            'Referer': 'https://watch.spectrum.net/',
        }
        token = config.get('access_token')
        if token:
            headers['Authorization'] = f'Bearer {token}'
        # Returned as the x-set-ast response header on stream/live/v6, per
        # channel — required in addition to the Bearer token or the license
        # server 400s with AST_AUTHORIZATION_HEADER_REQUIRED (confirmed live
        # 2026-09-17). Cached alongside the manifest URL, not source-wide.
        entry = (config.get('stream_cache') or {}).get(channel_id or '')
        ast = entry.get('ast') if isinstance(entry, dict) else None
        if ast:
            headers['AST-Authorization'] = ast
        return challenge, headers


def _fetch_refresh_ceiling(access_token: str, refresh_token: str, client_device_id: str) -> int | None:
    """validateSession is the only endpoint that reports refreshTokenMaxTTL —
    seconds actually remaining on the refresh_token's absolute ceiling, tied to
    the original login and confirmed live 2026-09-17 NOT to reset when the
    access token is refreshed (86373s at login, 58018s remaining after one
    refresh roughly 8h later — a countdown, not a sliding window). Callers
    persist now()+this as refresh_ceiling_at so the auto-relogin watchdog knows
    the real deadline instead of guessing ~24h. Best-effort: returns None on
    any failure, never raises — this is bookkeeping, not something worth
    failing a login or a refresh over."""
    try:
        r = requests.get(
            f'{_AUTH_BASE}/auth/oauth/v2/validateSession',
            params={'getLocation': 'false'},
            headers={
                'x-access-token': access_token or '',
                'x-refresh-token': refresh_token or '',
                'x-client-device-id': client_device_id or '',
                'x-client-id': _CLIENT_ID,
                'user-agent': _USER_AGENT,
            },
            timeout=15,
        )
        if not r.ok:
            return None
        ttl = r.json().get('refreshTokenMaxTTL')
        return int(ttl) if ttl is not None else None
    except Exception as exc:  # noqa: BLE001
        logger.debug('[spectrum] refreshTokenMaxTTL lookup failed: %s', exc)
        return None


def save_login_result(local_storage: dict, cox_cookies: list[dict] | None) -> None:
    """Persists tokens harvested by app.tve.browser_login.spectrum.run_spectrum_signin
    onto the spectrum Source row. local_storage is the raw {oauth_token,
    xoauth_refresh_token, device_id, xoauth_device_verifier, xoauth_username,
    xoauth_token_expiration} dict read directly out of the signed-in page's
    localStorage — these are the SAME keys the real watch.spectrum.net app reads
    on every load, so this works whether sign-in just happened in this session
    or silently carried over via the persistent Camoufox profile's cookies.
    cox_cookies is cached purely so a future Cox TVE integration (this account
    authenticates against Cox's own Okta org) can reuse this same session
    without a second interactive login; nothing reads it yet."""
    import time as _time
    from ..extensions import db
    from ..models import Source

    src = Source.query.filter_by(name='spectrum').first()
    if not src:
        src = Source(name='spectrum', display_name='Spectrum', is_enabled=False)
        db.session.add(src)
    cfg = dict(src.config or {})
    cfg['access_token'] = local_storage.get('oauth_token')
    cfg['refresh_token'] = local_storage.get('xoauth_refresh_token')
    cfg['device_verifier'] = local_storage.get('xoauth_device_verifier')
    cfg['token_captured_at'] = int(_time.time())
    cfg.pop('token_expires_at', None)
    expiration_ms = local_storage.get('xoauth_token_expiration')
    if expiration_ms:
        try:
            cfg['token_expires_at'] = int(expiration_ms) // 1000
        except (TypeError, ValueError):
            pass
    if local_storage.get('device_id'):
        cfg['client_device_id'] = local_storage['device_id']
    ceiling_ttl = _fetch_refresh_ceiling(
        cfg.get('access_token'), cfg.get('refresh_token'), cfg.get('client_device_id'))
    if ceiling_ttl is not None:
        cfg['refresh_ceiling_at'] = int(_time.time()) + ceiling_ttl
    # A fresh login means the auto-relogin watchdog's job is done — clear any
    # retry bookkeeping from a prior attempt so it doesn't carry over.
    cfg.pop('auto_relogin_last_attempt_at', None)
    cfg.pop('auto_relogin_last_error', None)
    if cox_cookies:
        cfg['cox_cookie_jar'] = cox_cookies
        cfg['cox_cookie_jar_captured_at'] = int(_time.time())
    src.config = cfg
    db.session.commit()

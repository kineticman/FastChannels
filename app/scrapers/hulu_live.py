"""Hulu + Live TV (source hulu_live) — the Live TV lineup with a Hulu account.

Scoped 2026-11-17 (dev/hulu/live_tv_scoping.md, HAR dev/hulu/1.har).

Sign-in: Hulu's device code, the same link the TV Everywhere Hulu sign-in uses
(app/tve/mvpd/hulu.py): the user enters a code at hulu.com/activate and we get
a ~1 year device token, renewed into 12 h user tokens with no password. The
device token ROTATES on every renewal, so each renewal is saved straight away
(_save_link) under a Redis lock. The link lives on this source
(config['hulu_device']); it can be copied once from a TV Everywhere account
that already has one, so a user with both enters one code.

Lineup and guide (Bearer user token, Android app identity):
  GET  guide.hulu.com/guide/views?lat&long   channels: id, callSign, name, logo;
                                              the LOCAL view is the market's locals
  POST guide.hulu.com/guide/listing           airings (eab, start, end, headline)
                                              for any channels and time range
  POST guide.hulu.com/guide/details           metadata for up to 500 eabs a call

Playback: POST play.hulu.com/v6/playlist with the channel and its current
airing's eab, as the web player (deejay 214; the Android id wants an
Android-only body). Every accepted config is DRM: Widevine/PlayReady DASH or
FairPlay HLS; clear configs get DJA-400-003. So the source is bridge-only
(FastChannels Player / PrismCast). The reply's stream_url is the MPD (its token
lasts minutes: resolve on every tune, never cache it) and wv_server a signed
license URL (~10 h) that takes the raw challenge with no headers; it's cached
per channel in dash_cache for the license proxy.

Location: Hulu picks locals from the lat/long sent with every guide and
playlist call, and refuses a local whose market doesn't cover them (403
BYA-403-013 "Roaming Geo"). We send the user's home location (a ZIP or
"lat, long" on the source; blank = this server's IP location), never anything else.

Each playlist call opens a stream session that counts against the account's
stream limit for ~2 minutes, so there's no stream audit.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote
from datetime import datetime, timedelta, timezone

import requests

from ..gracenote_map import resolve_gracenote
from ..tve.adobe_pass import TVEAuthError, TVENotAuthorizedError
from ..tve.mvpd import hulu
from .base import BaseScraper, ChannelData, ConfigField, ProgramData, ScrapeSkipError, infer_language_from_metadata
from .category_utils import category_for_channel

logger = logging.getLogger(__name__)

_DBG = {'at': 0.0, 'on': False}


def _dbg() -> bool:
    """Settings -> Advanced "debug logging" (or FC_HULU_LIVE_DEBUG / FC_DEBUG).
    The root logger is capped at INFO, so debug lines are logged at INFO under a
    [hulu_live][debug] tag when this is on. Cached 30 s: the MPD is polled every
    few seconds and the setting is a DB read."""
    now = time.time()
    if now - _DBG['at'] > 30:
        try:
            from ..debug_flag import debug_logging_enabled
            _DBG['on'] = bool(debug_logging_enabled('FC_HULU_LIVE_DEBUG'))
        except Exception:  # noqa: BLE001
            _DBG['on'] = False
        _DBG['at'] = now
    return _DBG['on']

GUIDE = 'https://guide.hulu.com/guide'
PLAYLIST = 'https://play.hulu.com/v6/playlist'
LICENSE_BASE = 'https://hulu.playback.edge.bamgrid.com/widevine-hulu/v1/hulu/linear/obtain-license'
WEB_ORIGIN = 'https://www.hulu.com'
SCHEME = 'hulu://channel/'
LINK_KEY = hulu.ACCOUNT_KEY  # config['hulu_device'], the same shape as the TVE link

LINK_STATUS_KEY = 'hulu_live:link:status'
_LOCK_KEY = 'hulu_live:refresh_lock'
_LINK_TIMEOUT = 900
_LINK_POLL_SECONDS = 5
SIGNIN_CONFIG_KEYS = (LINK_KEY, 'live_tv', 'max_streams', 'plan_checked_at', 'signin_error')
_RELINK_HINT = 'Sign in to Hulu again on the source.'
_NO_LIVE_TV = "This Hulu account doesn't include Live TV, so there are no channels to add."

_EPG_HOURS = 72
_ART_WARM_HOURS = 24  # program images are warmed this far ahead (see _warm_images)
_DETAILS_BATCH = 500
_LICENSE_TTL = 8 * 3600  # signed license URLs are good for ~10 h
_IP_LOCATION_TTL = 24 * 3600
_MPD_KEY = 'hulu_live:mpd:'
_MPD_RENEW_MARGIN = 45   # re-resolve this long before the manifest URL's 300 s token ends
_PIN_KEY = 'hulu_live:pin:'
_MPD_KEEP_SECONDS = 1800  # periods that ended longer ago than this are trimmed from the MPD
# Channels DVR gives up on a tuner after 20 s; each play-time call gets this.
_PLAY_TIMEOUT = (4, 8)

_WEB_DEEJAY_ID = 214
_PLAYBACK = {
    'version': 2,
    'video': {'codecs': {'values': [{'type': 'H264', 'width': 1920, 'height': 1080, 'framerate': 30,
                                     'level': '4.1', 'profile': 'HIGH'}], 'selection_mode': 'ONE'}},
    'audio': {'codecs': {'values': [{'type': 'AAC'}], 'selection_mode': 'ONE'}},
    'drm': {'values': [{'type': 'WIDEVINE', 'version': 'MODULAR', 'security_level': 'L3'},
                       {'type': 'PLAYREADY', 'version': 'V2', 'security_level': 'SL2000'}],
            'selection_mode': 'ALL'},
    'manifest': {'type': 'DASH', 'https': True, 'multiple_cdns': True, 'patch_updates': False, 'hulu_types': True,
                 'live_dai': True, 'multiple_periods': True, 'xlink': False, 'secondary_audio': True,
                 'unified_asset_signaling': False, 'live_fragment_delay': 3},
    'segments': {'values': [{'type': 'FMP4', 'encryption': {'mode': 'CENC', 'type': 'CENC'}, 'https': True}],
                 'selection_mode': 'ONE'},
}

# FastChannels Player's devices have hardware Widevine (L1); asking as L1 gets
# Hulu's 1080p60 ladder (h264=FHD_60) instead of the web's 720p30. A software
# CDM (the admin/watch players, PrismCast's Chrome) is L3 and must keep _PLAYBACK.
_PLAYBACK_HD = {
    **_PLAYBACK,
    'video': {'codecs': {'values': [{'type': 'H264', 'width': 1920, 'height': 1080, 'framerate': 60,
                                     'level': '4.2', 'profile': 'HIGH'}], 'selection_mode': 'ONE'}},
    'drm': {'values': [{'type': 'WIDEVINE', 'version': 'MODULAR', 'security_level': 'L1'}], 'selection_mode': 'ALL'},
}

_LATLON_RE = re.compile(r'^\s*(-?\d{1,2}(?:\.\d+)?)\s*[, ]\s*(-?\d{1,3}(?:\.\d+)?)\s*$')
_ZIP_RE = re.compile(r'^\s*(\d{5})(?:-\d{4})?\s*$')


def _redis():
    try:
        import redis
        return redis.from_url(os.environ.get('REDIS_URL', 'redis://localhost:6379/0'), decode_responses=True,
                              socket_timeout=2)
    except Exception:  # noqa: BLE001
        return None


def _source():
    from ..models import Source
    return Source.query.filter_by(name='hulu_live').first()


def _save_link(source_id: int, link: dict | None) -> None:
    """Writes the link now, not at the end of the run: the device token it
    replaces is already superseded."""
    from ..config_store import persist_source_config_updates
    persist_source_config_updates(source_id, {LINK_KEY: link})


def _stored_link() -> dict | None:
    """The link as saved in the DB right now (another worker may have rotated it)."""
    from ..extensions import db
    src = _source()
    if not src:
        return None
    db.session.refresh(src)
    link = (src.config or {}).get(LINK_KEY)
    return link if isinstance(link, dict) and link.get('device_token') and link.get('serial') else None


def plan_summary(link: dict) -> dict:
    """{'live_tv', 'max_streams'} from the account's subscription."""
    sub = hulu.user_info(link).get('subscription') or {}
    packages = {str(p) for p in sub.get('package_ids') or []}
    streams = ((sub.get('features') or {}).get('CONCURRENT_STREAMS') or {}).get('max_limit')
    return {'live_tv': bool(packages & hulu.LIVE_TV_PACKAGES), 'max_streams': streams}


# ── sign-in (device code; the routes are in app/routes/api_sources.py) ──────

def start_link(source) -> dict:
    link = (source.config or {}).get(LINK_KEY) or {}
    serial = link.get('serial') or hulu.new_serial()  # relinking keeps one device on the account
    code = hulu.register_device_code(serial)
    return {'code': code, 'serial': serial, 'url': hulu.ACTIVATE_URL, 'expires_in': _LINK_TIMEOUT}


def link_status(ctx: dict, state: str, message: str = '') -> dict:
    return {'state': state, 'message': message, 'code': ctx['code'], 'url': ctx['url']}


def _finish_signin(source_id: int, link: dict) -> tuple[bool, str]:
    """Saves a new link and records the plan. Returns (ok, message)."""
    from ..extensions import db
    from ..models import Source
    try:
        plan = plan_summary(link)
    except Exception as exc:  # noqa: BLE001 — the next scrape checks again
        logger.warning('[hulu_live] plan check after sign-in failed: %s', exc)
        plan = {'live_tv': None, 'max_streams': None}
    src = db.session.get(Source, source_id)
    cfg = dict(src.config or {})
    cfg[LINK_KEY] = link
    cfg.pop('signin_error', None)
    cfg.update({'live_tv': plan['live_tv'], 'max_streams': plan['max_streams'], 'plan_checked_at': int(time.time())})
    cfg.setdefault('device_identifier', uuid.uuid4().hex.upper())
    src.config = cfg
    db.session.commit()
    if plan['live_tv'] is False:
        return False, _NO_LIVE_TV
    if src.is_enabled:
        from ..routes.tasks import trigger_scrape
        trigger_scrape('hulu_live', force_full=True)
    return True, ''


def run_link(ctx: dict) -> None:
    """RQ job: wait for the code to be entered at hulu.com/activate, then save
    the link on the source and rescrape. Quits once LINK_STATUS_KEY names a
    different code or is gone (cancelled)."""
    import redis

    from app.worker import flask_app

    with flask_app.app_context():
        r = redis.from_url(flask_app.config['REDIS_URL'])

        def superseded() -> bool:
            raw = r.get(LINK_STATUS_KEY)
            return not raw or json.loads(raw).get('code') != ctx['code']

        def set_status(state: str, message: str = '') -> None:
            if not superseded():
                r.setex(LINK_STATUS_KEY, _LINK_TIMEOUT + 120, json.dumps(link_status(ctx, state, message)))

        deadline = time.monotonic() + min(ctx.get('expires_in', _LINK_TIMEOUT), _LINK_TIMEOUT)
        link = None
        try:
            while time.monotonic() < deadline and not superseded():
                link = hulu.poll_device_code(ctx['code'], ctx['serial'])
                if link:
                    break
                time.sleep(_LINK_POLL_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[hulu_live] sign-in failed: %s', exc)
            set_status('error', str(exc)[:300])
            return
        if not link:
            if not superseded():
                set_status('expired', 'The code expired before it was entered.')
            return
        src = _source()
        if not src:
            set_status('error', 'The Hulu + Live TV source no longer exists.')
            return
        ok, message = _finish_signin(src.id, link)
        logger.info('[hulu_live] signed in to Hulu%s', '' if ok else f' ({message})')
        set_status('success' if ok else 'error', message)


def tve_link_account():
    """A TV Everywhere account that already has a Hulu link (the shared one
    first), or None."""
    from ..models import TVEAccount
    from ..tve.accounts import SHARED_PROVIDER_ID
    accounts = [a for a in TVEAccount.query.all() if hulu.hulu_link(a)]
    accounts.sort(key=lambda a: (a.provider_id != SHARED_PROVIDER_ID, a.id))
    return accounts[0] if accounts else None


def copy_tve_link(source) -> tuple[bool, str]:
    """Uses a TV Everywhere account's Hulu link for this source too (no new
    code). The copy renews at once, so each side then rotates its own token."""
    account = tve_link_account()
    if not account:
        return False, 'No TV Everywhere account has a Hulu sign-in to use.'
    link = hulu.refresh_link(dict(hulu.hulu_link(account)), relink_hint='Get a Hulu code instead.')
    link['linked_at'] = int(time.time())
    link['copied_from'] = account.provider_id
    return _finish_signin(source.id, link)


# ── location ─────────────────────────────────────────────────────────────────

def _geocode_zip(zip_code: str) -> tuple[float, float]:
    r = requests.get(f'https://api.zippopotam.us/us/{zip_code}', timeout=10)
    if r.status_code == 404:
        raise ValueError(f'{zip_code} is not a US ZIP code.')
    r.raise_for_status()
    place = ((r.json() or {}).get('places') or [{}])[0]
    return round(float(place['latitude']), 4), round(float(place['longitude']), 4)


def _ip_location() -> tuple[float, float]:
    r = requests.get('https://ipinfo.io/json', headers={'User-Agent': 'FastChannels'}, timeout=8)
    r.raise_for_status()
    lat, _, lon = ((r.json() or {}).get('loc') or '').partition(',')
    return float(lat), float(lon)


class HuluLiveScraper(BaseScraper):
    source_name = 'hulu_live'
    added_in = '6.0.0'
    display_name = 'Hulu + Live TV'
    source_category = 'premium'
    is_premium = True
    is_beta = True
    scrape_interval = 360
    epg_quality = 'full'
    license_url = LICENSE_BASE
    all_channels_require_drm_bridge = True
    # Each playlist call holds a stream slot for ~2 minutes: no audit, and no
    # manifest probe after a tune.
    stream_audit_enabled = False
    play_manifest_check = False
    config_required = True
    # Image-warming bookkeeping, only read during a scrape.
    LAZY_CACHE_KEYS = frozenset({'warm_logos', 'warm_art'})
    config_schema = [
        ConfigField('home_location', 'Home location', placeholder='43215  or  39.96, -83.00',
                    help_text='Your home ZIP code, or latitude and longitude. Hulu picks your local channels from '
                              'it and only plays locals inside their market. Blank uses this server\'s IP location.'),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._link_cache: dict | None = None

    # ── auth ──

    def _link(self) -> dict:
        link = self._link_cache or self.config.get(LINK_KEY)
        if not (isinstance(link, dict) and link.get('device_token') and link.get('serial')):
            raise ScrapeSkipError('Hulu + Live TV: sign in to Hulu on the source first.')
        return link

    def _signed_in_link(self, force: bool = False, timeout=20) -> dict:
        """The link with a user token that has some life left, renewing (and
        saving the rotated device token) when needed."""
        link = self._link()
        if not force and hulu.user_token_fresh(link):
            return link
        r = _redis()
        locked = False
        if r is not None:
            for _ in range(20):
                try:
                    locked = bool(r.set(_LOCK_KEY, '1', nx=True, ex=30))
                except Exception:  # noqa: BLE001
                    break
                if locked:
                    break
                time.sleep(0.5)
        try:
            # Another worker may have renewed it while we waited.
            stored = _stored_link() or link
            if hulu.user_token_fresh(stored) and (not force or stored.get('user_token') != link.get('user_token')):
                self._remember(stored)
                return stored
            try:
                new_link = hulu.refresh_link(stored, relink_hint=_RELINK_HINT, timeout=timeout)
            except hulu.HuluNotLinked as exc:
                self._signin_lost(str(exc))
                raise
            src = _source()
            if src:
                _save_link(src.id, new_link)
            self._remember(new_link)
            return new_link
        finally:
            if locked:
                try:
                    r.delete(_LOCK_KEY)
                except Exception:  # noqa: BLE001
                    pass

    def _remember(self, link: dict) -> None:
        self._link_cache = link
        self.config[LINK_KEY] = link  # not queued: _save_link already wrote it

    def _signin_lost(self, message: str) -> None:
        logger.warning('[hulu_live] %s', message)
        src = _source()
        if src:
            _save_link(src.id, None)
        self.config.pop(LINK_KEY, None)
        self._link_cache = None
        self._update_config('signin_error', message)

    def _request(self, method: str, url: str, timeout=30, **kw) -> requests.Response:
        for attempt in (0, 1):
            link = self._signed_in_link(force=bool(attempt), timeout=timeout)
            r = self.session.request(method, url, headers=hulu.api_headers(link), timeout=timeout, **kw)
            if r.status_code == 401 and not attempt:
                continue  # Hulu dropped the user token early
            return r
        return r

    # ── location ──

    def _latlon(self) -> tuple[float, float]:
        raw = str(self.config.get('home_location') or '').strip()
        m = _LATLON_RE.match(raw)
        if m:
            return float(m.group(1)), float(m.group(2))
        zm = _ZIP_RE.match(raw)
        if zm:
            saved = self.config.get('home_latlon') or {}
            if saved.get('input') == zm.group(1):
                return saved['lat'], saved['long']
            try:
                lat, lon = _geocode_zip(zm.group(1))
            except ValueError as exc:
                raise ScrapeSkipError(f'Hulu + Live TV: {exc}') from exc
            self._update_config('home_latlon', {'input': zm.group(1), 'lat': lat, 'long': lon})
            return lat, lon
        if raw:
            raise ScrapeSkipError('Hulu + Live TV: the home location must be a 5-digit ZIP code or "latitude, longitude".')
        cached = self.cache.get('ip_location') or {}
        if cached.get('lat') is not None and time.time() - float(cached.get('at') or 0) < _IP_LOCATION_TTL:
            return cached['lat'], cached['long']
        try:
            lat, lon = _ip_location()
        except Exception as exc:  # noqa: BLE001
            if cached.get('lat') is not None:
                return cached['lat'], cached['long']
            raise ScrapeSkipError('Hulu + Live TV: could not look up this server\'s location; '
                                  'set your home ZIP code on the source.') from exc
        self._update_cache('ip_location', {'lat': lat, 'long': lon, 'at': time.time()})
        return lat, lon

    # ── scrape ──

    def pre_run_setup(self) -> None:
        if not isinstance(self.config.get(LINK_KEY), dict):
            raise ScrapeSkipError('Hulu + Live TV: sign in to Hulu on the source first.')
        try:
            link = self._signed_in_link()
        except hulu.HuluNotLinked as exc:
            raise ScrapeSkipError(str(exc)) from exc
        except TVEAuthError as exc:
            raise ScrapeSkipError(f'Hulu + Live TV: {exc}') from exc
        try:
            plan = plan_summary(link)
        except Exception as exc:  # noqa: BLE001 — not worth failing the scrape over
            logger.warning('[hulu_live] plan check failed: %s', exc)
            return
        self._update_config('live_tv', plan['live_tv'])
        self._update_config('max_streams', plan['max_streams'])
        self._update_config('plan_checked_at', int(time.time()))
        if not plan['live_tv']:
            raise ScrapeSkipError(f'Hulu + Live TV: {_NO_LIVE_TV}')
        if self.config.get('signin_error'):
            self._update_config('signin_error', None)

    def _views(self) -> list[dict]:
        lat, lon = self._latlon()
        r = self._request('GET', f'{GUIDE}/views', params={'lat': lat, 'long': lon})
        r.raise_for_status()
        return (r.json() or {}).get('views') or []

    def fetch_channels(self) -> list[ChannelData]:
        views = self._views()
        by_name = {v.get('name'): (v.get('details') or {}).get('channels') or [] for v in views}
        locals_ = {c['id'] for c in by_name.get('LOCAL', []) if c.get('id')}
        out = []
        for c in by_name.get('CHANNELS', []):
            cid, name = c.get('id'), (c.get('name') or '').strip()
            if not cid or not name:
                continue
            call = (c.get('callSign') or '').strip()
            local = cid in locals_
            out.append(ChannelData(
                source_channel_id=cid,
                name=name,
                slug='hulu-' + re.sub(r'[^a-z0-9]+', '-', (call or name).lower()).strip('-'),
                stream_url=f'{SCHEME}{cid}',
                logo_url=art_url(c.get('logoUrl'), LOGO_SIZE, 'png'),
                category=category_for_channel(name, 'Broadcast' if local else None, self.source_name),
                language=infer_language_from_metadata(name),
                stream_type='dash',
                guide_key=f'HULU:{call}' if call else f'HULU:{cid}',
                gracenote_id=resolve_gracenote(self.source_name, lookup_key=call) if call else None,
            ))
        if not out:
            raise ScrapeSkipError('Hulu + Live TV: Hulu returned no channels.')
        self._warm_logos([c.logo_url for c in out if c.logo_url])
        logger.info('[hulu_live] %d channels (%d local)', len(out), len(locals_))
        return out

    def _warm_logos(self, urls: list[str]) -> None:
        self._warm_images('warm_logos', urls, 'logo')

    def _warm_images(self, cache_key: str, urls: list[str], label: str) -> None:
        """Hulu's image server renders each size on its first request and
        answers that one 408 ("Submitted for processing"). Ask once per new
        image URL here, so a player's first request finds it ready."""
        warmed = set(self.load_lazy_cache_key(cache_key) or [])
        todo = [u for u in dict.fromkeys(urls) if u not in warmed]
        if not todo:
            return

        def warm(url):
            for _ in range(3):
                try:
                    if requests.get(url, timeout=10).status_code == 200:
                        return url
                except requests.RequestException:
                    pass
                time.sleep(1)
            return None

        with ThreadPoolExecutor(max_workers=12) as pool:
            done = [u for u in pool.map(warm, todo) if u]
        keep = set(urls)
        self._update_cache(cache_key, sorted((warmed & keep) | set(done)))
        logger.info('[hulu_live] warmed %d/%d new %s images', len(done), len(todo), label)

    def _listing(self, channel_ids: list[str], start: datetime, end: datetime, timeout=60) -> list[dict]:
        r = self._request('POST', f'{GUIDE}/listing', timeout=timeout, json={
            'start_time': _iso(start), 'end_time': _iso(end), 'channels': channel_ids})
        r.raise_for_status()
        return (r.json() or {}).get('channels') or []

    def _details(self, eabs: list[str]) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for i in range(0, len(eabs), _DETAILS_BATCH):
            batch = eabs[i:i + _DETAILS_BATCH]
            try:
                r = self._request('POST', f'{GUIDE}/details', timeout=60, json={'eabs': batch})
                r.raise_for_status()
            except Exception as exc:  # noqa: BLE001 — titles from the listing still make a guide
                logger.warning('[hulu_live] guide details batch failed: %s', exc)
                continue
            for item in (r.json() or {}).get('items') or []:
                if item.get('eab'):
                    out[item['eab']] = item
        return out

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        ids = [c.source_channel_id for c in channels]
        if not ids:
            return []
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        airings: list[tuple[str, dict]] = []
        seen: set[tuple[str, str]] = set()
        for day in range(0, _EPG_HOURS, 24):
            start, end = now + timedelta(hours=day), now + timedelta(hours=min(day + 24, _EPG_HOURS))
            try:
                rows = self._listing(ids, start, end)
            except Exception as exc:  # noqa: BLE001
                if not airings:
                    raise
                logger.warning('[hulu_live] guide listing from %s failed: %s', _iso(start), exc)
                continue
            for ch in rows:
                for p in ch.get('programs') or []:
                    key = (ch.get('id'), p.get('airingId') or p.get('eab'))
                    if ch.get('id') and p.get('eab') and key not in seen:
                        seen.add(key)
                        airings.append((ch['id'], p))
        details = self._details(list(dict.fromkeys(p['eab'] for _, p in airings if _real_eab(p))))
        out = [prog for cid, p in airings if (prog := self._program(cid, p, details.get(p['eab']) or {}))]
        out += self._off_air(airings, now, {c.source_channel_id: c.name for c in channels})
        soon = now + timedelta(hours=_ART_WARM_HOURS)
        self._warm_images('warm_art', [p.poster_url for p in out if p.poster_url and p.start_time < soon], 'program')
        logger.info('[hulu_live] %d EPG entries (%d with details)', len(out), len(details))
        return out

    @staticmethod
    def _off_air(airings: list[tuple[str, dict]], window_start: datetime, names: dict[str, str]) -> list[ProgramData]:
        """Part-time channels (Adult Swim East/West) list the hours they're off
        the air as an "unscheduled" entry with no program and only an end time
        (Hulu won't play the channel then). Shown as Off Air from the previous
        airing's end, or the window start, so the guide isn't just empty. An
        unscheduled entry with no end is only the edge of Hulu's published
        schedule, not a known off-air block, and is left out."""
        out = []
        last_end: dict[str, datetime] = {}
        for cid, p in sorted(airings, key=lambda a: (a[0], _parse(a[1].get('airingEnd')) or window_start)):
            end = _parse(p.get('airingEnd'))
            if p.get('availabilityState') == 'unscheduled' and end and not p.get('airingStart'):
                start = last_end.get(cid, window_start)
                if end > start:
                    out.append(ProgramData(
                        source_channel_id=cid, title='Off Air', start_time=start, end_time=end,
                        description=f"{names.get(cid) or 'This channel'} isn't on the air on Hulu during these hours."))
            if end:
                last_end[cid] = max(end, last_end.get(cid, end))
        return out

    @staticmethod
    def _program(channel_id: str, airing: dict, d: dict) -> ProgramData | None:
        start, end = _parse(airing.get('airingStart')), _parse(airing.get('airingEnd'))
        title = (d.get('series_name') or d.get('headline') or airing.get('headline') or '').strip()
        if not start or not end or end <= start or not title:
            return None
        episode_title = (d.get('episode_name') or '').strip()
        if episode_title.casefold() == title.casefold():
            episode_title = ''
        season, episode = d.get('season_number'), d.get('episode_number')
        kind = (d.get('type') or '').lower()
        sports = d.get('sports_info') or {}
        art = d.get('artwork') or {}
        href = d.get('details_href') or {}
        premiere = _parse(d.get('premiere_date'))
        return ProgramData(
            source_channel_id=channel_id,
            title=title,
            start_time=start,
            end_time=end,
            description=(d.get('description') or '').strip() or None,
            # panel is the episode's own still (16:9), thumbnail the series art.
            poster_url=art_url(art.get('panel') or art.get('thumbnail'), PROGRAM_ART_SIZE, 'jpeg'),
            category='Sports' if sports else None,
            rating=d.get('rating') or None,
            episode_title=episode_title or None,
            season=season if isinstance(season, int) and season > 0 else None,
            episode=episode if isinstance(episode, int) and episode > 0 else None,
            original_air_date=premiere.date() if premiere else None,
            program_type='movie' if kind == 'movie' else ('episode' if kind == 'episode' else None),
            series_id=href.get('id') if href.get('type') == 'series' else None,
            episode_id=d.get('id') or None,
        )

    # ── playback ──

    def audit_resolve(self, raw_url: str) -> str:
        return raw_url  # no audit (stream_audit_enabled is off); never open a session

    def _current_eab(self, channel_id: str) -> str:
        now = datetime.now(timezone.utc)
        rows = self._listing([channel_id], now - timedelta(minutes=1), now + timedelta(hours=1), timeout=_PLAY_TIMEOUT)
        programs = next((c.get('programs') or [] for c in rows if c.get('id') == channel_id), [])
        for p in programs:
            start, end = _parse(p.get('airingStart')), _parse(p.get('airingEnd'))
            if _real_eab(p) and start and end and start <= now < end:
                return p['eab']
        first = programs[0] if programs else {}
        if first.get('availabilityState') == 'unscheduled':
            # A part-time channel (Adult Swim) between dayparts: Hulu has no
            # airing to play and refuses the channel (404 "Could not get live airings").
            back = _parse(first.get('airingEnd'))
            if back:
                from ..timezone_utils import current_zoneinfo
                back = back.astimezone(current_zoneinfo())
            raise RuntimeError('Hulu + Live TV: this channel is off the air right now'
                               + (f" (back at {back.strftime('%-I:%M %p %Z')})" if back else ''))
        if _real_eab(first):
            return first['eab']
        raise RuntimeError(f'Hulu + Live TV: nothing is airing on {channel_id} right now')

    def resolve(self, raw_url: str, hd: bool = False) -> str:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported Hulu stream URL: {raw_url}')
        cid = raw_url[len(SCHEME):]
        lat, lon = self._latlon()
        device = self.config.get('device_identifier') or uuid.uuid4().hex.upper()
        if not self.config.get('device_identifier'):
            self._update_config('device_identifier', device)
        body = {
            'device_identifier': device, 'deejay_device_id': _WEB_DEEJAY_ID, 'version': 1, 'all_cdn': True,
            'content_eab_id': self._current_eab(cid), 'channel_id': cid, 'play_intent': 'live',
            'lat': lat, 'long': lon, 'region': 'US', 'language': 'en', 'guid': device, 'unencrypted': True,
            'xlink_support': False, 'interface_version': '1.15.1', 'network_mode': 'wifi', 'playback': _PLAYBACK_HD if hd else _PLAYBACK,
        }
        r = self._request('POST', PLAYLIST, timeout=_PLAY_TIMEOUT, json=body)
        try:
            data = r.json() or {}
        except ValueError:
            data = {}
        if r.status_code != 200 or not data.get('stream_url'):
            code, message = data.get('code') or '', data.get('message') or r.text[:200]
            if code == 'BYA-403-013':
                raise TVENotAuthorizedError(
                    'Hulu + Live TV: this channel is outside the home location set on the source (Hulu: Roaming Geo).')
            if r.status_code == 403:
                raise TVENotAuthorizedError(f'Hulu + Live TV refused this channel: {message} ({code})')
            raise RuntimeError(f'Hulu + Live TV playlist: HTTP {r.status_code} {message} ({code})')
        if data.get('wv_server'):
            self._update_cache('dash_cache', {cid: {'license_url': data['wv_server'], 'cached_at': time.time()}})
        logger.info('[hulu_live] tuned %s (%s)', cid, data.get('content_eab_id', '')[:60])
        return data['stream_url']

    def live_manifest(self, raw_url: str, hd: bool = False) -> str:
        """A full, current MPD for a channel, for players to poll.

        Hulu's MPD carries a <Location> whose refreshes are Hulu's own patch
        format (EssentialProperty urn:com:hulu:schema:mpd:2017:patch: just the
        new segments), which no standard player can apply, and the web player
        is always put on it (patch_updates and the URL's signed patch=true
        can't be turned off). But the playlist's own manifest URL returns the
        full, advancing MPD on every fetch until its token expires (300 s,
        then 410), and a fresh playlist continues the same timeline. So: keep
        that URL per channel (shared by everyone watching it), re-resolve
        shortly before it expires, drop the <Location> so players keep polling
        us, and trim ad periods that ended a while ago (A&E's 3 h window is
        ~40 periods, ~300 KB every 3 s otherwise). hd asks as an L1 device for
        the 1080p ladder (FastChannels Player only). Each re-resolve opens a new
        stream session; Hulu lets the old one lapse after ~2 minutes."""
        cid = raw_url[len(SCHEME):] if raw_url.startswith(SCHEME) else raw_url
        key = f'{_MPD_KEY}{cid}{":hd" if hd else ""}'
        r = _redis()

        def read_cache():
            try:
                return json.loads(r.get(key) or 'null') if r is not None else None
            except Exception:  # noqa: BLE001
                return None

        def usable(entry):
            return bool(entry and float(entry.get('exp') or 0) - time.time() >= _MPD_RENEW_MARGIN)

        cached = read_cache()
        for attempt in (0, 1):
            if not usable(cached) or attempt:
                # Players poll in parallel (FastChannels Player fetches the MPD
                # twice at start); one resolve under a lock, so they share one
                # stream session instead of opening one each.
                locked = False
                if r is not None:
                    for _ in range(int(_PLAY_TIMEOUT[1] / 0.25)):
                        try:
                            locked = bool(r.set(f'{key}:lock', '1', nx=True, ex=15))
                        except Exception:  # noqa: BLE001
                            break
                        if locked:
                            break
                        time.sleep(0.25)
                        fresh = read_cache()
                        if usable(fresh) and (not attempt or fresh.get('url') != cached.get('url')):
                            cached = fresh
                            break
                try:
                    fresh = read_cache()
                    if usable(fresh) and (not attempt or not cached or fresh.get('url') != cached.get('url')):
                        cached = fresh
                    else:
                        url = self.resolve(f'{SCHEME}{cid}', hd=hd)
                        m = re.search(r'[?&]token=(\d{10})_', url)
                        cached = {'url': url, 'exp': int(m.group(1)) if m else time.time() + 240}
                        if _dbg():
                            logger.info('[hulu_live][debug] %s new stream session (hd=%s, replaces=%s, token ends in %ss)',
                                        cid, hd, 'expired url' if not attempt else 'rejected url',
                                        int(cached['exp'] - time.time()))
                        if r is not None:
                            try:
                                r.set(key, json.dumps(cached), ex=max(int(cached['exp'] - time.time()), 1))
                            except Exception:  # noqa: BLE001
                                pass
                finally:
                    if locked:
                        try:
                            r.delete(f'{key}:lock')
                        except Exception:  # noqa: BLE001
                            pass
            resp = requests.get(cached['url'], timeout=_PLAY_TIMEOUT,
                                headers={'Origin': WEB_ORIGIN, 'Referer': WEB_ORIGIN + '/'})
            if resp.status_code in (403, 404, 410) and not attempt:
                continue  # the token lapsed early; get a new playlist
            resp.raise_for_status()
            mpd = trim_periods(re.sub(r'\s*<Location>.*?</Location>', '', resp.text, flags=re.DOTALL))
            mpd = _pin_periods_cached(r, f'{_PIN_KEY}{cid}{":hd" if hd else ""}', mpd)
            if _dbg():
                _log_period_changes(f'{cid}{":hd" if hd else ""}', cached['url'], mpd)
            return mpd
        raise RuntimeError('Hulu + Live TV: no manifest')

    # ── DRM license proxy wiring ──

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        # The license proxy merges source_cache into config; resolve() put this
        # tune's signed URL there.
        entry = (config.get('dash_cache') or {}).get(channel_id or '') or {}
        if entry.get('license_url') and time.time() - float(entry.get('cached_at') or 0) < _LICENSE_TTL:
            return entry['license_url']
        return cls.license_url

    @classmethod
    def prepare_license_request(cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs):
        return challenge, {'Content-Type': 'application/octet-stream', 'Origin': WEB_ORIGIN,
                           'Referer': WEB_ORIGIN + '/'}

    @classmethod
    def community_map_keys(cls, channel) -> list[str]:
        key = (getattr(channel, 'guide_key', None) or '')
        return [key[len('HULU:'):]] if key.startswith('HULU:') else []


_PERIOD_RE = re.compile(r'<Period\b[^>]*>.*?</Period>\s*', re.DOTALL)
_PERIOD_START_RE = re.compile(r'<Period\b[^>]*\sstart="PT([\d.]+)S"')
_AST_RE = re.compile(r'availabilityStartTime="([^"]+)"')


def trim_periods(mpd: str, keep_seconds: float = _MPD_KEEP_SECONDS) -> str:
    """Drops whole Periods that ended more than keep_seconds ago (a Period ends
    where the next one starts). The newest Period is always kept, and nothing
    changes if the timing can't be read."""
    ast = _parse((_AST_RE.search(mpd) or [None, None])[1])
    periods = list(_PERIOD_RE.finditer(mpd))
    if not ast or len(periods) < 2:
        return mpd
    starts = []
    for p in periods:
        m = _PERIOD_START_RE.match(p.group(0))
        if not m:
            return mpd
        starts.append(ast.timestamp() + float(m.group(1)))
    cutoff = time.time() - keep_seconds
    drop = 0
    while drop < len(periods) - 1 and starts[drop + 1] < cutoff:
        drop += 1
    if not drop:
        return mpd
    return mpd[:periods[0].start()] + mpd[periods[drop].start():]


_PERIOD_TAG_RE = re.compile(r'<Period\b[^>]*>')
_PERIOD_ID_RE = re.compile(r'\bid="([^"]*)"')
_PERIOD_STARTSTR_RE = re.compile(r'\bstart="PT([\d.]+)S"')
_SEGTMPL_RE = re.compile(r'<SegmentTemplate\b[^>]*>')
_PTO_RE = re.compile(r'presentationTimeOffset="(\d+)"')
_TIMESCALE_RE = re.compile(r'\btimescale="(\d+)"')
_PIN_TOLERANCE_S = 0.05   # audio offsets move in whole AAC frames (~21 ms), video/start in 5.005 s


def pin_periods(mpd: str, pins: dict) -> tuple[str, dict]:
    """Holds each Period's start and presentationTimeOffset at the values it first
    had. Local channels (e.g. an NBC affiliate) come as ONE Period whose start,
    presentationTimeOffset and first segment time all advance by a segment on
    every refresh, as Hulu slides the window. Media3 reads that as the Period
    being removed and replaced: it flushes the buffer and decoder and steps back
    a few seconds, once per segment (a ~0.4 s stall every ~5 s at 1080p60).
    Segment presentation time is start + (t - presentationTimeOffset) / timescale,
    so the same segment lands on the same time when start and offset are held
    together, which is only done while that stays true for every template (to
    50 ms: audio offsets move in whole 1024-sample frames); otherwise the Period
    is re-pinned as it is.

    pins: {period id: {'start': 'PT...S', 'pto': [[pto, timescale], ...]}}.
    Returns (mpd, pins) with pins reduced to the Periods in this manifest."""
    out, new_pins, last = [], {}, 0
    for m in _PERIOD_RE.finditer(mpd):
        blk = m.group(0)
        tag_m = _PERIOD_TAG_RE.match(blk)
        tag = tag_m.group(0)
        idm, stm = _PERIOD_ID_RE.search(tag), _PERIOD_STARTSTR_RE.search(tag)
        tmpls = _SEGTMPL_RE.findall(blk)
        cur = []
        for t in tmpls:
            po, ts = _PTO_RE.search(t), _TIMESCALE_RE.search(t)
            if not po or not ts:
                cur = None
                break
            cur.append([int(po.group(1)), int(ts.group(1))])
        if not idm or not stm or not cur:
            continue
        pid, start = idm.group(1), float(stm.group(1))
        pin = pins.get(pid)
        usable = bool(pin and len(pin['pto']) == len(cur))
        if usable:
            pstart = float(pin['start'][2:-1])
            usable = all(abs((pstart - po0 / ts0) - (start - po / ts)) <= _PIN_TOLERANCE_S
                         for (po0, ts0), (po, ts) in zip(pin['pto'], cur))
        if not usable:
            new_pins[pid] = {'start': f'PT{stm.group(1)}S', 'pto': cur}
            continue
        new_pins[pid] = pin
        if pin['start'] == f'PT{stm.group(1)}S' and pin['pto'] == cur:
            continue
        nt = _PERIOD_STARTSTR_RE.sub(lambda _m: f'start="{pin["start"]}"', tag, count=1)
        idx = iter(range(len(cur)))
        body = blk[tag_m.end():]

        def fix(tm):
            i = next(idx)
            return _PTO_RE.sub(f'presentationTimeOffset="{pin["pto"][i][0]}"', tm.group(0), count=1)
        body = _SEGTMPL_RE.sub(fix, body)
        out.append((m.start(), m.end(), nt + body))
    if not out:
        return mpd, new_pins
    res, pos = [], 0
    for a, b, text in out:
        res.append(mpd[pos:a])
        res.append(text)
        pos = b
    res.append(mpd[pos:])
    return ''.join(res), new_pins


def _pin_periods_cached(r, key: str, mpd: str) -> str:
    """pin_periods with the pins kept in redis per channel (shared by every
    gunicorn worker and player). Never breaks playback: any failure serves the
    manifest as fetched."""
    if r is None:
        return mpd
    try:
        pins = json.loads(r.get(key) or '{}')
        fixed, new_pins = pin_periods(mpd, pins)
        if new_pins != pins:
            r.set(key, json.dumps(new_pins), ex=6 * 3600)
        return fixed
    except Exception as exc:  # noqa: BLE001
        logger.warning('[hulu_live] period pinning skipped: %s', exc)
        return mpd


_PERIOD_SIG: dict[str, tuple] = {}   # per channel: last logged (session, period ids)
_ATTR_RES = {k: re.compile(rf'\b{k}="([^"]*)"') for k in
             ('id', 'start', 'duration', 'width', 'height', 'frameRate', 'codecs', 'cenc:default_KID')}


def _log_period_changes(key: str, url: str, mpd: str) -> None:
    """DEBUG only. Logs a one-line summary per Period (id, start, duration,
    own BaseURL, video size/fps/codec, default_KID) whenever the Period list or
    the stream session changes, not on every poll. For ad-break freezes: shows
    whether ads switch resolution, codec or key, and whether a re-resolve hands
    back a different Period layout."""
    try:
        periods = []
        for m in _PERIOD_RE.finditer(mpd):
            blk = m.group(0)
            tag = blk[:blk.index('>') + 1]
            get = lambda pat, text: (_ATTR_RES[pat].search(text) or [None, '-'])[1]  # noqa: E731
            video = re.search(r'<AdaptationSet\b[^>]*(?:contentType="video"|mimeType="video)[^>]*>.*?</AdaptationSet>',
                              blk, flags=re.DOTALL)
            vtxt = video.group(0) if video else blk
            reps = re.findall(r'<Representation\b[^>]*>', vtxt)
            top = max(reps, key=lambda r: int(get('height', r) if get('height', r).isdigit() else 0)) if reps else ''
            periods.append('%s start=%s dur=%s base=%s v=%sx%s@%s %s kid=%s' % (
                get('id', tag), get('start', tag), get('duration', tag),
                'own' if '<BaseURL' in blk else 'inherit',
                get('width', top), get('height', top), get('frameRate', top), get('codecs', top),
                (get('cenc:default_KID', blk) or '-')[:8]))
        sess = re.sub(r'token=\d+_[0-9a-f]+', 'token=*', url)[-60:]
        sig = (sess, tuple(p.split(' ', 1)[0] for p in periods))
        if _PERIOD_SIG.get(key) == sig:
            return
        prev = _PERIOD_SIG.get(key)
        _PERIOD_SIG[key] = sig
        note = '' if not prev else (' (new session)' if prev[0] != sess else '')
        logger.info('[hulu_live][debug] %s manifest: %d periods%s, url …%s', key, len(periods), note, sess)
        for line in periods[-8:]:
            logger.info('[hulu_live][debug] %s   %s', key, line)
    except Exception as e:  # noqa: BLE001
        logger.info('[hulu_live][debug] period summary failed: %s', e)


LOGO_SIZE = '400x400'
# The size Hulu's web app asks for, so nearly every image is already rendered.
PROGRAM_ART_SIZE = '600x600'


def art_url(url: str | None, size: str, fmt: str) -> str | None:
    """Hulu artwork URLs (img*.hulu.com/user/v3/artwork/...) answer 400 "No
    resize was requested" without an operations parameter."""
    if not url:
        return None
    ops = json.dumps([{'resize': f'{size}|max'}, {'format': fmt}], separators=(',', ':'))
    return f"{url}{'&' if '?' in url else '?'}operations={quote(ops, safe='')}"


def _real_eab(airing: dict) -> bool:
    """Off-air blocks carry a placeholder eab, EAB::null::null::null."""
    eab = airing.get('eab') or ''
    return bool(eab) and 'null' not in eab.split('::')[1:2]


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')


def _parse(val) -> datetime | None:
    if not val:
        return None
    try:
        return datetime.fromisoformat(str(val).replace('Z', '+00:00'))
    except ValueError:
        return None

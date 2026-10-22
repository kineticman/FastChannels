"""Paramount+ (source paramount) — live sports matches as event channels.

Scoped 2026-10-21 (dev/paramount/SCOPED.md). Paramount+'s live sports leagues
(UEFA, Serie A, EFL, UFC, rugby, …) are "discrete" live channels: each match
is its own stream, listed anonymously by the app API (live/channels.json,
channelTypes 'discrete'; live/channels/<slug>/listings.json per league).

Streams are Google DAI linear HLS encrypted with plain AES-128 (assetType
live_hls_aes), not DRM. The key URL (cbsi.live.ott.irdeto.com/licenseServer/
hls/v1/cbsi/key) needs `Authorization: Bearer <token>`. Paramount issues that
token, about 8 hours, in the web player's per-match metadata.json for a
signed-in session. Playback goes through /play/paramount/<id>/proxy.m3u8
(app/routes/paramount_proxy.py), which points the key lines at our own key
endpoint; segments load straight from the CDN.

Sign-in is the Paramount+ TV app's activation code: POST apps-api/v2.0/
androidtv/ott/auth/code.json → code; the user enters it at
paramountplus.com/activate/androidtv with a Paramount+ account or "Sign in
with partner" (TV provider); status.json then answers success and sets the
CBS_COM session cookie (1 year).

The saved session is checked for a valid sign-in. Paramount+ determines
match availability when playback metadata is requested.
"""
from __future__ import annotations

import base64
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone

import requests

from ..tve.adobe_pass import TVEAuthError
from .base import BaseScraper, ChannelData, ConfigField, ProgramData, StreamDeadError, EVENT_CHANNEL_TAG

logger = logging.getLogger(__name__)

_PPLUS = 'https://www.paramountplus.com'
_APPS_API = f'{_PPLUS}/apps-api'
_APP_TOKEN = 'ABCvvU1Pv0BRR9aWYFLAm+m8bcIJXm7a9GYpMwXFtDuq1P5ARAg6o60yilK8oQ2Eaxc='
_TV_UA = 'Paramount+/15.0 (androidtv)'
_WEB_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
           '(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36')
_IMAGES = 'https://wwwimage-us.pplusstatic.com/base/'
_THUMBS = 'https://wwwimage-us.pplusstatic.com/thumbnails/photos/w370-q80/'
ACTIVATE_URL = f'{_PPLUS}/activate/androidtv/'

EVENT_SCHEME = 'paramount://event/'
EVENT_CHANNEL_PREFIX = 'event:'
# Hosts the play proxy may fetch for this source.
KEY_HOST = 'cbsi.live.ott.irdeto.com'
PLAYLIST_HOSTS = ('dai.google.com',)

_EVENT_LOOKAHEAD_HOURS = 48
# Listings' stream window already pads the match by an hour or more.
EVENT_END_GRACE_SECONDS = 3600
_STATUS_TTL = 6 * 3600
_TOKEN_MARGIN = 10 * 60
_LINK_TIMEOUT = 600
_LINK_POLL_SECONDS = 5.0

LINK_STATUS_KEY = 'paramount:link:status'
SIGNIN_CONFIG_KEYS = ('cookies', 'user_id', 'signed_in_at', 'entitled', 'account_status', 'status_at')
_SIGNIN_LOST = 'The Paramount+ sign-in expired or was removed — sign in again.'

# contentId → (Authorization header value, expiry epoch). Per process; a miss
# costs one metadata.json fetch.
_KEY_TOKENS: dict[str, tuple[str, float]] = {}


class ParamountAuthError(TVEAuthError):
    pass


# ── account ──────────────────────────────────────────────────────────────────

def _app_params(**params) -> dict:
    return {'at': _APP_TOKEN, 'locale': 'en-us', **params}


def account_status(cookies: dict) -> dict:
    """Sign-in state for a saved session."""
    r = requests.get(f'{_APPS_API}/v3.0/androidtv/login/status.json', params=_app_params(),
                     cookies=cookies, headers={'User-Agent': _TV_UA}, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    return {'logged_in': bool(data.get('isLoggedIn')),
            'status': (data.get('userStatus') or {}).get('description'),
            'user_id': data.get('userId')}


def _signin_config(cookies: dict, status: dict) -> dict:
    now = int(time.time())
    return {'cookies': cookies, 'user_id': status.get('user_id'), 'signed_in_at': now,
            'account_status': status.get('status'), 'status_at': now}


# ── activation-code sign-in ──────────────────────────────────────────────────

def start_link(device_id: str) -> dict:
    r = requests.post(f'{_APPS_API}/v2.0/androidtv/ott/auth/code.json', params=_app_params(),
                      data={'deviceId': device_id}, headers={'User-Agent': _TV_UA}, timeout=20)
    data = (r.json() or {}) if r.ok else {}
    if not data.get('activationCode'):
        raise ParamountAuthError(f'Paramount+: no activation code (HTTP {r.status_code})')
    return {'code': data['activationCode'], 'device_token': data['deviceToken'], 'device_id': device_id}


def link_status(ctx: dict, state: str, message: str = '') -> dict:
    return {'state': state, 'message': message, 'code': ctx['code'], 'url': ACTIVATE_URL}


def run_link(ctx: dict) -> None:
    """RQ job: wait for the code to be entered, then save the session onto the
    paramount source and rescrape. Quits quietly once LINK_STATUS_KEY names a
    different code or is gone (cancelled)."""
    import redis

    from app.worker import flask_app
    from ..extensions import db
    from ..models import Source

    with flask_app.app_context():
        r = redis.from_url(flask_app.config['REDIS_URL'])

        def superseded() -> bool:
            raw = r.get(LINK_STATUS_KEY)
            return not raw or json.loads(raw).get('code') != ctx['code']

        def set_status(state: str, message: str = '') -> None:
            if not superseded():
                r.setex(LINK_STATUS_KEY, 720, json.dumps(link_status(ctx, state, message)))

        session = requests.Session()
        session.headers['User-Agent'] = _TV_UA
        deadline = time.monotonic() + _LINK_TIMEOUT
        cookies = None
        try:
            while time.monotonic() < deadline and not superseded():
                resp = session.get(f'{_APPS_API}/v2.0/androidtv/ott/auth/status.json', timeout=20,
                                   params=_app_params(activationCode=ctx['code'], deviceToken=ctx['device_token'],
                                                      deviceId=ctx['device_id']))
                if resp.ok and (resp.json() or {}).get('success'):
                    cookies = {c.name: c.value for c in session.cookies if c.name.startswith('CBS_')}
                    break
                time.sleep(_LINK_POLL_SECONDS)
            if cookies is not None and not cookies.get('CBS_COM'):
                raise ParamountAuthError('Paramount+ accepted the code but returned no session.')
            status = account_status(cookies) if cookies else None
            if status and not status['logged_in']:
                raise ParamountAuthError('Paramount+ accepted the code but the session is not signed in.')
        except Exception as exc:  # noqa: BLE001
            logger.warning('[paramount] sign-in failed: %s', exc)
            set_status('error', str(exc)[:300])
            return
        if not cookies:
            if superseded():
                logger.info('[paramount] activation code %s stopped', ctx['code'])
            else:
                set_status('expired', 'The code expired before the sign-in finished.')
            return

        src = Source.query.filter_by(name='paramount').first()
        if not src:
            set_status('error', 'Paramount+ source no longer exists.')
            return
        cfg = dict(src.config or {})
        cfg.pop('signin_error', None)
        cfg.pop('entitled', None)
        cfg.update(_signin_config(cookies, status))
        src.config = cfg
        db.session.commit()
        logger.info('[paramount] signed in (%s)', status.get('status'))
        set_status('success')
        if src.is_enabled:
            from ..routes.tasks import trigger_scrape
            trigger_scrape('paramount', force_full=True)


# ── helpers ──────────────────────────────────────────────────────────────────

def _epoch_ms(value) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value) / 1000, timezone.utc) if value else None
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _jwt_exp(header_value: str) -> float:
    try:
        payload = header_value.split()[-1].split('.')[1]
        payload += '=' * (-len(payload) % 4)
        return float(json.loads(base64.urlsafe_b64decode(payload)).get('exp') or 0)
    except (IndexError, ValueError, TypeError):
        return 0.0


def parse_event_url(raw_url: str) -> tuple[str, str, int | None]:
    """(contentId, league slug, stream end epoch) from an event stream_url."""
    m = re.match(r'paramount://event/([\w-]+)\?slug=([\w-]+)&end=(\d+)$', raw_url or '')
    if not m:
        raise ValueError(f'Unsupported Paramount+ stream URL: {raw_url}')
    return m.group(1), m.group(2), int(m.group(3))


# ── scraper ──────────────────────────────────────────────────────────────────

class ParamountScraper(BaseScraper):
    source_name = 'paramount'
    display_name = 'Paramount+'
    source_category = 'premium'
    is_premium = True
    is_beta = True
    scrape_interval = 360
    # Every channel is a match; its listing is the check (see audit_resolve).
    stream_audit_enabled = False

    config_required = True
    # Set by the activation-code sign-in, never typed; hidden from the card.
    config_schema = [
        ConfigField('cookies', 'Paramount+ session', secret=True, hidden=True),
    ]

    # ── auth ──

    def _cookies(self) -> dict:
        return self.config.get('cookies') or {}

    def _signed_in(self) -> bool:
        return bool(self._cookies().get('CBS_COM'))

    def _refresh_status(self) -> None:
        """Re-check the saved sign-in every 6 hours."""
        if not self._signed_in() or time.time() - float(self.config.get('status_at') or 0) < _STATUS_TTL:
            return
        status = account_status(self._cookies())
        if not status['logged_in']:
            self._signin_lost()
            return
        self._update_config('account_status', status.get('status'))
        self._update_config('status_at', int(time.time()))

    def _signin_lost(self) -> None:
        logger.warning('[paramount] %s', _SIGNIN_LOST)
        for key in SIGNIN_CONFIG_KEYS:
            self._update_config(key, None)
        self._update_config('signin_error', _SIGNIN_LOST)

    def pre_run_setup(self) -> None:
        self._refresh_status()

    # ── schedule ──

    def _get(self, path: str, **params) -> dict:
        r = self.session.get(f'{_APPS_API}/v3.0/androidphone/{path}', timeout=30,
                             params=_app_params(_clientRegion='US', **params),
                             headers={'User-Agent': _TV_UA})
        r.raise_for_status()
        return r.json() or {}

    def _leagues(self) -> list[dict]:
        return [c for c in self._get('live/channels.json', rows=200, start=0).get('channels') or []
                if 'discrete' in (c.get('channelTypes') or []) and c.get('slug')]

    def _events(self) -> list[tuple[dict, dict]]:
        """(league, listing) for every match whose stream window overlaps the next two days."""
        now = datetime.now(timezone.utc)
        horizon = now + timedelta(hours=_EVENT_LOOKAHEAD_HOURS)
        found: dict[str, tuple[dict, dict]] = {}
        for league in self._leagues():
            try:
                listing = self._get(f'live/channels/{league["slug"]}/listings.json', rows=100, start=0,
                                    showListing='true').get('listing') or []
            except requests.RequestException as exc:
                logger.warning('[paramount] %s listings unavailable: %s', league['slug'], exc)
                continue
            for item in listing:
                start = _epoch_ms(item.get('streamStartTimestamp') or item.get('startTimestamp'))
                end = _epoch_ms(item.get('streamEndTimestamp') or item.get('endTimestamp'))
                cid = item.get('videoContentId')
                if not cid or not start or not end or end <= start or end <= now or start >= horizon:
                    continue
                found.setdefault(cid, (league, item))
        return sorted(found.values(), key=lambda li: (li[1].get('startTimestamp') or 0, li[1]['videoContentId']))

    # ── channels / EPG ──

    def fetch_channels(self) -> list[ChannelData]:
        self._event_items = []
        self.hide_missing_event_channels = True
        if not self._signed_in():
            return []
        self._event_items = self._events()
        channels = []
        for league, item in self._event_items:
            cid = item['videoContentId']
            end = _epoch_ms(item.get('streamEndTimestamp') or item.get('endTimestamp'))
            thumb = (item.get('filePathThumb') or '').removeprefix('files/')
            logo = league.get('filePathLogoSelected') or league.get('filePathLogo')
            channels.append(ChannelData(
                source_channel_id=f'{EVENT_CHANNEL_PREFIX}{cid}',
                name=f'{league.get("channelName") or "Paramount+"} - {item.get("title") or "Live"}'[:256],
                stream_url=f'{EVENT_SCHEME}{cid}?slug={league["slug"]}&end={int(end.timestamp())}',
                logo_url=_THUMBS + thumb if thumb else (_IMAGES + logo if logo else None),
                category='Sports',
                stream_type='hls',
                guide_key=f'{EVENT_CHANNEL_PREFIX}{cid}',
                tags=[EVENT_CHANNEL_TAG, 'Paramount+ Sports'],
                gracenote_mode='off',
                retire_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS),
                delete_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS + 24 * 3600),
            ))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {ch.source_channel_id for ch in channels}
        programs = []
        for league, item in getattr(self, '_event_items', []):
            cid = f'{EVENT_CHANNEL_PREFIX}{item["videoContentId"]}'
            start = _epoch_ms(item.get('startTimestamp') or item.get('streamStartTimestamp'))
            end = _epoch_ms(item.get('endTimestamp') or item.get('streamEndTimestamp'))
            if cid not in wanted or not start or not end or end <= start:
                continue
            thumb = (item.get('filePathThumb') or '').removeprefix('files/')
            league_name = league.get('channelName')
            programs.append(ProgramData(
                source_channel_id=cid,
                title=item.get('title') or league_name or 'Paramount+',
                start_time=start,
                end_time=end,
                description=item.get('description') or None,
                poster_url=_THUMBS + thumb if thumb else None,
                category=';'.join(x for x in ('Sports Event', league_name) if x),
                episode_title=league_name if league_name != item.get('title') else None,
                is_live=True,
                episode_id=str(item.get('id') or item['videoContentId']),
            ))
        return programs

    # ── playback ──

    def _require_signed_in(self) -> None:
        if not self._signed_in():
            raise ParamountAuthError('Paramount+: sign in on the Sources page.')
        self._refresh_status()
        if not self._signed_in():
            raise ParamountAuthError(_SIGNIN_LOST)

    def _metadata(self, content_id: str, slug: str) -> dict:
        r = self.session.get(f'{_PPLUS}/live-tv/live-event/{slug}/{content_id}/metadata.json', timeout=20,
                             cookies=self._cookies(),
                             headers={'User-Agent': _WEB_UA, 'X-Requested-With': 'XMLHttpRequest'})
        r.raise_for_status()
        return r.json() or {}

    def _playback(self, content_id: str, slug: str) -> tuple[str, str | None]:
        """(Authorization header for the key server, DAI asset key) for a match.
        Callers must have passed _require_signed_in."""
        data = self._metadata(content_id, slug)
        aes = (((data.get('CONTENT') or {}).get('drm') or {}).get('aes') or {})
        auth = ((aes.get('header') or {}).get('Authorization') or '').strip()
        if not auth:
            raise RuntimeError('Paramount+: this match isn\'t streaming yet or isn\'t available to this account')
        _KEY_TOKENS[content_id] = (auth, _jwt_exp(auth) or time.time() + 3600)
        video = (data.get('listing') or {}).get('contentCANVideo') or {}
        return auth, video.get('daistreamKey') or (data.get('metaData') or {}).get('daistreamKey')

    def key_authorization(self, raw_url: str, force: bool = False) -> str:
        """The key server's Authorization header for this match, cached until near expiry."""
        content_id, slug, _ = parse_event_url(raw_url)
        self._require_signed_in()
        cached = _KEY_TOKENS.get(content_id)
        if not force and cached and time.time() < cached[1] - _TOKEN_MARGIN:
            return cached[0]
        return self._playback(content_id, slug)[0]

    def audit_resolve(self, raw_url: str) -> str:
        # An upcoming match has no stream yet; its place in the listings is the check.
        return raw_url

    def resolve(self, raw_url: str) -> str:
        """The match's DAI master playlist. Its key lines need the proxy
        (app/routes/paramount_proxy.py), so play() never redirects here directly."""
        content_id, slug, end = parse_event_url(raw_url)
        if end is not None and time.time() >= end + EVENT_END_GRACE_SECONDS:
            raise StreamDeadError('Paramount+ match has ended')
        self._require_signed_in()
        _, asset_key = self._playback(content_id, slug)
        if not asset_key:
            raise RuntimeError('Paramount+: no stream for this match')
        r = self.session.post(f'https://pubads.g.doubleclick.net/ssai/event/{asset_key}/streams', timeout=20,
                              headers={'Content-Type': 'application/x-www-form-urlencoded'})
        manifest = (r.json() or {}).get('stream_manifest') if r.ok else None
        if not manifest:
            raise RuntimeError(f'Paramount+: no stream for this match (HTTP {r.status_code})')
        return manifest

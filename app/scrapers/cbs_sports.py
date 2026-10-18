"""CBS (source cbs_sports) — the local CBS station, CBS Sports Network, CBS game windows and the free CBS Sports channels.

All streams are clear HLS (checked 2026-10-17):
  - CBS Sports HQ, Golazo and the Champions League channel are free Google DAI
    linear streams (POST pubads.g.doubleclick.net/ssai/event/<assetKey>/streams).
  - CBS Sports Network and CBS (the broadcast network, from the viewer's local
    affiliate) come from Syncbak through a TV-provider sign-in, up to 1080p.
    The CBS Sports app only carries the local CBS station during CBS Sports
    events — outside them the feed is a "game unavailable" slate — so the
    24/7 CBS channel comes from cbs.com's live TV instead (below).

TV-provider sign-in is the CBS Sports TV app's short code:
  GET video-api.cbssports.com/vms/shortcode/v1?deviceId=… → {code}; the user
  enters it at cbssports.com/androidtv/activate and signs in to their provider
  (Adobe Pass, requestor CBS_SPORTS); /shortcode/v1/status flips
  subscriptions.adobe from 'p' to 'y'. The sign-in itself lives at Adobe,
  keyed by our device id, and is read with Adobe's v1 legacy API — requests
  signed HMAC-SHA1 with the app's key pair, no client registration. It lasts
  about 90 days.

Entitlement: the Syncbak secure-URL request names only the provider and DMA —
it carries no Adobe token, and the URL it returns isn't bound to the viewer.
So every tune first asks Adobe to authorize the resource (CBS_SPORTS_NETWORK
or CBS_ENTERTAINMENT) for this sign-in, and only an authorized resource gets a
stream. Never call _syncbak_url without a passing _authorize.

The local CBS station 24/7 (added 2026-10-18): cbs.com/live-tv puts the
visitor's IP location in the page (CBS.Registry.geoLoc), /live-tv/dma/<lat>/
<lon> names the station, and /live-tv/channels/<callsign>/metadata.json with
mvpd_name returns a Syncbak URL (90-second token, clear 1080p HLS). That call
too only names the provider — cbs.com's own page authorizes CBS_ENTERTAINMENT
at Adobe first — so it is gated on _authorize exactly like _syncbak_url. Its
guide is Paramount+'s listings for the station's slug.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import re
import secrets
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests

from ..gracenote_map import resolve_gracenote
from ..tve.adobe_pass import TVEAuthError, TVENotAuthorizedError
from .base import BaseScraper, ChannelData, ConfigField, ProgramData, EVENT_CHANNEL_TAG

logger = logging.getLogger(__name__)

_UA = 'CBSSports/7.4.0-1739899507 (androidtv)'
_VIDEO_API = 'https://video-api.cbssports.com'
_API_KEY = 'lyR2U37Sie80c02cJMpOH4C3gJety4LO1WnLA1FO'
_ADOBE = 'https://api.auth.adobe.com'
_REQUESTOR = 'CBS_SPORTS'
_ADOBE_PRIVATE = 'wGxdacCKMS8tXnAS'
_ADOBE_PUBLIC = 'GF6qD5qat3lLw9ay8Ijg80b3NPH7cFEG'
_SYNCBAK_KEY = b'0efbe79d96f24f928d91f5fd8955d143'
_SYNCBAK_PUBLIC = '1b3c727ca1164a19851a102ea650e49d'
ACTIVATE_URL = 'https://www.cbssports.com/androidtv/activate/'

SCHEME = 'cbssports://network/'
EVENT_SCHEME = 'cbssports://event/'
EVENT_CHANNEL_PREFIX = 'event:'

# Adobe-gated networks: schedule network code → Adobe resource.
RESOURCES = {'CBSSN': 'CBS_SPORTS_NETWORK', 'CBSE': 'CBS_ENTERTAINMENT'}
# 24/7 channels. 'dai' ones are free; 'resource' ones need the sign-in.
NETWORKS = {
    'CBSSN': {'name': 'CBS Sports Network', 'gracenote': '59250', 'resource': 'CBS_SPORTS_NETWORK',
              'logo': 'https://tmsimg.fancybits.co/assets/s59250_ll_h15_aa.png'},
    'CBSSHQ': {'name': 'CBS Sports HQ', 'gracenote': '108919', 'dai': '9Lq0ERvoSR-z9AwvFS-xYA',
               'pplus': 'cbssportshq', 'logo': 'https://tmsimg.fancybits.co/assets/s108919_ll_h15_aa.png'},
    'CBSSGOLAZO': {'name': 'CBS Sports Golazo Network', 'gracenote': '133691', 'dai': '7f3Wv6f7QEKfQna22jHqLQ',
                   'pplus': 'golazo', 'logo': 'https://tmsimg.fancybits.co/assets/s133691_ll_h15_aa.png'},
    'CBSCHAMPIONSLEAGUE': {'name': 'CBS Sports Champions League', 'dai': 'ydKcHHYQSt27vbSP38xMVw'},
}
EVENT_LABELS = {'CBSE': 'CBS', 'CBSSN': 'CBS Sports Network'}
# The viewer's local CBS station: a 24/7 channel under the CBSE code, so CBS
# games fold into it the way CBSSN's do into CBS Sports Network.
LOCAL_CODE = 'CBSE'
_CBS_SITE = 'https://www.cbs.com'
_CBS_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
_PPLUS_IMAGES = 'https://wwwimage-us.pplusstatic.com/base/'

# HQ and Golazo's full program grids come from Paramount+'s live-channel
# listings (CBS Sports' own API only lists events). Anonymous with the
# Paramount+ app token; one call covers ~4-6 days (checked 2026-10-17).
_PPLUS_LISTINGS = 'https://www.paramountplus.com/apps-api/v3.0/androidphone/live/channels/{slug}/listings.json'
_PPLUS_TOKEN = 'ABCvvU1Pv0BRR9aWYFLAm+m8bcIJXm7a9GYpMwXFtDuq1P5ARAg6o60yilK8oQ2Eaxc='
_PPLUS_THUMBS = 'https://wwwimage-us.pplusstatic.com/thumbnails/photos/w370-q80/'

_SCHEDULE_TTL = 10 * 60
_AUTHZ_MARGIN = 10 * 60
_EVENT_LOOKAHEAD_HOURS = 48
EVENT_END_GRACE_SECONDS = 2 * 3600
_LINK_TIMEOUT = 600
_LINK_POLL_SECONDS = 3.0

LINK_STATUS_KEY = 'cbs_sports:link:status'
SIGNIN_CONFIG_KEYS = ('adobe_user_id', 'adobe_mvpd', 'adobe_expires_at', 'signed_in_at', 'authz')
_SIGNIN_LOST = 'The TV-provider sign-in expired or was removed — sign in again.'


class CBSAuthError(TVEAuthError):
    pass


# ── Adobe Pass v1 (legacy signed requests) ───────────────────────────────────

def _adobe_header(method: str, path: str) -> str:
    message = (f'{method} requestor_id={_REQUESTOR}, nonce={secrets.token_hex(16)}, '
               f'signature_method=HMAC-SHA1, request_time={int(time.time() * 1000)}, request_uri={path}')
    signature = base64.b64encode(hmac.new(_ADOBE_PRIVATE.encode(), message.encode(), hashlib.sha1).digest()).decode()
    return f'{message}, public_key={_ADOBE_PUBLIC}, signature={signature}'


def _adobe_get(endpoint: str, sig_path: str, device_id: str, **params) -> requests.Response:
    return requests.get(f'{_ADOBE}/api/v1/{endpoint}', timeout=20,
                        params={'requestor': _REQUESTOR, 'deviceId': device_id, **params},
                        headers={'Authorization': _adobe_header('GET', sig_path),
                                 'User-Agent': _UA, 'Accept': 'application/json'})


def adobe_authn(device_id: str) -> dict | None:
    """The provider sign-in Adobe holds for this device, or None if there isn't one."""
    r = _adobe_get('tokens/authn', '/authn', device_id, deviceType='androidtv')
    if r.status_code in (404, 410, 412):
        return None
    if not r.ok:
        raise CBSAuthError(f'CBS Sports: Adobe sign-in check failed (HTTP {r.status_code})')
    data = r.json() or {}
    return data if data.get('userId') else None


def adobe_logout(device_id: str) -> None:
    requests.delete(f'{_ADOBE}/api/v1/logout', timeout=20,
                    params={'requestor': _REQUESTOR, 'deviceId': device_id},
                    headers={'Authorization': _adobe_header('DELETE', '/logout'), 'User-Agent': _UA})


def _signin_config(authn: dict) -> dict:
    return {'adobe_user_id': authn['userId'], 'adobe_mvpd': authn.get('mvpd'),
            'adobe_expires_at': int(int(authn.get('expires') or 0) / 1000) or None,
            'signed_in_at': int(time.time())}


# ── short-code sign-in ───────────────────────────────────────────────────────

def _video_api(path: str, **params) -> dict:
    r = requests.get(f'{_VIDEO_API}{path}', params=params, timeout=20,
                     headers={'User-Agent': _UA, 'x-api-key': _API_KEY, 'Content-Type': 'application/json'})
    if not r.ok:
        raise CBSAuthError(f'CBS Sports {path}: HTTP {r.status_code}')
    return r.json() or {}


def start_link(device_id: str) -> dict:
    code = _video_api('/vms/shortcode/v1', deviceId=device_id, deviceType='androidtv',
                      authTypes='adobe', currentSubscriptions='').get('code')
    if not code:
        raise CBSAuthError('CBS Sports: no activation code')
    return {'code': code, 'device_id': device_id}


def link_status(ctx: dict, state: str, message: str = '') -> dict:
    return {'state': state, 'message': message, 'code': ctx['code'], 'url': ACTIVATE_URL}


def run_link(ctx: dict) -> None:
    """RQ job: wait for the code to be entered and the provider sign-in to land,
    then save it onto the cbs_sports source and rescrape. Quits quietly once
    LINK_STATUS_KEY names a different code or is gone (cancelled)."""
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

        deadline = time.monotonic() + _LINK_TIMEOUT
        authn = None
        try:
            while time.monotonic() < deadline and not superseded():
                status = _video_api('/vms/shortcode/v1/status', shortcode=ctx['code'])
                if (status.get('subscriptions') or {}).get('adobe') == 'y':
                    authn = adobe_authn(ctx['device_id'])
                    if authn:
                        break
                time.sleep(_LINK_POLL_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[cbs_sports] sign-in failed: %s', exc)
            set_status('error', str(exc)[:300])
            return
        if not authn:
            if superseded():
                logger.info('[cbs_sports] activation code %s stopped', ctx['code'])
            else:
                set_status('expired', 'The code expired before the sign-in finished.')
            return

        src = Source.query.filter_by(name='cbs_sports').first()
        if not src:
            set_status('error', 'CBS Sports source no longer exists.')
            return
        cfg = dict(src.config or {})
        cfg['device_id'] = ctx['device_id']
        cfg.pop('authz', None)
        cfg.pop('signin_error', None)
        cfg.update(_signin_config(authn))
        src.config = cfg
        db.session.commit()
        logger.info('[cbs_sports] signed in with %s', authn.get('mvpd'))
        set_status('success')
        if src.is_enabled:
            from ..routes.tasks import trigger_scrape
            trigger_scrape('cbs_sports', force_full=True)


# ── helpers ──────────────────────────────────────────────────────────────────

def _epoch(value) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value), timezone.utc) if value else None
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _event_window(event: dict) -> tuple[datetime | None, datetime | None]:
    schedule = (event.get('video') or {}).get('schedule') or {}
    return _epoch(schedule.get('videoStartDate')), _epoch(schedule.get('videoEndDate'))


def event_end_timestamp(raw_url: str | None) -> int | None:
    m = re.search(r'[?&]end=(\d+)', raw_url or '')
    return int(m.group(1)) if m else None


def _image(about: dict) -> str | None:
    images = about.get('images') or {}
    return images.get('baseImage16X9') or images.get('baseImage2x3') or images.get('baseImage16X5')


# ── scraper ──────────────────────────────────────────────────────────────────

class CBSSportsScraper(BaseScraper):
    source_name = 'cbs_sports'
    display_name = 'CBS'
    source_category = 'premium'
    is_premium = True
    scrape_interval = 360
    LAZY_CACHE_KEYS = frozenset({'cbs_events'})

    config_schema = [
        ConfigField('include_events', 'Include game channels', field_type='toggle', default='false',
                    help_text='Adds CBS and CBS Sports Network games your TV provider includes as temporary channels.'),
    ]

    # ── auth ──

    def _device_id(self) -> str:
        if not self.config.get('device_id'):
            self._update_config('device_id', str(uuid.uuid4()))
        return self.config['device_id']

    def _signed_in(self) -> bool:
        return bool(self.config.get('adobe_user_id'))

    def _authorize(self, resource: str, force: bool = False) -> bool:
        """Whether Adobe authorizes `resource` for this sign-in. Cached until the
        authorization's own expiry; a denial is cached for an hour."""
        if not self._signed_in():
            return False
        authz = dict(self.config.get('authz') or {})
        cached = authz.get(resource) or {}
        if not force and time.time() < float(cached.get('until') or 0):
            return bool(cached.get('ok'))
        r = _adobe_get('authorize', '/authorize', self._device_id(), resource=resource)
        if r.status_code == 403:
            ok, until = False, time.time() + 3600
        elif r.status_code in (401, 404, 410, 412):
            # The provider sign-in is gone or expired at Adobe.
            self._signin_lost()
            raise CBSAuthError(_SIGNIN_LOST)
        elif r.ok:
            data = r.json() or {}
            expires = int(data.get('expires') or 0) / 1000 or time.time() + 3600
            ok, until = True, expires - _AUTHZ_MARGIN
        else:
            raise CBSAuthError(f'CBS Sports: Adobe authorization failed (HTTP {r.status_code})')
        authz[resource] = {'ok': ok, 'until': until}
        self._update_config('authz', authz)
        return ok

    def _dma(self) -> str:
        if not self.config.get('dma') or time.time() - float(self.config.get('dma_at') or 0) > 86400:
            r = requests.get('https://video-api-geo.cbssports.com/', timeout=20,
                             headers={'User-Agent': _UA, 'x-api-key': _API_KEY})
            dma = str((r.json() or {}).get('dmaId') or '') if r.ok else ''
            if dma:
                self._update_config('dma', dma)
                self._update_config('dma_at', int(time.time()))
        return self.config.get('dma') or ''

    def _station(self) -> dict | None:
        """The local CBS station for this server's location (cbs.com's own
        IP lookup): {call_sign, lat, lon, dma_name, logo, gracenote}. Daily."""
        cached = self.config.get('cbs_station') or {}
        if cached.get('call_sign') and time.time() - float(cached.get('at') or 0) < 86400:
            return cached
        try:
            headers = {'User-Agent': _CBS_UA}
            html = self.session.get(f'{_CBS_SITE}/live-tv/stream/', headers=headers, timeout=20).text
            geo = json.loads(re.search(r'CBS\.Registry\.geoLoc\s*=\s*(\{[^}]*\})', html).group(1))
            r = self.session.get(f'{_CBS_SITE}/live-tv/dma/{geo["lat"]}/{geo["lng"]}', headers=headers, timeout=20)
            dma = ((r.json() or {}).get('result') or {}).get('dmas') or []
            if not dma or not dma[0].get('callSign'):
                raise ValueError('cbs.com has no CBS station for this location')
            call = dma[0]['callSign'].upper()
            station = {'call_sign': call, 'lat': geo['lat'], 'lon': geo['lng'],
                       'dma_name': dma[0].get('dmaName'), 'at': time.time()}
            r = self.session.get(_PPLUS_LISTINGS.format(slug=call.lower()), timeout=20, params={
                'at': _PPLUS_TOKEN, 'locale': 'en-us', 'rows': 1, 'start': 0,
                'showListing': 'true', '_clientRegion': 'US'})
            channel = next(iter((r.json() or {}).get('channel') or []), {}) if r.ok else {}
            if channel.get('filePathLogoSelected'):
                station['logo'] = _PPLUS_IMAGES + channel['filePathLogoSelected']
            if channel.get('prgSvcId'):
                station['gracenote'] = str(channel['prgSvcId'])
        except (requests.RequestException, ValueError, AttributeError, KeyError) as exc:
            logger.warning('[cbs_sports] could not find the local CBS station: %s', exc)
            return cached or None
        self._update_config('cbs_station', station)
        return station

    def pre_run_setup(self) -> None:
        self._dma()
        if self._signed_in() and not adobe_authn(self._device_id()):
            self._signin_lost()

    def _signin_lost(self) -> None:
        """Forget a sign-in Adobe no longer has; the card shows why."""
        logger.warning('[cbs_sports] %s', _SIGNIN_LOST)
        for key in SIGNIN_CONFIG_KEYS:
            self._update_config(key, None)
        self._update_config('signin_error', _SIGNIN_LOST)

    # ── schedule ──

    def _events(self) -> list[dict]:
        cached = self.load_lazy_cache_key('cbs_events') or {}
        if cached.get('items') is not None and time.time() - float(cached.get('at') or 0) < _SCHEDULE_TTL:
            return cached['items']
        r = self.session.get(f'{_VIDEO_API}/vms/events/v5/', timeout=30,
                             params={'device': 'firetv', 'transform': 'ottv5', 'dma': self._dma()},
                             headers={'User-Agent': _UA, 'x-api-key': _API_KEY})
        r.raise_for_status()
        items = [e for e in (r.json() or []) if e and e.get('video')]
        self._update_cache('cbs_events', {'at': time.time(), 'items': items})
        return items

    def _games(self, authorized: set[str], local: bool = False) -> list[dict]:
        """Adobe-gated events in the next two days on a network this sign-in can
        play. CBS Sports Network's events are left to its 24/7 channel, and
        CBS's to the local station when there is one."""
        linear = authorized_linear(authorized) | ({LOCAL_CODE} if local else set())
        now = datetime.now(timezone.utc)
        horizon = now + timedelta(hours=_EVENT_LOOKAHEAD_HOURS)
        grace = timedelta(seconds=EVENT_END_GRACE_SECONDS)
        found: dict[tuple, dict] = {}
        for event in self._events():
            video = event['video']
            network = video.get('network')
            if (network not in RESOURCES or network in linear
                    or 'adobe' not in (video.get('authentication') or [])
                    or (video.get('properties') or {}).get('type') != 'live'
                    or RESOURCES[network] not in authorized):
                continue
            start, end = _event_window(event)
            if not start or not end or end <= start or end + grace <= now or start >= horizon:
                continue
            # The same game is often listed twice ("FAU vs. Army" / "Florida
            # Atlantic at Army"); keep the one that names its sport.
            key = (network, start)
            if key not in found or (not (found[key]['video'].get('properties') or {}).get('sport')
                                    and (video.get('properties') or {}).get('sport')):
                found[key] = event
        return sorted(found.values(), key=lambda e: (_event_window(e)[0], e['id']))

    def _events_enabled(self) -> bool:
        return str(self.config.get('include_events', 'false')).strip().lower() in ('1', 'true', 'yes', 'on')

    # ── channels / EPG ──

    def fetch_channels(self) -> list[ChannelData]:
        authorized = {res for res in RESOURCES.values() if self._authorize(res)}
        channels = []
        station = self._station() if 'CBS_ENTERTAINMENT' in authorized else None
        if station:
            call = station['call_sign']
            channels.append(ChannelData(
                source_channel_id=LOCAL_CODE,
                name=f'CBS ({call})',
                stream_url=SCHEME + LOCAL_CODE,
                logo_url=station.get('logo'),
                category='Entertainment',
                stream_type='hls',
                guide_key=f'CBS:{call}',
                gracenote_id=resolve_gracenote('cbs_sports', upstream_id=station.get('gracenote'), lookup_key=call),
            ))
        for code, meta in NETWORKS.items():
            if meta.get('resource') and meta['resource'] not in authorized:
                continue
            channels.append(ChannelData(
                source_channel_id=code,
                name=meta['name'],
                stream_url=SCHEME + code,
                logo_url=meta.get('logo'),
                category='Sports',
                stream_type='hls',
                guide_key=code,
                gracenote_id=resolve_gracenote('cbs_sports', upstream_id=meta.get('gracenote'), lookup_key=code),
            ))
        self._game_items = []
        if self._events_enabled():
            self._game_items = self._games(authorized, local=bool(station))
            self.hide_missing_event_channels = True
            for event in self._game_items:
                video = event['video']
                start, end = _event_window(event)
                title = (video.get('about') or {}).get('title') or 'CBS Sports'
                network = video['network']
                channels.append(ChannelData(
                    source_channel_id=f'{EVENT_CHANNEL_PREFIX}{event["id"]}',
                    name=f'{EVENT_LABELS[network]} - {title}'[:256],
                    stream_url=f'{EVENT_SCHEME}{event["id"]}?net={network}&end={int(end.timestamp())}',
                    logo_url=_image(video.get('about') or {}),
                    category='Sports',
                    stream_type='hls',
                    guide_key=f'{EVENT_CHANNEL_PREFIX}{event["id"]}',
                    tags=[EVENT_CHANNEL_TAG, 'CBS Sports Games'],
                    gracenote_mode='off',
                    retire_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS),
                    delete_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS + 24 * 3600),
                ))
        return channels

    def _pplus_programs(self, code: str, slug: str, category: str | None = 'Sports') -> list[ProgramData]:
        r = self.session.get(_PPLUS_LISTINGS.format(slug=slug), timeout=30, params={
            'at': _PPLUS_TOKEN, 'locale': 'en-us', 'rows': 500, 'start': 0,
            'showListing': 'true', '_clientRegion': 'US'})
        r.raise_for_status()
        programs = []
        for item in (r.json() or {}).get('listing') or []:
            start, end = _epoch((item.get('startTimestamp') or 0) // 1000), _epoch((item.get('endTimestamp') or 0) // 1000)
            if not start or not end or end <= start:
                continue
            thumb = (item.get('filePathThumb') or '').removeprefix('files/')
            title = (item.get('title') or '').strip() or (NETWORKS.get(code) or {}).get('name') or 'CBS'
            episode = (item.get('episodeTitle') or '').strip()
            programs.append(ProgramData(
                source_channel_id=code,
                title=title,
                start_time=start,
                end_time=end,
                description=item.get('description') or None,
                poster_url=_PPLUS_THUMBS + thumb if thumb else None,
                category=category,
                episode_title=episode if episode and episode != title else None,
                episode_id=item.get('tmsProgramId') or None,
            ))
        return programs

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {ch.source_channel_id for ch in channels}
        programs = []
        # Channels with a full Paramount+ grid skip the event list below.
        gridded: set[str] = set()
        station = self.config.get('cbs_station') or {}
        if LOCAL_CODE in wanted and station.get('call_sign'):
            try:
                grid = self._pplus_programs(LOCAL_CODE, station['call_sign'].lower(), category=None)
            except Exception as exc:  # noqa: BLE001
                logger.warning('[cbs_sports] %s listings unavailable: %s', station['call_sign'], exc)
                grid = []
            if grid:
                programs.extend(grid)
                gridded.add(LOCAL_CODE)
        for code, meta in NETWORKS.items():
            if code in wanted and meta.get('pplus'):
                try:
                    grid = self._pplus_programs(code, meta['pplus'])
                except Exception as exc:  # noqa: BLE001
                    logger.warning('[cbs_sports] %s listings unavailable, using events: %s', meta['name'], exc)
                    continue
                if grid:
                    programs.extend(grid)
                    gridded.add(code)
        try:
            events = self._events()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[cbs_sports] schedule unavailable: %s', exc)
            return programs
        seen: set[tuple] = set()
        game_ids = {e['id'] for e in getattr(self, '_game_items', [])}
        for event in events:
            video = event['video']
            if (video.get('properties') or {}).get('type') != 'live':
                continue  # 'live-linear' entries are the channels themselves
            cid = f'{EVENT_CHANNEL_PREFIX}{event["id"]}' if event['id'] in game_ids else video.get('network')
            start, end = _event_window(event)
            if cid in gridded or cid not in wanted or not start or not end or end <= start or (cid, start) in seen:
                continue
            seen.add((cid, start))
            about = video.get('about') or {}
            props = video.get('properties') or {}
            sport = props.get('sport')
            programs.append(ProgramData(
                source_channel_id=cid,
                title=about.get('title') or 'CBS Sports',
                start_time=start,
                end_time=end,
                description=about.get('description') or None,
                poster_url=_image(about),
                category=';'.join(x for x in ('Sports Event', sport, props.get('leagueDisplayName')) if x),
                episode_title=props.get('leagueDisplayName') if props.get('leagueDisplayName') != about.get('title') else None,
                is_live=True,
                episode_id=str(event['id']),
            ))
        return programs

    # ── playback ──

    def _syncbak_url(self, resource: str) -> str:
        """Stream for an Adobe-gated network. Callers must have authorized `resource`."""
        token = hmac.new(_SYNCBAK_KEY, f'{int(time.time())}{_SYNCBAK_PUBLIC}'.encode(), hashlib.sha1).hexdigest()
        url = (f'https://www.cbssports.com/api/content/video/syncbak/get-secure-url/{_SYNCBAK_PUBLIC}/{token}/'
               f'{resource}/{self.config["adobe_mvpd"]}/8/{self._dma()}/')
        r = self.session.get(url, params={'as': 'json', 'version': 4}, timeout=20, headers={'User-Agent': _UA})
        stream = (r.json() or {}).get('SUCCESS') if r.ok else None
        if not stream or not isinstance(stream, str):
            raise RuntimeError(f'CBS Sports: no stream for {resource} (HTTP {r.status_code})')
        return stream

    def _local_url(self) -> str:
        """The local CBS station 24/7. Callers must have authorized CBS_ENTERTAINMENT."""
        station = self._station()
        if not station:
            raise RuntimeError('CBS Sports: cbs.com has no CBS station for this location')
        r = self.session.get(f'{_CBS_SITE}/live-tv/channels/{station["call_sign"].lower()}/metadata.json',
                             params={'lat': station['lat'], 'lon': station['lon'],
                                     'mvpd_name': self.config['adobe_mvpd']},
                             headers={'User-Agent': _CBS_UA}, timeout=20)
        data = (r.json() or {}) if r.ok else {}
        if not data.get('success') or not data.get('assetURL'):
            raise RuntimeError(f'CBS Sports: no stream for {station["call_sign"]} (HTTP {r.status_code})')
        return data['assetURL']

    def _gated_stream(self, resource: str, label: str, local: bool = False) -> str:
        if not self._signed_in():
            raise CBSAuthError('CBS Sports: sign in with your TV provider on the Sources page.')
        if not self._authorize(resource):
            raise TVENotAuthorizedError(f'CBS Sports: your TV package does not include {label}')
        return self._local_url() if local else self._syncbak_url(resource)

    def resolve(self, raw_url: str) -> str:
        if raw_url.startswith(EVENT_SCHEME):
            m = re.search(r'[?&]net=(\w+)', raw_url)
            network = m.group(1) if m else ''
            if network not in RESOURCES:
                raise ValueError(f'Unsupported CBS Sports event URL: {raw_url}')
            end = event_end_timestamp(raw_url)
            if end is not None and time.time() >= end + EVENT_END_GRACE_SECONDS:
                from .base import StreamDeadError
                raise StreamDeadError('CBS Sports event has ended')
            return self._gated_stream(RESOURCES[network], EVENT_LABELS[network])
        code = raw_url.removeprefix(SCHEME)
        if code == LOCAL_CODE:
            return self._gated_stream('CBS_ENTERTAINMENT', 'CBS', local=True)
        meta = NETWORKS.get(code)
        if not meta:
            raise ValueError(f'Unsupported CBS Sports stream URL: {raw_url}')
        if meta.get('resource'):
            return self._gated_stream(meta['resource'], meta['name'])
        r = self.session.post(f'https://pubads.g.doubleclick.net/ssai/event/{meta["dai"]}/streams', timeout=20,
                              headers={'Content-Type': 'application/x-www-form-urlencoded'})
        manifest = (r.json() or {}).get('stream_manifest') if r.ok else None
        if not manifest:
            raise RuntimeError(f'CBS Sports: no stream for {meta["name"]} (HTTP {r.status_code})')
        return manifest


def authorized_linear(authorized: set[str]) -> set[str]:
    """Event networks whose events already air on an authorized 24/7 channel."""
    return {code for code, meta in NETWORKS.items() if meta.get('resource') in authorized}

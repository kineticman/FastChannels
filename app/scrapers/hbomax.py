"""
hbomax.py — HBO Max live sports events (TBS / TNT / truTV) via a direct
subscription sign-in.

Scoped and proven 2026-11-17; see dev/hbomax/BUILD_PLAN.md and the REA doc
(dev/hbomax/HBO_Max_AndroidTV_7.14.0.69_REA.md) for the full trace.

Key facts:
- Max runs on WBD's "Bolt" platform, the same stack as cnn_tve.py. The web API
  host is *.api.hbomax.com (homemarket default.any-amer.prd.api.hbomax.com); the
  Widevine license proxy is on *.api.discomax.com.
- The web client authenticates by COOKIE (an `st` JWT), with no Authorization
  header and no HMAC. The raw password POST is Arkose-gated, so the sign-in is a
  headless Camoufox web login (hbomax_login.py) that harvests the cookie jar —
  the same cookie-jar pattern as the Xfinity TVE source. The Android device-code
  flow is HMAC-signed and intentionally un-reproducible, so it is not used.
- Live content: GET /cms/routes/sport-schedule (JSON:API) lists LIVE `video`
  items, each with an `edit` relationship (the editId) and a `primaryChannel`
  (TBS/TNT/truTV). These are modelled as temporary event channels.
- Playback: POST /any/playback/v1/playbackInfo with the editId → Widevine DASH
  (dash-cmaf-cenc) and a pre-signed license URL. Bridge-only (DRM), default-off.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, EVENT_CHANNEL_TAG, infer_language
from ..tve.adobe_pass import TVEAuthError

logger = logging.getLogger(__name__)

SCHEME = 'hbomax://edit/'
UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36')
SITE = 'https://www.hbomax.com'

# Bolt hosts (web).
ANON = 'https://default.any-any.prd.api.hbomax.com'    # anonymous token, bootstrap, playbackInfo
HOME = 'https://default.any-amer.prd.api.hbomax.com'   # homemarket: cms routes/collections
# Any Bolt license URL; each tune's real pre-signed one comes from playbackInfo.
LICENSE_BASE = 'https://busy.any-any.prd.api.discomax.com/drm-proxy/any/drm-proxy/drm/license/widevine'

# Web client identity (from dev/hbomax/1.har).
CLIENT_VERSION = '22.0.1'
PUBLIC_CLIENT_ID = 'da0cdd94-5a39-42ef-aa68-54cbc1b852c3'

# Live events are discovered from these routes and merged (deduped by editId):
# `home` is the richest surface (reaches ~2 weeks out) and `sport-schedule` is the
# dedicated schedule page — each occasionally carries an event the other omits.
# (`live-sports-schedule` is a dead route; the `sport/<id>` hub id is session-bound.)
SCHEDULE_ROUTE = '/cms/routes/sport-schedule'
_EVENT_ROUTES = ('/cms/routes/home', SCHEDULE_ROUTE)
_ROUTE_PARAMS = {'include': 'default',
                 'decorators': 'viewingHistory,isFavorite,contentAction,badges'}

EVENT_END_GRACE_SECONDS = 2 * 3600          # keep the channel this long past scheduled end
_EVENT_LOOKAHEAD_HOURS = 48                  # list events airing now or starting within this window
_EVENT_PREFIX = 'event-'
_SIGN_IN_MSG = ('HBO Max: not signed in yet — add your email and password and use '
                '"Sign in" for HBO Max under Settings.')


class HBOMaxScraper(BaseScraper):
    """HBO Max live sports events (TBS / TNT / truTV) via a direct subscription."""

    source_name = 'hbomax'
    added_in = '6.0.0'
    display_name = 'HBO Max'
    source_category = 'premium'
    is_premium = True
    scrape_interval = 120
    # Every channel is a time-boxed event; an upcoming one has no stream to audit.
    stream_audit_enabled = False
    license_url = LICENSE_BASE
    all_channels_require_drm_bridge = True
    config_required = True

    # Email/password are typed; the cookie jar + device id are written by the
    # Camoufox sign-in (hbomax_login.py) and hidden from the card.
    config_schema = [
        ConfigField('email', 'HBO Max email', placeholder='you@example.com'),
        ConfigField('password', 'HBO Max password', field_type='password', secret=True),
        ConfigField('cookie_jar', 'HBO Max session', secret=True, hidden=True),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({'User-Agent': UA})
        self.hide_missing_event_channels = True

    # ── auth / session ───────────────────────────────────────────────────────

    def _signed_in(self) -> bool:
        jar = self.config.get('cookie_jar') or {}
        return bool(jar.get('cookies'))

    def _device_id(self) -> str:
        jar = self.config.get('cookie_jar') or {}
        return jar.get('device_id') or str(uuid.uuid5(uuid.NAMESPACE_DNS, 'fc-hbomax'))

    def _cookie_header(self) -> str:
        jar = (self.config.get('cookie_jar') or {}).get('cookies') or []
        return '; '.join(f"{c['name']}={c['value']}" for c in jar if c.get('name'))

    def _headers(self, extra: dict | None = None) -> dict:
        dev = self._device_id()
        h = {
            'Origin': SITE, 'Referer': SITE + '/',
            'Accept-Language': 'en-US,en',
            'x-device-info': (f'hbomax/{CLIENT_VERSION} (desktop/desktop; Windows/NT 10.0; '
                              f'{dev}/{PUBLIC_CLIENT_ID})'),
            'x-disco-client': f'WEB:NT 10.0:hbomax:{CLIENT_VERSION}',
            'x-disco-params': 'realm=bolt,bid=beam,features=ar',
            'x-wbd-preferred-language': 'en-US,en',
        }
        cookie = self._cookie_header()
        if cookie:
            h['Cookie'] = cookie
        if extra:
            h.update(extra)
        return h

    def _bootstrap_state(self) -> str:
        """A fresh x-wbd-session-state for this cookie session (playbackInfo wants
        one), cached briefly in the source cache."""
        cached = self.cache.get('hbomax_state') or {}
        if cached.get('state') and time.time() - float(cached.get('at') or 0) < 600:
            return cached['state']
        r = self.session.post(f'{ANON}/session-context/headwaiter/v1/bootstrap',
                              headers=self._headers(), json={}, timeout=20)
        state = r.headers.get('x-wbd-session-state') or ''
        self._update_cache('hbomax_state', {'state': state, 'at': time.time()})
        return state

    def _sign_in_needed(self):
        from ..tve.signin_notice import mark_signin_needed
        mark_signin_needed('hbomax', _SIGN_IN_MSG)
        from ..tve.adobe_pass import TVEAuthError
        return TVEAuthError(_SIGN_IN_MSG)

    # ── channels / EPG ───────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        self.excluded_channel_ids = set()
        if not self._signed_in():
            logger.info('[hbomax] not signed in; no channels')
            return []
        events = self._live_events()
        out = []
        now = datetime.now(timezone.utc)
        for ev in events:
            end = ev['end'] or (now + timedelta(hours=4))
            title = ev['title']
            chan = ev['channel'] or 'HBO Max'
            out.append(ChannelData(
                source_channel_id=f'{_EVENT_PREFIX}{ev["edit_id"]}',
                name=f'{chan} - {title}'[:256],
                slug=f'hbomax-{ev["edit_id"]}',
                logo_url=ev.get('logo'),
                stream_url=f'{SCHEME}{ev["edit_id"]}',
                stream_type='dash',
                category='Sports',
                language=infer_language(title),
                country='US',
                guide_key=f'{_EVENT_PREFIX}{ev["edit_id"]}',
                tags=[EVENT_CHANNEL_TAG, 'HBO Max Sports'],
                sport=ev.get('sport'),
                league=ev.get('league'),
                gracenote_mode='off',
                retire_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS),
                delete_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS + 24 * 3600),
            ))
        # Optional VOD smoke-test channel (config `test_vod_edit`): a playable,
        # always-on asset for exercising the DRM bridge when nothing is live.
        test_edit = (self.config.get('test_vod_edit') or '').strip()
        if test_edit:
            out.append(ChannelData(
                source_channel_id=f'{_EVENT_PREFIX}{test_edit}',
                name='HBO Max VOD Test',
                slug=f'hbomax-test-{test_edit}',
                stream_url=f'{SCHEME}{test_edit}',
                stream_type='dash',
                category='Movies',
                language='en',
                country='US',
                gracenote_mode='off',
            ))
            logger.info('[hbomax] + VOD test channel (%s)', test_edit)
        logger.info('[hbomax] %d live event channels', len(out))
        return out

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        programs = []
        by_edit = {c.source_channel_id: c for c in channels}
        # EPG-only runs skip fetch_channels, so populate the event cache if needed.
        events = getattr(self, '_events_cache', None)
        if events is None:
            events = self._live_events() if self._signed_in() else []
        for ev in events:
            cid = f'{_EVENT_PREFIX}{ev["edit_id"]}'
            if cid not in by_edit:
                continue
            start = ev['start'] or datetime.now(timezone.utc)
            end = ev['end'] or (start + timedelta(hours=3))
            programs.append(ProgramData(
                source_channel_id=cid,
                title=ev['title'],
                start_time=start,
                end_time=end,
                description=ev.get('description'),
                category='Sports',
                is_live=True,
            ))
        logger.info('[hbomax] %d guide entries', len(programs))
        return programs

    def _live_events(self) -> list[dict]:
        """LIVE video events, merged from the discovery routes and deduped by
        editId (first route wins)."""
        merged: dict[str, dict] = {}
        errors = 0
        for route in _EVENT_ROUTES:
            try:
                for ev in self._events_from_route(route):
                    merged.setdefault(ev['edit_id'], ev)
            except TVEAuthError:
                raise
            except Exception as exc:  # noqa: BLE001
                errors += 1
                logger.warning('[hbomax] event route %s failed: %s', route, exc)
        if errors == len(_EVENT_ROUTES):
            raise RuntimeError('HBO Max: could not load the live schedule from any route')
        events = list(merged.values())
        self._events_cache = events
        return events

    def _events_from_route(self, route: str) -> list[dict]:
        """Parse one CMS route (JSON:API) for LIVE video events."""
        r = self.session.get(f'{HOME}{route}', headers=self._headers(),
                             params=_ROUTE_PARAMS, timeout=30)
        if r.status_code in (401, 403):
            raise self._sign_in_needed()
        r.raise_for_status()
        data = r.json() or {}
        inc = data.get('included') or []
        chans = {x['id']: (x.get('attributes') or {}).get('name')
                 for x in inc if x.get('type') == 'channel'}
        edits = {x['id']: (x.get('attributes') or {})
                 for x in inc if x.get('type') == 'edit'}
        images = {x['id']: (x.get('attributes') or {})
                  for x in inc if x.get('type') == 'image'}
        tax = {x['id']: (x.get('attributes') or {})
               for x in inc if x.get('type') == 'taxonomyNode'}
        now = datetime.now(timezone.utc)
        horizon = now + timedelta(hours=_EVENT_LOOKAHEAD_HOURS)
        events = []
        for x in inc:
            if x.get('type') != 'video':
                continue
            a = x.get('attributes') or {}
            if a.get('videoType') != 'LIVE':
                continue
            rel = x.get('relationships') or {}
            edit = ((rel.get('edit') or {}).get('data') or {})
            edit_id = edit.get('id') if isinstance(edit, dict) else None
            if not edit_id:
                continue
            ea = edits.get(edit_id, {})
            start = _dt(a.get('scheduleStart') or ea.get('playableStart') or a.get('firstAvailableDate'))
            end = _dt(a.get('scheduleEnd') or ea.get('playableEnd'))
            # Skip events already over, or starting beyond the lookahead window.
            if end and end < now:
                continue
            if start and start > horizon:
                continue
            ch = ((rel.get('primaryChannel') or {}).get('data') or {})
            chan = chans.get(ch.get('id')) if isinstance(ch, dict) else None
            if chan == 'Unbranded':
                chan = None
            img_refs = (rel.get('images') or {}).get('data') or []
            # league: the competition (NCAA) if present, else the event (NHL);
            # sport from txSports (Ice Hockey / Basketball / Wrestling / …).
            league = _tax_name(rel, 'txCompetition', tax) or _tax_name(rel, 'txEvent', tax)
            sport = _tax_name(rel, 'txSports', tax)
            events.append({
                'edit_id': edit_id,
                'title': (a.get('name') or 'Live').strip(),
                'channel': chan,
                'description': a.get('description'),
                'start': start,
                'end': end,
                'playable_start': _dt(ea.get('playableStart')),
                'playable_end': _dt(ea.get('playableEnd')),
                'logo': _pick_image(img_refs, images),
                'sport': sport,
                'league': league,
            })
        return events

    # ── playback ─────────────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        self.resolve(raw_url)
        return raw_url

    def resolve(self, raw_url: str) -> str:
        edit_id = raw_url[len(SCHEME):] if raw_url.startswith(SCHEME) else ''
        if not edit_id:
            raise ValueError(f'Unsupported HBO Max stream URL: {raw_url}')
        if not self._signed_in():
            raise self._sign_in_needed()
        state = self._bootstrap_state()
        r = self.session.post(
            f'{ANON}/any/playback/v1/playbackInfo',
            headers=self._headers({'Content-Type': 'application/json',
                                   **({'x-wbd-session-state': state} if state else {})}),
            json=self._playback_body(edit_id), timeout=25)
        if r.status_code in (401, 403):
            # cookie session rejected → drop it so the next sign-in refreshes
            if 'anonymous' in _err(r) or r.status_code == 401:
                logger.warning('[hbomax] session rejected (HTTP %s); re-auth needed', r.status_code)
                raise self._sign_in_needed()
            raise RuntimeError(f'HBO Max: not entitled for this event: HTTP {r.status_code} {_err(r)}')
        body = _json(r)
        url = (body.get('manifest') or {}).get('url')
        if not r.ok or not url:
            raise RuntimeError(f'HBO Max: no stream for {edit_id}: HTTP {r.status_code} {_err(r)}')
        license_url = ((((body.get('drm') or {}).get('schemes') or {}).get('widevine')) or {}).get('licenseUrl')
        streams = dict(self.cache.get('hbomax_streams') or {})
        streams[edit_id] = {'license': license_url, 'url': url, 'at': time.time()}
        self._update_cache('hbomax_streams', streams)
        logger.info('[hbomax] stream session for %s (%s)', edit_id, url.split('/')[2])
        return url

    def _playback_body(self, edit_id: str) -> dict:
        return {
            'appBundle': 'hbomax', 'consumptionType': 'streaming', 'editId': edit_id,
            'deviceInfo': {
                'deviceId': self._device_id(), 'browser': {'name': 'Chrome', 'version': '141.0.0.0'},
                'make': 'desktop', 'model': 'desktop', 'os': {'name': 'Windows', 'version': 'NT 10.0'},
                'platform': 'WEB', 'deviceType': 'web',
                'player': {'sdk': {'name': 'Beam Player Desktop', 'version': CLIENT_VERSION},
                           'mediaEngine': {'name': 'GLUON_BROWSER', 'version': '11.1.0'},
                           'playerView': {'height': 1080, 'width': 1920}},
            },
            'capabilities': {
                'manifests': {'formats': {'dash': {}}},
                'contentProtection': {'contentDecryptionModules': [
                    {'drmKeySystem': 'widevine', 'maxSecurityLevel': 'L3'}]},
            },
            'gdpr': False, 'firstPlay': False, 'userPreferences': {'uiLanguage': 'en'},
            'playbackSessionId': str(uuid.uuid4()), 'applicationSessionId': str(uuid.uuid4()),
        }

    # ── DRM license proxy wiring ─────────────────────────────────────────────

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        key = (channel_id or '')[len(_EVENT_PREFIX):] if (channel_id or '').startswith(_EVENT_PREFIX) else channel_id
        cached = ((config.get('hbomax_streams') or {}).get(key or '') or {}).get('license')
        return cached or cls.license_url

    @classmethod
    def prepare_license_request(cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs):
        return challenge, {'Content-Type': 'application/octet-stream', 'User-Agent': UA,
                           'Origin': SITE, 'Referer': SITE + '/'}


def _tax_name(rel: dict, key: str, tax: dict) -> str | None:
    """First taxonomyNode name for a JSON:API relationship (e.g. txSports)."""
    d = (rel.get(key) or {}).get('data')
    if not d:
        return None
    refs = d if isinstance(d, list) else [d]
    for ref in refs:
        name = (tax.get((ref or {}).get('id')) or {}).get('name')
        if name:
            return name
    return None


# Preferred image kinds for a channel logo/tile, best first.
_IMAGE_KIND_PREFERENCE = ('default-wide', 'default', 'logo-left', 'centered-background-small')


def _pick_image(refs, images: dict) -> str | None:
    """Choose the best event image src from a JSON:API images relationship."""
    if isinstance(refs, dict):
        refs = [refs]
    by_kind = {}
    for ref in refs or []:
        attrs = images.get((ref or {}).get('id')) or {}
        src = attrs.get('src')
        if src:
            by_kind.setdefault(attrs.get('kind'), src)
    for kind in _IMAGE_KIND_PREFERENCE:
        if by_kind.get(kind):
            return by_kind[kind]
    return next(iter(by_kind.values()), None)


def _dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


def _json(r):
    try:
        body = r.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _err(r) -> str:
    body = _json(r)
    errors = body.get('errors') if isinstance(body, dict) else None
    if errors and isinstance(errors[0], dict):
        return errors[0].get('code') or errors[0].get('detail') or ''
    return ''

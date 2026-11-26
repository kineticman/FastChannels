"""
hbomax.py — HBO Max via a direct subscription sign-in: the linear HBO channels
and live sports events (TBS / TNT / truTV).

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
- Linear channels: GET /cms/routes/channels lists `distributionChannel` items.
  The ones with an `edit` relationship (HBO, Comedy, Drama, Hits, Movies) are
  playable; the rest are themed hubs with no stream. Their `airing` items are
  the guide (about 10 per channel).
- Live events: GET /cms/routes/sport-schedule (JSON:API) lists LIVE `video`
  items, each with an `edit` relationship (the editId) and a `primaryChannel`
  (TBS/TNT/truTV). These are modelled as temporary event channels.
- Playback: POST /any/playback/v1/playbackInfo with the editId → Widevine DASH
  (dash-cmaf-cenc) and a pre-signed license URL. Bridge-only (DRM), default-off.
  An account without HBO is switched off per channel by the stream audit.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, EVENT_CHANNEL_TAG, infer_language
from ..gracenote_map import resolve_gracenote
from ..tve.adobe_pass import TVEAuthError, TVENotAuthorizedError

logger = logging.getLogger(__name__)

SCHEME = 'hbomax://edit/'                    # a sports event: <editId>
CHANNEL_SCHEME = 'hbomax://channel/'         # a linear channel: <channelId>/<editId>
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
_CHANNEL_PREFIX = 'channel-'
CHANNELS_ROUTE = '/cms/routes/channels'
_CHANNEL_CATEGORIES = {'HBO': 'Entertainment', 'HBO Comedy': 'Comedy', 'HBO Drama': 'Drama',
                       'HBO Hits': 'Movies', 'HBO Movies': 'Movies'}
_SIGN_IN_MSG = ('HBO Max: not signed in yet — add your email and password and use '
                '"Sign in" for HBO Max under Settings.')


class HBOMaxScraper(BaseScraper):
    """HBO Max linear channels and live sports events via a direct subscription."""

    source_name = 'hbomax'
    added_in = '6.0.0'
    display_name = 'HBO Max'
    source_category = 'premium'
    is_premium = True
    scrape_interval = 120
    # The default (linear) audit scope covers the HBO channels: it resolves each
    # one, switches off what the account can't play, and fills the resolution
    # badge. Events are skipped unless the audit is run with events included.
    stream_audit_enabled = True
    license_url = LICENSE_BASE
    all_channels_require_drm_bridge = True
    config_required = True

    # Email/password are typed; the cookie jar + device id are written by the
    # Camoufox sign-in (hbomax_login.py) and hidden from the card.
    config_schema = [
        ConfigField('email', 'HBO Max email', placeholder='you@example.com'),
        ConfigField('password', 'HBO Max password', field_type='password', secret=True),
        ConfigField('cookie_jar', 'HBO Max session', secret=True, hidden=True),
        ConfigField('include_channels', 'Include HBO channels', field_type='toggle', default='true',
                    help_text='24/7 HBO channels; needs a plan that includes them.'),
        ConfigField('include_events', 'Include live sports events', field_type='toggle', default='true',
                    help_text='TNT, TBS and truTV events as temporary channels, one per event.'),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({'User-Agent': UA})
        self.hide_missing_event_channels = True

    # ── auth / session ───────────────────────────────────────────────────────

    def _signed_in(self) -> bool:
        jar = self.config.get('cookie_jar') or {}
        return bool(jar.get('cookies'))

    def _flag(self, key: str) -> bool:
        return str(self.config.get(key, 'true')).strip().lower() in ('1', 'true', 'yes', 'on')

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
        linear, kept = [], []
        if self._flag('include_channels'):
            try:
                linear = self._linear_channels()
            except TVEAuthError:
                raise
            except Exception as e:
                # The channel page failing mustn't stop the events refreshing, nor
                # drop the HBO channels already stored.
                kept = self._stored_linear_channels()
                if not kept and not self._flag('include_events'):
                    raise
                logger.warning('[hbomax] channel page failed (%s); keeping %d stored channel(s)', e, len(kept))
        events = self._live_events() if self._flag('include_events') else []
        self._events_cache = events
        self.excluded_channel_ids = self._switched_off_ids()
        out = list(kept)
        for ch in linear:
            out.append(ChannelData(
                source_channel_id=f'{_CHANNEL_PREFIX}{ch["id"]}',
                name=ch['name'],
                slug=f'hbomax-{ch["id"]}',
                logo_url=ch['logo'],
                stream_url=f'{CHANNEL_SCHEME}{ch["id"]}/{ch["edit_id"]}',
                stream_type='dash',
                category=_CHANNEL_CATEGORIES.get(ch['name'], 'Entertainment'),
                language='en',
                country='US',
                gracenote_id=resolve_gracenote('hbomax', lookup_key=ch['id']),
                guide_key=f'{_CHANNEL_PREFIX}{ch["id"]}',
            ))
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
        logger.info('[hbomax] %d channels (%d linear)', len(out), len(linear) + len(kept))
        return out

    def _switched_off_ids(self) -> set[str]:
        """Stored rows of a kind the user has turned off, so they leave the
        output as filtered rather than as missing upstream."""
        prefixes = tuple(p for flag, p in (('include_channels', _CHANNEL_PREFIX),
                                           ('include_events', _EVENT_PREFIX))
                         if not self._flag(flag))
        if not prefixes:
            return set()
        from ..models import Channel, Source
        rows = (Channel.query.join(Source).with_entities(Channel.source_channel_id)
                .filter(Source.name == 'hbomax').all())
        return {cid for (cid,) in rows if cid.startswith(prefixes)}

    def _stored_linear_channels(self) -> list[ChannelData]:
        """The linear channels as last scraped, for a run where their page failed."""
        from ..models import Channel, Source
        rows = (Channel.query.join(Source)
                .filter(Source.name == 'hbomax', Channel.source_channel_id.startswith(_CHANNEL_PREFIX))
                .all())
        return [ChannelData(
            source_channel_id=c.source_channel_id, name=c.name, slug=c.slug, logo_url=c.logo_url,
            stream_url=c.stream_url, stream_type=c.stream_type, category=c.category,
            language=c.language, country=c.country, guide_key=c.guide_key,
            gracenote_id=resolve_gracenote('hbomax', lookup_key=c.source_channel_id[len(_CHANNEL_PREFIX):]),
        ) for c in rows]

    def _linear_channels(self) -> list[dict]:
        """The playable linear channels, each with its upcoming airings."""
        cached = getattr(self, '_linear_cache', None)
        if cached is not None:
            return cached
        r = self.session.get(f'{HOME}{CHANNELS_ROUTE}', headers=self._headers(),
                             params=_ROUTE_PARAMS, timeout=30)
        if r.status_code in (401, 403):
            raise self._sign_in_needed()
        r.raise_for_status()
        channels = _parse_channels(r.json() or {})
        if not channels:
            # Never seen from a working session; don't let it drop the channels.
            raise RuntimeError('HBO Max: the channel page listed no playable channels')
        self._linear_cache = channels
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        programs = []
        if self._signed_in() and self._flag('include_channels'):
            wanted = {c.source_channel_id for c in channels}
            try:
                linear = self._linear_channels()
            except TVEAuthError:
                raise
            except Exception as e:
                logger.warning('[hbomax] no channel guide this run: %s', e)
                linear = []    # stored airings are left as they are
            for ch in linear:
                cid = f'{_CHANNEL_PREFIX}{ch["id"]}'
                if cid in wanted:
                    programs.extend(ProgramData(source_channel_id=cid, **a) for a in ch['airings'])
        by_edit = {c.source_channel_id: c for c in channels}
        # EPG-only runs skip fetch_channels, so populate the event cache if needed.
        events = getattr(self, '_events_cache', None)
        if events is None:
            wanted = self._signed_in() and self._flag('include_events')
            events = self._live_events() if wanted else []
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
        if (raw_url or '').startswith(CHANNEL_SCHEME):
            return self.resolve(raw_url)
        # An upcoming event has no stream yet; its place in the schedule is the check.
        return raw_url

    def resolve(self, raw_url: str) -> str:
        key, edit_id = _parse_stream_url(raw_url)
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
            if 'before.playable' in _err(r):
                # Not an entitlement problem: the event's playback window hasn't
                # opened yet. It plays once the window opens.
                raise RuntimeError(
                    f'HBO Max: event is not playable yet (its playback window has not opened): '
                    f'HTTP {r.status_code} {_err(r)}')
            if _not_in_plan(_err(r)):
                raise TVENotAuthorizedError(
                    f'HBO Max: your plan does not include this channel: HTTP {r.status_code} {_err(r)}')
            raise RuntimeError(f'HBO Max: playback refused: HTTP {r.status_code} {_err(r)}')
        body = _json(r)
        url = (body.get('manifest') or {}).get('url')
        if not r.ok or not url:
            raise RuntimeError(f'HBO Max: no stream for {edit_id}: HTTP {r.status_code} {_err(r)}')
        license_url = ((((body.get('drm') or {}).get('schemes') or {}).get('widevine')) or {}).get('licenseUrl')
        # Merged on write (config_store), so two channels tuned at once both keep
        # their license URL.
        streams = dict(self.cache.get('hbomax_streams') or {})
        streams[key] = {'license': license_url, 'at': time.time()}
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
        key = (channel_id or '').removeprefix(_EVENT_PREFIX).removeprefix(_CHANNEL_PREFIX)
        cached = ((config.get('hbomax_streams') or {}).get(key) or {}).get('license')
        return cached or cls.license_url

    @classmethod
    def prepare_license_request(cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs):
        return challenge, {'Content-Type': 'application/octet-stream', 'User-Agent': UA,
                           'Origin': SITE, 'Referer': SITE + '/'}


def _parse_stream_url(raw_url: str) -> tuple[str, str]:
    """(license cache key, editId). The key is the channel id for a linear
    channel and the editId for an event, matching the source_channel_id."""
    if (raw_url or '').startswith(CHANNEL_SCHEME):
        key, _, edit_id = raw_url[len(CHANNEL_SCHEME):].partition('/')
    else:
        key = edit_id = raw_url[len(SCHEME):] if (raw_url or '').startswith(SCHEME) else ''
    if not (key and edit_id):
        raise ValueError(f'Unsupported HBO Max stream URL: {raw_url}')
    return key, edit_id


def _not_in_plan(code: str) -> bool:
    """A 403 that means the account lacks the content, as opposed to a location
    or stream-limit refusal that a later try could pass."""
    code = (code or '').lower()
    if any(word in code for word in ('geo', 'concurren', 'limit', 'region')):
        return False
    return any(word in code for word in ('access.denied', 'entitle', 'package', 'subscription'))


def _parse_channels(data: dict) -> list[dict]:
    inc = data.get('included') or []
    images = {x['id']: (x.get('attributes') or {})
              for x in inc if x.get('type') == 'image'}

    def ref(x, name):
        d = ((x.get('relationships') or {}).get(name) or {}).get('data')
        return d.get('id') if isinstance(d, dict) else None

    def image(x, *kinds):
        refs = ((x.get('relationships') or {}).get('images') or {}).get('data') or []
        by_kind = {}
        for r in refs:
            attrs = images.get(r.get('id')) or {}
            by_kind.setdefault(attrs.get('kind'), attrs.get('src'))
        return next((by_kind[k] for k in kinds if by_kind.get(k)), None)

    airings: dict[str, dict] = {}
    for x in inc:
        if x.get('type') != 'airing':
            continue
        a = x.get('attributes') or {}
        start, end = _dt(a.get('scheduleStart')), _dt(a.get('scheduleEnd'))
        title = (a.get('showName') or a.get('name') or '').strip()
        if not (start and end and end > start and title):
            continue
        name = (a.get('name') or '').strip()
        airings.setdefault(ref(x, 'distributionChannel'), {})[start] = {
            'title': title,
            'episode_title': name if name and name != title else None,
            'start_time': start,
            'end_time': end,
            'description': a.get('description'),
            'season': a.get('seasonNumber'),
            'episode': a.get('episodeNumber'),
            'program_type': 'episode' if a.get('episodeNumber') else None,
            'poster_url': image(x, 'default', 'cover-artwork-horizontal', 'cover-artwork'),
        }

    out = []
    for x in inc:
        if x.get('type') != 'distributionChannel' or not ref(x, 'edit'):
            continue
        name = ((x.get('attributes') or {}).get('name') or '').strip()
        if not name:
            continue
        out.append({
            'id': x['id'], 'name': name, 'edit_id': ref(x, 'edit'),
            'logo': image(x, 'cover-artwork-square', 'cover-artwork', 'default'),
            'airings': [v for _, v in sorted(airings.get(x['id'], {}).items())],
        })
    return out


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

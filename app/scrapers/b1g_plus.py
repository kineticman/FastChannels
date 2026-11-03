"""B1G+ (source b1g_plus) — Big Ten Plus live events as event channels.

Scoped 2026-11-02 (dev/hitlist/b1g_plus_scoping_2026-11-02.md,
dev/b1g/b1g-firetv-signin-apk-analysis.md). B1G+ is a Sportradar "Spott" OTT
site with Cleeng subscriptions; everything lives under www.bigtenplus.com/api/v3.

The schedule is public: modules/<schedule module>/contents lists one item per
event (startTime, no endTime), with home/away competitors, ranks, the sport
(category3, e.g. "Basketball (W)") and its playable content id.

Sign-in is the Fire TV app's code: POST cleeng/request-signin-code {device_id}
returns a 6-digit code (5 minutes); the user enters it at
www.bigtenplus.com/code while signed in; POST cleeng/signin-with-code
{code, device_id} answers 401 code_not_unauthorized until then and 200 with the
Cleeng user (id, token, offers) after. The session token is a JWT with no exp;
cleeng/extend_token swaps it for a fresh one and is the liveness check.

Playback (verified 2026-11-02 on a live game): POST contents/<id>/check-access
(Bearer session) → content token; POST contents/<id>/access/hls (Bearer
content token) → an Akamai master.m3u8 whose hdnea token expires about a
minute after it's issued, so resolve() always issues a fresh one. Streams are
clear MPEG-TS HLS, 720p. Content flagged drmProtectionEnabled is skipped.
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from ..matchups import matchup_body, matchup_description, matchup_title
from ..sports import normalize_sport
from ..tve.adobe_pass import TVEAuthError, TVENotAuthorizedError
from .base import BaseScraper, ChannelData, ConfigField, ProgramData, StreamDeadError, EVENT_CHANNEL_TAG

logger = logging.getLogger(__name__)

API = 'https://www.bigtenplus.com/api/v3/'
ACTIVATE_URL = 'https://www.bigtenplus.com/code'
# The Fire TV app is a WebView of bigtenplus.com/bigscreen/app/firetv (web app 3.3.2).
_UA = ('Mozilla/5.0 (Linux; Android 9; AFTKA Build/PS7633; wv) AppleWebKit/537.36 '
       '(KHTML, like Gecko) Version/4.0 Chrome/130.0.6723.102 Mobile Safari/537.36')
_OTT_HEADERS = {'x-ott-app-name': 'Amazon', 'x-ott-app-version': '3.3.2',
                'x-ott-app-device-name': 'FastChannels'}
_BASE_HEADERS = {'Accept': 'application/json', 'Content-Type': 'application/json', 'User-Agent': _UA}

# The schedule page's event-list module (page 7285 on the site).
SCHEDULE_MODULE = 138951
_DEVICE_CATEGORY = 3  # Smart TV
_ET = ZoneInfo('America/New_York')

EVENT_SCHEME = 'b1gplus://event/'
EVENT_CHANNEL_PREFIX = 'event:'
_EVENT_LOOKAHEAD_HOURS = 48
# Events have no scheduled end; these are typical lengths per sport.
_DEFAULT_HOURS = 3.0
_SPORT_HOURS = {'Basketball': 2.5, 'Volleyball': 2.5, 'Soccer': 2.25, 'Field Hockey': 2.0,
                'Football': 3.5, 'Hockey': 3.0, 'Wrestling': 3.0, 'Baseball': 3.0, 'Softball': 2.5,
                'Lacrosse': 2.5, 'Gymnastics': 2.5, 'Swimming': 3.0}
# Past the estimate an event still called live gets this much more each scrape.
_LIVE_EXTENSION = timedelta(minutes=45)
EVENT_END_GRACE_SECONDS = 3600
_TOKEN_CHECK_TTL = 12 * 3600
_LINK_POLL_SECONDS = 5.0
# Codes last 5 minutes; a little slack for clock drift.
_LINK_TIMEOUT = 330

LINK_STATUS_KEY = 'b1g_plus:link:status'
SIGNIN_CONFIG_KEYS = ('token', 'user_id', 'email', 'signed_in_at', 'token_checked_at',
                      'subscription', 'subscription_active')
_SIGNIN_LOST = 'The B1G+ sign-in expired or was removed — sign in again.'


class B1GAuthError(TVEAuthError):
    pass


# ── account ──────────────────────────────────────────────────────────────────

def _headers(token: str | None = None, **extra) -> dict:
    h = dict(_BASE_HEADERS, **extra)
    if token:
        h['Authorization'] = 'Bearer ' + token
    return h


def _subscription_info(user: dict) -> tuple[str | None, bool]:
    """(the active pass's title or offer id, whether one is active) for a Cleeng user."""
    now = time.time()
    for sub in user.get('subscriptions') or []:
        if (sub.get('status') or '').lower() == 'active' and float(sub.get('expiresAt') or 0) > now:
            return sub.get('offerTitle') or sub.get('offerId'), True
    for offer in user.get('offers') or []:
        if (offer.get('status') or '').lower() == 'active' and float(offer.get('expiresAt') or 0) > now:
            return offer.get('offerTitle') or offer.get('offerId'), True
    return None, False


def extend_token(token: str) -> str | None:
    """A fresh session token, or None when the session is gone."""
    r = requests.post(API + 'cleeng/extend_token', headers=_headers(token), timeout=20)
    if r.status_code in (401, 403):
        return None
    r.raise_for_status()
    return (r.json() or {}).get('token')


def profile(token: str) -> dict:
    r = requests.get(API + 'cleeng/profile', params={'includeAuth0Token': 'true'},
                     headers=_headers(token), timeout=20)
    r.raise_for_status()
    return r.json() or {}


def logout(token: str) -> None:
    requests.get(API + 'cleeng/logout', headers=_headers(token), timeout=20)


def _signin_config(user: dict, device_id: str) -> dict:
    now = int(time.time())
    title, active = _subscription_info(user)
    return {'token': user['token'], 'user_id': str(user.get('id') or ''), 'email': user.get('email'),
            'device_id': device_id, 'signed_in_at': now, 'token_checked_at': now,
            'subscription': title, 'subscription_active': active}


# ── activation-code sign-in ──────────────────────────────────────────────────

def start_link(device_id: str) -> dict:
    r = requests.post(API + 'cleeng/request-signin-code', json={'device_id': device_id},
                      headers=_headers(), timeout=20)
    data = (r.json() or {}) if r.ok else {}
    code = data.get('code') or (data.get('data') or {}).get('code')
    if not code:
        raise B1GAuthError(f'B1G+: no sign-in code (HTTP {r.status_code})')
    return {'code': str(code), 'device_id': device_id, 'expires': data.get('expire_date')}


def link_status(ctx: dict, state: str, message: str = '') -> dict:
    return {'state': state, 'message': message, 'code': ctx['code'], 'url': ACTIVATE_URL}


def poll_link(ctx: dict) -> dict | None:
    """None until the code is entered; the Cleeng user (with token) after."""
    r = requests.post(API + 'cleeng/signin-with-code', params={'includeAuth0Token': 'true'},
                      json={'code': ctx['code'], 'device_id': ctx['device_id']},
                      headers=_headers(**_OTT_HEADERS), timeout=20)
    if r.status_code == 401:
        return None
    if not r.ok:
        raise B1GAuthError(f'B1G+ sign-in: HTTP {r.status_code} {r.text[:200]}')
    user = r.json() or {}
    if not user.get('token'):
        raise B1GAuthError('B1G+ accepted the code but returned no session.')
    return user


def run_link(ctx: dict) -> None:
    """RQ job: wait for the code to be entered, then save the session onto the
    b1g_plus source and rescrape. Quits quietly once LINK_STATUS_KEY names a
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

        deadline = time.monotonic() + _LINK_TIMEOUT
        user = None
        try:
            while time.monotonic() < deadline and not superseded():
                user = poll_link(ctx)
                if user:
                    break
                time.sleep(_LINK_POLL_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[b1g_plus] sign-in failed: %s', exc)
            set_status('error', str(exc)[:300])
            return
        if not user:
            if superseded():
                logger.info('[b1g_plus] sign-in code %s stopped', ctx['code'])
            else:
                set_status('expired', 'The code expired before it was entered.')
            return

        src = Source.query.filter_by(name='b1g_plus').first()
        if not src:
            set_status('error', 'B1G+ source no longer exists.')
            return
        cfg = dict(src.config or {})
        cfg.pop('signin_error', None)
        cfg.update(_signin_config(user, ctx['device_id']))
        src.config = cfg
        db.session.commit()
        logger.info('[b1g_plus] signed in (%s)', cfg.get('subscription') or 'no active pass')
        set_status('success')
        if src.is_enabled:
            from ..routes.tasks import trigger_scrape
            trigger_scrape('b1g_plus', force_full=True)


# ── schedule helpers ─────────────────────────────────────────────────────────

def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


_GENDER = re.compile(r'\s*\((M|W)\)\s*$', re.I)


def sport_and_gender(event: dict) -> tuple[str | None, str | None]:
    """("Basketball", "W") from category3 "Basketball (W)", falling back to the gender metadata."""
    raw = ((event.get('category3') or {}).get('name') or '').strip()
    m = _GENDER.search(raw)
    gender = m.group(1).upper() if m else None
    sport = normalize_sport(raw[:m.start()] if m else raw)
    if not gender:
        meta = {((x.get('field') or {}).get('name') or '').lower(): x.get('name') for x in event.get('metadata') or []}
        g = (meta.get('gender') or '').lower()
        gender = 'W' if g.startswith('w') else 'M' if g.startswith('m') else None
    return sport, gender


def league_for(sport: str | None, gender: str | None) -> str:
    """League in the feed filter's vocabulary, also the title's label: ESPN's
    NCAAM/NCAAW for basketball, "NCAA Women's Volleyball" style for the rest."""
    if sport == 'Basketball' and gender in ('M', 'W'):
        return 'NCAAM' if gender == 'M' else 'NCAAW'
    if not sport:
        return 'NCAA'
    prefix = {'M': "Men's ", 'W': "Women's "}.get(gender, '') if sport not in ('Football',) else ''
    return f'NCAA {prefix}{sport}'


def _team(comp: dict | None) -> str | None:
    """"Michigan State Spartans" from name "Michigan State" + fullName "Spartans"."""
    if not comp:
        return None
    name = (comp.get('name') or comp.get('shortName') or '').strip()
    nick = (comp.get('fullName') or '').strip()
    if nick and name and not nick.startswith(name) and nick.casefold() != name.casefold():
        return f'{name} {nick}'
    return nick or name or None


def matchup_for(event: dict) -> dict | None:
    home, away = _team(event.get('homeCompetitor')), _team(event.get('awayCompetitor'))
    if home and away:
        return {'home': home, 'away': away, 'neutral': False}
    return None


def _image(images: list | None, size: str) -> str | None:
    for img in images or []:
        variants = img.get('variants') or {}
        if size in variants:
            return variants[size]
    return (images or [{}])[0].get('url') if images else None


def _content_title(content: dict) -> str | None:
    tr = ((content.get('editorial') or {}).get('translations') or {})
    return ((tr.get('en') or next(iter(tr.values()), {})) or {}).get('title')


def parse_event_url(raw_url: str) -> tuple[int, int | None]:
    m = re.match(r'b1gplus://event/(\d+)(?:\?end=(\d+))?$', raw_url or '')
    if not m:
        raise ValueError(f'Unsupported B1G+ stream URL: {raw_url}')
    return int(m.group(1)), int(m.group(2)) if m.group(2) else None


# ── scraper ──────────────────────────────────────────────────────────────────

class B1GPlusScraper(BaseScraper):
    source_name = 'b1g_plus'
    display_name = 'B1G+'
    source_category = 'premium'
    is_premium = True
    scrape_interval = 120
    # Every channel is an event; an upcoming one has no stream to audit.
    stream_audit_enabled = False

    config_required = True
    # Set by the code sign-in, never typed; hidden from the card.
    config_schema = [
        ConfigField('token', 'B1G+ session', secret=True, hidden=True),
    ]

    # ── auth ──

    def _token(self) -> str | None:
        return self.config.get('token') or None

    def _signin_lost(self) -> None:
        logger.warning('[b1g_plus] %s', _SIGNIN_LOST)
        for key in SIGNIN_CONFIG_KEYS:
            self._update_config(key, None)
        self._update_config('signin_error', _SIGNIN_LOST)

    def _check_token(self, force: bool = False) -> None:
        """Swap the session token for a fresh one twice a day (and on demand)."""
        token = self._token()
        if not token or (not force and time.time() - float(self.config.get('token_checked_at') or 0) < _TOKEN_CHECK_TTL):
            return
        fresh = extend_token(token)
        if not fresh:
            self._signin_lost()
            return
        self._update_config('token', fresh)
        self._update_config('token_checked_at', int(time.time()))
        try:
            title, active = _subscription_info(profile(fresh))
            self._update_config('subscription', title)
            self._update_config('subscription_active', active)
        except requests.RequestException as exc:
            logger.info('[b1g_plus] profile check failed: %s', exc)

    def pre_run_setup(self) -> None:
        self._check_token()

    # ── schedule ──

    def _day_events(self, day: str) -> list[dict]:
        events, page = [], 1
        while True:
            r = self.session.get(f'{API}modules/{SCHEDULE_MODULE}/contents', timeout=30, headers=_BASE_HEADERS,
                                 params={'filter[deviceCategory]': _DEVICE_CATEGORY,
                                         'filter[dateTimeFrom]': f'{day}T00:00:00{self._et_offset(day)}',
                                         'filter[dateTimeTo]': f'{day}T23:59:59{self._et_offset(day)}',
                                         'limit': 50, 'page': page})
            r.raise_for_status()
            data = r.json() or {}
            events.extend(data.get('data') or [])
            if page >= int((data.get('meta') or {}).get('last_page') or 1) or page >= 10:
                return events
            page += 1

    @staticmethod
    def _et_offset(day: str) -> str:
        noon = datetime.fromisoformat(day + 'T12:00:00').replace(tzinfo=_ET)
        off = noon.utcoffset() or timedelta(0)
        mins = int(off.total_seconds() // 60)
        sign = '-' if mins < 0 else '+'
        return f'{sign}{abs(mins) // 60:02d}:{abs(mins) % 60:02d}'

    def _events(self) -> list[tuple[dict, dict, datetime, datetime]]:
        """(event, content, start, end) for every event live now or starting in the next two days."""
        now = datetime.now(timezone.utc)
        horizon = now + timedelta(hours=_EVENT_LOOKAHEAD_HOURS)
        today = now.astimezone(_ET).date()
        found: dict[int, tuple[dict, dict, datetime, datetime]] = {}
        for offset in (-1, 0, 1, 2):
            day = (today + timedelta(days=offset)).isoformat()
            try:
                events = self._day_events(day)
            except requests.RequestException as exc:
                logger.warning('[b1g_plus] schedule for %s unavailable: %s', day, exc)
                continue
            for event in events:
                for content in event.get('contents') or []:
                    status = (content.get('status') or {}).get('name')
                    cid = content.get('id')
                    # Replays, condensed and uncut games are VOD items that also say "Live".
                    dist = content.get('distributionType')
                    dist = (dist.get('name') if isinstance(dist, dict) else dist) or ''
                    if (not cid or dist.lower() != 'live' or status not in ('Announced', 'Live')
                            or content.get('isArchived')):
                        continue
                    if content.get('drmProtectionEnabled'):
                        logger.info('[b1g_plus] skipping DRM event %s', cid)
                        continue
                    start = _parse_time(content.get('startTime') or event.get('startTime'))
                    if not start:
                        continue
                    end = _parse_time(content.get('endTime') or event.get('endTime'))
                    if not end or end <= start:
                        sport, _ = sport_and_gender(event)
                        end = start + timedelta(hours=_SPORT_HOURS.get(sport, _DEFAULT_HOURS))
                    if status == 'Live' and end <= now:
                        end = now + _LIVE_EXTENSION
                    if end <= now or start >= horizon:
                        continue
                    found.setdefault(int(cid), (event, content, start, end))
        return sorted(found.values(), key=lambda e: (e[2], e[1]['id']))

    # ── channels / EPG ──

    def _describe(self, event: dict, content: dict) -> tuple[str, str | None, str, dict | None]:
        """(title body, sport, league, matchup) for an event."""
        sport, gender = sport_and_gender(event)
        league = league_for(sport, gender)
        matchup = matchup_for(event)
        body = matchup_body(matchup, sport) if matchup else (_content_title(content) or event.get('title') or 'B1G+ Live')
        return body, sport, league, matchup

    def fetch_channels(self) -> list[ChannelData]:
        self._event_items = []
        self.hide_missing_event_channels = True
        if not self._token():
            return []
        self._event_items = self._events()
        channels = []
        for event, content, start, end in self._event_items:
            cid = content['id']
            body, sport, league, _ = self._describe(event, content)
            label = league
            home = event.get('homeCompetitor') or {}
            logo = (home.get('image') or {}).get('url') or _image((content.get('editorial') or {}).get('images'), '400x225')
            channels.append(ChannelData(
                source_channel_id=f'{EVENT_CHANNEL_PREFIX}{cid}',
                name=f'B1G+ - {label}: {body}'[:256] if label else f'B1G+ - {body}'[:256],
                stream_url=f'{EVENT_SCHEME}{cid}?end={int(end.timestamp())}',
                logo_url=logo,
                category='Sports',
                stream_type='hls',
                guide_key=f'{EVENT_CHANNEL_PREFIX}{cid}',
                tags=[EVENT_CHANNEL_TAG, 'B1G+'],
                sport=sport,
                league=league,
                gracenote_mode='off',
                retire_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS),
                delete_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS + 24 * 3600),
            ))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {ch.source_channel_id for ch in channels}
        programs = []
        for event, content, start, end in getattr(self, '_event_items', []):
            cid = f'{EVENT_CHANNEL_PREFIX}{content["id"]}'
            if cid not in wanted:
                continue
            body, sport, league, matchup = self._describe(event, content)
            label = league
            editorial = (content.get('editorial') or {}).get('translations') or {}
            desc = ((editorial.get('en') or {}) or {}).get('description')
            ranks = [f'#{r} {t}' for r, t in ((event.get('awayRank'), (matchup or {}).get('away')),
                                              (event.get('homeRank'), (matchup or {}).get('home'))) if r and t]
            if ranks:
                desc = f'{desc}. Ranked: {", ".join(ranks)}.' if desc else f'Ranked: {", ".join(ranks)}.'
            programs.append(ProgramData(
                source_channel_id=cid,
                title=matchup_title(label, matchup, sport) if matchup else (f'{label}: {body}' if label else body),
                start_time=start,
                end_time=end,
                description=matchup_description(matchup, sport, desc) if matchup else desc,
                poster_url=_image((content.get('editorial') or {}).get('images'), '800x450'),
                category=';'.join(x for x in ('Sports Event', sport, league) if x),
                is_live=True,
                episode_id=str(content['id']),
            ))
        return programs

    # ── playback ──

    def _require_token(self) -> str:
        token = self._token()
        if not token:
            raise B1GAuthError('B1G+: sign in on the Sources page.')
        return token

    def _check_access(self, content_id: int, token: str):
        return self.session.post(f'{API}contents/{content_id}/check-access', json={},
                                 headers=_headers(token), timeout=20)

    def audit_resolve(self, raw_url: str) -> str:
        # An upcoming event has no stream yet; its place in the schedule is the check.
        return raw_url

    def resolve(self, raw_url: str) -> str:
        content_id, end = parse_event_url(raw_url)
        if end is not None and time.time() >= end + EVENT_END_GRACE_SECONDS:
            raise StreamDeadError('B1G+ event has ended')
        token = self._require_token()
        r = self._check_access(content_id, token)
        if r.status_code == 401:
            # A stale session: one extend_token decides whether it's gone.
            self._check_token(force=True)
            token = self._require_token()
            r = self._check_access(content_id, token)
            if r.status_code == 401:
                self._signin_lost()
                raise B1GAuthError(_SIGNIN_LOST)
        if r.status_code == 403:
            raise TVENotAuthorizedError('B1G+: this account has no active pass for this event')
        if not r.ok:
            raise RuntimeError(f'B1G+ check-access: HTTP {r.status_code} {r.text[:200]}')
        content_token = (r.json() or {}).get('data')
        if not content_token:
            raise RuntimeError('B1G+: no access token for this event')
        r = self.session.post(f'{API}contents/{content_id}/access/hls', json={},
                              headers=_headers(content_token), timeout=20)
        data = r.json() if r.headers.get('content-type', '').startswith('application/json') else {}
        if not r.ok or data.get('status') == 'error':
            code = data.get('code')
            if code == 201:
                raise RuntimeError('B1G+: this account is already streaming on another device')
            raise RuntimeError(f'B1G+: no stream for this event yet (HTTP {r.status_code} {data.get("message") or ""})'.strip())
        stream = (data.get('data') or {}).get('stream')
        if not stream:
            raise RuntimeError('B1G+: no stream for this event yet')
        return stream

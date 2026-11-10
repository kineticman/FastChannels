"""Northeast Sports Network (nsnsports.net): live college and high-school
games from New England (NESCAC, NEWMAC, CNE, NEPSAC, Vermont/New Hampshire
high schools), each an event channel while it's on.

NSN broadcasts on Hudl vCloud (formerly BlueFrame, customer 193). The site's
WordPress proxies the broadcast list:
  ajax.php?action=nsn_schedule&list=upcoming_or_streaming → broadcasts[] with
  id, title ("Endicott vs Bowdoin"), section_title ("Men's Basketball"),
  site_title, date (epoch ms), status, require_login ("no" | "ppv"), images.
  ajax.php?action=nsn_player&video=<id> → the same record for one broadcast.
Playback is the broadcast's VMAP (vcloud.hudl.com/api/broadcast/vmap/<id>):
its <Content type="live" streaming="true"> is a clear HLS URL, no sign-in.

Published, available broadcasts are carried regardless of require_login.
"""
from __future__ import annotations

import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

from ..sports import normalize_sport
from .base import BaseScraper, ChannelData, ProgramData, StreamDeadError, EVENT_CHANNEL_TAG

SITE = 'https://www.nsnsports.net'
AJAX = f'{SITE}/ajax.php'
VMAP = 'https://vcloud.hudl.com/api/broadcast/vmap/{}'
EMBED = 'https://vcloud.hudl.com/broadcast/embed/{}'
_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
       '(KHTML, like Gecko) Chrome/140.0 Safari/537.36')

EVENT_SCHEME = 'nsn://broadcast/'
EVENT_CHANNEL_PREFIX = 'event:'
_EVENT_LOOKAHEAD_HOURS = 48
_PER_PAGE = 50
_MAX_PAGES = 10
# Broadcasts have no scheduled end (expected_duration is usually null).
_DEFAULT_HOURS = 2.5
_SPORT_HOURS = {'Basketball': 2.25, 'Hockey': 2.75, 'Football': 3.25, 'Volleyball': 2.0,
                'Soccer': 2.25, 'Lacrosse': 2.5, 'Baseball': 3.0, 'Softball': 2.5,
                'Field Hockey': 2.0, 'Wrestling': 3.0}
# Past the estimate a broadcast still streaming gets this much more each scrape.
_LIVE_EXTENSION = timedelta(minutes=45)
EVENT_END_GRACE_SECONDS = 3600
_GENDER_RE = re.compile(r"^\s*(men'?s|women'?s|boys'?|girls'?)\s+", re.I)


def _headers() -> dict:
    return {'User-Agent': _UA, 'Accept': 'application/json', 'Referer': f'{SITE}/'}


def is_available(b: dict) -> bool:
    """Published and available, regardless of payment or login requirements."""
    return b.get('available') is not False and not b.get('hidden')


def sport_and_gender(b: dict) -> tuple[str | None, str | None]:
    """("Basketball", "M") from section_title "Men's Basketball"; "Ice Hockey" → Hockey."""
    raw = (b.get('section_title') or '').strip()
    m = _GENDER_RE.match(raw)
    gender = None
    if m:
        word = m.group(1).lower()
        gender = 'M' if word.startswith(('men', 'boy')) else 'W'
        raw = raw[m.end():]
    return normalize_sport(raw) or (raw or None), gender


def is_high_school(b: dict) -> bool:
    section = (b.get('section_title') or '').lower()
    return section.startswith(('boys', 'girls')) or 'high school' in (b.get('site_title') or '').lower()


def league_for(b: dict) -> str:
    """League in the feed filter's vocabulary (matches B1G+'s for college)."""
    sport, gender = sport_and_gender(b)
    if is_high_school(b):
        prefix = {'M': "Boys' ", 'W': "Girls' "}.get(gender, '')
        return f'High School {prefix}{sport}'.strip() if sport else 'High School'
    if sport == 'Basketball' and gender in ('M', 'W'):
        return 'NCAAM' if gender == 'M' else 'NCAAW'
    if not sport:
        return 'NCAA'
    prefix = {'M': "Men's ", 'W': "Women's "}.get(gender, '') if sport != 'Football' else ''
    return f'NCAA {prefix}{sport}'


# ── event filter (sports and/or schools, like B1G+) ──────────────────────────

# NSN's member schools (its Colleges and High Schools menus), listed in the
# picker even before they have a game scheduled; schools seen on the schedule
# are added.
BASE_SCHOOLS = (
    'Amherst', 'Bates', 'Bowdoin', 'Colby', 'Connecticut College', 'Hamilton', 'Middlebury',
    'Norwich', 'NVU-Lyndon', 'Trinity', 'Tufts', 'Wesleyan', 'Williams',
    'Berlin', 'Blue Mountain', 'Colebrook', 'Danville', 'Essex', 'Gorham', 'Groveton', 'Lamoille',
    'Littleton', 'NCU', 'PCDS', 'Stowe', 'Twinfield', 'Woodsville',
)
BASE_SPORTS = ('Football', "Men's Basketball", "Women's Basketball", "Men's Hockey", "Women's Hockey",
               "Women's Volleyball", "Men's Soccer", "Women's Soccer", 'Field Hockey',
               "Men's Lacrosse", "Women's Lacrosse", 'Baseball', 'Softball',
               "Boys' Basketball", "Girls' Basketball")
_ONE_GENDER_SPORTS = frozenset({'Football', 'Baseball', 'Softball', 'Field Hockey'})
SEEN_SPORTS_CACHE_KEY = 'nsn_seen_sports'
SEEN_SCHOOLS_CACHE_KEY = 'nsn_seen_schools'
# Site names that aren't a school (a conference's championship page).
_NOT_A_SCHOOL_RE = re.compile(r'championship|tournament|conference|\bnescac\b|\bnewmac\b', re.I)
# "Connecticut College" keeps its suffix; "Connecticut" alone would be the state.
_KEEP_FULL_NAME = frozenset({'connecticut college'})


def school_name(site_title: str | None) -> str | None:
    """The picker's name for a school site: "Bowdoin College" → "Bowdoin", "Tufts (NSN)" → "Tufts"."""
    name = re.sub(r'\s*\(NSN\)\s*$', '', ' '.join(str(site_title or '').split()), flags=re.I)
    if not name or _NOT_A_SCHOOL_RE.search(name):
        return None
    if name.casefold() in _KEEP_FULL_NAME:
        return name
    short = re.sub(r'\s+(college|university|high school|school|academy)$', '', name, flags=re.I).strip()
    short = re.sub(r'^(university of|college of)\s+', '', short, flags=re.I).strip()
    return short or name


def sport_label(b: dict) -> str:
    """The picker's name for a broadcast's sport: "Men's Hockey", "Football", "Girls' Basketball"."""
    sport, gender = sport_and_gender(b)
    sport = sport or 'Other'
    if gender not in ('M', 'W') or sport in _ONE_GENDER_SPORTS:
        return sport
    if is_high_school(b):
        return f"{'Boys' if gender == 'M' else 'Girls'}' {sport}"
    return f"{'Men' if gender == 'M' else 'Women'}'s {sport}"


def _side_is_school(side: str, school: str) -> bool:
    side, school = side.casefold().strip(), school.casefold()
    return side == school or side.startswith(school + ' ') or side.endswith(' ' + school)


def event_schools(b: dict, known: list[str] | tuple[str, ...] = BASE_SCHOOLS) -> list[str]:
    """The NSN schools a broadcast involves: the school whose site carries it,
    plus any known school named on either side of "A vs B", so "Colby" also
    picks up Colby's away games shown on another school's site."""
    names = []
    home = school_name(b.get('site_title'))
    if home:
        names.append(home)
    for shared in b.get('shared_sites') or []:
        name = school_name(shared.get('title') if isinstance(shared, dict) else None)
        if name:
            names.append(name)
    title = re.sub(r'#\d+\s*', '', str(b.get('title') or ''))
    sides = re.split(r'\s+(?:vs\.?|v\.?|at|@)\s+', title, maxsplit=1, flags=re.I)
    if len(sides) == 2:
        for school in known:
            if any(_side_is_school(side, school) for side in sides):
                names.append(school)
    return list(dict.fromkeys(names))


def _clean_names(value, what: str) -> list[str]:
    if not isinstance(value, list) or len(value) > 200 or any(
            not isinstance(v, str) or not v.strip() or len(v) > 100 for v in value):
        raise ValueError(f'Invalid {what}')
    return list(dict.fromkeys(v.strip() for v in value))


def clean_event_filter(value) -> dict:
    """Validate the saved filter; 'all' includes sports and schools added later."""
    if value is None:
        return {'sport_mode': 'all', 'sports': [], 'school_mode': 'all', 'schools': []}
    if not isinstance(value, dict):
        raise ValueError('Invalid event filter')
    sport_mode, school_mode = value.get('sport_mode', 'all'), value.get('school_mode', 'all')
    if sport_mode not in ('all', 'selected'):
        raise ValueError('Sports must be All or Only the ones I pick')
    if school_mode not in ('all', 'selected'):
        raise ValueError('Schools must be All or Only the ones I pick')
    return {'sport_mode': sport_mode, 'sports': _clean_names(value.get('sports', []), 'sport'),
            'school_mode': school_mode, 'schools': _clean_names(value.get('schools', []), 'school')}


def event_matches_filter(b: dict, config: dict, known_schools=BASE_SCHOOLS) -> bool:
    try:
        policy = clean_event_filter((config or {}).get('event_filter'))
    except ValueError:
        return True
    if policy['sport_mode'] == 'selected' and sport_label(b).casefold() not in {
            s.casefold() for s in policy['sports']}:
        return False
    if policy['school_mode'] == 'selected':
        picked = {s.casefold() for s in policy['schools']}
        known = list(dict.fromkeys([*known_schools, *policy['schools']]))
        if not any(s.casefold() in picked for s in event_schools(b, known)):
            return False
    return True


def _start(b: dict) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(b.get('date')) / 1000, tz=timezone.utc)
    except (TypeError, ValueError):
        return None


def _duration(b: dict) -> timedelta:
    raw = b.get('expected_duration')
    try:
        minutes = int(raw)
        if 30 <= minutes <= 8 * 60:
            return timedelta(minutes=minutes)
    except (TypeError, ValueError):
        pass
    sport, _ = sport_and_gender(b)
    return timedelta(hours=_SPORT_HOURS.get(sport, _DEFAULT_HOURS))


def is_streaming(b: dict) -> bool:
    return str(b.get('status') or '').lower() in ('streaming', 'live')


def parse_event_url(raw_url: str) -> tuple[str, int | None]:
    """('4792779', end epoch) from nsn://broadcast/4792779?end=..."""
    rest = raw_url[len(EVENT_SCHEME):] if raw_url.startswith(EVENT_SCHEME) else raw_url
    bid, _, query = rest.partition('?')
    end = None
    m = re.search(r'(?:^|&)end=(\d+)', query)
    if m:
        end = int(m.group(1))
    if not bid.isdigit():
        raise ValueError(f'bad NSN stream url: {raw_url}')
    return bid, end


class NSNScraper(BaseScraper):
    source_name = 'nsn'
    display_name = 'Northeast Sports Network'
    source_category = 'fast'
    grouped_under = 'free_networks'   # a small free network's own website, listed under Free Networks
    scrape_interval = 120
    # Every channel is an event; an upcoming one has no stream to audit.
    stream_audit_enabled = False

    def _schedule(self) -> list[dict]:
        """Upcoming and streaming broadcasts, soonest first, up to the lookahead."""
        horizon = datetime.now(timezone.utc) + timedelta(hours=_EVENT_LOOKAHEAD_HOURS)
        found: list[dict] = []
        for page in range(1, _MAX_PAGES + 1):
            r = self.session.get(AJAX, headers=_headers(), timeout=30, params={
                'action': 'nsn_schedule', 'list': 'upcoming_or_streaming',
                'per_page': _PER_PAGE, 'page': page, 'keep_list': 1})
            r.raise_for_status()
            data = r.json() or {}
            batch = data.get('broadcasts') or []
            found.extend(batch)
            starts = [s for s in (_start(b) for b in batch) if s]
            if not batch or data.get('last_page') or (starts and min(starts) > horizon):
                break
        return found

    def _events(self) -> list[tuple[dict, datetime, datetime]]:
        """(broadcast, start, end) for available broadcasts live now or starting within the lookahead."""
        now = datetime.now(timezone.utc)
        horizon = now + timedelta(hours=_EVENT_LOOKAHEAD_HOURS)
        out: dict[str, tuple[dict, datetime, datetime]] = {}
        self.excluded_channel_ids = set()
        schedule = self._schedule()
        # Remember sports and schools seen on the schedule (any date) for the picker.
        stamp = int(time.time())
        seen_sports = dict(self.cache.get(SEEN_SPORTS_CACHE_KEY) or {})
        seen_schools = dict(self.cache.get(SEEN_SCHOOLS_CACHE_KEY) or {})
        for b in schedule:
            seen_sports[sport_label(b)] = stamp
            home = school_name(b.get('site_title'))
            if home:
                seen_schools[home] = stamp
        cutoff = stamp - 365 * 24 * 3600
        self._update_cache(SEEN_SPORTS_CACHE_KEY, {k: v for k, v in seen_sports.items() if v >= cutoff})
        self._update_cache(SEEN_SCHOOLS_CACHE_KEY, {k: v for k, v in seen_schools.items() if v >= cutoff})
        known_schools = list(dict.fromkeys([*BASE_SCHOOLS, *sorted(seen_schools)]))
        for b in schedule:
            bid = str(b.get('id') or '')
            start = _start(b)
            if not bid.isdigit() or not start or str(b.get('status') or '').lower() == 'archived':
                continue
            end = start + _duration(b)
            if is_streaming(b) and end <= now:
                end = now + _LIVE_EXTENSION
            if end <= now or start >= horizon:
                continue
            if not is_available(b):
                continue
            if not event_matches_filter(b, self.config, known_schools):
                self.excluded_channel_ids.add(f'{EVENT_CHANNEL_PREFIX}{bid}')
                continue
            out.setdefault(bid, (b, start, end))
        return sorted(out.values(), key=lambda e: (e[1], e[0]['id']))

    @staticmethod
    def _label_and_title(b: dict) -> tuple[str, str]:
        league = league_for(b)
        title = ' '.join(str(b.get('title') or '').split()) or b.get('site_title') or 'NSN Live'
        return league, title

    def fetch_channels(self) -> list[ChannelData]:
        self.hide_missing_event_channels = True
        self._event_items = self._events()
        channels = []
        for b, start, end in self._event_items:
            bid = str(b['id'])
            league, title = self._label_and_title(b)
            sport, _ = sport_and_gender(b)
            channels.append(ChannelData(
                source_channel_id=f'{EVENT_CHANNEL_PREFIX}{bid}',
                # "<League> - <matchup>", like the other event sources.
                name=f'{league} - {title}'[:256],
                stream_url=f'{EVENT_SCHEME}{bid}?end={int(end.timestamp())}',
                logo_url=b.get('medium_image') or b.get('large_image'),
                category='Sports',
                stream_type='hls',
                guide_key=f'{EVENT_CHANNEL_PREFIX}{bid}',
                tags=[EVENT_CHANNEL_TAG, 'NSN'],
                sport=sport,
                league=league,
                gracenote_mode='off',
                retire_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS),
                delete_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS + 24 * 3600),
            ))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {ch.source_channel_id for ch in channels}
        items = getattr(self, '_event_items', None)
        if items is None:
            items = self._events()
        programs = []
        for b, start, end in items:
            cid = f'{EVENT_CHANNEL_PREFIX}{b["id"]}'
            if cid not in wanted:
                continue
            league, title = self._label_and_title(b)
            sport, _ = sport_and_gender(b)
            parts = [p for p in (b.get('section_title'), b.get('site_title')) if p]
            desc = (b.get('description') or '').strip() or (' · '.join(parts) + '. Live on Northeast Sports Network.')
            programs.append(ProgramData(
                source_channel_id=cid,
                title=f'{league}: {title}',
                start_time=start,
                end_time=end,
                description=desc,
                poster_url=b.get('large_image'),
                category=';'.join(x for x in ('Sports Event', sport, league) if x),
                is_live=True,
                episode_id=str(b['id']),
            ))
        return programs

    # ── playback ──

    def audit_resolve(self, raw_url: str) -> str:
        # An upcoming broadcast has no stream yet; its place in the schedule is the check.
        return raw_url

    def _broadcast(self, bid: str) -> dict | None:
        r = self.session.get(AJAX, headers=_headers(), timeout=20,
                             params={'action': 'nsn_player', 'video': bid})
        r.raise_for_status()
        items = (r.json() or {}).get('broadcasts') or []
        return items[0] if items else None

    def resolve(self, raw_url: str) -> str:
        bid, end = parse_event_url(raw_url)
        if end is not None and time.time() >= end + EVENT_END_GRACE_SECONDS:
            raise StreamDeadError('NSN broadcast has ended')
        b = self._broadcast(bid)
        if not b:
            raise StreamDeadError('NSN: broadcast not found')
        if not is_available(b):
            raise StreamDeadError('NSN: broadcast is unavailable')
        r = self.session.get(VMAP.format(bid), timeout=20,
                             headers={'User-Agent': _UA, 'Referer': EMBED.format(bid)})
        r.raise_for_status()
        try:
            root = ET.fromstring(r.text)
        except ET.ParseError as exc:
            raise RuntimeError(f'NSN: unreadable player config ({exc})') from exc
        for content in root.iter('Content'):
            url = (content.text or '').strip()
            if content.get('type') == 'live' and content.get('streaming') == 'true' and '.m3u8' in url:
                return url
        status = (b.get('status') or 'Upcoming')
        if status.lower() == 'archived':
            raise StreamDeadError('NSN broadcast has ended')
        raise RuntimeError(f'NSN: no live stream yet ({status})')

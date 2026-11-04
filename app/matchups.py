"""Full team names for event titles (#77).

Sources title games their own way, often with short names ("Man. United vs.
Roma", "UNC vs. UConn", "Celtics vs. Rockets"), which make DVR passes hard to
write: a rule on "Tar Heels" or "Manchester United" misses them. Each event
title is rebuilt from the full team names, home and away:

    "<League>: <Away> at <Home>"     most sports
    "<League>: <Home> vs. <Away>"    soccer, rugby, cricket (home first)
    "<League>: <Away> vs. <Home>"    a neutral site

A source with structured teams (ESPN's site API) passes them in directly.
Others are matched against ESPN's public scoreboard for the event's league:
a game starting within a few hours of the event with at least one of the
title's teams. A game with no match keeps its source title.
"""
from __future__ import annotations

import logging
import re
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from .sports import normalize_league, normalize_sport, sport_for_league

logger = logging.getLogger(__name__)

MATCHUP_CACHE_KEY = 'event_matchups'

_SCOREBOARD = 'https://site.api.espn.com/apis/site/v2/sports/{path}/scoreboard'
_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'
# ESPN's scoreboard days are US Eastern.
_ET = ZoneInfo('America/New_York')
_START_TOLERANCE = timedelta(hours=3)
# A game's teams never change; a miss may (teams announced later).
_MISS_RETRY_SECONDS = 6 * 3600
_CACHE_MAX_AGE_SECONDS = 10 * 24 * 3600

# Sports that name the home side first.
_HOME_FIRST_SPORTS = frozenset({'Soccer', 'Rugby', 'Cricket', 'Australian Football'})

# League (as any source names it) → ESPN scoreboards, first match wins.
# A board is "sport/league" plus optional query parameters: college boards
# list only featured games unless given the whole division's group.
_CFB = ('football/college-football?groups=80', 'football/college-football?groups=81')
_MCBB = ('basketball/mens-college-basketball?groups=50',)
_WCBB = ('basketball/womens-college-basketball?groups=50',)
_BOARDS: tuple[tuple[str, str | None, tuple[str, ...]], ...] = (
    (r"women'?s champions league|uwcl", None, ('soccer/uefa.wchampions',)),
    (r'youth league|\bu-?\d\d\b', None, ()),
    (r"women'?s super league|\bwsl\b", None, ('soccer/eng.w.1',)),
    (r'champions league|\bucl\b|\bchlg\b', None, ('soccer/uefa.champions',)),
    (r'conference league|conf\. league|\buecl\b', None, ('soccer/uefa.europa.conf',)),
    (r'europa league|\buel\b', None, ('soccer/uefa.europa',)),
    (r'\bnfl\b|national football league', None, ('football/nfl',)),
    (r'college football|\bncaaf\b|ncaa football', None, _CFB),
    (r'\bwnba\b', None, ('basketball/wnba',)),
    (r'\bnba\b|national basketball association', None, ('basketball/nba',)),
    (r"women'?s college basketball|\bncaaw\b|ncaa women'?s basketball", None, _WCBB),
    (r"men'?s college basketball|college basketball|\bncaam\b|\bncaab\b|ncaa men'?s basketball", None, _MCBB),
    (r'\bnhl\b|national hockey league', None, ('hockey/nhl',)),
    (r'\bmlb\b|major league baseball', None, ('baseball/mlb',)),
    (r'\bnwsl\b', None, ('soccer/usa.nwsl',)),
    (r'\bmls\b|major league soccer', None, ('soccer/usa.1',)),
    (r'scottish premiership', None, ('soccer/sco.1',)),
    (r'scottish championship', None, ('soccer/sco.2',)),
    (r'scottish|\bspfl\b|\bscot\b', None, ('soccer/sco.1', 'soccer/sco.2')),
    (r'fa cup', None, ('soccer/eng.fa',)),
    (r'carabao|league cup|efl cup', None, ('soccer/eng.league_cup',)),
    (r'efl championship', None, ('soccer/eng.2',)),
    (r'english football league|\befl\b', None, ('soccer/eng.2', 'soccer/eng.3', 'soccer/eng.4')),
    (r'premier league|\bepl\b', None, ('soccer/eng.1',)),
    (r'serie a|\bseri\b', None, ('soccer/ita.1',)),
    (r'la ?liga|laliga', None, ('soccer/esp.1',)),
    (r'dfb[- ]pokal', None, ('soccer/ger.dfb_pokal',)),
    (r'bundesliga', None, ('soccer/ger.1',)),
    (r'ligue 1', None, ('soccer/fra.1',)),
    (r'ligue 2', None, ('soccer/fra.2',)),
    (r'primeira liga', None, ('soccer/por.1',)),
    (r'eredivisie', None, ('soccer/ned.1',)),
    (r'liga mx|\bfmf\b', None, ('soccer/mex.1',)),
    (r'argentin', None, ('soccer/arg.1',)),
    (r'^primera a$', 'Soccer', ('soccer/col.1',)),
    (r'ekstraklasa', None, ('soccer/pol.1',)),
    # Amazon's college leagues name no sport.
    (r'^ncaa division i \(w\)$', 'Basketball', _WCBB),
    (r'^ncaa division i$', 'Basketball', _MCBB),
    (r'^ncaa division i', 'Football', _CFB),
)
_BOARD_PATTERNS = [(re.compile(pattern, re.I), sport, boards) for pattern, sport, boards in _BOARDS]
# Events a source gives a sport but no league ("Soccer", league "Other") try
# that sport's main boards. An unmapped league (ECHL, a rugby cup) doesn't:
# its games aren't on them.
_SPORT_BOARDS = {
    'Soccer': ('soccer/uefa.champions', 'soccer/uefa.europa', 'soccer/eng.1', 'soccer/eng.2', 'soccer/esp.1',
               'soccer/ita.1', 'soccer/ger.1', 'soccer/fra.1', 'soccer/por.1', 'soccer/ned.1', 'soccer/sco.1',
               'soccer/usa.1', 'soccer/mex.1', 'soccer/arg.1', 'soccer/pol.1'),
    'Basketball': ('basketball/nba', 'basketball/wnba', *_MCBB, *_WCBB),
    'Football': ('football/nfl', *_CFB),
    'Hockey': ('hockey/nhl',),
    'Baseball': ('baseball/mlb',),
}

# One game per title: the part after a "Label: " prefix, then the one
# " - " segment holding a separator ("Veterans Classic - Penn vs. Navy").
_TEAM_SEP = re.compile(r'\s+(?:vs\.?|v\.?|at|@)\s+', re.I)
_RANK = re.compile(r'^(?:#|no\.\s*)\d+\s+', re.I)
# Words that make a different team of the same club.
_SQUAD_WORDS = frozenset({'u17', 'u18', 'u19', 'u20', 'u21', 'u23', 'women', 'womens', 'w', 'ii', 'b', 'reserves'})
# Words that don't tell clubs apart.
_FILLER_WORDS = frozenset({'fc', 'cf', 'afc', 'sc', 'ac', 'sk', 'fk', 'the', 'club', 'calcio', 'de'})


def _words(text) -> list[str]:
    text = unicodedata.normalize('NFKD', str(text or ''))
    text = ''.join(c for c in text if not unicodedata.combining(c)).casefold().replace('&', ' and ')
    return re.findall(r'[a-z0-9]+', text.replace("'", ''))


def split_teams(title: str | None) -> list[str] | None:
    """The two sides of a one-game title, or None when it isn't one."""
    text = ' '.join(str(title or '').split())
    text = text.rsplit(': ', 1)[-1]
    segments = [s for s in text.split(' - ') if _TEAM_SEP.search(s)]
    if len(segments) != 1:
        return None
    sides = _TEAM_SEP.split(segments[0])
    if len(sides) != 2:
        return None
    sides = [_RANK.sub('', side.strip()).strip() for side in sides]
    if any(not side or len(side) > 60 for side in sides):
        return None
    return sides


def scoreboards(league: str | None, sport: str | None) -> tuple[str, ...]:
    """ESPN scoreboards that list this league's games; () when none does."""
    league = normalize_league(league)
    sport = normalize_sport(sport) or sport_for_league(league)
    if not league:
        return _SPORT_BOARDS.get(sport, ())
    for pattern, want, boards in _BOARD_PATTERNS:
        if (want is None or want == sport) and pattern.search(league):
            return boards
    return ()


def _side_matches(side: str, team: dict) -> bool:
    """Whether a title's side names this team: one of its names exactly, or
    each word the start of a word of its full name, in order ("Man. United" →
    Manchester United, "Jacksonville St" → Jacksonville State)."""
    words = [w for w in _words(side) if w not in _FILLER_WORDS]
    full = [w for w in _words(team.get('displayName')) if w not in _FILLER_WORDS]
    if not words or (set(words) & _SQUAD_WORDS) - set(full):
        return False
    for key in ('displayName', 'shortDisplayName', 'location', 'name', 'abbreviation'):
        if words == [w for w in _words(team.get(key)) if w not in _FILLER_WORDS]:
            return True
    i = 0
    for word in words:
        while i < len(full) and not full[i].startswith(word):
            i += 1
        if i == len(full):
            return False
        i += 1
    return True


def matchup_body(matchup: dict, sport: str | None) -> str:
    """"Away at Home", or home first where that's the sport's convention."""
    away, home = matchup['away'], matchup['home']
    if matchup.get('neutral'):
        return f'{away} vs. {home}'
    if normalize_sport(sport) in _HOME_FIRST_SPORTS:
        return f'{home} vs. {away}'
    return f'{away} at {home}'


def event_label(league: str | None, sport: str | None) -> str | None:
    """The title's leading label: the league, plus the sport when the league
    doesn't name one ("NCAA Division I" → "NCAA Division I Basketball")."""
    league = normalize_league(league)
    sport = normalize_sport(sport)
    if league and sport and league.casefold().startswith('ncaa') and not sport_for_league(league):
        women = re.search(r'\s*\(w\)$', league, re.I)
        league = league[:women.start()] + " Women's" if women else league
        return f'{league} {sport}'
    return league or sport


def matchup_title(label: str | None, matchup: dict, sport: str | None) -> str:
    body = matchup_body(matchup, sport)
    return f'{label}: {body}' if label else body


def matchup_description(matchup: dict, sport: str | None, description: str | None) -> str:
    """The description, led by both full names unless it already has them."""
    body = matchup_body(matchup, sport)
    description = (description or '').strip()
    folded = description.casefold()
    if matchup['away'].casefold() in folded and matchup['home'].casefold() in folded:
        return description
    return f'{body}. {description}' if description else f'{body}.'


def team_name(display_name: str | None, location: str | None) -> str:
    """ESPN's full team name for a title. Opponents outside Division I come
    as the school plus a capitalized placeholder ("Regis University REGIS
    UNIVERSITY", "Ottawa (KS) OTTAWA (KS)"); those read as just the school."""
    name, location = (display_name or '').strip(), (location or '').strip()
    rest = name[len(location):].strip() if location and name.startswith(location) else ''
    if rest and rest.isupper() and len(re.sub(r'[^A-Z]', '', rest)) > 3:
        return location
    return name


def _competitor(comp: dict, side: str) -> str | None:
    team = next((c for c in comp.get('competitors') or [] if c.get('homeAway') == side), {}).get('team') or {}
    return team_name(team.get('displayName'), team.get('location')) or None


class MatchupResolver:
    """Looks up events' teams on ESPN's scoreboards, remembering each event's
    result in a source cache key (MATCHUP_CACHE_KEY) between scrapes."""

    def __init__(self, cache: dict | None, source: str = ''):
        now = time.time()
        self.source = source
        self.cache = {key: entry for key, entry in (cache or {}).items()
                      if isinstance(entry, dict) and now - float(entry.get('at') or 0) < _CACHE_MAX_AGE_SECONDS}
        self._boards: dict[tuple[str, str], list | None] = {}
        self.games = 0
        self.found = 0

    def _board(self, board: str, day: str) -> list | None:
        """A scoreboard's events for one Eastern day; None when it failed."""
        key = (board, day)
        if key not in self._boards:
            path, _, query = board.partition('?')
            params = dict(p.split('=', 1) for p in query.split('&') if '=' in p)
            try:
                r = requests.get(_SCOREBOARD.format(path=path), timeout=15, headers={'User-Agent': _UA},
                                 params={'dates': day, 'limit': 500, **params})
                r.raise_for_status()
                self._boards[key] = (r.json() or {}).get('events') or []
            except Exception as exc:  # noqa: BLE001 — a failed board only means short titles
                logger.debug('[%s] ESPN scoreboard %s %s failed: %s', self.source, board, day, exc)
                self._boards[key] = None
        return self._boards[key]

    def lookup(self, key: str, title: str | None, start: datetime | None,
               league: str | None = None, sport: str | None = None) -> dict | None:
        """{'away', 'home', 'neutral'} for one event, or None."""
        sides = split_teams(title)
        boards = scoreboards(league, sport)
        if not sides or not boards or not start:
            return None
        self.games += 1
        # A new title (teams announced, a fixed typo) is a new lookup.
        key = f'{key}|{title}'
        now = time.time()
        entry = self.cache.get(key)
        if entry and (entry.get('m') or now - float(entry.get('at') or 0) < _MISS_RETRY_SECONDS):
            self.found += bool(entry.get('m'))
            return entry.get('m')
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        days = sorted({(start + offset).astimezone(_ET).strftime('%Y%m%d')
                       for offset in (-_START_TOLERANCE, timedelta(0), _START_TOLERANCE)})
        best: dict[str, tuple[int, dict]] = {}
        complete = True
        for board in boards:
            for day in days:
                events = self._board(board, day)
                if events is None:
                    complete = False
                    continue
                for event in events:
                    try:
                        when = datetime.fromisoformat(str(event.get('date')).replace('Z', '+00:00'))
                        comp = (event.get('competitions') or [])[0]
                    except (ValueError, IndexError):
                        continue
                    if abs(when - start) > _START_TOLERANCE:
                        continue
                    teams = [(c.get('team') or {}) for c in comp.get('competitors') or []]
                    hits = sum(1 for side in sides if any(_side_matches(side, team) for team in teams))
                    if hits and hits > best.get(str(event.get('id')), (0,))[0]:
                        best[str(event.get('id'))] = (hits, comp)
        ranked = sorted(best.values(), key=lambda item: -item[0])
        matchup = None
        # Two games matching equally well (a team listed twice) is no answer.
        if ranked and (len(ranked) == 1 or ranked[1][0] < ranked[0][0]):
            comp = ranked[0][1]
            home, away = _competitor(comp, 'home'), _competitor(comp, 'away')
            if home and away:
                matchup = {'away': away, 'home': home, 'neutral': bool(comp.get('neutralSite'))}
        if matchup or complete:
            self.cache[key] = {'m': matchup, 'at': int(now)}
        self.found += bool(matchup)
        return matchup

    def export(self) -> dict:
        if self.games:
            logger.info('[%s] full team names for %d of %d games', self.source, self.found, self.games)
        return self.cache

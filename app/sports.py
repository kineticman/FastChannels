"""One sport vocabulary for event channels, whatever the source calls it.

Each source names sports its own way (ESPN "Hockey", Amazon "Ice Hockey",
CBS "SOCCER"), so a feed filtering on sport needs one name per sport.
normalize_sport() maps a source's own label to that name, sport_for_league()
names the sport of a league when the source gives only the league, and
infer_sport() is the last resort for sources that give neither.

None means "unknown"; nothing here guesses from team names alone.
"""
from __future__ import annotations

import re

# Source labels that name no sport.
_NO_SPORT = {'', 'other', '(uncategorized)', 'uncategorized', 'live tv', 'sports', 'sport', 'none'}

# Exact (case-insensitive) aliases. Anything not listed keeps its own name,
# so a sport we haven't seen still shows up and can be filtered on.
_ALIASES = {
    'ice hockey': 'Hockey',
    'american football': 'Football',
    'futbol': 'Soccer',
    'fútbol': 'Soccer',
    'association football': 'Soccer',
    'motorsport': 'Motorsports',
    'motor sports': 'Motorsports',
    'auto racing': 'Motorsports',
    'motor racing': 'Motorsports',
    'mixed martial arts': 'MMA',
    'professional wrestling': 'Pro Wrestling',
    'athletics': 'Track and Field',
    'track & field': 'Track and Field',
}

# Whole words, case-insensitive, first match wins: the more specific sports
# come before the ones whose words they contain (field hockey before hockey,
# Australian football and soccer's league names before football, horse
# racing before motorsports). "Racing" and "wrestling" alone stay ambiguous.
# ESPN's bare NCAAM/NCAAW mean basketball, but "NCAAW Soccer" names its sport,
# so they come last.
_SPORT_KEYWORDS = (
    (r'volleyball', 'Volleyball'), (r'softball', 'Softball'), (r'baseball|mlb|milb', 'Baseball'),
    (r'basketball|w?nba', 'Basketball'), (r'field hockey', 'Field Hockey'),
    (r'hockey|nhl|ahl|echl|qmjhl|pwhl', 'Hockey'),
    (r'billiards?|snooker', 'Billiards'), (r'horse racing|thoroughbred|harness racing|day at the races', 'Horse Racing'),
    (r'nascar|formula (?:1|one)|f1|indycar|motogp|motorsports?|auto racing|supercross|motocross', 'Motorsports'),
    (r'lumberjacks?|timbersports', 'Timbersports'), (r'padel', 'Padel'), (r'pickleball', 'Pickleball'),
    (r'lacrosse', 'Lacrosse'), (r'rugby', 'Rugby'), (r'cricket', 'Cricket'), (r'squash', 'Squash'),
    (r'tennis|atp|wta', 'Tennis'), (r'golf|pga|lpga', 'Golf'), (r'boxing', 'Boxing'),
    (r'mma|ufc|mixed martial arts|bellator', 'MMA'), (r'wwe|aew|pro wrestling', 'Pro Wrestling'),
    (r'darts', 'Darts'), (r'bowling', 'Bowling'), (r'cycling|mountain bike|mtb', 'Cycling'),
    (r'australian football|afl', 'Australian Football'),
    (r'soccer|mls|nwsl|usl|futbol|fútbol|premier league|la ?liga|serie a|bundesliga|ligue 1|liga mx|'
     r'uefa|fifa|concacaf|champions league|europa league|english football league|efl|carabao cup|league cup|fa cup|'
     r'scottish professional football league|spfl|women\'s super league|wsl', 'Soccer'),
    (r'football|nfl|ncaaf|cfl|ufl', 'Football'),
    (r'ncaa[mw]', 'Basketball'),
)
_SPORT_PATTERNS = [(re.compile(rf'\b(?:{pattern})\b', re.I), name) for pattern, name in _SPORT_KEYWORDS]

# Offered by the feed editor even when no event of that sport is listed yet,
# so a feed can be set up before its first game. Sources may report others.
KNOWN_SPORTS = tuple(sorted(
    {name for _, name in _SPORT_KEYWORDS} | {'Swimming', 'Track and Field', 'Wrestling'},
    key=str.casefold,
))


def normalize_sport(name: str | None) -> str | None:
    """The canonical name for a source's sport label, or None for no sport."""
    text = ' '.join(str(name or '').split())
    key = text.casefold()
    if key in _NO_SPORT:
        return None
    if key in _ALIASES:
        return _ALIASES[key]
    # "SOCCER" → "Soccer"; mixed case ("MMA", "Track and Field") is kept.
    return text.title() if text.isupper() and len(text) > 3 else text


def normalize_league(name: str | None) -> str | None:
    """A league as the source names it, or None for a placeholder like "Other"."""
    text = ' '.join(str(name or '').split())
    return None if text.casefold() in _NO_SPORT else text


def infer_sport(texts) -> str | None:
    """The sport most of these texts name, by keyword; None when none does.

    Each text counts once, for its first matching sport.
    """
    counts: dict[str, int] = {}
    for text in texts:
        for pattern, name in _SPORT_PATTERNS:
            if pattern.search(text or ''):
                counts[name] = counts.get(name, 0) + 1
                break
    return max(counts, key=counts.get) if counts else None


def sport_for_league(league: str | None) -> str | None:
    """The sport a league is played in, when its name says so.

    "NHL", "ECHL Hockey", "Serie A" and "NCAA Women's Volleyball" name theirs;
    "NCAA Division I" doesn't, and gets None.
    """
    league = normalize_league(league)
    return infer_sport([league]) if league else None

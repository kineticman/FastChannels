# app/scrapers/category_utils.py
#
# Shared name-based category inference and normalization for FAST channel scrapers.
#
# Usage:
#   from .category_utils import infer_category_from_name, normalize_category
#
#   category = infer_category_from_name(channel_name)  # returns str | None
#   category = normalize_category(raw_category)        # maps dirty → canonical

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical category list
# ---------------------------------------------------------------------------
# Every category stored in the DB should be one of these strings.
CANONICAL_CATEGORIES: tuple[str, ...] = (
    'Action & Adventure',
    'Ambiance',
    'Anime',
    'Automotive',
    'Classic TV',
    'Comedy',
    'Documentary',
    'Drama',
    'Entertainment',
    'Faith',
    'Food',
    'Game Shows',
    'Gaming',
    'History',
    'Home & DIY',
    'Horror',
    'International',
    'Kids',
    'Latino',
    'Lifestyle',
    'Local News',
    'Movies',
    'Music',
    'Nature',
    'News',
    'Outdoors',
    'Reality TV',
    'Sci-Fi',
    'Science',
    'Shopping',
    'Sports',
    'Travel',
    'True Crime',
    'Westerns',
)

# ---------------------------------------------------------------------------
# Normalization map — lowercase raw value → canonical label
# ---------------------------------------------------------------------------
# Any category NOT in this map and NOT already in CANONICAL_CATEGORIES is
# passed through unchanged (future-proofing for new scrapers).
_CANONICAL_MAP: dict[str, str] = {
    # Action & Adventure
    'action':                       'Action & Adventure',
    'action & drama':               'Action & Adventure',
    'action/adventure':             'Action & Adventure',

    # Anime
    'anime & gaming':               'Anime',

    # Classic TV
    'classics':                     'Classic TV',

    # Documentary
    'documentaries':                'Documentary',
    'factual':                      'Documentary',

    # Drama
    'tv dramas':                    'Drama',

    # Vizio WatchFree+ compound categories
    'kids + family':                'Kids',
    'food + travel':                'Food',
    'nature + science':             'Nature',
    'history + docs':               'History',
    'inspiration + faith':          'Faith',
    'westerns + classics':          'Classic TV',
    'news + opinion':               'News',
    'crime + thriller':             'True Crime',
    'mood + ambiance':              'Ambiance',
    'culture + lifestyle':          'Lifestyle',
    'en español':                   'Latino',
    'home':                         'Home & DIY',
    'featured':                     'Entertainment',

    # Entertainment — misc catch-alls and source artifacts
    'tv':                           'Entertainment',   # Vizio WatchFree+ generic bucket
    'general':                      'Entertainment',
    'pop culture':                  'Entertainment',
    'black entertainment':          'Entertainment',
    'recommended':                  'Entertainment',
    'her stories':                  'Entertainment',
    'new on local now':             'Entertainment',
    'more cities':                  'Entertainment',
    'live tv':                      'Entertainment',
    'daytime tv':                   'Entertainment',
    'talk show':                    'Entertainment',

    # Faith
    'faith & family':               'Faith',
    'family and faith':             'Faith',

    # Food
    'cooking':                      'Food',
    'good eats':                    'Food',
    'quality eats':                 'Food',

    # Game Shows
    'game show':                    'Game Shows',
    'games & competition':          'Game Shows',
    'daytime + game shows':         'Game Shows',

    # History
    'history & learning':           'History',
    'history + science':            'History',

    # Home & DIY
    'home & design':                'Home & DIY',

    # Home & Food — lifestyle-oriented combo, mapped to Lifestyle
    'home & food':                  'Lifestyle',
    'home + food':                  'Lifestyle',

    # Horror (combos leading with Horror)
    'horror & sci-fi':              'Horror',
    'horror and scifi':             'Horror',
    'april ghouls':                 'Horror',

    # International
    'bollywood':                    'International',

    # Kids
    'kids & family':                'Kids',
    'family':                       'Kids',

    # Latino — all Spanish-language genre variants
    'en espanol':                   'Latino',
    'en español':                   'Latino',
    'español':                      'Latino',
    'spanish':                      'Latino',
    'spanish language':             'Latino',
    'latin':                        'Latino',

    # Lifestyle
    'lifestyle & pop culture':      'Lifestyle',

    # Movies
    'movie channels':               'Movies',
    'movies and tv':                'Movies',
    'tv & movies':                  'Movies',

    # Music
    'music & radio':                'Music',
    'music video':                  'Music',
    'music videos':                 'Music',

    # Nature
    'animals & nature':             'Nature',
    'animals + nature':             'Nature',
    'nature and outdoors':          'Nature',
    'science & nature':             'Nature',
    'nature, history & science':    'Science',

    # Local News
    'local channels':               'Local News',   # Vizio WatchFree+ category

    # News
    'national news':                'News',
    'global news':                  'News',
    'news & opinion':               'News',
    'news + opinion':               'News',
    'news and opinion':             'News',
    'business news':                'News',
    'business':                     'News',
    'weather':                      'News',

    # Outdoors
    'sports & outdoors':            'Outdoors',

    # Reality TV
    'reality':                      'Reality TV',
    'reality competition':          'Reality TV',
    'competition reality':          'Reality TV',
    'competition and reality':      'Reality TV',

    # Sci-Fi (combos leading with Sci-Fi)
    'sci-fi & horror':              'Sci-Fi',
    'sci-fi & supernatural':        'Sci-Fi',
    'science fiction':              'Sci-Fi',

    # Science
    'technology':                   'Science',

    # Sports
    'motor sports':                 'Sports',
    'combat sports':                'Sports',
    'sports on now':                'Sports',

    # Travel
    'travel & lifestyle':           'Travel',

    # True Crime
    'crime':                        'True Crime',
    'crime tv':                     'True Crime',
    'mystery':                      'True Crime',
    'thriller':                     'True Crime',

    # Westerns
    'western':                      'Westerns',
    'western & classic tv':         'Westerns',
    'westerns & country':           'Westerns',
}


_PRESENTED_BY_RE = re.compile(r'\s+presented\s+by\s+.+$', re.IGNORECASE)
_CANONICAL_LOWER: dict[str, str] = {}  # populated lazily on first use


def normalize_category(raw: str | None) -> str | None:
    """Map a raw scraper category string to a canonical category label.

    Priority:
      1. Explicit alias in _CANONICAL_MAP
      2. Already a canonical category (case-insensitive pass-through)
      3. Unknown → None  (logged at DEBUG so new values are visible in logs)
    """
    if not raw:
        return raw

    # Strip sponsor suffixes like "Quality Eats Presented by Capital One".
    cleaned = _PRESENTED_BY_RE.sub('', raw.strip()).strip()
    key = cleaned.lower()

    # 1. Explicit alias
    if key in _CANONICAL_MAP:
        return _CANONICAL_MAP[key]

    # 2. Already canonical
    if not _CANONICAL_LOWER:
        _CANONICAL_LOWER.update({c.lower(): c for c in CANONICAL_CATEGORIES})
    if key in _CANONICAL_LOWER:
        return _CANONICAL_LOWER[key]

    # 3. Unknown — discard rather than let garbage through
    logger.debug("normalize_category: unrecognized value %r (raw: %r)", cleaned, raw)
    return None


# ---------------------------------------------------------------------------
# Name-based category inference
# ---------------------------------------------------------------------------
# Each entry: (set-of-substrings, canonical category label).
# All comparisons are lowercased before matching.
# Rules are checked in order; the first keyword match wins.
_NAME_CATEGORY_RULES: list[tuple[set[str], str]] = [
    # Sports — checked before News so "CBS Sports" doesn't fall through
    ({
        'sport', 'deportes',
        'nfl', 'nba ', 'nhl', 'mlb', 'nascar', 'nhra', 'pga tour',
        'ufc', 'mma', 'tennis', 'golf', 'wrestling', 'boxing', 'ringside',
        'billiard', 'pickleball', 'bassmaster', 'x games', 'pbr:',
        'motocross', 'f1 channel', 'espn', 'fubo', 'fanduel tv', 'fanduel',
        'draftkings', 'sportsgrid', 'speed sport', 'swerve combat',
        'swerve women', 'hbo boxing', 'one championship', 'pfl mma',
        'dazn', 'top rank', 'lucha plus', 'big 12 studios', 'acc digital',
        'red bull tv', 'outside tv', 'myoutdoortv', 'racer select',
        'racing america', 'top barça', 'uefa', 'fifa+', 'pursuitup',
        'rig tv', 'monster jam', 'hong kong fight', 'hi-yah',
        'american ninja', 'american gladiator', 'meateater',
        'nesn', 'overtime', 'fuel tv', 'team usa tv', 'fear factor',
        'jim rome',
        'nhl network', 'nbc sports', 'bowling', 'poker', 'surf league',
        'baseball tv', 'beinsport', 'bein sport', 'sportsnet', 'willow sport',
    }, 'Sports'),
    # Music
    ({
        'iheart', 'vevo', 'stingray', 'tiktok radio', 'revolt mixtape',
        'circle country', 'electric now', 'mvstv', 'lamusica', 'lamúsica',
        'musica tv', 'música tv', 'fuse +',
        'bet pluto', 'mtv pluto',
        'mtv ', 'concerts by stingray', 'country network',
        'b4u music', 'yrf music', 'raj mus', 'saga music', 'aghani',
    }, 'Music'),
    # Local News — checked before general News so local stations don't fall through
    ({
        'local now',           # Local Now city-specific channels (all markets)
        'abc7', 'abc13', 'abc30', 'abc6 ', 'abc11',
        'kiro 7', 'wpxi', 'wsb ', 'wsoc', 'wftv', 'wapa+',
        "arizona's family", 'first alert',
        'abc localish',
        'news center',         # e.g. News Center Maine
    }, 'Local News'),
    # News / Weather
    ({
        'news', 'noticias', 'weather', 'cnn', 'fox local',
        'usa today', 'the hill', 'tyt-go', 'newsmax', 'oan plus',
        'liveno', 'scripps', 'rcn noticias', 'telemundo al día',
        'telemundo ahora', 'fuerza informativa', 'telediario',
        'inside edition',
        'cheddar', 'bloomberg', 'euronews', 'france 24',
        'al jazeera', 'al arabiya', 'al hadath', 'al araby',
    }, 'News'),
    # True Crime & Mystery
    ({
        'crime', 'mystery', 'court tv', 'cold case', 'first 48', 'cops',
        'jail', 'law & crime', 'forensic files', 'dateline', 'live pd',
        'to catch a', 'american crimes', 'trublu', 'total crime',
        'unsolved', 'i (almost)', 'living with evil', 'dr. g:',
        'chaos on cam', 'untold stories of the er',
        'murder she wrote', 'mysteria', 'mysterious', 'caught in providence',
        'confess by nosey', 'paternity court', 'ghost hunter',
        '48 hours', '20/20',
        'murder', 'killer', 'chasing criminal', 'bounty hunter',
    }, 'True Crime'),
    # Horror
    ({
        'horror', 'scary', 'screambox', 'haunt', 'fear zone', 'dark fears',
        'cine de horror', 'scares by shudder', 'universal monsters',
        'z nation', 'unxplained', 'ghosts are real', 'survive or die',
    }, 'Horror'),
    # Sci-Fi
    ({
        'sci-fi', 'star trek', 'stargate', 'outersphere', 'space & beyond',
        'alien nation', 'sci fi', 'doctor who', 'pluto tv fantastic',
    }, 'Sci-Fi'),
    # Anime
    ({
        'anime', 'crunchyroll', 'retrocrush', 'retro crush', 'yu-gi-oh',
        'hidive',
    }, 'Anime'),
    # Food & Cooking
    ({
        'food network', 'tastemade', 'cooking', 'kitchen', 'chef',
        'emeril', 'jamie oliver', 'bon appetit', 'pbs food',
        "america's test kitchen", 'bobby flay', 'martha stewart',
        'great british baking', 'bbc food', 'delicious eats',
        'gusto', 'foodxp',
    }, 'Food'),
    # Nature & Wildlife
    ({
        'nature', 'wildlife', 'wildearth', 'love nature', 'jack hanna',
        'naturaleza', 'national geographic', 'wicked tuna', 'life below zero',
        'dog whisperer', 'incredible dr. pol', 'paws & claws',
        'magellan', 'curiosity', 'earthday', 'love the planet',
        'bbc earth', 'real disaster', 'pet collective',
        'earth touch', 'wild ocean', 'terra mater',
    }, 'Nature'),
    # Home & DIY
    ({
        'this old house', 'home & diy', 'home crashers', 'homeful',
        'chip & jo', 'gardening', 'tiny house', 'home improvement',
        'powernation', 'inside outside', 'at home with', 'rustic retreat',
        'home.made', 'ultimate builds', 'bbc home & garden', 'repair shop',
    }, 'Home & DIY'),
    # Reality TV
    ({
        'real housewives', 'bravo vault', 'bridezillas', 'braxton family',
        'dance moms', 'jersey shore', 'love & hip hop', 'love after lockup',
        'million dollar listing', 'project runway', 'say yes to the dress',
        'storage wars', 'teen mom', 'bad girls club', 'growing up hip hop',
        'all reality', 'reality rocks', 'pawn stars', 'duck dynasty',
        'survivor', 'the challenge', 'shark tank', 'deal or no deal',
        'supermarket sweep', 'supernanny', 'the masked singer',
        'extreme makeover', 'extreme jobs', 'bachelor nation',
        "dallas cowboys cheerleader", 'world of love island',
        'matched married', 'ax men', 'ice road trucker', 'dog the bounty',
        'the amazing race', 'e! keeping up', 'cheaters',
        'divorce court', 'judge nosey', 'the judge judy channel',
        'judge judy', 'dr. phil', 'the doctors',
        'caso cerrado', 'ellen channel', 'nosey',
    }, 'Reality TV'),
    # Game Shows
    ({
        'game show', 'price is right', 'family feud', 'buzzr',
        "let's make a deal", 'who wants to be a millionaire',
        'celebrity name game',
    }, 'Game Shows'),
    # Comedy
    ({
        'comedy', 'laugh', 'lol network', 'just for laughs', 'sitcom',
        'snl vault', 'portlandia', 'get comedy', 'laff',
        'funniest home video', 'mst3k', 'failarmy', "wild 'n out",
        'national lampoon', 'pink panther', 'johnny carson',
        'carol burnett', 'anger management',
        'cheers + frasier', 'cougar town', 'according to jim',
        'are we there yet', 'saved by the bell', 'my wife and kids',
        'the conners', 'bernie mac', 'dick van dyke', 'life with derek',
        'blossom', 'seinfeld', 'the goldbergs', 'leave it to beaver',
        'ed sullivan', 'the red green channel',
        'funny', 'gags', 'always funny', 'comedy central',
    }, 'Comedy'),
    # Kids & Family
    ({
        'kids', 'family', 'children',
        'dino', 'animation+', 'animation +',
        'junior', 'jr.', 'cartoon', 'barney', 'dinos 24',
        'my little pony', 'strawberry shortcake', 'power rangers',
        'kartoon', 'pocket.watch', 'baby',
    }, 'Kids'),
    # Drama & Soaps
    ({
        'drama', 'primetime soaps', 'lifetime love', 'lifetime movie',
        'hallmark', 'tv land drama', 'tv amor', 'kanal d drama',
        'novela', 'supernatural drama', 'general hospital',
        'law & order', 'nypd blue', 'csi', 'the practice',
        'the walking dead', 'silent witness', 'midsomer', 'felicity',
        'degrassi', 'baywatch', 'beverly hills 90210', 'xena',
        'nash bridges', 'bull ', 'heartland classic', 'acorn tv',
        'britbox', 'sundance now',
        'cw forever', 'cw gold', 'allblk', 'alfred hitchcock',
        'tyler perry', 'in the heat of the night', 'tribeca',
        'shout factory',
    }, 'Drama'),
    # Movies
    ({
        'movies', 'movie', 'cinema', 'film', 'cinevault', 'miramax',
        'mgm', 'filmrise', 'samuel goldwyn', 'gravitas', 'asylum',
        'lionsgate', 'paramount movie', 'universal action', 'universal crime',
        'universal westerns', 'xumo free', 'just movies', 'cine',
        'filmex', 'great american rom', 'my time movie', 'cinépolis',
        'maverick black cinema', 'pam grier',
        'amc+', 'kino lorber', 'blackpix', 'shades of black',
        'cinemax', 'mgm+', 'mgm plus', 'ifc', 'sundance channel',
    }, 'Movies'),
    # Westerns
    ({
        'western', 'gunsmoke', 'wild west', 'lone ranger', 'virginian',
        'classic movie western',
    }, 'Westerns'),
    # Faith & Inspiration
    ({
        'dove channel', 'osteen', 'up faith', 'aspire', 'highway to heaven',
        'little house',
        'holiday', 'christmas', 'lifestyle',
        'tbn', 'quran', 'bhajan', 'dharm', 'faith & family', 'christian',
        'padre pio', 'noursat', 'aastha',
    }, 'Faith'),
    # Travel & Adventure
    ({
        'travel', 'adventure', 'exploration', 'xplore', 'places & spaces',
        'no reservations', 'bizarre foods', 'highway thru hell',
        'locked up abroad',
        'voyage', 'travelxp', 'go traveler', 'tv5monde voyage',
    }, 'Travel'),
    # History — checked before Science so "history" keyword routes correctly
    ({
        'history', 'smithsonian', 'ancient aliens', 'modern marvels',
        'military heroes', 'history & warfare', 'combat war',
        'antiques roadshow', 'american pickers',
    }, 'History'),
    # Documentary
    ({
        'docu', 'docurama', 'magellan tv', 'pbs genealogy',
        'documentary', 'curiosity stream', 'nhk', 'get factual', 'pbs',
    }, 'Documentary'),
    # Science
    ({
        'science', 'mythbusters', 'science is amazing', 'science quest',
        'modern innovations', 'classic car auction',
    }, 'Science'),
    # Gaming & Esports
    ({
        'gaming', 'esports', 'league of legends', 'fgteev', 'unspeakable',
        'mrbeast', 'mythical', 'team liquid',
    }, 'Gaming'),
    # Automotive
    ({
        'top gear', 'torque tv', 'mecum', 'discovery turbo',
        'in the garage', 'car chase', 'motortrend', 'velocity',
        'roadkill channel', 'hot rod',
    }, 'Automotive'),
    # Outdoors
    ({
        'outdoor', 'waypoint tv', 'wired2fish', 'xtreme outdoor',
    }, 'Outdoors'),
    # Latino — name-based fallback for channels without a language tag
    ({
        'flixlatino', 'vix ', 'vix+', 'canela.tv', 'canela tv',
        'venevisión', 'novelísima', 'novelisima',
        'remezcla', 'en español', 'atresplayer', 'pitufo',
        'mi raza', 'sobreviví', 'sobrevivi', 'c4 en alerta',
        'telemundo acción', 'telemundo accion', 'telemundo puerto',
        'emoción atres', 'emocion atres', 'única tv', 'unica tv',
        'cine exclusivo', 'azteca', 'univision', 'canal estrellas',
        'imagen tv', 'tvnotas', 'bandamax', 'ritmoson',
    }, 'Latino'),
    # Shopping
    ({
        'qvc', 'hsn', 'jewelry television', 'deal zone', 'shopping',
        'amazon live',
    }, 'Shopping'),
]


# ---------------------------------------------------------------------------
# Name-based hard overrides
# ---------------------------------------------------------------------------
# Maps lowercase channel name → canonical category.  These take priority over
# whatever category the scraper reports, so scrape runs won't undo manual
# corrections.  Add entries here whenever an audit finds a channel that is
# consistently miscategorized by its upstream source.
_NAME_OVERRIDES: dict[str, str] = {
    # ── Action & Adventure ───────────────────────────────────────────────────
    'universal action':                     'Action & Adventure',
    'xena':                                 'Action & Adventure',
    'xena warrior princess':               'Action & Adventure',
    'the outpost':                          'Action & Adventure',

    # ── Anime ────────────────────────────────────────────────────────────────
    'hunter x hunter':                      'Anime',
    'naruto':                               'Anime',
    'one piece':                            'Anime',
    'boruto: naruto next generations':      'Anime',
    'inuyasha':                             'Anime',
    "jojo's bizarre adventure":             'Anime',
    'sailor moon':                          'Anime',
    'yu-gi-oh!':                            'Anime',
    'anime x hidive':                       'Anime',

    # ── Automotive ───────────────────────────────────────────────────────────
    'bbc top gear':                         'Automotive',
    'top gear':                             'Automotive',
    'the grand tour':                       'Automotive',
    'drivetribe':                           'Automotive',
    'fifth gear':                           'Automotive',
    'fifth gear (uk)':                      'Automotive',
    'hagerty':                              'Automotive',
    'torque':                               'Automotive',
    'choppertown':                          'Automotive',
    'classic car auctions':                 'Automotive',
    'classic car auctions by history':      'Automotive',
    'powertube tv':                         'Automotive',
    'speedvision':                          'Automotive',
    'motorvision tv':                       'Automotive',
    'motorvision tv español':               'Automotive',
    'discovery turbo tv':                   'Automotive',
    'powernation':                          'Automotive',
    'power nation':                         'Automotive',
    'car chase':                            'Automotive',
    'in the garage':                        'Automotive',
    'racer select':                         'Automotive',
    'torque presented by history':          'Automotive',
    'xtreme outdoor by history':            'Outdoors',

    # ── Classic TV ───────────────────────────────────────────────────────────
    'shout! tv':                            'Classic TV',
    'a-z best classic tv':                  'Classic TV',
    'a-z classic flix':                     'Classic TV',
    'filmrise classic tv':                  'Classic TV',
    "non-stop '90s":                        'Classic TV',
    'lassie':                               'Classic TV',
    'little house on the prairie':          'Classic TV',

    # ── Comedy ───────────────────────────────────────────────────────────────
    'the conners':                          'Comedy',
    'the marvelous mrs. maisel':            'Comedy',
    'upload':                               'Comedy',
    "wild 'n out":                          'Comedy',
    'classic tv comedy':                    'Comedy',
    'mystery science theater 3000':        'Comedy',
    'mystery science theater 3000 (mst3k)':'Comedy',
    'mst3k':                                'Comedy',
    'are we there yet':                     'Comedy',
    'the carol burnett show':               'Comedy',

    # ── Documentary ──────────────────────────────────────────────────────────
    'documentary+':                         'Documentary',
    'ted':                                  'Documentary',
    'vice':                                 'Documentary',
    'pbs genealogy':                        'Documentary',
    'real disaster channel':                'Documentary',
    'inwonder':                             'Documentary',

    # ── Drama ────────────────────────────────────────────────────────────────
    'bbc drama':                            'Drama',
    'bet x tyler perry drama':             'Drama',
    'drama life':                           'Drama',
    'tv land drama':                        'Drama',
    'murdoch mysteries':                    'Drama',
    'bosch':                                'Drama',
    'designated survivor':                  'Drama',
    'heartland':                            'Drama',
    'ion':                                  'Drama',
    'ion plus':                             'Drama',
    'las vegas':                            'Drama',
    'lawless':                              'Drama',
    'leverage':                             'Drama',
    'primetime soaps':                      'Drama',
    'the bold and the beautiful':           'Drama',
    'rings of power':                       'Drama',
    'stories by amc':                       'Drama',
    'teen wolf':                            'Drama',
    'teen wolf by mgm':                     'Drama',
    'weeds & nurse jackie':                 'Drama',
    'weeds and nurse jackie':               'Drama',
    'nash bridges':                         'Drama',
    'nashville':                            'Drama',
    'nikita':                               'Drama',
    'nip/tuck':                             'Drama',
    'nip / tuck':                           'Drama',
    'spooks (mi-5)':                        'Drama',
    'mi-5':                                 'Drama',
    'law & order':                          'Drama',
    'midsomer murders':                     'Drama',
    'murder, she wrote':                    'Drama',
    'silent witness and new tricks':        'Drama',
    'silent witness & new tricks':          'Drama',
    'silent witness|new tricks':            'Drama',
    "mcleod's daughters":                   'Drama',
    'britbox mysteries':                    'Drama',
    'series k edge':                        'Drama',
    'series k heart':                       'Drama',
    'series k legacy':                      'Drama',
    'k-drama by cj enm':                   'Drama',
    'k-drama+':                             'Drama',
    'k-stories by cj enm':                 'Drama',
    'kocowa k-drama':                       'Drama',
    'genie k-drama':                        'Drama',
    'baywatch':                             'Drama',

    # ── Faith ────────────────────────────────────────────────────────────────
    'byu tv':                               'Faith',
    'byutv':                                'Faith',
    'daystar tv':                           'Faith',
    'daystar tv - espanol':                 'Faith',
    'elevation church':                     'Faith',
    'elevation church network':             'Faith',
    'impact gospel':                        'Faith',
    'in touch +':                           'Faith',
    'jltv':                                 'Faith',
    'rightnow tv':                          'Faith',
    'right now tv':                         'Faith',
    't.d. jakes':                           'Faith',
    'joel osteen network':                  'Faith',
    'dove':                                 'Faith',
    'daystar español':                      'Faith',

    # ── Food ─────────────────────────────────────────────────────────────────
    "america's test kitchen":               'Food',
    'bbc food':                             'Food',
    'bizarre foods with andrew zimmern':    'Food',
    "cook's country":                       'Food',
    'come dine with me':                    'Food',
    'great british menu':                   'Food',
    "cook's country channel":               'Food',
    'pbs food':                             'Food',
    'the emeril lagasse channel':           'Food',
    'the jamie oliver channel':             'Food',

    # ── Game Shows ───────────────────────────────────────────────────────────
    'deal or no deal':                      'Game Shows',
    'supermarket sweep':                    'Game Shows',

    # ── Gaming ───────────────────────────────────────────────────────────────
    'gameplay minecraft':                   'Gaming',
    'gameplay roblox':                      'Gaming',
    'estrella games':                       'Gaming',
    'dungeons & dragons adventures':        'Gaming',
    'jacksepticeye':                        'Gaming',
    'lazarbeam':                            'Gaming',
    'prestonplayz':                         'Gaming',
    'team liquid':                          'Gaming',

    # ── History ──────────────────────────────────────────────────────────────
    'history & warfare now':                'History',
    'history 365':                          'History',
    'history film channel':                 'History',
    'military heroes':                      'History',
    'modern marvels presented by history':  'History',
    'true history':                         'History',
    'the curse of oak island':              'History',
    'the unxplained with william shatner':  'History',
    'unidentified':                         'History',
    'declassified':                         'History',

    # ── Home & DIY ───────────────────────────────────────────────────────────
    'at home with family handyman':         'Home & DIY',
    'bbc home & garden':                    'Home & DIY',
    'craftsytv':                            'Home & DIY',
    'gardening with monty don':             'Home & DIY',
    'the design network':                   'Home & DIY',
    'this old house':                       'Home & DIY',
    'tiny house nation':                    'Home & DIY',
    'welcome home':                         'Home & DIY',
    'property brothers':                    'Home & DIY',
    'property brothers channel':            'Home & DIY',
    'grand designs':                        'Home & DIY',
    'my first place':                       'Home & DIY',
    'nbc lx home':                          'Home & DIY',
    'tastemade home':                       'Home & DIY',
    '5-minute crafts':                      'Home & DIY',
    'home.made.nation':                     'Home & DIY',
    'homeful':                              'Home & DIY',

    # ── Horror ───────────────────────────────────────────────────────────────
    'ghost dimension':                      'Horror',
    'ghost hunters':                        'Horror',
    'ghost hunters channel':                'Horror',
    'ghosts are real':                      'Horror',
    'ghost stories':                        'Horror',
    'the walking dead channel':             'Horror',
    'the walking dead universe':            'Horror',
    'beyond paranormal':                    'Horror',
    'paranormal files':                     'Horror',
    'screams tv':                           'Horror',
    'universal monsters':                   'Horror',
    'van helsing':                          'Horror',
    'amc thrillers':                        'Horror',
    'trailers from hell':                   'Horror',
    'watch it scream':                      'Horror',
    'dread tv':                             'Horror',
    'frightflix':                           'Horror',
    'filmrise horror':                      'Horror',
    'horror by alter':                      'Horror',
    'horror stories':                       'Horror',
    'scream factory tv':                    'Horror',
    'screambox tv':                         'Horror',
    'haunt tv':                             'Horror',
    'haunttv':                              'Horror',

    # ── Kids ─────────────────────────────────────────────────────────────────
    # Note: "Kids TV", "Kidz Bop TV", "Kung Fu Movies" and similar are caught
    # by the call-sign regex (K/W + 3 alpha chars) and routed to Local News
    # incorrectly — explicit overrides are required.
    'kids tv':                              'Kids',
    'kidz bop tv':                          'Kids',
    'ok gamer':                             'Gaming',
    'pokémon':                              'Kids',
    'super mario':                          'Kids',
    'tmnt':                                 'Kids',
    'transformers':                         'Kids',
    'he-man & the masters of the universe': 'Kids',
    'dinos 24/7':                           'Kids',
    'kiddo+':                               'Kids',
    'toon goggles en español':              'Kids',
    'sonic':                                'Kids',

    # ── Lifestyle ────────────────────────────────────────────────────────────
    'growthday network':                    'Lifestyle',
    'tony robbins network':                 'Lifestyle',
    'backstage':                            'Lifestyle',
    'antiques road trip':                   'Lifestyle',
    'pbs antiques roadshow':                'Lifestyle',
    'omstars tv':                           'Lifestyle',
    'more u':                               'Lifestyle',
    'antiques roadshow':                    'Lifestyle',
    'the bob ross channel':                 'Lifestyle',
    'the martha stewart channel':           'Lifestyle',
    'zenlife by stingray':                  'Lifestyle',

    # ── Latino ───────────────────────────────────────────────────────────────
    'box cinema':                           'Latino',
    'el rey rebel':                         'Latino',
    'latino vibes':                         'Latino',
    'todo cine':                            'Latino',

    # ── Local News ───────────────────────────────────────────────────────────
    # The call-sign regex (W/K + 3 alpha + non-alpha) catches non-news channels
    # whose names happen to start with W or K — override them explicitly:
    'kung fu movies':                       'Action & Adventure',
    'wild west tv':                         'Westerns',
    'wine watches & whiskey':               'Lifestyle',
    'witz comedy tv':                       'Comedy',
    'witz-comedy tv':                       'Comedy',
    # Stations that don't match the call-sign or network patterns above
    '9&10 news northern michigan':          'Local News',
    '12 news beaumont tx':                  'Local News',
    'erie news now':                        'Local News',
    'news center maine nbc portland-bangor me': 'Local News',
    'newsday tv long island ny':            'Local News',
    'onnj on new jersey':                   'Local News',
    'localish':                             'Local News',
    'fox 11 green bay wi 2':                'Local News',
    'fox 9 steubenville oh':                'Local News',
    'fox45 wbff baltimore':                 'Local News',
    'fox47 news lansing (wsym)':            'Local News',
    'fox 25 oklahoma city ok':              'Local News',
    'fox 26 fresno ca':                     'Local News',
    'action news jax (cbs47 / fox30)':      'Local News',
    'atlanta news first':                   'Local News',
    'boston 25 news (fox)':                 'Local News',
    'channel 3 eyewitness news':            'Local News',
    'cleveland 19 news plus':               'Local News',
    'local 12 cincinnati oh':               'Local News',
    'local now':                            'Local News',
    'news 3 nbc las vegas nv':              'Local News',
    'news 4 san antonio tx':                'Local News',
    'news 4 tucson (kvoa)':                 'Local News',
    'news 9 oklahoma city ok':              'Local News',
    'news on 6 tulsa ok':                   'Local News',
    'news10nbc rochester ny':               'Local News',
    'newschannel 13 albany ny':             'Local News',
    'spectrum news+':                       'Local News',
    'tampa bay 28':                         'Local News',
    'tmj4 news milwaukee':                  'Local News',
    'wbtv news':                            'Local News',
    'wbtv news 3':                          'Local News',
    'wcnc':                                 'Local News',
    'wn charlotte':                         'Local News',
    'wral news+ raleigh-durham':            'Local News',
    'wsmv 4 news':                          'Local News',
    'wsoc charlotte':                       'Local News',
    '10 nbc wjar providence ri':            'Local News',
    '6 news nbc johnstown pa':              'Local News',
    'apple valley news now':                'Local News',
    'argus news':                           'Local News',
    'ksl-tv -5 salt lake city ut':          'Local News',
    'news22 abc dayton oh':                 'Local News',
    'news 22 abc dayton oh':                'Local News',

    # ── Movies ───────────────────────────────────────────────────────────────
    'maverick black cinema':                'Movies',
    'encore+':                              'Movies',
    'free movies plus':                     'Movies',
    'outflix movies':                       'Movies',
    'lifetime movie favorites':             'Movies',
    'new kmovies':                          'Movies',

    # ── Music ────────────────────────────────────────────────────────────────
    'smooth jazz':                          'Music',
    'easy listening':                       'Music',
    'def jam':                              'Music',
    'circle country':                       'Music',
    'billboard tv':                         'Music',

    # ── Nature ───────────────────────────────────────────────────────────────
    'bbc travel':                           'Travel',
    'gousa tv':                             'Travel',
    'pbs nature':                           'Nature',
    'love nature 4k':                       'Nature',
    'dog whisperer':                        'Nature',
    'nat geo sharks':                       'Nature',
    'wild nature':                          'Nature',
    'paws and claws':                       'Nature',
    'paws & claws':                         'Nature',
    'love pets':                            'Nature',
    'unleashed by dogtv':                   'Nature',
    'lucky dog':                            'Nature',
    'rovr pets':                            'Nature',
    'samsung wild life':                    'Nature',
    'the pet collective':                   'Nature',
    'inwild':                               'Nature',

    # ── News ─────────────────────────────────────────────────────────────────
    'fox weather':                          'News',
    'weatherspy':                           'News',
    'cnn originals':                        'News',
    'reuters 60':                           'News',
    "real america's voice":                 'News',
    'spot on news':                         'News',
    'telemundo al dia':                     'News',
    '60 minutes':                           'News',
    'today all day':                        'News',
    'usa today':                            'News',
    'cheddar news':                         'News',
    'american stories network':             'News',
    'thegrio':                              'News',
    'noticias telemundo ahora':             'News',
    'telemundo al día':                     'News',
    'telemundo al dia':                     'News',
    'rcn noticias':                         'News',

    # ── Outdoors ─────────────────────────────────────────────────────────────
    'mlb channel':                          'Sports',
    'nfl channel':                          'Sports',
    'absinthe tv':                          'Sports',
    'the ringer from spotify':              'Sports',
    'wild tv':                              'Outdoors',
    'rvtv':                                 'Outdoors',
    'the boat show':                        'Outdoors',
    'yachting tv':                          'Outdoors',
    'game & fish tv':                       'Outdoors',
    'hunt fish tv':                         'Outdoors',
    'pursuit up':                           'Outdoors',
    'pursuitup':                            'Outdoors',

    # ── Reality TV ───────────────────────────────────────────────────────────
    "america's got talent":                 'Reality TV',
    'bad girls club':                       'Reality TV',
    'reality gone wild':                    'Reality TV',
    'the biggest loser':                    'Reality TV',
    'the apprentice':                       'Reality TV',
    'the osbournes':                        'Reality TV',
    'the girls next door':                  'Reality TV',
    'bondi rescue':                         'Reality TV',
    'highway thru hell':                    'Reality TV',
    'fear factor':                          'Reality TV',
    'fear factor usa':                      'Reality TV',
    'wipeout extra':                        'Reality TV',
    'wipeout xtra':                         'Reality TV',
    'wipeoutxtra':                          'Reality TV',
    'confess by nosey':                     'Reality TV',
    'best of dr. phil':                     'Reality TV',
    'judge nosey':                          'Reality TV',
    'nosey':                                'Reality TV',
    'divorce court':                        'Reality TV',
    'dr. phil':                             'Reality TV',
    'judge faith':                          'Reality TV',
    'paternity court':                      'Reality TV',
    'the doctors':                          'Reality TV',
    'bravo vault':                          'Reality TV',

    # ── Sci-Fi ───────────────────────────────────────────────────────────────
    'bbc sci-fi':                           'Sci-Fi',
    'filmrise sci-fi':                      'Sci-Fi',
    'farscape':                             'Sci-Fi',
    'stargate by mgm':                      'Sci-Fi',
    'the outer limits':                     'Sci-Fi',
    'z nation':                             'Sci-Fi',
    'classic doctor who':                   'Sci-Fi',
    'ancient aliens':                       'Sci-Fi',
    'mysterious worlds':                    'Sci-Fi',
    'unxplained zone':                      'Sci-Fi',

    # ── Science ──────────────────────────────────────────────────────────────
    'mythbusters':                          'Science',
    'pluto tv science':                     'Science',
    'robot wars by mech+':                  'Science',
    'startalk':                             'Science',
    'startalk tv':                          'Science',
    'popular science':                      'Science',
    'nasa+':                                'Science',

    # ── Shopping ─────────────────────────────────────────────────────────────
    'hsn':                                  'Shopping',
    'qvc':                                  'Shopping',
    'qvc2':                                 'Shopping',
    'shop lc':                              'Shopping',
    'shoplc':                               'Shopping',
    'jtv jewelry love':                     'Shopping',

    # ── Sports ───────────────────────────────────────────────────────────────
    'acc digital network':                  'Sports',
    'big 12 studios':                       'Sports',
    'billiard tv':                          'Sports',
    'cbs sports hq':                        'Sports',
    'combate global mma':                   'Sports',
    'draftkings network':                   'Sports',
    "espn8: the ocho":                      'Sports',
    'fifa+':                                'Sports',
    'fifa +':                               'Sports',
    'fox sports':                           'Sports',
    'fanduel tv extra':                     'Sports',
    'golfpass':                             'Sports',
    'msg sportszone':                       'Sports',
    'milb':                                 'Sports',
    'nbc sports now':                       'Sports',
    'nesn nation':                          'Sports',
    'pac-12 insider':                       'Sports',
    'pickleballtv':                         'Sports',
    'pickletv':                             'Sports',
    'roku sports channel':                  'Sports',
    'slvr':                                 'Sports',
    'surfer tv':                            'Sports',
    'sportsgrid':                           'Sports',
    'stadium':                              'Sports',
    'tna wrestling':                        'Sports',
    'team usa tv':                          'Sports',
    'the jim rome show':                    'Sports',
    'the nba channel':                      'Sports',
    'ufc':                                  'Sports',
    'unbeaten sports channel':              'Sports',
    'victory+':                             'Sports',
    "women's sports network":               'Sports',
    'world poker tour':                     'Sports',
    "yahoo! sports network":                'Sports',
    'bein sports xtra':                     'Sports',
    'fubo sports network':                  'Sports',
    't2':                                   'Sports',
    'pfl':                                  'Sports',
    'pga tour':                             'Sports',
    'american ninja warrior':               'Sports',
    'sports first - stream free now':       'Sports',
    'real madrid tv':                       'Sports',
    'rugbypass tv':                         'Sports',
    'nascar channel':                       'Sports',
    'motogp':                               'Sports',
    'monster jam':                          'Sports',
    'f1 tv':                                'Sports',
    'speed sport 1':                        'Sports',
    'flohockey 24/7':                       'Sports',
    'floracing 24/7':                       'Sports',
    'msg national':                         'Sports',
    'msgsn national':                       'Sports',
    'for the fans':                         'Sports',
    'gopro tv':                             'Sports',
    'hi-yah!':                              'Sports',
    'lacrosse tv':                          'Sports',
    'uefa champions league':                'Sports',

    # ── Travel ───────────────────────────────────────────────────────────────
    'journy':                               'Travel',
    'travel + adventure':                   'Travel',
    'tastemade travel':                     'Travel',
    'gotraveler':                           'Travel',
    'intravel':                             'Travel',

    # ── True Crime ───────────────────────────────────────────────────────────
    'court tv':                             'True Crime',
    'law & crime':                          'True Crime',
    'crime beat tv':                        'True Crime',
    'crime thrillher':                      'True Crime',
    'ion mystery':                          'True Crime',
    'pluto tv crime drama':                 'True Crime',
    'mhz mysteries':                        'True Crime',
    'crimeflix - free crime tv that\'ll keep you hooked': 'True Crime',
    'lapd: life on the beat':               'True Crime',
    'the fbi':                              'True Crime',
    'the fbi files':                        'True Crime',
    'the new detectives':                   'True Crime',
    "sheriffs: el dorado county":           'True Crime',
    'i  (almost) got away with it':         'True Crime',
    'introuble':                            'True Crime',
    'locked up abroad':                     'True Crime',

    # ── Westerns ─────────────────────────────────────────────────────────────
    'the rifleman':                         'Westerns',
    'wanted: dead or alive':                'Westerns',
    'death valley days':                    'Westerns',
    'lone star':                            'Westerns',
    'outlaw':                               'Westerns',
    'bonanza-billies tv':                   'Westerns',
    'the young riders':                     'Westerns',
    'life and legend of wyatt earp':        'Westerns',
    'cowboy movie channel':                 'Westerns',
    'grit xtra':                            'Westerns',
    'old west tv':                          'Westerns',
}


def category_for_channel(name: str, raw_category: str | None) -> str | None:
    """Return the canonical category for a channel, applying hard overrides first.

    Priority order:
      1. Exact name match in _NAME_OVERRIDES (scraper-proof corrections)
      2. Pattern checks for reliable name-based signals (XITE, K-Drama, Very Local)
      3. normalize_category(raw_category) — scraper-provided value after mapping

    This ensures that re-scraping never undoes manual category corrections.
    """
    name_lower = (name or '').strip().lower()

    # 1. Exact override
    override = _NAME_OVERRIDES.get(name_lower)
    if override:
        return override

    # 2. High-confidence name patterns
    if name_lower.startswith('xite '):
        return 'Music'
    if 'k-drama' in name_lower or 'kdrama' in name_lower:
        return 'Drama'
    if name_lower.endswith(' westerns') or name_lower.endswith(' western'):
        return 'Westerns'
    if name_lower.startswith('western ') or ' western ' in name_lower:
        return 'Westerns'

    # Local News patterns
    # Hearst "Very Local" stations
    if name_lower.startswith('very ') and ' by ' in name_lower:
        return 'Local News'
    # FOX Local city streams
    if 'fox local' in name_lower:
        return 'Local News'
    # CBS News [City] local streams — but not national feeds
    if name_lower.startswith('cbs news ') and not any(
        x in name_lower for x in ('24/7', ' now', '24x7')
    ):
        return 'Local News'
    # CBS [N] News [City] affiliate stations
    if name_lower.startswith('cbs ') and name_lower[4:5].isdigit():
        return 'Local News'
    # NBC [N] [City] News city streams
    if name_lower.startswith('nbc ') and name_lower[4:5].isdigit():
        return 'Local News'
    # ABC [N] [City] local affiliates (not "ABC News Live" which is national)
    if name_lower.startswith('abc ') and name_lower[4:5].isdigit():
        return 'Local News'
    if name_lower.startswith('abc news ') and name_lower[9:10].isdigit():
        return 'Local News'
    # US broadcast call-sign stations: K/W + 2-4 letters, then space/digit/end
    if len(name_lower) >= 3 and name_lower[0] in ('k', 'w') and name_lower[1:4].isalpha():
        fourth = name_lower[4:5]
        if not fourth or not fourth.isalpha():
            return 'Local News'
    # Numbered local affiliates: "10 NBC ...", "6 NEWS NBC ...", "News 12 ..."
    if name_lower.startswith(('news 12', 'news10', 'news channel', 'newsday')):
        return 'Local News'

    # 3. Scraper-provided category, normalized
    return normalize_category(raw_category)


def explain_category(name: str, raw_category: str | None) -> dict:
    """Return a human-readable explanation of how a channel's category was resolved.

    Returns a dict with:
      source   – 'override' | 'name_pattern' | 'scraper' | 'name_inference' | 'unknown'
      rule     – short machine label for the rule that fired
      detail   – human-readable sentence suitable for a tooltip
    """
    name_lower = (name or '').strip().lower()

    # 1. Exact override
    if name_lower in _NAME_OVERRIDES:
        return {
            'source': 'override',
            'rule': 'name_override',
            'detail': 'Matched a hard-coded name override — takes priority over all scraper data.',
        }

    # 2. High-confidence name patterns
    if name_lower.startswith('xite '):
        return {'source': 'name_pattern', 'rule': 'xite_prefix', 'detail': 'Name starts with "XITE" → Music.'}
    if 'k-drama' in name_lower or 'kdrama' in name_lower:
        return {'source': 'name_pattern', 'rule': 'kdrama', 'detail': 'Name contains "K-Drama" or "KDrama" → Drama.'}
    if (name_lower.endswith(' westerns') or name_lower.endswith(' western')
            or name_lower.startswith('western ') or ' western ' in name_lower):
        return {'source': 'name_pattern', 'rule': 'western', 'detail': 'Name contains "Western" → Westerns.'}
    if name_lower.startswith('very ') and ' by ' in name_lower:
        return {'source': 'name_pattern', 'rule': 'very_local', 'detail': 'Hearst "Very Local by …" station → Local News.'}
    if 'fox local' in name_lower:
        return {'source': 'name_pattern', 'rule': 'fox_local', 'detail': 'FOX Local city stream → Local News.'}
    if name_lower.startswith('cbs news ') and not any(x in name_lower for x in ('24/7', ' now', '24x7')):
        return {'source': 'name_pattern', 'rule': 'cbs_news_city', 'detail': 'CBS News [City] local stream → Local News.'}
    if name_lower.startswith('cbs ') and name_lower[4:5].isdigit():
        return {'source': 'name_pattern', 'rule': 'cbs_numbered', 'detail': 'CBS [N] affiliate pattern → Local News.'}
    if name_lower.startswith('nbc ') and name_lower[4:5].isdigit():
        return {'source': 'name_pattern', 'rule': 'nbc_numbered', 'detail': 'NBC [N] city stream pattern → Local News.'}
    if name_lower.startswith('abc ') and name_lower[4:5].isdigit():
        return {'source': 'name_pattern', 'rule': 'abc_numbered', 'detail': 'ABC [N] local affiliate pattern → Local News.'}
    if name_lower.startswith('abc news ') and name_lower[9:10].isdigit():
        return {'source': 'name_pattern', 'rule': 'abc_news_numbered', 'detail': 'ABC News [N] local stream → Local News.'}
    if len(name_lower) >= 3 and name_lower[0] in ('k', 'w') and name_lower[1:4].isalpha():
        if not name_lower[4:5] or not name_lower[4:5].isalpha():
            return {'source': 'name_pattern', 'rule': 'call_sign', 'detail': f'Broadcast call sign ({name[:4].upper()}) → Local News.'}
    if name_lower.startswith(('news 12', 'news10', 'news channel', 'newsday')):
        return {'source': 'name_pattern', 'rule': 'numbered_local', 'detail': 'Numbered local news affiliate pattern → Local News.'}

    # 3. Scraper-provided category
    normalized = normalize_category(raw_category)
    if normalized:
        raw_display = raw_category or '(empty)'
        if normalized == raw_category:
            return {
                'source': 'scraper',
                'rule': 'scraper_passthrough',
                'detail': f'Passed through directly from the scraper as "{raw_display}".',
            }
        return {
            'source': 'scraper',
            'rule': 'scraper_normalized',
            'detail': f'Scraper provided "{raw_display}", normalized to "{normalized}".',
        }

    return {
        'source': 'unknown',
        'rule': 'no_match',
        'detail': 'No override, pattern, or scraper value matched — category may be unset.',
    }


def infer_category_from_name(title: str) -> str | None:
    """Infer a canonical category label from a channel name via keyword matching.

    Returns the matched category string, or None if nothing matches.
    The caller decides the fallback (e.g. "Entertainment").
    """
    tl = title.lower()
    for keywords, label in _NAME_CATEGORY_RULES:
        if any(kw in tl for kw in keywords):
            return label
    return None

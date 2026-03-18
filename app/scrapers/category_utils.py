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

    # Entertainment — misc catch-alls and source artifacts
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


def normalize_category(raw: str | None) -> str | None:
    """Map a raw scraper category string to a canonical category label.

    Already-canonical values pass through unchanged. Unknown values also
    pass through unchanged so new scrapers don't silently lose their data.
    """
    if not raw:
        return raw
    return _CANONICAL_MAP.get(raw.strip().lower(), raw)


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

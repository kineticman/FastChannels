"""
Migration 009 — normalize channel categories to canonical list.

Collapses ~80 raw category variants down to 34 canonical labels.

Run:
    docker exec fastchannels python /app/migrations/009_normalize_categories.py
"""
import sqlite3, sys, pathlib

DB_PATH = pathlib.Path("/data/fastchannels.db")
if not DB_PATH.exists():
    print(f"DB not found at {DB_PATH}", file=sys.stderr)
    sys.exit(1)

# raw value (case-insensitive) → canonical label
NORMALIZE_MAP = {
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
    # Entertainment
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
    # Lifestyle (home+food combo buckets)
    'home & food':                  'Lifestyle',
    'home + food':                  'Lifestyle',
    # Horror combos
    'horror & sci-fi':              'Horror',
    'horror and scifi':             'Horror',
    # International
    'bollywood':                    'International',
    # Kids
    'kids & family':                'Kids',
    'family':                       'Kids',
    # Latino
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
    # Sci-Fi
    'sci-fi & horror':              'Sci-Fi',
    'sci-fi & supernatural':        'Sci-Fi',
    'science fiction':              'Sci-Fi',
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

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

total_updated = 0
for raw, canonical in NORMALIZE_MAP.items():
    cur.execute(
        "UPDATE channels SET category = ? WHERE LOWER(category) = ?",
        (canonical, raw.lower()),
    )
    n = cur.rowcount
    if n:
        print(f"  {n:4d}  {raw!r:40s} → {canonical!r}")
        total_updated += n

con.commit()
con.close()
print(f"\nMigration 009 done — {total_updated} rows updated.")

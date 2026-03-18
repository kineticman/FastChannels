"""
Migration 010 — second-pass category corrections.

Fixes channels that landed in the wrong bucket after 009's normalization:
 - Hearst "Very Local" stations → Local News
 - ABC/CBS/NBC/FOX affiliates (with call signs) → Local News
 - XITE music video channels → Music
 - K-Drama branded channels → Drama
 - Western-branded channels scattered across Movies/Classic TV/etc → Westerns
 - Horror-named channels scattered in True Crime / Sci-Fi → Horror
 - Automotive channels scattered across Lifestyle/Sports/History → Automotive
 - Sports channels sitting in Outdoors → Sports
 - Shopping channels sitting in Game Shows / Travel → Shopping
 - Science/History cross-contamination cleanup
 - Misc obvious name-based corrections

Run:
    docker exec fastchannels python /app/migrations/010_recategorize_channels.py
"""
import sqlite3, sys, pathlib

DB_PATH = pathlib.Path("/data/fastchannels.db")
if not DB_PATH.exists():
    print(f"DB not found at {DB_PATH}", file=sys.stderr)
    sys.exit(1)

con = sqlite3.connect(DB_PATH)
cur = con.cursor()
total = 0


def run(sql, params=()):
    global total
    cur.execute(sql, params)
    n = cur.rowcount
    if n:
        total += n
        print(f"  {n:4d}  {sql[:80].strip()}")


# ── Local News ─────────────────────────────────────────────────────────────
# Hearst "Very Local" stations
run("UPDATE channels SET category='Local News' WHERE LOWER(name) LIKE 'very % by %' AND category != 'Local News'")
# Network affiliates with parenthetical call signs
run("UPDATE channels SET category='Local News' WHERE category != 'Local News' AND ("
    "name LIKE 'ABC % (%)' OR name LIKE 'CBS % (%)' OR name LIKE 'NBC % (%)' "
    "OR name LIKE 'FOX % (%)' OR name LIKE 'KIRO % (%)' OR name LIKE 'WHIO % (%)'"
    ")")
# Remaining obvious local news names
for name in (
    "9&10 News Northern Michigan", "12 News Beaumont TX", "Erie News Now",
    "News Center Maine NBC Portland-Bangor ME", "Newsday TV Long Island NY",
    "ONNJ On New Jersey", "Very Alabama by WVTM", "Localish",
    "FOX 11 Green Bay WI 2",
):
    run("UPDATE channels SET category='Local News' WHERE name=? AND category!='Local News'", (name,))

# ── Music ───────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Music' WHERE name LIKE 'XITE %' AND category!='Music'")
for name in ("Smooth Jazz", "Easy Listening", "Def Jam", "Circle Country"):
    run("UPDATE channels SET category='Music' WHERE name=? AND category!='Music'", (name,))

# ── Westerns ────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Westerns' WHERE category!='Westerns' AND ("
    "name LIKE '%Westerns%' OR name LIKE '%Western%' OR name LIKE '%Wild West%' "
    "OR name LIKE 'a-z Western%' OR name LIKE 'Old West%' OR name LIKE 'Grjngo%'"
    ")")
for name in (
    "The Rifleman", "Wanted: Dead or Alive", "Death Valley Days", "Lone Star",
    "Outlaw", "OUTLAW", "Bonanza-Billies TV", "The Young Riders",
    "Life and Legend of Wyatt Earp", "Cowboy Movie Channel",
):
    run("UPDATE channels SET category='Westerns' WHERE name=? AND category NOT IN ('Westerns')", (name,))

# ── Drama ───────────────────────────────────────────────────────────────────
# K-Drama channels stuck in Action & Adventure
run("UPDATE channels SET category='Drama' WHERE category='Action & Adventure' AND ("
    "name LIKE 'K-Drama%' OR name LIKE 'KOCOWA K-%' OR name LIKE 'Genie K-%' "
    "OR name LIKE 'Series K %' OR name LIKE 'K-Stories%'"
    ")")
for name in (
    "BBC Drama", "BET x Tyler Perry Drama", "Drama Life", "TV Land Drama",
    "Murdoch Mysteries", "Bosch", "Designated Survivor", "Heartland",
    "ION", "ION Plus", "Las Vegas", "Lawless", "Leverage", "Primetime Soaps",
    "The Bold and the Beautiful", "Rings Of Power", "Stories by AMC",
    "Teen Wolf", "Teen Wolf by MGM", "Weeds & Nurse Jackie",
    "Weeds and Nurse Jackie", "Nash Bridges", "Nashville", "Nikita",
    "Nip/Tuck", "Nip / Tuck", "Spooks (MI-5)", "MI-5",
    "Law & Order", "Midsomer Murders", "Murder, She Wrote",
    "Silent Witness and New Tricks", "Silent Witness & New Tricks",
    "Silent Witness|New Tricks", "McLeod's Daughters",
    "BritBox Mysteries", "Britbox Mysteries",
):
    run("UPDATE channels SET category='Drama' WHERE name=? AND category!='Drama'", (name,))

# ── Sci-Fi ──────────────────────────────────────────────────────────────────
for name in (
    "BBC Sci-Fi", "FilmRise Sci-Fi", "Farscape", "Stargate by MGM",
    "The Outer Limits", "Z Nation", "Classic Doctor Who",
    "Ancient Aliens", "Mysterious Worlds", "UnXplained Zone",
):
    run("UPDATE channels SET category='Sci-Fi' WHERE name=? AND category!='Sci-Fi'", (name,))

# ── Horror ──────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Horror' WHERE category!='Horror' AND ("
    "name LIKE '%Horror%' OR name LIKE 'Scream%' OR name LIKE 'FrightFlix%' "
    "OR name LIKE 'Haunt%'"
    ")")
for name in (
    "Ghost Dimension", "Ghost Hunters", "Ghost Hunters Channel",
    "Ghosts Are Real", "Ghosts are Real", "Ghost Stories",
    "The Walking Dead Channel", "The Walking Dead Universe",
    "Beyond Paranormal", "Paranormal Files", "Screams TV",
    "Universal Monsters", "Van Helsing", "AMC Thrillers",
    "Trailers From Hell", "Watch it SCREAM",
):
    run("UPDATE channels SET category='Horror' WHERE name=? AND category!='Horror'", (name,))

# ── True Crime ──────────────────────────────────────────────────────────────
for name in (
    "Crime Beat TV", "Crime ThrillHer", "ION Mystery", "Ion Mystery",
    "Pluto TV Crime Drama", "MHz Mysteries",
    "CrimeFlix - Free Crime TV That'll Keep You Hooked",
    "LAPD: Life on the Beat", "The FBI", "The FBI Files",
    "The New Detectives", "Sheriffs: El Dorado County",
    "I  (Almost) Got Away With It", "InTrouble", "Locked Up Abroad",
):
    run("UPDATE channels SET category='True Crime' WHERE name=? AND category!='True Crime'", (name,))

# ── Comedy ──────────────────────────────────────────────────────────────────
for name in (
    "The Conners", "The Marvelous Mrs. Maisel", "Upload",
    "Wild 'N Out", "Classic TV Comedy",
    "Mystery Science Theater 3000", "Mystery Science Theater 3000 (MST3K)",
    "MST3K",
):
    run("UPDATE channels SET category='Comedy' WHERE name=? AND category!='Comedy'", (name,))

# ── Automotive ──────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Automotive' WHERE category!='Automotive' AND ("
    "name LIKE '%Top Gear%' OR name LIKE 'Discovery Turbo%' OR name LIKE 'Motorvision%' "
    "OR name LIKE 'POWERNATION%' OR name LIKE 'PowerNation%' OR name LIKE 'Powernation%'"
    ")")
for name in (
    "BBC Top Gear", "DriveTribe", "Hagerty", "The Grand Tour", "Torque",
    "Fifth Gear", "Fifth Gear (UK)", "Choppertown", "Classic Car Auctions",
    "Classic Car Auctions by History", "POWERtube TV",
    "SPEEDVISION",
):
    run("UPDATE channels SET category='Automotive' WHERE name=? AND category!='Automotive'", (name,))

# ── Sports ──────────────────────────────────────────────────────────────────
# Most Outdoors channels that are really sports
run("UPDATE channels SET category='Sports' WHERE category='Outdoors' AND ("
    "name LIKE '%Sports%' OR name LIKE '%NFL%' OR name LIKE '%NBA%' "
    "OR name LIKE '%MLB%' OR name LIKE '%Soccer%' OR name LIKE '%Rugby%' "
    "OR name LIKE '%Wrestling%' OR name LIKE '%MMA%' OR name LIKE '%Golf%' "
    "OR name LIKE '%Tennis%' OR name LIKE '%Lacrosse%'"
    ")")
for name in (
    "ACC Digital Network", "Big 12 Studios", "Billiard TV",
    "CBS Sports HQ", "Combate Global MMA", "DraftKings Network",
    "ESPN8: The Ocho", "FIFA+", "FIFA +", "FOX Sports", "FanDuel TV Extra",
    "GolfPass", "MSG SportsZone", "MiLB", "NBC Sports NOW",
    "NESN Nation", "Pac-12 Insider", "PickleballTV", "PickleTV",
    "Roku Sports Channel", "SLVR", "SURFER TV", "SportsGrid",
    "Stadium", "TNA Wrestling", "Team USA TV", "The Jim Rome Show",
    "The NBA Channel", "UFC", "Unbeaten Sports Channel", "Victory+",
    "Women's Sports Network", "World Poker Tour", "Yahoo! Sports Network",
    "beIN SPORTS XTRA", "fubo Sports Network", "T2", "PFL", "PGA Tour",
    "American Ninja Warrior", "Sports First - Stream Free Now",
    "Real Madrid TV", "RugbyPass TV", "NASCAR Channel", "MotoGP",
    "Monster Jam", "F1 TV", "SPEED SPORT 1", "FloHockey 24/7",
    "FloRacing 24/7", "MSG NATIONAL", "MSGSN NATIONAL",
):
    run("UPDATE channels SET category='Sports' WHERE name=? AND category!='Sports'", (name,))

# ── Shopping ────────────────────────────────────────────────────────────────
for name in ("HSN", "QVC", "QVC2", "Shop LC", "ShopLC", "JTV Jewelry Love"):
    run("UPDATE channels SET category='Shopping' WHERE name=? AND category!='Shopping'", (name,))

# ── Kids ─────────────────────────────────────────────────────────────────────
for name in (
    "Pokémon", "Super Mario", "TMNT", "Transformers",
    "HE-MAN & THE MASTERS OF THE UNIVERSE", "Dinos 24/7", "KIDDO+",
    "Toon Goggles en Español",
):
    run("UPDATE channels SET category='Kids' WHERE name=? AND category!='Kids'", (name,))

# ── Gaming ───────────────────────────────────────────────────────────────────
for name in (
    "Gameplay Minecraft", "Gameplay Roblox", "Estrella Games",
    "Dungeons & Dragons Adventures", "JackSepticEye",
    "LazarBeam", "PrestonPlayz", "Team Liquid",
):
    run("UPDATE channels SET category='Gaming' WHERE name=? AND category!='Gaming'", (name,))

# ── Reality TV ───────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Reality TV' WHERE category IN ('Game Shows','Entertainment') AND ("
    "name LIKE 'Real Housewives%' OR name LIKE 'Bravo Vault%'"
    ")")
for name in (
    "America's Got Talent", "Bad Girls Club", "Reality Gone Wild",
    "The Biggest Loser", "The Apprentice", "The Osbournes",
    "The Girls Next Door", "Bondi Rescue", "Highway Thru Hell",
    "Fear Factor", "Fear Factor USA", "Wipeout Extra",
    "Wipeout Xtra", "WipeoutXtra", "Confess by Nosey",
    "Best of Dr. Phil", "Judge Nosey", "Nosey",
):
    run("UPDATE channels SET category='Reality TV' WHERE name=? AND category!='Reality TV'", (name,))

# ── Game Shows ───────────────────────────────────────────────────────────────
for name in ("Deal or No Deal", "Supermarket Sweep",):
    run("UPDATE channels SET category='Game Shows' WHERE name=? AND category!='Game Shows'", (name,))

# ── Faith ────────────────────────────────────────────────────────────────────
for name in (
    "Daystar TV", "Daystar TV - Espanol", "Elevation Church",
    "Elevation Church Network", "Impact Gospel", "In Touch +",
    "JLTV", "RightNow TV", "Right Now Tv", "T.D. Jakes",
    "Joel Osteen network",
):
    run("UPDATE channels SET category='Faith' WHERE name=? AND category!='Faith'", (name,))

# ── Science ───────────────────────────────────────────────────────────────────
for name in (
    "MythBusters", "Pluto TV Science", "Robot Wars by Mech+",
    "Robot Wars by MECH+", "StarTalk", "NASA+",
):
    run("UPDATE channels SET category='Science' WHERE name=? AND category!='Science'", (name,))

# ── History ───────────────────────────────────────────────────────────────────
for name in (
    "History & Warfare Now", "History 365", "History Film Channel",
    "Military Heroes", "Modern Marvels Presented by History",
    "True History", "The Curse of Oak Island",
    "The UnXplained with William Shatner", "Unidentified",
    "DECLASSIFIED", "Declassified",
):
    run("UPDATE channels SET category='History' WHERE name=? AND category!='History'", (name,))

# ── Documentary ───────────────────────────────────────────────────────────────
for name in (
    "Documentary+", "DOCUMENTARY+", "TED", "VICE",
    "PBS Genealogy", "Real Disaster Channel", "INWONDER",
):
    run("UPDATE channels SET category='Documentary' WHERE name=? AND category!='Documentary'", (name,))

# ── Home & DIY ────────────────────────────────────────────────────────────────
for name in (
    "At Home with Family Handyman", "BBC Home & Garden", "CraftsyTV",
    "Gardening with Monty Don", "The Design Network", "This Old House",
    "Tiny House Nation", "Welcome Home", "Property Brothers",
    "Property Brothers Channel", "Grand Designs", "My First Place",
    "NBC LX Home", "Tastemade Home",
):
    run("UPDATE channels SET category='Home & DIY' WHERE name=? AND category!='Home & DIY'", (name,))

# ── Food ─────────────────────────────────────────────────────────────────────
for name in (
    "Bizarre Foods with Andrew Zimmern", "Cook's Country",
    "Come Dine With Me", "Great British Menu",
):
    run("UPDATE channels SET category='Food' WHERE name=? AND category!='Food'", (name,))

# ── Lifestyle ─────────────────────────────────────────────────────────────────
for name in ("GrowthDay Network", "Tony Robbins Network", "Antiques Road Trip",
             "PBS Antiques Roadshow", "Omstars TV", "MORE U",):
    run("UPDATE channels SET category='Lifestyle' WHERE name=? AND category!='Lifestyle'", (name,))

# ── Nature ────────────────────────────────────────────────────────────────────
for name in (
    "Dog Whisperer", "Nat Geo Sharks", "Wild Nature", "Paws and Claws",
    "Paws & Claws", "Love Pets", "Unleashed by DOGTV",
    "Lucky Dog", "Rovr Pets", "Samsung Wild Life", "The Pet Collective",
    "INWILD",
):
    run("UPDATE channels SET category='Nature' WHERE name=? AND category!='Nature'", (name,))

# ── Outdoors ──────────────────────────────────────────────────────────────────
for name in ("Wild TV", "RVTV", "The Boat Show", "Yachting tv",):
    run("UPDATE channels SET category='Outdoors' WHERE name=? AND category!='Outdoors'", (name,))

# ── News ──────────────────────────────────────────────────────────────────────
for name in (
    "FOX Weather", "WeatherSpy", "CNN Originals", "Reuters 60",
    "Real America's Voice", "spot on news", "Telemundo al Dia",
    "60 Minutes", "TODAY All Day", "USA Today", "Cheddar News",
    "American Stories Network",
):
    run("UPDATE channels SET category='News' WHERE name=? AND category!='News'", (name,))

# ── Travel ────────────────────────────────────────────────────────────────────
for name in ("Travel + Adventure", "Tastemade Travel",):
    run("UPDATE channels SET category='Travel' WHERE name=? AND category!='Travel'", (name,))

# ── Entertainment fallback cleanups ───────────────────────────────────────────
for name in (
    "5-Minute Crafts", "TMZ", "FOX SOUL", "Revry", "MVMT Culture",
    "Cirque du Soleil", "MrBeast",
):
    run("UPDATE channels SET category='Entertainment' WHERE name=? AND category!='Entertainment'", (name,))

# ── Classic TV ────────────────────────────────────────────────────────────────
for name in ("Shout! TV", "a-z Best Classic TV", "a-z Classic Flix",
             "FilmRise Classic TV", "Non-Stop '90s",):
    run("UPDATE channels SET category='Classic TV' WHERE name=? AND category!='Classic TV'", (name,))

# ── Ambiance ──────────────────────────────────────────────────────────────────
for name in ("Stingray Naturescape", "Holidayscapes",):
    run("UPDATE channels SET category='Ambiance' WHERE name=? AND category!='Ambiance'", (name,))

# ── Movies ────────────────────────────────────────────────────────────────────
for name in ("OUTFLIX MOVIES", "OUTflix Movies", "Lifetime Movie Favorites",):
    run("UPDATE channels SET category='Movies' WHERE name=? AND category!='Movies'", (name,))


con.commit()
con.close()
print(f"\nMigration 010 done — {total} rows updated.")

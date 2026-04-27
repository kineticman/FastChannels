"""
Migration 023 — Spanish-language channel audit.

Two classes of fix:
1. Category fixes: Spanish-language channels misplaced outside Latino, and
   Latino channels that belong in a specific content category (Sports, Nature,
   History, True Crime, Food, Home & DIY, Westerns, Horror, Reality TV).
2. Language fixes: channels that are clearly Spanish but were tagged language='en'
   by scrapers that didn't recognise the name — set language='es' for those rows.

Run:
    docker exec fastchannels python /app/migrations/023_spanish_language_audit.py
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


# ── 1. Non-Latino → Latino ────────────────────────────────────────────────
# Spanish channels that landed in Drama, Movies, Home&DIY, or Entertainment
for name in (
    "Apostarías por Mí: Multicámaras",
    "Aquí y Ahora",
    "CSI en español",
    "Cinépolis Channel",
    "FILMEX Acción",
    "FreeTV Acción",
    "FreeTV Sureño",
    "Homeful en Español",
    "Nosey Escándalos",
    "Runtime Sangre Fría",
    "Todo Novelas Más Pasiones",
    "Todo Novelas mas Pasiones",
):
    run("UPDATE channels SET category='Latino' WHERE name=? AND category!='Latino'", (name,))

# ── 2. Latino → Sports (deportes / lucha / MMA) ───────────────────────────
for name in (
    "Canela.TV Deportes",
    "ITV Deportes",
    "Telemundo Deportes Ahora",
    "FOX Deportes",
    "CombaTV",
    "CG MMA En Español",
    "Lucha Plus",
    "LUCHA PLUS",
    "Lucha Libre AAA",
):
    run("UPDATE channels SET category='Sports' WHERE name=? AND category NOT IN ('Sports')", (name,))

# ── 3. Latino → True Crime (crimen / misterios) ───────────────────────────
for name in (
    "Crimen",
    "Crímenes Verdaderos",
    "Crímenes imperfectos",
    "Delito",
    "Investiga",
    "Misterios sin Resolver",
    "Misterios sin resolver",
    "Todo Crimen",
    "Zona Investigación",
):
    run("UPDATE channels SET category='True Crime' WHERE name=? AND category NOT IN ('True Crime')", (name,))

# ── 4. Latino → Nature ────────────────────────────────────────────────────
for name in (
    "Naturaleza",
    "Naturaleza Salvaje",
    "Love Nature en Español",
    "Love Nature En Espanol",
    "Love Nature Spanish",
    "Curiosity Animales",
):
    run("UPDATE channels SET category='Nature' WHERE name=? AND category NOT IN ('Nature')", (name,))

# ── 5. Latino → History ───────────────────────────────────────────────────
run("UPDATE channels SET category='History' WHERE name='Historia' AND category NOT IN ('History')")

# ── 6. Latino → Food ──────────────────────────────────────────────────────
for name in ("Saborear TV", "FilmRise Concursos de Cocina"):
    run("UPDATE channels SET category='Food' WHERE name=? AND category NOT IN ('Food')", (name,))

# ── 7. Latino → Home & DIY ────────────────────────────────────────────────
run("UPDATE channels SET category='Home & DIY' WHERE name='Ideas En 5 Minutos' AND category NOT IN ('Home & DIY')")

# ── 8. Latino → Westerns ──────────────────────────────────────────────────
for name in ("Grjngo - Películas Del Oeste", "Grjngo - Peliculas Del Oeste"):
    run("UPDATE channels SET category='Westerns' WHERE name=? AND category NOT IN ('Westerns')", (name,))

# ── 9. Latino → Horror ────────────────────────────────────────────────────
for name in (
    "Cine de Horror",
    "The Walking Dead en español",
    "The Walking Dead Espanol",
):
    run("UPDATE channels SET category='Horror' WHERE name=? AND category NOT IN ('Horror')", (name,))

# ── 10. Latino → Reality TV ───────────────────────────────────────────────
for name in ("Ice Pilots NWT (en español)", "La Fiebre del Jade"):
    run("UPDATE channels SET category='Reality TV' WHERE name=? AND category NOT IN ('Reality TV')", (name,))

# ── 11. Fix language='en' on clearly Spanish channels ─────────────────────
# These channels have at least one row tagged 'en' by a scraper that didn't
# recognise the Spanish name.  The _SPANISH_LANGUAGE_MARKERS update in base.py
# prevents recurrence; this fixes existing rows.
for name in (
    "Cinépolis Channel",
    "Crimen",
    "FILMEX Acción",
    "FreeTV Acción",
    "FreeTV Sureño",
    "Lucha Plus",
    "LUCHA PLUS",
    "Runtime Sangre Fría",
):
    run("UPDATE channels SET language='es' WHERE name=? AND language='en'", (name,))


con.commit()
con.close()
print(f"\nMigration 023 done — {total} rows updated.")

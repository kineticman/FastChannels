"""
Migration 024 — third-pass category audit corrections.

Fixes from the May 2026 category audit:
 - Travel and Adventure: Food → Travel
 - Wild Oceans: Local News → Nature
 - WITZ Comedy: Local News → Comedy
 - How To / How-To: History/Lifestyle → Home & DIY
 - FilmRise Food: Movies → Food
 - PowerNation Spanish: Home & DIY → Automotive
 - Amazon Live: Lifestyle → Shopping
 - Women's Wellness by Commune: Faith → Lifestyle
 - Outdoor Channel: Lifestyle → Outdoors
 - LATV: Lifestyle → Latino
 - WWE Superstar Central: Lifestyle → Sports
 - People Are Awesome / People are Awesome: Nature → Entertainment
 - McLeods Daughters: Classic TV/Westerns → Drama
 - Circle: Westerns/Entertainment → Music
 - Space Live Powered by Sen: Nature → Science
 - The Great British Baking Channel: Reality TV → Food
 - InTrouble HD: Entertainment → True Crime
 - Garden with Monty Don: Food/Lifestyle → Home & DIY
 - Million Dollar Dream Home: Food → Home & DIY
 - JOURNY TV: Lifestyle → Travel
 - ZENlife by Stingray / ZenLIFE by Stingray: Lifestyle → Ambiance
 - Automotions: Sports → Automotive
 - TEST MY RIDE: Sports → Automotive

Run:
    docker exec fastchannels python /app/migrations/024_recategorize_channels_3.py
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


# ── Travel ────────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Travel' WHERE name='Travel and Adventure' AND category!='Travel'")
run("UPDATE channels SET category='Travel' WHERE name='JOURNY TV' AND category!='Travel'")

# ── Nature ────────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Nature' WHERE name='Wild Oceans' AND category!='Nature'")

# ── Comedy ────────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Comedy' WHERE name='WITZ Comedy' AND category!='Comedy'")

# ── Home & DIY ────────────────────────────────────────────────────────────────
for name in ("How To", "How-To", "Garden with Monty Don", "GARDEN with Monty Don",
             "Million Dollar Dream Home"):
    run("UPDATE channels SET category='Home & DIY' WHERE name=? AND category NOT IN ('Home & DIY')", (name,))

# ── Food ──────────────────────────────────────────────────────────────────────
for name in ("FilmRise Food", "The Great British Baking Channel"):
    run("UPDATE channels SET category='Food' WHERE name=? AND category NOT IN ('Food')", (name,))

# ── Automotive ────────────────────────────────────────────────────────────────
for name in ("PowerNation Spanish", "Automotions", "TEST MY RIDE"):
    run("UPDATE channels SET category='Automotive' WHERE name=? AND category NOT IN ('Automotive')", (name,))

# ── Shopping ──────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Shopping' WHERE name='Amazon Live' AND category!='Shopping'")

# ── Lifestyle ─────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Lifestyle' WHERE name=\"Women's Wellness by Commune\" AND category!='Lifestyle'")

# ── Outdoors ──────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Outdoors' WHERE name='Outdoor Channel' AND category!='Outdoors'")

# ── Latino ────────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Latino' WHERE name='LATV' AND category!='Latino'")

# ── Sports ────────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Sports' WHERE name='WWE Superstar Central' AND category!='Sports'")

# ── Entertainment ─────────────────────────────────────────────────────────────
for name in ("People Are Awesome", "People are Awesome"):
    run("UPDATE channels SET category='Entertainment' WHERE name=? AND category NOT IN ('Entertainment')", (name,))

# ── Drama ─────────────────────────────────────────────────────────────────────
for name in ("McLeods Daughters", "McLeod's Daughters"):
    run("UPDATE channels SET category='Drama' WHERE name=? AND category NOT IN ('Drama')", (name,))

# ── Music ─────────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Music' WHERE name='Circle' AND category NOT IN ('Music')")

# ── Science ───────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Science' WHERE name='Space Live Powered by Sen' AND category!='Science'")

# ── True Crime ────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='True Crime' WHERE name='InTrouble HD' AND category!='True Crime'")

# ── Ambiance ──────────────────────────────────────────────────────────────────
for name in ("ZENlife by Stingray", "ZenLIFE by Stingray", "Stingray Zen Life"):
    run("UPDATE channels SET category='Ambiance' WHERE name=? AND category NOT IN ('Ambiance')", (name,))


con.commit()
con.close()
print(f"\nMigration 024 done — {total} rows updated.")

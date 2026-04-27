"""
Migration 022 — second-pass category audit corrections.

Fixes channels that landed in the wrong bucket or were left as NULL:
 - NULL category channels → correct category (7 channels)
 - Entertainment catch-all cleanup → specific categories
 - Cross-category bugs: Sonic/Anime→Kids, LOL!/Movies→Comedy,
   Mi Miedo Canal/Movies→Horror, Degrassi/A&A→Drama,
   Wicked Tuna/Reality TV→Nature, Beverly Hillbillies/Movies→Classic TV,
   MrBeast/Comedy+GameShows→Gaming, ElectricNow/Music→Action&Adventure,
   Scares by Shudder/Movies+TrueCrime→Horror, Dark Matter/multi→Horror,
   Alien Nation by DUST/History+Movies→Sci-Fi
 - Zona TUDN: Latino → Sports (sports channel, not language bucket)

Run:
    docker exec fastchannels python /app/migrations/022_recategorize_channels_2.py
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


# ── Fix NULL categories ────────────────────────────────────────────────────
for name, cat in (
    ("Alfred Hitchcock Presents", "Drama"),
    ("Alien Nation by DUST",      "Sci-Fi"),
    ("Dark Matter TV",            "Horror"),
    ("Murder She Wrote",          "Drama"),
    ("Scares by Shudder",        "Horror"),
    ("The Asylum",                "Movies"),
    ("Xumo Free Crime TV",        "True Crime"),
):
    run("UPDATE channels SET category=? WHERE name=? AND category IS NULL", (cat, name))

# ── Sci-Fi ────────────────────────────────────────────────────────────────
for name in ("Alien Nation by DUST", "Alien Nation", "Doctor Who Classic"):
    run("UPDATE channels SET category='Sci-Fi' WHERE name=? AND category NOT IN ('Sci-Fi')", (name,))

# ── Horror ────────────────────────────────────────────────────────────────
for name in ("Dark Matter TV", "Scares by Shudder", "Gritos TV", "Mi Miedo Canal", "The Dead Files"):
    run("UPDATE channels SET category='Horror' WHERE name=? AND category NOT IN ('Horror')", (name,))

# ── Drama ─────────────────────────────────────────────────────────────────
for name in (
    "Alfred Hitchcock Presents", "Murder She Wrote",
    "Bull", "Degrassi", "ION Television", "Legacy",
):
    run("UPDATE channels SET category='Drama' WHERE name=? AND category NOT IN ('Drama')", (name,))

# ── Movies ────────────────────────────────────────────────────────────────
for name in ("The Asylum", "Universal Movies"):
    run("UPDATE channels SET category='Movies' WHERE name=? AND category NOT IN ('Movies')", (name,))

# ── True Crime ────────────────────────────────────────────────────────────
for name in ("Forensic Files", "Xumo Free Crime TV"):
    run("UPDATE channels SET category='True Crime' WHERE name=? AND category NOT IN ('True Crime')", (name,))

# ── Comedy ────────────────────────────────────────────────────────────────
for name in ("LOL! Network", "Love Thy Neighbor", "Portlandia", "Tosh.0"):
    run("UPDATE channels SET category='Comedy' WHERE name=? AND category NOT IN ('Comedy')", (name,))

# ── Reality TV ────────────────────────────────────────────────────────────
for name in (
    "Duck Dynasty by A&E", "Ice Road Truckers", "Ink Master",
    "Real Housewives Vault",
):
    run("UPDATE channels SET category='Reality TV' WHERE name=? AND category NOT IN ('Reality TV')", (name,))

# ── Game Shows ────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Game Shows' WHERE name='Are You Smarter Than a 5th Grader?' AND category!='Game Shows'")

# ── Kids ──────────────────────────────────────────────────────────────────
for name in ("Sonic The Hedgehog", "The LEGO Channel"):
    run("UPDATE channels SET category='Kids' WHERE name=? AND category NOT IN ('Kids')", (name,))

# ── Classic TV ────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Classic TV' WHERE name='The Beverly Hillbillies' AND category!='Classic TV'")

# ── Music ─────────────────────────────────────────────────────────────────
for name in ("REVOLT Mixtape", "Revolt Mixtape"):
    run("UPDATE channels SET category='Music' WHERE name=? AND category NOT IN ('Music')", (name,))

# ── Action & Adventure ────────────────────────────────────────────────────
for name in (
    "Action Packed!", "Wu Tang Collection TV",
    "ElectricNOW", "ElectricNow", "Electric Now",
):
    run("UPDATE channels SET category='Action & Adventure' WHERE name=? AND category NOT IN ('Action & Adventure')", (name,))

# ── Nature ────────────────────────────────────────────────────────────────
for name in ("BarkTV", "The Wicked Tuna Channel", "Wicked Tuna"):
    run("UPDATE channels SET category='Nature' WHERE name=? AND category NOT IN ('Nature')", (name,))

# ── News ──────────────────────────────────────────────────────────────────
for name in ("Bloomberg Originals", "Milenio Television", "NBCLX"):
    run("UPDATE channels SET category='News' WHERE name=? AND category NOT IN ('News')", (name,))

# ── Outdoors ──────────────────────────────────────────────────────────────
for name in ("Field & Stream TV", "H2O TV", "H20 TV", "WayPoint"):
    run("UPDATE channels SET category='Outdoors' WHERE name=? AND category NOT IN ('Outdoors')", (name,))

# ── Sports ────────────────────────────────────────────────────────────────
for name in ("RACER", "Zona TUDN"):
    run("UPDATE channels SET category='Sports' WHERE name=? AND category NOT IN ('Sports')", (name,))

# ── Documentary ───────────────────────────────────────────────────────────
for name in ("Docu Vision", "Vice Entertainment"):
    run("UPDATE channels SET category='Documentary' WHERE name=? AND category NOT IN ('Documentary')", (name,))

# ── Science ───────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Science' WHERE name='AIR & SPACE' AND category!='Science'")

# ── Anime ─────────────────────────────────────────────────────────────────
for name in ("RetroCrush", "It's Anime"):
    run("UPDATE channels SET category='Anime' WHERE name=? AND category NOT IN ('Anime')", (name,))

# ── Westerns ──────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Westerns' WHERE name='Rawhide' AND category!='Westerns'")

# ── Gaming ────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Gaming' WHERE name='MrBeast' AND category NOT IN ('Gaming')")

# ── Faith ─────────────────────────────────────────────────────────────────
run("UPDATE channels SET category='Faith' WHERE name='Trinity Broadcast Network' AND category NOT IN ('Faith')")


con.commit()
con.close()
print(f"\nMigration 022 done — {total} rows updated.")

"""
Migration 003 — add epg_only flag to sources table.

EPG-only sources are excluded from M3U output but their program data
is used to enrich EPG for channels matched by name in other sources.

Run:
    docker exec fastchannels python /app/migrations/003_epg_only.py
"""
import sqlite3, sys, pathlib

DB_PATH = pathlib.Path("/data/fastchannels.db")
if not DB_PATH.exists():
    print(f"DB not found at {DB_PATH}", file=sys.stderr)
    sys.exit(1)

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# Check if column already exists
cols = [r[1] for r in cur.execute("PRAGMA table_info(sources)").fetchall()]
if "epg_only" in cols:
    print("Column epg_only already exists — skipping.")
else:
    cur.execute("ALTER TABLE sources ADD COLUMN epg_only BOOLEAN NOT NULL DEFAULT 0")
    con.commit()
    print("Added epg_only column to sources.")

con.close()
print("Migration 003 done.")

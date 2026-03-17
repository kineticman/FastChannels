"""
Migration 008 — add is_duplicate flag to channels table.

Marks a channel as a manual duplicate without disabling it.
Used by the admin UI to label channels for filtering purposes only.

Run:
    docker exec fastchannels python /app/migrations/008_is_duplicate.py
"""
import sqlite3, sys, pathlib

DB_PATH = pathlib.Path("/data/fastchannels.db")
if not DB_PATH.exists():
    print(f"DB not found at {DB_PATH}", file=sys.stderr)
    sys.exit(1)

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cols = [r[1] for r in cur.execute("PRAGMA table_info(channels)").fetchall()]
if "is_duplicate" in cols:
    print("Column is_duplicate already exists — skipping.")
else:
    cur.execute("ALTER TABLE channels ADD COLUMN is_duplicate BOOLEAN NOT NULL DEFAULT 0")
    con.commit()
    print("Added is_duplicate column to channels.")

con.close()
print("Migration 008 done.")

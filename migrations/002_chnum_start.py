"""
Migration: add chnum_start to sources and feeds tables.
Run with: docker exec fastchannels python /app/migrations/002_chnum_start.py
Safe to re-run — checks before altering anything.
"""
import sqlite3

DB_PATH = '/data/fastchannels.db'

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# ── sources.chnum_start ────────────────────────────────────────────────────────
cur.execute("PRAGMA table_info(sources)")
cols = {row[1] for row in cur.fetchall()}
if 'chnum_start' not in cols:
    cur.execute("ALTER TABLE sources ADD COLUMN chnum_start INTEGER")
    print("✅ Added sources.chnum_start")
else:
    print("✔  sources.chnum_start already exists")

# ── feeds.chnum_start ──────────────────────────────────────────────────────────
cur.execute("PRAGMA table_info(feeds)")
cols = {row[1] for row in cur.fetchall()}
if 'chnum_start' not in cols:
    cur.execute("ALTER TABLE feeds ADD COLUMN chnum_start INTEGER")
    print("✅ Added feeds.chnum_start")
else:
    print("✔  feeds.chnum_start already exists")

con.commit()
con.close()
print("\nMigration complete.")

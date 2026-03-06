"""
One-time migration: add gracenote_id to channels, fix feeds table schema.
Run with: docker exec fastchannels python /app/migrate.py
Safe to re-run — checks before altering anything.
"""
import sqlite3
import sys

DB_PATH = '/data/fastchannels.db'

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# ── channels.gracenote_id ─────────────────────────────────────────────────────
cur.execute("PRAGMA table_info(channels)")
cols = {row[1] for row in cur.fetchall()}
if 'gracenote_id' not in cols:
    cur.execute("ALTER TABLE channels ADD COLUMN gracenote_id VARCHAR(32)")
    print("✅ Added channels.gracenote_id")
else:
    print("✔  channels.gracenote_id already exists")

# ── feeds table ───────────────────────────────────────────────────────────────
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='feeds'")
if not cur.fetchone():
    cur.execute("""
        CREATE TABLE feeds (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            slug        VARCHAR(64) UNIQUE NOT NULL,
            name        VARCHAR(128) NOT NULL,
            description TEXT DEFAULT '',
            filters     JSON DEFAULT '{}',
            is_enabled  BOOLEAN DEFAULT 1,
            created_at  DATETIME,
            updated_at  DATETIME
        )
    """)
    print("✅ Created feeds table")
else:
    # Table exists — make sure all expected columns are present
    cur.execute("PRAGMA table_info(feeds)")
    feed_cols = {row[1] for row in cur.fetchall()}
    needed = {
        'description': 'TEXT DEFAULT ""',
        'filters':     'JSON DEFAULT "{}"',
        'is_enabled':  'BOOLEAN DEFAULT 1',
        'created_at':  'DATETIME',
        'updated_at':  'DATETIME',
    }
    for col, definition in needed.items():
        if col not in feed_cols:
            cur.execute(f"ALTER TABLE feeds ADD COLUMN {col} {definition}")
            print(f"✅ Added feeds.{col}")
        else:
            print(f"✔  feeds.{col} already exists")

con.commit()
con.close()
print("\nMigration complete.")

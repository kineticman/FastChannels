"""Migration 031: add channels.provider_number.

Stores the provider's own channel number for a channel ("213", "305.1") as
reported by the source's scraper — currently DirecTV Stream, which exposes its
guide numbers on the AllChannels endpoint. The M3U generators emit it as
tvg-chno only when the source's "Use DirecTV channel numbers" toggle is on.

Fresh installs get the column from db.create_all(); app/schema.py also adds it
at boot on existing SQLite installs, so this script is only needed when
upgrading a database that the app is not going to boot against first.

Idempotent: does nothing when the column already exists.
"""
import sqlite3

DB_PATH = "/data/fastchannels.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()
columns = {row[1] for row in cur.execute("PRAGMA table_info(channels)")}

if "provider_number" not in columns:
    cur.execute("ALTER TABLE channels ADD COLUMN provider_number VARCHAR(16)")

con.commit()
con.close()
print("Migration 031 done — channels.provider_number is present.")

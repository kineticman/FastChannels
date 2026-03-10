"""
Migration 005 — add channels_dvr_url to app_settings.

Stores the Channels DVR server base URL (e.g. http://192.168.1.x:8089)
so feeds can be pushed to DVR as custom M3U sources with one click.

Run:
    docker exec fastchannels python /app/migrations/005_channels_dvr_url.py
"""
import sqlite3, sys, pathlib

DB_PATH = pathlib.Path("/data/fastchannels.db")
if not DB_PATH.exists():
    print(f"DB not found at {DB_PATH}", file=sys.stderr)
    sys.exit(1)

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cols = [r[1] for r in cur.execute("PRAGMA table_info(app_settings)").fetchall()]
if "channels_dvr_url" in cols:
    print("Column channels_dvr_url already exists — skipping.")
else:
    cur.execute("ALTER TABLE app_settings ADD COLUMN channels_dvr_url TEXT")
    con.commit()
    print("Added channels_dvr_url to app_settings.")

con.close()
print("Migration 005 done.")

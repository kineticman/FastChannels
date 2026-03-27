"""
Migration 015: reduce Local Now scrape_interval from 360 to 60 minutes.

The Local Now API always returns exactly 5 programs per channel regardless of
the program_size parameter.  The worst-case channels (e.g. Euronews English)
cover only ~1 hour of future EPG per scrape.  With the old 360-minute interval
those channels had no guide data for ~5 hours between scrapes.

This migration resets the interval to 60 minutes for any localnow source that
still has the old 360-minute default.  Users who have already manually tuned
the interval to a different value are not affected.

Safe to re-run.
"""
import sqlite3

DB_PATH = '/data/fastchannels.db'

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute(
    "UPDATE sources SET scrape_interval=60 WHERE name='localnow' AND scrape_interval=360"
)
changed = cur.rowcount or 0
con.commit()
con.close()

if changed:
    print(f"✅ Updated Local Now scrape_interval to 60 minutes ({changed} source(s))")
else:
    print("ℹ Local Now scrape_interval already customised — no change made")

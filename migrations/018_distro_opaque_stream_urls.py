"""
Migration 018: rewrite Distro channel stream_urls to opaque distro://channel/ scheme.

Before this change, stream_url stored the raw CDN URL captured at scrape time
(e.g. https://d35j504z0x2vu2.cloudfront.net/...). These URLs are session-based
and expire within a few hours, causing 404s between scrapes.

After this change, stream_url stores distro://channel/<source_channel_id>
(e.g. distro://channel/US:39730). resolve() fetches a fresh URL from the
Distro API at play time using a 30-minute in-process cache.

Safe to re-run.
"""
import sqlite3

DB_PATH = "/data/fastchannels.db"

CHANNEL_SCHEME = "distro://channel/"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("SELECT id FROM sources WHERE name = 'distro'")
source_rows = [row[0] for row in cur.fetchall()]

updated = 0
for source_id in source_rows:
    cur.execute(
        "SELECT id, source_channel_id FROM channels WHERE source_id = ? AND stream_url NOT LIKE 'distro://%'",
        (source_id,),
    )
    for channel_id, source_channel_id in cur.fetchall():
        cur.execute(
            "UPDATE channels SET stream_url = ? WHERE id = ?",
            (f"{CHANNEL_SCHEME}{source_channel_id}", channel_id),
        )
        updated += 1

con.commit()
con.close()

print(f"Migration 018 done — rewrote {updated} Distro channel stream_urls to opaque scheme.")

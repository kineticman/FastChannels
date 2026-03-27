"""
Migration 016: reduce amazon_prime_free scrape_interval from 360 to 100 minutes.

Amazon DASH stream URLs have a ~2-hour TTL. The scraper caches them with a
1.5-hour TTL (90 min) to stay safely under that. With the old 360-minute
interval the entire cache was stale long before the next scrape, causing every
play request to fall through to an on-demand PRS resolution call.

This migration resets the interval to 100 minutes for any amazon_prime_free
source still at the old 360-minute default. Custom values are not touched.

Safe to re-run.
"""
import sqlite3

DB_PATH = '/data/fastchannels.db'

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute(
    "UPDATE sources SET scrape_interval=100 WHERE name='amazon_prime_free' AND scrape_interval=360"
)
changed = cur.rowcount or 0
con.commit()
con.close()

if changed:
    print(f"✅ Updated amazon_prime_free scrape_interval to 100 minutes ({changed} source(s))")
else:
    print("ℹ amazon_prime_free scrape_interval already customised — no change made")

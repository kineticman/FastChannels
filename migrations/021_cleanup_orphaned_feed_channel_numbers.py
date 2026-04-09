"""
Migration 021: purge orphaned feed_channel_numbers rows.

Before foreign_keys=ON was enabled, deleting a feed did not cascade-delete
its feed_channel_numbers rows.  This migration cleans up any such orphans so
the table is consistent when FK enforcement is active.

Safe to re-run (DELETE WHERE NOT IN is idempotent).
"""
import sqlite3

DB_PATH = "/data/fastchannels.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("""
    DELETE FROM feed_channel_numbers
    WHERE feed_id NOT IN (SELECT id FROM feeds)
""")
cur.execute("""
    DELETE FROM feed_channel_numbers
    WHERE channel_id NOT IN (SELECT id FROM channels)
""")
deleted = cur.rowcount
con.commit()
print(f"Migration 021 done — removed {deleted} orphaned feed_channel_numbers rows.")
con.close()

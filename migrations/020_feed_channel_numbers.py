"""
Migration 020: create feed_channel_numbers table for sticky feed tvg-chno.

Stores the persistent channel-number assignment for each (feed, channel) pair
so that disabling a channel does not reshuffle numbers for the rest of the feed.

Safe to re-run (CREATE TABLE IF NOT EXISTS).
"""
import sqlite3

DB_PATH = "/data/fastchannels.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS feed_channel_numbers (
        feed_id    INTEGER NOT NULL REFERENCES feeds(id)    ON DELETE CASCADE,
        channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
        number     INTEGER NOT NULL,
        PRIMARY KEY (feed_id, channel_id)
    )
""")
cur.execute("""
    CREATE INDEX IF NOT EXISTS ix_feed_channel_numbers_feed_id
    ON feed_channel_numbers (feed_id)
""")
con.commit()
print("Migration 020 done — created feed_channel_numbers table.")
con.close()

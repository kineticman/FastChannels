"""
Migration 019: add stream_info JSON column to channels table.

Populated by the stream audit and single-channel inspect.
Stores: max_resolution, max_width, max_height, video_codec, has_4k, has_hd, variants list.

Safe to re-run (ALTER TABLE is skipped if column already exists).
"""
import sqlite3

DB_PATH = "/data/fastchannels.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

existing = {row[1] for row in cur.execute("PRAGMA table_info(channels)").fetchall()}
if 'stream_info' not in existing:
    cur.execute("ALTER TABLE channels ADD COLUMN stream_info TEXT")
    con.commit()
    print("Migration 019 done — added stream_info column to channels.")
else:
    print("Migration 019 skipped — stream_info column already exists.")

con.close()

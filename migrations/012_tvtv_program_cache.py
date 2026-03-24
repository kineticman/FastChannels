"""
Migration 012: create tvtv_program_cache table.

Rolling 3-day cache of tvtv.us guide data for FAST channel stations.
Refreshed nightly by the background worker.

Run with: docker exec fastchannels python /app/migrate.py
Safe to re-run.
"""
import sqlite3

DB_PATH = '/data/fastchannels.db'

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tvtv_program_cache'")
if cur.fetchone():
    # Table exists — ensure subtitle column is present (added after initial migration)
    cur.execute("PRAGMA table_info(tvtv_program_cache)")
    cols = {row[1] for row in cur.fetchall()}
    if 'subtitle' not in cols:
        cur.execute("ALTER TABLE tvtv_program_cache ADD COLUMN subtitle VARCHAR(512)")
        con.commit()
        print("✅  Added subtitle column to tvtv_program_cache")
    else:
        print("✔  tvtv_program_cache already exists and is up to date")
else:
    cur.execute("""
        CREATE TABLE tvtv_program_cache (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id  VARCHAR(32)  NOT NULL,
            lineup      VARCHAR(64)  NOT NULL,
            program_id  VARCHAR(32),
            title       VARCHAR(512) NOT NULL,
            subtitle    VARCHAR(512),
            start_time  DATETIME     NOT NULL,
            end_time    DATETIME     NOT NULL,
            fetched_at  DATETIME     NOT NULL,
            UNIQUE (station_id, start_time)
        )
    """)
    cur.execute("CREATE INDEX idx_tvtv_station_start ON tvtv_program_cache (station_id, start_time)")
    cur.execute("CREATE INDEX idx_tvtv_end_time      ON tvtv_program_cache (end_time)")
    con.commit()
    print("✅  Created tvtv_program_cache")

con.close()

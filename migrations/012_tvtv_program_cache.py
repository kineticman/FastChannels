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
    print("✔  tvtv_program_cache already exists")
else:
    cur.execute("""
        CREATE TABLE tvtv_program_cache (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id  VARCHAR(32)  NOT NULL,
            lineup      VARCHAR(64)  NOT NULL,
            program_id  VARCHAR(32),
            title       VARCHAR(512) NOT NULL,
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

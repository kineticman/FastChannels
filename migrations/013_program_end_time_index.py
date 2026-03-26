"""
Migration 013: add index on programs.end_time.

Speeds up hourly EPG prune jobs so they do not scan the entire programs table.

Run with: docker exec fastchannels python /app/migrations/013_program_end_time_index.py
Safe to re-run.
"""
import sqlite3

DB_PATH = '/data/fastchannels.db'
INDEX_NAME = 'idx_programs_end_time'

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (INDEX_NAME,))
if cur.fetchone():
    print("✔  idx_programs_end_time already exists")
else:
    cur.execute(f"CREATE INDEX {INDEX_NAME} ON programs (end_time)")
    con.commit()
    print("✅  Created idx_programs_end_time")

con.close()

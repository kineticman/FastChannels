"""
Migration 004 — create app_settings table for global configuration.

Adds a single-row table to store app-wide settings like global_chnum_start.

Run:
    docker exec fastchannels python /app/migrations/004_app_settings.py
"""
import sqlite3, sys, pathlib

DB_PATH = pathlib.Path("/data/fastchannels.db")
if not DB_PATH.exists():
    print(f"DB not found at {DB_PATH}", file=sys.stderr)
    sys.exit(1)

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
if "app_settings" in tables:
    print("Table app_settings already exists — skipping.")
else:
    cur.execute("""
        CREATE TABLE app_settings (
            id                 INTEGER PRIMARY KEY,
            global_chnum_start INTEGER
        )
    """)
    cur.execute("INSERT INTO app_settings (id) VALUES (1)")
    con.commit()
    print("Created app_settings table.")

con.close()
print("Migration 004 done.")

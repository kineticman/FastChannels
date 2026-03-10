"""
Migration 006 — add public_base_url to app_settings.

Run with:
    docker exec fastchannels python /app/migrations/006_public_base_url.py
"""
import sqlite3

DB_PATH = "/data/fastchannels.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
if "app_settings" not in tables:
    print("Table app_settings does not exist — skipping.")
else:
    cols = [r[1] for r in cur.execute("PRAGMA table_info(app_settings)").fetchall()]
    if "public_base_url" in cols:
        print("Column public_base_url already exists — skipping.")
    else:
        cur.execute("ALTER TABLE app_settings ADD COLUMN public_base_url TEXT")
        conn.commit()
        print("Added public_base_url to app_settings.")

conn.close()

"""
Migration 011 — mark name-matched Gracenote channels as gracenote_mode='off'.

The cross-source name matching feature (commit f6d5cd4) was reverted because it
caused widespread incorrect Gracenote routing.  That feature routed any channel
to the Gracenote M3U if its name matched a channel that had an explicit
gracenote_id — regardless of whether that channel itself had one.

Without this migration, those channels revert to gracenote_mode='auto', which
means they could be picked up again if name matching is re-introduced or if a
scraper later writes a gracenote_id to them unexpectedly.

This migration sets gracenote_mode='off' on every channel that:
  - has no gracenote_id of its own
  - has gracenote_mode='auto' (i.e. not already manually configured)
  - whose name (case-insensitive) matches at least one channel that does
    have an explicit gracenote_id

This explicitly opts them out of Gracenote routing rather than leaving them
in an ambiguous 'auto' state.

Run:
    docker exec fastchannels python /app/migrations/011_disable_name_matched_gracenote.py
"""
import sqlite3, sys, pathlib

DB_PATH = pathlib.Path("/data/fastchannels.db")
if not DB_PATH.exists():
    print(f"DB not found at {DB_PATH}", file=sys.stderr)
    sys.exit(1)

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("""
    UPDATE channels
    SET gracenote_mode = 'off'
    WHERE (gracenote_id IS NULL OR gracenote_id = '')
      AND gracenote_mode NOT IN ('off', 'manual')
      AND LOWER(name) IN (
          SELECT LOWER(name)
          FROM channels
          WHERE gracenote_id IS NOT NULL AND gracenote_id != ''
      )
""")
n = cur.rowcount
con.commit()
con.close()
print(f"Migration 011 done — {n} channels set to gracenote_mode='off'.")

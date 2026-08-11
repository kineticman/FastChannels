"""
Migration 027: clear the literal string 'TBD' out of channels.gracenote_id.

Background: FOX One's API returns the placeholder string "TBD" for
gracenote.station_id on channels it hasn't assigned a real Gracenote ID to
yet. Prior to this fix, app/scrapers/fox_one.py stored that placeholder
as-is, so any install that scraped fox_one got a channel with a literal
'TBD' gracenote_id — which breaks Channels DVR guide matching once routed
through the Gracenote-variant output. The scraper fix (2026-08-10) stops
writing new 'TBD' values, but does nothing for rows that already have one:
the normal sync in worker.py's _upsert_channels only overwrites
gracenote_id on a content_changed event or when the freshly-computed value
is non-None, so an already-stored 'TBD' just sits there untouched forever.

Idempotent: a second run finds no more 'TBD' rows and does nothing. Fresh
installs never wrote this value, so this is a no-op for them. Scoped to the
exact literal 'TBD' (case-sensitive, matching what the API actually
returns) — never touches a real Gracenote ID, which is always numeric.
"""
import sqlite3

DB_PATH = "/data/fastchannels.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("SELECT COUNT(*) FROM channels WHERE gracenote_id = 'TBD'")
(count,) = cur.fetchone()

if count:
    cur.execute("UPDATE channels SET gracenote_id = NULL WHERE gracenote_id = 'TBD'")
    con.commit()
    print(f"Migration 027 done — cleared {count} channel(s) with gracenote_id='TBD'.")
else:
    print("Migration 027 done — no channels with gracenote_id='TBD' found.")

con.close()

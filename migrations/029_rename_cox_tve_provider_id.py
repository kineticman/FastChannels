"""
Migration 029: rename the shared TVE (Adobe Pass) account's provider_id from
'cox' to 'mvpd'.

Background: the single shared tve_accounts row used by every Adobe-Pass-based
TVE scraper (aenetworks_tve, amcn_tve, discovery_tve, fox_tve, fox_one,
nbc_tve, warner_tve) was keyed as provider_id='cox' because Cox was the first
MVPD wired up during development. The account itself is MSO-agnostic — its
config.selected_mso_id can be any Adobe Pass MSO (Sling, Spectrum, etc.), not
just Cox — so keeping 'cox' as its identity was misleading. Routes and code
now use provider_id='mvpd'; this carries the existing row (credentials,
config, auth history) forward under the new id so upgrading installs don't
lose their saved TVE account.

Idempotent: no-ops if there's no 'cox' row, or if a 'mvpd' row already
exists (never overwrites).
"""
import sqlite3

DB_PATH = "/data/fastchannels.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cox_row = cur.execute("SELECT id FROM tve_accounts WHERE provider_id = 'cox'").fetchone()
mvpd_row = cur.execute("SELECT id FROM tve_accounts WHERE provider_id = 'mvpd'").fetchone()

if cox_row is None:
    print("Migration 029 done — no 'cox' tve_accounts row to rename.")
elif mvpd_row is not None:
    print("Migration 029 done — 'mvpd' tve_accounts row already exists, left 'cox' row untouched.")
else:
    cur.execute("UPDATE tve_accounts SET provider_id = 'mvpd' WHERE provider_id = 'cox'")
    con.commit()
    print("Migration 029 done — renamed tve_accounts provider_id 'cox' -> 'mvpd'.")

con.close()

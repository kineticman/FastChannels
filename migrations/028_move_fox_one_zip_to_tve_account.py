"""
Migration 028: move FOX One's home_zip_code out of sources.config into the
shared tve_accounts.config (provider_id='cox').

Background: home_zip_code used to be a per-source ConfigField on fox_one
only. It's really a household fact (which market you're in), not something
specific to FOX One, so it moved to the shared TVE account config in
Settings > TV Everywhere — any future TVE source that needs a home market
(e.g. an nbc_tve local-affiliate lookup) can read the same value instead of
every source collecting its own copy. app/scrapers/fox_one.py now reads/
writes it via TVEAccount(provider_id='cox').config['home_zip_code']
exclusively; this migration carries forward any value an existing install
already had sitting in fox_one's own config so upgrading doesn't silently
blank it out.

Idempotent: a second run finds fox_one's config already missing
home_zip_code and does nothing. Fresh installs never had it in fox_one's
config to begin with. Never overwrites an already-set value on the shared
account (e.g. if the user already re-entered it in Settings since
upgrading).
"""
import json
import sqlite3
from datetime import datetime, timezone

DB_PATH = "/data/fastchannels.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

row = cur.execute("SELECT config FROM sources WHERE name = 'fox_one'").fetchone()
fox_one_config = json.loads(row[0]) if row and row[0] else {}
zip_code = (fox_one_config.get('home_zip_code') or '').strip()

if not zip_code:
    print("Migration 028 done — fox_one has no home_zip_code to move.")
else:
    now = datetime.now(timezone.utc).isoformat()
    account_row = cur.execute(
        "SELECT config FROM tve_accounts WHERE provider_id = 'cox'"
    ).fetchone()

    if account_row is None:
        cur.execute(
            "INSERT INTO tve_accounts "
            "(provider_id, display_name, is_enabled, config, created_at, updated_at) "
            "VALUES ('cox', 'Cox', 0, ?, ?, ?)",
            (json.dumps({'home_zip_code': zip_code}), now, now),
        )
        print(f"Migration 028 done — created tve_accounts row with home_zip_code={zip_code!r}.")
    else:
        account_config = json.loads(account_row[0]) if account_row[0] else {}
        if account_config.get('home_zip_code'):
            print("Migration 028 done — tve_accounts already has its own home_zip_code, left as-is.")
        else:
            account_config['home_zip_code'] = zip_code
            cur.execute(
                "UPDATE tve_accounts SET config = ?, updated_at = ? WHERE provider_id = 'cox'",
                (json.dumps(account_config), now),
            )
            print(f"Migration 028 done — copied home_zip_code={zip_code!r} onto tve_accounts.")

    fox_one_config.pop('home_zip_code', None)
    cur.execute(
        "UPDATE sources SET config = ? WHERE name = 'fox_one'",
        (json.dumps(fox_one_config),),
    )

con.commit()
con.close()

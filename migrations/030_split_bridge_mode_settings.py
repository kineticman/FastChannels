"""Migration 030: split the old PrismCast-only bridge toggle into policy + method.

Older versions stored ``drm_bridge_enabled`` for both “keep DRM channels eligible”
and “use PrismCast”.  The Bridge page now has a global ``bridge_enabled`` policy,
plus ``prismcast_enabled`` as an independent method toggle.  Player-only installs
remain active after upgrade; PrismCast installs retain both switches.

Idempotent: existing values of the new columns are never overwritten.
"""
import sqlite3

DB_PATH = "/data/fastchannels.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()
columns = {row[1] for row in cur.execute("PRAGMA table_info(app_settings)")}

if "bridge_enabled" not in columns:
    cur.execute("ALTER TABLE app_settings ADD COLUMN bridge_enabled BOOLEAN NOT NULL DEFAULT 0")
    player_clause = " OR COALESCE(fc_player_bridge_enabled, 0)" if "fc_player_bridge_enabled" in columns else ""
    cur.execute(
        "UPDATE app_settings SET bridge_enabled = "
        f"CASE WHEN COALESCE(drm_bridge_enabled, 0){player_clause} THEN 1 ELSE 0 END"
    )

if "prismcast_enabled" not in columns:
    cur.execute("ALTER TABLE app_settings ADD COLUMN prismcast_enabled BOOLEAN NOT NULL DEFAULT 0")
    cur.execute("UPDATE app_settings SET prismcast_enabled = COALESCE(drm_bridge_enabled, 0)")

con.commit()
con.close()
print("Migration 030 done — Bridge policy and PrismCast method settings are present.")

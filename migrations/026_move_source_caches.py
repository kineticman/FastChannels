"""
Migration 026: relocate large regenerable caches out of sources.config into the
dedicated source_cache key/value table.

Background: sources.config (a JSON column) held both real config and big runtime
caches, mixed by key. Any query that materialized a Source entity deserialized the
whole blob — Roku's config grew to ~1.24 MB (~94 % cache), and a (Channel, Source)
report join json.loads()'d it once per joined row, spiking a worker to 2.4 GB.
Hotfix 9bc8d7f added defer(Source.config); this migration is the durable root fix:
move the caches so no Source-entity load ever pays to deserialize them.

What it does, per source:
  - copies each known cache key from config into a source_cache row, then
  - pops that key from config and writes back the slimmed config.

Copy (not drop-then-rebuild) so the first post-migration scrape doesn't cold-fetch
(esp. Roku's 14-day description cache). INSERT OR IGNORE so if the new code already
wrote a fresher row for a key, the stale config copy is simply discarded, never
clobbering the live value. Idempotent: a second run finds the keys already gone
from config and does nothing. Fresh installs never carried these keys in config.
"""
import json
import sqlite3
from datetime import datetime, timezone

DB_PATH = "/data/fastchannels.db"

# Keys that are regenerable runtime caches (now homed in source_cache). Listed as a
# union across every scraper that used config for caching, plus the audit reports,
# which can be written on any audited source. Keys absent from a given source's
# config are simply skipped.
CACHE_KEYS = {
    # Roku
    "description_cache",
    "selector_url_cache",
    "play_id_cache",
    "osm_session",
    "playback_query_cache",
    # Roku + Amazon share this key name
    "stream_url_cache",
    # Amazon
    "channel_pe",
    # Frndly
    "content_cache",
    # Audit reports (any source)
    "last_audit_report",
    "last_audit_result",
}


con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# Self-create the table so the migration is safe to run standalone (entrypoint's
# ensure_runtime_schema() also creates it on every boot). Mirrors models.SourceCache.
cur.execute(
    "CREATE TABLE IF NOT EXISTS source_cache ("
    " id INTEGER PRIMARY KEY,"
    " source_id INTEGER NOT NULL REFERENCES sources(id),"
    " cache_key VARCHAR(64) NOT NULL,"
    " value JSON,"
    " updated_at DATETIME,"
    " CONSTRAINT uq_source_cache_key UNIQUE (source_id, cache_key)"
    ")"
)
cur.execute(
    "CREATE INDEX IF NOT EXISTS idx_source_cache_source ON source_cache (source_id)"
)

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

sources_touched = 0
rows_copied = 0
keys_dropped = 0

cur.execute("SELECT id, name, config FROM sources")
for source_id, name, raw_config in cur.fetchall():
    if not raw_config:
        continue
    try:
        config = json.loads(raw_config) if isinstance(raw_config, str) else raw_config
    except (ValueError, TypeError):
        continue
    if not isinstance(config, dict):
        continue

    present = [k for k in config if k in CACHE_KEYS]
    if not present:
        continue

    for key in present:
        value = config.pop(key)
        keys_dropped += 1
        # INSERT OR IGNORE: keep any fresher row the running app already wrote.
        cur.execute(
            "INSERT OR IGNORE INTO source_cache (source_id, cache_key, value, updated_at)"
            " VALUES (?, ?, ?, ?)",
            (source_id, key, json.dumps(value), now),
        )
        rows_copied += cur.rowcount or 0

    cur.execute(
        "UPDATE sources SET config = ? WHERE id = ?",
        (json.dumps(config), source_id),
    )
    sources_touched += 1
    print(f"  {name}: moved {len(present)} cache key(s) out of config: {sorted(present)}")

con.commit()
con.close()

print(
    f"Migration 026 done — slimmed {sources_touched} source(s), "
    f"copied {rows_copied} new cache row(s), dropped {keys_dropped} config key(s)."
)

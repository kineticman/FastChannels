"""
Migration 007 — rename Amazon Prime Free source id to snake_case.

Run with:
    docker exec fastchannels python /app/migrations/007_amazon_source_name_snake_case.py
"""
import json
import sqlite3

DB_PATH = "/data/fastchannels.db"
OLD = "amazon-prime-free"
NEW = "amazon_prime_free"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("UPDATE sources SET name = ? WHERE name = ?", (NEW, OLD))
sources_updated = cur.rowcount

feeds_updated = 0
rows = cur.execute("SELECT id, filters FROM feeds").fetchall()
for feed_id, raw_filters in rows:
    if not raw_filters:
        continue
    try:
        filters = json.loads(raw_filters) if isinstance(raw_filters, str) else raw_filters
    except Exception:
        continue
    if not isinstance(filters, dict):
        continue

    sources = filters.get("sources")
    if not isinstance(sources, list) or OLD not in sources:
        continue

    filters["sources"] = [NEW if value == OLD else value for value in sources]
    cur.execute("UPDATE feeds SET filters = ? WHERE id = ?", (json.dumps(filters), feed_id))
    feeds_updated += 1

conn.commit()
conn.close()

print(f"Updated sources rows: {sources_updated}")
print(f"Updated feed filters: {feeds_updated}")

"""
Migration 014: seed Channel.number from the current effective lineup.

This snapshots the numbering users already see today so the new sticky
allocator preserves it going forward instead of renumbering thousands of
channels on first deploy.

Safe to re-run.
"""
import sqlite3

DB_PATH = '/data/fastchannels.db'


def _sort_key(row):
    number = row['number']
    return (
        number is None,
        number if number is not None else 0,
        (row['name'] or '').lower(),
        row['source_channel_id'] or '',
    )


con = sqlite3.connect(DB_PATH)
con.row_factory = sqlite3.Row
cur = con.cursor()

default_feed_start = None
cur.execute("SELECT chnum_start FROM feeds WHERE slug='default' AND is_enabled=1 LIMIT 1")
row = cur.fetchone()
if row:
    default_feed_start = row['chnum_start']

global_start = None
cur.execute("SELECT global_chnum_start FROM app_settings WHERE id=1 LIMIT 1")
row = cur.fetchone()
if row:
    global_start = row['global_chnum_start']

rows = cur.execute(
    """
    SELECT
      c.id,
      c.name,
      c.number,
      c.number_pinned,
      c.source_channel_id,
      s.name AS source_name,
      s.display_name AS source_display_name,
      s.chnum_start AS source_chnum_start
    FROM channels c
    JOIN sources s ON s.id = c.source_id
    WHERE c.is_active = 1
      AND c.is_enabled = 1
      AND s.is_enabled = 1
      AND s.epg_only = 0
      AND c.stream_url IS NOT NULL
    """
).fetchall()

if not rows:
    print("ℹ No active channels found for sticky numbering seed")
    con.close()
    raise SystemExit(0)

channels = [dict(r) for r in rows]
pinned_numbers = {
    ch['number'] for ch in channels
    if ch['number_pinned'] and ch['number'] is not None
}
updates: dict[int, int] = {}

if default_feed_start is not None:
    cursor = default_feed_start
    for ch in sorted(channels, key=_sort_key):
        if ch['number_pinned'] and ch['number'] is not None:
            continue
        while cursor in pinned_numbers:
            cursor += 1
        updates[ch['id']] = cursor
        cursor += 1
else:
    by_source: dict[str, list[dict]] = {}
    source_starts: dict[str, int] = {}
    source_labels: dict[str, str] = {}
    for ch in channels:
        src = ch['source_name']
        by_source.setdefault(src, []).append(ch)
        source_labels[src] = ch['source_display_name'] or src
        if ch['source_chnum_start']:
            source_starts[src] = ch['source_chnum_start']

    for src in by_source:
        by_source[src].sort(key=_sort_key)

    base_map: dict[str, int] = {}
    if global_start is not None:
        cursor = global_start
        unconfigured = sorted(
            (src for src in by_source if src not in source_starts),
            key=lambda src: (source_labels.get(src, src).lower(), src),
        )
        for src in unconfigured:
            base_map[src] = cursor
            cursor += len(by_source[src])

    ordered_sources = sorted(
        by_source,
        key=lambda src: (
            src not in source_starts,
            source_starts.get(src, 0),
            source_labels.get(src, src).lower(),
            src,
        ),
    )

    for src in ordered_sources:
        chs = by_source[src]
        if src in source_starts:
            cursor = source_starts[src]
            for ch in chs:
                if ch['number_pinned'] and ch['number'] is not None:
                    continue
                while cursor in pinned_numbers:
                    cursor += 1
                updates[ch['id']] = cursor
                cursor += 1
        elif src in base_map:
            cursor = base_map[src]
            for ch in chs:
                if ch['number_pinned'] and ch['number'] is not None:
                    continue
                while cursor in pinned_numbers:
                    cursor += 1
                updates[ch['id']] = cursor
                cursor += 1

changed = 0
for channel_id, number in updates.items():
    cur.execute("UPDATE channels SET number=? WHERE id=? AND number_pinned=0", (number, channel_id))
    changed += cur.rowcount or 0

con.commit()
con.close()
print(f"✅ Seeded sticky channel numbers for {changed} channel(s)")

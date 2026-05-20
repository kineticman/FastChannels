"""
Migration 025: normalize legacy Vidaa channel IDs to region-qualified IDs.

Before multi-region Vidaa support, source_channel_id values were stored as raw
UUIDs such as "14813f01-2a15-41bb-a38b-c950d76898ba". Multi-region support now
uses qualified IDs such as "US:14813f01-2a15-41bb-a38b-c950d76898ba". Existing
installs that had already scraped Vidaa ended up with both forms — the bare UUID
went inactive after enough missed scrapes, while the US:-prefixed twin became the
live channel, leaving a permanent "hangover" duplicate that never self-cleans.

This migration repairs existing Vidaa rows by:
  - merging collisions where both "uuid" and "US:uuid" exist (bare deleted)
  - rewriting lone legacy IDs from "uuid" to "US:uuid" (safety fallback)
  - repointing program rows to the surviving channel
  - deleting obsolete legacy duplicate rows

Safe to re-run.
"""
import sqlite3

DB_PATH = "/data/fastchannels.db"


def _coalesce_channel(cur: sqlite3.Cursor, legacy_id: int, prefixed_id: int) -> None:
    cur.execute(
        """
        SELECT
            name, slug, logo_url, stream_url, stream_type, category,
            category_override, language, language_override, country, tags,
            number, number_pinned, gracenote_id, gracenote_locked,
            gracenote_mode, guide_key, disable_reason, is_duplicate,
            is_active, is_enabled, last_seen_at, missed_scrapes
        FROM channels
        WHERE id = ?
        """,
        (legacy_id,),
    )
    legacy = cur.fetchone()
    if not legacy:
        return

    (
        legacy_name, legacy_slug, legacy_logo_url, legacy_stream_url,
        legacy_stream_type, legacy_category, legacy_category_override,
        legacy_language, legacy_language_override, legacy_country, legacy_tags,
        legacy_number, legacy_number_pinned, legacy_gracenote_id,
        legacy_gracenote_locked, legacy_gracenote_mode, legacy_guide_key,
        legacy_disable_reason, legacy_is_duplicate, legacy_is_active,
        legacy_is_enabled, legacy_last_seen_at, legacy_missed_scrapes,
    ) = legacy

    cur.execute(
        """
        UPDATE channels
        SET
            name = COALESCE(NULLIF(name, ''), ?),
            slug = COALESCE(NULLIF(slug, ''), ?),
            logo_url = COALESCE(NULLIF(logo_url, ''), ?),
            stream_url = COALESCE(NULLIF(stream_url, ''), ?),
            stream_type = COALESCE(NULLIF(stream_type, ''), ?),
            category = COALESCE(NULLIF(category, ''), ?),
            category_override = COALESCE(NULLIF(category_override, ''), ?),
            language = COALESCE(NULLIF(language, ''), ?),
            language_override = COALESCE(NULLIF(language_override, ''), ?),
            country = COALESCE(NULLIF(country, ''), ?),
            tags = COALESCE(NULLIF(tags, ''), ?),
            number = COALESCE(number, ?),
            number_pinned = COALESCE(number_pinned, 0) OR COALESCE(?, 0),
            gracenote_id = COALESCE(NULLIF(gracenote_id, ''), ?),
            gracenote_locked = COALESCE(gracenote_locked, 0) OR COALESCE(?, 0),
            gracenote_mode = COALESCE(NULLIF(gracenote_mode, ''), ?),
            guide_key = COALESCE(NULLIF(guide_key, ''), ?),
            disable_reason = COALESCE(NULLIF(disable_reason, ''), ?),
            is_duplicate = COALESCE(is_duplicate, 0) OR COALESCE(?, 0),
            is_active = COALESCE(is_active, 0) OR COALESCE(?, 0),
            is_enabled = COALESCE(is_enabled, 0) OR COALESCE(?, 0),
            last_seen_at = COALESCE(last_seen_at, ?),
            missed_scrapes = MIN(COALESCE(missed_scrapes, 0), COALESCE(?, 0))
        WHERE id = ?
        """,
        (
            legacy_name, legacy_slug, legacy_logo_url, legacy_stream_url,
            legacy_stream_type, legacy_category, legacy_category_override,
            legacy_language, legacy_language_override, legacy_country, legacy_tags,
            legacy_number, legacy_number_pinned, legacy_gracenote_id,
            legacy_gracenote_locked, legacy_gracenote_mode, legacy_guide_key,
            legacy_disable_reason, legacy_is_duplicate, legacy_is_active,
            legacy_is_enabled, legacy_last_seen_at, legacy_missed_scrapes,
            prefixed_id,
        ),
    )


con = sqlite3.connect(DB_PATH)
cur = con.cursor()
cur.execute("PRAGMA foreign_keys=ON")  # ensures feed_channel_numbers cascade-deletes on channel DELETE

cur.execute("SELECT id FROM sources WHERE name = 'vidaa'")
source_rows = [row[0] for row in cur.fetchall()]

merged = 0
renamed = 0
programs_repointed = 0

for source_id in source_rows:
    cur.execute(
        "SELECT id, source_channel_id FROM channels WHERE source_id = ? AND instr(source_channel_id, ':') = 0",
        (source_id,),
    )
    legacy_rows = cur.fetchall()

    for legacy_id, raw_id in legacy_rows:
        prefixed_sid = f"US:{raw_id}"
        cur.execute(
            "SELECT id FROM channels WHERE source_id = ? AND source_channel_id = ?",
            (source_id, prefixed_sid),
        )
        prefixed = cur.fetchone()

        if prefixed:
            prefixed_channel_id = prefixed[0]
            if prefixed_channel_id != legacy_id:
                _coalesce_channel(cur, legacy_id, prefixed_channel_id)
                cur.execute(
                    "UPDATE programs SET channel_id = ? WHERE channel_id = ?",
                    (prefixed_channel_id, legacy_id),
                )
                programs_repointed += cur.rowcount or 0
                cur.execute("DELETE FROM channels WHERE id = ?", (legacy_id,))
                merged += 1
            continue

        cur.execute(
            "UPDATE channels SET source_channel_id = ? WHERE id = ?",
            (prefixed_sid, legacy_id),
        )
        renamed += cur.rowcount or 0

con.commit()
con.close()

print(
    f"Migration 025 done — renamed {renamed} Vidaa channel IDs, "
    f"merged {merged} duplicate pairs, repointed {programs_repointed} program rows."
)

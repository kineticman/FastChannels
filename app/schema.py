from sqlalchemy import text

from .extensions import db

_DEFAULT_FEEDS = (
    {
        "slug": "default",
        "name": "Default",
        "description": "Built-in feed with all enabled channels.",
        "filters": "{}",
        "chnum_start": 5000,
        "is_enabled": 1,
    },
)


def _merge_source_name(conn, old_name: str, new_name: str) -> None:
    old_rows = conn.execute(
        text("SELECT id FROM sources WHERE name = :name ORDER BY id"),
        {"name": old_name},
    ).fetchall()
    if not old_rows:
        return

    new_row = conn.execute(
        text("SELECT id FROM sources WHERE name = :name ORDER BY id LIMIT 1"),
        {"name": new_name},
    ).fetchone()

    if not new_row:
        conn.execute(
            text("UPDATE sources SET name = :new_name WHERE name = :old_name"),
            {"new_name": new_name, "old_name": old_name},
        )
        return

    target_source_id = new_row[0]
    for (old_source_id,) in old_rows:
        channel_rows = conn.execute(
            text(
                "SELECT id, source_channel_id FROM channels "
                "WHERE source_id = :source_id ORDER BY id"
            ),
            {"source_id": old_source_id},
        ).fetchall()

        for old_channel_id, source_channel_id in channel_rows:
            existing_channel = conn.execute(
                text(
                    "SELECT id FROM channels "
                    "WHERE source_id = :source_id AND "
                    "((source_channel_id = :source_channel_id) OR "
                    "(:source_channel_id IS NULL AND source_channel_id IS NULL)) "
                    "ORDER BY id LIMIT 1"
                ),
                {
                    "source_id": target_source_id,
                    "source_channel_id": source_channel_id,
                },
            ).fetchone()

            if existing_channel:
                conn.execute(
                    text(
                        "UPDATE programs SET channel_id = :target_channel_id "
                        "WHERE channel_id = :old_channel_id"
                    ),
                    {
                        "target_channel_id": existing_channel[0],
                        "old_channel_id": old_channel_id,
                    },
                )
                conn.execute(
                    text("DELETE FROM channels WHERE id = :channel_id"),
                    {"channel_id": old_channel_id},
                )
                continue

            conn.execute(
                text("UPDATE channels SET source_id = :target_source_id WHERE id = :channel_id"),
                {
                    "target_source_id": target_source_id,
                    "channel_id": old_channel_id,
                },
            )

        conn.execute(text("DELETE FROM sources WHERE id = :source_id"), {"source_id": old_source_id})


def ensure_runtime_schema() -> None:
    engine = db.engine
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as conn:
        tables = {
            row[0]
            for row in conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
        }
        if "app_settings" not in tables or "feeds" not in tables:
            db.create_all()
            tables = {
                row[0]
                for row in conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            }
        if "feeds" not in tables:
            return

        # source_cache: key/value home for large regenerable scraper caches that
        # used to bloat Source.config (see models.SourceCache). Fresh installs get
        # this via db.create_all(); existing installs need the guard so the table
        # exists before the workers start writing caches.
        if "source_cache" not in tables:
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS source_cache ("
                " id INTEGER PRIMARY KEY,"
                " source_id INTEGER NOT NULL REFERENCES sources(id),"
                " cache_key VARCHAR(64) NOT NULL,"
                " value JSON,"
                " updated_at DATETIME,"
                " CONSTRAINT uq_source_cache_key UNIQUE (source_id, cache_key)"
                ")"
            ))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_source_cache_source"
                " ON source_cache (source_id)"
            ))
            tables.add("source_cache")

        if "app_settings" in tables:
            cols = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(app_settings)"))
            }
            if "public_base_url" not in cols:
                conn.execute(text("ALTER TABLE app_settings ADD COLUMN public_base_url TEXT"))
            if "timezone_name" not in cols:
                conn.execute(text("ALTER TABLE app_settings ADD COLUMN timezone_name TEXT"))
            if "gracenote_auto_fill" not in cols:
                # Existing installs default ON — preserve current auto-fill behaviour.
                conn.execute(text(
                    "ALTER TABLE app_settings ADD COLUMN gracenote_auto_fill BOOLEAN NOT NULL DEFAULT 1"
                ))
            if "gracenote_map_url" not in cols:
                conn.execute(text("ALTER TABLE app_settings ADD COLUMN gracenote_map_url TEXT"))
            if "migration_012_done" not in cols:
                conn.execute(text(
                    "ALTER TABLE app_settings ADD COLUMN migration_012_done BOOLEAN NOT NULL DEFAULT 0"
                ))
            if "migration_025_done" not in cols:
                conn.execute(text(
                    "ALTER TABLE app_settings ADD COLUMN migration_025_done BOOLEAN NOT NULL DEFAULT 0"
                ))
            if "gracenote_contribution_url" not in cols:
                conn.execute(text(
                    "ALTER TABLE app_settings ADD COLUMN gracenote_contribution_url TEXT"
                ))
            if "last_contribution_at" not in cols:
                conn.execute(text(
                    "ALTER TABLE app_settings ADD COLUMN last_contribution_at DATETIME"
                ))
            if "dvr_epg_auto_refresh" not in cols:
                conn.execute(text(
                    "ALTER TABLE app_settings ADD COLUMN dvr_epg_auto_refresh BOOLEAN NOT NULL DEFAULT 1"
                ))
            if "image_proxy_enabled" not in cols:
                conn.execute(text(
                    "ALTER TABLE app_settings ADD COLUMN image_proxy_enabled BOOLEAN NOT NULL DEFAULT 1"
                ))
            if "auto_allow_new_channels" not in cols:
                # Existing installs default ON — preserve current behaviour where
                # newly-scraped channels flow straight into feeds.  Turning this OFF
                # makes new channels land in the review queue (per-source override
                # via Source.new_channel_policy still wins).
                conn.execute(text(
                    "ALTER TABLE app_settings ADD COLUMN auto_allow_new_channels BOOLEAN NOT NULL DEFAULT 1"
                ))
            if "prismcast_url" not in cols:
                conn.execute(text("ALTER TABLE app_settings ADD COLUMN prismcast_url TEXT"))
            if "prismcast_inner_url" not in cols:
                conn.execute(text("ALTER TABLE app_settings ADD COLUMN prismcast_inner_url TEXT"))

        if "sources" in tables:
            src_cols = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(sources)"))
            }
            if "last_audited_at" not in src_cols:
                conn.execute(text(
                    "ALTER TABLE sources ADD COLUMN last_audited_at DATETIME"
                ))
            if "scrape_cron" not in src_cols:
                conn.execute(text(
                    "ALTER TABLE sources ADD COLUMN scrape_cron TEXT"
                ))
            if "last_channel_fetch_at" not in src_cols:
                # Gates the channel_refresh_hours skip independently of last_scraped_at
                # (which EPG-only runs bump every interval). NULL on existing installs
                # forces one full fetch_channels() on the next scrape, then self-heals.
                conn.execute(text(
                    "ALTER TABLE sources ADD COLUMN last_channel_fetch_at DATETIME"
                ))
            if "last_epg_success_at" not in src_cols:
                # Stamped only when the EPG phase commits programs successfully.
                # last_scraped_at is bumped right after the channel commit (before
                # EPG runs), so a channels-OK / EPG-failed run looks like a full
                # success. This distinct clock lets the staleness canary detect EPG
                # that has silently stopped advancing while channels keep refreshing.
                # NULL on existing installs → next successful EPG run stamps it.
                conn.execute(text(
                    "ALTER TABLE sources ADD COLUMN last_epg_success_at DATETIME"
                ))
            if "gracenote_resync_done" not in src_cols:
                conn.execute(text(
                    "ALTER TABLE sources ADD COLUMN gracenote_resync_done BOOLEAN NOT NULL DEFAULT 0"
                ))
            if "new_channel_policy" not in src_cols:
                # 'inherit' = follow AppSettings.auto_allow_new_channels;
                # 'enabled' = always add new channels enabled;
                # 'review'  = always hold new channels for review.
                # Existing sources default to 'inherit' so behaviour is unchanged.
                conn.execute(text(
                    "ALTER TABLE sources ADD COLUMN new_channel_policy VARCHAR(16) NOT NULL DEFAULT 'inherit'"
                ))

        if "channels" in tables:
            ch_cols = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(channels)"))
            }
            if "category_override" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN category_override VARCHAR(128)"
                ))
            if "language_override" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN language_override VARCHAR(16)"
                ))
            if "tags" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN tags TEXT"
                ))
            if "is_duplicate" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN is_duplicate BOOLEAN NOT NULL DEFAULT 0"
                ))
            if "last_seen_at" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN last_seen_at DATETIME"
                ))
            if "missed_scrapes" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN missed_scrapes INTEGER NOT NULL DEFAULT 0"
                ))
            if "guide_key" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN guide_key TEXT"
                ))
            if "number_pinned" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN number_pinned BOOLEAN NOT NULL DEFAULT 0"
                ))
            if "gracenote_locked" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN gracenote_locked BOOLEAN NOT NULL DEFAULT 0"
                ))
            if "gracenote_mode" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN gracenote_mode TEXT NOT NULL DEFAULT 'auto'"
                ))
            if "logo_url_pinned" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN logo_url_pinned BOOLEAN NOT NULL DEFAULT 0"
                ))
            if "description" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN description TEXT"
                ))
            if "custom_headers" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN custom_headers JSON"
                ))
            if "proxy_segments" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN proxy_segments BOOLEAN NOT NULL DEFAULT 0"
                ))
            if "page_url" not in ch_cols:
                conn.execute(text("ALTER TABLE channels ADD COLUMN page_url TEXT"))
            if "redetect_on_play" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN redetect_on_play BOOLEAN NOT NULL DEFAULT 0"
                ))
            if "guide_block_minutes" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN guide_block_minutes INTEGER"
                ))
            if "scrape_pinned" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN scrape_pinned BOOLEAN NOT NULL DEFAULT 0"
                ))
            if "went_inactive_at" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN went_inactive_at DATETIME"
                ))
                conn.execute(text(
                    "UPDATE channels SET went_inactive_at = last_seen_at "
                    "WHERE is_active = 0 AND last_seen_at IS NOT NULL"
                ))
            if "returned_at" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN returned_at DATETIME"
                ))
            if "user_note" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN user_note TEXT"
                ))
            if "identity_changed_at" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN identity_changed_at DATETIME"
                ))
            if "previous_name" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN previous_name VARCHAR(256)"
                ))
            if "previous_gracenote_id" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN previous_gracenote_id VARCHAR(32)"
                ))
            if "content_swap_count" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN content_swap_count INTEGER NOT NULL DEFAULT 0"
                ))
            if "review_state" not in ch_cols:
                # 'approved' = reviewed/auto-allowed and eligible for output;
                # 'pending'  = newly discovered, held out of all feeds until reviewed.
                # Existing channels are all 'approved' so nothing disappears on upgrade.
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN review_state VARCHAR(16) NOT NULL DEFAULT 'approved'"
                ))
            if "first_seen_at" not in ch_cols:
                conn.execute(text(
                    "ALTER TABLE channels ADD COLUMN first_seen_at DATETIME"
                ))
                # Backfill: treat existing rows' created_at as their first-seen time so
                # any future "new in last N days" filtering has sane history.
                conn.execute(text(
                    "UPDATE channels SET first_seen_at = COALESCE(created_at, CURRENT_TIMESTAMP) "
                    "WHERE first_seen_at IS NULL"
                ))
            conn.execute(text(
                "UPDATE channels SET went_inactive_at = last_seen_at "
                "WHERE is_active = 0 AND last_seen_at IS NOT NULL AND went_inactive_at IS NULL"
            ))
            conn.execute(text(
                "UPDATE channels SET missed_scrapes = 0 WHERE missed_scrapes IS NULL"
            ))
            conn.execute(text(
                "UPDATE channels SET gracenote_locked = 0 WHERE gracenote_locked IS NULL"
            ))
            # Backfill the user/scraper flag columns. The oldest installs created these
            # as nullable (create_all before they gained nullable=False) and schema.py
            # never ALTERed them, so a stray NULL is possible — and a NULL is_enabled /
            # is_active reads as falsy, silently dropping the channel from M3U/EPG.
            # No-op on any install that never had NULLs.
            conn.execute(text(
                "UPDATE channels SET is_enabled = 1 WHERE is_enabled IS NULL"
            ))
            conn.execute(text(
                "UPDATE channels SET is_active = 1 WHERE is_active IS NULL"
            ))
            conn.execute(text(
                "UPDATE channels SET is_duplicate = 0 WHERE is_duplicate IS NULL"
            ))
            conn.execute(text(
                "UPDATE channels SET gracenote_mode = 'manual' "
                "WHERE gracenote_locked = 1 AND gracenote_id IS NOT NULL AND TRIM(gracenote_id) != ''"
            ))
            conn.execute(text(
                "UPDATE channels SET gracenote_mode = 'off' "
                "WHERE (gracenote_id IS NULL OR TRIM(gracenote_id) = '') "
                "AND gracenote_mode IS NULL"
            ))
            conn.execute(text(
                "UPDATE channels SET gracenote_mode = 'auto' "
                "WHERE gracenote_mode IS NULL OR TRIM(gracenote_mode) = ''"
            ))
            conn.execute(text(
                "UPDATE channels "
                "SET last_seen_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP) "
                "WHERE is_active = 1 AND last_seen_at IS NULL"
            ))
            # Migration 011: channels that had no gracenote_id of their own but shared a
            # name with a channel that did were silently routed to the Gracenote M3U by
            # the cross-source name-matching feature (commit f6d5cd4, reverted).  Set
            # gracenote_mode='off' on those channels so they stay out of Gracenote
            # routing even if name matching is re-introduced.
            # Guard: only run if any gracenote_ids exist — when auto-fill is OFF this
            # is a no-op and avoids incorrectly setting channels to 'off' on restart.
            _has_gn = conn.execute(text(
                "SELECT 1 FROM channels WHERE gracenote_id IS NOT NULL AND gracenote_id != '' LIMIT 1"
            )).fetchone()
            if _has_gn:
                conn.execute(text(
                    "UPDATE channels "
                    "SET gracenote_mode = 'off' "
                    "WHERE (gracenote_id IS NULL OR gracenote_id = '') "
                    "AND gracenote_mode NOT IN ('off', 'manual') "
                    "AND LOWER(name) IN ("
                    "    SELECT LOWER(name) FROM channels "
                    "    WHERE gracenote_id IS NOT NULL AND gracenote_id != ''"
                    ")"
                ))

        # Migration 012: clear gracenote_ids that came from the community CSV rather
        # than the native scraper API.  Channels with gracenote_mode='manual' are left
        # untouched (user explicitly set them).  Cleared channels get gracenote_mode='off'
        # so the scraper won't re-populate them from the CSV on the next scrape, and users
        # can re-assign via the Gracenote helper popup if desired.
        # This migration is one-time: once applied it is marked done so that community CSV
        # updates (including the bundled baseline) don't keep clearing scraped IDs on restart.
        if "channels" in tables and "sources" in tables and "app_settings" in tables:
            _m012_done = conn.execute(
                text("SELECT migration_012_done FROM app_settings WHERE id = 1")
            ).fetchone()
            if not _m012_done or not _m012_done[0]:
                from .gracenote_map import lookup_gracenote
                rows = conn.execute(text(
                    "SELECT c.id, c.gracenote_id, s.name, c.source_channel_id "
                    "FROM channels c JOIN sources s ON c.source_id = s.id "
                    "WHERE c.gracenote_id IS NOT NULL AND c.gracenote_id != '' "
                    "AND (c.gracenote_mode IS NULL OR c.gracenote_mode NOT IN ('manual', 'off'))"
                )).fetchall()
                to_clear = [
                    row[0] for row in rows
                    if (m := lookup_gracenote(row[2], row[3])) and m.get('tmsid') == row[1]
                ]
                if to_clear:
                    conn.execute(
                        text("UPDATE channels SET gracenote_id = NULL, gracenote_mode = 'off' WHERE id = :id"),
                        [{'id': rid} for rid in to_clear],
                    )
                conn.execute(text("UPDATE app_settings SET migration_012_done = 1 WHERE id = 1"))

        # Migration 025: collapse legacy bare-UUID Vidaa channel IDs into their
        # region-qualified US: counterparts.  Before multi-region support, channels
        # were stored without a region prefix; each scrape since then has been
        # creating a US:-prefixed twin while the old bare row stagnated inactive.
        if "channels" in tables and "sources" in tables and "app_settings" in tables:
            _m025_done = conn.execute(
                text("SELECT migration_025_done FROM app_settings WHERE id = 1")
            ).fetchone()
            if not _m025_done or not _m025_done[0]:
                conn.execute(text("PRAGMA foreign_keys = ON"))
                vidaa_sources = conn.execute(
                    text("SELECT id FROM sources WHERE name = 'vidaa'")
                ).fetchall()
                _m025_merged = 0
                _m025_renamed = 0
                for (_vsrc_id,) in vidaa_sources:
                    legacy_rows = conn.execute(text(
                        "SELECT id, source_channel_id FROM channels "
                        "WHERE source_id = :sid AND instr(source_channel_id, ':') = 0"
                    ), {"sid": _vsrc_id}).fetchall()
                    for _leg_id, _raw_id in legacy_rows:
                        _prefixed_sid = f"US:{_raw_id}"
                        _twin = conn.execute(text(
                            "SELECT id FROM channels WHERE source_id = :sid AND source_channel_id = :scid"
                        ), {"sid": _vsrc_id, "scid": _prefixed_sid}).fetchone()
                        if _twin and _twin[0] != _leg_id:
                            _twin_id = _twin[0]
                            # Merge user-set fields from the legacy row into the live twin,
                            # then repoint any programs and delete the stale bare-UUID row.
                            _leg = conn.execute(text(
                                "SELECT number, number_pinned, gracenote_id, gracenote_locked,"
                                "       gracenote_mode, is_enabled, missed_scrapes"
                                " FROM channels WHERE id = :lid"
                            ), {"lid": _leg_id}).fetchone()
                            if _leg:
                                conn.execute(text(
                                    "UPDATE channels SET"
                                    "  number = COALESCE(number, :num),"
                                    "  number_pinned = COALESCE(number_pinned, 0) OR COALESCE(:np, 0),"
                                    "  gracenote_id = COALESCE(NULLIF(gracenote_id,''), :gid),"
                                    "  gracenote_locked = COALESCE(gracenote_locked,0) OR COALESCE(:gl,0),"
                                    "  gracenote_mode = COALESCE(NULLIF(gracenote_mode,''), :gm),"
                                    "  is_enabled = COALESCE(is_enabled, 0) OR COALESCE(:en, 0),"
                                    "  missed_scrapes = MIN(COALESCE(missed_scrapes,0), COALESCE(:ms,0))"
                                    " WHERE id = :tid"
                                ), {
                                    "num": _leg[0], "np": _leg[1], "gid": _leg[2],
                                    "gl": _leg[3], "gm": _leg[4], "en": _leg[5],
                                    "ms": _leg[6], "tid": _twin_id,
                                })
                            conn.execute(text(
                                "UPDATE programs SET channel_id = :tid WHERE channel_id = :lid"
                            ), {"tid": _twin_id, "lid": _leg_id})
                            conn.execute(text("DELETE FROM channels WHERE id = :lid"), {"lid": _leg_id})
                            _m025_merged += 1
                        elif not _twin:
                            conn.execute(text(
                                "UPDATE channels SET source_channel_id = :scid WHERE id = :lid"
                            ), {"scid": _prefixed_sid, "lid": _leg_id})
                            _m025_renamed += 1
                if _m025_merged or _m025_renamed:
                    import logging as _logging
                    _logging.getLogger(__name__).info(
                        "Migration 025: merged %d / renamed %d Vidaa channel ID(s)",
                        _m025_merged, _m025_renamed,
                    )
                conn.execute(text(
                    "INSERT OR IGNORE INTO app_settings (id, migration_025_done) VALUES (1, 0)"
                ))
                conn.execute(text("UPDATE app_settings SET migration_025_done = 1 WHERE id = 1"))

        if "tvtv_program_cache" in tables:
            tvtv_cols = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(tvtv_program_cache)"))
            }
            if "subtitle" not in tvtv_cols:
                conn.execute(text(
                    "ALTER TABLE tvtv_program_cache ADD COLUMN subtitle VARCHAR(512)"
                ))

        if "programs" in tables:
            program_cols = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(programs)"))
            }
            if "original_air_date" not in program_cols:
                conn.execute(text(
                    "ALTER TABLE programs ADD COLUMN original_air_date DATE"
                ))
            if "is_live" not in program_cols:
                conn.execute(text(
                    "ALTER TABLE programs ADD COLUMN is_live BOOLEAN"
                ))
            if "program_type" not in program_cols:
                conn.execute(text(
                    "ALTER TABLE programs ADD COLUMN program_type VARCHAR(16)"
                ))
            if "series_id" not in program_cols:
                conn.execute(text(
                    "ALTER TABLE programs ADD COLUMN series_id VARCHAR(64)"
                ))
            if "episode_id" not in program_cols:
                conn.execute(text(
                    "ALTER TABLE programs ADD COLUMN episode_id VARCHAR(64)"
                ))
            existing_indexes = {
                row[1]
                for row in conn.execute(text("PRAGMA index_list(programs)"))
            }
            if "idx_programs_end_time" not in existing_indexes:
                conn.execute(text(
                    "CREATE INDEX idx_programs_end_time ON programs (end_time)"
                ))
            if "idx_programs_channel_end_start" not in existing_indexes:
                conn.execute(text(
                    "CREATE INDEX idx_programs_channel_end_start"
                    " ON programs (channel_id, end_time, start_time)"
                ))

        # Normalize the one hyphenated internal source id to snake_case so
        # source naming stays consistent across code paths and fresh installs.
        # Older installs may have both names present due to alias seeding, so
        # merge rows first to avoid violating the unique constraint.
        _merge_source_name(conn, "amazon-prime-free", "amazon_prime_free")

        # Backfill stream_type='dash' for existing Amazon Prime Free channels
        # so the Shaka player is selected in the preview panel without a re-scrape.
        if "channels" in tables and "sources" in tables:
            conn.execute(text(
                "UPDATE channels SET stream_type = 'dash' "
                "WHERE (stream_type IS NULL OR stream_type = 'hls') "
                "AND source_id IN (SELECT id FROM sources WHERE name = 'amazon_prime_free')"
            ))

        feed_rows = conn.execute(text("SELECT id, filters FROM feeds")).fetchall()
        for feed_id, raw_filters in feed_rows:
            if not raw_filters:
                continue
            try:
                import json
                filters = json.loads(raw_filters) if isinstance(raw_filters, str) else raw_filters
            except Exception:
                continue
            if not isinstance(filters, dict):
                continue
            sources = filters.get("sources")
            if not isinstance(sources, list) or "amazon-prime-free" not in sources:
                continue
            filters["sources"] = [
                "amazon_prime_free" if value == "amazon-prime-free" else value
                for value in sources
            ]
            conn.execute(
                text("UPDATE feeds SET filters = :filters WHERE id = :feed_id"),
                {"filters": json.dumps(filters), "feed_id": feed_id},
            )

        existing_slugs = {
            row[0]
            for row in conn.execute(text("SELECT slug FROM feeds"))
        }
        for feed in _DEFAULT_FEEDS:
            if feed["slug"] in existing_slugs:
                continue
            conn.execute(
                text(
                    "INSERT INTO feeds "
                    "(slug, name, description, filters, chnum_start, is_enabled) "
                    "VALUES (:slug, :name, :description, :filters, :chnum_start, :is_enabled)"
                ),
                feed,
            )

        # Plex compound channel ID migration.
        # Plex channel IDs have the format "{server_id}-{channel_id}" where the
        # server prefix rotates when Plex migrates infrastructure.  The stable
        # identifier is the channel part (= gridKey).  This block normalises all
        # source_channel_id / stream_url values to the channel part and removes
        # duplicate channel rows that arose from a prefix rotation.
        # Fast-exit: if no plex channels with a hyphenated ID exist, there is
        # nothing to do (idempotent on every subsequent boot).
        if "channels" in tables and "sources" in tables:
            _plex_src = conn.execute(
                text("SELECT id FROM sources WHERE name = 'plex' LIMIT 1")
            ).fetchone()
            if _plex_src:
                _plex_src_id = _plex_src[0]
                _has_compound = conn.execute(text(
                    "SELECT 1 FROM channels "
                    "WHERE source_id = :sid AND instr(source_channel_id, '-') > 0 LIMIT 1"
                ), {"sid": _plex_src_id}).fetchone()
                if _has_compound:
                    import re as _plex_re
                    _PLEX_CID_RE = _plex_re.compile(r'^[0-9a-f]{24}-([0-9a-f]{24})$')

                    _plex_rows = conn.execute(text(
                        "SELECT id, source_channel_id, guide_key "
                        "FROM channels WHERE source_id = :sid"
                    ), {"sid": _plex_src_id}).fetchall()

                    from collections import defaultdict as _dd
                    _by_part: dict = _dd(list)
                    for _ch_id, _scid, _gk in _plex_rows:
                        _m = _PLEX_CID_RE.match(_scid or '')
                        _part = _gk or (_m.group(1) if _m else _scid)
                        if _part:
                            _by_part[_part].append(_ch_id)

                    _deduped = 0
                    _normalized = 0
                    for _part, _cids in _by_part.items():
                        _cids.sort()          # lowest id = oldest row = has programs
                        _winner = _cids[0]
                        # Grab the newest loser's stream_url — it carries the current
                        # server prefix which the Plex play API requires.
                        _newest_loser_url = None
                        for _loser in _cids[1:]:
                            _loser_row = conn.execute(text(
                                "SELECT stream_url FROM channels WHERE id = :l"
                            ), {"l": _loser}).fetchone()
                            if _loser_row and _loser_row[0]:
                                _newest_loser_url = _loser_row[0]
                            conn.execute(text(
                                "UPDATE programs SET channel_id = :w WHERE channel_id = :l"
                            ), {"w": _winner, "l": _loser})
                            conn.execute(text(
                                "DELETE FROM feed_channel_numbers WHERE channel_id = :l"
                            ), {"l": _loser})
                            conn.execute(text(
                                "DELETE FROM channels WHERE id = :l"
                            ), {"l": _loser})
                            _deduped += 1
                        # Normalize source_channel_id to the stable channel part.
                        # Update stream_url to the newest loser's URL (current server prefix)
                        # if available; otherwise keep stream_url as-is.
                        _update = {"part": _part, "w": _winner}
                        if _newest_loser_url:
                            conn.execute(text(
                                "UPDATE channels "
                                "SET source_channel_id = :part, stream_url = :url "
                                "WHERE id = :w"
                            ), {**_update, "url": _newest_loser_url})
                        else:
                            conn.execute(text(
                                "UPDATE channels SET source_channel_id = :part WHERE id = :w"
                            ), _update)
                        _normalized += 1

                    if _deduped or _normalized:
                        import logging as _log
                        _log.getLogger(__name__).info(
                            "Plex channel ID migration: %d normalized, %d duplicates removed",
                            _normalized, _deduped,
                        )

        # Apply category corrections to all existing channels on startup so
        # upgrading users don't have to wait for a full re-scrape cycle.
        if "channels" in tables:
            from .scrapers.category_utils import category_for_channel
            # LEFT JOIN so channels with a NULL or dangling source_id still get
            # category corrections backfilled (INNER JOIN would silently skip them).
            rows = conn.execute(text(
                "SELECT c.id, c.name, c.category, s.name "
                "FROM channels c LEFT JOIN sources s ON c.source_id = s.id"
            )).fetchall()
            updates = []
            for row_id, name, cat, source_name in rows:
                new_cat = category_for_channel(name, cat, source_name)
                if new_cat != cat:
                    updates.append((new_cat, row_id))
            if updates:
                conn.execute(
                    text("UPDATE channels SET category = :cat WHERE id = :id"),
                    [{"cat": cat, "id": row_id} for cat, row_id in updates],
                )

        # Migrate global_chnum_start from AppSettings → default Feed.chnum_start.
        # AppSettings.global_chnum_start is now legacy; the Feed column is authoritative.
        if "app_settings" in tables:
            as_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(app_settings)"))}
            if "global_chnum_start" in as_cols:
                row = conn.execute(
                    text("SELECT global_chnum_start FROM app_settings WHERE id = 1")
                ).fetchone()
                if row and row[0] is not None:
                    default_feed_row = conn.execute(
                        text("SELECT id, chnum_start FROM feeds WHERE slug = 'default' LIMIT 1")
                    ).fetchone()
                    if default_feed_row and default_feed_row[1] is None:
                        conn.execute(
                            text("UPDATE feeds SET chnum_start = :val WHERE id = :fid"),
                            {"val": row[0], "fid": default_feed_row[0]},
                        )
                    conn.execute(
                        text("UPDATE app_settings SET global_chnum_start = NULL WHERE id = 1")
                    )

        # Clear stale state on sources that are never auto-scraped (scrape_interval=0).
        # A force-refresh-all could have stamped last_scraped_at and incremented
        # missed_scrapes on their channels (since fetch_channels returns []), causing
        # a perpetually overdue next-scrape date and false "at risk" channel flags.
        conn.execute(
            text("UPDATE sources SET last_scraped_at = NULL WHERE scrape_interval = 0")
        )
        conn.execute(
            text(
                "UPDATE channels SET missed_scrapes = 0"
                " WHERE missed_scrapes > 0"
                " AND source_id IN (SELECT id FROM sources WHERE scrape_interval = 0)"
            )
        )

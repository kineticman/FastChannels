from sqlalchemy import text

from .extensions import db

_DEFAULT_FEEDS = (
    {
        "slug": "default",
        "name": "Default",
        "description": "Built-in feed with all enabled channels.",
        "filters": "{}",
        "chnum_start": None,
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
        if "feeds" not in tables:
            return

        if "app_settings" in tables:
            cols = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(app_settings)"))
            }
            if "public_base_url" not in cols:
                conn.execute(text("ALTER TABLE app_settings ADD COLUMN public_base_url TEXT"))
            if "timezone_name" not in cols:
                conn.execute(text("ALTER TABLE app_settings ADD COLUMN timezone_name TEXT"))

        if "sources" in tables:
            src_cols = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(sources)"))
            }
            if "last_audited_at" not in src_cols:
                conn.execute(text(
                    "ALTER TABLE sources ADD COLUMN last_audited_at DATETIME"
                ))

        if "channels" in tables:
            ch_cols = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(channels)"))
            }
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
            conn.execute(text(
                "UPDATE channels SET missed_scrapes = 0 WHERE missed_scrapes IS NULL"
            ))
            conn.execute(text(
                "UPDATE channels "
                "SET last_seen_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP) "
                "WHERE is_active = 1 AND last_seen_at IS NULL"
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

        # Normalize the one hyphenated internal source id to snake_case so
        # source naming stays consistent across code paths and fresh installs.
        # Older installs may have both names present due to alias seeding, so
        # merge rows first to avoid violating the unique constraint.
        _merge_source_name(conn, "amazon-prime-free", "amazon_prime_free")

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

        # Apply category corrections to all existing channels on startup so
        # upgrading users don't have to wait for a full re-scrape cycle.
        if "channels" in tables:
            from .scrapers.category_utils import category_for_channel
            rows = conn.execute(text("SELECT id, name, category FROM channels")).fetchall()
            updates = [
                (category_for_channel(name, cat), row_id)
                for row_id, name, cat in rows
                if category_for_channel(name, cat) != cat
            ]
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

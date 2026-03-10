from sqlalchemy import text

from .extensions import db


def ensure_runtime_schema() -> None:
    engine = db.engine
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as conn:
        tables = {
            row[0]
            for row in conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
        }
        if "app_settings" not in tables:
            return

        cols = {
            row[1]
            for row in conn.execute(text("PRAGMA table_info(app_settings)"))
        }
        if "public_base_url" not in cols:
            conn.execute(text("ALTER TABLE app_settings ADD COLUMN public_base_url TEXT"))

        # Normalize the one hyphenated internal source id to snake_case so
        # source naming stays consistent across code paths and fresh installs.
        conn.execute(
            text("UPDATE sources SET name = 'amazon_prime_free' WHERE name = 'amazon-prime-free'")
        )

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

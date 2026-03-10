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

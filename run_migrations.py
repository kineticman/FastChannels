#!/usr/bin/env python3
"""
Run numbered upgrade migrations exactly once per database.

Safe to invoke on every container startup:
 - creates the schema_migrations tracking table if missing
 - runs only unapplied scripts in /app/migrations
 - records a migration only after it exits successfully
"""
from __future__ import annotations

import pathlib
import sqlite3
import subprocess
import sys


DB_PATH = pathlib.Path("/data/fastchannels.db")
MIGRATIONS_DIR = pathlib.Path("/app/migrations")


def _ensure_tracking_table(con: sqlite3.Connection) -> set[str]:
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name TEXT PRIMARY KEY,
            applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.commit()
    cur.execute("SELECT name FROM schema_migrations")
    return {row[0] for row in cur.fetchall()}


def main() -> int:
    if not DB_PATH.exists():
        print(f"DB not found at {DB_PATH}", file=sys.stderr)
        return 1

    con = sqlite3.connect(DB_PATH)
    try:
        applied = _ensure_tracking_table(con)
        scripts = sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.py"))
        for script in scripts:
            name = script.name
            if name in applied:
                continue

            print(f"==> Running migration {name}")
            subprocess.run([sys.executable, str(script)], check=True)
            con.execute("INSERT INTO schema_migrations (name) VALUES (?)", (name,))
            con.commit()
            print(f"==> Applied migration {name}")
    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

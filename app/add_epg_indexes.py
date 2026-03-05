"""
add_epg_indexes.py — run once to add performance indexes.

Usage:
    docker exec fastchannels python3 /app/add_epg_indexes.py
"""
from app import create_app
from app.extensions import db

app = create_app()
with app.app_context():
    indexes = [
        # Programs: the two columns hit hardest by EPG queries
        ("idx_programs_channel_id",  "CREATE INDEX IF NOT EXISTS idx_programs_channel_id  ON programs (channel_id)"),
        ("idx_programs_id",          "CREATE INDEX IF NOT EXISTS idx_programs_id           ON programs (id)"),
        ("idx_programs_start_time",  "CREATE INDEX IF NOT EXISTS idx_programs_start_time   ON programs (start_time)"),
        # Channels: source lookups and active/enabled filters
        ("idx_channels_source_id",   "CREATE INDEX IF NOT EXISTS idx_channels_source_id    ON channels (source_id)"),
        ("idx_channels_active",      "CREATE INDEX IF NOT EXISTS idx_channels_active        ON channels (is_active, is_enabled)"),
    ]

    for name, sql in indexes:
        db.session.execute(db.text(sql))
        print(f"  ✓ {name}")

    db.session.commit()
    print("\nDone. Run ANALYZE to update query planner statistics:")
    db.session.execute(db.text("ANALYZE"))
    db.session.commit()
    print("  ✓ ANALYZE complete")

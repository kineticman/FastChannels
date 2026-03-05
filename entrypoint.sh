#!/bin/bash
set -e

echo "🚀 Starting FastChannels..."

# Start Redis
redis-server --daemonize yes --logfile /var/log/redis.log
echo "✅ Redis ready"

# Create DB tables directly from models
cd /app
python -c "from app import create_app; from app.extensions import db; app = create_app(); app.app_context().push(); db.create_all()"
echo "✅ DB ready"

# Seed sources
python -c "from app.worker import seed_sources; seed_sources()" || true
echo "✅ Sources seeded"

# Start background worker
python -m app.worker &
echo "✅ Worker started"

echo "✅ Starting gunicorn on port 5523"
exec gunicorn --bind 0.0.0.0:5523 --workers 2 --timeout 120 "app:create_app()"

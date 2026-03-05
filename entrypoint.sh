#!/bin/bash
set -e

echo "🚀 Starting FastChannels..."

# Start Redis
redis-server --daemonize yes --logfile /var/log/redis.log
echo "✅ Redis started"

# Wait for Redis to be ready before proceeding
echo "⏳ Waiting for Redis..."
for i in $(seq 1 30); do
    if redis-cli ping > /dev/null 2>&1; then
        echo "✅ Redis ready"
        break
    fi
    if [ "$i" = "30" ]; then
        echo "❌ Redis did not become ready in time"
        exit 1
    fi
    sleep 0.5
done

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

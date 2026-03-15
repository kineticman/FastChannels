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

# Ensure the default SQLite data directory exists before app startup.
mkdir -p /data

# Create DB tables directly from models
cd /app
python -c "from app import create_app; from app.extensions import db; app = create_app(); app.app_context().push(); db.create_all()"
echo "✅ DB ready"

# Seed sources
python -c "from app.worker import seed_sources; seed_sources()" || true
echo "✅ Sources seeded"

wait_for_network() {
    echo "⏳ Waiting for outbound network and DNS..."
    for i in $(seq 1 30); do
        if python - <<'PY'
import socket
import sys

targets = [
    ("therokuchannel.roku.com", 443),
    ("watch.sling.com", 443),
    ("tubitv.com", 443),
    ("valencia-app-mds.xumo.com", 443),
]

try:
    for host, port in targets:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        last_error = None
        connected = False
        for family, socktype, proto, _, sockaddr in infos:
            try:
                with socket.socket(family, socktype, proto) as sock:
                    sock.settimeout(3)
                    sock.connect(sockaddr)
                connected = True
                break
            except OSError as exc:
                last_error = exc
        if not connected:
            raise last_error or OSError(f"could not connect to {host}:{port}")
except Exception as exc:
    print(f"network check failed: {exc}", file=sys.stderr)
    sys.exit(1)
PY
        then
            echo "✅ Network ready"
            return 0
        fi
        sleep 2
    done

    echo "⚠ Network was not ready after 60s; starting anyway"
    return 0
}

wait_for_network

# Start background worker
python -m app.worker &
echo "✅ Worker started"

echo "✅ Starting gunicorn on port 5523"
exec gunicorn \
    --bind 0.0.0.0:5523 \
    --workers 4 \
    --timeout 300 \
    --keep-alive 0 \
    --worker-tmp-dir /dev/shm \
    --access-logfile - \
    --access-logformat '%(h)s "%(r)s" %(s)s %(b)s %(T)ss' \
    "app:create_app()"

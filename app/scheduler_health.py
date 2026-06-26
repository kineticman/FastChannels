"""Scheduler liveness heartbeat.

The scheduler runs in its own supervised process (see entrypoint.sh). If it
crash-loops or hangs, the restart wrapper masks the failure into a repeating log
line and scrapes silently stop — nothing in the UI ever says so. (This is exactly
what a bad container ``TZ`` did once: BackgroundScheduler() raised at construction,
the supervisor restarted it every 5s, and no scrapes ran for days unnoticed.)

To make that impossible to miss, the scheduler stamps a heartbeat in Redis on
every healthy tick. The web process — which is independent of the scheduler — reads
it on the dashboard and shows a loud banner the moment it goes stale or never
appears. The heartbeat is written ONLY from the recurring job and a successful
start, never from process startup, so a crash loop (which never reaches a healthy
tick) leaves it stale/absent rather than falsely fresh.
"""

import time

import redis

HEARTBEAT_KEY = 'fc:scheduler:heartbeat'
# The auto_scrape job ticks every 60s; allow 5 missed ticks before alarming so a
# single slow cycle or brief restart doesn't flap the banner.
STALE_AFTER_SECONDS = 300


def write_heartbeat(redis_url: str) -> None:
    """Stamp the current time as the scheduler's last healthy tick. Best-effort:
    never let heartbeat bookkeeping raise into the scheduler loop."""
    try:
        redis.from_url(redis_url).set(HEARTBEAT_KEY, str(int(time.time())))
    except Exception:
        pass


def read_heartbeat(redis_url: str) -> dict:
    """Return ``{ts, age_seconds, present, stale}`` describing the last tick.

    ``present`` is False when no heartbeat has ever been written (fresh boot, or a
    scheduler that has never reached a healthy tick). Callers decide how to treat
    an absent heartbeat — on the dashboard, absent + enabled sources = alarm.
    """
    try:
        raw = redis.from_url(redis_url).get(HEARTBEAT_KEY)
    except Exception:
        return {'ts': None, 'age_seconds': None, 'present': False, 'stale': False}
    if not raw:
        return {'ts': None, 'age_seconds': None, 'present': False, 'stale': False}
    try:
        ts = int(raw)
    except (TypeError, ValueError):
        return {'ts': None, 'age_seconds': None, 'present': False, 'stale': False}
    age = max(0.0, time.time() - ts)
    return {'ts': ts, 'age_seconds': age, 'present': True, 'stale': age > STALE_AFTER_SECONDS}

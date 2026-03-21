#!/usr/bin/env python3
"""
FastChannels two-queue stress test.

Usage:
    python3 stress_test.py [--base-url http://localhost:5523] [--source-id 1]

What it does:
  1. Discovers sources and picks the longest-running one (Roku > Plex > others)
  2. Pre-probes all endpoints to establish their baseline state
  3. Triggers a force-full scrape on that source (occupies the scraper queue)
  4. Concurrently hammers all endpoints for 90 seconds with 6 threads
  5. Reports per-endpoint p50/p95/max latency and pass/fail verdict
  6. PASS: fast-queue endpoints stay < 5s; cached reads stay < 10s

Key assertion: /m3u, /epg.xml, scrape-status should all respond quickly
even while a Roku/Plex scrape is in progress — proving queue isolation.

Note on /m3u 503s: a 503 means the M3U artifact hasn't been built yet
(cold cache — requires at least one completed scrape). The test detects
this upfront and handles it accordingly:
  - Cold at start → 503 during test is expected, not an error
  - Warm at start → any 503 during test IS an error (regression)
"""

import argparse
import concurrent.futures
import sys
import time
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests

# ── Config ────────────────────────────────────────────────────────────────────

FAST_THRESHOLD_S  = 5.0   # API + fast-queue endpoints
READ_THRESHOLD_S  = 10.0  # cached artifact endpoints (/m3u, /epg.xml)
HAMMER_DURATION_S = 90    # seconds to hammer after scrape is triggered
CONCURRENCY       = 6     # concurrent threads
REQUEST_TIMEOUT   = 30    # per-request timeout

PREFERRED_LONG_SOURCES = ['roku', 'plex', 'samsung', 'pluto', 'tubi']

# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class Result:
    elapsed: float
    status:  int
    ok:      bool

@dataclass
class Stats:
    label:     str
    threshold: float
    cold:      bool = False   # True when endpoint returned 503 at pre-probe
    results:   List[Result] = field(default_factory=list)
    _lock:     threading.Lock = field(default_factory=threading.Lock)

    def add(self, r: Result):
        with self._lock:
            self.results.append(r)

    def times(self):
        with self._lock:
            return [r.elapsed for r in self.results]

    def errors(self):
        with self._lock:
            return [r for r in self.results if not r.ok]

    def status_counts(self):
        counts: Dict[int, int] = {}
        with self._lock:
            for r in self.results:
                counts[r.status] = counts.get(r.status, 0) + 1
        return counts

    def p(self, pct):
        t = sorted(self.times())
        if not t:
            return 0.0
        idx = max(0, min(int(len(t) * pct / 100), len(t) - 1))
        return t[idx]

    def passed(self):
        if not self.times():
            return False
        return self.p(95) < self.threshold

    def summary(self):
        t = self.times()
        errs = self.errors()
        if not t:
            return f"  {self.label:<52}  NO DATA"

        flag = "✓ PASS" if self.passed() else "✗ FAIL"
        cold_note = "  [cold cache — 503s expected]" if self.cold else ""
        status_str = "  statuses:" + ",".join(f"{k}×{v}" for k, v in sorted(self.status_counts().items()))

        return (
            f"  {self.label:<52}  "
            f"n={len(t):4d}  "
            f"p50={self.p(50):5.2f}s  "
            f"p95={self.p(95):5.2f}s  "
            f"max={max(t):5.2f}s  "
            f"err={len(errs):3d}  "
            f"{flag}  (≤{self.threshold:.0f}s)"
            f"{status_str}"
            f"{cold_note}"
        )


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _request(session, method, url, stats: Stats, ok_statuses=(200,)):
    t0 = time.monotonic()
    try:
        if method == "POST":
            r = session.post(url, json={}, timeout=REQUEST_TIMEOUT)
        else:
            r = session.get(url, timeout=REQUEST_TIMEOUT)
        elapsed = time.monotonic() - t0
        ok = r.status_code in ok_statuses
        stats.add(Result(elapsed, r.status_code, ok))
    except Exception as e:
        elapsed = time.monotonic() - t0
        stats.add(Result(elapsed, 0, False))


def probe(session, url) -> int:
    """Single GET, returns status code (0 on exception)."""
    try:
        r = session.get(url, timeout=10)
        return r.status_code
    except Exception:
        return 0


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='FastChannels two-queue stress test')
    parser.add_argument('--base-url', default='http://localhost:5523')
    parser.add_argument('--source-id', type=int, default=None)
    args = parser.parse_args()

    base = args.base_url.rstrip('/')
    session = requests.Session()
    session.headers['User-Agent'] = 'FastChannels-StressTest/1.0'

    print(f"\n{'='*78}")
    print(f"  FastChannels Two-Queue Stress Test  →  {base}")
    print(f"{'='*78}\n")

    # ── 1. Discover sources ──────────────────────────────────────────────────
    print("► Discovering sources...")
    try:
        sources = session.get(f"{base}/api/sources", timeout=10).json()
    except Exception as e:
        print(f"  FATAL: {e}")
        sys.exit(1)

    if not sources:
        print("  FATAL: no sources configured")
        sys.exit(1)

    for s in sources:
        print(f"    id={s['id']:3d}  {s['name']:<22s}  enabled={s.get('is_enabled')}")

    target = None
    if args.source_id:
        target = next((s for s in sources if s['id'] == args.source_id), None)
        if not target:
            print(f"  FATAL: source_id={args.source_id} not found")
            sys.exit(1)
    else:
        for pref in PREFERRED_LONG_SOURCES:
            target = next((s for s in sources if pref in s['name'].lower()), None)
            if target:
                break
        target = target or sources[0]

    sid = target['id']
    print(f"\n  → Scrape target: id={sid} name={target['name']}\n")

    # ── 2. Define endpoints ───────────────────────────────────────────────────
    # (label, url, threshold, method)
    endpoint_defs = [
        ("GET /m3u",                           f"{base}/m3u",                                      READ_THRESHOLD_S, "GET"),
        ("GET /epg.xml",                       f"{base}/epg.xml",                                  READ_THRESHOLD_S, "GET"),
        (f"GET /api/sources/{sid}/scrape-status", f"{base}/api/sources/{sid}/scrape-status",       FAST_THRESHOLD_S, "GET"),
        ("GET /api/stats",                     f"{base}/api/stats",                                FAST_THRESHOLD_S, "GET"),
        ("GET /api/system-stats",              f"{base}/api/system-stats",                         FAST_THRESHOLD_S, "GET"),
        ("GET /api/sources",                   f"{base}/api/sources",                              FAST_THRESHOLD_S, "GET"),
        ("GET /api/channels?page=1",           f"{base}/api/channels?page=1&per_page=50",          FAST_THRESHOLD_S, "GET"),
        ("POST /api/sources/force-refresh",    f"{base}/api/sources/force-refresh",                FAST_THRESHOLD_S, "POST"),
    ]

    # ── 3. Pre-probe to establish baseline ───────────────────────────────────
    print("► Pre-probing endpoints (baseline before scrape)...")
    stats_map: Dict[str, Stats] = {}
    for label, url, threshold, method in endpoint_defs:
        if method == "POST":
            status = 200  # don't trigger the actual POST pre-flight
        else:
            status = probe(session, url)

        cold = (status == 503)
        stats_map[label] = Stats(label=label, threshold=threshold, cold=cold)
        note = "COLD (503 — artifact not built yet)" if cold else f"HTTP {status}"
        print(f"    {label:<52}  {note}")

    # Output endpoints where 503 is accepted (cold cache) vs must-be-200
    def ok_statuses_for(label: str, cold: bool):
        if cold and label in ("GET /m3u", "GET /epg.xml"):
            return (200, 503)  # cold cache: 503 is still valid during warm-up
        return (200,)

    # ── 4. Trigger the long scrape ────────────────────────────────────────────
    print(f"\n► Triggering force-full scrape on '{target['name']}' to occupy scraper queue...")
    try:
        r = session.post(f"{base}/api/sources/{sid}/run", timeout=10)
        print(f"  HTTP {r.status_code} — {r.text[:100]}")
    except Exception as e:
        print(f"  WARNING: {e}")

    time.sleep(2)  # let scrape actually start

    # ── 5. Hammer ─────────────────────────────────────────────────────────────
    print(f"\n► Hammering {len(endpoint_defs)} endpoints for {HAMMER_DURATION_S}s "
          f"with {CONCURRENCY} threads (silent — see results below)...\n")

    deadline = time.monotonic() + HAMMER_DURATION_S

    def worker():
        s = requests.Session()
        s.headers['User-Agent'] = 'FastChannels-StressTest/1.0'
        while time.monotonic() < deadline:
            for label, url, _, method in endpoint_defs:
                if time.monotonic() >= deadline:
                    break
                ok_st = ok_statuses_for(label, stats_map[label].cold)
                _request(s, method, url, stats_map[label], ok_statuses=ok_st)
                time.sleep(0.1)
            time.sleep(0.3)

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = [pool.submit(worker) for _ in range(CONCURRENCY)]
        t_start = time.monotonic()
        while time.monotonic() < deadline:
            elapsed = time.monotonic() - t_start
            total = sum(len(s.times()) for s in stats_map.values())
            print(f"  {elapsed:4.0f}s elapsed  |  {total:5d} requests fired", end='\r')
            time.sleep(2)
        concurrent.futures.wait(futures)

    print(f"\n")

    # ── 6. Final scrape status ────────────────────────────────────────────────
    print("► Scrape status at end of test:")
    try:
        r = session.get(f"{base}/api/sources/{sid}/scrape-status", timeout=10)
        print(f"  HTTP {r.status_code} — {r.text[:200]}")
    except Exception as e:
        print(f"  {e}")

    # ── 7. Results ────────────────────────────────────────────────────────────
    print(f"\n{'='*78}")
    print("  RESULTS")
    print(f"{'='*78}")
    print(f"  {'Endpoint':<52}  {'n':>5}  {'p50':>6}  {'p95':>6}  {'max':>6}  {'err':>4}  result")
    print(f"  {'-'*52}  {'-'*5}  {'-'*6}  {'-'*6}  {'-'*6}  {'-'*4}  {'─'*20}")

    all_passed = True
    for label, _, _, _ in endpoint_defs:
        st = stats_map[label]
        print(st.summary())
        if not st.passed():
            all_passed = False

    total_reqs = sum(len(s.times()) for s in stats_map.values())
    total_errs = sum(len(s.errors()) for s in stats_map.values())

    print(f"\n  Total requests: {total_reqs}  |  Real errors: {total_errs}")
    print(f"\n{'='*78}")
    if all_passed:
        print("  OVERALL: PASS — all endpoints stayed within threshold during scrape")
        print("  Fast queue is isolated from scraper queue ✓")
    else:
        print("  OVERALL: FAIL — one or more endpoints breached their threshold")
        print("  Check: is the fast worker thread running? (look for 'fast-worker' in logs)")
    print(f"{'='*78}\n")

    sys.exit(0 if all_passed else 1)


if __name__ == '__main__':
    main()

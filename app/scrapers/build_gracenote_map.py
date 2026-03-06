#!/usr/bin/env python3
"""
build_gracenote_map.py
----------------------
One-time (or periodic) script to build a station_id → gracenote_id mapping
by scanning all channels via the Roku content proxy.

The RokuScraper uses this map to emit tvc-guide-stationid tags in the M3U,
telling Channels DVR which channels have full Gracenote EPG available.

Output: gracenote_map.json  {"station_id": "gracenote_id", ...}

Usage:
    python3 build_gracenote_map.py
    python3 build_gracenote_map.py --input roku_channels.csv --output gracenote_map.json
"""

import requests
import json
import csv
import re
import time
import argparse
import sys
from pathlib import Path
from urllib.parse import quote

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

BASE        = "https://therokuchannel.roku.com"
LIVE_TV     = f"{BASE}/live-tv"
CSRF_URL    = f"{BASE}/api/v1/csrf"
PROXY_BASE  = f"{BASE}/api/v2/homescreen/content/"
CONTENT_TPL = "https://content.sr.roku.com/content/v1/roku-trc/{sid}"


def get_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent":      USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
    })
    s.get(LIVE_TV, timeout=15)
    for attempt in range(5):
        r = s.get(CSRF_URL, timeout=10)
        if r.status_code == 200:
            csrf = r.json()["csrf"]
            return s, csrf
        time.sleep(2 ** attempt)
    return None, None


def get_gracenote_id(s, csrf, station_id):
    content_url = f"{CONTENT_TPL.format(sid=station_id)}?featureInclude=linearSchedule"
    proxy_url   = PROXY_BASE + quote(content_url, safe="")
    headers = {
        "csrf-token": csrf,
        "origin":     BASE,
        "referer":    LIVE_TV,
    }
    try:
        r = s.get(proxy_url, headers=headers, timeout=10)
        if r.status_code != 200:
            return None
        ids = re.findall(r'"stationId"\s*:\s*"(\d+)"', r.text)
        return ids[0] if ids else None
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default="roku_channels.csv")
    parser.add_argument("--output", default="gracenote_map.json")
    parser.add_argument("--delay",  type=float, default=0.3)
    args = parser.parse_args()

    if not Path(args.input).exists():
        print(f"ERROR: {args.input} not found. Run fetch_streams.py first.")
        sys.exit(1)

    channels = []
    with open(args.input) as f:
        for row in csv.DictReader(f):
            channels.append(row)

    print(f"Scanning {len(channels)} channels for Gracenote IDs...")

    s, csrf = get_session()
    if not s:
        print("ERROR: Could not get session")
        sys.exit(1)

    existing = {}
    if Path(args.output).exists():
        with open(args.output) as f:
            existing = json.load(f)
        print(f"Loaded {len(existing)} existing mappings")

    result = dict(existing)
    new_found = 0

    for i, ch in enumerate(channels):
        sid = ch["station_id"]
        if sid in result:
            continue  # already mapped

        gn_id = get_gracenote_id(s, csrf, sid)
        if gn_id:
            result[sid] = gn_id
            new_found += 1
            print(f"[{i+1}/{len(channels)}] {ch['title']:40s} GN:{gn_id}")
        else:
            result[sid] = ""  # mark as checked, no Gracenote ID

        time.sleep(args.delay)

    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    mapped = {k: v for k, v in result.items() if v}
    print(f"\nDone.")
    print(f"  Total channels:       {len(result)}")
    print(f"  With Gracenote ID:    {len(mapped)}")
    print(f"  Newly found:          {new_found}")
    print(f"  Output:               {args.output}")
    print(f"\nGracenote channels:")
    for sid, gn_id in sorted(mapped.items(), key=lambda x: x[1]):
        ch_name = next((c["title"] for c in channels if c["station_id"] == sid), sid)
        print(f"  {gn_id:10s}  {ch_name}")


if __name__ == "__main__":
    main()

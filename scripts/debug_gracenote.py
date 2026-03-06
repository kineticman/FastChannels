#!/usr/bin/env python3
"""
debug_gracenote.py — FastChannels Gracenote diagnostic

Run inside the container:
  docker exec fastchannels python /app/debug_gracenote.py

Or from the host if you have Python + sqlite3:
  python debug_gracenote.py
"""

import re
import sqlite3
import sys
import urllib.request

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH  = "/data/fastchannels.db"
BASE_URL = "http://localhost:5523"

# Must match m3u.py exactly
GRACENOTE_RE = re.compile(r'^(\d+|(EP|SH|MV|SP|TR)\d+)$')

SLUG_SEP = "|"

W  = "\033[93m"   # yellow  WARNING
E  = "\033[91m"   # red     ERROR
OK = "\033[92m"   # green   OK
B  = "\033[1m"    # bold
R  = "\033[0m"    # reset

def ok(msg):  print(f"  {OK}✔{R}  {msg}")
def warn(msg): print(f"  {W}⚠{R}  {msg}")
def err(msg):  print(f"  {E}✗{R}  {msg}")
def hdr(msg):  print(f"\n{B}{'─'*60}\n  {msg}\n{'─'*60}{R}")

# ── 1. DB inspection ──────────────────────────────────────────────────────────
hdr("1. DATABASE — channels table")

try:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
except Exception as e:
    err(f"Cannot open DB at {DB_PATH}: {e}")
    sys.exit(1)

# Column existence
cur.execute("PRAGMA table_info(channels)")
cols = {row[1] for row in cur.fetchall()}
if "gracenote_id" in cols:
    ok("gracenote_id column exists")
else:
    err("gracenote_id column MISSING — run migrate.py first")

# Total channels
cur.execute("SELECT COUNT(*) FROM channels WHERE is_active=1")
total = cur.fetchone()[0]
print(f"\n  Total active channels: {total}")

# Channels with gracenote_id column set
cur.execute("SELECT COUNT(*) FROM channels WHERE is_active=1 AND gracenote_id IS NOT NULL AND gracenote_id != ''")
gn_col = cur.fetchone()[0]
print(f"  With gracenote_id column set: {gn_col}")

# Channels with gracenote encoded in slug
cur.execute("SELECT COUNT(*) FROM channels WHERE is_active=1 AND slug LIKE '%|%'")
slug_pipe = cur.fetchone()[0]
print(f"  With '|' in slug: {slug_pipe}")

# Validate format of gracenote_id column values
hdr("2. DATABASE — gracenote_id format validation")
cur.execute("""
    SELECT source_channel_id, name, gracenote_id
    FROM channels
    WHERE is_active=1 AND gracenote_id IS NOT NULL AND gracenote_id != ''
    ORDER BY name
""")
rows = cur.fetchall()

bad_format = []
good_format = []
for sid, name, gid in rows:
    if GRACENOTE_RE.match(gid.strip()):
        good_format.append((sid, name, gid))
    else:
        bad_format.append((sid, name, gid))

ok(f"{len(good_format)} channels have valid gracenote_id format")
if bad_format:
    err(f"{len(bad_format)} channels have INVALID gracenote_id values:")
    for sid, name, gid in bad_format[:20]:
        print(f"       {name[:40]:<40} gracenote_id={gid!r}")

# ── 2. Slug fallback check ────────────────────────────────────────────────────
hdr("3. DATABASE — slug fallback ('{play_id}|{gracenote_id}')")
cur.execute("""
    SELECT source_channel_id, name, slug, gracenote_id
    FROM channels
    WHERE is_active=1 AND slug LIKE '%|%'
    ORDER BY name
""")
slug_rows = cur.fetchall()

slug_valid   = []
slug_empty   = []
slug_invalid = []
slug_redundant = []

for sid, name, slug, gid in slug_rows:
    candidate = slug.split(SLUG_SEP, 1)[1].strip() if SLUG_SEP in slug else ""
    if not candidate:
        slug_empty.append((name, slug, gid))
    elif GRACENOTE_RE.match(candidate):
        if gid and gid.strip():
            slug_redundant.append((name, slug, gid))  # both set
        else:
            slug_valid.append((name, slug, candidate))
    else:
        slug_invalid.append((name, slug, candidate))

ok(f"{len(slug_valid)} channels rely on slug fallback (no DB column set yet)")
if slug_redundant:
    ok(f"{len(slug_redundant)} channels have gracenote_id in BOTH column and slug (fine)")
print(f"  {len(slug_empty)} channels have slug with '|' but empty gracenote portion (excluded from Gracenote M3U — correct)")
if slug_invalid:
    warn(f"{len(slug_invalid)} channels have non-empty slug gracenote portion that fails format check:")
    for name, slug, candidate in slug_invalid[:10]:
        print(f"       {name[:40]:<40} slug={slug!r}  candidate={candidate!r}")

# Show sample of slug-fallback channels
if slug_valid:
    print(f"\n  Sample slug-fallback channels (first 5):")
    for name, slug, gid in slug_valid[:5]:
        print(f"    {name[:45]:<45} → {gid}")

# ── 3. Effective Gracenote channel count ──────────────────────────────────────
hdr("4. EFFECTIVE — channels that will appear in /m3u/gracenote")

# Replicate _parse_gracenote_id logic exactly
cur.execute("SELECT source_channel_id, name, slug, gracenote_id FROM channels WHERE is_active=1 AND is_enabled=1")
all_channels = cur.fetchall()

gracenote_channels = []
xmltv_channels     = []

for sid, name, slug, gid in all_channels:
    # Priority 1: dedicated column
    resolved = None
    if gid and GRACENOTE_RE.match(gid.strip()):
        resolved = gid.strip()
    # Priority 2: slug fallback
    elif slug and SLUG_SEP in slug:
        candidate = slug.split(SLUG_SEP, 1)[1].strip()
        if candidate and GRACENOTE_RE.match(candidate):
            resolved = candidate

    if resolved:
        gracenote_channels.append((name, resolved))
    else:
        xmltv_channels.append(name)

ok(f"{len(gracenote_channels)} channels → /m3u/gracenote  (tvc-guide-stationid)")
ok(f"{len(xmltv_channels)} channels → /m3u  (tvg-id / XMLTV)")

if gracenote_channels:
    print(f"\n  Sample Gracenote channels (first 10):")
    for name, gid in gracenote_channels[:10]:
        print(f"    {name[:45]:<45} stationid={gid}")

con.close()

# ── 4. Live endpoint checks ───────────────────────────────────────────────────
hdr("5. LIVE ENDPOINTS")

def fetch(path):
    try:
        req = urllib.request.Request(f"{BASE_URL}{path}",
                                     headers={"User-Agent": "debug_gracenote/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        return None, str(e)

# /m3u/gracenote
print(f"\n  Fetching {BASE_URL}/m3u/gracenote …")
body = fetch("/m3u/gracenote")
if body is None:
    err("/m3u/gracenote fetch failed")
else:
    lines = body.splitlines()
    extinf_lines = [l for l in lines if l.startswith("#EXTINF")]
    tvc_lines    = [l for l in lines if "tvc-guide-stationid" in l]
    tvgid_lines  = [l for l in lines if "tvg-id" in l]
    ok(f"/m3u/gracenote returned {len(extinf_lines)} channels")
    ok(f"  {len(tvc_lines)} lines contain tvc-guide-stationid")
    if tvgid_lines:
        err(f"  {len(tvgid_lines)} lines contain tvg-id (should be 0 in Gracenote feed!)")
    if len(extinf_lines) != len(tvc_lines):
        warn(f"  Mismatch: {len(extinf_lines)} channels but only {len(tvc_lines)} have tvc-guide-stationid")

    # Validate stationid values in output
    bad_ids = []
    for line in tvc_lines:
        m = re.search(r'tvc-guide-stationid="([^"]*)"', line)
        if m:
            val = m.group(1)
            if not GRACENOTE_RE.match(val):
                bad_ids.append(val)
    if bad_ids:
        err(f"  {len(bad_ids)} tvc-guide-stationid values fail format check: {bad_ids[:5]}")
    else:
        ok(f"  All tvc-guide-stationid values pass format check")

    # Sample output
    if extinf_lines:
        print(f"\n  Sample output (first 3 channels):")
        stream_lines = [l for l in lines if l.startswith("http")]
        for i in range(min(3, len(extinf_lines))):
            print(f"    {extinf_lines[i][:100]}")
            if i < len(stream_lines):
                print(f"    {stream_lines[i][:80]}")

# /m3u — check no tvc-guide-stationid leaks in
print(f"\n  Fetching {BASE_URL}/m3u …")
body2 = fetch("/m3u")
if body2 is None:
    err("/m3u fetch failed")
else:
    lines2    = body2.splitlines()
    extinf2   = [l for l in lines2 if l.startswith("#EXTINF")]
    tvc2      = [l for l in lines2 if "tvc-guide-stationid" in l]
    ok(f"/m3u returned {len(extinf2)} channels")
    if tvc2:
        err(f"  {len(tvc2)} lines contain tvc-guide-stationid (should be 0 — Gracenote channels leaking into XMLTV feed!)")
        for l in tvc2[:3]:
            print(f"    {l[:100]}")
    else:
        ok(f"  No tvc-guide-stationid found in /m3u (correct)")

# ── 5. Summary ────────────────────────────────────────────────────────────────
hdr("6. SUMMARY")
total_gn = len(gracenote_channels)
total_xmltv = len(xmltv_channels)
print(f"  Gracenote feed (/m3u/gracenote): {total_gn} channels")
print(f"  XMLTV feed     (/m3u):           {total_xmltv} channels")
if total_gn == 0:
    err("No Gracenote channels found — re-scrape Roku or check slug format")
elif gn_col == 0 and len(slug_valid) > 0:
    warn("Gracenote IDs are coming from slug fallback only — run a Roku re-scrape to populate the gracenote_id column")
else:
    ok("Everything looks good")
print()

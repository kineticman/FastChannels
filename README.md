# FastChannels

FAST channel aggregator — scrapes Pluto TV, Tubi, Roku, Sling Freestream, Plex, DistroTV, Xumo, and more, outputs M3U playlists and XMLTV EPG guides for use in any IPTV player (Jellyfin, Plex, Channels DVR, TiviMate, etc.).

## Stack

- **Python 3.12** + Flask 3
- **SQLite** — channel & EPG data (file at `/data/fastchannels.db`)
- **Redis + RQ** — background scrape jobs
- **Docker** — single container (Flask, Worker, Redis all inside)

## Quick Start

```bash
git clone https://github.com/kineticman/FastChannels.git
cd FastChannels

# Optional: set a real secret key
cp .env.example .env

# Build and start
docker compose up -d --build
```

That's it. On first boot:
1. Database is created automatically (no migrations to run)
2. All sources are seeded
3. The scheduler starts all scrapes within 60 seconds
4. Open `http://localhost:5523/admin/` — channels will populate in a few minutes

Sources that need credentials (Sling, Amazon Prime Free) will appear in the UI but won't scrape until configured under **Settings**.

## URLs

| URL | Description |
|-----|-------------|
| `http://localhost:5523/admin/` | Admin dashboard |
| `http://localhost:5523/admin/sources` | Enable/disable sources, run scrapes |
| `http://localhost:5523/admin/channels` | Browse channels, toggle individual 

## Filtered M3U Output

```
/m3u?source=pluto
/m3u?source=pluto&source=distro
/m3u?source=pluto&category=Sports
/m3u?search=news
```

## Named Feeds

Feeds are named, persistent filtered sub-feeds that expose their own `/m3u` and `/epg.xml` URLs. Create and manage them in the admin UI or via the API.

```
/feeds/sports/m3u
/feeds/sports/epg.xml
/feeds/sports/m3u/gracenote   # Channels DVR gracenote variant
```

Filter keys: `sources`, `categories`, `languages`, `max_channels`.

## Configuration

Source credentials (Pluto TV login, Amazon Prime cookies, etc.) are set through the **Settings** page in the admin UI — not environment variables.

## Architecture

### Proxy-based stream resolution

M3U entries do **not** contain direct CDN URLs. Instead they point to a proxy endpoint:

```
http://host:5523/play/{source}/{channel_id}.m3u8
```

At playback time the proxy calls `scraper.resolve(raw_url)` which:
- Substitutes URL macros (cache busters, device IDs, etc.) with fresh values
- Resolves HLS master playlists to the best-bandwidth variant
- Handles JWT auth (Pluto TV stitcher tokens, Sling bearer tokens, etc.)
- Issues a `302` redirect to the final CDN URL

This means streams never go stale — every play request gets a fresh URL.

### Stream Audit

Sources that opt in (`stream_audit_enabled = True`) support a Stream Audit job that health-checks every channel's stream and marks dead channels inactive. Triggered from the admin UI per source. Currently enabled for: Pluto TV, Tubi, Roku, Sling Freestream, DistroTV.

### EPG-only sources

A source can be flagged **EPG Only** in the admin UI. EPG-only sources are excluded from M3U output but their program data is used to enrich EPG for title-matched channels from other sources. Amazon Prime Free is the primary use case — it has no playable streams but provides accurate guide data.

### Channel enable/disable

Two separate flags on each channel:

- **`is_active`** — set by the scraper. Means the channel still exists upstream. Re-scrapes update this automatically.
- **`is_enabled`** — set by you via the admin UI toggle. Means include this channel in M3U/EPG output. Survives re-scrapes.

Disabling a **source** deletes all its channels from the DB. Re-enabling and running a scrape restores them.

## Current Sources

| Source | Auth | Notes |
|--------|------|-------|
| Pluto TV | Optional login | Session pool (10 slots), per-country feeds, JWT stitcher auth |
| DistroTV | None | Android TV UA required, URL macro substitution |
| Tubi TV | Optional email/password | Bearer token auth |
| The Roku Channel | None | Session cookie auth, HLS variant selection |
| Sling Freestream | Optional OAuth creds | Falls back to browser bootstrap to capture Bearer JWT |  **ALL STREAMS DRM AT THIS TIME**
| Plex | None | Session cookie auth |
| Xumo Play | None | Public API |
| Amazon Prime Free | Optional cookie header | EPG-only by default; pagination requires auth | **ALL STREAMS DRM AT THIS TIME**
| FreeLiveSports | None | Public API |

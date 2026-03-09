# FastChannels

FAST channel aggregator — scrapes Pluto TV, Tubi, Roku, Sling Freestream, Plex, DistroTV, Xumo, and more, outputs M3U playlists and XMLTV EPG guides for use in any IPTV player (Jellyfin, Plex, Channels DVR, TiviMate, etc.).

## Stack

- **Python 3.12** + Flask 3
- **SQLite** — channel & EPG data (file at `/data/fastchannels.db`)
- **Redis + RQ** — background scrape jobs
- **Docker** — single container (Flask, Worker, Redis all inside)

## Quick Start

```bash
cd /home/brad/Projects/FastChannels

# Build and start
docker compose up -d --build

# Trigger first scrape (sources are seeded automatically on startup)
curl -X POST http://localhost:5523/api/sources/1/run
curl -X POST http://localhost:5523/api/sources/2/run
```

## URLs

| URL | Description |
|-----|-------------|
| `http://localhost:5523/admin/` | Admin dashboard |
| `http://localhost:5523/admin/sources` | Enable/disable sources, run scrapes |
| `http://localhost:5523/admin/channels` | Browse channels, toggle individual channels on/off |
| `http://localhost:5523/admin/settings` | Credentials and options per source |
| `http://localhost:5523/admin/feeds` | Named filtered sub-feeds |
| `http://localhost:5523/admin/logs` | Live scrape log viewer |
| `http://localhost:5523/m3u` | Full M3U playlist |
| `http://localhost:5523/epg.xml` | Full XMLTV EPG |
| `http://localhost:5523/feeds/<slug>/m3u` | Feed-specific M3U |
| `http://localhost:5523/feeds/<slug>/epg.xml` | Feed-specific EPG |
| `http://localhost:5523/api/sources` | Sources API |
| `http://localhost:5523/api/channels` | Channels API |
| `http://localhost:5523/api/feeds` | Feeds API |

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

### Source config

Each scraper declares a `config_schema` (list of `ConfigField`) describing what credentials or options it needs. The Settings page auto-renders the right form fields for each source — no template changes needed when adding new scrapers. Secrets are masked (`••••••••`) in the UI and never returned by the API.

## Adding a New Scraper

1. Create `app/scrapers/yourservice.py`
2. Subclass `BaseScraper`, set `source_name` and `display_name`
3. Implement `fetch_channels()` → returns `list[ChannelData]`
4. Optionally implement `fetch_epg()` → returns `list[ProgramData]`
5. Optionally implement `resolve(raw_url)` → returns playable URL at request time
6. Optionally declare `config_schema` for any credentials needed
7. Optionally set `stream_audit_enabled = True` to enable the Stream Audit job
8. Restart — auto-discovered, seeded as a Source, and appears in Settings

```python
from .base import BaseScraper, ChannelData, ConfigField

class YourServiceScraper(BaseScraper):
    source_name  = 'yourservice'
    display_name = 'Your Service'
    stream_audit_enabled = True   # optional

    config_schema = [
        ConfigField('api_key', 'API Key', secret=True,
                    help_text='Found in your account settings.'),
    ]

    def fetch_channels(self) -> list[ChannelData]:
        r = self.get('https://api.yourservice.com/channels',
                     headers={'X-API-Key': self.config.get('api_key', '')})
        return [
            ChannelData(
                source_channel_id=ch['id'],
                name=ch['title'],
                stream_url=ch['stream'],   # stored raw; resolve() called at playback time
                logo_url=ch['logo'],
                category=ch['genre'],
            )
            for ch in r.json()
        ]

    def resolve(self, raw_url: str) -> str:
        # Exchange raw_url for a fresh playable URL here
        return raw_url
```

## Project Structure

```
FastChannels/
├── docker-compose.yml
├── Dockerfile
├── entrypoint.sh
├── requirements.txt
├── README.md
└── app/
    ├── __init__.py
    ├── config.py
    ├── extensions.py
    ├── models.py
    ├── worker.py
    ├── play.py              # /play/<source>/<id>.m3u8 proxy
    ├── logfile.py           # structured log capture for admin log viewer
    ├── scrapers/
    │   ├── base.py          # BaseScraper, ChannelData, ProgramData, ConfigField
    │   ├── registry.py      # auto-discovery
    │   ├── pluto.py         # Pluto TV (session pool, JWT auth, per-country feeds)
    │   ├── distro.py        # DistroTV (macro substitution, HLS variant selection)
    │   ├── tubi.py          # Tubi TV (optional email/password auth)
    │   ├── roku.py          # The Roku Channel (session cookie auth)
    │   ├── sling.py         # Sling Freestream (OAuth or browser bootstrap for JWT)
    │   ├── plex.py          # Plex (session cookie auth)
    │   ├── xumo.py          # Xumo Play (public API)
    │   ├── amazon_prime_free.py  # Amazon Prime Free (EPG-only; optional cookie auth)
    │   └── freelivesports.py     # FreeLiveSports (public API)
    ├── generators/
    │   ├── m3u.py           # M3U playlist generator
    │   └── xmltv.py         # XMLTV EPG generator (with EPG-only source enrichment)
    └── routes/
        ├── admin.py         # Admin UI routes
        ├── api.py           # REST API
        ├── feeds_api.py     # /api/feeds CRUD
        ├── output.py        # /m3u, /epg.xml, /feeds/<slug>/… endpoints
        └── tasks.py         # RQ job dispatch (scrape, stream audit)
    └── templates/admin/
        ├── dashboard.html
        ├── sources.html
        ├── channels.html
        ├── settings.html
        ├── feeds.html
        └── logs.html
```

## Current Sources

| Source | Auth | Notes |
|--------|------|-------|
| Pluto TV | Optional login | Session pool (10 slots), per-country feeds, JWT stitcher auth |
| DistroTV | None | Android TV UA required, URL macro substitution |
| Tubi TV | Optional email/password | Bearer token auth |
| The Roku Channel | None | Session cookie auth, HLS variant selection |
| Sling Freestream | Optional OAuth creds | Falls back to browser bootstrap to capture Bearer JWT |
| Plex | None | Session cookie auth |
| Xumo Play | None | Public API |
| Amazon Prime Free | Optional cookie header | EPG-only by default; pagination requires auth |
| FreeLiveSports | None | Public API |

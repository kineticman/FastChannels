# FastChannels

FAST channel aggregator — scrapes Pluto TV, DistroTV and more, outputs M3U playlists and XMLTV EPG guides for use in any IPTV player (Jellyfin, Plex, Channels DVR, TiviMate, etc.).

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
| `http://localhost:5523/m3u` | Full M3U playlist |
| `http://localhost:5523/epg.xml` | Full XMLTV EPG |
| `http://localhost:5523/api/sources` | Sources API |
| `http://localhost:5523/api/channels` | Channels API |

## Filtered M3U Output

```
/m3u?source=pluto
/m3u?source=pluto&source=distro
/m3u?source=pluto&category=Sports
/m3u?search=news
```

## Architecture

### Proxy-based stream resolution

M3U entries do **not** contain direct CDN URLs. Instead they point to a proxy endpoint:

```
http://host:5523/play/{source}/{channel_id}.m3u8
```

At playback time the proxy calls `scraper.resolve(raw_url)` which:
- Substitutes URL macros (cache busters, device IDs, etc.) with fresh values
- Resolves HLS master playlists to the best-bandwidth variant
- Handles JWT auth (Pluto TV stitcher tokens)
- Issues a `302` redirect to the final CDN URL

This means streams never go stale — every play request gets a fresh URL.

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
7. Restart — auto-discovered, seeded as a Source, and appears in Settings

```python
from .base import BaseScraper, ChannelData, ConfigField

class YourServiceScraper(BaseScraper):
    source_name  = 'yourservice'
    display_name = 'Your Service'

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
    ├── scrapers/
    │   ├── base.py          # BaseScraper, ChannelData, ProgramData, ConfigField
    │   ├── registry.py      # auto-discovery
    │   ├── pluto.py         # Pluto TV (session pool, JWT auth, per-country feeds)
    │   └── distro.py        # DistroTV (macro substitution, HLS variant selection)
    ├── generators/
    │   ├── m3u.py           # M3U playlist generator
    │   └── xmltv.py         # XMLTV EPG generator
    └── routes/
        ├── admin.py         # Admin UI routes
        ├── api.py           # REST API
        ├── output.py        # /m3u and /epg.xml endpoints
        ├── play.py          # /play/<source>/<id>.m3u8 proxy endpoint
        └── tasks.py         # RQ job dispatch
    └── templates/admin/
        ├── dashboard.html
        ├── sources.html
        ├── channels.html
        └── settings.html
```

## Current Sources

| Source | Channels | Auth | Notes |
|--------|----------|------|-------|
| Pluto TV | ~410 | Optional login | Session pool (10 slots), per-country feeds, JWT stitcher auth |
| DistroTV | ~304 | None | Android TV UA required, URL macro substitution |

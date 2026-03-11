# FastChannels

FAST channel aggregator — scrapes Pluto TV, Tubi, Roku, Sling Freestream, Plex, DistroTV, Xumo, and more, outputs M3U playlists and XMLTV EPG guides for use in any IPTV player (Jellyfin, Plex, Channels DVR, TiviMate, etc.).

## Deploy with Portainer

In Portainer, create a new stack and paste this:

```yaml
version: '3.9'

services:
  fastchannels:
    image: ghcr.io/kineticman/fastchannels:latest
    container_name: fastchannels
    restart: unless-stopped
    ports:
      - "5523:5523"
    volumes:
      - db_data:/data

volumes:
  db_data:
```

- Deploy the stack.
- Open `http://<your-server>:5523/admin/`.
- On first boot, sources seed automatically and channels begin populating within a few minutes.
- If you want a specific published version, replace `:latest` with a tag like `:v1.0.0`.
- Keep the `/data` volume mount so the SQLite database survives container recreation.

## Deploy with Docker

The published image is currently hosted on GitHub Container Registry:

```bash
docker run -d \
  --name fastchannels \
  --restart unless-stopped \
  -p 5523:5523 \
  -v fastchannels_data:/data \
  ghcr.io/kineticman/fastchannels:latest
```

Then open `http://localhost:5523/admin/`.

If you prefer Docker Compose with the published image:

```bash
docker compose -f docker-compose.ghcr.yml up -d
```

This works without a `.env` file. Only use `.env` if you want to override the image owner or tag:

```bash
GHCR_OWNER=kineticman
FASTCHANNELS_IMAGE_TAG=latest
```

Optional: advanced users can also preseed a few app settings with environment variables. These are not required, and the normal setup path is still the **Settings** page in the admin UI.

```yaml
environment:
  MASTER_CHANNEL_NUMBER_START: "1000"
  FASTCHANNELS_SERVER_URL: "http://192.168.1.50:5523"
  CHANNELS_DVR_SERVER_URL: "http://192.168.1.60:8089"
```

If you access the admin UI via `localhost` but want generated M3U / EPG / feed URLs to use a LAN IP or hostname instead, set:

```bash
PUBLIC_BASE_URL=http://192.168.1.50:5523
```

There is not currently a separate Docker Hub image documented in this repo.

## URLs

| URL | Description |
|-----|-------------|
| `http://localhost:5523/admin/` | Admin dashboard |
| `http://localhost:5523/admin/sources` | Enable/disable sources, run scrapes |
| `http://localhost:5523/admin/channels` | Browse channels and enable/disable them |
| `http://localhost:5523/admin/settings` | Enter source credentials and options |
| `http://localhost:5523/m3u` | Full M3U playlist |
| `http://localhost:5523/epg.xml` | Full XMLTV EPG |

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

Source credentials and source-specific options are set through the **Settings** page in the admin UI.

Advanced users may optionally set a few defaults with environment variables:

- `MASTER_CHANNEL_NUMBER_START` — default for the master tvg-chno start used by sources without their own start value
- `FASTCHANNELS_SERVER_URL` — default for the FastChannels server URL used in generated M3U/feed links
- `CHANNELS_DVR_SERVER_URL` — default for the Channels DVR server URL used by the DVR push integration

These environment variables are optional.

- You do not need them for a normal install.
- Values saved later in **admin/settings** override the environment variable.
- If a DB value is cleared in the UI, FastChannels falls back to the environment variable again.

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
| Sling Freestream | Optional OAuth creds | Falls back to browser bootstrap to capture Bearer JWT. Streams are DRM-only right now, but the scraper remains active for potential EPG data. |
| Plex | None | Session cookie auth |
| Xumo Play | None | Public API |
| Amazon Prime Free | Optional cookie header | EPG-only by default; pagination requires auth. Streams are DRM-only right now, but the scraper remains active for potential EPG data. |
| FreeLiveSports | None | Public API |

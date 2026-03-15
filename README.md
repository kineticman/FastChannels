# FastChannels

FAST channel aggregator — scrapes Pluto TV, Tubi, Roku, Samsung TV Plus, Sling Freestream, Plex, DistroTV, Xumo, and more, then outputs M3U playlists and XMLTV EPG guides for use in any IPTV player (Jellyfin, Plex, Channels DVR, TiviMate, etc.).

## Deploy with Portainer

In Portainer, create a new stack and paste this:

```yaml
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

## Admin UI

| URL | Description |
|-----|-------------|
| `/admin/` | Dashboard — source status, channel counts, feed links |
| `/admin/sources` | Enable/disable sources, run scrapes, configure credentials |
| `/admin/channels` | Browse, enable/disable, inspect, and resolve duplicate channels |
| `/admin/feeds` | Create and manage named output feeds |
| `/admin/settings` | Server URLs and system stats |
| `/admin/logs` | Live log tail |
| `/admin/help` | In-app help and source gotchas |

## Feeds

Feeds are the primary way to get output out of FastChannels. Each feed is a named, filtered slice of your channels with its own stable M3U and EPG URLs.

A built-in **Default** feed is created automatically and includes all enabled channels. Create additional feeds to build filtered outputs for specific players or purposes — by source, category, language, or a manually picked channel list.

```
/feeds/default/m3u
/feeds/default/epg.xml
/feeds/sports/m3u
/feeds/sports/epg.xml
/feeds/sports/m3u/gracenote    # Channels DVR gracenote variant
```

Feed outputs are cached and served from disk — fast for players polling on a schedule.

- **Channel Number Start**: set per feed to number all channels sequentially from a given number.
- **Add to Channels DVR**: registers the feed as a custom M3U source in your DVR with one click. Configure the DVR server URL in **Settings** first.
- **Max Channels**: Channels DVR works best with 750 or fewer channels per source.

## Configuration

Source credentials and options are configured on the **Sources** page — click into any source card to expand its settings. Changes take effect on the next scrape.

A few global defaults can optionally be set with environment variables (not required for a normal install):

```yaml
environment:
  FASTCHANNELS_SERVER_URL: "http://192.168.1.50:5523"   # LAN address other devices use to reach FastChannels
  CHANNELS_DVR_SERVER_URL: "http://192.168.1.60:8089"   # Channels DVR server
  MASTER_CHANNEL_NUMBER_START: "1000"                   # Default tvg-chno start for the master feed
```

Values saved in **Settings** override environment variables. If a DB value is cleared, FastChannels falls back to the environment variable.

## Architecture

### Proxy-based stream resolution

M3U entries point to a proxy endpoint rather than direct CDN URLs:

```
http://host:5523/play/{source}/{channel_id}.m3u8
```

At playback time the proxy resolves the stream by:
- Substituting URL macros (cache busters, device IDs, etc.) with fresh values
- Resolving HLS master playlists to the best-bandwidth variant
- Handling JWT auth (Pluto TV stitcher tokens, Sling bearer tokens, etc.)
- Issuing a `302` redirect to the final CDN URL

Streams never go stale — every play request gets a fresh URL.

### Output caching

M3U and EPG XML outputs are cached to disk and served as fast file reads. The cache is invalidated automatically after each scrape. Cold builds of the full EPG can take a few seconds; subsequent requests are near-instant.

### Stream Audit

Sources that support it have a **Stream Audit** button that health-checks every channel's stream URL and marks dead or DRM-protected channels inactive. Currently supported: Pluto TV, Tubi, Roku, Samsung TV Plus, Sling Freestream, DistroTV.

### Channel Inspect

The **Inspect** button on the Channels page tests a single channel's full resolve/playback path. Useful for diagnosing dead manifests, VOD-only streams, DRM-protected streams, and resolver failures. Also shows stream variant stats (resolution, bitrate, codecs).

### Duplicate resolution

The **Resolve Duplicates** helper on the Channels page works on enabled channels with matching names across sources. It:

- prefers healthy channels over channels flagged `DRM`, `Dead`, or inactive
- uses source priority as a tie-breaker between otherwise healthy matches
- disables the whole group if every duplicate is unhealthy

### EPG-only sources

A source can be flagged **EPG Only** on the Sources page. EPG-only sources are excluded from M3U output but their program data enriches EPG for title-matched channels from other sources. Amazon Prime Free is the primary use case.

### Channel flags

- **`is_active`** — set by the scraper; means the channel still exists upstream. Updated automatically on re-scrape.
- **`is_enabled`** — set by you; means include this channel in M3U/EPG output. Survives re-scrapes.

Disabling a source deletes all its channels from the DB. Re-enabling and running a scrape restores them.

## Source Notes

| Source | Auth | Notes |
|--------|------|-------|
| Pluto TV | Optional login | Session pool size configurable (default 10); per-country feeds; JWT stitcher auth |
| DistroTV | None | Android TV UA required, URL macro substitution |
| Tubi TV | Optional email/password | Bearer token auth |
| The Roku Channel | None | Session cookie auth, HLS variant selection |
| Sling Freestream | Optional OAuth creds | Streams are DRM-only for generic IPTV clients; scraper provides EPG data |
| Plex | None | Session cookie auth |
| Xumo Play | None | Public API |
| Amazon Prime Free | Optional cookie header | EPG-only by default; streams are DRM-only |
| Samsung TV Plus | None | Channel data and EPG via [Matt Huisman's public mirror](https://github.com/matthuisman/samsung-tvplus-for-channels). Region configurable (default: `us`). |
| FreeLiveSports | None | Public API |

- **Roku**: some channels expose sparse future guide data; short EPG windows are expected on those channels.
- **Amazon Prime Free**: without a valid cookie header, channel discovery pagination is limited.
- **Samsung TV Plus**: EPG covers approximately the current day. All credit for the data to [Matt Huisman](https://github.com/matthuisman/samsung-tvplus-for-channels).

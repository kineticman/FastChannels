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
- If you want a specific published version, replace `:latest` with a tag like `:v1.7.0`.
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

## Getting Started

After deploying, follow these steps to get a clean, working lineup:

**1. Let the scrapers run.**
On first boot all enabled sources scrape automatically. Give it a few minutes — channel counts on the dashboard will climb as each source finishes.

**2. Configure Settings.**
Go to **Admin → Settings** and set two things:
- **FastChannels Server URL** — the LAN address other devices use to reach this server (e.g. `http://192.168.1.50:5523`). Stream URLs in your M3U will use this address.
- **Channels DVR Server URL** — if you use Channels DVR, set this now so the one-click "Add to Channels DVR" button on the Feeds page works.

**3. Configure Sources.**
Go to **Admin → Sources**. Enable or disable sources to taste, and expand any source card to enter credentials (Pluto TV optional login, Tubi optional email/password, etc.). Changes take effect on the next scrape.

**4. Run Stream Audits.**
Once channels are populated, run a Stream Audit on each source (see [Stream Audit](#stream-audit) below). This identifies dead and DRM-protected channels and disables them automatically — highly recommended before building your feeds.

**5. Clean up duplicates.**
If you have multiple sources enabled, you'll likely have the same channel appearing more than once. Go to **Admin → Channels**, filter by **Duplicates**, and click **⚡ Resolve Duplicates** to sort them out in bulk.

**6. Create your feeds.**
Go to **Admin → Feeds** and build filtered channel lists for your players (see [Feeds](#feeds) below).

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

## Channels Page

The Channels page is where you fine-tune your lineup after scraping.

**Filtering** — seven ways to slice the list:
- Free-text search by channel name
- Filter by source, category, or language
- Show only Enabled, Disabled, or All
- Filter by stream health (DRM only, Dead only, or clean only)
- Filter by Gracenote coverage (has it / missing it)
- Duplicates only — channels whose name appears in more than one source

**Per-channel actions:**
- **Enable/Disable toggle** — removes a channel from M3U/EPG output without deleting it
- **Gracenote ID** — click any Gracenote field to edit it inline; auto-saves on tab-out. Useful if a channel is missing guide data and you know its station ID.
- **Preview (eye icon)** — shows current/next program info and an in-browser stream preview
- **Inspect (magnifying glass)** — does a live check of that one channel's stream: confirms it's Live, or tells you it's DRM, Dead, VOD, or not sending data. Useful for spot-checking without running a full audit.

**Bulk actions:** Check any rows (or use "select all") and a bulk action bar appears at the bottom — enable or disable everything selected at once.

**Duplicate resolution:** The Duplicates filter shows channels whose name appears in more than one source. Hit **⚡ Resolve Duplicates** to sort them out in bulk — it lets you drag-to-reorder sources by priority (recommending the one with the best Gracenote coverage), then disables all lower-priority duplicates at once.

## Feeds

Feeds are the primary way to get output out of FastChannels. Each feed is a named, filtered slice of your channels with its own stable M3U and EPG URLs.

A built-in **Default** feed is created automatically and includes all enabled channels. Create additional feeds to build filtered outputs for specific players or purposes — by source, category, language, or a manually picked channel list.

```
/feeds/default/m3u
/feeds/default/epg.xml
/feeds/sports/m3u
/feeds/sports/epg.xml
/feeds/sports/m3u/gracenote    # Channels DVR Gracenote variant
```

Feed outputs are cached and served from disk — fast for players polling on a schedule.

**Feed filter options:**
- **Sources** — only include channels from specific sources (e.g. Roku, Pluto, Plex)
- **Categories** — filter by genre (News, Sports, Movies, etc.)
- **Languages** — filter by language code (e.g. `en`, `es`)
- **Gracenote** — only channels that have a matched Gracenote ID; great for a clean-EPG feed
- **Manual selection** — pick specific channels by hand for full control

As you set filters, the feed modal shows a live count of matching channels.

**Example feeds to get you started:**
- *Sports Only* — filter categories: Sports
- *English Only* — filter languages: en
- *Guide-Ready* — filter Gracenote: has (all channels have matched EPG data)
- *Pluto Everything* — filter sources: Pluto TV
- *Movies* — filter categories: Movies
- *Anime* — filter categories: Anime

**Other feed options:**
- **Channel Number Start** — numbers all channels sequentially from a given value.
- **Add to Channels DVR** — registers the feed as a custom M3U source in Channels DVR with one click. Configure the DVR server URL in **Settings** first.
- **Max Channels** — Channels DVR works best with 750 or fewer channels per source. The feed modal warns you if you're over.

## Configuration

Source credentials and options are configured on the **Sources** page — click into any source card to expand its settings. Changes take effect on the next scrape.

A few global defaults can optionally be set with environment variables (not required for a normal install):

```yaml
environment:
  PUBLIC_BASE_URL: "http://192.168.1.50:5523"         # LAN address other devices use to reach FastChannels
  CHANNELS_DVR_SERVER_URL: "http://192.168.1.60:8089" # Channels DVR server
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

Sources that support it have a **📋 Stream Audit** button on the Sources page that health-checks every channel's stream URL and automatically marks dead or DRM-protected channels inactive. Currently supported: Pluto TV, Tubi TV, The Roku Channel, Samsung TV Plus, Sling Freestream, Plex, DistroTV, Xumo Play, Local Now, LG Channels, FreeLiveSports, STIRR.

Running a Stream Audit after your initial scrape is strongly recommended. It shows a live progress bar and a running count of DRM and dead channels found as it works through the list. Depending on channel count it may take several minutes.

Which sources tend to have the most DRM? Generally the ones carrying premium or cable content — Pluto TV, Sling Freestream, and Roku are the most common. Tubi, Samsung TV Plus, and Plex tend to be cleaner. Results vary by region, so running the audit on each source is the only way to know for sure.

After the audit completes, disabled channels are hidden from your feeds automatically.

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
| Tubi TV | Optional email/password | Bearer token auth |
| The Roku Channel | None | Session cookie auth, HLS variant selection; Cloudflare-sensitive — avoid hammering if you get 403s |
| Plex | None | Session cookie auth |
| Xumo Play | None | Public API |
| Samsung TV Plus | None | Channel data and EPG via [Matt Huisman's public mirror](https://github.com/matthuisman/samsung-tvplus-for-channels). Region configurable (default: `us`). |
| Sling Freestream | Optional OAuth creds | Streams are DRM-only for generic IPTV clients; keep enabled for EPG enrichment |
| DistroTV | None | Android TV UA required, URL macro substitution |
| LG Channels | None | Country configurable (default: `US`) |
| Local Now | None | Public API |
| STIRR | None | Public API |
| FreeLiveSports | None | Public API |
| Amazon Prime Free | Optional cookie header | EPG-only by default; streams are DRM-only |

- **Roku**: Cloudflare rate-limiting can cause occasional 403 errors during scraping or playback. If this happens, wait a few minutes before retrying — repeated attempts make it worse. Some channels also expose sparse future guide data; short EPG windows are expected on those channels.
- **Amazon Prime Free**: without a valid cookie header, channel discovery pagination is limited.
- **Sling Freestream**: streams are DRM-only for generic IPTV clients. Keep it enabled for EPG enrichment if you want guide data for those channels.
- **Samsung TV Plus**: EPG covers approximately the current day. All credit for the data to [Matt Huisman](https://github.com/matthuisman/samsung-tvplus-for-channels).

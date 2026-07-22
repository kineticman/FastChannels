# FastChannels

FAST channel aggregator — scrapes Pluto TV, Tubi, Roku, Samsung TV Plus, Sling Freestream, Plex, DistroTV, Xumo, LG Channels, Local Now, STIRR, FreeLiveSports, Bally Sports, Hallmark, TCL TV+, Vidaa Free TV, Vizio WatchFree+, Whale TV+, Adult Swim, Frndly TV, FreeCast, Fubo TV, DirecTV Stream, your own HDHomeRun tuner, and more, then outputs M3U playlists and XMLTV EPG guides for use in any IPTV player (Jellyfin, Plex, Channels DVR, TiviMate, etc.).

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
- If you want a specific published version, replace `:latest` with a tag like `:v4.4.6`.
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

### Cox beta image

The Cox integration is published from the `beta/cox-integration` branch as a beta image. To test it without switching to the stable `latest` image, run:

```bash
FASTCHANNELS_IMAGE_TAG=cox-beta docker compose -f docker-compose.ghcr.yml up -d
```

For Portainer, use `ghcr.io/kineticman/fastchannels:cox-beta` as the image tag. The default `latest` image remains the stable build from `main`.

## Getting Started

After deploying, follow these steps to get a clean, working lineup:

**1. Let the scrapers run.**
On first boot all enabled sources scrape automatically. Give it a few minutes — channel counts on the dashboard will climb as each source finishes.

**2. Configure Settings.**
Go to **Admin → Settings** and set two things:
- **FastChannels Server URL** — the LAN address other devices use to reach this server (e.g. `http://192.168.1.50:5523`). Stream URLs in your M3U will use this address.
- **Channels DVR Server URL** — if you use Channels DVR, set this now so the one-click "Add to Channels DVR" button on the Feeds page works.

**3. Configure Sources.**
Go to **Admin → Sources**. Enable or disable sources to taste, and expand any source card to enter credentials. Changes take effect on the next scrape.

Some sources ship **disabled by default** because they need credentials, a local device, carry mostly DRM content, or have a diminished channel lineup: Pluto TV, Sling Freestream, Local Now, Amazon Prime Free, Frndly TV, Fubo TV, FreeCast, DistroTV, DirecTV Stream, and HDHomeRun. Enable the ones you want and fill in their settings. In particular, **Pluto TV now requires a login** (a free account works), and Frndly/Fubo/FreeCast/DirecTV Stream require account credentials. See [Source Notes](#source-notes) for per-source details.

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
| `/admin/guide` | Preview the EPG grid as your players will see it |
| `/admin/settings` | Server URLs, Gracenote options, and system stats |
| `/admin/logs` | Live log tail |
| `/admin/reports/channel-changes` | Inferred New / Now Inactive / At Risk channels (BETA) |
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
- **Channel Number Start** — numbers all channels sequentially from a given value. Fresh installs start the built-in Default feed at channel 5000.
- **Add to Channels DVR** — registers the feed as a custom M3U source in Channels DVR with one click. Configure the DVR server URL in **Settings** first.
- **Max Channels** — Channels DVR works best with 750 or fewer channels per source. The feed modal warns you if you're over.

### Using with Plex (unsupported)

Jellyfin, Emby, and Channels DVR accept a feed's M3U + EPG URLs directly — paste and go. **Plex**
is the exception: its Live TV & DVR only ingests from HDHomeRun-style tuners, so it needs a bridge
(Channels DVR's Plex-tuner mode, or Threadfin) in front of FastChannels. It works but we **don't
officially support it** — if you have the choice, use one of the apps above instead. For power users
who want to make Plex work anyway, **[docs/plex.md](docs/plex.md)** covers both bridge paths.

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

Every scraped source has a **📋 Stream Audit** button on the Sources page that health-checks every channel's stream URL and automatically marks dead or DRM-protected channels inactive. (Only the Custom Channels source, which you populate by hand, has no audit.)

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

### Custom Channels

A built-in **Custom Channels** source lets you add any HLS/M3U8 stream by URL — a webcam, a personal re-stream, or anything else not covered by a scraper. Add channels from the Channels page; FastChannels auto-detects special stream types and handles polling and header quirks automatically. Custom channels are never auto-scraped and aren't part of the Stream Audit.

### HDHomeRun

If you run an HDHomeRun network tuner, enable the **HDHomeRun** source and point it at the device's LAN address (its `discover.json` BaseURL). FastChannels pulls the lineup and the device's Gracenote-sourced XMLTV guide, and proxies the tuner streams alongside your FAST channels. Note that tuner streams are MPEG-2/AC-3 — fine for Channels DVR, but browser and mobile playback need transcoding; HDHomeRun EXTEND models can transcode in hardware via the **Transcode profile** option.

### Gracenote

Gracenote station IDs link channels to Channels DVR's guide database for rich EPG matching. Each channel has one of three modes:

- **Auto** — a scraper or the community CSV assigns the ID automatically when available (e.g. Pluto's native station IDs)
- **Manual** — you set the ID yourself; a scrape never overwrites it
- **Off** — the channel is excluded from Gracenote routing entirely

Feeds expose a `/m3u/gracenote` variant that emits Gracenote IDs for Channels DVR. A curated community CSV fills in IDs for sources that don't expose native ones; configure or browse it from **Settings**.

### EPG-only sources

A source can be flagged **EPG Only** on the Sources page. EPG-only sources are excluded from M3U output but still scrape and store their guide data. Amazon Prime Free is the primary use case.

### Channel flags

- **`is_active`** — set by the scraper; means the channel still exists upstream. Updated automatically on re-scrape.
- **`is_enabled`** — set by you; means include this channel in M3U/EPG output. Survives re-scrapes.

Disabling a source deletes all its channels from the DB. Re-enabling and running a scrape restores them.

## Source Notes

"Default off" sources are seeded disabled — enable them on the Sources page.

| Source | Auth | Notes |
|--------|------|-------|
| Pluto TV | Login required | **Default off.** Free Pluto account required; per-country feeds; configurable session pool (default 10); JWT stitcher auth |
| Tubi TV | Optional email/password | Bearer token auth |
| The Roku Channel | None | Session cookie auth, HLS variant selection; Cloudflare-sensitive — avoid hammering if you get 403s |
| Plex | None | Session cookie auth |
| Xumo Play | None | Public API |
| Samsung TV Plus | None | Channel data and EPG via [Matt Huisman's public mirror](https://github.com/matthuisman/samsung-tvplus-for-channels). Region configurable (default: `us`). |
| Sling Freestream | Optional email/password | **Default off.** Freestream plus optional paid subscription channels; streams are DRM-only for generic IPTV clients |
| DistroTV | None | **Default off.** Upstream lineup has shrunk considerably. Android TV UA required, URL macro substitution |
| LG Channels | None | Country configurable (default: `US`) |
| Local Now | None | **Default off.** Public API |
| STIRR | None | Public API |
| FreeLiveSports | None | Public API |
| Vizio WatchFree+ | None | Public API; clear HLS |
| Whale TV+ | None | Public API |
| Adult Swim | None | 24/7 marathon streams |
| Amazon Prime Free | Optional cookie header | **Default off.** EPG-only by default; streams are DRM-only |
| Bally Sports Live | None | Free, unauthenticated |
| Hallmark | None | Free, unauthenticated |
| TCL TV+ | None | Country configurable (default: `US`) |
| Vidaa Free TV | None | Free, unauthenticated |
| Frndly TV | Email/password required | **Default off.** Paid subscription required |
| Fubo TV | Email/password required | **Default off.** Account required |
| FreeCast | Email/password required | **Default off.** Free account at watch.freecast.com required for playback |
| DirecTV Stream | Email/password required | **Default off.** Paid subscription required; streams are DRM-only, with browser playback via Widevine and M3U playback via PrismCast bridge |
| HDHomeRun | Device address | **Default off.** Your own LAN tuner; optional hardware transcode (EXTEND models). See [HDHomeRun](#hdhomerun) |
| Custom Channels | None | User-added HLS/M3U8 streams; never auto-scraped. See [Custom Channels](#custom-channels) |

- **Roku**: Cloudflare rate-limiting can cause occasional 403 errors during scraping or playback. If this happens, wait a few minutes before retrying — repeated attempts make it worse. Some channels also expose sparse future guide data; short EPG windows are expected on those channels.
- **Amazon Prime Free**: without a valid cookie header, channel discovery pagination is limited.
- **Sling Freestream**: streams are DRM-only for generic IPTV clients. Optional Sling credentials can add channels from a paid subscription when enabled in source settings.
- **Samsung TV Plus**: EPG covers approximately the current day. All credit for the data to [Matt Huisman](https://github.com/matthuisman/samsung-tvplus-for-channels).

## Advanced

These settings are only needed in unusual setups and should be left at their defaults for most installs.

### EPG scrape timeout (`SCRAPE_EPG_TIMEOUT`)

FastChannels limits how long the EPG fetch phase can run per source. The default is **900 seconds**, which covers all known sources including per-channel-per-day scrapers like Plex and TCL with multiple days of guide data.

If you're routing traffic through a VPN or have high-latency network conditions and a source's EPG scrape is timing out (you'll see `epg phase timed out` in the logs), you can raise this ceiling:

```yaml
# docker-compose.yml
services:
  fastchannels:
    environment:
      SCRAPE_EPG_TIMEOUT: "1800"
```

The value is in seconds and applies to all sources. Individual sources that need a higher ceiling (Roku, Vidaa) already override it internally — this env var only raises the floor for everything else.

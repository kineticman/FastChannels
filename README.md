# FastChannels

One live-TV lineup from everything you can watch: free streaming services, the cable networks your TV provider includes, the subscriptions you already pay for, and your own tuner or playlists. FastChannels gathers the channels and their guide data and serves them as M3U playlists and XMLTV guides for Channels DVR, Jellyfin, Emby, TiviMate or any other IPTV player.

It started as a FAST (free ad-supported TV) aggregator, which is where the name comes from, and grew from there.

## Sources

Sources are grouped the same way as on the **Sources** page. A handful of free sources are on out of the box; the rest are one switch away. See [Source Notes](#source-notes) for sign-in and playback details on each.

| Group | Sources |
|-------|---------|
| **Free** | The Roku Channel, Samsung TV Plus, Plex, Xumo Play, LG Channels, Vizio WatchFree+, TCL TV+, Whale TV+, STIRR, FreeLiveSports, Bally Sports Live, Hallmark, Adult Swim, Pluto TV, Sling Freestream, Local Now, Vidaa Free TV, PBS, C-SPAN, ViX, Zeam, and **Free Networks** (30+ networks that stream on their own sites: DW, NHK, Al Jazeera, Bloomberg, CBS News, NewsNation, The CW's FAST lineup, HSN, QVC and more) |
| **Free with an account** | Tubi, Amazon Prime Free Channels, FreeCast |
| **Subscription** | DirecTV Stream, Spectrum, Fubo, Philo, Frndly TV, ESPN, NFL, CBS, FOX One, Paramount+ |
| **TV provider** (TV Everywhere) | Sign in with your cable or streaming provider: A+E Networks, AMC Networks, SundanceTV, CNN, C-SPAN, DAZN (YES, MSG), Discovery, Disney (ABC, FX, Nat Geo, Disney Channel), FOX, NBCUniversal, Versant (USA, E!, Syfy, CNBC, MS NOW, Golf), Warner Bros. Discovery (TBS, TNT, truTV, TCM), Univision, Tennis Channel, NESN |
| **Local** | Your HDHomeRun tuner |
| **Custom** | Any HLS stream by URL, or your own M3U playlists with their XMLTV guides |

## What it does

- **Feeds**: named, filtered slices of your channels (by source, category, language, Gracenote coverage or hand-picked), each with stable M3U and EPG URLs and one-click **Add to Channels DVR**.
- **Fresh streams**: playlist entries point at FastChannels, which resolves a live stream URL each time a channel is tuned, so nothing goes stale.
- **DRM playback**: Widevine-protected channels play through a real DRM bridge, either browser-based [PrismCast](https://github.com/hjdhjd/prismcast) or FastChannels' own Fire TV / Android TV player app, instead of being dropped.
- **Guide data**: each source's own guide, plus Gracenote station IDs (native or from a community map) for Channels DVR's guide.
- **Event channels**: ESPN+, NFL, CBS and Paramount+ games show up as temporary channels and leave when they're over.
- **Lineup cleanup**: Stream Audit flags dead and DRM channels, and the duplicate resolver picks one copy of a channel carried by several sources.

## Deploy with Portainer

In Portainer, create a new stack and paste this:

```yaml
services:
  fastchannels:
    image: ghcr.io/kineticman/fastchannels:latest
    container_name: fastchannels
    ports:
      - 5523:5523
    volumes:
      - db_data:/data
      - adb_keys:/root/.android
    restart: unless-stopped

volumes:
  db_data:
  adb_keys:
```

- Deploy the stack.
- Open `http://<your-server>:5523/admin/`.
- On first boot, sources seed automatically and channels begin populating within a few minutes.
- If you want a specific published version, replace `:latest` with a version tag from the [Releases page](https://github.com/kineticman/FastChannels/releases).
- Keep the `/data` volume mount so the SQLite database survives container recreation.
- Keep the `/root/.android` volume mount too: it holds the ADB key the FastChannels Player bridge pairs with your Fire TV / Android TV device. Without it, every container recreate forces you to re-approve the authorization prompt on the TV. Harmless to leave in place even if you never use the bridge.

## Deploy with Docker

```bash
docker run -d \
  --name fastchannels \
  -p 5523:5523 \
  -v fastchannels_data:/data \
  -v fastchannels_adb:/root/.android \
  --restart unless-stopped \
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
Go to **Admin → Sources**. Enable or disable sources to taste, and expand any source card to enter credentials. Changes take effect on the next scrape.

Only free sources that need nothing from you start enabled: The Roku Channel, Samsung TV Plus, Plex, Xumo Play, LG Channels, Vizio WatchFree+, TCL TV+, Whale TV+, STIRR, FreeLiveSports, Bally Sports Live, Hallmark and Adult Swim. Everything else ships **disabled** because it needs an account, a TV provider, a local device or a DRM bridge. Enable the ones you want and expand the card to sign in. Many sign-ins are a code or link you approve on your phone, so no password is stored. TV-provider sources share one sign-in from **Settings → TV Everywhere**. Note that **Tubi needs a free account** now. See [Source Notes](#source-notes) for per-source details.

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
| `/admin/sources` | Enable/disable sources, run scrapes, configure credentials, add your own playlists |
| `/admin/channels` | Browse, enable/disable, inspect, and resolve duplicate channels |
| `/admin/feeds` | Create and manage named output feeds |
| `/admin/guide` | Preview the EPG grid as your players will see it |
| `/admin/settings` | Server URLs, Gracenote options, TV Everywhere sign-in, and system stats |
| `/admin/bridge` | DRM bridge setup — HDMI Capture, ah4c Capture, PrismCast, post-install healthcheck, tuner authorization checks |
| `/admin/logs` | Live log tail |
| `/admin/reports/channel-changes` | Inferred New / Now Inactive / At Risk channels (BETA) |
| `/admin/help` | In-app help and source gotchas |

## Channels Page

The Channels page is where you fine-tune your lineup after scraping.

**Filtering** — ways to slice the list:
- Free-text search by channel name
- Filter by source, category, or language
- Show only Enabled, Disabled, or All
- Filter by stream health (DRM only, Dead only, or clean only)
- Filter by Gracenote coverage (has it / missing it)
- Duplicates only — channels whose name appears in more than one source
- Events only, or everything but events (see [Event channels](#event-channels))

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
/feeds/sports/m3u/mixed        # both in one playlist (Guide Mode: mixed)
/feeds/sports/native/m3u       # every channel on the scraped guide, no Gracenote
/feeds/sports/native/epg.xml
```

Feed outputs are cached and served from disk — fast for players polling on a schedule.

**Feed filter options:**
- **Sources** — only include channels from specific sources (e.g. Roku, Pluto, Plex)
- **Categories** — filter by genre (News, Sports, Movies, etc.)
- **Languages** — filter by language code (e.g. `en`, `es`)
- **Gracenote** — only channels that have a matched Gracenote ID; great for a clean-EPG feed
- **Events** — only event channels, or none of them (see [Event channels](#event-channels))
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
- **Add to Channels DVR** — registers the feed as a custom M3U source in Channels DVR with one click. Configure the DVR server URL in **Settings** first. Re-adding keeps settings you made in the DVR, such as a stream limit.
- **Guide Mode** (Beta) — *Separate* (the default) gives a standard playlist and a Gracenote playlist, added to Channels DVR as two sources. *Mixed* puts both in one playlist, so the feed is one source. Mixed needs Channels DVR 2026.08.25 or newer; switching doesn't change channel numbers.
- **Add Native to Channels DVR** — adds the feed's native output as its own source: every channel in the feed, all using FastChannels' scraped guide instead of Gracenote.
- **Add whole feed as one source** (Beta, mixed feeds with [ah4c](#fastchannels-player)) — one Channels DVR source carrying the feed's regular channels and its ah4c-bridged DRM channels. Each channel is routed when it's tuned, so it keeps its URL if it starts or stops needing the bridge.
- **Max Channels** — Channels DVR works best with 750 or fewer channels per source. The feed modal warns you if you're over.

### Using with Plex (unsupported)

Jellyfin, Emby, and Channels DVR accept a feed's M3U + EPG URLs directly — paste and go. **Plex**
is the exception: its Live TV & DVR only ingests from HDHomeRun-style tuners, so it needs a bridge
(Channels DVR's Plex-tuner mode, or Threadfin) in front of FastChannels. It works but we **don't
officially support it** — if you have the choice, use one of the apps above instead. For power users
who want to make Plex work anyway, **[docs/plex.md](docs/plex.md)** covers both bridge paths.

### FastChannels Player

DRM channels that need real Widevine playback (Sling, PBS, Amazon Prime Free, Vidaa,
Philo, Roku, Fubo, DirecTV Stream, Spectrum, ESPN, ViX, and the NBCUniversal, Warner Bros. Discovery,
Versant, Disney, CNN, SundanceTV, Tennis Channel, DAZN and NESN TV Everywhere sources) can be routed through a real Fire TV /
Android TV device running FastChannels' own player app, captured back off its HDMI
output and re-published as a normal channel — no browser/PrismCast bridge needed for
these sources. An alternate HDMI-capture front end, [ah4c](https://github.com/sullrich/ah4c),
is also supported for installs that already have that hardware in place. Not every
DRM source can ride this bridge. Hardware-and-software
setup, not a toggle: **[docs/fc-player-setup.md](docs/fc-player-setup.md)** is the
full walkthrough.

A single capture device plays one channel at a time. To stop a second viewer or a
recording from retuning it — and to fall back to PrismCast when it's busy — set a stream
limit on the source in Channels DVR; see
[Stream limits and PrismCast fallback](docs/fc-player-setup.md#stream-limits-and-prismcast-fallback).

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

Validate a saved XMLTV artifact against the upstream XMLTV DTD with `scripts/validate_xmltv.sh /path/to/epg.xml`. The Docker image includes `xmllint` via `libxml2-utils` for this check. Note: output intentionally includes two non-DTD elements, so expect both to fail strict validation: `<series-id>`, which Channels DVR requires for stable series/recording-pass identity, and `<live/>` on event-channel programmes, which Channels DVR shows as a Live badge (widely used, though undocumented by Channels).

### Stream Audit

Every scraped source has a **📋 Stream Audit** button on the Sources page that health-checks every channel's stream URL and automatically marks dead or DRM-protected channels inactive. (Custom Channels and Custom Playlists have no audit: FastChannels never probes streams you supply yourself.)

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

### Custom Playlists

Add your own M3U playlist, with its XMLTV guide if it has one, from **Sources → Custom Sources → + Add Playlist**. Each playlist becomes its own source: its channels and guide come out of your normal feeds, with channel numbers, categories, review and enable switches working as they do for built-in sources. Channels the guide doesn't cover get placeholder guide blocks, which you can turn off per playlist.

Each group in the playlist can have a rule for channels that drop out of it: **Linear** groups disable a missing channel after a set number of missed imports, and **Event** groups treat their channels as [event channels](#event-channels), hiding one as soon as it's missing and deleting it after a set number of misses.

FastChannels never connects to a playlist's streams itself, so it adds nothing to a provider's stream limit. That also means playlists have no Stream Audit, and streams that need special request headers may not play yet. Full guide: **[docs/custom-playlists.md](docs/custom-playlists.md)**.

### Event channels

Some sources carry one-off events rather than 24/7 channels. FastChannels adds each event as a temporary channel with its own guide entry and removes it automatically when it's over. Five sources produce them today:

- **ESPN** — opt-in ESPN+, ACC Extra and SEC Plus events in the next 48 hours, filtered by sport and league on the ESPN source card. An event channel retires two hours after its scheduled end (leaving room for overtime) and is deleted a day later. Events dropped from ESPN's schedule, or excluded by your filter, leave the output right away.
- **NFL** — opt-in live games in the next 48 hours that your sign-in can play (in-market games through your TV provider or NFL+). Games carried on NFL Network stay on that channel. Same retire/delete timing as ESPN.
- **CBS** — opt-in CBS games in the next 48 hours from your local CBS station, through your TV provider. CBS Sports Network games stay on that channel. Same retire/delete timing as ESPN.
- **Paramount+** — live sports matches (UEFA, Serie A, EFL, UFC and more), each as its own channel.
- **Custom playlists** — any group set to **Event** (see [Custom Playlists](#custom-playlists)).

Event channels are marked with a calendar icon on the Channels and Guide pages. Channels, Guide and feed filters can show events only or leave them out. A channel you disable yourself stays disabled; FastChannels only brings back events it hid automatically.

### HDHomeRun

If you run an HDHomeRun network tuner, enable the **HDHomeRun** source and point it at the device's LAN address (its `discover.json` BaseURL). FastChannels pulls the lineup and the device's Gracenote-sourced XMLTV guide, and proxies the tuner streams alongside your FAST channels. Note that tuner streams are MPEG-2/AC-3 — fine for Channels DVR, but browser and mobile playback need transcoding; HDHomeRun EXTEND models can transcode in hardware via the **Transcode profile** option.

### Gracenote

Gracenote station IDs link channels to Channels DVR's guide database for rich EPG matching. Each channel has one of three modes:

- **Auto** — a scraper or the community CSV assigns the ID automatically when available (e.g. Pluto's native station IDs)
- **Manual** — you set the ID yourself; a scrape never overwrites it
- **Off** — the channel is excluded from Gracenote routing entirely

Feeds expose a `/m3u/gracenote` variant that emits Gracenote IDs for Channels DVR. A curated community CSV fills in IDs for sources that don't expose native ones; configure or browse it from **Settings**.

### Channel flags

- **`is_active`** — set by the scraper; means the channel still exists upstream. Updated automatically on re-scrape.
- **`is_enabled`** — set by you; means include this channel in M3U/EPG output. Survives re-scrapes.

Disabling a source deletes all its channels from the DB. Re-enabling and running a scrape restores them.

## Source Notes

"Default off" sources are seeded disabled. Enable them on the Sources page.

### Free

| Source | Auth | Notes |
|--------|------|-------|
| The Roku Channel | None | Session cookie auth, HLS variant selection; Cloudflare-sensitive, so avoid hammering if you get 403s |
| Samsung TV Plus | None | Channel data and EPG via [Matt Huisman's public mirror](https://github.com/matthuisman/samsung-tvplus-for-channels). Region configurable (default: `us`) |
| Plex | None | Session cookie auth |
| Xumo Play | None | Public API |
| LG Channels | None | Country configurable (default: `US`) |
| Vizio WatchFree+ | None | Public API; clear HLS |
| TCL TV+ | None | Country configurable (default: `US`) |
| Whale TV+ | None | Public API |
| STIRR | None | Public API |
| FreeLiveSports | None | Public API |
| Bally Sports Live | None | Free, unauthenticated |
| Hallmark | None | Free, unauthenticated |
| Adult Swim | None | 24/7 marathon streams |
| Pluto TV | None (login optional) | **Default off.** Works anonymously out of the box; signing in with a Pluto account is optional; per-country feeds; configurable session pool (default 10); JWT stitcher auth |
| Sling Freestream | Optional (paid) | **Default off.** Two modes: Freestream-only (free, anonymous) or a paid Sling account for premium channels; streams are DRM-only for generic IPTV clients |
| Local Now | None | **Default off.** Public API |
| Vidaa Free TV | None | **Default off.** Free, unauthenticated; most channels are clear HLS but some are Widevine DRM, played through the DRM bridge |
| PBS | None (ZIP code optional) | **Default off.** Auto-locates your local station plus a curated set of national/secondary feeds (World, Create, NHK, FNX); add more ZIP codes for additional clear feeds. Stations that require DRM are served through an opt-in bridge feed |
| C-SPAN | None | **Default off.** Free congressional/public-affairs event streams (floor sessions, hearings, Washington Journal). The 24/7 C-SPAN 1/2/3 networks are a separate [TV provider](#tv-provider-tv-everywhere) source |
| ViX | None | **Default off.** About 80 free Spanish-language channels (N+ Univision news, telenovelas, movies, comedy, sports). Nearly all are Widevine DRM, so they play through the DRM bridge |
| Zeam | None | **Default off.** Syncbak's free local-news and FAST service (formerly VUit): about 400 channels across every market. Clear HLS |
| Free Networks | None | **Default off.** 30+ networks that stream free on their own websites: news (DW, Al Jazeera, TRT, NHK, Arirang, Bloomberg, CBS News, Scripps, Newsmax, NewsNation, Sky News, KSL and more), faith (EWTN, TBN, CBN), shopping (HSN, QVC), Sinclair's diginets, The CW's FAST lineup, A+E's FAST channels, Tastemade, Outside TV. Each network is fetched separately, so one outage doesn't affect the others |

### Free with an account

| Source | Auth | Notes |
|--------|------|-------|
| Tubi TV | Email/password required | **Default off.** Tubi no longer exposes its channel list to signed-out requests; a free Tubi account is required for channel discovery |
| Amazon Prime Free | Optional cookie header | **Default off.** Streams are DRM-only and play through the DRM bridge |
| FreeCast | Email/password required | **Default off.** Free account at watch.freecast.com required for playback |

### Subscription

| Source | Auth | Notes |
|--------|------|-------|
| DirecTV Stream | Sign-in code or email/password | **Default off.** **Sign in with a code** (approve a link in your own browser, no password stored) or with email/password. Streams are DRM-only, with browser playback via Widevine and M3U playback via the DRM bridge |
| Spectrum | Browser sign-in | **Default off.** Spectrum TV subscription. Click **Sign in to Spectrum** on the source card: a real browser session opens in an admin-UI modal. Streams are DRM-only and play through the DRM bridge |
| Fubo TV | Email/password required | **Default off.** Mostly clear FAST channels; a paid account unlocks ~835 subscription channels, some of which are Widevine DRM and play through the DRM bridge |
| Philo | Email (passwordless code) | **Default off.** Sign-in sends a one-time code by email/text, no password. Streams are DRM-only and play through the DRM bridge |
| Frndly TV | Email/password required | **Default off.** Paid subscription required |
| ESPN | ESPN account and/or TV provider | **Default off.** ESPN, ESPN2, ESPNU, ESPNews, ESPN Deportes, SEC Network and ACC Network through your TV provider or an ESPN Unlimited plan; NFL Network and MLB Network need an ESPN plan. Plain ESPN+ adds events only. Optional ESPN+ / ACC Extra / SEC Plus [event channels](#event-channels). Playback uses the DRM bridge |
| NFL | None for NFL Channel; TV provider and/or NFL account for the rest | **Default off.** NFL Channel plays without signing in. A TV-provider or NFL+ sign-in (a code entered at nfl.com/activate) adds NFL Network, RedZone where your package or NFL+ Premium includes it, and optional live-game [event channels](#event-channels). Clear HLS, no DRM bridge |
| CBS | None for HQ/Golazo/Champions League; TV provider for the rest | **Default off.** CBS Sports HQ, Golazo and the Champions League channel play without signing in. A TV-provider sign-in (a code entered at cbssports.com/androidtv/activate, lasts about 90 days) adds your local CBS station, CBS Sports Network and optional CBS game [event channels](#event-channels), if your package includes them. Clear HLS, no DRM bridge |
| FOX One | TV provider (optional) | **Default off.** FOX Sports direct-subscription streaming; free content works anonymously, paid FOX Sports channels need a linked TV provider |
| Paramount+ | Paramount+ account | **Default off.** Live sports matches (UEFA, Serie A, EFL, UFC and more) as [event channels](#event-channels). Sign in with a code entered at paramountplus.com/activate/androidtv. If you get Paramount+ through your TV provider, connect the provider to your Paramount+ account first (Account → TV Provider on paramountplus.com). Clear HLS, no DRM bridge |

- **Roku**: Cloudflare rate-limiting can cause occasional 403 errors during scraping or playback. If this happens, wait a few minutes before retrying; repeated attempts make it worse. Some channels also expose sparse future guide data, so short EPG windows are expected on those channels.
- **Amazon Prime Free**: without a valid cookie header, channel discovery pagination is limited.
- **Sling Freestream**: streams are DRM-only for generic IPTV clients. Toggle on "Paid Sling account (premium channels)" to add premium channels. Off = Freestream-only (free, anonymous; no sign-in, no browser).
  - Sling gates its login form with invisible hCaptcha Enterprise, which reliably challenges automated browsers regardless of fingerprint spoofing, so signing in still needs a human to solve the captcha when one is shown. Save your email/password in the source config, then click **Sign in to Sling**: FastChannels launches a real anti-detect browser (Camoufox) against the actual sign-in page, auto-fills your saved credentials, and streams it live in an admin-UI modal. You only need to solve the captcha if one appears. Once signed in, it captures the session automatically and caches the OAuth credentials; no manual token pasting needed.
- **Samsung TV Plus**: EPG covers approximately the current day. All credit for the data to [Matt Huisman](https://github.com/matthuisman/samsung-tvplus-for-channels).
- **DirecTV Stream / Sling**: both source cards have an **Exclude FAST channels** toggle to leave out free, ad-supported channels. Saving a change to this toggle on an enabled source queues a full scrape. That scrape marks excluded channels inactive without waiting for the normal miss-threshold grace period, while retaining their saved settings.
- **DirecTV Stream**: resolves Gracenote IDs from its own API first (the `externalListingId` DirecTV attaches to each channel), with community-map fallback; an HD channel with no ID of its own falls back to its SD twin's. Filters out non-streamable satellite-only lineup entries.
- **DirecTV Stream — Use DirecTV channel numbers**: off by default. On, DirecTV channels are numbered in M3U output exactly as DirecTV numbers them (ESPN 206, CNN 202, ...) instead of getting an app-assigned number, so a DVR guide matches the DirecTV guide. Feeds that share one DirecTV number get subchannel suffixes: the main feed keeps the bare number, "... Alternate" feeds get sequential `.1`, `.2`, and feeds named with their own index ("WNBA on ION 2", "Big Ten Network Overflow 3") get that index (`305.2`, `610.3`). On satellite lineups the SD copy of an HD channel is left out so the pair does not claim the same number. Other sources are automatically numbered around the DirecTV numbers; a channel you have locked (🔒) keeps its number, and any clash shows up as a channel-number overlap warning. Changing the toggle queues a channel refresh.

### TV provider (TV Everywhere)

These sources sign in through your cable or streaming provider (MVPD) rather than a per-source login. Go to **Settings → TV Everywhere**, pick your provider, and sign in, either in a real browser session that opens in an admin-UI modal or by opening a link on your phone. **Sign in to all** repeats that for every TV-provider network in one pass, with a short pause between each to avoid tripping your provider's rate limiting.

If one source should sign in with a different TV provider than the rest, choose **A different sign-in, just for this source** on that source's card. It then signs in each of its networks by a link you open on your phone, independently of the shared login.

| Source | Channels | Notes |
|--------|----------|-------|
| A+E Networks TVE | A&E, Lifetime, History, FYI and sister networks | **Default off.** PrismCast bridge only |
| AMC Networks TVE | AMC, BBC America, IFC, WE tv | **Default off.** PrismCast bridge only |
| SundanceTV | SundanceTV (24/7) | **Default off.** Needs a Sundance account as well as your provider. DRM bridge |
| CNN TVE | CNN, CNN International, HLN | **Default off.** One sign-in covers all three. DRM bridge |
| C-SPAN TVE | C-SPAN, C-SPAN2, C-SPAN3 (24/7) | **Default off.** Clear HLS, no DRM bridge |
| DAZN | DAZN 1, Rally TV, Red Bull TV, Unbeaten, Fubo Sports; YES and MSG | **Default off.** The free channels need a free DAZN account (signed in with a phone link); YES and MSG need a provider that carries them. DRM bridge |
| Discovery TVE | Discovery, HGTV, Food Network, TLC and sister networks | **Default off.** PrismCast bridge only |
| Disney TVE | Your local ABC station, Freeform, FX, FXX, FXM, Nat Geo, Nat Geo Wild, Disney Channel, Disney Junior, Disney XD; free ABC News Live | **Default off.** Two provider sign-ins (ABC/FX/Nat Geo and the Disney channels). DRM bridge |
| FOX TVE | FOX broadcast plus FS1/FS2 | **Default off.** PrismCast bridge only |
| NBCUniversal TVE | Local NBC/Telemundo affiliate (auto-detected by IP, overridable), Bravo, NBC News NOW, NBC Sports Now, Telemundo and Universo channels | **Default off.** DRM bridge |
| Versant TVE | USA, E!, Syfy, Oxygen (East and West), CNBC, MS NOW, Golf Channel | **Default off.** One sign-in covers all 11. DRM bridge |
| Warner Bros. Discovery TVE | TBS, TNT, truTV, TCM | **Default off.** DRM bridge |
| Univision TVE | Univision, UniMás, Galavisión, TUDN; free N+ Univision 24/7, Zona TUDN, Canal ViX | **Default off.** Clear HLS, no DRM bridge |
| Tennis Channel TVE | Tennis Channel; free Tennis Channel 2 and Pickleball TV | **Default off.** Tennis Channel plays through the DRM bridge; the free channels are clear |
| NESN TVE (Beta) | NESN, NESN Plus | **Default off.** New England providers only. Needs a NESN account (signed in with an emailed code) as well as your provider. DRM bridge |

"DRM bridge" means PrismCast or [FastChannels Player](#fastchannels-player); "PrismCast bridge only" sources can't use FastChannels Player yet. The sign-in has been verified with **Cox**, **Spectrum**, **Sling TV** and **Xfinity/Comcast**. Adobe Pass lists many more providers, but not every provider carries every network, and some may fail to sign in.

### Local and Custom

| Source | Auth | Notes |
|--------|------|-------|
| HDHomeRun | Device address | **Default off.** Your own LAN tuner; optional hardware transcode (EXTEND models). See [HDHomeRun](#hdhomerun) |
| Custom Channels | None | User-added HLS/M3U8 streams; never auto-scraped. See [Custom Channels](#custom-channels) |
| Custom Playlists | None | Your own M3U + XMLTV, one source per playlist. See [Custom Playlists](#custom-playlists) |

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

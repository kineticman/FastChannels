# FastChannels Private Changelog

This file is for internal development notes only.
It is intentionally more detailed than anything posted publicly.

## 1.8.0

Release focus: memory reduction, XML/EPG caching cleanup, safer background processing, and general operational stability.

### Memory / XML / Feed Work

- Reworked XMLTV caching to use file-backed artifacts instead of building and serving the full XML document from a long-lived web worker.
- Unfiltered `/epg.xml` and `/feeds/<slug>/epg.xml` now serve cached XML artifacts from disk instead of loading large XML strings into Python memory on every request.
- Removed XML artifact rebuilds from the Gunicorn request path. Web workers no longer cold-build XML artifacts.
- XML artifact refresh now happens through the background worker flow, including startup and post-invalidation refreshes.
- Added file-level XML artifact locking/staleness handling so refreshes are atomic and stale artifacts can remain serveable while a new one is being built.
- Feed XML refresh behavior was tightened so new or edited feeds get refreshed through the worker queue instead of staying stale until the next scrape.
- Feed XML artifact files are now cleaned up on feed delete.
- Tightened XML enrichment to the active EPG window instead of loading broader EPG-only data than necessary.

### Additional Memory Fixes

- Reduced feed validation memory usage by replacing heavy ORM channel loads in overlap validation with lightweight channel/source stubs.
- Moved XML refresh work into a short-lived subprocess so the long-lived background worker does not retain large XML-generation heaps after finishing a refresh.
- Tuned Gunicorn defaults:
  - default workers lowered to `2`
  - enabled `max_requests`
  - enabled `max_requests_jitter`
- Result in local/live testing: memory dropped from the earlier problematic range down to a much healthier steady-state footprint.

### Queue / DB Concurrency Work

- Kept small row-level edits synchronous, but moved heavier DB work off the web request path.
- Added queued background handling for:
  - XML artifact refresh
  - source channel/program purge
  - bulk channel enable/disable
  - `/play` auto-disable persistence
- `/play` still returns immediately; only the eventual DB write is queued.
- Added simple deduping for XML refresh and channel auto-disable queue jobs to reduce duplicate work.
- Updated the Sources UI so disabling a source no longer makes a second direct delete call after the initial disable request.

### Admin / Observability

- Added memory stats to the System Stats card in `/admin/settings`:
  - container memory
  - process RSS/swap
  - host memory availability
- Added lightweight CPU stats:
  - load average
  - CPU core count
  - process CPU time
- Suppressed noisy access log lines for cached logo/poster requests and other low-signal polling endpoints.

### Backup / Restore

- Added backend support for settings export/import.
- Current backup payload includes:
  - app settings
  - sources
  - feeds
  - channel overrides
- Import is merge-based, not destructive replacement.
- UI was added temporarily for testing, then hidden again until more confidence is built in the workflow and semantics.
- `dev/todo.md` now tracks the remaining validation items before exposing this publicly.

### Notes / Follow-up

- `dev/todo.md` remains the place for open work and validation tasks.
- One thing still worth reviewing after the queue/subprocess changes: repeated worker startup log lines that may be related to subprocess-based XML refreshes or queue execution bootstrap paths.

## 1.9.0

Release focus: finishing the big stabilization pass, removing the remaining web-worker memory traps, improving source/channel state clarity, and cleaning up several rough edges that had become visible while testing 1.8.x in the wild.

This version ended up being less about one flashy feature and more about turning a pile of hard-won debugging into durable product behavior. A lot of this release came directly from reproducing real user reports, instrumenting the running app, and then methodically removing the request paths that were poisoning Gunicorn workers or leaving confusing state behind in the admin UI.

### Memory / Worker Stability

- Found and fixed the remaining big M3U-related memory regression:
  - `/feeds/default/m3u` and similar output paths were still inflating web workers badly even when the cache file on disk was small.
  - M3U output is now served from prebuilt disk artifacts instead of building/reading large Python strings in the request path.
- Verified with live testing that the old M3U path could take a worker from roughly `75-90 MB RSS` to `300-350 MB RSS`, and then confirmed that the new artifact-backed path keeps the same request essentially flat.
- Optimized `/admin/channels` so it no longer defeats pagination by loading too much global channel state in process:
  - removed full active-channel loads for channel-number mapping
  - limited duplicate-name work to the current page
- Optimized the Channels DVR push endpoints so they no longer materialize full channel ORM lists in Gunicorn just to answer “how many channels are there?” and “is there any Gracenote content?”
  - this was one of the biggest remaining real-world request-path memory traps
  - we caught it red-handed with live tracing: one `push-to-dvr` request could leave a worker `+215 MB`
  - after the summary-query rewrite, the same flow dropped to about `+1 MB`
- Added temporary per-request worker RSS tracing to catch the remaining culprits, used it to isolate the bad routes, then removed it once the picture stabilized.
- Final state during the last validation pass looked healthy again:
  - container memory back around the low `200 MB` range
  - Gunicorn workers around the low `80 MB RSS` range
  - no more obvious request paths that instantly poison a worker

### Artifact / Feed Output Reliability

- Fixed a race where new feed artifacts might not exist yet when a user immediately pushed that new feed to Channels DVR.
  - `push-to-dvr` now waits briefly for the specific feed artifacts to exist before handing the URLs to DVR.
- Tightened artifact refresh behavior further:
  - added a retry if an invalidation/build race causes standard M3U artifacts to come up missing after a refresh pass
  - this specifically protected `master/default` M3Us from getting stranded missing after concurrent refresh activity
- Corrected Default-feed numbering so it consistently respects the feed’s configured `chnum_start`, including in generated M3U output and the admin UI.

### Source Scrapes / Queue Behavior

- Fixed duplicate source scrape enqueueing.
  - manual run + scheduler tick could enqueue the same source twice because the scheduler only looked at `last_scraped_at` and its own bookkeeping
  - both manual and scheduled scrapes now use the same stable job id and dedupe against queued/started jobs
- Confirmed that Roku’s `403` cooldown behavior exits cleanly and does not hang the worker.
- Added better Plex phase logging around auth, RSC fetch, and each grid window fetch so future failures are much easier to attribute.
- Reduced noisy “worker starting” log lines by suppressing import-time chatter from RQ work-horse processes.

### Roku / Plex / Provider Recovery Work

- Roku:
  - improved empty-channel diagnostics
  - forced one session refresh retry on empty-channel fetches
  - added more explicit phase timeout handling
  - fixed poster XML/proxy behavior so Roku poster URLs are now actually usable and self-heal older stale `/posters/...` links
- Plex:
  - found a bad failure mode where a bad full channel refresh could collapse the active lineup and then later scrapes would stay trapped in EPG-only mode against that bad active set
  - manual source runs now force a full channel refresh
  - added a guardrail against catastrophic channel-refresh collapse
  - this allowed a bad Plex lineup state to recover back to a normal active-channel count instead of staying effectively broken
- LG / Plex / Roku “stale rows” investigation:
  - added `last_seen_at` / `missed_scrapes` so we can distinguish real current upstream lineup changes from ancient lingering rows
  - validated after a full refresh that the remaining odd rows are a small, understandable residue rather than a large hidden parser failure

### Channel State / Admin Clarity

- Added `channels.last_seen_at`
- Added `channels.missed_scrapes`
- Both are auto-migrated on SQLite startup; no manual migration step is required for normal upgrades.
- New scrape behavior:
  - seen this scrape: `last_seen_at=now`, `missed_scrapes=0`
  - not seen: increment `missed_scrapes`
  - only mark inactive after repeated misses instead of one bad scrape
- Manual enable and bulk enable now also reset the stale counters and reactivate the row cleanly.
- `/admin/channels` now:
  - shows the real `TVG-CHNO` from the Default feed instead of the old approximation
  - refreshes visible `TVG-CHNO` values after single-row toggles without reloading the whole page
  - shows a hover hint on the channel name for inactive/stale rows:
    - inactive
    - missed scrapes
    - last seen
- Reworked the channels filter area after it got too cluttered:
  - primary row now focuses on `Search`, `Source`, and main actions
  - secondary filters live behind a `More filters` drawer
  - active-filter chips now read clearly as `Active Filters`
  - stale/debug toggles are available without taking over the whole toolbar

### Guide Metadata / XMLTV

- Fixed a real Plex mapping issue where series title and episode title were effectively reversed for episodic content.
- Improved Pluto metadata handling by pulling the real original release date from the raw nested Pluto payload instead of ignoring it.
  - also filtered out bogus `1970-01-01` placeholder dates
- XMLTV now emits:
  - `original_air_date` via `<date>` where available
  - both `xmltv_ns` and `onscreen` episode numbering where appropriate
- This should make Channels/Event Inspector and similar clients behave more sensibly with season/episode and air-date data.

### Images / Posters / Logos

- Relaxed logo normalization so already-safe logos are left alone instead of being unnecessarily shrunk and recompressed.
- Reduced logo prewarm concurrency and deduped URLs before scheduling work.
- Verified with real samples that:
  - safe logos stay untouched
  - oversized assets still get normalized
- Clarified why poster cache often stays small:
  - logos are prewarmed aggressively
  - posters are mostly on-demand
- Fixed Roku poster proxy behavior so XML poster URLs are usable and stale legacy poster links can recover instead of just 404ing.

### Sources / Settings / Admin UX

- Added `Force Complete Refresh` to the Sources page.
  - clears stale scrape timestamps and queues fresh runs across enabled sources
- Added per-source scrape interval controls with server-side bounds/validation.
- Added GitHub version-link/update-check plumbing in the admin nav:
  - current version links to GitHub
  - latest-version status is cached locally and fails quietly if unreachable
- Clarified memory stats in `/admin/settings`:
  - made it clearer when the big “memory” number is mostly file cache rather than bloated Python worker heap
- Hid/removed the old `EPG Only` toggle from the Sources UI and normalized old `epg_only` rows away on startup so upgrading users don’t get stuck in a hidden state.
- Simplified the EPG-only concept itself:
  - removed the half-finished XML enrichment behavior
  - kept the source behavior simpler and more predictable

### Backup / Restore

- The backup/restore backend remains in place, but we continued to hold the line on not exposing it publicly yet.
- This was the right call; the rest of the stabilization work ended up being higher priority, and it still deserves another full validation pass before being surfaced in the UI as a user-facing promise.

### Upgrade / Migration Notes

- For normal existing SQLite installs, the queued schema-related upgrades remain startup-safe:
  - `public_base_url`
  - `last_audited_at`
  - `is_duplicate`
  - `original_air_date`
  - `last_seen_at`
  - `missed_scrapes`
- No manual user migration step should be required.
- Important limitation remains:
  - the current runtime migration helper is SQLite-only
  - if/when PostgreSQL becomes real, all of this needs proper Alembic-style migrations instead of the current bootstrap approach

### Overall Read

- 1.8.0 got the app out of the danger zone.
- 1.9.0 is the version where the remaining major request-path memory traps, stale state confusion, and scrape/queue rough edges were cleaned up enough that the app feels much more coherent again.
- The main remaining big-ticket non-urgent project after this is still a real database migration story (especially if PostgreSQL support is going to happen), not another obvious production fire.

## 2.5.0

Release focus: Amazon Prime Free FAST channel stream resolution, worker process isolation, sticky automatic channel numbering, and operational reliability improvements.

*Note: changelog entries for 2.2.0 – 2.4.0 are not yet written. See git log between tags.*

### Amazon Prime Free — Live Stream Resolution

- Implemented full live DASH stream URL resolution for Amazon's 880+ FAST channels.
- After investigation, confirmed that Amazon's `GetPlaybackResources` PRS endpoint accepts direct HTTP requests — no browser required. The `nerid` token passed by the SDK is not server-validated.
- `_resolve_channels()` fires parallel HTTP requests to PRS using a `ThreadPoolExecutor` (20 workers). All 884 channels resolve in ~23s.
- Full scrape (channel list + EPG + stream URLs) completes in ~58s. Previous Playwright-based approach timed out at 120s having resolved only 18/884 channels due to SDK-internal request serialization (~13s/channel).
- Streams are CENC-encrypted DASH (Widevine + PlayReady). `LA_URL = https://prls.atv-ps.amazon.com/cdp`. DRM-capable clients only (Kodi + inputstream.adaptive works).
- Qwilt CDN variant preferred (clean URL, no Akamai auth token obfuscation in path).
- Stream URLs cached in `source.config["stream_url_cache"]` with 1.5h TTL (Amazon's actual TTL is 2h). `scrape_interval = 100` min keeps cache warm.
- `prs_device_id` generated once and persisted to `source.config` for session continuity across scrapes.
- `resolve()` fallback: cache miss triggers a single on-demand PRS call (~0.5s) rather than a full Playwright session.
- Playwright dependency removed entirely from the scraper.

**Investigation path (preserved for reference):**
- Initial approach used Playwright + ATVWebPlayerSDK hook. Worked but SDK serializes `requestResources()` calls; benchmarked at ~13s/channel, making 884 channels ≈ 3h.
- Intermediate step: switched to lazy on-demand resolve only (no bulk pre-cache during scrape).
- Final discovery: captured full PRS request params from Playwright intercept, confirmed direct HTTP works, benchmarked 50/50 channels in 1.7s. Playwright removed.

### Worker Architecture

- **Split scheduler from scraper worker** (`cfa98bf`): each role (`scheduler`, `fast`, `scraper`, `maintenance`) now runs as a separate process, each with its own watchdog restart loop in `entrypoint.sh`. Previously, the fast worker ran as a thread inside the scraper process, and the scheduler and scraper shared a single process.
- **Isolated fast worker** (`e453148`): moved from thread to child process to avoid deadlocks from forking with live threads. Then removed `daemon=True` (`65b47d1`) so the fast worker's RQ workhorse subprocesses can themselves fork (daemon processes cannot spawn children).
- **Maintenance queue** (`30e0f8a`): heavier non-urgent background jobs (source channel purge, bulk channel enable/disable, Gracenote auto-clear, TVTV cache refresh) moved off the scraper queue to a new `maintenance` queue with its own worker. Keeps the scraper queue clear for actual scrape jobs.
- **SIGALRM restoration** (`b62f175`): scrape phase timeouts now correctly restore the parent RQ job timeout after each phase. Previously the parent timer was cancelled after the first phase completed. Also reduced RQ job timeout from 3600s → 600s.
- **Batched EPG prune** (`eace0e0`): `_prune_old_programs()` now deletes in batches of 2000 rows instead of one large DELETE, avoiding SQLite lock contention. Added `idx_programs_end_time` index (migration 013) to make the prune query fast.

### Startup Migrations

- `run_migrations.py` (`40f409d`): new script runs numbered `migrations/[0-9][0-9][0-9]_*.py` scripts exactly once per DB using a `schema_migrations` tracking table. Invoked at container startup in `entrypoint.sh` before workers/gunicorn start. `set -e` in entrypoint means a failed migration aborts startup cleanly.
- Migration 013: adds `idx_programs_end_time` index.
- Migration 014: seeds `Channel.number` from the current effective lineup so the new sticky allocator preserves existing numbering on first deploy instead of renumbering everything.

### Sticky Automatic Channel Numbering

- Automatic channel numbers are now stable across scrapes (`60d6af8`). Previously, `Channel.number` was set by the scraper on every upsert, causing numbers to shift whenever channel counts changed or scrapers re-ordered results.
- New behavior: existing non-pinned `Channel.number` values are kept when still valid and collision-free. Only new channels or channels with conflicting/invalid numbers get fresh allocations.
- `_refresh_auto_channel_numbers()` runs at the end of every `_upsert_channels()` call, rebuilding the full chnum map and writing back only changed values.
- New channels enter the DB with `number=None`; the allocator assigns them on first flush.
- Scraper-supplied numbers are now ignored (previously set on new channel rows).
- This complements the user-set pinning from 2.4.0: user pins are immovable; auto numbers are stable but can shift to resolve conflicts.

### Admin UI

- **Feed filter in channels admin** (`727bbac`): "More filters" drawer now includes a Feed selector. Filtering by feed shows only channels that would appear in that feed's output (respects source, category, language, gracenote, excluded-channel rules). Works with pagination.
- **Channels preview modal** (`0f20e11`): wider (760→940px), action buttons now in a CSS grid layout instead of flex-wrap, Gracenote mode selector made more compact.
- **Code**: deduplicated feed membership filter logic — `api.py`'s `_apply_channel_filters` now calls `_apply_admin_feed_membership_filters` from `admin.py` instead of duplicating 18 lines.

### Local Now — EPG Coverage Fix

- **Root cause identified**: The Local Now `/live/epg/US/website` API always returns exactly 5 programs per channel regardless of the `program_size` parameter (API ignores it). Short-duration-show channels (e.g. Euronews English, 30-min news segments) yield only ~1 hour of future EPG coverage per scrape. With the old 360-minute scrape interval, those channels had zero guide data for ~5 hours between runs.
- **Fix**: `scrape_interval` reduced from 360 to 60 minutes. A single scrape fetches all ~433 channels in one bulk API call, so the additional frequency adds negligible load. The rolling EPG upsert logic (added in 2.1.3) handles the rest — each hourly scrape fills the next window forward.
- **Migration 015**: resets `scrape_interval` to 60 for any localnow source still at the old 360 default. Custom values are not touched.
- `program_size` config option is still accepted but its help text now correctly states the API ignores it.

**Coverage characteristics (observed, March 2026):**
- API always returns exactly 5 programs/channel — no pagination, no time-window params.
- Worst channel: Euronews English, ~1.02h future coverage per scrape.
- 10th percentile: ~2h. Median: ~4h. Best: 26h (NatureStream.TV, long-format nature content).
- 112/433 channels under 3h per scrape; hourly refresh ensures continuous coverage for all of them.

### Upgrade / Migration Notes

- Migrations 013, 014, and 015 run automatically on container startup via `run_migrations.py`.
- 013 adds a DB index — fast, safe to run on any DB size.
- 014 seeds `Channel.number` for active non-pinned channels — one-time write, preserves existing numbering.
- 015 updates Local Now scrape interval from 360 to 60 minutes (only if still at the old default).
- `prs_device_id` is generated and saved to `source.config` on first amazon_prime_free scrape — no manual action needed.
- Playwright is no longer used or imported by the amazon_prime_free scraper. It can be removed from the container image in a future cleanup if no other scraper uses it.

---

## 2.1.3

Release focus: Roku EPG horizon expansion.

### Roku

- EPG data now accumulates across hourly fetches instead of being replaced each run. Previously each scrape deleted all future programs and re-inserted only the ~2.5h window the Roku API returns, keeping the guide horizon flat. Now `_upsert_programs` scopes its delete to the exact time window covered by the incoming batch, so programs from earlier fetches that extend beyond that window are preserved. A full day of runs builds ~24h of guide data; coverage grows further over subsequent days.

### Upgrade / Migration Notes

- No schema changes. No manual migration required.
- Existing Roku EPG data is retained on upgrade; the horizon will grow automatically with each subsequent scrape run.

---

## 2.0.x → 2.1.0

Release focus: Gracenote coverage expansion, Roku stream reliability, worker stability, and logo cache overhaul.

### Gracenote

- Added cross-source name matching at M3U generation time — any channel whose name matches a channel with a known Gracenote ID is automatically routed to the Gracenote M3U, regardless of install, source, or stored `gracenote_id`. Works with no config or CSV entries required.
- Added 150 Roku Gracenote ID mappings to `gracenote_map.csv` via cross-source name matching (Roku's API doesn't expose GN IDs directly).
- Added 29 Plex Gracenote ID mappings to `gracenote_map.csv` via cross-source name matching.
- Samsung, Xumo, LG Channels, LocalNow, Stirr, Distro, FreeLiveSports now inherit Gracenote coverage from name matches against Pluto/Tubi/Roku/Plex — all were at 0% before.
- Plex CSV entries use suffix matching (channel UUID only, server UUID stripped) so mappings work across all Plex installs automatically.

### Roku

- Persist OSM session token (`session_token`, `trace_id`, `cached_at`) to `sources.config` so subsequent scraper instances and `/play` resolves reuse it without hitting `/api/v3/playback` each time. TTL matches the 5h stream URL cache.
- Migration path built in: first resolve after upgrade seeds the token from the existing stream URL cache.
- Fixed dead channel detection over-triggering the fallback content API on channels that are genuinely dead rather than just experiencing a 403 cooldown.
- Audit worker now detects active 403 cooldown and sleeps it out instead of counting toward the 20-consecutive-error abort threshold.

### Worker / Reliability

- Added a dedicated `fast` Redis queue for latency-sensitive jobs (XML/M3U artifact refresh). Previously these competed with slow scrape jobs on the same queue.
- Fast worker runs as a thread with its own Redis connection (SIGALRM disabled for thread safety).
- Fixed startup DB lock race: schema migration now runs once in the entrypoint before worker/gunicorn start (`FC_SCHEMA_READY=1` flag prevents re-runs).
- Fixed M3U 503 gap during refresh — new artifact is written to a temp file then atomically swapped in; old file remains serveable until the swap.
- Fixed background manifest check thread losing Flask app context, causing errors on DRM/dead stream auto-disable.
- SSL handshake failures now flagged as dead streams rather than transient network errors.
- Fixed XML cache race condition where concurrent refresh could leave artifacts in an inconsistent state.

### Logo Cache

- Switched from TTL-based expiry (3-day fixed TTL) to URL-change expiry — logos are now kept until the upstream source URL changes, not re-fetched on a timer.
- `.url` sidecar files excluded from logo count in system stats.
- Hardened against bad image data and partial writes.

### Other

- Added `wsgi.py` entry point to ensure gevent monkey-patching runs before any app imports (prevents subtle gevent/stdlib conflicts under gunicorn).
- Hidden movie episode metadata (season/episode numbers) from XMLTV output for VOD-style content.
- Admin settings page split into separate cards; gunicorn app preloaded at startup.

### Upgrade / Migration Notes

- No schema changes. No manual migration required.
- Roku OSM session token is optional — missing key is handled gracefully; first scrape after upgrade seeds it.
- Existing cached logos are kept on upgrade; no purge occurs when switching to URL-driven expiry.

# What's Next — 2026-03-15

## Roku Posters Still Blank in Channels DVR

**Root cause:** Channels DVR cached the old 302 redirect responses from before the
image proxy fix. At that time, our proxy redirected on miss → Roku CDN → 403. DVR
cached the failure and isn't re-requesting.

**Proxy is confirmed working.** All Roku poster URLs return HTTP 200 with real image
data through our proxy. The server side is correct.

**Fix:** Force a full EPG rescan in Channels DVR so it re-downloads the EPG XML and
re-fetches poster artwork with fresh URLs.

In Channels DVR:
- Go to Settings → Sources → your FastChannels source
- "Refresh Guide Data" or remove+re-add the source
- Alternatively: Settings → Support → Clear Guide Cache, then refresh

---

## Completed This Session

- Switched gunicorn to gevent workers (4 workers × 1000 connections each)
  — fixes logo/poster burst requests starving M3U/EPG workers
- Image proxy overhaul:
  - Fetch-on-miss inline (no more 302 redirects) — clients always get real images
  - `send_file()` for cache hits — OS-level file serving
  - Split cache: `logos/` (3-day TTL) and `posters/` (DB-driven, deleted 2h after program ends)
  - Roku posters proxied/cached; all other sources serve poster URLs raw to clients
- Suppressed noisy logs: scrape-status polling, image proxy hits, Chrome HTTPS-First TLS warnings
- Deleted orphaned `app/play.py`
- Migrated old flat logo cache into new `logos/` subdir

---

## Other Things To Look At

- **SSD upgrade** — main drive at 88% used (29GB free). Consider upgrading.
- **Poster size outliers** — some posters are 3-8MB (Roku channel logos especially).
  Could add a max-size skip (e.g. >500KB → don't cache, serve raw) to keep disk usage sane.
- **Prewarm posters** — currently posters are only cached on first client request.
  Could prewarm Roku posters after each scrape the same way logos are prewarmed.
  Would eliminate all first-request delays for Roku artwork.
- **gunicorn workers** — currently 4 workers hardcoded. Could make this
  configurable via env var for easier tuning.

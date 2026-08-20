import logging
import re


# Suppress high-frequency / low-signal endpoints from the access log.
_SUPPRESS_PATTERNS = (
    'scrape-status',           # admin UI polls every 2s during scrape
    'disable-status',          # admin UI polls every 1s while a source disable is queued
    'audit-status',            # admin UI polls every 2s during audit
    'directv-auth-status',     # admin UI polls every 2s during DirecTV login
    'sling-browser-login/state', # admin UI polls every ~400ms during Sling sign-in
    'sling-browser-login/input', # mousemove/click/key forwarding — up to ~16/s while dragging
    'tve/browser-login/state',  # admin UI polls every ~400ms during MVPD browser sign-in
    'tve/browser-login/input',  # mousemove/click/key forwarding — up to ~16/s while dragging
    'tve/nbc/browser-login/state',
    'tve/nbc/browser-login/input',
    'tve/fox/browser-login/state',
    'tve/fox/browser-login/input',
    'tve/amcn/browser-login/state',
    'tve/amcn/browser-login/input',
    'tve/discovery/browser-login/state',
    'tve/discovery/browser-login/input',
    'tve/foxone/browser-login/state',   # gap: 'foxone/' breaks the generic 'tve/browser-login/' substring match above
    'tve/foxone/browser-login/input',
    'tve/google/browser-login/state',   # admin UI polls every ~250ms during the standalone Google sign-in
    'tve/google/browser-login/input',   # mousemove/click/key forwarding — up to ~16/s while dragging
    '/api/custom-channels/detect/', # stream detection status polling
    '/images/proxy',           # per-image cache hits — too noisy
    '/logos/',                 # cached logo file hits — too noisy
    '/posters/',               # cached poster file hits — too noisy
    '/play/custom/segment',    # segment proxy is high-volume by design
    '/play/stirr/segment',     # stirr relay segments — one per ~5s per viewer
    '/play/cspan/segment',     # cspan relay segments — one per ~6s per viewer
    '/play/distro/segment',    # distro relay segments — one per ~5s per viewer
    '/play/stirr/variant',     # stirr variant manifest refresh every ~5s
    '/proxy.m3u8',             # manifest proxy polls every ~3s during playback
    '"GET /static/',           # static asset cache hits — 304s add no signal
    '"HEAD /static/',          # deployment/sanity probes of static assets
    'GET /api/gracenote/community-summary',  # polled frequently, low signal
    'GET /api/gracenote/remote-map/status', # polled frequently, low signal
    'GET /favicon.ico',                     # browser auto-request, always 404
    'GET /api/system-stats',               # polled frequently, low signal
    'GET /api/channels/inactive-count',    # dashboard badge polling
    'GET /api/settings/local-backups',     # settings-page backup snapshot lookup
    'GET /api/settings/tve/status',        # settings-page TVE network status — fires on every page load, can take several seconds
    '/api/sources/chnum',      # overlap-banner polling
    '/api/feeds/chnum-ranges', # feed page chnum conflict checker
    '/play/amazon_prime_free/license', # Amazon DRM license — fires per key rotation
    '/play/fox_tve/key',      # FOX Weather Uplynk AES keys — one per key rotation
    'GET /api/sources HTTP',   # sources list fetched on every poll cycle finish
    '"GET /admin/',            # admin page navigation GETs (POSTs still logged)
    '"HEAD /admin/',           # deployment/sanity probes of admin pages
    'GET /api/logs',           # log viewer polling
)

# Suppress GET /api/sources/{id}/config but keep POSTs and action endpoints
_SUPPRESS_RE = re.compile(r'GET /api/sources/\d+/config |GET /api/channels/\d+/preview|/api/channels/\d+/inspect|GET /api/channels/\d+/feed-membership')

# Suppress feed/M3U/EPG requests — healthy DVR polling, not worth logging
_SUPPRESS_FEED_RE = re.compile(r'"GET /feeds/|"GET /m3u/|"GET /output/')

# Suppress fullscreen player-page hits. PrismCast's headless Chrome (and browser
# players) load /watch/<id> on every tune — HEAD+GET, each doubled by the ?_v
# cache-bust redirect = 4 lines per channel. The actual channel resolution is
# still logged by the app.routes.play logger, so these access lines add no signal.
_SUPPRESS_WATCH_RE = re.compile(r'"(?:GET|HEAD) /watch/\d+')

# DRM DASH manifest polls and license fetches — the channel-was-called-to-play event
# is already logged once by app.routes.play (or the /play/.../m3u8 entry point);
# these are just the player re-polling during an ongoing session, so they're fully
# suppressed on success rather than throttled. Errors (4xx/5xx) still get through.
_SUCCESS_ACCESS_RE = re.compile(r'HTTP/\d(?:\.\d)?" [23]\d\d ')
_SUCCESS_SUPPRESS_PATTERNS = (
    '/play/philo/license',     # Philo DRM license — noisy during startup/key rotation
)
_SUCCESS_SUPPRESS_RE = re.compile(r'(?:GET|HEAD) /play/directv/browser-asset\?url=')
_DASH_RE = re.compile(r'(?:GET|HEAD) /play/(amazon_prime_free|cox|philo|sling|pbs|vidaa)/[^/]+/dash\.mpd')
# cox uses path-form license URLs (/license/<id>), vidaa uses query-form
# (/license?channel_id=<id>) — matched via the alternated separator below.
_LICENSE_RE = re.compile(r'POST /play/(cox|vidaa)/license(?:/|\?channel_id=)')


class _AccessFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        is_success = bool(_SUCCESS_ACCESS_RE.search(msg))
        if is_success and any(p in msg for p in _SUCCESS_SUPPRESS_PATTERNS):
            return False
        if is_success and _SUCCESS_SUPPRESS_RE.search(msg):
            return False
        if is_success and _DASH_RE.search(msg):
            return False
        if is_success and _LICENSE_RE.search(msg):
            return False
        if any(p in msg for p in _SUPPRESS_PATTERNS):
            return False
        if _SUPPRESS_RE.search(msg):
            return False
        if _SUPPRESS_FEED_RE.search(msg):
            return False
        if _SUPPRESS_WATCH_RE.search(msg):
            return False
        return True


# Suppress TLS handshake warnings — Chrome's HTTPS-First mode sends a TLS
# Client Hello to our plain-HTTP port on every navigation, gets rejected, then
# falls back to HTTP automatically.  The warning is harmless but noisy.
#
# Also suppress routine worker-recycling lines from the arbiter (--max-requests
# forces each worker to restart every ~250 requests; "Autorestarting worker",
# "Worker exiting", "Booting worker" fire constantly and add no signal at INFO).
_WORKER_RECYCLE_PATTERNS = (
    'Autorestarting worker after current request',
    'Worker exiting',
    'Booting worker with pid',
)


class _TLSHandshakeFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        if 'Invalid HTTP method' in msg or 'Invalid HTTP request line' in msg:
            return False
        if any(p in msg for p in _WORKER_RECYCLE_PATTERNS):
            return False
        return True


def on_starting(server):
    from app.timezone_utils import make_tz_formatter
    _fmt = make_tz_formatter('%(asctime)s %(levelname)-8s %(name)s: %(message)s')
    for name in ('gunicorn.error', 'gunicorn.access'):
        lg = logging.getLogger(name)
        for h in lg.handlers:
            h.setFormatter(_fmt)
        lg.propagate = False  # prevent double-logging via root handler

    logging.getLogger('gunicorn.access').addFilter(_AccessFilter())
    logging.getLogger('gunicorn.error').addFilter(_TLSHandshakeFilter())

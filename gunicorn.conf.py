import logging
import re
import time


# Suppress high-frequency / low-signal endpoints from the access log.
_SUPPRESS_PATTERNS = (
    'scrape-status',           # admin UI polls every 2s during scrape
    'audit-status',            # admin UI polls every 2s during audit
    '/api/custom-channels/detect/', # stream detection status polling
    '/images/proxy',           # per-image cache hits — too noisy
    '/logos/',                 # cached logo file hits — too noisy
    '/posters/',               # cached poster file hits — too noisy
    '/play/custom/segment',    # segment proxy is high-volume by design
    '/play/stirr/segment',     # stirr relay segments — one per ~5s per viewer
    '/play/cspan/segment',     # cspan relay segments — one per ~6s per viewer
    '/play/stirr/variant',     # stirr variant manifest refresh every ~5s
    '/proxy.m3u8',             # manifest proxy polls every ~3s during playback
    '"GET /static/',           # static asset cache hits — 304s add no signal
    'GET /api/gracenote/community-summary',  # polled frequently, low signal
    'GET /api/gracenote/remote-map/status', # polled frequently, low signal
    'GET /favicon.ico',                     # browser auto-request, always 404
    'GET /api/system-stats',               # polled frequently, low signal
    '/api/sources/chnum',      # overlap-banner polling
    '/api/feeds/chnum-ranges', # feed page chnum conflict checker
    '/play/amazon_prime_free/license', # Amazon DRM license — fires per key rotation
    'GET /api/sources HTTP',   # sources list fetched on every poll cycle finish
    '"GET /admin/',            # admin page navigation GETs (POSTs still logged)
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

# Match DRM DASH manifest polls: (GET|HEAD) /play/<source>/<channel_id>/dash.mpd
_SUCCESS_ACCESS_RE = re.compile(r'HTTP/\d(?:\.\d)?" [23]\d\d ')
_SUCCESS_SUPPRESS_PATTERNS = (
    '/play/philo/license',     # Philo DRM license — noisy during startup/key rotation
)
_SUCCESS_SUPPRESS_RE = re.compile(r'(?:GET|HEAD) /play/philo/[^/]+/dash\.mpd')
_DASH_RE = re.compile(r'(?:GET|HEAD) /play/(amazon_prime_free|philo)/([^/]+)/dash\.mpd')
_DASH_COOLDOWN = 120  # seconds — log first request, suppress repeats within this window


class _AccessFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self._dash_last: dict[str, float] = {}

    def filter(self, record):
        msg = record.getMessage()
        is_success = bool(_SUCCESS_ACCESS_RE.search(msg))
        if is_success and any(p in msg for p in _SUCCESS_SUPPRESS_PATTERNS):
            return False
        if is_success and _SUCCESS_SUPPRESS_RE.search(msg):
            return False
        if any(p in msg for p in _SUPPRESS_PATTERNS):
            return False
        if _SUPPRESS_RE.search(msg):
            return False
        if _SUPPRESS_FEED_RE.search(msg):
            return False
        if _SUPPRESS_WATCH_RE.search(msg):
            return False
        m = _DASH_RE.search(msg)
        if m and is_success:
            channel_id = f'{m.group(1)}:{m.group(2)}'
            now = time.monotonic()
            if now - self._dash_last.get(channel_id, 0) < _DASH_COOLDOWN:
                return False
            self._dash_last[channel_id] = now
        return True


# Suppress TLS handshake warnings — Chrome's HTTPS-First mode sends a TLS
# Client Hello to our plain-HTTP port on every navigation, gets rejected, then
# falls back to HTTP automatically.  The warning is harmless but noisy.
class _TLSHandshakeFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return 'Invalid HTTP method' not in msg and 'Invalid HTTP request line' not in msg


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

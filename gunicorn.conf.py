import logging
import re


# Suppress high-frequency / low-signal endpoints from the access log.
_SUPPRESS_PATTERNS = (
    'scrape-status',           # admin UI polls every 2s during scrape
    'audit-status',            # admin UI polls every 2s during audit
    '/images/proxy',           # per-image cache hits — too noisy
    '/logos/',                 # cached logo file hits — too noisy
    '/posters/',               # cached poster file hits — too noisy
    '/api/sources/chnum',      # overlap-banner polling
    'GET /api/sources HTTP',   # sources list fetched on every poll cycle finish
    '"GET /admin/',            # admin page navigation GETs (POSTs still logged)
    'GET /api/logs',           # log viewer polling
)

# Suppress GET /api/sources/{id}/config but keep POSTs and action endpoints
_SUPPRESS_RE = re.compile(r'GET /api/sources/\d+/config ')

# Suppress feed/M3U/EPG requests — healthy DVR polling, not worth logging
_SUPPRESS_FEED_RE = re.compile(r'"GET /feeds/|"GET /m3u/|"GET /output/')


class _AccessFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        if any(p in msg for p in _SUPPRESS_PATTERNS):
            return False
        if _SUPPRESS_RE.search(msg):
            return False
        if _SUPPRESS_FEED_RE.search(msg):
            return False
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

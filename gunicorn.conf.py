import logging


# Suppress high-frequency polling endpoints from the access log.
_SUPPRESS_PATTERNS = (
    'scrape-status',
    'audit-status',
    '/images/proxy',   # per-image cache hits — too noisy
)


class _AccessFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return not any(p in msg for p in _SUPPRESS_PATTERNS)


# Suppress TLS handshake warnings — Chrome's HTTPS-First mode sends a TLS
# Client Hello to our plain-HTTP port on every navigation, gets rejected, then
# falls back to HTTP automatically.  The warning is harmless but noisy.
class _TLSHandshakeFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return 'Invalid HTTP method' not in msg and 'Invalid HTTP request line' not in msg


def on_starting(server):
    logging.getLogger('gunicorn.access').addFilter(_AccessFilter())
    logging.getLogger('gunicorn.error').addFilter(_TLSHandshakeFilter())

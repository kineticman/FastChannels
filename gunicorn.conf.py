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


def on_starting(server):
    logging.getLogger('gunicorn.access').addFilter(_AccessFilter())

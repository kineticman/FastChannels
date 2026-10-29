from __future__ import annotations

from .base import BaseScraper, ChannelData, ProgramData, StreamDeadError

# Stream types the audit can read as a manifest. Anything else (MPEG-TS, MP4,
# MJPEG, ...) is an endless or large body, so audit_resolve probes it instead.
_AUDIT_MANIFEST_TYPES = ('hls', 'dash')


class CustomScraper(BaseScraper):
    """
    Stub scraper for user-created custom channels.

    Channels under this source are created via the admin API, not by scraping.
    fetch_channels / fetch_epg are no-ops; resolve() passes stream_url through
    unchanged so the play proxy handles it directly.
    """

    source_name = 'custom'
    display_name = 'Custom Channels'
    scrape_interval = 0          # never auto-scraped (scheduler skips interval=0)
    stream_audit_enabled = True  # Audit button only; nothing schedules it
    config_schema = []

    def fetch_channels(self) -> list[ChannelData]:
        return []

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        return []

    def resolve(self, raw_url: str) -> str:
        return raw_url

    def audit_resolve(self, raw_url: str) -> str:
        """Check a custom channel the way the play proxy plays it.

        A channel added from a page (YouTube, a web player) stores the last
        stream found there, which expires, so its page is detected afresh; a
        page with no stream left is dead. The channel's stored headers
        (Referer, User-Agent) go on the session the audit fetches the manifest
        with, so a stream that needs them isn't read as a 403. Streams that
        aren't a manifest are probed here and returned as an opaque URL, which
        the audit takes as confirmed.
        """
        from ..models import Channel, Source

        if not hasattr(self, '_audit_base_headers'):
            self._audit_base_headers = dict(self.session.headers)

        ch = (Channel.query.join(Source)
              .filter(Source.name == self.source_name, Channel.stream_url == raw_url)
              .first())
        url, headers = raw_url, dict((ch.custom_headers if ch else None) or {})
        if ch is not None and ch.page_url:
            from ..routes.custom_proxy import _redetect_custom_stream_with_info
            url, headers, info = _redetect_custom_stream_with_info(ch, ttl=0)
            if info.get('path') == 'detect-failed':
                raise StreamDeadError(f"No stream found on {ch.page_url}: {info.get('error') or 'detection failed'}")

        # _-prefixed keys are internal flags, never sent upstream.
        headers = {k: v for k, v in headers.items() if not k.startswith('_')}
        self.session.headers.clear()
        self.session.headers.update(self._audit_base_headers)
        self.session.headers.update(headers)

        from .stream_detector import StreamDetector
        stream_type = ((ch.stream_type if ch else None) or StreamDetector.infer_stream_type(url) or 'hls').lower()
        if stream_type in _AUDIT_MANIFEST_TYPES:
            return url

        with self.session.get(url, timeout=15, stream=True, allow_redirects=True) as r:
            if r.status_code in (404, 410):
                raise StreamDeadError(f'HTTP {r.status_code}')
            r.raise_for_status()
            if not next(r.iter_content(4096), b''):
                raise StreamDeadError('Empty response')
        return f'custom-probe://{ch.id if ch else 0}'

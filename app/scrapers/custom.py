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
    # Audit button only: not part of Audit All Sources, and nothing schedules it.
    stream_audit_enabled = True
    stream_audit_in_audit_all = False
    audit_skip_reason = 'Added from a page: detected fresh each time it plays'
    config_schema = []

    def fetch_channels(self) -> list[ChannelData]:
        return []

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        return []

    def resolve(self, raw_url: str) -> str:
        return raw_url

    def audit_candidates(self, query):
        """Direct stream links only. A channel added from a page (YouTube,
        Twitch, a web player) is detected afresh each time it plays, and a
        part-time stream between broadcasts would read as dead, so the audit
        leaves those alone."""
        from ..models import Channel
        return query.filter(Channel.page_url.is_(None))

    def audit_resolve(self, raw_url: str) -> str:
        """Check a direct custom channel with its stored headers.

        The channel's headers (Referer, User-Agent) go on the session the audit
        fetches the manifest with, so a stream that needs them isn't read as a
        403. Streams that aren't a manifest are probed here and returned as an
        opaque URL, which the audit takes as confirmed.
        """
        from ..models import Channel, Source

        if not hasattr(self, '_audit_base_headers'):
            self._audit_base_headers = dict(self.session.headers)

        ch = (Channel.query.join(Source)
              .filter(Source.name == self.source_name, Channel.stream_url == raw_url)
              .first())
        # _-prefixed keys are internal flags, never sent upstream.
        headers = {k: v for k, v in ((ch.custom_headers if ch else None) or {}).items()
                   if not k.startswith('_')}
        self.session.headers.clear()
        self.session.headers.update(self._audit_base_headers)
        self.session.headers.update(headers)

        from .stream_detector import StreamDetector
        stream_type = ((ch.stream_type if ch else None) or StreamDetector.infer_stream_type(raw_url) or 'hls').lower()
        if stream_type in _AUDIT_MANIFEST_TYPES:
            return raw_url

        with self.session.get(raw_url, timeout=15, stream=True, allow_redirects=True) as r:
            if r.status_code in (404, 410):
                raise StreamDeadError(f'HTTP {r.status_code}')
            r.raise_for_status()
            if not next(r.iter_content(4096), b''):
                raise StreamDeadError('Empty response')
        return f'custom-probe://{ch.id if ch else 0}'

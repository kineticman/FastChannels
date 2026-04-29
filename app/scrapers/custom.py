from __future__ import annotations

from .base import BaseScraper, ChannelData, ProgramData


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
    stream_audit_enabled = False
    config_schema = []

    def fetch_channels(self) -> list[ChannelData]:
        return []

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        return []

    def resolve(self, raw_url: str) -> str:
        return raw_url

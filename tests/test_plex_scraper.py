from __future__ import annotations

import pytest

from app.scrapers.base import StreamDeadError
from app.scrapers.plex import PlexScraper


class _FakeResponse:
    def __init__(self, status_code: int, url: str = "https://example.test/manifest.m3u8"):
        self.status_code = status_code
        self.url = url


class _FakeSession:
    def __init__(self, status_code: int):
        self.status_code = status_code
        self.calls: list[str] = []

    def post(self, *args, **kwargs):
        return None

    def get(self, url: str, timeout: int | None = None, allow_redirects: bool = True):
        self.calls.append(url)
        return _FakeResponse(self.status_code, url=f"https://example.test/{self.status_code}")


def test_plex_resolve_treats_hard_http_failures_as_dead(monkeypatch):
    scraper = PlexScraper(config={})
    scraper._ensure_auth = lambda force=False: True
    scraper.session = _FakeSession(404)

    with pytest.raises(StreamDeadError, match=r"HTTP 404"):
        scraper.resolve("plex://channel-123")


def test_plex_audit_resolve_treats_hard_http_failures_as_dead(monkeypatch):
    scraper = PlexScraper(config={})
    scraper._ensure_auth = lambda force=False: True
    scraper.session = _FakeSession(410)

    with pytest.raises(StreamDeadError, match=r"HTTP 410"):
        scraper.audit_resolve("plex://channel-123")

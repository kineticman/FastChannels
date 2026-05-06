from __future__ import annotations

from app.scrapers.base import format_http_reason


def test_format_http_reason_includes_http_code():
    assert format_http_reason("[plex] channel not playable", 404, "abc123") == "[plex] channel not playable (HTTP 404): abc123"


def test_format_http_reason_omits_detail_when_missing():
    assert format_http_reason("[roku] channel not found", 404) == "[roku] channel not found (HTTP 404)"

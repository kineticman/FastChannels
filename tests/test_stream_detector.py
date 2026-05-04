from __future__ import annotations

from app.scrapers.stream_detector import StreamDetector


class _FakeResponse:
    def __init__(self, text: str = "", status_code: int = 200, headers: dict[str, str] | None = None):
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def close(self) -> None:
        pass


class _FakeSession:
    def __init__(self):
        self.headers: dict[str, str] = {}
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    def get(self, url: str, timeout: int | None = None, headers: dict[str, str] | None = None, stream: bool = False):
        self.calls.append((url, headers.copy() if headers else None))

        if url == "https://watch.test/page":
            return _FakeResponse(
                text='<html><body><iframe src="https://embed.test/player"></iframe></body></html>',
            )

        if url == "https://embed.test/player":
            if not headers or headers.get("Referer") != "https://watch.test/page":
                return _FakeResponse(status_code=403)
            return _FakeResponse(
                text='<html><body>https://cdn.test/live.m3u8</body></html>',
            )

        if url == "https://cdn.test/live.m3u8":
            return _FakeResponse(
                text="#EXTM3U\n#EXT-X-VERSION:3\n",
                headers={"Content-Type": "application/vnd.apple.mpegurl"},
            )

        return _FakeResponse(status_code=404)


def test_detects_hls_through_iframe_referer_chain(monkeypatch):
    fake_session = _FakeSession()
    monkeypatch.setattr("app.scrapers.stream_detector.requests.Session", lambda: fake_session)

    result = StreamDetector().detect("https://watch.test/page")

    assert result.success is True
    assert result.stream_type == "hls"
    assert result.stream_url == "https://cdn.test/live.m3u8"
    assert any(
        url == "https://embed.test/player" and headers and headers.get("Referer") == "https://watch.test/page"
        for url, headers in fake_session.calls
    )

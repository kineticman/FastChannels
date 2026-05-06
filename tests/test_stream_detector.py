from __future__ import annotations

from types import SimpleNamespace

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


class _FakePlaywrightContext:
    def __init__(self, stream_url: str):
        self._stream_url = stream_url

    def __enter__(self):
        class _Chromium:
            def __init__(self, stream_url: str):
                self._stream_url = stream_url

            def launch(self, headless: bool = True):
                return _FakeBrowser(self._stream_url)

        return SimpleNamespace(chromium=_Chromium(self._stream_url))

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeBrowser:
    def __init__(self, stream_url: str):
        self._stream_url = stream_url

    def new_page(self):
        return _FakePage(self._stream_url)

    def close(self):
        pass


class _FakePage:
    def __init__(self, stream_url: str):
        self._stream_url = stream_url
        self._request_handler = None

    def on(self, event: str, handler):
        if event == "request":
            self._request_handler = handler

    def goto(self, url: str, wait_until: str | None = None, timeout: int | None = None):
        if self._request_handler:
            self._request_handler(SimpleNamespace(url=self._stream_url))

    def wait_for_timeout(self, timeout_ms: int):
        pass


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


def test_detects_final_stream_from_pooembed_embed(monkeypatch):
    fake_session = _FakeSession()
    fake_session.calls = []

    def fake_get(url: str, timeout: int | None = None, headers: dict[str, str] | None = None, stream: bool = False):
        fake_session.calls.append((url, headers.copy() if headers else None))

        if url == "https://watch.test/page":
            return _FakeResponse(text='<html><body><iframe src="https://embed.test/player"></iframe></body></html>')

        if url == "https://embed.test/player":
            if not headers or headers.get("Referer") != "https://watch.test/page":
                return _FakeResponse(status_code=403)
            return _FakeResponse(text='<html><body><iframe src="https://pooembed.eu/embed-noads/laliga/2026-05-04/sev-rso"></iframe></body></html>')

        if url == "https://pooembed.eu/embed-noads/laliga/2026-05-04/sev-rso":
            if not headers or headers.get("Referer") != "https://embed.test/player":
                return _FakeResponse(status_code=403)
            return _FakeResponse(text="<html><body></body></html>")

        if url == "https://netanyahu.modifiles.fans/secure/uxySqtBEesgWjdSfHeQMzGWlVwhTtLNP/1777910400/1777937400/laligatv/index.m3u8":
            return _FakeResponse(
                text="#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-STREAM-INF:BANDWIDTH=6730000\ntracks-v1a1/mono.ts.m3u8\n",
                headers={"Content-Type": "application/vnd.apple.mpegurl"},
            )

        return _FakeResponse(status_code=404)

    fake_session.get = fake_get

    monkeypatch.setattr("app.scrapers.stream_detector.requests.Session", lambda: fake_session)
    monkeypatch.setattr(
        "app.scrapers.stream_detector._sync_playwright",
        lambda: (lambda: _FakePlaywrightContext("https://netanyahu.modifiles.fans/secure/uxySqtBEesgWjdSfHeQMzGWlVwhTtLNP/1777910400/1777937400/laligatv/index.m3u8")),
    )

    result = StreamDetector().detect("https://watch.test/page")

    assert result.success is True
    assert result.stream_type == "hls"
    assert result.stream_url == "https://netanyahu.modifiles.fans/secure/uxySqtBEesgWjdSfHeQMzGWlVwhTtLNP/1777910400/1777937400/laligatv/index.m3u8"
    assert any(
        url == "https://pooembed.eu/embed-noads/laliga/2026-05-04/sev-rso"
        and headers
        and headers.get("Referer") == "https://embed.test/player"
        for url, headers in fake_session.calls
    )


def test_generic_playwright_fallback_finds_final_stream(monkeypatch):
    class _GenericSession:
        def __init__(self):
            self.headers: dict[str, str] = {}
            self.calls: list[tuple[str, dict[str, str] | None]] = []

        def get(self, url: str, timeout: int | None = None, headers: dict[str, str] | None = None, stream: bool = False):
            self.calls.append((url, headers.copy() if headers else None))

            if url == "https://generic.test/player":
                return _FakeResponse(text="<html><body><script>jwplayer({});</script></body></html>")

            if url == "https://cdn.test/final/index.m3u8":
                return _FakeResponse(
                    text="#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-STREAM-INF:BANDWIDTH=6730000\ntracks-v1a1/mono.ts.m3u8\n",
                    headers={"Content-Type": "application/vnd.apple.mpegurl"},
                )

            return _FakeResponse(status_code=404)

    fake_session = _GenericSession()

    monkeypatch.setattr("app.scrapers.stream_detector.requests.Session", lambda: fake_session)
    monkeypatch.setattr(
        "app.scrapers.stream_detector._sync_playwright",
        lambda: (lambda: _FakePlaywrightContext("https://cdn.test/final/index.m3u8")),
    )

    result = StreamDetector().detect("https://generic.test/player")

    assert result.success is True
    assert result.stream_type == "hls"
    assert result.stream_url == "https://cdn.test/final/index.m3u8"
    assert any(url == "https://generic.test/player" for url, _ in fake_session.calls)
    assert any(url == "https://cdn.test/final/index.m3u8" for url, _ in fake_session.calls)

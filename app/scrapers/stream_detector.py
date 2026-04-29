"""
Stream detector: given a URL (web page or direct HLS link), discover the
working stream URL and the minimum HTTP headers the player needs.

Detection flow:
  1. If URL already looks like a stream (.m3u8), go straight to probe step.
  2. Otherwise fetch the page, extract all .m3u8 candidates from JS/HTML,
     then follow <iframe src> chains up to MAX_IFRAME_DEPTH levels deep.
  3. For each candidate, try four header combos in order:
       bare → User-Agent only → + Referer → + Origin
     Both the playlist fetch and a sample segment must succeed.
  4. Return DetectionResult with the first working URL + headers combo.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urljoin

import requests

logger = logging.getLogger(__name__)

_BROWSER_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/145.0.0.0 Safari/537.36'
)

# Match bare .m3u8 URLs in HTML/JS source
_M3U8_RE = re.compile(
    r'https?://[^\s"\'<>()\[\]{}]+?\.m3u8(?:\?[^\s"\'<>()\[\]{}]*)?',
    re.IGNORECASE,
)
# Match JS object properties like src: "...", file: "...", url: "..."
_PROP_RE = re.compile(
    r'''(?:src|source|file|url|hls)\s*[:=]\s*['"]?(https?://[^\s'"<>()\[\]{}]+?\.m3u8(?:\?[^\s'"<>()\[\]{}]*)?)''',
    re.IGNORECASE,
)
_IFRAME_RE = re.compile(r'''<iframe[^>]+?src\s*=\s*['"]?([^'">\s]+)''', re.IGNORECASE)


@dataclass
class DetectionResult:
    stream_url: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    needs_proxy: bool = False   # True when segment access also requires headers
    success: bool = False
    error: str | None = None


class StreamDetector:
    MAX_IFRAME_DEPTH = 3
    TIMEOUT = 12

    def detect(self, input_url: str) -> DetectionResult:
        session = requests.Session()
        session.headers.update({'User-Agent': _BROWSER_UA})

        try:
            if self._is_stream_url(input_url):
                candidates = [input_url]
                page_url = input_url
            else:
                page_url = input_url
                candidates = self._extract_from_page(session, input_url, depth=0)
                if not candidates:
                    return DetectionResult(error='No HLS stream URL found on page or in iframes')

            origin = self._origin_of(page_url)

            for candidate in candidates:
                result = self._probe(session, candidate, page_url, origin)
                if result.success:
                    return result

            if candidates:
                return DetectionResult(
                    stream_url=candidates[0],
                    error='Stream URL found but no header combination allowed access',
                )
            return DetectionResult(error='No HLS stream found')

        except Exception as exc:
            logger.warning('[detector] unexpected error for %s: %s', input_url[:80], exc)
            return DetectionResult(error=str(exc))

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _is_stream_url(url: str) -> bool:
        path = urlsplit(url).path.lower()
        return '.m3u8' in path or path.endswith('.ts')

    @staticmethod
    def _origin_of(url: str) -> str:
        p = urlsplit(url)
        return f'{p.scheme}://{p.netloc}'

    def _extract_from_page(self, session: requests.Session, url: str, depth: int) -> list[str]:
        if depth > self.MAX_IFRAME_DEPTH:
            return []
        try:
            r = session.get(url, timeout=self.TIMEOUT)
            if not r.ok:
                return []
            text = r.text
        except Exception as exc:
            logger.debug('[detector] page fetch failed %s: %s', url[:80], exc)
            return []

        candidates: list[str] = []

        for m in _M3U8_RE.finditer(text):
            c = m.group(0).rstrip('"\'\\')
            if c not in candidates:
                candidates.append(c)

        for m in _PROP_RE.finditer(text):
            c = m.group(1).rstrip('"\'\\')
            if c not in candidates:
                candidates.append(c)

        for m in _IFRAME_RE.finditer(text):
            src = m.group(1).strip()
            if not src or src.startswith('javascript:') or src.startswith('about:'):
                continue
            iframe_url = urljoin(url, src)
            for c in self._extract_from_page(session, iframe_url, depth + 1):
                if c not in candidates:
                    candidates.append(c)

        return candidates

    def _probe(
        self,
        session: requests.Session,
        stream_url: str,
        page_url: str,
        origin: str,
    ) -> DetectionResult:
        combos: list[dict[str, str]] = [
            {},
            {'User-Agent': _BROWSER_UA},
            {'User-Agent': _BROWSER_UA, 'Referer': page_url},
            {'User-Agent': _BROWSER_UA, 'Referer': page_url, 'Origin': origin},
        ]

        for headers in combos:
            try:
                r = session.get(stream_url, headers=headers, timeout=self.TIMEOUT)
                if r.status_code != 200:
                    continue
                text = r.text
                if '#EXTM3U' not in text:
                    continue

                seg_url = self._first_segment_url(session, text, stream_url, headers)
                needs_proxy = False
                if seg_url:
                    needs_proxy = not self._segment_ok(session, seg_url, headers)

                return DetectionResult(
                    stream_url=stream_url,
                    headers=headers,
                    needs_proxy=needs_proxy,
                    success=True,
                )
            except Exception as exc:
                logger.debug('[detector] probe error %s: %s', stream_url[:80], exc)
                continue

        return DetectionResult(
            error=f'Stream URL found but no header combination worked: {stream_url[:80]}'
        )

    def _first_segment_url(
        self,
        session: requests.Session,
        text: str,
        base_url: str,
        headers: dict,
    ) -> str | None:
        """
        Return the first .ts segment URL from a playlist.
        If the playlist is a master, fetches the first variant to find a segment.
        """
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        skip_next = False
        for line in lines:
            if line.startswith('#EXT-X-STREAM-INF'):
                skip_next = True
                continue
            if line.startswith('#'):
                continue
            abs_url = line if line.startswith('http') else urljoin(base_url, line)
            if skip_next:
                # It's a variant playlist URL; fetch it to get an actual segment
                skip_next = False
                try:
                    vr = session.get(abs_url, headers=headers, timeout=self.TIMEOUT)
                    if vr.ok and '#EXTM3U' in vr.text:
                        return self._first_segment_url(session, vr.text, abs_url, headers)
                except Exception:
                    pass
                return None
            return abs_url
        return None

    @staticmethod
    def _segment_ok(session: requests.Session, seg_url: str, headers: dict) -> bool:
        try:
            r = session.get(
                seg_url,
                headers={**headers, 'Range': 'bytes=0-2047'},
                timeout=10,
                stream=True,
            )
            r.close()
            return r.status_code in (200, 206)
        except Exception:
            return False

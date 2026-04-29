"""
Stream detector: given a URL (web page or direct stream link), discover the
working stream URL and the minimum HTTP headers the player needs.

Detection flow:
  1. YouTube URLs → resolved via yt-dlp (separate path).
  2. If URL already looks like a stream (.m3u8, .mp4, .webm), go straight to probe.
  3. Otherwise fetch the page, extract all stream candidates from JS/HTML,
     then follow <iframe src> chains up to MAX_IFRAME_DEPTH levels deep.
     HLS (.m3u8) candidates are collected first; direct video (.mp4, .webm)
     candidates are appended as lower-priority fallbacks.
  4. For each candidate, try four header combos in order:
       bare → User-Agent only → + Referer → + Origin
     HLS: the playlist must contain #EXTM3U.
     Direct video: a HEAD (or GET) must return a video/* content-type.
  5. Return DetectionResult with the first working URL + headers combo.
"""
from __future__ import annotations
import logging
import html
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
# Covers both HLS (.m3u8) and direct video (.mp4, .webm) values
_PROP_RE = re.compile(
    r'''(?:src|source|file|url|hls|video|stream)\s*[:=]\s*['"]?(https?://[^\s'"<>()\[\]{}]+?\.(?:m3u8|mp4|webm)(?:\?[^\s'"<>()\[\]{}]*)?)''',
    re.IGNORECASE,
)
# Match bare direct-video URLs (.mp4, .webm) — lower priority than HLS
_VIDEO_URL_RE = re.compile(
    r'https?://[^\s"\'<>()\[\]{}]+?\.(?:mp4|webm)(?:\?[^\s"\'<>()\[\]{}]*)?',
    re.IGNORECASE,
)
_IFRAME_RE = re.compile(r'''<iframe[^>]+?src\s*=\s*['"]?([^'">\s]+)''', re.IGNORECASE)
_SCRIPT_SRC_RE = re.compile(r'''<script[^>]+?src\s*=\s*['"]?([^'">\s]+)''', re.IGNORECASE)
_SCRIPT_BLOCK_RE = re.compile(r'''<script\b[^>]*>(.*?)</script>''', re.IGNORECASE | re.DOTALL)
_JSON_URL_RE = re.compile(r'''https?://[^\s"'<>()\[\]{}]+?\.json(?:\?[^\s"'<>()\[\]{}]*)?''', re.IGNORECASE)
_JSON_REL_RE = re.compile(r'''['"]([^'"]+?\.json(?:\?[^'"]*)?)['"]''', re.IGNORECASE)
_BROWNRICE_STREAMURL_RE = re.compile(r'''camera\[['"]streamurl['"]\]\s*=\s*['"]([^'"]+)['"]''')
_BROWNRICE_NAME_RE = re.compile(r'''camera\[['"]name['"]\]\s*=\s*['"]([^'"]+)['"]''')
_BROWNRICE_TYPEID_RE = re.compile(r'''camera\[['"]typeid['"]\]\s*=\s*['"]([^'"]+)['"]''')
_PLAYER_RELATIVE_MEDIA_RE = re.compile(
    r'''(?:file|src|contentUrl)\s*[:=]\s*['"]([^'"]+?\.(?:m3u8|mp4|webm)(?:\?[^'"]*)?)['"]''',
    re.IGNORECASE,
)

_YOUTUBE_RE = re.compile(
    r'^https?://(www\.)?(youtube\.com/(watch|live|embed/|@|channel|user|c/|shorts/)|youtu\.be/)',
    re.IGNORECASE,
)
_YT_PLAYER_CLIENTS = ('tv_embedded', 'web_safari', 'web', 'ios')

# Content-types accepted for non-HLS direct video probing
_VIDEO_CONTENT_TYPES = ('video/', 'audio/', 'application/octet-stream')


@dataclass
class DetectionResult:
    stream_url: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    needs_proxy: bool = False   # True when segment access also requires headers
    success: bool = False
    error: str | None = None
    is_youtube: bool = False


class StreamDetector:
    MAX_IFRAME_DEPTH = 3
    TIMEOUT = 12

    def detect(self, input_url: str) -> DetectionResult:
        if _YOUTUBE_RE.match(input_url):
            return self._resolve_youtube(input_url)

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
                    return DetectionResult(error='No stream URL found on page or in iframes')

            origin = self._origin_of(page_url)

            for candidate in candidates:
                if _YOUTUBE_RE.match(candidate):
                    result = self._resolve_youtube(html.unescape(candidate))
                    if result.success:
                        return result
                    continue
                result = self._probe(session, candidate, page_url, origin)
                if result.success:
                    return result

            if candidates:
                return DetectionResult(
                    stream_url=candidates[0],
                    error='Stream URL found but no header combination allowed access',
                )
            return DetectionResult(error='No stream found')

        except Exception as exc:
            logger.warning('[detector] unexpected error for %s: %s', input_url[:80], exc)
            return DetectionResult(error=str(exc))

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _is_stream_url(url: str) -> bool:
        path = urlsplit(url).path.lower()
        return any(ext in path for ext in ('.m3u8', '.mp4', '.webm')) or path.endswith('.ts')

    @staticmethod
    def _is_hls_url(url: str) -> bool:
        path = urlsplit(url).path.lower()
        return '.m3u8' in path

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

        hls_candidates, video_candidates = self._extract_generic_candidates(text)

        # Provider-specific handlers run before iframe recursion so they can use
        # page-local bootstrap config without needing to brute-force every script.
        for c in self._extract_provider_candidates(session, url, text):
            if '.m3u8' in c.lower():
                if c not in hls_candidates:
                    hls_candidates.append(c)
            elif c not in video_candidates:
                video_candidates.append(c)

        return self._merge_with_iframe_candidates(session, url, depth, text, hls_candidates, video_candidates)

    @staticmethod
    def _extract_generic_candidates(text: str) -> tuple[list[str], list[str]]:
        hls_candidates: list[str] = []
        video_candidates: list[str] = []

        # HLS — highest priority
        for m in _M3U8_RE.finditer(text):
            c = m.group(0).rstrip('"\'\\')
            if c not in hls_candidates:
                hls_candidates.append(c)

        for m in _PROP_RE.finditer(text):
            c = m.group(1).rstrip('"\'\\')
            if '.m3u8' in c.lower():
                if c not in hls_candidates:
                    hls_candidates.append(c)
            else:
                if c not in video_candidates:
                    video_candidates.append(c)

        # Direct video (mp4, webm) — lower priority
        for m in _VIDEO_URL_RE.finditer(text):
            c = m.group(0).rstrip('"\'\\')
            if c not in hls_candidates and c not in video_candidates:
                video_candidates.append(c)

        return hls_candidates, video_candidates

    def _extract_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        candidates: list[str] = []

        for extractor in (
            self._extract_brownrice_provider_candidates,
            self._extract_player_config_candidates,
            self._extract_json_config_candidates,
        ):
            for c in extractor(session, url, text):
                if c not in candidates:
                    candidates.append(c)

        return candidates

    def _merge_with_iframe_candidates(
        self,
        session: requests.Session,
        url: str,
        depth: int,
        text: str,
        hls_candidates: list[str],
        video_candidates: list[str],
    ) -> list[str]:

        # Recurse into iframes, keeping HLS-first ordering
        iframe_hls: list[str] = []
        iframe_video: list[str] = []
        for m in _IFRAME_RE.finditer(text):
            src = m.group(1).strip()
            if not src or src.startswith('javascript:') or src.startswith('about:'):
                continue
            iframe_url = urljoin(url, html.unescape(src))
            if _YOUTUBE_RE.match(iframe_url):
                iframe_hls.append(iframe_url)
                continue
            for c in self._extract_from_page(session, iframe_url, depth + 1):
                if c in hls_candidates or c in video_candidates:
                    continue
                if '.m3u8' in c.lower():
                    iframe_hls.append(c)
                else:
                    iframe_video.append(c)

        return hls_candidates + iframe_hls + video_candidates + iframe_video

    def _extract_brownrice_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        candidates: list[str] = []

        # Brownrice embeds sometimes expose the synthesized stream components
        # directly in page JS.
        for c in self._extract_brownrice_candidates(text):
            if c not in candidates:
                candidates.append(c)

        # Some Brownrice embeds only expose a bootstrap script URL in the page.
        for script_url in self._brownrice_script_urls(text, url):
            try:
                sr = session.get(script_url, timeout=self.TIMEOUT)
                if not sr.ok:
                    continue
                for c in self._extract_brownrice_candidates(sr.text):
                    if c not in candidates:
                        candidates.append(c)
            except Exception as exc:
                logger.debug('[detector] brownrice script fetch failed %s: %s', script_url[:80], exc)

        return candidates

    @staticmethod
    def _extract_brownrice_candidates(text: str) -> list[str]:
        streamurl_m = _BROWNRICE_STREAMURL_RE.search(text)
        name_m = _BROWNRICE_NAME_RE.search(text)
        typeid_m = _BROWNRICE_TYPEID_RE.search(text)
        if not streamurl_m or not name_m:
            return []

        streamurl = streamurl_m.group(1).rstrip('/')
        name = name_m.group(1).strip()
        typeid = (typeid_m.group(1).strip() if typeid_m else '')
        if not streamurl or not name:
            return []

        if typeid == '20':
            suffix = f'/{name}/{name}.stream_360p/playlist.m3u8'
        elif typeid == '5':
            suffix = f'/{name}/{name}.stream_aac/playlist.m3u8'
        else:
            suffix = f'/{name}/{name}.stream/main_playlist.m3u8'
        return [f'{streamurl}{suffix}']

    @staticmethod
    def _brownrice_script_urls(text: str, base_url: str) -> list[str]:
        urls: list[str] = []
        for m in _SCRIPT_SRC_RE.finditer(text):
            src = (m.group(1) or '').strip()
            if not src:
                continue
            abs_url = urljoin(base_url, src)
            if 'brownrice.com/' not in abs_url:
                continue
            if 'sn=' not in abs_url and 'bri_embed' not in text:
                continue
            if abs_url not in urls:
                urls.append(abs_url)
        return urls

    def _extract_player_config_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        candidates: list[str] = []

        # JW Player / Video.js style config often stores media URLs in script
        # blocks as relative or absolute file/src/contentUrl values.
        for block in _SCRIPT_BLOCK_RE.findall(text):
            if not any(token in block.lower() for token in ('jwplayer', 'videojs', 'sources', 'playlist', 'contenturl')):
                continue
            for m in _PLAYER_RELATIVE_MEDIA_RE.finditer(block):
                candidate = urljoin(url, m.group(1).strip())
                if candidate not in candidates:
                    candidates.append(candidate)

        return candidates

    def _extract_json_config_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        candidates: list[str] = []

        for json_url in self._config_json_urls(text, url):
            try:
                r = session.get(json_url, timeout=self.TIMEOUT)
                if not r.ok:
                    continue
                data = r.json()
            except Exception as exc:
                logger.debug('[detector] config json fetch failed %s: %s', json_url[:80], exc)
                continue

            for c in self._media_urls_from_json(data, json_url):
                if c not in candidates:
                    candidates.append(c)

        return candidates

    @staticmethod
    def _config_json_urls(text: str, base_url: str) -> list[str]:
        urls: list[str] = []

        for m in _JSON_URL_RE.finditer(text):
            candidate = m.group(0).strip()
            if candidate not in urls:
                urls.append(candidate)

        for block in _SCRIPT_BLOCK_RE.findall(text):
            if not any(token in block.lower() for token in ('json', 'config', 'playlist', 'source', 'media')):
                continue
            for m in _JSON_REL_RE.finditer(block):
                candidate = urljoin(base_url, m.group(1).strip())
                if candidate not in urls:
                    urls.append(candidate)

        return urls

    @staticmethod
    def _media_urls_from_json(data, base_url: str) -> list[str]:
        candidates: list[str] = []

        def add_candidate(value: str):
            value = (value or '').strip()
            if not value:
                return
            lower = value.lower()
            if not any(ext in lower for ext in ('.m3u8', '.mp4', '.webm')):
                return
            candidate = urljoin(base_url, value)
            if candidate not in candidates:
                candidates.append(candidate)

        def walk(node):
            if isinstance(node, dict):
                for key, value in node.items():
                    if isinstance(value, str) and key.lower() in ('file', 'src', 'contenturl', 'url', 'stream'):
                        add_candidate(value)
                    walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)
            elif isinstance(node, str):
                add_candidate(node)

        walk(data)
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

        if self._is_hls_url(stream_url):
            return self._probe_hls(session, stream_url, page_url, origin, combos)
        return self._probe_direct(session, stream_url, combos)

    def _probe_hls(
        self,
        session: requests.Session,
        stream_url: str,
        page_url: str,
        origin: str,
        combos: list[dict],
    ) -> DetectionResult:
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
                logger.debug('[detector] hls probe error %s: %s', stream_url[:80], exc)
                continue

        return DetectionResult(
            error=f'Stream URL found but no header combination worked: {stream_url[:80]}'
        )

    def _probe_direct(
        self,
        session: requests.Session,
        stream_url: str,
        combos: list[dict],
    ) -> DetectionResult:
        """Probe a non-HLS direct video URL (mp4, webm, etc.)."""
        for headers in combos:
            try:
                # Prefer HEAD to avoid downloading the file
                r = session.head(stream_url, headers=headers, timeout=self.TIMEOUT,
                                 allow_redirects=True)
                if r.status_code == 405:
                    # Server doesn't support HEAD — tiny range GET instead
                    r = session.get(stream_url,
                                    headers={**headers, 'Range': 'bytes=0-1023'},
                                    timeout=self.TIMEOUT, stream=True)
                    r.close()
                if r.status_code not in (200, 206):
                    continue
                ct = r.headers.get('Content-Type', '').lower()
                if not any(ct.startswith(t) for t in _VIDEO_CONTENT_TYPES):
                    continue

                logger.info('[detector] direct video probe OK %s ct=%s', stream_url[:80], ct)
                return DetectionResult(
                    stream_url=stream_url,
                    headers=headers,
                    needs_proxy=False,
                    success=True,
                )
            except Exception as exc:
                logger.debug('[detector] direct probe error %s: %s', stream_url[:80], exc)
                continue

        return DetectionResult(
            error=f'Direct video URL found but not accessible: {stream_url[:80]}'
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

    def _resolve_youtube(self, url: str) -> DetectionResult:
        """
        Extract a playable stream URL from a YouTube page using yt-dlp.
        Tries multiple player clients in order; prefers HLS (m3u8) formats.
        YouTube URLs expire, so callers should set redetect_on_play=True.
        """
        try:
            import yt_dlp
        except ImportError:
            return DetectionResult(
                is_youtube=True,
                error='yt-dlp is not installed — rebuild the container to enable YouTube support',
            )

        last_error = 'Extraction failed'
        for client in _YT_PLAYER_CLIENTS:
            try:
                ydl_opts = {
                    'quiet': True,
                    'no_warnings': True,
                    'skip_download': True,
                    # Prefer combined HLS (covers m3u8, m3u8_native, etc.), fall back to best
                    'format': 'best[protocol~=m3u8]/best',
                    'extractor_args': {
                        'youtube': {
                            'player_client': [client],
                            # Include formats that lack a PO token (needed for server-side extraction)
                            'formats': ['missing_pot'],
                            'skip': ['dash', 'translated_subs'],
                        }
                    },
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)

                if not info:
                    continue

                protocol = info.get('protocol', '')
                # Prefer manifest_url for m3u8 protocols (HLS playlist), else url
                if 'm3u8' in protocol:
                    stream_url = info.get('manifest_url') or info.get('url')
                else:
                    stream_url = info.get('url') or info.get('manifest_url')

                if not stream_url or not stream_url.startswith('http'):
                    continue

                if not self._yt_n_ok(stream_url):
                    logger.debug('[youtube] client=%s n-param unsolved, skipping', client)
                    last_error = 'Extracted URL appears throttled (n parameter too long)'
                    continue

                logger.info('[youtube] resolved via client=%s url=%s…', client, stream_url[:80])
                return DetectionResult(
                    stream_url=stream_url,
                    headers={},
                    needs_proxy=False,
                    success=True,
                    is_youtube=True,
                )
            except Exception as exc:
                last_error = str(exc)
                logger.debug('[youtube] client=%s failed: %s', client, exc)
                continue

        return DetectionResult(is_youtube=True, error=f'YouTube extraction failed: {last_error}')

    @staticmethod
    def _yt_n_ok(url: str) -> bool:
        """Return False if the YouTube n parameter looks unsolved (throttled)."""
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(url).query)
        n_vals = qs.get('n', [])
        if not n_vals:
            return True  # no n param — not a throttled CDN URL
        return len(n_vals[0]) <= 20  # short = solved, long = failed

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

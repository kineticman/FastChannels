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
import base64
import hashlib
import hmac
import json
import logging
import html
import re
import secrets
import time
from functools import lru_cache
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit, urljoin, parse_qs, unquote, quote

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
    r'''(?:src|source|file|url|hls|video|stream)\s*[:=]\s*['"]?(https?://[^\s'"<>()\[\]{}]+?\.(?:m3u8|mp4|webm|mjpg|mjpeg|jpg|jpeg)(?:\?[^\s'"<>()\[\]{}]*)?)''',
    re.IGNORECASE,
)
# Match bare direct-video URLs (.mp4, .webm) — lower priority than HLS
_VIDEO_URL_RE = re.compile(
    r'https?://[^\s"\'<>()\[\]{}]+?\.(?:mp4|webm|mjpg|mjpeg|jpg|jpeg)(?:\?[^\s"\'<>()\[\]{}]*)?',
    re.IGNORECASE,
)
_IFRAME_RE = re.compile(r'''<iframe[^>]+?src\s*=\s*['"]?([^'">\s]+)''', re.IGNORECASE)
_SCRIPT_SRC_RE = re.compile(r'''<script[^>]+?src\s*=\s*['"]?([^'">\s]+)''', re.IGNORECASE)
_SCRIPT_BLOCK_RE = re.compile(r'''<script\b[^>]*>(.*?)</script>''', re.IGNORECASE | re.DOTALL)
_META_VIDEO_RE = re.compile(
    r'''<meta[^>]+(?:property|name)\s*=\s*['"](og:video|twitter:video)['"][^>]+content\s*=\s*['"]([^'"]+)['"]''',
    re.IGNORECASE,
)
_META_PLAYER_RE = re.compile(
    r'''<meta[^>]+(?:property|name)\s*=\s*['"](twitter:player)['"][^>]+content\s*=\s*['"]([^'"]+)['"]''',
    re.IGNORECASE,
)
_JSON_URL_RE = re.compile(r'''https?://[^\s"'<>()\[\]{}]+?\.json(?:\?[^\s"'<>()\[\]{}]*)?''', re.IGNORECASE)
_JSON_REL_RE = re.compile(r'''['"]([^'"]+?\.json(?:\?[^'"]*)?)['"]''', re.IGNORECASE)
_BROWNRICE_STREAMURL_RE = re.compile(r'''camera\[['"]streamurl['"]\]\s*=\s*['"]([^'"]+)['"]''')
_BROWNRICE_NAME_RE = re.compile(r'''camera\[['"]name['"]\]\s*=\s*['"]([^'"]+)['"]''')
_BROWNRICE_TYPEID_RE = re.compile(r'''camera\[['"]typeid['"]\]\s*=\s*['"]([^'"]+)['"]''')
_PLAYER_RELATIVE_MEDIA_RE = re.compile(
    r'''(?:file|src|contentUrl)\s*[:=]\s*['"]([^'"]+?\.(?:m3u8|mp4|webm|mjpg|mjpeg|jpg|jpeg)(?:\?[^'"]*)?)['"]''',
    re.IGNORECASE,
)
_STATE_URL_RE = re.compile(
    r'''(?:["']?(?:streamingUrl|liveStreamingUrl|manifestUrl|masterUrl|hlsUrl|dashUrl|videoUrl|playerUrl|contentUrl|url|src|file|stream)["']?)\s*[:=]\s*['"]([^'"]+?\.(?:m3u8|mp4|webm|mjpg|mjpeg|jpg|jpeg)(?:\?[^'"]*)?)['"]''',
    re.IGNORECASE,
)
_OXBLUE_IFRAME_RE = re.compile(r'https?://app\.oxblue\.com/\?openlink=([^&"\']+)', re.IGNORECASE)
_OXBLUE_OPENLINK_RE = re.compile(r'^https?://app\.oxblue\.com/\?openlink=([^&]+)', re.IGNORECASE)
_STEAM_WATCH_RE = re.compile(r'^https?://steamcommunity\.com/broadcast/watch/(\d+)', re.IGNORECASE)
_STEAM_BROADCASTSINFO_RE = re.compile(r'''data-broadcastsinfo="([^"]+)"''', re.IGNORECASE)
_STEAM_SESSION_RE = re.compile(r'''g_sessionID\s*=\s*"([^"]+)"''', re.IGNORECASE)
_EXPLORE_LIVECAM_RE = re.compile(r'^https?://(www\.)?explore\.org/livecams(?:/|$)', re.IGNORECASE)
_ABCNEWS_LIVE_RE = re.compile(r'^https?://(www\.)?abcnews\.com/(live|Live)(?:[?#].*)?$', re.IGNORECASE)
_CBS_LIVE_STREAM_RE = re.compile(r'^https?://(www\.)?cbs\.com/live-tv/stream/[^/?#]+/?(?:[?#].*)?$', re.IGNORECASE)
_SHOUT_TV_LIVE_RE = re.compile(r'^https?://(www\.)?watch\.shout-tv\.com/live/\d+(?:[?#].*)?$', re.IGNORECASE)
_NBCNEWS_WATCH_RE = re.compile(r'^https?://(www\.)?nbcnews\.com/watch(?:[?#].*)?$', re.IGNORECASE)
_NBCNEWS_CALLLETTERS_RE = re.compile(r'''callLetters":"([^"]+)''', re.IGNORECASE)
_NBCNEWS_PLAYER_CALLLETTERS_RE = re.compile(
    r'''var\s+callletters\s*=\s*decodeURIComponent\(\s*'([^']+)'\s*\)''',
    re.IGNORECASE,
)
_CBS_STREAMING_URL_RE = re.compile(
    r'''"(?:streamingUrl|liveStreamingUrl)"\s*:\s*"?((?:https?:\\?/\\?/)[^"']+)''',
    re.IGNORECASE,
)
_NBCNEWS_FASTCHANNEL_ENTRY_RE = re.compile(
    r'''([A-Za-z0-9_]+):\{streamKey:.*?,hashUrl:"([^"]*)",scheduleKey:"([^"]+)"\}'''
)
_NBCNEWS_PORTABLEPLAYER_RE = re.compile(
    r'''([A-Z0-9_]+):"(https?://[^"]+/portableplayer/[^"]+)"'''
)
_NEWSON_STATION_RE = re.compile(
    r'''^https?://(www\.)?newson\.us/stationDetails/(\d+)(?:[?#].*)?/?$''',
    re.IGNORECASE,
)
_THETVAPP_RE = re.compile(
    r'''^https?://(www\.)?thetvapp\.to/tv/([^/?#]+?)(?:-live-stream)?/?(?:[?#].*)?$''',
    re.IGNORECASE,
)
_THETVAPP_STREAM_NAME_RE = re.compile(
    r'''<div[^>]+id=["']stream_name["'][^>]+name=["']([^"']+)["']''',
    re.IGNORECASE,
)
_GRAY_QUICKPLAY_PLAYER_RE = re.compile(
    r'''<meta[^>]+(?:property|name)\s*=\s*['"](player)['"][^>]+content\s*=\s*['"]quickplay['"]''',
    re.IGNORECASE,
)
_GRAY_QUICKPLAY_FEATURE_RE = re.compile(r'VisualMedia/QuickplayLivePlayer', re.IGNORECASE)
_GRAY_QUICKPLAY_LIVE_CARD_RE = re.compile(
    r'''qp-live-card.*?quickplay\.com/image/([A-Z0-9-]+)/0-16x9\.jpg\?width=250''',
    re.IGNORECASE | re.DOTALL,
)
_OZOLIO_IFRAME_RE = re.compile(
    r'''https?://relay\.ozolio\.com/pub\.api\?[^"'<> ]*cmd=iframe[^"'<> ]*oid=([^&"'<> ]+)''',
    re.IGNORECASE,
)
_OZOLIO_CAMERA_DOC_RE = re.compile(
    r'''camera_doc\s*:\s*["']([^"']+)["']''',
    re.IGNORECASE,
)
_ANTMEDIA_PLAY_RE = re.compile(
    r'^https?://([^/]+)/(?:[^/?#]+)/play\.html(?:[?#].*)?$',
    re.IGNORECASE,
)
_BALTIC_AUTH_TOKEN_RE = re.compile(
    r'''action\s*:\s*['"]auth_token['"][^{}]{0,200}?id\s*:\s*(\d+)''',
    re.IGNORECASE | re.DOTALL,
)
_SKYLINE_SOURCE_RE = re.compile(
    r'''source\s*:\s*['"]([^'"]+?m3u8\?a=[^'"]+)['"]''',
    re.IGNORECASE,
)
_EARTHCAM_VIDEO_EMBED_RE = re.compile(r'''(?:https?:)?//[^'"\s>]+?/js/video/embed\.php\?[^'"\s>]+''', re.IGNORECASE)
_TVPASS_CHANNEL_RE = re.compile(r'^https?://(www\.)?tvpass\.org/channel/([^/?#]+)', re.IGNORECASE)

_YOUTUBE_RE = re.compile(
    r'^https?://(www\.)?(youtube\.com/(watch|live|embed/|@|channel|user|c/|shorts/)|youtu\.be/)',
    re.IGNORECASE,
)
_TWITCH_RE = re.compile(
    r'^https?://(www\.)?(twitch\.tv/(?:videos/|clip/|[^/?#]+)|player\.twitch\.tv/[^?#]+)',
    re.IGNORECASE,
)
_YT_PLAYER_CLIENTS = ('tv_embedded', 'web_safari', 'web', 'ios')

# Content-types accepted for non-HLS direct video probing
_VIDEO_CONTENT_TYPES = ('video/', 'audio/', 'application/octet-stream')
_BLOCKED_STATUS_CODES = {401, 403, 429, 451, 503}
_OXBLUE_APP_ID = 'fc18eb502cb52d060bd93897e21d9491'
_OXBLUE_API_BASE = 'https://api.oxblue.com/v1'


@dataclass
class DetectionResult:
    stream_url: str | None = None
    stream_type: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    needs_proxy: bool = False   # True when segment access also requires headers
    success: bool = False
    error: str | None = None
    is_youtube: bool = False
    resolver: str | None = None


class StreamDetector:
    MAX_IFRAME_DEPTH = 3
    TIMEOUT = 12

    def detect(self, input_url: str) -> DetectionResult:
        self._candidate_resolvers: dict[str, str] = {}
        # Fast path for yt-dlp-native sites we know it can own cleanly.
        if _YOUTUBE_RE.match(input_url):
            return self._resolve_youtube(input_url)
        if _TWITCH_RE.match(input_url):
            return self._resolve_twitch(input_url)
        best_failure: DetectionResult | None = None
        _p = urlsplit(input_url)
        _url_for_extractor_check = urlunsplit((_p.scheme, _p.netloc, _p.path, '', ''))
        if self._yt_dlp_has_dedicated_extractor(_url_for_extractor_check):
            result = self._resolve_yt_dlp_url(input_url)
            if result.success:
                return result
            if result.stream_type or result.error:
                best_failure = result

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
                    return best_failure or DetectionResult(error='No stream URL found on page or in iframes')

            origin = self._origin_of(page_url)

            for candidate in candidates:
                candidate = self._unwrap_stream_wrapper(html.unescape(candidate))
                if _YOUTUBE_RE.match(candidate):
                    result = self._resolve_youtube(candidate)
                    if result.success:
                        return result
                    if result.stream_type or result.error:
                        if self._is_blocked_failure(best_failure) and not self._is_blocked_failure(result):
                            continue
                        if self._is_blocked_failure(result):
                            best_failure = result
                            continue
                        best_failure = result
                    continue
                if _TWITCH_RE.match(candidate):
                    result = self._resolve_twitch(candidate)
                    if result.success:
                        return result
                    if result.stream_type or result.error:
                        if self._is_blocked_failure(best_failure) and not self._is_blocked_failure(result):
                            continue
                        if self._is_blocked_failure(result):
                            best_failure = result
                            continue
                        best_failure = result
                    continue
                result = self._probe(session, candidate, page_url, origin)
                if result.success:
                    if not result.resolver:
                        result.resolver = self._candidate_resolvers.get(candidate) or 'page scrape'
                    return result
                if result.stream_type or result.error:
                    if self._is_blocked_failure(best_failure) and not self._is_blocked_failure(result):
                        continue
                    if self._is_blocked_failure(result):
                        best_failure = result
                        continue
                    best_failure = result

            if best_failure:
                if not best_failure.stream_url and candidates:
                    best_failure.stream_url = self._unwrap_stream_wrapper(html.unescape(candidates[0]))
                return best_failure

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
        return any(ext in path for ext in ('.m3u8', '.mp4', '.webm', '.mpd', '.mjpg', '.mjpeg', '.jpg', '.jpeg')) or path.endswith('.ts')

    @staticmethod
    def _is_hls_url(url: str) -> bool:
        path = urlsplit(url).path.lower()
        return '.m3u8' in path

    @staticmethod
    def infer_stream_type(url: str | None, content_type: str | None = None) -> str | None:
        path = urlsplit(url or '').path.lower()
        ct = (content_type or '').split(';', 1)[0].strip().lower()

        if '.m3u8' in path or 'mpegurl' in ct:
            return 'hls'
        if '.mpd' in path or ct in ('application/dash+xml', 'video/vnd.mpeg.dash.mpd'):
            return 'dash'
        if path.endswith('.ts') or ct in ('video/mp2t', 'video/mp2t;charset=utf-8'):
            return 'mpegts'
        if '.mjpg' in path or '.mjpeg' in path or ct.startswith('multipart/x-mixed-replace'):
            return 'mjpeg'
        if '.jpg' in path or '.jpeg' in path or ct == 'image/jpeg':
            return 'jpeg_snapshot'
        if '.mp4' in path or ct == 'video/mp4':
            return 'mp4'
        if '.webm' in path or ct == 'video/webm':
            return 'webm'
        if '.mov' in path or ct == 'video/quicktime':
            return 'mov'
        if '.mkv' in path or ct == 'video/x-matroska':
            return 'mkv'
        if ct.startswith('video/') or ct.startswith('audio/'):
            return 'direct'
        return None

    @staticmethod
    def _origin_of(url: str) -> str:
        p = urlsplit(url)
        return f'{p.scheme}://{p.netloc}'

    @staticmethod
    def _is_blocked_failure(result: DetectionResult | None) -> bool:
        if not result or result.success:
            return False
        err = (result.error or '').lower()
        return (
            result.error == 'Unauthorized'
            or 'blocked or restricted' in err
            or 'access denied' in err
            or 'auth required' in err
        )

    @staticmethod
    def _unwrap_stream_wrapper(url: str) -> str:
        """
        Some pages expose player wrapper URLs whose query params contain the real
        stream URL. Prefer the nested stream when present.
        """
        try:
            parsed = urlsplit(url)
            params = parse_qs(parsed.query)
        except Exception:
            return url

        for key in ('param', 'src', 'url', 'stream', 'file'):
            values = params.get(key) or []
            for value in values:
                candidate = unquote((value or '').strip())
                if candidate and StreamDetector._is_stream_url(candidate):
                    return candidate
        return url

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
        state_hls, state_video = self._extract_embedded_state_candidates(text, url)
        for c in state_hls:
            if c not in hls_candidates:
                hls_candidates.append(c)
        for c in state_video:
            if c not in video_candidates:
                video_candidates.append(c)

        # Custom provider APIs run before iframe recursion so they can use
        # page-local bootstrap config without brute-forcing every nested document.
        for c in self._extract_custom_api_candidates(session, url, text):
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

    def _extract_custom_api_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        candidates: list[str] = []

        for extractor in (
            self._extract_gray_quickplay_provider_candidates,
            self._extract_abcnews_provider_candidates,
            self._extract_cbs_provider_candidates,
            self._extract_newson_provider_candidates,
            self._extract_thetvapp_provider_candidates,
            self._extract_ozolio_provider_candidates,
            self._extract_antmedia_provider_candidates,
            self._extract_balticlivecam_provider_candidates,
            self._extract_skyline_provider_candidates,
            self._extract_shouttv_provider_candidates,
            self._extract_explore_provider_candidates,
            self._extract_nbcnews_provider_candidates,
            self._extract_tvpass_provider_candidates,
            self._extract_twitter_player_candidates,
            lambda _session, _url, page_text: self._extract_meta_video_candidates(page_text, url),
            self._extract_brownrice_provider_candidates,
            self._extract_oxblue_provider_candidates,
            self._extract_steam_provider_candidates,
            self._extract_player_config_candidates,
            self._extract_json_config_candidates,
        ):
            for c in extractor(session, url, text):
                if c not in candidates:
                    candidates.append(c)
                    self._candidate_resolvers.setdefault(c, 'custom api')

        return candidates

    def _extract_gray_quickplay_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        parsed = urlsplit(url)
        host = parsed.netloc.lower()
        if 'gray' not in host and not (
            _GRAY_QUICKPLAY_PLAYER_RE.search(text)
            or _GRAY_QUICKPLAY_FEATURE_RE.search(text)
        ):
            return []

        if not parsed.scheme or not parsed.netloc:
            return []

        site_parts = [part for part in host.split('.') if part and part != 'www']
        site = site_parts[0] if site_parts else ''
        if not site:
            return []

        live_match = _GRAY_QUICKPLAY_LIVE_CARD_RE.search(text)
        if not live_match:
            return []

        content_id = (live_match.group(1) or '').strip()
        if not content_id:
            return []

        try:
            api = requests.Session()
            api.headers.update({'User-Agent': _BROWSER_UA})
            access_resp = api.get(
                urljoin(f'{parsed.scheme}://{parsed.netloc}', '/pf/api/v3/content/fetch/quickplay-platform-auth-iam'),
                params={'query': '{}'},
                timeout=self.TIMEOUT,
            )
            if not access_resp.ok:
                return []
            access_data = access_resp.json() or {}
            access_token = (access_data.get('access_token') or '').strip()
            if not access_token:
                return []

            device_id = str(secrets.token_hex(16))
            auth_resp = api.post(
                'https://auth-gw.api.gray.quickplay.com/platform/access/token',
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json',
                    'Referer': f'{parsed.scheme}://{parsed.netloc}/',
                    'X-Client-Id': 'gray-gm-web',
                    'User-Agent': _BROWSER_UA,
                },
                json={'deviceId': device_id},
                timeout=self.TIMEOUT,
            )
            if not auth_resp.ok:
                return []
            auth_data = auth_resp.json() or {}
            auth_token = ((auth_data.get('data') or {}).get('token') or '').strip()
            if not auth_token:
                return []

            reg_resp = api.post(
                'https://device-register-service.api.gray.quickplay.com/device/app/register',
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json',
                    'Referer': f'{parsed.scheme}://{parsed.netloc}/',
                    'X-Authorization': auth_token,
                    'X-Client-Id': 'gray-gm-web',
                    'User-Agent': _BROWSER_UA,
                },
                json={'uniqueId': device_id},
                timeout=self.TIMEOUT,
            )
            if not reg_resp.ok:
                return []
            reg_data = reg_resp.json() or {}
            reg_payload = reg_data.get('data') or {}
            secret_b64 = (reg_payload.get('secret') or '').strip()
            if not secret_b64:
                return []

            try:
                device_secret = base64.b64decode(secret_b64)
            except Exception:
                device_secret = secret_b64.encode('utf-8')
            now = int(time.time())
            device_jwt = self._gray_quickplay_jwt(
                {
                    'deviceId': device_id,
                    'aud': 'playback-auth-service',
                    'iat': now,
                    'exp': now + 86400 * 100,
                },
                device_secret,
            )

            play_resp = api.post(
                'https://playback-auth-service.api.gray.quickplay.com/media/content/authorize',
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json',
                    'Referer': f'{parsed.scheme}://{parsed.netloc}/',
                    'X-Authorization': auth_token,
                    'X-Client-Id': 'gray-gm-web',
                    'X-Device-Id': device_jwt,
                    'X-Property-Id': f'gm|{site}|{site}',
                    'User-Agent': _BROWSER_UA,
                },
                json={
                    'deviceName': 'web',
                    'deviceId': device_id,
                    'contentId': content_id,
                    'contentTypeId': 'live',
                    'catalogType': 'channel',
                    'mediaFormat': 'hls',
                    'drm': 'none',
                    'delivery': 'streaming',
                    'disableSsai': 'false',
                    'urlParameters': {
                        'ads.npa': '1',
                        'ads.url': url,
                        'ads.vpos': 'midroll',
                    },
                    'playbackMode': 'live',
                    'quality': 'medium',
                    'supportedResolution': 'FHD',
                },
                timeout=self.TIMEOUT,
            )
            if not play_resp.ok:
                return []

            play_data = play_resp.json() or {}
            content_url = ((play_data.get('data') or {}).get('contentUrl') or '').strip()
            if content_url:
                self._candidate_resolvers.setdefault(content_url, 'gray quickplay')
                return [content_url]
        except Exception as exc:
            logger.debug('[detector] gray quickplay lookup failed %s: %s', url[:80], exc)
            return []

        return []

    def _extract_thetvapp_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        match = _THETVAPP_RE.match(url)
        if not match:
            return []

        stream_name = self._thetvapp_stream_name(text)
        if not stream_name:
            return []

        # Fast path: the session already fetched the page (server-set cookies in jar).
        # The /token/ endpoint often works without JS-set auth cookies.
        stream_url = self._thetvapp_fetch_token(session, url, stream_name)
        if stream_url:
            self._candidate_resolvers.setdefault(stream_url, 'thetvapp')
            return [stream_url]

        # Slow path: JS on the page sets auth cookies before /token/ will respond.
        # Use Playwright and wait for network activity to settle instead of a fixed sleep.
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            logger.debug('[detector] playwright unavailable for thetvapp fallback: %s', exc)
            return []

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, wait_until='networkidle', timeout=30000)
                result = page.evaluate(
                    """async (name) => {
                      const resp = await fetch('/token/' + name, { credentials: 'same-origin' });
                      const text = await resp.text();
                      return { ok: resp.ok, status: resp.status, text };
                    }""",
                    stream_name,
                )
                browser.close()

            if not result or not result.get('ok'):
                return []

            try:
                data = json.loads(result.get('text') or '{}')
            except Exception:
                return []

            stream_url = (data.get('url') or '').strip()
            if stream_url:
                self._candidate_resolvers.setdefault(stream_url, 'thetvapp')
                return [stream_url]
        except Exception as exc:
            logger.debug('[detector] thetvapp lookup failed %s: %s', url[:80], exc)
            return []

        return []

    @staticmethod
    def _thetvapp_fetch_token(session: requests.Session, url: str, stream_name: str) -> str | None:
        """Call the /token/ endpoint directly using the existing requests session."""
        try:
            parsed = urlsplit(url)
            token_url = f'{parsed.scheme}://{parsed.netloc}/token/{stream_name}'
            resp = session.get(token_url, timeout=10)
            if not resp.ok:
                return None
            data = resp.json()
            return (data.get('url') or '').strip() or None
        except Exception:
            return None

    def _extract_cbs_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        if not _CBS_LIVE_STREAM_RE.match(url):
            return []

        candidates: list[str] = []
        body = html.unescape(text).replace('\\/', '/')
        for m in _CBS_STREAMING_URL_RE.finditer(body):
            candidate = html.unescape((m.group(1) or '').strip()).replace('\\/', '/')
            if candidate.startswith('http') and candidate not in candidates:
                candidates.append(candidate)
        return candidates

    @staticmethod
    def _gray_quickplay_jwt(payload: dict, secret: bytes) -> str:
        header = {'alg': 'HS256', 'typ': 'JWT'}
        header_b64 = base64.urlsafe_b64encode(
            json.dumps(header, separators=(',', ':'), sort_keys=True).encode('utf-8')
        ).rstrip(b'=')
        payload_b64 = base64.urlsafe_b64encode(
            json.dumps(payload, separators=(',', ':'), sort_keys=True).encode('utf-8')
        ).rstrip(b'=')
        signing_input = b'.'.join((header_b64, payload_b64))
        signature = hmac.new(secret, signing_input, hashlib.sha256).digest()
        signature_b64 = base64.urlsafe_b64encode(signature).rstrip(b'=')
        return b'.'.join((header_b64, payload_b64, signature_b64)).decode('ascii')

    @staticmethod
    def _thetvapp_stream_name(text: str) -> str | None:
        match = _THETVAPP_STREAM_NAME_RE.search(text)
        if not match:
            return None
        value = html.unescape((match.group(1) or '').strip())
        return value or None

    def _extract_newson_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        match = _NEWSON_STATION_RE.match(url)
        if not match:
            return []

        station_id = (match.group(2) or '').strip()
        if not station_id:
            return []

        try:
            api = requests.Session()
            api.headers.update({'User-Agent': _BROWSER_UA})
            detail_resp = api.get(
                f'https://newson-api.triple-it.nl/v5api/detail/station/{station_id}',
                params={'platformType': 'website'},
                timeout=self.TIMEOUT,
            )
            if not detail_resp.ok:
                return []

            detail = detail_resp.json() or {}
            item = detail.get('item') or {}
            playables = item.get('playables') or {}

            ordered_playables = [
                playables.get('live'),
                playables.get('alwayson'),
                playables.get('vod'),
            ]
            candidates: list[str] = []
            for playable in ordered_playables:
                if not isinstance(playable, dict):
                    continue
                playable_id = (playable.get('id') or '').strip()
                playable_type = (playable.get('videoType') or '').strip()
                if not playable_id or not playable_type:
                    continue

                item_resp = api.get(
                    f'https://newson-api.triple-it.nl/v5api/item/{playable_type}/{playable_id}',
                    params={'platformType': 'website'},
                    timeout=self.TIMEOUT,
                )
                if not item_resp.ok:
                    continue

                item_data = item_resp.json() or {}
                sources = item_data.get('sources') or []
                if not sources:
                    continue

                source = (sources[0].get('file') or '').strip()
                if source and source not in candidates:
                    candidates.append(source)

            return candidates
        except Exception as exc:
            logger.debug('[detector] newson lookup failed %s: %s', station_id, exc)
            return []

    def _extract_ozolio_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        oid = self._ozolio_camera_oid(url, text)
        if not oid:
            return []

        try:
            api = requests.Session()
            api.headers.update({'User-Agent': _BROWSER_UA})
            init = api.get(
                'https://relay.ozolio.com/ses.api',
                params={
                    'cmd': 'init',
                    'oid': oid,
                    'ver': '5',
                    'channel': '0',
                    'control': '0',
                    'document': url,
                },
                timeout=self.TIMEOUT,
            )
            if init.status_code != 200:
                return []
            init_data = init.json() or {}
            session_data = init_data.get('session') or {}
            session_id = (session_data.get('id') or '').strip()
            outputs = init_data.get('outputs') or []
            if not session_id or not outputs:
                return []

            output = next(
                (
                    item for item in outputs
                    if str(item.get('media') or '').upper() == 'LIVE'
                    and str(item.get('type') or '').lower() != 'preroll'
                ),
                outputs[0],
            )
            output_id = (output.get('id') or '').strip()
            formats = [f.strip().upper() for f in (output.get('formats') or '').split(';') if f.strip()]
            output_format = next(
                (fmt for fmt in ('M3U8', 'MJPEG', 'IMAGE', 'VAST') if fmt in formats),
                formats[0] if formats else 'M3U8',
            )
            if not output_id:
                return []

            open_resp = api.get(
                'https://relay.ozolio.com/ses.api',
                params={
                    'cmd': 'open',
                    'oid': session_id,
                    'output': output_id,
                    'format': output_format,
                    'profile': 'AUTO',
                },
                timeout=self.TIMEOUT,
            )
            if open_resp.status_code != 200:
                return []
            open_data = open_resp.json() or {}
            source = ((open_data.get('output') or {}).get('source') or '').strip()
            if source:
                return [source]
        except Exception as exc:
            logger.debug('[detector] ozolio lookup failed %s: %s', oid, exc)
            return []

        return []

    def _extract_antmedia_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        parsed = urlsplit(url)
        if 'antmedia.cloud' not in parsed.netloc.lower():
            return []

        params = parse_qs(parsed.query)
        stream_id = (params.get('id') or [''])[0].strip()
        play_order = (params.get('playOrder') or [''])[0].strip().lower()
        if not stream_id or 'hls' not in play_order:
            return []

        parts = [p for p in parsed.path.split('/') if p]
        if not parts:
            return []

        app = parts[0]
        manifest = f'{parsed.scheme}://{parsed.netloc}/{app}/streams/{stream_id}.m3u8'
        return [manifest]

    def _extract_balticlivecam_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        parsed = urlsplit(url)
        if not parsed.netloc.lower().endswith('balticlivecam.com'):
            return []

        match = _BALTIC_AUTH_TOKEN_RE.search(text)
        if not match:
            return []

        camera_id = (match.group(1) or '').strip()
        if not camera_id:
            return []

        try:
            ajax = requests.Session()
            ajax.headers.update({
                'User-Agent': _BROWSER_UA,
                'Referer': url,
                'X-Requested-With': 'XMLHttpRequest',
            })
            resp = ajax.post(
                urljoin(url, '/wp-admin/admin-ajax.php'),
                data={
                    'action': 'auth_token',
                    'id': camera_id,
                    'embed': '0',
                    'main_referer': url,
                },
                timeout=self.TIMEOUT,
            )
            if not resp.ok:
                return []

            body = html.unescape(resp.text)
            m = _M3U8_RE.search(body)
            if m:
                return [m.group(0).rstrip('"\'\\')]
        except Exception as exc:
            logger.debug('[detector] balticlivecam lookup failed %s: %s', camera_id, exc)
            return []

        return []

    def _extract_skyline_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        if 'skylinewebcams.com' not in urlsplit(url).netloc.lower():
            return []

        m = _SKYLINE_SOURCE_RE.search(text)
        if not m:
            return []

        source = html.unescape((m.group(1) or '').strip())
        if not source:
            return []

        source = source.replace('livee.', 'live.')
        if source.startswith('http'):
            return [source]

        return [f'https://hd-auth.skylinewebcams.com/{source.lstrip("/")}']

    @staticmethod
    def _ozolio_camera_oid(url: str, text: str) -> str | None:
        parsed = urlsplit(url)
        if parsed.netloc.lower().endswith('ozolio.com'):
            params = parse_qs(parsed.query)
            for key in ('oid', 'camera_doc', 'cameraDoc'):
                values = params.get(key) or []
                for value in values:
                    v = unquote((value or '').strip())
                    if v:
                        return v

        match = _OZOLIO_IFRAME_RE.search(text)
        if match:
            return html.unescape(match.group(1)).strip() or None

        match = _OZOLIO_CAMERA_DOC_RE.search(text)
        if match:
            return html.unescape(match.group(1)).strip() or None

        return None

    def _extract_abcnews_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        url_l = url.lower()
        if not (
            _ABCNEWS_LIVE_RE.match(url)
            or '/live/video/special-live-' in url_l
            or 'abcnews.com/live' in url_l
        ):
            return []

        candidates: list[str] = []
        playwright_url = self._abcnews_playwright_manifest_url(url)
        if playwright_url:
            candidates.append(playwright_url)

        return candidates

    def _extract_nbcnews_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        url_l = url.lower()
        if not (
            _NBCNEWS_WATCH_RE.match(url)
            or '/video-layout/amp_video/' in url_l
            or '/portableplayer/' in url_l
        ):
            return []

        candidates: list[str] = []
        if _NBCNEWS_WATCH_RE.match(url):
            hash_fragment = (urlsplit(url).fragment or '').strip()
            if hash_fragment:
                schedule_key = self._nbc_schedule_key_for_hash(session, url, text, hash_fragment)
                if schedule_key:
                    portableplayer_url = self._nbc_portableplayer_url_for_schedule(session, url, text, schedule_key)
                    if portableplayer_url:
                        layout_url = self._nbc_video_layout_url(session, portableplayer_url, url)
                        if layout_url:
                            candidates.extend(self._nbc_media_candidates_from_layout(session, layout_url))

                # The local NBC fast-channel pages are assembled client-side and can
                # still need one browser pass to resolve the fully interpolated HLS
                # URL. Use Playwright only as a last-mile fallback.
                playwright_url = self._nbc_playwright_manifest_url(url)
                if playwright_url and playwright_url not in candidates:
                    candidates.insert(0, playwright_url)

        if '/video-layout/amp_video/' in url_l:
            candidates.extend(self._nbc_media_candidates_from_layout(session, url))

        return candidates

    def _abcnews_playwright_manifest_url(self, live_url: str) -> str | None:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            logger.debug('[detector] playwright unavailable for ABC News fallback: %s', exc)
            return None

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                seen: list[str] = []

                def on_request(req):
                    req_url = req.url
                    parsed = urlsplit(req_url)
                    if '.m3u8' not in parsed.path.lower():
                        return
                    if 'linear-abcnews' not in parsed.netloc.lower() and 'media.dssott.com' not in parsed.netloc.lower():
                        return
                    seen.append(req_url)

                page.on('request', on_request)
                page.goto(live_url, wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(10000)
                browser.close()

            for candidate in reversed(seen):
                parsed = urlsplit(candidate)
                if '.m3u8' not in parsed.path.lower():
                    continue
                if 'media.dssott.com' not in parsed.netloc.lower():
                    continue
                return candidate
        except Exception as exc:
            logger.debug('[detector] playwright ABC News fallback failed %s: %s', live_url[:80], exc)
            return None

        return None

    def _nbc_media_candidates_from_layout(
        self,
        session: requests.Session,
        layout_url: str,
    ) -> list[str]:
        try:
            r = session.get(layout_url, timeout=self.TIMEOUT)
            if not r.ok:
                return []
        except Exception as exc:
            logger.debug('[detector] nbcnews layout fetch failed %s: %s', layout_url[:80], exc)
            return []

        body = html.unescape(r.text).replace('\\/', '/')
        hls_candidates, video_candidates = self._extract_generic_candidates(body)
        return hls_candidates + video_candidates

    def _nbc_schedule_key_for_hash(
        self,
        session: requests.Session,
        page_url: str,
        text: str,
        hash_fragment: str,
    ) -> str | None:
        hash_url = hash_fragment if hash_fragment.startswith('#') else f'#{hash_fragment}'
        for script_url in self._page_script_urls(text, page_url):
            try:
                r = session.get(script_url, timeout=self.TIMEOUT)
                if not r.ok:
                    continue
            except Exception as exc:
                logger.debug('[detector] nbcnews fastchannel fetch failed %s: %s', script_url[:80], exc)
                continue

            for m in _NBCNEWS_FASTCHANNEL_ENTRY_RE.finditer(r.text):
                if (m.group(2) or '').strip() == hash_url:
                    return (m.group(3) or '').strip() or None
        return None

    def _nbc_portableplayer_url_for_schedule(
        self,
        session: requests.Session,
        page_url: str,
        text: str,
        schedule_key: str,
    ) -> str | None:
        for script_url in self._page_script_urls(text, page_url):
            try:
                r = session.get(script_url, timeout=self.TIMEOUT)
                if not r.ok:
                    continue
            except Exception as exc:
                logger.debug('[detector] nbcnews player map fetch failed %s: %s', script_url[:80], exc)
                continue

            if 'portableplayer' not in r.text or schedule_key not in r.text:
                continue

            m = re.search(
                rf'{re.escape(schedule_key)}:"(https?://[^"]+/portableplayer/[^"]+)"',
                r.text,
            )
            if m:
                return html.unescape(m.group(1)).strip()

        return None

    @staticmethod
    def _nbc_callletters(text: str) -> str | None:
        match = _NBCNEWS_CALLLETTERS_RE.search(text)
        if not match:
            return None
        value = (match.group(1) or '').strip()
        return value or None

    def _nbc_video_layout_url(
        self,
        session: requests.Session,
        portableplayer_url: str,
        page_url: str,
    ) -> str | None:
        parsed_player = urlsplit(portableplayer_url)
        if not parsed_player.scheme or not parsed_player.netloc:
            return None

        query = (parsed_player.query or '').replace('CID=', 'noid=').replace('cmsID=', 'noid=')
        if not query:
            return None

        callletters = self._nbc_callletters_from_player(session, portableplayer_url)
        callletters = (callletters or '').lower()
        if not callletters:
            return None

        watch_url = page_url.split('#', 1)[0]
        watch_origin = self._origin_of(watch_url)
        random_token = secrets.token_hex(4)
        return (
            f'{parsed_player.scheme}://{parsed_player.netloc}/video-layout/amp_video/?'
            f'{query}'
            f'&turl={quote(watch_url, safe="")}'
            f'&ourl={quote(watch_origin, safe="")}'
            f'&lp=5&fullWidth=y'
            f'&random={random_token}'
            f'&callletters={quote(callletters, safe="")}'
            f'&embedded=true&autoplay=true'
        )

    def _nbc_callletters_from_player(
        self,
        session: requests.Session,
        portableplayer_url: str,
    ) -> str | None:
        try:
            r = session.get(portableplayer_url, timeout=self.TIMEOUT)
            if not r.ok:
                return None
        except Exception as exc:
            logger.debug('[detector] nbcnews player fetch failed %s: %s', portableplayer_url[:80], exc)
            return None

        match = _NBCNEWS_PLAYER_CALLLETTERS_RE.search(r.text)
        if match:
            value = html.unescape((match.group(1) or '').strip())
            return value or None

        return None

    def _nbc_playwright_manifest_url(self, watch_url: str) -> str | None:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            logger.debug('[detector] playwright unavailable for NBC fallback: %s', exc)
            return None

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                seen: list[str] = []

                page.on(
                    'request',
                    lambda req: seen.append(req.url)
                    if ('m3u8' in req.url or 'cloudfront' in req.url or 'freewheel' in req.url.lower())
                    else None,
                )
                page.goto(watch_url, wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(8000)
                browser.close()

            for candidate in reversed(seen):
                parsed = urlsplit(candidate)
                if '.m3u8' not in parsed.path.lower():
                    continue
                if 'scorecardresearch.com' in parsed.netloc.lower():
                    continue
                if 'cloudfront' not in parsed.netloc.lower() and 'freewheel' not in parsed.netloc.lower():
                    continue
                return candidate
        except Exception as exc:
            logger.debug('[detector] playwright NBC fallback failed %s: %s', watch_url[:80], exc)
            return None

        return None

    @staticmethod
    def _page_script_urls(text: str, base_url: str) -> list[str]:
        urls: list[str] = []
        for m in _SCRIPT_SRC_RE.finditer(text):
            src = html.unescape((m.group(1) or '').strip())
            if not src:
                continue
            abs_url = urljoin(base_url, src)
            if abs_url not in urls:
                urls.append(abs_url)
        return urls

    def _extract_explore_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        if not _EXPLORE_LIVECAM_RE.match(url):
            return []

        api = requests.Session()
        api.headers.update({'User-Agent': _BROWSER_UA})
        try:
            r = api.get(
                'https://omega.explore.org/api/initial',
                params={'contenttype': 'livecams', 'pageType': 'livecams'},
                timeout=self.TIMEOUT,
            )
            if r.status_code != 200:
                return []
            data = r.json() or {}
            livecam = (data.get('data') or {}).get('default_livecam') or {}
            video_id = (livecam.get('video_id') or '').strip()
            if not video_id:
                return []
        except Exception as exc:
            logger.debug('[detector] explore lookup failed %s: %s', url[:80], exc)
            return []

        candidates: list[str] = []
        for candidate in (
            f'https://www.youtube.com/live/{video_id}',
            f'https://www.youtube.com/watch?v={video_id}',
            f'https://www.youtube.com/embed/{video_id}',
        ):
            if candidate not in candidates:
                candidates.append(candidate)
        return candidates

    def _extract_tvpass_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        match = _TVPASS_CHANNEL_RE.match(url)
        if not match:
            return []

        slug = unquote(match.group(2)).strip()
        if not slug:
            return []

        page_session = requests.Session()
        page_session.headers.update({'User-Agent': _BROWSER_UA})
        try:
            page_resp = page_session.get(url, timeout=self.TIMEOUT)
            stream_name = self._tvpass_stream_name(page_resp.text) or slug
            token = page_session.get(
                f'https://tvpass.org/token/{stream_name}',
                headers={
                    'Referer': url,
                    'Origin': 'https://tvpass.org',
                    'User-Agent': _BROWSER_UA,
                },
                timeout=self.TIMEOUT,
            )
            if token.status_code != 200:
                return []
            payload = token.json() or {}
            stream_url = (payload.get('url') or '').strip()
            if not stream_url:
                return []
        except Exception as exc:
            logger.debug('[detector] tvpass lookup failed %s: %s', slug, exc)
            return []

        return [stream_url]

    @staticmethod
    def _tvpass_stream_name(text: str) -> str | None:
        match = re.search(r'''id=["']stream_name["'][^>]+name=["']([^"']+)["']''', text, re.IGNORECASE)
        if match:
            value = html.unescape(match.group(1)).strip()
            if value:
                return value
        return None

    def _extract_twitter_player_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        candidates: list[str] = []

        for m in _META_PLAYER_RE.finditer(text):
            player_url = urljoin(url, html.unescape((m.group(2) or '').strip()))
            if not player_url:
                continue
            try:
                r = session.get(player_url, timeout=self.TIMEOUT)
                if not r.ok:
                    continue
            except Exception as exc:
                logger.debug('[detector] twitter player fetch failed %s: %s', player_url[:80], exc)
                continue

            player_scripts = [
                urljoin(player_url, html.unescape((sm.group(1) or '').strip()))
                for sm in _SCRIPT_SRC_RE.finditer(r.text)
                if (sm.group(1) or '').strip()
            ]

            for script_url in player_scripts:
                try:
                    sr = session.get(script_url, timeout=self.TIMEOUT)
                    if not sr.ok:
                        continue
                except Exception as exc:
                    logger.debug('[detector] twitter player script fetch failed %s: %s', script_url[:80], exc)
                    continue

                script_text = sr.text.replace('\\/', '/')
                final_embed_urls = _EARTHCAM_VIDEO_EMBED_RE.findall(script_text)
                for final_embed_url in final_embed_urls:
                    final_url = urljoin(script_url, html.unescape(final_embed_url))
                    try:
                        fr = session.get(final_url, timeout=self.TIMEOUT)
                        if not fr.ok:
                            continue
                    except Exception as exc:
                        logger.debug('[detector] earthcam final embed fetch failed %s: %s', final_url[:80], exc)
                        continue

                    final_text = fr.text.replace('\\/', '/')
                    for c in self._extract_generic_candidates(final_text)[0] + self._extract_generic_candidates(final_text)[1]:
                        if c not in candidates:
                            candidates.append(c)
                    for c in self._extract_custom_api_candidates(session, final_url, final_text):
                        if c not in candidates:
                            candidates.append(c)

        return candidates

    @staticmethod
    def _extract_meta_video_candidates(text: str, base_url: str) -> list[str]:
        candidates: list[str] = []

        for m in _META_VIDEO_RE.finditer(text):
            candidate = urljoin(base_url, html.unescape((m.group(2) or '').strip()))
            if not candidate:
                continue
            if not (StreamDetector._is_stream_url(candidate) or _YOUTUBE_RE.match(candidate)):
                continue
            if candidate not in candidates:
                candidates.append(candidate)

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

    def _extract_oxblue_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        openlink = self._oxblue_openlink(url, text)
        if not openlink:
            return []

        try:
            api = requests.Session()
            api.headers.update({
                'User-Agent': _BROWSER_UA,
                'X-APP-ID': _OXBLUE_APP_ID,
            })
            auth = api.post(
                f'{_OXBLUE_API_BASE}/openlink-sessions',
                json={'openLink': openlink},
                timeout=self.TIMEOUT,
            )
            if auth.status_code not in (200, 201):
                return []
            session_id = (auth.json() or {}).get('sessionID')
            if not session_id:
                return []

            api.headers['Authorization'] = f'Bearer {session_id}'
            cameras = api.get(f'{_OXBLUE_API_BASE}/cameras', timeout=self.TIMEOUT)
            if cameras.status_code != 200:
                return []
            camera_list = (cameras.json() or {}).get('cameras') or []
        except Exception as exc:
            logger.debug('[detector] oxblue lookup failed %s: %s', openlink, exc)
            return []

        candidates: list[str] = []
        for camera in camera_list:
            # OxBlue open links sometimes present a "live" view that is really a
            # public recorded MP4 fallback; prefer the URL the player itself uses.
            use_rec_video = camera.get('useRecVideo')
            if use_rec_video in (True, 1, '1', 'true', 'True'):
                candidate = (camera.get('videoPathMP4') or '').strip()
                if candidate and candidate not in candidates:
                    candidates.append(candidate)
                continue

            cam_id = (camera.get('id') or '').strip()
            if cam_id:
                hls_url = f'https://livestream.oxblue.com/hls/OxStreamer/{cam_id}/index.m3u8'
                if hls_url not in candidates:
                    candidates.append(hls_url)

        return candidates

    @staticmethod
    def _oxblue_openlink(url: str, text: str) -> str | None:
        parsed = urlsplit(url)
        if parsed.netloc.lower() == 'app.oxblue.com':
            params = parse_qs(parsed.query)
            values = params.get('openlink') or params.get('openLink') or []
            if values:
                return unquote(values[0]).strip()

        match = _OXBLUE_IFRAME_RE.search(text)
        if match:
            return unquote(match.group(1)).strip()
        return None

    def _extract_steam_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        steamid = self._steam_broadcast_steamid(url, text)
        session_id = self._steam_page_session_id(text)
        if not steamid or not session_id:
            return []

        try:
            info = session.get(
                'https://steamcommunity.com/broadcast/getbroadcastinfo/',
                params={'steamid': steamid, 'broadcastid': 0},
                timeout=self.TIMEOUT,
            )
            if info.status_code != 200:
                return []
            info_data = info.json() or {}
            if not info_data.get('is_online'):
                return []

            mpd = session.get(
                'https://steamcommunity.com/broadcast/getbroadcastmpd/',
                params={
                    'steamid': steamid,
                    'broadcastid': 0,
                    'viewertoken': 0,
                    'sessionid': session_id,
                    'watchlocation': 5,
                },
                timeout=self.TIMEOUT,
            )
            if mpd.status_code != 200:
                return []
            mpd_data = mpd.json() or {}
        except Exception as exc:
            logger.debug('[detector] steam broadcast lookup failed %s: %s', steamid, exc)
            return []

        candidates: list[str] = []
        for key in ('hls_url', 'url'):
            candidate = (mpd_data.get(key) or '').strip()
            if candidate and candidate not in candidates:
                candidates.append(candidate)
        return candidates

    @staticmethod
    def _steam_broadcast_steamid(url: str, text: str) -> str | None:
        match = _STEAM_WATCH_RE.match(url)
        if match:
            return match.group(1)

        info_match = _STEAM_BROADCASTSINFO_RE.search(text)
        if not info_match:
            return None
        try:
            raw = html.unescape(info_match.group(1))
            data = json.loads(raw)
        except Exception:
            return None
        steamid = (data.get('steamid') or '').strip()
        return steamid or None

    @staticmethod
    def _steam_page_session_id(text: str) -> str | None:
        match = _STEAM_SESSION_RE.search(text)
        if not match:
            return None
        value = (match.group(1) or '').strip()
        return value or None

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

    def _extract_embedded_state_candidates(
        self,
        text: str,
        base_url: str,
    ) -> tuple[list[str], list[str]]:
        hls_candidates: list[str] = []
        video_candidates: list[str] = []

        blocks = list(_SCRIPT_BLOCK_RE.findall(text))
        if not blocks:
            blocks = [text]

        for raw_block in blocks:
            block = html.unescape(raw_block).replace('\\/', '/')

            # Common page-state keys like streamingUrl/liveStreamingUrl often
            # appear inside inline JS blobs rather than as stand-alone URLs.
            for m in _STATE_URL_RE.finditer(block):
                candidate = urljoin(base_url, html.unescape((m.group(1) or '').strip()))
                if not candidate:
                    continue
                if not self._is_stream_url(candidate):
                    continue
                stream_type = self.infer_stream_type(candidate)
                if stream_type == 'hls':
                    if candidate not in hls_candidates:
                        hls_candidates.append(candidate)
                else:
                    if candidate not in video_candidates:
                        video_candidates.append(candidate)

            # If a block looks like pure JSON, recurse through it directly.
            stripped = block.strip()
            if stripped.startswith('{') or stripped.startswith('['):
                try:
                    data = json.loads(stripped)
                except Exception:
                    continue
                for candidate in self._media_urls_from_json(data, base_url):
                    stream_type = self.infer_stream_type(candidate)
                    if stream_type == 'hls':
                        if candidate not in hls_candidates:
                            hls_candidates.append(candidate)
                    else:
                        if candidate not in video_candidates:
                            video_candidates.append(candidate)

        return hls_candidates, video_candidates

    def _extract_shouttv_provider_candidates(
        self,
        session: requests.Session,
        url: str,
        text: str,
    ) -> list[str]:
        if not _SHOUT_TV_LIVE_RE.match(url):
            return []

        candidates: list[str] = []
        playwright_url = self._shouttv_playwright_hls_url(url)
        if playwright_url:
            candidates.append(playwright_url)
        return candidates

    def _shouttv_playwright_hls_url(self, live_url: str) -> str | None:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            logger.debug('[detector] playwright unavailable for Shout TV fallback: %s', exc)
            return None

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                seen: list[str] = []
                hls_json: list[str] = []

                def on_response(resp):
                    if 'dge-streaming.imggaming.com/api/v3/streaming/events/' not in resp.url:
                        return
                    try:
                        data = resp.json()
                    except Exception:
                        return
                    for key in ('hlsUrl',):
                        candidate = (data.get(key) or '').strip()
                        if candidate and candidate not in hls_json:
                            hls_json.append(candidate)

                page.on('response', on_response)
                page.goto(live_url, wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(12000)
                browser.close()

                if hls_json:
                    return hls_json[0]
        except Exception as exc:
            logger.debug('[detector] playwright Shout TV fallback failed %s: %s', live_url[:80], exc)
            return None

        return None

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
            if not any(ext in lower for ext in ('.m3u8', '.mp4', '.webm', '.mjpg', '.mjpeg', '.jpg', '.jpeg')):
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
        seen_statuses: set[int] = set()
        for headers in combos:
            try:
                r = session.get(stream_url, headers=headers, timeout=self.TIMEOUT)
                if r.status_code != 200:
                    seen_statuses.add(r.status_code)
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
                    stream_type='hls',
                    headers=headers,
                    needs_proxy=needs_proxy,
                    success=True,
                    resolver=self._candidate_resolvers.get(stream_url) or 'page scrape',
                )
            except Exception as exc:
                logger.debug('[detector] hls probe error %s: %s', stream_url[:80], exc)
                continue

        if 401 in seen_statuses:
            return DetectionResult(
                stream_url=stream_url,
                stream_type='hls',
                error='Unauthorized',
            )
        if seen_statuses & _BLOCKED_STATUS_CODES:
            return DetectionResult(
                stream_url=stream_url,
                stream_type='hls',
                error='Blocked or restricted',
            )
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
        seen_statuses: set[int] = set()
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
                    seen_statuses.add(r.status_code)
                    continue
                ct = r.headers.get('Content-Type', '').lower()
                stream_type = self.infer_stream_type(r.url or stream_url, ct)
                if not any(ct.startswith(t) for t in _VIDEO_CONTENT_TYPES) and stream_type not in ('mjpeg', 'jpeg_snapshot'):
                    continue
                stream_type = stream_type or 'direct'
                if stream_type == 'mjpeg':
                    return DetectionResult(
                        stream_url=stream_url,
                        stream_type='mjpeg',
                        headers=headers,
                        needs_proxy=False,
                        success=False,
                        error='MJPEG stream detected, but MJPEG custom channels are not supported',
                        resolver=self._candidate_resolvers.get(stream_url) or 'page scrape',
                    )
                if stream_type == 'jpeg_snapshot':
                    return DetectionResult(
                        stream_url=stream_url,
                        stream_type='jpeg_snapshot',
                        headers=headers,
                        needs_proxy=False,
                        success=False,
                        error='JPEG snapshot feed detected, but JPEG snapshot custom channels are not supported',
                        resolver=self._candidate_resolvers.get(stream_url) or 'page scrape',
                    )

                logger.info('[detector] direct video probe OK %s ct=%s', stream_url[:80], ct)
                return DetectionResult(
                    stream_url=stream_url,
                    stream_type=stream_type,
                    headers=headers,
                    needs_proxy=False,
                    success=True,
                    resolver=self._candidate_resolvers.get(stream_url) or 'page scrape',
                )
            except Exception as exc:
                logger.debug('[detector] direct probe error %s: %s', stream_url[:80], exc)
                continue

        if 401 in seen_statuses:
            return DetectionResult(
                stream_url=stream_url,
                stream_type=self.infer_stream_type(stream_url) or 'direct',
                error='Unauthorized',
            )
        if seen_statuses & _BLOCKED_STATUS_CODES:
            return DetectionResult(
                stream_url=stream_url,
                stream_type=self.infer_stream_type(stream_url) or 'direct',
                error='Blocked or restricted',
            )
        return DetectionResult(
            error=f'Direct video URL found but not accessible: {stream_url[:80]}'
        )

    def _resolve_twitch(self, url: str) -> DetectionResult:
        """
        Extract a playable stream URL from a Twitch channel/page using yt-dlp.
        Twitch URLs can resolve to live HLS, clips, or archived VODs depending
        on what the channel exposes at the moment.
        """
        try:
            import yt_dlp
        except ImportError:
            return DetectionResult(
                error='yt-dlp is not installed — rebuild the container to enable Twitch support',
                resolver='yt-dlp',
            )

        last_error = 'Extraction failed'
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'skip_download': True,
                'format': 'best[protocol~=m3u8]/best',
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)

            if not info:
                return DetectionResult(error=last_error, resolver='yt-dlp')

            headers = self._extract_http_headers(info)
            protocol = str(info.get('protocol') or '')
            stream_url = info.get('manifest_url') if 'm3u8' in protocol else info.get('url') or info.get('manifest_url')
            if not stream_url or not str(stream_url).startswith('http'):
                return DetectionResult(error=last_error, resolver='yt-dlp')

            stream_url = str(stream_url)
            if not self._yt_n_ok(stream_url):
                logger.debug('[twitch] n-param unsolved, skipping')
                return DetectionResult(
                    stream_url=stream_url,
                    stream_type='hls' if 'm3u8' in protocol else (self.infer_stream_type(stream_url) or 'direct'),
                    error='Extracted URL appears throttled (n parameter too long)',
                    headers=headers,
                    needs_proxy=bool(headers),
                    resolver='yt-dlp',
                )

            logger.info('[twitch] resolved url=%s…', stream_url[:80])
            stream_type = 'hls' if 'm3u8' in protocol else (self.infer_stream_type(stream_url) or 'direct')
            return self._finalize_extracted_stream(stream_url, stream_type, headers, 'yt-dlp')
        except Exception as exc:
            last_error = str(exc)
            logger.debug('[twitch] failed: %s', exc)
            return DetectionResult(error=last_error, resolver='yt-dlp')

    @staticmethod
    @lru_cache(maxsize=1)
    def _yt_dlp_extractor_list() -> list:
        """Build the non-generic yt-dlp extractor list once per process."""
        try:
            from yt_dlp.extractor import gen_extractors
            return [
                ie for ie in gen_extractors()
                if (getattr(ie, 'IE_NAME', '') or type(ie).__name__).lower() != 'generic'
            ]
        except Exception:
            return []

    @staticmethod
    @lru_cache(maxsize=512)
    def _yt_dlp_has_dedicated_extractor(url: str) -> bool:
        for ie in StreamDetector._yt_dlp_extractor_list():
            try:
                if ie.suitable(url):
                    return True
            except Exception:
                continue
        return False

    @staticmethod
    def _extract_http_headers(info) -> dict[str, str]:
        """
        Collect playback-relevant HTTP headers from yt-dlp info objects.

        We keep Referer/Origin/User-Agent style headers, but intentionally drop
        Cookie so we do not forward extractor auth as a browser cookie.
        """
        headers: dict[str, str] = {}

        def _merge(candidate):
            if not isinstance(candidate, dict):
                return
            for key, value in candidate.items():
                if not value:
                    continue
                if str(key).lower() == 'cookie':
                    continue
                headers[str(key)] = str(value)

        if isinstance(info, dict):
            _merge(info.get('http_headers'))
            for fmt in info.get('requested_formats') or []:
                if isinstance(fmt, dict):
                    _merge(fmt.get('http_headers'))
            if not headers:
                for fmt in info.get('formats') or []:
                    if isinstance(fmt, dict):
                        _merge(fmt.get('http_headers'))
        return headers

    @staticmethod
    def _yt_dlp_no_stream_error(info, fallback: str = 'Extraction failed') -> str:
        """
        Build a more informative yt-dlp failure message when the extractor
        matched a page but did not return a playable stream URL.
        """
        if not isinstance(info, dict):
            return fallback

        extractor = (
            info.get('extractor_key')
            or info.get('ie_key')
            or info.get('extractor')
            or ''
        )
        title = (info.get('title') or '').strip()
        kind = 'playlist/page'
        if title and extractor:
            return f'yt-dlp matched {extractor} for {title!r}, but no playable stream was returned'
        if extractor:
            return f'yt-dlp matched {extractor}, but no playable stream was returned'
        if info.get('entries'):
            return 'yt-dlp extracted a playlist/page, but no playable stream was returned'
        return fallback

    def _probe_without_extra_headers(
        self,
        session: requests.Session,
        stream_url: str,
        stream_type: str,
    ) -> DetectionResult:
        """
        Second-stage probe using only the session defaults.

        This tells us whether extractor-returned headers are actually required
        or were just incidental metadata from yt-dlp.
        """
        origin = self._origin_of(stream_url)
        if stream_type == 'hls' or self._is_hls_url(stream_url):
            return self._probe_hls(session, stream_url, stream_url, origin, [{}])
        return self._probe_direct(session, stream_url, [{}])

    def _finalize_extracted_stream(
        self,
        stream_url: str,
        stream_type: str,
        headers: dict[str, str],
        resolver: str,
        *,
        is_youtube: bool = False,
    ) -> DetectionResult:
        """
        Normalize extracted streams so we only keep headers when they are
        actually required for playback.
        """
        if not headers or is_youtube:
            # YouTube CDN URLs (googlevideo.com) are always publicly accessible —
            # the yt-dlp headers are informational, not required for playback.
            return DetectionResult(
                stream_url=stream_url,
                stream_type=stream_type,
                headers={},
                needs_proxy=False,
                success=True,
                is_youtube=is_youtube,
                resolver=resolver,
            )

        session = requests.Session()
        session.headers.update({'User-Agent': _BROWSER_UA})
        bare = self._probe_without_extra_headers(session, stream_url, stream_type)
        if bare.success and not bare.needs_proxy:
            return DetectionResult(
                stream_url=stream_url,
                stream_type=stream_type,
                headers={},
                needs_proxy=False,
                success=True,
                is_youtube=is_youtube,
                resolver=resolver,
            )

        return DetectionResult(
            stream_url=stream_url,
            stream_type=stream_type,
            headers=headers,
            needs_proxy=True,
            success=True,
            is_youtube=is_youtube,
            resolver=resolver,
        )

    def _resolve_yt_dlp_url(self, url: str) -> DetectionResult:
        """
        Generic yt-dlp fallback for sites yt-dlp already has a dedicated extractor for.
        This is used before the page scraper so we don't re-implement common video hosts.
        """
        try:
            import yt_dlp
        except ImportError:
            return DetectionResult(
                error='yt-dlp is not installed — rebuild the container to enable yt-dlp support',
                resolver='yt-dlp',
            )

        last_error = 'Extraction failed'
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'skip_download': True,
                'format': 'best[protocol~=m3u8]/best',
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)

            if not info:
                return DetectionResult(error=last_error, resolver='yt-dlp')

            headers = self._extract_http_headers(info)
            protocol = str(info.get('protocol') or '')
            stream_url = info.get('manifest_url') if 'm3u8' in protocol else info.get('url') or info.get('manifest_url')
            if not stream_url or not str(stream_url).startswith('http'):
                return DetectionResult(
                    error=self._yt_dlp_no_stream_error(info, last_error),
                    resolver='yt-dlp',
                )

            stream_url = str(stream_url)
            if not self._yt_n_ok(stream_url):
                logger.debug('[ytdlp] n-param unsolved for %s', url[:80])
                return DetectionResult(
                    stream_url=stream_url,
                    stream_type='hls' if 'm3u8' in protocol else (self.infer_stream_type(stream_url) or 'direct'),
                    error='Extracted URL appears throttled (n parameter too long)',
                    headers=headers,
                    needs_proxy=bool(headers),
                    resolver='yt-dlp',
                )

            logger.info('[ytdlp] resolved url=%s…', stream_url[:80])
            stream_type = 'hls' if 'm3u8' in protocol else (self.infer_stream_type(stream_url) or 'direct')
            return self._finalize_extracted_stream(stream_url, stream_type, headers, 'yt-dlp')
        except Exception as exc:
            last_error = str(exc)
            logger.debug('[ytdlp] failed for %s: %s', url[:80], exc)
            return DetectionResult(error=last_error, resolver='yt-dlp')

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
                resolver='yt-dlp',
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

                headers = self._extract_http_headers(info)
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
                stream_type = 'hls' if 'm3u8' in protocol else (self.infer_stream_type(stream_url) or 'direct')
                return self._finalize_extracted_stream(
                    stream_url,
                    stream_type,
                    headers,
                    'yt-dlp',
                    is_youtube=True,
                )
            except Exception as exc:
                last_error = str(exc)
                logger.debug('[youtube] client=%s failed: %s', client, exc)
                continue

        return DetectionResult(is_youtube=True, error=f'YouTube extraction failed: {last_error}', resolver='yt-dlp')

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

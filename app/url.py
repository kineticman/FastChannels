import hashlib
import os
import socket
from pathlib import Path
from urllib.parse import quote, urlsplit

from flask import current_app, request

from .models import AppSettings


def _detect_lan_ip() -> str | None:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.connect(("8.8.8.8", 80))
            ip = sock.getsockname()[0]
        finally:
            sock.close()
        if ip and not ip.startswith("127."):
            return ip
    except OSError:
        return None
    return None


def _running_in_docker() -> bool:
    return Path("/.dockerenv").exists()


def detected_base_url() -> str:
    base = request.host_url.rstrip("/")
    parsed = urlsplit(base)
    host = (parsed.hostname or "").lower()
    if host not in {"localhost", "127.0.0.1", "::1", "[::1]"}:
        return base

    # Inside Docker, simple socket-based LAN detection usually returns the
    # container bridge IP (for example 172.18.x.x), which is not reachable by
    # clients on the LAN. In that case prefer leaving localhost in place and
    # let the user set Public Base URL explicitly in Settings.
    if _running_in_docker():
        return base

    lan_ip = _detect_lan_ip()
    if not lan_ip:
        return base

    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{lan_ip}{port}"


_LOGO_CACHE_ROOT   = '/data/logo_cache/logos'
_POSTER_CACHE_ROOT = '/data/logo_cache/posters'

# Formats that Channels DVR native apps (Android TV, iOS, etc.) cannot display.
# Fall back to the upstream CDN URL for these so the client gets something usable.
_UNSUPPORTED_TYPES = ('webp', 'svg')


def proxy_logo_url(url: str | None, base_url: str, img_type: str = 'logo') -> str | None:
    """Return the best logo URL for M3U/XMLTV output.

    If the image is already cached locally, return a direct static-file URL
    (/logos/{hash}.jpg) — no proxy overhead, no query params, no client compat
    issues.  If not cached yet, return the upstream CDN URL directly so the
    client can still fetch it while the prewarm catches up.

    Posters are different: they are fetched on demand rather than prewarmed, so
    the generated XML should always point at our proxy route. Otherwise the XML
    artifact becomes dependent on the poster cache state at build time and can
    emit stale /posters/... URLs after cache clears.

    WebP/SVG cached files fall back to the upstream URL because Channels DVR
    native apps cannot display those formats.
    """
    if not url or not base_url:
        return url

    key = hashlib.md5(url.encode()).hexdigest()
    ext = 'jpg'
    for candidate in ('jpg', 'jpeg', 'png', 'gif', 'webp'):
        if f'.{candidate}' in url.lower():
            ext = candidate
            break

    cache_root = _POSTER_CACHE_ROOT if img_type == 'poster' else _LOGO_CACHE_ROOT
    img_path   = os.path.join(cache_root, key)

    if img_type == 'poster':
        # Write the .url sidecar now so the hash-based proxy route can resolve
        # the original URL without a query string in the XML output.
        url_path = os.path.join(cache_root, key + '.url')
        if not os.path.exists(url_path):
            try:
                os.makedirs(cache_root, exist_ok=True)
                with open(url_path, 'w') as _f:
                    _f.write(url)
            except OSError:
                pass
        return f"{base_url}/images/proxy/poster/{key}.{ext}"

    if os.path.exists(img_path):
        # Skip unsupported formats — serve upstream URL instead
        ct_path = img_path + '.ct'
        if os.path.exists(ct_path):
            ct = open(ct_path).read().strip().lower()
            if any(t in ct for t in _UNSUPPORTED_TYPES):
                return url
        static_dir = 'posters' if img_type == 'poster' else 'logos'
        return f"{base_url}/{static_dir}/{key}.{ext}"

    # Not cached yet — write a .url sidecar so the proxy route can fetch on
    # demand, and return the proxy URL.  This keeps all logo traffic through our
    # server so clients never need to trust upstream CDN certificates directly.
    url_path = os.path.join(cache_root, key + '.url')
    if not os.path.exists(url_path):
        try:
            os.makedirs(cache_root, exist_ok=True)
            with open(url_path, 'w') as _f:
                _f.write(url)
        except OSError:
            return url
    return f"{base_url}/images/proxy/logo/{key}.{ext}"


def public_base_url() -> str:
    settings_value = (AppSettings.get().effective_public_base_url() or "").strip().rstrip("/")
    if settings_value:
        # User explicitly set a URL — honour it as-is. If they've configured
        # http:// deliberately (e.g. Channels DVR accesses FastChannels directly
        # over HTTP while the admin UI is behind an HTTPS reverse proxy) we must
        # not silently upgrade the scheme or feed/play URLs will break.
        return settings_value

    configured = (current_app.config.get("PUBLIC_BASE_URL") or "").strip().rstrip("/")
    if configured:
        return configured

    # No explicit URL configured — fall back to auto-detection. ProxyFix has
    # already corrected request.host_url to reflect the public scheme/host set
    # by the reverse proxy, so detected_base_url() returns the right value.
    return detected_base_url()

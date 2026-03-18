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


def proxy_logo_url(url: str | None, base_url: str, img_type: str = 'logo') -> str | None:
    """Rewrite a remote image URL to route through our local image proxy.

    Uses a hash-based filename so the URL genuinely ends in an image extension
    (e.g. /images/proxy/logo/a3f2c1d4….jpg) — clients that check the path
    extension recognise it as an image without needing to parse query params.

    A .url sidecar file is written alongside the cache entry so the proxy
    route can fetch the image on a cache miss without a query parameter.
    """
    if not url or not base_url:
        return url

    key = hashlib.md5(url.encode()).hexdigest()
    ext = 'jpg'
    for candidate in ('jpg', 'jpeg', 'png', 'gif', 'webp'):
        if f'.{candidate}' in url.lower():
            ext = candidate
            break

    # Write .url sidecar so the proxy route can fetch the original on cache miss.
    cache_root = _POSTER_CACHE_ROOT if img_type == 'poster' else _LOGO_CACHE_ROOT
    try:
        os.makedirs(cache_root, exist_ok=True)
        url_path = os.path.join(cache_root, key + '.url')
        if not os.path.exists(url_path):
            with open(url_path, 'w') as fh:
                fh.write(url)
    except OSError:
        pass

    return f"{base_url}/images/proxy/{img_type}/{key}.{ext}"


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

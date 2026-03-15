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


def proxy_logo_url(url: str | None, base_url: str, img_type: str = 'logo') -> str | None:
    """Rewrite a remote image URL to route through our local image proxy.

    A dummy filename with extension is included in the path so clients that
    require a file extension (e.g. Channels DVR native apps) recognise the
    response as an image.
    """
    if not url or not base_url:
        return url
    # Carry the original extension if present, otherwise default to jpg
    ext = 'jpg'
    for candidate in ('jpg', 'jpeg', 'png', 'gif', 'webp'):
        if f'.{candidate}' in url.lower():
            ext = candidate
            break
    return f"{base_url}/images/proxy/{img_type}/image.{ext}?url={quote(url, safe='')}"


def public_base_url() -> str:
    settings_value = (AppSettings.get().effective_public_base_url() or "").strip().rstrip("/")
    if settings_value:
        return settings_value

    configured = (current_app.config.get("PUBLIC_BASE_URL") or "").strip().rstrip("/")
    if configured:
        return configured

    return detected_base_url()

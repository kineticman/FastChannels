import socket
from urllib.parse import urlsplit

from flask import current_app, request


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


def public_base_url() -> str:
    configured = (current_app.config.get("PUBLIC_BASE_URL") or "").strip().rstrip("/")
    if configured:
        return configured

    base = request.host_url.rstrip("/")
    parsed = urlsplit(base)
    host = (parsed.hostname or "").lower()
    if host not in {"localhost", "127.0.0.1", "::1", "[::1]"}:
        return base

    lan_ip = _detect_lan_ip()
    if not lan_ip:
        return base

    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{lan_ip}{port}"

from __future__ import annotations

import json
import re
import subprocess
from dataclasses import asdict, dataclass, field
from urllib.parse import urlsplit

_SECRET_RE = re.compile(r'(?i)(password|passwd|token|authorization|auth|sig|key)=([^\s&]+)')


@dataclass(frozen=True)
class TVEProbeResult:
    ok: bool
    url: str
    extractor: str | None = None
    title: str | None = None
    is_live: bool | None = None
    formats: int = 0
    protocols: tuple[str, ...] = ()
    stream_hosts: tuple[str, ...] = ()
    error: str | None = None
    warnings: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict:
        return asdict(self)


def _sanitize(text: str, secrets: tuple[str, ...] = ()) -> str:
    cleaned = text or ''
    for secret in secrets:
        if secret:
            cleaned = cleaned.replace(secret, '<redacted>')
    cleaned = _SECRET_RE.sub(r'\1=<redacted>', cleaned)
    return cleaned.strip()


def _stream_host(raw_url: str | None) -> str | None:
    if not raw_url:
        return None
    host = urlsplit(raw_url).netloc.lower()
    return host or None


def probe_ytdlp_url(
    url: str,
    *,
    mso_id: str | None = None,
    username: str | None = None,
    password: str | None = None,
    timeout: int = 75,
) -> TVEProbeResult:
    """Run a safe yt-dlp metadata probe and summarize extractor/stream shape.

    The probe does not download media. It asks yt-dlp for JSON metadata so we can
    quickly learn whether a URL is extractable, which extractor handled it, and
    what kind of stream URLs came back.
    """
    cmd = [
        'yt-dlp',
        '--simulate',
        '--dump-single-json',
        '--no-playlist',
        '--no-warnings',
    ]
    if mso_id:
        cmd.extend(['--ap-mso', mso_id])
    if username:
        cmd.extend(['--ap-username', username])
    if password:
        cmd.extend(['--ap-password', password])
    cmd.append(url)

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return TVEProbeResult(ok=False, url=url, error=f'yt-dlp probe timed out after {timeout}s')
    except OSError as exc:
        return TVEProbeResult(ok=False, url=url, error=str(exc))

    secrets = tuple(v for v in (username, password) if v)
    if proc.returncode != 0:
        error = _sanitize((proc.stderr or proc.stdout or '').splitlines()[-1] if (proc.stderr or proc.stdout) else 'yt-dlp probe failed', secrets)
        return TVEProbeResult(ok=False, url=url, error=error)

    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        return TVEProbeResult(ok=False, url=url, error=f'yt-dlp returned invalid JSON: {exc}')

    formats = data.get('formats') or []
    protocols = sorted({str(fmt.get('protocol') or '').strip() for fmt in formats if fmt.get('protocol')})
    hosts = sorted({host for fmt in formats if (host := _stream_host(fmt.get('url')))} )
    warnings: list[str] = []
    if not formats and data.get('url'):
        host = _stream_host(data.get('url'))
        if host:
            hosts = [host]
    if any('dash' in proto.lower() for proto in protocols):
        warnings.append('DASH formats present; check DRM before treating as playable.')
    if any('m3u8' in proto.lower() or 'hls' in proto.lower() for proto in protocols):
        warnings.append('HLS formats present; inspect manifest for DRM and auth headers.')

    return TVEProbeResult(
        ok=True,
        url=url,
        extractor=data.get('extractor_key') or data.get('extractor'),
        title=data.get('title'),
        is_live=data.get('is_live'),
        formats=len(formats),
        protocols=tuple(protocols),
        stream_hosts=tuple(hosts[:8]),
        warnings=tuple(warnings),
    )

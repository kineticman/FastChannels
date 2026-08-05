"""Generic multi-MSO Adobe Pass authentication via yt-dlp's built-in login flows.

yt-dlp maintains provider-specific TV Everywhere login handlers (Sling, Spectrum/
Charter, Fubo, Suddenlink, AlticeOne, plus a generic form-POST fallback for
everyone else) behind ``AdobePassIE._extract_mvpd_auth()``. We call that method
directly as a library — not the CLI — so we get back the same short-lived Adobe
"shortAuthorize" token that ``AdobePassCoxClient.authorize()`` produces for Cox.
That makes this a drop-in for any scraper already speaking the legacy
sp.auth.adobe.com XML protocol (see app/tve/adobe_pass.py); it does NOT help
scrapers with their own bespoke Adobe flow (e.g. amcn_tve.py's v2 session API).
"""
from __future__ import annotations

import yt_dlp
from yt_dlp.extractor.adobepass import AdobePassIE
from yt_dlp.networking.impersonate import ImpersonateTarget
from yt_dlp.utils import ExtractorError

from .adobe_pass import TVEAuthError, TVENotAuthorizedError


class _NullLogger:
    def debug(self, msg):
        pass

    def info(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass


def _resource_with_guid(resource: str, requestor_id: str) -> str:
    """Inject a <guid> into XML resource blobs that lack one.

    Our own AdobePassCoxClient never needed a guid, so several scrapers' XML
    resource templates omit it — but yt-dlp's _extract_mvpd_auth requires one
    (as a cache key) whenever the resource contains a '<'.
    """
    if '<' not in resource or '<guid>' in resource:
        return resource
    if '<item>' in resource:
        return resource.replace('<item>', f'<item><guid>{requestor_id}</guid>', 1)
    return resource


def authorize_via_ytdlp(
    *,
    mso_id: str,
    username: str,
    password: str,
    requestor_id: str,
    resource: str,
    software_statement: str,
    redirect_url: str,
    video_id: str = 'fastchannels-tve',
) -> str:
    """Authenticate against `mso_id` via yt-dlp's Adobe Pass login flow.

    Returns the short-lived Adobe authorization token (same shape as
    AdobePassCoxClient.authorize()'s return value).
    """
    ydl_opts = {
        'ap_mso': mso_id,
        'ap_username': username,
        'ap_password': password,
        'quiet': True,
        'no_warnings': True,
        'logger': _NullLogger(),
        # MSO login pages (e.g. identity.sling.com) WAF-block yt-dlp's default
        # HTTP client as a bot (HTTP 417) — impersonate a real browser's TLS/HTTP
        # fingerprint via curl_cffi, same as app/scrapers/cox.py does directly.
        'impersonate': ImpersonateTarget.from_str('chrome'),
    }
    resource = _resource_with_guid(resource, requestor_id)
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ie = AdobePassIE(ydl)
            token = ie._extract_mvpd_auth(redirect_url, video_id, requestor_id, resource, software_statement)
    except ExtractorError as exc:
        message = str(exc)
        if 'not authorized' in message.lower() or 'not entitled' in message.lower():
            raise TVENotAuthorizedError(message) from exc
        raise TVEAuthError(message) from exc
    if not token:
        raise TVEAuthError(f'{mso_id}: yt-dlp Adobe Pass auth did not return a token.')
    return token

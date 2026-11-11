"""Bally Sports Live (ballysports.com): free, unauthenticated channels.

Was its own source ('ballysports') until it was folded into Free Networks; the
network key is the old source name, channel ids are the old ones (the channel
UUID), and the stream ref is the old opaque bally://channel/<host>/<path> URL,
so moved channel rows keep their tvg-id, play URL and settings
(app/folded_sources.py).

Channel metadata and the guide are embedded in the Next.js RSC payload of any
channel page. Each channel's public CDN URL (<host>.channels.ballys.tv) needs
no token and is stable, so the ref keeps the host and path verbatim; hosts
vary ("fast01", "lnr02", "linear30"). Very old rows stored a bare numeric
suffix ("6") meaning linear06; resolve() still takes those.

The payload's layout has moved three times (latest 2026-10-25: the guide
array went from epgData to epgItems, with epgData left as a back-reference),
so each push() chunk is scanned on its own for a real array.
"""
from __future__ import annotations

import json
import logging
import re

from ._common import Network, NetChannel, parse_iso
from ..base import ProgramData

logger = logging.getLogger('app.scrapers.free_networks')

_PAGE = 'https://www.ballysports.com/channels/milb'   # any channel page carries every channel + guide
_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/148.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}
_SCHEME = 'bally://channel/'
_CDN_RE = re.compile(r'https://([a-z0-9]+)\.channels\.ballys\.tv/(.+)$')


class BallySports(Network):
    key = 'ballysports'
    name = 'Bally Sports Live'

    def channels(self, s) -> list[NetChannel]:
        entries, _ = self._page(s)
        out = []
        for e in entries:
            uuid_val, name, cdn = e.get('channelUuid'), e.get('channelName'), e.get('public_cdn_url')
            m = _CDN_RE.match(cdn or '')
            if not uuid_val or not name or not m:
                continue
            out.append(NetChannel(str(uuid_val), name, f'{_SCHEME}{m.group(1)}/{m.group(2)}',
                                  logo=e.get('logo') or None, category='Sports',
                                  description=e.get('metadata_description') or None))
        return out

    def programs(self, s, channels: list[NetChannel]) -> list[ProgramData] | None:
        _, events = self._page(s)
        if not events:
            return None   # page or payload missing: keep the stored guide
        wanted = {c.id for c in channels}
        out = []
        for ev in events:
            cid = str(ev.get('channelUuid', ''))
            since, till = parse_iso(ev.get('since')), parse_iso(ev.get('till'))
            if cid not in wanted or not ev.get('title') or not since or not till:
                continue
            live = ev.get('is_live')
            out.append(ProgramData(source_channel_id=cid, title=ev['title'], start_time=since, end_time=till,
                                   description=ev.get('comment') or None,
                                   is_live=live if isinstance(live, bool) else None))
        return out

    def resolve(self, s, ref: str) -> str:
        if not ref.startswith(_SCHEME):
            return ref
        host, _, path = ref[len(_SCHEME):].partition('/')
        if host.isdigit():   # legacy: bare linear{NN} suffix, manifest in `path`
            return f'https://linear{host.zfill(2)}.channels.ballys.tv/abr_default/{path or "index_dvr.m3u8"}'
        return f'https://{host}.channels.ballys.tv/{path or "abr_default/index_dvr.m3u8"}'

    def _page(self, s) -> tuple[list[dict], list[dict]]:
        def fetch():
            r = s.get(_PAGE, headers=_HEADERS)
            return _parse_page(r.text) if r else ([], [])
        return s.memo('ballysports.page', fetch)


def _parse_page(html: str) -> tuple[list[dict], list[dict]]:
    """channelData and the guide array from the page's RSC payload. Values use
    a "$D" prefix for dates and "$undefined" for undefined."""
    chunks = _rsc_chunks(html)
    if not chunks:
        logger.warning('[free_networks] Bally Sports: RSC payload not found in page')
        return [], []
    channels: list[dict] = []
    events: list[dict] = []
    for chunk in chunks:
        cleaned = re.sub(r'"\$undefined"', 'null', re.sub(r'"\$D([^"]+)"', r'"\1"', chunk))
        if not channels:
            channels = _json_array(cleaned, 'channelData') or []
        if not events:
            events = _json_array(cleaned, 'epgItems') or _json_array(cleaned, 'epgData') or []
        if channels and events:
            break
    if not channels:
        logger.warning('[free_networks] Bally Sports: channelData not found in RSC payload')
    if not events:
        logger.warning('[free_networks] Bally Sports: epgItems/epgData not found in RSC payload')
    return channels, events


def _rsc_chunks(html: str) -> list[str]:
    """The inner strings of every self.__next_f.push([1, "..."]) whose payload
    mentions channelData, epgItems or epgData."""
    out = []
    for m in re.finditer(r'self\.__next_f\.push\((\[.*?\])\)', html, re.DOTALL):
        try:
            arr = json.loads(m.group(1))
        except Exception:  # noqa: BLE001
            continue
        if (isinstance(arr, list) and len(arr) >= 2 and arr[0] == 1 and isinstance(arr[1], str)
                and any(k in arr[1] for k in ('channelData', 'epgItems', 'epgData'))):
            out.append(arr[1])
    return out


def _json_array(s: str, key: str) -> list | None:
    """The JSON array after the first `"<key>":` that is followed by a real
    array (not a "$..." back-reference string)."""
    pattern = f'"{key}":'
    idx = 0
    while True:
        pos = s.find(pattern, idx)
        if pos == -1:
            return None
        start = pos + len(pattern)
        while start < len(s) and s[start] in ' \t\n\r':
            start += 1
        if start >= len(s) or s[start] != '[':
            idx = pos + 1
            continue
        depth, in_str, esc, i = 0, False, False, start
        while i < len(s):
            c = s[i]
            if in_str:
                if esc:
                    esc = False
                elif c == '\\':
                    esc = True
                elif c == '"':
                    in_str = False
            elif c == '"':
                in_str = True
            elif c == '[':
                depth += 1
            elif c == ']':
                depth -= 1
                if depth == 0:
                    i += 1
                    break
            i += 1
        try:
            return json.loads(s[start:i])
        except Exception:  # noqa: BLE001
            idx = pos + 1

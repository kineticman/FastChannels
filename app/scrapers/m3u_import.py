# app/scrapers/m3u_import.py
"""
Tolerant parser for user-supplied M3U playlists.

Real-world playlists are messy: attributes in any order, unquoted values,
commas inside quoted values and channel names, names wrapped onto a second
line, player-specific option lines between #EXTINF and the URL, BOMs, CRLF and
non-UTF-8 bytes. parse_m3u() takes all of that and returns plain entries; it
does no filtering and assigns no ids — see assign_channel_ids() for those.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass, field

_ATTR_RE = re.compile(r'([A-Za-z0-9_-]+)=(?:"([^"]*)"|([^\s",]+))')

# #EXTM3U header attributes that point at the playlist's XMLTV guide.
_GUIDE_URL_ATTRS = ('url-tvg', 'x-tvg-url', 'tvg-url')

# Request headers a playlist can declare for a stream. We record them so the
# import can report how many entries need them; nothing sends them yet.
_VLC_HEADER_OPTS = {
    'http-user-agent': 'User-Agent',
    'http-referrer':   'Referer',
    'http-referer':    'Referer',
    'http-origin':     'Origin',
}


@dataclass
class M3UEntry:
    name: str
    url: str
    attrs: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    has_drm_props: bool = False

    def attr(self, *keys: str) -> str:
        for key in keys:
            value = (self.attrs.get(key) or '').strip()
            if value:
                return value
        return ''

    @property
    def group(self) -> str:
        return self.attr('group-title')


@dataclass
class M3UPlaylist:
    entries: list[M3UEntry]
    guide_url: str | None = None
    skipped: int = 0   # entries dropped for having no usable http(s) URL


def decode_m3u(data: bytes) -> str:
    try:
        return data.decode('utf-8-sig')
    except UnicodeDecodeError:
        return data.decode('latin-1')


def _split_extinf(body: str) -> tuple[str, str]:
    """Split the text after '#EXTINF:' into (attribute part, display name) at
    the first comma that isn't inside a quoted value."""
    in_quotes = False
    for i, ch in enumerate(body):
        if ch == '"':
            in_quotes = not in_quotes
        elif ch == ',' and not in_quotes:
            return body[:i], body[i + 1:]
    return body, ''


def _parse_attrs(text: str) -> dict[str, str]:
    return {
        m.group(1).lower(): (m.group(2) if m.group(2) is not None else m.group(3))
        for m in _ATTR_RE.finditer(text)
    }


def _split_pipe_headers(url: str) -> tuple[str, dict[str, str]]:
    """Kodi-style 'http://host/x.m3u8|User-Agent=foo&Referer=bar'."""
    if '|' not in url:
        return url, {}
    base, _, tail = url.partition('|')
    headers = {}
    for pair in tail.split('&'):
        key, sep, value = pair.partition('=')
        if sep and key.strip():
            headers[key.strip()] = value.strip()
    return base.strip(), headers


def parse_m3u(text: str) -> M3UPlaylist:
    entries: list[M3UEntry] = []
    guide_url = None
    skipped = 0
    pending: M3UEntry | None = None
    pending_group = ''

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if line.upper().startswith('#EXTM3U'):
            header = _parse_attrs(line[7:])
            for key in _GUIDE_URL_ATTRS:
                if header.get(key):
                    # Some playlists list several guides, comma-separated.
                    guide_url = header[key].split(',')[0].strip() or None
                    break
            continue

        if line.upper().startswith('#EXTINF:'):
            if pending is not None:
                skipped += 1   # previous #EXTINF never got a URL
            attr_text, name = _split_extinf(line[8:])
            pending = M3UEntry(name=name.strip(), url='', attrs=_parse_attrs(attr_text))
            continue

        if line.startswith('#'):
            upper = line.upper()
            if upper.startswith('#EXTGRP:'):
                pending_group = line[8:].strip()
            elif upper.startswith('#EXTVLCOPT:') and pending is not None:
                key, sep, value = line[11:].partition('=')
                header = _VLC_HEADER_OPTS.get(key.strip().lower())
                if sep and header and value.strip():
                    pending.headers[header] = value.strip()
            elif upper.startswith('#KODIPROP:') and pending is not None:
                if 'license' in line.lower():
                    pending.has_drm_props = True
            continue

        if pending is None:
            continue   # a bare URL with no #EXTINF — nothing to name it by

        if '://' not in line:
            # A channel name wrapped onto the next line.
            pending.name = f'{pending.name} {line}'.strip()
            continue

        url, pipe_headers = _split_pipe_headers(line)
        if not url.lower().startswith(('http://', 'https://')):
            skipped += 1
            pending, pending_group = None, ''
            continue
        pending.url = url
        pending.headers.update(pipe_headers)
        if pending_group and not pending.attrs.get('group-title'):
            pending.attrs['group-title'] = pending_group
        if not pending.name:
            pending.name = pending.attr('tvg-name') or pending.attr('tvg-id') or url
        entries.append(pending)
        pending, pending_group = None, ''

    if pending is not None:
        skipped += 1
    return M3UPlaylist(entries=entries, guide_url=guide_url, skipped=skipped)


def slugify(value: str, max_len: int = 80) -> str:
    """Lowercase ASCII slug of [a-z0-9._-]. Used for anything that ends up in a
    URL path or tvg-id, where a slash or space would break the route match."""
    text = unicodedata.normalize('NFKD', value or '').encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r'[^A-Za-z0-9._-]+', '-', text).strip('-.').lower()
    return text[:max_len].strip('-.')


def _id_base(entry: M3UEntry) -> str:
    """The part of an entry's id that should survive a playlist refresh: its
    tvg-id when it has one, else its name. Never the URL — URLs rotate."""
    raw = entry.attr('tvg-id') or entry.attr('channel-id') or entry.name
    slug = slugify(raw)
    if slug:
        return slug
    # Nothing ASCII to work with (e.g. an all-CJK name): hash it instead.
    return 'ch-' + hashlib.sha1(raw.encode('utf-8')).hexdigest()[:12]


def assign_channel_ids(entries: list[M3UEntry]) -> list[tuple[str, M3UEntry]]:
    """Pair every entry with a unique, URL-safe id.

    Playlists commonly repeat a tvg-id for HD/SD/backup variants of a channel.
    A repeat is told apart by its name first (stable if the playlist is
    reordered) and only then by a counter."""
    used: set[str] = set()
    out: list[tuple[str, M3UEntry]] = []
    for entry in entries:
        base = _id_base(entry)
        candidate = base
        if candidate in used:
            name_slug = slugify(entry.name, 40)
            if name_slug and name_slug != base:
                candidate = f'{base}-{name_slug}'
        n = 2
        unique = candidate
        while unique in used:
            unique = f'{candidate}-{n}'
            n += 1
        used.add(unique)
        out.append((unique, entry))
    return out

# app/scrapers/xmltv_import.py
"""
Shared XMLTV → ProgramData parsing for sources whose guide arrives as an XMLTV
document (HDHomeRun's cloud guide, user-supplied playlist guides).

Two layers:
  - program_from_xmltv()  one <programme> element → ProgramData
  - iter_xmltv()          stream <channel>/<programme> elements out of a file
                          without holding the document in memory
"""
from __future__ import annotations

import gzip
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import BinaryIO, Iterator

from .base import ProgramData

logger = logging.getLogger(__name__)

_EPISODE_RE = re.compile(r'S(\d+)\s*E(\d+)', re.IGNORECASE)

# "20240101120000 +0000" is the spec form. In the wild the offset is often
# missing (meaning UTC), glued on without a space, or the seconds are dropped.
_TIME_RE = re.compile(r'^\s*(\d{14}|\d{12})\s*(Z|[+-]\d{4})?\s*$')

# XMLTV <category> values that are too generic to use as programme category.
_SKIP_CATEGORIES = frozenset({'Series', 'Movie'})

# Decompressed-size ceiling for a guide file. Public guides run to a few
# hundred MB; anything past this is a mistake or a decompression bomb.
MAX_XMLTV_BYTES = 1024 * 1024 * 1024


class XMLTVTooLargeError(ValueError):
    pass


def parse_xmltv_time(value: str | None) -> datetime | None:
    m = _TIME_RE.match(value or '')
    if not m:
        return None
    digits, offset = m.groups()
    try:
        dt = datetime.strptime(digits, '%Y%m%d%H%M%S' if len(digits) == 14 else '%Y%m%d%H%M')
    except ValueError:
        return None
    if not offset or offset == 'Z':
        return dt.replace(tzinfo=timezone.utc)
    sign = -1 if offset[0] == '-' else 1
    delta = timedelta(hours=int(offset[1:3]), minutes=int(offset[3:5]))
    return dt.replace(tzinfo=timezone(sign * delta))


def _season_episode(prog: ET.Element) -> tuple[int | None, int | None]:
    """Season/episode from <episode-num>. 'onscreen' ("S01E06") wins when it
    parses; otherwise 'xmltv_ns' ("0.5.0/1" — zero-based, optional /total)."""
    xmltv_ns = None
    for ep_el in prog.findall('episode-num'):
        system = (ep_el.get('system') or '').lower()
        text = ep_el.text or ''
        if system == 'onscreen':
            m = _EPISODE_RE.search(text)
            if m:
                return int(m.group(1)), int(m.group(2))
        elif system == 'xmltv_ns' and xmltv_ns is None:
            xmltv_ns = text
    if xmltv_ns:
        parts = [p.split('/')[0].strip() for p in xmltv_ns.split('.')]
        season = int(parts[0]) + 1 if parts and parts[0].isdigit() else None
        episode = int(parts[1]) + 1 if len(parts) > 1 and parts[1].isdigit() else None
        return season, episode
    return None, None


def program_from_xmltv(source_channel_id: str, prog: ET.Element,
                       *, allow_open_end: bool = False,
                       live_flag: bool = True,
                       movie_from_program_id: bool = True) -> ProgramData | None:
    """Map one <programme> element onto ProgramData.

    With allow_open_end, a programme that has no usable `stop` comes back with
    end_time=None so the caller can infer it from the next programme on the
    same channel (see close_open_ends).

    live_flag reads <live/> into is_live; movie_from_program_id treats an
    "MV…" dd_progid as a movie even without an MV series-id. HDHomeRun turns
    both off to keep the guide it has always produced."""
    title = prog.findtext('title')
    start = parse_xmltv_time(prog.get('start'))
    end = parse_xmltv_time(prog.get('stop'))
    if not title or start is None:
        return None
    if end is None and not allow_open_end:
        return None

    season, episode = _season_episode(prog)

    # Series ID from <series-id system="cseries">.
    series_id = None
    for sid_el in prog.findall('series-id'):
        if sid_el.get('system') == 'cseries':
            series_id = (sid_el.text or '').strip() or None
            break

    # Per-episode ID from <episode-num system="dd_progid"> (TMS program ID).
    episode_id = None
    for ep_el in prog.findall('episode-num'):
        if ep_el.get('system') == 'dd_progid':
            episode_id = (ep_el.text or '').strip() or None
            break

    if (series_id or '').startswith('MV') or (movie_from_program_id and (episode_id or '').startswith('MV')):
        program_type = 'movie'
    elif season is not None or episode is not None:
        program_type = 'episode'
    else:
        program_type = None

    # First specific category (skip generic "Series"/"Movie" labels).
    categories = [el.text.strip() for el in prog.findall('category') if el.text and el.text.strip()]
    category = next(
        (c for c in categories if c not in _SKIP_CATEGORIES),
        categories[0] if categories else None,
    )

    date_str = (prog.findtext('date') or '').strip()
    original_air_date = None
    if len(date_str) >= 8:
        try:
            original_air_date = datetime.strptime(date_str[:8], '%Y%m%d').date()
        except ValueError:
            pass

    icon_el = prog.find('icon')
    poster_url = icon_el.get('src') if icon_el is not None else None

    rating = None
    rating_el = prog.find('rating')
    if rating_el is not None:
        rating = (rating_el.findtext('value') or '').strip() or None

    return ProgramData(
        source_channel_id=source_channel_id,
        title=title,
        start_time=start,
        end_time=end,
        description=prog.findtext('desc'),
        poster_url=poster_url,
        category=category,
        rating=rating,
        episode_title=prog.findtext('sub-title') or None,
        season=season,
        episode=episode,
        original_air_date=original_air_date,
        is_live=True if live_flag and prog.find('live') is not None else None,
        program_type=program_type,
        series_id=series_id,
        episode_id=episode_id,
    )


def close_open_ends(programs: list[ProgramData]) -> list[ProgramData]:
    """Give programmes parsed with allow_open_end an end time: the start of the
    next programme on the same channel. Any left open (the last one on a
    channel) are dropped."""
    by_channel: dict[str, list[ProgramData]] = {}
    for p in programs:
        by_channel.setdefault(p.source_channel_id, []).append(p)
    closed: list[ProgramData] = []
    for items in by_channel.values():
        items.sort(key=lambda p: p.start_time)
        for i, p in enumerate(items):
            if p.end_time is None and i + 1 < len(items):
                p.end_time = items[i + 1].start_time
            if p.end_time is not None and p.end_time > p.start_time:
                closed.append(p)
    return closed


class _CappedReader:
    """File-like wrapper that refuses to hand out more than `limit` bytes, so a
    gzip bomb fails fast instead of filling memory or disk."""

    def __init__(self, fp: BinaryIO, limit: int):
        self._fp = fp
        self._left = limit

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = 1024 * 1024
        data = self._fp.read(size)
        self._left -= len(data)
        if self._left < 0:
            raise XMLTVTooLargeError('XMLTV guide is larger than the allowed size once decompressed')
        return data


def open_xmltv(path: str, limit: int = MAX_XMLTV_BYTES):
    """Open an XMLTV file for reading, transparently gunzipping. Sniffs the
    gzip magic rather than trusting the extension — guides are routinely
    served gzipped from a URL that doesn't end in .gz, and vice versa."""
    fp = open(path, 'rb')
    magic = fp.read(2)
    fp.seek(0)
    if magic == b'\x1f\x8b':
        fp = gzip.GzipFile(fileobj=fp)
    return _CappedReader(fp, limit), fp


def iter_xmltv(path: str, limit: int = MAX_XMLTV_BYTES) -> Iterator[tuple[str, ET.Element]]:
    """Yield ('channel' | 'programme', element) from an XMLTV file on disk.

    Streamed: each element is discarded after the consumer has seen it, so a
    multi-hundred-MB guide costs a few MB of memory. Raises ET.ParseError on
    malformed XML and XMLTVTooLargeError past the size limit.

    Safe against hostile input without an extra dependency: ElementTree never
    fetches external entities, and expat's own amplification limit (libexpat
    >= 2.4) rejects nested-entity expansion bombs.
    """
    reader, raw = open_xmltv(path, limit)
    try:
        root = None
        for event, el in ET.iterparse(reader, events=('start', 'end')):
            if event == 'start':
                if root is None:
                    root = el
                continue
            if el.tag in ('channel', 'programme'):
                yield el.tag, el
                # Drop the finished element (and its siblings) from the tree.
                root.clear()
    finally:
        raw.close()

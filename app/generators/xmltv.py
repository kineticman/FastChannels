"""
XMLTV / EPG generator.

Performance design:
  - JOIN instead of IN(1001 ids) — SQLite handles joins far better than
    large IN clauses which degrade O(n²).
  - Keyset pagination (WHERE id > last_id) instead of LIMIT/OFFSET —
    OFFSET scans all preceding rows on every page, so page 700 of 200
    scans 140,000 rows just to skip them.
  - Streaming generator — yields chunks so Flask/gunicorn never blocks
    waiting for the full 56MB to build in memory.
  - /epg.xml.gz endpoint serves pre-gzipped content (~5MB vs ~56MB).
    Also honours Accept-Encoding: gzip on /epg.xml.
"""
from __future__ import annotations

import gzip
import io
import re
import logging
from datetime import datetime, timedelta, timezone
from xml.etree.ElementTree import Element, SubElement, tostring

from flask import request as flask_request

from ..extensions import db
from ..models import Channel, Program, Source
from .m3u import _build_channel_query, _selected_channels, _tvg_id

log = logging.getLogger(__name__)


def _norm_name(name: str) -> str:
    """Normalise a channel name for fuzzy matching against EPG-only sources."""
    n = (name or '').lower()
    n = re.sub(r'[^a-z0-9 ]', ' ', n)
    return re.sub(r'\s+', ' ', n).strip()


def _build_enrich_index() -> dict:
    """
    Load all programs from EPG-only sources into an in-memory enrichment index.

    Returns:
        dict mapping normalised channel name →
            sorted list of (start_time, end_time, title_lower, description, poster_url, rating)

    The index is used to back-fill missing descriptions/posters/ratings for
    programs in other sources whose channel name matches an EPG-only channel.
    """
    epg_only_source_ids = [
        s.id for s in Source.query.filter_by(epg_only=True, is_enabled=True).all()
    ]
    if not epg_only_source_ids:
        return {}

    rows = (
        db.session.query(Program, Channel.name)
        .join(Channel, Program.channel_id == Channel.id)
        .filter(Channel.source_id.in_(epg_only_source_ids))
        .all()
    )

    index: dict[str, list] = {}
    for prog, ch_name in rows:
        key = _norm_name(ch_name)
        index.setdefault(key, []).append((
            prog.start_time, prog.end_time,
            (prog.title or '').lower(),
            prog.description, prog.poster_url, prog.rating,
        ))

    for key in index:
        index[key].sort(key=lambda x: x[0])

    log.debug('[xmltv] enrichment index: %d EPG-only channels loaded', len(index))
    return index


def _titles_similar(a: str, b: str) -> bool:
    """
    Return True if two programme titles are similar enough to trust enrichment.

    Rules (applied to lowercased, punctuation-stripped, whitespace-normalised):
      - Exact match
      - One title is a prefix of the other (handles "Fox 32 News at 5" vs
        "FOX 32 News at 5 PM" style variations)
      - Titles share at least 3 non-stopword words in common (handles minor
        differences while rejecting completely different programmes)
    """
    def _clean(s):
        s = re.sub(r'[^a-z0-9 ]', ' ', s.lower())
        return re.sub(r'\s+', ' ', s).strip()

    a = _clean(a)
    b = _clean(b)
    if a == b:
        return True
    if a.startswith(b) or b.startswith(a):
        return True
    stop = {'a', 'an', 'the', 'of', 'at', 'in', 'on', 'and', 'or'}
    shared = (set(a.split()) & set(b.split())) - stop
    return len(shared) >= 3


def _enrich_prog(index: dict, ch_name: str, prog: Program):
    """
    Return (description, poster_url, rating) for *prog*, supplemented from
    the enrichment index where the channel name matches, the EPG-only
    program's time window covers prog.start_time, AND the titles are
    similar enough to avoid cross-programme contamination.

    Only fills in fields that are None on the original program.
    """
    if prog.description and prog.poster_url and prog.rating:
        return prog.description, prog.poster_url, prog.rating

    key = _norm_name(ch_name)
    entries = index.get(key)
    if not entries:
        return prog.description, prog.poster_url, prog.rating

    prog_title = (prog.title or '').lower()
    for (start, end, enrich_title, desc, poster, rating) in entries:
        if start <= prog.start_time < end and _titles_similar(prog_title, enrich_title):
            return (
                prog.description or desc,
                prog.poster_url  or poster,
                prog.rating      or rating,
            )

    return prog.description, prog.poster_url, prog.rating


def generate_xmltv(filters: dict = None, base_url: str = None) -> str:
    """Compatibility wrapper — full XML as a string. Use streaming for HTTP."""
    return ''.join(generate_xmltv_stream(filters, base_url))


def generate_xmltv_gz(filters: dict = None, base_url: str = None) -> bytes:
    """Return the full XML gzip-compressed as bytes."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=6) as gz:
        for chunk in generate_xmltv_stream(filters, base_url):
            gz.write(chunk.encode('utf-8'))
    return buf.getvalue()


def generate_xmltv_stream(filters: dict = None, base_url: str = None):
    """
    Generator — yields UTF-8 text chunks of the XMLTV document.

    Key changes vs previous version:
      - Programs fetched via JOIN on the filtered channel set, not IN(ids).
      - Keyset pagination: WHERE program.id > last_seen_id ORDER BY id.
        Avoids the O(n²) OFFSET scan that made page 700 read 140k rows.
    """
    filters  = filters or {}
    base_url = (base_url or 'http://localhost:5523').rstrip('/')

    # Standard XMLTV must expose the same channel identity set as the standard
    # XMLTV-backed M3U; otherwise clients that join on tvg-id see orphaned or
    # shifted channels. Gracenote-backed channels are intentionally excluded.
    channels = _selected_channels(filters, gracenote=False)

    tvg_map      = {ch.id: _tvg_id(ch) for ch in channels}
    # Channel category map — used as fallback when prog.category is None
    ch_cat_map   = {ch.id: ch.category for ch in channels if ch.category}
    # Source display name map — added as a final <category> tag on every programme
    ch_src_map   = {ch.id: ch.source.display_name for ch in channels}
    # Channel name map — used for enrichment lookup
    ch_name_map  = {ch.id: ch.name for ch in channels}
    channel_ids = set(tvg_map.keys())

    # Build EPG enrichment index from epg_only sources (e.g. Amazon Prime Free)
    enrich_index = _build_enrich_index()

    # ── Header ────────────────────────────────────────────────────────────
    yield '<?xml version="1.0" encoding="UTF-8"?>\n'
    yield f'<tv generator-info-name="FastChannels" generator-info-url="{_esc_attr(base_url)}">\n'

    # ── Channel elements ──────────────────────────────────────────────────
    for ch in channels:
        el = Element('channel', id=tvg_map[ch.id])
        SubElement(el, 'display-name').text = ch.name
        if ch.logo_url:
            SubElement(el, 'icon', src=ch.logo_url)
        yield tostring(el, encoding='unicode') + '\n'

    # ── Programme elements — JOIN + keyset pagination ─────────────────────
    # JOIN against the filtered channel set rather than IN(1001 ids).
    # Keyset: track last Program.id seen, next page = WHERE id > last_id.
    # This is O(1) per page regardless of offset depth.
    BATCH   = 500
    last_id = 0

    # Build a select() of filtered channel IDs for use in .in_()
    # Using .select() avoids SAWarning from passing a Subquery directly to .in_()
    ch_id_subq = (
        _build_channel_query(filters)
        .with_entities(Channel.id)
        .subquery()
        .select()
    )

    # Rolling 5-day window: include currently-airing programs (up to 2h ago)
    # through 5 days from now. Naive UTC matches how SQLite stores the values.
    epg_start = datetime.utcnow() - timedelta(hours=2)
    epg_end   = datetime.utcnow() + timedelta(days=5)

    while True:
        programs = (
            db.session.query(Program)
            .join(Channel, Program.channel_id == Channel.id)
            .filter(
                Channel.id.in_(ch_id_subq),
                Program.id > last_id,
                Program.end_time   > epg_start,
                Program.start_time < epg_end,
            )
            .order_by(Program.id.asc())
            .limit(BATCH)
            .all()
        )
        if not programs:
            break

        for prog in programs:
            tvg_id = tvg_map.get(prog.channel_id)
            if not tvg_id:
                continue

            # Enrich missing fields from EPG-only sources (e.g. Amazon Prime Free)
            ch_name = ch_name_map.get(prog.channel_id, '')
            desc, poster, rating = _enrich_prog(enrich_index, ch_name, prog)

            el = Element('programme', attrib={
                'start':   _dt(prog.start_time),
                'stop':    _dt(prog.end_time),
                'channel': tvg_id,
            })
            SubElement(el, 'title', lang='en').text = prog.title or ''
            if desc:
                SubElement(el, 'desc', lang='en').text = desc
            if prog.category or ch_cat_map.get(prog.channel_id):
                # Use prog.category if set, fall back to channel category.
                # Split semicolon-joined strings into multiple <category> tags —
                # XMLTV allows multiple per programme and clients filter by them.
                for cat in (prog.category or ch_cat_map.get(prog.channel_id) or '').split(';'):
                    cat = cat.strip()
                    if cat:
                        SubElement(el, 'category', lang='en').text = cat
            # Always add source name as a category so clients can filter by provider
            src_name = ch_src_map.get(prog.channel_id)
            if src_name:
                SubElement(el, 'category', lang='en').text = src_name
            if poster:
                SubElement(el, 'icon', src=poster)
            if rating:
                r = SubElement(el, 'rating', system='MPAA')
                SubElement(r, 'value').text = rating
            if prog.episode_title:
                SubElement(el, 'sub-title', lang='en').text = prog.episode_title
            if prog.season and prog.episode:
                SubElement(el, 'episode-num', system='xmltv_ns').text = \
                    f'{prog.season - 1}.{prog.episode - 1}.'
            yield tostring(el, encoding='unicode') + '\n'

        last_id = programs[-1].id

    yield '</tv>\n'


# ── Helpers ───────────────────────────────────────────────────────────────

def _dt(dt) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime('%Y%m%d%H%M%S %z')


def _esc_attr(s: str) -> str:
    return (s or '').replace('&', '&amp;').replace('"', '&quot;')

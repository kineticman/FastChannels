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
from datetime import timezone
from xml.etree.ElementTree import Element, SubElement, tostring

from flask import request as flask_request

from ..extensions import db
from ..models import Channel, Program, Source
from .m3u import _build_channel_query, _tvg_id


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

    channels = _build_channel_query(filters).order_by(Channel.name.asc()).all()

    max_ch = filters.get('max_channels')
    if max_ch:
        channels = channels[:int(max_ch)]

    tvg_map      = {ch.id: _tvg_id(ch) for ch in channels}
    # Channel category map — used as fallback when prog.category is None
    ch_cat_map   = {ch.id: ch.category for ch in channels if ch.category}
    # Source display name map — added as a final <category> tag on every programme
    ch_src_map   = {ch.id: ch.source.display_name for ch in channels}
    channel_ids = set(tvg_map.keys())

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

    while True:
        programs = (
            db.session.query(Program)
            .join(Channel, Program.channel_id == Channel.id)
            .filter(
                Channel.id.in_(ch_id_subq),
                Program.id > last_id,
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
            el = Element('programme', attrib={
                'start':   _dt(prog.start_time),
                'stop':    _dt(prog.end_time),
                'channel': tvg_id,
            })
            SubElement(el, 'title', lang='en').text = prog.title or ''
            if prog.description:
                SubElement(el, 'desc', lang='en').text = prog.description
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
            if prog.poster_url:
                SubElement(el, 'icon', src=prog.poster_url)
            if prog.rating:
                r = SubElement(el, 'rating', system='MPAA')
                SubElement(r, 'value').text = prog.rating
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

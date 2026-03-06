import re
from ..models import Channel, Source

# Gracenote ID prefixes recognised by Channels DVR
_GRACENOTE_PREFIX_RE = re.compile(r'^(\d+|(EP|SH|MV|SP|TR)\d+)$')


def _parse_gracenote_id(ch) -> str | None:
    """
    Returns the Gracenote station ID for a channel, or None.

    Resolution order:
      1. channel.gracenote_id  — explicitly stored (set by scraper or user via admin UI)
      2. slug fallback          — Roku scrapers encode "{play_id}|{gracenote_id}" in the
                                  slug before the dedicated column existed; still honoured
                                  so existing data keeps working without a re-scrape.
    """
    # 1. Dedicated column (preferred)
    gid = (ch.gracenote_id or '').strip()
    if gid and _GRACENOTE_PREFIX_RE.match(gid):
        return gid

    # 2. Slug fallback for Roku-style "{play_id}|{gracenote_id}"
    slug = ch.slug or ''
    if '|' in slug:
        candidate = slug.split('|', 1)[1].strip()
        if candidate and _GRACENOTE_PREFIX_RE.match(candidate):
            return candidate

    return None


def _build_channel_query(filters: dict):
    """Shared filtered query for active, enabled channels."""
    query = Channel.query.join(Source).filter(
        Channel.is_active  == True,
        Channel.is_enabled == True,
        Source.is_enabled  == True,
        Channel.stream_url != None,
    )
    if sources := filters.get('source'):
        query = query.filter(Source.name.in_(sources))
    if categories := filters.get('category'):
        query = query.filter(Channel.category.in_(categories))
    if language := filters.get('language'):
        query = query.filter(Channel.language == language)
    if search := filters.get('search'):
        query = query.filter(Channel.name.ilike(f'%{search}%'))
    return query.order_by(Channel.number.asc().nullslast(), Channel.name.asc())


def generate_m3u(filters: dict = None, base_url: str = None) -> str:
    """
    Standard XMLTV-backed playlist.
    Excludes channels with a valid Gracenote ID — those belong in /m3u/gracenote
    so Channels DVR doesn't mix EPG sources within a single M3U source.
    """
    filters  = filters or {}
    base_url = (base_url or '').rstrip('/')

    lines = ['#EXTM3U']
    for ch in _build_channel_query(filters).all():
        if _parse_gracenote_id(ch):
            continue  # belongs in /m3u/gracenote
        tvg_id = _tvg_id(ch)
        attrs = [
            f'tvg-id="{tvg_id}"',
            f'tvg-name="{_esc(ch.name)}"',
            f'group-title="{_esc(ch.category or ch.source.display_name)}"',
        ]
        if ch.logo_url:
            attrs.append(f'tvg-logo="{ch.logo_url}"')
        if ch.number:
            attrs.append(f'tvg-chno="{ch.number}"')
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{ch.name}')
        lines.append(f'{base_url}/play/{ch.source.name}/{ch.source_channel_id}.m3u8')

    return '\n'.join(lines)


def generate_gracenote_m3u(filters: dict = None, base_url: str = None) -> str:
    """
    Gracenote-backed playlist for Channels DVR.

    Only includes channels with a valid Gracenote ID (from channel.gracenote_id
    or the legacy "{play_id}|{gracenote_id}" slug encoding).
    Uses tvc-guide-stationid so Channels DVR routes guide data through Gracenote
    rather than our XMLTV — the two cannot be mixed per source.
    """
    filters  = filters or {}
    base_url = (base_url or '').rstrip('/')

    lines = ['#EXTM3U']
    for ch in _build_channel_query(filters).all():
        gracenote_id = _parse_gracenote_id(ch)
        if not gracenote_id:
            continue
        attrs = [
            f'tvc-guide-stationid="{gracenote_id}"',
            f'tvg-name="{_esc(ch.name)}"',
            f'group-title="{_esc(ch.category or ch.source.display_name)}"',
        ]
        if ch.logo_url:
            attrs.append(f'tvg-logo="{ch.logo_url}"')
        if ch.number:
            attrs.append(f'tvg-chno="{ch.number}"')
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{ch.name}')
        lines.append(f'{base_url}/play/{ch.source.name}/{ch.source_channel_id}.m3u8')

    return '\n'.join(lines)


def _tvg_id(ch) -> str:
    return f'{ch.source.name}.{ch.source_channel_id}'


def _esc(s):
    return (s or '').replace('"', "'")

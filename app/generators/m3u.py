from ..models import Channel, Source


def generate_m3u(filters: dict = None, base_url: str = None) -> str:
    filters  = filters or {}
    base_url = (base_url or '').rstrip('/')

    channels = _build_channel_query(filters) \
        .order_by(Channel.number.asc().nullslast(), Channel.name.asc()).all()

    lines = ['#EXTM3U']
    for ch in channels:
        tvg_id = _tvg_id(ch)
        group  = _group_title(ch)
        attrs  = [
            f'tvg-id="{tvg_id}"',
            f'tvg-name="{_esc(ch.name)}"',
            f'group-title="{_esc(group)}"',
        ]
        if ch.logo_url:
            attrs.append(f'tvg-logo="{ch.logo_url}"')
        if ch.number:
            attrs.append(f'tvg-chno="{ch.number}"')
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{ch.name}')
        lines.append(f'{base_url}/play/{ch.source.name}/{ch.source_channel_id}.m3u8')

    return '\n'.join(lines)


def _build_channel_query(filters: dict):
    """
    Shared channel query used by both generate_m3u() and generate_xmltv_stream().
    Returns a SQLAlchemy query (not yet executed) so callers can add
    their own ordering / slicing.
    """
    q = Channel.query.join(Source).filter(
        Channel.is_active  == True,
        Channel.is_enabled == True,
        Source.is_enabled  == True,
        Channel.stream_url != None,
    )
    # Accept both singular (query-string style) and plural (feed filters style)
    sources    = filters.get('sources') or filters.get('source') or []
    categories = filters.get('categories') or filters.get('category') or []
    languages  = filters.get('languages') or (
                     [filters['language']] if filters.get('language') else [])

    if sources:
        q = q.filter(Source.name.in_(sources))
    if categories:
        # Category filter: match channels where ANY of their semicolon-joined
        # categories contains one of the requested categories.
        from sqlalchemy import or_
        cat_filters = [Channel.category.ilike(f'%{c}%') for c in categories]
        q = q.filter(or_(*cat_filters))
    if languages:
        q = q.filter(Channel.language.in_(languages))
    if search := filters.get('search'):
        q = q.filter(Channel.name.ilike(f'%{search}%'))

    # Hard cap — applied after all filters
    if max_ch := filters.get('max_channels'):
        q = q.limit(int(max_ch))

    return q


def _group_title(ch) -> str:
    """
    Build the group-title value.
    Format: "{category};{source display_name}"
    e.g. "Sports;Pluto TV"  or  "News;Tubi TV"

    If no category exists, falls back to just the source display name
    so every channel always has a group.
    """
    source_label = ch.source.display_name
    category     = (ch.category or '').strip()

    if category:
        return f'{category};{source_label}'
    return source_label


def _tvg_id(ch) -> str:
    """
    Stable, human-readable tvg-id that survives re-scrapes and DB rebuilds.
    Format: "{source_name}.{source_channel_id}"
    e.g. "pluto.abc-news-1"  or  "distro.12345"

    Must match exactly what generate_xmltv() writes into <channel id="...">
    and <programme channel="...">.
    """
    return f'{ch.source.name}.{ch.source_channel_id}'


def _esc(s):
    return (s or '').replace('"', "'")

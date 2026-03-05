from ..models import Channel, Source


def generate_m3u(filters: dict = None, base_url: str = None) -> str:
    filters  = filters or {}
    base_url = (base_url or '').rstrip('/')

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

    channels = query.order_by(Channel.number.asc().nullslast(), Channel.name.asc()).all()

    lines = ['#EXTM3U']
    for ch in channels:
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

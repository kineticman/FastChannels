from datetime import timezone
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

from ..models import Channel, Program, Source


def generate_xmltv(filters: dict = None, base_url: str = None) -> str:
    filters  = filters or {}
    base_url = (base_url or 'http://localhost:5523').rstrip('/')

    query = Channel.query.join(Source).filter(
        Channel.is_active  == True,
        Channel.is_enabled == True,
        Source.is_enabled  == True,
    )
    if sources := filters.get('source'):
        query = query.filter(Source.name.in_(sources))
    if categories := filters.get('category'):
        query = query.filter(Channel.category.in_(categories))

    channels = query.order_by(Channel.name.asc()).all()

    # Build lookup: db channel.id → stable tvg_id
    # Used to set <programme channel=""> consistently with <channel id="">
    tvg_map = {ch.id: _tvg_id(ch) for ch in channels}

    channel_ids = list(tvg_map.keys())
    programs = (
        Program.query
        .filter(Program.channel_id.in_(channel_ids))
        .order_by(Program.start_time.asc())
        .all()
    )

    root = Element('tv', attrib={
        'generator-info-name': 'FastChannels',
        'generator-info-url':  base_url,
    })

    for ch in channels:
        el = SubElement(root, 'channel', id=tvg_map[ch.id])
        SubElement(el, 'display-name').text = ch.name
        if ch.logo_url:
            SubElement(el, 'icon', src=ch.logo_url)

    for prog in programs:
        tvg_id = tvg_map.get(prog.channel_id)
        if not tvg_id:
            continue   # channel was filtered out
        el = SubElement(root, 'programme', attrib={
            'start':   _dt(prog.start_time),
            'stop':    _dt(prog.end_time),
            'channel': tvg_id,
        })
        SubElement(el, 'title', lang='en').text = prog.title
        if prog.description:
            SubElement(el, 'desc', lang='en').text = prog.description
        if prog.category:
            SubElement(el, 'category', lang='en').text = prog.category
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

    raw = tostring(root, encoding='unicode')
    return minidom.parseString(raw).toprettyxml(indent='  ', encoding=None)


def _tvg_id(ch) -> str:
    """
    Stable tvg-id — must be identical to what generate_m3u() produces.
    Format: "{source_name}.{source_channel_id}"
    e.g. "pluto.abc-news-1"  or  "distro.12345"
    """
    return f'{ch.source.name}.{ch.source_channel_id}'


def _dt(dt) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime('%Y%m%d%H%M%S %z')

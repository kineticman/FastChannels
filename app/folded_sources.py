"""Sources folded into Free Networks as networks, and the names they keep.

Adult Swim, Bally Sports Live and Hallmark were sources of their own before
they became Free Networks networks. Their old source name is the network key,
so a moved channel's source_channel_id is "<old source>.<old id>". Everything
a client or a feed could have stored keeps the old identity:

- tvg-id / XMLTV channel id: "<old source>.<old id>", which is just the
  channel's source_channel_id (Channels DVR keys its guide mapping, favorites
  and hidden channels on it).
- /play/<old source>/<old id>.m3u8 URLs still play (url_value_preprocessor in
  app/routes/play.py).
- Feed source filters and source orders: the old name still picks those
  channels, and "free_networks" picks Free Networks' other channels, so every
  saved feed selects exactly what it did (source_clause).
- Community Gracenote rows under the old provider name still match
  (app/gracenote_map.py).

The channel rows themselves are moved in place by app/schema.py
(_fold_into_free_networks), keeping their ids and so their numbers, locks,
Gracenote IDs, overrides and enabled state.

No ORM at import: app/schema.py imports this before the schema is current.
"""
from __future__ import annotations

HOST = 'free_networks'
# old source name (= network key) -> the name feeds show for it
FOLDED: dict[str, str] = {
    'adultswim': 'Adult Swim',
    'ballysports': 'Bally Sports Live',
    'hallmark': 'Hallmark',
}


def folded_key(source_name: str | None, source_channel_id: str | None) -> str | None:
    """The folded source a channel belongs to, or None."""
    if source_name != HOST or not source_channel_id or '.' not in source_channel_id:
        return None
    key = source_channel_id.split('.', 1)[0]
    return key if key in FOLDED else None


def _names(ch) -> tuple[str | None, str | None]:
    src = getattr(ch, 'source', None)
    return getattr(src, 'name', None), getattr(ch, 'source_channel_id', None)


def output_source_name(ch) -> str | None:
    """The source name a channel goes by in feeds and tvg-ids: the old source
    name for a folded channel, else its source's name."""
    name, scid = _names(ch)
    return folded_key(name, scid) or name


def output_source_display(ch) -> str:
    name, scid = _names(ch)
    key = folded_key(name, scid)
    if key:
        return FOLDED[key]
    src = getattr(ch, 'source', None)
    return getattr(src, 'display_name', None) or name or ''


def tvg_id(ch) -> str:
    name, scid = _names(ch)
    if folded_key(name, scid):
        return scid
    return f'{name}.{scid}'


def legacy_play_target(source_name: str, channel_id: str) -> tuple[str, str] | None:
    """(source, channel id) an old /play/<folded source>/<id> URL now means."""
    if source_name in FOLDED:
        return HOST, f'{source_name}.{channel_id}'
    return None


def _prefix(keys):
    from sqlalchemy import or_
    from .models import Channel
    return or_(*[Channel.source_channel_id.like(f'{k}.%') for k in sorted(keys)])


def source_clause(names):
    """SQL clause for channels whose output_source_name() is in `names`.
    The query must join Source."""
    from sqlalchemy import and_, false, not_, or_
    from .models import Source
    names = {str(n) for n in names or [] if n}
    folded = names & set(FOLDED)
    plain = names - set(FOLDED) - {HOST}
    parts = []
    if plain:
        parts.append(Source.name.in_(plain))
    if HOST in names:
        parts.append(and_(Source.name == HOST, not_(_prefix(FOLDED))))
    if folded:
        parts.append(and_(Source.name == HOST, _prefix(folded)))
    return or_(*parts) if parts else false()


def exclude_clause(names):
    from sqlalchemy import not_
    return not_(source_clause(names))

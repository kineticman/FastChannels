from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ..base import ProgramData

logger = logging.getLogger('app.scrapers.free_networks')

UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36'
ET = ZoneInfo('America/New_York')
UTC = timezone.utc


@dataclass
class NetChannel:
    """One channel of a network. `id` is the network's own key for it and is what
    programs() uses to match schedules back to channels; `stream` is whatever
    resolve() needs (usually just the live HLS URL)."""
    id: str
    name: str
    stream: str
    logo: str | None = None
    category: str | None = None
    language: str = 'en'
    country: str = 'US'
    gracenote_id: str | None = None


class Network:
    """A free network's channels and guide. Networks are stateless; `s` is the
    FreeNetworksScraper running them, for HTTP (s.get / s.session / s.impersonated)
    and the source cache."""
    key = ''
    name = ''

    def channels(self, s) -> list[NetChannel]:
        raise NotImplementedError

    def programs(self, s, channels: list[NetChannel]) -> list[ProgramData] | None:
        """Guide entries for these channels. Return None when the guide was
        deliberately not fetched this run (throttled, blocked) so stored
        programs are kept untouched."""
        return []

    def has_guide(self, channel_id: str) -> bool:
        """Whether programs() ever covers this channel. Channels it doesn't get
        tvtv data or placeholder blocks; channels it does are never given
        placeholders, so a failed fetch can't overwrite their real guide."""
        return type(self).programs is not Network.programs

    def borrows_tvtv(self, channel_id: str) -> bool:
        """Whether a channel with no guide of its own may take tvtv's guide for
        its Gracenote ID. False for channels whose Gracenote schedule is known
        not to match the stream; they keep the ID but get placeholders."""
        return True

    def resolve(self, s, ref: str) -> str:
        return ref


def parse_iso(val) -> datetime | None:
    if not val:
        return None
    try:
        dt = datetime.fromisoformat(str(val).replace('Z', '+00:00'))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def from_epoch(val) -> datetime | None:
    try:
        n = float(val)
    except (TypeError, ValueError):
        return None
    if n > 1e11:  # milliseconds
        n /= 1000
    return datetime.fromtimestamp(n, UTC)


def clean(text) -> str | None:
    """Strip tags and collapse whitespace; None for empty."""
    if text is None:
        return None
    import html
    out = re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]+>', ' ', str(text)))).strip()
    return out or None


def program(channel_id: str, title, start: datetime | None, end: datetime | None, **kw) -> ProgramData | None:
    """ProgramData with basic sanity checks; None if unusable."""
    title = clean(title)
    if not title or not start or not end or end <= start:
        return None
    return ProgramData(
        source_channel_id=channel_id,
        title=title[:512],
        start_time=start.astimezone(UTC),
        end_time=end.astimezone(UTC),
        **{k: (clean(v) if k in ('description', 'episode_title') else v) for k, v in kw.items()},
    )


def chain_ends(rows: list[tuple[datetime, dict]], max_gap_hours: float = 4, fallback_minutes: int = 60):
    """For schedules that only give start times: each row ends at the next row's
    start, unless that's further than max_gap_hours (an unlisted block), in which
    case it gets fallback_minutes. Yields (start, end, row)."""
    from datetime import timedelta
    rows = sorted(rows, key=lambda r: r[0])
    for i, (start, row) in enumerate(rows):
        nxt = rows[i + 1][0] if i + 1 < len(rows) else None
        if nxt and timedelta(0) < nxt - start <= timedelta(hours=max_gap_hours):
            end = nxt
        else:
            end = start + timedelta(minutes=fallback_minutes)
        yield start, end, row


def xmltv_programs(channel_id: str, content: bytes) -> list[ProgramData]:
    """ProgramData from an XMLTV document (all its programmes, as one channel).
    Handles both '20261017175530 +0000' and '2026-10-16T10:00:00.000+0000'
    start/stop forms."""
    import xml.etree.ElementTree as ET

    def when(val):
        for fmt in ('%Y%m%d%H%M%S %z', '%Y-%m-%dT%H:%M:%S.%f%z', '%Y-%m-%dT%H:%M:%S%z'):
            try:
                return datetime.strptime(val or '', fmt)
            except ValueError:
                continue
        return None

    out = []
    for el in ET.fromstring(content).iter('programme'):
        icons = [i.get('src') for i in el.findall('icon') if i.get('src')]
        season = episode = None
        ns = next((e.text for e in el.findall('episode-num') if e.get('system') == 'xmltv_ns'), None)
        if ns and ns.count('.') == 2:
            try:
                season, episode = int(ns.split('.')[0]) + 1, int(ns.split('.')[1]) + 1
            except ValueError:
                pass
        p = program(channel_id, el.findtext('title'), when(el.get('start')), when(el.get('stop')),
                    episode_title=el.findtext('sub-title'), description=el.findtext('desc'),
                    category=el.findtext('category'), poster_url=icons[0] if icons else None,
                    season=season, episode=episode)
        if p:
            out.append(p)
    return out

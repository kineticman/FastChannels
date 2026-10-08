# app/scrapers/playlist.py
"""
User-supplied playlists: an M3U URL, optionally paired with an XMLTV guide.

Each playlist a user adds is its own Source row named `m3u_<slug>`, so it gets
its own schedule, enable toggle, channel-number range and feed filter like any
built-in source. There is one scraper class for all of them — the registry
resolves any `m3u_*` name to a per-source subclass of PlaylistScraper (see
registry.get), and the playlist's URLs live in that source's config.

Streams are passed through untouched (plain 302 at play time). Guide data is
imported into Program rows and re-emitted by our own XMLTV generator, so feeds,
numbering and enable/disable apply to these channels the same as scraped ones.
"""
from __future__ import annotations

import logging
import os
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit

import requests

from .base import (
    BaseScraper, ChannelData, ConfigField, ProgramData, ScrapeSkipError,
    infer_language_from_metadata,
)
from .m3u_import import (
    M3UEntry, M3UPlaylist, assign_channel_ids, decode_m3u, parse_m3u, slugify,
)
from .xmltv_import import (
    XMLTVTooLargeError, close_open_ends, iter_xmltv, program_from_xmltv,
)

logger = logging.getLogger(__name__)

# Source.name prefix for playlist sources. Permanent: it is part of every
# tvg-id and play URL these channels are published under.
SOURCE_PREFIX = 'm3u_'
MAX_SLUG_LEN = 40

DEFAULT_MAX_CHANNELS = 2000
MAX_M3U_BYTES = 50 * 1024 * 1024
MAX_XMLTV_DOWNLOAD_BYTES = 400 * 1024 * 1024   # as served (usually gzipped)

# The XMLTV we publish covers a rolling 5 days; a little past that is plenty.
# Up to this many channels, the guide is imported for all of them, enabled or
# not. A playlist's channels usually arrive in the review queue; without this
# they would have no guide when approved, until the next scheduled refresh.
# Bigger playlists import the guide for enabled channels only, to bound rows.
GUIDE_ALL_CHANNELS_MAX = 500

EPG_PAST_HOURS = 2
EPG_FUTURE_DAYS = 7

_LANGUAGE_NAMES = {
    'english': 'en', 'spanish': 'es', 'espanol': 'es', 'español': 'es',
    'french': 'fr', 'german': 'de', 'italian': 'it', 'portuguese': 'pt',
    'dutch': 'nl', 'russian': 'ru', 'arabic': 'ar', 'hindi': 'hi',
    'chinese': 'zh', 'japanese': 'ja', 'korean': 'ko', 'turkish': 'tr',
    'polish': 'pl', 'greek': 'el', 'maori': 'mi',
}


class PlaylistError(ScrapeSkipError):
    """A playlist or its guide couldn't be fetched or parsed. A skip, not a
    crash: the worker records the message and leaves existing channels alone."""


def is_playlist_source(name: str | None) -> bool:
    return bool(name) and name.startswith(SOURCE_PREFIX)


def redact_url(url: str | None) -> str:
    """URL safe to log or show in last_error: no credentials, no query string.
    Playlist URLs routinely carry a username and password in the query."""
    try:
        parts = urlsplit(url or '')
    except ValueError:
        return '(invalid URL)'
    host = parts.hostname or ''
    if parts.port:
        host = f'{host}:{parts.port}'
    return urlunsplit((parts.scheme, host, parts.path, '…' if parts.query else '', ''))


def validate_url(url: str | None, label: str) -> str:
    url = (url or '').strip()
    try:
        parts = urlsplit(url)
    except ValueError:
        parts = None
    if not parts or parts.scheme not in ('http', 'https') or not parts.hostname:
        raise PlaylistError(f'{label} must be an http:// or https:// URL')
    return url


def _download(session: requests.Session, url: str, label: str, limit: int,
              user_agent: str | None = None, dest=None) -> bytes | None:
    """GET `url` with a hard size cap. Returns the body, or writes it to `dest`
    (a binary file object) and returns None. Errors never include the full URL."""
    headers = {'User-Agent': user_agent} if user_agent else {}
    try:
        with session.get(url, headers=headers, stream=True, timeout=(15, 120)) as r:
            if r.status_code >= 400:
                raise PlaylistError(f'{label} request failed: HTTP {r.status_code} from {redact_url(url)}')
            # raw + decode_content: honour Content-Encoding, but count the
            # bytes we actually keep.
            r.raw.decode_content = True
            chunks, total = [], 0
            while True:
                chunk = r.raw.read(256 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > limit:
                    raise PlaylistError(f'{label} is larger than {limit // (1024 * 1024)} MB; refusing to import it')
                if dest is not None:
                    dest.write(chunk)
                else:
                    chunks.append(chunk)
            return None if dest is not None else b''.join(chunks)
    except PlaylistError:
        raise
    except Exception as exc:
        # Deliberately not chained: requests' messages embed the full URL, and
        # a user's unreachable playlist host is not a network outage for the
        # worker to back off every other source over.
        raise PlaylistError(
            f'{label} could not be fetched from {redact_url(url)}: {type(exc).__name__}'
        ) from None


def fetch_playlist(session: requests.Session, url: str, user_agent: str | None = None) -> M3UPlaylist:
    url = validate_url(url, 'Playlist URL')
    body = _download(session, url, 'Playlist', MAX_M3U_BYTES, user_agent)
    text = decode_m3u(body)
    if '#EXTINF' not in text.upper():
        raise PlaylistError('That URL did not return an M3U playlist (no #EXTINF entries found)')
    playlist = parse_m3u(text)
    if not playlist.entries:
        raise PlaylistError('The playlist has no entries with a usable http(s) stream URL')
    return playlist


def _as_list(value) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if str(v).strip()]
    return [part.strip() for part in str(value or '').split(',') if part.strip()]


def filter_entries(entries: list[M3UEntry], config: dict) -> list[M3UEntry]:
    raw_groups = config.get('groups_include')
    # Kept as-is, blanks included: '' selects entries with no group-title.
    groups = {str(g).strip().casefold() for g in raw_groups} if isinstance(raw_groups, (list, tuple)) else set()
    excludes = [x.casefold() for x in _as_list(config.get('name_exclude'))]
    out = []
    for entry in entries:
        if groups and entry.group.casefold() not in groups:
            continue
        name = entry.name.casefold()
        if any(x in name for x in excludes):
            continue
        out.append(entry)
    return out


def max_channels(config: dict) -> int:
    try:
        return max(1, int(config.get('max_channels') or DEFAULT_MAX_CHANNELS))
    except (TypeError, ValueError):
        return DEFAULT_MAX_CHANNELS


def _language(entry: M3UEntry, default: str) -> str:
    """An entry's own tvg-language wins. Without one, fall back to the same
    name-based Spanish detection the built-in scrapers use, then to the
    playlist's default."""
    raw = entry.attr('tvg-language').split(';')[0].split(',')[0].strip().casefold()
    if raw in _LANGUAGE_NAMES:
        return _LANGUAGE_NAMES[raw]
    if 2 <= len(raw) <= 3 and raw.isalpha():
        return raw
    return infer_language_from_metadata(entry.name, entry.group, default=default)


def _country(entry: M3UEntry, default: str) -> str:
    raw = entry.attr('tvg-country').split(';')[0].split(',')[0].strip().upper()
    return raw if len(raw) == 2 and raw.isalpha() else default


def _stream_type(url: str) -> str:
    path = urlsplit(url).path.lower()
    if '.mpd' in path:
        return 'dash'
    if path.endswith('.ts'):
        return 'mpegts'
    return 'hls'


def _gracenote_id(entry: M3UEntry) -> str | None:
    from ..gracenote_map import normalize_gracenote_id
    raw = entry.attr('tvc-guide-stationid')
    return normalize_gracenote_id(raw) if raw else None


def _split_values(raw: str) -> list[str]:
    return [part.strip() for part in raw.replace(';', ',').split(',') if part.strip()]


def _raw_category(group: str, genres: list[str]) -> str | None:
    """The raw category handed to category_for_channel(). Some playlists put
    several genres in one group-title ("Movies;Series"); that string as a whole
    normalises to nothing, so prefer the first part that does."""
    from .category_utils import normalize_category
    parts = [p.strip() for p in group.split(';') if p.strip()]
    if len(parts) > 1:
        for part in parts:
            if normalize_category(part):
                return part
    return group or (genres[0] if genres else None)


def _placeholder_minutes(entry: M3UEntry) -> int | None:
    """tvc-guide-placeholders is the placeholder block length in seconds."""
    raw = entry.attr('tvc-guide-placeholders')
    if not raw.isdigit():
        return None
    return min(max(int(raw) // 60, 5), 24 * 60)


def entries_to_channels(entries: list[M3UEntry], config: dict) -> list[ChannelData]:
    default_language = (config.get('default_language') or 'en').strip().lower() or 'en'
    default_country = (config.get('default_country') or 'US').strip().upper() or 'US'
    channels = []
    for channel_id, entry in assign_channel_ids(entries):
        group = entry.group
        # Channels DVR's per-channel guide hints. tvc-guide-genres/-tags are
        # free-form labels; kept with the group name as the channel's raw tags.
        genres = _split_values(entry.attr('tvc-guide-genres'))
        tags = ([group] if group else []) + [
            t for t in genres + _split_values(entry.attr('tvc-guide-tags')) if t != group
        ]
        guide_title = entry.attr('tvc-guide-title')
        channels.append(ChannelData(
            source_channel_id=channel_id,
            name=entry.name,
            stream_url=entry.url,
            logo_url=entry.attr('tvg-logo') or None,
            slug=slugify(entry.name) or channel_id,
            # Raw group name in; the worker runs it through
            # category_for_channel() like every other source's category.
            category=_raw_category(group, genres),
            language=_language(entry, default_language),
            country=_country(entry, default_country),
            stream_type=_stream_type(entry.url),
            # The playlist's own number is kept as text only — the integer
            # allocator owns Channel.number.
            provider_number=entry.attr('tvg-chno', 'channel-number') or None,
            gracenote_id=_gracenote_id(entry),
            # The untouched tvg-id is what the XMLTV guide is keyed on.
            guide_key=entry.attr('tvg-id') or None,
            tags=tags,
            # Shown in the placeholder guide blocks of a channel the XMLTV
            # doesn't cover, and passed on in our own M3U.
            description=entry.attr('tvc-guide-description', 'tvg-description') or None,
            guide_title=guide_title if guide_title and guide_title != entry.name else None,
            guide_art=entry.attr('tvc-guide-art') or None,
            guide_block_minutes=_placeholder_minutes(entry),
        ))
    return channels


class PlaylistScraper(BaseScraper):
    # No source_name: this class is never registered or seeded itself. The
    # registry hands out a subclass per `m3u_*` source with the name filled in.
    source_name = None
    display_name = 'Playlist'
    source_category = 'custom'
    scrape_interval = 720
    min_scrape_interval = 60
    stream_audit_enabled = False
    epg_quality = 'full'

    phase_timeouts = {
        'init':      30,
        'bootstrap': 60,
        'channels':  300,
        'epg':       1800,
    }

    # The playlist and guide URLs, groups, language and country are edited in
    # the playlist dialog (which validates and previews them), not here.
    config_schema = [
        ConfigField(
            'name_exclude', 'Skip channels containing',
            placeholder='e.g. backup, 4K, [VOD]',
            help_text='Comma-separated. Any channel whose name contains one of these is left out of the import.',
        ),
        ConfigField(
            'max_channels', 'Channel limit', field_type='number',
            default=DEFAULT_MAX_CHANNELS,
            help_text='The import refuses to run if the playlist (after the group filter) has more channels than this.',
        ),
        ConfigField(
            'user_agent', 'User-Agent',
            placeholder='Leave blank unless the playlist host requires one',
            help_text='Sent when downloading the playlist and guide. Not sent to the streams themselves.',
        ),
    ]

    def _user_agent(self) -> str | None:
        return (self.config.get('user_agent') or '').strip() or None

    # ── channels ─────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        # Any failure raises. Returning [] would read as "every channel is gone"
        # to the reconcile step.
        playlist = fetch_playlist(self.session, self.config.get('m3u_url'), self._user_agent())
        entries = filter_entries(playlist.entries, self.config)
        if not entries:
            raise PlaylistError(
                f'The playlist has {len(playlist.entries)} channels but none match the selected groups'
            )
        limit = max_channels(self.config)
        if len(entries) > limit:
            raise PlaylistError(
                f'The playlist has {len(entries)} channels after filtering, over the limit of {limit}. '
                'Select fewer groups or raise the channel limit.'
            )
        channels = entries_to_channels(entries, self.config)
        with_headers = sum(1 for e in entries if e.headers)
        logger.info('[%s] %d channels from playlist (%d skipped without a stream URL, %d declare request headers)',
                    self.source_name, len(channels), playlist.skipped, with_headers)
        return channels

    # ── EPG ──────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        epg_url = (self.config.get('epg_url') or '').strip()
        if not epg_url:
            return []
        epg_url = validate_url(epg_url, 'Guide URL')

        enabled_ids = kwargs.get('enabled_ids')
        if len(channels) <= GUIDE_ALL_CHANNELS_MAX:
            enabled_ids = None
        wanted = [ch for ch in channels if enabled_ids is None or ch.source_channel_id in enabled_ids]
        if not wanted:
            logger.info('[%s] guide: skipped, no enabled channels to import it for', self.source_name)
            return []

        # XMLTV channel id → our channel ids. Several playlist entries can
        # share one guide channel (HD/SD variants).
        by_guide_id: dict[str, list[str]] = {}
        by_name: dict[str, list[str]] = {}
        for ch in wanted:
            if ch.guide_key:
                by_guide_id.setdefault(ch.guide_key.casefold(), []).append(ch.source_channel_id)
            else:
                # No tvg-id: fall back to matching the guide's display-name.
                by_name.setdefault(ch.name.casefold(), []).append(ch.source_channel_id)

        now = datetime.now(timezone.utc)
        window_start = now - timedelta(hours=EPG_PAST_HOURS)
        window_end = now + timedelta(days=EPG_FUTURE_DAYS)

        fd, path = tempfile.mkstemp(prefix='fc-xmltv-', suffix='.tmp')
        programs: list[ProgramData] = []
        try:
            with os.fdopen(fd, 'wb') as fp:
                _download(self.session, epg_url, 'Guide', MAX_XMLTV_DOWNLOAD_BYTES, self._user_agent(), dest=fp)

            targets: dict[str, list[str]] = {}
            try:
                for tag, el in iter_xmltv(path):
                    if tag == 'channel':
                        xmltv_id = el.get('id') or ''
                        sids = list(by_guide_id.get(xmltv_id.casefold(), ()))
                        if not sids and by_name:
                            for dn in el.findall('display-name'):
                                sids = by_name.get((dn.text or '').strip().casefold(), [])
                                if sids:
                                    break
                        if sids:
                            targets.setdefault(xmltv_id, []).extend(sids)
                        continue

                    xmltv_id = el.get('channel') or ''
                    # A guide that lists <programme> before (or without) its
                    # <channel> elements still matches on tvg-id.
                    sids = targets.get(xmltv_id) or by_guide_id.get(xmltv_id.casefold())
                    if not sids:
                        continue
                    first = program_from_xmltv(sids[0], el, allow_open_end=True)
                    if first is None or first.start_time > window_end:
                        continue
                    if first.end_time is not None and first.end_time < window_start:
                        continue
                    programs.append(first)
                    for sid in sids[1:]:
                        programs.append(program_from_xmltv(sid, el, allow_open_end=True))
            except ET.ParseError as exc:
                raise PlaylistError(f'Guide is not valid XMLTV: {exc}') from None
            except (OSError, EOFError) as exc:
                raise PlaylistError(f'Guide could not be read (truncated or corrupt download): {type(exc).__name__}') from None
            except XMLTVTooLargeError as exc:
                raise PlaylistError(str(exc)) from None
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

        programs = [p for p in close_open_ends(programs) if p.end_time >= window_start]
        matched = len({p.source_channel_id for p in programs})
        logger.info('[%s] guide: %d programmes for %d of %d channels',
                    self.source_name, len(programs), matched, len(wanted))
        return programs

    # ── resolve ──────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        return raw_url

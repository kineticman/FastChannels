from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, unquote

from ..base import BaseScraper, ChannelData, ConfigField, ProgramData
from ...gracenote_map import resolve_gracenote
from ._common import UA, NetChannel
from . import (
    aetn, aljazeera, arirang, bloomberg, cbsnews, dw, ewtn, fstv, hsn, newsmax, nhk,
    positiv, qvc, scripps, simple, sinclair, tastemade, trt,
)

logger = logging.getLogger(__name__)

NETWORKS = [
    sinclair.Sinclair(), dw.DW(), aljazeera.AlJazeera(), trt.TRT(), nhk.NHK(),
    arirang.Arirang(), bloomberg.Bloomberg(), cbsnews.CBSNews(), scripps.Scripps(),
    newsmax.Newsmax(), fstv.FreeSpeechTV(), ewtn.EWTN(), positiv.PosiTV(),
    hsn.HSN(), qvc.QVC(), tastemade.Tastemade(), aetn.AETN(), *simple.NETWORKS,
]
_BY_KEY = {n.key: n for n in NETWORKS}


class FreeNetworksScraper(BaseScraper):
    """
    Free networks that stream on their own websites: news (DW, Al Jazeera, TRT,
    NHK, Arirang, Bloomberg, CBS News, Scripps, Newsmax2, Free Speech TV, Sky
    News), faith (EWTN, PosiTV), shopping (HSN, QVC), Sinclair's diginets
    (Comet, Charge!, TheNest), Tastemade, A+E's FAST channels, Outside TV and a
    few singles.

    Each network is a small module in this package (see _common.Network) with
    its own channel list and guide fetch; one failing never takes the others
    down. Channel IDs are "<network>.<id>" and stream URLs are opaque
    "freenet://<network>/<ref>", resolved by the network at play time.

    Channels whose network has no guide of its own use the tvtv cache for their
    Gracenote ID, if they have one (built-in, community CSV under provider
    'free_networks', or set by the user); with neither, they get hourly blocks
    named after the channel. A network whose guide fetch fails or is skipped
    (Network.programs returning None) keeps its stored guide.
    """

    source_name = 'free_networks'
    display_name = 'Free Networks'
    scrape_interval = 360
    stream_audit_enabled = True
    source_category = 'specialty'
    config_schema = [
        ConfigField(
            'disabled_networks', 'Networks', field_type='checklist',
            options=[{'value': n.key, 'label': n.name} for n in sorted(NETWORKS, key=lambda n: n.name.lower())],
            help_text='Untick a network to drop its channels. New networks start out on.',
        ),
    ]
    phase_timeouts = {
        'init':      30,
        'bootstrap': 60,
        'channels':  300,
        'epg':       900,
    }

    _SCHEME = 'freenet://'

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({'User-Agent': UA})
        self._memo: dict = {}
        self._imp_session = None

    # ── HTTP helpers for networks ────────────────────────────

    def impersonated(self):
        """curl_cffi session with a Chrome TLS fingerprint, for sites behind
        bot protection (Bloomberg's PerimeterX, CBS News)."""
        if self._imp_session is None:
            from curl_cffi import requests as cffi_requests
            self._imp_session = cffi_requests.Session(impersonate='chrome')
        return self._imp_session

    def memo(self, key, fn):
        """Per-run memo so channels() and programs() can share one fetch."""
        if key not in self._memo:
            self._memo[key] = fn()
        return self._memo[key]

    # ── Required ─────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        out: list[ChannelData] = []
        disabled = self.disabled_networks(self.config)
        if disabled:
            logger.info('[%s] skipping disabled networks: %s', self.source_name, ', '.join(sorted(disabled)))
        for net in NETWORKS:
            if net.key in disabled:
                continue
            t0 = time.monotonic()
            try:
                chans = net.channels(self)
            except Exception as exc:
                logger.warning('[%s] %s channels failed: %s', self.source_name, net.name, exc)
                continue
            for ch in chans:
                out.append(self._channel_data(net, ch))
            logger.info('[%s] %s: %d channels (%.1fs)', self.source_name, net.name,
                        len(chans), time.monotonic() - t0)
        logger.info('[%s] %d channels from %d networks', self.source_name, len(out),
                    len(NETWORKS) - len(disabled))
        return out

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        by_net: dict[str, list[tuple[ChannelData, NetChannel]]] = {}
        disabled = self.disabled_networks(self.config)
        for cd in channels:
            parsed = self._split_id(cd.source_channel_id)
            if not parsed:
                continue
            key, local_id = parsed
            if key in _BY_KEY and key not in disabled:
                ref = self._split_url(cd.stream_url)
                nc = NetChannel(id=local_id, name=cd.name, stream=ref[1] if ref else '')
                by_net.setdefault(key, []).append((cd, nc))

        gracenote = self._gracenote_ids(channels)
        programs: list[ProgramData] = []
        for key, pairs in by_net.items():
            net = _BY_KEY[key]
            t0 = time.monotonic()
            skipped = False
            try:
                progs = net.programs(self, [nc for _, nc in pairs])
            except Exception as exc:
                logger.warning('[%s] %s guide failed: %s — keeping stored guide', self.source_name, net.name, exc)
                progs = None
            if progs is None:
                skipped, progs = True, []
            covered = set()
            for p in progs:
                covered.add(p.source_channel_id)
                p.source_channel_id = f'{key}.{p.source_channel_id}'
            programs.extend(progs)

            # Borrow the tvtv cache for channels this network gave no guide for;
            # failing that, fill hourly blocks named after the channel (what
            # custom channels get) so clients don't show an empty row. A skipped
            # or failed fetch leaves the network's guided channels untouched.
            borrowed = filled = 0
            for cd, nc in pairs:
                if nc.id in covered or (skipped and net.has_guide(nc.id)):
                    continue
                gid = gracenote.get(cd.source_channel_id)
                rows = self._tvtv_programs(cd.source_channel_id, gid) if gid else []
                if rows:
                    borrowed += len(rows)
                elif not net.has_guide(nc.id):
                    rows = self._filler_programs(cd)
                    filled += 1
                programs.extend(rows)
            extra = ''.join([' (not fetched, kept stored guide)' if skipped else '',
                             f' + {borrowed} from tvtv' if borrowed else '',
                             f', {filled} channel(s) with placeholder blocks' if filled else ''])
            logger.info('[%s] %s: %d guide entries%s (%.1fs)', self.source_name, net.name, len(progs),
                        extra, time.monotonic() - t0)

        logger.info('[%s] %d total EPG entries', self.source_name, len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        parsed = self._split_url(raw_url)
        if not parsed:
            return raw_url
        key, ref = parsed
        net = _BY_KEY.get(key)
        return net.resolve(self, ref) if net else ref

    # ── Internals ────────────────────────────────────────────

    @staticmethod
    def disabled_networks(config: dict | None) -> set[str]:
        """Network keys the user unticked (the 'disabled_networks' checklist)."""
        raw = (config or {}).get('disabled_networks') or ''
        return {k.strip() for k in str(raw).split(',') if k.strip()}


    def _channel_data(self, net, ch: NetChannel) -> ChannelData:
        scid = f'{net.key}.{ch.id}'
        return ChannelData(
            source_channel_id=scid,
            name=ch.name,
            stream_url=f'{self._SCHEME}{net.key}/{quote(ch.stream, safe="")}',
            logo_url=ch.logo,
            category=ch.category,
            language=ch.language,
            country=ch.country,
            stream_type='hls',
            gracenote_id=resolve_gracenote(self.source_name, lookup_key=scid) or ch.gracenote_id,
        )

    @staticmethod
    def _split_id(scid: str | None) -> tuple[str, str] | None:
        if not scid or '.' not in scid:
            return None
        key, local_id = scid.split('.', 1)
        return key, local_id

    def _split_url(self, raw_url: str | None) -> tuple[str, str] | None:
        if not raw_url or not raw_url.startswith(self._SCHEME):
            return None
        rest = raw_url[len(self._SCHEME):]
        if '/' not in rest:
            return None
        key, ref = rest.split('/', 1)
        return key, unquote(ref)

    def _gracenote_ids(self, channels: list[ChannelData]) -> dict[str, str]:
        """source_channel_id -> Gracenote ID, preferring what's stored on the
        channel (a user's manual ID) over what the scrape supplied."""
        ids = {cd.source_channel_id: cd.gracenote_id for cd in channels if cd.gracenote_id}
        try:
            from app.models import Channel, Source
            src = Source.query.filter_by(name=self.source_name).first()
            if src:
                for scid, gid, mode in (Channel.query
                                        .filter(Channel.source_id == src.id, Channel.gracenote_id.isnot(None))
                                        .with_entities(Channel.source_channel_id, Channel.gracenote_id,
                                                       Channel.gracenote_mode)):
                    if (mode or 'auto') != 'off':
                        ids[scid] = gid
        except Exception:
            logger.debug('[%s] could not read stored Gracenote IDs', self.source_name, exc_info=True)
        return ids

    _FILLER_HOURS = 48

    @classmethod
    def _filler_programs(cls, cd: ChannelData) -> list[ProgramData]:
        start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        return [ProgramData(source_channel_id=cd.source_channel_id, title=cd.name,
                            start_time=start + timedelta(hours=h), end_time=start + timedelta(hours=h + 1),
                            category=cd.category)
                for h in range(-1, cls._FILLER_HOURS)]

    def stored_guide_end(self, net_key: str, channel_ids) -> datetime | None:
        """The earliest, across these channels, of how far each one's stored
        guide runs; None if any of them has no stored guide at all. Lets a
        throttled network tell 'keep what's stored' from 'nothing is stored'."""
        try:
            from app.models import Channel, Program, Source
            from sqlalchemy import func
            src = Source.query.filter_by(name=self.source_name).first()
            if not src:
                return None
            scids = [f'{net_key}.{cid}' for cid in channel_ids]
            ends = dict(Channel.query
                        .filter(Channel.source_id == src.id, Channel.source_channel_id.in_(scids))
                        .outerjoin(Program, Program.channel_id == Channel.id)
                        .group_by(Channel.id)
                        .with_entities(Channel.source_channel_id, func.max(Program.end_time))
                        .all())
        except Exception:
            logger.debug('[%s] could not read stored guide', self.source_name, exc_info=True)
            return None
        if not scids or any(ends.get(scid) is None for scid in scids):
            return None
        earliest = min(ends.values())
        return earliest if earliest.tzinfo else earliest.replace(tzinfo=timezone.utc)

    def _tvtv_programs(self, scid: str, station_id: str) -> list[ProgramData]:
        try:
            from app.models import TvtvProgramCache
            now = datetime.now(timezone.utc)
            rows = (TvtvProgramCache.query
                    .filter(TvtvProgramCache.station_id == str(station_id),
                            TvtvProgramCache.end_time > now - timedelta(hours=1))
                    .order_by(TvtvProgramCache.start_time)
                    .all())
        except Exception:
            logger.debug('[%s] tvtv cache unavailable', self.source_name, exc_info=True)
            return []
        out = []
        for r in rows:
            start = r.start_time if r.start_time.tzinfo else r.start_time.replace(tzinfo=timezone.utc)
            end = r.end_time if r.end_time.tzinfo else r.end_time.replace(tzinfo=timezone.utc)
            out.append(ProgramData(source_channel_id=scid, title=r.title, start_time=start,
                                   end_time=end, episode_title=r.subtitle or None))
        return out

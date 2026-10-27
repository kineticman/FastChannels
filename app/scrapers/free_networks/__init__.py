from __future__ import annotations

import logging
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, unquote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..base import BaseScraper, ChannelData, ConfigField, ProgramData
from ...gracenote_map import resolve_gracenote
from ._common import UA, NetChannel
from . import (
    aetn, aljazeera, arirang, beond, bloomberg, cbn, cbsnews, cineverse, cw, dw, ewtn, fstv, gameandfish, hsn, ksl, mansa,
    newsmax, newsnation, nhk, og_network, outside, positiv, qvc, scripps, simple, sinclair, tastemade, tbn, trt,
)

logger = logging.getLogger(__name__)

NETWORKS = [
    sinclair.Sinclair(), dw.DW(), aljazeera.AlJazeera(), trt.TRT(), nhk.NHK(),
    arirang.Arirang(), bloomberg.Bloomberg(), cbsnews.CBSNews(), scripps.Scripps(),
    newsmax.Newsmax(), newsnation.NewsNation(), fstv.FreeSpeechTV(), ewtn.EWTN(), positiv.PosiTV(), tbn.TBN(),
    hsn.HSN(), qvc.QVC(), tastemade.Tastemade(), aetn.AETN(), ksl.KSL(), cbn.CBN(),
    gameandfish.GameAndFish(), outside.OutsideTV(), cw.CW(), cineverse.Cineverse(), og_network.OGNetwork(), beond.BeOND(), mansa.Mansa(), *simple.NETWORKS,
]
_BY_KEY = {n.key: n for n in NETWORKS}

# A network taking at least this long gets its own INFO line; the rest only
# show up in the per-phase summary (per-network detail is at DEBUG).
_SLOW_NETWORK_SECS = 5.0

# Each network's last non-empty channel list, re-used when its listing fails
# or comes back empty. Reconcile would otherwise count every channel of a
# network whose listing endpoint is down as missing upstream and deactivate
# them after channel_miss_threshold scrapes (18h), though their streams are
# usually fine. Past _LAST_GOOD_MAX_AGE an empty network is taken at its word.
_LAST_GOOD_CACHE_KEY = 'free_networks_last_good_channels'
_LAST_GOOD_MAX_AGE = 3 * 86400


# Per-network time limits. Networks run one after another inside one phase
# timeout, so a single host that never answers would otherwise starve every
# network queued behind it. Normal: channel listings take under ~5s; Outside
# TV's guide takes 65-85s.
_CHANNELS_BUDGET_SECS = 60
_GUIDE_BUDGET_SECS = 180


_HOLE = timedelta(minutes=30)


def _has_hole(rows: list[ProgramData]) -> bool:
    """Whether a network's guide for one channel leaves a stretch of at least
    _HOLE uncovered between now and its last entry."""
    t = datetime.now(timezone.utc)
    for p in sorted(rows, key=lambda p: p.start_time):
        if p.start_time - t >= _HOLE:
            return True
        t = max(t, p.end_time)
    return False


def _outside(extra: list[ProgramData], rows: list[ProgramData]) -> list[ProgramData]:
    """The entries of `extra` that don't overlap any of `rows`."""
    spans = [(p.start_time, p.end_time) for p in rows]
    return [p for p in extra if not any(p.start_time < b and a < p.end_time for a, b in spans)]


class NetworkBudgetExceeded(requests.exceptions.Timeout):
    """A network used up its time limit. A Timeout, so the networks' own
    request-error handling treats it like any other timed-out request."""


class _BudgetAdapter(HTTPAdapter):
    """Caps every request's timeout at the running network's remaining time,
    and fails it at once when none is left."""

    def __init__(self, scraper, **kwargs):
        self._scraper = scraper
        super().__init__(**kwargs)

    def send(self, request, timeout=None, **kwargs):
        return super().send(request, timeout=self._scraper.budget_timeout(timeout), **kwargs)


class _BudgetedSession:
    """The curl_cffi session with the same per-network time limit."""

    def __init__(self, scraper, session):
        self._scraper = scraper
        self._session = session

    def get(self, url, **kwargs):
        kwargs['timeout'] = self._scraper.budget_timeout(kwargs.get('timeout', 30))
        return self._session.get(url, **kwargs)

    def post(self, url, **kwargs):
        kwargs['timeout'] = self._scraper.budget_timeout(kwargs.get('timeout', 30))
        return self._session.post(url, **kwargs)

    def __getattr__(self, name):
        return getattr(self._session, name)


def _reraise_if_phase_timeout(exc: Exception) -> None:
    # The worker's per-phase SIGALRM raises ScrapePhaseTimeoutError (an
    # Exception, defined in app.worker) inside whichever network is running.
    # Swallowed as that network's failure, the one-shot timer is spent and the
    # rest of the phase runs with no limit.
    if type(exc).__name__ == 'ScrapePhaseTimeoutError':
        raise exc


class FreeNetworksScraper(BaseScraper):
    """
    Free networks that stream on their own websites: news (DW, Al Jazeera, TRT,
    NHK, Arirang, Bloomberg, CBS News, Scripps, Newsmax2 + World at War, NewsNation, Free Speech TV, Sky
    News, Global News, KSL), faith (EWTN, PosiTV, TBN, CBN, NRB, theDove), shopping
    (HSN, QVC), Sinclair's diginets (Comet, Charge!, TheNest), Tastemade, A+E's
    FAST channels, The CW's FAST lineup, Cineverse's channels, OG Network, BeOND, Mansa, Outside TV, Game & Fish TV and a few
    singles.

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
    source_category = 'fast'
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
            self._imp_session = _BudgetedSession(self, cffi_requests.Session(impersonate='chrome'))
        return self._imp_session

    # ── Per-network time limit ───────────────────────────────

    _net_deadline: float | None = None

    def _retry_config(self) -> Retry:
        # One retry, not the base three: urllib3 retries inside a single send,
        # each attempt with the full (capped) timeout, so more retries would
        # let one request run several times past its network's limit.
        return Retry(total=2, connect=1, read=1, status=1, backoff_factor=1.0,
                     status_forcelist=(429, 500, 502, 503, 504), allowed_methods=None,
                     raise_on_status=False)

    def _configure_session(self, session) -> None:
        adapter = _BudgetAdapter(self, max_retries=self._retry_config())
        session.mount('https://', adapter)
        session.mount('http://', adapter)

    def budget_timeout(self, timeout):
        """timeout capped at what the running network has left. Raises
        NetworkBudgetExceeded once it has none. Unchanged outside a scrape
        (play-time resolve)."""
        if self._net_deadline is None:
            return timeout
        remaining = self._net_deadline - time.monotonic()
        if remaining <= 0:
            raise NetworkBudgetExceeded('network time limit reached')
        if timeout is None:
            return remaining
        if isinstance(timeout, tuple):
            return tuple(min(t, remaining) if t is not None else remaining for t in timeout)
        return min(timeout, remaining)

    def get(self, url: str, **kwargs):
        # Past the limit, skip quietly: _run_network logs the cut-off once,
        # where BaseScraper.get would log an ERROR for every skipped request.
        if self._net_deadline is not None and time.monotonic() >= self._net_deadline:
            return None
        return super().get(url, **kwargs)

    def _run_network(self, net, budget: int, fn, *args):
        """fn(*args) under the network's time limit."""
        self._net_deadline = time.monotonic() + budget
        try:
            return fn(*args)
        finally:
            over = time.monotonic() >= self._net_deadline
            self._net_deadline = None
            if over:
                logger.warning('[%s] %s hit its %ds time limit — cut short', self.source_name, net.name, budget)

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
        last_good = dict(self.cache.get(_LAST_GOOD_CACHE_KEY) or {})
        last_good_changed = False
        run_t0 = time.monotonic()
        for net in NETWORKS:
            if net.key in disabled:
                continue
            t0 = time.monotonic()
            try:
                chans = self._run_network(net, _CHANNELS_BUDGET_SECS, net.channels, self)
            except Exception as exc:
                _reraise_if_phase_timeout(exc)
                logger.warning('[%s] %s channels failed: %s', self.source_name, net.name, exc)
                chans = []
            if chans:
                last_good[net.key] = {'at': time.time(), 'channels': [asdict(ch) for ch in chans]}
                last_good_changed = True
            else:
                kept = last_good.get(net.key) or {}
                if kept.get('channels') and time.time() - float(kept.get('at') or 0) < _LAST_GOOD_MAX_AGE:
                    chans = [NetChannel(**c) for c in kept['channels']]
                    logger.info('[%s] %s listed no channels — keeping its last %d', self.source_name,
                                net.name, len(chans))
            for ch in chans:
                out.append(self._channel_data(net, ch))
            elapsed = time.monotonic() - t0
            noun = 'channel' if len(chans) == 1 else 'channels'
            if not chans:
                logger.info('[%s] %s: 0 channels (%.1fs)', self.source_name, net.name, elapsed)
            elif elapsed >= _SLOW_NETWORK_SECS:
                logger.info('[%s] %s channels slow: %d %s (%.1fs)', self.source_name, net.name,
                            len(chans), noun, elapsed)
            else:
                logger.debug('[%s] %s: %d %s (%.1fs)', self.source_name, net.name,
                             len(chans), noun, elapsed)
        if last_good_changed:
            self._update_cache(_LAST_GOOD_CACHE_KEY, last_good)
        logger.info('[%s] %d channels from %d networks (%.1fs)', self.source_name, len(out),
                    len(NETWORKS) - len(disabled), time.monotonic() - run_t0)
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
        native_total = borrowed_total = 0
        placeholders: dict[str, int] = {}
        run_t0 = time.monotonic()
        for key, pairs in by_net.items():
            net = _BY_KEY[key]
            t0 = time.monotonic()
            skipped = False
            try:
                progs = self._run_network(net, _GUIDE_BUDGET_SECS, net.programs, self, [nc for _, nc in pairs])
            except Exception as exc:
                _reraise_if_phase_timeout(exc)
                logger.warning('[%s] %s guide failed: %s — keeping stored guide', self.source_name, net.name, exc)
                progs = None
            if progs is None:
                skipped, progs = True, []
            covered: dict[str, list[ProgramData]] = {}
            for p in progs:
                covered.setdefault(p.source_channel_id, []).append(p)
                p.source_channel_id = f'{key}.{p.source_channel_id}'
            programs.extend(progs)

            # Borrow the tvtv cache for channels this network gave no guide for;
            # failing that, fill hourly blocks named after the channel (what
            # custom channels get) so clients don't show an empty row. A skipped
            # or failed fetch leaves the network's guided channels untouched.
            borrowed = filled = 0
            for cd, nc in pairs:
                gid = gracenote.get(cd.source_channel_id)
                if nc.id in covered:
                    # A guide of its own with holes (Tastemade Travel's can start
                    # half a day out): fill them from tvtv where it has rows.
                    if gid and net.borrows_tvtv(nc.id) and _has_hole(covered[nc.id]):
                        rows = _outside(self._tvtv_programs(cd.source_channel_id, gid), covered[nc.id])
                        borrowed += len(rows)
                        programs.extend(rows)
                    continue
                if skipped and net.has_guide(nc.id):
                    continue
                rows = self._tvtv_programs(cd.source_channel_id, gid) if gid and net.borrows_tvtv(nc.id) else []
                if rows:
                    borrowed += len(rows)
                elif not net.has_guide(nc.id):
                    rows = self._filler_programs(cd)
                    filled += 1
                programs.extend(rows)
            extra = ''.join([' (not fetched, kept stored guide)' if skipped else '',
                             f' + {borrowed} from tvtv' if borrowed else '',
                             f', {filled} channel(s) with placeholder blocks' if filled else ''])
            elapsed = time.monotonic() - t0
            native_total += len(progs)
            borrowed_total += borrowed
            if filled:
                placeholders[net.name] = filled
            # A skip is routine (Bloomberg's daily limit); failures warn above.
            if elapsed >= _SLOW_NETWORK_SECS:
                logger.info('[%s] %s guide slow: %d entries%s (%.1fs)', self.source_name, net.name,
                            len(progs), extra, elapsed)
            else:
                logger.debug('[%s] %s: %d guide entries%s (%.1fs)', self.source_name, net.name,
                             len(progs), extra, elapsed)

        summary = f'{native_total} native + {borrowed_total} tvtv-borrowed'
        if placeholders:
            breakdown = ', '.join(f'{name} {n}' for name, n in
                                  sorted(placeholders.items(), key=lambda kv: (-kv[1], kv[0])))
            summary += (f', {sum(placeholders.values())} channel(s) on placeholder blocks '
                        f'across {len(placeholders)} network(s) ({breakdown})')
        logger.info('[%s] %d EPG entries: %s (%.1fs)', self.source_name, len(programs), summary,
                    time.monotonic() - run_t0)
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
        channel (a user's manual ID) over what the scrape supplied. A channel
        the user set to 'off' gets none, whatever the scrape supplied."""
        ids = {cd.source_channel_id: cd.gracenote_id for cd in channels if cd.gracenote_id}
        try:
            from app.models import Channel, Source
            src = Source.query.filter_by(name=self.source_name).first()
            if src:
                for scid, gid, mode in (Channel.query
                                        .filter(Channel.source_id == src.id)
                                        .with_entities(Channel.source_channel_id, Channel.gracenote_id,
                                                       Channel.gracenote_mode)):
                    if (mode or 'auto') == 'off':
                        ids.pop(scid, None)
                    elif gid:
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
        """The earliest, across these channels that have a stored guide, of
        how far it runs; None if none of them has one. Lets a throttled network
        tell 'keep what's stored' from 'nothing is stored'. One channel with no
        guide (its schedule parsed empty, or a block cut the fetch short) must
        not count as 'nothing stored', or the throttle never holds."""
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
        stored = [end for end in ends.values() if end is not None]
        if not stored:
            return None
        earliest = min(stored)
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

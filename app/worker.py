"""
Background worker — run with: python -m app.worker
"""
import ctypes as _ctypes
import logging
import multiprocessing
import gc
import re
import signal
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Callable

import redis
import requests as _req
from rq.job import Job
from rq import Worker, Queue, Connection
from rq.worker import SimpleWorker as _SimpleWorker
from rq.timeouts import BaseDeathPenalty as _BaseDeathPenalty
from rq.registry import StartedJobRegistry
from apscheduler.schedulers.background import BackgroundScheduler
from croniter import croniter as _croniter
from sqlalchemy import or_, text
from sqlalchemy.exc import OperationalError as _SAOperationalError
from sqlalchemy.orm.attributes import flag_modified as _flag_modified
from app import create_app
from app.config_store import (
    persist_source_config_updates,
    persist_source_cache_updates,
    load_source_cache,
)
from app.extensions import db
from app.hls import inspect_hls_drm, parse_stream_info as _parse_stream_info, parse_dash_stream_info as _parse_dash_stream_info, WIDEVINE_UUID, PLAYREADY_UUID
from app.models import Source, Channel, Program, Feed, AppSettings, SourceCache
import time as _time
from urllib.parse import urljoin as _urljoin
from app.scrapers import registry
from app.scrapers.base import (
    StreamDeadError,
    ScrapeSkipError,
    is_ssl_handshake_failure,
    is_transient_network_error,
)
from app.scrapers.category_utils import category_for_channel
from app.xml_cache import ensure_xml_artifact, get_artifact, invalidate_xml_cache, write_artifact
from app.routes.images import delete_cached_logo

from app.timezone_utils import make_tz_formatter
if not logging.root.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(make_tz_formatter('%(asctime)s %(levelname)-8s %(name)s: %(message)s'))
    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(_handler)

# APScheduler logs every job execution at INFO — suppress to WARNING
logging.getLogger('apscheduler').setLevel(logging.WARNING)
logging.getLogger('rq.worker').setLevel(logging.WARNING)
logging.getLogger('rq.registry').setLevel(logging.WARNING)

from app.logfile import setup as _setup_logfile
_setup_logfile()
logger = logging.getLogger(__name__)
_CHANNEL_MISS_THRESHOLD = 3
_STALE_STARTED_JOB_GRACE_SECONDS = 300
_SCRAPER_MISSING_GRACE_DAYS = 7

flask_app = create_app()
from app.config import VERSION as _VERSION
# RQ work-horse job processes import this module to execute queued callables.
# Keep the app object at module scope, but only log startup for the long-lived
# `python -m app.worker` process so job imports don't look like worker restarts.
if __name__ == '__main__':
    logger.info('FastChannels worker v%s starting', _VERSION)
_NETWORK_OUTAGE_UNTIL = 0.0
_NETWORK_OUTAGE_REASON = ''


class ScrapePhaseTimeoutError(Exception):
    pass


class AuditChannelTimeoutError(TimeoutError):
    pass


def _run_with_signal_timeout(label: str, timeout_seconds: int | None, fn):
    if not timeout_seconds:
        return fn()

    def _alarm_handler(_signum, _frame):
        raise AuditChannelTimeoutError(f"{label} timed out after {timeout_seconds}s")

    previous_handler = signal.getsignal(signal.SIGALRM)
    parent_remaining, _ = signal.getitimer(signal.ITIMER_REAL)
    signal.signal(signal.SIGALRM, _alarm_handler)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    step_start = _time.monotonic()
    try:
        return fn()
    finally:
        step_elapsed = _time.monotonic() - step_start
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if parent_remaining > 0:
            signal.setitimer(signal.ITIMER_REAL, max(1, parent_remaining - step_elapsed))


_STATUS_CODE_RE = re.compile(r'\b(?:HTTP\s+|returned\s+|status\s+)(\d{3})\b')
_GEO_BLOCK_STATUS_CODES = (403, 451)


def _http_status_from_exception(exc: Exception) -> int | None:
    """Best-effort HTTP status extraction from a scraper's raised message text."""
    match = _STATUS_CODE_RE.search(str(exc))
    return int(match.group(1)) if match else None


def _is_geo_block_error(exc: Exception) -> bool:
    """True for a resolve()-time exception signalling an IP-level geo/legal
    block (403/451). Covers scrapers that embed the status in the message
    (e.g. Roku's raw RuntimeError) as well as ones that raise ScrapeSkipError
    with a static message and no status code at all (e.g. LocalNow's shared
    403/451 homepage-bootstrap check)."""
    return isinstance(exc, ScrapeSkipError) or _http_status_from_exception(exc) in _GEO_BLOCK_STATUS_CODES


def _audit_reason_from_exception(exc: Exception) -> str:
    message = str(exc).strip()
    name = type(exc).__name__
    if not message:
        return name
    if message == name or message.startswith(f'{name}:'):
        return message
    http_match = _STATUS_CODE_RE.search(message)
    if http_match:
        return f'HTTP {http_match.group(1)}: {message}'
    return f'{name}: {message}'


def _enqueue_xml_refresh_job() -> None:
    try:
        r = redis.from_url(flask_app.config['REDIS_URL'])
        q = Queue('fast', connection=r)
        job_id = 'xml-refresh'

        # If the job appears to be running (in StartedJobRegistry), verify a live
        # fast worker actually owns it. After a container restart the registry entry
        # persists in Redis (RDB snapshot) even though no worker is executing the
        # job. In that case it's a zombie and must be cleared before re-enqueuing.
        started_registry = StartedJobRegistry(q.name, connection=q.connection)
        if job_id in started_registry.get_job_ids():
            from rq import Worker as _Worker
            live_fast_workers = [
                w for w in _Worker.all(connection=r)
                if 'fast' in w.queue_names()
            ]
            if not live_fast_workers:
                started_registry.remove(job_id)
                try:
                    job = Job.fetch(job_id, connection=r)
                    job.delete()
                except Exception:
                    pass
                logger.warning('[xml-cache] cleared orphaned xml-refresh from StartedJobRegistry (no live fast workers)')
            else:
                logger.info('[xml-cache] refresh already running')
                return

        queued_ids = set(q.get_job_ids())
        if job_id in queued_ids:
            logger.info('[xml-cache] refresh already queued')
            return

        try:
            job = Job.fetch(job_id, connection=q.connection)
            status = job.get_status(refresh=False)
            if status in {'queued', 'deferred', 'scheduled'}:
                logger.info('[xml-cache] refresh already queued/running')
                return
            if status == 'started':
                # Zombie hash not in any registry — delete so enqueue can proceed.
                try:
                    job.delete()
                except Exception:
                    pass
                logger.warning('[xml-cache] deleted zombie xml-refresh job, enqueuing fresh one')
        except Exception:
            pass
        q.enqueue('app.worker.run_xml_refresh', job_timeout=1800, job_id=job_id)
        logger.info('[xml-cache] enqueued refresh job')
    except Exception:
        logger.exception('[xml-cache] could not enqueue refresh job')


def _cleanup_stale_started_job(q: Queue, job_id: str) -> bool:
    registry = StartedJobRegistry(q.name, connection=q.connection)
    if job_id not in registry:
        return False
    try:
        job = Job.fetch(job_id, connection=q.connection)
    except Exception:
        registry.remove(job_id)
        logger.warning('[rq] removed stale started-job marker for missing job %s', job_id)
        return True

    if job.get_status(refresh=False) != 'started':
        registry.remove(job)
        try:
            job.delete()
        except Exception:
            pass
        logger.warning('[rq] removed stale started-job marker for non-started job %s', job_id)
        return True

    now = datetime.now(timezone.utc)
    started_at = _utc_aware(getattr(job, 'started_at', None))
    last_heartbeat = _utc_aware(getattr(job, 'last_heartbeat', None))
    heartbeat_age = (now - last_heartbeat).total_seconds() if last_heartbeat else None
    started_age = (now - started_at).total_seconds() if started_at else None

    if heartbeat_age is not None and heartbeat_age > _STALE_STARTED_JOB_GRACE_SECONDS:
        registry.remove(job)
        try:
            job.delete()
        except Exception:
            pass
        logger.warning('[rq] removed stale started job %s after %.0fs without heartbeat', job_id, heartbeat_age)
        return True

    if last_heartbeat is None and started_age is not None and started_age > _STALE_STARTED_JOB_GRACE_SECONDS:
        registry.remove(job)
        try:
            job.delete()
        except Exception:
            pass
        logger.warning(
            '[rq] removed stale started job %s after %.0fs without heartbeat metadata',
            job_id,
            started_age,
        )
        return True

    return False


def _scrape_job_already_active(q: Queue, source_name: str) -> bool:
    job_id = f'scrape-{source_name}'
    _cleanup_stale_started_job(q, job_id)
    active_ids = set(q.get_job_ids()) | set(StartedJobRegistry(q.name, connection=q.connection).get_job_ids())
    if job_id in active_ids:
        return True
    try:
        job = Job.fetch(job_id, connection=q.connection)
        return job.get_status(refresh=False) in {'queued', 'started', 'deferred', 'scheduled'}
    except Exception:
        return False


def _any_scrapes_active() -> bool:
    """Return True if any scraper jobs are queued or running."""
    try:
        r = redis.from_url(flask_app.config['REDIS_URL'])
        q = Queue('scraper', connection=r)
        queued = [jid for jid in q.get_job_ids() if jid.startswith('scrape-')]
        running = [
            jid for jid in StartedJobRegistry(q.name, connection=r).get_job_ids()
            if jid.startswith('scrape-')
        ]
        return bool(queued or running)
    except Exception:
        return False


def _no_scrapes_pending(current_source_name: str) -> bool:
    """Return True if no other scrape jobs are queued or running.

    When multiple sources share a cron schedule they land in the scraper queue
    back-to-back. Triggering the xml-refresh after the first one finishes means
    the fast worker rebuilds M3U/EPG while the remaining sources haven't written
    their data yet. Deferring until the queue drains ensures one clean rebuild
    captures all sources.
    """
    try:
        r = redis.from_url(flask_app.config['REDIS_URL'])
        q = Queue('scraper', connection=r)
        current_job_id = f'scrape-{current_source_name}'
        other_running = [
            jid for jid in StartedJobRegistry(q.name, connection=r).get_job_ids()
            if jid != current_job_id
        ]
        queued = [jid for jid in q.get_job_ids() if jid.startswith('scrape-')]
        if other_running or queued:
            logger.info('[%s] deferring xml refresh — %d scraper job(s) still pending',
                        current_source_name, len(other_running) + len(queued))
            return False
        return True
    except Exception:
        return True  # safe fallback: don't suppress the refresh


def _utc_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def run_scraper(source_name: str, force_full: bool = False):
    with flask_app.app_context():
        db.session.remove()
        from .models import AppSettings
        _app_settings = AppSettings.get()
        _gracenote_auto_fill = getattr(_app_settings, 'gracenote_auto_fill', True)
        source = Source.query.filter_by(name=source_name).first()
        if not source:
            logger.error(f'Source not found: {source_name}')
            return
        if not source.is_enabled:
            logger.info('[%s] Scrape skipped: source disabled', source_name)
            return

        outage_reason = _active_network_outage()
        if outage_reason:
            source.last_error = outage_reason
            db.session.commit()
            logger.warning('[%s] Scrape skipped: %s', source_name, outage_reason)
            return

        scraper_cls = registry.get(source_name)
        if not scraper_cls:
            source.last_error = f'No scraper registered for {source_name}'
            db.session.commit()
            return

        t0 = time.monotonic()
        logger.info('[%s] Scrape job started', source_name)
        scraper = None
        channels = None
        programs = None
        db_channels = None
        epg_input = None
        _progress = _make_progress_writer(source_name)
        try:
            phase_timeouts = dict(getattr(scraper_cls, 'phase_timeouts', {}) or {})
            _env_epg = AppSettings._env_int('SCRAPE_EPG_TIMEOUT')
            if _env_epg:
                # Raise the ceiling only — never lower a source's own override
                # (Roku/Vidaa set epg=900) below what it already needs.
                phase_timeouts['epg'] = max(phase_timeouts.get('epg', 0), _env_epg)

            def _phase_timeout(phase_name: str) -> int | None:
                value = phase_timeouts.get(phase_name)
                return int(value) if value else None

            def _run_phase(phase_name: str, fn, *args, **kwargs):
                timeout_seconds = _phase_timeout(phase_name)
                if not timeout_seconds:
                    return fn(*args, **kwargs)

                def _alarm_handler(_signum, _frame):
                    raise ScrapePhaseTimeoutError(
                        f'[{source_name}] {phase_name} phase timed out after {timeout_seconds}s'
                    )

                previous_handler = signal.getsignal(signal.SIGALRM)
                parent_remaining, _ = signal.getitimer(signal.ITIMER_REAL)
                signal.signal(signal.SIGALRM, _alarm_handler)
                signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
                phase_start = _time.monotonic()
                try:
                    return fn(*args, **kwargs)
                finally:
                    phase_elapsed = _time.monotonic() - phase_start
                    signal.setitimer(signal.ITIMER_REAL, 0)
                    signal.signal(signal.SIGALRM, previous_handler)
                    if parent_remaining > 0:
                        new_remaining = max(1, parent_remaining - phase_elapsed)
                        signal.setitimer(signal.ITIMER_REAL, new_remaining)

            scraper = _run_phase('init', scraper_cls, config=source.config or {})
            scraper._progress_cb = _progress
            refresh_hours = getattr(scraper_cls, 'channel_refresh_hours', 0)

            # Decide whether to skip the channel list fetch this run.
            # If channel_refresh_hours > 0 and we fetched channels within that window,
            # only refresh EPG using the existing DB channel list.
            #
            # This MUST be gated on last_channel_fetch_at, not last_scraped_at:
            # EPG-only runs bump last_scraped_at every scrape_interval, so for any
            # source where scrape_interval < channel_refresh_hours, gating on
            # last_scraped_at meant age_hours never reached the window and
            # fetch_channels() was permanently skipped (channels went stale, only
            # resolve() kept streams alive). last_channel_fetch_at is stamped only
            # when a full channel fetch succeeds. NULL (existing installs / never
            # fetched) → skip_channels stays False → one full fetch, then self-heals.
            skip_channels = False
            if refresh_hours > 0 and source.last_channel_fetch_at:
                last = _utc_aware(source.last_channel_fetch_at)
                age_hours = (datetime.now(timezone.utc) - last).total_seconds() / 3600
                skip_channels = age_hours < refresh_hours
            if force_full:
                skip_channels = False

            # Run pre_run_setup (e.g. auth bootstrap) and persist any config
            # updates (like tokens) immediately — before the long scrape starts —
            # so they survive even if the job times out mid-EPG.
            _progress('bootstrap')
            _run_phase('bootstrap', scraper.pre_run_setup)
            _apply_scraper_config_updates(source, scraper)
            for _pre_attempt in range(3):
                try:
                    db.session.commit()
                    break
                except _SAOperationalError:
                    db.session.rollback()
                    if _pre_attempt == 2:
                        raise
                    time.sleep(5 * (_pre_attempt + 1))

            if skip_channels:
                from app.scrapers.base import ChannelData as _CD
                db_channels = _epg_channels_for_source(source)
                epg_input   = [_CD(source_channel_id=ch.source_channel_id,
                                   name=ch.name,
                                   stream_url=ch.stream_url or '',
                                   slug=ch.slug or '',
                                   guide_key=ch.guide_key) for ch in db_channels]
                enabled_ids = {
                    ch.source_channel_id
                    for ch in db_channels
                    if ch.is_enabled and ch.source_channel_id
                }
                _progress('epg', 0, len(epg_input))
                programs = _run_phase(
                    'epg',
                    scraper.fetch_epg,
                    epg_input,
                    skip_ids=_fresh_epg_sids(source),
                    enabled_ids=enabled_ids,
                )
                for _attempt in range(3):
                    try:
                        _upsert_programs(source, programs, progress_cb=_progress)
                        _apply_scraper_config_updates(source, scraper)
                        _now = datetime.now(timezone.utc)
                        source.last_scraped_at     = _now
                        source.last_epg_success_at = _now
                        source.last_error          = None
                        db.session.commit()
                        break
                    except _SAOperationalError:
                        db.session.rollback()
                        if _attempt == 2:
                            raise
                        _wait = 5 * (_attempt + 1)
                        logger.warning('[%s] DB locked (EPG-only, attempt %d/3), retrying in %ds',
                                       source_name, _attempt + 1, _wait)
                        time.sleep(_wait)
                invalidate_xml_cache()
                if _no_scrapes_pending(source_name):
                    _enqueue_xml_refresh_job()
                elapsed = time.monotonic() - t0
                logger.info('[%s] EPG-only run complete — %d channels, %d programs (%.1fs)',
                            source_name, len(db_channels), len(programs), elapsed)
            else:
                _progress('channels')
                channels = _run_phase('channels', scraper.fetch_channels)

                # Commit channels before running EPG so that a timeout in the EPG
                # phase doesn't discard a successful channel fetch (issue #14 —
                # first-run users on high-latency VPNs ended up with 0 channels).
                _active_geos = None
                if hasattr(scraper, '_geos'):
                    _active_geos = {g.upper() for g in scraper._geos()}
                for _attempt in range(3):
                    try:
                        if source.scrape_interval != 0:
                            _upsert_channels(
                                source, channels, _gracenote_auto_fill,
                                active_geos=_active_geos,
                                miss_threshold=getattr(scraper, 'channel_miss_threshold', _CHANNEL_MISS_THRESHOLD),
                                rehome_by_guide_key=getattr(scraper, 'rehome_by_guide_key', False),
                            )
                        # Persist scraper config/cache FIRST. persist_*() call
                        # db.session.expire_all(), which DISCARDS unflushed attribute
                        # writes — so the timestamp stamps must come AFTER it (the
                        # EPG-commit path below already orders them this way). Doing it
                        # the other way silently dropped last_channel_fetch_at for every
                        # scraper that queues config/cache updates in the channel phase.
                        _apply_scraper_config_updates(source, scraper)
                        if source.scrape_interval != 0:
                            # Stamp last_scraped_at as soon as channels are committed.
                            # The EPG phase below re-stamps on success, but if it
                            # instead skips/fails (e.g. Roku's session gets rejected
                            # before EPG), the source would otherwise keep a full
                            # channel list while still reporting "Last scraped: Never".
                            source.last_scraped_at = datetime.now(timezone.utc)
                            # Stamp the channel-fetch clock too — this (not
                            # last_scraped_at) gates the channel_refresh_hours skip.
                            # Set only here, on a successful full fetch; EPG-only
                            # runs and failed fetches leave it untouched so the next
                            # scrape retries the full fetch.
                            source.last_channel_fetch_at = source.last_scraped_at
                        db.session.commit()
                        # Clear so the EPG commit's _apply_scraper_config_updates
                        # only persists updates added during the EPG phase, not a
                        # re-merge of the already-committed channel-phase snapshot.
                        if hasattr(scraper, '_pending_config_updates'):
                            scraper._pending_config_updates.clear()
                        if hasattr(scraper, '_pending_cache_updates'):
                            scraper._pending_cache_updates.clear()
                        break
                    except _SAOperationalError as _dbe:
                        db.session.rollback()
                        if _attempt == 2:
                            raise
                        _wait = 5 * (_attempt + 1)
                        logger.warning('[%s] DB locked (channel upsert, attempt %d/3), retrying in %ds',
                                       source_name, _attempt + 1, _wait)
                        time.sleep(_wait)

                _progress('epg', 0, len(channels))
                # Query enabled_ids after channels are committed so new channels
                # (added just above) are included and get EPG on the first run.
                enabled_ids = {
                    sid for (sid,) in (
                        db.session.query(Channel.source_channel_id)
                        .filter(
                            Channel.source_id == source.id,
                            Channel.is_enabled == True,
                            Channel.source_channel_id != None,
                        )
                        .all()
                    )
                }
                programs = _run_phase(
                    'epg',
                    scraper.fetch_epg,
                    channels,
                    skip_ids=_fresh_epg_sids(source),
                    enabled_ids=enabled_ids,
                )
                for _attempt in range(3):
                    try:
                        _upsert_programs(source, programs, progress_cb=_progress)
                        _apply_scraper_config_updates(source, scraper)
                        _now = datetime.now(timezone.utc)
                        source.last_scraped_at     = _now
                        source.last_epg_success_at = _now
                        source.last_error          = None
                        db.session.commit()
                        break
                    except _SAOperationalError as _dbe:
                        db.session.rollback()
                        if _attempt == 2:
                            raise
                        _wait = 5 * (_attempt + 1)
                        logger.warning('[%s] DB locked (attempt %d/3), retrying in %ds',
                                       source_name, _attempt + 1, _wait)
                        time.sleep(_wait)
                invalidate_xml_cache()
                if _no_scrapes_pending(source_name):
                    _enqueue_xml_refresh_job()
                elapsed = time.monotonic() - t0
                logger.info('[%s] Scrape complete — %d channels, %d programs (%.1fs)',
                            source_name, len(channels), len(programs), elapsed)
                logo_urls = [ch.logo_url for ch in channels if ch.logo_url]
                if logo_urls:
                    # Publish the phase change immediately so the UI does not sit
                    # on "EPG 100%" while the first cache callback is still pending.
                    _progress('logos', 0, len(set(logo_urls)))
                _prewarm_logos(source_name, logo_urls, progress_cb=_progress)
            _progress('done')
        except ScrapeSkipError as e:
            elapsed = time.monotonic() - t0
            logger.warning('[%s] Scrape skipped after %.1fs: %s', source_name, elapsed, e)
            db.session.rollback()
            _apply_scraper_config_updates(source, scraper)
            source.last_error = str(e)
            db.session.commit()
            _progress('done')
        except ScrapePhaseTimeoutError as e:
            elapsed = time.monotonic() - t0
            logger.error('[%s] Scrape aborted after %.1fs: %s', source_name, elapsed, e)
            db.session.rollback()
            _apply_scraper_config_updates(source, scraper)
            source.last_error = str(e)
            db.session.commit()
            _progress('done')
        except Exception as e:
            elapsed = time.monotonic() - t0
            if _is_transient_network_error(e):
                reason = _network_error_summary(e)
                _mark_network_outage(reason)
                logger.warning('[%s] Scrape skipped after %.1fs due to transient network failure: %s',
                               source_name, elapsed, reason)
                db.session.rollback()
                _apply_scraper_config_updates(source, scraper)
                source.last_error = reason
                db.session.commit()
                _progress('done')
                return
            logger.exception('[%s] Scrape failed after %.1fs', source_name, elapsed)
            # Rollback any partial writes before recording the error, otherwise
            # the commit below will fail if the session is in a dirty/locked state.
            db.session.rollback()
            _apply_scraper_config_updates(source, scraper)
            source.last_error = str(e)
            try:
                db.session.commit()
            except Exception:
                logger.warning('[%s] Could not persist last_error to DB', source_name)
            _progress('done')
        finally:
            channels = None
            programs = None
            db_channels = None
            epg_input = None
            scraper = None
            gc.collect()
            try:
                _ctypes.CDLL('libc.so.6').malloc_trim(0)
            except Exception:
                pass


def _iter_exception_chain(exc: Exception):
    seen = set()
    current = exc
    while current and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _is_transient_network_error(exc: Exception) -> bool:
    return is_transient_network_error(exc)


def _is_ssl_handshake_failure(exc: Exception) -> bool:
    return is_ssl_handshake_failure(exc)


def _network_error_summary(exc: Exception) -> str:
    for err in _iter_exception_chain(exc):
        text = str(err).strip()
        lowered = text.lower()
        if 'network is unreachable' in lowered:
            return 'Network unavailable: no route to the internet. FastChannels will retry automatically.'
        if 'temporary failure in name resolution' in lowered or 'failed to resolve' in lowered or 'err_name_not_resolved' in lowered:
            return 'Network unavailable: DNS resolution failed. FastChannels will retry automatically.'
    return 'Network unavailable: transient connectivity failure. FastChannels will retry automatically.'


def _mark_network_outage(reason: str, cooldown_seconds: int = 90) -> None:
    global _NETWORK_OUTAGE_UNTIL, _NETWORK_OUTAGE_REASON
    _NETWORK_OUTAGE_UNTIL = time.monotonic() + cooldown_seconds
    _NETWORK_OUTAGE_REASON = reason


def _active_network_outage() -> str | None:
    if time.monotonic() < _NETWORK_OUTAGE_UNTIL:
        return _NETWORK_OUTAGE_REASON
    return None



def run_stream_audit(source_name: str):
    """
    Stream Audit — resolves every channel (active and previously dead/VOD) via
    the scraper, fetches the HLS manifest using the scraper's session (so
    source-specific headers like Origin/Referer are included), drills master →
    variant playlist, and checks for dead streams, VOD-only content, and
    SAMPLE-AES DRM encryption.  Flagged channels are marked is_active=False so
    they drop out of M3U/EPG output; previously-dead channels that pass are
    re-activated automatically.
    """
    with flask_app.app_context():
        source = Source.query.filter_by(name=source_name).first()
        if not source:
            logger.error('[audit] source not found: %s', source_name)
            return
        if not source.is_enabled:
            logger.info('[audit] %s: source disabled, skipping', source_name)
            return

        scraper_cls = registry.get(source_name)
        if not scraper_cls or not getattr(scraper_cls, 'stream_audit_enabled', False):
            logger.info('[audit] %s: stream audit not enabled for this source, skipping', source_name)
            return

        _required = getattr(scraper_cls, 'audit_requires_config', [])
        _cfg = source.config or {}
        _missing = [k for k in _required if not (_cfg.get(k) or '').strip()]
        if _missing:
            _skip_msg = f"Required config missing: {', '.join(_missing)}"
            logger.warning('[audit] %s: %s — skipping audit', source_name, _skip_msg)
            persist_source_cache_updates(source.id, {'last_audit_result': {
                'skipped_reason': _skip_msg,
                'ts': datetime.now(timezone.utc).isoformat(),
            }})
            return

        scraper = scraper_cls(config=source.config or {})
        try:
            scraper.pre_run_setup()
        except Exception as _pre_exc:
            logger.debug('[audit] pre_run_setup failed (non-fatal): %s', _pre_exc)

        # Some scrapers (e.g. Tubi) need a full channel fetch before auditing
        # to warm their URL cache and establish the correct session cookies.
        # Without this, per-channel resolve() calls lack session context and
        # CDN requests return 422.
        if getattr(scraper_cls, 'scrape_before_audit', False):
            logger.info('[audit] %s: pre-audit channel refresh to warm URL cache…', source_name)
            try:
                scraper.fetch_channels()
            except Exception as _refresh_exc:
                logger.warning('[audit] %s: pre-audit refresh failed (non-fatal): %s', source_name, _refresh_exc)

        channels = source.channels.filter(
            db.or_(
                Channel.is_active == True,
                Channel.disable_reason.in_(['Dead', 'VOD']),
                Channel.disable_reason.like('AuditError:%'),
            )
        ).all()
        total    = len(channels)
        checked  = 0
        flagged  = 0
        bridged  = 0   # DRM channels kept active and routed via the PrismCast bridge
        dead     = 0
        vod      = 0
        errors   = 0
        skipped_403 = 0
        # A DRM (FairPlay) channel is bridged — kept active + marked requires_drm_bridge so
        # it flows into the PrismCast feed — only when BOTH the global DRM-bridge mode is on
        # AND the source has license handling. Otherwise it keeps the legacy disable
        # behavior (is_active=False). Default-off means non-PrismCast users are unaffected.
        _bridge_capable = bool(getattr(scraper_cls, 'license_url', None))
        _drm_bridge_mode = bool(AppSettings.get().drm_bridge_enabled)
        consecutive_errors = 0
        consecutive_skipped_403 = 0  # geo-block detector
        consecutive_transient_errors = 0  # resolve-timeout detector
        report_channels = []
        _audit_ignore_4xx = getattr(scraper_cls, 'audit_ignore_4xx', False)
        _audit_ignore_vod = getattr(scraper_cls, 'audit_ignore_vod', False)

        logger.info('[audit] %s: checking %d channels…', source_name, total)

        # Live progress → Redis key audit:progress:{source_name}
        _audit_key = f'audit:progress:{source_name}'
        try:
            _redis_audit = redis.from_url(flask_app.config['REDIS_URL'])
            _redis_audit.ping()
        except Exception:
            _redis_audit = None

        import json as _json_audit
        def _audit_progress(done, total_, flagged_=0, dead_=0, vod_=0, errors_=0, skipped_403_=0, phase='checking'):
            if not _redis_audit:
                return
            try:
                if phase == 'done':
                    _redis_audit.delete(_audit_key)
                else:
                    # Surface any active rate-limit cooldown (e.g. Roku 403) so the
                    # audit modal can show the paused state instead of freezing.
                    _cd_remaining = None
                    _cd_reason = None
                    _cd_active = getattr(scraper, '_cooldown_active', None)
                    if callable(_cd_active) and _cd_active():
                        _cd_rem_fn = getattr(scraper, '_cooldown_remaining', None)
                        _cd_remaining = int(_cd_rem_fn()) if callable(_cd_rem_fn) else None
                        _cd_reason = getattr(scraper, '_cooldown_reason', None)
                    _redis_audit.setex(_audit_key, 600, _json_audit.dumps({
                        'phase': phase, 'done': done, 'total': total_,
                        'flagged': flagged_, 'dead': dead_, 'vod': vod_, 'errors': errors_,
                        'skipped_403': skipped_403_,
                        'cooldown_remaining': _cd_remaining,
                        'cooldown_reason': _cd_reason,
                        'current_index': getattr(_audit_progress, '_current_index', None),
                        'current_channel': getattr(_audit_progress, '_current_channel', None),
                        'ts': _time.time(),
                    }))
            except Exception:
                pass

        _audit_progress(0, total)

        def _mark_audit_error_inactive(channel, reason):
            channel.is_active = False
            channel.disable_reason = reason

        # Brief warmup pause — gives any residual rate-limit ban time to clear
        _time.sleep(5)

        # Use the scraper's own session so source-specific headers (Origin, Referer,
        # auth tokens, etc.) are included in every CDN request.
        sess = scraper.session
        _audit_channel_timeout = int(getattr(scraper_cls, "audit_channel_timeout_seconds", 20 if source_name == "plex" else 0) or 0)
        for i, ch in enumerate(channels, 1):
            try:
                _audit_item_t0 = _time.monotonic()
                _audit_verbose = source_name == 'plex'
                if _audit_verbose:
                    logger.debug('[audit-debug] %s %d/%d start id=%s name=%s url=%s',
                                source_name, i, total, ch.source_channel_id, ch.name, (ch.stream_url or '')[:120])
                _audit_progress._current_index = i
                _audit_progress._current_channel = ch.name
                _audit_progress(i - 1, total, flagged, dead, vod, errors, skipped_403)
                # Resolve the raw stream URL. Use audit_resolve() if the scraper
                # provides a lighter-weight bulk-check variant (e.g. Plex skips tune).
                _resolve = getattr(scraper, 'audit_resolve', scraper.resolve)
                _resolve_t0 = _time.monotonic()
                try:
                    resolved_url = _run_with_signal_timeout(
                        f"[audit] {source_name} {i}/{total} resolve {ch.name}",
                        _audit_channel_timeout,
                        lambda: _resolve(ch.stream_url),
                    )
                    if _audit_verbose:
                        logger.debug('[audit-debug] %s %d/%d resolved in %.2fs -> %s',
                                    source_name, i, total, _time.monotonic() - _resolve_t0, (resolved_url or '')[:160])
                except StreamDeadError as dead_exc:
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'Dead'
                    dead += 1
                    consecutive_errors = 0
                    report_channels.append({
                        'id': ch.id,
                        'name': ch.name,
                        'status': 'dead',
                        'reason': _audit_reason_from_exception(dead_exc),
                    })
                    logger.info('[audit] dead stream: %s  (confirmed by scraper)', ch.name)
                    continue
                except Exception as re_exc:
                    if _is_transient_network_error(re_exc):
                        logger.warning('[audit] transient resolve failure for %s: %s', ch.name, re_exc)
                        errors += 1
                        consecutive_transient_errors += 1
                        if consecutive_transient_errors >= 20:
                            logger.warning('[audit] %s: %d consecutive transient resolve failures — '
                                           'source API may be unreachable, aborting audit.',
                                           source_name, consecutive_transient_errors)
                            break
                        continue
                    # If the scraper entered a rate-limit cooldown, wait it out rather
                    # than burning through the consecutive-error budget on channels that
                    # will all fail until the cooldown expires.
                    _cooldown_active = getattr(scraper, '_cooldown_active', None)
                    _cooldown_remaining = getattr(scraper, '_cooldown_remaining', None)
                    if callable(_cooldown_active) and _cooldown_active():
                        wait = int((_cooldown_remaining() if callable(_cooldown_remaining) else 60) + 2)
                        logger.warning('[audit] %s: rate-limit cooldown active — waiting %ds',
                                       source_name, wait)
                        # Sleep in short slices, refreshing the progress heartbeat each
                        # time so the audit modal shows a live "paused — cooldown" state
                        # instead of going stale (the status endpoint drops us after 90s
                        # without a heartbeat). Resume early if the cooldown clears.
                        _waited = 0
                        while _waited < wait:
                            _audit_progress(i - 1, total, flagged, dead, vod, errors,
                                            skipped_403, phase='cooldown')
                            _time.sleep(min(15, wait - _waited))
                            _waited += 15
                            if not _cooldown_active():
                                break
                        errors += 1
                        continue
                    if _is_geo_block_error(re_exc):
                        # 403/451 (and scrapers that raise ScrapeSkipError for the same
                        # reason, e.g. LocalNow) are IP-level geo/legal blocks — skip,
                        # don't penalize (GH #22).
                        skipped_403 += 1
                        consecutive_skipped_403 += 1
                        consecutive_errors = 0
                        report_channels.append({
                            'id': ch.id,
                            'name': ch.name,
                            'status': 'rate-limited',
                            'reason': _audit_reason_from_exception(re_exc),
                        })
                        logger.info('[audit] %s: resolve hit a geo/legal block for %s, skipping: %s',
                                    source_name, ch.name, re_exc)
                        if consecutive_skipped_403 >= 30:
                            logger.warning('[audit] %s: %d consecutive 403/skip responses — '
                                           'source appears geo-blocked, aborting audit.',
                                           source_name, consecutive_skipped_403)
                            break
                        continue
                    logger.warning('[audit] resolve failed for %s: %s', ch.name, re_exc)
                    errors += 1
                    consecutive_errors += 1
                    report_channels.append({
                        'id': ch.id,
                        'name': ch.name,
                        'status': 'error',
                        'reason': _audit_reason_from_exception(re_exc),
                    })
                    if consecutive_errors >= 20:
                        logger.error('[audit] %s: 20 consecutive errors — aborting.', source_name)
                        break
                    continue

                # audit_resolve() may return an opaque internal URL (e.g. stirr://)
                # as a sentinel meaning "channel confirmed alive, skip manifest fetch".
                # None means the scraper could not resolve the URL (e.g. PRS failure).
                if not resolved_url:
                    errors += 1
                    consecutive_errors += 1
                    logger.warning('[audit] %s: resolve() returned None for %s', source_name, ch.name)
                    if consecutive_errors >= 20:
                        logger.error('[audit] %s: 20 consecutive errors — aborting.', source_name)
                        break
                    continue
                if not resolved_url.startswith('http'):
                    checked += 1
                    consecutive_errors = 0
                    consecutive_skipped_403 = 0
                    consecutive_transient_errors = 0
                    if not ch.is_active:
                        ch.is_active = True
                        ch.disable_reason = None
                        logger.info('[audit] re-activated previously dead channel: %s', ch.name)
                    # Opaque-URL scrapers (stirr/distro/xumo/roku/localnow/plex) confirm
                    # liveness without fetching the manifest, so stream_info (the
                    # resolution/codec badge) would otherwise never be populated by an
                    # audit. Backfill it once when missing via a play-time resolve +
                    # master parse, so a fresh audit fills in absent resolution badges.
                    # Best-effort: skip non-HLS sources (e.g. Amazon DASH/DRM) and
                    # swallow any failure so it never affects the liveness verdict.
                    if ch.stream_info is None and (ch.stream_type or 'hls').lower() == 'hls':
                        try:
                            play_url = scraper.resolve(ch.stream_url)
                            if play_url and play_url.startswith('http'):
                                # Some opaque-URL scrapers (e.g. Stirr's weathernationtv
                                # CDN) front legacy-cipher hosts that reject the audit
                                # session's default SECLEVEL=2 handshake. Prefer the
                                # scraper's lax-TLS CDN session — the same one the play
                                # proxy uses — so the badge backfill matches playback.
                                _cdn = getattr(scraper, '_cdn_session', None) or sess
                                rinfo = _cdn.get(play_url, timeout=12, allow_redirects=True)
                                if rinfo.status_code == 200 and '#EXT-X-STREAM-INF' in rinfo.text:
                                    si = _parse_stream_info(rinfo.text)
                                    if si:
                                        ch.stream_info = si
                                        logger.debug('[audit] backfilled stream_info for %s: %s',
                                                     ch.name, si.get('max_resolution') or '?')
                        except Exception as _si_exc:
                            logger.debug('[audit] stream_info backfill failed for %s: %s',
                                         ch.name, _si_exc)
                    logger.debug('[audit] %s: opaque URL — existence confirmed by scraper, skipping manifest fetch', ch.name)
                    continue

                _manifest_t0 = _time.monotonic()
                try:
                    r = _run_with_signal_timeout(
                        f"[audit] {source_name} {i}/{total} manifest {ch.name}",
                        _audit_channel_timeout,
                        lambda: sess.get(resolved_url, timeout=15, allow_redirects=True),
                    )
                    if _audit_verbose:
                        logger.debug('[audit-debug] %s %d/%d manifest in %.2fs status=%s bytes=%s final=%s',
                                    source_name, i, total, _time.monotonic() - _manifest_t0, r.status_code, len(r.content), (r.url or '')[:160])
                except Exception as req_exc:
                    if _is_ssl_handshake_failure(req_exc):
                        ch.is_active      = False
                        ch.is_enabled     = False
                        ch.disable_reason = 'Dead'
                        dead += 1
                        consecutive_errors = 0
                        report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'dead', 'reason': 'SSL'})
                        logger.info('[audit] dead stream: %s  (SSL handshake rejected by server)', ch.name)
                        continue
                    if _is_transient_network_error(req_exc):
                        # DNS failure after we've already checked several channels means
                        # the network is fine but this specific hostname doesn't resolve —
                        # treat as dead.  If checked < 5 we may be in a full network
                        # outage, so keep it transient to avoid false mass-kills.
                        dns_markers = ('name resolution', 'failed to resolve',
                                       'temporary failure in name resolution',
                                       'err_name_not_resolved', 'nameresolut')
                        exc_text = str(req_exc).lower()
                        is_dns = any(m in exc_text for m in dns_markers)
                        if is_dns and checked >= 5:
                            ch.is_active      = False
                            ch.is_enabled     = False
                            ch.disable_reason = 'Dead'
                            dead += 1
                            consecutive_errors = 0
                            report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'dead', 'reason': 'DNS'})
                            logger.info('[audit] dead stream: %s  (hostname does not resolve)', ch.name)
                        else:
                            logger.warning('[audit] transient manifest fetch failure for %s: %s', ch.name, req_exc)
                            errors += 1
                        continue
                    raise

                if r.status_code in (403, 429, 500, 502, 503, 504):
                    # 403 is an IP-level geo-block; a long sleep won't help, but a
                    # brief one (10s) avoids hammering the CDN on the first few hits
                    # before we decide it's a persistent block.  429/5xx get the full
                    # graduated backoff as before.
                    if r.status_code == 403 and consecutive_skipped_403 < 5:
                        _time.sleep(10)
                    elif r.status_code != 403 and consecutive_skipped_403 < 5:
                        wait = 30 + skipped_403 * 5
                        logger.warning('[audit] %s rate-limited (%d), backing off %ds…',
                                       source_name, r.status_code, wait)
                        _time.sleep(min(wait, 30))
                    r = _run_with_signal_timeout(
                        f"[audit] {source_name} {i}/{total} manifest-retry {ch.name}",
                        _audit_channel_timeout,
                        lambda: sess.get(resolved_url, timeout=15, allow_redirects=True),
                    )

                if r.status_code in (400, 404, 410, 422):
                    if _audit_ignore_4xx:
                        checked += 1
                        consecutive_errors = 0
                        consecutive_skipped_403 = 0
                        continue
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'Dead'
                    dead += 1
                    consecutive_errors = 0
                    consecutive_skipped_403 = 0
                    report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'dead', 'reason': f'HTTP {r.status_code}'})
                    logger.info('[audit] dead stream: %s  (HTTP %d)', ch.name, r.status_code)
                    continue

                if r.status_code in (*_GEO_BLOCK_STATUS_CODES, 429, 500, 502, 503, 504):
                    # Still rate-limited or transient server error after backoff —
                    # skip without penalising the consecutive-error budget.
                    # 500/502/504 are CDN hiccups, not stream problems. 451 is
                    # permanent so it's deliberately excluded from the backoff tuple above.
                    skipped_403 += 1
                    consecutive_skipped_403 += 1
                    consecutive_errors = 0
                    report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'rate-limited', 'reason': f'HTTP {r.status_code}'})
                    logger.info('[audit] %s transient error (%d) after backoff, skipping',
                                ch.name, r.status_code)
                    if consecutive_skipped_403 >= 30:
                        logger.warning('[audit] %s: %d consecutive 403/skip responses — '
                                       'source appears geo-blocked, aborting audit.',
                                       source_name, consecutive_skipped_403)
                        break
                    continue

                if r.status_code != 200:
                    _mark_audit_error_inactive(ch, f'AuditError: HTTP {r.status_code}')
                    logger.warning('[audit] error: %s (HTTP %d) — marked inactive', ch.name, r.status_code)
                    errors += 1
                    consecutive_errors += 1
                    consecutive_skipped_403 = 0
                    report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'error', 'reason': f'HTTP {r.status_code}'})
                    if consecutive_errors >= 20:
                        logger.error('[audit] %s: 20 consecutive errors — aborting. '
                                     'Source may be rate-limiting or down.', source_name)
                        break
                    continue

                consecutive_errors = 0
                consecutive_skipped_403 = 0
                consecutive_transient_errors = 0
                checked += 1
                manifest_text = r.text
                manifest_url  = r.url

                # ── DASH/MPD manifest ──────────────────────────────────────
                if '<MPD ' in manifest_text or (manifest_text.lstrip().startswith('<?xml')
                                                and '<MPD' in manifest_text):
                    if 'type="static"' in manifest_text:
                        if _audit_ignore_vod:
                            continue
                        ch.is_active      = False
                        ch.is_enabled     = False
                        ch.disable_reason = 'VOD'
                        vod += 1
                        report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'vod', 'reason': 'VOD'})
                        logger.info('[audit] DASH VOD (not live): %s', ch.name)
                        continue
                    # Resolution/codec badge from the MPD's video Representations — the DASH
                    # equivalent of the HLS stream_info parse below (Amazon, Sling, etc.).
                    _dash_info = _parse_dash_stream_info(manifest_text)
                    if _dash_info:
                        ch.stream_info = _dash_info
                    _widevine  = WIDEVINE_UUID
                    _playready = PLAYREADY_UUID
                    if _widevine in manifest_text.lower() or _playready in manifest_text.lower():
                        _dash_drm_type = 'Widevine' if _widevine in manifest_text.lower() else 'PlayReady'
                        if _bridge_capable and _drm_bridge_mode:
                            # DASH+Widevine (e.g. Amazon, Sling) plays via the browser/EME
                            # PrismCast bridge — keep it active and mark it for the bridge
                            # rather than disabling.
                            ch.requires_drm_bridge = True
                            if not ch.is_active:
                                ch.is_active = True
                            ch.disable_reason = None
                            bridged += 1
                            report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'drm_bridge', 'reason': _dash_drm_type})
                            logger.info('[audit] DASH DRM→PrismCast bridge: %s (%s)', ch.name, _dash_drm_type)
                        else:
                            ch.requires_drm_bridge = False
                            ch.is_active      = False
                            ch.is_enabled     = False
                            ch.disable_reason = f'DRM:{_dash_drm_type}'
                            flagged += 1
                            report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'drm', 'reason': _dash_drm_type})
                            logger.info('[audit] DASH DRM: %s  →  %s (%s)', ch.name, manifest_url[:80], _dash_drm_type)
                    else:
                        # DASH alive (no VOD, no DRM) — clear any stale bridge marker and
                        # re-activate if it was previously dead.
                        if getattr(ch, 'requires_drm_bridge', False):
                            ch.requires_drm_bridge = False
                        if not ch.is_active:
                            ch.is_active = True
                            ch.disable_reason = None
                            logger.info('[audit] re-activated previously dead channel: %s', ch.name)
                    continue   # DASH — skip HLS checks below

                # EXT-X-KEY only appears in media playlists, not master playlists.
                # If we landed on a master, parse stream_info then fetch the first
                # variant to continue DRM / VOD checks on the media playlist.
                if '#EXT-X-STREAM-INF' in manifest_text:
                    stream_info = _parse_stream_info(manifest_text)
                    if stream_info:
                        ch.stream_info = stream_info
                        logger.debug('[audit] stream_info for %s: %s %s %s',
                                     ch.name,
                                     stream_info.get('max_resolution') or '?',
                                     stream_info.get('video_codec') or '?',
                                     '4K' if stream_info.get('has_4k') else '')
                    variant_url = None
                    for line in manifest_text.splitlines():
                        line = line.strip()
                        if line and not line.startswith('#'):
                            variant_url = _urljoin(manifest_url, line)
                            break
                    if variant_url and not variant_url.lower().split('?')[0].endswith('.ts'):
                        try:
                            rv = sess.get(variant_url, timeout=10)
                            if rv.status_code == 200:
                                manifest_text = rv.text
                                logger.debug('[audit] variant fetched for %s (%d bytes)',
                                             ch.name, len(manifest_text))
                            else:
                                logger.debug('[audit] variant returned %d for %s',
                                             rv.status_code, ch.name)
                        except Exception as ve:
                            logger.debug('[audit] variant fetch failed for %s: %s', ch.name, ve)

                if (
                    'EXT-X-PLAYLIST-TYPE:VOD' in manifest_text
                    and '#EXT-X-ENDLIST' in manifest_text
                ):
                    if _audit_ignore_vod:
                        continue
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'VOD'
                    vod += 1
                    report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'vod', 'reason': 'VOD'})
                    logger.info('[audit] finished VOD (not live): %s', ch.name)
                    continue

                drm = inspect_hls_drm(manifest_text)
                if drm:
                    _drm_type = drm.get('drm_type', 'DRM')
                    if _bridge_capable and _drm_bridge_mode:
                        # Bridge mode + source can serve a browser-decryptable variant:
                        # keep the channel active and mark it for the PrismCast bridge — it's
                        # held out of the standard feed (unplayable on a normal client) but
                        # bridged in the PrismCast feed.
                        ch.requires_drm_bridge = True
                        if not ch.is_active:
                            ch.is_active = True
                        ch.disable_reason = None
                        bridged += 1
                        report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'drm_bridge', 'reason': _drm_type})
                        logger.info('[audit] DRM→PrismCast bridge: %s (%s)', ch.name, _drm_type)
                    else:
                        # Disable mode (or non-bridge-capable source): drop it as before.
                        ch.requires_drm_bridge = False
                        ch.is_active      = False
                        ch.is_enabled     = False
                        ch.disable_reason = f'DRM:{_drm_type}'
                        flagged += 1
                        report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'drm', 'reason': _drm_type})
                        logger.info('[audit] DRM: %s  →  %s (%s)', ch.name, manifest_url[:80], _drm_type)
                else:
                    # Clear HLS — plays directly. Clear any stale bridge marker so the
                    # channel returns to the standard feed, and re-activate if it was dead.
                    if getattr(ch, 'requires_drm_bridge', False):
                        ch.requires_drm_bridge = False
                        logger.info('[audit] DRM-bridge cleared (now clear HLS): %s', ch.name)
                    if not ch.is_active:
                        ch.is_active = True
                        ch.disable_reason = None
                        logger.info('[audit] re-activated previously dead channel: %s', ch.name)

            except Exception as e:
                if _is_transient_network_error(e):
                    logger.warning('[audit] transient audit failure for %s: %s', ch.name, e)
                    errors += 1
                    continue
                logger.warning('[audit] error for %s: %s', ch.name, e)
                errors += 1
                consecutive_errors += 1

            finally:
                if i % 25 == 0:
                    source.last_audited_at = datetime.now(timezone.utc)
                    db.session.commit()
                    persist_source_cache_updates(source.id, {'last_audit_result': {
                        'total': i, 'checked': checked, 'flagged': flagged, 'bridged': bridged,
                        'dead': dead, 'vod': vod, 'errors': errors, 'skipped_403': skipped_403,
                        'ts': datetime.now(timezone.utc).isoformat(),
                        'partial': True,
                    }})
                    _audit_progress(i, total, flagged, dead, vod, errors, skipped_403)
                    logger.info('[audit] %s: %d/%d — checked=%d flagged=%d dead=%d vod=%d errors=%d skipped_403=%d',
                                source_name, i, total, checked, flagged, dead, vod, errors, skipped_403)

                if source_name == 'plex':
                    logger.debug('[audit-debug] %s %d/%d finish elapsed=%.2fs checked=%d dead=%d flagged=%d vod=%d errors=%d',
                                source_name, i, total, _time.monotonic() - locals().get('_audit_item_t0', _time.monotonic()),
                                checked, dead, flagged, vod, errors)

                _time.sleep(0.3)

        source.last_audited_at = datetime.now(timezone.utc)
        db.session.commit()
        persist_source_cache_updates(source.id, {
            'last_audit_result': {
                'total': total, 'checked': checked, 'flagged': flagged, 'bridged': bridged,
                'dead': dead, 'vod': vod, 'errors': errors, 'skipped_403': skipped_403,
                'ts': datetime.now(timezone.utc).isoformat(),
            },
            'last_audit_report': {
                'channels': report_channels,
                'ts': datetime.now(timezone.utc).isoformat(),
            },
        })
        _audit_progress(0, 0, phase='done')
        logger.info('[audit] %s: done — total=%d checked=%d flagged=%d bridged=%d dead=%d vod=%d errors=%d skipped_403=%d',
                    source_name, total, checked, flagged, bridged, dead, vod, errors, skipped_403)


def run_stream_audit_recheck(source_name: str, channel_ids: list):
    """
    Re-audit a specific subset of channels (e.g. rate-limited ones from last run).
    Merges results back into last_audit_report and last_audit_result in-place.
    """
    logger.info('[audit-recheck] %s: starting recheck of %d channel(s): %s',
                source_name, len(channel_ids), channel_ids)
    with flask_app.app_context():
        source = Source.query.filter_by(name=source_name).first()
        if not source:
            logger.warning('[audit-recheck] %s: source not found', source_name)
            return
        if not source.is_enabled:
            logger.info('[audit-recheck] %s: source disabled, skipping', source_name)
            return

        scraper_cls = registry.get(source_name)
        if not scraper_cls:
            logger.warning('[audit-recheck] %s: no scraper registered', source_name)
            return

        scraper = scraper_cls(config=source.config or {})
        try:
            scraper.pre_run_setup()
        except Exception:
            pass

        channels = Channel.query.filter(Channel.id.in_(channel_ids)).all()
        total = len(channels)
        if not total:
            return

        _audit_key = f'audit:progress:{source_name}'
        try:
            _redis_rc = redis.from_url(flask_app.config['REDIS_URL'])
            _redis_rc.ping()
        except Exception:
            _redis_rc = None

        import json as _json_rc
        def _rc_progress(done, total_, phase='checking'):
            if not _redis_rc:
                return
            try:
                if phase == 'done':
                    _redis_rc.delete(_audit_key)
                else:
                    _redis_rc.setex(_audit_key, 600, _json_rc.dumps({
                        'phase': 'recheck', 'done': done, 'total': total_,
                        'ts': _time.time(),
                    }))
            except Exception:
                pass

        _rc_progress(0, total)
        sess = scraper.session
        recheck_results = {}  # channel_id → {'status', 'reason'} or None if ok

        for i, ch in enumerate(channels, 1):
            try:
                _resolve = getattr(scraper, 'audit_resolve', scraper.resolve)
                try:
                    resolved_url = _resolve(ch.stream_url)
                except StreamDeadError as dead_exc:
                    ch.is_active = False
                    ch.is_enabled = False
                    ch.disable_reason = 'Dead'
                    recheck_results[ch.id] = {
                        'status': 'dead',
                        'reason': _audit_reason_from_exception(dead_exc),
                    }
                    _rc_progress(i, total)
                    continue
                except Exception as re_exc:
                    recheck_results[ch.id] = {
                        'status': 'rate-limited',
                        'reason': f'Resolve failed: {_audit_reason_from_exception(re_exc)}',
                    }
                    _rc_progress(i, total)
                    continue

                try:
                    r = sess.get(resolved_url, timeout=15, allow_redirects=True)
                except Exception as req_exc:
                    recheck_results[ch.id] = {
                        'status': 'rate-limited',
                        'reason': f'Network error: {_audit_reason_from_exception(req_exc)}',
                    }
                    _rc_progress(i, total)
                    continue

                if r.status_code in (403, 429, 503):
                    _time.sleep(30)
                    r = sess.get(resolved_url, timeout=15, allow_redirects=True)

                if r.status_code in (400, 404, 410, 422):
                    ch.is_active = False
                    ch.is_enabled = False
                    ch.disable_reason = 'Dead'
                    recheck_results[ch.id] = {'status': 'dead', 'reason': f'HTTP {r.status_code}'}
                    _rc_progress(i, total)
                    continue

                if r.status_code in (403, 429, 503):
                    recheck_results[ch.id] = {'status': 'rate-limited', 'reason': f'HTTP {r.status_code}'}
                    _rc_progress(i, total)
                    continue

                if r.status_code != 200:
                    recheck_results[ch.id] = {'status': 'rate-limited', 'reason': f'HTTP {r.status_code}'}
                    _rc_progress(i, total)
                    continue

                # Live — re-enable if it was previously flagged
                if not ch.is_active:
                    ch.is_active = True
                    ch.disable_reason = None
                recheck_results[ch.id] = None  # ok

            except Exception as e:
                logger.warning('[audit-recheck] unexpected error for %s: %s', ch.name, e)
                recheck_results[ch.id] = {'status': 'rate-limited', 'reason': _audit_reason_from_exception(e)}

            _rc_progress(i, total)
            _time.sleep(0.3)

        db.session.commit()

        # Merge recheck results back into the saved report (now in source_cache)
        _audit_cache = load_source_cache(source.id)
        report = dict(_audit_cache.get('last_audit_report') or {})
        existing = {c['id']: c for c in (report.get('channels') or []) if c.get('id')}
        for ch_id, result in recheck_results.items():
            if result is None:
                # Now passing — remove from report
                existing.pop(ch_id, None)
            else:
                # Still failing — update reason in report
                ch = next((c for c in channels if c.id == ch_id), None)
                existing[ch_id] = {'id': ch_id, 'name': ch.name if ch else str(ch_id), **result}

        report['channels'] = list(existing.values())
        report['ts'] = datetime.now(timezone.utc).isoformat()

        # Update result summary skipped_403 count
        result_summary = dict(_audit_cache.get('last_audit_result') or {})
        still_limited = sum(1 for r in recheck_results.values() if r and r['status'] == 'rate-limited')
        result_summary['skipped_403'] = still_limited
        result_summary['ts'] = datetime.now(timezone.utc).isoformat()

        persist_source_cache_updates(source.id, {
            'last_audit_report': report,
            'last_audit_result': result_summary,
        })
        _rc_progress(0, 0, phase='done')
        logger.info('[audit-recheck] %s: done — rechecked=%d still_limited=%d',
                    source_name, total, still_limited)


def _make_progress_writer(source_name: str):
    """Return a callable(phase, done=0, total=0) that writes scrape progress to Redis.
    Phase 'done' deletes the key.  Silently no-ops if Redis is unavailable."""
    import json as _json
    key = f'scrape:progress:{source_name}'
    try:
        r = redis.from_url(flask_app.config['REDIS_URL'], socket_timeout=3, socket_connect_timeout=3)
        r.ping()
    except Exception:
        return lambda *a, **kw: None

    def _write(phase: str, done: int = 0, total: int = 0):
        try:
            if phase == 'done':
                r.delete(key)
            else:
                r.setex(key, 600, _json.dumps({'phase': phase, 'done': done, 'total': total}))
        except Exception:
            pass
    return _write


def _apply_scraper_config_updates(source, scraper) -> None:
    """Persist any config + cache updates the scraper queued.

    Config updates merge into source.config; cache updates upsert into the
    source_cache table (so they never bloat the config blob)."""
    if not scraper:
        return
    if scraper._pending_config_updates:
        persist_source_config_updates(source.id, scraper._pending_config_updates)
        logger.debug('[%s] persisting %d config update(s): %s',
                     source.name, len(scraper._pending_config_updates),
                     list(scraper._pending_config_updates.keys()))
    if getattr(scraper, '_pending_cache_updates', None):
        persist_source_cache_updates(source.id, scraper._pending_cache_updates)
        logger.debug('[%s] persisting %d cache update(s): %s',
                     source.name, len(scraper._pending_cache_updates),
                     list(scraper._pending_cache_updates.keys()))


def _epg_channels_for_source(source) -> list[Channel]:
    """Return DB channels that should participate in EPG refreshes.

    DRM-bridge channels stay is_active=True (they're only held out of the standard
    feed, not disabled), so they're naturally included here and keep their guide."""
    return source.channels.filter(Channel.is_active == True).all()


def _prewarm_logos(source_name: str, logo_urls: list[str], progress_cb=None) -> None:
    """
    Pre-warm the logo cache for *logo_urls*.  Runs inside the RQ job process
    after a full channel scrape; uses an internal ThreadPoolExecutor so fetches
    are concurrent without blocking the job thread.
    """
    from app.routes.images import prewarm_logo_cache
    urls = [u for u in logo_urls if u]
    if not urls:
        return

    def _cb(done: int, cb_total: int) -> None:
        if progress_cb:
            progress_cb('logos', done, cb_total)

    try:
        prewarm_logo_cache(urls, progress_cb=_cb)
    except Exception:
        logger.exception('[%s] logo cache pre-warm failed', source_name)


def _refresh_xml_artifacts() -> None:
    """Refresh master/feed XML and M3U artifacts after scrape commits land."""
    from app.generators.m3u import generate_gracenote_m3u, generate_m3u, generate_native_m3u, generate_prismcast_m3u, feed_gracenote_start, feed_namespace_start, feed_to_query_filters, _MASTER_GRACENOTE_START
    from app.generators.xmltv import write_xmltv

    for attempt in range(2):
        _settings = AppSettings.get()
        base_url = (
            (_settings.effective_public_base_url() or '').strip().rstrip('/')
            or 'http://localhost:5523'
        )
        xml_artifacts: list[tuple[str, Callable]] = [
            ('master', lambda fp: write_xmltv(fp, {}, base_url=base_url)),
        ]
        # PrismCast DRM-bridge artifacts are only built when a PrismCast server is
        # configured (most installs won't run one).
        prismcast_url = (_settings.effective_prismcast_url() or '').strip().rstrip('/')
        prismcast_inner = (_settings.effective_prismcast_inner_url() or base_url).strip().rstrip('/')
        m3u_artifacts: list[tuple[str, Callable]] = [
            ('master-m3u', lambda fp: fp.write(generate_m3u({}, base_url=base_url))),
        ]
        if prismcast_url:
            m3u_artifacts.append((
                'master-prismcast-m3u',
                lambda fp: fp.write(generate_prismcast_m3u(
                    {}, base_url=base_url, prismcast_url=prismcast_url, inner_base_url=prismcast_inner)),
            ))
        default_feed = Feed.query.filter_by(slug='default').first()
        default_gn_start = feed_gracenote_start(default_feed) if default_feed else _MASTER_GRACENOTE_START
        m3u_artifacts.append((
            'master-gracenote-m3u',
            lambda fp: fp.write(generate_gracenote_m3u({}, base_url=base_url, namespace_start=default_gn_start)),
        ))
        if prismcast_url:
            m3u_artifacts.append((
                'master-prismcast-gracenote-m3u',
                lambda fp: fp.write(generate_prismcast_m3u(
                    {}, base_url=base_url, prismcast_url=prismcast_url, inner_base_url=prismcast_inner,
                    namespace_start=default_gn_start, gracenote=True)),
            ))
        for feed in Feed.query.filter_by(is_enabled=True).order_by(Feed.slug).all():
            filters = feed_to_query_filters(feed.filters or {})
            xml_artifacts.append((
                f'feed-{feed.slug}',
                lambda fp, filters=filters, feed_name=feed.name: write_xmltv(
                    fp,
                    filters,
                    base_url=base_url,
                    feed_name=feed_name,
                ),
            ))
            xml_artifacts.append((
                f'feed-{feed.slug}-native',
                lambda fp, filters=filters, feed_name=feed.name: write_xmltv(
                    fp,
                    filters,
                    base_url=base_url,
                    feed_name=feed_name,
                    native=True,
                ),
            ))
            if feed.chnum_start is not None:
                std_kw = {'feed_chnum_start': feed.chnum_start, 'feed_id': feed.id}
            else:
                std_kw = {'namespace_start': feed_namespace_start(feed, gracenote=False)}
            m3u_artifacts.append((
                f'feed-{feed.slug}-m3u',
                lambda fp, filters=filters, std_kw=std_kw: fp.write(
                    generate_m3u(filters, base_url=base_url, **std_kw)
                ),
            ))
            # The native playlist carries all channels (incl. Gracenote-mapped ones
            # Plex ignores) and is description-stripped: Threadfin's M3U parser bleeds
            # the long, comma-bearing channel blurb into the channel name. The blurb is
            # a channel-level attribute only — program data still rides in the EPG XML —
            # so stripping it is lossless for guides and makes the native M3U import
            # cleanly into Threadfin/Plex and other bridges.
            m3u_artifacts.append((
                f'feed-{feed.slug}-native-m3u',
                lambda fp, filters=filters, std_kw=std_kw: fp.write(
                    generate_native_m3u(filters, base_url=base_url, include_description=False, **std_kw)
                ),
            ))
            if prismcast_url:
                m3u_artifacts.append((
                    f'feed-{feed.slug}-prismcast-m3u',
                    lambda fp, filters=filters, std_kw=std_kw: fp.write(
                        generate_prismcast_m3u(
                            filters, base_url=base_url, prismcast_url=prismcast_url,
                            inner_base_url=prismcast_inner, **std_kw)
                    ),
                ))
            if feed.chnum_start is not None:
                gn_kw = {'feed_chnum_start': feed.chnum_start, 'feed_id': feed.id}
            else:
                gn_kw = {'namespace_start': feed_gracenote_start(feed)}
            m3u_artifacts.append((
                f'feed-{feed.slug}-gracenote-m3u',
                lambda fp, filters=filters, gn_kw=gn_kw: fp.write(
                    generate_gracenote_m3u(filters, base_url=base_url, **gn_kw)
                ),
            ))
            if prismcast_url:
                m3u_artifacts.append((
                    f'feed-{feed.slug}-prismcast-gracenote-m3u',
                    lambda fp, filters=filters, gn_kw=gn_kw: fp.write(
                        generate_prismcast_m3u(
                            filters, base_url=base_url, prismcast_url=prismcast_url,
                            inner_base_url=prismcast_inner, gracenote=True, **gn_kw)
                    ),
                ))

        rebuilt_xml = 0
        for cache_key, writer in xml_artifacts:
            try:
                ensure_xml_artifact(cache_key, writer)
                rebuilt_xml += 1
            except Exception:
                logger.exception('[xml-cache] failed to refresh %s', cache_key)
        rebuilt_m3u = 0
        for cache_key, writer in m3u_artifacts:
            try:
                write_artifact(cache_key, writer, ext='m3u')
                rebuilt_m3u += 1
            except Exception:
                logger.exception('[m3u-cache] failed to refresh %s', cache_key)

        missing_m3u = [cache_key for cache_key, _writer in m3u_artifacts if get_artifact(cache_key, ext='m3u') is None]
        if missing_m3u and attempt == 0:
            logger.warning('[artifacts] missing M3U artifact(s) after refresh pass; retrying once: %s', missing_m3u)
            continue

        logger.info('[artifacts] refreshed %d XML artifact(s) and %d M3U artifact(s)', rebuilt_xml, rebuilt_m3u)
        if missing_m3u:
            logger.warning('[artifacts] still missing M3U artifact(s) after retry: %s', missing_m3u)
        break


def _refresh_xml_artifacts_job() -> None:
    # Forked child inherits the parent's root logger handlers.  Reset to a
    # single clean StreamHandler so the child never double-logs.
    logging.root.handlers = []
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(make_tz_formatter('%(asctime)s %(levelname)-8s %(name)s: %(message)s'))
    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(_h)
    with flask_app.app_context():
        # The forked child inherits the parent's SQLAlchemy connection pool,
        # and SQLite connections must never be used across a fork.  Replace
        # the pool without closing the parent's connections (SQLAlchemy's
        # documented post-fork recipe).
        db.engine.dispose(close=False)
        _refresh_xml_artifacts()


def _refresh_xml_artifacts_subprocess(timeout_seconds: int = 1800) -> None:
    proc = multiprocessing.Process(target=_refresh_xml_artifacts_job, name='xml-refresh')
    proc.start()
    proc.join(timeout_seconds)
    if proc.is_alive():
        logger.error('[xml-cache] refresh subprocess exceeded %ss; terminating', timeout_seconds)
        proc.terminate()
        proc.join(10)
    elif proc.exitcode not in (0, None):
        logger.error('[xml-cache] refresh subprocess exited with code %s', proc.exitcode)


def run_xml_refresh():
    # Runs on the 'fast' queue, whose SimpleWorker executes jobs in-process
    # (no fork).  Building ~150-200MB of artifacts inline permanently bloats
    # the worker's RSS via allocator fragmentation, so do the build in a
    # short-lived child process instead — the memory dies with the child.
    _refresh_xml_artifacts_subprocess()


def run_tvtv_cache_refresh():
    """Fetch 2 days of tvtv guide data for all indexed FAST stations and store in DB."""
    with flask_app.app_context():
        from app.tvtv_cache import refresh_tvtv_cache
        from app.tvtv_lookup import get_station_entry
        import sqlalchemy as sa

        # Applied channel IDs.
        applied = set(
            str(r) for r in db.session.execute(
                sa.select(sa.func.distinct(Channel.gracenote_id))
                .where(Channel.gracenote_id.isnot(None))
            ).scalars().all() if r
        )

        # Community map suggestion IDs — pre-cache so the suggestions modal can
        # show guide previews even before an ID has been applied to a channel.
        from app.gracenote_map import get_all_tmsids
        community = set(str(t) for t in (get_all_tmsids() or []) if t)

        # Only include IDs that are actually in the station index.
        station_ids = [sid for sid in (applied | community) if get_station_entry(sid)]
        logger.info('[tvtv-cache] fetching %d station IDs (%d applied + %d community-only)',
                    len(station_ids), len(applied & set(station_ids)),
                    len(community & set(station_ids) - applied))

        summary = refresh_tvtv_cache(days=2, station_ids=station_ids)
        logger.info('[tvtv-cache] refresh complete: %s', summary)


def _invalidate_and_refresh_xml() -> None:
    invalidate_xml_cache()
    # Subprocess so callers on the non-forking 'fast' worker (e.g. channel
    # auto-disable) don't accumulate the build's RSS.  Maintenance-queue
    # callers already run in a forked work-horse; the extra fork is free.
    _refresh_xml_artifacts_subprocess()


def _channel_ids_for_filters(filters: dict) -> list[int]:
    # Delegates to the canonical bulk-filter helper so this path can't drift
    # from the API (it previously did — silently ignoring feed/duplicates=unique
    # /drm=vod/presence=pinned). Lazy import avoids a circular import at load.
    from app.routes.api import _apply_channel_filters
    q = _apply_channel_filters(Channel.query.join(Source), filters)
    return [row[0] for row in q.with_entities(Channel.id).all()]


def run_gracenote_auto_clear():
    """Clear gracenote_id from all channels with gracenote_mode='auto'.
    Called when the user disables global auto-fill and confirms the clear."""
    with flask_app.app_context():
        rows = Channel.query.filter_by(gracenote_mode='auto').all()
        cleared = 0
        for ch in rows:
            if ch.gracenote_id:
                ch.gracenote_id = None
                cleared += 1
        db.session.commit()
        logger.info('[gracenote-clear] cleared gracenote_id from %d auto-mode channels', cleared)


def run_gracenote_clear_all():
    """Clear ALL Gracenote IDs and set all channels to mode='off'.
    Used by the settings-page 'Disable & Clear All' action."""
    with flask_app.app_context():
        rows = Channel.query.filter(Channel.gracenote_mode != 'off').all()
        count = 0
        for ch in rows:
            ch.gracenote_id = None
            ch.gracenote_mode = 'off'
            ch.gracenote_locked = False
            count += 1
        db.session.commit()
        _invalidate_and_refresh_xml()
        logger.info('[gracenote-clear-all] set %d channels to off mode', count)


def _purge_source_channels_and_programs(source) -> tuple[int, int]:
    """Delete all channels and programs belonging to `source`. Batches the
    channel-id IN-clause to stay under SQLite's default variable limit."""
    ch_ids = [row[0] for row in source.channels.with_entities(Channel.id).all()]
    deleted_programs = 0
    _ID_BATCH = 900
    for i in range(0, len(ch_ids), _ID_BATCH):
        deleted_programs += Program.query.filter(
            Program.channel_id.in_(ch_ids[i:i + _ID_BATCH])
        ).delete(synchronize_session=False)
    deleted_channels = source.channels.delete(synchronize_session=False)
    return deleted_channels or 0, deleted_programs


def run_source_channel_purge(source_id: int):
    with flask_app.app_context():
        source = Source.query.get(source_id)
        if not source:
            logger.warning('[source-purge] source_id=%s not found', source_id)
            return
        deleted_channels, deleted_programs = _purge_source_channels_and_programs(source)
        db.session.commit()
        _invalidate_and_refresh_xml()
        logger.info(
            '[source-purge] source=%s deleted %d channels and %d programs',
            source.name, deleted_channels, deleted_programs,
        )


def run_bulk_channel_update(filters: dict, enable: bool):
    with flask_app.app_context():
        ids = _channel_ids_for_filters(filters or {})
        updated = 0
        if ids:
            # Any explicit bulk enable/disable counts as reviewing the channel, so
            # clear the 'pending' marker — it leaves the "Needs review" filter.
            values = {'is_enabled': enable, 'review_state': 'approved'}
            if enable:
                values['is_active'] = True
                values['disable_reason'] = None
                values['last_seen_at'] = datetime.now(timezone.utc)
                values['missed_scrapes'] = 0
            updated = Channel.query.filter(Channel.id.in_(ids)).update(
                values, synchronize_session=False
            )
            db.session.commit()
            _invalidate_and_refresh_xml()
        logger.info(
            '[channel-bulk] %s %d channel(s)',
            'enabled' if enable else 'disabled',
            updated,
        )


def run_bulk_channel_review(filters: dict):
    with flask_app.app_context():
        ids = _channel_ids_for_filters(filters or {})
        updated = 0
        if ids:
            updated = Channel.query.filter(Channel.id.in_(ids)).update(
                {'review_state': 'approved'}, synchronize_session=False
            )
            db.session.commit()
            # No XML refresh: these channels stay is_enabled=False, so M3U/EPG
            # output is unchanged by clearing the pending marker.
        logger.info('[channel-review-bulk] marked %d channel(s) reviewed', updated)


def run_channel_auto_disable(channel_id: int, reason: str):
    with flask_app.app_context():
        ch = Channel.query.get(channel_id)
        if not ch:
            logger.warning('[play] auto-disable skipped; channel_id=%s not found', channel_id)
            return
        if not ch.is_active and not ch.is_enabled and ch.disable_reason == reason:  # exact match is fine; reason already includes DRM type
            return
        ch_name = ch.name
        ch_source_name = ch.source.name if ch.source else '?'
        ch_source_channel_id = ch.source_channel_id

        def _commit_with_retry():
            for _attempt in range(3):
                try:
                    db.session.commit()
                    return
                except _SAOperationalError:
                    db.session.rollback()
                    if _attempt == 2:
                        raise
                    time.sleep(3 * (_attempt + 1))

        # DRM caught at play time: if bridge mode is on and the source can be bridged, keep
        # the channel active and route it to the PrismCast feed (same as the audit) instead
        # of disabling it. Otherwise fall through to the legacy disable.
        if reason.startswith('DRM') and bool(AppSettings.get().drm_bridge_enabled):
            scraper_cls = registry.get(ch_source_name)
            if scraper_cls and getattr(scraper_cls, 'license_url', None):
                if ch.requires_drm_bridge and ch.is_active:
                    return  # already bridged
                ch.requires_drm_bridge = True
                ch.is_active = True
                ch.disable_reason = None
                _commit_with_retry()
                _invalidate_and_refresh_xml()
                logger.info(
                    '[play] %s detected — bridged channel %s (%s/%s) via PrismCast',
                    reason, ch_name, ch_source_name, ch_source_channel_id,
                )
                return

        was_active = ch.is_active
        ch.requires_drm_bridge = False
        ch.is_active = False
        ch.is_enabled = False
        ch.disable_reason = reason
        if was_active:
            ch.went_inactive_at = datetime.now(timezone.utc)
        _commit_with_retry()
        _invalidate_and_refresh_xml()
        logger.warning(
            '[play] %s detected — auto-disabled channel %s (%s/%s)',
            reason,
            ch_name,
            ch_source_name,
            ch_source_channel_id,
        )


def _fresh_epg_sids(source, horizon_hours: float = 2.0) -> set[str]:
    """Return source_channel_ids whose programs already cover the next horizon_hours.

    Used to skip redundant content-proxy calls for channels whose EPG data is
    still fresh, reducing API request volume during scrape runs.
    """
    min_end = datetime.now(timezone.utc) + timedelta(hours=horizon_hours)
    rows = (
        db.session.query(Channel.source_channel_id)
        .join(Program, Program.channel_id == Channel.id)
        .filter(
            Channel.source_id == source.id,
            Program.end_time > min_end,
        )
        .distinct()
        .all()
    )
    return {row[0] for row in rows}


_WIN1252_REMAP = str.maketrans({
    0x80: '€',  0x81: None,  0x82: '‚',  0x83: 'ƒ',  0x84: '„',
    0x85: '…',  0x86: '†',  0x87: '‡',  0x88: 'ˆ',  0x89: '‰',
    0x8A: 'Š',  0x8B: '‹',  0x8C: 'Œ',  0x8D: None,  0x8E: 'Ž',
    0x8F: None,  0x90: None,  0x91: ''',  0x92: ''',  0x93: '"',
    0x94: '"',  0x95: '•',  0x96: '–',  0x97: '—',  0x98: '˜',
    0x99: '™',  0x9A: 'š',  0x9B: '›',  0x9C: 'œ',  0x9D: None,
    0x9E: 'ž',  0x9F: 'Ÿ',
    0x00A0: ' ',   # NO-BREAK SPACE → regular space
    0x200B: None,  # ZERO WIDTH SPACE
    0xFFFD: None,  # REPLACEMENT CHARACTER
})


def _try_fix_mojibake(s: str) -> str:
    """Fix UTF-8 bytes that were decoded as Latin-1 (up to two rounds)."""
    for _ in range(2):
        try:
            fixed = s.encode('latin-1').decode('utf-8')
            if fixed == s:
                break
            s = fixed
        except (UnicodeEncodeError, UnicodeDecodeError):
            break
    return s


def _sanitize_description(s: str | None) -> str | None:
    if not s:
        return None
    s = _try_fix_mojibake(s)
    s = s.translate(_WIN1252_REMAP)
    s = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]+', '', s)  # strip remaining C0 controls
    s = re.sub(r'[\r\n\t]+', ' ', s)
    s = re.sub(r'  +', ' ', s).strip()
    return s or None


def _validate_logo_url(url: str, cache: dict[str, bool]) -> bool:
    cached = cache.get(url)
    if cached is not None:
        return cached

    ok = False
    try:
        resp = _req.head(url, allow_redirects=True, timeout=5)
        content_type = (resp.headers.get('content-type') or '').lower()
        ok = resp.ok and (not content_type or content_type.startswith('image/'))
        if not ok:
            resp = _req.get(url, allow_redirects=True, timeout=5, stream=True)
            content_type = (resp.headers.get('content-type') or '').lower()
            ok = resp.ok and content_type.startswith('image/')
            resp.close()
    except Exception:
        ok = False

    cache[url] = ok
    return ok


def _resolved_logo_url(existing_logo: str | None, incoming_logo: str | None, cache: dict[str, bool]) -> str | None:
    current = (existing_logo or '').strip() or None
    incoming = (incoming_logo or '').strip() or None

    if not incoming:
        return current
    if not current or incoming == current:
        return incoming
    if not incoming.startswith(('http://', 'https://')):
        return current
    # Never keep a non-absolute existing URL when we have an absolute replacement.
    if not (current or '').startswith(('http://', 'https://')):
        if _validate_logo_url(incoming, cache):
            return incoming
        return current
    if _validate_logo_url(incoming, cache):
        return incoming
    return current


def _refresh_auto_channel_numbers() -> None:
    """Assign stable automatic numbers to non-pinned active channels.

    Channel.number is system-managed unless the user pins it. Scraper-supplied
    numbers are ignored; this allocator preserves existing non-pinned numbers
    where possible and only fills gaps for channels that are new or invalid.

    Standard and Gracenote channels share a single contiguous block for the
    master (source-based) numbering: standard channels first, then Gracenote
    channels starting immediately after the highest standard number.

    For feeds with an explicit chnum_start, all channels (standard and Gracenote)
    share one unified number pool persisted in FeedChannelNumber.  A channel keeps
    its assigned number when its Gracenote status changes — it just moves between
    the standard and Gracenote M3U files without being renumbered.
    """
    from app.generators.m3u import (
        _build_source_chnum_map, _build_sticky_gn_chnum_map,
        _build_feed_chnum_map, _selected_channel_stubs, feed_to_query_filters,
    )
    from app.models import Feed, FeedChannelNumber

    with db.session.no_autoflush:
        all_channels = (
            Channel.query
            .join(Source)
            .filter(
                Channel.is_active == True,
                Channel.is_enabled == True,
                Source.is_enabled == True,
                Source.epg_only == False,
                Channel.stream_url != None,
            )
            .all()
        )
    if not all_channels:
        return

    std_channels = [ch for ch in all_channels if not (ch.gracenote_id or '').strip()]
    gn_channels  = [ch for ch in all_channels if (ch.gracenote_id or '').strip()]

    std_map, _ = _build_source_chnum_map(std_channels) if std_channels else ({}, [])

    gn_map = {}
    if gn_channels:
        gn_start = (max(std_map.values()) + 1) if std_map else (AppSettings.get().effective_global_chnum_start() or 1)
        gn_map = _build_sticky_gn_chnum_map(gn_channels, gn_start, set(std_map.values()))

    chnum_map = {**std_map, **gn_map}
    for ch in all_channels:
        if ch.number_pinned:
            continue
        next_number = chnum_map.get(ch.id)
        if ch.number != next_number:
            ch.number = next_number

    # Persist feed-specific channel numbers for feeds with explicit chnum_start.
    all_channel_ids = {ch.id for ch in all_channels}
    feeds_with_chnum = Feed.query.filter(
        Feed.chnum_start != None,
        Feed.is_enabled == True,
    ).all()
    for feed in feeds_with_chnum:
        filters = feed_to_query_filters(feed.filters or {})
        with db.session.no_autoflush:
            std_stubs = _selected_channel_stubs(filters, gracenote=False)
            gn_stubs  = _selected_channel_stubs(filters, gracenote=True)
        # Sort the combined list by master number so GN and non-GN interleave
        # by position rather than arriving as two separate blocks.
        all_stubs = sorted(
            std_stubs + gn_stubs,
            key=lambda ch: (ch.number is None, ch.number or 0, (ch.name or '').lower()),
        )
        feed_channel_ids = {s.id for s in all_stubs}

        # Load existing stored numbers for stickiness.
        stored = {
            fcn.channel_id: fcn.number
            for fcn in FeedChannelNumber.query.filter_by(feed_id=feed.id).all()
        }
        new_map   = _build_feed_chnum_map(all_stubs, feed.chnum_start, stored_numbers=stored)

        # Upsert new assignments.
        existing_fcn = {fcn.channel_id: fcn for fcn in FeedChannelNumber.query.filter_by(feed_id=feed.id).all()}
        for channel_id, number in new_map.items():
            if channel_id in existing_fcn:
                if existing_fcn[channel_id].number != number:
                    existing_fcn[channel_id].number = number
            else:
                db.session.add(FeedChannelNumber(feed_id=feed.id, channel_id=channel_id, number=number))

        # Remove stale rows for channels no longer in this feed.
        for channel_id, fcn in existing_fcn.items():
            if channel_id not in feed_channel_ids:
                db.session.delete(fcn)


_IDENTITY_STOPWORDS = {
    'the', 'a', 'an', 'and', '&', 'by', 'of', 'tv', 'hd', 'sd', 'uhd', '4k',
    'channel', 'network', 'live', 'plus', 'es', 'en', 'español', 'espanol',
}


def _name_tokens(name: str) -> set[str]:
    """Lowercase alphanumeric word set with stopwords removed, for identity comparison."""
    words = re.findall(r'[a-z0-9]+', (name or '').lower())
    return {w for w in words if w not in _IDENTITY_STOPWORDS}


def _is_identity_swap(old_name: str, new_name: str) -> bool:
    """True when a channel's name changed enough that it's effectively a different
    channel occupying the same upstream slot (e.g. Vizio reusing a channelId).

    Conservative: ignores case/stopword/HD-suffix noise and only fires when the
    two names share almost no meaningful words, so ordinary renames don't trip it.
    """
    old = (old_name or '').strip()
    new = (new_name or '').strip()
    if not old or not new or old == new:
        return False
    a, b = _name_tokens(old), _name_tokens(new)
    if not a or not b:
        return False
    # Substring/superset rename (e.g. "A&E" → "A&E HD") is not a swap.
    if a <= b or b <= a:
        return False
    overlap = len(a & b) / len(a | b)
    return overlap < 0.34


def _extract_gracenote_id(cd):
    """Gracenote ID a scraper supplied for a channel: the explicit field, or the
    Roku-style "{play_id}|{gracenote_id}" slug encoding."""
    gid = getattr(cd, 'gracenote_id', None) or None
    if not gid and getattr(cd, 'slug', None) and '|' in cd.slug:
        candidate = cd.slug.split('|', 1)[1].strip()
        if candidate and candidate.isdigit():
            gid = candidate
    return gid


def _backfill_stale_native_gracenote(source, channel_data_list):
    """One-time per source: re-sync auto-mode native Gracenote IDs to the source's
    current value, clearing IDs left stale on rotating slots that settled before
    the content-change re-sync existed. CSV and manual IDs are preserved, and no
    review flag is set (silent correction). Reuses the data this scrape already
    fetched, so it costs no extra network and retries until a scrape succeeds."""
    from app.gracenote_map import lookup_gracenote
    current = {cd.source_channel_id: _extract_gracenote_id(cd)
               for cd in channel_data_list if cd.source_channel_id is not None}
    cleared = resynced = 0
    for ch in source.channels.all():
        mode = (getattr(ch, 'gracenote_mode', None)
                or ('manual' if getattr(ch, 'gracenote_locked', False) else 'auto')).strip().lower()
        if mode != 'auto' or ch.source_channel_id not in current:
            continue
        stored = ch.gracenote_id or None
        if not stored:
            continue  # never introduce a new ID here
        csv_match = lookup_gracenote(source.name, ch.source_channel_id)
        if csv_match and csv_match.get('tmsid') == stored:
            continue  # community-CSV mapping — preserve
        new_val = current[ch.source_channel_id]  # source's current native value (may be None)
        if stored == (new_val or None):
            continue
        ch.gracenote_id = new_val
        if new_val is None:
            cleared += 1
            logger.info('[%s] gracenote backfill: cleared stale ID %s on %r (id=%s)',
                        source.name, stored, ch.name, ch.source_channel_id)
        else:
            resynced += 1
    if cleared or resynced:
        logger.info('[%s] gracenote backfill: cleared %d stale, re-synced %d native ID(s)',
                    source.name, cleared, resynced)


def _sync_intrinsic_drm_bridge(source) -> None:
    """A DASH channel from a DRM-capable source (e.g. Amazon, Sling) is intrinsically
    bridge-only — it can never play on a normal client. Mirror that in the
    requires_drm_bridge flag (gated on bridge mode) so the admin badge/filter/count match
    the feed routing without needing an audit. The standard feed already excludes these by
    stream type; this just keeps the flag — and its UI — in sync. Only flips the flag,
    never is_active (the audit owns disable)."""
    scraper_cls = registry.get(source.name)
    if not (scraper_cls and getattr(scraper_cls, 'license_url', None)):
        return
    want = bool(AppSettings.get().drm_bridge_enabled)
    changed = 0
    for ch in source.channels.filter(Channel.stream_type == 'dash').all():
        if bool(ch.requires_drm_bridge) != want:
            ch.requires_drm_bridge = want
            changed += 1
    if changed:
        logger.info('[%s] requires_drm_bridge synced on %d DASH channel(s) (bridge=%s)',
                    source.name, changed, want)


def _upsert_channels(source, channel_data_list, gracenote_auto_fill: bool = True, active_geos: set | None = None,
                     miss_threshold: int = _CHANNEL_MISS_THRESHOLD, rehome_by_guide_key: bool = False):
    existing = {ch.source_channel_id: ch for ch in source.channels.all()}

    # Build a guide_key → channel index so we can re-use an existing DB row
    # when a scraper assigns a new uuid to the same content (e.g. Vidaa rotating
    # a channel slot).  Only channels with a guide_key participate; if two DB rows
    # share the same guide_key the slot is ambiguous and rehoming is skipped.
    gk_index: dict[str, object] = {}
    gk_ambiguous: set[str] = set()
    if rehome_by_guide_key:
        for _src_id, _ch in existing.items():
            _gk = _ch.guide_key or ''
            if not _gk:
                continue
            if _gk in gk_index:
                gk_ambiguous.add(_gk)
            else:
                gk_index[_gk] = _ch
    incoming_ids = {cd.source_channel_id for cd in channel_data_list} if rehome_by_guide_key else set()

    logo_validation_cache: dict[str, bool] = {}
    seen_at = datetime.now(timezone.utc)

    # Resolve how newly-discovered channels should enter: 'enabled' (flow straight
    # into feeds, the historical default) or 'review' (held in the review queue with
    # is_enabled=False, review_state='pending' — invisible to every feed/M3U/EPG
    # until a user approves).  Per-source policy wins; 'inherit' defers to the
    # global AppSettings.auto_allow_new_channels switch.  Only affects true inserts;
    # returning/rehomed channels keep their prior state.
    _policy = (getattr(source, 'new_channel_policy', None) or 'inherit')
    if _policy == 'inherit':
        _auto_allow = getattr(AppSettings.get(), 'auto_allow_new_channels', True)
        _policy = 'enabled' if _auto_allow else 'review'
    _born_pending = _policy == 'review'

    for cd in channel_data_list:
        if cd.name:
            try:
                cd.name = cd.name.encode('latin-1').decode('utf-8')
            except (UnicodeDecodeError, UnicodeEncodeError):
                pass
            cd.name = cd.name.replace(',', '')
        ch = existing.get(cd.source_channel_id)

        # Secondary lookup: when the uuid changed but guide_key is stable, migrate
        # the existing DB row to the new uuid so user settings are preserved.
        if ch is None and rehome_by_guide_key:
            _gk = getattr(cd, 'guide_key', None) or ''
            if _gk and _gk not in gk_ambiguous:
                _candidate = gk_index.get(_gk)
                if (_candidate is not None
                        and _candidate.missed_scrapes > 0
                        and _candidate.source_channel_id not in incoming_ids):
                    logger.info(
                        '[%s] rehoming channel by guide_key %r: %s → %s (%r → %r)',
                        source.name, _gk,
                        _candidate.source_channel_id, cd.source_channel_id,
                        _candidate.name, cd.name,
                    )
                    del existing[_candidate.source_channel_id]
                    _candidate.source_channel_id = cd.source_channel_id
                    existing[cd.source_channel_id] = _candidate
                    del gk_index[_gk]  # prevent double-rehoming the same slot
                    ch = _candidate

        gracenote_id = _extract_gracenote_id(cd)

        if ch:
            stream_url_changed = ch.stream_url != cd.stream_url
            # Detect when a slot now carries *different content* than before — e.g.
            # Vizio rotating a fixed FEATURED promo slot from "Duck Dynasty by A&E"
            # to "Garfield and Friends". This invalidates any retained auto
            # Gracenote ID (otherwise the slot keeps serving the previous
            # occupant's guide). Prefer guide_key, the source's content/schedule
            # key, when both sides have one; fall back to a conservative name-swap
            # heuristic for sources that don't expose a guide_key. A pure rebrand
            # with a stable guide_key is NOT a content change.
            old_name = ch.name
            old_gk   = (ch.guide_key or '').strip()
            new_gk   = (getattr(cd, 'guide_key', None) or '').strip()
            if old_gk and new_gk:
                content_changed = old_gk != new_gk
            else:
                content_changed = _is_identity_swap(old_name, cd.name)
            if content_changed:
                # Persistent rotator signal — count every swap regardless of enabled
                # state, so a rotating slot (Vizio FEATURED*, etc.) is flagged on any
                # source. Snapshot the prior occupant before name/gracenote_id are
                # overwritten below (used by the changes report and badge tooltip).
                ch.content_swap_count    = (ch.content_swap_count or 0) + 1
                ch.previous_name         = old_name
                ch.previous_gracenote_id = ch.gracenote_id or None
                if ch.is_enabled:
                    logger.warning('[%s] enabled slot content changed (id=%s): %r → %r (guide_key %r → %r); flagging for Gracenote review',
                                   source.name, ch.source_channel_id, old_name, cd.name,
                                   old_gk or None, new_gk or None)
                    ch.identity_changed_at = seen_at
            ch.name          = cd.name
            ch.stream_url    = cd.stream_url
            ch.stream_type   = cd.stream_type
            old_logo_url = ch.logo_url
            if not getattr(ch, 'logo_url_pinned', False):
                next_logo = _resolved_logo_url(ch.logo_url, cd.logo_url, logo_validation_cache)
                if next_logo != (ch.logo_url or None) and next_logo != (cd.logo_url or '').strip():
                    logger.info('[%s] keeping existing logo for %s after invalid replacement URL from scrape',
                                source.name, cd.name)
                ch.logo_url = next_logo
                if old_logo_url and old_logo_url != (next_logo or ''):
                    delete_cached_logo(old_logo_url)
                    logger.debug('[%s] evicted cached logo for %s (URL changed)', source.name, cd.name)
            ch.slug          = cd.slug
            ch.category      = ch.category_override or category_for_channel(cd.name, cd.category, source.name)
            ch.language      = ch.language_override or cd.language
            ch.country       = cd.country
            ch.tags          = ','.join(cd.tags) if getattr(cd, 'tags', None) else None
            if getattr(cd, 'description', None):
                ch.description = _sanitize_description(cd.description)
            if getattr(cd, 'guide_key', None):
                ch.guide_key = cd.guide_key
            # Don't resurrect channels the stream audit flagged as Dead, VOD, or DRM
            # unless the stream URL changed (source may have fixed the channel).
            _flagged = ch.disable_reason in ('Dead', 'VOD') or (ch.disable_reason or '').startswith('DRM')
            if _flagged and not stream_url_changed:
                ch.is_active  = False  # re-enforce — a prior scrape may have revived it
                ch.is_enabled = False
            else:
                if (ch.missed_scrapes or 0) > 0:
                    ch.returned_at = seen_at
                ch.is_active = True
                if stream_url_changed and _flagged:
                    ch.disable_reason = None  # clear flag; let next audit re-check
            ch.last_seen_at = seen_at
            ch.missed_scrapes = 0
            mode = (getattr(ch, 'gracenote_mode', None) or ('manual' if getattr(ch, 'gracenote_locked', False) else 'auto')).strip().lower()
            # Auto mode tracks the source. Normally we keep an existing ID when a
            # scrape returns nothing (transient source gaps shouldn't wipe a good
            # ID). But when the slot's *content* changed we must re-sync to the
            # source's current value — even if that's None — so a rotating slot
            # can't keep serving the previous occupant's Gracenote schedule.
            # Manual/Off modes are user-owned and left untouched (the content-change
            # flag still surfaces so the user can fix a now-wrong manual ID).
            if mode == 'auto' and gracenote_auto_fill:
                if content_changed:
                    if (ch.gracenote_id or None) != (gracenote_id or None):
                        logger.warning('[%s] re-syncing auto Gracenote ID on content change for %r: %s → %s',
                                       source.name, cd.name, ch.gracenote_id, gracenote_id)
                    ch.gracenote_id = gracenote_id  # may be None → drops the stale ID
                elif gracenote_id is not None:
                    ch.gracenote_id = gracenote_id
        else:
            db.session.add(Channel(
                source_id         = source.id,
                source_channel_id = cd.source_channel_id,
                name              = cd.name,
                stream_url        = cd.stream_url,
                stream_type       = cd.stream_type,
                logo_url          = cd.logo_url,
                slug              = cd.slug,
                category          = category_for_channel(cd.name, cd.category, source.name),
                language          = cd.language,
                country           = cd.country,
                tags              = ','.join(cd.tags) if getattr(cd, 'tags', None) else None,
                description       = _sanitize_description(cd.description) if getattr(cd, 'description', None) else None,
                number            = None,
                gracenote_id      = gracenote_id if gracenote_auto_fill else None,
                gracenote_locked  = False,
                gracenote_mode    = (getattr(cd, 'gracenote_mode', None) or 'auto'),
                guide_key         = getattr(cd, 'guide_key', None),
                last_seen_at      = seen_at,
                first_seen_at     = seen_at,
                is_enabled        = not _born_pending,
                review_state      = 'pending' if _born_pending else 'approved',
                missed_scrapes    = 0,
            ))

    seen = {cd.source_channel_id for cd in channel_data_list}
    existing_active_ids = {ch_id for ch_id, ch in existing.items() if ch.is_active}
    missing_active_ids = existing_active_ids - seen

    # Channels from regions the scraper no longer has configured are intentionally
    # absent — exclude them from the collapse ratio so a region removal doesn't
    # trigger the false-positive guard.
    if active_geos is not None:
        region_removed_ids = {
            ch_id for ch_id in missing_active_ids
            if (existing[ch_id].country or '').upper() not in active_geos
        }
    else:
        region_removed_ids = set()
    missing_active_organic = missing_active_ids - region_removed_ids

    # Guard against upstream/parser glitches returning a tiny partial lineup.
    # If we previously had a substantial active set and the new fetch would
    # deactivate most of it, keep the old rows active and log loudly instead
    # of collapsing the source to a handful of channels.
    organic_existing = len(existing_active_ids) - len(region_removed_ids)
    if organic_existing > 0:
        missing_ratio = len(missing_active_organic) / max(organic_existing, 1)
    else:
        missing_ratio = 0.0
    suspicious_collapse = (
        organic_existing >= 50
        and len(seen) < max(25, int(organic_existing * 0.35))
        and missing_ratio >= 0.6
    )

    # Always deactivate channels from removed regions regardless of collapse guard.
    # Clear last_seen_at so the orphan-cleanup query treats them as immediately
    # eligible — their last_seen_at reflects the previous scrape (today), which
    # would otherwise keep them past the N-day cutoff.
    for ch_id in region_removed_ids:
        ch = existing[ch_id]
        ch.missed_scrapes = (ch.missed_scrapes or 0) + 1
        if ch.is_active:
            ch.went_inactive_at = seen_at
        ch.is_active = False
        ch.last_seen_at = None
        logger.info(
            '[%s] marking inactive — region %s no longer configured: %s (%s)',
            source.name,
            ch.country,
            ch.name,
            ch.source_channel_id,
        )

    if suspicious_collapse:
        logger.warning(
            '[%s] suspicious channel refresh collapse: existing_active=%d incoming=%d missing_active=%d; preserving prior active rows',
            source.name,
            len(existing_active_ids),
            len(seen),
            len(missing_active_organic),
        )
    else:
        for ch_id, ch in existing.items():
            if ch_id not in seen and ch_id not in region_removed_ids:
                if not ch.is_active:
                    continue  # already inactive — don't touch to avoid bumping updated_at
                next_missed = (ch.missed_scrapes or 0) + 1
                ch.missed_scrapes = next_missed
                if next_missed >= miss_threshold:
                    if ch.scrape_pinned:
                        logger.info(
                            '[%s] missed %d scrapes but scrape_pinned — keeping active: %s (%s)',
                            source.name,
                            next_missed,
                            ch.name,
                            ch.source_channel_id,
                        )
                    else:
                        ch.is_active = False
                        ch.went_inactive_at = seen_at
                        logger.info(
                            '[%s] marking inactive after %d missed channel scrapes: %s (%s)',
                            source.name,
                            next_missed,
                            ch.name,
                            ch.source_channel_id,
                        )
    # One-time per source: correct Gracenote IDs left stale on rotating slots that
    # settled before the content-change re-sync existed (e.g. Vizio FEATURED promo
    # carousels still pointing at a previous occupant's schedule). Gated so it runs
    # once, only on a scrape that actually returned channels, and only when
    # auto-fill is enabled — matching the forward-fix's auto-mode semantics.
    if (gracenote_auto_fill and channel_data_list
            and not getattr(source, 'gracenote_resync_done', False)):
        _backfill_stale_native_gracenote(source, channel_data_list)
        source.gracenote_resync_done = True
    _sync_intrinsic_drm_bridge(source)
    db.session.flush()
    _refresh_auto_channel_numbers()


def _prune_old_programs(batch_size: int = 1000):
    """Delete programs that ended more than 2 hours ago, in batches.

    Use timezone-aware UTC to match the rest of the worker's program handling
    and avoid Python 3.12's utcnow() deprecation warning.

    Batches are committed individually so the SQLite write lock is held only
    briefly and yielded between batches. The batch size is deliberately small:
    a 5k+ row DELETE committed in one shot can hold the single writer long
    enough to exhaust a concurrent scrape's busy_timeout (observed as
    '[source] DB locked' retries). Keeping each commit sub-second lets any
    other writer slip in between batches well within busy_timeout.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
    total_deleted = 0

    while True:
        ids = [
            row[0] for row in (
                Program.query
                .filter(Program.end_time < cutoff)
                .order_by(Program.end_time.asc())
                .with_entities(Program.id)
                .limit(batch_size)
                .all()
            )
        ]
        if not ids:
            break

        deleted = (
            Program.query
            .filter(Program.id.in_(ids))
            .delete(synchronize_session=False)
        ) or 0
        db.session.commit()
        total_deleted += deleted

        if deleted < batch_size:
            break

    if total_deleted:
        logger.info('[worker] pruned %d expired EPG entries', total_deleted)


# Shared by _prune_bogus_programs and _upsert_programs, which must agree on
# what counts as a "sane" program timestamp.
_BOGUS_TIMESTAMP_FUTURE_DAYS = 90  # nothing legitimate is scheduled >90d out
_BOGUS_TIMESTAMP_PAST_DAYS   = 7   # anything this old should already be gone


def _prune_bogus_programs(batch_size: int = 1000):
    """Delete EPG rows with impossible timestamps a scraper mis-parsed.

    _prune_old_programs only trims the past; a scraper timestamp bug (observed:
    stirr emitting end_time values around year 8390) can leave far-future junk
    that the normal prune never touches and that sits in the table forever.
    """
    now = datetime.now(timezone.utc)
    hi = now + timedelta(days=_BOGUS_TIMESTAMP_FUTURE_DAYS)
    lo = now - timedelta(days=_BOGUS_TIMESTAMP_PAST_DAYS)
    total_deleted = 0

    while True:
        # Two single-column queries (each a seekable indexed range scan) instead
        # of one OR spanning both end_time and start_time — SQLite's OR
        # optimization only produces indexed seeks when every term shares a
        # column; mixing columns forces a full index scan on every batch.
        ids = sorted({
            row[0] for row in (
                Program.query
                .filter(or_(Program.end_time > hi, Program.end_time < lo))
                .with_entities(Program.id)
                .limit(batch_size)
                .all()
            )
        } | {
            row[0] for row in (
                Program.query
                .filter(or_(Program.start_time > hi, Program.start_time < lo))
                .with_entities(Program.id)
                .limit(batch_size)
                .all()
            )
        })
        if not ids:
            break

        deleted = (
            Program.query
            .filter(Program.id.in_(ids))
            .delete(synchronize_session=False)
        ) or 0
        db.session.commit()
        total_deleted += deleted

    if total_deleted:
        logger.info('[worker] pruned %d bogus-timestamp EPG entries', total_deleted)


def _cleanup_orphans(batch_size: int = 2000):
    """Delete rows whose parent records no longer exist, in small batches.

    Each batch is committed immediately so the write lock is held only briefly.
    Avoids locking contention with gunicorn workers during startup cleanup.
    """
    import sqlalchemy as _sa

    deleted_programs = 0
    while True:
        ids = [
            row[0] for row in db.session.execute(text(
                "SELECT p.id FROM programs p "
                "LEFT JOIN channels c ON p.channel_id = c.id "
                "WHERE c.id IS NULL LIMIT :n"
            ), {"n": batch_size}).fetchall()
        ]
        if not ids:
            break
        db.session.execute(
            _sa.delete(Program).where(Program.id.in_(ids))
        )
        deleted_programs += len(ids)
        db.session.commit()
        if len(ids) < batch_size:
            break

    deleted_channels = 0
    while True:
        ids = [
            row[0] for row in db.session.execute(text(
                "SELECT c.id FROM channels c "
                "LEFT JOIN sources s ON c.source_id = s.id "
                "WHERE s.id IS NULL LIMIT :n"
            ), {"n": batch_size}).fetchall()
        ]
        if not ids:
            break
        db.session.execute(
            _sa.delete(Channel).where(Channel.id.in_(ids))
        )
        deleted_channels += len(ids)
        db.session.commit()
        if len(ids) < batch_size:
            break

    if deleted_programs or deleted_channels:
        logger.info(
            '[worker] cleaned %d orphan programs and %d orphan channels',
            deleted_programs,
            deleted_channels,
        )


def _upsert_programs(source, program_data_list, progress_cb=None):
    if not program_data_list:
        return
    channels = {ch.source_channel_id: ch for ch in source.channels.all()}
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=2)

    # Reject programs with impossible timestamps before they influence anything
    # below — scraper-side date parsing bugs (unit-ambiguous epoch values,
    # corrupt upstream feed data) have produced multi-millennium start/end
    # times in the past (e.g. stirr ~year 8390, freelivesports ~year 1783). A
    # bogus far-future end_time left in would also blow out the delete-window
    # calculation just below, since win_end takes the max end_time seen.
    sanity_hi = now + timedelta(days=_BOGUS_TIMESTAMP_FUTURE_DAYS)
    sanity_lo = now - timedelta(days=_BOGUS_TIMESTAMP_PAST_DAYS)
    bogus_skipped = 0

    incoming_by_channel_id: dict[int, list] = {}
    for pd in program_data_list:
        ch = channels.get(pd.source_channel_id)
        if not ch:
            continue
        start = _utc_aware(pd.start_time)
        end = _utc_aware(pd.end_time)
        if start is None or end is None or not (sanity_lo <= start <= sanity_hi) or not (sanity_lo <= end <= sanity_hi):
            bogus_skipped += 1
            continue
        incoming_by_channel_id.setdefault(ch.id, []).append(pd)

    if bogus_skipped:
        logger.warning(
            '[%s] rejected %d program(s) with out-of-range start/end timestamps',
            source.name, bogus_skipped,
        )

    # Delete only the time window covered by the incoming batch, so programs
    # beyond that window (fetched in earlier runs) are preserved.  This lets
    # sources like Roku — which return a short lookahead per request — build
    # up a rolling horizon across repeated fetches.
    #
    # Group channels by their (win_start, win_end) so scrapers that return the
    # same range for every channel (e.g. TCL's 426 channels) collapse to a
    # single DELETE rather than one per channel.
    window_to_ids: dict[tuple, list[int]] = {}
    for channel_id, incoming_rows in incoming_by_channel_id.items():
        active_rows = [row for row in incoming_rows if _utc_aware(row.end_time) > cutoff]
        if not active_rows:
            continue
        # Delete window covers ALL incoming rows for this channel, not just active ones.
        # Using only active_rows for win_start would miss programs that aged past the
        # cutoff between the scrape and the upsert, leaving stale rows permanently.
        win_start = min(_utc_aware(row.start_time) for row in incoming_rows)
        win_end   = max(_utc_aware(row.end_time)   for row in incoming_rows)
        window_to_ids.setdefault((win_start, win_end), []).append(channel_id)

    _ID_BATCH = 900  # stay under SQLite's default variable limit
    for (win_start, win_end), ch_ids in window_to_ids.items():
        for i in range(0, len(ch_ids), _ID_BATCH):
            Program.query.filter(
                Program.channel_id.in_(ch_ids[i:i + _ID_BATCH]),
                Program.end_time   >  win_start,
                Program.start_time <  win_end,
            ).delete(synchronize_session=False)
    db.session.commit()

    rows = []
    for channel_id, pd_list in incoming_by_channel_id.items():
        for pd in pd_list:
            rows.append({
                'channel_id':    channel_id,
                'title':         pd.title,
                'description':   _sanitize_description(pd.description),
                'start_time':    pd.start_time,
                'end_time':      pd.end_time,
                'poster_url':    pd.poster_url,
                'category':      pd.category,
                'rating':        pd.rating,
                'episode_title': pd.episode_title,
                'season':        pd.season,
                'episode':       pd.episode,
                'original_air_date': pd.original_air_date,
                'is_live':           pd.is_live,
                'program_type':      getattr(pd, 'program_type', None),
                'series_id':         getattr(pd, 'series_id',    None),
                'episode_id':        getattr(pd, 'episode_id',   None),
            })
    # Commit in chunks so the write lock isn't held for the full batch.
    _CHUNK = 2000
    total_rows = len(rows)
    for i in range(0, total_rows, _CHUNK):
        db.session.execute(Program.__table__.insert(), rows[i:i + _CHUNK])
        db.session.commit()
        if progress_cb:
            progress_cb('db', min(i + _CHUNK, total_rows), total_rows)


# In-memory record of when each source was last enqueued, so we don't
# double-queue a source that's still running (last_scraped_at not yet updated).
_last_enqueued: dict[str, datetime] = {}

# In-memory record of when a source was first seen continuously "active" (a
# scrape job queued/started that the scheduler keeps skipping). A genuinely hung
# scrape holds its slot until the RQ job_timeout (up to an hour) with no signal,
# so we track how long it's been stuck and surface a warning once it crosses the
# threshold. Cleared as soon as the source is no longer active.
_active_since: dict[str, datetime] = {}
# Sources we've already warned about while currently stuck, so each hang logs
# (and stamps last_error) once rather than every scheduler tick. Cleared on clear.
_stuck_warned: set[str] = set()

# Prefix that tags the last_error WE set for a hung scrape, so _clear_source_stuck
# can recognise and retract its own message without touching a real scraper error.
_STUCK_ERROR_PREFIX = 'Scrape stuck/active'


def _note_source_stuck(source, now):
    """Track a source whose scrape slot is held by an active/hung job and warn
    (once) if it has been stuck well past its interval. Returns nothing."""
    started = _active_since.setdefault(source.name, now)
    stuck_min = (now - started).total_seconds() / 60
    # A normal scrape finishes inside one interval; flag at 2 intervals (floor
    # 30m) so slow-but-progressing runs don't trip it, but a true hang does.
    interval = source.scrape_interval or 0
    threshold = max(2 * interval, 30) if interval else 30
    if stuck_min > threshold and source.name not in _stuck_warned:
        _stuck_warned.add(source.name)
        logger.warning(
            '[scheduler] %s scrape has held its slot for %.0fm (interval=%dm) — '
            'job likely hung; not re-enqueuing until it clears.',
            source.name, stuck_min, interval,
        )
        # Don't clobber a genuine scraper error with the generic stuck message —
        # only surface "stuck" when nothing more specific is already recorded.
        if not (source.last_error or '').strip():
            try:
                source.last_error = (
                    f'{_STUCK_ERROR_PREFIX} for {stuck_min:.0f}m (interval={interval}m) — '
                    f'job likely hung; will not re-enqueue until it clears.'
                )
                db.session.commit()
            except Exception:
                db.session.rollback()


def _clear_source_stuck(source):
    """Forget stuck-state tracking for a source that is no longer active, and
    retract a stale stuck-message we set. A successful scrape resets last_error to
    None on its own, but a hang killed by RQ's job_timeout never runs that path, so
    the message would otherwise leave the source pinned red in the dashboard."""
    name = getattr(source, 'name', source)
    was_tracked = name in _active_since or name in _stuck_warned
    _active_since.pop(name, None)
    _stuck_warned.discard(name)
    if was_tracked and hasattr(source, 'last_error') \
            and (source.last_error or '').startswith(_STUCK_ERROR_PREFIX):
        try:
            source.last_error = None
            db.session.commit()
        except Exception:
            db.session.rollback()


def _scrape_due_calc(source, now, last):
    """Shared cron/interval due-check math. Returns (is_due, next_run_estimate).
    next_run_estimate is None when scrape_interval=0 (never) or the cron
    expression is invalid; used both to decide whether to enqueue a scrape
    now (_is_source_due) and to display an estimated next-scrape time
    (app/routes/admin.py's _next_scrape_estimate)."""
    if source.scrape_cron:
        try:
            prev = _croniter(source.scrape_cron, now).get_prev(datetime)
            is_due = last is None or prev >= last
            next_run = now if is_due else _croniter(source.scrape_cron, now).get_next(datetime)
            return is_due, next_run
        except Exception:
            logger.warning('[scheduler] Invalid cron expression for %s: %r', source.name, source.scrape_cron)
            return False, None
    if not source.scrape_interval:
        return False, None  # scrape_interval=0 means never auto-scrape
    if last is None:
        return True, now
    is_due = (now - last).total_seconds() >= source.scrape_interval * 60
    next_run = now if is_due else last + timedelta(minutes=source.scrape_interval)
    return is_due, next_run


def _is_source_due(source, now, last):
    """Return True if this source should be enqueued for a scrape right now."""
    return _scrape_due_calc(source, now, last)[0]


def _schedule_due_scrapes():
    """Enqueue scrapes for enabled sources whose interval has elapsed."""
    now = datetime.now(timezone.utc)
    with flask_app.app_context():
        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            q = Queue('scraper', connection=r)
        except Exception as e:
            logger.error('[scheduler] Redis unavailable: %s', e)
            return

        # Stamp the liveness heartbeat on every healthy tick. The dashboard reads
        # this to alarm if the scheduler stops ticking (crash loop / hang / bad TZ).
        from app.scheduler_health import write_heartbeat
        write_heartbeat(flask_app.config['REDIS_URL'])

        sources = Source.query.filter_by(is_enabled=True).all()
        for source in sources:
            if _scrape_job_already_active(q, source.name):
                _last_enqueued[source.name] = now
                _note_source_stuck(source, now)
                continue
            _clear_source_stuck(source)
            last_scraped = _utc_aware(source.last_scraped_at)
            last_queued = _utc_aware(_last_enqueued.get(source.name))
            candidates = [t for t in (last_scraped, last_queued) if t is not None]
            last = max(candidates) if candidates else None

            if _is_source_due(source, now, last):
                try:
                    q.enqueue('app.worker.run_scraper', source.name, job_timeout=3600, job_id=f'scrape-{source.name}')
                    _last_enqueued[source.name] = now
                    if source.scrape_cron:
                        logger.info('[scheduler] Enqueued %s (cron=%s)', source.name, source.scrape_cron)
                    else:
                        logger.info('[scheduler] Enqueued %s (interval=%dm, age=%s)',
                                    source.name, source.scrape_interval,
                                    f'{(now - last).total_seconds() / 60:.0f}m' if last else 'never')
                except Exception as e:
                    logger.error('[scheduler] Failed to enqueue %s: %s', source.name, e)


def seed_sources():
    with flask_app.app_context():
        scrapers = registry.get_all()
        default_disabled_sources = {'amazon_prime_free', 'sling', 'localnow', 'pluto', 'frndlytv', 'fubo', 'hdhomerun', 'freecast', 'vidaa'}
        # Custom Channels source: always seeded, always enabled, never auto-scraped
        if not Source.query.filter_by(name='custom').first():
            db.session.add(Source(
                name='custom',
                display_name='Custom Channels',
                scrape_interval=0,
                config={},
                epg_only=False,
                is_enabled=True,
            ))
            db.session.flush()
        seeded_names = set()
        for name, cls in scrapers.items():
            canonical_name = getattr(cls, 'source_name', None) or name
            if name != canonical_name or canonical_name in seeded_names:
                continue
            seeded_names.add(canonical_name)
            if not Source.query.filter_by(name=canonical_name).first():
                db.session.add(Source(
                    name            = canonical_name,
                    display_name    = cls.display_name or canonical_name.title(),
                    scrape_interval = cls.scrape_interval,
                    config          = {},
                    epg_only        = False,
                    is_enabled      = canonical_name not in default_disabled_sources,
                ))
        # Reset legacy flags so upgrading users do not get stuck with sources
        # silently excluded from M3U output after the UI toggle is removed.
        Source.query.filter_by(epg_only=True).update({'epg_only': False}, synchronize_session=False)
        db.session.commit()
        logger.info(f'Seeded {len(seeded_names)} sources')


def purge_orphaned_sources():
    """Force-purge a source (and its channels/programs) once its scraper class has
    been absent from the registry for _SCRAPER_MISSING_GRACE_DAYS straight boots.

    registry._discover() swallows import errors for a broken scraper module the
    same way it does for a deleted one, so a single missing-from-registry check
    can't tell "file removed" from "file present but failing to import" (bad
    deploy, missing dependency). The grace period buys time for a transient
    import failure to get fixed before real data is deleted; scraper_missing_since
    clears itself the moment the class reappears in the registry.
    """
    with flask_app.app_context():
        known = set(registry.get_all().keys())
        now = datetime.now(timezone.utc)
        for source in Source.query.all():
            if source.name in known:
                if source.scraper_missing_since is not None:
                    source.scraper_missing_since = None
                    db.session.commit()
                    logger.info('[source-orphan-check] %s scraper reappeared in registry; cleared missing marker', source.name)
                continue

            if source.scraper_missing_since is None:
                source.scraper_missing_since = now
                db.session.commit()
                logger.warning(
                    '[source-orphan-check] %s has no registered scraper class; starting '
                    '%dd grace period before its channels/programs are purged',
                    source.name, _SCRAPER_MISSING_GRACE_DAYS,
                )
                continue

            missing_since = source.scraper_missing_since
            if missing_since.tzinfo is None:
                missing_since = missing_since.replace(tzinfo=timezone.utc)
            age_days = (now - missing_since).total_seconds() / 86400
            if age_days < _SCRAPER_MISSING_GRACE_DAYS:
                logger.warning(
                    '[source-orphan-check] %s still has no registered scraper class '
                    '(%.1fd of %dd grace elapsed)',
                    source.name, age_days, _SCRAPER_MISSING_GRACE_DAYS,
                )
                continue

            source_id, source_name = source.id, source.name
            deleted_channels, deleted_programs = _purge_source_channels_and_programs(source)
            SourceCache.query.filter_by(source_id=source_id).delete(synchronize_session=False)
            db.session.delete(source)
            db.session.commit()
            _invalidate_and_refresh_xml()
            logger.warning(
                '[source-orphan-check] purged orphaned source=%s (id=%s): scraper class '
                'missing for %.1fd, deleted %d channels and %d programs',
                source_name, source_id, age_days, deleted_channels, deleted_programs,
            )


def _rq_prune():
    """RQ job target: prune expired EPG entries. Runs inside the RQ worker process."""
    with flask_app.app_context():
        _prune_old_programs()


def _warn_stale_channel_fetches():
    """Log a WARNING for any enabled source whose channel list hasn't been
    refreshed in well over its channel_refresh_hours window.

    This is the canary for the class of bug where a source keeps reporting
    successful scrapes (last_scraped_at advancing via EPG-only runs) while
    fetch_channels() silently never runs and the channel list rots — invisible
    because resolve() keeps existing streams playing. A divergence between
    "scraped recently" and "channels fetched recently" surfaces it within a day.
    """
    now = datetime.now(timezone.utc)
    for source in Source.query.filter_by(is_enabled=True).all():
        scraper_cls = registry.get(source.name)
        refresh_hours = getattr(scraper_cls, 'channel_refresh_hours', 0) if scraper_cls else 0
        if not refresh_hours or not source.scrape_interval:
            continue
        last_fetch = _utc_aware(source.last_channel_fetch_at)
        if last_fetch is None:
            continue  # never fetched under the new clock — next scrape will do a full fetch
        age_hours = (now - last_fetch).total_seconds() / 3600
        if age_hours > 2 * refresh_hours:
            logger.warning(
                '[integrity] %s channel list is stale: last full fetch %.1fh ago '
                '(channel_refresh_hours=%d). EPG may be advancing without channel refresh.',
                source.name, age_hours, refresh_hours,
            )


def _warn_stale_epg_refreshes():
    """Log a WARNING for any enabled source whose EPG hasn't refreshed in well
    over its scrape interval, even though it still reports recent scrapes.

    The mirror image of _warn_stale_channel_fetches. last_scraped_at is stamped
    right after the channel commit, *before* the EPG phase runs — so a run that
    fetches channels fine but whose EPG phase then hangs or fails looks like a
    full success. last_epg_success_at is stamped only when programs commit, so a
    divergence between "scraped recently" and "EPG refreshed recently" surfaces a
    silently-failing EPG (guide data rotting while channels stay healthy) within
    a couple of intervals.
    """
    now = datetime.now(timezone.utc)
    for source in Source.query.filter_by(is_enabled=True).all():
        if not source.scrape_interval:
            continue  # 0 = never auto-scraped; cron-only sources handled elsewhere
        if source.scrape_cron:
            continue  # interval isn't the governing clock for cron sources
        last_epg = _utc_aware(source.last_epg_success_at)
        if last_epg is None:
            continue  # never succeeded under the new clock — next good run stamps it
        age_min = (now - last_epg).total_seconds() / 60
        # Two full intervals of grace: a single failed/hung run won't trip it.
        if age_min > 2 * source.scrape_interval:
            logger.warning(
                '[integrity] %s EPG is stale: last successful EPG refresh %.0fm ago '
                '(scrape_interval=%dm). Channels may be refreshing while EPG silently '
                'fails or hangs — guide data is going stale.',
                source.name, age_min, source.scrape_interval,
            )


def _rq_integrity_cleanup():
    """RQ job target: delete orphan channels/programs. Runs inside the RQ worker process."""
    with flask_app.app_context():
        _cleanup_orphans()
        try:
            _warn_stale_channel_fetches()
        except Exception as exc:
            logger.warning('[integrity] stale-channel-fetch check failed: %s', exc)
        try:
            _warn_stale_epg_refreshes()
        except Exception as exc:
            logger.warning('[integrity] stale-epg-refresh check failed: %s', exc)
    # Runs on this job's own daily cadence rather than only at container boot --
    # a long-lived container that never restarts would otherwise never advance
    # (or ever clear) an orphaned source's grace period.
    try:
        purge_orphaned_sources()
    except Exception as exc:
        logger.warning('[integrity] orphaned-source purge check failed: %s', exc)


# Number of nightly DB backups to retain in /data/backups.
_DB_BACKUP_KEEP = 3
_DB_BACKUP_DIR  = '/data/backups'


def _rq_db_backup():
    """RQ job target: write a gzip-compressed online backup of the live SQLite DB
    into /data/backups and prune to the newest _DB_BACKUP_KEEP files.

    Uses sqlite3's online backup API so it is safe to run against the live DB.
    /data is volume-mounted, so these survive container rebuilds.
    """
    import os, shutil, gzip as _gzip, sqlite3 as _sqlite3, tempfile as _tempfile, glob as _glob

    db_path = '/data/fastchannels.db'
    if not os.path.exists(db_path):
        logger.warning('[db-backup] database file not found at %s; skipping', db_path)
        return

    os.makedirs(_DB_BACKUP_DIR, exist_ok=True)
    ts       = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    dest     = os.path.join(_DB_BACKUP_DIR, f'fastchannels_backup_{ts}.db.gz')
    # Stage the uncompressed snapshot on the /data volume, not the default
    # tempdir — /tmp is a small tmpfs in many container setups and a large DB
    # would blow it out with ENOSPC.
    tmp_db   = _tempfile.NamedTemporaryFile(suffix='.db', dir=_DB_BACKUP_DIR, delete=False)
    tmp_db.close()
    try:
        # SQLite online backup — consistent snapshot while the DB is live.
        src = _sqlite3.connect(db_path)
        dst = _sqlite3.connect(tmp_db.name)
        src.backup(dst)
        src.close(); dst.close()
        # Write compressed to a temp path, then atomically move into place so a
        # crash mid-write never leaves a truncated .db.gz that looks valid.
        dest_tmp = dest + '.part'
        with open(tmp_db.name, 'rb') as f_in, _gzip.open(dest_tmp, 'wb', compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.replace(dest_tmp, dest)
        size_mb = os.path.getsize(dest) / 1024 / 1024
        logger.info('[db-backup] wrote %s (%.1f MB)', dest, size_mb)
    except Exception as exc:
        logger.error('[db-backup] backup failed: %s', exc)
        try:
            os.unlink(dest + '.part')
        except OSError:
            pass
        return
    finally:
        try:
            os.unlink(tmp_db.name)
        except OSError:
            pass

    # Prune to the newest _DB_BACKUP_KEEP backups.
    try:
        backups = sorted(_glob.glob(os.path.join(_DB_BACKUP_DIR, 'fastchannels_backup_*.db.gz')))
        for old in backups[:-_DB_BACKUP_KEEP]:
            try:
                os.unlink(old)
                logger.info('[db-backup] pruned old backup %s', os.path.basename(old))
            except OSError as exc:
                logger.warning('[db-backup] could not remove %s: %s', old, exc)
    except Exception as exc:
        logger.warning('[db-backup] prune step failed: %s', exc)


# Only VACUUM when the free-page fraction exceeds this — avoids paying the
# full-file rewrite cost every week for a DB that isn't actually fragmented.
_VACUUM_FREE_PAGE_THRESHOLD = 0.25


def _rq_weekly_maintenance():
    """RQ job target: sanity-prune bogus EPG rows, then VACUUM if the live DB
    file is heavily fragmented. Runs on the scraper queue so its exclusive
    VACUUM lock is serialized against scrapes, same as _rq_prune.
    """
    with flask_app.app_context():
        _prune_bogus_programs()

        free = db.session.execute(text('PRAGMA freelist_count')).scalar() or 0
        total = db.session.execute(text('PRAGMA page_count')).scalar() or 1
        frac = free / total
        if frac > _VACUUM_FREE_PAGE_THRESHOLD:
            logger.info('[maint] VACUUM: %d/%d pages free (%.0f%%)', free, total, 100 * frac)
            # Engine is isolation_level=None (autocommit), so VACUUM runs outside
            # any open transaction as SQLite requires.
            db.session.execute(text('VACUUM'))
            logger.info('[maint] VACUUM complete')
        else:
            logger.info('[maint] VACUUM skipped: only %.0f%% free', 100 * frac)


if __name__ == '__main__':
    import os

    role = (os.environ.get('FC_WORKER_ROLE') or 'all').strip().lower()

    def _scheduled_prune():
        try:
            _r = redis.from_url(flask_app.config['REDIS_URL'])
            # Run on the scraper queue (not maintenance) so the prune's writes
            # never overlap a scrape's writes — the single scraper worker
            # serializes them, eliminating prune-vs-scrape SQLite write
            # contention. Stable job_id keeps it identifiable; the hourly
            # max_instances=1 schedule already prevents pile-ups.
            _q = Queue('scraper', connection=_r)
            _q.enqueue('app.worker._rq_prune', job_timeout=900, job_id='prune-epg')
            logger.info('[scheduler] enqueued _rq_prune job')
        except Exception as e:
            logger.error('[scheduler] could not enqueue prune job: %s', e)

    def _scheduled_integrity_cleanup():
        try:
            _r = redis.from_url(flask_app.config['REDIS_URL'])
            _q = Queue('maintenance', connection=_r)
            _q.enqueue('app.worker._rq_integrity_cleanup', job_timeout=300)
            logger.info('[scheduler] enqueued _rq_integrity_cleanup job')
        except Exception as e:
            logger.error('[scheduler] could not enqueue integrity_cleanup job: %s', e)

    def _scheduled_db_backup():
        try:
            _r = redis.from_url(flask_app.config['REDIS_URL'])
            _q = Queue('maintenance', connection=_r)
            _q.enqueue('app.worker._rq_db_backup', job_timeout=600)
            logger.info('[scheduler] enqueued _rq_db_backup job')
        except Exception as e:
            logger.error('[scheduler] could not enqueue db_backup job: %s', e)

    def _scheduled_weekly_maintenance():
        try:
            _r = redis.from_url(flask_app.config['REDIS_URL'])
            # Scraper queue, not maintenance — same reasoning as _scheduled_prune:
            # the single scraper worker serializes VACUUM's exclusive lock away
            # from any concurrent scrape write.
            _q = Queue('scraper', connection=_r)
            _q.enqueue('app.worker._rq_weekly_maintenance', job_timeout=1800, job_id='weekly-maint')
            logger.info('[scheduler] enqueued _rq_weekly_maintenance job')
        except Exception as e:
            logger.error('[scheduler] could not enqueue weekly_maintenance job: %s', e)

    def _scheduled_logo_cache_cleanup():
        import os as _os
        from app.routes.images import (
            sweep_orphaned_logos, cleanup_poster_cache,
            _LOGO_DIR, _POSTER_DIR,
        )

        with flask_app.app_context():
            active_urls = [
                row[0] for row in
                db.session.query(Channel.logo_url)
                .join(Source, Channel.source_id == Source.id)
                .filter(Channel.logo_url.isnot(None), Source.is_enabled == True)
                .distinct()
                .all()
            ]
        removed = sweep_orphaned_logos(active_urls)
        if removed:
            logger.info('[logo_cache] removed %d orphaned logo files', removed)

        # Delete cached posters for programs that ended more than 2 hours ago
        with flask_app.app_context():
            cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
            expired_urls = [
                row[0] for row in
                db.session.query(Program.poster_url)
                .filter(Program.end_time < cutoff, Program.poster_url.isnot(None))
                .distinct()
                .all()
            ]
        poster_removed = cleanup_poster_cache(expired_urls)
        if poster_removed:
            logger.info('[logo_cache] removed %d expired poster files', poster_removed)

        # Cache stats
        def _dir_stats(path):
            files = size = 0
            try:
                for f in _os.scandir(path):
                    if f.is_file() and not f.name.endswith('.ct'):
                        files += 1
                        size  += f.stat().st_size
            except FileNotFoundError:
                pass
            return files, size

        logo_n,   logo_b   = _dir_stats(_LOGO_DIR)
        poster_n, poster_b = _dir_stats(_POSTER_DIR)
        logger.info(
            '[logo_cache] stats — logos: %d files / %.1fMB  |  posters: %d files / %.1fMB  |  total: %.1fMB',
            logo_n,   logo_b   / 1024 / 1024,
            poster_n, poster_b / 1024 / 1024,
            (logo_b + poster_b) / 1024 / 1024,
        )

    def _run_scheduler():
        # Resolve the scheduler's default timezone through our guarded path BEFORE
        # constructing it. APScheduler defaults to tzlocal.get_localzone(), which
        # HARD-RAISES on a legacy/invalid container TZ (e.g. TZ=US/Eastern) — that
        # would crash this whole worker on startup and silently stop all scrapes.
        # current_zoneinfo() never raises (falls back to system tz, then UTC), and
        # the same value drives the cron jobs below so the UI and scheduler agree.
        with flask_app.app_context():
            from app.models import AppSettings as _AS
            from app.timezone_utils import current_zoneinfo
            _tz = current_zoneinfo(_AS.get().timezone_name)
        scheduler = BackgroundScheduler(daemon=True, timezone=_tz)
        scheduler.add_job(_schedule_due_scrapes, 'interval', minutes=1, id='auto_scrape',
                          max_instances=1, coalesce=True, misfire_grace_time=60)
        scheduler.add_job(_scheduled_prune, 'interval', hours=1, id='epg_prune',
                          max_instances=1, coalesce=True, misfire_grace_time=3600)
        scheduler.add_job(_scheduled_integrity_cleanup, 'interval', days=1, id='integrity_cleanup',
                          max_instances=1, coalesce=True, misfire_grace_time=3600)
        scheduler.add_job(_scheduled_logo_cache_cleanup, 'interval', hours=6, id='logo_cache_cleanup',
                          max_instances=1, coalesce=True, misfire_grace_time=3600)

        def _scheduled_remote_gracenote_refresh():
            from app.gracenote_map import fetch_remote_gracenote_map
            with flask_app.app_context():
                from app.models import AppSettings
                url = AppSettings.get().effective_gracenote_map_url()
            ok, msg = fetch_remote_gracenote_map(url)
            if not ok:
                logger.warning('[gracenote-map] scheduled remote refresh failed: %s', msg)

        scheduler.add_job(_scheduled_remote_gracenote_refresh, 'interval', hours=24,
                          id='gracenote_remote_refresh', max_instances=1, coalesce=True)

        def _scheduled_tvtv_cache_refresh() -> str:
            try:
                r = redis.from_url(flask_app.config['REDIS_URL'])
                q = Queue('maintenance', connection=r)
                job_id = 'tvtv-cache-refresh'
                if _any_scrapes_active():
                    logger.info('[tvtv-cache] scraper work active; deferring refresh')
                    return 'deferred'
                active_ids = set(q.get_job_ids()) | set(StartedJobRegistry(q.name, connection=q.connection).get_job_ids())
                if job_id in active_ids:
                    logger.info('[tvtv-cache] refresh already queued/running, skipping')
                    return 'active'
                q.enqueue('app.worker.run_tvtv_cache_refresh', job_timeout=1800, job_id=job_id)
                logger.info('[tvtv-cache] enqueued refresh job')
                return 'queued'
            except Exception as exc:
                logger.warning('[tvtv-cache] could not enqueue via RQ: %s', exc)
                return 'error'

        # 03:00 user local time — _tz (resolved at construction above) follows the
        # timezone configured in admin/settings, via the same guarded zoneinfo path
        # the rest of the app uses, so the UI and scheduler can't silently diverge.
        scheduler.add_job(_scheduled_tvtv_cache_refresh, 'cron',
                          hour=3, minute=0, timezone=_tz,
                          id='tvtv_cache_refresh_night', max_instances=1, coalesce=True,
                          misfire_grace_time=3600)

        # Nightly DB backup at 03:30 user-local — staggered 30 min after the tvtv
        # refresh so they don't both hit the DB at once. Keeps the newest 3 in
        # /data/backups (volume-mounted, survives container rebuilds).
        scheduler.add_job(_scheduled_db_backup, 'cron',
                          hour=3, minute=30, timezone=_tz,
                          id='db_backup_night', max_instances=1, coalesce=True,
                          misfire_grace_time=3600)

        # Weekly, Sunday 04:30 user-local — after the nightly backup so a fresh
        # backup exists before VACUUM rewrites the live file.
        scheduler.add_job(_scheduled_weekly_maintenance, 'cron',
                          day_of_week='sun', hour=4, minute=30, timezone=_tz,
                          id='weekly_maint', max_instances=1, coalesce=True,
                          misfire_grace_time=3600)

        def _scheduled_dvr_epg_refresh():
            import re as _re
            import requests as _requests
            with flask_app.app_context():
                from app.models import AppSettings as _AS, Feed as _Feed
                _settings = _AS.get()
                if not (_settings.dvr_epg_auto_refresh if _settings.dvr_epg_auto_refresh is not None else True):
                    return
                dvr_url = (_settings.effective_channels_dvr_url() or '').strip().rstrip('/')
                if not dvr_url:
                    return
                feed_names = [
                    f'FastChannels {f.name}'
                    for f in _Feed.query.filter_by(is_enabled=True).all()
                ]
            import time as _time
            refreshed, errors = [], []
            for name in feed_names:
                safe      = _re.sub(r'[^a-zA-Z0-9]', '', name)
                lineup_id = f'XMLTV-{safe}'
                try:
                    r = _requests.put(f'{dvr_url}/dvr/lineups/{lineup_id}', timeout=15, verify=False)
                    if r.ok:
                        refreshed.append(lineup_id)
                    else:
                        errors.append(f'{lineup_id}={r.status_code}')
                except Exception as exc:
                    errors.append(f'{lineup_id}={exc}')
                _time.sleep(2)
            if refreshed:
                logger.info('[dvr-epg] pushed guide refresh for %d lineup(s): %s',
                            len(refreshed), ', '.join(refreshed))
            if errors:
                logger.warning('[dvr-epg] guide refresh errors: %s', ', '.join(errors))

        scheduler.add_job(_scheduled_dvr_epg_refresh, 'interval', hours=1,
                          id='dvr_epg_refresh', max_instances=1, coalesce=True,
                          misfire_grace_time=3600)

        # Amazon DRM no longer uses a global playbackEnvelope: each channel's PE is harvested
        # from the livetv carousel during the normal scrape (and re-minted via enrichItemMetadata
        # on demand), so there is no separate PE auto-refresh job.

        scheduler.start()
        # Mark the scheduler alive the instant it starts, so the dashboard's
        # liveness check has a fresh heartbeat before the first 60s tick — a
        # crash loop never reaches this line, so it can't fake liveness.
        from app.scheduler_health import write_heartbeat
        write_heartbeat(flask_app.config['REDIS_URL'])
        logger.info('Scheduler started — checking sources every 60s')
        with flask_app.app_context():
            try:
                _r = redis.from_url(flask_app.config['REDIS_URL'])
                _q = Queue('maintenance', connection=_r)
                _q.enqueue('app.worker._rq_integrity_cleanup', job_timeout=300)
            except Exception as _e:
                logger.warning('[scheduler] could not enqueue startup integrity cleanup: %s', _e)
            enabled_sources = Source.query.filter_by(is_enabled=True).count()
            total_sources = Source.query.count()
            from app.models import Feed
            enabled_feeds = Feed.query.filter_by(is_enabled=True).count()
            logger.info(
                'Startup summary — enabled_sources=%d total_sources=%d enabled_feeds=%d',
                enabled_sources,
                total_sources,
                enabled_feeds,
            )
            try:
                _enqueue_xml_refresh_job()
            except Exception:
                logger.exception('[xml-cache] startup refresh failed')

            # Trigger tvtv cache refresh at startup if the cache is empty or stale.
            try:
                from app.models import TvtvProgramCache
                from sqlalchemy import func as sa_func
                from datetime import timezone as _tz
                newest = db.session.query(sa_func.max(TvtvProgramCache.fetched_at)).scalar()
                if newest is None:
                    stale = True
                else:
                    if newest.tzinfo is None:
                        newest = newest.replace(tzinfo=_tz.utc)
                    age_hours = (datetime.now(_tz.utc) - newest).total_seconds() / 3600
                    stale = age_hours > 25
                if stale:
                    startup_status = _scheduled_tvtv_cache_refresh()
                    logger.info('[tvtv-cache] stale/empty at startup (newest=%s) — status=%s', newest, startup_status)
                    if newest is None and startup_status == 'deferred':
                        retry_at = datetime.now(_tz.utc) + timedelta(minutes=30)
                        scheduler.add_job(
                            _scheduled_tvtv_cache_refresh,
                            'date',
                            run_date=retry_at,
                            id='tvtv_cache_empty_retry',
                            replace_existing=True,
                            misfire_grace_time=3600,
                        )
                        logger.info('[tvtv-cache] empty cache startup retry scheduled for %s', retry_at.isoformat())
            except Exception:
                logger.exception('[tvtv-cache] startup staleness check failed')

            try:
                from app.gracenote_map import fetch_remote_gracenote_map
                url = AppSettings.get().effective_gracenote_map_url()
                ok, msg = fetch_remote_gracenote_map(url)
                if ok:
                    logger.info('[gracenote-map] startup remote fetch: %s', msg)
                else:
                    logger.warning('[gracenote-map] startup remote fetch failed: %s', msg)
            except Exception:
                logger.exception('[gracenote-map] startup remote fetch error')

        while True:
            time.sleep(3600)

    class _NoopDeathPenalty(_BaseDeathPenalty):
        """Job timeout enforcer that does nothing — safe for non-main threads.

        UnixSignalDeathPenalty (the RQ default) uses SIGALRM which is only
        available in the main thread.  Fast-queue jobs are short-lived so we
        simply let them run to completion without a signal-based timeout.
        """
        def setup_death_penalty(self):
            pass

        def cancel_death_penalty(self):
            pass

    class _FastWorker(_SimpleWorker):
        """SimpleWorker variant safe for a non-main thread.

        SimpleWorker runs jobs in-process (no forking), but its base class
        work() still installs SIGINT/SIGTERM/SIGALRM handlers via signal.signal(),
        which Python only permits in the main thread.  We skip both — the daemon
        thread dies automatically when the main process exits.
        """
        death_penalty_class = _NoopDeathPenalty

        def _install_signal_handlers(self):
            pass

        def perform_job(self, job, queue, **kwargs):
            result = super().perform_job(job, queue, **kwargs)
            gc.collect()
            try:
                _ctypes.CDLL('libc.so.6').malloc_trim(0)
            except Exception:
                pass
            return result

    def _run_fast_worker():
        r_fast = redis.from_url(flask_app.config['REDIS_URL'])
        w = _FastWorker(queues=[Queue('fast', connection=r_fast)], connection=r_fast)
        logger.info('Fast worker listening on queue: fast')
        w.work(logging_level=logging.WARNING)

    def _run_maintenance_worker():
        r_maintenance = redis.from_url(flask_app.config['REDIS_URL'])
        with Connection(r_maintenance):
            worker = Worker(queues=[Queue('maintenance', connection=r_maintenance)])
            logger.info('Maintenance worker listening on queue: maintenance')
            worker.work(logging_level=logging.WARNING)

    def _run_scraper_worker():
        r = redis.from_url(flask_app.config['REDIS_URL'])
        with Connection(r):
            worker = Worker(queues=[Queue('scraper', connection=r)])
            logger.info('Scraper worker listening on queue: scraper')
            worker.work(logging_level=logging.WARNING)

    if role == 'scheduler':
        try:
            _run_scheduler()
        except Exception:
            # The scheduler is the only thing that enqueues scrapes/EPG refreshes.
            # If it dies at startup (bad TZ, import error, DB hiccup) the supervisor
            # in entrypoint.sh restarts it every 5s — a crash loop that scrolls past
            # as a benign-looking warning while NOTHING scrapes. Make it unmistakable
            # in the logs, and back off so the loop doesn't flood them. The stale
            # heartbeat will also light up the dashboard banner.
            logger.critical(
                'Scheduler worker crashed during startup — NO scrapes or EPG '
                'refreshes will run until this is fixed. Backing off 60s before the '
                'supervisor restarts it.', exc_info=True)
            time.sleep(60)
            raise
    elif role == 'fast':
        _run_fast_worker()
    elif role == 'maintenance':
        _run_maintenance_worker()
    elif role == 'scraper':
        _run_scraper_worker()
    else:
        logger.error('Unknown FC_WORKER_ROLE=%r', role)
        sys.exit(2)

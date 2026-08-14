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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Callable

import redis
import requests as _req
from rq.job import Job
from rq import Worker, Queue, Connection, get_current_job
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
from app.models import Source, Channel, Program, Feed, AppSettings, SourceCache, TVEAccount
import time as _time
from urllib.parse import urljoin as _urljoin
from urllib.parse import urlsplit as _urlsplit
from app.scrapers import registry
from app.scrapers.base import (
    StreamDeadError,
    ScrapeSkipError,
    is_ssl_handshake_failure,
    is_transient_network_error,
)
from app.scrapers.category_utils import category_for_channel
from app.tve.adobe_pass import AdobePassCoxClient, TVEAuthError, TVENotAuthorizedError, TVEPendingAuthError, save_xfinity_cookie_jar
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
_TVTV_CACHE_STARTUP_RETRY_COOLDOWN_HOURS = 12

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


def _scrape_job_ids(source_name: str) -> tuple[str, str]:
    base = f'scrape-{source_name}'
    return base, f'{base}-force-full'


def _scrape_job_already_active(q: Queue, source_name: str) -> bool:
    job_ids = _scrape_job_ids(source_name)
    for job_id in job_ids:
        _cleanup_stale_started_job(q, job_id)
    active_ids = set(q.get_job_ids()) | set(StartedJobRegistry(q.name, connection=q.connection).get_job_ids())
    if any(job_id in active_ids for job_id in job_ids):
        return True
    for job_id in job_ids:
        try:
            job = Job.fetch(job_id, connection=q.connection)
            if job.get_status(refresh=False) in {'queued', 'started', 'deferred', 'scheduled'}:
                return True
        except Exception:
            pass
    return False


def _any_scrapes_active() -> bool:
    """Return True if any scraper jobs are queued or running.

    tvtv-cache-refresh (both the nightly cron and manual triggers) defers
    while this is True, so a started-job marker left behind by a scrape
    worker that died mid-job must not count as "active" forever — reuse the
    same heartbeat-based staleness check _scrape_job_already_active() uses,
    or a single stuck marker would starve tvtv-cache-refresh indefinitely.
    """
    try:
        r = redis.from_url(flask_app.config['REDIS_URL'])
        q = Queue('scraper', connection=r)
        queued = [jid for jid in q.get_job_ids() if jid.startswith('scrape-')]
        registry = StartedJobRegistry(q.name, connection=r)
        running = []
        for jid in registry.get_job_ids():
            if not jid.startswith('scrape-'):
                continue
            if _cleanup_stale_started_job(q, jid):
                continue
            running.append(jid)
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
        current_job_ids = set(_scrape_job_ids(current_source_name))
        other_running = [
            jid for jid in StartedJobRegistry(q.name, connection=r).get_job_ids()
            if jid not in current_job_ids
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
                                allow_suspicious_collapse=getattr(scraper, 'allow_suspicious_channel_collapse', False),
                                pinned_channel_ids=getattr(scraper, 'pinned_channel_ids', frozenset()),
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
        except TVENotAuthorizedError as e:
            # A known, understood condition (e.g. the configured MVPD isn't a
            # participating provider for this network) — the stream audit
            # already logs the exact same exception at INFO with no
            # traceback (see _audit_progress's not_authorized handling); the
            # scrape path should be just as quiet instead of an ERROR-level
            # full traceback for something that isn't a surprise.
            elapsed = time.monotonic() - t0
            logger.info('[%s] Scrape skipped after %.1fs: not authorized — %s', source_name, elapsed, e)
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
            # Unconditional (not just on the success path) — a scrape pile-up's
            # deferred startup refresh must still fire once the queue drains
            # even if the LAST job to finish hit an exception (e.g. not
            # authorized, DB lock exhausted); otherwise M3U/EPG output stays
            # stale until that source's next scheduled cycle. Idempotent/
            # dedup-safe (see _enqueue_xml_refresh_job), so this is harmless
            # alongside any other trigger.
            try:
                if _no_scrapes_pending(source_name):
                    _enqueue_xml_refresh_job()
            except Exception:
                logger.warning('[%s] deferred xml-refresh check failed', source_name, exc_info=True)
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

        # Sources such as DirecTV are intrinsically bridge-only. Re-sync before
        # auditing so a prior generic audit or manual state drift cannot leave
        # channels out of the bridge feed.
        _sync_intrinsic_drm_bridge(source)

        channels = source.channels.filter(
            db.or_(
                Channel.is_active == True,
                Channel.disable_reason.in_(['Dead', 'VOD', 'NotAuthorized']),
                Channel.disable_reason.like('AuditError:%'),
            )
        ).all()
        total    = len(channels)
        checked  = 0
        flagged  = 0
        bridged  = 0   # DRM channels kept active and routed via the PrismCast bridge
        dead     = 0
        vod      = 0
        not_authorized = 0
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
        def _audit_progress(done, total_, flagged_=0, dead_=0, vod_=0, errors_=0, skipped_403_=0, phase='checking', not_authorized_=0):
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
                        'not_authorized': not_authorized_,
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
                _audit_progress(i - 1, total, flagged, dead, vod, errors, skipped_403, not_authorized_=not_authorized)
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
                except TVENotAuthorizedError as auth_exc:
                    ch.is_active = False
                    ch.is_enabled = False
                    ch.disable_reason = 'NotAuthorized'
                    not_authorized += 1
                    consecutive_errors = 0
                    consecutive_transient_errors = 0
                    report_channels.append({
                        'id': ch.id,
                        'name': ch.name,
                        'status': 'not-authorized',
                        'reason': _audit_reason_from_exception(auth_exc),
                    })
                    logger.info('[audit] not authorized: %s  (%s)', ch.name, auth_exc)
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
                                            skipped_403, phase='cooldown', not_authorized_=not_authorized)
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
                        'dead': dead, 'vod': vod, 'not_authorized': not_authorized, 'errors': errors, 'skipped_403': skipped_403,
                        'ts': datetime.now(timezone.utc).isoformat(),
                        'partial': True,
                    }})
                    _audit_progress(i, total, flagged, dead, vod, errors, skipped_403, not_authorized_=not_authorized)
                    logger.info('[audit] %s: %d/%d — checked=%d flagged=%d dead=%d vod=%d not_authorized=%d errors=%d skipped_403=%d',
                                source_name, i, total, checked, flagged, dead, vod, not_authorized, errors, skipped_403)

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
                'dead': dead, 'vod': vod, 'not_authorized': not_authorized, 'errors': errors, 'skipped_403': skipped_403,
                'ts': datetime.now(timezone.utc).isoformat(),
            },
            'last_audit_report': {
                'channels': report_channels,
                'ts': datetime.now(timezone.utc).isoformat(),
            },
        })
        _audit_progress(0, 0, phase='done')
        logger.info('[audit] %s: done — total=%d checked=%d flagged=%d bridged=%d dead=%d vod=%d not_authorized=%d errors=%d skipped_403=%d',
                    source_name, total, checked, flagged, bridged, dead, vod, not_authorized, errors, skipped_403)


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
                except TVENotAuthorizedError as auth_exc:
                    ch.is_active = False
                    ch.is_enabled = False
                    ch.disable_reason = 'NotAuthorized'
                    recheck_results[ch.id] = {
                        'status': 'not-authorized',
                        'reason': _audit_reason_from_exception(auth_exc),
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
        result_summary['not_authorized'] = sum(1 for c in existing.values() if c.get('status') == 'not-authorized')
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

        settings = AppSettings.get()
        settings.tvtv_cache_last_attempt_at = datetime.now(timezone.utc)
        db.session.commit()

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

        # Fetch every applied/community ID, not just ones in the bundled
        # station_index.json — tvtv's fragment endpoint can serve guide data
        # for stations outside the index (see _UNINDEXED_LINEUP in tvtv_cache).
        station_ids = sorted(applied | community)
        unindexed = sum(1 for sid in station_ids if not get_station_entry(sid))
        logger.info('[tvtv-cache] fetching %d station IDs (%d applied + %d community-only, %d unindexed)',
                    len(station_ids), len(applied),
                    len(community - applied), unindexed)

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


def run_source_disable(source_id: int):
    """Background counterpart to update_source's disable path (app/routes/
    api.py) — flips is_enabled off, then runs the same cancel/purge/xml-
    refresh follow-up, off the request thread.

    Queued instead of committing inline because SQLite's writer lock is
    database-wide, not per-row: disabling one source could otherwise have to
    wait out busy_timeout (30s) behind an unrelated active scrape's chunked
    commits — and since that wait happened inside the HTTP request, it froze
    whichever gunicorn worker was holding it for the full 30s (observed live
    2026-08-14: with only 2 workers, this made the whole admin UI look
    locked up). A background job can absorb that same wait fine; a browser
    tab shouldn't have to sit through it.
    """
    with flask_app.app_context():
        source = Source.query.get(source_id)
        if not source or not source.is_enabled:
            return  # already disabled, or deleted, before this job ran
        source.is_enabled = False
        db.session.commit()

        from app.routes.tasks import cancel_source_jobs, trigger_source_channel_purge
        cancel_source_jobs(source.name)
        if source.name != 'custom':
            # Mark stale now (cheap — old M3U/EPG stays on disk and keeps
            # being served, see invalidate_xml_cache's docstring) rather than
            # running the full subprocess rebuild here: the queued purge job
            # below does its own _invalidate_and_refresh_xml() right after,
            # against a query that already excludes this source's channels
            # via Source.is_enabled == True — a rebuild here would produce
            # byte-identical output to that one, just paid for twice.
            invalidate_xml_cache()
            trigger_source_channel_purge(source.id)
        else:
            # No purge job follows for 'custom' (its channels aren't
            # deleted on disable), so this is the only rebuild that will
            # happen — must be the real one.
            _invalidate_and_refresh_xml()
        logger.info('[source-disable] %s disabled', source.name)


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


SLING_BROWSER_LOGIN_STATUS_KEY = 'sling:browser-login:status'
SLING_BROWSER_LOGIN_SHOT_KEY = 'sling:browser-login:screenshot'
SLING_BROWSER_LOGIN_INPUT_KEY = 'sling:browser-login:input'
SLING_BROWSER_LOGIN_STOP_KEY = 'sling:browser-login:stop'
_SLING_BROWSER_LOGIN_TIMEOUT_SECONDS = 1800

MVPD_BROWSER_LOGIN_STATUS_KEY = 'mvpd:browser-login:status'
MVPD_BROWSER_LOGIN_SHOT_KEY = 'mvpd:browser-login:screenshot'
MVPD_BROWSER_LOGIN_INPUT_KEY = 'mvpd:browser-login:input'
MVPD_BROWSER_LOGIN_STOP_KEY = 'mvpd:browser-login:stop'
MVPD_BROWSER_LOGIN_HINT_KEY = 'mvpd:browser-login:hint'
# How long a single silent/automated wait (autofill detection, or the
# post-submit poll-for-completion loop) can run before we admit we don't know
# whether it's actually stuck or just slow, and suggest the human glance at
# the screenshot. Most silent waits resolve well under this even when no
# password field ever shows (SSO carries over via background API polling,
# not anything visible on the page) — this is intentionally a "we're not
# sure, take a look" nudge, not a "this IS broken" claim.
_MVPD_STUCK_HINT_SECONDS = 10.0
_MVPD_BROWSER_LOGIN_TIMEOUT_SECONDS = 1800
# Adobe's own session polling — short and frequent, since this is a plain
# GET/POST exchange, not something that needs to be rate-limited.
_MVPD_SESSION_POLL_SECONDS = 2.0


def _safe_page_url(page) -> str:
    try:
        return page.url
    except Exception:  # noqa: BLE001
        return '<unknown - page unresponsive>'


def _same_page_url(actual: str, expected: str) -> bool:
    """Compares scheme+host+path only, ignoring query/fragment — Adobe or the
    destination site sometimes appends tracking params on the bounce-back, so
    an exact string match would miss real matches."""
    try:
        a = _urlsplit(actual)
        e = _urlsplit(expected)
        return (a.scheme, a.netloc, a.path.rstrip('/')) == (e.scheme, e.netloc, e.path.rstrip('/'))
    except Exception:  # noqa: BLE001
        return False


def _try_autofill_credentials(
    page, username: str, password: str, wait_seconds: float = 12.0, r=None,
    stop_key: str | None = None, input_key: str | None = None,
    shot_key: str | None = None, hint_key: str | None = None,
) -> bool:
    """Best-effort, short-timeout sibling of _autofill_sling_credentials for
    run_mvpd_browser_login's single-network browser-assisted flow.

    If r (a redis client) is given, this also relays screenshots/input and
    surfaces a "may need your input" hint the same way the poll loops after
    it do (see _relay_input_and_screenshot) — previously this whole up-to-12s
    wait had NO screenshot/input activity at all, so the modal looked frozen
    for the first 12s of every single network, on top of whatever it froze
    for afterward. Optional and defaults to off so callers that don't have a
    redis client handy (e.g. the F5-recovery retry) still work unchanged.

    Empirically (2026-08-05), Sling's login session does NOT reliably carry
    over via cookies between separate Camoufox launches/navigations even
    within the same persistent profile — every additional network genuinely
    needed a fresh interactive login, not just a silent SSO redirect. Rather
    than depend on that, this actively fills and submits the stored
    credentials on whatever login form the shared page lands on, using the
    SAME saved account the human already used for the first network. Never
    raises — returns False (and the caller falls through to its normal
    poll-and-give-up path) if no matching form shows up in time, e.g.
    because SSO actually DID carry over this time, or a captcha blocks it.

    Mechanics proven by a live dry-run against Sling (2026-08-05): the Adobe
    authenticate URL first lands on a BLANK F5 "bookend" interstitial
    (firstbookend.php) whose only inputs are hidden SAML relay fields; the
    real authSynacor login form replaces it ~4-5s later. So the wait keys on
    a VISIBLE password field, never a raw input count — the old count>=2
    check matched the hidden bookend fields and "filled" an invisible form,
    which is why autofill appeared to do nothing. The real form also animates
    in (clicks need a settle + generous timeout, with force/JS-focus
    fallbacks) and can re-render mid-fill, so both values are verified to
    have actually stuck before submitting, with one retry.
    """
    deadline = time.monotonic() + wait_seconds
    wait_started = time.monotonic()
    # Seeded to wait_started, not 0 — this loop's very first iteration runs
    # immediately after the caller's page.goto() to the NEXT provider, before
    # the new page has had any time to actually paint. A screenshot taken
    # that instant (0.0 makes the "at least 1s since last relay" check pass
    # on iteration one) captures a stale/transitional frame — reported live
    # 2026-08-10: "the initial screenshot when moving from one provider to
    # the next provider is stale". Seeding to wait_started delays the first
    # capture by ~1s, giving the new page time to settle first.
    last_relay = wait_started
    while time.monotonic() < deadline:
        try:
            if page.locator('input[type="password"]:visible').count() > 0:
                break
        except Exception as exc:  # noqa: BLE001
            logger.info('[mvpd-login] autofill: locator query failed: %s', exc)
            return False
        if r is not None:
            now = time.monotonic()
            if now - last_relay >= 1.0:
                last_relay = now
                if _relay_input_and_screenshot(
                    page, r, waiting_since=wait_started,
                    stop_key=stop_key, input_key=input_key, shot_key=shot_key, hint_key=hint_key,
                ):
                    logger.info('[mvpd-login] autofill: cancelled while waiting for a password field')
                    return False
        page.wait_for_timeout(300)
    else:
        logger.info('[mvpd-login] autofill: no visible password field after %.1fs (SSO already past login, a captcha-first page, or an unrecognized form) url=%s', wait_seconds, _safe_page_url(page))
        return False

    page.wait_for_timeout(1200)  # the form animates in — let it become clickable/stable

    def _focus_and_type(field, value):
        try:
            field.click(timeout=8000)
        except Exception:  # noqa: BLE001
            try:
                field.click(timeout=2000, force=True)
            except Exception:  # noqa: BLE001
                field.evaluate('el => el.focus()')
        # Clear any prefilled/remembered value first — Cox's Okta widget
        # remembers the username in the persistent profile, and typing appends
        # (observed live 2026-08-06: 9 remembered + 17 typed = 26-char user
        # field, compounding on the verify-retry). Also cleans up our own
        # attempt-1 leftovers on a retry.
        try:
            if field.input_value(timeout=1500):
                field.fill('', timeout=3000)
        except Exception:  # noqa: BLE001
            pass
        field.press_sequentially(value, delay=30, timeout=15000)

    try:
        for fill_attempt in (1, 2):
            pw_loc = page.locator('input[type="password"]:visible')
            if pw_loc.count() == 0:
                logger.info('[mvpd-login] autofill: password field disappeared before fill (page likely mid-redirect) url=%s', _safe_page_url(page))
                return False
            pw_field = pw_loc.first

            user_field = None
            for selector in (
                'input[type="email"]:visible',
                'input[type="text"][name*="email" i]:visible, input[type="text"][name*="user" i]:visible',
                'input[type="text"]:visible',
            ):
                loc = page.locator(selector)
                if loc.count() > 0:
                    user_field = loc.first
                    break
            if user_field is None:
                logger.info('[mvpd-login] autofill: password field present but no visible email/text input url=%s', _safe_page_url(page))
                return False

            _focus_and_type(user_field, username)
            _focus_and_type(pw_field, password)
            page.wait_for_timeout(300)

            got_user = user_field.input_value(timeout=2000)
            got_pw = pw_field.input_value(timeout=2000)
            if got_user != username or got_pw != password:
                logger.info('[mvpd-login] autofill: values did not stick (attempt %d): user %d/%d chars, password %d/%d chars — page likely re-rendered mid-fill',
                            fill_attempt, len(got_user), len(username), len(got_pw), len(password))
                page.wait_for_timeout(700)
                continue

            pw_field.press('Enter')
            logger.info('[mvpd-login] autofill: filled and submitted credentials for %s (attempt %d)', username, fill_attempt)
            return True

        logger.info('[mvpd-login] autofill: gave up — could not get a stable filled form url=%s', _safe_page_url(page))
        return False
    except Exception as exc:  # noqa: BLE001
        logger.info('[mvpd-login] autofill: exception mid-fill: %s', exc)
        return False


_XFINITY_USER_SELECTOR = "prism-input-text[name='user'] input:visible, input[autocomplete='username']:visible"
_XFINITY_PASSWORD_SELECTOR = "prism-input-text[name='passwd'] input:visible, input[type='password']:visible"
_XFINITY_SUBMIT_SELECTOR = "prism-button[prism-id='sign_in'], button[type='submit']"


def _autofill_xfinity_credentials(
    page, username: str, password: str, wait_seconds: float = 20.0, r=None,
    stop_key: str | None = None, input_key: str | None = None,
    shot_key: str | None = None, hint_key: str | None = None,
) -> bool:
    """Xfinity-specific sibling of _try_autofill_credentials for
    run_mvpd_browser_login's Comcast_SSO path.

    The generic single-step autofill above waits for a password field to be
    visible before touching anything, then fills both fields at once. That
    never fires here: Xfinity's login is identifier-first (submit username
    alone; a password step only renders afterward) and built on Comcast's
    own Prism UI web components (<prism-input-text>, <prism-button> — not
    plain <input>/<button>). Confirmed live 2026-08-14 that Playwright's
    shadow-DOM piercing reaches the real underlying <input> fine via
    `prism-input-text[name='user'] input` / `[name='passwd'] input`, and
    both steps submit via `prism-button[prism-id='sign_in']`.

    Also best-effort dismisses the post-login "Add your email address"
    account-hygiene screen via its "Ask me later" skip — confirmed live
    2026-08-14 this is a skippable nag, not a real second factor, at least
    for the account tested. A genuine OTP/2FA prompt still falls through to
    the caller's normal poll-and-give-up path untouched.

    Never raises — returns False if no matching form shows up in time.
    """
    def _wait_for_any(selector: str, deadline: float) -> bool:
        while time.monotonic() < deadline:
            try:
                if page.locator(selector).count() > 0:
                    return True
            except Exception as exc:  # noqa: BLE001
                logger.info('[mvpd-login] xfinity autofill: locator query failed: %s', exc)
                return False
            if r is not None:
                nonlocal last_relay
                now = time.monotonic()
                if now - last_relay >= 1.0:
                    last_relay = now
                    if _relay_input_and_screenshot(
                        page, r, waiting_since=wait_started,
                        stop_key=stop_key, input_key=input_key, shot_key=shot_key, hint_key=hint_key,
                    ):
                        logger.info('[mvpd-login] xfinity autofill: cancelled mid-wait')
                        raise _XfinityAutofillCancelled()
            page.wait_for_timeout(300)
        return False

    class _XfinityAutofillCancelled(Exception):
        pass

    wait_started = time.monotonic()
    last_relay = wait_started

    try:
        if not _wait_for_any(_XFINITY_USER_SELECTOR, time.monotonic() + wait_seconds):
            # The page can look fully rendered (real login form visible in a
            # screenshot) while the underlying React app failed to hydrate —
            # confirmed live 2026-08-14 via a "Minified React error #418"
            # (hydration mismatch) in the page's console right before this
            # exact timeout, most likely from stale cookies/localStorage in
            # the persistent browser profile (shared across every network's
            # different client_id) conflicting with THIS page's server-
            # rendered assumptions. A reload forces a fresh hydration attempt
            # instead of staring at a permanently-dead shell for the rest of
            # the job's budget.
            logger.info('[mvpd-login] xfinity autofill: no visible username field after %.1fs — reloading once and retrying url=%s', wait_seconds, _safe_page_url(page))
            try:
                page.reload(wait_until='domcontentloaded', timeout=30000)
            except Exception as exc:  # noqa: BLE001
                logger.info('[mvpd-login] xfinity autofill: reload failed: %s', exc)
                return False
            if not _wait_for_any(_XFINITY_USER_SELECTOR, time.monotonic() + wait_seconds):
                logger.info('[mvpd-login] xfinity autofill: still no visible username field after reload (SSO already past login, or an unrecognized form) url=%s', _safe_page_url(page))
                return False

        user_field = page.locator(_XFINITY_USER_SELECTOR).first
        user_field.click(force=True, timeout=8000)
        try:
            if user_field.input_value(timeout=1000):
                user_field.fill('', timeout=2000)
        except Exception:  # noqa: BLE001
            pass
        user_field.fill(username)

        submit = page.locator(_XFINITY_SUBMIT_SELECTOR).first
        try:
            submit.click(force=True, timeout=5000)
        except Exception:  # noqa: BLE001
            page.keyboard.press('Enter')
        logger.info('[mvpd-login] xfinity autofill: submitted username for %s', username)

        if not _wait_for_any(_XFINITY_PASSWORD_SELECTOR, time.monotonic() + wait_seconds):
            # Unlike the username step, reloading here is NOT safe to retry:
            # confirmed live 2026-08-14 that reloading after the username has
            # already been submitted lands on a bare, unbranded login page
            # (the client_id/step context is lost, not resumed) — worse than
            # just giving up. Fall through to the caller's normal poll loop,
            # where a human can complete it via the (now screenshot-relayed)
            # modal if this was a genuine one-off render hiccup.
            logger.info('[mvpd-login] xfinity autofill: no visible password field after username submit url=%s', _safe_page_url(page))
            return False

        pw_field = page.locator(_XFINITY_PASSWORD_SELECTOR).first
        pw_field.click(force=True, timeout=8000)
        pw_field.fill(password)

        submit = page.locator(_XFINITY_SUBMIT_SELECTOR).first
        try:
            submit.click(force=True, timeout=5000)
        except Exception:  # noqa: BLE001
            page.keyboard.press('Enter')
        logger.info('[mvpd-login] xfinity autofill: filled and submitted password for %s', username)
    except _XfinityAutofillCancelled:
        return False
    except Exception as exc:  # noqa: BLE001
        logger.info('[mvpd-login] xfinity autofill: exception mid-fill: %s', exc)
        return False

    for _ in range(8):
        page.wait_for_timeout(1500)
        url = _safe_page_url(page) or ''
        if 'login.xfinity.com' not in url and 'idm.xfinity.com' not in url:
            break
        for sel in ("text=/ask me later/i", "button:has-text('Skip')"):
            loc = page.locator(sel).first
            try:
                loc.wait_for(state='visible', timeout=500)
                loc.click(force=True, timeout=3000)
                logger.info('[mvpd-login] xfinity autofill: dismissed post-login nag via %s', sel)
            except Exception:  # noqa: BLE001
                continue
    return True


def _autofill_sling_credentials(page, username: str, password: str) -> None:
    """Fill and submit the saved username/password so the human only has to
    solve the captcha, not retype credentials under a live challenge. Not
    security-relevant - hCaptcha challenges the session either way, before
    or after this runs, so this doesn't change what a human still has to do."""
    for _ in range(30):
        page.wait_for_timeout(1000)
        if page.locator('input').count() >= 2:
            break
    page.wait_for_timeout(1000)

    inputs = page.locator('input')
    email_idx = None
    for i in range(min(inputs.count(), 6)):
        typ = (inputs.nth(i).get_attribute('type') or '').lower()
        name = (inputs.nth(i).get_attribute('name') or '').lower()
        placeholder = (inputs.nth(i).get_attribute('placeholder') or '').lower()
        if typ in ('email', 'text') and ('email' in name or 'email' in placeholder or 'user' in name):
            email_idx = i
            break
    if email_idx is None and inputs.count() >= 2:
        email_idx = 0
    if email_idx is None:
        raise RuntimeError('could not find the email input')

    inputs.nth(email_idx).click()
    page.keyboard.type(username, delay=50)
    pw_idx = email_idx + 1
    if pw_idx >= inputs.count():
        raise RuntimeError('could not find the password input')
    inputs.nth(pw_idx).click()
    page.keyboard.type(password, delay=55)
    page.wait_for_timeout(400)
    inputs.nth(pw_idx).press('Enter')


def _apply_sling_browser_login_input(page, cmd: dict) -> None:
    kind = cmd.get('type')
    if kind == 'click':
        page.mouse.click(float(cmd['x']), float(cmd['y']))
    elif kind == 'mousemove':
        page.mouse.move(float(cmd['x']), float(cmd['y']))
    elif kind == 'mousedown':
        # move-then-down (not click()) so a drag can start here: subsequent
        # mousemove commands are dispatched with the button already held,
        # which the page sees as a drag rather than independent hovers.
        page.mouse.move(float(cmd['x']), float(cmd['y']))
        page.mouse.down()
    elif kind == 'mouseup':
        page.mouse.move(float(cmd['x']), float(cmd['y']))
        page.mouse.up()
    elif kind == 'key':
        page.keyboard.press(str(cmd['key']))


def _extract_sling_oauth_tokens(page) -> tuple[str | None, str | None]:
    """Sling's SPA persists OAuth1 credentials somewhere under localStorage
    (documented at DevTools -> Application -> Local Storage -> 'persist:root'
    -> user -> userData -> oauth_token/oauth_token_secret). Search recursively
    rather than assuming that exact nesting, since it's redux-persist-shaped
    JSON-strings-within-JSON and the exact structure isn't guaranteed stable
    across app versions."""
    found = page.evaluate("""() => {
        function tryParse(v) { try { return JSON.parse(v); } catch (e) { return v; } }
        function deepFind(obj, keys, found, seen) {
            seen = seen || new Set();
            if (obj == null || typeof obj !== 'object' || seen.has(obj)) return;
            seen.add(obj);
            for (const k in obj) {
                if (keys.includes(k) && typeof obj[k] === 'string' && obj[k]) {
                    found[k] = obj[k];
                } else if (typeof obj[k] === 'object') {
                    deepFind(obj[k], keys, found, seen);
                } else if (typeof obj[k] === 'string') {
                    const parsed = tryParse(obj[k]);
                    if (parsed && typeof parsed === 'object') deepFind(parsed, keys, found, seen);
                }
            }
        }
        const found = {};
        const wanted = ['oauth_token', 'oauth_token_secret'];
        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            const raw = localStorage.getItem(key);
            const parsed = tryParse(raw);
            if (parsed && typeof parsed === 'object') deepFind(parsed, wanted, found);
            else if (wanted.includes(key)) found[key] = raw;
        }
        return found;
    }""")
    return found.get('oauth_token'), found.get('oauth_token_secret')


def run_sling_browser_login():
    """Drive a real, human-operated sign-in to sling.com.

    A Camoufox (anti-detect Firefox) tab auto-fills the saved credentials and
    loads the real sign-in page. The admin UI streams periodic screenshots of
    it and forwards the admin's own clicks/keystrokes back to the page (via
    Redis, since screenshots/input may be served by a different gunicorn
    worker than the one running this job) — so the human sees the real page
    and solves the real hCaptcha challenge themselves. The OAuth token is
    captured either straight from the auth-callback URL (primary path) or
    from localStorage as a fallback, and saved to the source config.
    """
    with flask_app.app_context():
        import json as _json_login

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[sling-login] Redis unavailable, aborting: %s', exc)
            return

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    SLING_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': url}),
                )
            except Exception:  # noqa: BLE001
                pass

        r.delete(SLING_BROWSER_LOGIN_STOP_KEY)
        r.delete(SLING_BROWSER_LOGIN_INPUT_KEY)
        set_status('starting', 'Launching browser…')

        source = Source.query.filter_by(name='sling').first()
        if not source:
            set_status('error', 'Sling source not found')
            return

        scraper_cls = registry.get('sling')
        if not scraper_cls:
            set_status('error', 'Sling scraper not registered')
            return
        scraper = scraper_cls(config=dict(source.config or {}))

        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            set_status('error', 'Camoufox is not installed on this container')
            return

        profile_dir = '/data/browser_profiles/sling'
        try:
            import os as _os_login
            _os_login.makedirs(profile_dir, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[sling-login] could not create profile dir %s: %s', profile_dir, exc)

        try:
            # Camoufox (anti-detect Firefox) instead of stock Playwright Chromium:
            # fingerprint spoofing (WebGL, navigator, etc.) happens in the compiled
            # browser itself rather than via JS patches layered on top, and it's
            # Firefox-based so it isn't subject to CDP's Runtime.enable leak at all.
            # headless='virtual' runs a real (non-headless) Firefox against an
            # internal Xvfb display - plain headless=True disables WebGL entirely.
            # A persistent profile (not a fresh context every run) so cookies and
            # device-trust state accumulate across attempts instead of presenting
            # as a never-before-seen browser each time.
            with Camoufox(
                headless='virtual',
                os='windows',
                persistent_context=True,
                user_data_dir=profile_dir,
                window=(1280, 800),
            ) as context:
                page = context.pages[0] if context.pages else context.new_page()
                page.on('crash', lambda p: logger.warning('[sling-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
                page.on('close', lambda p: logger.warning('[sling-login] page CLOSE event fired'))
                page.on('pageerror', lambda exc: logger.warning('[sling-login] page JS error: %s', str(exc)[:500]))
                try:
                    page.goto('https://www.sling.com/sign-in', wait_until='domcontentloaded', timeout=30000)
                except Exception as exc:  # noqa: BLE001
                    set_status('error', f'Failed to load sign-in page: {exc}')
                    return

                _sling_username = (source.config or {}).get('username', '').strip()
                _sling_password = (source.config or {}).get('password', '').strip()
                if _sling_username and _sling_password:
                    try:
                        _autofill_sling_credentials(page, _sling_username, _sling_password)
                        set_status('running', 'Solve the captcha below if shown.', page.url)
                    except Exception as exc:  # noqa: BLE001
                        # Not fatal - the human can still fill the form manually from here.
                        logger.info('[sling-login] credential autofill failed, falling back to manual: %s', exc)
                        set_status('running', 'Sign in below, including the captcha if shown.', page.url)
                else:
                    set_status('running', 'Sign in below, including the captcha if shown.', page.url)

                deadline = time.monotonic() + _SLING_BROWSER_LOGIN_TIMEOUT_SECONDS
                last_shot = 0.0
                last_heartbeat = 0.0
                consecutive_failures = 0
                # RQ only calls job.heartbeat() once at start and once at completion -
                # never during execution (confirmed against the installed rq==1.16.2
                # source). This job can legitimately run up to
                # _SLING_BROWSER_LOGIN_TIMEOUT_SECONDS, but _job_already_active()'s
                # staleness check (tasks.py) treats ANY job with no heartbeat for 300s
                # as a dead zombie and deletes it - which would let a second click
                # launch a duplicate Camoufox instance against the same locked
                # persistent profile dir while this one is still genuinely running.
                # Self-heartbeat periodically so RQ's own bookkeeping doesn't disagree
                # with reality.
                current_job = get_current_job()
                # A dead/crashed page makes every call below fail. Each one is caught
                # individually (a single transient hiccup on one call shouldn't kill
                # the session) - but if a screenshot attempt fails this many times in
                # a row, the page is gone, not just having a bad moment. Bail out with
                # a real error instead of silently looping against a dead browser for
                # the full deadline, which is what happened before this fix: nothing
                # ever raised past the loop, so it just spun uselessly for 30 minutes.
                _MAX_CONSECUTIVE_FAILURES = 15
                while time.monotonic() < deadline:
                    if r.exists(SLING_BROWSER_LOGIN_STOP_KEY):
                        set_status('stopped', 'Cancelled')
                        return

                    if page.is_closed():
                        set_status('error', 'Browser page closed unexpectedly.')
                        return

                    for _ in range(20):
                        raw = r.lpop(SLING_BROWSER_LOGIN_INPUT_KEY)
                        if raw is None:
                            break
                        try:
                            _apply_sling_browser_login_input(page, _json_login.loads(raw))
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[sling-login] input apply failed: %s', exc)

                    now = time.monotonic()
                    if current_job and now - last_heartbeat > 60:
                        try:
                            current_job.heartbeat(datetime.now(timezone.utc), 180)
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[sling-login] self-heartbeat failed: %s', exc)
                        last_heartbeat = now

                    if now - last_shot > 0.25:
                        try:
                            shot = page.screenshot(type='jpeg', quality=60)
                            r.setex(SLING_BROWSER_LOGIN_SHOT_KEY, 30, shot)
                            set_status('running', 'Sign in below, including the captcha if shown.', page.url)
                            consecutive_failures = 0
                        except Exception as exc:  # noqa: BLE001
                            consecutive_failures += 1
                            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                                logger.warning('[sling-login] page unresponsive after %d failed screenshots: %s', consecutive_failures, exc)
                                set_status('error', f'Browser session died: {exc}')
                                return
                        last_shot = now

                    # The SPA lands here on success (.../sign-in/auth-callback?transient_token=...)
                    # then does its OWN in-page exchange and closes itself shortly after - which
                    # we've seen fail with "NetworkError when attempting to fetch resource" before
                    # closing, seemingly unrelated to the sign-in itself. Grab the token straight
                    # from the URL the moment it appears and exchange it ourselves via plain HTTP
                    # (the same code path the manual "paste this URL" fallback already uses) rather
                    # than depend on the in-page fetch succeeding or the page surviving long enough
                    # for localStorage to populate.
                    if 'transient_token=' in page.url:
                        raw_token = page.url.split('transient_token=', 1)[1].split('&', 1)[0].strip()
                        if raw_token:
                            try:
                                scraper._exchange_browser_auth_token(raw_token)
                                persist_source_config_updates(source.id, scraper._pending_config_updates)
                                set_status('success', 'Signed in — OAuth token saved.')
                                logger.info('[sling-login] exchanged transient_token captured from URL')
                                return
                            except Exception as exc:  # noqa: BLE001
                                logger.warning('[sling-login] transient_token exchange failed: %s', exc)
                                set_status('error', f'Sign-in succeeded but token exchange failed: {exc}')
                                return

                    try:
                        token, secret = _extract_sling_oauth_tokens(page)
                    except Exception:  # noqa: BLE001
                        token, secret = None, None
                    if token and secret:
                        persist_source_config_updates(source.id, {
                            'oauth_token': token,
                            'oauth_token_secret': secret,
                            'oauth_token_time': int(time.time()),
                            'browser_auth_token': '',
                        })
                        set_status('success', 'Signed in — OAuth token saved.')
                        logger.info('[sling-login] captured and saved OAuth token from localStorage')
                        return

                    page.wait_for_timeout(80)

                set_status('error', 'Timed out waiting for sign-in to complete.')
                return
        except BaseException as exc:  # noqa: BLE001
            # BaseException, not Exception: two prior runs vanished silently
            # (RQ 'finished', not 'failed'; nothing logged) after the browser
            # process died mid-session - whatever that was didn't surface as a
            # plain Exception here, so widen the net to actually see it next time.
            logger.exception('[sling-login] browser session failed')
            try:
                set_status('error', f'Browser session failed: {exc}')
            except Exception:  # noqa: BLE001
                pass
            return


def _save_mvpd_authn_token(requestor_id: str, authn_token: str) -> None:
    account = TVEAccount.query.filter_by(provider_id='mvpd').first()
    if not account:
        return
    cfg = dict(account.config or {})
    mvpd_authn = dict(cfg.get('mvpd_authn') or {})
    mvpd_authn[requestor_id] = {'authn_token': authn_token, 'captured_at': int(time.time())}
    cfg['mvpd_authn'] = mvpd_authn
    account.config = cfg
    db.session.commit()


def _harvest_and_save_xfinity_cookies(context) -> None:
    """Grabs the real, JS-matured Akamai Bot Manager + Xfinity SESSION
    cookies out of a Camoufox context right after a successful Comcast_SSO
    pairing, and persists them so authorize_mvpd() can do every SUBSEQUENT
    Comcast_SSO sign-in (any network, not just this one — the cookies are
    account/browser-session-scoped, not per-client_id) over plain HTTP
    without a browser at all. Confirmed live 2026-08-14: login.xfinity.com's
    credential POST is blocked by Akamai Bot Manager for any bare HTTP
    client's own freshly-issued cookies — only cookies matured by a real
    browser session pass. See app/tve/adobe_pass.py's
    authenticate_with_xfinity_cookies() and
    dev/comcast/XFINITY_ADOBE_PASS_DIRECT_HTTP_RESEARCH.md. Best-effort,
    never raises — a failed harvest just means the next Comcast_SSO login
    falls back to needing another browser-assisted pairing, same as today.
    """
    try:
        raw_cookies = context.cookies()
    except Exception as exc:  # noqa: BLE001
        logger.info('[mvpd-login] xfinity cookie harvest failed: %s', exc)
        return
    jar = {}
    for c in raw_cookies:
        domain = (c.get('domain') or '').lstrip('.')
        if 'xfinity.com' not in domain:
            continue
        jar[c['name']] = {'value': c['value'], 'domain': domain, 'path': c.get('path') or '/'}
    if not jar:
        logger.info('[mvpd-login] xfinity cookie harvest found no xfinity.com cookies')
        return
    account = TVEAccount.query.filter_by(provider_id='mvpd').first()
    if not account:
        return
    save_xfinity_cookie_jar(account, jar)
    logger.info('[mvpd-login] harvested and saved %d xfinity.com cookies for future pure-HTTP sign-ins', len(jar))


def _record_tve_login_error(key: str, message: str) -> None:
    """Records the last failed sign-in attempt for one TVE network/requestor,
    keyed the same way app/tve/status.py's tve_network_status() keys its
    per-network entries (requestor_id for the legacy family, 'nbc'/'fox'/
    'amcn'/'discovery'/'foxone' for the rest). A network that has never
    signed in successfully otherwise just shows "Never" on the admin
    settings page with no indication why — e.g. bad entitlement for that
    specific network on an otherwise-working Cox account (confirmed live
    2026-08-11: FYI came back "not entitled" while its A+E siblings
    succeeded). No need to explicitly clear this on a later success —
    tve_network_status() only surfaces an error note when it's newer than
    the last successful sign-in, so a subsequent success naturally
    supersedes it.
    """
    account = TVEAccount.query.filter_by(provider_id='mvpd').first()
    if not account:
        return
    cfg = dict(account.config or {})
    errors = dict(cfg.get('tve_last_error') or {})
    errors[key] = {'message': str(message)[:300], 'at': int(time.time())}
    cfg['tve_last_error'] = errors
    account.config = cfg
    db.session.commit()


def _cox_login_error_detail(exc: Exception, label: str) -> str:
    """Classifies an exception from a Cox-branch scripted sign-in attempt
    into a short, human-readable detail (never includes the network's own
    label/name — callers prepend that themselves only where it's not
    already implied by context, e.g. the admin modal's status line but not
    the "last attempt failed" note, which sits directly under that
    network's own row label already).

    Shared by run_mvpd_browser_login's and run_fox_browser_login's Cox
    branches, which had this exact three-way classification hand-copied
    and already drifted slightly inconsistent between them (code review,
    2026-08-11). run_nbc_browser_login deliberately does NOT use this —
    NbcTveScraper._ensure_entitled() raises exceptions that already have
    their own full context baked in one layer down (e.g. "NBC TVE:
    <mso_id> is not authorized: <reason>"), so wrapping them again here
    would double up the framing instead of clarifying it. Legacy's
    AdobePassCoxClient and FOX's _fox_sports_mvpd_token both raise bare,
    terse exceptions that actually need this context added.
    """
    if isinstance(exc, TVENotAuthorizedError):
        return f'not entitled — {exc}'
    if isinstance(exc, TVEAuthError):
        return str(exc)
    logger.exception('[mvpd-login] unexpected failure for %s', label)
    return str(exc)


def _relay_input_and_screenshot(
    page, r, waiting_since: float | None = None,
    stop_key: str | None = None, input_key: str | None = None,
    shot_key: str | None = None, hint_key: str | None = None,
) -> bool:
    """Keep the streamed browser modal alive and interactive during a silent
    wait phase (e.g. _try_autofill_credentials's wait for a password field
    to render), and report whether the human clicked Stop/Cancel.

    Without the relay half, human input queued via the modal is never
    drained and no new screenshots are ever taken once the primary loop
    hands off to this wait — so the modal freezes on whatever page was
    showing the instant the wait started, while the real browser has
    already moved on. A human watching that frozen frame and clicking on it
    looks exactly like "stuck, can't click" (confirmed live 2026-08-05) even
    though their clicks WERE being queued server-side the whole time, just
    never applied.

    Without the stop-check half, clicking Stop/Cancel during this wait does
    nothing at all — only the primary loop ever checked that key, so a
    human canceling mid-wait would see repeated Stop/Start clicks fail
    silently while the job kept polling on its own budget (confirmed live
    2026-08-05). Returns True if the human asked to stop — callers should
    break out of their own loop when this happens.

    waiting_since, if given, is the monotonic time this particular silent
    wait (autofill detection, or the post-submit poll-for-completion loop)
    started. Once it's run longer than _MVPD_STUCK_HINT_SECONDS, a short-TTL
    hint is published alongside the screenshot suggesting the human glance at
    it — landing on a generic "Sign In" gate page and sitting there is USUALLY
    fine (the real auth resolves via background API polling, independent of
    what's on screen — confirmed live 2026-08-10 against AMC Networks TVE's
    BBC America/IFC/WE tv gate pages), but occasionally a page genuinely does
    need a manual click and there's no reliable way to tell those apart
    server-side. The hint has a 5s TTL and this function is called ~1/s while
    waiting, so it stays lit for the duration of a genuinely long wait and
    disappears within 5s of the wait actually ending — no explicit clear call
    needed.

    Best-effort; never raises. Call this every ~1s from within a silent
    wait's own poll loop, same cadence as the primary loop.

    stop_key/input_key/shot_key/hint_key default to the shared legacy
    'mvpd:browser-login:*' keys — correct for legacy/AMC/Discovery (which
    genuinely share one modal/redis-namespace by design). NBC's and FOX's
    own STANDALONE primary loops have their
    own separate 'nbc-mvpd:*'/'fox-mvpd:*' namespace and must pass their own
    keys explicitly — passing the defaults there would write screenshots
    nobody's modal ever reads (confirmed live via code review, 2026-08-10:
    the NBC/FOX autofill-wait screenshot fix silently didn't work because of
    exactly this default). Params are keyword-only in spirit; resolved
    inside the function body (not as literal defaults) since NBC_/FOX_
    BROWSER_LOGIN_*_KEY aren't defined yet at this point in the file.
    """
    import json as _json
    stop_key = stop_key or MVPD_BROWSER_LOGIN_STOP_KEY
    input_key = input_key or MVPD_BROWSER_LOGIN_INPUT_KEY
    shot_key = shot_key or MVPD_BROWSER_LOGIN_SHOT_KEY
    hint_key = hint_key or MVPD_BROWSER_LOGIN_HINT_KEY
    stopped = False
    try:
        stopped = bool(r.exists(stop_key))
    except Exception:  # noqa: BLE001
        pass
    for _ in range(20):
        try:
            raw = r.lpop(input_key)
        except Exception:  # noqa: BLE001
            break
        if raw is None:
            break
        try:
            _apply_sling_browser_login_input(page, _json.loads(raw))
        except Exception:  # noqa: BLE001
            pass
    try:
        shot = page.screenshot(type='jpeg', quality=60)
        r.setex(shot_key, 30, shot)
    except Exception:  # noqa: BLE001
        pass
    if waiting_since is not None and time.monotonic() - waiting_since > _MVPD_STUCK_HINT_SECONDS:
        try:
            r.setex(
                hint_key, 5,
                "Taking a while — if the screen below shows a Sign In or Continue button, "
                "clicking it can help. It may also just be working in the background.",
            )
        except Exception:  # noqa: BLE001
            pass
    return stopped


_F5_REJECTION_MARKER = 'The requested URL was rejected'


def _sling_f5_recover(
    page, login_url: str, username: str, password: str, r=None,
    stop_key: str | None = None, input_key: str | None = None,
    shot_key: str | None = None, hint_key: str | None = None,
) -> bool:
    """Detect Sling's F5 bot-defense block page ("The requested URL was
    rejected. Please consult with your administrator.") that replaces the
    login form in-place when a submitted POST gets flagged. Observed live
    2026-08-05: AETV's autofilled submit was rejected while LIFETIME/FYI's
    identical submits under a minute later passed — the block is transient
    per-attempt scoring, so one backoff + reload + refill retry is usually
    enough. Returns True if the block page was detected and a retry was
    attempted (caller should extend its poll window), False otherwise."""
    try:
        if page.get_by_text(_F5_REJECTION_MARKER).count() == 0:
            return False
    except Exception:  # noqa: BLE001
        return False
    logger.info('[mvpd-login] Sling bot-defense rejected the submitted login — backing off 8s and retrying once')
    try:
        page.wait_for_timeout(8000)
        page.goto(login_url, wait_until='domcontentloaded', timeout=30000)
        if username and password:
            _try_autofill_credentials(
                page, username, password, r=r,
                stop_key=stop_key, input_key=input_key, shot_key=shot_key, hint_key=hint_key,
            )
    except Exception as exc:  # noqa: BLE001
        logger.info('[mvpd-login] F5-recovery reload failed: %s', exc)
    return True


def run_amcn_browser_login(mso_id: str):
    """Standalone "Sign in" for AMC Networks TVE.

    Same reasoning as run_discovery_browser_login: resolve() already does a
    fully scripted Cox login per channel family via
    AMCNetworksTVEScraper._adobe_decision_token() (register session, follow
    the Adobe redirect, then _cox_saml_login()'s direct POST to
    login.cox.com/api/v1/authn) — no browser needed. Unlike Discovery, AMCN
    caches auth separately per requestor_id (AMC/BBCA/IFC/WETV each has its
    own adobe_auth:<requestor_id> cache entry, see _adobe_auth_cache_key),
    so "Sign in" here warms all four instead of just one. Passes force=True
    to _adobe_decision_token so a click always exercises a live Cox login
    instead of silently returning a still-valid cached token untested — same
    "Sign in should be real" reasoning as FOX's and FOX One's own buttons
    (see run_fox_browser_login's docstring); without it, re-clicking within
    a token's ~24h lifetime logged a "paired"/"authorized" success in
    milliseconds with no request actually sent and no admin/settings
    timestamp movement to show for it (reported live 2026-08-12).
    """
    with flask_app.app_context():
        import json as _json_login
        from app.scrapers.amcn_tve import AMCNetworksTVEScraper, CHANNELS

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[amcn-mvpd-login] Redis unavailable, aborting: %s', exc)
            return

        def set_status(state: str, message: str = ''):
            try:
                r.setex(
                    MVPD_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': '', 'requestor_id': 'AMC Networks TVE', 'steps': []}),
                )
            except Exception:  # noqa: BLE001
                pass

        r.delete(MVPD_BROWSER_LOGIN_STOP_KEY)
        r.delete(MVPD_BROWSER_LOGIN_INPUT_KEY)
        set_status('running', 'Signing in to AMC Networks TVE…')

        source = Source.query.filter_by(name='amcn_tve').first()
        if not source:
            set_status('error', 'AMC Networks TVE source not found.')
            return
        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if not account or not account.is_enabled or not account.has_credentials():
            set_status('error', 'TVE credentials are not configured in Settings.')
            return
        scraper = AMCNetworksTVEScraper(config=dict(source.config or {}))
        device_id = scraper._device_id()
        # Force the lazy cache load here, in this thread, before any worker
        # threads touch scraper.cache below — otherwise two threads racing
        # the "is it loaded yet" check could both trigger it concurrently.
        scraper.cache  # noqa: B018

        # The 4 channels' logins are fully independent (each its own
        # requestor_id, own adobe_auth:<requestor_id> cache entry, own
        # AdobePassCoxClient/requests.Session — nothing shared but this one
        # scraper instance, and each writes to its own distinct dict key,
        # safe under the GIL) — running them one at a time was ~4x slower
        # than necessary. throttle_cox_login() still serializes the actual
        # Cox POSTs to the same safe spacing either way, so this doesn't
        # change how fast real credential attempts hit Cox/Okta — it only
        # overlaps the OTHER three Adobe API calls each channel makes,
        # which don't touch Cox at all (code review, 2026-08-11; measured
        # live: 4 sequential cold-cache logins took ~30s, ~16s of which was
        # pure serial throttle waiting).
        #
        # Force-stop is checked once up front rather than between channels
        # now — once dispatched, an in-flight login couldn't be interrupted
        # either way (same limitation the old sequential loop had for
        # whichever channel was actively running), and the whole batch is
        # now short enough (~5-10s) that mid-flight cancellation matters
        # much less than it did at ~30s.
        stopped = r.exists(MVPD_BROWSER_LOGIN_STOP_KEY)
        authorized, failed = [], []
        if not stopped:
            def _sign_in_one(channel):
                with flask_app.app_context():
                    try:
                        scraper._adobe_decision_token(channel, account, device_id, force=True)
                        return channel.name, None
                    except TVENotAuthorizedError as exc:
                        return channel.name, f'not entitled ({exc})'
                    except TVEAuthError as exc:
                        return channel.name, str(exc)
                    except Exception as exc:  # noqa: BLE001
                        logger.exception('[amcn-mvpd-login] unexpected failure for %s', channel.name)
                        return channel.name, str(exc)

            with ThreadPoolExecutor(max_workers=4, thread_name_prefix='amcn-mvpd-login') as pool:
                # Submitted (not as_completed) order so the final message
                # lists networks in CHANNELS' own order regardless of which
                # thread happens to finish first.
                futures = [pool.submit(_sign_in_one, channel) for channel in CHANNELS.values()]
                for future in futures:
                    name, error = future.result()
                    if error is None:
                        authorized.append(name)
                    else:
                        failed.append(f'{name}: {error}')

        persist_source_config_updates(source.id, scraper._pending_config_updates)
        persist_source_cache_updates(source.id, scraper._pending_cache_updates)

        if stopped:
            set_status('stopped', f'Cancelled — authorized: {", ".join(authorized)}.' if authorized else 'Cancelled.')
        elif authorized:
            message = f'Signed in — authorized: {", ".join(authorized)}.'
            if failed:
                message += ' Not authorized: ' + '; '.join(failed) + '.'
            set_status('success', message)
            logger.info('[amcn-mvpd-login] paired mso_id=%s authorized=%s failed=%s (scripted, no browser)', mso_id, authorized, failed)
        else:
            message = '; '.join(failed) or 'No AMC Networks channels authorized.'
            _record_tve_login_error('amcn', message)
            set_status('error', message)


def run_discovery_browser_login(mso_id: str):
    """Standalone "Sign in" for Discovery TVE.

    Unlike AMC/NBC/FOX, Discovery's Cox login never actually needs a
    browser: DiscoveryTVEScraper._authenticate() already does the whole
    thing scripted (register, gauth authorize, _cox_saml_login's direct
    POST to login.cox.com/api/v1/authn, code exchange, entitlement check)
    on every session refresh during normal scraping. Confirmed live
    2026-08-11: a full authenticate+entitlement round trip in ~3s with the
    real Cox account, zero Camoufox involved. The old page.goto()+autofill
    version routed this same login through a full Firefox launch for no
    reason, which is almost certainly why Discovery was one of the networks
    reported stuck/timing out in the community thread — Camoufox
    render/timeout budgets, not anything about Discovery's actual auth
    requirements. This only supports Cox (the only
    MSO _authenticate() has wired up); non-Cox reports back as an error
    same as before.

    No stop-key check here (unlike run_amcn_browser_login's per-channel
    loop, code review 2026-08-11) — _authenticate() is one ~3s scripted call
    with no natural interruption point partway through, so there's nothing
    meaningful to cancel into; worst case is bounded by its own per-request
    timeouts (30s each) rather than the old ~30min browser session.
    """
    with flask_app.app_context():
        import json as _json_login

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[discovery-mvpd-login] Redis unavailable, aborting: %s', exc)
            return

        def set_status(state: str, message: str = ''):
            try:
                r.setex(
                    MVPD_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': '', 'requestor_id': 'Discovery TVE', 'steps': []}),
                )
            except Exception:  # noqa: BLE001
                pass

        r.delete(MVPD_BROWSER_LOGIN_STOP_KEY)
        r.delete(MVPD_BROWSER_LOGIN_INPUT_KEY)
        set_status('running', 'Signing in to Discovery TVE…')

        from app.scrapers.discovery_tve import DiscoveryTVEScraper

        source = Source.query.filter_by(name='discovery_tve').first()
        if not source:
            set_status('error', 'Discovery TVE source not found.')
            return
        scraper = DiscoveryTVEScraper(config=dict(source.config or {}))
        try:
            scraper._authenticate()
        except TVENotAuthorizedError as exc:
            _record_tve_login_error('discovery', f'not entitled — {exc}')
            set_status('error', f'Discovery TVE: not entitled — {exc}')
            return
        except TVEAuthError as exc:
            _record_tve_login_error('discovery', str(exc))
            set_status('error', f'Discovery TVE: {exc}')
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception('[discovery-mvpd-login] unexpected failure')
            _record_tve_login_error('discovery', str(exc))
            set_status('error', f'Discovery TVE: {exc}')
            return
        persist_source_cache_updates(source.id, scraper._pending_cache_updates)
        set_status('success', 'Signed in — Discovery TVE authorized.')
        logger.info('[discovery-mvpd-login] paired mso_id=%s (scripted, no browser)', mso_id)


def _summarize_pairing_results(results: dict[str, tuple[bool, str]]) -> str:
    authorized = [k for k, (ok, _) in results.items() if ok]
    other = [(k, msg) for k, (ok, msg) in results.items() if not ok]
    parts = []
    if authorized:
        parts.append(f'Signed in — authorized: {", ".join(authorized)}.')
    else:
        parts.append('Signed in.')
    if other:
        parts.append('Not available: ' + '; '.join(f'{k} ({msg})' for k, msg in other) + '.')
    return ' '.join(parts)


_BROWSER_LOGIN_MAX_ATTEMPTS = 3


class _BrowserSessionDied(Exception):
    """The Camoufox page/browser died mid-login (page CLOSE fired, target
    crashed, or the page stopped answering screenshots). Observed
    intermittently on first attempts across NBC/FOX/legacy flows (2026-08-05,
    cause unknown — a plain retry has worked every time), so the login jobs
    treat it as retryable and relaunch the browser instead of failing out to
    the user."""


def _is_browser_death(exc: BaseException) -> bool:
    """True if exc means the browser/page itself died (retryable), as opposed
    to a real auth/protocol failure."""
    if isinstance(exc, _BrowserSessionDied):
        return True
    try:
        from playwright.sync_api import Error as _PlaywrightError
    except ImportError:
        return False
    if not isinstance(exc, _PlaywrightError):
        return False
    msg = str(exc)
    return 'has been closed' in msg or 'Target crashed' in msg or 'Connection closed' in msg


def run_mvpd_browser_login(requestor_id: str, resource: str, software_statement: str, redirect_url: str, mso_id: str, _attempt: int = 1, _deadline: float | None = None):
    """Drive a real, human-operated sign-in to an MVPD's Adobe Pass login page
    for just requestor_id — each network is signed into independently (the
    admin UI's "Sign in to all" batches these calls client-side, one per
    network; there is no shared-browser-session sweep anymore, see below).

    For MSOs whose login page blocks scripted clients outright (Sling's
    identity.sling.com returns HTTP 417 to yt-dlp even with browser TLS
    impersonation — see app/tve/ytdlp_mvpd.py), the only thing that reliably
    gets through is an actual browser. Adobe Pass's legacy protocol is built
    for exactly this "second screen" case: setup_client/register_device/
    create_regcode happen here, scripted, same as always (that part was never
    blocked — only the MSO's own login form is). We then hand the resulting
    authenticate/saml URL to a real Camoufox tab for a human to complete
    Sling's actual login in, while polling Adobe's /adobe-services/session
    endpoint with our own reg_code from this process. Adobe's backend binds
    the browser's completed SAML round-trip to our reg_code server-side, so
    polling picks it up regardless of the browser and this process being
    entirely separate HTTP sessions — no token-scraping from the page needed.
    The resulting authn_token is long-lived and cached, so this browser
    session is a one-time cost per requestor_id (see authorize_mvpd()).
    """
    with flask_app.app_context():
        import json as _json_login

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[mvpd-login] Redis unavailable, aborting: %s', exc)
            return

        # Live-updated checklist (single entry — one network per call) so the
        # modal shows real progress instead of one static message.
        steps: list[dict] = [{'label': requestor_id, 'state': 'running'}]

        def _step(label: str, state: str, message: str = ''):
            for entry in steps:
                if entry['label'] == label:
                    entry['state'] = state
                    entry['message'] = message
                    break
            else:
                steps.append({'label': label, 'state': state, 'message': message})

        # Camoufox's underlying Firefox process has been observed to die on its
        # own mid-session (confirmed live 2026-08-05). By that point the job has
        # often already finished its real work and called set_status('success'/
        # 'stopped', ...). The `with Camoufox(...)` block's own teardown then
        # tries to close the already-dead browser, raises, and — since that
        # happens while Python is unwinding this function's `return` — REPLACES
        # the good terminal status with a misleading crash message unless we
        # explicitly remember a terminal status was already recorded.
        _terminal_status_set = {'v': False}

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    MVPD_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': url, 'requestor_id': requestor_id, 'steps': steps}),
                )
                if state in ('success', 'error', 'stopped'):
                    _terminal_status_set['v'] = True
            except Exception:  # noqa: BLE001
                pass

        r.delete(MVPD_BROWSER_LOGIN_STOP_KEY)
        r.delete(MVPD_BROWSER_LOGIN_INPUT_KEY)
        set_status('starting', 'Registering with Adobe Pass…')

        # One deadline shared across browser-crash retries (the RQ job_timeout
        # is only ~30s above _MVPD_BROWSER_LOGIN_TIMEOUT_SECONDS, so a retry
        # must never reset the clock).
        deadline = _deadline if _deadline is not None else time.monotonic() + _MVPD_BROWSER_LOGIN_TIMEOUT_SECONDS

        account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
        mvpd_username = (account_row.username if account_row else '') or ''
        mvpd_password = (account_row.password if account_row else '') or ''

        from app.tve.adobe_pass import _ensure_cox_device_fingerprint
        client = AdobePassCoxClient(
            requestor_id=requestor_id,
            resource=resource,
            software_statement=software_statement,
            redirect_url=redirect_url,
            device_fingerprint=_ensure_cox_device_fingerprint(account_row) if account_row else None,
        )

        if mso_id == 'Cox':
            # Cox's login step is already fully scripted:
            # AdobePassCoxClient.authorize_with_cox() does the whole thing —
            # register, regcode, direct login.cox.com/api/v1/authn POST
            # (authenticate_with_cox), fetch_session_token, authorize — exactly
            # what authorize_mvpd() already does automatically at play time for
            # this same "legacy" family (History/A&E/Warner). No browser
            # needed; confirmed live 2026-08-11 (History TVE authorized in a
            # few seconds, zero Camoufox). Only non-Cox MSOs (e.g. Sling,
            # whose login page blocks scripted clients outright) fall through
            # to the browser-assisted flow below.
            set_status('running', f'Signing in to {requestor_id}…')
            try:
                client.authorize_with_cox(mvpd_username, mvpd_password)
            except Exception as exc:  # noqa: BLE001
                detail = _cox_login_error_detail(exc, requestor_id)
                _step(requestor_id, 'failed', detail[:120])
                _record_tve_login_error(requestor_id, detail)
                set_status('error', f'{requestor_id}: {detail}')
                return
            _save_mvpd_authn_token(requestor_id, client.ctx.authn_token)
            _step(requestor_id, 'done', 'authorized')
            set_status('success', f'Signed in — {requestor_id} authorized.')
            logger.info('[mvpd-login] paired requestor_id=%s mso_id=Cox (scripted, no browser)', requestor_id)
            return

        if mso_id == 'Comcast_SSO':
            # Try a saved cookie jar (harvested from a previous successful
            # Comcast_SSO browser pairing — see _harvest_and_save_xfinity_
            # cookies below, and app/tve/adobe_pass.py's authorize_mvpd())
            # BEFORE ever opening a browser. Without this check, every
            # single network in a "Sign in to all" batch opened its own
            # fresh Camoufox window even though the FIRST one's success
            # already saved a jar good for every other network too —
            # confirmed live 2026-08-14 (HISTORY/AETV/LIFETIME each did a
            # full browser login back to back despite each one saving a
            # cookie jar right after). Only falls through to the browser
            # below when there's no jar yet, or the saved one has gone
            # stale (Akamai cookies expire in ~2-4h — see
            # dev/comcast/XFINITY_ADOBE_PASS_DIRECT_HTTP_RESEARCH.md).
            cookie_jar = (account_row.config or {}).get('xfinity_cookie_jar') if account_row else None
            if cookie_jar:
                set_status('running', 'Trying saved sign-in (no browser needed)…')
                try:
                    token = client.authorize_with_xfinity_cookies(mvpd_username, mvpd_password, cookie_jar)
                except TVENotAuthorizedError as exc:
                    _step(requestor_id, 'failed', 'not entitled')
                    _record_tve_login_error(requestor_id, str(exc))
                    set_status('error', f'{requestor_id}: not entitled ({exc})')
                    return
                except TVEAuthError as exc:
                    logger.info(
                        '[mvpd-login] saved xfinity cookie jar did not work for %s, falling back to browser: %s',
                        requestor_id, exc,
                    )
                else:
                    _save_mvpd_authn_token(requestor_id, client.ctx.authn_token)
                    _step(requestor_id, 'done', 'authorized')
                    set_status('success', f'Signed in — {requestor_id} authorized (no browser needed).')
                    logger.info(
                        '[mvpd-login] paired requestor_id=%s mso_id=Comcast_SSO via saved cookie jar (no browser)',
                        requestor_id,
                    )
                    return
            set_status('running', 'No usable saved sign-in — opening a browser…')

        try:
            client.setup_client()
            client.register_device()
            client.create_regcode()
        except TVEAuthError as exc:
            _step(requestor_id, 'failed', str(exc)[:120])
            set_status('error', f'Adobe Pass registration failed: {exc}')
            return
        auth_url = client.authenticate_redirect_url(mso_id)

        def _grace_poll_pairing(reason: str) -> bool:
            """See run_nbc_browser_login's helper: MSO completion pages (Cox's
            Okta widget) close themselves right after the credentials POST
            while Adobe binds the session server-side — poll browser-free
            before treating a dead page as a failed attempt. Returns True if
            the sign-in reached a terminal answer (status already set)."""
            logger.info('[mvpd-login] page gone (%s) — polling for server-side completion', reason)
            grace_deadline = min(time.monotonic() + 30, deadline)
            while time.monotonic() < grace_deadline:
                if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
                    return False
                try:
                    client.fetch_session_token()
                except TVEPendingAuthError:
                    time.sleep(2)
                    continue
                except TVEAuthError as exc:
                    _step(requestor_id, 'failed', str(exc)[:120])
                    set_status('error', f'Adobe Pass error: {exc}')
                    return True  # definitive answer — nothing to relaunch for
                _save_mvpd_authn_token(requestor_id, client.ctx.authn_token)
                if mso_id == 'Comcast_SSO':
                    _harvest_and_save_xfinity_cookies(context)
                results: dict[str, tuple[bool, str]] = {}
                try:
                    client.authorize()
                    results[requestor_id] = (True, 'authorized')
                    _step(requestor_id, 'done', 'authorized')
                    logger.info('[mvpd-login] paired requestor_id=%s (completed after the page closed itself)', requestor_id)
                except TVENotAuthorizedError:
                    results[requestor_id] = (False, 'not entitled')
                    _step(requestor_id, 'failed', 'not entitled')
                except TVEAuthError as exc:
                    results[requestor_id] = (False, str(exc)[:120])
                    _step(requestor_id, 'failed', str(exc)[:120])
                set_status('success', _summarize_pairing_results(results))
                return True
            return False

        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            _step(requestor_id, 'failed', 'Camoufox not installed')
            set_status('error', 'Camoufox is not installed on this container')
            return

        profile_dir = '/data/browser_profiles/mvpd_tve'
        try:
            import os as _os_login
            _os_login.makedirs(profile_dir, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[mvpd-login] could not create profile dir %s: %s', profile_dir, exc)

        try:
            # Same persistent-profile Camoufox setup as run_sling_browser_login
            # (see its comments for why: real, non-headless Firefox behind a
            # virtual display so WebGL/fingerprinting stays intact, and a
            # persistent profile so the MSO's session cookies survive across
            # runs — which is what lets sibling requestor_ids pair silently
            # afterward without a human involved again).
            with Camoufox(
                headless='virtual',
                os='windows',
                persistent_context=True,
                user_data_dir=profile_dir,
                window=(1280, 800),
            ) as context:
                page = context.pages[0] if context.pages else context.new_page()
                page.on('crash', lambda p: logger.warning('[mvpd-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
                page.on('close', lambda p: logger.warning('[mvpd-login] page CLOSE event fired'))
                page.on('pageerror', lambda exc: logger.warning('[mvpd-login] page JS error: %s', str(exc)[:500]))
                try:
                    if mso_id == 'Comcast_SSO':
                        # Xfinity's WAF (Akamai) flatly denies a cold top-level
                        # navigation to the Adobe Pass authenticate/saml URL —
                        # no Referer, sec-fetch-site: none — with an HTTP 403
                        # "Access Denied" page (server: AkamaiGHost). Confirmed
                        # live 2026-08-14 via both a scripted curl_cffi request
                        # and a real headful browser pasting the URL directly;
                        # neither is a bot-fingerprint issue (the plain-paste
                        # block hit even with a fully genuine browser). Landing
                        # on any real page first and redirecting via in-page JS
                        # (so the request carries a real Referer/Sec-Fetch-Site
                        # chain) sails through to the actual login form instead
                        # — verified end-to-end: real login + Adobe authorize +
                        # shortAuthorize + a live /play redirect to a real CDN
                        # URL. redirect_url's own origin is used as the landing
                        # page since it's guaranteed reachable for any
                        # requestor_id without needing extra per-network config.
                        origin = f'{_urlsplit(redirect_url).scheme}://{_urlsplit(redirect_url).netloc}'
                        page.goto(origin, wait_until='domcontentloaded', timeout=30000)
                        set_status('running', 'Loading sign-in page…', page.url)
                        # This whole navigation (landing page + fixed settle +
                        # in-page redirect + load-state wait) used to be one
                        # long blocking stretch with zero relay calls in it —
                        # the modal sat completely blank for up to ~30s
                        # (reported live 2026-08-14: "I don't see any
                        # screenshots"). Relay every ~1s throughout instead.
                        _settle_deadline = time.monotonic() + 3.0
                        while time.monotonic() < _settle_deadline:
                            _relay_input_and_screenshot(page, r)
                            page.wait_for_timeout(500)
                        page.evaluate('(u) => { window.location.href = u; }', auth_url)
                        _load_deadline = time.monotonic() + 30.0
                        while time.monotonic() < _load_deadline:
                            try:
                                page.wait_for_load_state('domcontentloaded', timeout=1000)
                                break
                            except Exception:  # noqa: BLE001
                                pass
                            _relay_input_and_screenshot(page, r)
                    else:
                        page.goto(auth_url, wait_until='domcontentloaded', timeout=30000)
                except Exception as exc:  # noqa: BLE001
                    if _is_browser_death(exc):
                        raise
                    _step(requestor_id, 'failed', 'failed to load sign-in page')
                    set_status('error', f'Failed to load provider sign-in page: {exc}')
                    return
                # If Adobe doesn't have this MVPD registered for this content
                # owner at all (confirmed live for Turner/TNT+Sling: a single
                # 302 straight back, never even touching Sling's login), the
                # very FIRST landing page is redirect_url itself — no MSO
                # domain was ever visited. Left undetected, the human just
                # stares at a dead, unexplained page for up to 30 minutes
                # (confirmed live 2026-08-05).
                #
                # But that landing is NOT unambiguous — this browser profile
                # is reused across every pairing run (persistent_context=True,
                # same user_data_dir below), so a warm Adobe SSO cookie left
                # over from an earlier successful pairing can bind the
                # regcode server-side before the browser ever touches the
                # MSO's domain, landing here on real success too. Give
                # fetch_session_token() (via _grace_poll_pairing, same helper
                # used when the MSO completion page closes itself) a real
                # chance to find a bound session before concluding this
                # network truly isn't participating — confirmed live
                # 2026-08-06: truTV reported "not a participating provider"
                # via this exact bounce right after TNT/TBS had just warmed
                # the same profile's Adobe session, even though Cox is
                # genuinely a listed truTV MVPD (a cold, cookie-free
                # `requests` session redirects cleanly to Cox's real login).
                if _same_page_url(_safe_page_url(page), redirect_url):
                    if _grace_poll_pairing('landed directly on redirect_url'):
                        return
                    _step(requestor_id, 'failed', 'not a participating provider')
                    set_status('error', f'{requestor_id}: {mso_id} does not appear to be a participating provider for this network.')
                    return
                if mvpd_username and mvpd_password:
                    if mso_id == 'Comcast_SSO':
                        _autofill_xfinity_credentials(page, mvpd_username, mvpd_password, r=r)
                    else:
                        _try_autofill_credentials(page, mvpd_username, mvpd_password, r=r)
                set_status('running', 'Sign in below, including any captcha if shown.', page.url)

                last_shot = 0.0
                last_heartbeat = 0.0
                last_poll = 0.0
                consecutive_failures = 0
                f5_retried = False
                current_job = get_current_job()
                _MAX_CONSECUTIVE_FAILURES = 15
                while time.monotonic() < deadline:
                    if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
                        _step(requestor_id, 'failed', 'cancelled')
                        set_status('stopped', 'Cancelled')
                        return

                    if page.is_closed():
                        if _grace_poll_pairing('page closed'):
                            return
                        if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
                            _step(requestor_id, 'failed', 'cancelled')
                            set_status('stopped', 'Cancelled')
                            return
                        raise _BrowserSessionDied('browser page closed and sign-in did not complete')

                    for _ in range(20):
                        raw = r.lpop(MVPD_BROWSER_LOGIN_INPUT_KEY)
                        if raw is None:
                            break
                        try:
                            _apply_sling_browser_login_input(page, _json_login.loads(raw))
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[mvpd-login] input apply failed: %s', exc)

                    now = time.monotonic()
                    if current_job and now - last_heartbeat > 60:
                        try:
                            current_job.heartbeat(datetime.now(timezone.utc), 180)
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[mvpd-login] self-heartbeat failed: %s', exc)
                        last_heartbeat = now

                    if now - last_shot > 0.25:
                        try:
                            shot = page.screenshot(type='jpeg', quality=60)
                            r.setex(MVPD_BROWSER_LOGIN_SHOT_KEY, 30, shot)
                            set_status('running', 'Sign in below, including any captcha if shown.', _safe_page_url(page))
                            consecutive_failures = 0
                        except Exception as exc:  # noqa: BLE001
                            consecutive_failures += 1
                            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                                logger.warning('[mvpd-login] page unresponsive after %d failed screenshots: %s', consecutive_failures, exc)
                                raise _BrowserSessionDied(f'page stopped answering screenshots: {exc}')
                        last_shot = now

                    if now - last_poll > _MVPD_SESSION_POLL_SECONDS:
                        last_poll = now
                        if not f5_retried and _sling_f5_recover(page, auth_url, mvpd_username, mvpd_password, r=r):
                            f5_retried = True
                            continue
                        try:
                            client.fetch_session_token()
                            # authn_token proves the MSO login itself succeeded — save it
                            # now, independent of whether authorize() below finds THIS
                            # requestor_id entitled. It's still reusable for any other
                            # requestor_id under the same MSO account (see authorize_mvpd()
                            # in app/tve/adobe_pass.py).
                            _save_mvpd_authn_token(requestor_id, client.ctx.authn_token)
                            if mso_id == 'Comcast_SSO':
                                _harvest_and_save_xfinity_cookies(context)
                        except TVEPendingAuthError:
                            continue  # human hasn't finished the MSO login yet
                        except TVEAuthError as exc:
                            _step(requestor_id, 'failed', str(exc)[:120])
                            set_status('error', f'Adobe Pass error: {exc}')
                            return

                        # Login itself succeeded (we have a real authn_token) the
                        # moment fetch_session_token() stops raising.
                        results: dict[str, tuple[bool, str]] = {}
                        try:
                            token = client.authorize()
                            results[requestor_id] = (True, 'authorized')
                            _step(requestor_id, 'done', 'authorized')
                            logger.info('[mvpd-login] paired requestor_id=%s (token len=%d)', requestor_id, len(token or ''))
                        except TVENotAuthorizedError:
                            results[requestor_id] = (False, 'not entitled')
                            _step(requestor_id, 'failed', 'not entitled')
                        except TVEAuthError as exc:
                            results[requestor_id] = (False, str(exc)[:120])
                            _step(requestor_id, 'failed', str(exc)[:120])
                            logger.warning('[mvpd-login] %s authorize failed: %s', requestor_id, exc)

                        set_status('success', _summarize_pairing_results(results))
                        return

                    page.wait_for_timeout(80)

                _step(requestor_id, 'failed', 'timed out')
                set_status('error', 'Timed out waiting for sign-in to complete.')
                return
        except BaseException as exc:  # noqa: BLE001
            if _terminal_status_set['v']:
                # A real success/error/stopped status was already recorded
                # before this — almost certainly the Camoufox `with` block's
                # own teardown failing to close an already-dead browser, not
                # an actual job failure. Don't clobber the real result.
                logger.info('[mvpd-login] ignoring cleanup-time exception after terminal status was already set: %s', exc)
                return
            if _is_browser_death(exc) and _grace_poll_pairing(str(exc)[:80]):
                return
            if (_is_browser_death(exc) and _attempt < _BROWSER_LOGIN_MAX_ATTEMPTS
                    and time.monotonic() < deadline - 30
                    and not r.exists(MVPD_BROWSER_LOGIN_STOP_KEY)):
                logger.warning('[mvpd-login] browser died (attempt %d/%d), relaunching: %s', _attempt, _BROWSER_LOGIN_MAX_ATTEMPTS, exc)
                set_status('starting', 'Browser hiccuped — relaunching…')
                return run_mvpd_browser_login(requestor_id, resource, software_statement, redirect_url, mso_id, _attempt=_attempt + 1, _deadline=deadline)
            if r.exists(MVPD_BROWSER_LOGIN_STOP_KEY):
                logger.info('[mvpd-login] stopped by request (browser force-killed to interrupt a stuck wait)')
                try:
                    _step(requestor_id, 'failed', 'cancelled')
                    set_status('stopped', 'Cancelled.')
                except Exception:  # noqa: BLE001
                    pass
                return
            logger.exception('[mvpd-login] browser session failed')
            try:
                _step(requestor_id, 'failed', str(exc)[:120])
                set_status('error', f'Browser session failed: {exc}')
            except Exception:  # noqa: BLE001
                pass
            return


NBC_BROWSER_LOGIN_STATUS_KEY = 'nbc-mvpd:browser-login:status'
NBC_BROWSER_LOGIN_SHOT_KEY = 'nbc-mvpd:browser-login:screenshot'
NBC_BROWSER_LOGIN_INPUT_KEY = 'nbc-mvpd:browser-login:input'
NBC_BROWSER_LOGIN_STOP_KEY = 'nbc-mvpd:browser-login:stop'
NBC_BROWSER_LOGIN_HINT_KEY = 'nbc-mvpd:browser-login:hint'
_NBC_BROWSER_LOGIN_TIMEOUT_SECONDS = 1800
_NBC_SESSION_POLL_SECONDS = 2.0


def _save_nbc_mvpd_auth(mso_id: str, access_token: str, device_fingerprint: str) -> None:
    account = TVEAccount.query.filter_by(provider_id='mvpd').first()
    if not account:
        return
    cfg = dict(account.config or {})
    cfg['nbc_mvpd_auth'] = {
        'mso_id': mso_id,
        'access_token': access_token,
        'device_fingerprint': device_fingerprint,
        'captured_at': int(time.time()),
    }
    account.config = cfg
    db.session.commit()


def run_nbc_browser_login(mso_id: str, _attempt: int = 1, _deadline: float | None = None):
    """Drive a real, human-operated sign-in for NBC TVE's Adobe Pass v2 flow.

    Same "second screen" idea as run_mvpd_browser_login, adapted to nbc.com's
    JSON REST Adobe Pass v2 API (app/scrapers/nbc_tve.py's AdobePassV2Client)
    instead of the legacy XML protocol — different endpoints (POST /sessions
    instead of regcode, GET /profiles/<mvpd> instead of /adobe-services/session),
    but the same underlying design: client registration + session creation are
    scripted (never blocked), a real browser completes the MVPD's own login
    page, and this process polls /profiles/<mvpd> independently using the same
    access_token + device fingerprint — confirmed live (2026-08-05) that this
    poll works from an entirely separate HTTP session as long as those two
    values match, so it doesn't matter that the browser and this process are
    different clients.

    Unlike the legacy protocol's authn_token, it's untested how long NBC's v2
    "authenticated" state survives reuse of the same access_token/device
    fingerprint across resolve() calls — cached in TVEAccount.config either
    way; if it stops working, resolve() will surface a clear error and this
    flow needs to be re-run.
    """
    with flask_app.app_context():
        import json as _json_login
        from app.scrapers.nbc_tve import NbcTveScraper, AdobePassV2Client, REQUESTOR_ID, DEFAULT_REDIRECT_URL, ADOBE_BASE as ADOBE_BASE_NBC

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[nbc-mvpd-login] Redis unavailable, aborting: %s', exc)
            return

        # Same teardown-clobber guard as run_mvpd_browser_login: a dead
        # browser's `with` teardown can raise while unwinding a successful
        # return and must not replace the real terminal status.
        _terminal_status_set = {'v': False}

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    NBC_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': url}),
                )
                if state in ('success', 'error', 'stopped'):
                    _terminal_status_set['v'] = True
            except Exception:  # noqa: BLE001
                pass

        r.delete(NBC_BROWSER_LOGIN_STOP_KEY)
        r.delete(NBC_BROWSER_LOGIN_INPUT_KEY)

        if mso_id == 'Cox':
            # Cox's login step is already fully scripted: NbcTveScraper.
            # _ensure_entitled() calls AdobePassV2Client.authorize(), which
            # does the direct login.cox.com/api/v1/authn POST (_cox_saml_login,
            # shared with fox_tve.py) on every entitlement refresh — same
            # pattern as resolve()'s own normal playback path. No browser
            # needed; confirmed live 2026-08-11 (full authorize+preauthorize
            # round trip with the real Cox account, zero Camoufox). Only
            # non-Cox MSOs fall through to the browser-assisted flow below.
            set_status('running', 'Signing in to NBC TVE…')
            source = Source.query.filter_by(name='nbc_tve').first()
            if not source:
                set_status('error', 'NBC TVE source not found.')
                return
            scraper = NbcTveScraper(config=dict(source.config or {}))
            try:
                guide = scraper._fetch_guide()
                if not guide:
                    set_status('error', 'NBC TVE: could not load channel guide.')
                    return
                resource_id = next(iter(guide.values())).resource_id
                # Force a fresh entitlement check — _ensure_entitled() short-
                # circuits on a still-fresh cached decision, which would make
                # a deliberate "Sign in" click silently no-op.
                scraper._update_cache('nbc_entitlements', {})
                scraper._ensure_entitled(resource_id)
            # Deliberately NOT using _cox_login_error_detail() here (unlike
            # the legacy/FOX Cox branches, code review 2026-08-11) —
            # _ensure_entitled()'s own exceptions already carry full context
            # ("NBC TVE: <mso_id> is not authorized: <reason>"), so running
            # them through that classifier too would double up the framing
            # instead of clarifying it. See that function's docstring.
            except (TVENotAuthorizedError, TVEAuthError) as exc:
                persist_source_config_updates(source.id, scraper._pending_config_updates)
                persist_source_cache_updates(source.id, scraper._pending_cache_updates)
                _record_tve_login_error('nbc', str(exc))
                set_status('error', f'NBC TVE: {exc}')
                return
            except Exception as exc:  # noqa: BLE001
                logger.exception('[nbc-mvpd-login] unexpected failure')
                _record_tve_login_error('nbc', str(exc))
                set_status('error', f'NBC TVE: {exc}')
                return
            persist_source_config_updates(source.id, scraper._pending_config_updates)
            persist_source_cache_updates(source.id, scraper._pending_cache_updates)
            set_status('success', 'Signed in — NBC TVE authorized.')
            logger.info('[nbc-mvpd-login] paired mso_id=Cox (scripted, no browser)')
            return

        if mso_id == 'Comcast_SSO':
            # Same idea as the Cox branch above, but via a saved cookie jar
            # (harvested from a previous successful Comcast_SSO browser
            # pairing — see _harvest_and_save_xfinity_cookies and
            # app/tve/adobe_pass.py's xfinity_cookie_jar_login()) instead of
            # a scripted credential POST — Xfinity's own login page blocks
            # scripted credential submission outright (Akamai Bot Manager),
            # confirmed live 2026-08-14, but a jar matured by a real browser
            # gets straight through over plain HTTP. Falls through to the
            # browser-assisted flow below only when there's no jar yet, or
            # the saved one has gone stale (TVEAuthError) — a definitive
            # TVENotAuthorizedError is NOT retried via browser, same as
            # every other MSO fast-path in this file, since a browser login
            # can't change Adobe's actual entitlement decision.
            cookie_jar_account = TVEAccount.query.filter_by(provider_id='mvpd').first()
            cookie_jar = (cookie_jar_account.config or {}).get('xfinity_cookie_jar') if cookie_jar_account else None
            if cookie_jar:
                set_status('running', 'Trying saved sign-in (no browser needed)…')
                source = Source.query.filter_by(name='nbc_tve').first()
                if source:
                    scraper = NbcTveScraper(config=dict(source.config or {}))
                    try:
                        guide = scraper._fetch_guide()
                        if not guide:
                            raise TVEAuthError('could not load channel guide')
                        resource_id = next(iter(guide.values())).resource_id
                        scraper._update_cache('nbc_entitlements', {})
                        scraper._ensure_entitled(resource_id)
                    except TVENotAuthorizedError as exc:
                        persist_source_config_updates(source.id, scraper._pending_config_updates)
                        persist_source_cache_updates(source.id, scraper._pending_cache_updates)
                        _record_tve_login_error('nbc', str(exc))
                        set_status('error', f'NBC TVE: {exc}')
                        return
                    except TVEAuthError as exc:
                        persist_source_config_updates(source.id, scraper._pending_config_updates)
                        persist_source_cache_updates(source.id, scraper._pending_cache_updates)
                        logger.info('[nbc-mvpd-login] saved xfinity cookie jar did not work, falling back to browser: %s', exc)
                    else:
                        persist_source_config_updates(source.id, scraper._pending_config_updates)
                        persist_source_cache_updates(source.id, scraper._pending_cache_updates)
                        set_status('success', 'Signed in — NBC TVE authorized (no browser needed).')
                        logger.info('[nbc-mvpd-login] paired mso_id=Comcast_SSO via saved cookie jar (no browser)')
                        return
            set_status('running', 'No usable saved sign-in — opening a browser…')

        set_status('starting', 'Registering with Adobe Pass…')

        # One deadline shared across browser-crash retries (RQ job_timeout is
        # only ~30s above _NBC_BROWSER_LOGIN_TIMEOUT_SECONDS, so a retry must
        # never reset the clock).
        deadline = _deadline if _deadline is not None else time.monotonic() + _NBC_BROWSER_LOGIN_TIMEOUT_SECONDS

        account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
        mvpd_username = (account_row.username if account_row else '') or ''
        mvpd_password = (account_row.password if account_row else '') or ''

        source = Source.query.filter_by(name='nbc_tve').first()
        scraper = NbcTveScraper(config=dict((source.config if source else {}) or {}))

        try:
            page_config = scraper._discover_page_config()
            device_fingerprint = scraper._ensure_device_fingerprint()
            if source:
                persist_source_config_updates(source.id, scraper._pending_config_updates)
        except Exception as exc:  # noqa: BLE001
            set_status('error', f'Could not discover NBC page config: {exc}')
            return

        client = AdobePassV2Client(REQUESTOR_ID, page_config['software_statement'], DEFAULT_REDIRECT_URL, device_fingerprint)
        try:
            client._register_client()
            r_sessions = client._post(
                f'{ADOBE_BASE_NBC}/api/v2/{client.requestor_id}/sessions',
                data={'mvpd': mso_id, 'redirectUrl': client.redirect_url, 'domainName': 'nbc.com'},
                headers={**client._bearer_headers(), 'Content-Type': 'application/x-www-form-urlencoded'},
            )
            if not r_sessions.ok:
                try:
                    detail = r_sessions.json().get('message') or r_sessions.text[:300]
                except ValueError:
                    detail = r_sessions.text[:300]
                set_status('error', f'Adobe session request failed for MVPD {mso_id}: {detail}')
                return
            auth_path = r_sessions.json().get('url')
            if not auth_path:
                set_status('error', 'Adobe Pass v2: sessions call did not return an authenticate url.')
                return
            r_redirect = client.session.get(
                f'{ADOBE_BASE_NBC}{auth_path}', headers=client._bearer_headers(),
                allow_redirects=False, timeout=20,
            )
            mso_login_url = r_redirect.headers.get('location') or ''
            if not mso_login_url:
                set_status('error', 'Adobe Pass v2: sessions authenticate call did not return an MVPD login redirect.')
                return
        except TVEAuthError as exc:
            set_status('error', f'Adobe Pass registration failed: {exc}')
            return

        def _grace_poll_pairing(reason: str) -> bool:
            """The pairing completes SERVER-side, and the MSO completion page
            (Cox's Okta widget, observed 3-for-3 on 2026-08-06) calls
            window.close() on itself ~1.5s after posting credentials — so the
            browser usually dies at the exact moment the flow is FINISHING.
            Before treating a dead page as a failed attempt (which would
            relaunch and re-submit a real MSO login — a login-storm risk),
            poll for completion browser-free. Returns True if it completed
            (terminal status already set)."""
            logger.info('[nbc-mvpd-login] page gone (%s) — polling for server-side completion', reason)
            grace_deadline = min(time.monotonic() + 30, deadline)
            while time.monotonic() < grace_deadline:
                if r.exists(NBC_BROWSER_LOGIN_STOP_KEY):
                    return False
                try:
                    r_profile = client._get(
                        f'{ADOBE_BASE_NBC}/api/v2/{client.requestor_id}/profiles/{mso_id}',
                        headers=client._bearer_headers(),
                    )
                    profile = ((r_profile.json() or {}).get('profiles') or {}).get(mso_id)
                except Exception:  # noqa: BLE001
                    profile = None
                if profile:
                    _save_nbc_mvpd_auth(mso_id, client.access_token, device_fingerprint)
                    if mso_id == 'Comcast_SSO':
                        _harvest_and_save_xfinity_cookies(context)
                    set_status('success', f'Signed in — NBC TVE authorized via {mso_id}.')
                    logger.info('[nbc-mvpd-login] paired mso_id=%s (completed after the page closed itself)', mso_id)
                    return True
                time.sleep(2)
            return False

        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            set_status('error', 'Camoufox is not installed on this container')
            return

        profile_dir = '/data/browser_profiles/mvpd_tve'
        try:
            import os as _os_login
            _os_login.makedirs(profile_dir, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[nbc-mvpd-login] could not create profile dir %s: %s', profile_dir, exc)

        try:
            with Camoufox(
                headless='virtual',
                os='windows',
                persistent_context=True,
                user_data_dir=profile_dir,
                window=(1280, 800),
            ) as context:
                page = context.pages[0] if context.pages else context.new_page()
                page.on('crash', lambda p: logger.warning('[nbc-mvpd-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
                page.on('close', lambda p: logger.warning('[nbc-mvpd-login] page CLOSE event fired'))
                page.on('pageerror', lambda exc: logger.warning('[nbc-mvpd-login] page JS error: %s', str(exc)[:500]))
                try:
                    if mso_id == 'Comcast_SSO':
                        # Same Akamai cold-navigation wall as the legacy
                        # family's Comcast_SSO path — see
                        # run_mvpd_browser_login's comment on this exact
                        # pattern for the full explanation.
                        origin = f'{_urlsplit(client.redirect_url).scheme}://{_urlsplit(client.redirect_url).netloc}'
                        page.goto(origin, wait_until='domcontentloaded', timeout=30000)
                        _settle_deadline = time.monotonic() + 3.0
                        while time.monotonic() < _settle_deadline:
                            _relay_input_and_screenshot(
                                page, r, stop_key=NBC_BROWSER_LOGIN_STOP_KEY, input_key=NBC_BROWSER_LOGIN_INPUT_KEY,
                                shot_key=NBC_BROWSER_LOGIN_SHOT_KEY, hint_key=NBC_BROWSER_LOGIN_HINT_KEY,
                            )
                            page.wait_for_timeout(500)
                        page.evaluate('(u) => { window.location.href = u; }', mso_login_url)
                        _load_deadline = time.monotonic() + 30.0
                        while time.monotonic() < _load_deadline:
                            try:
                                page.wait_for_load_state('domcontentloaded', timeout=1000)
                                break
                            except Exception:  # noqa: BLE001
                                pass
                            _relay_input_and_screenshot(
                                page, r, stop_key=NBC_BROWSER_LOGIN_STOP_KEY, input_key=NBC_BROWSER_LOGIN_INPUT_KEY,
                                shot_key=NBC_BROWSER_LOGIN_SHOT_KEY, hint_key=NBC_BROWSER_LOGIN_HINT_KEY,
                            )
                    else:
                        page.goto(mso_login_url, wait_until='domcontentloaded', timeout=30000)
                except Exception as exc:  # noqa: BLE001
                    if _is_browser_death(exc):
                        raise
                    set_status('error', f'Failed to load provider sign-in page: {exc}')
                    return
                if _same_page_url(_safe_page_url(page), client.redirect_url):
                    set_status('error', f'{mso_id} does not appear to be a participating provider for NBC TVE.')
                    return
                if mvpd_username and mvpd_password:
                    if mso_id == 'Comcast_SSO':
                        _autofill_xfinity_credentials(
                            page, mvpd_username, mvpd_password, r=r,
                            stop_key=NBC_BROWSER_LOGIN_STOP_KEY, input_key=NBC_BROWSER_LOGIN_INPUT_KEY,
                            shot_key=NBC_BROWSER_LOGIN_SHOT_KEY, hint_key=NBC_BROWSER_LOGIN_HINT_KEY,
                        )
                    else:
                        _try_autofill_credentials(
                            page, mvpd_username, mvpd_password, r=r,
                            stop_key=NBC_BROWSER_LOGIN_STOP_KEY, input_key=NBC_BROWSER_LOGIN_INPUT_KEY,
                            shot_key=NBC_BROWSER_LOGIN_SHOT_KEY, hint_key=NBC_BROWSER_LOGIN_HINT_KEY,
                        )
                set_status('running', 'Sign in below, including any captcha if shown.', page.url)

                last_shot = 0.0
                last_heartbeat = 0.0
                last_poll = 0.0
                consecutive_failures = 0
                f5_retried = False
                current_job = get_current_job()
                _MAX_CONSECUTIVE_FAILURES = 15
                while time.monotonic() < deadline:
                    if r.exists(NBC_BROWSER_LOGIN_STOP_KEY):
                        set_status('stopped', 'Cancelled')
                        return
                    if page.is_closed():
                        if _grace_poll_pairing('page closed'):
                            return
                        if r.exists(NBC_BROWSER_LOGIN_STOP_KEY):
                            set_status('stopped', 'Cancelled')
                            return
                        raise _BrowserSessionDied('browser page closed and pairing did not complete')

                    for _ in range(20):
                        raw = r.lpop(NBC_BROWSER_LOGIN_INPUT_KEY)
                        if raw is None:
                            break
                        try:
                            _apply_sling_browser_login_input(page, _json_login.loads(raw))
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[nbc-mvpd-login] input apply failed: %s', exc)

                    now = time.monotonic()
                    if current_job and now - last_heartbeat > 60:
                        try:
                            current_job.heartbeat(datetime.now(timezone.utc), 180)
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[nbc-mvpd-login] self-heartbeat failed: %s', exc)
                        last_heartbeat = now

                    if now - last_shot > 0.25:
                        try:
                            shot = page.screenshot(type='jpeg', quality=60)
                            r.setex(NBC_BROWSER_LOGIN_SHOT_KEY, 30, shot)
                            set_status('running', 'Sign in below, including any captcha if shown.', _safe_page_url(page))
                            consecutive_failures = 0
                        except Exception as exc:  # noqa: BLE001
                            consecutive_failures += 1
                            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                                logger.warning('[nbc-mvpd-login] page unresponsive after %d failed screenshots: %s', consecutive_failures, exc)
                                raise _BrowserSessionDied(f'page stopped answering screenshots: {exc}')
                        last_shot = now

                    if now - last_poll > _NBC_SESSION_POLL_SECONDS:
                        last_poll = now
                        if not f5_retried and _sling_f5_recover(
                            page, mso_login_url, mvpd_username, mvpd_password, r=r,
                            stop_key=NBC_BROWSER_LOGIN_STOP_KEY, input_key=NBC_BROWSER_LOGIN_INPUT_KEY,
                            shot_key=NBC_BROWSER_LOGIN_SHOT_KEY, hint_key=NBC_BROWSER_LOGIN_HINT_KEY,
                        ):
                            f5_retried = True
                            continue
                        try:
                            r_profile = client._get(
                                f'{ADOBE_BASE_NBC}/api/v2/{client.requestor_id}/profiles/{mso_id}',
                                headers=client._bearer_headers(),
                            )
                            profile = ((r_profile.json() or {}).get('profiles') or {}).get(mso_id)
                        except TVEAuthError as exc:
                            set_status('error', f'Adobe Pass error: {exc}')
                            return
                        if not profile:
                            continue  # human hasn't finished the MSO login yet
                        _save_nbc_mvpd_auth(mso_id, client.access_token, device_fingerprint)
                        if mso_id == 'Comcast_SSO':
                            _harvest_and_save_xfinity_cookies(context)
                        set_status('success', f'Signed in — NBC TVE authorized via {mso_id}.')
                        logger.info('[nbc-mvpd-login] paired mso_id=%s', mso_id)
                        return

                    page.wait_for_timeout(80)

                set_status('error', 'Timed out waiting for sign-in to complete.')
                return
        except BaseException as exc:  # noqa: BLE001
            if _terminal_status_set['v']:
                logger.info('[nbc-mvpd-login] ignoring cleanup-time exception after terminal status was already set: %s', exc)
                return
            if _is_browser_death(exc) and _grace_poll_pairing(str(exc)[:80]):
                return
            if (_is_browser_death(exc) and _attempt < _BROWSER_LOGIN_MAX_ATTEMPTS
                    and time.monotonic() < deadline - 30
                    and not r.exists(NBC_BROWSER_LOGIN_STOP_KEY)):
                logger.warning('[nbc-mvpd-login] browser died (attempt %d/%d), relaunching: %s', _attempt, _BROWSER_LOGIN_MAX_ATTEMPTS, exc)
                set_status('starting', 'Browser hiccuped — relaunching…')
                return run_nbc_browser_login(mso_id, _attempt=_attempt + 1, _deadline=deadline)
            if r.exists(NBC_BROWSER_LOGIN_STOP_KEY):
                logger.info('[nbc-mvpd-login] stopped by request (browser force-killed to interrupt a stuck wait)')
                try:
                    set_status('stopped', 'Cancelled.')
                except Exception:  # noqa: BLE001
                    pass
                return
            logger.exception('[nbc-mvpd-login] browser session failed')
            try:
                set_status('error', f'Browser session failed: {exc}')
            except Exception:  # noqa: BLE001
                pass
            return


FOX_BROWSER_LOGIN_STATUS_KEY = 'fox-mvpd:browser-login:status'
FOX_BROWSER_LOGIN_SHOT_KEY = 'fox-mvpd:browser-login:screenshot'
FOX_BROWSER_LOGIN_INPUT_KEY = 'fox-mvpd:browser-login:input'
FOX_BROWSER_LOGIN_STOP_KEY = 'fox-mvpd:browser-login:stop'
FOX_BROWSER_LOGIN_HINT_KEY = 'fox-mvpd:browser-login:hint'
_FOX_BROWSER_LOGIN_TIMEOUT_SECONDS = 1800
_FOX_SESSION_POLL_SECONDS = 2.0


def run_fox_browser_login(mso_id: str, _attempt: int = 1, _deadline: float | None = None):
    """Drive a real, human-operated sign-in for FOX Sports TVE's Adobe Pass flow.

    Same "second screen" idea as run_mvpd_browser_login/run_nbc_browser_login,
    adapted to fox.com's own api3.fox.com REST flow (app/scrapers/fox_tve.py's
    _fox_sports_mvpd_token) — POST /accountregcode/v2 + /mvpdlogin (scripted,
    never blocked) instead of the legacy protocol's regcode, GET
    /checkadobeauthn/v2 instead of /adobe-services/session or /profiles/<mvpd>.
    Confirmed live (2026-08-05) that /checkadobeauthn/v2 returns 404 "Token Not
    Found" pre-completion and works identically from a separate HTTP session as
    long as the anon access_token + device_id match, so cross-client polling
    works the same way here too. On success, saves directly into the SAME
    account config keys _fox_sports_access_token() already checks
    (fox_sports_access_token/_exp/_mso), so no extra wiring is needed there.
    """
    with flask_app.app_context():
        import json as _json_login
        import requests
        from app.scrapers.fox_tve import _fox_json_headers, _jwt_exp

        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            r.ping()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[fox-mvpd-login] Redis unavailable, aborting: %s', exc)
            return

        # Same teardown-clobber guard as run_mvpd_browser_login: a dead
        # browser's `with` teardown can raise while unwinding a successful
        # return and must not replace the real terminal status.
        _terminal_status_set = {'v': False}

        def set_status(state: str, message: str = '', url: str = ''):
            try:
                r.setex(
                    FOX_BROWSER_LOGIN_STATUS_KEY, 120,
                    _json_login.dumps({'state': state, 'message': message, 'url': url}),
                )
                if state in ('success', 'error', 'stopped'):
                    _terminal_status_set['v'] = True
            except Exception:  # noqa: BLE001
                pass

        r.delete(FOX_BROWSER_LOGIN_STOP_KEY)
        r.delete(FOX_BROWSER_LOGIN_INPUT_KEY)

        if mso_id == 'Cox':
            # Cox's login step is already fully scripted via
            # fox_tve._fox_sports_mvpd_token() (the direct login.cox.com/
            # api/v1/authn POST, same _cox_saml_login used elsewhere). No
            # browser needed; confirmed live 2026-08-11. Only non-Cox MSOs
            # fall through to the browser-assisted flow below.
            #
            # Calls _fox_sports_mvpd_token() directly rather than going
            # through _fox_sports_access_token()'s cache-first path — same
            # reasoning as foxone_signin()'s own docstring: a "Sign in"
            # click should always exercise a live Cox login, not silently
            # return a still-valid cached token untested. That cache-first
            # wrapper also swallows its own exceptions and falls back to an
            # anonymous preview token instead of raising (the right call at
            # play time, wrong for this button — code review, 2026-08-11:
            # this button was reading the account-wide last_auth_status
            # afterward instead of the actual outcome, which a DIFFERENT
            # network's more recent attempt could have overwritten, and the
            # cache-hit path never touched that field at all).
            set_status('running', 'Signing in to FOX TVE…')
            import uuid as _uuid
            from app.scrapers.fox_tve import _fox_sports_mvpd_token
            account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
            if not account_row or not account_row.is_enabled or not account_row.has_credentials():
                set_status('error', 'TVE credentials are not configured in Settings.')
                return
            fox_session = requests.Session()
            try:
                token = _fox_sports_mvpd_token(fox_session, str(_uuid.uuid4()), mso_id, account_row.username or '', account_row.password or '')
            except Exception as exc:  # noqa: BLE001
                detail = _cox_login_error_detail(exc, 'FOX TVE')
                message = f'FOX Sports {mso_id} auth failed: {detail}'
                account_row.last_auth_status = 'error'
                account_row.last_auth_message = message[:500]
                account_row.last_auth_at = datetime.now(timezone.utc)
                db.session.commit()
                _record_tve_login_error('fox', detail)
                set_status('error', f'FOX TVE: {detail}')
                return
            now = int(time.time())
            cfg = dict(account_row.config or {})
            cfg['fox_sports_access_token'] = token
            cfg['fox_sports_access_token_exp'] = _jwt_exp(token) or (now + 3600)
            cfg['fox_sports_access_token_mso'] = mso_id
            cfg['fox_sports_access_token_captured_at'] = now
            account_row.config = cfg
            account_row.last_auth_status = 'ok'
            account_row.last_auth_message = f'FOX Sports MVPD token obtained through {mso_id}.'
            account_row.last_auth_at = datetime.now(timezone.utc)
            db.session.commit()
            set_status('success', 'Signed in — FOX TVE authorized.')
            logger.info('[fox-mvpd-login] paired mso_id=Cox (scripted, no browser)')
            return

        if mso_id == 'Comcast_SSO':
            # Same idea as the Cox branch above, but via a saved cookie jar
            # instead of a scripted credential POST — see
            # run_nbc_browser_login's identical block for the full
            # reasoning. Falls through to the browser-assisted flow below
            # only when there's no jar yet or the saved one has gone stale.
            cookie_jar_account = TVEAccount.query.filter_by(provider_id='mvpd').first()
            cookie_jar = (cookie_jar_account.config or {}).get('xfinity_cookie_jar') if cookie_jar_account else None
            if cookie_jar and cookie_jar_account and cookie_jar_account.is_enabled and cookie_jar_account.has_credentials():
                set_status('running', 'Trying saved sign-in (no browser needed)…')
                import uuid as _uuid
                from app.scrapers.fox_tve import _fox_sports_mvpd_token
                fox_session = requests.Session()
                try:
                    token = _fox_sports_mvpd_token(
                        fox_session, str(_uuid.uuid4()), mso_id,
                        cookie_jar_account.username or '', cookie_jar_account.password or '',
                        cookie_jar=cookie_jar,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.info('[fox-mvpd-login] saved xfinity cookie jar did not work, falling back to browser: %s', exc)
                else:
                    now = int(time.time())
                    cfg = dict(cookie_jar_account.config or {})
                    cfg['fox_sports_access_token'] = token
                    cfg['fox_sports_access_token_exp'] = _jwt_exp(token) or (now + 3600)
                    cfg['fox_sports_access_token_mso'] = mso_id
                    cfg['fox_sports_access_token_captured_at'] = now
                    cookie_jar_account.config = cfg
                    cookie_jar_account.last_auth_status = 'ok'
                    cookie_jar_account.last_auth_message = f'FOX Sports MVPD token obtained through {mso_id} (no browser needed).'
                    cookie_jar_account.last_auth_at = datetime.now(timezone.utc)
                    db.session.commit()
                    set_status('success', 'Signed in — FOX TVE authorized (no browser needed).')
                    logger.info('[fox-mvpd-login] paired mso_id=Comcast_SSO via saved cookie jar (no browser)')
                    return
            set_status('running', 'No usable saved sign-in — opening a browser…')

        set_status('starting', 'Registering with FOX…')

        # One deadline shared across browser-crash retries (RQ job_timeout is
        # only ~30s above _FOX_BROWSER_LOGIN_TIMEOUT_SECONDS, so a retry must
        # never reset the clock).
        deadline = _deadline if _deadline is not None else time.monotonic() + _FOX_BROWSER_LOGIN_TIMEOUT_SECONDS

        account_row = TVEAccount.query.filter_by(provider_id='mvpd').first()
        mvpd_username = (account_row.username if account_row else '') or ''
        mvpd_password = (account_row.password if account_row else '') or ''

        fox_redirect_url = 'https://www.foxsports.com/live/fs1'
        import uuid as _uuid
        session = requests.Session()
        device_id = str(_uuid.uuid4())
        try:
            anon = session.post('https://api3.fox.com/v2.0/login', headers=_fox_json_headers(), json={'deviceId': device_id}, timeout=30)
            anon.raise_for_status()
            anon_token = anon.json()['accessToken']
            headers = _fox_json_headers(anon_token)

            reg = session.post(
                'https://api3.fox.com/v2.0/accountregcode/v2', headers=headers,
                json={'deviceId': device_id, 'isRegister': False, 'isMvpd': True, 'selectedMvpdId': mso_id},
                timeout=30,
            )
            reg.raise_for_status()
            code = reg.json()['code']

            mvpd = session.post(
                f'https://api3.fox.com/v2.0/accountregcode/{code}/mvpdlogin', headers=headers,
                json={'mvpdId': mso_id, 'redirectUrl': fox_redirect_url},
                timeout=30,
            )
            mvpd.raise_for_status()
            auth_url = mvpd.json()['authenticateUrl']

            r_redirect = session.get(auth_url, headers={'Accept': 'text/html,application/json'}, allow_redirects=False, timeout=30)
            mso_login_url = r_redirect.headers.get('location') or ''
            if not mso_login_url:
                set_status('error', 'FOX Adobe authenticate call did not return an MVPD login redirect.')
                return
        except requests.RequestException as exc:
            set_status('error', f'FOX registration failed: {exc}')
            return
        except (KeyError, ValueError) as exc:
            set_status('error', f'FOX registration returned an unexpected response: {exc}')
            return

        def _grace_poll_pairing(reason: str) -> bool:
            """Same as run_nbc_browser_login's helper: the MSO completion page
            (Cox's Okta widget) closes itself right after the credentials POST
            while the pairing completes server-side — poll browser-free before
            treating a dead page as a failed attempt. Returns True if it
            completed (terminal status already set)."""
            logger.info('[fox-mvpd-login] page gone (%s) — polling for server-side completion', reason)
            grace_deadline = min(time.monotonic() + 30, deadline)
            while time.monotonic() < grace_deadline:
                if r.exists(FOX_BROWSER_LOGIN_STOP_KEY):
                    return False
                token = None
                try:
                    check = session.get(
                        'https://api3.fox.com/v2.0/checkadobeauthn/v2', headers=headers,
                        params={'device_id': device_id, 'requestor': 'fbc-fox'},
                        timeout=30,
                    )
                    if check.ok:
                        token = (check.json() or {}).get('accessToken')
                except requests.RequestException:
                    token = None
                if token:
                    exp = _jwt_exp(token) or int(time.time()) + 3600
                    account = TVEAccount.query.filter_by(provider_id='mvpd').first()
                    if account:
                        acct_cfg = dict(account.config or {})
                        acct_cfg['fox_sports_access_token'] = token
                        acct_cfg['fox_sports_access_token_exp'] = exp
                        acct_cfg['fox_sports_access_token_mso'] = mso_id
                        acct_cfg['fox_sports_access_token_captured_at'] = int(time.time())
                        account.config = acct_cfg
                        account.last_auth_status = 'ok'
                        account.last_auth_message = f'FOX Sports MVPD token obtained through {mso_id} (browser-assisted).'
                        account.last_auth_at = datetime.now(timezone.utc)
                        db.session.commit()
                    if mso_id == 'Comcast_SSO':
                        _harvest_and_save_xfinity_cookies(context)
                    set_status('success', f'Signed in — FOX Sports authorized via {mso_id}.')
                    logger.info('[fox-mvpd-login] paired mso_id=%s (completed after the page closed itself)', mso_id)
                    return True
                time.sleep(2)
            return False

        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            set_status('error', 'Camoufox is not installed on this container')
            return

        profile_dir = '/data/browser_profiles/mvpd_tve'
        try:
            import os as _os_login
            _os_login.makedirs(profile_dir, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[fox-mvpd-login] could not create profile dir %s: %s', profile_dir, exc)

        try:
            with Camoufox(
                headless='virtual',
                os='windows',
                persistent_context=True,
                user_data_dir=profile_dir,
                window=(1280, 800),
            ) as context:
                page = context.pages[0] if context.pages else context.new_page()
                page.on('crash', lambda p: logger.warning('[fox-mvpd-login] page CRASH event fired (url was %s)', _safe_page_url(p)))
                page.on('close', lambda p: logger.warning('[fox-mvpd-login] page CLOSE event fired'))
                page.on('pageerror', lambda exc: logger.warning('[fox-mvpd-login] page JS error: %s', str(exc)[:500]))
                try:
                    if mso_id == 'Comcast_SSO':
                        # Same Akamai cold-navigation wall as the legacy/NBC
                        # Comcast_SSO paths — see run_mvpd_browser_login's
                        # comment on this exact pattern for the full
                        # explanation.
                        origin = f'{_urlsplit(fox_redirect_url).scheme}://{_urlsplit(fox_redirect_url).netloc}'
                        page.goto(origin, wait_until='domcontentloaded', timeout=30000)
                        _settle_deadline = time.monotonic() + 3.0
                        while time.monotonic() < _settle_deadline:
                            _relay_input_and_screenshot(
                                page, r, stop_key=FOX_BROWSER_LOGIN_STOP_KEY, input_key=FOX_BROWSER_LOGIN_INPUT_KEY,
                                shot_key=FOX_BROWSER_LOGIN_SHOT_KEY, hint_key=FOX_BROWSER_LOGIN_HINT_KEY,
                            )
                            page.wait_for_timeout(500)
                        page.evaluate('(u) => { window.location.href = u; }', mso_login_url)
                        _load_deadline = time.monotonic() + 30.0
                        while time.monotonic() < _load_deadline:
                            try:
                                page.wait_for_load_state('domcontentloaded', timeout=1000)
                                break
                            except Exception:  # noqa: BLE001
                                pass
                            _relay_input_and_screenshot(
                                page, r, stop_key=FOX_BROWSER_LOGIN_STOP_KEY, input_key=FOX_BROWSER_LOGIN_INPUT_KEY,
                                shot_key=FOX_BROWSER_LOGIN_SHOT_KEY, hint_key=FOX_BROWSER_LOGIN_HINT_KEY,
                            )
                    else:
                        page.goto(mso_login_url, wait_until='domcontentloaded', timeout=30000)
                except Exception as exc:  # noqa: BLE001
                    if _is_browser_death(exc):
                        raise
                    set_status('error', f'Failed to load provider sign-in page: {exc}')
                    return
                if _same_page_url(_safe_page_url(page), fox_redirect_url):
                    set_status('error', f'{mso_id} does not appear to be a participating provider for FOX TVE.')
                    return
                if mvpd_username and mvpd_password:
                    if mso_id == 'Comcast_SSO':
                        _autofill_xfinity_credentials(
                            page, mvpd_username, mvpd_password, r=r,
                            stop_key=FOX_BROWSER_LOGIN_STOP_KEY, input_key=FOX_BROWSER_LOGIN_INPUT_KEY,
                            shot_key=FOX_BROWSER_LOGIN_SHOT_KEY, hint_key=FOX_BROWSER_LOGIN_HINT_KEY,
                        )
                    else:
                        _try_autofill_credentials(
                            page, mvpd_username, mvpd_password, r=r,
                            stop_key=FOX_BROWSER_LOGIN_STOP_KEY, input_key=FOX_BROWSER_LOGIN_INPUT_KEY,
                            shot_key=FOX_BROWSER_LOGIN_SHOT_KEY, hint_key=FOX_BROWSER_LOGIN_HINT_KEY,
                        )
                set_status('running', 'Sign in below, including any captcha if shown.', page.url)

                last_shot = 0.0
                last_heartbeat = 0.0
                last_poll = 0.0
                consecutive_failures = 0
                f5_retried = False
                current_job = get_current_job()
                _MAX_CONSECUTIVE_FAILURES = 15
                while time.monotonic() < deadline:
                    if r.exists(FOX_BROWSER_LOGIN_STOP_KEY):
                        set_status('stopped', 'Cancelled')
                        return
                    if page.is_closed():
                        if _grace_poll_pairing('page closed'):
                            return
                        if r.exists(FOX_BROWSER_LOGIN_STOP_KEY):
                            set_status('stopped', 'Cancelled')
                            return
                        raise _BrowserSessionDied('browser page closed and pairing did not complete')

                    for _ in range(20):
                        raw = r.lpop(FOX_BROWSER_LOGIN_INPUT_KEY)
                        if raw is None:
                            break
                        try:
                            _apply_sling_browser_login_input(page, _json_login.loads(raw))
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[fox-mvpd-login] input apply failed: %s', exc)

                    now = time.monotonic()
                    if current_job and now - last_heartbeat > 60:
                        try:
                            current_job.heartbeat(datetime.now(timezone.utc), 180)
                        except Exception as exc:  # noqa: BLE001
                            logger.debug('[fox-mvpd-login] self-heartbeat failed: %s', exc)
                        last_heartbeat = now

                    if now - last_shot > 0.25:
                        try:
                            shot = page.screenshot(type='jpeg', quality=60)
                            r.setex(FOX_BROWSER_LOGIN_SHOT_KEY, 30, shot)
                            set_status('running', 'Sign in below, including any captcha if shown.', _safe_page_url(page))
                            consecutive_failures = 0
                        except Exception as exc:  # noqa: BLE001
                            consecutive_failures += 1
                            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                                logger.warning('[fox-mvpd-login] page unresponsive after %d failed screenshots: %s', consecutive_failures, exc)
                                raise _BrowserSessionDied(f'page stopped answering screenshots: {exc}')
                        last_shot = now

                    if now - last_poll > _FOX_SESSION_POLL_SECONDS:
                        last_poll = now
                        if not f5_retried and _sling_f5_recover(
                            page, mso_login_url, mvpd_username, mvpd_password, r=r,
                            stop_key=FOX_BROWSER_LOGIN_STOP_KEY, input_key=FOX_BROWSER_LOGIN_INPUT_KEY,
                            shot_key=FOX_BROWSER_LOGIN_SHOT_KEY, hint_key=FOX_BROWSER_LOGIN_HINT_KEY,
                        ):
                            f5_retried = True
                            continue
                        try:
                            check = session.get(
                                'https://api3.fox.com/v2.0/checkadobeauthn/v2', headers=headers,
                                params={'device_id': device_id, 'requestor': 'fbc-fox'},
                                timeout=30,
                            )
                        except requests.RequestException as exc:
                            set_status('error', f'FOX checkadobeauthn request failed: {exc}')
                            return
                        if check.status_code == 404:
                            continue  # human hasn't finished the MSO login yet
                        if not check.ok:
                            set_status('error', f'FOX checkadobeauthn returned HTTP {check.status_code}: {check.text[:300]}')
                            return
                        token = (check.json() or {}).get('accessToken')
                        if not token:
                            continue
                        exp = _jwt_exp(token) or int(time.time()) + 3600
                        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
                        if account:
                            acct_cfg = dict(account.config or {})
                            acct_cfg['fox_sports_access_token'] = token
                            acct_cfg['fox_sports_access_token_exp'] = exp
                            acct_cfg['fox_sports_access_token_mso'] = mso_id
                            acct_cfg['fox_sports_access_token_captured_at'] = int(time.time())
                            account.config = acct_cfg
                            account.last_auth_status = 'ok'
                            account.last_auth_message = f'FOX Sports MVPD token obtained through {mso_id} (browser-assisted).'
                            account.last_auth_at = datetime.now(timezone.utc)
                            db.session.commit()
                        if mso_id == 'Comcast_SSO':
                            _harvest_and_save_xfinity_cookies(context)
                        set_status('success', f'Signed in — FOX Sports authorized via {mso_id}.')
                        logger.info('[fox-mvpd-login] paired mso_id=%s', mso_id)
                        return

                    page.wait_for_timeout(80)

                set_status('error', 'Timed out waiting for sign-in to complete.')
                return
        except BaseException as exc:  # noqa: BLE001
            if _terminal_status_set['v']:
                logger.info('[fox-mvpd-login] ignoring cleanup-time exception after terminal status was already set: %s', exc)
                return
            if _is_browser_death(exc) and _grace_poll_pairing(str(exc)[:80]):
                return
            if (_is_browser_death(exc) and _attempt < _BROWSER_LOGIN_MAX_ATTEMPTS
                    and time.monotonic() < deadline - 30
                    and not r.exists(FOX_BROWSER_LOGIN_STOP_KEY)):
                logger.warning('[fox-mvpd-login] browser died (attempt %d/%d), relaunching: %s', _attempt, _BROWSER_LOGIN_MAX_ATTEMPTS, exc)
                set_status('starting', 'Browser hiccuped — relaunching…')
                return run_fox_browser_login(mso_id, _attempt=_attempt + 1, _deadline=deadline)
            if r.exists(FOX_BROWSER_LOGIN_STOP_KEY):
                logger.info('[fox-mvpd-login] stopped by request (browser force-killed to interrupt a stuck wait)')
                try:
                    set_status('stopped', 'Cancelled.')
                except Exception:  # noqa: BLE001
                    pass
                return
            logger.exception('[fox-mvpd-login] browser session failed')
            try:
                set_status('error', f'Browser session failed: {exc}')
            except Exception:  # noqa: BLE001
                pass
            return


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
    """Mirror channels that are inherently bridge-only into requires_drm_bridge.

    Most DRM-capable sources only need this for DASH rows. Some premium sources,
    such as DirecTV Stream, are all-DRM even when their manifests are HLS; those
    scrapers advertise all_channels_require_drm_bridge. The flag is gated on the
    global bridge mode and only flips requires_drm_bridge; audit still owns
    disable/dead state."""
    scraper_cls = registry.get(source.name)
    if not (scraper_cls and getattr(scraper_cls, 'license_url', None)):
        return
    want = bool(AppSettings.get().drm_bridge_enabled)
    q = source.channels
    label = 'all'
    if not getattr(scraper_cls, 'all_channels_require_drm_bridge', False):
        q = q.filter(Channel.stream_type == 'dash')
        label = 'DASH'
    changed = 0
    for ch in q.all():
        if bool(ch.requires_drm_bridge) != want:
            ch.requires_drm_bridge = want
            changed += 1
    if changed:
        logger.info('[%s] requires_drm_bridge synced on %d %s channel(s) (bridge=%s)',
                    source.name, changed, label, want)


def _upsert_channels(source, channel_data_list, gracenote_auto_fill: bool = True, active_geos: set | None = None,
                     miss_threshold: int = _CHANNEL_MISS_THRESHOLD, rehome_by_guide_key: bool = False,
                     allow_suspicious_collapse: bool = False, pinned_channel_ids: frozenset = frozenset()):
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
            # Don't resurrect channels the stream audit flagged as Dead, VOD, NotAuthorized, or DRM
            # unless the stream URL changed (source may have fixed the channel).
            _flagged = ch.disable_reason in ('Dead', 'VOD', 'NotAuthorized') or (ch.disable_reason or '').startswith('DRM')
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
            if cd.source_channel_id in pinned_channel_ids and not ch.scrape_pinned:
                ch.scrape_pinned = True  # scraper-declared exemption from channel_miss_threshold
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
                scrape_pinned     = cd.source_channel_id in pinned_channel_ids,
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

    if suspicious_collapse and not allow_suspicious_collapse:
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


def _normalize_episode(season, episode):
    """Strip season-prefixed compound episode codes (S4E01 arriving as 401).

    Several upstreams (Plex, TCL, Tubi, Pluto) send production-code style
    episode numbers with the season prepended: S4E01 → 401, S3E07 → 307.
    Detect via ``episode // 100 == season`` and keep only the episode part.

    Only applied for season >= 2. In season 1 the compound form (S1E18 → 118)
    is mathematically indistinguishable from a genuine high-numbered episode of
    a long single-season show — daily court/talk strips and telenovelas
    routinely run season 1 well past episode 100 — so stripping there silently
    collapsed real episodes (150 → 50) and corrupted EPG/DVR episode identity.
    Leaving season-1 values untouched is the safe choice; the worst case is a
    cosmetic prefix in the displayed episode number. Season-1 rows previously
    mangled self-heal to their genuine value as each scrape window is rewritten.

    Exact multiples of 100 (E00 doesn't exist in the compound scheme) and
    genuine large numbers (S5 E105, S12 E4991) fail the checks and pass through.
    Non-integer season/episode values are returned untouched (never coerced).
    """
    try:
        s = int(season)
        e = int(episode)
    except (TypeError, ValueError):
        return episode
    if s >= 2 and e >= 100 and e // 100 == s and e % 100 != 0:
        return e % 100
    return episode


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
                'episode':       _normalize_episode(pd.season, pd.episode),
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
        default_disabled_sources = {'amazon_prime_free', 'aenetworks_tve', 'fox_tve', 'discovery_tve', 'amcn_tve', 'fox_one', 'nbc_tve', 'warner_tve', 'cox', 'cspan', 'sling', 'localnow', 'pluto', 'frndlytv', 'fubo', 'hdhomerun', 'freecast', 'vidaa', 'distro', 'philo', 'directv', 'pbs'}
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


def _rq_integrity_cleanup(include_orphan_purge: bool = True):
    """RQ job target: delete orphan channels/programs. Runs inside the RQ worker process.

    include_orphan_purge=False is used only by the boot-time immediate enqueue
    (see scheduler startup below) — entrypoint.sh already runs
    purge_orphaned_sources() synchronously before any worker starts, so redoing
    it here on every single boot would just be duplicate scanning/logging. The
    recurring daily job (the default) still runs it, since that's what lets a
    long-lived container that never restarts advance/clear a grace period.
    """
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
    if include_orphan_purge:
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


_YTDLP_MASTER_REQ = 'yt-dlp[default] @ https://github.com/yt-dlp/yt-dlp/archive/master.tar.gz'


def _reload_gunicorn_workers():
    """Gracefully recycle gunicorn's worker processes via SIGHUP.

    yt_dlp is imported lazily (see stream_detector._resolve_youtube) and
    every real caller of it — app.routes.play, app.routes.api — runs inline
    inside a long-lived gunicorn/gevent request worker, never inside an RQ
    job. A worker that already imported yt_dlp keeps that in-memory copy
    for its whole life (up to GUNICORN_MAX_REQUESTS), so a pip upgrade on
    disk alone doesn't reach play-time resolution until a worker happens to
    recycle naturally. SIGHUP tells the arbiter to gracefully replace all
    workers with fresh ones, which pick up the just-installed code on their
    own first (lazy) import.
    """
    import os

    pidfile = '/tmp/gunicorn.pid'
    try:
        with open(pidfile) as f:
            pid = int(f.read().strip())
    except (FileNotFoundError, ValueError) as e:
        logger.warning('[maint] gunicorn pidfile unreadable (%s), skipping worker reload', e)
        return
    try:
        os.kill(pid, signal.SIGHUP)
        logger.info('[maint] sent SIGHUP to gunicorn master (pid %d) to recycle workers', pid)
    except ProcessLookupError:
        logger.warning('[maint] gunicorn master pid %d not found, skipping worker reload', pid)
    except Exception as e:
        logger.error('[maint] failed to signal gunicorn master: %s', e)


def _rq_ytdlp_upgrade():
    """RQ job target: refresh yt-dlp from GitHub master in the running
    container. The Dockerfile installs from master too, but only at image
    build time — YouTube's extractor breaks often enough that a container
    left running for weeks between rebuilds drifts stale.

    subprocess's own timeout is kept well under this job's RQ job_timeout so
    pip's timeout handler (which kills its child on expiry) always fires
    first — if the two timeouts raced, RQ's SIGALRM-based job_timeout could
    interrupt the pip child mid `--force-reinstall` and leave it running
    orphaned, unsupervised, partway through rewriting yt-dlp's package files.
    """
    import subprocess
    from importlib.metadata import version as _pkg_version, PackageNotFoundError

    try:
        old = _pkg_version('yt-dlp')
    except PackageNotFoundError:
        old = 'unknown'

    try:
        result = subprocess.run(
            ['pip', 'install', '--quiet', '--force-reinstall', _YTDLP_MASTER_REQ],
            capture_output=True, text=True, timeout=300,
        )
    except subprocess.TimeoutExpired:
        logger.error('[maint] yt-dlp upgrade timed out after 300s')
        return
    if result.returncode != 0:
        logger.error('[maint] yt-dlp upgrade failed: %s', (result.stderr or '')[-2000:])
        return

    try:
        new = _pkg_version('yt-dlp')
    except PackageNotFoundError:
        new = 'unknown'

    if new != old:
        logger.info('[maint] yt-dlp upgraded %s -> %s', old, new)
    else:
        logger.info('[maint] yt-dlp already at latest (%s)', new)

    # Reinstall succeeded either way — the version string alone doesn't prove
    # nothing changed (yt-dlp only bumps it at tagged releases, not per master
    # commit), so always recycle workers rather than trusting the comparison.
    _reload_gunicorn_workers()


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

    def _scheduled_ytdlp_upgrade():
        try:
            _r = redis.from_url(flask_app.config['REDIS_URL'])
            # Scraper queue, not maintenance — same reasoning as _scheduled_prune:
            # the single scraper worker serializes this pip reinstall away from
            # any concurrent scrape's `import yt_dlp`, which would otherwise race
            # a mid-write package directory on the maintenance queue's own worker.
            # job_timeout is well above the subprocess's own 300s timeout so pip's
            # timeout handler (which kills its child) always fires first.
            _q = Queue('scraper', connection=_r)
            _q.enqueue('app.worker._rq_ytdlp_upgrade', job_timeout=600, job_id='ytdlp-upgrade')
            logger.info('[scheduler] enqueued _rq_ytdlp_upgrade job')
        except Exception as e:
            logger.error('[scheduler] could not enqueue ytdlp_upgrade job: %s', e)

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

        # Weekly, Sunday 02:00 user-local — independent of the DB backup/VACUUM
        # window above; this just refreshes a pip package.
        scheduler.add_job(_scheduled_ytdlp_upgrade, 'cron',
                          day_of_week='sun', hour=2, minute=0, timezone=_tz,
                          id='ytdlp_upgrade', max_instances=1, coalesce=True,
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
            # Ask the DVR which lineups actually exist so DVR-side source names
            # that don't match the "FastChannels <feed>" template (e.g. the
            # "... PrismCast" DRM-bridge sources) are still covered, and lineups
            # with no DVR source aren't blindly PUT (the DVR 200s unknown IDs,
            # so those phantom pushes never surface as errors).
            lineup_ids = None
            try:
                r = _requests.get(f'{dvr_url}/dvr/lineups', timeout=15, verify=False)
                r.raise_for_status()
                lineup_ids = sorted({
                    lid for lid in r.json().values()
                    if isinstance(lid, str) and lid.startswith('XMLTV-FastChannels')
                })
            except Exception as exc:
                logger.warning('[dvr-epg] lineup discovery failed (%s); '
                               'falling back to feed-name construction', exc)
            if lineup_ids is None:
                lineup_ids = [
                    'XMLTV-' + _re.sub(r'[^a-zA-Z0-9]', '', name)
                    for name in feed_names
                ]
            refreshed, errors = [], []
            for lineup_id in lineup_ids:
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
                # Skip the orphan-source purge here — entrypoint.sh already ran it
                # synchronously moments ago, before this scheduler process even started.
                _q.enqueue('app.worker._rq_integrity_cleanup', False, job_timeout=300)
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
            # Run the first due-scrape sweep now instead of waiting for
            # auto_scrape's own first 60s tick, so the scraper queue is
            # already populated by the time we decide (right below) whether
            # to fire the startup xml-refresh — a container restart routinely
            # leaves EVERY enabled source overdue at once (their intervals
            # all lapsed while it was down), and this refresh's own
            # "was it enqueued" check has nothing to see if it runs first.
            try:
                _schedule_due_scrapes()
            except Exception:
                logger.exception('[scheduler] startup due-scrape sweep failed')
            try:
                # Same courtesy the post-scrape completion path already gives
                # this job (_no_scrapes_pending, see run_scraper) — skip it
                # here too when the sweep above just queued a pile of overdue
                # sources, rather than firing unconditionally and forcing the
                # ~150-200MB rebuild to run concurrently with that whole
                # marathon. Nothing is lost by skipping: the last scraper job
                # to finish re-triggers this exact call once the queue is
                # actually idle (observed live 2026-08-14 — a cold restart
                # with several overdue sources drove the host disk to ~100%
                # util and made gunicorn briefly unresponsive).
                if _any_scrapes_active():
                    logger.info('[xml-cache] deferring startup refresh — scraper work active')
                else:
                    _enqueue_xml_refresh_job()
            except Exception:
                logger.exception('[xml-cache] startup refresh failed')

            # Trigger tvtv cache refresh at startup if the cache is empty or stale.
            try:
                from app.models import TvtvProgramCache
                from sqlalchemy import func as sa_func
                from datetime import timezone as _tz
                now_utc = datetime.now(_tz.utc)
                newest = db.session.query(sa_func.max(TvtvProgramCache.fetched_at)).scalar()
                if newest is None:
                    stale = True
                else:
                    if newest.tzinfo is None:
                        newest = newest.replace(tzinfo=_tz.utc)
                    age_hours = (now_utc - newest).total_seconds() / 3600
                    stale = age_hours > 25
                if stale:
                    settings = AppSettings.get()
                    last_attempt = settings.tvtv_cache_last_attempt_at
                    if last_attempt is not None and last_attempt.tzinfo is None:
                        last_attempt = last_attempt.replace(tzinfo=_tz.utc)
                    attempt_age_hours = (
                        (now_utc - last_attempt).total_seconds() / 3600
                        if last_attempt is not None else None
                    )
                    if attempt_age_hours is not None and attempt_age_hours < _TVTV_CACHE_STARTUP_RETRY_COOLDOWN_HOURS:
                        logger.info(
                            '[tvtv-cache] stale/empty at startup (newest=%s), but last attempt was %.1fh ago; skipping startup retry',
                            newest,
                            attempt_age_hours,
                        )
                        startup_status = 'cooldown'
                    else:
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

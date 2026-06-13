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
from sqlalchemy import and_, not_, or_, text
from sqlalchemy.exc import OperationalError as _SAOperationalError
from sqlalchemy.orm.attributes import flag_modified as _flag_modified
from app import create_app
from app.config_store import persist_source_config_updates
from app.extensions import db
from app.hls import inspect_hls_drm, parse_stream_info as _parse_stream_info
from app.models import Source, Channel, Program, Feed, AppSettings
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


def _audit_reason_from_exception(exc: Exception) -> str:
    message = str(exc).strip()
    name = type(exc).__name__
    if not message:
        return name
    if message == name or message.startswith(f'{name}:'):
        return message
    http_match = re.search(r'\bHTTP\s+(\d{3})\b', message)
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
            # If channel_refresh_hours > 0 and we scraped within that window,
            # only refresh EPG using the existing DB channel list.
            skip_channels = False
            if refresh_hours > 0 and source.last_scraped_at:
                last = _utc_aware(source.last_scraped_at)
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
                        source.last_scraped_at = datetime.now(timezone.utc)
                        source.last_error      = None
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
                        _apply_scraper_config_updates(source, scraper)
                        db.session.commit()
                        # Clear so the EPG commit's _apply_scraper_config_updates
                        # only persists updates added during the EPG phase, not a
                        # re-merge of the already-committed channel-phase snapshot.
                        if hasattr(scraper, '_pending_config_updates'):
                            scraper._pending_config_updates.clear()
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
                        source.last_scraped_at = datetime.now(timezone.utc)
                        source.last_error      = None
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
            _skip_cfg = dict(source.config or {})
            _skip_cfg['last_audit_result'] = {
                'skipped_reason': _skip_msg,
                'ts': datetime.now(timezone.utc).isoformat(),
            }
            source.config = _skip_cfg
            _flag_modified(source, 'config')
            db.session.commit()
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
        dead     = 0
        vod      = 0
        errors   = 0
        skipped_403 = 0
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

                if r.status_code in (403, 429, 500, 502, 503, 504):
                    # Still rate-limited or transient server error after backoff —
                    # skip without penalising the consecutive-error budget.
                    # 500/502/504 are CDN hiccups, not stream problems.
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
                    _widevine  = 'edef8ba9-79d6-4ace-a3c8-27dcd51d21ed'
                    _playready = '9a04f079-9840-4286-ab92-e65be0885f95'
                    if _widevine in manifest_text.lower() or _playready in manifest_text.lower():
                        _dash_drm_type = 'Widevine' if _widevine in manifest_text.lower() else 'PlayReady'
                        ch.is_active      = False
                        ch.is_enabled     = False
                        ch.disable_reason = f'DRM:{_dash_drm_type}'
                        flagged += 1
                        report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'drm', 'reason': _dash_drm_type})
                        logger.info('[audit] DASH DRM: %s  →  %s (%s)', ch.name, manifest_url[:80], _dash_drm_type)
                    else:
                        # DASH alive (no VOD, no DRM)
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
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = f'DRM:{_drm_type}'
                    flagged += 1
                    report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'drm', 'reason': _drm_type})
                    logger.info('[audit] DRM: %s  →  %s (%s)', ch.name, manifest_url[:80], _drm_type)
                elif not ch.is_active:
                    # HLS alive — re-activate previously dead channel
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
                    _partial_cfg = dict(source.config or {})
                    _partial_cfg['last_audit_result'] = {
                        'total': i, 'checked': checked, 'flagged': flagged,
                        'dead': dead, 'vod': vod, 'errors': errors, 'skipped_403': skipped_403,
                        'ts': datetime.now(timezone.utc).isoformat(),
                        'partial': True,
                    }
                    source.last_audited_at = datetime.now(timezone.utc)
                    source.config = _partial_cfg
                    _flag_modified(source, 'config')
                    db.session.commit()
                    _audit_progress(i, total, flagged, dead, vod, errors, skipped_403)
                    logger.info('[audit] %s: %d/%d — checked=%d flagged=%d dead=%d vod=%d errors=%d skipped_403=%d',
                                source_name, i, total, checked, flagged, dead, vod, errors, skipped_403)

                if source_name == 'plex':
                    logger.debug('[audit-debug] %s %d/%d finish elapsed=%.2fs checked=%d dead=%d flagged=%d vod=%d errors=%d',
                                source_name, i, total, _time.monotonic() - locals().get('_audit_item_t0', _time.monotonic()),
                                checked, dead, flagged, vod, errors)

                _time.sleep(0.3)

        source.last_audited_at = datetime.now(timezone.utc)
        cfg = dict(source.config or {})
        cfg['last_audit_result'] = {
            'total': total, 'checked': checked, 'flagged': flagged,
            'dead': dead, 'vod': vod, 'errors': errors, 'skipped_403': skipped_403,
            'ts': datetime.now(timezone.utc).isoformat(),
        }
        cfg['last_audit_report'] = {
            'channels': report_channels,
            'ts': datetime.now(timezone.utc).isoformat(),
        }
        source.config = cfg
        _flag_modified(source, 'config')
        db.session.commit()
        _audit_progress(0, 0, phase='done')
        logger.info('[audit] %s: done — total=%d checked=%d flagged=%d dead=%d vod=%d errors=%d skipped_403=%d',
                    source_name, total, checked, flagged, dead, vod, errors, skipped_403)


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

        # Merge recheck results back into the saved report
        cfg = dict(source.config or {})
        report = dict(cfg.get('last_audit_report') or {})
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
        cfg['last_audit_report'] = report

        # Update result summary skipped_403 count
        result_summary = dict(cfg.get('last_audit_result') or {})
        still_limited = sum(1 for r in recheck_results.values() if r and r['status'] == 'rate-limited')
        result_summary['skipped_403'] = still_limited
        result_summary['ts'] = datetime.now(timezone.utc).isoformat()
        cfg['last_audit_result'] = result_summary

        source.config = cfg
        _flag_modified(source, 'config')
        db.session.commit()
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
    """Merge any config updates the scraper queued back into source.config."""
    if scraper and scraper._pending_config_updates:
        persist_source_config_updates(source.id, scraper._pending_config_updates)
        logger.debug('[%s] persisting %d config update(s): %s',
                     source.name, len(scraper._pending_config_updates),
                     list(scraper._pending_config_updates.keys()))


def _epg_channels_for_source(source) -> list[Channel]:
    """Return DB channels that should participate in EPG refreshes."""
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
    from app.generators.m3u import generate_gracenote_m3u, generate_m3u, generate_native_m3u, generate_watch_m3u, feed_gracenote_start, feed_namespace_start, feed_to_query_filters, _MASTER_GRACENOTE_START
    from app.generators.xmltv import write_xmltv

    for attempt in range(2):
        base_url = (
            (AppSettings.get().effective_public_base_url() or '').strip().rstrip('/')
            or (flask_app.config.get('PUBLIC_BASE_URL') or '').strip().rstrip('/')
            or 'http://localhost:5523'
        )
        xml_artifacts: list[tuple[str, Callable]] = [
            ('master', lambda fp: write_xmltv(fp, {}, base_url=base_url)),
        ]
        m3u_artifacts: list[tuple[str, Callable]] = [
            ('master-m3u', lambda fp: fp.write(generate_m3u({}, base_url=base_url))),
            ('master-watch-m3u', lambda fp: fp.write(generate_watch_m3u({}, base_url=base_url))),
        ]
        default_feed = Feed.query.filter_by(slug='default').first()
        default_gn_start = feed_gracenote_start(default_feed) if default_feed else _MASTER_GRACENOTE_START
        m3u_artifacts.append((
            'master-gracenote-m3u',
            lambda fp: fp.write(generate_gracenote_m3u({}, base_url=base_url, namespace_start=default_gn_start)),
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
            m3u_artifacts.append((
                f'feed-{feed.slug}-native-m3u',
                lambda fp, filters=filters, std_kw=std_kw: fp.write(
                    generate_native_m3u(filters, base_url=base_url, **std_kw)
                ),
            ))
            m3u_artifacts.append((
                f'feed-{feed.slug}-watch-m3u',
                lambda fp, filters=filters, std_kw=std_kw: fp.write(
                    generate_watch_m3u(filters, base_url=base_url, **std_kw)
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
    q = Channel.query.join(Source)
    if src := filters.get('source'):
        q = q.filter(Source.name == src)
    if cat := filters.get('category'):
        q = q.filter(Channel.category == cat)
    if lang := filters.get('language'):
        q = q.filter(Channel.language == lang)
    if search := filters.get('search'):
        q = q.filter(Channel.name.ilike(f'%{search}%'))
    if drm := filters.get('drm'):
        if drm == '1':
            q = q.filter(Channel.disable_reason.like('DRM%'))
        elif drm == 'dead':
            q = q.filter(Channel.disable_reason == 'Dead')
        elif drm == '0':
            q = q.filter(Channel.disable_reason == None)
    if ef := filters.get('enabled'):
        if ef == '1':
            q = q.filter(Channel.is_enabled == True)
        elif ef == '0':
            q = q.filter(Channel.is_enabled == False)
    if pf := filters.get('presence'):
        if pf == 'inactive':
            q = q.filter(Channel.is_active == False)
        elif pf == 'enabled_inactive':
            q = q.filter(Channel.is_enabled == True, Channel.is_active == False)
        elif pf == 'missed':
            q = q.filter(Channel.missed_scrapes >= 1)
        elif pf == 'active':
            q = q.filter(Channel.is_active == True)
    if gf := filters.get('gracenote'):
        if gf == '1':
            q = q.filter(Channel.gracenote_id != None, Channel.gracenote_id != '')
        elif gf == '0':
            q = q.filter(or_(Channel.gracenote_id == None, Channel.gracenote_id == ''))
    if gm := filters.get('gracenote_mode'):
        manual_mode = or_(
            Channel.gracenote_mode == 'manual',
            and_(
                Channel.gracenote_mode == None,
                Channel.gracenote_locked == True,
                Channel.gracenote_id != None,
                Channel.gracenote_id != '',
            ),
        )
        off_mode = Channel.gracenote_mode == 'off'
        if gm == 'manual':
            q = q.filter(manual_mode)
        elif gm == 'off':
            q = q.filter(off_mode)
        elif gm == 'auto':
            q = q.filter(not_(or_(manual_mode, off_mode)))
    if filters.get('duplicates') == '1':
        from app.routes.admin import _duplicate_name_sets
        exact_duplicate_names, possible_duplicate_names, _ = _duplicate_name_sets()
        all_duplicate_names = exact_duplicate_names | possible_duplicate_names
        q = q.filter(or_(Channel.name.in_(sorted(all_duplicate_names)), Channel.is_duplicate == True))
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


def run_source_channel_purge(source_id: int):
    with flask_app.app_context():
        source = Source.query.get(source_id)
        if not source:
            logger.warning('[source-purge] source_id=%s not found', source_id)
            return
        ch_ids = [row[0] for row in source.channels.with_entities(Channel.id).all()]
        deleted_programs = 0
        deleted_channels = 0
        if ch_ids:
            deleted_programs = Program.query.filter(
                Program.channel_id.in_(ch_ids)
            ).delete(synchronize_session=False)
            deleted_channels = source.channels.delete(synchronize_session=False)
        db.session.commit()
        _invalidate_and_refresh_xml()
        logger.info(
            '[source-purge] source=%s deleted %d channels and %d programs',
            source.name, deleted_channels or 0, deleted_programs or 0,
        )


def run_bulk_channel_update(filters: dict, enable: bool):
    with flask_app.app_context():
        ids = _channel_ids_for_filters(filters or {})
        updated = 0
        if ids:
            values = {'is_enabled': enable}
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
        was_active = ch.is_active
        ch.is_active = False
        ch.is_enabled = False
        ch.disable_reason = reason
        if was_active:
            ch.went_inactive_at = datetime.now(timezone.utc)
        for _attempt in range(3):
            try:
                db.session.commit()
                break
            except _SAOperationalError:
                db.session.rollback()
                if _attempt == 2:
                    raise
                time.sleep(3 * (_attempt + 1))
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

        # Extract gracenote_id from ChannelData if the scraper set it directly,
        # or fall back to the "{play_id}|{gracenote_id}" slug encoding (Roku).
        gracenote_id = getattr(cd, 'gracenote_id', None) or None
        if not gracenote_id and cd.slug and '|' in cd.slug:
            candidate = cd.slug.split('|', 1)[1].strip()
            if candidate and candidate.isdigit():
                gracenote_id = candidate

        if ch:
            stream_url_changed = ch.stream_url != cd.stream_url
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
            # Manual/Off modes are authoritative until the user switches back
            # to Auto, so scraper/helper data only fills gaps on auto rows.
            if mode == 'auto' and gracenote_id is not None and gracenote_auto_fill:
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
    db.session.flush()
    _refresh_auto_channel_numbers()


def _prune_old_programs(batch_size: int = 10000):
    """Delete programs that ended more than 2 hours ago, in batches.

    Use timezone-aware UTC to match the rest of the worker's program handling
    and avoid Python 3.12's utcnow() deprecation warning.

    Batches are committed individually so the write lock is held only briefly
    and never blocks concurrent guide/artifact refreshes for long. The batch
    size is large enough that a big backlog (e.g. after several missed runs)
    clears in a handful of commits rather than ~100 tiny ones.
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

    incoming_by_channel_id: dict[int, list] = {}
    for pd in program_data_list:
        ch = channels.get(pd.source_channel_id)
        if not ch:
            continue
        incoming_by_channel_id.setdefault(ch.id, []).append(pd)

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
    for pd in program_data_list:
        ch = channels.get(pd.source_channel_id)
        if not ch:
            continue
        rows.append({
            'channel_id':    ch.id,
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


def _is_source_due(source, now, last):
    """Return True if this source should be enqueued for a scrape right now."""
    if source.scrape_cron:
        try:
            prev = _croniter(source.scrape_cron, now).get_prev(datetime)
            return last is None or prev >= last
        except Exception:
            logger.warning('[scheduler] Invalid cron expression for %s: %r', source.name, source.scrape_cron)
            return False
    if not source.scrape_interval:
        return False  # scrape_interval=0 means never auto-scrape
    return last is None or (now - last).total_seconds() >= source.scrape_interval * 60


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

        sources = Source.query.filter_by(is_enabled=True).all()
        for source in sources:
            if _scrape_job_already_active(q, source.name):
                _last_enqueued[source.name] = now
                continue
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
        default_disabled_sources = {'amazon_prime_free', 'sling', 'localnow', 'pluto', 'frndlytv', 'fubo', 'hdhomerun'}
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


def _rq_prune():
    """RQ job target: prune expired EPG entries. Runs inside the RQ worker process."""
    with flask_app.app_context():
        _prune_old_programs()


def _rq_integrity_cleanup():
    """RQ job target: delete orphan channels/programs. Runs inside the RQ worker process."""
    with flask_app.app_context():
        _cleanup_orphans()


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
    tmp_db   = _tempfile.NamedTemporaryFile(suffix='.db', delete=False)
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


if __name__ == '__main__':
    import os

    role = (os.environ.get('FC_WORKER_ROLE') or 'all').strip().lower()

    def _scheduled_prune():
        try:
            _r = redis.from_url(flask_app.config['REDIS_URL'])
            _q = Queue('maintenance', connection=_r)
            _q.enqueue('app.worker._rq_prune', job_timeout=900)
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
        scheduler = BackgroundScheduler(daemon=True)
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

        # 03:00 user local time — reads timezone from AppSettings so it follows
        # whatever the user has configured in admin/settings.
        with flask_app.app_context():
            from app.models import AppSettings as _AS
            import pytz as _pytz
            _tz_name = (_AS.get().timezone_name or 'UTC')
            try:
                _tz = _pytz.timezone(_tz_name)
            except Exception:
                _tz = _pytz.utc
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
        _run_scheduler()
    elif role == 'fast':
        _run_fast_worker()
    elif role == 'maintenance':
        _run_maintenance_worker()
    elif role == 'scraper':
        _run_scraper_worker()
    else:
        logger.error('Unknown FC_WORKER_ROLE=%r', role)
        sys.exit(2)

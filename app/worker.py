"""
Background worker — run with: python -m app.worker
"""
import logging
import multiprocessing
import gc
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
from sqlalchemy import and_, not_, or_, text
from sqlalchemy.exc import OperationalError as _SAOperationalError
from app import create_app
from app.config_store import persist_source_config_updates
from app.extensions import db
from app.hls import inspect_hls_drm
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    stream=sys.stdout,
)
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


def _enqueue_xml_refresh_job() -> None:
    try:
        r = redis.from_url(flask_app.config['REDIS_URL'])
        q = Queue('fast', connection=r)
        job_id = 'xml-refresh'
        active_ids = set(q.get_job_ids()) | set(StartedJobRegistry(q.name, connection=q.connection).get_job_ids())
        if job_id in active_ids:
            logger.info('[xml-cache] refresh already queued/running')
            return
        try:
            job = Job.fetch(job_id, connection=q.connection)
            if job.get_status(refresh=False) in {'queued', 'started', 'deferred', 'scheduled'}:
                logger.info('[xml-cache] refresh already queued/running')
                return
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
        logger.warning('[rq] removed stale started-job marker for non-started job %s', job_id)
        return True

    now = datetime.now(timezone.utc)
    started_at = _utc_aware(getattr(job, 'started_at', None))
    last_heartbeat = _utc_aware(getattr(job, 'last_heartbeat', None))
    heartbeat_age = (now - last_heartbeat).total_seconds() if last_heartbeat else None
    started_age = (now - started_at).total_seconds() if started_at else None

    if heartbeat_age is not None and heartbeat_age > _STALE_STARTED_JOB_GRACE_SECONDS:
        registry.remove(job)
        logger.warning('[rq] removed stale started job %s after %.0fs without heartbeat', job_id, heartbeat_age)
        return True

    if last_heartbeat is None and started_age is not None and started_age > _STALE_STARTED_JOB_GRACE_SECONDS:
        registry.remove(job)
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
            phase_timeouts = getattr(scraper_cls, 'phase_timeouts', {}) or {}

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

                previous = signal.getsignal(signal.SIGALRM)
                signal.signal(signal.SIGALRM, _alarm_handler)
                signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
                try:
                    return fn(*args, **kwargs)
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0)
                    signal.signal(signal.SIGALRM, previous)

            logger.info('[%s] scraper init starting', source_name)
            scraper = _run_phase('init', scraper_cls, config=source.config or {})
            logger.info('[%s] scraper init complete', source_name)
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
            logger.info('[%s] bootstrap starting', source_name)
            _run_phase('bootstrap', scraper.pre_run_setup)
            logger.info('[%s] bootstrap complete', source_name)
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
                logger.info('[%s] EPG fetch starting', source_name)
                programs = _run_phase(
                    'epg',
                    scraper.fetch_epg,
                    epg_input,
                    skip_ids=_fresh_epg_sids(source),
                    enabled_ids=enabled_ids,
                )
                logger.info('[%s] EPG fetch complete', source_name)
                for _attempt in range(3):
                    try:
                        _upsert_programs(source, programs)
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
                _enqueue_xml_refresh_job()
                elapsed = time.monotonic() - t0
                logger.info('[%s] EPG-only run complete — %d channels, %d programs (%.1fs)',
                            source_name, len(db_channels), len(programs), elapsed)
            else:
                _progress('channels')
                logger.info('[%s] channel fetch starting', source_name)
                channels = _run_phase('channels', scraper.fetch_channels)
                logger.info('[%s] channel fetch complete', source_name)
                _progress('epg', 0, len(channels))
                logger.info('[%s] EPG fetch starting', source_name)
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
                logger.info('[%s] EPG fetch complete', source_name)
                for _attempt in range(3):
                    try:
                        _upsert_channels(source, channels, _gracenote_auto_fill)
                        _upsert_programs(source, programs)
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
                _enqueue_xml_refresh_job()
                elapsed = time.monotonic() - t0
                logger.info('[%s] Scrape complete — %d channels, %d programs (%.1fs)',
                            source_name, len(channels), len(programs), elapsed)
                _prewarm_logos(source_name, [ch.logo_url for ch in channels])
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
    Stream Audit — resolves each active channel via the scraper, fetches the
    HLS manifest using the scraper's session (so source-specific headers like
    Origin/Referer are included), drills master → variant playlist, and checks
    for dead streams, VOD-only content, and SAMPLE-AES DRM encryption.
    Flagged channels are marked is_active=False so they drop out of M3U/EPG output.
    """
    with flask_app.app_context():
        source = Source.query.filter_by(name=source_name).first()
        if not source:
            logger.error('[audit] source not found: %s', source_name)
            return

        scraper_cls = registry.get(source_name)
        if not scraper_cls or not getattr(scraper_cls, 'stream_audit_enabled', False):
            logger.info('[audit] %s: stream audit not enabled for this source, skipping', source_name)
            return

        scraper = scraper_cls(config=source.config or {})
        try:
            scraper.pre_run_setup()
        except Exception as _pre_exc:
            logger.debug('[audit] pre_run_setup failed (non-fatal): %s', _pre_exc)

        channels = source.channels.all()
        total    = len(channels)
        checked  = 0
        flagged  = 0
        dead     = 0
        errors   = 0
        skipped_403 = 0
        consecutive_errors = 0
        report_channels = []

        logger.info('[audit] %s: checking %d channels…', source_name, total)

        # Live progress → Redis key audit:progress:{source_name}
        _audit_key = f'audit:progress:{source_name}'
        try:
            _redis_audit = redis.from_url(flask_app.config['REDIS_URL'])
            _redis_audit.ping()
        except Exception:
            _redis_audit = None

        import json as _json_audit
        def _audit_progress(done, total_, flagged_=0, dead_=0, errors_=0, skipped_403_=0, phase='checking'):
            if not _redis_audit:
                return
            try:
                if phase == 'done':
                    _redis_audit.delete(_audit_key)
                else:
                    _redis_audit.setex(_audit_key, 600, _json_audit.dumps({
                        'phase': phase, 'done': done, 'total': total_,
                        'flagged': flagged_, 'dead': dead_, 'errors': errors_,
                        'skipped_403': skipped_403_,
                        'ts': _time.time(),
                    }))
            except Exception:
                pass

        _audit_progress(0, total)

        # Brief warmup pause — gives any residual rate-limit ban time to clear
        _time.sleep(5)

        # Use the scraper's own session so source-specific headers (Origin, Referer,
        # auth tokens, etc.) are included in every CDN request.
        sess = scraper.session

        for i, ch in enumerate(channels, 1):
            try:
                # Resolve the raw stream URL. Use audit_resolve() if the scraper
                # provides a lighter-weight bulk-check variant (e.g. Plex skips tune).
                _resolve = getattr(scraper, 'audit_resolve', scraper.resolve)
                try:
                    resolved_url = _resolve(ch.stream_url)
                except StreamDeadError:
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'Dead'
                    dead += 1
                    consecutive_errors = 0
                    report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'dead', 'reason': 'Resolve error'})
                    logger.info('[audit] dead stream: %s  (confirmed by scraper)', ch.name)
                    continue
                except Exception as re_exc:
                    if _is_transient_network_error(re_exc):
                        logger.warning('[audit] transient resolve failure for %s: %s', ch.name, re_exc)
                        errors += 1
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
                        _time.sleep(wait)
                        errors += 1
                        continue
                    logger.warning('[audit] resolve failed for %s: %s', ch.name, re_exc)
                    errors += 1
                    consecutive_errors += 1
                    if consecutive_errors >= 20:
                        logger.error('[audit] %s: 20 consecutive errors — aborting.', source_name)
                        break
                    continue

                try:
                    r = sess.get(resolved_url, timeout=15, allow_redirects=True)
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
                        logger.warning('[audit] transient manifest fetch failure for %s: %s', ch.name, req_exc)
                        errors += 1
                        continue
                    raise

                if r.status_code in (403, 429, 503):
                    wait = 30 + skipped_403 * 5  # escalate backoff with repeated 403s
                    logger.warning('[audit] %s rate-limited (%d), backing off %ds…',
                                   source_name, r.status_code, wait)
                    _time.sleep(min(wait, 120))
                    r = sess.get(resolved_url, timeout=15, allow_redirects=True)

                if r.status_code in (400, 404, 410, 422):
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'Dead'
                    dead += 1
                    consecutive_errors = 0
                    report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'dead', 'reason': f'HTTP {r.status_code}'})
                    logger.info('[audit] dead stream: %s  (HTTP %d)', ch.name, r.status_code)
                    continue

                if r.status_code in (403, 429, 503):
                    # Still rate-limited after backoff — skip without penalising the
                    # consecutive-error budget.  These are CDN/auth throttles, not
                    # real stream errors, so they should not abort the audit.
                    skipped_403 += 1
                    consecutive_errors = 0
                    report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'rate-limited', 'reason': f'HTTP {r.status_code}'})
                    logger.info('[audit] %s still rate-limited (%d) after backoff, skipping',
                                ch.name, r.status_code)
                    continue

                if r.status_code != 200:
                    logger.debug('[audit] %d for %s', r.status_code, ch.name)
                    errors += 1
                    consecutive_errors += 1
                    if consecutive_errors >= 20:
                        logger.error('[audit] %s: 20 consecutive errors — aborting. '
                                     'Source may be rate-limiting or down.', source_name)
                        break
                    continue

                consecutive_errors = 0
                checked += 1
                manifest_text = r.text
                manifest_url  = r.url

                # ── DASH/MPD manifest ──────────────────────────────────────
                if '<MPD ' in manifest_text or (manifest_text.lstrip().startswith('<?xml')
                                                and '<MPD' in manifest_text):
                    if 'type="static"' in manifest_text:
                        ch.is_active      = False
                        ch.is_enabled     = False
                        ch.disable_reason = 'Dead'
                        dead += 1
                        report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'dead', 'reason': 'VOD'})
                        logger.info('[audit] DASH VOD (not live): %s', ch.name)
                        continue
                    _widevine  = 'edef8ba9-79d6-4ace-a3c8-27dcd51d21ed'
                    _playready = '9a04f079-9840-4286-ab92-e65be0885f95'
                    if _widevine in manifest_text.lower() or _playready in manifest_text.lower():
                        ch.is_active      = False
                        ch.is_enabled     = False
                        ch.disable_reason = 'DRM'
                        flagged += 1
                        report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'drm', 'reason': 'DRM'})
                        logger.info('[audit] DASH DRM: %s  →  %s', ch.name, manifest_url[:80])
                    continue   # DASH — skip HLS checks below

                # EXT-X-KEY only appears in media playlists, not master playlists.
                # If we landed on a master, fetch the first variant to check properly.
                if '#EXT-X-STREAM-INF' in manifest_text:
                    variant_url = None
                    for line in manifest_text.splitlines():
                        line = line.strip()
                        if line and not line.startswith('#'):
                            variant_url = _urljoin(manifest_url, line)
                            break
                    if variant_url:
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
                    and not getattr(scraper, 'audit_ignore_playlist_type_vod', False)
                ):
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'Dead'
                    dead += 1
                    report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'dead', 'reason': 'VOD'})
                    logger.info('[audit] VOD (not live): %s', ch.name)
                    continue

                drm = inspect_hls_drm(manifest_text)
                if drm:
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'DRM'
                    flagged += 1
                    report_channels.append({'id': ch.id, 'name': ch.name, 'status': 'drm', 'reason': drm.get('drm_type', 'DRM')})
                    logger.info('[audit] DRM: %s  →  %s (%s)', ch.name, manifest_url[:80], drm['drm_type'])

            except Exception as e:
                if _is_transient_network_error(e):
                    logger.warning('[audit] transient audit failure for %s: %s', ch.name, e)
                    errors += 1
                    continue
                logger.debug('[audit] error for %s: %s', ch.name, e)
                errors += 1
                consecutive_errors += 1

            if i % 25 == 0:
                db.session.commit()
                _audit_progress(i, total, flagged, dead, errors, skipped_403)
                logger.info('[audit] %s: %d/%d — checked=%d flagged=%d dead=%d errors=%d skipped_403=%d',
                            source_name, i, total, checked, flagged, dead, errors, skipped_403)

            _time.sleep(0.3)

        source.last_audited_at = datetime.now(timezone.utc)
        cfg = dict(source.config or {})
        cfg['last_audit_result'] = {
            'total': total, 'checked': checked, 'flagged': flagged,
            'dead': dead, 'errors': errors, 'skipped_403': skipped_403,
            'ts': datetime.now(timezone.utc).isoformat(),
        }
        cfg['last_audit_report'] = {
            'channels': report_channels,
            'ts': datetime.now(timezone.utc).isoformat(),
        }
        source.config = cfg
        db.session.commit()
        _audit_progress(0, 0, phase='done')
        logger.info('[audit] %s: done — total=%d checked=%d flagged=%d dead=%d errors=%d skipped_403=%d',
                    source_name, total, checked, flagged, dead, errors, skipped_403)


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
                except StreamDeadError:
                    ch.is_active = False
                    ch.is_enabled = False
                    ch.disable_reason = 'Dead'
                    recheck_results[ch.id] = {'status': 'dead', 'reason': 'Resolve error'}
                    _rc_progress(i, total)
                    continue
                except Exception:
                    recheck_results[ch.id] = {'status': 'rate-limited', 'reason': 'Resolve failed'}
                    _rc_progress(i, total)
                    continue

                try:
                    r = sess.get(resolved_url, timeout=15, allow_redirects=True)
                except Exception:
                    recheck_results[ch.id] = {'status': 'rate-limited', 'reason': 'Network error'}
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
                recheck_results[ch.id] = {'status': 'rate-limited', 'reason': 'Error'}

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
    """Return DB channels that should participate in EPG refreshes.

    DRM-marked channels stay disabled for playback, but keeping them in the
    EPG refresh set preserves guide data in case support improves later.
    """
    return source.channels.filter(
        (Channel.is_active == True) | (Channel.disable_reason == 'DRM')
    ).all()


def _prewarm_logos(source_name: str, logo_urls: list[str]) -> None:
    """
    Pre-warm the logo cache for *logo_urls*.  Runs inside the RQ job process
    after a full channel scrape; uses an internal ThreadPoolExecutor so fetches
    are concurrent without blocking the job thread.
    """
    from app.routes.images import prewarm_logo_cache
    urls = [u for u in logo_urls if u]
    if not urls:
        return
    try:
        prewarm_logo_cache(urls)
    except Exception:
        logger.exception('[%s] logo cache pre-warm failed', source_name)


def _refresh_xml_artifacts() -> None:
    """Refresh master/feed XML and M3U artifacts after scrape commits land."""
    from app.generators.m3u import generate_gracenote_m3u, generate_m3u, feed_gracenote_start, feed_namespace_start, feed_to_query_filters, _MASTER_GRACENOTE_START
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
            if feed.chnum_start is not None:
                std_kw = {'feed_chnum_start': feed.chnum_start}
            else:
                std_kw = {'namespace_start': feed_namespace_start(feed, gracenote=False)}
            m3u_artifacts.append((
                f'feed-{feed.slug}-m3u',
                lambda fp, filters=filters, std_kw=std_kw: fp.write(
                    generate_m3u(filters, base_url=base_url, **std_kw)
                ),
            ))
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
    with flask_app.app_context():
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
    with flask_app.app_context():
        _refresh_xml_artifacts_subprocess()


def run_tvtv_cache_refresh():
    """Fetch 3 days of tvtv guide data for all indexed FAST stations and store in DB."""
    with flask_app.app_context():
        from app.tvtv_cache import refresh_tvtv_cache
        summary = refresh_tvtv_cache(days=3)
        logger.info('[tvtv-cache] refresh complete: %s', summary)


def _invalidate_and_refresh_xml() -> None:
    invalidate_xml_cache()
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
            q = q.filter(Channel.disable_reason == 'DRM')
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
        exact_duplicate_names, possible_duplicate_names = _duplicate_name_sets()
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
        if not ch.is_active and not ch.is_enabled and ch.disable_reason == reason:
            return
        ch.is_active = False
        ch.is_enabled = False
        ch.disable_reason = reason
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
            ch.name,
            ch.source.name if ch.source else '?',
            ch.source_channel_id,
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
    if _validate_logo_url(incoming, cache):
        return incoming
    return current


def _upsert_channels(source, channel_data_list, gracenote_auto_fill: bool = True):
    existing = {ch.source_channel_id: ch for ch in source.channels.all()}
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

        # Extract gracenote_id from ChannelData if the scraper set it directly,
        # or fall back to the "{play_id}|{gracenote_id}" slug encoding (Roku).
        gracenote_id = getattr(cd, 'gracenote_id', None) or None
        if not gracenote_id and cd.slug and '|' in cd.slug:
            candidate = cd.slug.split('|', 1)[1].strip()
            if candidate:
                gracenote_id = candidate or None

        if ch:
            stream_url_changed = ch.stream_url != cd.stream_url
            ch.name          = cd.name
            ch.stream_url    = cd.stream_url
            ch.stream_type   = cd.stream_type
            old_logo_url = ch.logo_url
            next_logo = _resolved_logo_url(ch.logo_url, cd.logo_url, logo_validation_cache)
            if next_logo != (ch.logo_url or None) and next_logo != (cd.logo_url or '').strip():
                logger.info('[%s] keeping existing logo for %s after invalid replacement URL from scrape',
                            source.name, cd.name)
            ch.logo_url      = next_logo
            if old_logo_url and old_logo_url != (next_logo or ''):
                delete_cached_logo(old_logo_url)
                logger.debug('[%s] evicted cached logo for %s (URL changed)', source.name, cd.name)
            ch.slug          = cd.slug
            ch.category      = ch.category_override or category_for_channel(cd.name, cd.category)
            ch.language      = cd.language
            ch.country       = cd.country
            ch.number        = cd.number
            if getattr(cd, 'guide_key', None):
                ch.guide_key = cd.guide_key
            # Don't resurrect channels the stream audit flagged as Dead or DRM
            # unless the stream URL changed (source may have fixed the channel).
            if ch.disable_reason in ('Dead', 'DRM') and not stream_url_changed:
                ch.is_active  = False  # re-enforce — a prior scrape may have revived it
                ch.is_enabled = False
            else:
                ch.is_active = True
                if stream_url_changed and ch.disable_reason in ('Dead', 'DRM'):
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
                category          = category_for_channel(cd.name, cd.category),
                language          = cd.language,
                country           = cd.country,
                number            = cd.number,
                gracenote_id      = gracenote_id if gracenote_auto_fill else None,
                gracenote_locked  = False,
                gracenote_mode    = 'auto',
                guide_key         = getattr(cd, 'guide_key', None),
                last_seen_at      = seen_at,
                missed_scrapes    = 0,
            ))

    seen = {cd.source_channel_id for cd in channel_data_list}
    existing_active_ids = {ch_id for ch_id, ch in existing.items() if ch.is_active}
    missing_active_ids = existing_active_ids - seen

    # Guard against upstream/parser glitches returning a tiny partial lineup.
    # If we previously had a substantial active set and the new fetch would
    # deactivate most of it, keep the old rows active and log loudly instead
    # of collapsing the source to a handful of channels.
    if existing_active_ids:
        missing_ratio = len(missing_active_ids) / max(len(existing_active_ids), 1)
    else:
        missing_ratio = 0.0
    suspicious_collapse = (
        len(existing_active_ids) >= 50
        and len(seen) < max(25, int(len(existing_active_ids) * 0.35))
        and missing_ratio >= 0.6
    )
    if suspicious_collapse:
        logger.warning(
            '[%s] suspicious channel refresh collapse: existing_active=%d incoming=%d missing_active=%d; preserving prior active rows',
            source.name,
            len(existing_active_ids),
            len(seen),
            len(missing_active_ids),
        )
    else:
        for ch_id, ch in existing.items():
            if ch_id not in seen:
                next_missed = (ch.missed_scrapes or 0) + 1
                ch.missed_scrapes = next_missed
                if ch.is_active and next_missed >= _CHANNEL_MISS_THRESHOLD:
                    ch.is_active = False
                    logger.info(
                        '[%s] marking inactive after %d missed channel scrapes: %s (%s)',
                        source.name,
                        next_missed,
                        ch.name,
                        ch.source_channel_id,
                    )
    db.session.flush()


def _prune_old_programs():
    """Delete programs that ended more than 2 hours ago.

    Use timezone-aware UTC to match the rest of the worker's program handling
    and avoid Python 3.12's utcnow() deprecation warning.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
    deleted = Program.query.filter(Program.end_time < cutoff).delete()
    db.session.commit()
    if deleted:
        logger.info('[worker] pruned %d expired EPG entries', deleted)


def _cleanup_orphans():
    """Delete rows whose parent records no longer exist."""
    deleted_programs = db.session.execute(text("""
        DELETE FROM programs
        WHERE channel_id NOT IN (SELECT id FROM channels)
    """)).rowcount or 0
    deleted_channels = db.session.execute(text("""
        DELETE FROM channels
        WHERE source_id NOT IN (SELECT id FROM sources)
    """)).rowcount or 0
    db.session.commit()
    if deleted_programs or deleted_channels:
        logger.info(
            '[worker] cleaned %d orphan programs and %d orphan channels',
            deleted_programs,
            deleted_channels,
        )


def _upsert_programs(source, program_data_list):
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
    for channel_id, incoming_rows in incoming_by_channel_id.items():
        active_rows = [row for row in incoming_rows if _utc_aware(row.end_time) > cutoff]
        if not active_rows:
            continue
        win_start = min(_utc_aware(row.start_time) for row in active_rows)
        win_end   = max(_utc_aware(row.end_time)   for row in active_rows)
        Program.query.filter(
            Program.channel_id == channel_id,
            Program.end_time   >  win_start,
            Program.start_time <  win_end,
        ).delete(synchronize_session=False)

    rows = []
    for pd in program_data_list:
        ch = channels.get(pd.source_channel_id)
        if not ch:
            continue
        rows.append({
            'channel_id':    ch.id,
            'title':         pd.title,
            'description':   pd.description,
            'start_time':    pd.start_time,
            'end_time':      pd.end_time,
            'poster_url':    pd.poster_url,
            'category':      pd.category,
            'rating':        pd.rating,
            'episode_title': pd.episode_title,
            'season':        pd.season,
            'episode':       pd.episode,
            'original_air_date': pd.original_air_date,
        })
    if rows:
        db.session.execute(Program.__table__.insert(), rows)


# In-memory record of when each source was last enqueued, so we don't
# double-queue a source that's still running (last_scraped_at not yet updated).
_last_enqueued: dict[str, datetime] = {}


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
            interval_secs = (source.scrape_interval or 360) * 60

            last_scraped = _utc_aware(source.last_scraped_at)
            last_queued = _utc_aware(_last_enqueued.get(source.name))
            candidates = [t for t in (last_scraped, last_queued) if t is not None]
            last = max(candidates) if candidates else None

            if last is None or (now - last).total_seconds() >= interval_secs:
                try:
                    if _scrape_job_already_active(q, source.name):
                        logger.info('[scheduler] %s already queued/running; skipping duplicate enqueue', source.name)
                        _last_enqueued[source.name] = now
                        continue
                    q.enqueue('app.worker.run_scraper', source.name, job_timeout=3600, job_id=f'scrape-{source.name}')
                    _last_enqueued[source.name] = now
                    logger.info('[scheduler] Enqueued %s (interval=%dm, age=%s)',
                                source.name, source.scrape_interval,
                                f'{(now - last).total_seconds() / 60:.0f}m' if last else 'never')
                except Exception as e:
                    logger.error('[scheduler] Failed to enqueue %s: %s', source.name, e)


def seed_sources():
    with flask_app.app_context():
        scrapers = registry.get_all()
        default_disabled_sources = {'amazon_prime_free', 'sling'}
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


if __name__ == '__main__':
    seed_sources()

    def _scheduled_prune():
        try:
            _r = redis.from_url(flask_app.config['REDIS_URL'])
            _q = Queue('scraper', connection=_r)
            _q.enqueue('app.worker._rq_prune', job_timeout=300)
            logger.info('[scheduler] enqueued _rq_prune job')
        except Exception as e:
            logger.error('[scheduler] could not enqueue prune job: %s', e)

    def _scheduled_integrity_cleanup():
        try:
            _r = redis.from_url(flask_app.config['REDIS_URL'])
            _q = Queue('scraper', connection=_r)
            _q.enqueue('app.worker._rq_integrity_cleanup', job_timeout=300)
            logger.info('[scheduler] enqueued _rq_integrity_cleanup job')
        except Exception as e:
            logger.error('[scheduler] could not enqueue integrity_cleanup job: %s', e)

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
                .filter(Channel.logo_url.isnot(None))
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

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(_schedule_due_scrapes, 'interval', minutes=1, id='auto_scrape',
                      max_instances=1, coalesce=True)
    scheduler.add_job(_scheduled_prune, 'interval', hours=1, id='epg_prune',
                      max_instances=1, coalesce=True)
    scheduler.add_job(_scheduled_integrity_cleanup, 'interval', days=1, id='integrity_cleanup',
                      max_instances=1, coalesce=True)
    scheduler.add_job(_scheduled_logo_cache_cleanup, 'interval', hours=6, id='logo_cache_cleanup',
                      max_instances=1, coalesce=True)

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

    def _scheduled_tvtv_cache_refresh():
        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            q = Queue('scraper', connection=r)
            job_id = 'tvtv-cache-refresh'
            active_ids = set(q.get_job_ids()) | set(StartedJobRegistry(q.name, connection=q.connection).get_job_ids())
            if job_id in active_ids:
                logger.info('[tvtv-cache] refresh already queued/running, skipping')
                return
            q.enqueue('app.worker.run_tvtv_cache_refresh', job_timeout=3600, job_id=job_id)
            logger.info('[tvtv-cache] enqueued refresh job')
        except Exception as exc:
            logger.warning('[tvtv-cache] could not enqueue via RQ (%s), falling back to thread', exc)
            import threading
            threading.Thread(target=run_tvtv_cache_refresh, daemon=True).start()

    # 03:00 UTC (pre-dawn US) — well after the tvtv grid window resets at 04:00 UTC,
    # so today + 2 days of data are all fresh.  15:00 UTC is a midday safety net
    # in case the overnight run failed.
    scheduler.add_job(_scheduled_tvtv_cache_refresh, 'cron',
                      hour=3, minute=0,
                      id='tvtv_cache_refresh_night', max_instances=1, coalesce=True,
                      misfire_grace_time=3600)
    scheduler.add_job(_scheduled_tvtv_cache_refresh, 'cron',
                      hour=15, minute=0,
                      id='tvtv_cache_refresh_day', max_instances=1, coalesce=True,
                      misfire_grace_time=3600)

    scheduler.start()
    logger.info('Scheduler started — checking sources every 60s')
    with flask_app.app_context():
        _cleanup_orphans()
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

        # Trigger tvtv cache refresh at startup if the cache is empty or stale
        # (last fetch older than 13 hours).  13h is just over half the gap between
        # the two daily cron runs (03:00 / 15:00 UTC = 12h apart), so a container
        # that restarts and misses a cron window will self-heal on the next boot.
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
                stale = age_hours > 13
            if stale:
                _scheduled_tvtv_cache_refresh()
                logger.info('[tvtv-cache] stale/empty at startup (newest=%s) — triggered refresh', newest)
        except Exception:
            logger.exception('[tvtv-cache] startup staleness check failed')

        # Fetch remote community Gracenote map on startup
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

    def _run_fast_worker():
        r_fast = redis.from_url(flask_app.config['REDIS_URL'])
        w = _FastWorker(queues=[Queue('fast', connection=r_fast)], connection=r_fast)
        logger.info('Fast worker listening on queue: fast')
        w.work(logging_level=logging.WARNING)

    fast_thread = threading.Thread(target=_run_fast_worker, daemon=True, name='fast-worker')
    fast_thread.start()
    logger.info('Fast worker thread started')

    r = redis.from_url(flask_app.config['REDIS_URL'])
    with Connection(r):
        worker = Worker(queues=[Queue('scraper', connection=r)])
        logger.info('Scraper worker listening on queue: scraper')
        worker.work(logging_level=logging.WARNING)

import hashlib
import json
import logging
from datetime import datetime, timezone
from flask import current_app
import redis
from rq import Queue
from rq.job import Job
from rq.registry import StartedJobRegistry

logger = logging.getLogger(__name__)
_STALE_STARTED_JOB_GRACE_SECONDS = 300


def get_queue():
    r = redis.from_url(current_app.config['REDIS_URL'])
    return Queue('scraper', connection=r)


def get_fast_queue():
    r = redis.from_url(current_app.config['REDIS_URL'])
    return Queue('fast', connection=r)


def get_maintenance_queue():
    r = redis.from_url(current_app.config['REDIS_URL'])
    return Queue('maintenance', connection=r)


def _utc_aware(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _cleanup_stale_started_job(q: Queue, job_id: str) -> bool:
    registry = StartedJobRegistry(q.name, connection=q.connection)
    if job_id not in registry:
        return False
    try:
        job = Job.fetch(job_id, connection=q.connection)
    except Exception:
        registry.remove(job_id)
        logger.warning('Removed stale started-job marker for missing job %s', job_id)
        return True

    if job.get_status(refresh=False) != 'started':
        registry.remove(job)
        try:
            job.delete()
        except Exception:
            pass
        logger.warning('Removed stale started-job marker for non-started job %s', job_id)
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
        logger.warning(
            'Removed stale started job %s after %.0fs without heartbeat',
            job_id,
            heartbeat_age,
        )
        return True

    if last_heartbeat is None and started_age is not None and started_age > _STALE_STARTED_JOB_GRACE_SECONDS:
        registry.remove(job)
        try:
            job.delete()
        except Exception:
            pass
        logger.warning(
            'Removed stale started job %s after %.0fs without heartbeat metadata',
            job_id,
            started_age,
        )
        return True

    return False


def _job_already_active(q: Queue, job_id: str) -> bool:
    if not job_id:
        return False
    _cleanup_stale_started_job(q, job_id)
    registries = (
        q.get_job_ids(),
        StartedJobRegistry(q.name, connection=q.connection).get_job_ids(),
    )
    for job_ids in registries:
        if job_id in job_ids:
            return True
    try:
        job = Job.fetch(job_id, connection=q.connection)
    except Exception:
        return False
    status = job.get_status(refresh=False)
    # Job hash exists with 'started' status but is not in any registry — zombie from
    # a dead worker that was already removed from StartedJobRegistry. Delete it so a
    # fresh job can be enqueued.
    if status == 'started':
        try:
            job.delete()
        except Exception:
            pass
        logger.warning('Deleted zombie job %s (started status, not in any registry)', job_id)
        return False
    return status in {'queued', 'deferred', 'scheduled'}


def _bulk_job_id(filters: dict, enable: bool) -> str:
    payload = {
        'action': 'enable' if enable else 'disable',
        'filters': filters or {},
    }
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha1(raw.encode('utf-8')).hexdigest()[:16]
    return f'channel-bulk-{digest}'


def _bulk_review_job_id(filters: dict) -> str:
    payload = {
        'action': 'mark_reviewed',
        'filters': filters or {},
    }
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha1(raw.encode('utf-8')).hexdigest()[:16]
    return f'channel-review-bulk-{digest}'


def cancel_source_jobs(source_name: str) -> list[str]:
    """Cancel queued/scheduled scraper jobs for a source.

    This is used when a source is disabled so a pending scrape/audit cannot
    run later and repopulate data.
    """
    try:
        q = get_queue()
    except Exception as e:
        logger.warning('RQ unavailable while cancelling jobs for %s: %s', source_name, e)
        return []

    canceled: list[str] = []
    for job_id in (
        f'scrape-{source_name}',
        f'scrape-{source_name}-force-full',
        f'audit-{source_name}',
        f'audit-recheck-{source_name}',
    ):
        try:
            job = Job.fetch(job_id, connection=q.connection)
        except Exception:
            continue

        status = job.get_status(refresh=False)
        if status not in {'queued', 'deferred', 'scheduled'}:
            continue

        try:
            job.delete()
            canceled.append(job_id)
            logger.info('Canceled %s job for disabled source %s', job_id, source_name)
        except Exception as e:
            logger.warning('Failed to cancel %s for disabled source %s: %s', job_id, source_name, e)

    return canceled


def trigger_scrape(source_name: str, *, force_full: bool = False):
    try:
        q = get_queue()
        job_id = f'scrape-{source_name}'
        force_job_id = f'{job_id}-force-full'
        base_active = _job_already_active(q, job_id)
        force_active = _job_already_active(q, force_job_id)
        if base_active or force_active:
            if force_full and not force_active:
                q.enqueue(
                    'app.worker.run_scraper',
                    source_name,
                    True,
                    job_timeout=3600,
                    job_id=force_job_id,
                )
                logger.info('Enqueued follow-up force-full scrape for %s', source_name)
                return
            logger.info('Scrape already queued/running for %s', source_name)
            return
        q.enqueue('app.worker.run_scraper', source_name, force_full, job_timeout=3600, job_id=job_id)
        logger.info('Enqueued scrape for %s%s', source_name, ' (force full)' if force_full else '')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for {source_name}')
        import threading
        from app.worker import run_scraper
        threading.Thread(target=run_scraper, args=(source_name, force_full), daemon=True).start()


def trigger_stream_audit(source_name: str):
    try:
        q = get_queue()
        job_id = f'audit-{source_name}'
        if _job_already_active(q, job_id):
            logger.info('Stream audit already queued/running for %s', source_name)
            return
        q.enqueue('app.worker.run_stream_audit', source_name, job_timeout=1800, job_id=job_id)
        logger.info(f'Enqueued stream audit for {source_name}')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for {source_name}')
        import threading
        from app.worker import run_stream_audit
        threading.Thread(target=run_stream_audit, args=(source_name,), daemon=True).start()


def trigger_stream_audit_recheck(source_name: str, channel_ids: list):
    try:
        q = get_queue()
        job_id = f'audit-recheck-{source_name}'
        if _job_already_active(q, job_id):
            logger.info('Stream audit recheck already queued/running for %s', source_name)
            return
        q.enqueue('app.worker.run_stream_audit_recheck', source_name, channel_ids, job_timeout=600, job_id=job_id)
        logger.info(f'Enqueued stream audit recheck for {source_name} ({len(channel_ids)} channels)')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for {source_name}')
        import threading
        from app.worker import run_stream_audit_recheck
        threading.Thread(target=run_stream_audit_recheck, args=(source_name, channel_ids), daemon=True).start()


def trigger_xml_refresh() -> bool:
    """Returns True if a job was enqueued, False if skipped due to debounce."""
    try:
        q = get_fast_queue()
        job_id = 'xml-refresh'
        # Atomic debounce: SET NX with 10s TTL prevents the check-then-enqueue
        # race when many feed requests arrive simultaneously (e.g. DVR polling).
        acquired = q.connection.set(f'lock:{job_id}', '1', nx=True, ex=10)
        if not acquired:
            logger.info('XML artifact refresh already queued/running')
            return False
        if _job_already_active(q, job_id):
            logger.info('XML artifact refresh already queued/running')
            return False
        q.enqueue('app.worker.run_xml_refresh', job_timeout=1800, job_id=job_id)
        logger.info('Enqueued XML artifact refresh')
        return True
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for XML refresh')
        import threading
        from app.worker import run_xml_refresh
        threading.Thread(target=run_xml_refresh, daemon=True).start()
        return True


def trigger_xml_refresh_catchup() -> None:
    """Enqueue a catch-up XML refresh using a separate job ID so it queues behind
    any currently-running xml-refresh job.  Used when a new feed is created while
    a refresh is already in-flight (which won't include the new feed)."""
    try:
        q = get_fast_queue()
        job_id = 'xml-refresh-catchup'
        if _job_already_active(q, job_id):
            logger.info('XML artifact catch-up refresh already queued')
            return
        q.enqueue('app.worker.run_xml_refresh', job_timeout=1800, job_id=job_id)
        logger.info('Enqueued XML artifact catch-up refresh')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), skipping catch-up XML refresh: {e}')


def trigger_source_disable(source_id: int) -> bool:
    """Returns True if a disable job was enqueued, False if one is already
    queued/running for this source (e.g. a double-click)."""
    try:
        q = get_fast_queue()
        job_id = f'source-disable-{source_id}'
        if _job_already_active(q, job_id):
            logger.info('Source disable already queued/running for source_id=%s', source_id)
            return False
        q.enqueue('app.worker.run_source_disable', source_id, job_timeout=120, job_id=job_id)
        logger.info('Enqueued source disable for source_id=%s', source_id)
        return True
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for source disable {source_id}')
        import threading
        from app.worker import run_source_disable
        threading.Thread(target=run_source_disable, args=(source_id,), daemon=True).start()
        return True


def get_source_disable_status(source_id: int) -> dict:
    """For the admin UI's disable-status poll (see trigger_source_disable).

    Returns {'status': 'pending'|'done'|'error', 'error': str|None}.
    'error' means the job actually raised — distinct from 'done', which
    previously covered both "genuinely finished" and "failed/vanished",
    silently reporting success either way (code review, 2026-08-14).

    Known gap: the raw-thread fallback used when RQ/Redis itself is
    unavailable isn't visible to any of this — same blind spot every other
    trigger_*'s thread fallback already has, not specific to this endpoint.
    """
    job_id = f'source-disable-{source_id}'
    try:
        q = get_fast_queue()
    except Exception:
        return {'status': 'done', 'error': None}
    if _job_already_active(q, job_id):
        return {'status': 'pending', 'error': None}
    try:
        from rq.registry import FailedJobRegistry
        failed_ids = FailedJobRegistry(q.name, connection=q.connection).get_job_ids()
        if job_id in failed_ids:
            job = Job.fetch(job_id, connection=q.connection)
            return {'status': 'error', 'error': (job.exc_info or '').strip().splitlines()[-1] if job.exc_info else 'Disable job failed.'}
    except Exception:
        pass
    return {'status': 'done', 'error': None}


def cancel_pending_source_disable(source_id: int) -> bool:
    """Cancel a still-QUEUED run_source_disable job for this source, if any.

    Called when a source is re-enabled — without this, a disable queued
    moments earlier (still waiting behind a busy queue) can run AFTER the
    re-enable commits, see is_enabled back to True (nothing marks it stale),
    and silently flip it back off + purge the channels the user just chose
    to keep (code review, 2026-08-14). Same idiom as cancel_source_jobs.

    Only helps if the job hasn't started yet — same residual gap
    cancel_source_jobs already accepts elsewhere: a job already past
    run_source_disable's own is_enabled check by the time this runs can't be
    stopped here. That window is milliseconds wide in practice.
    """
    try:
        q = get_fast_queue()
        job = Job.fetch(f'source-disable-{source_id}', connection=q.connection)
    except Exception:
        return False
    if job.get_status(refresh=False) not in {'queued', 'deferred', 'scheduled'}:
        return False
    try:
        job.delete()
        logger.info('Canceled pending source-disable job for source_id=%s (re-enabled)', source_id)
        return True
    except Exception as e:
        logger.warning('Failed to cancel pending source-disable for source_id=%s: %s', source_id, e)
        return False


def trigger_source_channel_purge(source_id: int):
    try:
        q = get_maintenance_queue()
        job_id = f'source-purge-{source_id}'
        if _job_already_active(q, job_id):
            logger.info('Source channel purge already queued/running for source_id=%s', source_id)
            return
        q.enqueue('app.worker.run_source_channel_purge', source_id, job_timeout=1800, job_id=job_id)
        logger.info('Enqueued source channel purge for source_id=%s', source_id)
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for source purge {source_id}')
        import threading
        from app.worker import run_source_channel_purge
        threading.Thread(target=run_source_channel_purge, args=(source_id,), daemon=True).start()


def trigger_bulk_channel_update(filters: dict, enable: bool):
    try:
        q = get_maintenance_queue()
        job_id = _bulk_job_id(filters or {}, enable)
        if _job_already_active(q, job_id):
            logger.info(
                'Bulk channel %s already queued/running',
                'enable' if enable else 'disable',
            )
            return
        q.enqueue(
            'app.worker.run_bulk_channel_update',
            filters or {},
            enable,
            job_timeout=1800,
            job_id=job_id,
        )
        logger.info('Enqueued bulk channel %s', 'enable' if enable else 'disable')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for bulk channel update')
        import threading
        from app.worker import run_bulk_channel_update
        threading.Thread(target=run_bulk_channel_update, args=(filters or {}, enable), daemon=True).start()


def trigger_bulk_channel_review(filters: dict):
    try:
        q = get_maintenance_queue()
        job_id = _bulk_review_job_id(filters or {})
        if _job_already_active(q, job_id):
            logger.info('Bulk channel review already queued/running')
            return
        q.enqueue(
            'app.worker.run_bulk_channel_review',
            filters or {},
            job_timeout=1800,
            job_id=job_id,
        )
        logger.info('Enqueued bulk channel review')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for bulk channel review')
        import threading
        from app.worker import run_bulk_channel_review
        threading.Thread(target=run_bulk_channel_review, args=(filters or {},), daemon=True).start()


def trigger_gracenote_auto_clear():
    try:
        q = get_maintenance_queue()
        job_id = 'gracenote-auto-clear'
        if _job_already_active(q, job_id):
            logger.info('Gracenote auto-clear already queued/running')
            return
        q.enqueue('app.worker.run_gracenote_auto_clear', job_timeout=300, job_id=job_id)
        logger.info('Enqueued gracenote auto-clear')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for gracenote auto-clear')
        import threading
        from app.worker import run_gracenote_auto_clear
        threading.Thread(target=run_gracenote_auto_clear, daemon=True).start()


def trigger_gracenote_clear_all():
    try:
        q = get_maintenance_queue()
        job_id = 'gracenote-clear-all'
        if _job_already_active(q, job_id):
            logger.info('Gracenote clear-all already queued/running')
            return
        q.enqueue('app.worker.run_gracenote_clear_all', job_timeout=300, job_id=job_id)
        logger.info('Enqueued gracenote clear-all')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for gracenote clear-all')
        import threading
        from app.worker import run_gracenote_clear_all
        threading.Thread(target=run_gracenote_clear_all, daemon=True).start()


def trigger_channel_auto_disable(channel_id: int, reason: str):
    try:
        q = get_fast_queue()
        job_id = f'channel-auto-disable-{channel_id}'
        if _job_already_active(q, job_id):
            logger.info('Channel auto-disable already queued/running for channel_id=%s', channel_id)
            return
        q.enqueue('app.worker.run_channel_auto_disable', channel_id, reason, job_timeout=300, job_id=job_id)
        logger.info('Enqueued channel auto-disable for channel_id=%s', channel_id)
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for channel auto-disable {channel_id}')
        import threading
        from app.worker import run_channel_auto_disable
        threading.Thread(target=run_channel_auto_disable, args=(channel_id, reason), daemon=True).start()


def trigger_sling_browser_login():
    """Returns True if a job was enqueued, False if one is already running."""
    try:
        q = get_fast_queue()
        job_id = 'sling-browser-login'
        if _job_already_active(q, job_id):
            logger.info('Sling browser login already running')
            return False
        q.enqueue('app.worker.run_sling_browser_login', job_timeout=1830, job_id=job_id)
        logger.info('Enqueued Sling browser login')
        return True
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for Sling browser login')
        import threading
        from app.worker import run_sling_browser_login
        # Sling's browser login locks a persistent Camoufox profile dir, so two
        # concurrent runs collide. The RQ path dedups via job_id; this fallback
        # has no queue to check, so dedup on a named thread instead.
        thread_name = 'sling-browser-login-fallback'
        if any(t.name == thread_name and t.is_alive() for t in threading.enumerate()):
            logger.info('Sling browser login fallback thread already running')
            return False
        threading.Thread(target=run_sling_browser_login, daemon=True, name=thread_name).start()
        return True


def stop_sling_browser_login():
    try:
        r = redis.from_url(current_app.config['REDIS_URL'])
        r.setex('sling:browser-login:stop', 30, '1')
    except Exception as e:
        logger.warning(f'Failed to signal Sling browser login stop: {e}')


# legacy/AMC/Discovery/NBC/FOX browser-login jobs are separate RQ job_ids
# (so their modals can poll independent redis-key namespaces — see the "NBC
# TVE browser sign-in" comment below) but they all drive Camoufox against the
# SAME on-disk profile dir, /data/browser_profiles/mvpd_tve (app/worker.py).
# Only one Firefox process can hold that profile at a time, so every trigger_*
# below must check ALL of these job_ids before enqueueing, not just its own —
# otherwise two families queue up back-to-back and the second one silently
# waits (up to job_timeout) for the first to release the shared profile.
_MVPD_TVE_PROFILE_JOB_IDS = ('mvpd-browser-login', 'nbc-mvpd-browser-login', 'fox-mvpd-browser-login')
_MVPD_TVE_PROFILE_THREAD_NAMES = (
    'mvpd-browser-login-fallback', 'nbc-mvpd-browser-login-fallback', 'fox-mvpd-browser-login-fallback',
)


def _mvpd_tve_profile_busy(q: Queue) -> bool:
    return any(_job_already_active(q, jid) for jid in _MVPD_TVE_PROFILE_JOB_IDS)


def _force_kill_mvpd_browser() -> None:
    """Actually kill the Camoufox process for the shared mvpd_tve profile,
    rather than just setting the soft stop-flag and hoping the job's own
    poll loop notices it soon.

    Confirmed live (2026-08-10): a job can be legitimately still running —
    not crashed, heartbeat still fresh — while genuinely stuck (e.g. the
    primary human-driven phase waiting on a login page that never resolves
    the way the modal implied it would). The stop-flag ALONE only helps if
    the job's Python code happens to be at a point where it checks that key;
    if it's blocked inside a lower-level Playwright/network call, nothing
    checks the flag until that call returns — which, for the primary loop's
    job_timeout, can be up to 1830s (its docstring/log line literally says
    "had to wait for it to time out").

    Killing the browser process forces whatever blocking call is in flight
    to fail immediately, which raises inside the job — and its own exception
    handler already checks the stop-flag before deciding whether to retry
    (see run_mvpd_browser_login's `not r.exists(MVPD_BROWSER_LOGIN_STOP_KEY)`
    guard), so setting the flag FIRST and killing the process SECOND means
    the job sees "stopped" and exits cleanly instead of relaunching a new
    browser and continuing the wait. Matches on 'mvpd_tve' specifically (the
    profile dir name) so it can't ever touch Sling's separate browser/profile.
    """
    import subprocess
    try:
        subprocess.run(['pkill', '-9', '-f', 'mvpd_tve'], timeout=5, check=False)
    except Exception as e:
        logger.warning(f'Failed to force-kill MVPD browser process: {e}')


def _mvpd_tve_profile_busy_fallback() -> bool:
    import threading
    names = set(_MVPD_TVE_PROFILE_THREAD_NAMES)
    return any(t.name in names and t.is_alive() for t in threading.enumerate())


def trigger_mvpd_browser_login(requestor_id: str, resource: str, software_statement: str, redirect_url: str, mso_id: str):
    """Returns True if a job was enqueued, False if one is already running.

    Signs in only requestor_id — useful for re-testing/debugging one
    specific network without disturbing the others' cached sign-ins. The
    admin UI's "Sign in to all" calls this once per network in a client-side
    loop (settings.js's signInToAllTve) rather than sweeping them server-side
    in one browser session.
    """
    try:
        q = get_fast_queue()
        job_id = 'mvpd-browser-login'
        if _mvpd_tve_profile_busy(q):
            # DEBUG, not INFO: the batch "Sign in to all" loop (settings.js's
            # signInToAllTve) hits this constantly by design while waiting
            # for a slow prior step (e.g. AMCN's ~30s run) to release the
            # shared profile lock, retrying once a second — at INFO that
            # single expected wait floods the log with ~30 near-identical
            # lines (reported live 2026-08-14).
            logger.debug('MVPD browser login already running')
            return False
        q.enqueue(
            'app.worker.run_mvpd_browser_login',
            requestor_id, resource, software_statement, redirect_url, mso_id,
            job_timeout=1830, job_id=job_id,
        )
        logger.info('Enqueued MVPD browser login for requestor_id=%s mso_id=%s', requestor_id, mso_id)
        return True
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for MVPD browser login')
        import threading
        from app.worker import run_mvpd_browser_login
        # Same persistent-profile-dir collision concern as Sling's fallback.
        thread_name = 'mvpd-browser-login-fallback'
        if _mvpd_tve_profile_busy_fallback():
            logger.info('MVPD browser login fallback thread already running')
            return False
        threading.Thread(
            target=run_mvpd_browser_login,
            args=(requestor_id, resource, software_statement, redirect_url, mso_id),
            daemon=True, name=thread_name,
        ).start()
        return True


def stop_mvpd_browser_login():
    try:
        r = redis.from_url(current_app.config['REDIS_URL'])
        r.setex('mvpd:browser-login:stop', 30, '1')
    except Exception as e:
        logger.warning(f'Failed to signal MVPD browser login stop: {e}')
    _force_kill_mvpd_browser()


def trigger_nbc_browser_login(mso_id: str):
    """Returns True if a job was enqueued, False if one is already running."""
    try:
        q = get_fast_queue()
        job_id = 'nbc-mvpd-browser-login'
        if _mvpd_tve_profile_busy(q):
            logger.debug('NBC MVPD browser login already running')  # see trigger_mvpd_browser_login
            return False
        q.enqueue('app.worker.run_nbc_browser_login', mso_id, job_timeout=1830, job_id=job_id)
        logger.info('Enqueued NBC MVPD browser login for mso_id=%s', mso_id)
        return True
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for NBC MVPD browser login')
        import threading
        from app.worker import run_nbc_browser_login
        thread_name = 'nbc-mvpd-browser-login-fallback'
        if _mvpd_tve_profile_busy_fallback():
            logger.info('NBC MVPD browser login fallback thread already running')
            return False
        threading.Thread(target=run_nbc_browser_login, args=(mso_id,), daemon=True, name=thread_name).start()
        return True


def stop_nbc_browser_login():
    try:
        r = redis.from_url(current_app.config['REDIS_URL'])
        r.setex('nbc-mvpd:browser-login:stop', 30, '1')
    except Exception as e:
        logger.warning(f'Failed to signal NBC MVPD browser login stop: {e}')
    _force_kill_mvpd_browser()


def trigger_fox_browser_login(mso_id: str):
    """Returns True if a job was enqueued, False if one is already running."""
    try:
        q = get_fast_queue()
        job_id = 'fox-mvpd-browser-login'
        if _mvpd_tve_profile_busy(q):
            logger.debug('FOX MVPD browser login already running')  # see trigger_mvpd_browser_login
            return False
        q.enqueue('app.worker.run_fox_browser_login', mso_id, job_timeout=1830, job_id=job_id)
        logger.info('Enqueued FOX MVPD browser login for mso_id=%s', mso_id)
        return True
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for FOX MVPD browser login')
        import threading
        from app.worker import run_fox_browser_login
        thread_name = 'fox-mvpd-browser-login-fallback'
        if _mvpd_tve_profile_busy_fallback():
            logger.info('FOX MVPD browser login fallback thread already running')
            return False
        threading.Thread(target=run_fox_browser_login, args=(mso_id,), daemon=True, name=thread_name).start()
        return True


def stop_fox_browser_login():
    try:
        r = redis.from_url(current_app.config['REDIS_URL'])
        r.setex('fox-mvpd:browser-login:stop', 30, '1')
    except Exception as e:
        logger.warning(f'Failed to signal FOX MVPD browser login stop: {e}')
    _force_kill_mvpd_browser()


# AMC Networks TVE / Discovery TVE standalone sign-in. Both do a fully
# scripted Cox login now (app.worker.run_amcn_browser_login/
# run_discovery_browser_login — no browser), but still reuse the legacy
# flow's job_id and redis keys (mvpd:browser-login:*), so there's no
# dedicated stop_*/state route for either; the existing
# /api/settings/tve/browser-login/{state,input,stop} endpoints already work.

def trigger_amcn_browser_login(mso_id: str):
    """Returns True if a job was enqueued, False if one is already running."""
    try:
        q = get_fast_queue()
        job_id = 'mvpd-browser-login'
        if _mvpd_tve_profile_busy(q):
            logger.debug('MVPD browser login already running')  # see trigger_mvpd_browser_login
            return False
        q.enqueue('app.worker.run_amcn_browser_login', mso_id, job_timeout=1830, job_id=job_id)
        logger.info('Enqueued AMC Networks TVE browser login for mso_id=%s', mso_id)
        return True
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for AMC Networks TVE browser login')
        import threading
        from app.worker import run_amcn_browser_login
        thread_name = 'mvpd-browser-login-fallback'
        if _mvpd_tve_profile_busy_fallback():
            logger.info('MVPD browser login fallback thread already running')
            return False
        threading.Thread(target=run_amcn_browser_login, args=(mso_id,), daemon=True, name=thread_name).start()
        return True


def trigger_discovery_browser_login(mso_id: str):
    """Returns True if a job was enqueued, False if one is already running."""
    try:
        q = get_fast_queue()
        job_id = 'mvpd-browser-login'
        if _mvpd_tve_profile_busy(q):
            logger.debug('MVPD browser login already running')  # see trigger_mvpd_browser_login
            return False
        q.enqueue('app.worker.run_discovery_browser_login', mso_id, job_timeout=1830, job_id=job_id)
        logger.info('Enqueued Discovery TVE browser login for mso_id=%s', mso_id)
        return True
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for Discovery TVE browser login')
        import threading
        from app.worker import run_discovery_browser_login
        thread_name = 'mvpd-browser-login-fallback'
        if _mvpd_tve_profile_busy_fallback():
            logger.info('MVPD browser login fallback thread already running')
            return False
        threading.Thread(target=run_discovery_browser_login, args=(mso_id,), daemon=True, name=thread_name).start()
        return True


def trigger_tvtv_cache_refresh() -> str:
    """Returns 'queued', 'deferred' (scraper work active), or 'active'
    (refresh already queued/running)."""
    try:
        from app.worker import _any_scrapes_active
        if _any_scrapes_active():
            # The scheduled 3am run defers for the same reason: the tvtv
            # refresh writes on the maintenance queue's own connection, and a
            # concurrent scrape (scraper queue, separate process/connection)
            # can hold SQLite's single writer lock past our busy_timeout,
            # surfacing as 'database is locked'. Manual triggers hit this too
            # if fired while a scrape is running.
            logger.info('tvtv cache refresh deferred: scraper work active')
            return 'deferred'
        q = get_maintenance_queue()
        job_id = 'tvtv-cache-refresh'
        if _job_already_active(q, job_id):
            logger.info('tvtv cache refresh already queued/running')
            return 'active'
        q.enqueue('app.worker.run_tvtv_cache_refresh', job_timeout=1800, job_id=job_id)
        logger.info('Enqueued tvtv cache refresh')
        return 'queued'
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for tvtv cache refresh')
        import threading
        from app.worker import run_tvtv_cache_refresh
        threading.Thread(target=run_tvtv_cache_refresh, daemon=True).start()
        return 'queued'

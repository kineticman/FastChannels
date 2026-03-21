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
        logger.warning('Removed stale started-job marker for non-started job %s', job_id)
        return True

    now = datetime.now(timezone.utc)
    started_at = _utc_aware(getattr(job, 'started_at', None))
    last_heartbeat = _utc_aware(getattr(job, 'last_heartbeat', None))
    heartbeat_age = (now - last_heartbeat).total_seconds() if last_heartbeat else None
    started_age = (now - started_at).total_seconds() if started_at else None

    if heartbeat_age is not None and heartbeat_age > _STALE_STARTED_JOB_GRACE_SECONDS:
        registry.remove(job)
        logger.warning(
            'Removed stale started job %s after %.0fs without heartbeat',
            job_id,
            heartbeat_age,
        )
        return True

    if last_heartbeat is None and started_age is not None and started_age > _STALE_STARTED_JOB_GRACE_SECONDS:
        registry.remove(job)
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
    return job.get_status(refresh=False) in {'queued', 'started', 'deferred', 'scheduled'}


def _bulk_job_id(filters: dict, enable: bool) -> str:
    payload = {
        'action': 'enable' if enable else 'disable',
        'filters': filters or {},
    }
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha1(raw.encode('utf-8')).hexdigest()[:16]
    return f'channel-bulk-{digest}'


def trigger_scrape(source_name: str, *, force_full: bool = False):
    try:
        q = get_queue()
        job_id = f'scrape-{source_name}'
        if _job_already_active(q, job_id):
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
        get_queue().enqueue('app.worker.run_stream_audit', source_name, job_timeout=1800)
        logger.info(f'Enqueued stream audit for {source_name}')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for {source_name}')
        import threading
        from app.worker import run_stream_audit
        threading.Thread(target=run_stream_audit, args=(source_name,), daemon=True).start()


def trigger_xml_refresh():
    try:
        q = get_fast_queue()
        job_id = 'xml-refresh'
        if _job_already_active(q, job_id):
            logger.info('XML artifact refresh already queued/running')
            return
        q.enqueue('app.worker.run_xml_refresh', job_timeout=1800, job_id=job_id)
        logger.info('Enqueued XML artifact refresh')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for XML refresh')
        import threading
        from app.worker import run_xml_refresh
        threading.Thread(target=run_xml_refresh, daemon=True).start()


def trigger_source_channel_purge(source_id: int):
    try:
        q = get_queue()
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
        q = get_queue()
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

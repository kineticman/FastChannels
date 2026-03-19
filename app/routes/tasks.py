import logging
from flask import current_app
import redis
from rq import Queue
from rq.job import Job
from rq.registry import StartedJobRegistry

logger = logging.getLogger(__name__)


def get_queue():
    r = redis.from_url(current_app.config['REDIS_URL'])
    return Queue('scraper', connection=r)


def _job_already_active(q: Queue, job_id: str) -> bool:
    if not job_id:
        return False
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


def trigger_scrape(source_name: str):
    try:
        get_queue().enqueue('app.worker.run_scraper', source_name, job_timeout=3600)
        logger.info(f'Enqueued scrape for {source_name}')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for {source_name}')
        import threading
        from app.worker import run_scraper
        threading.Thread(target=run_scraper, args=(source_name,), daemon=True).start()


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
        q = get_queue()
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
        get_queue().enqueue('app.worker.run_bulk_channel_update', filters or {}, enable, job_timeout=1800)
        logger.info('Enqueued bulk channel %s', 'enable' if enable else 'disable')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for bulk channel update')
        import threading
        from app.worker import run_bulk_channel_update
        threading.Thread(target=run_bulk_channel_update, args=(filters or {}, enable), daemon=True).start()


def trigger_channel_auto_disable(channel_id: int, reason: str):
    try:
        q = get_queue()
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

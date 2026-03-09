import logging
from flask import current_app
import redis
from rq import Queue

logger = logging.getLogger(__name__)


def get_queue():
    r = redis.from_url(current_app.config['REDIS_URL'])
    return Queue('scraper', connection=r)


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

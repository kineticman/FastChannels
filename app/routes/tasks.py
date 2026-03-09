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


def trigger_drm_check(source_name: str):
    try:
        get_queue().enqueue('app.worker.run_drm_check', source_name, job_timeout=1800)
        logger.info(f'Enqueued DRM check for {source_name}')
    except Exception as e:
        logger.warning(f'RQ unavailable ({e}), falling back to thread for {source_name}')
        import threading
        from app.worker import run_drm_check
        threading.Thread(target=run_drm_check, args=(source_name,), daemon=True).start()

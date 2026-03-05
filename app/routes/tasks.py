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
        get_queue().enqueue('app.worker.run_scraper', source_name, job_timeout=600)
        logger.info(f'Enqueued scrape for {source_name}')
    except Exception as e:
        logger.error(f'Failed to enqueue {source_name}: {e}')

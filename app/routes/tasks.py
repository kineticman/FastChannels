import logging
import threading

from flask import current_app

logger = logging.getLogger(__name__)


def trigger_scrape(source_name: str):
    """
    Enqueue a scrape job via RQ if Redis is available,
    otherwise run it in a background thread directly.
    This makes 'Run Now' work even when Redis is starting up
    or unavailable.
    """
    # Try RQ first
    try:
        import redis
        from rq import Queue
        r = redis.from_url(current_app.config['REDIS_URL'], socket_connect_timeout=2)
        r.ping()  # fast check — raises if Redis is down
        Queue('scraper', connection=r).enqueue(
            'app.worker.run_scraper', source_name, job_timeout=600
        )
        logger.info(f'[tasks] Enqueued {source_name} via RQ')
        return
    except Exception as e:
        logger.warning(f'[tasks] RQ unavailable ({e}), falling back to thread for {source_name}')

    # Fallback: run directly in a daemon thread
    app = current_app._get_current_object()

    def _run():
        with app.app_context():
            try:
                from app.worker import run_scraper
                run_scraper(source_name)
            except Exception as ex:
                logger.error(f'[tasks] Thread scrape failed for {source_name}: {ex}')

    t = threading.Thread(target=_run, daemon=True, name=f'scrape-{source_name}')
    t.start()
    logger.info(f'[tasks] Started background thread for {source_name}')

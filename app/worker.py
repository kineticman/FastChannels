"""
Background worker — run with: python -m app.worker
"""
import logging
import sys
from datetime import datetime, timezone

import redis
from rq import Worker, Queue, Connection

from app import create_app
from app.extensions import db
from app.models import Source, Channel, Program
from app.scrapers import registry

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

flask_app = create_app()


def run_scraper(source_name: str):
    with flask_app.app_context():
        source = Source.query.filter_by(name=source_name).first()
        if not source:
            logger.error(f'Source not found: {source_name}')
            return

        scraper_cls = registry.get(source_name)
        if not scraper_cls:
            source.last_error = f'No scraper registered for {source_name}'
            db.session.commit()
            return

        try:
            scraper = scraper_cls(config=source.config or {})
            channels, programs = scraper.run()
            _upsert_channels(source, channels)
            _upsert_programs(source, programs)
            source.last_scraped_at = datetime.now(timezone.utc)
            source.last_error      = None
            db.session.commit()
            logger.info(f'[{source_name}] Done — {len(channels)} channels, {len(programs)} programs')


        except Exception as e:
            logger.exception(f'[{source_name}] Scrape failed')
            source.last_error = str(e)
            db.session.commit()


def _upsert_channels(source, channel_data_list):
    existing = {ch.source_channel_id: ch for ch in source.channels.all()}
    for cd in channel_data_list:
        ch = existing.get(cd.source_channel_id)
        if ch:
            ch.name        = cd.name
            ch.stream_url  = cd.stream_url
            ch.stream_type = cd.stream_type
            ch.logo_url    = cd.logo_url
            ch.slug        = cd.slug
            ch.category    = cd.category
            ch.number      = cd.number
            ch.is_active   = True
        else:
            db.session.add(Channel(
                source_id=source.id,
                source_channel_id=cd.source_channel_id,
                name=cd.name, stream_url=cd.stream_url, stream_type=cd.stream_type,
                logo_url=cd.logo_url, slug=cd.slug, category=cd.category,
                language=cd.language, country=cd.country, number=cd.number,
            ))
    seen = {cd.source_channel_id for cd in channel_data_list}
    for ch_id, ch in existing.items():
        if ch_id not in seen:
            ch.is_active = False
    db.session.flush()


def _upsert_programs(source, program_data_list):
    if not program_data_list:
        return
    channels = {ch.source_channel_id: ch for ch in source.channels.all()}
    for pd in program_data_list:
        ch = channels.get(pd.source_channel_id)
        if not ch:
            continue
        db.session.add(Program(
            channel_id=ch.id, title=pd.title, description=pd.description,
            start_time=pd.start_time, end_time=pd.end_time,
            poster_url=pd.poster_url, category=pd.category, rating=pd.rating,
            episode_title=pd.episode_title, season=pd.season, episode=pd.episode,
        ))
    db.session.flush()


def seed_sources():
    with flask_app.app_context():
        scrapers = registry.get_all()
        for name, cls in scrapers.items():
            if not Source.query.filter_by(name=name).first():
                db.session.add(Source(
                    name=name,
                    display_name=cls.display_name or name.title(),
                    scrape_interval=cls.scrape_interval,
                    config={},
                ))
        db.session.commit()
        logger.info(f'Seeded {len(scrapers)} sources')


if __name__ == '__main__':
    seed_sources()
    r = redis.from_url(flask_app.config['REDIS_URL'])
    with Connection(r):
        worker = Worker(queues=[Queue('scraper', connection=r)])
        logger.info('Worker listening on queue: scraper')
        worker.work()


def run_drm_check(source_name: str):
    """
    Iterate all active channels for a source and check each for DRM encryption
    by hitting the /play/ proxy endpoint — the same URL IPTV clients use.

    Following the 302 redirect lands on the real manifest, which we scan for
    METHOD=SAMPLE-AES / METHOD=AES-128. This reuses all existing resolve() logic
    without duplicating it and works the same way for every source.

    Runs as a background RQ job — triggered via POST /api/sources/<id>/drm-check.
    """
    import time as _time
    import requests as _requests

    _DRM_METHODS = ('SAMPLE-AES',)  # AES-128 with key URL is standard HLS, not FairPlay

    with flask_app.app_context():
        source = Source.query.filter_by(name=source_name).first()
        if not source:
            logger.error('[drm-check] source not found: %s', source_name)
            return

        channels = source.channels.filter_by(is_active=True).all()
        total    = len(channels)
        checked  = 0
        flagged  = 0
        errors   = 0

        # Same host the worker is running on
        base_url = flask_app.config.get('BASE_URL', 'http://localhost:5523')

        logger.info('[drm-check] %s: checking %d channels via play proxy…', source_name, total)

        # Brief warmup pause — gives any residual rate-limit ban time to clear
        _time.sleep(5)

        sess = _requests.Session()
        sess.headers['User-Agent'] = 'FastChannels-DRMCheck/1.0'

        consecutive_errors = 0

        for i, ch in enumerate(channels):
            # Skip malformed IDs that would break the URL (e.g. Roku w. playlist tokens)
            if len(ch.source_channel_id) > 128 or '/' in ch.source_channel_id:
                logger.debug('[drm-check] skipping bad channel ID: %s', ch.source_channel_id[:40])
                errors += 1
                continue

            play_url = f'{base_url}/play/{source_name}/{ch.source_channel_id}.m3u8'
            try:
                r = sess.get(play_url, timeout=15, allow_redirects=True)

                if r.status_code in (403, 429, 503):
                    # Rate limited — back off and retry once
                    wait = 30
                    logger.warning('[drm-check] %s rate-limited (%d), backing off %ds…',
                                   source_name, r.status_code, wait)
                    _time.sleep(wait)
                    r = sess.get(play_url, timeout=15, allow_redirects=True)

                if r.status_code != 200:
                    logger.debug('[drm-check] %d for %s', r.status_code, ch.name)
                    errors += 1
                    consecutive_errors += 1
                    if consecutive_errors >= 20:
                        logger.error('[drm-check] %s: 20 consecutive errors — aborting. '
                                     'Source may be rate-limiting or down.', source_name)
                        break
                    continue

                consecutive_errors = 0
                checked += 1
                manifest_text = r.text
                manifest_url  = r.url

                # If this is a master playlist, fetch the first variant to get
                # the media playlist — EXT-X-KEY only appears there, not in master
                if '#EXT-X-STREAM-INF' in manifest_text:
                    from urllib.parse import urljoin as _urljoin
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
                                logger.debug('[drm-check] variant fetched for %s (%d bytes)',
                                             ch.name, len(manifest_text))
                            else:
                                logger.debug('[drm-check] variant returned %d for %s',
                                             rv.status_code, ch.name)
                        except Exception as ve:
                            logger.debug('[drm-check] variant fetch failed for %s: %s', ch.name, ve)
                    else:
                        logger.debug('[drm-check] no variant URL found in master for %s', ch.name)

                # Log a snippet of the manifest for known-DRM channels to aid debugging
                if 'universal' in ch.name.lower() or 'movies' in ch.name.lower():
                    key_lines = [l for l in manifest_text.splitlines() if 'KEY' in l or 'METHOD' in l]
                    logger.info('[drm-check] DEBUG %s key lines: %s', ch.name, key_lines or ['(none)'])

                drm = any(f'METHOD={m}' in manifest_text for m in _DRM_METHODS)
                if drm:
                    ch.is_active      = False
                    ch.disable_reason = 'DRM'
                    flagged += 1
                    logger.info('[drm-check] 🔒 DRM: %s  →  %s', ch.name, manifest_url[:80])

            except Exception as e:
                logger.debug('[drm-check] error for %s: %s', ch.name, e)
                errors += 1
                continue

            if (i + 1) % 25 == 0:
                db.session.flush()
                logger.info('[drm-check] %s: %d/%d — checked=%d flagged=%d errors=%d',
                            source_name, i + 1, total, checked, flagged, errors)

            _time.sleep(0.3)  # slightly more polite to avoid rate limiting

        db.session.commit()
        logger.info('[drm-check] %s: done — total=%d checked=%d flagged=%d errors=%d',
                    source_name, total, checked, flagged, errors)

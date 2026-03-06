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

        # Extract gracenote_id from ChannelData if the scraper set it directly,
        # or fall back to the "{play_id}|{gracenote_id}" slug encoding (Roku).
        gracenote_id = getattr(cd, 'gracenote_id', None) or None
        if not gracenote_id and cd.slug and '|' in cd.slug:
            candidate = cd.slug.split('|', 1)[1].strip()
            if candidate:
                gracenote_id = candidate or None

        if ch:
            ch.name          = cd.name
            ch.stream_url    = cd.stream_url
            ch.stream_type   = cd.stream_type
            ch.logo_url      = cd.logo_url
            ch.slug          = cd.slug
            ch.category      = cd.category
            ch.number        = cd.number
            ch.is_active     = True
            # Only overwrite gracenote_id if the scraper has one —
            # preserve any value the user set manually via the admin UI.
            if gracenote_id is not None:
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
                category          = cd.category,
                language          = cd.language,
                country           = cd.country,
                number            = cd.number,
                gracenote_id      = gracenote_id,
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
            channel_id    = ch.id,
            title         = pd.title,
            description   = pd.description,
            start_time    = pd.start_time,
            end_time      = pd.end_time,
            poster_url    = pd.poster_url,
            category      = pd.category,
            rating        = pd.rating,
            episode_title = pd.episode_title,
            season        = pd.season,
            episode       = pd.episode,
        ))
    db.session.flush()


def seed_sources():
    with flask_app.app_context():
        scrapers = registry.get_all()
        for name, cls in scrapers.items():
            if not Source.query.filter_by(name=name).first():
                db.session.add(Source(
                    name            = name,
                    display_name    = cls.display_name or name.title(),
                    scrape_interval = cls.scrape_interval,
                    config          = {},
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

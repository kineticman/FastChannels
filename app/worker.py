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
import time as _time
import requests as _requests
from urllib.parse import urljoin as _urljoin
from app.scrapers import registry

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    stream=sys.stdout,
)

from app.logfile import setup as _setup_logfile
_setup_logfile()
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
            scraper  = scraper_cls(config=source.config or {})
            refresh_hours = getattr(scraper_cls, 'channel_refresh_hours', 0)

            # Decide whether to skip the channel list fetch this run.
            # If channel_refresh_hours > 0 and we scraped within that window,
            # only refresh EPG using the existing DB channel list.
            skip_channels = False
            if refresh_hours > 0 and source.last_scraped_at:
                last = source.last_scraped_at.replace(tzinfo=timezone.utc) if source.last_scraped_at.tzinfo is None else source.last_scraped_at
                age_hours = (datetime.now(timezone.utc) - last).total_seconds() / 3600
                skip_channels = age_hours < refresh_hours

            if skip_channels:
                from app.scrapers.base import ChannelData as _CD
                db_channels = source.channels.filter_by(is_active=True).all()
                epg_input   = [_CD(source_channel_id=ch.source_channel_id,
                                   name=ch.name,
                                   stream_url=ch.stream_url or '',
                                   slug=ch.slug or '') for ch in db_channels]
                programs = scraper.fetch_epg(epg_input)
                _upsert_programs(source, programs)
                source.last_scraped_at = datetime.now(timezone.utc)
                source.last_error      = None
                db.session.commit()
                logger.info(f'[{source_name}] EPG-only run — {len(db_channels)} channels, {len(programs)} programs')
            else:
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



def run_drm_check(source_name: str):
    """
    Bulk DRM scanner — fetches every channel via the /play/ proxy, follows the
    redirect to the real CDN URL, drills master → variant playlist, and checks
    for SAMPLE-AES (Apple FairPlay). Channels that are DRM-encrypted are marked
    is_active=False with disable_reason='DRM' so they drop out of M3U/EPG output.
    """
    _DRM_METHODS = ('SAMPLE-AES',)  # AES-128 with key URL is standard HLS, not FairPlay

    with flask_app.app_context():
        source = Source.query.filter_by(name=source_name).first()
        if not source:
            logger.error('[drm-check] source not found: %s', source_name)
            return

        scraper_cls = registry.get(source_name)
        if not scraper_cls or not getattr(scraper_cls, 'drm_check_enabled', False):
            logger.info('[drm-check] %s: DRM check not enabled for this source, skipping', source_name)
            return

        channels = source.channels.filter_by(is_active=True).all()
        total    = len(channels)
        checked  = 0
        flagged  = 0
        dead     = 0
        errors   = 0
        consecutive_errors = 0

        logger.info('[drm-check] %s: checking %d channels via play proxy…', source_name, total)

        # Brief warmup pause — gives any residual rate-limit ban time to clear
        _time.sleep(5)

        sess = _requests.Session()
        sess.headers['User-Agent'] = 'FastChannels-DRMCheck/1.0'

        for i, ch in enumerate(channels, 1):
            # Skip malformed IDs that would break URL routing
            if len(ch.source_channel_id) > 128 or '/' in ch.source_channel_id:
                logger.debug('[drm-check] skipping bad channel ID: %s', ch.source_channel_id[:40])
                continue

            play_url = f'http://localhost:5523/play/{source_name}/{ch.source_channel_id}.m3u8'

            try:
                r = sess.get(play_url, timeout=15, allow_redirects=True)

                if r.status_code in (403, 429, 503):
                    wait = 30
                    logger.warning('[drm-check] %s rate-limited (%d), backing off %ds…',
                                   source_name, r.status_code, wait)
                    _time.sleep(wait)
                    r = sess.get(play_url, timeout=15, allow_redirects=True)

                if r.status_code == 451:
                    # Play proxy disabled this channel (DRM or VOD) during this request.
                    # Refresh the channel to read what disable_reason was set.
                    db.session.refresh(ch)
                    reason = ch.disable_reason or 'DRM'
                    if reason == 'Dead':
                        dead += 1
                        logger.info('[drm-check] VOD (caught by proxy): %s', ch.name)
                    else:
                        flagged += 1
                        logger.info('[drm-check] DRM (caught by proxy): %s', ch.name)
                    consecutive_errors = 0
                    continue

                if r.status_code in (404, 410):
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'Dead'
                    dead += 1
                    consecutive_errors = 0
                    logger.info('[drm-check] dead stream: %s  (HTTP %d)', ch.name, r.status_code)
                    continue

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
                                logger.debug('[drm-check] variant fetched for %s (%d bytes)',
                                             ch.name, len(manifest_text))
                            else:
                                logger.debug('[drm-check] variant returned %d for %s',
                                             rv.status_code, ch.name)
                        except Exception as ve:
                            logger.debug('[drm-check] variant fetch failed for %s: %s', ch.name, ve)

                if 'EXT-X-PLAYLIST-TYPE:VOD' in manifest_text:
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'Dead'
                    dead += 1
                    logger.info('[drm-check] VOD (not live): %s', ch.name)
                    continue

                drm = any(f'METHOD={m}' in manifest_text for m in _DRM_METHODS)
                if drm:
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'DRM'
                    flagged += 1
                    logger.info('[drm-check] DRM: %s  →  %s', ch.name, manifest_url[:80])

            except Exception as e:
                logger.debug('[drm-check] error for %s: %s', ch.name, e)
                errors += 1
                consecutive_errors += 1

            if i % 25 == 0:
                db.session.commit()
                logger.info('[drm-check] %s: %d/%d — checked=%d flagged=%d dead=%d errors=%d',
                            source_name, i, total, checked, flagged, dead, errors)

            _time.sleep(0.3)

        db.session.commit()
        logger.info('[drm-check] %s: done — total=%d checked=%d flagged=%d dead=%d errors=%d',
                    source_name, total, checked, flagged, dead, errors)


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

"""
Background worker — run with: python -m app.worker
"""
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import redis
import requests as _req
from rq import Worker, Queue, Connection
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import text
from app import create_app
from app.config_store import persist_source_config_updates
from app.extensions import db
from app.models import Source, Channel, Program
import time as _time
from urllib.parse import urljoin as _urljoin
from app.scrapers import registry
from app.scrapers.base import (
    StreamDeadError,
    ScrapeSkipError,
    is_transient_network_error,
)
from app.xml_cache import invalidate_xml_cache

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    stream=sys.stdout,
)
# APScheduler logs every job execution at INFO — suppress to WARNING
logging.getLogger('apscheduler').setLevel(logging.WARNING)
logging.getLogger('rq.worker').setLevel(logging.WARNING)
logging.getLogger('rq.registry').setLevel(logging.WARNING)

from app.logfile import setup as _setup_logfile
_setup_logfile()
logger = logging.getLogger(__name__)

flask_app = create_app()
_NETWORK_OUTAGE_UNTIL = 0.0
_NETWORK_OUTAGE_REASON = ''


def _utc_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def run_scraper(source_name: str):
    with flask_app.app_context():
        source = Source.query.filter_by(name=source_name).first()
        if not source:
            logger.error(f'Source not found: {source_name}')
            return

        outage_reason = _active_network_outage()
        if outage_reason:
            source.last_error = outage_reason
            db.session.commit()
            logger.warning('[%s] Scrape skipped: %s', source_name, outage_reason)
            return

        scraper_cls = registry.get(source_name)
        if not scraper_cls:
            source.last_error = f'No scraper registered for {source_name}'
            db.session.commit()
            return

        t0 = time.monotonic()
        logger.info('[%s] Scrape job started', source_name)
        scraper = None
        _progress = _make_progress_writer(source_name)
        try:
            scraper  = scraper_cls(config=source.config or {})
            scraper._progress_cb = _progress
            refresh_hours = getattr(scraper_cls, 'channel_refresh_hours', 0)

            # Decide whether to skip the channel list fetch this run.
            # If channel_refresh_hours > 0 and we scraped within that window,
            # only refresh EPG using the existing DB channel list.
            skip_channels = False
            if refresh_hours > 0 and source.last_scraped_at:
                last = _utc_aware(source.last_scraped_at)
                age_hours = (datetime.now(timezone.utc) - last).total_seconds() / 3600
                skip_channels = age_hours < refresh_hours

            # Run pre_run_setup (e.g. auth bootstrap) and persist any config
            # updates (like tokens) immediately — before the long scrape starts —
            # so they survive even if the job times out mid-EPG.
            _progress('bootstrap')
            scraper.pre_run_setup()
            _apply_scraper_config_updates(source, scraper)
            db.session.commit()

            if skip_channels:
                from app.scrapers.base import ChannelData as _CD
                db_channels = _epg_channels_for_source(source)
                epg_input   = [_CD(source_channel_id=ch.source_channel_id,
                                   name=ch.name,
                                   stream_url=ch.stream_url or '',
                                   slug=ch.slug or '') for ch in db_channels]
                _progress('epg', 0, len(epg_input))
                programs = scraper.fetch_epg(epg_input, skip_ids=_fresh_epg_sids(source))
                _upsert_programs(source, programs)
                source.last_scraped_at = datetime.now(timezone.utc)
                source.last_error      = None
                _apply_scraper_config_updates(source, scraper)
                db.session.commit()
                invalidate_xml_cache()
                elapsed = time.monotonic() - t0
                logger.info('[%s] EPG-only run complete — %d channels, %d programs (%.1fs)',
                            source_name, len(db_channels), len(programs), elapsed)
            else:
                _progress('channels')
                channels = scraper.fetch_channels()
                _progress('epg', 0, len(channels))
                programs = scraper.fetch_epg(channels, skip_ids=_fresh_epg_sids(source))
                _upsert_channels(source, channels)
                _upsert_programs(source, programs)
                source.last_scraped_at = datetime.now(timezone.utc)
                source.last_error      = None
                _apply_scraper_config_updates(source, scraper)
                db.session.commit()
                invalidate_xml_cache()
                elapsed = time.monotonic() - t0
                logger.info('[%s] Scrape complete — %d channels, %d programs (%.1fs)',
                            source_name, len(channels), len(programs), elapsed)
                _prewarm_logos(source_name, [ch.logo_url for ch in channels])
            _progress('done')
        except ScrapeSkipError as e:
            elapsed = time.monotonic() - t0
            logger.warning('[%s] Scrape skipped after %.1fs: %s', source_name, elapsed, e)
            _apply_scraper_config_updates(source, scraper)
            source.last_error = str(e)
            db.session.commit()
            _progress('done')
        except Exception as e:
            elapsed = time.monotonic() - t0
            if _is_transient_network_error(e):
                reason = _network_error_summary(e)
                _mark_network_outage(reason)
                logger.warning('[%s] Scrape skipped after %.1fs due to transient network failure: %s',
                               source_name, elapsed, reason)
                _apply_scraper_config_updates(source, scraper)
                source.last_error = reason
                db.session.commit()
                _progress('done')
                return
            logger.exception('[%s] Scrape failed after %.1fs', source_name, elapsed)
            # Persist any config updates the scraper queued before the failure
            # (e.g. a freshly bootstrapped token — don't lose it just because a
            # subsequent API call failed).
            _apply_scraper_config_updates(source, scraper)
            source.last_error = str(e)
            db.session.commit()
            _progress('done')


def _iter_exception_chain(exc: Exception):
    seen = set()
    current = exc
    while current and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _is_transient_network_error(exc: Exception) -> bool:
    return is_transient_network_error(exc)


def _network_error_summary(exc: Exception) -> str:
    for err in _iter_exception_chain(exc):
        text = str(err).strip()
        lowered = text.lower()
        if 'network is unreachable' in lowered:
            return 'Network unavailable: no route to the internet. FastChannels will retry automatically.'
        if 'temporary failure in name resolution' in lowered or 'failed to resolve' in lowered or 'err_name_not_resolved' in lowered:
            return 'Network unavailable: DNS resolution failed. FastChannels will retry automatically.'
    return 'Network unavailable: transient connectivity failure. FastChannels will retry automatically.'


def _mark_network_outage(reason: str, cooldown_seconds: int = 90) -> None:
    global _NETWORK_OUTAGE_UNTIL, _NETWORK_OUTAGE_REASON
    _NETWORK_OUTAGE_UNTIL = time.monotonic() + cooldown_seconds
    _NETWORK_OUTAGE_REASON = reason


def _active_network_outage() -> str | None:
    if time.monotonic() < _NETWORK_OUTAGE_UNTIL:
        return _NETWORK_OUTAGE_REASON
    return None



def run_stream_audit(source_name: str):
    """
    Stream Audit — resolves each active channel via the scraper, fetches the
    HLS manifest using the scraper's session (so source-specific headers like
    Origin/Referer are included), drills master → variant playlist, and checks
    for dead streams, VOD-only content, and SAMPLE-AES DRM encryption.
    Flagged channels are marked is_active=False so they drop out of M3U/EPG output.
    """
    _DRM_METHODS = ('SAMPLE-AES',)  # AES-128 with key URL is standard HLS, not FairPlay

    with flask_app.app_context():
        source = Source.query.filter_by(name=source_name).first()
        if not source:
            logger.error('[audit] source not found: %s', source_name)
            return

        scraper_cls = registry.get(source_name)
        if not scraper_cls or not getattr(scraper_cls, 'stream_audit_enabled', False):
            logger.info('[audit] %s: stream audit not enabled for this source, skipping', source_name)
            return

        scraper = scraper_cls(config=source.config or {})
        try:
            scraper.pre_run_setup()
        except Exception as _pre_exc:
            logger.debug('[audit] pre_run_setup failed (non-fatal): %s', _pre_exc)

        channels = source.channels.filter_by(is_active=True).all()
        total    = len(channels)
        checked  = 0
        flagged  = 0
        dead     = 0
        errors   = 0
        consecutive_errors = 0

        logger.info('[audit] %s: checking %d channels…', source_name, total)

        # Live progress → Redis key audit:progress:{source_name}
        _audit_key = f'audit:progress:{source_name}'
        try:
            _redis_audit = redis.from_url(flask_app.config['REDIS_URL'])
            _redis_audit.ping()
        except Exception:
            _redis_audit = None

        import json as _json_audit
        def _audit_progress(done, total_, flagged_=0, dead_=0, errors_=0, phase='checking'):
            if not _redis_audit:
                return
            try:
                if phase == 'done':
                    _redis_audit.delete(_audit_key)
                else:
                    _redis_audit.setex(_audit_key, 600, _json_audit.dumps({
                        'phase': phase, 'done': done, 'total': total_,
                        'flagged': flagged_, 'dead': dead_, 'errors': errors_,
                        'ts': _time.time(),
                    }))
            except Exception:
                pass

        _audit_progress(0, total)

        # Brief warmup pause — gives any residual rate-limit ban time to clear
        _time.sleep(5)

        # Use the scraper's own session so source-specific headers (Origin, Referer,
        # auth tokens, etc.) are included in every CDN request.
        sess = scraper.session

        for i, ch in enumerate(channels, 1):
            try:
                # Resolve the raw stream URL. Use audit_resolve() if the scraper
                # provides a lighter-weight bulk-check variant (e.g. Plex skips tune).
                _resolve = getattr(scraper, 'audit_resolve', scraper.resolve)
                try:
                    resolved_url = _resolve(ch.stream_url)
                except StreamDeadError:
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'Dead'
                    dead += 1
                    consecutive_errors = 0
                    logger.info('[audit] dead stream: %s  (confirmed by scraper)', ch.name)
                    continue
                except Exception as re_exc:
                    if _is_transient_network_error(re_exc):
                        logger.warning('[audit] transient resolve failure for %s: %s', ch.name, re_exc)
                        errors += 1
                        continue
                    logger.warning('[audit] resolve failed for %s: %s', ch.name, re_exc)
                    errors += 1
                    consecutive_errors += 1
                    if consecutive_errors >= 20:
                        logger.error('[audit] %s: 20 consecutive errors — aborting.', source_name)
                        break
                    continue

                try:
                    r = sess.get(resolved_url, timeout=15, allow_redirects=True)
                except Exception as req_exc:
                    if _is_transient_network_error(req_exc):
                        logger.warning('[audit] transient manifest fetch failure for %s: %s', ch.name, req_exc)
                        errors += 1
                        continue
                    raise

                if r.status_code in (403, 429, 503):
                    wait = 30
                    logger.warning('[audit] %s rate-limited (%d), backing off %ds…',
                                   source_name, r.status_code, wait)
                    _time.sleep(wait)
                    r = sess.get(resolved_url, timeout=15, allow_redirects=True)

                if r.status_code in (400, 404, 410):
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'Dead'
                    dead += 1
                    consecutive_errors = 0
                    logger.info('[audit] dead stream: %s  (HTTP %d)', ch.name, r.status_code)
                    continue

                if r.status_code != 200:
                    logger.debug('[audit] %d for %s', r.status_code, ch.name)
                    errors += 1
                    consecutive_errors += 1
                    if consecutive_errors >= 20:
                        logger.error('[audit] %s: 20 consecutive errors — aborting. '
                                     'Source may be rate-limiting or down.', source_name)
                        break
                    continue

                consecutive_errors = 0
                checked += 1
                manifest_text = r.text
                manifest_url  = r.url

                # ── DASH/MPD manifest ──────────────────────────────────────
                if '<MPD ' in manifest_text or (manifest_text.lstrip().startswith('<?xml')
                                                and '<MPD' in manifest_text):
                    if 'type="static"' in manifest_text:
                        ch.is_active      = False
                        ch.is_enabled     = False
                        ch.disable_reason = 'Dead'
                        dead += 1
                        logger.info('[audit] DASH VOD (not live): %s', ch.name)
                        continue
                    _widevine  = 'edef8ba9-79d6-4ace-a3c8-27dcd51d21ed'
                    _playready = '9a04f079-9840-4286-ab92-e65be0885f95'
                    if _widevine in manifest_text.lower() or _playready in manifest_text.lower():
                        ch.is_active      = False
                        ch.is_enabled     = False
                        ch.disable_reason = 'DRM'
                        flagged += 1
                        logger.info('[audit] DASH DRM: %s  →  %s', ch.name, manifest_url[:80])
                    continue   # DASH — skip HLS checks below

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
                                logger.debug('[audit] variant fetched for %s (%d bytes)',
                                             ch.name, len(manifest_text))
                            else:
                                logger.debug('[audit] variant returned %d for %s',
                                             rv.status_code, ch.name)
                        except Exception as ve:
                            logger.debug('[audit] variant fetch failed for %s: %s', ch.name, ve)

                if (
                    'EXT-X-PLAYLIST-TYPE:VOD' in manifest_text
                    and not getattr(scraper, 'audit_ignore_playlist_type_vod', False)
                ):
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'Dead'
                    dead += 1
                    logger.info('[audit] VOD (not live): %s', ch.name)
                    continue

                drm = any(f'METHOD={m}' in manifest_text for m in _DRM_METHODS)
                if drm:
                    ch.is_active      = False
                    ch.is_enabled     = False
                    ch.disable_reason = 'DRM'
                    flagged += 1
                    logger.info('[audit] DRM: %s  →  %s', ch.name, manifest_url[:80])

            except Exception as e:
                if _is_transient_network_error(e):
                    logger.warning('[audit] transient audit failure for %s: %s', ch.name, e)
                    errors += 1
                    continue
                logger.debug('[audit] error for %s: %s', ch.name, e)
                errors += 1
                consecutive_errors += 1

            if i % 25 == 0:
                db.session.commit()
                _audit_progress(i, total, flagged, dead, errors)
                logger.info('[audit] %s: %d/%d — checked=%d flagged=%d dead=%d errors=%d',
                            source_name, i, total, checked, flagged, dead, errors)

            _time.sleep(0.3)

        db.session.commit()
        _audit_progress(0, 0, phase='done')
        logger.info('[audit] %s: done — total=%d checked=%d flagged=%d dead=%d errors=%d',
                    source_name, total, checked, flagged, dead, errors)


def _make_progress_writer(source_name: str):
    """Return a callable(phase, done=0, total=0) that writes scrape progress to Redis.
    Phase 'done' deletes the key.  Silently no-ops if Redis is unavailable."""
    import json as _json
    key = f'scrape:progress:{source_name}'
    try:
        r = redis.from_url(flask_app.config['REDIS_URL'])
        r.ping()
    except Exception:
        return lambda *a, **kw: None

    def _write(phase: str, done: int = 0, total: int = 0):
        try:
            if phase == 'done':
                r.delete(key)
            else:
                r.setex(key, 600, _json.dumps({'phase': phase, 'done': done, 'total': total}))
        except Exception:
            pass
    return _write


def _apply_scraper_config_updates(source, scraper) -> None:
    """Merge any config updates the scraper queued back into source.config."""
    if scraper and scraper._pending_config_updates:
        persist_source_config_updates(source.id, scraper._pending_config_updates)
        logger.debug('[%s] persisting %d config update(s): %s',
                     source.name, len(scraper._pending_config_updates),
                     list(scraper._pending_config_updates.keys()))


def _epg_channels_for_source(source) -> list[Channel]:
    """Return DB channels that should participate in EPG refreshes.

    DRM-marked channels stay disabled for playback, but keeping them in the
    EPG refresh set preserves guide data in case support improves later.
    """
    return source.channels.filter(
        (Channel.is_active == True) | (Channel.disable_reason == 'DRM')
    ).all()


def _prewarm_logos(source_name: str, logo_urls: list[str]) -> None:
    """
    Pre-warm the logo cache for *logo_urls*.  Runs inside the RQ job process
    after a full channel scrape; uses an internal ThreadPoolExecutor so fetches
    are concurrent without blocking the job thread.
    """
    from app.routes.images import prewarm_logo_cache
    urls = [u for u in logo_urls if u]
    if not urls:
        return
    try:
        prewarm_logo_cache(urls)
    except Exception:
        logger.exception('[%s] logo cache pre-warm failed', source_name)


def _fresh_epg_sids(source, horizon_hours: float = 2.0) -> set[str]:
    """Return source_channel_ids whose programs already cover the next horizon_hours.

    Used to skip redundant content-proxy calls for channels whose EPG data is
    still fresh, reducing API request volume during scrape runs.
    """
    min_end = datetime.now(timezone.utc) + timedelta(hours=horizon_hours)
    rows = (
        db.session.query(Channel.source_channel_id)
        .join(Program, Program.channel_id == Channel.id)
        .filter(
            Channel.source_id == source.id,
            Program.end_time > min_end,
        )
        .distinct()
        .all()
    )
    return {row[0] for row in rows}


def _validate_logo_url(url: str, cache: dict[str, bool]) -> bool:
    cached = cache.get(url)
    if cached is not None:
        return cached

    ok = False
    try:
        resp = _req.head(url, allow_redirects=True, timeout=5)
        content_type = (resp.headers.get('content-type') or '').lower()
        ok = resp.ok and (not content_type or content_type.startswith('image/'))
        if not ok:
            resp = _req.get(url, allow_redirects=True, timeout=5, stream=True)
            content_type = (resp.headers.get('content-type') or '').lower()
            ok = resp.ok and content_type.startswith('image/')
            resp.close()
    except Exception:
        ok = False

    cache[url] = ok
    return ok


def _resolved_logo_url(existing_logo: str | None, incoming_logo: str | None, cache: dict[str, bool]) -> str | None:
    current = (existing_logo or '').strip() or None
    incoming = (incoming_logo or '').strip() or None

    if not incoming:
        return current
    if not current or incoming == current:
        return incoming
    if not incoming.startswith(('http://', 'https://')):
        return current
    if _validate_logo_url(incoming, cache):
        return incoming
    return current


def _upsert_channels(source, channel_data_list):
    existing = {ch.source_channel_id: ch for ch in source.channels.all()}
    logo_validation_cache: dict[str, bool] = {}
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
            stream_url_changed = ch.stream_url != cd.stream_url
            ch.name          = cd.name
            ch.stream_url    = cd.stream_url
            ch.stream_type   = cd.stream_type
            next_logo = _resolved_logo_url(ch.logo_url, cd.logo_url, logo_validation_cache)
            if next_logo != (ch.logo_url or None) and next_logo != (cd.logo_url or '').strip():
                logger.info('[%s] keeping existing logo for %s after invalid replacement URL from scrape',
                            source.name, cd.name)
            ch.logo_url      = next_logo
            ch.slug          = cd.slug
            ch.category      = cd.category
            ch.language      = cd.language
            ch.country       = cd.country
            ch.number        = cd.number
            # Don't resurrect channels the stream audit flagged as Dead or DRM
            # unless the stream URL changed (source may have fixed the channel).
            if ch.disable_reason in ('Dead', 'DRM') and not stream_url_changed:
                ch.is_active  = False  # re-enforce — a prior scrape may have revived it
                ch.is_enabled = False
            else:
                ch.is_active = True
                if stream_url_changed and ch.disable_reason in ('Dead', 'DRM'):
                    ch.disable_reason = None  # clear flag; let next audit re-check
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


def _prune_old_programs():
    """Delete programs that ended more than 2 hours ago.

    Use timezone-aware UTC to match the rest of the worker's program handling
    and avoid Python 3.12's utcnow() deprecation warning.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
    deleted = Program.query.filter(Program.end_time < cutoff).delete()
    db.session.commit()
    if deleted:
        logger.info('[worker] pruned %d expired EPG entries', deleted)


def _cleanup_orphans():
    """Delete rows whose parent records no longer exist."""
    deleted_programs = db.session.execute(text("""
        DELETE FROM programs
        WHERE channel_id NOT IN (SELECT id FROM channels)
    """)).rowcount or 0
    deleted_channels = db.session.execute(text("""
        DELETE FROM channels
        WHERE source_id NOT IN (SELECT id FROM sources)
    """)).rowcount or 0
    db.session.commit()
    if deleted_programs or deleted_channels:
        logger.info(
            '[worker] cleaned %d orphan programs and %d orphan channels',
            deleted_programs,
            deleted_channels,
        )


def _upsert_programs(source, program_data_list):
    if not program_data_list:
        return
    channels = {ch.source_channel_id: ch for ch in source.channels.all()}
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=2)

    incoming_by_channel_id: dict[int, list] = {}
    for pd in program_data_list:
        ch = channels.get(pd.source_channel_id)
        if not ch:
            continue
        incoming_by_channel_id.setdefault(ch.id, []).append(pd)

    # Collect DB channel IDs that have incoming programs, then delete their
    # existing future programs before inserting fresh data.  This prevents
    # duplicates caused by the same channel appearing in multiple country
    # feeds or by repeated scrape runs appending to the same window.
    incoming_channel_ids = set(incoming_by_channel_id)
    if incoming_channel_ids:
        existing_rows = db.session.query(
            Program.id,
            Program.channel_id,
            Program.start_time,
            Program.end_time,
        ).filter(
            Program.channel_id.in_(incoming_channel_ids),
            Program.end_time >= cutoff,
        ).all()

        existing_by_channel_id: dict[int, list[tuple[int, datetime, datetime]]] = {}
        for row_id, channel_id, start_time, end_time in existing_rows:
            existing_by_channel_id.setdefault(channel_id, []).append((row_id, start_time, end_time))

        preserve_ids: set[int] = set()
        preserved_channel_ids: list[int] = []
        preserved_row_count = 0
        for channel_id, incoming_rows in incoming_by_channel_id.items():
            future_rows = [row for row in incoming_rows if _utc_aware(row.end_time) > now]
            has_now_coverage = any(
                _utc_aware(row.start_time) <= now < _utc_aware(row.end_time)
                for row in future_rows
            )
            if has_now_coverage:
                continue

            earliest_incoming_start = min(
                (_utc_aware(row.start_time) for row in future_rows),
                default=None,
            )
            rows_to_preserve = []
            for existing_id, existing_start_raw, existing_end_raw in existing_by_channel_id.get(channel_id, []):
                existing_start = _utc_aware(existing_start_raw)
                existing_end = _utc_aware(existing_end_raw)
                if existing_end <= now:
                    continue
                if earliest_incoming_start is None:
                    rows_to_preserve.append(existing_id)
                    continue
                if existing_start < earliest_incoming_start and existing_end <= earliest_incoming_start:
                    rows_to_preserve.append(existing_id)

            if rows_to_preserve:
                preserve_ids.update(rows_to_preserve)
                preserved_channel_ids.append(channel_id)
                preserved_row_count += len(rows_to_preserve)

        if preserved_channel_ids:
            sample = ",".join(str(channel_id) for channel_id in preserved_channel_ids[:10])
            extra = "" if len(preserved_channel_ids) <= 10 else ",..."
            logger.info(
                '[%s] preserved %d existing EPG rows across %d channels with no now coverage (sample channel_ids=%s%s)',
                source.name,
                preserved_row_count,
                len(preserved_channel_ids),
                sample,
                extra,
            )

        delete_query = Program.query.filter(
            Program.channel_id.in_(incoming_channel_ids),
            Program.end_time >= cutoff,
        )
        if preserve_ids:
            delete_query = delete_query.filter(~Program.id.in_(preserve_ids))
        delete_query.delete(synchronize_session=False)

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
    _prune_old_programs()


# In-memory record of when each source was last enqueued, so we don't
# double-queue a source that's still running (last_scraped_at not yet updated).
_last_enqueued: dict[str, datetime] = {}


def _schedule_due_scrapes():
    """Enqueue scrapes for enabled sources whose interval has elapsed."""
    now = datetime.now(timezone.utc)
    with flask_app.app_context():
        try:
            r = redis.from_url(flask_app.config['REDIS_URL'])
            q = Queue('scraper', connection=r)
        except Exception as e:
            logger.error('[scheduler] Redis unavailable: %s', e)
            return

        sources = Source.query.filter_by(is_enabled=True).all()
        for source in sources:
            interval_secs = (source.scrape_interval or 360) * 60

            last_scraped = _utc_aware(source.last_scraped_at)
            last_queued = _utc_aware(_last_enqueued.get(source.name))
            candidates = [t for t in (last_scraped, last_queued) if t is not None]
            last = max(candidates) if candidates else None

            if last is None or (now - last).total_seconds() >= interval_secs:
                try:
                    q.enqueue('app.worker.run_scraper', source.name, job_timeout=3600)
                    _last_enqueued[source.name] = now
                    logger.info('[scheduler] Enqueued %s (interval=%dm, age=%s)',
                                source.name, source.scrape_interval,
                                f'{(now - last).total_seconds() / 60:.0f}m' if last else 'never')
                except Exception as e:
                    logger.error('[scheduler] Failed to enqueue %s: %s', source.name, e)


def seed_sources():
    with flask_app.app_context():
        scrapers = registry.get_all()
        default_epg_only_sources = {'amazon_prime_free', 'sling'}
        seeded_names = set()
        for name, cls in scrapers.items():
            canonical_name = getattr(cls, 'source_name', None) or name
            if name != canonical_name or canonical_name in seeded_names:
                continue
            seeded_names.add(canonical_name)
            if not Source.query.filter_by(name=canonical_name).first():
                db.session.add(Source(
                    name            = canonical_name,
                    display_name    = cls.display_name or canonical_name.title(),
                    scrape_interval = cls.scrape_interval,
                    config          = {},
                    epg_only        = canonical_name in default_epg_only_sources,
                ))
        db.session.commit()
        logger.info(f'Seeded {len(seeded_names)} sources')


if __name__ == '__main__':
    seed_sources()

    def _scheduled_prune():
        with flask_app.app_context():
            _prune_old_programs()

    def _scheduled_integrity_cleanup():
        with flask_app.app_context():
            _cleanup_orphans()

    def _scheduled_logo_cache_cleanup():
        from app.routes.images import cleanup_logo_cache, cleanup_poster_cache
        removed = cleanup_logo_cache()
        if removed:
            logger.info('[logo_cache] removed %d expired logo files', removed)

        # Delete cached posters for programs that ended more than 2 hours ago
        with flask_app.app_context():
            cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
            expired_urls = [
                row[0] for row in
                db.session.query(Program.poster_url)
                .filter(Program.end_time < cutoff, Program.poster_url.isnot(None))
                .distinct()
                .all()
            ]
        poster_removed = cleanup_poster_cache(expired_urls)
        if poster_removed:
            logger.info('[logo_cache] removed %d expired poster files', poster_removed)

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(_schedule_due_scrapes, 'interval', minutes=1, id='auto_scrape',
                      max_instances=1, coalesce=True)
    scheduler.add_job(_scheduled_prune, 'interval', hours=1, id='epg_prune',
                      max_instances=1, coalesce=True)
    scheduler.add_job(_scheduled_integrity_cleanup, 'interval', days=1, id='integrity_cleanup',
                      max_instances=1, coalesce=True)
    scheduler.add_job(_scheduled_logo_cache_cleanup, 'interval', hours=6, id='logo_cache_cleanup',
                      max_instances=1, coalesce=True)
    scheduler.start()
    logger.info('Scheduler started — checking sources every 60s')
    with flask_app.app_context():
        _cleanup_orphans()
        enabled_sources = Source.query.filter_by(is_enabled=True).count()
        total_sources = Source.query.count()
        from app.models import Feed
        enabled_feeds = Feed.query.filter_by(is_enabled=True).count()
        logger.info(
            'Startup summary — enabled_sources=%d total_sources=%d enabled_feeds=%d',
            enabled_sources,
            total_sources,
            enabled_feeds,
        )

    r = redis.from_url(flask_app.config['REDIS_URL'])
    with Connection(r):
        worker = Worker(queues=[Queue('scraper', connection=r)])
        logger.info('Worker listening on queue: scraper')
        worker.work(logging_level=logging.WARNING)

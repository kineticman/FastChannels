import logging
import re
import requests as _req

logger = logging.getLogger(__name__)
from datetime import datetime, timezone

from urllib.parse import urljoin as _urljoin
from flask import Blueprint, jsonify, request
from app.config_store import persist_source_config_updates, persist_source_cache_updates
from ..extensions import db
from ..models import Channel, Program, AppSettings, Feed
from ..scrapers import registry
from ..scrapers.base import StreamDeadError
from ..hls import (
    inspect_hls_drm,
    parse_stream_info as _parse_stream_info,
    parse_dash_stream_info as _parse_dash_stream_info,
    estimate_height_from_bandwidth as _estimate_height,
    nominal_resolution as _nominal_resolution,
    WIDEVINE_UUID,
    PLAYREADY_UUID,
)
try:
    from croniter import croniter as _croniter
except ImportError:
    _croniter = None
from ..generators.m3u import (
    feed_to_query_filters,
    _build_channel_query,
    _parse_gracenote_id,
    _selected_channel_stubs,
    _resolve_chnum_map,
    feed_gracenote_start,
    feed_namespace_start,
)

playback_bp = Blueprint('api_playback', __name__)


def _channel_feed_summaries(ch: Channel) -> list[dict]:
    """Return enabled feeds that include a channel, with its output channel number."""
    channel_id = ch.id
    gracenote_output = bool(_parse_gracenote_id(ch))
    feeds = Feed.query.filter(Feed.is_enabled == True).order_by(Feed.name).all()
    result = []

    for feed in feeds:
        filters = feed.filters or {}
        pinned = channel_id in (filters.get('pinned_channel_ids') or [])
        excluded = channel_id in (filters.get('excluded_channel_ids') or [])
        if excluded and not pinned:
            continue

        q_filters = feed_to_query_filters(filters)
        if pinned:
            status = 'pinned'
        else:
            in_feed = (
                _build_channel_query(q_filters, activity='any')
                .filter(Channel.id == channel_id)
                .count()
                > 0
            )
            if not in_feed:
                continue
            status = 'filtered'

        selected = _selected_channel_stubs(q_filters, gracenote=gracenote_output)
        if feed.chnum_start is not None:
            chnum_map, _ = _resolve_chnum_map(
                selected,
                feed_chnum_start=feed.chnum_start,
                feed_id=feed.id,
            )
        elif feed.slug == 'default' and gracenote_output:
            chnum_map, _ = _resolve_chnum_map(
                selected,
                namespace_start=feed_gracenote_start(feed),
            )
        elif feed.slug == 'default':
            chnum_map, _ = _resolve_chnum_map(selected)
        else:
            chnum_map, _ = _resolve_chnum_map(
                selected,
                namespace_start=feed_namespace_start(feed, gracenote=gracenote_output),
            )

        result.append({
            'feed_id': feed.id,
            'feed_name': feed.name,
            'feed_slug': feed.slug,
            'status': status,
            'feed_channel_number': chnum_map.get(channel_id),
            'output': 'gracenote' if gracenote_output else 'xmltv',
        })

    return result


def _parse_hls_variants(master_text: str) -> list[dict]:
    """Parse #EXT-X-STREAM-INF variant entries from an HLS master playlist."""
    _CODEC_NAMES = {
        'avc1': 'H.264', 'avc3': 'H.264',
        'hvc1': 'H.265', 'hev1': 'H.265',
        'mp4a': 'AAC',
        'ac-3': 'AC-3', 'ec-3': 'E-AC-3',
        'vp09': 'VP9', 'av01': 'AV1',
    }

    def _friendly_codecs(raw: str) -> str:
        seen, result = set(), []
        for part in raw.split(','):
            prefix = part.strip().split('.')[0].lower()
            name = _CODEC_NAMES.get(prefix, prefix)
            if name not in seen:
                seen.add(name)
                result.append(name)
        return '+'.join(result)

    variants = []
    lines = master_text.splitlines()
    for i, line in enumerate(lines):
        line = line.strip()
        if not line.startswith('#EXT-X-STREAM-INF:'):
            continue
        attrs = line[len('#EXT-X-STREAM-INF:'):]
        v = {}
        m = re.search(r'BANDWIDTH=(\d+)', attrs)
        if m:
            v['bandwidth'] = int(m.group(1))
        m = re.search(r'RESOLUTION=(\d+x\d+)', attrs, re.I)
        if m:
            v['resolution'] = m.group(1)
        m = re.search(r'CODECS="([^"]+)"', attrs)
        if m:
            v['codecs'] = _friendly_codecs(m.group(1))
        m = re.search(r'FRAME-RATE=([\d.]+)', attrs)
        if m:
            v['fps'] = round(float(m.group(1)), 3)
        variants.append(v)

    variants.sort(key=lambda v: v.get('bandwidth', 0), reverse=True)

    # No variant declared RESOLUTION (e.g. Pluto) — estimate from bitrate so the
    # inspect popup still shows a quality figure, flagged as approximate.
    if variants and not any(v.get('resolution') for v in variants):
        for v in variants:
            est_h = _estimate_height(v.get('bandwidth'))
            if est_h:
                v['resolution'] = _nominal_resolution(est_h)
                v['resolution_est'] = True

    return variants


def _isoformat_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def _inspect_drm_bridge(ch, source, scraper_cls):
    """Inspect a DRM-bridge channel via its DASH+Widevine path (the one it actually
    plays over through PrismCast), rather than reporting its protected variant as broken.

    Handles both shapes: scrapers with a dedicated resolve_dash() (Roku — returns the MPD
    plus a per-session license URL) and scrapers that natively resolve to DASH (Amazon,
    Sling — resolve() returns the MPD; license is served by the source's license proxy).
    Verifies everything checkable server-side — manifest reachable + live, Widevine
    ContentProtection present, the source has license handling, and a segment is flowing.
    Returns status 'bridge'. Actual decryption is only provable in a browser CDM
    (PrismCast), so this is a bridge-readiness check."""
    import re as _re
    from urllib.parse import urljoin as _urljoin
    _WIDEVINE = WIDEVINE_UUID

    scraper = scraper_cls(config=source.config or {})
    per_session_license = None
    try:
        if hasattr(scraper_cls, 'resolve_dash'):
            result = scraper.resolve_dash(ch.stream_url)
            mpd_url = (result or {}).get('mpd_url')
            per_session_license = (result or {}).get('license_url')
        else:
            mpd_url = scraper.resolve(ch.stream_url)
    except StreamDeadError as e:
        return jsonify({'status': 'dead', 'detail': str(e)})
    except Exception as e:
        return jsonify({'status': 'error', 'detail': f'DASH resolve failed: {e}'})
    finally:
        if scraper._pending_config_updates:
            try:
                persist_source_config_updates(source.id, scraper._pending_config_updates)
            except Exception:
                db.session.rollback()
        if getattr(scraper, '_pending_cache_updates', None):
            try:
                persist_source_cache_updates(source.id, scraper._pending_cache_updates)
            except Exception:
                db.session.rollback()

    if not mpd_url:
        return jsonify({'status': 'error', 'detail': 'No DASH manifest URL resolved'})

    # Does the source have a working license path? Per-session URL (Roku) or a license
    # proxy keyed on the channel (Amazon/Sling get_license_url).
    has_license = bool(per_session_license)
    if not has_license:
        try:
            has_license = bool(scraper_cls.get_license_url(source.config or {}, channel_id=ch.source_channel_id)
                               or getattr(scraper_cls, 'license_url', None))
        except Exception:
            has_license = bool(getattr(scraper_cls, 'license_url', None))

    try:
        r = scraper.session.get(mpd_url, timeout=15)
    except Exception as e:
        return jsonify({'status': 'error', 'detail': f'Manifest fetch failed: {e}'})
    if r.status_code != 200:
        return jsonify({'status': 'dead', 'detail': f'DASH manifest returned HTTP {r.status_code}'})
    mpd = r.text

    if 'type="static"' in mpd:
        return jsonify({'status': 'vod', 'detail': 'DASH manifest is static (VOD) — not a live channel'})

    # Populate the resolution/codec badge from the MPD's video Representations — the same
    # parse the stream audit does (worker.py), so hitting the inspector fills it in too.
    _dash_info = _parse_dash_stream_info(mpd)
    if _dash_info:
        ch.stream_info = _dash_info
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()

    if _WIDEVINE not in mpd.lower():
        return jsonify({'status': 'error', 'detail': 'DASH manifest has no Widevine ContentProtection'})
    if not has_license:
        return jsonify({'status': 'error',
                        'detail': 'Widevine manifest OK but source has no license path — bridge would fail'})

    # Pull one segment to confirm bytes are flowing (encrypted; decryption happens in the
    # browser CDM). Resolve the init segment against the BaseURL, which may be relative
    # (Amazon) or absolute (Roku) — urljoin against the manifest URL handles both.
    seg_bytes = 0
    try:
        base_m = _re.search(r'<BaseURL[^>]*>([^<]+)</BaseURL>', mpd)
        init_m = _re.search(r'initialization="([^"]+)"', mpd)
        rep_m  = _re.search(r'<Representation\s+id="([^"]+)"', mpd)
        if init_m:
            base = _urljoin(mpd_url, base_m.group(1).strip()) if base_m else mpd_url
            seg = _urljoin(base, init_m.group(1).strip())
            if '$RepresentationID$' in seg and rep_m:
                seg = seg.replace('$RepresentationID$', rep_m.group(1))
            rs = scraper.session.get(seg, timeout=10, stream=True)
            if rs.status_code == 200:
                chunk = next(rs.iter_content(8192), None)
                seg_bytes = len(chunk) if chunk else 0
            rs.close()
    except Exception:
        pass

    detail = 'DASH + Widevine OK, license path present'
    detail += f', segment {seg_bytes} bytes' if seg_bytes else ', segment not fetched'
    detail += ' — plays via PrismCast (decryption verified in-browser)'
    return jsonify({'status': 'bridge', 'detail': detail})


@playback_bp.route('/channels/<int:channel_id>/inspect', methods=['POST'])
def inspect_channel(channel_id):
    """
    Single-channel inspector: resolve the stream URL directly, parse the HLS manifest,
    check for DRM/VOD, then pull one segment to confirm video data is flowing.
    Returns: { status, detail, segment_bytes }
      status: 'live' | 'drm' | 'dead' | 'vod' | 'no_data' | 'error'
    """
    ch     = Channel.query.get_or_404(channel_id)
    source = ch.source

    if len(ch.source_channel_id) > 128 or '/' in ch.source_channel_id:
        return jsonify({'status': 'error', 'detail': 'Malformed channel ID'})

    # DRM-bridge channels don't play over their HLS variant (FairPlay) — they play as
    # DASH+Widevine through PrismCast. Inspecting the HLS path would wrongly report 'DRM'.
    # Test the bridge path instead: confirm the DASH manifest + Widevine + license + a
    # flowing segment. (Decryption itself can only be verified in a browser CDM — i.e.
    # PrismCast — so this is a 'bridge-ready' check, not a full decrypt.)
    _scraper_cls = registry.get(source.name)
    if getattr(ch, 'requires_drm_bridge', False) and _scraper_cls:
        _bridge_stream_type = (getattr(ch, 'stream_type', '') or '').strip().lower()
        if _bridge_stream_type == 'dash' or source.name in {'amazon_prime_free', 'philo', 'roku'}:
            return _inspect_drm_bridge(ch, source, _scraper_cls)

    # Resolve the stream URL directly — avoids a self-referential HTTP request to the
    # gunicorn server itself, which can deadlock all workers under concurrent inspect calls.
    scraper_cls = registry.get(source.name)
    if scraper_cls:
        scraper = scraper_cls(config=source.config or {})
        try:
            resolved_url = scraper.resolve(ch.stream_url)
        except StreamDeadError as e:
            return jsonify({'status': 'dead', 'detail': str(e)})
        except Exception as e:
            return jsonify({'status': 'error', 'detail': f'URL resolve failed: {e}'})
        finally:
            if scraper._pending_config_updates:
                try:
                    persist_source_config_updates(
                        source.id,
                        scraper._pending_config_updates,
                    )
                except Exception:
                    db.session.rollback()
            if getattr(scraper, '_pending_cache_updates', None):
                try:
                    persist_source_cache_updates(
                        source.id,
                        scraper._pending_cache_updates,
                    )
                except Exception:
                    db.session.rollback()
        sess = scraper.session
    else:
        resolved_url = ch.stream_url
        sess = _req.Session()
        sess.headers['User-Agent'] = 'FastChannels-Inspector/1.0'

    if not resolved_url:
        return jsonify({'status': 'error', 'detail': 'No stream URL'})

    # For channels that re-detect at play time, run the stream detector here too
    # so the inspector sees the real error (e.g. yt-dlp "stream not available")
    # instead of a stale CDN URL returning an opaque HTTP 403.
    if getattr(ch, 'redetect_on_play', False) and ch.page_url:
        from ..scrapers.stream_detector import StreamDetector as _SD
        _dr = _SD().detect(ch.page_url)
        if _dr.error and not _dr.stream_url:
            return jsonify({'status': 'error', 'detail': _dr.error})
        if _dr.stream_url:
            resolved_url = _dr.stream_url
            if _dr.headers:
                sess = _req.Session()
                sess.headers.update(_dr.headers)

    # For session-based CDNs (e.g. Broadpeak) that return intermittent 404s
    # when fetched server-side, verify via the scraper's audit_resolve() instead.
    # audit_resolve() checks feed/catalogue presence and raises StreamDeadError
    # if the channel is genuinely gone.  Scrapers advertise which CDN hosts need
    # this treatment via a class-level `session_cdn_hosts` frozenset.
    if scraper_cls and hasattr(scraper_cls, 'audit_resolve'):
        from urllib.parse import urlsplit as _us
        _session_cdn = getattr(scraper_cls, 'session_cdn_hosts', frozenset())
        if resolved_url.startswith('http') and _us(resolved_url).netloc in _session_cdn:
            try:
                scraper.audit_resolve(ch.stream_url)
                return jsonify({'status': 'live', 'detail': 'Session-based CDN — verified via feed (client playback should work)'})
            except StreamDeadError as e:
                return jsonify({'status': 'dead', 'detail': str(e)})
            except Exception:
                pass  # fall through to normal manifest fetch

    try:
        r = sess.get(resolved_url, timeout=15, allow_redirects=True)

        if r.status_code in (404, 410):
            return jsonify({'status': 'dead', 'detail': f'HTTP {r.status_code} — stream not found'})

        if r.status_code in (403, 429, 451, 503):
            return jsonify({'status': 'error', 'detail': f'HTTP {r.status_code} — blocked or restricted'})

        if r.status_code != 200:
            return jsonify({'status': 'error', 'detail': f'HTTP {r.status_code}'})

        manifest_url = r.url
        content_type = (r.headers.get('Content-Type') or '').split(';', 1)[0].strip().lower()

        # Direct video sources like MP4/WebM are valid custom channels, but they are
        # not HLS manifests and should not be run through playlist parsing.
        if (
            content_type.startswith('video/')
            or manifest_url.lower().split('?', 1)[0].endswith(('.mp4', '.webm', '.mkv', '.mov'))
        ):
            try:
                chunk = next(r.iter_content(8192), None)
            finally:
                r.close()
            seg_bytes = len(chunk) if chunk else 0
            if seg_bytes == 0:
                return jsonify({'status': 'no_data', 'detail': 'Direct video returned 0 bytes'})
            return jsonify({
                'status': 'live',
                'detail': f'Direct video OK — {seg_bytes} bytes received',
                'segment_bytes': seg_bytes,
                'variants': [],
            })

        manifest_text = r.text

        # ── DASH/MPD manifest ─────────────────────────────────────────────
        is_mpd = ('<MPD ' in manifest_text or manifest_text.lstrip().startswith('<?xml')
                  and '<MPD' in manifest_text)
        if is_mpd:
            # VOD check
            if 'type="static"' in manifest_text:
                return jsonify({'status': 'vod', 'detail': 'DASH VOD stream — not a live channel'})
            # Resolution/codec badge from the MPD's video Representations (same as the audit).
            _dash_info = _parse_dash_stream_info(manifest_text)
            if _dash_info:
                ch.stream_info = _dash_info
                try:
                    db.session.commit()
                except Exception:
                    db.session.rollback()
            # DRM check (Widevine / PlayReady)
            if WIDEVINE_UUID in manifest_text.lower() or PLAYREADY_UUID in manifest_text.lower():
                return jsonify({'status': 'drm', 'detail': 'DASH DRM detected (Widevine/PlayReady)'})
            return jsonify({'status': 'live', 'detail': 'DASH manifest OK (live)'})

        # Master playlist → parse variant stats, persist stream_info, then drill into first variant
        variants = []
        if '#EXT-X-STREAM-INF' in manifest_text:
            variants = _parse_hls_variants(manifest_text)
            stream_info = _parse_stream_info(manifest_text)
            if stream_info:
                ch.stream_info = stream_info
                try:
                    db.session.commit()
                except Exception:
                    db.session.rollback()
            for line in manifest_text.splitlines():
                line = line.strip()
                if line and not line.startswith('#'):
                    variant_url = _urljoin(manifest_url, line)
                    try:
                        rv = sess.get(variant_url, timeout=10)
                        if rv.status_code == 200:
                            manifest_text = rv.text
                            manifest_url  = rv.url
                    except Exception:
                        pass
                    break

        if '#EXT-X-PLAYLIST-TYPE:VOD' in manifest_text and '#EXT-X-ENDLIST' in manifest_text:
            return jsonify({'status': 'vod', 'detail': 'Finished VOD — not a live channel'})

        drm = inspect_hls_drm(manifest_text)
        if drm:
            detail = f"HLS DRM detected ({drm['drm_type']}"
            if drm.get('keyformat'):
                detail += f"; KEYFORMAT={drm['keyformat']}"
            detail += ')'
            if getattr(ch, 'requires_drm_bridge', False) and scraper_cls:
                try:
                    has_license = bool(scraper_cls.get_license_url(source.config or {}, channel_id=ch.source_channel_id))
                except Exception:
                    has_license = bool(getattr(scraper_cls, 'license_url', None))
                if has_license:
                    return jsonify({
                        'status': 'bridge',
                        'detail': f'{detail}, license path present -- plays via PrismCast',
                    })
            return jsonify({'status': 'drm', 'detail': detail})

        # Use the last media segment — live streams with rolling windows purge old
        # segments from the CDN even when still listed in the manifest, so the first
        # segment may already be 404 while the most recent one is always available.
        segment_url = None
        for line in manifest_text.splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                segment_url = _urljoin(manifest_url, line)

        if not segment_url:
            return jsonify({'status': 'live', 'detail': 'Manifest OK (no segments listed yet)',
                            'variants': variants})

        try:
            rs = sess.get(segment_url, timeout=10, stream=True)
            if rs.status_code != 200:
                return jsonify({'status': 'no_data',
                                'detail': f'Manifest OK but segment returned HTTP {rs.status_code}',
                                'variants': variants})
            chunk = next(rs.iter_content(8192), None)
            rs.close()
            seg_bytes = len(chunk) if chunk else 0
            if seg_bytes == 0:
                return jsonify({'status': 'no_data', 'detail': 'Segment returned 0 bytes',
                                'variants': variants})
            return jsonify({'status': 'live',
                            'detail': f'Stream OK — {seg_bytes} bytes received from segment',
                            'segment_bytes': seg_bytes,
                            'variants': variants})
        except Exception as e:
            return jsonify({'status': 'error', 'detail': f'Segment fetch failed: {e}'})

    except Exception as e:
        return jsonify({'status': 'error', 'detail': str(e)})


def _get_playback_info(ch, fast_mode=True):
    """
    Resolve playback URLs and mode for a channel.
    Returns dict: stream_type, preview_url, play_url, playback_mode, license_url,
    needs_detection.
    Must be called within a Flask request context (license_url uses request.host_url).
    """
    stream_type = (ch.stream_type or '').strip().lower()
    preview_url = None
    needs_detection = False

    if ch.source and ch.source.name == 'custom':
        if fast_mode and ch.page_url:
            needs_detection = True
            if stream_type in {'mp4', 'webm', 'mov', 'mkv', 'direct'} and ch.stream_url:
                preview_url = ch.stream_url
            elif ch.source_channel_id:
                preview_url = f'/play/custom/{ch.source_channel_id}/proxy.m3u8'
        else:
            try:
                from ..scrapers.stream_detector import StreamDetector
                from .play import _redetect_custom_stream

                resolved_url = None
                if ch.page_url:
                    resolved_url, _ = _redetect_custom_stream(ch)
                if resolved_url:
                    resolved_type = (StreamDetector.infer_stream_type(resolved_url) or stream_type or '').strip().lower()
                    stream_type = resolved_type
                    if resolved_type in {'mp4', 'webm', 'mov', 'mkv', 'direct'}:
                        preview_url = resolved_url
                    elif resolved_type == 'hls':
                        preview_url = f'/play/custom/{ch.source_channel_id}/proxy.m3u8'
                elif stream_type == 'hls':
                    preview_url = f'/play/custom/{ch.source_channel_id}/proxy.m3u8'
                elif stream_type in {'mp4', 'webm', 'mov', 'mkv', 'direct'} and ch.stream_url:
                    preview_url = ch.stream_url
            except Exception:
                preview_url = None

    if not preview_url and ch.source and ch.source.name == 'custom' and stream_type == 'hls':
        preview_url = f'/play/custom/{ch.source_channel_id}/proxy.m3u8'

    if stream_type in {'mp4', 'webm', 'mov', 'mkv', 'direct'}:
        playback_mode = 'native'
    elif stream_type in {'mjpeg', 'jpeg_snapshot'}:
        playback_mode = 'unsupported'
    elif stream_type == 'dash':
        playback_mode = 'dash'
    else:
        playback_mode = 'hls'

    # Roku DRM channels: the HLS variant is FairPlay (Chrome can't decrypt it), but
    # the same channel has a CENC DASH+Widevine variant Chrome CAN handle. Serve that
    # via EME so the watch page — and the PrismCast bridge that captures it — can play.
    roku_drm = bool(
        ch.source and ch.source.name == 'roku'
        and (getattr(ch, 'requires_drm_bridge', False)
             or (ch.disable_reason or '').startswith('DRM'))
    )

    # Fubo DRM channels: same shape as Roku above — the HLS variant is
    # FairPlay-only, but the same content has a CENC DASH+Widevine variant
    # (see resolve_dash() in fubo.py). Most Fubo channels are NOT DRM at all.
    fubo_drm = bool(
        ch.source and ch.source.name == 'fubo'
        and (getattr(ch, 'requires_drm_bridge', False)
             or (ch.disable_reason or '').startswith('DRM'))
    )

    # These sources use AES-128 encrypted TS; Shaka 4.x cannot decrypt via MSE
    # (error 4042). Force native mode so the watch page sets video.src directly
    # and lets the browser's native HLS stack handle decryption.
    if ch.source and ch.source.name in ('pluto', 'fubo', 'roku', 'discovery_tve') and not roku_drm and not fubo_drm:
        playback_mode = 'native'
    if roku_drm or fubo_drm:
        playback_mode = 'dash'

    license_url = None
    if ch.source:
        from ..scrapers.registry import get as _get_scraper
        _scraper_cls = _get_scraper(ch.source.name)
        # Roku/Fubo are DRM-capable per-channel: only DRM-flagged channels use the
        # license path; plain channels stay native HLS with no license URL.
        if _scraper_cls and not (ch.source.name == 'roku' and not roku_drm) \
                and not (ch.source.name == 'fubo' and not fubo_drm):
            from flask import request as _req
            from urllib.parse import quote as _quote
            _base = _req.host_url.rstrip('/')
            if ch.source.name == 'roku' and roku_drm:
                # Roku's per-session license URL is created when the DASH route
                # resolves the matching MPD. Advertise our proxy before that
                # request so Shaka has a license server when the MPD loads.
                license_url = f'{_base}/play/roku/license?channel_id={ch.source_channel_id}'
            elif ch.source.name == 'fubo' and fubo_drm:
                # Same reasoning as Roku above: get_license_url() only has
                # something to return once /play/fubo/<id>/dash.mpd has actually
                # resolved and cached the per-channel token, which happens after
                # this preview info is built. Advertise the proxy URL up front.
                license_url = f'{_base}/play/fubo/license?channel_id={ch.source_channel_id}'
            else:
                _lu = _scraper_cls.get_license_url(
                    ch.source.config or {},
                    channel_id=ch.source_channel_id,
                )
                if _lu:
                    _license_channel = _quote(ch.source_channel_id, safe='')
                    if ch.source.name == 'directv':
                        license_url = f'{_base}/play/{ch.source.name}/license/{_license_channel}'
                    else:
                        license_url = f'{_base}/play/{ch.source.name}/license?channel_id={_license_channel}'

    play_url = None
    if (
        ch.stream_url
        and ch.source
        and not ch.source.epg_only
        and ch.source.name
        and ch.source_channel_id
    ):
        play_url = f'/play/{ch.source.name}/{ch.source_channel_id}.m3u8'
    if not preview_url:
        preview_url = play_url

    if ch.source and ch.source.name == 'cspan' and ch.source_channel_id:
        from urllib.parse import quote as _quote
        _enc = _quote(ch.source_channel_id, safe='')
        preview_url = f'/play/cspan/{_enc}/proxy.m3u8'
        play_url = preview_url
        playback_mode = 'hls'
        stream_type = 'hls'

    if ch.source and ch.source.name == 'directv' and ch.source_channel_id:
        from urllib.parse import quote as _quote
        _enc = _quote(ch.source_channel_id, safe='')
        if request.args.get('directv_bridge') == '1' or request.args.get('directv_drm') == '1':
            preview_url = f'/play/directv/{_enc}/prismcast.m3u8'
            play_url = preview_url
            license_url = None
        else:
            play_url = f'/play/directv/{_enc}.m3u8'
            preview_url = f'/play/directv/{_enc}/browser.m3u8'
        playback_mode = 'hls'
        stream_type = 'hls'

    # For sources whose CDN blocks browser requests (TLS fingerprinting, IP-locked
    # tokens, CORS mismatches, or relative-URL resolution bugs on redirects), route
    # the watch-player preview through our server-side proxy.  The play_url
    # (/play/<source>/<id>.m3u8) is left as-is so M3U clients get a direct redirect.
    # Pluto: stitcher returns relative variant URLs (e.g. "640930/playlist.m3u8");
    # Chrome's native HLS resolves them against the original video.src URL (before
    # the 302) rather than the redirect target, sending sub-playlist requests back
    # to our server.  The proxy fetches the master server-side and rewrites all
    # variant URLs to absolute proxy URLs, eliminating the relative-URL problem.
    if ch.source and ch.source.name in ('fubo', 'tubi', 'pluto', 'samsung', 'distro') and ch.source_channel_id:
        from urllib.parse import quote as _quote
        _enc = _quote(ch.source_channel_id, safe='')
        preview_url = f'/play/{ch.source.name}/{_enc}/proxy.m3u8'

    # Custom YouTube channels: googlevideo's HLS CDN sends no
    # Access-Control-Allow-Origin for a third-party page, and the playback URL is
    # locked to the IP that resolved it (our server).  A direct browser fetch
    # therefore fails CORS (Shaka error 1002) even when the viewer's IP matches.
    # Route the browser preview through our manifest proxy (browser=1 → variant +
    # segments re-served same-origin from the server IP).  play_url stays a direct
    # redirect so IPTV clients, which don't enforce CORS, keep the lighter path.
    if ch.source and ch.source.name == 'custom' and ch.source_channel_id:
        from ..scrapers.stream_detector import _YOUTUBE_RE
        if (ch.page_url and _YOUTUBE_RE.match(ch.page_url)) or 'googlevideo.com' in (ch.stream_url or ''):
            from urllib.parse import quote as _quote
            _enc = _quote(ch.source_channel_id, safe='')
            preview_url = f'/play/custom/{_enc}/proxy.m3u8?browser=1'

    # Amazon DASH: CDN locks CORS to amazon.com, so route the preview through
    # our server-side manifest proxy which strips that restriction.
    if (
        not preview_url
        or (ch.source and ch.source.name == 'amazon_prime_free')
    ) and ch.source_channel_id:
        from urllib.parse import quote as _quote
        preview_url = f'/play/amazon_prime_free/{_quote(ch.source_channel_id, safe="")}/dash.mpd'

    # Roku DRM → the DASH+Widevine variant. Set last so no earlier proxy special-case
    # (which all target other sources) clobbers it. Roku's MPD CDN is CORS-open and
    # uses an absolute BaseURL, so the route just 302s Shaka to the live manifest.
    if roku_drm and ch.source_channel_id:
        from urllib.parse import quote as _quote
        preview_url = f'/play/roku/{_quote(ch.source_channel_id, safe="")}/dash.mpd'

    # Fubo DRM → the DASH+Widevine variant. Set last (same reasoning as Roku
    # above) so the earlier generic fubo proxy.m3u8 special-case doesn't win.
    # Fubo's DASH CDN is CORS-open too, so the route just 302s Shaka.
    if fubo_drm and ch.source_channel_id:
        from urllib.parse import quote as _quote
        preview_url = f'/play/fubo/{_quote(ch.source_channel_id, safe="")}/dash.mpd'

    # Cox TVE uses XCal/CENC-protected DASH+Widevine for browser playback.
    # The HLS-shaped TVE playlists remain available through /proxy.m3u8 for
    # inspection, but observed segments are not clear MPEG-TS.
    if ch.source and ch.source.name == 'cox' and ch.source_channel_id:
        from urllib.parse import quote as _quote
        _enc = _quote(ch.source_channel_id, safe='')
        preview_url = f'/play/cox/{_enc}/dash.mpd'
        license_url = f'{request.host_url.rstrip("/")}/play/cox/license/{_enc}'
        playback_mode = 'dash'
        stream_type = 'dash'

    # Philo → DASH+Widevine. Philo's MPD CDN locks CORS to philo.com, so the
    # route proxies the manifest body with permissive CORS (segments are CORS-*
    # so Shaka fetches them direct).
    if ch.source and ch.source.name == 'philo' and ch.source_channel_id:
        from urllib.parse import quote as _quote
        preview_url = f'/play/philo/{_quote(ch.source_channel_id, safe="")}/dash.mpd'

    # Sling → DASH+Widevine. Sling's CDN is currently browser-friendly, but use
    # a same-origin manifest route for admin/watch debugging consistency with
    # the other DASH DRM bridge sources.
    if ch.source and ch.source.name == 'sling' and ch.source_channel_id:
        from urllib.parse import quote as _quote
        preview_url = f'/play/sling/{_quote(ch.source_channel_id, safe="")}/dash.mpd'
        playback_mode = 'dash'
        stream_type = 'dash'

    # PBS DRM station feed (opt-in main/PBS KIDS) → DASH+Widevine via the same
    # PrismCast bridge pattern as Philo/Sling. Clear HLS PBS feeds are untouched
    # (stream_type stays 'hls', no license_url is returned for them).
    if ch.source and ch.source.name == 'pbs' and ch.source_channel_id and (ch.stream_type or '').lower() == 'dash':
        from urllib.parse import quote as _quote
        preview_url = f'/play/pbs/{_quote(ch.source_channel_id, safe="")}/dash.mpd'
        playback_mode = 'dash'
        stream_type = 'dash'

    # Vidaa DRM tiles → DASH+Widevine via the same PrismCast bridge pattern.
    # Vidaa's clear HLS channels are untouched (stream_type stays 'hls').
    if ch.source and ch.source.name == 'vidaa' and ch.source_channel_id and (ch.stream_type or '').lower() == 'dash':
        from urllib.parse import quote as _quote
        preview_url = f'/play/vidaa/{_quote(ch.source_channel_id, safe="")}/dash.mpd'
        playback_mode = 'dash'
        stream_type = 'dash'

    # NBC TVE: route through the manifest proxy that picks a single audio track —
    # see nbc_tve_dash_proxy's docstring in play.py. ?audio= is forwarded so the
    # codec choice can be exercised from the watch page without a code change.
    if ch.source and ch.source.name == 'nbc_tve' and ch.source_channel_id:
        from urllib.parse import quote as _quote
        preview_url = f'/play/nbc_tve/{_quote(ch.source_channel_id, safe="")}/dash.mpd'
        _audio_pref = (request.args.get('audio') or '').strip().lower()
        if _audio_pref in {'aac', 'ec3', 'all'}:
            preview_url += f'?audio={_audio_pref}'
        playback_mode = 'dash'
        stream_type = 'dash'

    # DRM-bridge-flagged channels (see worker._sync_intrinsic_drm_bridge) have no
    # playable direct .m3u8 — resolve() returns a raw encrypted manifest with no
    # CDM/license negotiation for a plain player. Route play_url through whichever
    # bridge is actually configured so "Open in VLC"/"Open in New Tab" hand back
    # something playable instead of a black screen.
    if getattr(ch, 'requires_drm_bridge', False) and ch.source and ch.source.name and ch.source_channel_id:
        from ..drm_bridge import DRM_BRIDGE_TRUSTED_SOURCES as _DRM_TRUSTED
        from .. import fc_player_bridge as _fc_player_bridge
        from urllib.parse import quote as _quote
        _settings = AppSettings.get()
        if (
            ch.source.name in _DRM_TRUSTED
            and _fc_player_bridge.hdmi_bridge_active(_settings)
        ):
            play_url = f'/play/fc-player/{ch.source.name}/{_quote(ch.source_channel_id, safe="")}.m3u8'
        elif _settings.prismcast_capture_configured():
            play_url = f'/play/prismcast/{ch.id}.ts'

    # True exactly when the URL the watch page will actually use
    # (info.get('preview_url') or info.get('play_url')) is the fc-player bridge URL —
    # i.e. this specific channel has no dedicated preview-proxy route, so /watch falls
    # through to the same trigger-the-device path Channels DVR uses. The watch page
    # uses this to know whether to send its "still watching" heartbeat (see
    # fc_player_bridge.note_web_heartbeat) — Channels DVR's own activity polling has
    # no visibility into someone watching through our own browser page instead.
    is_fc_player_bridge = bool(
        not preview_url and play_url and play_url.startswith('/play/fc-player/')
    )

    return {
        'stream_type': stream_type,
        'preview_url': preview_url,
        'play_url': play_url,
        'playback_mode': playback_mode,
        'license_url': license_url,
        'needs_detection': needs_detection,
        'is_fc_player_bridge': is_fc_player_bridge,
    }


@playback_bp.route('/channels/<int:channel_id>/preview', methods=['GET'])
def preview_channel(channel_id):
    ch = Channel.query.get_or_404(channel_id)
    now = datetime.now(timezone.utc)
    fast_mode = request.args.get('detect', '1') == '0'
    info = _get_playback_info(ch, fast_mode=fast_mode)
    stream_type = info['stream_type']
    preview_url = info['preview_url']
    play_url = info['play_url']
    playback_mode = info['playback_mode']
    license_url = info['license_url']
    needs_detection = info['needs_detection']

    current_program = (
        Program.query
        .filter(
            Program.channel_id == ch.id,
            Program.start_time <= now,
            Program.end_time > now,
        )
        .order_by(Program.start_time.asc())
        .first()
    )
    next_program = (
        Program.query
        .filter(
            Program.channel_id == ch.id,
            Program.start_time >= now,
        )
        .order_by(Program.start_time.asc())
        .first()
    )

    if current_program and next_program and current_program.id == next_program.id:
        next_program = (
            Program.query
            .filter(
                Program.channel_id == ch.id,
                Program.start_time >= current_program.end_time,
            )
            .order_by(Program.start_time.asc())
            .first()
        )

    def _program_dict(p):
        if not p:
            return None
        return {
            'title': p.title,
            'description': p.description,
            'start_time': _isoformat_utc(p.start_time),
            'end_time': _isoformat_utc(p.end_time),
            'category': p.category,
            'episode_title': p.episode_title,
            'season': p.season,
            'episode': p.episode,
            'original_air_date': p.original_air_date.isoformat() if p.original_air_date else None,
            'poster_url': p.poster_url or None,
        }

    future_count = Program.query.filter(
        Program.channel_id == ch.id,
        Program.end_time > now,
    ).count()
    last_future = (
        Program.query
        .filter(Program.channel_id == ch.id, Program.end_time > now)
        .order_by(Program.end_time.desc())
        .first()
    )
    last_end = last_future.end_time.replace(tzinfo=timezone.utc) if last_future and last_future.end_time.tzinfo is None else (last_future.end_time if last_future else None)
    epg_hours = round((last_end - now).total_seconds() / 3600, 1) if last_end else 0

    return jsonify({
        'channel': {
            'id': ch.id,
            'name': ch.name,
            'source_name': ch.source.name if ch.source else None,
            'source_display_name': ch.source.display_name if ch.source else None,
            'source_channel_id': ch.source_channel_id,
            'stream_type': stream_type or None,
            'playback_mode': playback_mode,
            'category': ch.category,
            'language': ch.language,
            'country': ch.country,
            'tags': [t for t in (ch.tags or '').split(',') if t] if ch.tags else [],
            'logo_url': ch.logo_url,
            'logo_display_url': ch.logo_display_url,
            'disable_reason': ch.disable_reason,
            'is_active': ch.is_active,
            'is_enabled': ch.is_enabled,
            'description': ch.description,
            'license_url': license_url,
        },
        'current_program': _program_dict(current_program),
        'next_program': _program_dict(next_program),
        'play_url': play_url,
        'preview_url': preview_url,
        'needs_detection': needs_detection,
        'epg_programs': future_count,
        'epg_hours': epg_hours,
        'feed_memberships': _channel_feed_summaries(ch),
    })

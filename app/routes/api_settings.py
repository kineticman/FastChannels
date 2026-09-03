import logging
import os as _os
import json
import requests as _req

logger = logging.getLogger(__name__)
from datetime import datetime, timezone
from urllib.parse import urlsplit

from flask import Blueprint, jsonify, request, current_app, Response
from ..extensions import db
from ..models import AppSettings
from ..timezone_utils import normalize_timezone_name, write_timezone_cache

from .api_shared import _invalidate_and_refresh_xml
from .api_dvr import _reconcile_drm_bridge_mode

settings_bp = Blueprint('api_settings', __name__)


def _normalize_server_url(value: str | None, default_port: int | None = None) -> str | None:
    raw = (value or '').strip()
    if not raw:
        return None

    if '://' not in raw:
        raw = f'http://{raw}'

    parsed = urlsplit(raw)
    scheme = parsed.scheme or 'http'
    netloc = parsed.netloc or parsed.path
    path = parsed.path if parsed.netloc else ''
    host = netloc.strip()

    if not host:
        return None

    if path not in ('', '/'):
        host = f'{host}{path}'

    if default_port is not None and ':' not in host.rsplit(']', 1)[-1]:
        host = f'{host}:{default_port}'

    return f'{scheme}://{host}'.rstrip('/')


def _bridge_host_from_input(value: str | None) -> str | None:
    """Extract a bare host from a device-bridge IP field — strips any scheme/port
    the user typed. The admin form takes one "IP[:port]" field and derives the
    adb address (:5555, not currently overridable) from it."""
    raw = (value or '').strip()
    if not raw:
        return None
    if '://' not in raw:
        raw = f'http://{raw}'
    return urlsplit(raw).hostname or None


@settings_bp.route('/settings', methods=['GET', 'POST'])
def app_settings():
    row = AppSettings.get()
    if request.method == 'POST':
        data = request.get_json(force=True) or {}
        if 'channels_dvr_url' in data:
            row.channels_dvr_url = _normalize_server_url(data['channels_dvr_url'], default_port=8089)
        if 'public_base_url' in data:
            row.public_base_url = _normalize_server_url(data['public_base_url'], default_port=None)
        if 'prismcast_url' in data:
            row.prismcast_url = _normalize_server_url(data['prismcast_url'], default_port=None)
        if 'prismcast_inner_url' in data:
            row.prismcast_inner_url = _normalize_server_url(data['prismcast_inner_url'], default_port=None)
        if 'prismcast_max_height' in data:
            try:
                max_height = int(data.get('prismcast_max_height') or 0)
            except (TypeError, ValueError):
                return jsonify({'error': 'Invalid PrismCast max resolution.'}), 422
            if max_height not in {0, 360, 480, 720, 1080}:
                return jsonify({'error': 'Invalid PrismCast max resolution.'}), 422
            row.prismcast_max_height = max_height
        if 'drm_bridge_enabled' in data:
            _new_drm_bridge = bool(data['drm_bridge_enabled'])
            if _new_drm_bridge != bool(row.drm_bridge_enabled):
                row.drm_bridge_enabled = _new_drm_bridge
                # Reconcile existing DRM channels immediately so the toggle takes effect
                # without waiting for the next stream audit.
                _reconcile_drm_bridge_mode()
        if 'timezone_name' in data:
            tz_name = normalize_timezone_name(data.get('timezone_name'))
            if data.get('timezone_name') and tz_name is None:
                return jsonify({'error': f"Invalid timezone: {data.get('timezone_name')}"}), 422
            row.timezone_name = tz_name
        if 'auto_allow_new_channels' in data:
            row.auto_allow_new_channels = bool(data['auto_allow_new_channels'])
        if 'gracenote_auto_fill' in data:
            row.gracenote_auto_fill = bool(data['gracenote_auto_fill'])
        if 'dvr_epg_auto_refresh' in data:
            row.dvr_epg_auto_refresh = bool(data['dvr_epg_auto_refresh'])
        if 'image_proxy_enabled' in data:
            row.image_proxy_enabled = bool(data['image_proxy_enabled'])
        if 'm3u_rewrite_timestamps' in data:
            row.m3u_rewrite_timestamps = bool(data['m3u_rewrite_timestamps'])
        if 'fc_player_enabled' in data:
            _new_fc_player = bool(data['fc_player_enabled'])
            if _new_fc_player != bool(row.fc_player_bridge_enabled):
                row.fc_player_bridge_enabled = _new_fc_player
                # Reconcile existing DRM channels immediately so the toggle takes effect
                # without waiting for the next stream audit — see drm_bridge.drm_bridge_mode_for.
                _reconcile_drm_bridge_mode()
        if 'fc_player_ip' in data:
            _fcp_host = _bridge_host_from_input(data['fc_player_ip'])
            if data.get('fc_player_ip') and not _fcp_host:
                return jsonify({'error': 'Invalid Firestick/Android TV IP address.'}), 422
            row.fc_player_bridge_adb_address = f'{_fcp_host}:5555' if _fcp_host else None
        if 'fc_player_encoder_url' in data:
            row.fc_player_bridge_encoder_url = _normalize_server_url(data['fc_player_encoder_url'], default_port=None)
        if 'fc_player_idle_stop_enabled' in data:
            row.fc_player_bridge_idle_stop_enabled = bool(data['fc_player_idle_stop_enabled'])
        if 'fc_player_captions_enabled' in data:
            row.fc_player_bridge_captions_enabled = bool(data['fc_player_captions_enabled'])
        if 'fc_player_ah4c_enabled' in data:
            row.fc_player_bridge_ah4c_enabled = bool(data['fc_player_ah4c_enabled'])
        if 'fc_player_ah4c_url' in data:
            row.fc_player_bridge_ah4c_url = _normalize_server_url(data['fc_player_ah4c_url'], default_port=None)
        if 'gracenote_map_url' in data:
            row.gracenote_map_url = (data['gracenote_map_url'] or '').strip() or None
        if 'gracenote_contribution_url' in data:
            row.gracenote_contribution_url = (data['gracenote_contribution_url'] or '').strip() or None
        db.session.commit()
        write_timezone_cache(row.timezone_name)
        _invalidate_and_refresh_xml()
        row = AppSettings.get()
    # Always :5555 (the standard adb port, not user-overridable), so just strip it
    # back off for display.
    _fc_player_ip_display = _bridge_host_from_input(row.effective_fc_player_bridge_adb_address()) or ''
    return jsonify({
        'channels_dvr_url':  row.effective_channels_dvr_url(),
        'public_base_url':   row.effective_public_base_url(),
        'timezone_name':     row.effective_timezone_name(),
        'auto_allow_new_channels': row.auto_allow_new_channels if row.auto_allow_new_channels is not None else True,
        'gracenote_auto_fill': row.gracenote_auto_fill if row.gracenote_auto_fill is not None else True,
        'dvr_epg_auto_refresh': row.dvr_epg_auto_refresh if row.dvr_epg_auto_refresh is not None else True,
        'image_proxy_enabled': row.image_proxy_enabled if row.image_proxy_enabled is not None else True,
        'm3u_rewrite_timestamps': bool(row.m3u_rewrite_timestamps),
        'gracenote_map_url': row.gracenote_map_url or '',
        'gracenote_contribution_url': row.gracenote_contribution_url or '',
        'prismcast_url': row.effective_prismcast_url() or '',
        'prismcast_inner_url': row.prismcast_inner_url or '',
        'prismcast_max_height': int(row.prismcast_max_height or 0),
        'drm_bridge_enabled': bool(row.drm_bridge_enabled),
        'fc_player_enabled': bool(row.fc_player_bridge_enabled),
        'fc_player_ip': _fc_player_ip_display,
        'fc_player_encoder_url': row.effective_fc_player_bridge_encoder_url() or '',
        'fc_player_idle_stop_enabled': bool(row.fc_player_bridge_idle_stop_enabled),
        'fc_player_captions_enabled': bool(row.fc_player_bridge_captions_enabled),
        'fc_player_ah4c_enabled': bool(row.fc_player_bridge_ah4c_enabled),
        'fc_player_ah4c_url': row.fc_player_bridge_ah4c_url or '',
        'channels_dvr_url_source': 'db' if (row.channels_dvr_url or '').strip() else ('env' if row.env_channels_dvr_url() is not None else 'unset'),
        'public_base_url_source': 'db' if (row.public_base_url or '').strip() else ('env' if row.effective_public_base_url() else 'unset'),
        'timezone_name_source': 'db' if (row.timezone_name or '').strip() else 'system',
    })


@settings_bp.route('/settings/fc-player/test', methods=['POST'])
def test_fc_player():
    """adb-reachability check for the configured FastChannels Player device (plus
    whether the player app is installed there), and a check that the encoder/capture
    stream URL is set and reachable — a green result here should mean a bridged
    channel would really work end-to-end, not just that the device is online."""
    from .. import fc_player_bridge
    if not fc_player_bridge.is_configured():
        return jsonify({'ok': False, 'message': 'Enable the bridge and set a device IP first.'}), 400
    ok, message = fc_player_bridge.test_connection()
    if not ok:
        return jsonify({'ok': False, 'message': message})

    encoder_url = AppSettings.get().effective_fc_player_bridge_encoder_url()
    if not encoder_url:
        return jsonify({
            'ok': True,
            'warning': True,
            'message': f'{message} No encoder/capture stream URL is set — bridged channels will fail with a 503 until you add one.',
        })

    try:
        with _req.get(encoder_url, stream=True, timeout=3) as r:
            if r.ok:
                return jsonify({'ok': True, 'message': f'{message} Encoder/capture stream URL is reachable.'})
            return jsonify({
                'ok': True,
                'warning': True,
                'message': f'{message} Encoder/capture stream URL returned HTTP {r.status_code} — bridged channels may fail.',
            })
    except _req.RequestException:
        return jsonify({
            'ok': True,
            'warning': True,
            'message': f'{message} Encoder/capture stream URL is not reachable — bridged channels will fail until that stream is up.',
        })


@settings_bp.route('/settings/fc-player/install', methods=['POST'])
def install_fc_player():
    """adb-installs the bundled FastChannels Player release APK onto the configured
    device — no manual sideload step. Live-verified 2026-08-25: works on a device
    that's never had "Apps from Unknown Sources" touched, since adb install goes
    through the privileged shell/package-manager path, not the tap-to-install UI flow
    that setting actually gates."""
    from .. import fc_player_bridge
    address = AppSettings.get().effective_fc_player_bridge_adb_address()
    if not address:
        return jsonify({'ok': False, 'message': 'Set a device IP first.'}), 400
    apk_path = fc_player_bridge.bundled_apk_path()
    if not apk_path:
        return jsonify({
            'ok': False,
            'message': 'No FastChannels Player release is bundled in this build.',
        }), 400
    ok, message = fc_player_bridge.install_app(apk_path)
    return jsonify({'ok': ok, 'message': message})


def _remember_fc_player_device_settings(previous: dict | None) -> None:
    """Save the pre-headless snapshot only once, so Restore stays meaningful."""
    if not previous:
        return
    row = AppSettings.get()
    if not row.fc_player_device_settings_backup:
        row.fc_player_device_settings_backup = json.dumps(previous, separators=(',', ':'))
        db.session.commit()


@settings_bp.route('/settings/fc-player/device-controls', methods=['GET'])
def fc_player_device_controls_status():
    """Live ADB diagnostics for the settings page's Fire TV Device Controls modal."""
    from .. import fc_player_bridge
    status = fc_player_bridge.device_controls_status()
    status['restore_available'] = bool(AppSettings.get().fc_player_device_settings_backup)
    return jsonify(status), (200 if status.get('ok') else 400)


@settings_bp.route('/settings/fc-player/device-controls/wake', methods=['POST'])
def wake_fc_player_device():
    from .. import fc_player_bridge
    ok, message = fc_player_bridge.wake_device()
    return jsonify({'ok': ok, 'message': message}), (200 if ok else 400)


@settings_bp.route('/settings/fc-player/device-controls/power', methods=['POST'])
def save_fc_player_device_power():
    """Apply explicit power settings, retaining the first pre-change snapshot."""
    from .. import fc_player_bridge
    data = request.get_json(silent=True) or {}
    stay_awake = data.get('stay_awake')
    screen_off_timeout = data.get('screen_off_timeout')
    sleep_timeout = data.get('sleep_timeout')
    if not isinstance(stay_awake, bool):
        return jsonify({'ok': False, 'message': 'Invalid keep-awake setting.'}), 400
    ok, message, previous = fc_player_bridge.set_device_power_settings(
        stay_awake=stay_awake,
        screen_off_timeout=screen_off_timeout,
        sleep_timeout=sleep_timeout,
    )
    if ok:
        _remember_fc_player_device_settings(previous)
    return jsonify({'ok': ok, 'message': message}), (200 if ok else 400)


@settings_bp.route('/settings/fc-player/device-controls/headless', methods=['POST'])
def apply_fc_player_headless_preset():
    from .. import fc_player_bridge
    ok, message, previous = fc_player_bridge.headless_power_settings()
    if ok:
        _remember_fc_player_device_settings(previous)
    return jsonify({'ok': ok, 'message': message}), (200 if ok else 400)


@settings_bp.route('/settings/fc-player/device-controls/restore', methods=['POST'])
def restore_fc_player_device_power():
    from .. import fc_player_bridge
    row = AppSettings.get()
    try:
        previous = json.loads(row.fc_player_device_settings_backup or '')
    except (TypeError, ValueError):
        previous = None
    ok, message = fc_player_bridge.restore_device_power_settings(previous)
    if ok:
        row.fc_player_device_settings_backup = None
        db.session.commit()
    return jsonify({'ok': ok, 'message': message}), (200 if ok else 400)


@settings_bp.route('/settings/fc-player/ah4c-scripts', methods=['GET'])
def export_ah4c_scripts():
    """Downloads a pre-configured ah4c STREAMER_APP script set (prebmitune.sh,
    bmitune.sh, stopbmitune.sh, reboot.sh) for driving FastChannels Player from
    ah4c instead of Hulu/YouTube TV/etc. — see project memory for why ah4c is a
    useful "any HDMI encoder brand" front end for the bridge.

    Takes ?url=<where this server is reachable from the ah4c host> rather than
    inferring it from this request's own Host header — the machine asking for
    this download (someone's browser) and the machine that will actually run
    these scripts (the ah4c container, often a different box entirely) aren't
    guaranteed to share a reachable address. See _normalize_server_url for what
    counts as valid input."""
    from ..ah4c_export import build_ah4c_scripts_tarball

    fastchannels_url = _normalize_server_url(request.args.get('url'))
    if not fastchannels_url:
        return jsonify({'ok': False, 'message': 'A valid FastChannels URL is required.'}), 400

    tarball = build_ah4c_scripts_tarball(fastchannels_url)
    return Response(
        tarball,
        mimetype='application/gzip',
        headers={'Content-Disposition': 'attachment; filename="fastchannels-ah4c-scripts.tar.gz"'},
    )


@settings_bp.route('/fc-player/heartbeat', methods=['POST'])
def fc_player_heartbeat():
    """Periodic "still watching" ping from the /watch page for a channel that fell
    through to the fc-player bridge path (app/templates/watch.html). Channels DVR's
    own activity polling has zero visibility into someone watching through our own
    browser page instead of through Channels DVR, so this is the idle-stop
    watchdog's other signal — see fc_player_bridge.note_web_heartbeat. Always
    best-effort and cheap: no-ops safely whether or not idle-stop is even enabled,
    so the /watch page never needs to know that detail."""
    from .. import fc_player_bridge
    data = request.get_json(silent=True) or {}
    source = (data.get('source') or '').strip()
    channel_id = (data.get('channel_id') or '').strip()
    if source and channel_id:
        fc_player_bridge.note_web_heartbeat(f'{source}:{channel_id}')
    return ('', 204)


@settings_bp.route('/settings/gracenote-auto-clear', methods=['POST'])
def gracenote_auto_clear():
    """Disable auto-fill and clear all auto-assigned Gracenote IDs."""
    from .tasks import trigger_gracenote_auto_clear
    row = AppSettings.get()
    row.gracenote_auto_fill = False
    db.session.commit()
    trigger_gracenote_auto_clear()
    return jsonify({'status': 'queued'})


@settings_bp.route('/settings/gracenote-clear-all', methods=['POST'])
def gracenote_clear_all():
    """Disable auto-fill, clear ALL Gracenote IDs, and set all channels to mode='off'."""
    from .tasks import trigger_gracenote_clear_all
    row = AppSettings.get()
    row.gracenote_auto_fill = False
    db.session.commit()
    trigger_gracenote_clear_all()
    return jsonify({'status': 'queued'})


@settings_bp.route('/settings/backup-db')
def backup_db():
    """Download a gzip-compressed copy of the live SQLite database."""
    import gzip, tempfile, os as _os, sqlite3 as _sqlite3
    db_path = '/data/fastchannels.db'
    if not _os.path.exists(db_path):
        return jsonify({'error': 'Database file not found.'}), 404
    tmp_db  = tempfile.NamedTemporaryFile(suffix='.db',    delete=False)
    tmp_gz  = tempfile.NamedTemporaryFile(suffix='.db.gz', delete=False)
    tmp_db.close(); tmp_gz.close()
    try:
        # SQLite online backup — safe while DB is live
        src = _sqlite3.connect(db_path)
        dst = _sqlite3.connect(tmp_db.name)
        src.backup(dst)
        src.close(); dst.close()
        # Compress
        with open(tmp_db.name, 'rb') as f_in, gzip.open(tmp_gz.name, 'wb', compresslevel=6) as f_out:
            f_out.write(f_in.read())
        ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        filename = f'fastchannels_backup_{ts}.db.gz'
        return current_app.response_class(
            open(tmp_gz.name, 'rb').read(),
            mimetype='application/gzip',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'},
        )
    finally:
        _os.unlink(tmp_db.name)
        _os.unlink(tmp_gz.name)


@settings_bp.route('/settings/restore-db', methods=['POST'])
def restore_db():
    """Accept a .db or .db.gz upload and atomically replace the live database."""
    import gzip as _gzip

    f = request.files.get('db_file')
    if not f:
        return jsonify({'error': 'No file uploaded.'}), 400

    filename = f.filename or ''
    _MAX_DB_SIZE = 200 * 1024 * 1024  # 200 MB
    data = f.read(_MAX_DB_SIZE + 1)
    if len(data) > _MAX_DB_SIZE:
        return jsonify({'error': 'Upload too large — 200 MB maximum.'}), 413

    if filename.lower().endswith('.gz'):
        try:
            data = _gzip.decompress(data)
        except Exception:
            return jsonify({'error': 'Could not decompress .gz file — is it a valid gzip archive?'}), 400

    ok, err = _write_db_and_reload(data)
    if not ok:
        return jsonify({'error': err}), 500
    return jsonify({'status': 'ok'})


def _write_db_and_reload(data: bytes):
    """Validate SQLite bytes, atomically swap them into the live DB, drop stale
    WAL/SHM, and SIGHUP gunicorn so workers reopen fresh connections.

    Returns (ok: bool, error_message: str | None). Shared by the upload-restore
    and on-disk-backup-restore endpoints.
    """
    import signal as _signal

    SQLITE_MAGIC = b'SQLite format 3\x00'
    if not data.startswith(SQLITE_MAGIC):
        return False, 'File does not appear to be a valid SQLite database.'

    db_path  = '/data/fastchannels.db'
    tmp_path = '/data/fastchannels_restore_tmp.db'
    try:
        with open(tmp_path, 'wb') as fp:
            fp.write(data)
        # Atomically replace the live DB first, then remove stale WAL/SHM so new
        # connections get a clean WAL rather than replaying the old one.
        _os.replace(tmp_path, db_path)
        for ext in ('-wal', '-shm'):
            stale = db_path + ext
            if _os.path.exists(stale):
                try:
                    _os.unlink(stale)
                except OSError:
                    pass
    except Exception as exc:
        try:
            _os.unlink(tmp_path)
        except OSError:
            pass
        return False, f'Failed to write database: {exc}'

    # Gracefully reload gunicorn workers so they open fresh connections to the new DB
    try:
        _os.kill(_os.getppid(), _signal.SIGHUP)
    except Exception:
        pass

    return True, None


# Directory of nightly on-disk DB backups written by app.worker._rq_db_backup.
_LOCAL_BACKUP_DIR = '/data/backups'


@settings_bp.route('/settings/local-backups')
def list_local_backups():
    """List the nightly on-disk DB backups in /data/backups, newest first."""
    import glob as _glob

    out = []
    try:
        for path in _glob.glob(_os.path.join(_LOCAL_BACKUP_DIR, 'fastchannels_backup_*.db.gz')):
            try:
                st = _os.stat(path)
            except OSError:
                continue
            out.append({
                'name':  _os.path.basename(path),
                'size':  st.st_size,
                'mtime': st.st_mtime,
            })
    except Exception:
        pass
    out.sort(key=lambda b: b['name'], reverse=True)
    return jsonify({'backups': out})


@settings_bp.route('/settings/restore-local-backup', methods=['POST'])
def restore_local_backup():
    """Restore the live DB from one of the on-disk nightly backups."""
    import gzip as _gzip

    payload = request.get_json(silent=True) or {}
    name = (payload.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'No backup specified.'}), 400
    # Guard against path traversal — basename only, and must match the expected shape.
    if (name != _os.path.basename(name)
            or not name.startswith('fastchannels_backup_')
            or not name.endswith('.db.gz')):
        return jsonify({'error': 'Invalid backup name.'}), 400

    path = _os.path.join(_LOCAL_BACKUP_DIR, name)
    if not _os.path.isfile(path):
        return jsonify({'error': 'Backup not found.'}), 404

    try:
        with _gzip.open(path, 'rb') as fp:
            data = fp.read()
    except Exception:
        return jsonify({'error': 'Could not read backup archive — it may be corrupt.'}), 400

    ok, err = _write_db_and_reload(data)
    if not ok:
        return jsonify({'error': err}), 500
    return jsonify({'status': 'ok'})

"""Every FastChannels Player device this install knows about, for the Bridge page's
Devices card.

Devices come from three places that used to be separate: the HDMI Capture device
IP setting, ah4c's configured TUNERn_IP list (read live from ah4c), and the
bridge_devices table — rows added by hand or recorded by trigger_channel() on
every real tune (so a stick reached only through ah4c's per-tune ?adb= override
is still remembered after a restart, when Redis's active-device keys are gone).

Listing is cheap (no adb). Probing is one device per call so the page can fire
them in parallel and fill each card in as its answer lands — an unreachable stick
costs a full adb timeout and shouldn't hold up the rest.
"""
import logging
import re
import subprocess
from datetime import datetime, timezone

from . import fc_player_bridge as fcp
from .extensions import db
from .models import AppSettings, BridgeDevice, Channel, Source

logger = logging.getLogger(__name__)

_ADDRESS_RE = re.compile(r'^[A-Za-z0-9.\-]{1,255}(?::\d{1,5})?$')
_SECTION = '__FC_SECTION__'


def normalize_address(raw: str | None) -> str | None:
    """host or host:port -> host:port (adb's :5555 default), or None if malformed."""
    raw = (raw or '').strip()
    if not raw or not _ADDRESS_RE.match(raw):
        return None
    return raw if ':' in raw else f'{raw}:5555'


def remember_tune(address: str, channel_key: str | None) -> None:
    """Record a tune against its device. Best-effort: a failed write must never
    block playback, and the watchdog's retunes can run without an app context."""
    # ah4c's ?adb= override may be a bare TUNERn_IP; store the same host:port
    # form known_devices() keys on so one stick doesn't show up twice.
    address = normalize_address(address) or address
    try:
        row = BridgeDevice.query.filter_by(address=address).first()
        if row is None:
            row = BridgeDevice(address=address)
            db.session.add(row)
        row.last_tuned_at = datetime.now(timezone.utc)
        if channel_key:
            row.last_channel_key = channel_key[:255]
        db.session.commit()
    except Exception as e:
        logger.debug('[bridge-devices] remember_tune(%s) failed: %s', address, e)
        try:
            db.session.rollback()
        except Exception:
            pass


def known_devices() -> tuple[list[dict], str | None]:
    """(devices, ah4c_error). Ordered HDMI Capture device first, then ah4c tuners
    in ah4c's order, then remembered devices by most recent tune."""
    settings = AppSettings.get()
    devices: dict[str, dict] = {}

    def entry(address: str) -> dict:
        if address not in devices:
            devices[address] = {
                'address': address,
                'host': address.rsplit(':', 1)[0] if address.endswith(':5555') else address,
                'label': None,
                'roles': [],
                'remembered': False,
                'added_manually': False,
                'last_tuned_at': None,
                'last_channel_key': None,
                'last_channel_name': None,
            }
        return devices[address]

    hdmi = normalize_address(settings.effective_fc_player_bridge_adb_address())
    if hdmi:
        entry(hdmi)['roles'].append('HDMI Capture')

    ah4c_error = None
    try:
        for idx, ip in enumerate(fcp.ah4c_tuner_ips(), start=1):
            address = normalize_address(ip)
            if address:
                entry(address)['roles'].append(f'ah4c tuner {idx}')
    except fcp.FcPlayerNotConfigured:
        pass
    except Exception as e:
        ah4c_error = f"Couldn't read ah4c's tuner list: {e}"

    rows = BridgeDevice.query.order_by(BridgeDevice.last_tuned_at.desc().nullslast()).all()
    for row in rows:
        d = entry(row.address)
        d.update({
            'label': row.label,
            'remembered': True,
            'added_manually': bool(row.added_manually),
            'last_tuned_at': _iso_utc(row.last_tuned_at),
            'last_channel_key': row.last_channel_key,
            'last_channel_name': channel_name(row.last_channel_key),
        })
    return list(devices.values()), ah4c_error


def _iso_utc(value: datetime | None) -> str | None:
    # SQLite hands DateTime(timezone=True) back naive; every write here is UTC.
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def is_known(address: str) -> bool:
    return any(d['address'] == address for d in known_devices()[0])


def channel_name(channel_key: str | None) -> str | None:
    """'<source>:<channel_id>' -> the channel's display name, falling back to the key."""
    if not channel_key or ':' not in channel_key:
        return channel_key
    source_name, channel_id = channel_key.split(':', 1)
    try:
        row = (db.session.query(Channel.name)
               .join(Source, Source.id == Channel.source_id)
               .filter(Source.name == source_name, Channel.source_channel_id == channel_id)
               .first())
    except Exception:
        row = None
    return row[0] if row and row[0] else channel_key


def _focus_label(focus: str) -> str | None:
    """mCurrentFocus=Window{... u0 com.pkg/com.pkg.Activity} -> a short label."""
    match = re.search(r'\s([A-Za-z0-9_.]+)/', focus or '')
    if not match:
        return None
    package = match.group(1)
    if package == fcp._PACKAGE_NAME:
        return 'FastChannels Player'
    if 'launcher' in package.lower():
        return 'Home screen'
    return package


def _device_extras(address: str) -> dict:
    """Identity, wake state, foreground app, and live playback state in one shell
    round-trip. Sections are delimited explicitly so a command that prints nothing
    (a missing grep match, an unset device_name) can't shift the others."""
    out = {'model': None, 'device_name': None, 'awake': None,
           'focus_app': None, 'player_playing': None}
    remote = f'; echo {_SECTION}; '.join([
        'getprop ro.product.model',
        'settings get global device_name',
        'dumpsys power | grep -m1 mWakefulness=',
        'dumpsys window | grep -m1 mCurrentFocus',
        'dumpsys media_session',
    ])
    try:
        res = subprocess.run(
            ['adb', '-s', address, 'shell', remote],
            capture_output=True, timeout=fcp._ADB_TIMEOUT, check=False, text=True,
        )
    except Exception:
        return out
    parts = [p.strip() for p in (res.stdout or '').split(_SECTION)]
    parts += [''] * (5 - len(parts))
    model, name, power, focus, sessions = parts[:5]

    out['model'] = model or None
    out['device_name'] = name if name and name.lower() != 'null' else None
    wake = re.search(r'mWakefulness=(\w+)', power)
    if wake:
        out['awake'] = wake.group(1).lower() == 'awake'
    out['focus_app'] = _focus_label(focus)
    if sessions:
        session = fcp._PLAYER_SESSION_RE.search(sessions)
        out['player_playing'] = bool(session and session.group(1) == '3')
    return out


def _active_channel_key(address: str) -> str | None:
    # Keyed by whatever address the tune used, which for ah4c can be the bare host.
    candidates = [address]
    if address.endswith(':5555'):
        candidates.append(address[:-len(':5555')])
    try:
        r = fcp._redis()
        for candidate in candidates:
            raw = r.get(fcp._ACTIVE_DEVICE_PREFIX + candidate)
            if raw:
                return raw.decode()
    except Exception:
        pass
    return None


def probe(address: str) -> dict:
    """Live adb status for one device. Fields degrade to None when unknown."""
    state, message = fcp._adb_state_for(address)
    result = {
        'address': address,
        'state': state,
        'authorized': state == 'device',
        'message': message,
    }
    if state != 'device':
        return result
    result.update(fcp._device_os_and_sleep(address))
    result.update(_device_extras(address))
    key = _active_channel_key(address)
    if result.get('player_playing') and key:
        result['now_playing_key'] = key
        result['now_playing'] = channel_name(key)
    return result


def save_label(address: str, label: str | None, *, manual: bool = False) -> BridgeDevice:
    row = BridgeDevice.query.filter_by(address=address).first()
    if row is None:
        row = BridgeDevice(address=address, added_manually=manual)
        db.session.add(row)
    elif manual:
        row.added_manually = True
    row.label = (label or '').strip()[:128] or None
    db.session.commit()
    return row


def forget(address: str) -> bool:
    row = BridgeDevice.query.filter_by(address=address).first()
    if row is None:
        return False
    db.session.delete(row)
    db.session.commit()
    return True

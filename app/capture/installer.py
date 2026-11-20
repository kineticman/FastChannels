"""Install, re-install and remove the capture add-on's system packages.

Run by the capture daemon (root) in a background thread, never by gunicorn.

Google Chrome is downloaded straight from Google at the user's request, so
FastChannels never redistributes it. PulseAudio and FFmpeg come from the image's
own Debian repositories. Every .deb apt fetches is kept in DEBS_DIR on /data:
after an image update recreates the container, reinstall() puts the same
packages back from that cache, re-downloading only what the new base needs.
"""
from __future__ import annotations

import glob
import logging
import os
import shutil
import subprocess
import time

import requests

from . import (ADDON_DIR, CHROME_BIN, DEBS_DIR, HOME_DIR, PROFILES_DIR,
               read_status, supported_platform, write_status)

logger = logging.getLogger(__name__)

CHROME_DEB_URL = 'https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb'
CHROME_DEB = os.path.join(DEBS_DIR, 'google-chrome-stable_current_amd64.deb')
APT_PACKAGES = ('pulseaudio', 'ffmpeg')
CAPTURE_USER = 'fccapture'

_APT_ENV = dict(os.environ, DEBIAN_FRONTEND='noninteractive')


def _run(cmd: list[str], timeout: int) -> subprocess.CompletedProcess:
    logger.info('[capture-install] $ %s', ' '.join(cmd))
    proc = subprocess.run(cmd, env=_APT_ENV, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or '').strip().splitlines()[-6:]
        raise RuntimeError(f'{os.path.basename(cmd[0])} exited {proc.returncode}: ' + ' | '.join(tail))
    return proc


def _debian_version() -> str:
    try:
        with open('/etc/debian_version', encoding='utf-8') as fp:
            return fp.read().strip()
    except OSError:
        return ''


def chrome_version() -> str:
    if not os.path.exists(CHROME_BIN):
        return ''
    try:
        out = subprocess.run([CHROME_BIN, '--version'], capture_output=True, text=True, timeout=20)
        return out.stdout.strip()
    except Exception:
        return ''


def widevine_present() -> bool:
    return os.path.exists('/opt/google/chrome/WidevineCdm/manifest.json')


def packages_present() -> bool:
    return (os.path.exists(CHROME_BIN) and shutil.which('pulseaudio') is not None
            and shutil.which('ffmpeg') is not None and shutil.which('Xvfb') is not None)


def ensure_capture_user() -> tuple[int, int]:
    """The unprivileged account the tuners run as. Recreated after every container
    recreate (it lives in the container layer); its home is on /data."""
    import pwd
    try:
        pw = pwd.getpwnam(CAPTURE_USER)
    except KeyError:
        _run(['useradd', '--system', '--no-create-home', '--home-dir', HOME_DIR,
              '--shell', '/usr/sbin/nologin', CAPTURE_USER], timeout=30)
        pw = pwd.getpwnam(CAPTURE_USER)
    for d in (HOME_DIR, PROFILES_DIR):
        os.makedirs(d, exist_ok=True)
        _chown_tree(d, pw.pw_uid, pw.pw_gid)
    return pw.pw_uid, pw.pw_gid


def _chown_tree(root: str, uid: int, gid: int) -> None:
    try:
        st = os.stat(root)
        if st.st_uid == uid and st.st_gid == gid:
            return  # already ours; skip walking a large profile tree on every boot
    except OSError:
        return
    for dirpath, dirnames, filenames in os.walk(root):
        for name in [dirpath] + [os.path.join(dirpath, n) for n in dirnames + filenames]:
            try:
                os.lchown(name, uid, gid)
            except OSError:
                pass


def _download_chrome(progress) -> None:
    os.makedirs(DEBS_DIR, exist_ok=True)
    tmp = CHROME_DEB + '.part'
    with requests.get(CHROME_DEB_URL, stream=True, timeout=(10, 60)) as r:
        r.raise_for_status()
        total = int(r.headers.get('Content-Length') or 0)
        done = 0
        last = 0.0
        with open(tmp, 'wb') as fp:
            for chunk in r.iter_content(1024 * 1024):
                fp.write(chunk)
                done += len(chunk)
                if total and time.monotonic() - last > 2:
                    last = time.monotonic()
                    progress(f'Downloading Google Chrome ({done * 100 // total}%)')
    os.replace(tmp, CHROME_DEB)


def _apt_install(progress) -> None:
    os.makedirs(os.path.join(DEBS_DIR, 'partial'), exist_ok=True)
    cache = f'Dir::Cache::archives={DEBS_DIR}'
    progress('Updating package lists')
    _run(['apt-get', 'update', '-q'], timeout=300)
    progress('Installing Chrome, PulseAudio and FFmpeg')
    _run(['apt-get', 'install', '-y', '-q', '--no-install-recommends', '-o', cache,
          *APT_PACKAGES, CHROME_DEB], timeout=1800)
    # Keep the cache (that's the point of DEBS_DIR) but drop the package lists from
    # the container layer, like the Dockerfile does.
    shutil.rmtree('/var/lib/apt/lists', ignore_errors=True)
    os.makedirs('/var/lib/apt/lists/partial', exist_ok=True)


def install(*, refresh_chrome: bool = False) -> None:
    """Full install (or Chrome update with refresh_chrome). Writes status.json as it
    goes; never raises."""
    def progress(msg):
        logger.info('[capture-install] %s', msg)
        write_status('installing', msg)

    if not supported_platform():
        write_status('failed', 'Google Chrome for Linux is only available on amd64 (x86_64).')
        return
    try:
        os.makedirs(ADDON_DIR, exist_ok=True)
        if refresh_chrome or not os.path.exists(CHROME_DEB):
            progress('Downloading Google Chrome')
            _download_chrome(progress)
        _apt_install(progress)
        if not packages_present():
            raise RuntimeError('packages installed but chrome, pulseaudio or ffmpeg is missing')
        ensure_capture_user()
        write_status('installed', 'Installed', chrome_version=chrome_version(),
                     widevine=widevine_present(), debian_version=_debian_version(),
                     cache_mb=_cache_mb())
        logger.info('[capture-install] done: %s', chrome_version())
    except Exception as e:
        logger.exception('[capture-install] failed')
        write_status('failed', f'Install failed: {e}')


def reinstall() -> bool:
    """Boot path: the add-on is installed per status.json but this container's layer
    lacks the packages (image update / container recreate). Reinstall from the cache;
    apt only downloads what the cache can't satisfy. Returns success."""
    if packages_present():
        return True
    logger.info('[capture-install] packages missing after container recreate — reinstalling from cache')
    prev = read_status()
    write_status('installing', 'Reinstalling after a container update')
    try:
        try:
            _apt_install(lambda m: write_status('installing', f'Reinstalling: {m}'))
        except Exception as e:
            # No network at boot: fall back to dpkg over the cached .debs whose
            # package isn't installed at all (never downgrade a base-image package).
            logger.warning('[capture-install] apt reinstall failed (%s); trying offline dpkg', e)
            _offline_dpkg()
        if not packages_present():
            raise RuntimeError('chrome, pulseaudio or ffmpeg still missing')
        ensure_capture_user()
        write_status('installed', 'Installed', chrome_version=chrome_version(),
                     widevine=widevine_present(), debian_version=_debian_version(),
                     cache_mb=_cache_mb(), installed_at=prev.get('installed_at'))
        return True
    except Exception as e:
        logger.exception('[capture-install] reinstall failed')
        write_status('failed', f'Reinstall after container update failed: {e}. Click Reinstall.')
        return False


def _offline_dpkg() -> None:
    debs = []
    for path in sorted(glob.glob(os.path.join(DEBS_DIR, '*.deb'))):
        try:
            pkg = subprocess.run(['dpkg-deb', '-f', path, 'Package'], capture_output=True,
                                 text=True, timeout=30).stdout.strip()
        except Exception:
            continue
        installed = subprocess.run(['dpkg-query', '-W', '-f=${Status}', pkg],
                                   capture_output=True, text=True).stdout
        if 'install ok installed' not in installed:
            debs.append(path)
    if debs:
        _run(['dpkg', '-i', *debs], timeout=1200)


def uninstall() -> None:
    """Remove the packages from the container and the add-on folder from /data."""
    write_status('uninstalling', 'Removing')
    try:
        subprocess.run(['apt-get', 'purge', '-y', '-q', 'google-chrome-stable'],
                       env=_APT_ENV, capture_output=True, timeout=600)
        # PulseAudio/FFmpeg are left in the container layer (removing them could take
        # shared libraries other tools use); the next container recreate drops them.
    except Exception:
        logger.exception('[capture-install] purge failed (continuing)')
    shutil.rmtree(ADDON_DIR, ignore_errors=True)
    logger.info('[capture-install] add-on removed')


def _cache_mb() -> int:
    total = 0
    for path in glob.glob(os.path.join(DEBS_DIR, '*.deb')):
        try:
            total += os.path.getsize(path)
        except OSError:
            pass
    return total // (1024 * 1024)

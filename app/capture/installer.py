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
import json
import logging
import os
import shutil
import subprocess
import time

import requests

from . import (ADDON_DIR, CHROME_BIN, DEBS_DIR, HOME_DIR, PROFILES_DIR, WSL_MESA_MIN_MAJOR,
               read_status, render_node, supported_platform, write_status, wsl_gpu)

logger = logging.getLogger(__name__)

CHROME_DEB_URL = 'https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb'
CHROME_DEB = os.path.join(DEBS_DIR, 'google-chrome-stable_current_amd64.deb')
APT_PACKAGES = ('pulseaudio', 'ffmpeg')
# VA-API drivers for hardware encoding, installed only when a GPU is passed in:
# Intel (iHD, Broadwell and newer) and AMD (Mesa radeonsi; tiny, since Chrome's
# dependencies already bring Mesa's gallium libraries). NVIDIA has no VA-API encoder.
GPU_PACKAGES = ('intel-media-va-driver', 'mesa-va-drivers')
# Docker on Windows: Mesa's d3d12 VA-API driver (in mesa-va-drivers), new enough
# to encode (see WSL_MESA_MIN_MAJOR); taken from <codename>-backports when the
# base image's Mesa is older.
WSL_GPU_PACKAGES = ('mesa-va-drivers', 'libva2')
# vainfo is only for the GPU diagnostics report (lists the encoders the GPU offers);
# i965-va-driver is the fallback encoder for Intel chips iHD can't encode on
# (Kaby Lake and older; detect_encoder tries it second). Never
# required: installed best-effort alongside the drivers, and not part of
# gpu_drivers_present(), so existing installs don't get pushed into a reinstall.
GPU_TOOLS = ('vainfo', 'i965-va-driver')
# Intel's non-free builds of the two drivers above, from Debian's non-free
# component. Some Intel chips can't encode with the free ones (Kaby Lake: the free
# iHD only offers constant-quality low-power encoding and the free i965 lacks the
# encode shaders). Installed only after the free driver's test encode fails on an
# Intel GPU, and kept only if the encode then works (intel_driver.json).
NONFREE_FOR = {'intel-media-va-driver': 'intel-media-va-driver-non-free',
               'i965-va-driver': 'i965-va-driver-shaders'}
INTEL_STATE_PATH = os.path.join(ADDON_DIR, 'intel_driver.json')
NONFREE_LIST = '/etc/apt/sources.list.d/fastchannels-capture-nonfree.list'
BACKPORTS_LIST = '/etc/apt/sources.list.d/fastchannels-capture-backports.list'
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


def _deb_installed(pkg: str) -> bool:
    out = subprocess.run(['dpkg-query', '-W', '-f=${Status}', pkg], capture_output=True, text=True).stdout
    return 'install ok installed' in out


def _deb_version(pkg: str) -> str:
    out = subprocess.run(['dpkg-query', '-W', '-f=${Status} ${Version}', pkg],
                         capture_output=True, text=True).stdout
    return out.rsplit(' ', 1)[-1] if out.startswith('install ok installed') else ''


def mesa_va_major() -> int:
    """Major version of the installed mesa-va-drivers ('26.1.6-1~bpo13+1' -> 26), 0 if none."""
    version = _deb_version('mesa-va-drivers').split(':')[-1]
    try:
        return int(version.split('.', 1)[0])
    except ValueError:
        return 0


def gpu_drivers_present() -> bool:
    # On Windows only presence counts here: a backports failure must not fail the
    # whole install. The daemon checks the version before trusting the GPU.
    if wsl_gpu():
        return _deb_installed('mesa-va-drivers')
    return all(_deb_installed(p) or (p in NONFREE_FOR and _deb_installed(NONFREE_FOR[p]))
               for p in GPU_PACKAGES)


def intel_state() -> dict:
    """{'nonfree': True} when this server needs Intel's non-free drivers,
    {'failed_on': <FastChannels version>} when they didn't help either, else {}.
    'restore': True means an attempt didn't finish: put the free drivers back."""
    try:
        with open(INTEL_STATE_PATH, encoding='utf-8') as fp:
            data = json.load(fp)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _save_intel_state(state: dict) -> None:
    os.makedirs(ADDON_DIR, exist_ok=True)
    with open(INTEL_STATE_PATH + '.tmp', 'w', encoding='utf-8') as fp:
        json.dump(state, fp)
    os.replace(INTEL_STATE_PATH + '.tmp', INTEL_STATE_PATH)


def nonfree_intel_installed() -> bool:
    return _deb_installed(NONFREE_FOR['intel-media-va-driver'])


def _driver_names(packages: tuple, nonfree: bool) -> tuple:
    return tuple(NONFREE_FOR.get(p, p) if nonfree else p for p in packages)


def _add_nonfree_source() -> None:
    codename = _debian_codename()
    if not codename:
        raise RuntimeError('unknown Debian release')
    with open(NONFREE_LIST, 'w') as fp:
        fp.write(f'deb http://deb.debian.org/debian {codename} non-free\n')


def _remove_nonfree_source() -> None:
    try:
        os.unlink(NONFREE_LIST)
    except OSError:
        pass


def _drop_apt_lists() -> None:
    shutil.rmtree('/var/lib/apt/lists', ignore_errors=True)
    os.makedirs('/var/lib/apt/lists/partial', exist_ok=True)


def switch_intel_drivers(nonfree: bool) -> None:
    """Swap between the free and non-free Intel drivers (each replaces the other).
    Raises if apt can't. The non-free source only exists while apt runs."""
    os.makedirs(os.path.join(DEBS_DIR, 'partial'), exist_ok=True)
    try:
        if nonfree:
            _add_nonfree_source()
        _run(['apt-get', 'update', '-q'], timeout=300)
        _run(['apt-get', 'install', '-y', '-q', '--no-install-recommends',
              '-o', f'Dir::Cache::archives={DEBS_DIR}',
              *_driver_names(('intel-media-va-driver', 'i965-va-driver'), nonfree)], timeout=900)
    finally:
        _remove_nonfree_source()
        _drop_apt_lists()


def begin_intel_nonfree() -> None:
    """Recorded before the swap, so a crash or restart mid-attempt still gets undone."""
    _save_intel_state({'restore': True})


def keep_intel_nonfree() -> None:
    _save_intel_state({'nonfree': True})


def restore_free_intel(failed: bool | None = None) -> bool:
    """Undo an attempt that didn't prove the non-free drivers. failed=True records
    that they were tested and didn't encode; None keeps what an earlier attempt
    recorded. False if apt couldn't do it yet (the 'restore' flag stays set)."""
    from ..config import VERSION
    state = intel_state()
    if failed is not None:
        state = {'failed_on': VERSION} if failed else {}
    state.pop('restore', None)
    if nonfree_intel_installed() or _deb_installed(NONFREE_FOR['i965-va-driver']):
        try:
            switch_intel_drivers(False)
        except Exception as e:
            logger.warning('[capture] could not restore the free Intel driver yet: %s', e)
            _save_intel_state({**state, 'restore': True})
            return False
    _save_intel_state(state)
    return True


def _debian_codename() -> str:
    try:
        with open('/etc/os-release') as fp:
            for line in fp:
                if line.startswith('VERSION_CODENAME='):
                    return line.split('=', 1)[1].strip().strip('"')
    except OSError:
        pass
    return ''


def _install_wsl_mesa(cache: str) -> None:
    """Mesa's d3d12 video driver for Docker on Windows. The base image's Mesa
    (25.0.7 on trixie) initialises but segfaults on the first encoded frame, so
    take mesa-va-drivers from backports when it's older than WSL_MESA_MIN_MAJOR.
    A failure here only means CPU encoding: the daemon's test encode decides."""
    if mesa_va_major() >= WSL_MESA_MIN_MAJOR:
        return
    codename = _debian_codename()
    if not codename:
        logger.warning('[capture-install] unknown Debian release; no newer Mesa for the Windows GPU')
        return
    try:
        with open(BACKPORTS_LIST, 'w') as fp:
            fp.write(f'deb http://deb.debian.org/debian {codename}-backports main\n')
        _run(['apt-get', 'update', '-q'], timeout=300)
        _run(['apt-get', 'install', '-y', '-q', '--no-install-recommends', '-o', cache,
              '-t', f'{codename}-backports', *WSL_GPU_PACKAGES], timeout=1200)
    except Exception as e:
        logger.warning('[capture-install] newer Mesa for the Windows GPU failed (%s); GPU encoding '
                       'will likely fall back to the CPU', e)


def packages_present() -> bool:
    """Everything the add-on needs in this container layer. With a GPU passed in,
    that includes the VA-API drivers, so adding /dev/dri to an existing install
    gets them on the next boot through the same reinstall path."""
    base = (os.path.exists(CHROME_BIN) and shutil.which('pulseaudio') is not None
            and shutil.which('ffmpeg') is not None and shutil.which('Xvfb') is not None)
    return base and (render_node() is None or gpu_drivers_present())


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
                if total and time.monotonic() - last > 1:
                    last = time.monotonic()
                    pct = done * 100 // total
                    progress(f'Downloading Google Chrome ({pct}%)', 'download', pct)
    os.replace(tmp, CHROME_DEB)


def _apt_install(progress) -> None:
    os.makedirs(os.path.join(DEBS_DIR, 'partial'), exist_ok=True)
    try:
        _apt_install_packages(progress)
    finally:
        # Keep the cache (that's the point of DEBS_DIR) but drop the package lists from
        # the container layer, like the Dockerfile does.
        _remove_nonfree_source()
        _drop_apt_lists()


def _apt_install_packages(progress) -> None:
    cache = f'Dir::Cache::archives={DEBS_DIR}'
    nonfree = bool(intel_state().get('nonfree'))
    progress('Updating package lists', 'packages')
    if nonfree:
        _add_nonfree_source()
    else:
        _remove_nonfree_source()
    _run(['apt-get', 'update', '-q'], timeout=300)
    progress('Installing Chrome, PulseAudio and FFmpeg', 'packages')
    wsl = bool(render_node()) and wsl_gpu()
    gpu = WSL_GPU_PACKAGES if wsl else _driver_names(GPU_PACKAGES, nonfree) if render_node() else ()
    _run(['apt-get', 'install', '-y', '-q', '--no-install-recommends', '-o', cache,
          *APT_PACKAGES, *gpu, CHROME_DEB], timeout=1800)
    if gpu:
        try:
            _run(['apt-get', 'install', '-y', '-q', '--no-install-recommends', '-o', cache,
                  *_driver_names(GPU_TOOLS, nonfree and not wsl)], timeout=300)
        except Exception as e:
            logger.warning('[capture-install] optional GPU tools (%s) not installed: %s', ', '.join(GPU_TOOLS), e)
    if wsl:
        progress('Installing GPU drivers for Docker on Windows', 'packages')
        _install_wsl_mesa(cache)


def install(*, refresh_chrome: bool = False) -> None:
    """Full install (or Chrome update with refresh_chrome). Writes status.json as it
    goes; never raises."""
    def progress(msg, step='download', pct=None):
        # step + pct drive the Bridge panel's progress steps; msg is the plain text.
        if pct is None:
            logger.info('[capture-install] %s', msg)
        write_status('installing', msg, step=step, pct=pct)

    if not supported_platform():
        write_status('failed', 'Google Chrome for Linux is only available on amd64 (x86_64).')
        return
    try:
        os.makedirs(ADDON_DIR, exist_ok=True)
        if 'failed_on' in intel_state():
            _save_intel_state({})   # Install/Reinstall clicked: try the Intel fallback again
        if refresh_chrome or not os.path.exists(CHROME_DEB):
            progress('Downloading Google Chrome', 'download', 0)
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
            _apt_install(lambda m, step='packages', pct=None:
                         write_status('installing', f'Reinstalling: {m}', step=step, pct=pct))
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
    # The cache can hold both builds of the Intel drivers; they conflict.
    skip = set(NONFREE_FOR) if intel_state().get('nonfree') else set(NONFREE_FOR.values())
    for path in sorted(glob.glob(os.path.join(DEBS_DIR, '*.deb'))):
        try:
            pkg = subprocess.run(['dpkg-deb', '-f', path, 'Package'], capture_output=True,
                                 text=True, timeout=30).stdout.strip()
        except Exception:
            continue
        installed = subprocess.run(['dpkg-query', '-W', '-f=${Status}', pkg],
                                   capture_output=True, text=True).stdout
        if 'install ok installed' not in installed and pkg not in skip:
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

"""Generates the ah4c STREAMER_APP script set for FastChannels Player.

ah4c (github.com/sullrich/ah4c) can drive any HDMI encoder hardware it already
supports (network encoders, Hauppauge/Magewell/DeckLink via a local command) as
long as it has a STREAMER_APP script set telling it what to adb-launch and how to
confirm playback started. These four files are that script set for our own
Android Bridge app (com.fastchannels.player) — see project memory for the design.

The scripts don't hardcode anything about a user's setup except one thing: the
URL this FastChannels server is reachable at from wherever the ah4c container
runs, which callers must supply (see api_settings.export_ah4c_scripts for why
that can't be inferred from the request that asks for this download).
That exported URL is the default; a non-empty FASTCHANNELS_URL in ah4c's
environment overrides it at runtime.

When you change any script in data/ah4c_scripts/, bump the ``# YYYY.MM.DD`` date
line in that script's header (line 3) so a re-exported copy is visibly newer than
whatever a user already has deployed in their STREAMER_APP directory. The newest
of those dates is the set's version: bmitune.sh reports it on every tune
(``&scripts=``) so the Bridge page can flag tuners still running an older set.
"""

import io
import os
import re
import tarfile
import time

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), 'data', 'ah4c_scripts')
_SCRIPT_NAMES = ('prebmitune.sh', 'bmitune.sh', 'stopbmitune.sh', 'reboot.sh')
_URL_PLACEHOLDER = '__FASTCHANNELS_URL__'
_VERSION_PLACEHOLDER = '__SCRIPTS_VERSION__'
_HEADER_DATE_RE = re.compile(r'^# (\d{4}\.\d{2}\.\d{2})$', re.MULTILINE)
SCRIPTS_VERSION_RE = re.compile(r'^\d{4}\.\d{2}\.\d{2}$')


def scripts_version() -> str:
    """The newest ``# YYYY.MM.DD`` header date across the script set."""
    dates = []
    for name in _SCRIPT_NAMES:
        with open(os.path.join(_TEMPLATE_DIR, name), 'r') as f:
            match = _HEADER_DATE_RE.search(f.read(512))
        if match:
            dates.append(match.group(1))
    return max(dates)


def build_ah4c_scripts_tarball(fastchannels_url: str) -> bytes:
    """A gzipped tar of the four ah4c scripts, with fastchannels_url substituted
    in wherever bmitune.sh needs it. Caller is responsible for validating/
    normalizing fastchannels_url first (a bare host, a stray trailing slash, or
    a scheme-less value would silently break the generated bmitune.sh's curl
    call) — see api_settings._normalize_server_url."""
    version = scripts_version()
    buf = io.BytesIO()
    mtime = int(time.time())
    with tarfile.open(fileobj=buf, mode='w:gz') as tar:
        for name in _SCRIPT_NAMES:
            with open(os.path.join(_TEMPLATE_DIR, name), 'r') as f:
                content = (f.read().replace(_URL_PLACEHOLDER, fastchannels_url)
                           .replace(_VERSION_PLACEHOLDER, version))
            data = content.encode('utf-8')
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mode = 0o755
            info.mtime = mtime
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()

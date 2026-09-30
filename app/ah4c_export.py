"""The ah4c STREAMER_APP script set for FastChannels Player.

ah4c (github.com/sullrich/ah4c) can drive any HDMI encoder hardware it already
supports (network encoders, Hauppauge/Magewell/DeckLink via a local command) as
long as it has a STREAMER_APP script set telling it what to adb-launch and how to
confirm playback started. That set lives upstream in ah4c as
scripts/firetv/fastchannels and ships inside the ah4c image; ah4c's maintainers
own it, and the scripts read this server's address from FASTCHANNELS_URL in
ah4c's environment.

data/ah4c_scripts/ is a verbatim copy of that directory at UPSTREAM_COMMIT. It
backs the "Export ah4c scripts" download for ah4c images that predate the set,
and its SCRIPTS_VERSION is what the Bridge page treats as current. Don't edit
the copy: send changes upstream, then re-copy all four files from the merged
commit and update UPSTREAM_COMMIT.
"""

import io
import os
import re
import tarfile
import time

UPSTREAM_COMMIT = 'fd5dcc8d6710b5052ab49cdf3c724eca3b24c510'

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), 'data', 'ah4c_scripts')
_SCRIPT_NAMES = ('prebmitune.sh', 'bmitune.sh', 'stopbmitune.sh', 'reboot.sh')
_VERSION_LINE_RE = re.compile(r'^SCRIPTS_VERSION="(\d{4}\.\d{2}\.\d{2})"$', re.MULTILINE)
# bmitune.sh's argument parsing; the exported default URL goes right after it.
_URL_ANCHOR = 'TUNERIP="$2"\n'
SCRIPTS_VERSION_RE = re.compile(r'^\d{4}\.\d{2}\.\d{2}$')
# The URL lands inside a double-quoted shell string in bmitune.sh.
_SHELL_UNSAFE_RE = re.compile(r'["$`\\\s]')


def _read(name: str) -> str:
    with open(os.path.join(_TEMPLATE_DIR, name), 'r') as f:
        return f.read()


def scripts_version() -> str:
    """The SCRIPTS_VERSION the bundled bmitune.sh sends with every tune."""
    match = _VERSION_LINE_RE.search(_read('bmitune.sh'))
    if not match:
        raise RuntimeError('bundled bmitune.sh has no SCRIPTS_VERSION line')
    return match.group(1)


def build_ah4c_scripts_tarball(fastchannels_url: str) -> bytes:
    """A gzipped tar of the four ah4c scripts. bmitune.sh gets fastchannels_url
    as its default, so the set works before FASTCHANNELS_URL is set in ah4c's
    environment; a non-empty FASTCHANNELS_URL still takes precedence. Caller is
    responsible for validating/normalizing fastchannels_url first (a bare host,
    a stray trailing slash, or a scheme-less value would fail bmitune.sh's URL
    check) — see api_settings._normalize_server_url. Raises ValueError for a URL
    that isn't safe to embed in the script."""
    if _SHELL_UNSAFE_RE.search(fastchannels_url):
        raise ValueError('URL contains characters that are not allowed in a script')
    buf = io.BytesIO()
    mtime = int(time.time())
    with tarfile.open(fileobj=buf, mode='w:gz') as tar:
        for name in _SCRIPT_NAMES:
            content = _read(name)
            if name == 'bmitune.sh':
                if _URL_ANCHOR not in content:
                    raise RuntimeError('bundled bmitune.sh has no TUNERIP="$2" line')
                content = content.replace(
                    _URL_ANCHOR,
                    _URL_ANCHOR + '# Default added by FastChannels\' "Update ah4c scripts".\n'
                    f'FASTCHANNELS_URL="${{FASTCHANNELS_URL:-{fastchannels_url}}}"\n', 1)
            data = content.encode('utf-8')
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mode = 0o755
            info.mtime = mtime
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()

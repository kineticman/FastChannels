"""
Generate a Threadfin restore .zip that pre-configures one or more FastChannels
feeds for Plex (via Threadfin's HDHomeRun emulation).

Threadfin's import format *is* its backup zip (Settings -> Restore, or the
ThreadfinRestore WS command). We hand it a complete config so the user skips
registering sources, setting the per-playlist buffer, and the tedious
per-channel Mapping/activation. On restore Threadfin re-inits and REBUILDS
urls.json + xepg.json itself (StartSystem) — auto-activating every channel,
numbering it from our tvg-chno, and EPG-mapping by tvg-id. Verified end-to-end
against a live Threadfin (2026-06-20).

The zip carries, per feed, an `files.m3u/<PID>` + `files.xmltv/<XID>` source
pointing `file.source` back at FastChannels (so Threadfin keeps it fresh on its
own schedule) plus a `data/<PID>.m3u` / `data/<XID>.xml` snapshot of the current
feed bytes. Multiple feeds = multiple source entries in one zip; Threadfin merges
them into a single Plex lineup.

Restore is FULL-REPLACE — this stands up a Threadfin dedicated to FastChannels.
(Merging into an existing Threadfin that already has other sources is a separate
mode, not handled here.)
"""
import io
import json
import random
import string
import zipfile

# Threadfin global-settings defaults — the sane shipped values. buffer +
# ffmpeg.options are what actually matter for FastChannels playback into Plex.
_SETTINGS_DEFAULTS = {
    "api": False, "authentication.api": False, "authentication.m3u": False,
    "authentication.pms": False, "authentication.web": False, "authentication.xml": False,
    "backup.keep": 10, "git.branch": "MAIN",
    "buffer": "ffmpeg", "buffer.size.kb": 1024, "buffer.timeout": 500,
    "cache.images": False, "epgSource": "XEPG",
    # ffmpeg options — VERIFIED working end-to-end with Plex DVR + FastChannels (2026-06-20).
    #  -c:a libmp3lame  : THE fix. Plex DVR chokes on AAC-in-MPEG-TS ("sample rate not set" ->
    #                     "Could not write header" -> "check your tuner or antenna"); transcoding
    #                     audio to MP3 is the documented xTeVe/Threadfin Plex fix. AAC (copy +
    #                     re-encode), VLC, and timestamp normalization all FAILED; MP3 works.
    #  no -map for video : ffmpeg default stream selection picks the HIGHEST-resolution variant (HD).
    #                     Default `-map 0:v` packs ALL variants into the TS (Plex chokes on
    #                     multi-video); `-map 0:v:0` picks the first, often SD and unordered.
    #                     Default selection grabs best video + 1 audio. -sn drops subtitles.
    "ffmpeg.options": "-hide_banner -loglevel error -i [URL] -c:v copy -c:a libmp3lame -b:a 192k -ar 48000 -ac 2 -sn -f mpegts pipe:1",
    "ffmpeg.forceHttp": False,
    "vlc.options": "-I dummy [URL] --sout #std{mux=ts,access=file,dst=-}",
    "files.update": True, "filter": {}, "language": "en", "log.entries.ram": 500,
    "m3u8.adaptive.bandwidth.mbps": 10, "mapping.first.channel": 1000,
    "ssdp": True,
    "update": ["0000"], "user.agent": "Threadfin", "udpxy": "",
    # NOTE: deliberately NO "version" key — the target Threadfin stamps its own schema version on
    # load. Hardcoding it risks a wrong/older value vs the target build's schema. Likewise ffmpeg.path,
    # vlc.path, temp.path and port are install-specific and set per-call (Advanced Settings), not here.
    "xepg.replace.missing.images": True, "xepg.replace.channel.title": False,
    "ThreadfinAutoUpdate": False, "storeBufferInRAM": True,
    "forceHttps": False, "httpsPort": 443, "bindIpAddress": "",
    "httpsThreadfinDomain": "", "httpThreadfinDomain": "", "enableNonAscii": False,
    "epgCategories": "Kids:kids|News:news|Movie:movie|Series:series|Sports:sports",
    "epgCategoriesColors": "kids:mediumpurple|news:tomato|movie:royalblue|series:gold|sports:yellowgreen",
    "dummy": False, "dummyChannel": "", "ignoreFilters": False,
}


def _tid() -> str:
    """A Threadfin-style 20-char uppercase/digit source id."""
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=20))


# Install-specific defaults (overridable via "Advanced Settings" in the export UI). The
# fyb3roptik/threadfin image ships ffmpeg/cvlc at these paths; bare-metal or other images differ.
DEFAULT_FFMPEG_PATH = "/usr/bin/ffmpeg"
DEFAULT_VLC_PATH = "/usr/bin/cvlc"
DEFAULT_TEMP_PATH = "/tmp/threadfin/"
DEFAULT_THREADFIN_PORT = "34400"


def build_threadfin_zip(feeds, base_url: str, tuner: int = 2, *,
                        ffmpeg_path: str = DEFAULT_FFMPEG_PATH,
                        vlc_path: str = DEFAULT_VLC_PATH,
                        temp_path: str = DEFAULT_TEMP_PATH,
                        port: str = DEFAULT_THREADFIN_PORT) -> bytes:
    """
    Build a Threadfin restore zip for the given feeds.

    feeds:    list of dicts {slug, name, m3u_bytes, epg_bytes}.
    base_url: FastChannels public base URL (e.g. http://192.168.1.50:5523) —
              Threadfin must be able to reach this.
    tuner:    simultaneous-stream count Threadfin advertises to Plex.

    Advanced / install-specific (sane defaults for the fyb3roptik/threadfin image):
    ffmpeg_path / vlc_path: binary paths on the TARGET Threadfin (it clears the
                            path if the binary isn't there). temp_path: its buffer
                            dir. port: its web port.

    Returns the zip file as bytes.
    """
    base = base_url.rstrip("/")
    tuner = max(1, min(int(tuner), 20))

    settings = dict(_SETTINGS_DEFAULTS)
    settings["tuner"] = tuner
    settings["ffmpeg.path"] = ffmpeg_path or DEFAULT_FFMPEG_PATH
    settings["vlc.path"] = vlc_path or DEFAULT_VLC_PATH
    settings["temp.path"] = temp_path or DEFAULT_TEMP_PATH
    settings["port"] = str(port or DEFAULT_THREADFIN_PORT)

    m3u_files: dict = {}
    xmltv_files: dict = {}
    data_entries: list[tuple[str, bytes]] = []

    for feed in feeds:
        slug = feed["slug"]
        name = feed["name"]
        pid, xid = _tid(), _tid()
        m3u_url = f"{base}/feeds/{slug}/m3u"
        epg_url = f"{base}/feeds/{slug}/epg.xml"
        m3u_files[pid] = {
            "type": "m3u", "name": name, "file.source": m3u_url,
            "file.threadfin": f"{pid}.m3u", "id.provider": pid,
            "buffer": "ffmpeg", "tuner": tuner,
            "http_proxy.ip": "", "http_proxy.port": "", "description": "",
        }
        xmltv_files[xid] = {
            "type": "xmltv", "name": f"{name} EPG", "file.source": epg_url,
            "file.threadfin": f"{xid}.xml", "id.provider": xid,
            "http_proxy.ip": "", "http_proxy.port": "", "description": "",
        }
        data_entries.append((f"data/{pid}.m3u", feed["m3u_bytes"]))
        data_entries.append((f"data/{xid}.xml", feed["epg_bytes"]))

    settings["files"] = {"hdhr": {}, "m3u": m3u_files, "xmltv": xmltv_files}
    auth = json.dumps({"dbVersion": "1.0", "hash": "sha256", "users": {}}, indent=2)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("authentication.json", auth)
        z.writestr("pms.json", "{}")
        z.writestr("settings.json", json.dumps(settings, indent=2))
        z.writestr("xepg.json", "{}")   # Threadfin rebuilds (auto-activate + number + map)
        z.writestr("urls.json", "{}")   # Threadfin rebuilds
        # Restore's unzip needs the data/ dir to exist before extracting files
        # into it — write the explicit directory entry first.
        z.writestr(zipfile.ZipInfo("data/"), "")
        for path, data in data_entries:
            z.writestr(path, data)
        z.writestr(zipfile.ZipInfo("data/images/"), "")

    return buf.getvalue()

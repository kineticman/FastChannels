# Plex (unsupported — for power users)

**Short version: FastChannels does not officially support Plex, and we don't recommend it.**

Plex Live TV & DVR is the odd one out. Jellyfin, Emby, and Channels DVR all read an M3U + XMLTV
feed directly — you paste FastChannels' feed URLs and you're done. Plex refuses to: its Live TV only
ingests from devices that speak the **HDHomeRun** network-tuner protocol. There is no "add an M3U
playlist" button, and Plex has repeatedly broken third-party tuner setups across updates.

So getting FastChannels into Plex means running a **bridge** that pretends to be an HDHomeRun tuner in
front of it. That works, but it adds a second service to babysit, its own quirks (buffering, codec,
channel-cap limits), and no help from us when Plex changes something. If you have the choice, **use
Jellyfin, Emby, or Channels DVR instead** — they're a direct M3U/EPG paste with none of this.

If you still want Plex, here are two power-user paths. Neither is officially supported.

---

## Option A — via Channels DVR (easiest, if you already run it)

Channels DVR can itself impersonate a Plex tuner: it detects when Plex hits its M3U URL and hands
back HDHomeRun-format data. Since FastChannels already pushes feeds into Channels DVR with one click
(**Add to Channels DVR** in the feed modal, after setting the DVR server URL in **Settings**), the
whole chain is:

```
FastChannels ──▶ Channels DVR ──(pretends to be an HDHomeRun tuner)──▶ Plex
```

Setup lives on the Channels DVR side — see the community write-up:
<https://community.getchannels.com/t/hack-use-channels-dvr-as-a-plex-tuner/42120>. The gist: add your
Channels DVR **M3U** URL under Plex's *Set Up Plex Tuner*, then paste the **XMLTV** URL as the guide.

Caveats (all Plex-side, per that thread): Plex won't let you keep both a zip-code guide and an XMLTV
guide, so you may have to drop an existing lineup; fresh Plex installs sometimes need the AAC decoder
codec downloaded separately; playback is solid on Windows/Android/Fire, flakier in some browsers.

---

## Option B — via Threadfin (from scratch, no Channels DVR)

[Threadfin](https://github.com/Threadfin/Threadfin) (the maintained xTeVe successor) reads a
FastChannels feed and presents it to Plex as an HDHomeRun tuner:

```
FastChannels ──(M3U + XMLTV)──▶ Threadfin ──(HDHomeRun + MPEG-TS)──▶ Plex
```

A minimal Threadfin container (web UI on port 34400):

```yaml
services:
  threadfin:
    image: fyb3roptik/threadfin
    container_name: threadfin
    ports:
      - "34400:34400"
    environment:
      - TZ=America/New_York
    volumes:
      - ./threadfin/conf:/home/threadfin/conf
      - ./threadfin/temp:/tmp/threadfin:rw
    restart: unless-stopped
```

Then, in the Threadfin UI (`http://<host>:34400/web/`):

1. **Use the feed's `native` endpoints**, not the standard ones:
   `…/feeds/<slug>/native/m3u` and `…/feeds/<slug>/native/epg.xml`. The native playlist includes
   Gracenote-mapped channels (the standard M3U routes those to Channels DVR, so they'd silently
   vanish in Plex) and strips channel descriptions (they otherwise bleed into the channel name in
   Threadfin's M3U parser). Add the M3U as a **Playlist** and the EPG as an **XMLTV** source.
2. **Set Buffer = FFmpeg on the playlist itself.** This is the #1 gotcha: Threadfin's buffer is
   *per-playlist*, not just global. With no buffer set on the source, streams connect but deliver
   **zero bytes** — Plex shows the channel and nothing plays.
3. **Fix the audio codec.** Plex DVR can't ingest AAC-in-MPEG-TS (it aborts with a bogus "check your
   tuner or antenna"). In **Settings → FFmpeg options**, transcode audio to MP3:
   ```
   -hide_banner -loglevel error -i [URL] -c:v copy -c:a libmp3lame -b:a 192k -ar 48000 -ac 2 -sn -f mpegts pipe:1
   ```
   Leaving out `-map` for video also lets ffmpeg pick the highest-resolution HLS variant (Threadfin's
   default `-map 0:v` packs every variant into one stream and Plex chokes).
4. In **Mapping / XEPG**, activate the channels you want and confirm each has its EPG mapped.
   Threadfin assigns its own channel numbers (from 1000) and caps a Plex lineup at 480 channels — so
   point it at a small, curated feed, not the everything feed.

Finally, in Plex: **Settings → Live TV & DVR → Set up Plex DVR**, let it find Threadfin as an
HDHomeRun device (or enter `http://<threadfin-host>:34400` manually), and use Threadfin's guide at
`http://<threadfin-host>:34400/xmltv/threadfin.xml`.

---

Whichever path you take: make sure your **FastChannels Server URL** (Settings, or `PUBLIC_BASE_URL`)
is an address the bridge can actually reach — not `localhost` — or it won't be able to pull streams.
Again: this is a power-user path we don't support. If Plex fights you, the sane answer is Jellyfin,
Emby, or Channels DVR.

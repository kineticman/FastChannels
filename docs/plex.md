# Using FastChannels with Plex (via Threadfin)

Plex Live TV & DVR does **not** read M3U playlists directly. Unlike Jellyfin, Emby, and Channels
DVR, Plex only ingests live TV from devices that speak the **HDHomeRun** network-tuner protocol. So
to get FastChannels into Plex you put a small bridge in front of it — **[Threadfin](https://github.com/Threadfin/Threadfin)**,
the maintained successor to xTeVe. Threadfin reads FastChannels' M3U + EPG, pretends to be an
HDHomeRun tuner, and remuxes the streams into the MPEG-TS format Plex expects.

```
FastChannels ──(M3U + XMLTV)──▶ Threadfin ──(HDHomeRun + MPEG-TS)──▶ Plex
```

> **Channels DVR / Jellyfin / Emby users don't need this.** Those apps accept FastChannels' feed
> M3U + EPG URLs directly. Threadfin is only needed for **Plex**.

---

## What you need

- A running FastChannels instance.
- The **server URL** other devices use to reach FastChannels — set this under **Settings →
  FastChannels Server URL** (or the `PUBLIC_BASE_URL` env var), e.g. `http://192.168.1.50:5523`.
  This matters: the stream links inside your M3U are built from this address, and **Threadfin must
  be able to reach it**. If it's left as `localhost`, the Threadfin container won't be able to pull
  streams.
- Somewhere to run a second container (Threadfin).

---

## Step 1 — Get your FastChannels feed URLs

Pick the feed you want in Plex (the built-in **Default** feed includes all enabled channels). Each
feed exposes two stable URLs:

```
http://192.168.1.50:5523/feeds/default/m3u        # playlist
http://192.168.1.50:5523/feeds/default/epg.xml    # XMLTV guide
```

Replace `192.168.1.50:5523` with your FastChannels server URL and `default` with your feed slug.

> **Tip — keep the lineup tidy.** Plex/Threadfin work best with a focused channel list. Consider
> building a dedicated feed in **Admin → Feeds** (filter by source/category/language) and setting a
> **Channel Number Start** so the numbering is stable. Smaller, curated feeds are easier to map and
> recover faster after changes.

---

## Step 2 — Run Threadfin

A minimal `docker-compose.yml` for Threadfin (its web UI is on port **34400**):

```yaml
services:
  threadfin:
    image: fyb3roptik/threadfin
    container_name: threadfin
    ports:
      - "34400:34400"
    environment:
      - PUID=1000
      - PGID=1000
      - TZ=America/New_York          # set your timezone
    volumes:
      - ./threadfin/conf:/home/threadfin/conf
      - ./threadfin/temp:/tmp/threadfin:rw
    restart: unless-stopped
```

```bash
docker compose up -d
```

Then open the Threadfin UI at `http://<host>:34400/web/`.

> If Threadfin runs on a **different machine** than FastChannels, double-check that the FastChannels
> server URL (Step 1) is an address Threadfin can reach over the network — not `localhost`.

---

## Step 3 — Configure Threadfin

In the Threadfin web UI:

1. **Playlist** → add a new M3U source. Type **M3U**, paste your FastChannels **`/feeds/<slug>/m3u`**
   URL. Save and let it update.
2. **XMLTV** → add a new guide source. Type **XMLTV**, paste your FastChannels
   **`/feeds/<slug>/epg.xml`** URL. Save and update.
3. **Mapping / XEPG** → Threadfin imports the channels (FastChannels' M3U parses cleanly — names,
   logos, `group-title`, and the `tvg-id` that matches the EPG are all picked up). Go to the
   **Mapping** tab, activate the channels you want, and confirm each has its EPG mapped.
   - **Numbering:** Threadfin assigns its *own* channel numbers here (starting at 1000 by default) —
     it does **not** automatically adopt FastChannels' `tvg-chno`. Set them in Mapping if you want a
     specific lineup order.
4. **Set the buffer to FFmpeg — on the playlist itself.** This is the single most important step,
   and the easiest to get wrong: Threadfin's buffer is a **per-playlist** setting, not only a global
   one. Open the FastChannels M3U source you added and set its **Buffer = FFmpeg** (also set
   **Settings → Buffer → FFmpeg** as the default). If the buffer is left empty/off, streams will
   connect but deliver **zero bytes** — Plex shows the channel but nothing plays.
   Why FFmpeg is required: FastChannels stream links are HTTP redirects to live HLS, and the FFmpeg
   buffer is what follows them and produces the continuous MPEG-TS Plex needs. Threadfin's default
   FFmpeg profile copies the video and re-encodes audio to AAC stereo (good for compatibility); Plex
   handles any further transcoding for the end client.
5. **Settings → Tuner / Connections** → set the number of tuners to how many simultaneous
   live/recording streams you want to allow (e.g. 4).

> **Channel limit:** Threadfin caps a Plex lineup at **480 channels** by default. The built-in
> **Default** feed can exceed this (it includes everything) — another reason to point Threadfin at a
> smaller, curated feed, or raise the limit in Threadfin's settings.

---

## Step 4 — Add Threadfin to Plex

1. In Plex: **Settings → Live TV & DVR → Set up Plex DVR**.
2. Plex scans for tuners. Threadfin should appear as an **HDHomeRun** device. If it doesn't,
   choose **"Don't see your device? Enter its network address manually"** and enter
   `http://<threadfin-host>:34400`.
3. Continue. For the **guide**, choose to use an **XMLTV** guide and point it at Threadfin's guide
   URL (`http://<threadfin-host>:34400/xmltv/threadfin.xml`) — *not* Plex's built-in guide, which
   only covers over-the-air/cable lineups.
4. Match up the channels, finish setup, and your FastChannels lineup appears under **Live TV**.

---

## Troubleshooting

- **Channels don't appear in Plex** — they're probably still *inactive* in Threadfin's **Mapping**
  tab. Activate them there first.
- **Guide is empty / wrong** — make sure you selected the **XMLTV** guide (Threadfin's, not Plex's
  built-in one) and that each active channel has its EPG mapped in Threadfin.
- **Channel appears in Plex but nothing plays / "no signal" / instant black screen** — two common causes:
  1. **Buffer not set on the playlist.** Threadfin's buffer is per-playlist: open your FastChannels
     M3U source and confirm **Buffer = FFmpeg**. With no buffer, Threadfin connects but streams zero
     bytes.
  2. **Plex connects then drops after a few seconds** ("check your tuner or antenna", or Plex logs
     show `sample rate not set` / `Could not write header`). Two ffmpeg-option changes fix it. In
     **Settings → FFmpeg options**, use:
     ```
     -hide_banner -loglevel error -i [URL] -c:v copy -c:a libmp3lame -b:a 192k -ar 48000 -ac 2 -sn -f mpegts pipe:1
     ```
     - **`-c:a libmp3lame`** (transcode audio to MP3) is the key fix: **Plex DVR can't ingest AAC in
       MPEG-TS** — it reports `sample rate not set` and aborts with the bogus "check your tuner or
       antenna." This is the documented xTeVe/Threadfin Plex fix for audio-codec incompatibility.
       (Copying AAC, re-encoding to AAC, and the VLC buffer all fail the same way; MP3 works.)
     - **No `-map` for video** lets ffmpeg's default stream selection pick the **highest-resolution**
       HLS variant (HD). Don't use Threadfin's default `-map 0:v` (it packs all 4–6 variants into one
       TS and Plex chokes on a multi-video stream) or `-map 0:v:0` (grabs the first variant, often SD —
       and the variants aren't sorted, so the first is unpredictable). `-sn` drops subtitles.

     Verified end-to-end: Plex then reads the stream as `h264` 1080p video + `mp3` 48000/stereo audio
     and plays in HD. (A short burst of `non-existing PPS` / `decode_slice_header` warnings at channel
     start is normal — Plex joining a live h264 stream mid-GOP — and self-corrects within ~1s.)
- **A channel buffers forever or won't play** — open the FastChannels stream link directly
  (`http://<fastchannels>:5523/play/<source>/<id>.m3u8`) in VLC to confirm the source is alive. If
  FastChannels plays it but Threadfin doesn't, the stream may have ad-insertion/codec changes that
  trip the copy; run a **Stream Audit** in FastChannels to drop dead/DRM channels.
- **"All tuners are in use"** — raise the tuner count in Threadfin's settings.
- **Threadfin can't load the playlist** — verify the FastChannels **server URL** (Step 1) is
  reachable from the Threadfin container, and that the feed URL returns data (`curl` it).

---

## A native, no-Threadfin option later?

Native HDHomeRun emulation *inside* FastChannels (so you'd point Plex straight at it, no second
container) is on the roadmap as a possible future feature. For now, the Threadfin bridge above is
the supported, battle-tested path to Plex.

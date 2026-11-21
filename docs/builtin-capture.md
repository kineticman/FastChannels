# Built-in Capture (beta)

Built-in Capture plays DRM channels in a copy of Google Chrome that runs inside
the FastChannels container, records what it shows, and hands Channels DVR a
normal HLS stream. There's no second container, Android device or HDMI encoder
to set up: you turn it on from the Bridge page.

It doesn't remove or get around DRM. Chrome decrypts the channel with its own
Widevine module after the provider's license server approves it, exactly as it
would on a desktop, and FastChannels only records the picture and sound.

> **Lite mode:** new installs start in Lite mode, which hides the **Bridge** page.
> Switch to **Advanced** with the **Lite mode / Advanced mode** menu at the top
> right of the admin UI (or **Settings → Interface**) first.

## Before you start

- **A 64-bit Intel or AMD server running Linux.** Google only makes Chrome for
  Linux on x86_64, so Built-in Capture isn't available on Raspberry Pi, ARM NAS
  boxes or Apple Silicon.
- **About 250 MB of disk** on your FastChannels data volume. Chrome comes
  straight from Google when you turn the feature on, plus a few helpers
  (PulseAudio and FFmpeg) from Debian. FastChannels doesn't ship Chrome itself.
- **CPU for each channel playing at once.** Recording and re-encoding video is
  real work. Rough numbers at the default 720p 60 fps:

  | Server | CPU encoding | With the GPU ([below](#use-your-gpu)) |
  |---|---|---|
  | Intel N95 mini PC (4 cores) | about 2.5 cores per channel | about 1.2 cores |
  | AMD Ryzen 7 7735HS (8 cores / 16 threads) | about 1.2 cores | about 0.7 cores |

  Each tuner also uses memory: about 0.8 GB while playing at 720p, and about
  0.6 GB while waiting for a channel (see *Between channels* below to free
  that). **Settings → System** shows what Built-in Capture is using right now. Capture runs at a lower
  priority than the rest of FastChannels, so scrapes and the admin pages stay
  responsive while it works.

## Turn it on

1. Open **Bridge** and pick **Built-in Capture** in the list on the left.
2. Turn on its switch. The first time, the panel asks before downloading and
   says whether it found a GPU to encode with. If it didn't, it shows the
   compose change to make ([below](#use-your-gpu)); you can do that before or
   after installing. Click **Install** and the panel shows each step (download,
   install, start the tuners), then refreshes itself when it's ready, usually
   within a minute or two. You can leave the page while it runs.
3. Turn on **Bridge mode** at the top of the page if it isn't on already.
   Without it, DRM channels stay disabled no matter which methods are on.
4. Make sure your DRM sources have had a **Stream Audit** (Sources page). The
   audit is what finds each source's DRM channels; the **Works with** list at
   the bottom of the panel shows how many are ready per source.

Then choose:

- **Tuners:** how many channels can play at once, 2 by default and up to 8.
  Each one costs the CPU and memory above, so only go higher on a server with
  room: roughly 2 CPU cores per playing tuner, under 1 with a GPU. Your
  streaming accounts' own limits on simultaneous streams still apply.
- **Quality:** *720p 60 fps* is the default and the best fit for sports and
  news. *720p 30 fps* is lighter on a small server. The 1080p options need a
  fast CPU: Chrome decoding a 1080p channel takes about two cores on its own,
  even with GPU encoding. On a 4-core mini PC (Intel N95), keep 1080p to one
  tuner; three 1080p tuners overloaded it and slowed FastChannels itself.
- **Encoder:** leave it on *Auto*. It uses your GPU when FastChannels can see
  one, and the CPU otherwise.
- **Between channels:** *Keep Chrome ready* (the default) leaves each tuner's
  Chrome open, so a channel starts in about 10 seconds; each tuner keeps about
  0.6 GB of memory in use, even when nothing is playing. *Close Chrome when
  idle* frees that memory after 5 minutes with nothing playing, and the next
  channel takes about 8 seconds longer to start while Chrome opens.

Changes to tuners, quality or encoder wait until nothing is playing, so they
never cut off someone who's watching.

## Add it to Channels DVR

Your capture playlists are on **Feeds**. Each feed's *Bridge-only (DRM)*
section has a **Built-in** tab with two ways to add it:

- **Add Built-in to Channels DVR** registers just this feed's DRM channels as
  their own source, next to the feed's regular source. FastChannels sets the
  format (HLS), the guide and the **stream limit** (your number of tuners) for
  you. With the limit set, Channels DVR refuses an extra stream straight away
  instead of asking for a tuner FastChannels doesn't have. If you change the
  limit in Channels later, adding the feed again keeps your value.
- **Add whole feed as one source** (feeds in *One mixed playlist* guide mode)
  replaces the feed's regular source with one that carries everything: free
  channels play directly and DRM channels go through Built-in Capture. There's
  no stream limit, because FastChannels turns a DRM channel away itself when all
  its tuners are busy, while free channels keep playing. Afterwards the Feeds
  page offers to remove the sources this one replaces.

Feeds in *Separate standard + Gracenote playlists* mode get two Built-in
playlists (standard and Gracenote), like their regular ones; the button adds
both, each with the tuner count as its limit. Channels DVR counts the two
separately, so between them they can ask for more streams than you have tuners
(FastChannels turns the extras away). *One mixed playlist* mode avoids that with
a single source.

**Adding it by hand:** copy the URLs from the Built-in tab and add them as a
custom channels source with stream format **HLS**, the feed's EPG URL as the
XMLTV guide, and a stream limit equal to your number of tuners. Without a feed,
`/m3u/capture` (with `/epg.xml`) and `/m3u/capture/gracenote` cover every DRM
channel; both take the same filters as the regular playlist, for example
`/m3u/capture?source=paramount&source=pbs`.

**What to expect:**

- The first tune of a channel takes about **10 seconds** while Chrome loads it.
- With one tuner, changing channel takes about **15 seconds**: the old channel
  is let go once Channels stops asking for it, then the new one starts.
- The picture runs about 6–10 seconds behind live.
- When the last viewer leaves, the tuner is freed within a minute.
- When every tuner is busy, a new DRM channel is refused within about 10
  seconds (Channels shows it as unavailable).

## Use your GPU

With an Intel (Quick Sync) or AMD GPU, the video encoding moves off the CPU. On
the Intel N95 above, that took the encoder from about 180% CPU to about 35% and
made 720p 60 fps smooth. Picture quality is about the same as CPU encoding.

Docker only lets a container use the GPU if you pass it in. Add this to the
FastChannels service in your compose file and recreate the container:

```yaml
    devices:
      - /dev/dri:/dev/dri
```

Only add it if your server has a `/dev/dri` folder (`ls /dev/dri` on the host).
If it doesn't, Docker refuses to start the container. That's also why it isn't
in the default compose file; it's there as a commented-out example.

After the restart, FastChannels installs the GPU drivers itself, tests the GPU,
and the panel's **Encoder** line says *Using your GPU (Intel)* or *(AMD)*. If
the test fails, it says *Using CPU (GPU didn't work)* and carries on with the
CPU.

### NVIDIA (experimental)

NVIDIA cards encode through NVENC. This is new and hasn't been tested on real
NVIDIA hardware yet, so please report how it goes. It's safe to try: if the
encoder test fails, Built-in Capture carries on with the CPU.

1. Install the NVIDIA driver and the
   [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
   on the server (the same setup Plex or Jellyfin use for NVIDIA).
2. Add this to the FastChannels service in your compose file and recreate the
   container:

   ```yaml
       runtime: nvidia
       environment:
         - NVIDIA_VISIBLE_DEVICES=all
         - NVIDIA_DRIVER_CAPABILITIES=compute,video,utility
   ```

   `video` is the part that matters: it's what makes the encoder available
   inside the container.
3. The **Encoder** line should then say *Using your GPU (NVIDIA)*. If it says
   the NVIDIA encode test failed, the Logs page has the reason (look for
   `[capture]` lines).

## What works

Built-in Capture works with the same DRM sources as PrismCast. Verified so far:
Paramount+, Sling, DirecTV Stream, Spectrum, ESPN, NBCUniversal TV Everywhere,
Disney TV Everywhere, PBS, SportsNet Pittsburgh, Amazon Sports, Amazon Prime
Free Channels, Philo, The Roku Channel, Versant (USA, E!, Syfy, CNBC and the
rest), ViX, Tennis Channel, Fubo and Vidaa. Other DRM sources should work the
same way but haven't been tried yet.

A few single channels only allow hardware players such as FastChannels Player,
even when the rest of their source works (on Fubo, for example, ABC News Live).
The first time one of them is tuned it fails with a message saying so, and
Built-in Capture leaves it out of its playlists from then on. It's checked again
after 30 days in case the provider changes its rules.

Not available:

- **CNN TV Everywhere**: its license server refuses Chrome on Linux.
- **HBO Max**: uses the same license system as CNN, so it's left out too
  (not yet confirmed during a live event).
- **DAZN**: its license server refuses Chrome on Linux ("Device not allowed to
  receive a license").

**Amazon** channels play in SD (576p). Amazon gives HD only to hardware players
such as FastChannels Player, not to Chrome.

## Which method to use

- **FastChannels Player** (Single-port HDMI or ah4c Capture) gives the best
  picture, because the Android device's hardware DRM gets full HD and nothing
  is re-encoded in software. It needs a Fire TV or Android TV device and an
  HDMI capture setup; see [Single-port HDMI and ah4c Capture](fc-player-setup.md).
- **Built-in Capture** is the easiest: nothing extra to buy or run. Picture
  quality is limited to what Chrome on Linux is allowed (usually up to 1080p,
  SD for Amazon), and it uses your server's CPU or GPU.
- **PrismCast** works the same way as Built-in Capture but runs as its own app,
  which suits setups that already use it or want capture on a different
  machine.

You can turn on more than one. Each has its own playlist, so add the ones you
want to Channels DVR as separate sources.

## Updating and removing

- **FastChannels updates:** the add-on lives on your data volume. After an
  image update, FastChannels reinstalls it from the files it kept, usually in
  under a minute, and the tuners come back on their own.
- **Update Chrome** in the panel downloads the current Chrome. Do this if a
  source suddenly stops playing after working before.
- **Uninstall** removes Chrome and the add-on's files from your data volume
  (the helper packages go at the next container update). Turning the switch
  off only stops the tuners and keeps it installed.

## Troubleshooting

### "All capture tuners are busy"

Every tuner is in use. Raise **Tuners** if your server has room. For a
separate Built-in source, keep Channels DVR's stream limit equal to the tuner
count; with the whole-feed source, this message is how extra DRM tunes are
turned away.

### A channel won't start

The Logs page shows a line starting with `[capture]` saying why, for example
*Capture failed: page error: …*. That's usually the source itself (an expired
sign-in, a channel your account doesn't include, or a blackout), not the
capture. Check the channel plays in the Channels page preview, and look at the
**Works with** list for a source marked *not available*.

### Install failed

The panel shows the reason. The download needs your server to reach
`dl.google.com` and the Debian package mirrors. Fix the connection, then click
**Reinstall**.

### The video is choppy

- On a small server, switch **Quality** to *720p 30 fps*, or pass in your GPU.
- Make sure **Tuners** isn't higher than your server can handle: two tuners at
  720p 60 fps on CPU alone needs about five cores.
- A short stutter right when a channel starts is normal while the player builds
  its buffer.

### The tuners don't start

The panel shows *Tuners couldn't start* with a reason, and the Logs page has
the details. A failed tuner retries on its own every 30 seconds. Restarting the
FastChannels container clears anything left over from an unclean shutdown.

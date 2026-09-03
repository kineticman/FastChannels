# FastChannels Player setup (experimental)

FastChannels Player is the Android playback companion for FastChannels. When
Channels DVR requests a bridge-only channel, FastChannels resolves the live
stream, launches the player on a Fire TV or Android TV device over ADB, and
returns the device's captured HDMI output as a normal channel.

Playback uses Media3/ExoPlayer and the Android device's own Widevine CDM. It
does not remove or bypass DRM: license and entitlement checks still go through
the provider's license server, and only the device's HDMI output is captured.

This is an experimental hardware-and-software setup with several parts. Choose
one capture method below, then follow the numbered steps in order.

## Two ways to capture the picture

FastChannels never captures the video itself — it only starts playback on the
Fire TV or Android TV device over ADB. Something else has to capture that
device's HDMI output and hand it back to Channels DVR. There are two ways to
do that:

- **Single HDMI encoder** (this guide's main path) — one fixed encoder/capture
  stream URL, e.g. Channels DVR's own `capture://` source. Simplest option if
  you already have the specific capture setup this guide walks through.
- **[ah4c](https://github.com/sullrich/ah4c)** — a separate, independently
  maintained project that already knows how to drive a broad range of HDMI
  capture hardware (Hauppauge, Magewell, Blackmagic DeckLink, and any network
  encoder) and ADB-controlled devices. Use this if your capture hardware isn't
  the one this guide documents. See [ah4c support](#ah4c-support) below —
  it's a genuine alternative to Step 1, not an add-on to it.

Steps 2 (ADB debugging) and 4 (installing the app) apply to both methods. Only
Step 1 and the *Single HDMI encoder* part of Step 3 are specific to the
single-encoder path.

## Before you begin

You will need:

- A dedicated Fire TV or Android TV device
- Either a compatible HDMI capture device (single-encoder method) or a running
  [ah4c](https://github.com/sullrich/ah4c) instance (ah4c method)
- A working capture source in Channels DVR (single-encoder method only — ah4c
  handles this itself)
- ADB access to the Fire TV or Android TV device
- FastChannels 5.1.0 or newer

FastChannels Player currently supports these sources:

- Sling
- PBS
- Amazon Prime Free
- Vidaa
- Philo
- Roku
- NBCUniversal TVE
- DirecTV Stream
- Fubo

Only channels FastChannels has identified as requiring the bridge appear in
the FastChannels Player feed. Clear channels from the same sources remain in
the regular feed and play directly.

## 1. Set up and test HDMI capture (single-encoder method)

If you're using ah4c instead, skip this step and continue with Step 2. You will
configure ah4c after installing the player in Step 4.

Set up your HDMI capture device before configuring FastChannels Player. The
capture source must already be working in Channels DVR.

For a capture setup walkthrough, see the first two posts here:
<https://community.getchannels.com/t/fastchannels-fast-channels-aggregator-manager/45986/2671>

Once the capture source appears as a device in Channels DVR:

1. Find the capture source card in the Channels DVR admin interface.
2. Select **Manage → Export → Copy M3U**.
3. Download the M3U and open it in a text editor.
4. Copy the full stream URL on the second line. It should look similar to:

   ```text
   http://<host>:8089/devices/<YourDevice>/channels/<N>/stream.mpg?format=ts&codec=copy
   ```

5. Test the URL by opening it in a browser or media player. It should begin
   returning video without redirecting to another playlist.

Save this URL. You will enter it as the **Capture/encoder stream URL** later.

> **Important:** Use the direct URL containing
> `/channels/<N>/stream.mpg`. Do not use the device's `channels.m3u` URL.
> That URL is only a channel list and can cause a **Tuner Unreachable** error.

## 2. Enable ADB debugging

FastChannels uses ADB to control playback on the Fire TV or Android TV device.

1. On the device, open **Settings → My Fire TV** (or **Device**) → **About**.
2. Select the device or build name seven times, until the developer message
   appears.
3. Go back one screen and open **Developer Options**.
4. Enable **ADB debugging**. You do not need to enable **Apps from Unknown
   Sources**; the FastChannels installer uses `adb install`, not Fire TV's
   on-device package installer.
5. Find the device's IP address under **Settings → My Fire TV → About →
   Network**.
6. From a computer on the same network, run:

   ```bash
   adb connect <device-ip>:5555
   ```

7. When the authorization prompt appears on the TV, select **Always allow from
   this computer**, then approve it.

If the prompt does not appear, run the connection command again. It sometimes
appears only after a second attempt.

> **Fire TV note:** Fire OS may occasionally remove the ADB authorization,
> especially after an update. If the connection later stops working, check the
> TV screen for a new authorization prompt.

> **Gotcha — each adb client needs its own approval:** ADB authorization is
> per-key, not per-device-config. Approving the prompt from your own computer
> (step 6 above) does **not** approve FastChannels itself — the app makes its
> own adb connection from inside its Docker container, using a different key,
> and needs its own separate approval the first time it connects (and again any
> time Fire OS drops it). You'll see this as `adb: device unauthorized. This
> adb server's $ADB_VENDOR_KEYS is not set` in logs, or an install/test action
> in the admin UI just hanging or failing. Fix: watch the TV screen right after
> triggering the action from FastChannels (not from a manual `adb connect`) and
> approve the prompt that appears. If you're not physically at the TV (e.g.
> viewing it through a capture/bridge feed), you'll need someone there, or to
> wait until you can see the screen — there's no way to approve this remotely.

## 3. Configure FastChannels

In the FastChannels admin interface, go to **Settings → FastChannels Player**.
The card is split into a shared section at the top and one section per capture
method below it, each with its own **Save** button.

In the shared section at the top, complete:

- **Enable FastChannels Player:** Turns the feature on — required before
  either capture method works.
- **Firestick / Android TV IP address:** Enter the IP address found in step 2.
  FastChannels adds ADB port `5555` automatically. This is the device the
  single-encoder method always triggers. The ah4c method triggers whichever
  device ah4c allocated for each tune (see [ah4c support](#ah4c-support)),
  falling back to this one only when ah4c doesn't name a device — so with more
  than one ah4c tuner, set this to any one of the sticks.
- **Stop playback when nobody's watching:** Optional. When enabled, playback
  stops after about five minutes without a confirmed viewer.
- **Show captions when available:** Optional. Renders an English subtitle/CC
  track when the stream advertises one.

Click **Save** in that section before continuing — the device IP must already
be saved for the install button below to work.

The automatic stop option detects viewers using Channels DVR's activity status
or the FastChannels `/watch` page. It cannot detect a third-party player
connected directly to the M3U. Leave this option off if you watch that way.

If you're using the single-encoder method, scroll to the **Single HDMI
encoder** section and enter:

- **Capture/encoder stream URL:** the direct stream URL saved in step 1.

Click **Save** in that section too. If you're using ah4c, skip this field and
complete the [ah4c configuration](#ah4c-support) after installing the player.

## 4. Install FastChannels Player

The player app does not require any on-device setup after installation. It is
controlled remotely by FastChannels.

### Install from the admin UI (recommended)

Click **Install FastChannels Player** on the same settings card. FastChannels
runs `adb install` using the release APK bundled into the Docker image, so no
download or manual sideload is needed. The device IP from step 3 must already
be saved, and ADB debugging must already be enabled on the device (step 2).

The same button is also the normal update path for future player versions. It
replaces the installed app while preserving its data, provided both versions
use the official release signature.

### Manual fallback: install a downloaded APK

If a custom or older image reports that no APK is bundled, download
`FastChannelsPlayer.apk` from the matching
[FastChannels release](https://github.com/kineticman/FastChannels/releases)
and install it directly:

```bash
adb -s <device-ip>:5555 install -r FastChannelsPlayer.apk
```

Building from source is also available in the [advanced
section](#advanced-build-the-app-from-source).

Once the app is installed, click **Test connection** in the **Single HDMI
encoder** section (single-encoder method only — ah4c has no equivalent test
button here, since it runs its own reachability checks). FastChannels will
check:

- Whether the device is reachable through ADB
- Whether FastChannels Player is installed
- Whether the capture stream URL is reachable

All checks should be green before continuing.

## ah4c support

[ah4c](https://github.com/sullrich/ah4c) is a separate, independently
maintained project — not part of FastChannels, and not something FastChannels
installs for you. Use it instead of the single-encoder method above if your
HDMI capture hardware isn't the one this guide documents; ah4c already knows
how to drive Hauppauge, Magewell, Blackmagic DeckLink, and any network
encoder, plus the same ADB-triggered app-launch mechanism as Step 2 above.

FastChannels never serves anything through ah4c's own M3U feature — instead,
FastChannels generates its own channel list with each entry pointing directly
at ah4c's `/play/tuner/<channel>`, and ah4c only ever sees individual tune
requests from Channels DVR, not a channel list request. There's no M3U file to
build or maintain on the ah4c side.

1. Set up and run your own ah4c instance, following [ah4c's own
   documentation](https://github.com/sullrich/ah4c) — `NUMBER_TUNERS`, a
   `TUNERn_IP` (the same Fire TV/Android TV device from Step 2 above, or a
   dedicated one) per tuner, and either `ENCODERn_URL` or `CMDn`/`CMDn_DEVICE`
   for your capture hardware. `IPADDRESS` should be set to wherever ah4c
   itself is reachable from — the same address you'll enter in FastChannels
   below. Multiple tuners each with their own `TUNERn_IP` and encoder are
   supported: ah4c allocates a tuner per tune and the exported `bmitune.sh`
   passes that tuner's device to FastChannels, so concurrent tunes each trigger
   their own streaming stick.
2. In FastChannels, go to **Settings → FastChannels Player → ah4c** and:
   - Toggle **Enable ah4c support** on.
   - Enter ah4c's **server URL** (e.g. `http://192.168.1.30:7654`) — the same
     address as `IPADDRESS` above. Click **Save**.
   - Click **Export ah4c scripts**. A modal asks for this FastChannels
     server's own address, as reachable from the machine running ah4c (it's
     pre-filled from your browser's address, but confirm it — the two
     machines aren't always the same one). Downloading produces
     `prebmitune.sh`, `bmitune.sh`, `stopbmitune.sh`, and `reboot.sh`, already
     configured with that address baked in.
   - Click **Check tuner authorization** (any time after the server URL is
     saved). FastChannels reads ah4c's configured `TUNERn_IP` list from ah4c's
     own `/api/status` and, for each one, reports whether *this* FastChannels
     container can reach it over ADB and has been authorized on the device.
     Because ah4c and FastChannels are separate ADB clients with separate keys,
     a tuner ah4c already drives can still show **Not authorized** here — fix it
     the same way as any other unapproved key (trigger an action, approve the
     prompt on that TV).
3. On the machine running ah4c, extract those four scripts into a new
   directory under its mounted scripts folder, e.g.
   `${HOST_DIR}/ah4c/scripts/firetv/fastchannels/`, and set
   `STREAMER_APP=scripts/firetv/fastchannels` in ah4c's own env file. Restart
   the ah4c container to pick up the change.
4. Confirm that you installed FastChannels Player in Step 4. The same app is
   used by both capture methods; ah4c changes only how tuning and HDMI capture
   are orchestrated.

### ah4c encoder and latency notes

- For a network encoder such as LinkPi, give ah4c its **HTTP MPEG-TS** output
  URL (for example, `http://<encoder-ip>:8090/stream0`), not its RTSP preview
  URL. ah4c's network-encoder input is HTTP; verify the exact URL in VLC
  before adding it as `ENCODERn_URL`.
- Match `ENCODER_CODEC` to the encoder. A LinkPi H.264 output needs
  `ENCODER_CODEC=h264`. `NULL_FRAME_INSERTION=true` is a useful companion for
  a capture device that briefly has no picture while the Fire TV changes apps.
- The ah4c container is its own ADB client. Its persistent `/root/.android`
  volume has a different key from FastChannels' container and from a desktop
  `adb` install, so authorize the prompt generated by ah4c as well. Keep that
  volume when recreating the container or the prompt will return.
- ah4c can delay output while it waits for a moving keyframe. That is a
  conservative default for finicky encoders, but it adds tune time. With a
  stable H.264 LinkPi stream, this tested low-latency profile worked:

  ```text
  PLAYBACK_DETECTION=false
  PLAYBACK_DELAY=0
  NULL_FRAME_INSERTION=true
  ```

  Start there only after confirming the raw encoder URL in VLC. If a client
  gets an initial **No Signal** or a corrupt first picture, restore playback
  detection and try a small delay (for example, 5 seconds); that gives ah4c
  time to hand off on a clean moving keyframe.

When a viewer leaves an ah4c channel, ah4c runs the exported
`stopbmitune.sh` immediately and force-stops FastChannels Player. This is the
primary cleanup path for ah4c. FastChannels' optional idle-stop watchdog still
tracks the tune, but is a slower fallback intended for direct bridge playback.

A busy-tuner response from ah4c (all tuners in use) is normal contention, not
a failure — it just means try again once a tuner frees up.

## 5. Add the feed to Channels DVR

Open `/admin/feeds` in FastChannels. Each configured feed includes up to two
sections, depending on which capture method(s) you've set up:

- **FastChannels Android Bridge Channels** — the single-encoder method, with
  ready-to-use M3U and EPG URLs.
- **FastChannels Android Bridge Channels (ah4c)** — the same channels, routed
  through ah4c instead, only shown once ah4c support is configured.

Both are bridge-only — just the channels that actually need the device
trigger, not your entire channel catalog or even the whole source. They're
meant to be imported alongside your regular feed M3U (which already excludes
these), not instead of it. Use whichever one matches your capture method —
importing both into Channels DVR at once would register the same channels
twice.

Click **📺 Add to Channels DVR** in the section you're using to register the
source(s) automatically — Channels DVR is set to the correct MPEG-TS stream
format for you. If you'd rather add it by hand (or Channels DVR isn't
reachable from FastChannels), copy the M3U/EPG URLs shown and add them as a
custom M3U and XMLTV source yourself; see the MPEG-TS note below if you do.

## Troubleshooting

### Tuner Unreachable

Confirm that the capture URL points directly to a specific channel and contains:

```text
/channels/<N>/stream.mpg
```

Do not use the device's `channels.m3u` playlist URL.

### Detected MPEG-TS instead of HLS playlist

Only applies if you added the source to Channels DVR by hand — the **Add to
Channels DVR** button already sets this correctly. Open the capture source's
settings in Channels DVR and set its stream format to **MPEG-TS/TS**, not HLS.
The capture pass-through uses MPEG-TS.

### ADB connection times out

- Make sure the Fire TV or Android TV device is online and its IP address has
  not changed.
- Confirm that ADB debugging is still enabled.
- Check the TV screen for a new authorization prompt.
- Run `adb connect <device-ip>:5555` again.

### device unauthorized / `$ADB_VENDOR_KEYS is not set`

This means the TV has not approved *this particular* adb client yet — usually
FastChannels itself, connecting from inside its own container with its own
key, separate from any adb key on your PC. Trigger the install/test action
again and watch the TV screen for the authorization prompt, then approve it.
You must be able to see the screen when this happens; it cannot be approved
after the fact or from a capture/bridge feed.

### No FastChannels Player release is bundled

Official release images include the player APK. If the install button reports
that no release is bundled, confirm that the running container is the current
release rather than an older or locally built image. You can still use the
[manual APK fallback](#manual-fallback-install-a-downloaded-apk).

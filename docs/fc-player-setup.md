# FastChannels Player setup (experimental)

FastChannels Player lets DRM channels that need real Widevine decode (Sling, PBS,
Amazon Prime Free, Vidaa, Philo, Roku, NBC TVE, DirecTV Stream) play through a real
Fire TV / Android TV device, captured back off its HDMI output and re-published as a
normal channel — no browser/PrismCast bridge needed for these sources. It's the
FastChannels-owned successor to an earlier Kodi-based version of this same idea; the
architecture (adb-triggered device, HDMI capture, re-ingest) is unchanged, the app
driving playback is now ours instead of Kodi.

This is a hardware-and-software setup with several moving pieces, not a toggle. Follow
the steps in order.

## 1. Set up your HDMI capture card first

FastChannels doesn't capture video itself — it triggers playback on a device and
expects something else (usually Channels DVR's own `capture://` support) to already be
grabbing that device's HDMI output and re-serving it as a stream. Set this up and
confirm it works **before** touching FastChannels at all; everything below assumes you
already have a working capture URL.

See the first two posts in the community thread for the walkthrough:
<https://community.getchannels.com/t/fastchannels-fast-channels-aggregator-manager/45986/2671>

Once your capture source shows up as a device in Channels DVR, get its direct stream
URL from the DVR admin UI itself:

1. Go to the capture source's card in Channels DVR and click **Manage → Export →
   Copy M3U**.
2. Download that M3U and open it in a text editor. It'll have one entry, shaped like:
   ```
   #EXTINF:-1 channel-id="70092" tvg-chno="70092" ... ,Your Capture Channel Name
   http://<host>:8089/devices/<YourDevice>/channels/<N>/stream.mpg?format=ts&codec=copy
   ```
3. That second line — the full URL including the `?format=ts&codec=copy` — is what you
   want. Confirm it's really live by pasting it into a browser or `curl`ing it directly;
   it should immediately start returning video, not redirect to another playlist.

**Important — this must be a direct stream URL, not a playlist.** The capture
source's own device page also exposes a `.../channels.m3u` URL (no `?format=ts`, no
specific channel number) — that's a *playlist listing* your channels, not a stream, and
using it here will silently break playback: Channels DVR will time out with "Tuner
Unreachable" trying to tune through it later. Confirmed live 2026-08-25 — the direct
`/channels/<N>/stream.mpg?format=ts&codec=copy` URL is correct; the `channels.m3u` one
is not.

Keep the URL from step 2; it's the **Capture/encoder stream
URL** setting below.

## 2. Enable ADB debugging on the Fire TV / Android TV device

FastChannels controls playback entirely over adb — there's no on-device UI to touch
after this step.

1. On the device: **Settings → My Fire TV (or Device) → About**, then click the
   device/build name **7 times** until it says "You are now a developer."
2. Back out one screen to **Developer Options**, and turn on:
   - **ADB debugging**
   - **Apps from Unknown Sources** (needed to sideload the player app in the next step)
3. Find the device's IP address (same screen, or **Settings → My Fire TV → About →
   Network**) — you'll need it for both installing the app and for FastChannels'
   settings.
4. From a machine that can reach the device on your network:
   ```bash
   adb connect <device-ip>:5555
   ```
   Accept the on-screen "Allow USB debugging?" prompt on the TV, checking **Always
   allow from this computer**. If you don't see the prompt, try the `adb connect`
   again — it sometimes only appears on a second attempt.

**Known gotcha:** Fire OS can silently revert this authorization even with "always
allow" checked — usually after an update. If FastChannels' Test Connection button (or
the idle-stop watchdog, if enabled) starts timing out, check the device screen for a
fresh authorization prompt before assuming something else broke.

## 3. Install the FastChannels Player app

The entire app is one screen with no UI to speak of — everything is driven remotely.

**Once a release is published** (not yet, as of this doc): download the latest APK
from <https://github.com/kineticman/FastChannels/releases> and install it:
```bash
adb -s <device-ip>:5555 install -r FastChannelsPlayer.apk
```

**Until then**, build it from source:
```bash
cd app/fc_player
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 ./gradlew assembleRelease  # or assembleDebug
adb -s <device-ip>:5555 install -r app/build/outputs/apk/release/app-release.apk
```
(The `JAVA_HOME` override is only needed if your machine's default JDK is one that
ships without a compiler, e.g. a JRE-only OpenJDK 21 install — check `java -version`
first and skip it if you already have a working JDK on `PATH`.)

Every future release uses the **same signing key**, so `adb install -r` over an
existing install works as a normal in-place update — you never need to uninstall
first, except when switching between a self-built debug APK and an official release
APK (different keys, Android refuses to treat that as an update — uninstall once, then
future installs from the same source update cleanly).

## 4. Configure FastChannels

In FastChannels' admin UI, go to **Settings → FastChannels Player**:

- **Enable FastChannels Player** — turns the feature on.
- **Firestick / Android TV IP address** — the device's IP from step 2 (adb port
  `:5555` is derived automatically).
- **Capture/encoder stream URL** — the working capture URL from step 1.
- **Stop playback when nobody's watching** (optional) — after ~5 minutes with no
  confirmed viewer (via Channels DVR's own activity status, or FastChannels'
  `/watch` page heartbeat), the device is stopped automatically. This can't see a
  third-party client pointed directly at the M3U — only enable it if Channels DVR
  and/or FastChannels' own web player are the only ways these channels get watched.

Click **Test connection** — it checks adb reachability, whether the player app is
installed, and whether the capture/encoder URL is actually reachable. All green means
a bridged channel should really work end to end, not just that the device is online.

**One more Channels DVR-side check, if playback fails with "Detected MPEG-TS instead
of HLS playlist" or "Tuner Unreachable":** the capture source's stream **format**
setting in Channels DVR needs to be MPEG-TS/TS, not HLS — this capture pass-through is
always raw MPEG-TS. If that source's format is set to HLS in Channels DVR, flip it to
MPEG-TS/TS on the source's own settings page; once it is, the plain stream URL from
step 1 works with or without the `?format=ts&codec=copy` query string. Confirmed live
2026-08-25 — this is a Channels DVR source setting, not anything on the FastChannels
side.

## 5. Get the feed into your client

Once configured, `/admin/feeds` shows a **FastChannels Player output** section per
feed with ready-to-use M3U + EPG URLs, scoped to just the trusted DRM sources listed
above (not your whole channel catalog). Add those URLs to Channels DVR (or another
client) as a custom M3U/XMLTV source the same way you would any other FastChannels
feed.

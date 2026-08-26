# FastChannels Player setup (experimental)

FastChannels Player lets supported DRM channels play through a Fire TV or
Android TV device. The device's HDMI output is captured and sent back to
Channels DVR as a normal channel.

This is an experimental hardware-and-software setup with several parts. Follow
the steps below in order.

## Before you begin

You will need:

- A dedicated Fire TV or Android TV device
- A compatible HDMI capture device
- A working capture source in Channels DVR
- ADB access to the Fire TV or Android TV device
- The FastChannels Player app

FastChannels Player currently supports these sources:

- Sling
- PBS
- Amazon Prime Free
- Vidaa
- Philo
- Roku
- NBC TVE
- DirecTV Stream

FastChannels does not capture the video itself. It starts playback on the Fire
TV or Android TV device, while your existing HDMI capture setup brings the
video back into Channels DVR.

## 1. Set up and test HDMI capture

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
4. Enable:
   - **ADB debugging**
   - **Apps from Unknown Sources**
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

In the FastChannels admin interface, go to **Settings → FastChannels Player**
and complete these fields:

- **Enable FastChannels Player:** Turns the feature on.
- **Firestick / Android TV IP address:** Enter the IP address found in step 2.
  FastChannels adds ADB port `5555` automatically.
- **Capture/encoder stream URL:** Enter the direct stream URL saved in step 1.
- **Stop playback when nobody's watching:** Optional. When enabled, playback
  stops after about five minutes without a confirmed viewer.
- **Show captions when available:** Optional. Renders an English subtitle/CC
  track when the stream advertises one.

The automatic stop option detects viewers using Channels DVR's activity status
or the FastChannels `/watch` page. It cannot detect a third-party player
connected directly to the M3U. Leave this option off if you watch that way.

Click **Save** before continuing to the next step — the device IP must already
be saved for the install button below to work.

## 4. Install FastChannels Player

The player app does not require any on-device setup after installation. It is
controlled remotely by FastChannels.

### Install from the admin UI (recommended)

Click **Install FastChannels Player** on the same settings card. This
adb-installs a release APK that's already bundled into the FastChannels
Docker image — no download or manual `adb install` needed. It requires the
device IP from step 3 to already be saved, and ADB debugging to already be
enabled on the device (step 2).

At the time of writing, no official release has been published yet, so no APK
is bundled and this button will report it's unavailable. Until then, use one
of the fallback methods below.

### Fallback: install a downloaded APK

If a release is available but not bundled into your running image, download
the APK from <https://github.com/kineticman/FastChannels/releases> and install
it directly:

```bash
adb -s <device-ip>:5555 install -r FastChannelsPlayer.apk
```

### Fallback: build from source

If no release has been published yet, build the app yourself using the
advanced instructions at the end of this guide.

Once the app is installed, click **Test connection** on the settings card.
FastChannels will check:

- Whether the device is reachable through ADB
- Whether FastChannels Player is installed
- Whether the capture stream URL is reachable

All checks should be green before continuing.

## 5. Add the feed to Channels DVR

Open `/admin/feeds` in FastChannels. Each configured feed will include a
**FastChannels Android Bridge Channels** section with ready-to-use M3U and EPG
URLs, including only the supported DRM sources, not your entire channel
catalog.

Click **📺 Add to Channels DVR** in that section to register the source(s)
automatically — Channels DVR is set to the correct MPEG-TS stream format for
you. If you'd rather add it by hand (or Channels DVR isn't reachable from
FastChannels), copy the M3U/EPG URLs shown and add them as a custom M3U and
XMLTV source yourself; see the MPEG-TS note below if you do.

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

## Advanced: Build the app from source

This section is only needed until an official APK is published, or if you want
to build your own version.

From the FastChannels project directory, run:

```bash
cd app/fc_player
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 ./gradlew assembleRelease
adb -s <device-ip>:5555 install -r app/build/outputs/apk/release/app-release.apk
```

You may use `assembleDebug` instead of `assembleRelease` for a debug build.

The `JAVA_HOME` override is only necessary when the system's default Java
installation does not include a compiler. If a working JDK is already on your
`PATH`, you can omit it.

Official releases use the same signing key, so future releases can normally be
installed over the existing app with `adb install -r`. Android will not install
an official release over a self-built debug version because the signing keys
differ. In that situation, uninstall the debug version once, install the
official release, and use in-place updates afterward.

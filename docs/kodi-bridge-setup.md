# Kodi HDMI Bridge — setup guide (experimental beta)

This walks you through building a **DRM bridge**: a real Fire TV Stick running Kodi
that FastChannels remote-triggers to decrypt and play DRM channels (Sling, PBS,
Amazon Prime Free, Vidaa, NBC TVE, DirecTV Stream today), captured back off its HDMI
output and re-published as a normal channel in Channels DVR.

**Status: experimental, hands-on, not for everyone.** This is real hardware in the
loop — a physical Fire TV Stick plus a physical USB HDMI capture dongle — not a
software-only feature. Budget 30–60 minutes the first time. If you don't need DRM
channels, skip this entirely; every non-DRM channel already works without any of this.

**One device = one channel at a time.** Everything below sets up a single
Firestick+capture pair, which can only show one channel at once — like a single-tuner
box. You can build more than one pair if you want more simultaneous DRM channels, but
that's out of scope for this guide.

## What you end up with

```
Channels DVR ──1. tune a DRM channel──▶ FastChannels
                                            │
                                  2. JSON-RPC trigger
                                            ▼
                                    Kodi on Fire TV Stick
                                            │
                                  3. decrypts + plays,
                                     real HDMI output
                                            ▼
                                 USB HDMI capture dongle
                                            │
                                  4. Channels DVR's own
                                     native capture:// source
                                            ▼
                              Channels DVR re-serves it back
                                    to the same viewer
```

FastChannels never touches decrypted video — it only tells Kodi what to play and
redirects the viewer to wherever Channels DVR is already capturing the result.

## What you'll need

- A **Fire TV Stick that still runs Fire OS** (Android), not Vega OS. Vega has no
  sideloading at all — Kodi can't be installed. Amazon is transitioning new sticks to
  Vega; as of this writing, **avoid** the Fire TV Stick 4K Select (2025) and Fire TV
  Stick HD 2nd gen (2026, rounded corners). The 4K Plus, 4K Max, Fire TV Cube, and the
  1st-gen Fire TV Stick HD are still Fire OS. Quick check after unboxing: **Settings →
  My Fire TV → About → software version** — Fire OS starts with `3`/`5`/`6`/`7`/`8`/`14`;
  Vega starts with `1.`.
- A cheap **USB HDMI capture dongle** (a MACROSILICON-chipset one, ~$20–30, is what
  this was validated against) plus a USB hub if your server doesn't have a free port,
  and an HDMI cable from the Firestick to the dongle.
- A computer on the same LAN with `adb` installed (`apt install android-tools-adb`,
  `brew install android-platform-tools`, or the Android SDK platform-tools package),
  to drive the one-time setup.
- Your FastChannels server's LAN IP and Channels DVR's LAN IP.

---

## 1. Put the Firestick on a static IP

Its IP can't change later — Kodi's JSON-RPC address and the capture setup both depend
on it. Set a DHCP reservation for it in your router, or a static IP directly on the
device (**Settings → Network → your Wi-Fi/Ethernet → advanced/static IP**, varies by
Fire OS version).

## 2. Enable Developer Options + ADB debugging

On the Firestick:
1. **Settings → My Fire TV → About**, click the device name (or "Fire TV Stick") **7
   times** — you'll see "You are now a developer."
2. Back out to **My Fire TV**, open the new **Developer Options** entry.
3. Turn on **ADB debugging**.
4. Turn on **Apps from Unknown Sources** (needed to sideload Kodi).

## 3. Pair `adb` from your computer

```bash
adb connect <firestick-ip>:5555
```

A popup appears **on the TV** asking to allow USB debugging from your computer's key
fingerprint. Check **"Always allow from this computer"** and select **Allow** — do
this via the actual remote, not adb (there's a chicken-and-egg problem otherwise).
Confirm it's paired:

```bash
adb devices -l
# should show:  <firestick-ip>:5555   device   ...
```

If it shows `unauthorized`, the popup either didn't appear yet or you missed it —
disconnect and reconnect (`adb disconnect <ip>:5555 && adb connect <ip>:5555`) and
watch the TV.

## 4. Sideload Kodi

```bash
curl -sL -o kodi.apk https://mirrors.kodi.tv/releases/android/arm/kodi-21.3-Omega-armeabi-v7a.apk
adb -s <firestick-ip>:5555 install -r kodi.apk

# First-run storage permission dialog is invisible over adb and silently hangs boot —
# grant it explicitly before first launch:
adb -s <firestick-ip>:5555 shell pm grant org.xbmc.kodi android.permission.READ_EXTERNAL_STORAGE
adb -s <firestick-ip>:5555 shell pm grant org.xbmc.kodi android.permission.WRITE_EXTERNAL_STORAGE
adb -s <firestick-ip>:5555 shell am start -n org.xbmc.kodi/.Splash
```

Kodi should launch and land on its home screen. Leave it there for the next steps.

## 5. Turn on Kodi's remote-control webserver (JSON-RPC)

This is what lets FastChannels trigger playback over the network — do it from the
on-screen UI, using the actual remote (don't try to script this part):

1. On Kodi's home screen, open the **gear icon** (top-left) for Settings.
2. You'll land in the **System** category. Back out **once** to the Settings grid —
   **Services** is a separate tile there (alongside System/Player/Media/PVR & Live
   TV/Interface/Profiles), not a sidebar item under System.
3. Open **Services → Control**.
4. If **"Require authentication"** is on with no password set, turn it **off first**
   — otherwise enabling the webserver throws a validation error. This is a LAN-only
   appliance; no auth needed.
5. Turn on **"Allow remote control via HTTP."**
6. Turn on **"Allow remote control from applications on other systems"** — the
   triggering host (your FastChannels server) isn't "this system," so this one has to
   be on too.

Verify from your computer:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"JSONRPC.Ping","id":1}' \
  http://<firestick-ip>:8080/jsonrpc
# → {"id":1,"jsonrpc":"2.0","result":"pong"}
```

**If that comes back empty, hangs, or returns garbage (e.g. raw bytes instead of
JSON) instead of `pong`**: some Fire OS builds run their own internal system service
("Turnstile") permanently bound to port 8080, which Kodi can never win a race against.
Change the port under **Services → Control → Port** to something else (e.g. `8081`),
re-verify against that port instead, and use the same `IP:8081` form in step 9's
FastChannels IP field.

## 6. Install InputStream Adaptive

This is Kodi's DRM/adaptive-streaming plugin — required for the Widevine sources
(Sling, PBS, Amazon Prime Free, Vidaa, NBC TVE, DirecTV Stream). It's not bundled with
the Android APK, and there's no JSON-RPC method to install it remotely, so this one
step needs the actual remote:

1. **Add-ons** (left sidebar) → the box/install icon → **Install from repository**.
2. **VideoPlayer InputStream → InputStream Adaptive → Install.**

Wait for the "Add-on installed" notification.

## 7. Install the FastChannels resolver addon

This tiny addon (`plugin.video.fc_bridge`) is what turns "play this manifest URL with
this license URL" into a single JSON-RPC call — FastChannels ships it for you, no
need to build anything yourself.

**Download it** from your own FastChannels server:

```bash
curl -o fc_bridge.zip http://<fastchannels-host>:5523/kodi/fc_bridge.zip
```

**Push it to the Firestick's public Download folder** (not the app-private storage —
Android 11+ blocks `adb` from writing there directly, even with root-equivalent
tricks; the Download folder has no such restriction):

```bash
adb -s <firestick-ip>:5555 push fc_bridge.zip /sdcard/Download/fc_bridge.zip
```

**Install it from inside Kodi:**

1. **Add-ons** → install-from-zip icon → **Install from zip file**.
2. **External storage → Download → fc_bridge.zip.**
3. Wait for "Add-on installed."

**If the Download folder shows 0 items even though you just pushed the zip there**
(common right after a fresh Kodi install or reinstall): Kodi lost its "all files
access" permission and can't see anything outside its own app folder yet. Fix it once
per install:

```bash
adb -s <firestick-ip>:5555 shell appops set org.xbmc.kodi MANAGE_EXTERNAL_STORAGE allow
adb -s <firestick-ip>:5555 shell am force-stop org.xbmc.kodi
adb -s <firestick-ip>:5555 shell monkey -p org.xbmc.kodi -c android.intent.category.LAUNCHER 1
```

Give Kodi a few seconds to relaunch, then retry the zip install.

**Enable it** — sideloaded addons default to disabled, and there's no on-screen
indicator that it's off (it'll just fail with "Unable to find plugin..." until you do
this):

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"Addons.SetAddonEnabled","params":{"addonid":"plugin.video.fc_bridge","enabled":true},"id":1}' \
  http://<firestick-ip>:8080/jsonrpc
```

### Updating the addon later

Whenever FastChannels ships an addon update, repeat steps 7's download/push/install —
Kodi recognizes a higher `version` in `addon.xml` as an upgrade automatically. You
don't need to uninstall the old one first, and it stays enabled across the upgrade.

### Don't have anyone near the TV? Do it fully headless

Every step above that "needs the actual remote" can also be driven blind over `adb`,
if you're comfortable with a bit more fiddling:

```bash
# take a screenshot to see the current screen
adb -s <firestick-ip>:5555 exec-out screencap -p > screen.png

# navigate with d-pad/select key events, then re-screenshot to confirm position
adb -s <firestick-ip>:5555 shell input keyevent 20   # DPAD_DOWN
adb -s <firestick-ip>:5555 shell input keyevent 21   # DPAD_LEFT
adb -s <firestick-ip>:5555 shell input keyevent 22   # DPAD_RIGHT
adb -s <firestick-ip>:5555 shell input keyevent 19   # DPAD_UP
adb -s <firestick-ip>:5555 shell input keyevent 23   # DPAD_CENTER (select)
adb -s <firestick-ip>:5555 shell input keyevent 4    # BACK
```

Pull the screenshot with your image tool of choice after each move to see where focus
landed, adjust, repeat. Slow but fully reliable — this whole guide, including a
from-scratch addon reinstall, has been done this way with nobody touching the actual
remote at all.

## 8. Keep Kodi running 24/7

Fire TV has **no user-facing sleep-timer setting at all** — the visible "Sleep"
option under Settings is a bare immediate action, not a config screen. The real
control is `adb`-only:

```bash
adb -s <firestick-ip>:5555 shell settings put secure sleep_timeout 0
adb -s <firestick-ip>:5555 shell settings put system screen_off_timeout 2147483647
adb -s <firestick-ip>:5555 shell settings put secure screensaver_enabled 0
```

This survives a real reboot. Two things it does **not** fix, worth knowing:
- **Kodi can't be the default launcher** and its `BOOT_COMPLETED` receiver doesn't
  actually relaunch it — after a power cycle, Amazon's launcher wins and Kodi stays
  closed until something restarts it. FastChannels' scheduler runs a watchdog job
  every 45s that pings Kodi and relaunches it via `adb` if it's unresponsive — that's
  what actually keeps this working unattended (see step 9's "Keep alive" toggle).
- Your **TV/monitor's own auto-off** is separate and this doesn't touch it — the
  capture side needs to see a live HDMI signal at all times.

## 9. Configure FastChannels

In FastChannels: **Settings → Kodi HDMI Bridge** card.

1. Turn on **Enable Kodi bridge**.
2. Turn on **Keep alive** (the watchdog from step 8).
3. **Kodi / Firestick IP address** — just the IP; FastChannels derives the JSON-RPC
   (`:8080`) and adb (`:5555`) addresses from it automatically. If you had to move
   Kodi's webserver off the default port (see step 5's Turnstile note), append it
   instead — e.g. `192.168.1.50:8081`.
4. Click **Save**, then **Test connection** — it should confirm Kodi is reachable.
   It'll also warn you that the encoder/capture stream URL isn't set yet — that's
   expected until step 10.

## 10. Set up the capture card

Physically connect the Firestick's HDMI output into the USB capture dongle, and plug
the dongle into whatever machine runs Channels DVR (a hub works fine if you're short
a port).

**Find the video and audio device names** on that machine:

```bash
# video: look for the entry with ID_V4L_CAPABILITIES=:capture: (there may be a
# second, non-capture node from the same dongle — skip that one)
for d in /dev/video*; do echo "$d:"; udevadm info --query=all --name="$d" | grep -E "ID_MODEL|ID_V4L_CAPABILITIES"; done

# audio: find the dongle's ALSA card number
cat /proc/asound/cards
```

You want something like `/dev/video0` for video, and `hw:<card>,0` for audio (e.g.
`hw:1,0` if the dongle shows up as card 1).

**Add it as a Custom Channels M3U source in Channels DVR:**

```
#EXTM3U
#EXTINF:-1 channel-id="kodi-bridge-capture",Kodi HDMI Bridge Capture
capture://v4l2/video0/hw:1,0/?framerate=60
```

(swap `video0`/`hw:1,0` for whatever you found above). Add this as a **Custom
Channels → M3U Playlist** source in Channels DVR's own Settings → Sources.

Once it's imported, find that one channel's **guide number** in Channels DVR (e.g.
`70092`) — you'll need it for the next step.

**Set the encoder/capture stream URL back in FastChannels**, using that guide number:

```
http://<channels-dvr-host>:8089/devices/ANY/channels/<guide number>/stream.mpg
```

Paste that into the **Capture/encoder stream URL** field on the same Settings →
Kodi HDMI Bridge card, save, and **Test connection** again — it should now report
everything working.

## 11. Add the FastChannels feed to Channels DVR

Go to **FastChannels → Feeds**. Pick the feed you want (or use `default`), open its
**Kodi HDMI Bridge output** section, and click **📺 Add Kodi Bridge to Channels DVR** —
this registers the right M3U source(s) directly with Channels DVR for you (splits
into a Gracenote-guide source too, if any channels in that feed carry Gracenote IDs).

Alternatively, copy the URLs shown there and add them manually as a **Custom Channels
→ M3U Playlist** source (pair the M3U with the EPG URL shown alongside it). Either
way, set that source's **max simultaneous streams to 1** — there's only one physical
Firestick behind every bridged channel, so anything higher will just let two viewers
fight over the same device.

## Try it

Tune to a DRM channel (Sling, PBS, Amazon Prime Free, Vidaa, NBC TVE, or DirecTV
Stream) through Channels DVR's guide. Kodi should switch to it within a couple of
seconds, and the capture channel should show real video shortly after. Everything
else in that feed (non-DRM channels) plays exactly like it always did — this bridge
never touches them.

---

## Troubleshooting

**"Enable the bridge and set a device IP first" from Test connection**: the enable
toggle, IP, and Save haven't all landed yet — check the toggle is on and the IP field
has something in it, then Save again.

**Test connection says Kodi responded but the stream URL isn't reachable**: double
check the guide number in the encoder URL matches the actual capture-card channel in
Channels DVR (**Settings → Sources**, find "Capture Card" or whatever you named it),
and that the capture card is actually seeing a live HDMI signal (check the TV/monitor
attached to the capture path isn't asleep — see step 8's last note).

**`adb push`/`pull` into `Android/data` fails with "Permission denied"**: this is
expected on Android 11+ Firestick models (scoped storage blocks even `adb shell` from
reaching other apps' private storage) — that's exactly why steps 7 and above push to
the public `/sdcard/Download/` folder and use Kodi's own zip installer instead of
pushing files directly into `.kodi/addons/`.

**JSON-RPC ping fails, hangs, or returns non-JSON garbage even though the webserver
settings look right**: some Fire OS builds run an internal system service ("Turnstile")
permanently bound to port 8080 — Kodi's own webserver can never claim it. Move Kodi's
webserver to a different port under **Services → Control → Port** (`8081` is a safe
choice), and put `IP:8081` in FastChannels' Kodi IP field. See step 5.

**"Install from zip file" shows the Download folder as empty right after a fresh Kodi
install/reinstall, even though the addon zip is definitely there**: Kodi lost its
all-files-access permission on the (re)install. See the `appops set ... allow` +
force-stop/relaunch fix in step 7 — one-time per install.

**A channel plays garbled/black, or license requests fail**: not every DRM source
works through this bridge — see `dev/kodi/README.md`'s results table if you have
access to it, or just check which sources are in FastChannels'
`KODI_BRIDGE_TRUSTED_SOURCES` list (`app/kodi_bridge.py`). Cox and Warner TVE are
confirmed **not** to work through Kodi specifically (a real `inputstream.adaptive`
session-handling limitation, not a FastChannels bug) and are excluded from the bridge
feed outright — those sources still play fine through their normal, non-bridge path
if you're not trying to route them through Kodi.

**Kodi keeps getting backgrounded / a bridged channel randomly stops working**: make
sure "Keep alive" is on in Settings — the watchdog is what recovers Kodi automatically
after a crash, background, or reboot. Without it, nothing relaunches Kodi on its own.

**Minor audio/video sync offset on the capture channel**: this is a known
characteristic of cheap USB capture dongles (the video path runs through real
hardware MJPEG encoding with more latency than the near-instant audio path) — not a
FastChannels or Kodi bug. Usually small enough to live with; there's currently no
setting to compensate for it.

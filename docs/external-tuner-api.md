# External tuner API

FastChannels' own hardware capture paths ([HDMI Capture and ah4c](fc-player-setup.md))
start FastChannels Player over ADB. Some tuners control their Android TV / Fire TV
devices another way: APITuner, for example, uses its own on-device Agent app. The
external tuner API lets a tuner like that use FastChannels for the parts it can't do
itself:

- which channels need FastChannels Player (the bridge-only channels),
- how to start the Player for one of them, with a fresh manifest and license, and
- the guide for those channels.

The external tuner launches and stops the Player itself and captures the device's
HDMI output with its own encoder. FastChannels never connects to the device, never
allocates capture hardware and doesn't track these tunes.

Like the rest of the bridge, this doesn't remove or bypass DRM. The Player plays the
stream with the device's own Widevine CDM, license requests still go through the
provider's license server, and only the device's HDMI output is captured.

> **Status:** v1, on the `development` branch; it isn't in a published release yet.
> APITuner is the first client
> ([issue #94](https://github.com/kineticman/FastChannels/issues/94)).

## Turning it on

1. Pick a long random secret and set it as `FC_EXTERNAL_PLAYER_TOKEN` on the
   FastChannels container, then recreate the container (a restart doesn't pick up a
   new environment variable):

   ```yaml
   services:
     fastchannels:
       environment:
         - FC_EXTERNAL_PLAYER_TOKEN=change-me-to-a-long-random-string
   ```

   Give the same value to the external tuner. FastChannels doesn't generate this token.
2. Turn on **Bridge mode** on the **Bridge** page (switch to **Advanced** mode first if
   the Bridge page is hidden).
3. Run a scrape, or a Stream Audit, of the DRM sources you want to bridge. That marks
   their channels for bridge output.

You don't need to set up HDMI Capture, ah4c or a device IP. A configured token counts
as a capture method, so DRM channels stay active and marked for bridge output on an
install with no ADB device at all. The Bridge healthcheck shows **External tuner API:
On** when the token is set.

Without the token the API answers every request with `503`.

## Authentication

Every request needs:

```
Authorization: Bearer <FC_EXTERNAL_PLAYER_TOKEN>
```

A missing or wrong token gets `401`. The API doesn't use the admin login, so it works
the same whether or not an admin password is set. Responses are sent with
`Cache-Control: no-store`.

The token gates this API only. The manifest and license URLs that `prepare` returns
are FastChannels' normal `/play/...` proxy routes, which are open on your network like
every other playback URL FastChannels serves. Treat the token as a key to the channel
list, not as protection for the streams.

## Endpoints

All paths are under `/api/external-player/v1`.

### `GET /capabilities`

```json
{
  "version": 1,
  "capabilities": ["catalog", "prepare", "xmltv"],
  "controls_devices": false,
  "package": "com.fastchannels.player"
}
```

Use it to check that the URL and token work and which features the server has.

### `GET /channels`

The channels that play only through FastChannels Player: the same set as the
`/m3u/fc-player` playlist. That means channels that are enabled, active and marked for
bridge output, from an enabled source the Player can decrypt. Retired channels are
left out.

```json
{
  "version": 1,
  "channels": [
    {
      "source": "directv",
      "channel_id": "1000956",
      "name": "El Rey Rebel",
      "tvg_id": "directv.1000956",
      "gracenote_id": "124328",
      "logo_url": "https://...",
      "group": "Latino",
      "number": 5000
    }
  ]
}
```

- `source` + `channel_id` together identify a channel. They're stable across scrapes;
  use them as your key and to call `prepare`.
- `tvg_id` matches the `channel` attribute in `/xmltv`.
- `gracenote_id`, `logo_url`, `group` and `number` can be `null`. `number` is
  FastChannels' own master channel number; you're free to number channels yourself.

A channel that disappears from this list (disabled, gone upstream, or no longer
needing the bridge) answers `prepare` with `404`. Keep your own copy and mark it
unavailable rather than deleting it, since it may come back.

### `POST /prepare`

Request body, exactly these two keys:

```json
{"source": "directv", "channel_id": "1000956"}
```

Response:

```json
{
  "version": 1,
  "package": "com.fastchannels.player",
  "component": "com.fastchannels.player/.PlaybackActivity",
  "intent_extras": {
    "stream_url": "http://192.168.1.10:5523/play/directv/1000956/browser.m3u8",
    "license_url": "http://192.168.1.10:5523/play/directv/license/1000956",
    "drm": true,
    "title": "El Rey Rebel",
    "channel_key": "directv:1000956",
    "captions": false,
    "audio_mode": "auto"
  }
}
```

Call it for every tune, right before launching the Player. The URLs are built from the
address you used to reach FastChannels, so call it at an address the Android device
can also reach (not `localhost` or a Docker-only name).

`license_url` is present only when `drm` is `true`. `captions` and `audio_mode` reflect
the user's FastChannels Player settings.

| Status | Meaning |
|---|---|
| 200 | Ready to launch |
| 400 | Body wasn't JSON with exactly `source` and `channel_id` (strings, max 512 chars) |
| 404 | Not a current bridge channel |
| 410 | The channel has been retired |
| 502 | The provider couldn't be resolved right now (sign-in, entitlement, upstream outage). Retry later; the reason is in the FastChannels log, never in the response |

### `GET /xmltv`

XMLTV guide for every channel in `/channels`, keyed by `tvg_id`. It can be large: an
install with about 1,300 bridge channels produces roughly 33 MB, so stream or size
your parser for that. An empty catalog returns an empty `<tv/>`.

## Launching FastChannels Player

Start `component` with an explicit intent carrying `intent_extras`, keeping each
value's type: `drm` and `captions` are **booleans**, everything else is a string. The
Player reads booleans with `getBooleanExtra`, so the string `"true"` doesn't work.

The ADB equivalent, which is what FastChannels' own bridge sends:

```sh
am start -n com.fastchannels.player/.PlaybackActivity \
  --es stream_url 'http://192.168.1.10:5523/play/directv/1000956/browser.m3u8' \
  --es license_url 'http://192.168.1.10:5523/play/directv/license/1000956' \
  --ez drm true \
  --es title 'El Rey Rebel' \
  --es channel_key 'directv:1000956' \
  --ez captions false \
  --es audio_mode auto
```

Notes:

- The Player has no launcher icon. An app that targets Android 11 (API 30) or later
  needs a `<queries>` entry for `com.fastchannels.player` to see it in its
  installed-package list.
- A second launch while the Player is running reuses the same Activity and switches
  channel (`onNewIntent`), which is faster than a cold start.
- You can replace `channel_key` with your own value, for example one per tune. The
  Player uses it to match stop commands and in its logs. Something that still contains
  FastChannels' key (such as `directv:1000956#tune-42`) keeps logs easy to match up.

### Stopping

Send the same component an intent with:

| Extra | Type | Value |
|---|---|---|
| `command` | string | `warm_stop` |
| `channel_key` | string | the key you launched with |

The Player stops playback but stays in the foreground, so the next launch is quick.
A `warm_stop` whose `channel_key` doesn't match the current tune is ignored, so a late
stop from an old tune can't stop a newer one. Leaving out `channel_key` stops whatever
is playing.

## Telling whether it's playing

The Player publishes a MediaSession, readable without ADB through a notification
listener (`MediaSessionManager.getActiveSessions`):

- **PlaybackState:** `PLAYING` while video is playing; `BUFFERING` or another state
  while it recovers.
- **Metadata** (FastChannels Player 5.2.14 and later): `METADATA_KEY_TITLE` /
  `METADATA_KEY_DISPLAY_TITLE` = the `title` extra, and `METADATA_KEY_MEDIA_ID` = the
  `channel_key` extra.
- **Session extras** (`MediaController.getExtras()`, 5.2.14 and later):

  | Key | Type | Meaning |
  |---|---|---|
  | `channel_key` | string | The tune's `channel_key` |
  | `tune_started_ms` | long | When the Player received the launch (device wall clock) |
  | `first_frame_ms` | long | When the tune first rendered video, `0` until it does |

  The extras are cleared on `warm_stop`.

Match `METADATA_KEY_MEDIA_ID` or the `channel_key` extra against the key you launched
with to confirm the device is on the right channel, and wait for a non-zero
`first_frame_ms` to know a picture is showing.

### Don't stop on short not-PLAYING spells

The Player recovers from a lot by itself, and it isn't `PLAYING` while it does:

- Sling ad gaps: segments simply stop for 6–58 s (most 16–20 s), and the Player keeps
  rejoining the live edge for up to 120 s.
- Transient errors are retried a few times before the Player gives up.
- A stalled picture is rebuilt after about 15 s without progress.

If it really gives up, the Player finishes and leaves the foreground. Treat "the Player
left the foreground" as the failure signal, and give a not-`PLAYING` state a grace
period of a couple of minutes before ending a stream. Otherwise DVR recordings get cut
short at ad breaks.

## What FastChannels' own ADB bridge does that you take over

With this API your tuner owns the device, so these FastChannels features don't apply
to its tunes:

- **Idle stop:** FastChannels' bridge stops the Player when nobody is watching. Stop it
  yourself when your stream ends.
- **Sling block-boundary retune:** the Player handles Sling's block boundaries by
  itself; FastChannels' ADB retune is only a backup and doesn't run for your tunes.
- **Waking the device:** FastChannels sends `KEYCODE_WAKEUP` before each ADB tune; a
  sleeping device accepts the launch but never plays.
- **Diagnostics:** the Bridge page's device list, logcat error reports and audio
  diagnostics only cover ADB devices.

## Compatibility

v1 stays compatible: fields may be added to responses, but none are renamed, removed
or change type, and the paths and status codes stay the same. Ignore fields you don't
recognize. An incompatible change would get a new path (`/api/external-player/v2`), and
`/capabilities` lists what a server supports.

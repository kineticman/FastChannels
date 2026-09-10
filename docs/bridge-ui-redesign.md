# Bridge page redesign

## Purpose

The Bridge page should answer one user question first: **how will FastChannels
turn a DRM stream into something the DVR can watch?** It should not require the
user to understand the current implementation names, fallback rules, or which
settings happen to live on the same card.

The existing dedicated `/admin/bridge` page is the first step: it removes
PrismCast and FastChannels Player configuration from the increasingly general
Settings page. The next iteration should split the FastChannels Player card by
capture architecture.

## Proposed page structure

```text
Bridge
├── Bridge policy
│   └── Enable broad DRM bridge mode
├── Choose a bridge method
│   ├── ah4c Capture — best for multi-tuner setups
│   ├── HDMI Capture — best for one budget stream
│   └── PrismCast Capture — browser capture
├── HDMI Capture
│   ├── Enable Android Player bridge
│   ├── Player device / ADB setup
│   ├── Install Player and device controls
│   ├── Captions and idle-stop behavior
│   └── Fixed encoder stream URL and connection test
├── ah4c Capture
│   ├── Enable ah4c capture
│   ├── ah4c server URL
│   ├── Check tuner authorization(s)
│   ├── Per-tuner OS and sleep status
│   ├── Export ah4c scripts
│   └── Advanced: legacy/default Player device fallback
├── PrismCast Capture
│   ├── PrismCast server and browser/watch-page URL
│   ├── Resolution limit
│   └── End-to-end diagnostics
└── Feed outputs
    └── Explain which Bridge M3Us to import into Channels DVR
```

## User-facing recommendation

### ah4c multi-tuner capture

Use ah4c when two or more channels may be watched at the same time, or when
each Fire TV/Android TV device has its own encoder. ah4c allocates a tuner for
each tune and passes that device address to FastChannels, preventing concurrent
tunes from taking over the same player.

This is the preferred setup for a multi-tuner installation.

### HDMI Capture (Single Stream)

Use a single HDMI encoder for the lowest-cost, one-stream setup: one Player
device and one fixed capture input. It is intentionally simple, but a second
simultaneous tune is not supported.

This card owns the Player-device controls because that device is the capture
path for this method.

### PrismCast

Use PrismCast when browser-based software capture is preferable to dedicated
Android/Fire TV hardware and HDMI capture. PrismCast plays DRM in Chrome and
captures it in software. It is the broadest fallback for sources that the
native Player cannot handle.

## Routing and state rules

The UI must make these rules explicit:

1. **Broad DRM bridge mode** keeps DRM-capable channels active for the
   PrismCast path. With it off, a channel that has no Player route follows the
   normal disabled-DRM behavior.
2. **FastChannels Player** can independently serve its verified source list.
   When both Player and PrismCast can serve a source, Player wins.
3. **ah4c** is a Player capture method, not a second DRM implementation. It
   owns selection of the actual tuner/Fire TV device through `TUNERn_IP`.
4. The global Player-device address remains only as a direct single-encoder
   target and an ah4c compatibility fallback. It should be hidden under an
   Advanced disclosure on the ah4c card, not presented as normal ah4c setup.
5. When a bridge method is enabled for the first time, users must run Stream
   Audit on each intended source. The Bridge page shows the enabled
   bridge-capable inventory and each source's currently marked candidate count
   so that this step is visible rather than implicit.

## Implemented settings model

1. `bridge_enabled` is the global policy gate. It controls whether eligible DRM
   channels remain active at all.
2. `prismcast_enabled` independently enables the Chrome-capture method.
3. `fc_player_bridge_enabled` independently enables FastChannels Player; ah4c
   remains its dedicated capture-method toggle.
4. Migration 030 maps existing installations safely: the global gate becomes
   the old PrismCast switch **or** Player switch, while PrismCast inherits only
   the old PrismCast switch. This preserves Player-only installs and the
   existing Player-over-PrismCast preference.

## Documentation links shown in the UI

- [FastChannels Player setup](fc-player-setup.md)
- [ah4c Capture setup](fc-player-setup.md#ah4c-capture-multi-tuner)
- [HDMI Capture setup](fc-player-setup.md#1-set-up-and-test-hdmi-capture-single-stream)
- [PrismCast](https://github.com/hjdhjd/prismcast)

# LG Channels — EU/International API Research

## Summary

The current LG Channels scraper uses a **US-only public web API**. European regions require
a separate device-authenticated API that is not publicly accessible.

---

## US API (Current Scraper)

- **Endpoint:** `https://api.lgchannels.com/api/v1.0/schedulelist`
- **Auth:** None — public, no credentials required
- **Country header:** `x-device-country: US`
- **Channels:** ~193 channels across 18 categories
- **Non-US behavior:** Returns HTTP 500 with error `"getScheduleList : Failed to get category
  and schedule with countryCode (GB)"` for every non-US code tested (GB, IE, DE, FR, SE, NO,
  DK, FI, CA, AU, MX, BR)

---

## EU API (Device Auth Required)

- **Endpoint:** `https://eic.cdpsvc.lgtvcommon.com/api/v1.0/schedulelist`
- **Location:** AWS Dublin (EU infrastructure)
- **Same path structure** as the US API — likely same response format
- **Auth:** Requires a chain of headers from a real LG TV device session:
  - `Authorization: Bearer {device_token}` — TV-issued token, server-validated
  - `X-Device-Platform: {code}` — must match LG's internal whitelist
  - `X-Device-ID`, `X-Device-Model` — real device identifiers
- Without valid auth, returns HTTP 400/401 with `AUTH.ERR.106: No Authorization Header`

### Full list of accepted headers (from OPTIONS response)
`X-Authentication`, `Authorization`, `X-Login-Session`, `X-Device-ID`, `X-Device-Product`,
`X-Device-Platform`, `X-Device-Model`, `X-Device-Brand`, `X-Device-Country`,
`X-Device-Country-Group`, `X-Device-FCK`, `X-Device-Pairing`, `X-Device-Sales-Model`,
`X-Device-EPG`, `X-Svc-cookie`, `X-Country-CP`

---

## Supported EU Countries

From LG's web app JS (`app.e245034a.js`):

```
GB, DE, IT, FR, ES, AT, CH, IE, PT, FI, NL, SE, NO, DK, BE, LU
```

---

## EU Stream CDN Patterns

EU streams use different CDNs than US:

| CDN | Pattern | Notes |
|-----|---------|-------|
| AWS MediaTailor | `*.mediatailor.us-east-1.amazonaws.com/.../LG-gb_{channel}/playlist.m3u8` | Confirmed for GB |
| Wurl/transmit.live | `wurl-lg-gb.global.transmit.live` / `wurl-lg-eu.global.transmit.live` | Most EU channels |

Known GB stream URL examples (from user report, confirmed live):
- `https://b964b70ba343416aab54f1d990e55d04.mediatailor.us-east-1.amazonaws.com/v1/master/44f73ba4d03e9607dcd9bebdcb8494d86964f1d8/LG-gb_AutenticHistory/playlist.m3u8`
- `https://0317aa59be904633bd9a7ae4d5419bb8.mediatailor.us-east-1.amazonaws.com/v1/master/44f73ba4d03e9607dcd9bebdcb8494d86964f1d8/LG-gb_AutenticTravel/playlist.m3u8`

US channels use amagi.tv and `lg-us` suffixed transmit.live CDN paths.

---

## LG Service Domain Architecture

| Region | Schedule API | App Store |
|--------|-------------|-----------|
| US | `api.lgchannels.com` (public) | `us.ibs.lgappstv.com` |
| Americas | `aic.cdpsvc.lgtvcommon.com` (auth required) | — |
| Europe | `eic.cdpsvc.lgtvcommon.com` (auth required) | `eic-lgappstv-com.aws-prd.net` |

---

## Why There's No Easy Workaround

The EU lgchannels.com website does **not** serve a live TV guide to European visitors — it shows
only VOD/promotions. The live channel data is exclusively served through the TV-native
authenticated API. There is no public web equivalent.

IP spoofing and header overrides do not work — country detection on the US API is enforced
server-side and returns hard 500s, not geo-blocks.

---

## What Would Be Needed to Build an EU Scraper

1. **HAR capture from a real UK/EU LG TV** — capture the network traffic from the LG Channels
   app on a physical LG TV (UK model) while browsing the channel guide. This would reveal the
   exact auth token format, device platform code, and full request structure.

2. **Or: reverse engineer LG webOS device auth** — the `lgtv-sdp` project on GitHub
   (https://github.com/wisq/lgtv-sdp) has done some work on the LG SDP protocol, but it targets
   app installation, not channel APIs.

**Best path:** Ask a community member with a UK LG TV to run a HAR capture (browser dev tools
or mitmproxy on their network) while using the LG Channels app and share the output.

---

## Workaround for Individual Channels

Users who know specific EU LG stream URLs can add them manually as a custom M3U source in
FastChannels. The MediaTailor URLs appear to be long-lived (no session tokens in the path).

---

## References

- LG Channels Kodi Forum: https://forum.kodi.tv/showthread.php?tid=377662&page=2
- LG Channels Lineup: https://channel-lineup.lgchannels.com/
- webOS Homebrew Glossary: https://www.webosbrew.org/pages/glossary.html
- lgtv-sdp fake server (GitHub): https://github.com/wisq/lgtv-sdp
- Wurl FAST channels on LG: https://www.broadbandtvnews.com/2022/04/08/wurl-powered-fast-channels-land-on-lg-channels/
- LG Channels Expands in Europe: https://www.lgcorp.com/media/release/27603

# TVE network availability by cable/streaming provider

Channels DVR (getchannels.com) publishes a per-MVPD ("provider") breakdown of
which TV Everywhere networks are reachable through that provider's Adobe Pass
sign-in. This is a snapshot of that data (fetched 2026-08-18), scoped to
cross-referencing it against our own TVE scraper families
(`app/scrapers/{warner,amcn,discovery,aenetworks,fox}_tve.py` /
`fox_one.py`).

Full data: [`tve_provider_availability.json`](tve_provider_availability.json)
(560 providers).

## Source and how to refresh it

Page: https://getchannels.com/docs/channels-dvr-server/tv-everywhere/availability/

The provider dropdown on that page is client-side JS (`pickProvider()` in the
page's inline script) that fetches a per-provider HTML fragment from:

```
https://getchannels.com/docs/channels-dvr-server/tv-everywhere/providers/<provider_id>/
```

The list of valid `<provider_id>` values (560 of them, e.g. `Cox`,
`Comcast_SSO`, `YouTubeTV`, `DTV`, plus hundreds of small regional cable
co-ops) is the `<option value="...">` list in a `<select id="provider-select">`
on the main availability page. Each fragment is a flat list of `{channel
name, channel number, sign-in URL}` rows — same ~42-network catalog for
every provider, with the sign-in URL present or absent per network to signal
availability.

Re-fetching all 560 providers takes about 5 minutes at a polite ~0.35s/request.

## Important caveat: "available" here means "listed", not "confirmed"

A present sign-in URL means the network's Adobe Pass integration lists that
provider as a *participating MVPD* — it does not mean a given account's plan
actually carries that channel. We proved this directly: getchannels lists
AMC, BBC America, IFC, and WE tv (`amcn_tve`) as available via `YouTubeTV`,
but a real YouTube TV account tested live against our `amcn_tve` scraper on
2026-08-18 got a clean Adobe Pass deny for all four —
`Adobe did not authorize <X> for YouTubeTV`. Meanwhile NBC, FOX (FS1), and
FOX One all paired successfully on that same account/day, and none of those
three even appear in getchannels' 42-network catalog at all (it only tracks
Turner, A&E Networks, AMC Networks, Discovery Networks, and a handful of
standalone news/sports networks — not NBCUniversal or Fox's own properties).

Use this data to decide **what's worth trying**, not as a substitute for a
real login test.

## Our scraper families vs. major providers

`Y` = getchannels lists a sign-in URL for that network+provider combo. `-` =
no URL listed (provider dropped from the site's "participating" list, or
never had one).

| Scraper | Network | Cox | Xfinity | YouTube TV | DIRECTV | DIRECTV STREAM | DISH | AT&T U-verse | Spectrum | Verizon FiOS | FuboTV | Sling TV | Philo | Hulu Live |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| aenetworks_tve | A&E | Y | Y | - | Y | Y | Y | Y | Y | Y | - | Y | Y | Y |
| aenetworks_tve | FYI | Y | Y | - | Y | Y | Y | Y | Y | Y | - | Y | Y | Y |
| aenetworks_tve | History | Y | Y | - | Y | Y | Y | Y | Y | Y | - | Y | Y | Y |
| aenetworks_tve | Lifetime | Y | Y | - | Y | Y | Y | Y | Y | Y | - | Y | Y | Y |
| amcn_tve | AMC | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | Y | - |
| amcn_tve | BBC America | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | Y | - |
| amcn_tve | IFC | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | Y | - |
| amcn_tve | WE tv | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | Y | - |
| discovery_tve | AHC | - | Y | - | Y | Y | Y | Y | Y | Y | - | - | Y | Y |
| discovery_tve | Animal Planet | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | Y | Y |
| discovery_tve | Cooking Channel | Y | Y | - | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| discovery_tve | Destination America | - | Y | - | Y | Y | Y | Y | Y | Y | - | - | Y | Y |
| discovery_tve | Discovery | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | Y | Y |
| discovery_tve | Discovery Life | - | Y | - | Y | Y | Y | Y | Y | Y | - | - | Y | Y |
| discovery_tve | Discovery Turbo | - | Y | - | Y | Y | Y | Y | Y | Y | - | - | Y | Y |
| discovery_tve | Food Network | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| discovery_tve | HGTV | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| discovery_tve | Investigation Discovery | - | Y | Y | Y | Y | Y | Y | Y | Y | - | - | Y | Y |
| discovery_tve | Magnolia Network | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| discovery_tve | OWN | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | Y | - |
| discovery_tve | Science | - | Y | - | Y | Y | Y | Y | Y | Y | - | - | Y | Y |
| discovery_tve | TLC | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | Y | Y |
| discovery_tve | Travel Channel | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| fox_tve | Fox Business Network | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | Y |
| fox_tve | Fox News Channel | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | Y |
| warner_tve | TBS | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | - | - | Y |
| warner_tve | TCM (unprobed) | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | - | - | Y |
| warner_tve | TNT | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | - | - | Y |
| warner_tve | truTV | Y | Y | Y | Y | Y | Y | Y | Y | Y | - | - | - | Y |

Notably: getchannels shows **Cox with no listing** for six digital-only
Discovery networks (AHC, Destination America, Discovery Life, Discovery
Turbo, Investigation Discovery, Science) — worth a real login test before
assuming Cox covers `discovery_tve` end to end the way it does the rest.

## Provider IDs for the MSOs we use

| MSO | getchannels `provider_id` |
|---|---|
| Cox | `Cox` |
| Xfinity | `Comcast_SSO` |
| YouTube TV | `YouTubeTV` |
| DIRECTV | `DTV` |
| DIRECTV STREAM | `ATTOTT` |
| DISH | `Dish` |
| AT&T U-verse | `ATT` |
| Spectrum | `Spectrum` |
| Verizon FiOS | `Verizon` |
| FuboTV | `Fubo` |
| Sling TV | `slingtv` |
| Philo | `Philo` |
| Hulu Live | `Hulu` |

## Data file structure

```json
{
  "source": "...",
  "fetched": "2026-08-18",
  "note": "...",
  "network_catalog": {"AMC HD": "6086", "...": "..."},
  "providers": {
    "Cox": {
      "label": "Cox",
      "available": {"AMC HD": "https://amc.com", "...": "..."}
    }
  }
}
```

`network_catalog` is the full ~42-network list getchannels tracks, with its
channel number. `providers[<id>].available` only includes networks that had
a sign-in URL for that provider — absence means no listing.

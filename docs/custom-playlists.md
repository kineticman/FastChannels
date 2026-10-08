# Custom playlists (your own M3U + XMLTV guide)

You can add your own M3U playlist to FastChannels, with its XMLTV guide if it has one. The channels
and guide data are imported and come out of your normal FastChannels M3U and EPG, so feeds, channel
numbers, categories and the enable switches work on them the same as on built-in sources.

Each playlist you add becomes its own source under **Sources → Custom Sources**.

## Adding a playlist

1. Go to **Sources** and click **+ Add Playlist**.
2. Give it a name, paste the playlist URL, and paste the guide URL if you have one. If the playlist
   names its own guide (an `x-tvg-url` or `url-tvg` header), the guide URL is filled in for you.
3. Click **Check playlist**. You'll see how many channels it has, how many the guide covers, and its
   groups.
4. Untick any groups you don't want.
5. Set the language and country for the playlist. These apply to channels that don't declare their
   own. Channels with a recognisably Spanish name are marked Spanish either way.
6. Choose whether the channels are enabled straight away or sent to review, then click
   **Add playlist**. The first import starts immediately.

Whichever you choose, channels the playlist adds *later* are sent to review (find them in
**Channels** under the "Needs review" filter). You can change that with the source's
**New channels** setting.

The name you pick when adding becomes part of these channels' permanent guide IDs and stream URLs.
You can rename the playlist later; the IDs stay the same, so nothing breaks in your DVR.

## How the guide is matched

Channels are matched to the guide by `tvg-id`. A channel with no `tvg-id` is matched by name against
the guide's display names. Channels the guide doesn't cover get hourly placeholder blocks so they
still appear in the grid.

If a playlist entry carries a `tvc-guide-stationid`, it is stored as the channel's Gracenote ID, and
the channel uses Gracenote guide data in Channels DVR like any other Gracenote-mapped channel.

Channels DVR's per-channel guide hints are read from the playlist and used for those placeholder
blocks: `tvc-guide-title` (the block's title, if different from the channel name),
`tvc-guide-description` or `tvg-description` (its description), `tvc-guide-art` (its image) and
`tvc-guide-placeholders` (its length). `tvc-guide-genres` and `tvc-guide-tags` are kept as the
channel's tags.

On playlists of up to 500 channels the guide is imported for every channel, so it is already there
when you approve channels from review. On bigger playlists it is imported for **enabled** channels
only: after approving channels there, click
**Scrape Now** on the playlist to fetch their guide without waiting for the next scheduled refresh.

Some details in your XMLTV file have nowhere to go and are dropped on import: cast and crew,
star ratings, and "new" / "previously shown" flags. Titles, descriptions, times, artwork, category,
rating, season and episode numbers, episode titles, air dates and live flags are kept.

## Refreshing, pausing, deleting

- **Refresh:** playlists refresh every 12 hours by default. Change the schedule under **Configure**.
- **Edit:** **Configure → Edit playlist…** changes the URLs, the groups imported, and the default
  language and country.
- **Pause:** switch the playlist off. Its channels leave your M3U and EPG but are kept, along with
  any edits and channel numbers. Switch it back on to bring them back.
- **Delete:** **Configure → Delete playlist** removes the playlist and its channels for good.

If a refresh fails (the URL is down, or returns something that isn't a playlist), the existing
channels are left alone and the error is shown on the source row.

## Limits and what isn't supported

- Streams are passed through as plain redirects. **HLS, DASH and MPEG-TS streams that play without
  special request headers work.** Streams that need a specific `User-Agent` or `Referer`
  (`#EXTVLCOPT` lines or a `|User-Agent=…` suffix in the playlist) are imported, but those headers
  are not sent yet, so some may not play. The Check step tells you how many entries declare them.
- DRM streams from playlists are not supported.
- A playlist is limited to 2,000 channels after the group filter. Use the group checkboxes to cut it
  down, or raise **Channel limit** under **Configure**.
- FastChannels never contacts a playlist's streams itself; your player is the only thing that
  connects to them, so nothing extra counts against a provider's stream limit. That also means
  playlist channels get no resolution badge and are never disabled automatically if a stream dies.
  Disable or remove dead ones yourself.
- We can't help with why a third-party playlist's streams don't play.

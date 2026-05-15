# Pre-configured channel catalog — add entries here to make them available
# in the admin "Browse Channel Catalog" modal under Custom Channels.
#
# Each channel needs: name, stream_url, logo_url, category, language.
# Set redetect_on_play=True for any source whose stream URL is a player
# page (stream detector resolves the real URL at play time).

CHANNEL_CATALOG = [
    {
        "network": "myAEW",
        "channels": [
            {
                "name": "AEW FAST Channel",
                "stream_url": "https://amg16221-amg16221c1-amgplt0795.playout.now3.amagi.tv/ts-us-e2-n2/playlist/amg16221-amg16221c1-amgplt0795/playlist.m3u8",
                "logo_url": "https://d31l2nn7dlh4li.cloudfront.net/Images/f8fc6e5f-39af-4378-b68a-73f02c2227c7.jpg",
                "category": "Sports",
                "language": "en",
            },
        ],
    },
    {
        "network": "ItsRealGoodTV",
        "channels": [
            {
                "name": "The Heartland Network",
                "stream_url": "https://watch.itsrealgoodtv.com/player/50167/50167",
                "logo_url": "https://st1-fs.cdn01.net/channels/0000050/0050167/thumb/0050167xl.jpg?v1",
                "category": "Entertainment",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "Retro TV",
                "stream_url": "https://watch.itsrealgoodtv.com/player/50177/50177",
                "logo_url": "https://st1-fs.cdn01.net/channels/0000050/0050177/thumb/0050177xl.jpg?v1",
                "category": "Entertainment",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "REV'N ACTION",
                "stream_url": "https://watch.itsrealgoodtv.com/player/49983/49983",
                "logo_url": "https://st1-fs.cdn01.net/channels/0000049/0049983/thumb/0049983xl.jpg?v4",
                "category": "Sports",
                "language": "en",
                "redetect_on_play": True,
            },
        ],
    },
]

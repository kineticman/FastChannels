# Pre-configured channel catalog — add entries here to make them available
# in the admin "Browse Channel Catalog" modal under Custom Channels.
#
# Each channel needs: name, stream_url, logo_url, category, language.
# Set redetect_on_play=True for any source whose stream URL is a player
# page (stream detector resolves the real URL at play time).

CHANNEL_CATALOG = [
    {
        "network": "Kaloopy",
        "channels": [
            {
                "name": "Kaloopy",
                "stream_url": "https://tv.kaloopy.com/",
                "logo_url": "https://a.jsrdn.com/hls/22868/kaloopy/logo_20231219_214555_68.png",
                "category": "Ambiance",
                "language": "en",
                "redetect_on_play": True,
            },
        ],
    },
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
        "network": "Heartland+",
        "channels": [
            {
                "name": "The Heartland Network",
                "stream_url": "https://watchheartlandplus.com/live/play/heartland",
                "logo_url": "https://st1-fs.cdn01.net/channels/0000050/0050167/thumb/0050167xl.jpg?v1",
                "category": "Entertainment",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "Retro TV",
                "stream_url": "https://watchheartlandplus.com/live/play/retrotv",
                "logo_url": "https://st1-fs.cdn01.net/channels/0000050/0050177/thumb/0050177xl.jpg?v1",
                "category": "Entertainment",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "REV'N ACTION",
                "stream_url": "https://watchheartlandplus.com/live/play/revn",
                "logo_url": "https://st1-fs.cdn01.net/channels/0000049/0049983/thumb/0049983xl.jpg?v4",
                "category": "Sports",
                "language": "en",
                "redetect_on_play": True,
            },
        ],
    },
    {
        # Free 24/7 live channels delivered as YouTube live streams. The
        # stream_detector YouTube fast path resolves these at play time; all
        # verified live + DRM-free.
        "network": "Free News & Live (YouTube)",
        "channels": [
            {
                "name": "Al Jazeera English",
                "stream_url": "https://www.youtube.com/@aljazeeraenglish/live",
                "logo_url": "https://yt3.googleusercontent.com/XsTga3Nsfc1E6ZgC6HfHfzTG_3zhuZleOnsKxSK2aILMjwkkIm-0vdALFaU-yt0Lw07iLtbSifk=s0",
                "category": "News",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "DW News",
                "stream_url": "https://www.youtube.com/@dwnews/live",
                "logo_url": "https://yt3.googleusercontent.com/NSOdTQTWlqMy8O_j32dx-ftfTCHMOt04Hm7KZ4pfAK6-eQzQSZMWvvss90kG8KQfJ7iNP3phyA=s0",
                "category": "News",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "NASA",
                "stream_url": "https://www.youtube.com/@NASA/live",
                "logo_url": "https://yt3.googleusercontent.com/eIf5fNPcIcj9ig-wZBeq4stFy1lgjWTW1nLT5dYlFkHZprZ03QBiMcbpwNMB6XSBjrSFGtAGQg=s0",
                "category": "Science",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "Sky News",
                "stream_url": "https://www.youtube.com/@SkyNews/live",
                "logo_url": "https://yt3.googleusercontent.com/dGnkztdrLtXRlzkdqReeL-NES2761xxmNVcJhGKqFpR0vQBoP9XaxnXF95FDpwrjyFr2iJvV8Es=s0",
                "category": "News",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "ABC News Live",
                "stream_url": "https://www.youtube.com/@ABCNews/live",
                "logo_url": "https://yt3.googleusercontent.com/GJ8V0NX6NddGh9bf4zED4tsjPjjBK2hdp5FWHMy09pV7sdSkkE3yEhCRSch4waEb9ZavyUrWfw=s0",
                "category": "News",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "NBC News NOW",
                "stream_url": "https://www.youtube.com/@NBCNews/live",
                "logo_url": "https://yt3.googleusercontent.com/PJj5jtuEOi5UmkFy4IBonj5WcabNcnJAIJe-jZMd1ArwIuVyQxFH_2zryBHwvfv6mJujwRpWDCM=s0",
                "category": "News",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "CBS News",
                "stream_url": "https://www.youtube.com/@CBSNews/live",
                "logo_url": "https://yt3.googleusercontent.com/ytc/AIdro_niBFv49gSx4rr1afMZU_Pv7SeuPKO2SMHvv0Ar7OKxM4o=s0",
                "category": "News",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "LiveNOW from FOX",
                "stream_url": "https://www.youtube.com/@LiveNOWFOX/live",
                "logo_url": "https://yt3.googleusercontent.com/AqzY5ePezRAQ2136-TbM_88d43JfIovkzztge92WKQ4K_ISfJMFA9yuX0Nw87DLWms4W9r2c3Q=s0",
                "category": "News",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "Euronews",
                "stream_url": "https://www.youtube.com/@euronews/live",
                "logo_url": "https://yt3.googleusercontent.com/8MyE7rxMBfLZOpYkJVJFm1C8I9jxbceBbOJS9OhrepZMVGxGV-OEJU-UdLOew_qR_l-knETWeu4=s0",
                "category": "News",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "TRT World",
                "stream_url": "https://www.youtube.com/@TRTWorld/live",
                "logo_url": "https://yt3.googleusercontent.com/luLQWmGFG4iDC1U_2JlzZ1mquci_sUdfZfFl4eWgBkDpW6tvZT0MEA4c4JebJdi9hCo518q1=s0",
                "category": "News",
                "language": "en",
                "redetect_on_play": True,
            },
            {
                "name": "CNA (Channel NewsAsia)",
                "stream_url": "https://www.youtube.com/@channelnewsasia/live",
                "logo_url": "https://yt3.googleusercontent.com/ytc/AIdro_n00DzE39o-4IFJ07IP3gG__TMqKxXXbwYaARinwTXTDFM=s0",
                "category": "News",
                "language": "en",
                "redetect_on_play": True,
            },
        ],
    },
]

# app/scrapers/roku.py
#
# The Roku Channel — FAST live TV scraper
#
# Auth flow (fully headless, no browser):
#   1. GET /live-tv              → session cookies
#   2. GET /api/v1/csrf          → csrf token
#   3. GET content proxy         → playId + linearSchedule (now/next EPG)
#   4. POST /api/v3/playback     → JWT-signed osm.sr.roku.com stream URL
#
# stream_url stored as: roku://{station_id}
# resolve() boots a fresh session on demand and calls /api/v3/playback
# Token caching: csrf + cookies cached for 55 minutes (they last ~1hr)

from __future__ import annotations

import logging
import re
import base64
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote

from .base import BaseScraper, ChannelData, ProgramData

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

_BASE        = "https://therokuchannel.roku.com"
_LIVE_TV     = f"{_BASE}/live-tv"
_CSRF_URL    = f"{_BASE}/api/v1/csrf"
_PLAYBACK    = f"{_BASE}/api/v3/playback"
_CONTENT_TPL = "https://content.sr.roku.com/content/v1/roku-trc/{sid}"
_PROXY_BASE  = f"{_BASE}/api/v2/homescreen/content/"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

_SESSION_TTL = 55 * 60  # seconds before we refresh cookies + csrf


# ── Scraper ────────────────────────────────────────────────────────────────────


# ---------------------------------------------------------------------------
# Category slug → clean display name
# ---------------------------------------------------------------------------
_CATEGORY_MAP = {
    "cat-action":                       "Action",
    "cat-action-adventure":             "Action & Adventure",
    "cat-action-movie-channels":        "Action Movies",
    "cat-action-movies":                "Action Movies",
    "cat-action-scifi":                 "Action & Sci-Fi",
    "cat-action-sports":                "Action Sports",
    "cat-adventure":                    "Adventure",
    "cat-ae-stunt-category":            "A&E",
    "cat-african-american":             "African American",
    "cat-ages-1-3":                     "Kids (1-3)",
    "cat-ages-4-6":                     "Kids (4-6)",
    "cat-ages-7-9":                     "Kids (7-9)",
    "cat-ages-10plus":                  "Kids (10+)",
    "cat-american-football":            "Football",
    "cat-animated":                     "Animation",
    "cat-anime":                        "Anime",
    "cat-ani-may":                      "Anime",
    "cat-auction":                      "Shopping",
    "cat-authentic-autumn":             "Lifestyle",
    "cat-auto-racing":                  "Motorsports",
    "cat-bake-offs":                    "Food & Cooking",
    "cat-basketball":                   "Basketball",
    "cat-best-of-british":              "British TV",
    "cat-british-tv":                   "British TV",
    "cat-bus--financial":               "Business & Finance",
    "cat-cancon":                       "Canadian",
    "cat-card-games":                   "Gaming",
    "cat-cars-motoring":                "Cars & Motoring",
    "cat-classic":                      "Classic TV",
    "cat-classic-tv":                   "Classic TV",
    "cat-comedy":                       "Comedy",
    "cat-cookery":                      "Food & Cooking",
    "cat-coronation":                   "Drama",
    "cat-creatorspace-livefeed":        "Creator",
    "cat-crime":                        "Crime",
    "cat-crime-drama":                  "Crime Drama",
    "cat-daytime-tv-shows":             "Daytime",
    "cat-destinations-new-pop":         "Travel",
    "cat-entertainment":                "Entertainment",
    "cat-epg-neighborhood-en-espanol":  "Spanish",
    "cat-epg-neighborhood-local-news":  "Local News",
    "cat-epg-neighborhood-news":        "News",
    "cat-epg-neighborhood-news-opinion":"News & Opinion",
    "cat-80s-action-stars":             "Action",
    "cat-animals":                      "Nature & Animals",
}


def _normalize_category(raw: str | None) -> str | None:
    if not raw:
        return None
    return _CATEGORY_MAP.get(raw.lower().strip(), raw)


# ---------------------------------------------------------------------------
# Static channel seed — (station_id, title, gracenote_station_id)
# Generated from therokuchannel.roku.com/live-tv page.json (399 channels).
# Roku has no headless API that returns the full channel list, so this tuple
# is the source of truth for channel discovery on new installs.
# To refresh: capture page.json from a real browser session and re-run
# app/scrapers/build_gracenote_map.py to regenerate this list.
# ---------------------------------------------------------------------------
_ROKU_CHANNELS = (
    ('ec9fd01d5fda09e9eda78ad0af2152d6', 'Gunsmoke', ''),
    ('7df7b616c8b2562191428c1eea9550bf', 'ION', '122912'),
    ('54478298594f574c88887ed70a086267', 'Little House on the Prairie', '138449'),
    ('0e8076d22d4711f94cd261087500576a', 'Law & Order', '189561'),
    ('f9c0d48b540a55dcb98b04d2cc8abf25', 'LiveNOW from FOX', ''),
    ('18c5f5feae8855baba4d1ccfd75be3db', 'Dateline 24/7', '113957'),
    ('483af83e821059b8b3f0825ae2d43e31', 'ABC News Live', '113380'),
    ('b5fe4277ff365ccda0a7d43028d53d90', 'NBC News NOW', ''),
    ('56b8909fc7e75f2fabd2abc22a256020', 'Murder She Wrote', '138397'),
    ('c0de867f29485305b9197b14cd08240f', 'Like Nastya', ''),
    ('227141061ce994a5401122a76c090f34', 'Leave It to Beaver', '138451'),
    ('70881c73b6275623ace6c5c3603a339c', 'CBS News 24/7', '104846'),
    ('7c274e1b0ad35b2cbaebd850422f1af2', 'NBA', ''),
    ('2d7bb74dc4d05bc2be40336d2f9dda7c', 'FOX LOCAL Tampa Bay', ''),
    ('a361d429db9e46665be277da42af89a4', 'Monster Jam', ''),
    ('8346cc0f3d415e39875fca80b67fd951', 'Universal Westerns', '138445'),
    ('ef036bfdb4235c9a9c92061a0b4eeb43', 'Family Feud', ''),
    ('7ef5e16ef5d95718b8e14b7bd657fa81', 'CINEVAULT: WESTERNS', ''),
    ('b89f979c548a5576a27bbe515c906814', 'Game Show Central', ''),
    ('40d73ba5be775428a377908b02033b4c', 'BABY SHARK TV', ''),
    ('42523afdfa3a0230d7268a238df5ef65', 'CNN Headlines', ''),
    ('5a28fbf15c135b51b3638e8bd02d6577', 'N+ Univision 24/7', ''),
    ('9aff620ece2457a3a7572886ecae0495', 'Super Simple Songs', ''),
    ('0838d5ccb1d5564d98fb804ab314c85e', 'FOX Weather', ''),
    ('16f751e2330d5a09a5e1a25a52b2b09c', 'ION Mystery', ''),
    ('b694de953eb75288a23bfa4b2cdcd36d', 'BET x Tyler Perry Comedy', ''),
    ('add7a09bd437e6598057a7c7fc55a81d', 'First 48', ''),
    ('beba92338d91d6675ed207e15601875a', 'The Virginian', '198209'),
    ('5cfea8cd274d24260febe8e405dfda2f', 'My Wife and Kids', '196854'),
    ('1a50b7ba48389669b3e0bc6750fe6b31', 'COPS', ''),
    ('57716baa323e2fb356d638f04e5b277e', 'The Price Is Right', ''),
    ('6d0103bde74463836393fd560eceb0d4', 'Home Improvement', '196855'),
    ('c8b9f1b78fa5123c38738c9ffb7505fd', 'Life Below Zero', '184779'),
    ('a4b1f915a8221221c2e3486bac42420b', 'CSI', ''),
    ('e0c3a43132e99f2188f63d4b3d95b9ac', 'The Judge Judy Channel', ''),
    ('1be9d5beddd9ff41a507e3f32558b60c', 'Heartland Classic', ''),
    ('4120b2fcb44e5abc918869e3c7a1fd56', 'The Goldbergs', ''),
    ('d8cdbc78c116d67731e2ac4d71476664', 'Best of American Pickers', ''),
    ('2d1c01b8e6728d4b59d89bf3dba04ba9', 'I (Almost) Got Away With It', ''),
    ('d1e4e369eda359c897e1fdfd8286b19b', 'Midsomer Murders', ''),
    ('0de9839c79b758589c580dcdd4b40a8a', 'Caso Cerrado', '138528'),
    ('34d90b2ece59551f9bd3257d7d132c25', 'Duck Dynasty', ''),
    ('bf4508c9597e5f618e508ae00b01f66f', 'Court TV', ''),
    ('383d3bee9ed152fdb46735576775fa91', 'Bull', ''),
    ('0be9ac2de942511596318c8c249236c8', 'Mysterious Worlds', ''),
    ('998e7fd5826653fc84880c91e2620a33', 'NFL Channel', '121705'),
    ('546a82f751540cf69e2f5a2f5af375ee', 'NYPD Blue', '160643'),
    ('c6f4945dee0671cc7404eb126591c52e', 'Tyler Perry\'s Love Thy Neighbor', ''),
    ('fc96185da1301a4b18c4e9cc6dffd884', 'Incredible Dr. Pol', '160649'),
    ('62945e18ab825bfd8b4995d9bd14917b', 'FilmRise Forensic Files', ''),
    ('a164a2f39b815cb9b6559d99ae0f6e8c', 'Caillou', ''),
    ('af7b719197365e69aff9c86f42fa3306', 'Universal Action', '138446'),
    ('52bd91c09770a78d5c3c840735edd608', 'According to Jim', '160647'),
    ('f2d2afeb40ccfeaf7c4c02b75e710b6f', 'The Conners', ''),
    ('108bd1c824eaca47592ec02c00f0a18a', 'In The Heat of the Night', ''),
    ('08973e5adc625d15aaa9c4c13cfb0307', 'Hallmark Movies & More', ''),
    ('d1501ed3781f53bfb1143efc162c6aee', 'BET x Tyler Perry Drama', ''),
    ('da9067306b4baa620da486ba6c9a3520', 'First 48 & Beyond', ''),
    ('b3ad0323cd466f4bb0c04ef9bee54543', 'Live PD: Presents', ''),
    ('ce5378542a9fad88bfba4a5ff6269fcf', 'ABC 20/20', '161600'),
    ('0322f5b9c0d954169122de194a8005ac', 'Universal Crime', '138454'),
    ('e1df79771ae4850c0e48ecc6284065d0', '48 Hours', '160400'),
    ('4eecfff035754a876ae4e0e0425e6540', 'LoveKills', ''),
    ('816baa1d516b7c968849e65568654fc9', 'Living With Evil', ''),
    ('c26908c5d48b50cd903e0f1786d0cebb', 'Oxygen True Crime Archives', '138585'),
    ('ec00d5b4b8e651b499d3063f48d9e516', 'ION Plus', ''),
    ('4ecf313b40975382b983c183bf7d86f0', 'Unsolved Mysteries', ''),
    ('7390ae841de55137a5736a3750ec446e', 'American Crimes', '138396'),
    ('75abc51f2ac655c7af92ff6043f4259e', 'Divorce Court', ''),
    ('1c4a9f1c7a21521ab83fbf968ee9ae7f', 'Las Vegas', '138398'),
    ('04e5bbc5f478580dabee165ba747ea16', 'Crime ThrillHer', ''),
    ('a3b243e7a1ea5ac99bb3e5560721d1f6', 'Cold Case Files', ''),
    ('0455f88e83625b3c8c302db0c30fe01e', 'A&E Crime 360', ''),
    ('e9b7110443305dea8b639880920644f9', 'Crime Scenes', ''),
    ('8abbea29b86a5a5da299c7ef2e288cab', 'Reelz Famous and Infamous', ''),
    ('14fafce34e4d5af99db4141edb1db371', 'Crimes Cults Killers', ''),
    ('0141916713c53d074650f14e2c9ff61e', 'Britbox Mysteries', ''),
    ('aa09e1b0f36f5ea8954d49dc254c2fa2', 'To Catch a Smuggler', '185032'),
    ('a1e38b83ae2a56f09f4d2ee8ed206b35', 'Weathernation', ''),
    ('97a1c32a9f1057cf974a920a4279a801', 'TODAY All Day', '114138'),
    ('6183f9f73a64394cf3c55690605af2a7', 'BBC News', '101443'),
    ('d4a795f8590d54218069b495d273ac5a', 'Inside Edition', '130385'),
    ('d3f36d4a35fa5f40a24b96a51556b9b5', 'Scripps News', ''),
    ('8e0bb4b7ca7c5ea1af52427462297f38', 'Telemundo Al Día', ''),
    ('34beeb8efcb656f08d91298bb94e1815', 'WAPA+', '148054'),
    ('a4ed8522d196587cbc82a399f95db51c', 'Reuters 60', ''),
    ('f95c40a17cfe5b9fb1970192e378fb0c', 'AccuWeather NOW', ''),
    ('0af429c36c6b517e8df2025e809b3964', 'Estrella News', ''),
    ('989feef9c39a5d41a7e88bd5d54f38b9', 'Sky News', '114448'),
    ('8db0f9f4abf6542ba95f97a545f98513', 'CBC News', '124721'),
    ('a7ef3da6761e5b768557b00ab1ab85e4', 'adn Noticias', ''),
    ('ceb64d994c7756c2a8f09ace94c3e43f', 'Telediario Now', ''),
    ('8bfd3a488b59590eb2fa5602c2120168', 'The Hill', ''),
    ('dafd06b712a35fbe9935d4c04c2e8b53', 'Euronews World', ''),
    ('8b77c262c90652658b21fdb9ee292ab7', 'ABC Localish Network', '118952'),
    ('5321194d102d57e094d47d50949088b8', 'USA Today', ''),
    ('0e9a0d968d225a3eb93784404f7edadf', 'AmericaTeve', ''),
    ('8acb4f9b8a255216966db355daaa1749', 'Euronews Español', ''),
    ('a29b3851293f5dab92a694af758f3822', 'KTRK ABC13 Houston', '131576'),
    ('69f8d2659a475342b88db93b63f755d0', 'WSB Atlanta', ''),
    ('e8caaff8cc0d526cb6903b4d4957fb2a', 'FOX LOCAL Atlanta', ''),
    ('0945fe89efea5f5ea622db91854864eb', 'FOX LOCAL Dallas | Fort Worth', ''),
    ('b2062b2fb08950e3bab5c093d53b0e81', 'KABC ABC7 Los Angeles', '131577'),
    ('a480d18b23c655d38e2f2bf7275c02cb', 'WLS ABC7 Chicago', '131574'),
    ('2b241f8d1dec5015ae35f13954fcb7f7', 'FOX LOCAL Detroit', ''),
    ('6d055d4b94c95e159e110e96c06c00df', 'WPVI ABC6 Philadelphia', '131580'),
    ('28cca0d6e56f5527bb6bc15c8a30d133', 'WFTV 9 Orlando', ''),
    ('4f5800ea64695a5e86ad7da241abafd9', 'FOX LOCAL Washington DC', ''),
    ('65f1e34427ef5d61a2db61cb9a2819d0', 'WABC ABC7 New York', '131575'),
    ('d0d9af56b7735c318b66afe21089d028', 'Cleveland 19 News Plus', ''),
    ('d53b1fb8c41f574387cde82d084515ea', 'WSMV 4 News', ''),
    ('bf6c0073422758408e9336eaefbf77db', 'Boston 25 News', ''),
    ('2b91bedf0bf25b7e83c05291634da59a', 'WSOC Charlotte', ''),
    ('ce021f67f6595e8a8a8cca45a5561440', 'WTVD ABC11 North Carolina', '131579'),
    ('e38d66dbb4e05f9690f6c573417fc231', 'WPXI Pittsburgh', ''),
    ('c3320fcc14b65a54b1fe9b631eaee140', 'FOX LOCAL Houston', ''),
    ('8e083a62068c58d2aaf1999df2e7307c', 'FOX LOCAL Minneapolis-St. Paul', ''),
    ('eb947af99ddc59afaa796eb6a1c8534a', 'FOX LOCAL Philadelphia', ''),
    ('80734e4fe58c5bf08ee466c51d97b956', 'NBC 10 Philadelphia News', '124030'),
    ('5c8d77c2f6b85da3a7e74223b769ef5c', 'KPTV FOX 12 Oregon News', ''),
    ('021707311e0b595597f97a389e0051e6', 'NBC 4 Washington News', '124033'),
    ('e8e17e907b6b5efda825bb4fd5391511', 'FOX LOCAL Milwaukee', ''),
    ('a447479aa733519cb36d46bd68d496e9', 'FOX LOCAL New York', ''),
    ('307cbde3d30957828ff68b3b979d6958', 'Channel 3 Eyewitness News', ''),
    ('1a8678ddf7715dc1b381559418c3a722', 'FOX LOCAL Phoenix', ''),
    ('c8f55d28c4dd5370b50f17e230eaffd6', 'FOX LOCAL Orlando', ''),
    ('f389d9e365a5526b93b076fbb73a8160', 'NBC 4 New York News', '124034'),
    ('e8f8972f377a598590592ecd079e7cc4', 'First Alert 4 St. Louis', ''),
    ('41d5a59655975c379aff0ea4c729313e', 'CBS News New York', '123125'),
    ('ec2faed50af3503e934f0c342a7952fe', 'Arizona\'s Family', ''),
    ('9dfd5acd22f655768795c1f1bd731a53', 'NBC 5 Dallas-Fort Worth News', '124035'),
    ('89a7b90b417e58638319cc4d5cabb400', 'FOX LOCAL San Francisco', ''),
    ('1747fb8d80a152d4bb4a7ceba945604f', 'FOX LOCAL Seattle', ''),
    ('e3fa9982ac3c555298eb72fcb3ffd8a4', 'CBS News Los Angeles', '123126'),
    ('9c0ccaa28c0b50f28ad27aaf2f224cf5', 'NBC 5 Chicago News', '124036'),
    ('a8d6e41d1b01510ca2c0d82a0c33d191', 'FOX LOCAL Los Angeles', ''),
    ('5ff5d4ad0db2591994479ff4c7573b26', 'NBC 10 Boston News', '124125'),
    ('5d3e1334e104572ca7968d337fc5c453', 'Lifetime Movie Favorites', ''),
    ('e2422e42301a3176997019f882471d77', '50 Cent Action', ''),
    ('6d4ae9d9d0df5122a0811cff961b24b2', 'Best of Dr. Phil', ''),
    ('1c60f162dd0f5178a922f6121ff28008', 'Xtreme Outdoor Presented by HISTORY', ''),
    ('ce8a61a75e94b71f52ad51ed92b1ec65', 'Paternity Court', ''),
    ('091bf303c7fd2bc2ec6683b1dbbadae1', 'Storage Wars by A&E', ''),
    ('5e8ae9bdd978fbd560811917b847f26d', 'Love After Lockup', ''),
    ('d216c2141a835cfcbe34a7d1b7d2e078', 'Nosey', ''),
    ('57c3b0c8270c5f839e55661af8b4c88d', 'Miramax Movies', ''),
    ('963c9d482c88568c9ff66b8fd07e9ad3', 'Deal Zone', ''),
    ('4ad47ace83b25065955adca3f8e9bdcf', 'The Price Is Right: The Barker Era', ''),
    ('856fca464da0544784204ba8f6161dac', 'PBS Antiques Roadshow', ''),
    ('390053bfbc6858999cb0d6354b57d938', 'Confess by Nosey', ''),
    ('1e234e88e79d5de99b222cbe4361767a', 'Deal or No Deal', ''),
    ('562baf35c5da56cc91cdea41537b43f9', 'BUZZR', ''),
    ('1c235612cec3540f85bb5e2bf7982047', 'Are We There Yet?', ''),
    ('c10210ce87bb56669714df0d2de5b442', 'Dance Moms', ''),
    ('41a03e0853d45c06bdfdedc5d26b78d8', 'BBC Earth', ''),
    ('1d3f60995d80521882a379fc06e47679', 'The Bob Ross Channel', '114491'),
    ('046c803ae0285ad5b831fa5b6d33a18c', 'Great British Baking Show', ''),
    ('a4d5fe37a0f5592dee5dbbb242d73fac', 'Growing Up Hip Hop WeTV', ''),
    ('015c83c4485a597d9e2132252e2a75ae', 'QVC', '60222'),
    ('e3cd976116d5578ab9be32baa738ade2', 'The Carol Burnett Show', ''),
    ('cfd904d2f64e108809ada1e095a044dc', 'Primetime Soaps', ''),
    ('dde77d33fefaf29efc69e1c0eed382f6', 'Bachelor Nation', ''),
    ('c7825e03df4a5bf4bdc87d65b7e3cdbb', 'This Old House Classic', ''),
    ('03eb704203f05a6fa06d9dce0d0ffccc', 'The Emeril Lagasse Channel', ''),
    ('0057bb20ce6c5717a5a025f2a73b682a', 'HSN', '62077'),
    ('707e6e216be8369fb30f6960f8cf667e', 'The Ed Sullivan Show', ''),
    ('c37101187ab95eec8640657f6ae2a322', 'NBC LX Home', ''),
    ('1a2266aeb6892914c687ebb3b58c2324', 'The Doctors', ''),
    ('e06d38b8b60be65974ff4069f82ee170', 'The Ellen Channel', ''),
    ('fb0f3c89533dc272a21e0488e2ca3fde', 'Jewelry Television', '16604'),
    ('bdf6b34e48585f588789f7e7857a84a6', 'FOX Soul', ''),
    ('923cd8e7aa1e5a5898aad41fdf6a0f0f', 'Grit Xtra', ''),
    ('7f28cd98ab575aa784d0f80698279e70', 'Bounce XL', ''),
    ('acd87247fd59591c9111b468bfb9cadc', 'Strawberry Shortcake', ''),
    ('8c4f4c99fe8d54fdad5ecc202a11efc1', 'Barney', ''),
    ('69791edb54d953268aa8933e2cb29a32', 'Pet Collective', ''),
    ('d20927e3b82c57ffa623db10618f115d', 'Teenage Mutant Ninja Turtles', ''),
    ('f1584888a8625b1abf77aecb4690abe0', 'Rev and Roll', ''),
    ('99a7477e0d38503d9b5456f07c1cf520', 'Yo Gabba Gabba', ''),
    ('c51c18ade0bd5b68b5e9bfa83410e372', 'ViX Jajaja', ''),
    ('9dd23031622757d1944e4782b2a192ef', 'Ninja Kidz TV', ''),
    ('85f1333ff9065bf2923c8fa5eee50036', 'Rainbow Ruby', ''),
    ('1d889b474b205fe385a2c552aa909b94', 'ViX Novelas en familia', ''),
    ('d1bfe824cfee5369a493d7a8bbd96ec1', 'Ryan and Friends', ''),
    ('498b53f6f63455bbba27754ceb5fcefc', 'Sonic The Hedgehog', ''),
    ('a673e94f2e565b3ab01168183745a6c6', 'Teletubbies', ''),
    ('e43d478938ff53f1a6810a11a1192d74', 'Tayo+', ''),
    ('cb52a20719394e05a8273177247b3768', 'Pokemon', ''),
    ('34335ef32c02519d897664a78767cc59', 'PBS Retro', ''),
    ('aeef4f63419a5d5fabcf0055585f9ccc', 'FailArmy', ''),
    ('f66aa243a9465896ac3e7eaef7cba3fd', 'Power Rangers', ''),
    ('b5cde121f98257329346020e2a60295a', 'Moonbug Kids', ''),
    ('86c08261fc015a58866210bea63eb8f4', 'Always Funny Videos', ''),
    ('9bd570b8bf145f73bba7532221e12666', 'Transformers', ''),
    ('8e0ba996e9985beb9c5e7f7f994ddc2e', 'Toony Planet', ''),
    ('77946e082d8958f599087890aebf1623', 'Yu-Gi-Oh!', ''),
    ('a3c405bfca9b525983054aa761b10dfb', 'Super Mario Brothers', ''),
    ('098bce44be895630a3e437516fe70853', 'Naturaleza Salvaje', ''),
    ('77761dc1928357d6809a2032fc2e17b8', 'The LEGO Channel', ''),
    ('d8b7e94b7edc53918e6afa251822df14', 'Pocket.watch Game-On', ''),
    ('144b3079deeb52f486e582a843546dba', 'Slugterra', ''),
    ('ac7629367b3f5fae93753623469c440d', 'Sensical Gaming', '132319'),
    ('313d259c8c5e530fb75a390e0ec75b17', 'Sony One Shark Tank', ''),
    ('23cf98a043e9d1336c958e6b707f81dc', 'Curiosity Explora', ''),
    ('37638e4236da59f2baf44138b1482fa7', 'How To', ''),
    ('f2dbaed0a18d5488a19038cee2660d6f', 'Ghosts Are Real', ''),
    ('9ef266ff45705a50b5cf5f57e9d27a99', 'Ice Road Truckers', ''),
    ('8ba514251e1a58b7b53368177baa1041', 'Law & Crime', ''),
    ('ebb810a8868251f18ac64056834c749a', 'FilmRise True Crime', ''),
    ('a96f9c338b3e5ab29ec3b436c01c84da', 'America\'s Test Kitchen', ''),
    ('479fe0d11f3f5132a3f36b617547da3b', 'Love Nature English', ''),
    ('ca27e73ea5315dd59ad7a75feff46b25', 'Paws & Claws', ''),
    ('845b77875b6056d192401e10949948d2', 'Dr. G: Medical Examiner', ''),
    ('285f3ba210e8561fa5261af0871ace90', 'Haunt TV', ''),
    ('a7a15769f0275c0ea9b2064b95637cfd', 'Real Disaster Channel', ''),
    ('938d8156a8c45e719f24022d8c1c972c', 'PGA TOUR', ''),
    ('cd2b8b03da52da0868e63927b54f9e7e', 'Modern Innovations by History', ''),
    ('2b50362a8bbc5a12a203aa48e6ea9fe1', 'The Jack Hanna Channel', ''),
    ('0b88dcd80f6a5e838c83a67e7cfa4b3a', 'MagellanTV Wildest', ''),
    ('cc7c474f22c9a6b9019fc8c4ad9d44ab', 'Science is Amazing', ''),
    ('cc72c25b76295e07862192d21bbff05b', 'Real Crime', ''),
    ('94598743eeb76f0dae3caf86ed026deb', 'PBS Travel', ''),
    ('0022e523d93f4bf65b170e7a03786b71', 'Dallas Cowboys Cheerleaders', ''),
    ('79b8b2c24f6a5680b7670bf2d832bc2b', 'Mysteria', ''),
    ('b182852fa5860ed0017de42c7b9ce05a', 'Nature Moments', ''),
    ('4240bb4bf71d5dcb8d8046d6a6a5eb15', 'NHL', ''),
    ('7b9a45d26315568a98a2b136972bde8d', 'WildEarth', ''),
    ('8ad68e46b0d35e61a7787b7042b61eb0', 'Love The Planet En Español', ''),
    ('28b13463c3cf9b26eb0f87ff11c2a8ec', 'Untold Stories Of The ER', ''),
    ('34e1284fa498d76281c395e5c7781d2b', 'Best of Pawn Stars', ''),
    ('f22e101ef2ef31e3324bf9c33e042065', 'In the Garage', ''),
    ('36de64356f407c48ced83a8e283855d5', 'Wicked Tuna', ''),
    ('56d6a285b9095e39a711a69a3058410c', 'Dog the Bounty Hunter', ''),
    ('f34d8f97f51e5df88571485528deb830', 'Real Housewives Vault', '138455'),
    ('b8671587aa35a03aaab52b1891f15a87', 'Love & Hip Hop', ''),
    ('a5de566e8ec2f3736be8a9db178f2689', 'Say Yes to the Dress', ''),
    ('99e44ea6699928b32c444dce774b9b32', 'Chaos on Cam by A&E', ''),
    ('4b73f84b1b72b6d4883f972c3de7394e', 'Survive or Die', ''),
    ('0b8a86b44ef0780c3b3226a63be07e0f', 'Highway Thru Hell', ''),
    ('ec4638e5e8ebbc1950ef9f0b71a1e175', 'CW Gold', ''),
    ('4f05781ce64f5b25a42a10c70853312a', 'E! Keeping Up', '138393'),
    ('b557ad9e550cce33ad4a5d90caf43523', 'The Repair Shop', ''),
    ('5bb5fc612a5e580f8a5832fa3b7089e7', 'Torque', ''),
    ('363301aa078e555ebd034290488bc9cf', 'Cheaters', ''),
    ('e3a9171a7b9155ca89ba9f7f3e1a6bde', 'Ghost Hunters', ''),
    ('c5a72a598a355db4b1781ca075280773', 'FilmRise Western', ''),
    ('11c7c3660c6b6f18a21838fd404a7a1d', 'Stargate', ''),
    ('be88a28806f251338d65ec08119f5c81', 'The Walking Dead Universe', ''),
    ('e0590ea8555bd7f12e4579ee172429e4', 'CW Forever', ''),
    ('8c36c765b4415db78a85078c37df4a5f', 'Lassie', '138452'),
    ('e2e10d08139b56aba73a448f5555b6fd', 'FilmRise Classic TV', ''),
    ('cb515abf2b733ce38c17b4a1a384d069', 'Nash Bridges', ''),
    ('df060784c402175739d219b5952838ca', 'TV Land Drama', ''),
    ('eb1dbd690001553eacdf32d7b155b933', 'SPARK TV', ''),
    ('15cc44e50d8453e18f21ea37713319c5', 'Electric Now', ''),
    ('95eb22cb730119300271c915c085c1db', 'Beverly Hills 90210', ''),
    ('ab47c5037be851e6a929a4d09daafeac', 'Baywatch', ''),
    ('834e2a09799752b3be7ecaab726b7242', 'Maverick Black Cinema', ''),
    ('dead0e7a46da533c94c61c8710ef3418', 'FilmRise British TV', ''),
    ('16722bc43ef05012afa2297def830266', 'FilmRise Mysteries', ''),
    ('6f3c26dab03826c9fdb44e0e61932e68', 'The Practice', '181900'),
    ('f6ddb474d9cd5ac88f5d1e85186b9cb9', 'Stories by AMC', ''),
    ('5ae9fb0c6e015b0d951063f6a6b39ee5', 'Telemundo Acción', '138527'),
    ('302d8d86738c0ab83d0ca3a5e2df3492', 'Xena', ''),
    ('1b5062d4ed345baeaceb0f0777057d2e', 'Hersphere', ''),
    ('c7254defc71c54e170206329d0d55c43', 'BBC Drama', ''),
    ('09430bcd30bf5b30a96ca22ecb87e3e1', 'ViX Grandes Parejas', ''),
    ('c423f0f050355a6fb8bcf7644af61852', 'FilmRise Sci-Fi', ''),
    ('52c137ae3cdc50768ad7ea3cdaf0f2f4', 'LOL Network', ''),
    ('2547ef42e2f45bbaa84752657cffe3c9', 'Tribeca Channel', ''),
    ('3b1d7acb9fd1264e4946c6cc157952e8', 'Classic Movie Westerns', ''),
    ('0e8fdbfae3f2547fa8ed55eda852488b', 'National Lampoon Channel', ''),
    ('e1192d17771c5a059930192d439957ee', 'MOVIESPHERE by Lionsgate', ''),
    ('34e2d8f442ab5fa3aed29436c2f8ed63', 'Shades of Black', ''),
    ('d32267711c6a5cf36520716c96b5b3fa', 'Movie Hub', ''),
    ('0f26179eb1225698ab79961ba4e04c41', 'Universal Movies', '164080'),
    ('c5cbd4129753f5732d7160596440404f', 'Movie Hub Action', ''),
    ('c490011275a8562b87366dbf80b62af2', 'CINEVAULT', ''),
    ('e2855d7a70c960c11ab278e386f11921', 'Paramount Movie Channel', ''),
    ('31d5929928835b90af7bee669e6dd217', 'El Rey Rebel', ''),
    ('d93912b4b21158fd95af2ceb8979c586', 'Dove', '110480'),
    ('8d8befc9060e861eb1bdd5b21c0403b3', 'Lifetime Love & Drama', ''),
    ('9d21287eff4c09a5d1078558e8245cfb', 'Pluto TV Fantastic', ''),
    ('acc2631dfc555b5e8cf100c66efdaff1', 'At the Movies', ''),
    ('5263332f746c6b1bed25aa6c105a79d9', 'MGM Presents', ''),
    ('aa16704e1ee052dea67cee1f79618754', 'Alien Nation by DUST', ''),
    ('349e0f0840cf21b888bff327b41a2413', 'Scares by Shudder', ''),
    ('77672e0655c5540095b1c492c91c37d3', 'Great American RomComs', ''),
    ('8ca348cf2f985d4b94388c9a455e7f13', 'CINEVAULT: Classics', ''),
    ('7e202ec14b17515a8e732f662aec155d', 'The Asylum', ''),
    ('be3c81de355c56f89cbf99301735cfd2', 'My Time Movie Network', ''),
    ('e7b30e2a58409242100d0edd2bfebbfd', 'Pam Grier\'s Soul Flix', ''),
    ('1e2a4ec82eb65f729ad7484337c0bcd4', 'FILMEX', ''),
    ('2deae9b4e83550f88f6776c45df08315', 'Hi-YAH!', ''),
    ('0abef49cfd6759b18f2ce64290cbdda3', 'FilmRise Action', ''),
    ('50cd4d6c632d5172834eb5b77fa395c6', 'FILMEX Clásico', ''),
    ('4b631847108256fd8c77f6ba3bf3af03', 'NBC Comedy Vault', '138448'),
    ('ba74e52d6fab9c1bc21698a8a2c89c48', 'Cheers + Frasier', ''),
    ('05a58f8f0d1b55999a9ab0e9caae8a47', 'Saved By The Bell', '138563'),
    ('d54dae14466b380a18d261ff2048e25e', 'GET TV', ''),
    ('3d3f3113ff49ca22c3ad51ee00fe7e9d', 'Comedy Central Pluto TV', ''),
    ('ed99462338da90bd0e53db5fe7ec7f11', 'The Bernie Mac Show', '197850'),
    ('7fbae583c30451e989ea55b4c605e8a9', 'Anger Management', '125128'),
    ('11a0e337ea9b53d4b7a9517e219d7f5b', 'Unspeakable', ''),
    ('e63d49e8acd7557596d091b3fa262999', 'FGTeeV', ''),
    ('b84d4953c086589c9d5202b7895552a7', 'The Dick Van Dyke Show', ''),
    ('9349c516ca6456b9b5ed7e20cb1c1384', 'SNL Vault', '113678'),
    ('0ce1935dc620b3afcc5b0022656fbeb3', 'Wild \'N Out', ''),
    ('65f83bbbf7a2eb952b798c96300bb329', 'Pink Panther', ''),
    ('51cc9cb7f052534cb6e6f55ac4980b06', 'Mythical 24/7', ''),
    ('92b50144a69f5862a6d398c478fa6aa0', 'Johnny Carson TV', ''),
    ('d812492073755dc4bb4293091e25df15', 'Laff More', ''),
    ('ffe5d9e0480d1c5e1dcead3abd966ac7', 'Let\'s Make a Deal', ''),
    ('e489e5ac7007959b9be9d838e146ae23', 'Shark Tank', '191580'),
    ('b7d8f61f313458c58e670180f0e3ce21', 'Project Runway', ''),
    ('62b4eba68f3051c68d2beb22dc94ebbc', 'Supermarket Sweep', ''),
    ('eec9d131b4095083a23c485d804c8b2d', 'American Ninja Warrior', '155369'),
    ('38f074c80d4c0a6208a8c50257f41d0f', 'Survivor', ''),
    ('b8c4f2efe56a512890042011e5cc0884', 'Hell\'s Kitchen | Kitchen Nightmares', ''),
    ('8ab612dd8b845046b641f38c1412303d', 'Fear Factor', ''),
    ('45439036a47c558cf55ed752f9d9a65d', 'Who Wants to be a Millionaire', '197849'),
    ('3a7f2ae8da19530ba33c0c3502becd32', 'Sony One Competencias', ''),
    ('23427355f7f35851b75a210d7cca22a4', 'Celebrity Name Game', ''),
    ('34b686f999ecaacfbcd91bb5195c3124', 'The Masked Singer', ''),
    ('e64d0ca363b1e9e5475da2559a97f6b5', 'The Challenge', ''),
    ('7885c5d26ffc5e31a7290e2d73d419d8', 'Perform', ''),
    ('1b4a3b14ce19baa18082c2cb7e0fb086', 'The Amazing Race', ''),
    ('22d9d1cb36e855a2b40ba752313a50d9', 'FilmRise Food', ''),
    ('417de64d5d2255119088209d40ebfee6', 'Estrella Games', ''),
    ('bb654468dac3575ba2acace0255024f7', 'Impossible', ''),
    ('cdfd1ee39343a406984976d2687e894d', 'Vas O No Vas USA', ''),
    ('cf7c191b9f2a4207cf119fb095e965e7', 'American Gladiators', ''),
    ('06434efa44ba22806c99a9dc1c90e2e3', 'Apostarías por Mí: Multicámaras', ''),
    ('28844a800a4e54909eec9695aa3762b2', 'NEWSMAX2', '121162'),
    ('d748045d77105901b8e25fcc34aa9e2f', 'America\'s Voice', ''),
    ('e29e205ea1fb529f996ad862a2d6724e', 'OAN Plus', ''),
    ('ee70e4ad68b55abb88e7b95b8940566a', 'TYT-Go', ''),
    ('0a0eb9ec33b4594eb3ec4905913eb9eb', 'Telemundo Florida', '136631'),
    ('b5ae401518c35149915192abecb88af9', 'Telemundo Noreste', '136630'),
    ('e6f5a623c45d5757a50e602eb0591f62', 'Noticias Telemundo Ahora', '138525'),
    ('14219ef551f3597388e236349187983f', 'Telemundo California', '136629'),
    ('396f57d10b5d5ec98648f810dcbe1f11', 'ViX Novelas de romance', ''),
    ('50194124ea78578093f1b87ecc5a2048', 'Telemundo Texas', '136700'),
    ('e3823bb513585861904499a21a736c4f', 'Canal ViX', ''),
    ('42ee7ce6165d5342be3e509f0afe5048', 'Canela.TV Clásicos', ''),
    ('7b5699f58b2b54be9da4d23fe354fb59', 'ViX Villanos de Novela', ''),
    ('5b18fd4082c85d698f6f71702d82be28', 'COPS en Español', ''),
    ('5e709cfe55d955ec899903cde681a5a4', 'Azteca Internacional', ''),
    ('91dff2b8a62b5fe1bdf64de721032cba', 'Estrella TV', '117477'),
    ('6d63114f44f05a9cbba0ac0477ab1254', 'RCN Mas', ''),
    ('27e2e5e62ce817aab29fe1fdd6abd764', 'CSI en español', ''),
    ('ab6cd6569bf6557a8c5d9798cfce202b', 'ViX Novelas de oro', ''),
    ('8cc525b5848c561da7e2d47b934adf0c', 'Moovimex', ''),
    ('03a419fc4dff59b39b176d9f76b09e38', 'Love Nature Spanish', ''),
    ('db95a237643ea0eea113c6c024515df2', 'Todo Novelas, Más Pasiones', ''),
    ('1cec89adc1796217721b1ee2cb21255c', 'Mi Raza', ''),
    ('35633f08c98857cb9e85c91eae17c0d5', 'ViX Lo Mejor de Liga MX', ''),
    ('59d2dbddc997559bb7b689a7c887cba4', 'FreeTV Sureño', ''),
    ('1887d3420e48951dcb31ce014c7a8815', 'Cougar Town', '200288'),
    ('6a81c543eb73fec1643f03f627ec39ae', 'Blossom', '200287'),
    ('e2dc03a4dcf7653c5f3655ead48ae0ac', 'MeatEater', ''),
    ('20a6aa8be0f3aff9d50f3daf3dfe6ba8', 'America\'s Funniest Home Videos', ''),
    ('f53e794f65b781ff1a0d37843c704827', 'FILMEX Acción', ''),
    ('0ef20bc07040189f1894cf2aa4546ab4', 'Felicity', '160652'),
    ('141ee5cd661897880f825e9fe8062b5e', 'Telemundo Puerto Rico', '194973'),
    ('8b2c97d22c7fed4d9d302a1796d5e009', 'FILMEX Comedia', ''),
    ('e4a278e5f27cc0c97aae4ddbefdbc5e6', 'Sobreviví', ''),
    ('52b548a9c2dbafed837a8fc1ae28c1c4', 'CNN Xpress', ''),
    ('52ed4fc0c2635136b6ea44e2dc4a8a00', 'CTV News', ''),
    ('10b859e5e8a1e710680c11dfdfb85c11', 'FUEL TV', '116115'),
    ('52361d5aea45dadb3168c2ecea7d984a', 'Willow Sports', '164602'),
    ('fe08b9ce5c80c8a52c5253d026d9395c', 'Ancient Aliens', ''),
    ('562b0413f28e57899e3542b71ae5443a', 'Alfred Hitchcock Presents', '138568'),
    ('509701eee823e871c99a2b8e768ff8e8', 'Star Trek', ''),
    ('714890342efe5cd6b236e85415b661ca', 'Universal Monsters', '138450'),
    ('21520542993cb0e350d453c13a2f3654', 'Space & Beyond', ''),
    ('96d8548fc5765585b95b8138b4f9f2ef', 'UnXplained Zone', ''),
    ('0e297acfe2a0fd483eedd7adf90ce1d4', 'Supernatural Drama', ''),
    ('ee3feca0341056b7ac74c27e37ef085f', 'AMC Thrillers', ''),
    ('1f62c81a73575fdca9fdc8a36c55d8b9', 'MST3K', ''),
    ('6a0629e467b5502c906136dadac290a0', 'FilmRise Horror', ''),
    ('d79c47ebb9745dc4873f24d4c9fc68ce', 'Screambox TV', '116607'),
    ('8b564fb868f8f1d49a1986123724ef18', 'Stingray Stargaze', ''),
    ('9e646a33ba748470122f6ffe57efb656', 'Horror Stories', ''),
    ('d5381e91f6f651d0a933e7e51fdadc7c', 'Classic Doctor Who', ''),
    ('307b3b9ea321539b9688148fc97a8bfd', 'Crunchyroll', ''),
    ('e6d36fc89adf1de07aa090ec5669e0ca', 'Outersphere', ''),
    ('bfced9f99e1a685204996285b2788dbc', 'Pluto TV Anime', ''),
    ('72a54a3c7a3d5381a1baa223a5dc8d23', 'RetroCrush', '119355'),
    ('6727481eeb0951dfa692259e25055293', 'AMC en Español', ''),
    ('308d792301e9897cbd41d0390abef68f', 'Cine de Horror', ''),
)

class RokuScraper(BaseScraper):

    source_name     = "roku"
    display_name    = "The Roku Channel"
    scrape_interval = 1440        # channel list is static; refresh once daily

    # No config needed — fully anonymous, no credentials
    config_schema = []

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.session.headers.update({
            "User-Agent":      _UA,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept":          "application/json",
        })

        # Session state — refreshed when expired
        self._csrf_token:    Optional[str]   = None
        self._session_born:  Optional[float] = None   # epoch seconds

    # ── Session management ─────────────────────────────────────────────────────

    def _session_is_fresh(self) -> bool:
        if not self._csrf_token or not self._session_born:
            return False
        return (time.time() - self._session_born) < _SESSION_TTL

    def _refresh_session(self) -> bool:
        """Boot a fresh Roku browser session. Returns True on success."""
        try:
            # Step 1: hit live-tv to collect cookies
            r1 = self.session.get(_LIVE_TV, timeout=15)
            if r1.status_code != 200:
                logger.error("[roku] live-tv returned %d", r1.status_code)
                return False

            # Step 2: fetch csrf token (retry up to 4 times)
            csrf = None
            for attempt in range(5):
                r2 = self.session.get(_CSRF_URL, timeout=10)
                if r2.status_code == 200:
                    csrf = r2.json().get("csrf")
                    break
                wait = 2 ** attempt
                logger.warning("[roku] csrf attempt %d returned %d, retry in %ds",
                               attempt + 1, r2.status_code, wait)
                time.sleep(wait)

            if not csrf:
                logger.error("[roku] could not obtain csrf token")
                return False

            self._csrf_token   = csrf
            self._session_born = time.time()
            logger.debug("[roku] session refreshed, csrf=%s…", csrf[:12])
            return True

        except Exception as exc:
            logger.error("[roku] session refresh failed: %s", exc)
            return False

    def _ensure_session(self) -> bool:
        if not self._session_is_fresh():
            return self._refresh_session()
        return True

    def _api_headers(self) -> dict:
        return {
            "csrf-token":                         self._csrf_token or "",
            "origin":                             _BASE,
            "referer":                            _LIVE_TV,
            "content-type":                       "application/json",
            "x-roku-reserved-amoeba-ids":         "",
            "x-roku-reserved-experiment-configs": "e30=",
            "x-roku-reserved-experiment-state":   "W10=",
            "x-roku-reserved-lat":                "0",
        }

    # ── Content proxy helper ───────────────────────────────────────────────────

    def _fetch_content(self, station_id: str, feature_include: str = "") -> Optional[dict]:
        """Call the therokuchannel content proxy for a given station_id."""
        qs = f"?featureInclude={feature_include}" if feature_include else ""
        content_url = _CONTENT_TPL.format(sid=station_id) + qs
        proxy_url   = _PROXY_BASE + quote(content_url, safe="")
        try:
            r = self.session.get(proxy_url, headers=self._api_headers(), timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception as exc:
            logger.warning("[roku] content fetch error for %s: %s", station_id, exc)
        return None

    # ── fetch_channels ─────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        if not self._ensure_session():
            return []

        channels: list[ChannelData] = []
        total = len(_ROKU_CHANNELS)
        logger.info("[roku] fetching metadata for %d channels…", total)

        for i, (sid, title, gracenote_id) in enumerate(_ROKU_CHANNELS):
            # Skip playlist/curated tokens — they start with 'w.' and are not
            # linear channel IDs. They break URL routing and the playback API.
            if sid.startswith('w.') or len(sid) > 128:
                logger.debug('[roku] skipping non-channel ID: %s (%s)', sid[:40], title)
                continue

            play_id  = None
            logo     = None
            category = None

            try:
                data = self._fetch_content(sid)
                if data:
                    view_opts = data.get("viewOptions") or [{}]
                    play_id   = view_opts[0].get("playId") if view_opts else None

                    image_map = data.get("imageMap") or {}
                    for key in ("grid", "detailBackground", "detailPoster"):
                        img = image_map.get(key)
                        if img and img.get("path"):
                            logo = img["path"]
                            break

                    cats = data.get("categories") or data.get("categoryObjects") or []
                    category = _normalize_category(cats[0] if cats else None)

            except Exception as exc:
                logger.warning("[roku] metadata fetch failed for %s (%s): %s", title, sid, exc)

            channels.append(ChannelData(
                source_channel_id = sid,
                name              = title,
                stream_url        = "roku://{}".format(sid),
                logo_url          = logo,
                category          = category,
                language          = "en",
                country           = "US",
                stream_type       = "hls",
                slug              = "{}|{}".format(play_id or "", gracenote_id),
            ))

            if (i + 1) % 50 == 0:
                logger.info("[roku] metadata progress: %d/%d", i + 1, total)

            time.sleep(0.1)

        logger.info("[roku] %d channels fetched", len(channels))
        return channels

    def fetch_epg(self, channels: list[ChannelData]) -> list[ProgramData]:
        if not self._ensure_session():
            return []

        programs: list[ProgramData] = []
        total = len(channels)

        for i, ch in enumerate(channels):
            sid = ch.source_channel_id
            try:
                data = self._fetch_content(sid, feature_include="linearSchedule")
                if not data:
                    continue

                schedule = data.get("features", {}).get("linearSchedule", [])
                for entry in schedule:
                    prog = self._parse_program(sid, entry)
                    if prog:
                        programs.append(prog)

            except Exception as exc:
                logger.warning("[roku] EPG error for %s (%s): %s", ch.name, sid, exc)

            if (i + 1) % 50 == 0:
                logger.info("[roku] EPG progress: %d/%d channels", i + 1, total)

            time.sleep(0.25)  # be polite

        logger.info("[roku] %d EPG entries fetched for %d channels", len(programs), total)
        return programs

    def _parse_program(self, station_id: str, entry: dict) -> Optional[ProgramData]:
        try:
            start_str = entry.get("date", "")
            start = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            duration = entry.get("duration", 0)
            end = start + timedelta(seconds=duration)
        except (ValueError, TypeError):
            return None

        c       = entry.get("content", {})
        series  = c.get("series", {})
        ep_title = c.get("title", "")
        series_title = series.get("title", "")
        title   = series_title or ep_title or "Unknown"

        # Description
        descs = c.get("descriptions") or {}
        description = (
            descs.get("250") or descs.get("60") or descs.get("40") or c.get("description")
        )

        # Artwork — prefer gridEpg, fall back to grid
        image_map = c.get("imageMap") or {}
        poster = (
            (image_map.get("gridEpg") or {}).get("path")
            or (image_map.get("grid") or {}).get("path")
        )

        # Rating
        ratings = c.get("parentalRatings") or []
        rating  = ratings[0].get("code") if ratings else None

        # Season / Episode
        season  = c.get("seasonNumber")
        episode = c.get("episodeNumber")
        try:
            season  = int(season)  if season  else None
            episode = int(episode) if episode else None
        except (ValueError, TypeError):
            season = episode = None

        # Category from genres
        genres = c.get("genres") or []
        category = genres[0].capitalize() if genres else None

        return ProgramData(
            source_channel_id = station_id,
            title             = title,
            start_time        = start,
            end_time          = end,
            description       = description,
            poster_url        = poster,
            category          = category,
            rating            = rating,
            episode_title     = ep_title if series_title and ep_title != series_title else None,
            season            = season,
            episode           = episode,
        )

    # ── resolve ────────────────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        """
        raw_url format: roku://{station_id}
        Returns a live osm.sr.roku.com HLS/DASH stream URL.
        Calls /api/v3/playback with a fresh session each time.
        The JWT in the stream URL is short-lived so we always fetch fresh.
        """
        if not raw_url.startswith("roku://"):
            return raw_url

        station_id = raw_url[len("roku://"):]

        if not self._ensure_session():
            logger.error("[roku] resolve failed — could not obtain session")
            return raw_url

        # Step 1: try to get playId from the channel's stored slug (set at scrape time)
        # slug format: "{playId}|{gracenoteId}"  — avoids a content proxy round-trip
        play_id = None
        from ..models import Channel as _Channel, Source as _Source
        try:
            ch = (
                _Channel.query
                .join(_Source)
                .filter(_Source.name == self.source_name,
                        _Channel.source_channel_id == station_id)
                .first()
            )
            if ch and ch.slug and "|" in ch.slug:
                stored_play_id = ch.slug.split("|", 1)[0]
                if stored_play_id:
                    play_id = stored_play_id
                    logger.debug("[roku] using stored playId for %s", station_id)
        except Exception:
            pass  # DB not available in this context — fall through to live fetch

        if not play_id:
            # Fall back: hit content proxy for a fresh playId
            data = self._fetch_content(station_id)
            if data:
                view_opts = data.get("viewOptions") or [{}]
                play_id   = view_opts[0].get("playId") if view_opts else None

        if not play_id:
            # Last resort: regex scan of raw proxy response
            content_url = _CONTENT_TPL.format(sid=station_id)
            proxy_url   = _PROXY_BASE + quote(content_url, safe="")
            try:
                r = self.session.get(proxy_url, headers=self._api_headers(), timeout=10)
                pids = re.findall(r's-[a-z0-9_]+\.[A-Za-z0-9+/=]+', r.text)
                play_id = pids[0] if pids else None
            except Exception:
                pass

        if not play_id:
            logger.warning("[roku] no playId found for %s", station_id)
            return raw_url

        # Decode to determine media format
        try:
            decoded = base64.b64decode(play_id.split(".", 1)[1]).decode()
            media_format = "mpeg-dash" if "dash" in decoded.lower() else "m3u"
        except Exception:
            media_format = "m3u"

        # Step 2: call /api/v3/playback
        session_id = self.session.cookies.get("_usn", "roku-scraper")
        body = {
            "rokuId":      station_id,
            "playId":      play_id,
            "mediaFormat": media_format,
            "drmType":     "widevine",
            "quality":     "fhd",
            "bifUrl":      None,
            "adPolicyId":  "",
            "providerId":  "rokuavod",
            "playbackContextParams": (
                f"sessionId={session_id}"
                "&pageId=trc-us-live-ml-page-en-current"
                "&isNewSession=0&idType=roku-trc"
            ),
        }
        try:
            r2 = self.session.post(
                _PLAYBACK,
                headers=self._api_headers(),
                json=body,
                timeout=10,
            )
            if r2.status_code == 200:
                stream_url = r2.json().get("url", "")
                if stream_url:
                    logger.debug("[roku] resolved %s -> %s…", station_id, stream_url[:60])
                    return stream_url
            logger.warning("[roku] playback returned %d for %s", r2.status_code, station_id)
        except Exception as exc:
            logger.error("[roku] playback request failed for %s: %s", station_id, exc)

        return raw_url

    # ── M3U extras ─────────────────────────────────────────────────────────────
    # FastChannels calls generate_m3u() which uses ChannelData fields.
    # We stuffed "playId|gracenoteId" into slug. Override the M3U line builder
    # to emit tvc-guide-stationid for channels that have a Gracenote ID.
    # BaseScraper's generate_m3u() calls channel_m3u_tags() if it exists.

    def channel_m3u_tags(self, ch: ChannelData) -> dict[str, str]:
        """
        Return extra M3U tags for this channel.
        Called by BaseScraper.generate_m3u() if the method exists.
        """
        tags: dict[str, str] = {}

        # Unpack gracenoteId from slug field (format: "playId|gracenoteId")
        if ch.slug and "|" in ch.slug:
            _, gracenote_id = ch.slug.split("|", 1)
            if gracenote_id and gracenote_id.isdigit():
                # This tells Channels DVR to pull full guide data from Gracenote
                tags["tvc-guide-stationid"] = gracenote_id

        return tags

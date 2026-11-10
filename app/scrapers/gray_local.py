"""Gray Local News: live streams from Gray Media's local TV stations (newscasts,
24/7 weather, breaking news and live events, station sports channels and
Telemundo affiliates), plus InvestigateTV and Local News Live. Free, no
account. Listed under Free Networks; the user picks their station(s).

Gray's station sites (wbrc.com, azfamily.com, ...) run on Arc XP with a
Quickplay back end:
  data-store-cdn.api.graycms.quickplay.com/content/epglite?reg=us&dt=web&
    client=gray-gm-web&prop=gm|<arc site>|<arc site>&dma=all&start&end
    → that station's channels (callSign "cs", content id "cid", name, image
      ids) with their airings (title, start/end, TitanTV poster). Needs a
      browser User-Agent (403 otherwise). One call per picked station.
  Playback: Quickplay's content authorizer gives guests (drm none)
    live-streaming-mcdn.api.graylive.quickplay.com/<cs-with-dashes>/<same>.isml/index.m3u8,
    which plays directly with no token, so the URL is built from the callSign.
    Clear HLS, 720p.
InvestigateTV (wvue_xos1_input_4) and Local News Live (dcb_xos1_input_4) are
on every station's list and are always carried, once. Station table (Arc site
id, domain, market) built 2026-11-10 from graymedia.com/brands/markets and
each site's /livestream page; seven Gray domains not on this platform are left
out.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

from .base import BaseScraper, ChannelData, ProgramData, StreamDeadError

logger = logging.getLogger(__name__)

EPG_URL = 'https://data-store-cdn.api.graycms.quickplay.com/content/epglite'
IMAGE_URL = 'https://image-resizer-cloud-cdn.api.graycms.quickplay.com/image/{cid}/{ref}.jpg?width=400'
STREAM_URL = 'https://live-streaming-mcdn.api.graylive.quickplay.com/{d}/{d}.isml/index.m3u8'
SCHEME = 'gray://'
_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36'
# Carried for everyone; their airings come from this station's list when the
# user hasn't picked it.
SHARED = {'wvue_xos1_input_4': 'InvestigateTV', 'dcb_xos1_input_4': 'Local News Live'}
_SHARED_FROM = 'wvue'
_GUIDE_HOURS = 48
# Gray fills channels with no schedule (breaking news / live events, cameras)
# with hourly airings titled this; they get the channel's name instead.
_NO_GUIDE_TITLE = 'epg feed not available'
# epglite returns at most 10 airings per channel per call, so the guide is
# fetched in windows short enough to stay under that.
_WINDOW_HOURS = 4

# (Arc site id, domain, market)
STATIONS = [
    ('ktuu', 'alaskasnewssource.com', 'Anchorage, AK'),
    ('ktvf', 'webcenterfairbanks.com', 'Fairbanks, AK'),
    ('wbrc', 'wbrc.com', 'Birmingham, AL'),
    ('wtvy', 'wtvy.com', 'Dothan, AL'),
    ('waff', 'waff.com', 'Huntsville, AL'),
    ('wala', 'fox10tv.com', 'Mobile, AL'),
    ('wsfa', 'wsfa.com', 'Montgomery, AL'),
    ('kait', 'kait8.com', 'Jonesboro, AR'),
    ('kpho', 'azfamily.com', 'Phoenix, AZ'),
    ('kold', 'kold.com', 'Tucson, AZ'),
    ('wfsb', 'wfsb.com', 'Hartford, CT'),
    ('wcjb', 'wcjb.com', 'Gainesville, FL'),
    ('wjhg', 'wjhg.com', 'Panama City, FL'),
    ('wwsb', 'mysuncoast.com', 'Sarasota, FL'),
    ('wctv', 'wctv.tv', 'Tallahassee, FL'),
    ('walb', 'walb.com', 'Albany, GA'),
    ('wgcl', 'atlantanewsfirst.com', 'Atlanta, GA'),
    ('wktb', 'telemundoatlanta.com', 'Atlanta, GA'),
    ('wrdw', 'wrdw.com', 'Augusta, GA'),
    ('wtvm', 'wtvm.com', 'Columbus, GA'),
    ('wpga', 'wpganews.com', 'Macon, GA'),
    ('wtoc', 'wtoc.com', 'Savannah, GA'),
    ('khnl', 'hawaiinewsnow.com', 'Honolulu, HI'),
    ('kcrg', 'kcrg.com', 'Cedar Rapids, IA'),
    ('kwqc', 'kwqc.com', 'Davenport, IA'),
    ('kyou', 'kyoutv.com', 'Ottumwa, IA'),
    ('ktiv', 'ktiv.com', 'Sioux City, IA'),
    ('week', 'week.com', 'Peoria, IL'),
    ('wgem', 'wgem.com', 'Quincy, IL'),
    ('wifr', 'wifr.com', 'Rockford, IL'),
    ('wfie', '14news.com', 'Evansville, IN'),
    ('wpta', 'wpta21.com', 'Fort Wayne, IN'),
    ('wndu', 'wndu.com', 'South Bend, IN'),
    ('wthi', 'wthitv.com', 'Terre Haute, IN'),
    ('wlfi', 'wlfi.com', 'West Lafayette, IN'),
    ('wibw', 'wibw.com', 'Topeka, KS'),
    ('kwch', 'kwch.com', 'Wichita, KS'),
    ('wbko', 'wbko.com', 'Bowling Green, KY'),
    ('wymt', 'wymt.com', 'Hazard, KY'),
    ('wkyt', 'wkyt.com', 'Lexington, KY'),
    ('wave', 'wave3.com', 'Louisville, KY'),
    ('kalb', 'kalb.com', 'Alexandria, LA'),
    ('wafb', 'wafb.com', 'Baton Rouge, LA'),
    ('katc', 'katc.com', 'Lafayette, LA'),
    ('kplc', 'kplctv.com', 'Lake Charles, LA'),
    ('knoe', 'knoe.com', 'Monroe, LA'),
    ('wvue', 'fox8live.com', 'New Orleans, LA'),
    ('ksla', 'ksla.com', 'Shreveport, LA'),
    ('wggb', 'westernmassnews.com', 'Springfield, MA'),
    ('wabi', 'wabi.tv', 'Bangor, ME'),
    ('wagm', 'wagmtv.com', 'Presque Isle, ME'),
    ('wilx', 'fox47news.com', 'Lansing, MI'),
    ('wluc', 'uppermichiganssource.com', 'Marquette, MI'),
    ('wnem', 'wnem.com', 'Saginaw, MI'),
    ('kbjr', 'northernnewsnow.com', 'Duluth, MN'),
    ('keyc', 'keyc.com', 'Mankato, MN'),
    ('kttc', 'kttc.com', 'Rochester, MN'),
    ('kfvs12', 'kfvs12.com', 'Cape Girardeau, MO'),
    ('kctv', 'kctv5.com', 'Kansas City, MO'),
    ('ky3', 'ky3.com', 'Springfield, MO'),
    ('kmov', 'firstalert4.com', 'St. Louis, MO'),
    ('wdam', 'wdam.com', 'Hattiesburg, MS'),
    ('wlbt', 'wlbt.com', 'Jackson, MS'),
    ('wtok', 'wtok.com', 'Meridian, MS'),
    ('wtva', 'wtva.com', 'Tupelo, MS'),
    ('wbtv', 'wbtv.com', 'Charlotte, NC'),
    ('witn', 'witn.com', 'Greenville, NC'),
    ('wect', 'wect.com', 'Wilmington, NC'),
    ('kfyr', 'kfyrtv.com', 'Bismarck, ND'),
    ('kvly', 'valleynewslive.com', 'Fargo, ND'),
    ('ksnb', 'ksnblocal4.com', 'Hastings, NE'),
    ('koln', '1011now.com', 'Lincoln, NE'),
    ('knop', 'knopnews2.com', 'North Platte, NE'),
    ('wowt', 'wowt.com', 'Omaha, NE'),
    ('kvvu', 'fox5vegas.com', 'Las Vegas, NV'),
    ('kolo', 'kolotv.com', 'Reno, NV'),
    ('wbng', 'wbng.com', 'Binghamton, NY'),
    ('wwny', 'wwnytv.com', 'Watertown, NY'),
    ('investigatetv', 'investigatetv.com', 'National'),
    ('wxix', 'fox19.com', 'Cincinnati, OH'),
    ('woio', 'cleveland19.com', 'Cleveland, OH'),
    ('wtvg', '13abc.com', 'Toledo, OH'),
    ('kswo', 'kswo.com', 'Lawton, OK'),
    ('kptv', 'kptv.com', 'Portland, OR'),
    ('wcsc', 'live5news.com', 'Charleston, SC'),
    ('wistv', 'wistv.com', 'Columbia, SC'),
    ('whns', 'foxcarolina.com', 'Greenville, SC'),
    ('wmbf', 'wmbfnews.com', 'Myrtle Beach, SC'),
    ('ksfy', 'dakotanewsnow.com', 'Sioux Falls, SD'),
    ('wbbj', 'wbbjtv.com', 'Jackson, TN'),
    ('wvlt', 'wvlt.tv', 'Knoxville, TN'),
    ('wmctv', 'actionnews5.com', 'Memphis, TN'),
    ('wsmv', 'wsmv.com', 'Nashville, TN'),
    ('kfda', 'newschannel10.com', 'Amarillo, TX'),
    ('kgns', 'kgns.tv', 'Laredo, TX'),
    ('kcbd', 'kcbd.com', 'Lubbock, TX'),
    ('ktre', 'ktre.com', 'Lufkin, TX'),
    ('kosa', 'cbs7.com', 'Midland, TX'),
    ('kxii', 'kxii.com', 'Sherman, TX'),
    ('kltv', 'kltv.com', 'Tyler, TX'),
    ('wvir', '29news.com', 'Charlottesville, VA'),
    ('whsv', 'whsv.com', 'Harrisonburg, VA'),
    ('wwbt', '12onyourside.com', 'Richmond, VA'),
    ('wdbj', 'wdbj7.com', 'Roanoke, VA'),
    ('weau', 'weau.com', 'Eau Claire, WI'),
    ('wbay', 'wbay.com', 'Green Bay, WI'),
    ('wmtv', 'wmtv15news.com', 'Madison, WI'),
    ('wsaw', 'wsaw.com', 'Wausau, WI'),
    ('wvva', 'wvva.com', 'Bluefield, WV'),
    ('wdtv', 'wdtv.com', 'Clarksburg, WV'),
    ('wsaz', 'wsaz.com', 'Huntington, WV'),
    ('wtap', 'wtap.com', 'Parkersburg, WV'),
]
_BY_SITE = {site: (dom, market) for site, dom, market in STATIONS}


def station_label(site: str) -> str:
    dom, market = _BY_SITE.get(site, ('', ''))
    return f'{market} — {site.upper()} ({dom})' if market else site.upper()


def clean_station_filter(value) -> list[str]:
    """Validate the picked stations (Arc site ids); unknown ones are dropped."""
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > len(STATIONS) or any(not isinstance(v, str) for v in value):
        raise ValueError('Invalid station list')
    return [v for v in dict.fromkeys(v.strip().lower() for v in value) if v in _BY_SITE]


def _parse(val) -> datetime | None:
    try:
        return datetime.fromisoformat(str(val).replace('Z', '+00:00'))
    except (TypeError, ValueError):
        return None


def _name(lon) -> str:
    for item in lon or []:
        if item.get('lang') == 'en' and item.get('n'):
            return item['n'].strip()
    return ((lon or [{}])[0].get('n') or '').strip()


_EPISODE_RE = re.compile(r'^(?P<show>.+?):\s+(?P<episode>.+?)\s*\(S(?P<s>\d+)E(?P<e>\d+)\)\s*$')
_SXEY_RE = re.compile(r'\s*\(S(?P<s>\d+)E(?P<e>\d+)\)\s*$')


def split_title(title: str) -> tuple[str, str | None, int | None, int | None]:
    """(show, episode title, season, episode) from Gray's packed titles:
    "InvestigateTV+: Shrinkflation continues ... (S2E21)" → ("InvestigateTV+",
    "Shrinkflation continues ...", 2, 21). Titles without an (SxEy) code are
    left whole ("Georgia's Gitmo: Part 1")."""
    m = _EPISODE_RE.match(title)
    if m:
        return m['show'].strip(), m['episode'].strip(), int(m['s']), int(m['e'])
    m = _SXEY_RE.search(title)
    if m:
        return title[:m.start()].strip(), None, int(m['s']), int(m['e'])
    return title, None, None, None


def _category(name: str) -> str:
    n = name.lower()
    if re.search(r'weather|first alert|storm|radar|forecast', n):
        return 'Weather'
    if re.search(r'sport|warrior', n):
        return 'Sports'
    if 'investigatetv' in n:
        return 'News'
    return 'Local News'


class GrayLocalScraper(BaseScraper):
    source_name = 'gray_local'
    display_name = 'Gray Local News'
    source_category = 'fast'
    grouped_under = 'free_networks'   # a free network's own sites, listed under Free Networks
    scrape_interval = 360
    stream_audit_enabled = True

    def _picked(self) -> list[str]:
        try:
            return clean_station_filter((self.config or {}).get('station_filter'))
        except ValueError:
            return []

    def _window(self, site: str, start: datetime, end: datetime) -> list[dict]:
        stamp = lambda d: d.strftime('%Y-%m-%dT%H:%M:%S.000Z')  # noqa: E731
        r = self.session.get(EPG_URL, timeout=30, headers={'User-Agent': _UA, 'Accept': 'application/json'},
                             params={'reg': 'us', 'dt': 'web', 'client': 'gray-gm-web', 'prop': f'gm|{site}|{site}',
                                     'dma': 'all', 'start': stamp(start), 'end': stamp(end)})
        r.raise_for_status()
        body = r.json() or {}
        if (body.get('header') or {}).get('code', 0) != 0:
            raise RuntimeError(f'Gray {site}: {(body.get("header") or {}).get("message")}')
        return body.get('data') or []

    def _station_rows(self, site: str) -> list[dict]:
        """The station's channel rows, each with every airing across the guide
        window (merged from the 4-hour calls). The first call failing fails the
        station; a later one only shortens its guide."""
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        rows: dict[str, dict] = {}
        t, first = now - timedelta(hours=2), True
        while t < now + timedelta(hours=_GUIDE_HOURS):
            try:
                batch = self._window(site, t, t + timedelta(hours=_WINDOW_HOURS))
            except Exception:
                if first:
                    raise
                break
            for row in batch:
                cid = row.get('cid')
                if cid not in rows:
                    rows[cid] = {'cid': cid, 'airing': []}
                rows[cid]['airing'].extend(row.get('airing') or [])
            t, first = t + timedelta(hours=_WINDOW_HOURS), False
        return list(rows.values())

    def _collect(self) -> dict[str, dict]:
        """callSign → {cid, name, site, images, airings} for the picked stations plus the shared channels."""
        picked = self._picked()
        sites = list(picked)
        if _SHARED_FROM not in sites:
            sites.append(_SHARED_FROM)
        out: dict[str, dict] = {}
        for site in sites:
            try:
                rows = self._station_rows(site)
            except Exception as exc:  # noqa: BLE001 — one station failing leaves the rest
                logger.warning('[gray_local] %s: listing failed: %s', site, exc)
                continue
            for row in rows:
                airings = row.get('airing') or []
                ch = (airings[0].get('ch') if airings else None) or {}
                cs = ch.get('cs') or ch.get('acs')
                if not cs or (site not in picked and cs not in SHARED):
                    continue
                entry = out.setdefault(cs, {'cid': row.get('cid'), 'name': _name(ch.get('lon')) or SHARED.get(cs, cs),
                                            'site': site, 'images': ch.get('ia') or [], 'airings': []})
                seen = {a.get('id') for a in entry['airings']}
                entry['airings'].extend(a for a in airings if a.get('id') not in seen)
        return out

    def fetch_channels(self) -> list[ChannelData]:
        self._items = self._collect()
        channels = []
        for cs, item in self._items.items():
            name = item['name']
            # Generic names ("First Alert 24/7") get the station's call letters,
            # unless the name already says which station it is.
            site = item['site']
            marks = {site, cs.split('_', 1)[0], _BY_SITE.get(site, ('',))[0].split('.')[0]}
            if (cs not in SHARED and 'telemundo' not in name.lower()
                    and not any(m and m.lower() in name.lower().replace(' ', '') for m in marks)):
                name = f'{site.upper()} {name}'
            images = item['images']
            ref = next((i for i in images if i.endswith('1x1')), None) or (images[0] if images else None)
            spanish = 'telemundo' in name.lower()
            channels.append(ChannelData(
                source_channel_id=cs,
                name=name,
                stream_url=f'{SCHEME}{cs}',
                logo_url=IMAGE_URL.format(cid=item['cid'], ref=ref) if ref and item.get('cid') else None,
                category=_category(name),
                language='es' if spanish else 'en',
                country='US',
                stream_type='hls',
                description=None if cs in SHARED else f'{station_label(item["site"])}',
            ))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        items = getattr(self, '_items', None)
        if items is None:
            items = self._collect()
        wanted = {ch.source_channel_id for ch in channels}
        names = {ch.source_channel_id: ch.name for ch in channels}
        programs = []
        for cs, item in items.items():
            if cs not in wanted:
                continue
            for a in item['airings']:
                pgm = a.get('pgm') or {}
                title = _name(pgm.get('lon'))
                start, end = _parse(a.get('sc_st_dt')), _parse(a.get('sc_ed_dt'))
                if not title or not start or not end or end <= start:
                    continue
                poster = next((i.get('u') for i in pgm.get('ex_ia') or [] if i.get('u')), None)
                if title.strip().lower() == _NO_GUIDE_TITLE:
                    title = names.get(cs) or item['name']
                show, episode_title, season, episode = split_title(title)
                desc = _name(pgm.get('lod'))
                if desc and desc.strip().lower() in (title.lower(), show.lower()):
                    desc = ''
                programs.append(ProgramData(source_channel_id=cs, title=show[:512], start_time=start, end_time=end,
                                            episode_title=episode_title, season=season, episode=episode,
                                            description=desc or None, poster_url=poster,
                                            category=_category(item['name'])))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            raise StreamDeadError(f'Unsupported Gray stream URL: {raw_url}')
        cs = raw_url[len(SCHEME):]
        if not re.fullmatch(r'[a-z0-9_]+', cs):
            raise StreamDeadError(f'Bad Gray callSign: {cs}')
        return STREAM_URL.format(d=cs.replace('_', '-'))

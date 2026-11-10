"""Heartland+ (watchheartlandplus.com/live-tv): Get After It Media's free
live TV app: Heartland, The Doctors, Retro TV, Hasbro Legends, Unbeaten,
Rev'n, ~25 Stingray channels (music, Naturescape, Qello, Just for Laughs, ...)
and a dozen partner channels. No account.

The site is a Next.js front end on Revlet (YuppTV's OTT platform, tenant
"lukentv"):
  /get/token?tenant_code=lukentv&box_id=<uuid>&device_id=5&...  → guest sessionId
    (Revlet answers "Unmatched path" unless the request accepts gzip, which
    requests does by default, and unless the comma/slash in device_sub_type
    and timezone are left unencoded)
  every other call sends Session-Id, Box-Id and Tenant-Code headers;
  /page/content?path=live-tv&count=200     → the channel cards (name, live/play/<slug>, logo)
  /page/stream?path=live/play/<slug>       → streams[] (the network's own SSAI HLS:
                                             vgcdn, Stingray, MediaTailor, Amagi), fresh per tune
  /tvguide/channels + /static/tvguide?start_time&end_time&channel_ids  → ~3 days
                                             of listings for every channel in one call
  /system/config                           → image URL prefixes ("common,channel/logos/x.png")
Stream URLs come back with raw spaces in their ad parameters; they're
percent-encoded before a client sees them. All clear HLS, mostly 1080p.

Some of these channels are also in OG Network and Mansa; they're carried here
too, as Heartland+'s own feed. Checked 2026-11-10.
"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlencode

from ._common import Network, NetChannel, clean, from_epoch, program

_API = 'https://lukentv-api.revlet.net/service/api/v1'
_SITE = 'https://watchheartlandplus.com'
# Accept: application/json gets a 406 from tvguide/channels.
_HEADERS = {'Origin': _SITE, 'Referer': f'{_SITE}/', 'Accept': '*/*'}
_COMMON_PREFIX = 'https://d229kpbsb5jevy.cloudfront.net/lukentv/content/common/'
_GUIDE_DAYS = 3
_SESSION_MAX_AGE = 6 * 3600
# Gracenote IDs of the same stations on Sling/Fubo/Pluto (2026-11-10), only
# where one ID matched; the guide itself comes from Heartland+.
_GRACENOTE = {
    'heartland': '195569', 'unbeaten': '129048', 'zenlife-by-stingray': '146460',
    'qello-concerts-by-stingray': '113296', 'stingray-holidayscapes': '147936',
    'stingray-classic-rock': '67567', 'stingray-hot-country': '67557', 'stingray-remember-the-80s': '67566',
    'stingray-today-s-latin-pop': '68639', 'stingray-hit-list': '67555', 'stingray-easy-listening': '67573',
    'stingray-smooth-jazz': '67571', 'stingray-spa': '68632',
}
_SPANISH = {'stingray-romance-latino', 'stingray-today-s-latin-pop', 'box-cinema', 'galli-lucha-libre',
            'historias-de-coraz-n', 'hits-mexicanos', 'flou-detonacion'}
_CATEGORIES = {
    'heartland': 'Entertainment', 'the-doctor': 'Lifestyle', 'retrotv': 'Classic TV',
    'hasbro-legends': 'Kids', 'unbeaten': 'Sports', 'revn': 'Automotive', 'free-riding': 'Sports',
    'stingray-naturescape': 'Nature', 'stingray-holidayscapes': 'Lifestyle', 'zenlife-by-stingray': 'Lifestyle',
    'stingray-spa': 'Lifestyle', 'stingray-hooked': 'Outdoors', 'just-for-laughs-stand-up': 'Comedy',
    'galli-lucha-libre': 'Sports', 'fighttvplus': 'Sports', 'toonovation': 'Kids', 'old-west-tv': 'Westerns',
    'kung-fu-movies': 'Movies', 'cinepast': 'Movies', 'box-cinema': 'Movies', 'ampd-tv': 'Documentary',
}

_session_lock = threading.Lock()
_session: dict | None = None
_prefixes: dict[str, str] = {}


def _new_session(s) -> dict:
    box = str(uuid.uuid4())
    # Revlet rejects an encoded comma or slash here ("Unmatched path").
    query = urlencode({'tenant_code': 'lukentv', 'box_id': box, 'product': 'lukentv', 'device_id': '5',
                       'display_lang_code': 'ENG', 'device_sub_type': 'Chrome,130,Linux',
                       'timezone': 'America/New_York'}, safe=',/')
    r = s.session.get(f'{_API}/get/token?{query}', headers=_HEADERS, timeout=20)
    r.raise_for_status()
    sid = ((r.json() or {}).get('response') or {}).get('sessionId')
    if not sid:
        raise RuntimeError('Heartland+ gave no guest session')
    return {'Session-Id': sid, 'Box-Id': box, 'Tenant-Code': 'lukentv', 'at': time.time()}


def _headers(s, fresh: bool = False) -> dict:
    global _session
    with _session_lock:
        if fresh or not _session or time.time() - _session['at'] > _SESSION_MAX_AGE:
            _session = _new_session(s)
        return {**_HEADERS, **{k: v for k, v in _session.items() if k != 'at'}}


def _api(s, path: str, params: dict | None = None) -> dict:
    """GET an API path; a refused session gets one fresh retry."""
    for fresh in (False, True):
        r = s.session.get(f'{_API}/{path}', params=params, headers=_headers(s, fresh), timeout=30)
        body = r.json() if r.ok else {}
        if r.ok and body.get('status') is not False:
            return body.get('response') or {}
        code = ((body.get('error') or {}).get('code')) if isinstance(body, dict) else None
        if fresh or (r.status_code not in (401, 403) and code not in (401, 403)):
            raise RuntimeError(f'Heartland+ {path}: HTTP {r.status_code} {body.get("error") if body else ""}'.strip())
    raise RuntimeError(f'Heartland+ {path}: refused')


def _image(s, ref: str | None) -> str | None:
    """'common,channel/logos/x.png' → URL, via system/config's resourceProfiles."""
    if not ref:
        return None
    if ref.startswith('http'):
        return ref
    code, _, path = ref.partition(',')
    if not path:
        code, path = 'common', ref
    if not _prefixes:
        try:
            for p in _api(s, 'system/config').get('resourceProfiles') or []:
                if p.get('code') and p.get('urlPrefix'):
                    _prefixes[p['code']] = p['urlPrefix']
        except Exception:  # noqa: BLE001 — fall back to the known prefix
            pass
    prefix = _prefixes.get(code) or (_COMMON_PREFIX if code == 'common' else None)
    return f'{prefix}{path}' if prefix else None


def _slug(path: str) -> str:
    return path.rsplit('/', 1)[-1]


class Heartland(Network):
    key = 'heartland'
    name = 'Heartland+'

    def channels(self, s):
        page = _api(s, 'page/content', {'path': 'live-tv', 'count': 200})
        out, seen = [], set()
        for section in page.get('data') or []:
            for card in ((section.get('section') or {}).get('sectionData') or {}).get('data') or []:
                els = {e.get('key'): e.get('value') for e in (card.get('hover') or {}).get('elements') or []}
                path = els.get('targetPath') or (card.get('target') or {}).get('path') or ''
                name = clean(els.get('name') or (card.get('display') or {}).get('title'))
                if not path.startswith('live/play/') or not name or path in seen:
                    continue
                seen.add(path)
                slug = _slug(path)
                stingray_music = slug.startswith('stingray-') and slug not in _CATEGORIES
                out.append(NetChannel(
                    slug, name, path,
                    logo=_image(s, els.get('bgImage') or (card.get('display') or {}).get('imageUrl')),
                    category=_CATEGORIES.get(slug) or ('Music' if stingray_music or 'concert' in slug
                                                       or 'hits' in slug else 'Entertainment'),
                    language='es' if slug in _SPANISH else 'en',
                    gracenote_id=_GRACENOTE.get(slug),
                ))
        return out

    def programs(self, s, channels):
        wanted = {ch.stream: ch.id for ch in channels}
        guide_ids = {}
        for ch in (_api(s, 'tvguide/channels').get('data') or []):
            path = (ch.get('target') or {}).get('path')
            if path in wanted and ch.get('id') is not None:
                guide_ids[str(ch['id'])] = wanted[path]
        if not guide_ids:
            return []
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        start = int((now - timedelta(hours=3)).timestamp() * 1000)
        end = int((now + timedelta(days=_GUIDE_DAYS)).timestamp() * 1000)
        guide = _api(s, 'static/tvguide', {'start_time': start, 'end_time': end, 'page': 0,
                                           'channel_ids': ','.join(guide_ids)})
        out = []
        for row in guide.get('data') or []:
            cid = guide_ids.get(str(row.get('channelId')))
            if not cid:
                continue
            for prog in row.get('programs') or []:
                disp = prog.get('display') or {}
                marks = disp.get('markers') or {}
                attrs = (prog.get('target') or {}).get('pageAttributes') or {}
                p = program(cid, disp.get('title'),
                            from_epoch((marks.get('startTime') or {}).get('value') or attrs.get('startTime')),
                            from_epoch((marks.get('endTime') or {}).get('value') or attrs.get('endTime')),
                            description=disp.get('subtitle2') or None,
                            poster_url=_image(s, disp.get('imageUrl')),
                            category=(attrs.get('genre') or '').split(',')[0].strip() or None)
                if p:
                    out.append(p)
        return out

    def resolve(self, s, ref):
        body = _api(s, 'page/stream', {'path': ref})
        streams = body.get('streams') or []
        url = next((st.get('url') for st in streams if '.m3u8' in (st.get('url') or '')), None)
        if not url:
            raise RuntimeError(f'Heartland+ gave no HLS stream for {ref}')
        # Ad parameters arrive with raw spaces ("ads.stationName=Stingray Hooked").
        return quote(url, safe=":/?&=%#,;@+~!$'()*[]")

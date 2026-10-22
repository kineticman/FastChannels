from __future__ import annotations

import base64
import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone

from ..gracenote_map import resolve_gracenote
from .base import BaseScraper, ChannelData, ProgramData

logger = logging.getLogger(__name__)

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
ORIGIN = 'https://vix.com'
SCHEME = 'vix://'  # vix://<Lura video id>/<channel id>
GQL = 'https://client-api.vix.com/gql/v2'
ANON_USER = ORIGIN + '/api/proxy/user/create-anon-user'
VIDEO_TOKEN = ORIGIN + '/api/video/token'
LURA_PLAY = 'https://nxs.mp.lura.live/v1/play/{vid}'
# Any Lura license host; each tune's real, pre-signed URL comes from the play
# response and is what get_license_url() hands the license proxy.
LICENSE_BASE = 'https://drm3.mp.lura.live/fp'

_CATEGORIES_QUERY = 'query EpgData { epgCategories { edges { node { id title } } } }'
# upcomingSchedule tops out at 100 rows (3-4 days); the API rejects queries over
# its complexity budget, so this asks for no more than the guide needs.
_CHANNELS_QUERY = '''query EpgCategoryData($id: ID!, $count: Int!) {
  channelsByCategory(categoryId: $id) { edges { node {
    id title channelNumber description
    imageAssets { imageRole link }
    stream { mcpId }
    channelAvailability { isBlocked reason }
    upcomingSchedule(count: $count) { startDate endDate title subtitle }
  } } }
}'''
_SCHEDULE_COUNT = 100

_CATEGORY = {
    'noticias': 'News', 'deportes': 'Sports', 'humor': 'Comedy', 'cine': 'Movies',
    'novelas': 'Drama', 'docu': 'Documentary', 'series': 'Drama',
}

_SESSION_MARGIN = 300         # re-create the anonymous user this long before expiry
_VIDEO_TOKEN_MARGIN = 3600    # video tokens last ~24 h


class ViXScraper(BaseScraper):
    """
    ViX's free channels (vix.com/canales): ~80 Spanish-language channels —
    N+ Univision news (national and local), telenovela, movie, comedy and
    sports channels.

    No account: vix.com mints an anonymous user (create-anon-user) whose token
    opens the GraphQL guide API and the Lura (Anvato) player. All but one
    channel are Widevine DASH, so the source is bridge-only through
    FastChannels Player.

    resolve(): /api/video/token for the channel's Lura video id, then Lura's
    play call, which returns the DASH manifest and a pre-signed license URL
    (~3 h). The license needs no headers beyond the challenge; the proxy just
    forwards it to that tune's URL, cached per channel.
    """

    source_name = 'vix'
    display_name = 'ViX'
    source_category = 'fast'
    scrape_interval = 360
    stream_audit_enabled = True
    license_url = LICENSE_BASE
    all_channels_require_drm_bridge = True

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({'User-Agent': UA, 'Origin': ORIGIN, 'Referer': ORIGIN + '/'})
        self._nodes: dict[str, dict] | None = None

    # ── Required ─────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        nodes = self._channel_nodes()
        titles: dict[str, int] = {}
        for n in nodes.values():
            titles[n['title']] = titles.get(n['title'], 0) + 1
        out = []
        for cid, n in nodes.items():
            name = self._name(n, titles)
            desc = (n.get('description') or '').strip()
            out.append(ChannelData(
                source_channel_id=_short(cid),
                name=name,
                slug='vix-' + re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-'),
                stream_url=f'{SCHEME}{n["stream"]["mcpId"]}/{cid}',
                logo_url=self._image(n, 'CHANNEL_LOGO'),
                category=self._category(n),
                language='es',
                country='US',
                stream_type='dash',
                number=n.get('channelNumber'),
                guide_key=f'VIX:{_short(cid)}',
                description=desc if desc and desc.lower() != (n.get('title') or '').strip().lower() else None,
                gracenote_id=resolve_gracenote('vix', lookup_key=_short(cid)),
            ))
        logger.info('[%s] %d channels', self.source_name, len(out))
        return out

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        nodes = {_short(cid): n for cid, n in self._channel_nodes().items()}
        out = []
        for ch in channels:
            for row in (nodes.get(ch.source_channel_id) or {}).get('upcomingSchedule') or []:
                start, end = _parse(row.get('startDate')), _parse(row.get('endDate'))
                title = (row.get('title') or '').strip()
                if not start or not end or end <= start or not title:
                    continue
                out.append(ProgramData(
                    source_channel_id=ch.source_channel_id, title=title, start_time=start, end_time=end,
                    episode_title=(row.get('subtitle') or '').strip() or None,
                ))
        logger.info('[%s] %d EPG entries', self.source_name, len(out))
        return out

    def audit_resolve(self, raw_url: str) -> str:
        # Widevine everywhere, with the markers in the DASH manifest the audit
        # doesn't parse; confirm the play flow works and hand back the opaque
        # URL so the audit skips manifest inspection (as versant_tve does).
        if raw_url.startswith(SCHEME):
            self.resolve(raw_url)
        return raw_url

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            raise ValueError(f'Unsupported ViX stream URL: {raw_url}')
        vid, _, cid = raw_url[len(SCHEME):].partition('/')
        if not vid or not cid:
            raise ValueError(f'Unsupported ViX stream URL: {raw_url}')
        token = self._video_token(cid, vid)
        config = {'name': str(uuid.uuid4()), 'lura': {}}
        payload = base64.urlsafe_b64encode(json.dumps(config, separators=(',', ':')).encode()).decode().rstrip('=')
        r = self.session.post(LURA_PLAY.format(vid=vid), params={'guid': str(uuid.uuid4())},
                              data={'token': f'{token}.{payload}'}, timeout=20)
        r.raise_for_status()
        media = ((r.json() or {}).get('content') or {}).get('media') or []
        dash = next((m for m in media if 'dash' in (m.get('type') or '')), None)
        if not dash or not dash.get('url'):
            raise RuntimeError(f'ViX: no DASH stream returned for {cid}')
        playback = dict(self.cache.get('vix_playback') or {})
        playback[_short(cid)] = {'license_url': dash.get('licenseUrl'), 'at': time.time()}
        self._update_cache('vix_playback', playback)
        return dash['url']

    # ── DRM license proxy wiring ─────────────────────────────

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        # The license proxy merges source_cache into config, so a tune's
        # pre-signed URL is here once resolve() has run for it.
        cached = ((config.get('vix_playback') or {}).get(channel_id or '') or {}).get('license_url')
        return cached or cls.license_url

    @classmethod
    def prepare_license_request(cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs):
        return challenge, {'Content-Type': 'application/octet-stream', 'Origin': ORIGIN, 'Referer': ORIGIN + '/'}

    # ── Internals ────────────────────────────────────────────

    def _anon(self, force: bool = False) -> dict:
        """The anonymous user's tokens (~2 h), kept in the source cache."""
        sess = self.cache.get('vix_session') or {}
        if not force and sess.get('access_token') and float(sess.get('exp') or 0) - time.time() > _SESSION_MARGIN:
            return sess
        install = str(uuid.uuid4())
        r = self.session.post(ANON_USER, json={'installationId': install}, timeout=20)
        r.raise_for_status()
        tok = r.json()
        sess = {
            'access_token': tok['accessToken'], 'user_token': tok.get('userToken') or '',
            'install_id': install,
            'install_date': datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
            'exp': time.time() + float(tok.get('expiresIn') or 7200),
        }
        self._update_cache('vix_session', sess)
        return sess

    def _gql(self, query: str, variables: dict | None = None, op: str | None = None) -> dict:
        for attempt in (0, 1):
            sess = self._anon(force=bool(attempt))
            r = self.session.post(GQL, json={'operationName': op, 'query': query, 'variables': variables or {}}, timeout=30,
                                  headers={'Authorization': f'Bearer {sess["access_token"]}',
                                           'x-vix-user-token': sess['user_token'],
                                           'x-vix-install-id': sess['install_id'],
                                           'x-vix-install-date': sess['install_date'],
                                           'x-vix-platform': 'web', 'x-vix-device-type': 'desktop',
                                           'x-vix-app-version': 'v2.32.0'})
            if r.status_code in (401, 403) and not attempt:
                continue
            r.raise_for_status()
            body = r.json() or {}
            if body.get('errors') and not body.get('data'):
                raise RuntimeError(f'ViX GraphQL {op}: {body["errors"][0].get("message")}')
            return body.get('data') or {}
        return {}

    def _channel_nodes(self) -> dict[str, dict]:
        """Every channel once, keyed by its id, with the ViX category it was
        first seen under (Top is a highlights shelf, so it's read last)."""
        if self._nodes is not None:
            return self._nodes
        cats = [e['node'] for e in (self._gql(_CATEGORIES_QUERY, op='EpgData').get('epgCategories') or {}).get('edges') or []]
        cats.sort(key=lambda c: (c.get('title') or '').lower() == 'top')
        nodes: dict[str, dict] = {}
        for cat in cats:
            data = self._gql(_CHANNELS_QUERY, {'id': cat['id'], 'count': _SCHEDULE_COUNT}, op='EpgCategoryData')
            for edge in (data.get('channelsByCategory') or {}).get('edges') or []:
                n = edge.get('node') or {}
                if not n.get('id') or not (n.get('stream') or {}).get('mcpId'):
                    continue
                if (n.get('channelAvailability') or {}).get('isBlocked'):
                    continue
                nodes.setdefault(n['id'], {**n, 'vix_category': cat.get('title') or ''})
        self._nodes = nodes
        return nodes

    def _video_token(self, cid: str, vid: str) -> str:
        tokens = self.cache.get('vix_video_tokens') or {}
        cached = tokens.get(vid) or {}
        if cached.get('token') and float(cached.get('exp') or 0) - time.time() > _VIDEO_TOKEN_MARGIN:
            return cached['token']
        for attempt in (0, 1):
            sess = self._anon(force=bool(attempt))
            # Without the three player headers the endpoint 400s
            # ("unable to generate video token").
            r = self.session.get(VIDEO_TOKEN, params={'videoId': vid, 'timestamp': int(time.time() * 1000), 'contentId': cid},
                                 headers={'Authorization': f'Bearer {sess["access_token"]}', 'Referer': ORIGIN + '/es-es/canales',
                                          'x-player-version': 'AnvatoPlayer', 'x-video-type': 'Livestream',
                                          'x-video-pod-serve': 'ULTRON'}, timeout=20)
            if r.status_code in (401, 403) and not attempt:
                continue
            r.raise_for_status()
            token = (r.json() or {}).get('token')
            if not token:
                raise RuntimeError(f'ViX: no video token for {cid}')
            exp = _jwt_exp(token) or time.time() + 6 * 3600
            self._update_cache('vix_video_tokens', {**tokens, vid: {'token': token, 'exp': exp}})
            return token
        raise RuntimeError(f'ViX: video token refused for {cid}')

    @staticmethod
    def _image(node: dict, role: str) -> str | None:
        return next((i.get('link') for i in node.get('imageAssets') or [] if i.get('imageRole') == role and i.get('link')), None)

    @staticmethod
    def _name(node: dict, titles: dict[str, int]) -> str:
        """ViX lists its Miami and Orlando local news under the national
        "N+ Univision 24/7" title; only their descriptions ("... - Miami")
        name the city, so a shared title gets the city appended."""
        title = (node.get('title') or '').strip()
        desc = (node.get('description') or '').strip()
        if titles.get(node.get('title'), 0) > 1 and ' - ' in desc and len(desc) <= 60:
            return f"{title} – {desc.rsplit(' - ', 1)[1].strip()}"
        return title

    @staticmethod
    def _category(node: dict) -> str | None:
        title = (node.get('title') or '').lower()
        if title.startswith(('n+', 'noticias', 'rcn noticias')) or 'news' in title:
            return 'News'
        return _CATEGORY.get((node.get('vix_category') or '').lower())


def _short(cid: str) -> str:
    """'channel:mcp:callsign:Z224' -> 'Z224' (the FastChannels channel id)."""
    return cid.rsplit(':', 1)[-1]


def _parse(val) -> datetime | None:
    if not val:
        return None
    try:
        return datetime.fromisoformat(str(val).replace('Z', '+00:00'))
    except ValueError:
        return None


def _jwt_exp(token: str) -> float | None:
    try:
        part = token.split('.')[1]
        return float(json.loads(base64.urlsafe_b64decode(part + '=' * (-len(part) % 4))).get('exp'))
    except (IndexError, ValueError, TypeError):
        return None

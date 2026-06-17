from __future__ import annotations

import base64
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from .base import (
    BaseScraper,
    ChannelData,
    ConfigField,
    ProgramData,
    ScrapeSkipError,
    StreamDeadError,
    infer_language_from_metadata,
)
from .category_utils import category_for_channel

logger = logging.getLogger(__name__)

# FreeCast web app (watch.freecast.com) is a React SPA backed by REST APIs at
# api-services.freecast.com.  The channel list, categories, and EPG are all
# anonymous; only stream resolution requires a (free) registered-account token.
#
# Streams are clean HLS (H.264/AAC, no DRM) on the free tier.  Each free channel
# is an Amagi FAST channel wrapped in IndiCue server-side ad insertion — the
# stream endpoint returns the playable IndiCue manifest URL.
#
# The paid "value" package (~18 channels) is Widevine/FairPlay DRM and is not
# scraped.
_API = 'https://api-services.freecast.com'
_BRAND = 'watch-freecast-com'
_LIVE_BASE = f'{_API}/live/api/v8/{_BRAND}/web'
_LOGIN_URL = f'{_API}/auth/api/v4/{_BRAND}/web/jwt/'
_REFRESH_URL = f'{_API}/auth/api/v3/{_BRAND}/web/jwt/refresh/'
_PACKAGE = 'free'

_EPG_CHUNK = 40         # channel slugs per EPG request
_EPG_WORKERS = 4        # concurrent EPG chunk fetches
_TOKEN_MARGIN = 600     # refresh access token this many seconds before expiry

# FreeCast's channel `category_slug` values don't match the categories endpoint
# slugs, so map them to readable raw categories that category_utils understands.
# Value is the raw category string handed to category_for_channel() (which then
# normalises + applies name overrides). Unknown slugs fall through to name-based
# categorisation.
_CATEGORY_SLUGS = {
    'news': 'News',
    'sports': 'Sports',
    'tv': 'Entertainment',
    'lifestyle': 'Lifestyle',
    'international': 'International',
    'movies': 'Movies',
    'caribbean': 'International',
    'spiritual': 'Faith',
    'faith-featured': 'Faith',
    'kids': 'Kids',
    'music': 'Music',
    'shopping': 'Shopping',
    'local': 'Local News',
    # FreeCast's Spanish-language package; the project convention routes
    # Spanish-language channels to the Latino category.
    'mis-canales': 'Latino',
    'espanol': 'Latino',
}

# Category slugs whose channels are Spanish-language regardless of name.
_SPANISH_SLUGS = {'mis-canales', 'espanol'}


class FreecastScraper(BaseScraper):
    """
    Scraper for FreeCast (watch.freecast.com) — free, ad-supported live TV.

    Requires a free FreeCast account: the channel list and EPG are anonymous,
    but the per-channel stream endpoint is gated behind a logged-in JWT.  The
    access token (14-day) and refresh token (30-day) are cached in source config
    and rotated automatically.

    Only the free package is scraped; the paid "value" package is DRM-protected.

    Note: a large share of FreeCast's free lineup is Amagi-sourced FAST channels
    and Local Now local-market feeds, so expect significant overlap with other
    sources (e.g. Stirr, Local Now). Use Resolve Duplicates / feeds to manage it.
    """

    source_name = 'freecast'
    display_name = 'FreeCast'
    scrape_interval = 360
    stream_audit_enabled = True
    audit_requires_config = ['username', 'password']
    config_required = True
    is_premium = False
    source_category = 'fast'

    config_schema = [
        ConfigField('username', 'Username / Email', required=True,
                    placeholder='you@example.com',
                    help_text='Your FreeCast login email. A free account at watch.freecast.com is required for playback.'),
        ConfigField('password', 'Password', field_type='password', required=True,
                    secret=True, help_text='Your FreeCast password.'),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36',
            'Origin': 'https://watch.freecast.com',
            'Referer': 'https://watch.freecast.com/',
        })

    # ── Auth ────────────────────────────────────────────────────────────────

    def _client_id(self) -> str:
        cid = self.config.get('client_id')
        if not cid:
            cid = str(uuid.uuid4())
            self._update_config('client_id', cid)
        return cid

    @staticmethod
    def _jwt_exp(token: str) -> int:
        """Return the exp claim (epoch seconds) of a JWT, or 0 if unreadable."""
        try:
            import json
            payload = token.split('.')[1]
            payload += '=' * (-len(payload) % 4)
            return int(json.loads(base64.urlsafe_b64decode(payload)).get('exp', 0))
        except Exception:
            return 0

    def _login(self) -> None:
        username = (self.config.get('username') or '').strip()
        password = (self.config.get('password') or '').strip()
        if not username or not password:
            raise ScrapeSkipError('FreeCast requires a username and password — configure credentials in Sources')

        payload = {
            'username': username,
            'password': password,
            'device_name': 'FastChannels on Web',
            'client_id': self._client_id(),
        }
        r = self.session.post(_LOGIN_URL, json=payload, timeout=20)
        if r.status_code in (400, 401, 403, 422):
            raise RuntimeError(f'FreeCast login failed (HTTP {r.status_code}): check credentials')
        r.raise_for_status()
        data = r.json()
        access = data.get('access')
        refresh = data.get('refresh')
        if not access:
            raise RuntimeError('FreeCast login returned no access token')
        self._store_tokens(access, refresh)
        logger.info('[freecast] logged in successfully')

    def _refresh(self) -> bool:
        """Try to mint a fresh access token from the refresh token. Returns success."""
        refresh = self.config.get('refresh_token')
        if not refresh or self._jwt_exp(refresh) <= time.time() + _TOKEN_MARGIN:
            return False
        try:
            r = self.session.post(_REFRESH_URL, json={'refresh': refresh}, timeout=20)
            if not r.ok:
                return False
            data = r.json()
            access = data.get('access')
            if not access:
                return False
            # Refresh rotates both tokens; keep the new refresh if present.
            self._store_tokens(access, data.get('refresh') or refresh)
            logger.debug('[freecast] refreshed access token')
            return True
        except Exception as exc:
            logger.debug('[freecast] token refresh failed: %s', exc)
            return False

    def _store_tokens(self, access: str, refresh: str | None) -> None:
        self._update_config('access_token', access)
        if refresh:
            self._update_config('refresh_token', refresh)
        self.session.headers['Authorization'] = f'Bearer {access}'

    def _ensure_auth(self) -> None:
        """Make sure self.session carries a valid Bearer token, logging in if needed."""
        access = self.config.get('access_token')
        if access and self._jwt_exp(access) > time.time() + _TOKEN_MARGIN:
            self.session.headers['Authorization'] = f'Bearer {access}'
            return
        if self._refresh():
            return
        self._login()

    def pre_run_setup(self) -> None:
        # Authenticate if credentials are present so the audit/resolve path is
        # warm and tokens are persisted before the long scrape. Channel/EPG
        # fetches are anonymous, so missing credentials must not abort the run.
        try:
            self._ensure_auth()
        except ScrapeSkipError:
            logger.info('[freecast] no credentials configured — catalog will scrape but playback needs login')
        except Exception as exc:
            logger.warning('[freecast] auth setup failed (continuing anonymously for catalog): %s', exc)

    # ── API helpers ──────────────────────────────────────────────────────────

    def _get_json(self, url: str, params: dict | None = None) -> Any:
        r = self.session.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def _anon_session(self):
        """A session without the Authorization header.

        The authenticated channel list is a personalised (and smaller) lineup
        that hides the Local Now local-market feeds; the anonymous list is the
        full, playable catalogue. Channel list, categories, and EPG are all
        anonymous endpoints, so strip the bearer token for them.
        """
        if getattr(self, '_anon', None) is None:
            self._anon = self.new_session(headers={
                k: v for k, v in self.session.headers.items() if k.lower() != 'authorization'
            })
        return self._anon

    def _get_json_anon(self, url: str, params=None) -> Any:
        r = self._anon_session().get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    # ── fetch_channels ────────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        rows = self._get_json_anon(f'{_LIVE_BASE}/packages/{_PACKAGE}/channels/')
        if not isinstance(rows, list):
            raise RuntimeError('FreeCast channel list returned unexpected payload')

        try:
            from app.gracenote_map import resolve_gracenote
        except ImportError:
            resolve_gracenote = None

        channels: list[ChannelData] = []
        for row in rows:
            slug = row.get('slug')
            name = (row.get('name') or '').strip()
            if not slug or not name:
                continue
            if (row.get('type') or '').lower() not in ('live/video', ''):
                continue

            cat_slug = row.get('category_slug')
            raw_cat = _CATEGORY_SLUGS.get(cat_slug, cat_slug)
            category = category_for_channel(name, raw_cat, self.source_name)

            if cat_slug in _SPANISH_SLUGS:
                language = 'es'
            else:
                language = infer_language_from_metadata(name)

            number = None
            lcn = row.get('lcn')
            if lcn and str(lcn).isdigit():
                number = int(lcn)

            gracenote_id = (resolve_gracenote('freecast', lookup_key=slug)
                            if resolve_gracenote else None)

            channels.append(ChannelData(
                source_channel_id=slug,
                name=name,
                stream_url=f'freecast://{_PACKAGE}/{slug}',
                logo_url=row.get('logo') or row.get('image'),
                slug=slug,
                category=category,
                language=language,
                country='US',
                stream_type='hls',
                number=number,
                gracenote_id=gracenote_id,
                description=(row.get('description') or '').strip() or None,
            ))

        channels.sort(key=lambda c: c.name.lower())
        logger.info('[freecast] %d channels', len(channels))
        return channels

    # ── fetch_epg ─────────────────────────────────────────────────────────────

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        slugs = [c.source_channel_id for c in channels if c.source_channel_id]
        chunks = [slugs[i:i + _EPG_CHUNK] for i in range(0, len(slugs), _EPG_CHUNK)]
        programs: list[ProgramData] = []
        done = 0

        # The EPG endpoint is anonymous; fetch chunks concurrently with an
        # auth-free session so a stale Authorization header can't 401 us.
        session = self._anon_session()

        def fetch_chunk(chunk: list[str]) -> list[ProgramData]:
            params = [('slug', s) for s in chunk]
            try:
                r = session.get(f'{_LIVE_BASE}/packages/{_PACKAGE}/epgs/',
                                params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
            except Exception as exc:
                logger.warning('[freecast] EPG chunk fetch failed: %s', exc)
                return []
            out: list[ProgramData] = []
            for entry in data if isinstance(data, list) else []:
                ch_slug = entry.get('slug')
                if not ch_slug:
                    continue
                for prog in entry.get('epg_programs') or []:
                    p = self._parse_program(ch_slug, prog)
                    if p is not None:
                        out.append(p)
            return out

        with ThreadPoolExecutor(max_workers=_EPG_WORKERS) as pool:
            futures = {pool.submit(fetch_chunk, c): c for c in chunks}
            for future in as_completed(futures):
                programs.extend(future.result())
                done += 1
                if self._progress_cb:
                    self._progress_cb('epg', done, len(chunks))

        logger.info('[freecast] %d EPG entries across %d channels', len(programs), len(slugs))
        return programs

    @staticmethod
    def _parse_time(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)
        except (ValueError, TypeError):
            return None

    def _parse_program(self, ch_slug: str, prog: dict) -> ProgramData | None:
        title = (prog.get('title') or '').strip()
        start = self._parse_time(prog.get('start_time'))
        end = self._parse_time(prog.get('end_time'))
        if not title or not start or not end:
            return None
        subtitle = (prog.get('subtitle') or '').strip() or None
        rating = ((prog.get('metadata') or {}).get('rating') or '').strip() or None
        return ProgramData(
            source_channel_id=ch_slug,
            title=title,
            start_time=start,
            end_time=end,
            description=(prog.get('description') or '').strip() or None,
            poster_url=prog.get('thumbnail') or None,
            rating=rating,
            episode_title=subtitle,
            episode_id=prog.get('uuid') or None,
        )

    # ── resolve ────────────────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith('freecast://'):
            return raw_url
        rest = raw_url[len('freecast://'):]
        package, _, slug = rest.partition('/')
        slug = slug.strip('/')
        if not package or not slug:
            raise RuntimeError(f'Invalid freecast URL: {raw_url}')

        self._ensure_auth()
        data = self._get_json(f'{_LIVE_BASE}/packages/{package}/channels/{slug}/streams/')
        streams = data.get('streams') if isinstance(data, dict) else None
        if not streams:
            # config.url is the fallback master the player uses when streams is empty.
            cfg_url = (data.get('config') or {}).get('url') if isinstance(data, dict) else None
            if cfg_url:
                return cfg_url
            raise StreamDeadError(f'FreeCast channel {slug} returned no streams')

        stream = streams[0]
        url = stream.get('data') or stream.get('url')
        if not url:
            raise StreamDeadError(f'FreeCast channel {slug} stream has no playable URL')
        return url

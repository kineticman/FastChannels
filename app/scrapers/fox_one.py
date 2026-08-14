from __future__ import annotations

import base64
import gzip
import json
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

from .base import BaseScraper, ChannelData, ConfigField, ProgramData
from .fox_tve import FoxTVEScraper, CHANNELS as FOX_TVE_CHANNELS, _cox_saml_login, _jwt_exp

_SCHEME = 'fox-one://'
_API_BASE = 'https://api.fox.com/dtc'
_LIVE_PAGE = '/product/page/v1/landing/653897111489?page=1&size=25'
_API_KEY = 'sVMTVogE67faKqBLOlZBVoYuay2FquzW'
_PLAYBACK_API = 'https://prod.api.digitalvideoplatform.com/foxdtc/v3.0/watchlive'
_ID_BASE = 'https://id.fox.com'
_HYDRA_TOKEN_API = f'{_ID_BASE}/identityhydra/oauth2/token'
_HYDRA_CLIENT_ID = '21af937a-0ed4-4321-b87f-51d3b93976d4'
# Platform-level API Gateway key fronting id.fox.com's mvpd/regcode endpoints; distinct
# from _API_KEY, which identifies the FOX One client app itself (sent alongside it).
_PLATFORM_API_KEY = '049f8b7844b84b9cb5f830f28f08648c'
_ENT_BASE = 'https://ent.fox.com'
_ADOBE_AUTHENTICATE_HOST = 'api.auth.adobe.com'
_TOKEN_REFRESH_SKEW = 300
_LOCATION_TTL = 6 * 60 * 60
_ENTITLEMENTS_TTL = 6 * 60 * 60
_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'


@dataclass(frozen=True)
class FoxOneChannel:
    source_channel_id: str
    name: str
    container_id: str
    call_sign: str
    target_fox_tve_id: str | None
    logo_url: str | None
    category: str
    gracenote_id: str | None = None

    @property
    def stream_url(self) -> str:
        target = self.target_fox_tve_id or '-'
        return f'{_SCHEME}{self.source_channel_id}/{target}/{self.container_id}'


_SUPPORTED_BY_CALL_SIGN: dict[str, tuple[str, str, str]] = {
    'FOX': ('fox_sports_fox', 'FOX', 'Sports'),
    'FS1': ('fox_sports_fs1', 'FS1', 'Sports'),
    'FS2': ('fox_sports_fs2', 'FS2', 'Sports'),
    'BTN': ('fox_sports_btn', 'Big Ten Network', 'Sports'),
    'FOXD': ('fox_deportes', 'FOX Deportes', 'Sports'),
    'FWX': ('fox_weather', 'Fox Weather', 'News'),
    'FBN': ('fox_business', 'Fox Business Network', 'Business'),
    'FNC': ('fox_news', 'Fox News Channel', 'News'),
}

_TITLE_TO_CALL_SIGN = {
    'B1G': 'BTN',
    'B10': 'BTN',
    'FOXD': 'FOXD',
    'FWX': 'FWX',
    'FBN': 'FBN',
    'FNC': 'FNC',
}

_CHANNEL_OVERRIDES: dict[str, tuple[str, str]] = {
    'FSD': ('FOX Sports Digital', 'Sports'),
    'LIVENOW': ('LiveNOW from FOX', 'News'),
    'SOUL': ('FOX Soul', 'Entertainment'),
    'TMZ': ('TMZ', 'Entertainment'),
    'TMZFO': ('TMZ', 'Entertainment'),
    'FXNA': ('FOX Nation', 'Entertainment'),
    'FXNB': ('FOX Nation Binge', 'Entertainment'),
    'FXN1': ('FOX Nation 1', 'Entertainment'),
    'FXN2': ('FOX Nation 2', 'Entertainment'),
}

# FOX's own API gives this call sign no real identity (network: "FOX Digital",
# station_name: null) — it's a rotating stunt/block channel, not a fixed
# network. Content rotates independently of the name (confirmed live
# 2026-08-10: labeled "FOX Movies" here while actually airing a Masked Singer
# marathon — see community report). No reliable name AND no real Gracenote ID
# (FOX's API literally returns the placeholder "TBD" for it), so it's dropped
# rather than guessed at.
_EXCLUDED_CALL_SIGNS = {'FMSC', 'FMSCFO'}


def _headers() -> dict[str, str]:
    return {
        'User-Agent': _UA,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Origin': 'https://www.fox.com',
        'Referer': 'https://www.fox.com/',
        'x-fox-apikey': _API_KEY,
        'x-fox-client': 'app_version:3.61.0;os:Windows;os_version:19.0.0;platform:web',
    }


def _slug(value: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', value.lower()).strip('-')


def _decode_location_part(value: str) -> dict:
    """One comma-separated half of the opaque x-platform-location blob is
    itself base64 JSON (see _ensure_platform_location) — decode it back out
    to pull the plain zip/DMA values location-gated content APIs want as
    separate headers (see _location_headers)."""
    try:
        padded = value + '=' * (-len(value) % 4)
        data = json.loads(base64.b64decode(padded))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def _parse_date(value: str | None):
    dt = _parse_time(value)
    return dt.date() if dt else None


def _image(images: dict | None, *keys: str) -> str | None:
    if not isinstance(images, dict):
        return None
    for key in keys:
        url = images.get(key)
        if isinstance(url, str) and url:
            return url
    return None


def _genre(item: dict, fallback: str) -> str:
    for genre in item.get('genre_metadata') or []:
        if isinstance(genre, dict) and genre.get('genre_type') == 'primary' and genre.get('display_name'):
            return genre['display_name']
    if item.get('is_sportingevent'):
        return 'Sports'
    return fallback


def _stream_parts(raw_url: str) -> tuple[str, str | None, str | None]:
    if not raw_url.startswith(_SCHEME):
        raise ValueError(f'Unsupported FOX One stream URL: {raw_url}')
    parsed = urlparse(raw_url)
    parts = [part for part in parsed.path.split('/') if part]
    target = parts[0] if parts and parts[0] != '-' else None
    container_id = parts[1] if len(parts) > 1 else None
    return parsed.netloc, target, container_id


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class FoxOneScraper(BaseScraper):
    source_name = 'fox_one'
    display_name = 'FOX One'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True
    # home_zip_code is NOT here — it moved to the shared TVEAccount config
    # (Settings > TV Everywhere) so it's entered once and available to any
    # TVE source that needs a home market, not just FOX One. See
    # _shared_home_zip_code()/_persist_shared_home_zip_code().
    config_schema = [
        ConfigField(
            'refresh_token',
            'FOX One refresh token (fallback)',
            field_type='password',
            secret=True,
            hidden=True,
            help_text='Optional fallback used only if no linked TV-provider (MVPD) account is configured, or MVPD re-auth fails. Capture the identityhydra refresh_token from an authenticated fox.com browser session. Anonymous-only: cannot unlock paid FOX Sports channels.',
        ),
        ConfigField(
            'access_token',
            'FOX One access token (advanced)',
            field_type='password',
            secret=True,
            hidden=True,
            placeholder='Bearer ...',
            help_text='Optional fallback for debugging. The scraper refreshes this automatically through the linked TV-provider (MVPD) account when configured.',
        ),
        ConfigField(
            'platform_location',
            'FOX One platform location (advanced)',
            field_type='password',
            secret=True,
            hidden=True,
            help_text='Optional fallback for debugging. The scraper derives and refreshes this automatically from FOX location services.',
        ),
    ]

    def _location_headers(self) -> dict[str, str]:
        """Extra headers FOX's real web client sends on every product/curated
        content call, confirmed via a real-browser HAR capture
        (dev/foxone/4.har): without these, the two D2C_LR_LINEAR_GUIDE
        containers on the live landing page (the local-affiliate slot) come
        back with item_count: 0 even though they're listed — the anonymous
        _headers() alone only gets you the fixed national lineup. Distinct
        from the opaque x-platform-location value _dtc_headers() uses for
        *playback* — this API wants the zip/DMA split into their own plain
        headers, plus a bearer token under a different header name
        (x-fox-userauth, not x-access-token).

        x-fox-zipcode/x-fox-dma must be the ACCOUNT'S HOME location (the
        home_zip_code config, via TVEAccount), not the caller's IP-geolocated
        "current" location — confirmed live 2026-08-12: sending the IP-geo
        zip/DMA there populated whichever market this server's own outbound
        IP happens to geolocate to (Ohio, in this box's case) and left the
        SECOND local-guide container empty; sending the account's actual home
        zip/DMA in all four fields populated BOTH containers with that home
        market's real local stations instead. This is what makes
        home_zip_code do something real rather than being a red herring next
        to server-side IP geolocation.

        Best-effort: swallows any auth/location failure and returns {} so an
        install with no TV-provider (MVPD) account configured still gets the national
        lineup exactly as before, just without the location-gated local
        channel.
        """
        try:
            access_token = self._ensure_access_token()
            combined = self._ensure_platform_location(access_token)
        except Exception:
            return {}
        parts = combined.split(',')
        home = _decode_location_part(parts[1]) if len(parts) > 1 else {}
        headers = {'x-fox-userauth': f'Bearer {access_token}'}
        if home.get('home_zip_code'):
            headers['x-fox-zipcode'] = str(home['home_zip_code'])
            headers['x-fox-home-zipcode'] = str(home['home_zip_code'])
        if home.get('home_metro_code'):
            headers['x-fox-dma'] = str(home['home_metro_code'])
            headers['x-fox-home-dma'] = str(home['home_metro_code'])
        try:
            entitlements = self._ensure_entitlements(access_token)
        except Exception:
            entitlements = ''
        if entitlements:
            headers['x-fox-content-entitlement'] = base64.b64encode(gzip.compress(entitlements.encode())).decode()
        return headers

    def _get_json(self, path_or_url: str) -> dict:
        if path_or_url.startswith('http'):
            url = path_or_url
        else:
            url = f'{_API_BASE}/{path_or_url.lstrip("/")}'
        headers = {**_headers(), **self._location_headers()}
        r = self.session.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data.get('status'):
            error = data.get('error') or {}
            raise RuntimeError(error.get('err_msg') or error.get('err_str') or f'FOX One API failed for {url}')
        return data.get('data') or {}

    def _container_page(self, uri: str) -> dict:
        return self._get_json(uri)

    def _discover_channels(self) -> list[FoxOneChannel]:
        page = self._get_json(_LIVE_PAGE)
        channels: list[FoxOneChannel] = []
        seen: set[str] = set()

        for container in page.get('containers') or []:
            if not isinstance(container, dict):
                continue
            uri = container.get('uri')
            container_id = str(container.get('container_id') or '')
            if not uri or not container_id:
                continue

            detail = self._container_page(uri)
            items = detail.get('items') or []
            first = next((item for item in items if isinstance(item, dict)), {})
            call_sign = str(first.get('call_sign') or _TITLE_TO_CALL_SIGN.get(str(container.get('title') or ''), '')).upper()
            if not call_sign:
                network = str(first.get('network') or '').upper()
                if network == 'FOX':
                    call_sign = 'FOX'
            if not call_sign or call_sign in seen:
                continue
            if call_sign in _EXCLUDED_CALL_SIGNS:
                seen.add(call_sign)
                continue

            supported = _SUPPORTED_BY_CALL_SIGN.get(call_sign)
            override = _CHANNEL_OVERRIDES.get(call_sign)
            target_id = supported[0] if supported else None
            fallback_name = (supported[1] if supported else None) or (override[0] if override else call_sign)
            category = (supported[2] if supported else None) or (override[1] if override else 'Entertainment')
            if override:
                name = override[0]
            elif supported:
                name = str(first.get('network') or first.get('station_name') or fallback_name)
            else:
                # Unmapped call signs are local-affiliate/sister stations discovered
                # via the geo-personalized live guide (see _location_headers) —
                # their item's `network` field is just the generic literal
                # "FOX"/"MNTV", not a real station name, so skip straight to the
                # call sign instead of that unhelpful generic value.
                name = str(first.get('station_name') or fallback_name)
            logo = (
                _image(first.get('networks_info', [{}])[0].get('images') if first.get('networks_info') else None, 'primary', 'secondary')
                or first.get('station_affiliate_logo')
                or _image(detail.get('images'), 'primary')
                or _image(container.get('images'), 'primary')
                or (FOX_TVE_CHANNELS[target_id].logo_url if target_id in FOX_TVE_CHANNELS else None)
            )
            gracenote = first.get('gracenote') if isinstance(first.get('gracenote'), dict) else {}
            # FOX's API returns the literal placeholder string "TBD" for
            # station_id on channels it hasn't assigned a real Gracenote ID
            # to yet (confirmed live 2026-08-10) — stored as-is that becomes a
            # bogus gracenote_id that breaks Channels DVR guide matching once
            # routed through the Gracenote-variant output.
            raw_station_id = str(gracenote.get('station_id') or '').strip()
            gracenote_id = raw_station_id if raw_station_id and raw_station_id.upper() != 'TBD' else None
            channels.append(FoxOneChannel(
                source_channel_id=call_sign.lower(),
                name=name,
                container_id=container_id,
                call_sign=call_sign,
                target_fox_tve_id=target_id,
                logo_url=logo,
                category=category,
                gracenote_id=gracenote_id,
            ))
            seen.add(call_sign)
        return channels

    def fetch_channels(self):
        return [
            ChannelData(
                source_channel_id=channel.source_channel_id,
                name=channel.name,
                slug=f'fox-one-{_slug(channel.name)}',
                logo_url=channel.logo_url,
                stream_url=channel.stream_url,
                stream_type='hls',
                category=channel.category,
                language='en',
                country='US',
                gracenote_id=channel.gracenote_id,
                guide_key=f'FOXONE:{channel.source_channel_id.upper()}',
                description=None,
            )
            for channel in self._discover_channels()
        ]

    def fetch_epg(self, channels, **kwargs):
        by_id = {channel.source_channel_id: channel for channel in self._discover_channels()}
        wanted = {ch.source_channel_id for ch in channels}
        programs: list[ProgramData] = []

        for source_channel_id in wanted:
            channel = by_id.get(source_channel_id)
            if not channel:
                continue
            uri = f'/product/curated/container/v1/live-geo/detail/{channel.container_id}?page=1&size=15'
            for _ in range(30):
                data = self._container_page(uri)
                for item in data.get('items') or []:
                    if not isinstance(item, dict):
                        continue
                    start = _parse_time(item.get('start_time'))
                    end = _parse_time(item.get('end_time'))
                    if not start or not end or end <= start:
                        continue
                    images = item.get('images') if isinstance(item.get('images'), dict) else {}
                    programs.append(ProgramData(
                        source_channel_id=source_channel_id,
                        title=item.get('title') or channel.name,
                        description=item.get('description'),
                        start_time=start,
                        end_time=end,
                        poster_url=_image(images, 'still', 'video_list', 'series_list', 'series_detail', 'dcg_mark_poster'),
                        category=_genre(item, channel.category),
                        rating=(item.get('ratings') or [{}])[0].get('rating') if item.get('ratings') else None,
                        season=item.get('season_number') or None,
                        episode=item.get('episode_number') or None,
                        original_air_date=_parse_date(item.get('first_release_date')),
                        is_live=item.get('airing_type') == 'live',
                        series_id=item.get('series_id'),
                        episode_id=item.get('entity_id') or item.get('tms_id') or item.get('foxipedia_id'),
                    ))
                next_uri = ((data.get('scroll') or {}).get('next') or '').replace('\\u0026', '&')
                if not next_uri or next_uri == uri:
                    break
                uri = next_uri
        return programs

    def _current_asset_id(self, container_id: str | None) -> str | None:
        if not container_id:
            return None
        data = self._container_page(f'/product/curated/container/v1/live-geo/detail/{container_id}?page=1&size=15')
        now = _now_utc()
        fallback = None
        for item in data.get('items') or []:
            if not isinstance(item, dict):
                continue
            asset_id = item.get('entity_id')
            if not asset_id:
                continue
            fallback = fallback or asset_id
            start = _parse_time(item.get('start_time'))
            end = _parse_time(item.get('end_time'))
            if start and end and start <= now < end:
                return asset_id
        return fallback

    def _clean_token(self, value: str | None) -> str:
        token = (value or '').strip()
        if token.lower().startswith('bearer '):
            token = token[7:].strip()
        return token

    def _ensure_device_id(self) -> str:
        device_id = (self.config.get('device_id') or '').strip()
        if not device_id:
            device_id = str(uuid.uuid4())
            self._update_config('device_id', device_id)
        return device_id

    def _token_expires_at(self) -> float:
        for key in ('access_expires_at', 'access_token_expires_at'):
            try:
                expires = float(self.config.get(key) or 0)
            except (TypeError, ValueError):
                expires = 0
            if expires > 10_000_000_000:
                expires /= 1000
            if expires:
                return expires
        return 0

    def _mvpd_account(self):
        from ..models import TVEAccount

        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if account and account.is_enabled and account.has_credentials():
            return account
        return None

    @staticmethod
    def _account_mso_id(account) -> str:
        cfg = account.config or {}
        return (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or cfg.get('adobe_mso_id') or 'Cox').strip()

    def _shared_home_zip_code(self) -> str:
        """Home ZIP lives on the shared TVEAccount config (Settings > TV
        Everywhere), not per-source — it's a household fact, not something
        specific to FOX One, and living there means any future TVE source
        that needs a home market (e.g. an nbc_tve local-affiliate lookup)
        can read the same value instead of collecting its own copy."""
        from ..models import TVEAccount

        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        return ((account.config or {}).get('home_zip_code') or '').strip() if account else ''

    def _persist_shared_home_zip_code(self, zip_code: str) -> None:
        from .. import db
        from ..models import TVEAccount

        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
        if not account:
            account = TVEAccount(provider_id='mvpd', display_name='TV Provider', is_enabled=False, config={})
            db.session.add(account)
        if (account.config or {}).get('home_zip_code'):
            return
        cfg = dict(account.config or {})
        cfg['home_zip_code'] = zip_code
        account.config = cfg
        db.session.commit()

    def _authenticate_via_mvpd(
        self, mso_id: str, username: str, password: str, cookie_jar: dict | None = None,
    ) -> tuple[str, float]:
        """Link this device to FOX One's entitlement system through an MVPD
        (TV-provider) sign-in, mirroring the same Adobe Pass SAML dance
        fox_tve.py already automates — just via FOX One's own adobeauthn/
        regcode endpoints instead of fox_tve's legacy api3.fox.com ones.
        Tokens from the two are not interchangeable (different OAuth
        client_id), so FOX One needs its own pass at this.

        adoberegcode/adobeauthn are already MVPD-agnostic — mso_id flows
        straight through to Adobe Pass, which redirects to whichever MVPD's
        real login page. Only the final login step is currently wired up:
        Cox gets a fast scripted native login (_cox_saml_login, same Okta
        API fox_tve.py's legacy path uses). Any other MVPD raises a clear
        error below rather than silently POSTing its credentials into Cox's
        login form — a scripted login for it hasn't been built yet, same
        gap as fox_tve.py's _fox_sports_mvpd_token. Wiring one up (or
        falling back to the shared browser-assisted pairing flow) is
        future work, gated on having real credentials for that MVPD to
        verify against.

        A device_id can get stuck replaying an old, never-completed MVPD-link
        request — adoberegcode just echoes our own redirect_url back as a no-op
        authenticateURL instead of a real api.auth.adobe.com link, and retrying
        with the same device_id never clears it. One retry with a freshly minted
        device_id always gets a clean slot, so that's tried automatically before
        giving up — no manual "re-auth" button needed."""
        for attempt in range(2):
            device_id = self._ensure_device_id()
            xid = str(uuid.uuid4())
            session = self.new_session(headers={'User-Agent': _UA})

            start_url = (
                f'{_ID_BASE}/adobeauthn/v1/auth?client_id={_HYDRA_CLIENT_ID}&device_id={device_id}'
                '&redirect_uri=https%3A%2F%2Fwww.fox.com%2Fcallback'
                f'&options=apikey%3D{_API_KEY}%26xid%3D{xid}'
            )
            r = session.get(start_url, allow_redirects=True, timeout=20)
            r.raise_for_status()
            match = re.search(r'request_id=([^&]+)', r.url)
            if not match:
                raise RuntimeError('FOX One adobeauthn did not return a request_id')
            request_id = match.group(1)

            headers = {
                'Accept': 'application/json',
                'x-api-key': _PLATFORM_API_KEY,
                'x-ori-client-api-key': _API_KEY,
                'Referer': 'https://auth.fox.com/',
                'Origin': 'https://auth.fox.com',
            }
            # The redirect_url must be the SPA's own /callback route with polling_mvpd
            # and a flat apikey param appended, matching what the real fox.com frontend
            # sends — not the bare landing-page URL adobeauthn redirected to (r.url).
            # A mismatched redirect_url makes adoberegcode degrade to echoing it back
            # as a no-op authenticateURL instead of a real api.auth.adobe.com link,
            # which just serves FOX's generic (long-cached, often stale) landing page.
            callback_url = r.url.replace('/foxone/mvpd?', '/foxone/mvpd/callback?', 1)
            redirect_url = f'{callback_url}&polling_mvpd={mso_id}&apikey={_API_KEY}'
            params = {'mvpd_id': mso_id, 'device_id': device_id, 'first_screen': 'true', 'redirect_url': redirect_url}
            r2 = session.get(f'{_ID_BASE}/regcode/v1/adoberegcode', params=params, headers=headers, timeout=20)
            r2.raise_for_status()
            auth_url = r2.json().get('authenticateURL')
            if not auth_url:
                raise RuntimeError('FOX One adoberegcode did not return authenticateURL')

            if urlparse(auth_url).netloc == _ADOBE_AUTHENTICATE_HOST:
                break
            if attempt == 0:
                self._update_config('device_id', '')
                self._update_config('platform_location', '')
                self._update_config('platform_location_cached_at', 0)
                continue
            raise RuntimeError(
                f'FOX One adoberegcode returned a non-Adobe authenticateURL host '
                f'even after a fresh device_id: {urlparse(auth_url).netloc}'
            )

        r3 = session.get(auth_url, allow_redirects=False, timeout=20)
        mso_login_url = r3.headers.get('location') or ''

        if mso_id == 'Cox':
            if 'login.cox.com' not in mso_login_url:
                raise RuntimeError(f'Unexpected FOX One Adobe redirect host: {urlparse(mso_login_url).netloc}')
            _cox_saml_login(session, mso_login_url, username, password)
        elif mso_id == 'Comcast_SSO':
            if 'xfinity.com' not in mso_login_url:
                raise RuntimeError(f'Unexpected FOX One Adobe redirect host: {urlparse(mso_login_url).netloc}')
            if not cookie_jar:
                raise ValueError(
                    'FOX One: Comcast_SSO needs a saved Xfinity cookie jar — '
                    'needs a browser-assisted sign-in to harvest one first.'
                )
            # Uses its own dedicated session internally, not `session` (which
            # carries FOX's own request_id-linked state used by the
            # completion call below) — FOX binds the completed login
            # server-side to `request_id` regardless of which HTTP session
            # did the MSO login, same pattern already proven for NBC/AMCN.
            from ..tve.adobe_pass import xfinity_cookie_jar_login
            xfinity_cookie_jar_login(mso_login_url, username, password, cookie_jar)
        else:
            raise ValueError(
                f'FOX One native sign-in is not built yet for MVPD {mso_id} '
                f'(only scripted Cox login and Comcast_SSO cookie-jar login are wired up here) — '
                f'same gap as FOX TVE\'s _fox_sports_mvpd_token.'
            )

        rc = session.post(
            f'{_ID_BASE}/adobeauthn/v1/requests/complete',
            json={'request_id': request_id, 'mvpd_id': mso_id, 'status': 'authenticated'},
            headers={**headers, 'Content-Type': 'application/json'},
            timeout=20,
        )
        rc.raise_for_status()
        if not rc.json().get('redirect_to'):
            raise RuntimeError('FOX One MVPD completion did not report success')

        check = session.get(
            f'{_ID_BASE}/adobeauthn/v3/checkauthn',
            params={'device_id': device_id, 'requestor': 'foxone', 'client_id': _HYDRA_CLIENT_ID},
            headers=headers,
            timeout=20,
        )
        check.raise_for_status()
        access_token = self._clean_token(check.json().get('accessToken'))
        if not access_token:
            raise RuntimeError('FOX One checkauthn did not return accessToken')

        expires_at = _jwt_exp(access_token) or (time.time() + 3600)
        return access_token, expires_at

    def _ensure_access_token(self) -> str:
        from .. import db

        access_token = self._clean_token(self.config.get('access_token'))
        if access_token and self._token_expires_at() > time.time() + _TOKEN_REFRESH_SKEW:
            return access_token

        account = self._mvpd_account()
        mvpd_exc: Exception | None = None
        if account:
            mso_id = self._account_mso_id(account)
            try:
                access_token, expires_at = self._authenticate_via_mvpd(
                    mso_id, account.username, account.password, (account.config or {}).get('xfinity_cookie_jar'),
                )
                account.last_auth_status = 'ok'
                account.last_auth_message = f'FOX One access token obtained through {mso_id} MVPD.'
                account.last_auth_at = datetime.now(timezone.utc)
                db.session.commit()
                self._update_config('access_token', access_token)
                self._update_config('access_expires_at', expires_at)
                self._update_config('access_token_captured_at', int(time.time()))
                return access_token
            except Exception as exc:
                account.last_auth_status = 'error'
                account.last_auth_message = f'FOX One {mso_id} MVPD auth failed: {exc}'[:500]
                account.last_auth_at = datetime.now(timezone.utc)
                db.session.commit()
                mvpd_exc = exc
                if not access_token:
                    raise

        refresh_token = self._clean_token(self.config.get('refresh_token') or self.config.get('fox_one_refresh_token'))
        if not refresh_token:
            # Only reuse the existing token if it hasn't actually expired yet
            # (it failed the refresh-skew check above, but may still be good
            # for a few more minutes). A genuinely expired token must not be
            # handed out as if auth succeeded — that just turns a clear MVPD
            # auth failure into an opaque 401 further downstream.
            if access_token and self._token_expires_at() > time.time():
                return access_token
            raise mvpd_exc or ValueError('FOX One native playback requires either a linked TV-provider (MVPD) account or a configured refresh_token.')

        r = self.session.post(
            _HYDRA_TOKEN_API,
            headers={
                'User-Agent': _UA,
                'Accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://www.fox.com',
                'Referer': 'https://www.fox.com/',
                'x-api-key': _API_KEY,
            },
            data={
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'scope': 'openid offline',
                'client_id': _HYDRA_CLIENT_ID,
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        access_token = self._clean_token(data.get('access_token'))
        if not access_token:
            raise RuntimeError('FOX One token refresh did not return access_token')

        expires_at = data.get('expires_at')
        try:
            expires_at = float(expires_at or 0)
        except (TypeError, ValueError):
            expires_at = 0
        if expires_at > 10_000_000_000:
            expires_at /= 1000
        if not expires_at:
            expires_at = time.time() + float(data.get('expires_in') or 86400)

        self._update_config('access_token', access_token)
        self._update_config('access_expires_at', expires_at)
        self._update_config('access_token_captured_at', int(time.time()))
        new_refresh = self._clean_token(data.get('refresh_token'))
        if new_refresh:
            self._update_config('refresh_token', new_refresh)
        return access_token

    def _ent_headers(self, access_token: str, *, include_device: bool = True) -> dict[str, str]:
        headers = {
            'User-Agent': _UA,
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Origin': 'https://www.fox.com',
            'Referer': 'https://www.fox.com/',
            'Authorization': f'Bearer {access_token}',
            'x-access-token': f'Bearer {access_token}',
            'x-api-key': _API_KEY,
        }
        if include_device:
            headers['x-device-xid'] = self._ensure_device_id()
        return headers

    def _metadata_header(self, data: dict, key: str) -> str | None:
        metadata = ((data.get('data') or {}).get('metadata') or data.get('metadata') or {})
        value = metadata.get(key)
        return value.strip() if isinstance(value, str) and value.strip() else None

    def _ensure_platform_location(self, access_token: str) -> str:
        configured = (self.config.get('platform_location') or '').strip()
        try:
            cached_at = float(self.config.get('platform_location_cached_at') or 0)
        except (TypeError, ValueError):
            cached_at = 0
        has_dynamic_auth = bool(self.config.get('refresh_token')) or bool(self._mvpd_account())
        if configured and (not has_dynamic_auth or cached_at > time.time() - _LOCATION_TTL):
            return configured

        locator = self.session.get(
            f'{_ENT_BASE}/locator/v1/location',
            headers=self._ent_headers(access_token),
            timeout=30,
        )
        locator.raise_for_status()
        locator_data = locator.json()
        platform_location = self._metadata_header(locator_data, 'x-platform-location')
        if not platform_location:
            raise RuntimeError('FOX locator response did not include x-platform-location')

        location_data = (locator_data.get('data') or {}).get('location') or {}
        home_zip = (self._shared_home_zip_code() or location_data.get('zip_code') or '').strip()
        home_location = None
        home_headers = self._ent_headers(access_token, include_device=False)
        home = self.session.get(
            f'{_ENT_BASE}/user-preferences/v1/home-location',
            headers=home_headers,
            timeout=30,
        )
        if home.status_code == 404 and home_zip:
            home = self.session.put(
                f'{_ENT_BASE}/user-preferences/v1/home-location',
                headers=home_headers,
                json={'home_zip_code': home_zip},
                timeout=30,
            )
        if home.status_code != 404:
            home.raise_for_status()
            home_location = self._metadata_header(home.json(), 'x-ent-home-location')

        combined = ','.join(part for part in (platform_location, home_location) if part)
        self._update_config('platform_location', combined)
        self._update_config('platform_location_cached_at', time.time())
        if home_zip:
            self._persist_shared_home_zip_code(home_zip)
        return combined

    def _ensure_entitlements(self, access_token: str) -> str:
        """The account's granted content SKUs, as the comma-joined
        `sku:type` string FOX's own client sends verbatim in the
        x-fox-content-entitlement header (see _location_headers). Confirmed
        live via dev/foxone/4.har: without this header, location-gated
        containers — including the local-affiliate guide slot, gated behind
        the `us.foxlocal` SKU — silently return item_count: 0 instead of an
        error, even though the caller is otherwise fully authenticated and
        located."""
        cached = self.cache.get('entitlements') or {}
        cached_at = float(cached.get('cached_at') or 0)
        if cached.get('value') and (time.time() - cached_at) < _ENTITLEMENTS_TTL:
            return cached['value']

        r = self.session.get(
            f'{_ENT_BASE}/userentitlements/v1/userentitlements',
            headers={
                'User-Agent': _UA,
                'Accept': 'application/json',
                'Authorization': f'Bearer {access_token}',
                'x-api-key': _API_KEY,
            },
            timeout=30,
        )
        r.raise_for_status()
        results = ((r.json() or {}).get('data') or {}).get('results') or []
        skus = [
            f'{item["contentSku"]}:{item["entitlementType"][0]}'
            for item in results
            if isinstance(item, dict) and item.get('contentSku') and item.get('entitlementType')
        ]
        value = ','.join(skus)
        self._update_cache('entitlements', {'value': value, 'cached_at': time.time()})
        return value

    def _dtc_headers(self) -> dict[str, str]:
        access_token = self._ensure_access_token()
        platform_location = self._ensure_platform_location(access_token)
        return {
            'User-Agent': _UA,
            'Accept': '*/*',
            'Content-Type': 'application/json',
            'Origin': 'https://www.fox.com',
            'Referer': 'https://www.fox.com/',
            'x-access-token': f'Bearer {access_token}',
            'x-api-key': _API_KEY,
            'x-device-capabilities': 'drm/widevine',
            'x-platform-location': platform_location,
        }

    def _has_native_playback_config(self) -> bool:
        return bool(
            self.config.get('refresh_token')
            or (self.config.get('access_token') and self.config.get('platform_location'))
            or self._mvpd_account()
        )

    def _resolve_dtc(self, container_id: str | None) -> str:
        # NOTE: watchlive's playbackUrl is a single-use ticket — its embedded `exp`
        # is a 24h signature window, but Fox's edge rejects a *second* GET to the
        # same ticket URL with 403 "Too late" (replay protection). Do not cache or
        # reuse the returned URL across calls; always redeem a fresh one.
        asset_id = self._current_asset_id(container_id)
        if not asset_id:
            raise ValueError(f'FOX One could not determine current asset for container {container_id}')
        device_id = self._ensure_device_id()
        payload = {
            'asset': {'id': asset_id},
            'stream': {'type': 'live'},
            'device': {
                'capabilities': ['drm/widevine'],
                'model': ' WPF Device',
                'width': 1215,
                'height': 922,
                'os': 'Windows',
                'osv': '10',
            },
            'ad': {
                'did': device_id,
                'customParams': {'xid': device_id, 'isNationSku': False, 'isNationSub': False},
                'capabilities': ['ssai'],
            },
            'debug': {'traceId': ''},
            'privacy': {'us': '1YNN', 'lat': False},
        }
        r = self.session.post(_PLAYBACK_API, headers=self._dtc_headers(), json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        playback_url = ((data.get('stream') or {}).get('playbackUrl') or '').strip()
        if not playback_url:
            raise RuntimeError(f'FOX One playback response did not include playbackUrl for {asset_id}')
        return playback_url

    def _delegate(self) -> FoxTVEScraper:
        return FoxTVEScraper(config=self.config)

    def _absorb_delegate_updates(self, delegate: FoxTVEScraper) -> None:
        for key, value in getattr(delegate, '_pending_config_updates', {}).items():
            self._pending_config_updates[key] = value
        for key, value in getattr(delegate, '_pending_cache_updates', {}).items():
            self._pending_cache_updates[key] = value

    def audit_resolve(self, raw_url: str) -> str:
        _, target_id, container_id = _stream_parts(raw_url)
        if self._has_native_playback_config():
            return self._resolve_dtc(container_id)
        if not target_id or target_id not in FOX_TVE_CHANNELS:
            raise ValueError(f'FOX One stream requires native playback config: {raw_url}')
        delegate = self._delegate()
        try:
            return delegate.audit_resolve(FOX_TVE_CHANNELS[target_id].stream_url)
        finally:
            self._absorb_delegate_updates(delegate)

    def resolve(self, raw_url: str) -> str:
        _, target_id, container_id = _stream_parts(raw_url)
        if self._has_native_playback_config():
            return self._resolve_dtc(container_id)
        if not target_id or target_id not in FOX_TVE_CHANNELS:
            raise ValueError(f'FOX One stream requires native playback config: {raw_url}')
        delegate = self._delegate()
        try:
            return delegate.resolve(FOX_TVE_CHANNELS[target_id].stream_url)
        finally:
            self._absorb_delegate_updates(delegate)

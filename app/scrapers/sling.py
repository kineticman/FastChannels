from __future__ import annotations

import base64
import json
import logging
import random
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any
from urllib.parse import quote

import requests

try:
    from requests_oauthlib import OAuth1
except ImportError:  # pragma: no cover - optional until Docker deps are rebuilt
    OAuth1 = None

try:
    from .base import BaseScraper, ChannelData, ConfigField, ProgramData, StreamDeadError, format_http_reason, infer_language_from_metadata
    from .category_utils import infer_category_from_name, normalize_category
except ImportError:  # pragma: no cover - local staging outside FastChannels package
    from app.scrapers.base import BaseScraper, ChannelData, ConfigField, ProgramData, StreamDeadError, format_http_reason, infer_language_from_metadata
    from app.scrapers.category_utils import infer_category_from_name, normalize_category

logger = logging.getLogger(__name__)

# Matches internal Sling test/slate channel call signs regardless of any display-name overrides.
_TEST_CHANNEL_RE = re.compile(r'^SLATEPO\d|^HYBRID-SIGNALTEST-', re.IGNORECASE)


def _join_categories(values: list[str] | tuple[str, ...] | None) -> str | None:
    if not values:
        return None
    unique = list(dict.fromkeys(v.strip() for v in values if v and v.strip()))
    return ';'.join(unique) or None


class SlingScraper(BaseScraper):
    """
    FastChannels scraper for Sling Freestream, with optional Sling account
    credentials to include entitled subscription channels.

    - Freestream channel inventory: public channel summary feed (no auth).
    - Subscription channel inventory: authenticated subscriptionpack channel feed.
    - EPG: per-channel schedule.qvt windows.
    - Streams: CENC-encrypted DASH (Widevine + PlayReady).
    - DRM: Widevine proxy at p-drmwv.movetv.com accepts the challenge wrapped in a
      JSON envelope {"env":..,"user_id":..,"channel_id":..,"message":[bytes]}.
      Freestream accepts an anonymous UUID; subscription channels use the saved
      Sling subscriber id.
    """

    source_name = "sling"
    display_name = "Sling Freestream"
    scrape_interval = 360
    stream_audit_enabled = True
    epg_quality = 'basic'     # thumbnails only; no program descriptions
    source_category = 'drm'
    # Sling Freestream channels use the CENC/Widevine bridge path.
    all_channels_require_drm_bridge = True
    license_url = 'https://p-drmwv.movetv.com/widevine/proxy'
    config_schema = [
        ConfigField('username', 'Email', placeholder='you@example.com',
                    help_text='Optional. Freestream does not require sign-in; enter this only to include paid Sling subscription channels.'),
        ConfigField('password', 'Password', field_type='password', secret=True,
                    help_text='Optional. Required only with a paid Sling account when subscription channels are enabled.'),
        ConfigField('include_subscription_channels', 'Include paid subscription channels',
                    field_type='toggle', default='false',
                    help_text='Off keeps Sling Freestream-only. Turn on only when email/password are saved for an active paid Sling account.'),
    ]
    kodi_props = {
        'inputstream': 'inputstream.adaptive',
        'inputstream.adaptive.manifest_type': 'mpd',
        'inputstream.adaptive.license_type': 'com.widevine.alpha',
    }
    channel_refresh_hours = 0    # fetch channel list every run — one summary call; avoids channel-list staleness

    CMW_FAST = "https://p-cmwnext-fast.movetv.com"
    CMW = "https://p-cmwnext.movetv.com"
    CMS = "https://cbd46b77.cdn.cms.movetv.com"
    UMS = "https://ums.p.sling.com"
    EXTAUTH = "https://int.p.sling.com"
    MS = "https://ms.p.sling.com"
    GEO = "https://p-geo.movetv.com/geo"

    _OAUTH_CONSUMER_KEY = "4rvjj7tdCLxg5ed8vcYElMejjmkDhE2jcuam0VNX"
    _OAUTH_CONSUMER_SECRET = "MnaIjrORUh8WIIG3t3wsVJkk1o0wGPLQT65KUfaA"

    DEFAULT_FOCUS_CHANNEL_ID = "21ec280634b247cfa0688744fb7a7e8a"

    SUMMARY_URL = f"{CMS}/cms/publish3/domain/summary/ums/1.json"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update(
            {
                "accept": "application/json, text/plain, */*",
                "origin": "https://watch.sling.com",
                "referer": "https://watch.sling.com/",
                "client-config": "rn-client-config",
                "client-version": "7.1.32",
                "device-model": "Chrome",
                "player-version": "9.1.0",
                "response-config": "ar_browser_1_1",
                "dma": "535",
                "geo-zipcode": "43017",
                "time-zone-id": "America/New_York",
                "timezone": "-0500",
                "features": "enable_ad_tracking,web_browser",
            }
        )

    # ------------------------------------------------------------------ DRM

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs
    ) -> tuple[bytes, dict]:
        """Wrap the Widevine challenge in Sling's JSON envelope.
        No auth token needed — any UUID works as user_id for Freestream channels."""
        if not channel_id:
            logger.warning('[sling] license request missing channel_id')
            return challenge, {}
        body = json.dumps({
            'env': 'production',
            'user_id': (config.get('subscriber_id') or str(uuid.uuid4())),
            'channel_id': channel_id,
            'message': list(challenge),
        }).encode()
        return body, {
            'Content-Type': 'text/plain;charset=UTF-8',
            'Origin': 'https://watch.sling.com',
            'Referer': 'https://watch.sling.com/',
        }

    @classmethod
    def get_kodi_props_for_channel(cls, base_url: str, source_channel_id: str) -> dict[str, str]:
        props = dict(cls.kodi_props)
        props['inputstream.adaptive.license_key'] = (
            f'{base_url}/play/sling/license?channel_id={source_channel_id}||R{{SSM}}|'
        )
        return props

    # ------------------------------------------------------------------ setup

    def pre_run_setup(self) -> None:
        if self._include_subscription_channels():
            try:
                self._ensure_subscription_auth()
            except Exception as exc:  # noqa: BLE001
                logger.warning('[sling] subscription auth failed during setup; Freestream will still scrape: %s', exc)

    def fetch_channels(self) -> list[ChannelData]:
        payload = self._get_json(self.SUMMARY_URL)
        summary_channels = payload.get("channels") or []
        channels: dict[str, ChannelData] = {}

        for item in summary_channels:
            channel = self._channel_from_summary(item)
            if channel is not None:
                channels[channel.source_channel_id] = channel

        if self._include_subscription_channels():
            for channel in self._fetch_subscription_channels():
                channels[channel.source_channel_id] = channel

        result = sorted(channels.values(), key=lambda c: (c.name or "", c.source_channel_id))
        logger.info("[%s] %d channels", self.source_name, len(result))
        return result

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        max_windows = 48   # walk ~48 schedule.qvt windows → ~22h floor (p10), ~40h median guide
        max_workers = 100  # channels walked in parallel; 100 keeps a full-EPG scrape ≈ 1.8 min
        selected    = channels
        total       = len(selected)

        headers_snapshot = dict(self.session.headers)

        programs: list[ProgramData] = []
        lock     = threading.Lock()
        thread_local = threading.local()
        done     = [0]   # mutable counter accessible from threads

        def fetch_one(channel_id: str) -> None:
            sess = getattr(thread_local, "session", None)
            if sess is None:
                sess = self.new_session(headers=headers_snapshot)
                thread_local.session = sess
            try:
                result = self._fetch_epg_for_channel_with_session(channel_id, max_windows, sess)
            except Exception as exc:  # noqa: BLE001
                resp = getattr(exc, 'response', None)
                if resp is not None and resp.status_code == 404:
                    logger.debug("[%s] no EPG for %s", self.source_name, channel_id)
                else:
                    logger.warning("[%s] EPG fetch failed for %s: %s", self.source_name, channel_id, exc)
                result = []
            with lock:
                programs.extend(result)
                done[0] += 1
                if self._progress_cb:
                    self._progress_cb('epg', done[0], total)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_one, ch.source_channel_id) for ch in selected}
            for future in as_completed(futures):
                exc = future.exception()
                if exc and type(exc).__name__ == 'JobTimeoutException':
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise exc

        programs.sort(key=lambda p: (p.source_channel_id, p.start_time, p.title))
        logger.info("[%s] %d EPG entries", self.source_name, len(programs))
        return programs

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith("sling://"):
            return raw_url

        channel_guid = raw_url.split("sling://", 1)[1].strip()
        if not channel_guid:
            return raw_url

        try:
            payload = self._get_json(self._channel_schedule_url(channel_guid))
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                raise StreamDeadError(format_http_reason("[sling] channel not found", 404, channel_guid)) from exc
            raise

        playback = payload.get("playback_info") or {}
        for key in ("dash_manifest_url", "live_m3u8_url_template", "m3u8_url_template"):
            url = (playback.get(key) or "").strip()
            if url and "{" not in url and url.startswith("http"):
                return url
        raise RuntimeError(f"No playable URL found for sling channel {channel_guid}")

    def _get_json(self, url: str) -> dict[str, Any]:
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _channel_schedule_url(self, channel_guid: str) -> str:
        return f"{self.CMS}/playermetadata/sling/v1/api/channels/{channel_guid}/current/schedule.qvt"

    def _channel_schedule_24_url(self, channel_guid: str) -> str:
        stamp = datetime.utcnow().strftime("%y%m%d%H%M")
        return f"{self.CMS}/cms/publish3/channel/schedule/24/{stamp}/1/{channel_guid}.json"

    def _include_subscription_channels(self) -> bool:
        return str(self.config.get('include_subscription_channels', '')).strip().lower() in {'1', 'true', 'yes', 'on'}

    def _oauth(self, token: str | None = None, token_secret: str | None = None):
        if OAuth1 is None:
            raise RuntimeError('requests-oauthlib is required for Sling subscription auth; rebuild the container')
        return OAuth1(
            self._OAUTH_CONSUMER_KEY,
            self._OAUTH_CONSUMER_SECRET,
            token or None,
            token_secret or None,
        )

    def _subscription_headers(self, *, content_type: str | None = 'application/json;charset=UTF-8') -> dict[str, str]:
        headers = dict(self.session.headers)
        for key in list(headers):
            if key.lower() in {'origin', 'referer', 'content-type'}:
                headers.pop(key, None)
        headers.update({
            'Origin': 'https://www.sling.com',
            'Referer': 'https://www.sling.com',
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'
            ),
        })
        if content_type:
            headers['Content-Type'] = content_type
        return headers

    def _ensure_device_id(self) -> str:
        device_id = (self.config.get('device_id') or '').strip()
        if not device_id:
            device_id = str(uuid.uuid4())
            self._update_config('device_id', device_id)
        return device_id

    def _lookup_subscription_account(self, username: str) -> dict[str, Any]:
        payload = {
            'request_context': {
                'application_name': 'Browser',
                'interaction_id': f'Browser:{self._ensure_device_id()[:7]}',
                'partner_name': 'Browser',
                'request_id': str(random.randint(0, 999999)),
                'timestamp': str(int(time.time())),
            },
            'request': {'email': username},
        }
        resp = self.session.post(
            f'{self.EXTAUTH}/user/lookup',
            headers=self._subscription_headers(),
            data=json.dumps(payload),
            auth=self._oauth(),
            timeout=30,
        )
        resp.raise_for_status()
        account = (resp.json().get('response') or {})
        subscriber_id = (account.get('guid') or '').strip()
        account_status = (account.get('account_status') or '').strip()
        if subscriber_id:
            self._update_config('subscriber_id', subscriber_id)
        if account_status:
            self._update_config('account_status', account_status)
        return account

    def _ensure_subscription_auth(self) -> None:
        username = (self.config.get('username') or '').strip()
        password = (self.config.get('password') or '').strip()
        browser_token = self._browser_auth_token()

        token = (self.config.get('oauth_token') or '').strip()
        token_secret = (self.config.get('oauth_token_secret') or '').strip()
        if token and token_secret:
            try:
                self._refresh_subscription_context()
                return
            except Exception as exc:  # noqa: BLE001
                logger.info('[sling] cached OAuth token did not validate; logging in again: %s', exc)

        if username:
            account = self._lookup_subscription_account(username)
            account_status = (account.get('account_status') or '').strip().lower()
            if account_status and account_status != 'active':
                raise RuntimeError(f'Sling account is {account_status}; paid subscription channels require an active account')

        if not username or not password:
            if not browser_token:
                logger.warning('[sling] include_subscription_channels is enabled but no cached OAuth, browser token, or username/password are saved')
                return

        if browser_token:
            try:
                self._exchange_browser_auth_token(browser_token)
                return
            except Exception as exc:  # noqa: BLE001
                logger.info('[sling] browser auth token exchange failed; trying legacy login: %s', exc)

        if not username or not password:
            logger.warning('[sling] legacy Sling login requires username/password')
            return

        device_id = self._ensure_device_id()
        payload = f"email={quote(username)}&password={quote(password)}&device_guid={quote(device_id)}"
        resp = self.session.put(
            f'{self.UMS}/v3/xauth/access_token.json',
            headers=self._subscription_headers(content_type='application/x-www-form-urlencoded'),
            data=payload,
            auth=self._oauth(),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        token = (data.get('oauth_token') or '').strip()
        token_secret = (data.get('oauth_token_secret') or '').strip()
        if not token or not token_secret:
            raise RuntimeError('Sling login response did not include OAuth tokens')
        self._update_config('oauth_token', token)
        self._update_config('oauth_token_secret', token_secret)
        self._update_config('oauth_token_time', int(time.time()))
        self._refresh_subscription_context()

    def _subscription_auth(self):
        return self._oauth(
            (self.config.get('oauth_token') or '').strip(),
            (self.config.get('oauth_token_secret') or '').strip(),
        )

    def _browser_auth_token(self) -> str:
        token = (self.config.get('browser_auth_token') or '').strip()
        if not token:
            return ''
        if 'AUTHORIZATION:Token=' in token:
            token = token.split('AUTHORIZATION:Token=', 1)[1].split(';', 1)[0].strip()
        if token.lower().startswith('bearer '):
            token = token.split(None, 1)[1].strip()
        return token

    def _watch_headers(self, *, content_type: str | None = 'application/json; charset=UTF-8') -> dict[str, str]:
        headers = dict(self.session.headers)
        headers.update({
            'Origin': 'https://watch.sling.com',
            'Referer': 'https://watch.sling.com/',
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'
            ),
        })
        if content_type:
            headers['Content-Type'] = content_type
        else:
            headers.pop('Content-Type', None)
        return headers

    def _exchange_browser_auth_token(self, browser_token: str) -> dict[str, Any]:
        form = {
            'token': browser_token,
            'device_guid': self._ensure_device_id(),
            'client_application': 'browser',
        }
        oauth_resp = self.session.post(
            f'{self.UMS}/v5/users/access_from_jwt',
            headers=self._watch_headers(content_type='application/x-www-form-urlencoded; charset=UTF-8'),
            data=form,
            auth=self._oauth(),
            timeout=30,
        )
        oauth_resp.raise_for_status()
        access_token = (oauth_resp.json().get('access_token') or {})
        token = (access_token.get('token') or '').strip()
        token_secret = (access_token.get('secret') or '').strip()
        if not token or not token_secret:
            raise RuntimeError('Sling access_from_jwt response did not include OAuth tokens')

        self._update_config('oauth_token', token)
        self._update_config('oauth_token_secret', token_secret)
        self._update_config('oauth_token_time', int(time.time()))
        self._update_config('browser_auth_token', '')
        logger.info('[sling] exchanged browser auth token for cached OAuth credentials')
        return self._refresh_subscription_context()

    def _refresh_subscription_context(self) -> dict[str, Any]:
        resp = self.session.get(
            f'{self.UMS}/v2/user.json',
            headers=self._subscription_headers(content_type=None),
            auth=self._subscription_auth(),
            timeout=30,
        )
        resp.raise_for_status()
        info = resp.json()

        subscriber_id = (info.get('guid') or info.get('user_guid') or info.get('subscriber_id') or '').strip()
        if subscriber_id:
            self._update_config('subscriber_id', subscriber_id)
        if info.get('account_status'):
            self._update_config('account_status', info.get('account_status'))
        if info.get('lineup_key'):
            self._update_config('lineup_key', info.get('lineup_key'))
        if info.get('billing_zipcode'):
            self._update_config('billing_zip', str(info.get('billing_zipcode')).split('-', 1)[0])

        subscriptions = info.get('subscriptionpacks') or []
        legacy_ids: list[str] = []
        package_guids: list[str] = []
        for sub in subscriptions:
            legacy_id = str(sub.get('id') or '').strip()
            package_guid = str(sub.get('guid') or '').strip()
            if legacy_id:
                legacy_ids.append(legacy_id)
            if package_guid:
                package_guids.append(package_guid)
        self._update_config('legacy_subs', '+'.join(dict.fromkeys(legacy_ids)) if legacy_ids else '')
        self._update_config('user_subs', '+'.join(dict.fromkeys(package_guids)) if package_guids else '')

        if subscriber_id:
            self._refresh_region_context(subscriber_id)
        return info

    def _refresh_region_context(self, subscriber_id: str) -> None:
        params = {'subscriber_id': subscriber_id, 'device_id': self._ensure_device_id()}
        resp = self.session.get(
            self.GEO,
            params=params,
            headers=self._subscription_headers(content_type=None),
            timeout=30,
        )
        if not resp.ok:
            logger.warning('[sling] region lookup failed: HTTP %s', resp.status_code)
            return
        data = resp.json()
        if data.get('dma'):
            self._update_config('user_dma', str(data.get('dma')))
        if data.get('time_zone_offset'):
            self._update_config('user_offset', str(data.get('time_zone_offset')))
        if data.get('zip_code'):
            self._update_config('user_zip', str(data.get('zip_code')))

    def _fetch_subscription_channels(self) -> list[ChannelData]:
        try:
            self._ensure_subscription_auth()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[sling] subscription auth failed; keeping Freestream-only lineup: %s', exc)
            return []

        legacy_subs = (self.config.get('legacy_subs') or '').strip()
        if not legacy_subs:
            logger.warning('[sling] Sling account has no subscription package ids; keeping Freestream-only lineup')
            return []

        user_offset = (self.config.get('user_offset') or '-0500').strip()
        user_dma = (self.config.get('user_dma') or '535').strip()
        encoded_subs = base64.b64encode(legacy_subs.replace('+', ',').encode()).decode().strip()
        url = f'{self.CMS}/cms/publish3/domain/channels/v4/{user_offset}/{user_dma}/{encoded_subs}/1.json'
        payload = self._get_json(url)

        channel_ids: dict[str, dict[str, Any]] = {}
        for pack in payload.get('subscriptionpacks') or []:
            for item in pack.get('channels') or []:
                channel_guid = (item.get('channel_guid') or item.get('guid') or '').strip()
                if channel_guid:
                    channel_ids[channel_guid] = item

        if not channel_ids:
            logger.warning('[sling] subscription channel feed returned no channels')
            return []

        channels: list[ChannelData] = []
        max_workers = min(40, max(1, len(channel_ids)))
        headers_snapshot = dict(self.session.headers)
        thread_local = threading.local()

        def fetch_one(channel_guid: str) -> ChannelData | None:
            sess = getattr(thread_local, 'session', None)
            if sess is None:
                sess = self.new_session(headers=headers_snapshot)
                thread_local.session = sess
            try:
                resp = sess.get(self._channel_schedule_24_url(channel_guid), timeout=15)
                resp.raise_for_status()
                schedule = (resp.json().get('schedule') or {})
                channel = self._channel_from_summary(schedule, require_free=False)
                if channel is not None:
                    channel.tags = list(dict.fromkeys([*channel.tags, 'Sling Subscription']))
                return channel
            except Exception as exc:  # noqa: BLE001
                fallback = self._channel_from_subscription_item(channel_ids[channel_guid])
                if fallback is not None:
                    fallback.tags = list(dict.fromkeys([*fallback.tags, 'Sling Subscription']))
                    return fallback
                logger.debug('[sling] subscription channel detail failed for %s: %s', channel_guid, exc)
                return None

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_one, channel_guid) for channel_guid in channel_ids}
            for future in as_completed(futures):
                channel = future.result()
                if channel is not None:
                    channels.append(channel)

        logger.info('[%s] %d subscription channels', self.source_name, len(channels))
        return channels

    def _channel_from_subscription_item(self, item: dict[str, Any]) -> ChannelData | None:
        channel_guid = (item.get('channel_guid') or item.get('guid') or '').strip()
        if not channel_guid:
            return None
        name = self._best_summary_channel_name(item) or item.get('network_affiliate_name') or item.get('title')
        if not name:
            return None
        logo_url = ((item.get('thumbnail') or {}).get('url'))
        category = self._infer_summary_group(item, name)
        return ChannelData(
            source_channel_id=channel_guid,
            name=name.strip(),
            stream_url=f'sling://{channel_guid}',
            logo_url=logo_url,
            slug=self._slugify(name),
            category=category,
            language=infer_language_from_metadata(name, category, (item.get('metadata') or {}).get('language')),
            country='US',
            stream_type='dash',
            number=self._to_int(item.get('channel_number')),
            gracenote_id=str(item.get('gracenote_channel_id') or '').strip() or None,
            guide_key=(item.get('qvt_url') or item.get('qvt') or '').strip() or None,
            tags=['Sling Subscription'],
        )

    def _fetch_epg_for_channel(self, channel_guid: str, max_windows: int) -> list[ProgramData]:
        return self._fetch_epg_for_channel_with_session(channel_guid, max_windows, self.session)

    def _fetch_epg_for_channel_with_session(self, channel_guid: str, max_windows: int, session) -> list[ProgramData]:
        url = self._channel_schedule_url(channel_guid)
        seen_urls: set[str] = set()
        programs: dict[tuple[str, str], ProgramData] = {}
        first = True

        for _ in range(max_windows):
            if not url or url in seen_urls:
                break
            seen_urls.add(url)

            resp = session.get(url, timeout=15)
            if first:
                resp.raise_for_status()
                first = False
            elif not resp.ok:
                if resp.status_code != 404:
                    logger.debug("[%s] EPG window %s returned %s, stopping", self.source_name, url, resp.status_code)
                break
            payload = resp.json()
            playback = payload.get("playback_info") or {}
            asset = playback.get("asset") or {}
            program = self._program_from_asset(channel_guid, asset, payload)
            if program is not None:
                key = (program.source_channel_id, program.start_time.isoformat())
                programs[key] = program
            url = payload.get("_next")

        return sorted(programs.values(), key=lambda p: p.start_time)


    def _channel_from_summary(self, item: dict[str, Any], require_free: bool = True) -> ChannelData | None:
        metadata = item.get("metadata") or {}
        visibility = item.get("visibility") or {}
        channel_guid = (item.get("channel_guid") or item.get("external_id") or "").strip()
        qvt_url = (item.get("qvt_url") or item.get("qvt") or "").strip()
        if not channel_guid or not qvt_url:
            return None
        if not visibility.get("visible", True):
            return None
        if require_free and not metadata.get("is_linear_channel"):
            return None
        if not require_free and metadata.get("is_linear_channel") is False:
            return None
        if require_free and not metadata.get("is_free"):
            return None
        if item.get("cmw_hide_channel"):
            return None

        # Skip internal test/slate channels.  call_sign is the stable internal
        # identifier — display names are sometimes overridden with real channel
        # names (e.g. SLATEPO11 → "Jalsha Movies") which would defeat name-only
        # filtering.  Genre "Test" is a secondary signal for channels that don't
        # follow the SLATEPO/HYBRID-SIGNALTEST naming convention.
        call_sign = metadata.get("call_sign", "")
        genres = metadata.get("genre", [])
        if _TEST_CHANNEL_RE.search(call_sign) or _TEST_CHANNEL_RE.search(item.get("title", "")):
            return None
        if isinstance(genres, list) and any(g.lower() == "test" for g in genres):
            return None

        name = self._best_summary_channel_name(item)
        if not name:
            return None

        logo_url = ((item.get("thumbnail") or {}).get("url"))
        category = self._infer_summary_group(item, name)

        return ChannelData(
            source_channel_id=channel_guid,
            name=name,
            stream_url=f"sling://{channel_guid}",
            logo_url=logo_url,
            slug=self._slugify(name),
            category=category,
            language=infer_language_from_metadata(name, category, metadata.get("language")),
            country="US",
            stream_type="dash",
            number=self._to_int(item.get("channel_number")),
            gracenote_id=str(item.get("gracenote_channel_id") or "").strip() or None,
            guide_key=qvt_url,
            tags=['Sling Freestream'] if require_free else ['Sling Subscription'],
        )

    def _best_summary_channel_name(self, item: dict[str, Any]) -> str | None:
        metadata = item.get("metadata") or {}
        for candidate in (
            metadata.get("channel_name"),
            item.get("network_affiliate_name"),
            item.get("title"),
            metadata.get("call_sign"),
        ):
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return None

    def _infer_summary_group(self, item: dict[str, Any], name: str = "") -> str | None:
        if name:
            inferred = infer_category_from_name(name)
            if inferred:
                return inferred

        metadata = item.get("metadata") or {}
        genres = metadata.get("genre") or []
        if isinstance(genres, list):
            _skip = {"sling free", "freestream", "international"}
            for genre in genres:
                if not isinstance(genre, str) or not genre.strip():
                    continue
                if genre.strip().lower() in _skip:
                    continue
                normalized = normalize_category(genre.strip())
                if normalized:
                    return normalized
        return "Entertainment"

    def _program_from_asset(
        self,
        channel_guid: str,
        asset: dict[str, Any],
        payload: dict[str, Any],
    ) -> ProgramData | None:
        start = self._parse_dt(asset.get("schedule_start"))
        end = self._parse_dt(asset.get("schedule_end"))
        title = asset.get("title") or asset.get("franchise_title")
        if not start or not end or not title:
            return None

        thumbnail = None
        shows = payload.get("shows") or []
        if shows:
            thumbnail = ((shows[0].get("thumbnail") or {}).get("url"))

        genre = asset.get("genre") or asset.get("channel_genre")
        if isinstance(genre, list):
            genre = _join_categories(genre)

        rating = asset.get("rating")
        if isinstance(rating, list):
            rating = ", ".join(x for x in rating if x)

        _season  = self._to_int(asset.get("season_number"))
        _episode = self._to_int(asset.get("episode_number"))
        return ProgramData(
            source_channel_id=channel_guid,
            title=title,
            start_time=start,
            end_time=end,
            description=None,
            poster_url=thumbnail,
            category=genre,
            rating=rating,
            episode_title=asset.get("episode_title"),
            season=_season,
            episode=_episode,
            program_type="episode" if (_season or _episode) else None,
        )

    def _slugify(self, value: str) -> str:
        cleaned = []
        last_dash = False
        for char in value.lower():
            if char.isalnum():
                cleaned.append(char)
                last_dash = False
            elif not last_dash:
                cleaned.append("-")
                last_dash = True
        return "".join(cleaned).strip("-") or "sling"

    def _parse_dt(self, value: str | None) -> datetime | None:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _to_int(self, value: Any) -> int | None:
        try:
            if value in (None, ""):
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

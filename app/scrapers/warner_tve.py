"""
warner_tve.py — Warner Bros Discovery / Turner cable networks: TBS, TNT, truTV.

DRM: Widevine CENC HLS via Adobe Pass Cox + Turner's NGTV token stack. Same
native license-proxy pattern as app/scrapers/directv.py — resolve() returns an
encrypted HLS manifest URL, and the generic license_proxy route in
app/routes/play.py lets a real browser's Shaka player pull a Widevine license.
Full investigation: dev/warner/probe_playback.py and
/home/brad/.claude/plans/scalable-percolating-coral.md.

Key facts established by probing (not derivable from a single HAR capture):

- tntdrama.com/tbs.com/trutv.com are Drupal sites, not Next.js — there is no
  A+E-style JS-bundle scraping needed. Every public watch page embeds a
  plaintext `<script type="application/json" data-drupal-selector=
  "drupal-settings-json">` blob whose `top2` key carries, per brand:
  `softwareStatement` (Adobe), `serviceAppId` (the literal JWT used verbatim
  as the `appId` query param against token.ngtv.io/medium.ngtv.io), and
  `auth.authBrand` (the exact Adobe Pass requestor_id — truTV's is cased
  `truTV`, not `TRUTV`). No login required to read this.
- The same blob publishes a `companyId` per media id — confirmed to be the
  SAME value as the trailing UUID in
  `widevine.license.istreamplanet.com/widevine/api/license/<uuid>` from an
  earlier HAR capture (dev/tnt/README.md), and identical across east/west of
  a brand. I.e. the Widevine license URL is a static, publicly-published
  per-brand constant — NOT a dynamic per-session value, so this scraper uses
  a static per-brand `get_license_url()` lookup (DirecTV-style), not a
  Roku-style cached-per-session URL.
- token.ngtv.io's token_ngtv call needs `accessTokenType=adobe` — the prior
  HAR notes said `turner`, which returns error 2505 "Invalid token format"
  against a live shortAuthorize token (confirmed 2026-08-03).
- medium.ngtv.io's `widevine.cenc` block directly returns a ready-to-fetch
  manifest `url` (no FairPlay-URL substitution needed in practice).
- Real EPG *is* available, just not from `sbp.ngtv.io`/`medium.ngtv.io` (both
  came back empty/scheduleless during the DRM investigation). Each brand's
  public `/schedule` page embeds an iframe pointing at a third-party widget —
  `https://tnets-dvs-schedule.wme-digital.com/?network=<TNT|TBS|TRUTV>` — a
  Next.js app whose `__NEXT_DATA__` blob (server-side rendered, no auth, no
  extra API calls needed) carries a full ~3-week-forward `groupedSchedule`
  with real titles, episode descriptions, ratings, and season/episode
  numbers. Each entry gives the SAME airing's local time in all four US
  timezones — `eastern`/`central`/`mountain` are the same instant (the East
  feed), `pacific` is a separate ~3-hours-later instant (the tape-delayed
  West feed) — confirmed by converting each to UTC. So `eastern` feeds the
  `-east` channels and `pacific` feeds the `-west` channels from ONE fetch
  per brand (not one per channel).
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests

from .base import BaseScraper, ChannelData, ProgramData, null_placeholder_season_episode
from ..models import TVEAccount
from ..tve.adobe_pass import TVEAuthError, TVENotAuthorizedError, authorize_mvpd

SCHEME = 'warner-tve://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'
WIDEVINE_LICENSE_BASE = 'https://widevine.license.istreamplanet.com/widevine/api/license/'

# Third-party schedule widget embedded (via iframe) on each brand's public
# /schedule page — see module docstring. No auth needed; ?network=<code>
# selects the brand.
SCHEDULE_WIDGET_URL = 'https://tnets-dvs-schedule.wme-digital.com/'
SCHEDULE_NETWORK_CODES = {'tnt': 'TNT', 'tbs': 'TBS', 'trutv': 'TRUTV'}
_EAST_TZ = ZoneInfo('America/New_York')
_WEST_TZ = ZoneInfo('America/Los_Angeles')
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', re.S,
)

# top2 config (software statement / appId / companyId / adsProfile) is public,
# unauthenticated, and rarely changes — cache it well past a single scrape/play
# cycle. Mirrors the TTL shape of app/tve/adobe_pass.py's own statement cache.
_CONFIG_CACHE_TTL = 6 * 3600
# medium.ngtv.io manifest URLs embed Yospace SSAI session context; re-resolve
# well inside the ~1hr NGTV token / 8hr ISP JWT TTLs observed during probing.
_MANIFEST_CACHE_TTL = 20 * 60


@dataclass(frozen=True)
class WarnerBrandSite:
    domain: str
    watch_path: str

    @property
    def url(self) -> str:
        return f'https://{self.domain}{self.watch_path}'


BRAND_SITES: dict[str, WarnerBrandSite] = {
    'tnt': WarnerBrandSite(domain='www.tntdrama.com', watch_path='/watchtnt/east'),
    'tbs': WarnerBrandSite(domain='www.tbs.com', watch_path='/watchtbs/east'),
    'trutv': WarnerBrandSite(domain='www.trutv.com', watch_path='/watchtrutv/east'),
}


@dataclass(frozen=True)
class WarnerChannel:
    channel_id: str
    name: str
    brand_key: str
    media_id: str
    logo_url: str
    category: str = 'Entertainment'

    @property
    def stream_url(self) -> str:
        return f'{SCHEME}{self.channel_id}'


CHANNELS: dict[str, WarnerChannel] = {
    'tnt-east': WarnerChannel(
        channel_id='tnt-east', name='TNT (East)', brand_key='tnt', media_id='tnt-east',
        logo_url='https://www.tntdrama.com/themes/custom/ten_theme/images/tnt_logo_white.png',
    ),
    'tnt-west': WarnerChannel(
        channel_id='tnt-west', name='TNT (West)', brand_key='tnt', media_id='tnt-west',
        logo_url='https://www.tntdrama.com/themes/custom/ten_theme/images/tnt_logo_white.png',
    ),
    'tbs-east': WarnerChannel(
        channel_id='tbs-east', name='TBS (East)', brand_key='tbs', media_id='tbs-east',
        logo_url='https://www.tbs.com/themes/custom/ten_theme/images/tbs_logo_white.png',
    ),
    'tbs-west': WarnerChannel(
        channel_id='tbs-west', name='TBS (West)', brand_key='tbs', media_id='tbs-west',
        logo_url='https://www.tbs.com/themes/custom/ten_theme/images/tbs_logo_white.png',
    ),
    'tru-east': WarnerChannel(
        channel_id='tru-east', name='truTV (East)', brand_key='trutv', media_id='tru-east',
        logo_url='https://www.trutv.com/themes/custom/ten_theme/images/trutv_logo_white.png',
    ),
    'tru-west': WarnerChannel(
        channel_id='tru-west', name='truTV (West)', brand_key='trutv', media_id='tru-west',
        logo_url='https://www.trutv.com/themes/custom/ten_theme/images/trutv_logo_white.png',
    ),
}


def channel_for_url(raw_url: str) -> WarnerChannel | None:
    if not raw_url or not raw_url.startswith(SCHEME):
        return None
    return CHANNELS.get(raw_url[len(SCHEME):])


def _fetch_top2_config(domain: str, watch_path: str) -> dict[str, Any] | None:
    """Public, unauthenticated Drupal settings blob — see module docstring."""
    try:
        r = requests.get(f'https://{domain}{watch_path}', headers={'User-Agent': UA}, timeout=20)
    except requests.RequestException:
        return None
    if not r.ok:
        return None
    m = re.search(
        r'<script type="application/json" data-drupal-selector="drupal-settings-json">(.*?)</script>',
        r.text, flags=re.S,
    )
    if not m:
        return None
    try:
        data = json.loads(m.group(1))
    except (ValueError, json.JSONDecodeError):
        return None
    top2 = data.get('top2')
    return top2 if isinstance(top2, dict) else None


def _fetch_widget_schedule(network: str) -> dict[str, Any] | None:
    """Public, unauthenticated Next.js schedule widget — see module docstring.
    Returns the `groupedSchedule` dict (date string -> list of airings) or
    None on any failure."""
    try:
        r = requests.get(
            SCHEDULE_WIDGET_URL, params={'network': network},
            headers={'User-Agent': UA}, timeout=20,
        )
    except requests.RequestException:
        return None
    if not r.ok:
        return None
    m = _NEXT_DATA_RE.search(r.text)
    if not m:
        return None
    try:
        data = json.loads(m.group(1))
    except (ValueError, json.JSONDecodeError):
        return None
    grouped = ((data.get('props') or {}).get('pageProps') or {}).get('groupedSchedule')
    return grouped if isinstance(grouped, dict) else None


def _parse_widget_time(value: str | None, tz) -> datetime | None:
    """Parse e.g. "Mon Aug 03 06:04:00 GMT 2026" / "...PDT 2026". Drops the
    tz-abbreviation token and localizes the naive value with the given
    zoneinfo/timezone ourselves — strptime's %Z handling of non-UTC US
    abbreviations (PDT/EDT/etc) is unreliable across platforms."""
    if not value or not isinstance(value, str):
        return None
    parts = value.split()
    if len(parts) != 6:
        return None
    naive_str = ' '.join(parts[:4] + parts[5:])
    try:
        naive = datetime.strptime(naive_str, '%a %b %d %H:%M:%S %Y')
    except ValueError:
        return None
    return naive.replace(tzinfo=tz).astimezone(timezone.utc)


class WarnerTVEScraper(BaseScraper):
    """Warner Bros Discovery cable networks — TBS, TNT, truTV — resolved via
    Adobe Pass Cox + Turner's NGTV/Widevine-CENC playback stack."""

    source_name = 'warner_tve'
    display_name = 'Warner Bros Discovery TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True
    license_url = WIDEVINE_LICENSE_BASE  # class-level presence only: signals DRM-capable
                                          # to registry.drm_capable_source_names(); the real
                                          # per-brand URL comes from get_license_url() below.
    # Every channel here is Widevine-CENC DRM — keep them out of standard IPTV
    # outputs and route through the PrismCast bridge immediately, matching
    # directv.py rather than waiting for Stream Audit to discover it.
    all_channels_require_drm_bridge = True

    def fetch_channels(self) -> list[ChannelData]:
        return [
            ChannelData(
                source_channel_id=channel.channel_id,
                name=channel.name,
                slug=f'{channel.channel_id}-tve',
                logo_url=channel.logo_url,
                stream_url=channel.stream_url,
                stream_type='hls',
                category=channel.category,
                language='en',
                country='US',
                guide_key=channel.brand_key.upper(),
                description='Warner Bros Discovery TV Everywhere live stream authorized through the configured Cox account.',
            )
            for channel in CHANNELS.values()
        ]

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {ch.source_channel_id for ch in channels}
        brands_needed = {CHANNELS[cid].brand_key for cid in wanted if cid in CHANNELS}

        # One fetch per brand covers both east/west channels (see module
        # docstring) — never fetch per-channel.
        schedules: dict[str, dict] = {
            brand_key: (_fetch_widget_schedule(SCHEDULE_NETWORK_CODES[brand_key]) or {})
            for brand_key in brands_needed
        }

        programs: list[ProgramData] = []
        for channel_id, channel in CHANNELS.items():
            if channel_id not in wanted:
                continue
            parsed = self._parse_widget_schedule(channel, schedules.get(channel.brand_key) or {})
            programs.extend(parsed or self._placeholder_epg(channel))
        null_placeholder_season_episode(programs)
        return programs

    @staticmethod
    def _parse_widget_schedule(channel: WarnerChannel, grouped: dict) -> list[ProgramData]:
        is_west = channel.channel_id.endswith('-west')
        # 'gmt' is the East feed's airing already expressed as an absolute
        # instant (no DST-abbreviation parsing needed); 'pacific' is a
        # separate ~3-hours-later instant for the tape-delayed West feed —
        # see module docstring for how this was confirmed.
        field, tz = ('pacific', _WEST_TZ) if is_west else ('gmt', timezone.utc)

        programs: list[ProgramData] = []
        seen: set[tuple] = set()
        for entries in grouped.values():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                start = _parse_widget_time(entry.get(field), tz)
                if not start:
                    continue
                try:
                    slot_minutes = int(entry.get('slotLength') or 0)
                except (TypeError, ValueError):
                    slot_minutes = 0
                if slot_minutes <= 0:
                    continue
                end = start + timedelta(minutes=slot_minutes)

                show = entry.get('show') or {}
                episode = show.get('episode') or {}
                title = (show.get('programTitle') or '').strip() or channel.name
                episode_title = (episode.get('episodeTitle') or '').strip() or None
                if episode_title == title:
                    episode_title = None
                description = (
                    (episode.get('description') or '').strip()
                    or (show.get('seriesDesc') or '').strip()
                    or None
                )
                season_val = entry.get('seasonNumber')
                episode_val = episode.get('episodeNumber')
                season_raw = str(season_val) if season_val is not None else ''
                episode_raw = str(episode_val) if episode_val is not None else ''
                series_id = show.get('seriesId')

                key = (start, end, title, episode_title or '')
                if key in seen:
                    continue
                seen.add(key)

                programs.append(ProgramData(
                    source_channel_id=channel.channel_id,
                    title=title,
                    description=description,
                    start_time=start,
                    end_time=end,
                    category=channel.category,
                    rating=(episode.get('parentalRating') or '').strip() or None,
                    episode_title=episode_title,
                    season=int(season_raw) if season_raw.isdigit() else None,
                    episode=int(episode_raw) if episode_raw.isdigit() else None,
                    series_id=str(series_id) if series_id else None,
                    is_live=True,
                ))
        programs.sort(key=lambda p: p.start_time)
        return programs

    @staticmethod
    def _placeholder_epg(channel: WarnerChannel) -> list[ProgramData]:
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        return [
            ProgramData(
                source_channel_id=channel.channel_id,
                title=f'{channel.name} Live',
                description=f'Live {channel.name} TV Everywhere programming.',
                start_time=now + timedelta(hours=i),
                end_time=now + timedelta(hours=i + 1),
                category=channel.category,
                is_live=True,
            )
            for i in range(24)
        ]

    # ── brand config (software statement / appId / companyId / adsProfile) ──

    def _brand_config(self, brand_key: str) -> dict[str, Any]:
        all_cfg = self.cache.get('warner_brand_config') or {}
        cached = all_cfg.get(brand_key) if isinstance(all_cfg, dict) else None
        if cached and (time.time() - float(cached.get('cached_at', 0))) < _CONFIG_CACHE_TTL:
            return cached

        site = BRAND_SITES[brand_key]
        top2 = _fetch_top2_config(site.domain, site.watch_path)
        if not top2:
            if cached:
                return cached  # stale beats nothing
            raise RuntimeError(f'Warner TVE: could not fetch config for brand {brand_key}')

        entry = {
            'requestor_id': (top2.get('auth') or {}).get('authBrand') or brand_key.upper(),
            'software_statement': top2.get('softwareStatement') or '',
            'app_id': top2.get('serviceAppId') or '',
            'media': {
                media_id: {'company_id': cfg.get('companyId'), 'ads_profile': cfg.get('adsProfile')}
                for media_id, cfg in top2.items()
                if isinstance(cfg, dict) and cfg.get('companyId')
            },
            'cached_at': time.time(),
        }
        if not entry['requestor_id'] or not entry['software_statement'] or not entry['app_id']:
            if cached:
                return cached
            raise RuntimeError(f'Warner TVE: incomplete config discovered for brand {brand_key}')

        updated = dict(all_cfg) if isinstance(all_cfg, dict) else {}
        updated[brand_key] = entry
        self._update_cache('warner_brand_config', updated)
        return entry

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        channel = CHANNELS.get(channel_id or '')
        if not channel:
            return cls.license_url
        media_cfg = ((config.get('warner_brand_config') or {}).get(channel.brand_key) or {}).get('media', {})
        company_id = (media_cfg.get(channel.media_id) or {}).get('company_id')
        if not company_id:
            return cls.license_url
        return WIDEVINE_LICENSE_BASE + company_id

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs,
    ) -> tuple[bytes, dict]:
        # Confirmed via a real HAR capture of a successful license exchange on
        # tbs.com (dev/tbs/1.har): the site's player sends the ISP token
        # (fetched from token.ngtv.io/token/token_isp during resolve(), cached
        # alongside the manifest URL below) as a plain `X-ISP-Token` header —
        # no Bearer prefix, no JSON envelope. Omitting it gets a real,
        # well-formed challenge rejected with HTTP 403 ErrorCode 8008.
        headers = dict(cls.license_request_headers(config))
        manifest_cache = (config.get('warner_manifest') or {}).get(channel_id or '') or {}
        isp_token = manifest_cache.get('isp_token')
        if isp_token:
            headers['X-ISP-Token'] = isp_token
        return challenge, headers

    # ── playback resolve ─────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        # Every channel here is intrinsically DRM (Widevine CENC) even though
        # the resolved manifest looks like ordinary HLS to the generic audit —
        # the #EXT-X-KEY/CENC markers live in the child variant playlists, not
        # the top-level Yospace-wrapped master manifest resolve() returns, so
        # the audit's manifest inspection was misreading these as clear HLS
        # and clearing requires_drm_bridge. Validate entitlement via
        # resolve(), then return the opaque URL sentinel so the audit skips
        # manifest inspection entirely — same pattern as directv.py.
        if raw_url.startswith(SCHEME):
            self.resolve(raw_url)
            return raw_url
        return raw_url

    def resolve(self, raw_url: str) -> str:
        channel = channel_for_url(raw_url)
        if not channel:
            raise ValueError(f'Unsupported Warner TVE stream URL: {raw_url}')

        cached_manifest = self.cache.get('warner_manifest') or {}
        cached = cached_manifest.get(channel.channel_id) if isinstance(cached_manifest, dict) else None
        if cached and (time.time() - float(cached.get('cached_at', 0))) < _MANIFEST_CACHE_TTL:
            return cached['url']

        account = TVEAccount.query.filter_by(provider_id='cox').first()
        if not account or not account.is_enabled or not account.has_credentials():
            raise TVEAuthError('TVE credentials are not configured in Settings.')

        brand_cfg = self._brand_config(channel.brand_key)
        site = BRAND_SITES[channel.brand_key]

        try:
            adobe_token, session = authorize_mvpd(
                account,
                requestor_id=brand_cfg['requestor_id'],
                resource=brand_cfg['requestor_id'],
                software_statement=brand_cfg['software_statement'],
                redirect_url=site.url,
            )
        except TVENotAuthorizedError as exc:
            raise TVENotAuthorizedError(f'Warner TVE: MVPD is not authorized for {channel.brand_key}: {exc}') from exc
        except TVEAuthError as exc:
            raise TVEAuthError(f'Warner TVE: Adobe Pass auth failed for {channel.brand_key}: {exc}') from exc

        manifest_url, isp_token = self._resolve_manifest(session, brand_cfg, channel, adobe_token)
        if not manifest_url:
            raise RuntimeError(f'Warner TVE: could not resolve manifest for {channel.channel_id}')

        manifests = dict(cached_manifest) if isinstance(cached_manifest, dict) else {}
        manifests[channel.channel_id] = {
            'url': manifest_url, 'isp_token': isp_token, 'cached_at': time.time(),
        }
        self._update_cache('warner_manifest', manifests)
        return manifest_url

    @staticmethod
    def _resolve_manifest(
        session: requests.Session, brand_cfg: dict, channel: WarnerChannel, adobe_token: str,
    ) -> tuple[str | None, str | None]:
        app_id = brand_cfg['app_id']
        media_cfg = brand_cfg.get('media', {}).get(channel.media_id) or {}

        # Validation/gate step observed in the real auth chain — not consumed
        # downstream (medium.ngtv.io and token_isp both authenticate with the
        # Adobe shortAuthorize token directly, not this token's value), but
        # kept in sequence since that's the proven-working order.
        ngtv = session.get(
            'https://token.ngtv.io/token/token_ngtv',
            params={'appId': app_id, 'accessTokenType': 'adobe', 'accessToken': adobe_token,
                    'fname': 'ngtv', 'format': 'json'},
            timeout=20,
        )
        if ngtv.status_code != 200:
            return None, None
        try:
            ngtv_data = ngtv.json()
        except ValueError:
            return None, None
        if not ((ngtv_data.get('auth') or {}).get('ngtv_token')):
            return None, None

        ssai_profile = media_cfg.get('ads_profile') or ''
        desktop = session.get(
            f'https://medium.ngtv.io/v2/media/{channel.media_id}/desktop',
            params={'appId': app_id, 'ssaiProfile': ssai_profile},
            timeout=20,
        )
        if desktop.status_code != 200:
            return None, None
        try:
            desktop_data = desktop.json()
        except ValueError:
            return None, None

        media_data = ((desktop_data.get('media') or {}).get('desktop')) or {}
        widevine = media_data.get('widevine', {}).get('cenc', {}) if isinstance(media_data.get('widevine'), dict) else {}
        fairplay = media_data.get('fairplay', {}).get('cbcs', {}) if isinstance(media_data.get('fairplay'), dict) else {}
        asset_id = widevine.get('assetId') or fairplay.get('assetId')
        manifest_url = widevine.get('url')
        if not manifest_url and fairplay.get('url'):
            # Not observed in practice (widevine.cenc.url has always been present
            # directly), but kept as a fallback per the original HAR investigation.
            manifest_url = fairplay['url'].replace('-cbcs', '-cenc').replace('_fp_', '_wv_')
        if not manifest_url:
            return None, None

        isp_token = None
        if asset_id:
            isp = session.get(
                'https://token.ngtv.io/token/token_isp',
                params={'assetId': asset_id, 'appId': app_id, 'format': 'json',
                        'mediaId': channel.media_id, 'accessToken': adobe_token,
                        'accessTokenType': 'adobe'},
                timeout=20,
            )
            # This JWT is NOT needed to build the manifest URL (widevine.cenc.url
            # already resolves and plays without it), but it IS required by the
            # license server as the `X-ISP-Token` header — confirmed via a real
            # HAR capture of a successful license exchange (dev/tbs/1.har).
            if isp.status_code == 200:
                try:
                    isp_token = isp.json().get('jwt')
                except ValueError:
                    isp_token = None

        return manifest_url, isp_token

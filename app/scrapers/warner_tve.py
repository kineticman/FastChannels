"""
warner_tve.py — Warner Bros Discovery / Turner cable networks: TBS, TNT,
truTV and TCM. TCM signs in through Adobe Pass v2 and has its own public
Next.js page config / GraphQL guide.

Playback: plain AES-128 HLS, no DRM (verified 2026-10-26 — the same feeds
Channels DVR's TVE plays). For TBS/TNT/truTV, Turner's older v1 media API on
the `tv` platform (medium.ngtv.io/media/<id>/tv, not the v2 API the website
uses) returns a `bulkaes` rendition on tve-live-aka.warnermediacdn.com. TCM
isn't in the v1 API, so its masters on turnerlive.akamaized.net are fixed
URLs, as Channels DVR has them. token.ngtv.io/token/token_spe swaps the Adobe
token for an Akamai `hdnts` for the master. The master's variant, key and
segment URLs then carry an `hdntl` token whose HMAC covers its literal text,
lowercase `%2f` included — python-requests, urllib3 and ffmpeg all rewrite
that to `%2F` and get a 403. So these channels play through
app/routes/warner_proxy.py, which fetches every upstream URL byte for byte,
rather than a 302.

The websites themselves play Widevine CENC HLS from the v2 media API
(medium.ngtv.io/v2/media/<id>/desktop, license at
widevine.license.istreamplanet.com with an X-ISP-Token header). This scraper
played that, bridge-only, until 2026-10; the complete, verified Widevine path
is in commit 38ae38c if Turner ever drops the AES feeds. Full investigation:
dev/warner/probe_playback.py, dev/hitlist/sources_todo.json
(warner_tve_direct_claim) and
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
- token.ngtv.io calls need `accessTokenType=adobe` — old HAR notes said
  `turner`, which returns error 2505 "Invalid token format" against a live
  shortAuthorize token (confirmed 2026-08-03).
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

import base64
import json
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import requests

from .base import BaseScraper, ChannelData, ProgramData, null_placeholder_season_episode
from ..gracenote_map import resolve_gracenote
from ..models import TVEAccount
from ..tve.accounts import tve_account_for
from ..tve.adobe_pass import (MvpdCooldownMixin, TVEAuthError, TVENotAuthorizedError,
                              authorize_mvpd, save_adobe_client_creds)

SCHEME = 'warner-tve://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'

# Third-party schedule widget embedded (via iframe) on each brand's public
# /schedule page — see module docstring. No auth needed; ?network=<code>
# selects the brand.
SCHEDULE_WIDGET_URL = 'https://tnets-dvs-schedule.wme-digital.com/'
SCHEDULE_NETWORK_CODES = {'tnt': 'TNT', 'tbs': 'TBS', 'trutv': 'TRUTV'}
TCM_LIVE_URL = 'https://www.tcm.com/watchtcm/livestream'
TCM_GUIDE_URL = 'https://wme-gep-graphql-prod.wme-digital.com/graphql'
# The site's public Apollo persisted queries use a different hash per feed.
TCM_GUIDE_HASHES = {
    'tcm-east': '93c7a879d46f6a788dcdba08f79b670a09d2fa09eadb5e04506d26768489e5c7',
    'tcm-west': '0d063c0ae8c94dc525fe9eac45ffff42a34acaa8c3b9fa048dbf8e562dc968fc',
}
_EAST_TZ = ZoneInfo('America/New_York')
_WEST_TZ = ZoneInfo('America/Los_Angeles')
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', re.S,
)

# top2 config (software statement / appId) is public, unauthenticated, and
# rarely changes — cache it well past a single scrape/play cycle. Mirrors the
# TTL shape of app/tve/adobe_pass.py's own statement cache.
_CONFIG_CACHE_TTL = 6 * 3600
# token_spe's hdnts lasts 300s and is only needed to fetch the master; the
# master hands out its own ~8h hdntl for everything below it.
_MANIFEST_CACHE_TTL = 4 * 60

NGTV_MEDIA_V1 = 'https://medium.ngtv.io/media'
NGTV_TOKEN_SPE = 'https://token.ngtv.io/token/token_spe'
AES_TOKEN_PATH = '/hls/live/*'
# Playlists and segments come from tve-live-aka.warnermediacdn.com, AES keys
# from turnerlive.akamaized.net.
AES_KEY_HOST = 'turnerlive.akamaized.net'
AES_CDN_SUFFIX = '.warnermediacdn.com'


def aes_cdn_url_allowed(url: str) -> bool:
    parts = urlsplit(url)
    host = (parts.hostname or '').lower()
    return parts.scheme == 'https' and (host == AES_KEY_HOST or host.endswith(AES_CDN_SUFFIX))


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
    'tcm': WarnerBrandSite(domain='www.tcm.com', watch_path='/watchtcm/livestream'),
}


@dataclass(frozen=True)
class WarnerChannel:
    channel_id: str
    name: str
    brand_key: str
    media_id: str
    logo_url: str
    category: str = 'Entertainment'
    # AES master for feeds the v1 media API doesn't list (TCM).
    master_url: str | None = None

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
    'tcm-east': WarnerChannel(
        channel_id='tcm-east', name='TCM (East)', brand_key='tcm', media_id='tcm-east',
        logo_url='https://static.tcm.com/2025-11/legacy-watch-tcm-logo.png', category='Movies',
        master_url=f'https://{AES_KEY_HOST}/hls/live/2023186/tcmeast/noslate/master.m3u8',
    ),
    'tcm-west': WarnerChannel(
        channel_id='tcm-west', name='TCM (West)', brand_key='tcm', media_id='tcm-west',
        logo_url='https://static.tcm.com/2025-11/legacy-watch-tcm-logo.png', category='Movies',
        master_url=f'https://{AES_KEY_HOST}/hls/live/2023187/tcmwest/noslate/master.m3u8',
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


def _fetch_tcm_schedule(channel_id: str) -> list[dict]:
    now = datetime.now(timezone.utc) - timedelta(hours=2)
    params = {
        'operationName': 'getListings',
        'variables': json.dumps({
            'feed': ['tcm' if channel_id == 'tcm-east' else 'tcm-west'],
            'brand': 'tcm', 'startDate': now.isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
            'count': 40,
        }, separators=(',', ':')),
        'extensions': json.dumps({'persistedQuery': {
            'version': 1, 'sha256Hash': TCM_GUIDE_HASHES[channel_id],
        }}, separators=(',', ':')),
    }
    try:
        response = requests.get(TCM_GUIDE_URL, params=params, headers={'User-Agent': UA}, timeout=20)
        response.raise_for_status()
        entries = (response.json().get('data') or {}).get('getScheduleEntries') or []
        return entries if isinstance(entries, list) else []
    except (requests.RequestException, ValueError, AttributeError):
        return []


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


class WarnerTVEScraper(MvpdCooldownMixin, BaseScraper):
    """TBS, TNT, truTV and TCM via Adobe Pass + NGTV AES-128."""

    source_name = 'warner_tve'
    display_name = 'Warner Bros Discovery TVE'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 720
    stream_audit_enabled = True

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
                description=None,
                gracenote_id=resolve_gracenote('warner_tve', lookup_key=channel.channel_id),
            )
            for channel in CHANNELS.values()
        ]

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        wanted = {ch.source_channel_id for ch in channels}
        brands_needed = {CHANNELS[cid].brand_key for cid in wanted
                         if cid in CHANNELS and CHANNELS[cid].brand_key != 'tcm'}

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
            if channel.brand_key == 'tcm':
                parsed = self._parse_tcm_schedule(channel, _fetch_tcm_schedule(channel_id))
            else:
                parsed = self._parse_widget_schedule(channel, schedules.get(channel.brand_key) or {})
            programs.extend(parsed or self._placeholder_epg(channel))
        null_placeholder_season_episode(programs)
        return programs

    @staticmethod
    def _parse_tcm_schedule(channel: WarnerChannel, entries: list[dict]) -> list[ProgramData]:
        programs = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                start = datetime.fromisoformat(entry['scheduledTimestamp'].replace('Z', '+00:00'))
                duration = int(entry['scheduledDuration'])
            except (KeyError, ValueError, TypeError):
                continue
            if duration <= 0:
                continue
            show = entry.get('show') or {}
            title = (((show.get('title') or {}).get('en_US') or {}).get('full') or '').strip()
            summary = entry.get('summary') or {}
            programs.append(ProgramData(
                source_channel_id=channel.channel_id,
                title=title or channel.name,
                description=summary.get('full') or summary.get('short') or None,
                start_time=start,
                end_time=start + timedelta(seconds=duration),
                category=entry.get('category') or channel.category,
                rating=entry.get('ratingCode') or None,
                is_live=True,
            ))
        programs.sort(key=lambda p: p.start_time)
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

    # ── brand config (requestor / software statement / appId) ───────────────

    def _brand_config(self, brand_key: str) -> dict[str, Any]:
        all_cfg = self.cache.get('warner_brand_config') or {}
        cached = all_cfg.get(brand_key) if isinstance(all_cfg, dict) else None
        if cached and (time.time() - float(cached.get('cached_at', 0))) < _CONFIG_CACHE_TTL:
            return cached

        if brand_key == 'tcm':
            return self._tcm_config(cached, all_cfg)

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

    def _tcm_config(self, cached: dict | None, all_cfg: dict) -> dict[str, Any]:
        try:
            response = requests.get(TCM_LIVE_URL, headers={'User-Agent': UA}, timeout=20)
            response.raise_for_status()
            match = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', response.text, re.S)
            if not match:
                raise ValueError('Next.js page data missing')
            page = json.loads(match.group(1))['props']['pageProps']
            player = page['siteConfig']['VIDEOPLAYER']
            auth = player['auth']
            entry = {
                'requestor_id': auth['brand'],
                'software_statement': auth.get('softwareStatementV2') or auth['softwareStatement'],
                'app_id': player['mediaAppId'],
                'cached_at': time.time(),
            }
            if entry['requestor_id'] != 'TCM' or not all(entry[k] for k in ('software_statement', 'app_id')):
                raise ValueError('incomplete TCM page config')
        except (requests.RequestException, ValueError, KeyError, TypeError):
            if cached:
                return cached
            raise RuntimeError('Warner TVE: could not discover TCM page config')
        updated = dict(all_cfg) if isinstance(all_cfg, dict) else {}
        updated['tcm'] = entry
        self._update_cache('warner_brand_config', updated)
        return entry

    # ── playback resolve ─────────────────────────────────────────────────────

    def audit_resolve(self, raw_url: str) -> str:
        # The variant playlists 403 for the audit's requests session (it
        # rewrites the hdntl token; see module docstring), so a generic
        # manifest check would read every channel as broken. Validate
        # entitlement via resolve(), then return the opaque URL sentinel so
        # the audit skips manifest inspection — same pattern as directv.py.
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

        account = tve_account_for(self.source_name)
        if not account or not account.is_usable():
            raise TVEAuthError('TVE credentials are not configured in Settings.')

        brand_cfg = self._brand_config(channel.brand_key)
        site = BRAND_SITES[channel.brand_key]

        try:
            if channel.brand_key == 'tcm':
                adobe_token, session = self._tcm_adobe_token(account, brand_cfg)
            else:
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
            # See fox_tve.py's _fox_sports_access_token() for why this also
            # needs the per-network status — authorize_mvpd() never touches
            # TVEAccount.last_auth_message either, so without this an
            # auth failure here is invisible everywhere, not just the
            # per-network banner.
            try:
                from ..tve.browser_login.common import _record_tve_login_error
                _record_tve_login_error(brand_cfg['requestor_id'], str(exc)[:300])
            except Exception:  # noqa: BLE001
                pass
            raise TVEAuthError(f'Warner TVE: Adobe Pass auth failed for {channel.brand_key}: {exc}') from exc

        manifest_url = self._resolve_manifest(session, brand_cfg, channel, adobe_token)
        if not manifest_url:
            raise RuntimeError(f'Warner TVE: could not resolve manifest for {channel.channel_id}')

        manifests = dict(cached_manifest) if isinstance(cached_manifest, dict) else {}
        manifests[channel.channel_id] = {'url': manifest_url, 'cached_at': time.time()}
        self._update_cache('warner_manifest', manifests)
        return manifest_url

    def _tcm_adobe_token(self, account: TVEAccount, brand_cfg: dict) -> tuple[str, requests.Session]:
        """TCM uses Adobe Pass v2; its decision token is base64 on the wire.

        The TCM player base64-decodes `serializedToken` before giving it to
        token.ngtv.io. Confirmed against the successful East/West HAR capture.
        """
        from ..extensions import db
        from .nbc_tve import AdobePassV2Client

        cfg = account.config or {}
        mso_id = (cfg.get('yt_dlp_mso_id') or cfg.get('selected_mso_id') or 'Cox').strip()
        fingerprint = self.config.get('tcm_device_fingerprint')
        if not fingerprint:
            fingerprint = str(uuid.uuid4())
            self._update_config('tcm_device_fingerprint', fingerprint)

        saved = (cfg.get('adobe_client_creds') or {}).get('TCM') or {}
        client = AdobePassV2Client(
            'TCM', brand_cfg['software_statement'], TCM_LIVE_URL, fingerprint,
            client_creds=saved or None, origin='https://www.tcm.com',
        )
        if saved and time.time() - float(saved.get('captured_at') or 0) >= 5 * 3600:
            client.client_id = saved.get('client_id')
            client.client_secret = saved.get('client_secret')
            if client.client_id and client.client_secret:
                client.refresh_access_token()
                client._client_creds = {
                    **saved, 'access_token': client.access_token,
                }

        client.authorize(mso_id, account.username or '', account.password or '', cfg.get('xfinity_cookie_jar'))
        save_adobe_client_creds(account, 'TCM', client.client_id, client.client_secret, client.access_token)
        response = client._post(
            f'https://sp.auth.adobe.com/api/v2/TCM/decisions/authorize/{mso_id}',
            json={'resources': ['TCM']},
            headers={**client._bearer_headers(), 'Content-Type': 'application/json'},
        )
        decision = next((d for d in (response.json().get('decisions') or [])
                         if d.get('resource') == 'TCM'), None)
        if not decision or not decision.get('authorized'):
            raise TVENotAuthorizedError(f'TCM: {mso_id} account is not entitled to TCM.')
        serialized = ((decision.get('token') or {}).get('serializedToken') or '')
        try:
            adobe_token = base64.b64decode(serialized, validate=True).decode('utf-8')
        except (ValueError, UnicodeDecodeError) as exc:
            raise TVEAuthError('TCM: Adobe did not return a usable decision token.') from exc
        if not adobe_token:
            raise TVEAuthError('TCM: Adobe did not return a decision token.')

        new_cfg = dict(account.config or {})
        new_cfg['tcm_mvpd_auth'] = {'mso_id': mso_id, 'captured_at': int(time.time())}
        account.config = new_cfg
        db.session.commit()
        return adobe_token, client.session

    @staticmethod
    def _resolve_manifest(
        session: requests.Session, brand_cfg: dict, channel: WarnerChannel, adobe_token: str,
    ) -> str | None:
        """The AES-128 master URL, with its hdnts appended verbatim."""
        app_id = brand_cfg['app_id']
        master_url = channel.master_url
        if not master_url:
            media = session.get(f'{NGTV_MEDIA_V1}/{channel.media_id}/tv', params={'appId': app_id}, timeout=20)
            if media.status_code != 200:
                return None
            try:
                bulkaes = ((media.json().get('media') or {}).get('tv') or {}).get('bulkaes') or {}
            except ValueError:
                return None
            master_url = bulkaes.get('secureUrl')
        if not master_url or not aes_cdn_url_allowed(master_url):
            return None

        spe = session.get(
            NGTV_TOKEN_SPE,
            params={'appId': app_id, 'accessToken': adobe_token, 'accessTokenType': 'adobe',
                    'format': 'json', 'path': AES_TOKEN_PATH},
            timeout=20,
        )
        if spe.status_code != 200:
            return None
        try:
            token = (spe.json().get('auth') or {}).get('token')
        except ValueError:
            return None
        if not token:
            return None
        return f'{master_url}?hdnts={token}'

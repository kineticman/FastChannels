"""
reelz_tve.py — the REELZ linear channel from reelzplus.com, with a REELZ+
account that has a TV provider linked to it. Scoped 2026-10-26 from a
signed-in HAR (dev/hitlist/hars/reelzplus.com.har).

Key facts established by probing:

- reelzplus.com is a Katapy site (server-rendered PHP pages, no JSON API for
  playback). Access is either a paid REELZ+ subscription or a linked TV
  provider. Katapy drives Adobe Pass itself (requestor `reelz`): the site's
  /action/link-provider {tvProvider=<MVPD id>} answers a 302 to
  sp.auth.adobe.com/api/v2/authenticate/reelz/<regcode>, and after the
  provider's SAML round trip /action/link-provider-redirect lands on
  link-provider-success or link-provider-no-access.
- Seen in the HAR: Spectrum → no-access, DIRECTV (ATTOTT) → success.
- The TV-provider route has no user-facing account: Get Access → "Sign in
  with your TV provider" pre-fills a hidden random <hex>@random.com email
  and password, and the only visible step is a reCAPTCHA v2 checkbox before
  /action/subscribe creates that account. The captcha is enforced server-side
  (error 506 "Missing recaptcha" without it), so FastChannels can't create
  the account. Instead the user does that step in a browser and pastes the
  next page's URL, link-provider?mode=linkProvider&token=<64 hex>: the token
  is the account's `reelz-u` session cookie (1 year), and it works as the
  cookie from any client. Opening that URL without the cookie does not sign
  in, so the token is set as a cookie directly.
- Paid REELZ+ subscribers have a real email/password instead. Sign-in
  (/action/sign-in, a plain form post) has no captcha. It always sets a
  `reelz-u` cookie; a good one redirects to /login-success, a bad one to
  /login-error.
- /live-tv/263-reelz is the gate: signed in with access it is a 200 whose
  player script carries `var hlsUrl = "..."`; without access it is a 302 to
  /tv-guide. resolve() reads the URL from that page, so a lost entitlement
  stops playback here even though the CDN itself does not check.
- The stream is cdn.katapy.io/cache/stream/reelztv/reelztv/playlist.m3u8:
  clear HLS (no EXT-X-KEY), up to 1280x720 H.264/AAC with CC1, no token,
  no Referer needed. Playback is a plain 302.
- Guide: /tv-guide/263-reelz is public HTML with ~3 days of programmes
  (epoch data-start, series, episode title, description). The series names
  are upper case on the page. Gracenote 68385 (ReelzChannel HD, as DirecTV
  carries it) is attached too.
"""
from __future__ import annotations

import html
import logging
import re
import threading
import time
from datetime import datetime, timedelta, timezone

import requests

from .base import BaseScraper, ChannelData, ConfigField, ProgramData
from ..gracenote_map import resolve_gracenote
from ..tve.adobe_pass import TVEAuthError

logger = logging.getLogger(__name__)

SCHEME = 'reelz://'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
SITE = 'https://reelzplus.com'
BASE = f'{SITE}/reelz-en'
SIGN_IN_URL = f'{BASE}/action/sign-in'
CHANNEL_ID = '263'
CHANNEL_SLUG = '263-reelz'
LIVE_URL = f'{BASE}/live-tv/{CHANNEL_SLUG}'
GUIDE_URL = f'{BASE}/tv-guide/{CHANNEL_SLUG}'
SESSION_COOKIE = 'reelz-u'
_LOGO = 'https://cdn.katapy.io/ktdata3/7846/img/160717_o.PNG'
_GRACENOTE = '68385'

_URL_TTL = 30 * 60         # reuse a checked stream URL this long before re-reading the live page
_LAST_SLOT = timedelta(minutes=30)  # the final guide row has no next start

_HLS_RE = re.compile(r'var\s+hlsUrl\s*=\s*"([^"]+)"')
_TOKEN_RE = re.compile(r'(?:token=|^\s*)([0-9a-fA-F]{64})\b')
_ROW_RE = re.compile(
    r'live-event-row"\s+data-start="(\d+)".*?epg-row-item-nm">(.*?)</h2>'
    r'.*?epg-row-item-num">(.*?)</h3>.*?epg-item-body[^"]*">(.*?)</div>',
    re.S,
)
_TAG_RE = re.compile(r'<[^>]+>')
_WS_RE = re.compile(r'\s+')

_SMALL_WORDS = {'a', 'an', 'and', 'as', 'at', 'by', 'for', 'in', 'of', 'on', 'or', 'the', 'to'}
_KEEP_UPPER = {'COPS'}

# One checked stream URL per process, shared by the play workers.
_url_lock = threading.Lock()
_url_cache: dict = {}


def _text(fragment: str) -> str:
    return _WS_RE.sub(' ', html.unescape(_TAG_RE.sub(' ', fragment))).strip()


def _series_title(raw: str) -> str:
    """Title-case the guide's upper-case series names ('UNTOLD STORIES OF
    THE E.R.' → 'Untold Stories of the E.R.'); mixed-case names pass through."""
    if not raw.isupper():
        return raw
    words = []
    for i, word in enumerate(raw.split(' ')):
        if word in _KEEP_UPPER or '.' in word:
            words.append(word)
        elif i and word.lower() in _SMALL_WORDS:
            words.append(word.lower())
        else:
            words.append(word.capitalize() if '&' not in word else word.title())
    return ' '.join(words)


class ReelzTVEScraper(BaseScraper):
    """REELZ (24/7) via reelzplus.com with a TV provider linked to a REELZ+ account."""

    source_name = 'reelz_tve'
    display_name = 'REELZ'
    source_category = 'tve'
    is_premium = True
    scrape_interval = 360  # the guide page reaches ~3 days ahead
    stream_audit_enabled = True
    audit_requires_config = [('session_link', 'email')]

    config_schema = [
        ConfigField('session_link', 'REELZ+ link URL', field_type='password', secret=True,
                    placeholder='https://reelzplus.com/reelz-en/link-provider?mode=linkProvider&token=…',
                    help_text='For TV-provider access: at reelzplus.com choose Get Access → "Sign in with your '
                              'TV provider" and tick the captcha. On the next page (choose your TV provider), '
                              'copy the address from the address bar and paste it here, then finish signing in '
                              'with your provider on that page.'),
        ConfigField('email', 'REELZ+ email (subscribers)', placeholder='you@example.com',
                    help_text='Only for a paid REELZ+ subscription. Leave empty when using the link URL above.'),
        ConfigField('password', 'REELZ+ password (subscribers)', field_type='password', secret=True),
    ]

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.session.headers.update({'User-Agent': UA})

    # ── channels + guide ─────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        return [ChannelData(
            source_channel_id=CHANNEL_ID,
            name='REELZ',
            slug='reelz-tve-reelz',
            logo_url=_LOGO,
            stream_url=f'{SCHEME}{CHANNEL_SLUG}',
            stream_type='hls',
            category='Entertainment',
            language='en',
            country='US',
            gracenote_id=resolve_gracenote('reelz_tve', lookup_key=CHANNEL_ID) or _GRACENOTE,
        )]

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        try:
            r = self.session.get(GUIDE_URL, timeout=30)
            r.raise_for_status()
            page = r.text
        except requests.RequestException as exc:
            logger.warning('[reelz_tve] guide fetch failed: %s', exc)
            return []

        rows = []
        for start, series, episode, desc in _ROW_RE.findall(page):
            title = _series_title(_text(series))
            if title:
                rows.append((int(start), title, _text(episode) or None, _text(desc) or None))
        rows.sort(key=lambda row: row[0])

        programs = []
        for ch in channels:
            for i, (start, title, episode, desc) in enumerate(rows):
                begin = datetime.fromtimestamp(start, timezone.utc)
                end = (datetime.fromtimestamp(rows[i + 1][0], timezone.utc)
                       if i + 1 < len(rows) else begin + _LAST_SLOT)
                if end <= begin:
                    continue
                programs.append(ProgramData(
                    source_channel_id=ch.source_channel_id,
                    title=title,
                    start_time=begin,
                    end_time=end,
                    episode_title=episode,
                    description=desc,
                ))
        logger.info('[reelz_tve] %d guide entries', len(programs))
        return programs

    # ── session ──────────────────────────────────────────────────────────────

    def _credentials(self) -> tuple[str, str] | None:
        email = (self.config.get('email') or '').strip()
        password = self.config.get('password') or ''
        return (email, password) if email and password else None

    def _link_token(self) -> str | None:
        """The session token from the pasted link URL (or a bare token)."""
        m = _TOKEN_RE.search(self.config.get('session_link') or '')
        return m.group(1).lower() if m else None

    def _sign_in(self, creds: tuple[str, str]) -> str:
        email, password = creds
        self.session.cookies.clear()
        r = self.session.post(
            SIGN_IN_URL, timeout=20, allow_redirects=False,
            data={'username': email, 'password': password, 'remember-me': 'true'},
            headers={'Origin': SITE, 'Referer': f'{BASE}/signin'},
        )
        # A failed sign-in also sets a (guest) reelz-u cookie; only the
        # redirect tells them apart: login-success?token=… vs login-error.
        token = r.cookies.get(SESSION_COOKIE)
        if not token or 'login-success' not in (r.headers.get('Location') or ''):
            raise TVEAuthError('REELZ: sign-in failed — check the REELZ+ email and password.')
        self._update_config('session_token', token)
        logger.info('[reelz_tve] signed in')
        return token

    def _live_page(self, token: str) -> requests.Response:
        self.session.cookies.set(SESSION_COOKIE, token, domain='reelzplus.com', path='/')
        return self.session.get(LIVE_URL, timeout=20, allow_redirects=False,
                                headers={'Referer': f'{BASE}/tv-guide'})

    def _stream_url(self) -> str:
        """The stream URL from the live page, trying the pasted link's
        session, then the session from the last sign-in, then signing in
        again (subscribers only)."""
        link, creds = self._link_token(), self._credentials()
        if not link and not creds:
            raise TVEAuthError('REELZ: paste your REELZ+ link URL (or a subscriber email and password) '
                               'in the source settings.')
        r = None
        for token in dict.fromkeys(t for t in (link, self.config.get('session_token')) if t):
            r = self._live_page(token)
            if r.status_code == 200:
                break
        if (r is None or r.status_code != 200) and creds:
            r = self._live_page(self._sign_in(creds))
        if r.status_code in (301, 302, 303):
            if creds:
                raise TVEAuthError('REELZ: signed in, but this REELZ+ account has no access to the live channel.')
            raise TVEAuthError(
                'REELZ: the REELZ+ link has no access — either the TV-provider sign-in on reelzplus.com was not '
                'finished, your provider does not include REELZ, or the link expired. Run Get Access → '
                '"Sign in with your TV provider" again and paste the new link URL.')
        r.raise_for_status()
        m = _HLS_RE.search(r.text)
        if not m:
            raise RuntimeError('REELZ: the live page no longer carries a stream URL (site layout changed?)')
        return m.group(1)

    def audit_preflight(self) -> None:
        # One check up front instead of the audit failing the channel with a
        # vague error when the account has no provider linked.
        self.resolve(f'{SCHEME}{CHANNEL_SLUG}')

    # ── playback ─────────────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        if not raw_url.startswith(SCHEME):
            return raw_url
        key = self._link_token() or self.config.get('email') or ''
        with _url_lock:
            hit = _url_cache.get(key)
            if hit and hit[1] > time.time():
                return hit[0]
        url = self._stream_url()
        with _url_lock:
            _url_cache[key] = (url, time.time() + _URL_TTL)
        return url

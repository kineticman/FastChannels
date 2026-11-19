"""Gizmott (Gizmeon / OTT Studio) white-label FAST apps: Christmas Plus and
Documentary+. One network per app, so users can switch them separately.

api.gizmott.com serves every app; the tenant is picked by request headers
(pubid + channelid; GET /api/v1/config with a `website: <site url>` header
maps a site to them). Scoped 2026-11-18:

- GET /api/v1/schedule/fastchannelsv2?timezone=UTC needs no token and gives
  the whole lineup (ids, names, logos, category ids) plus ~2 days of guide,
  titles only, in the timezone asked for. Without tenant headers it answers
  for some default tenant, so the headers matter.
- GET /api/v1/fastchannel/details/<id> carries the stream (live_link) and
  wants a guest token: POST /account/register/guest (once per tenant, gives
  a user_id), then GET /account/authenticate with `uid` gives a JWT (~90 days).
  The guest identity lives in the source cache and is reused; a new guest is
  registered only when the API no longer knows the stored one.
- live_link is a fixed, unsigned playlist (Amagi, OTT Studio's b-cdn SSAI,
  QVC's Akamai) stuffed with unfilled [MACRO] ad parameters. With those
  dropped every channel plays clear, mostly 1080p, so the cleaned URL is the
  stream ref and play time needs no API call. A changed URL is picked up on
  the next scrape.

Christmas Plus also lists QVC/HSN shopping channels (QVC, QVC 2, QVC 365,
QVC En Español, HSN); they're left out on purpose (_SKIP).
"""
from __future__ import annotations

import base64
import json
import re
import time
import uuid
from datetime import datetime
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from ..base import infer_language
from ._common import UA, UTC, Network, NetChannel, logger, program

_API = 'https://api.gizmott.com/api/v1'
_MACRO = re.compile(r'\[[A-Z0-9_]+\]')
# Take a new token once the stored one has less than this left.
_TOKEN_MARGIN = 7 * 86400

# Category per Gizmott category name, then per channel name (first match wins).
# Channels left out of every tenant: shopping (Christmas Plus carries QVC/HSN).
_SKIP = re.compile(r'\b(qvc|hsn)\b', re.I)
_CATEGORY_BY_GROUP = {
    'lifestyle channels': 'Lifestyle',
    'christmas channels': 'Movies',
    'movies & tv channels': 'Movies',
}
_CATEGORY_BY_NAME = [
    (re.compile(r'yule log', re.I), 'Ambiance'),
    (re.compile(r'santa tracker', re.I), 'Kids'),
    (re.compile(r'cooking', re.I), 'Food'),
    (re.compile(r"sitcom|tv specials", re.I), 'Classic TV'),
    (re.compile(r'popular science', re.I), 'Science'),
    (re.compile(r'simply tv', re.I), 'Entertainment'),
]


def _strip_macros(url: str) -> str:
    """live_link without the [MACRO] placeholder parameters the apps fill in."""
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if not _MACRO.search(v)]
    return urlunsplit(parts._replace(query=urlencode(query)))


def _jwt_exp(token: str) -> float:
    try:
        payload = token.split('.')[1]
        return float(json.loads(base64.urlsafe_b64decode(payload + '=' * (-len(payload) % 4))).get('exp') or 0)
    except (IndexError, ValueError, TypeError):
        return 0


class Gizmott(Network):
    def __init__(self, key: str, name: str, pubid: str, channelid: str, website: str,
                 default_category: str | None = None):
        self.key = key
        self.name = name
        self.pubid = pubid
        self.channelid = channelid
        self.website = website
        self.default_category = default_category

    # ── HTTP ─────────────────────────────────────────────

    def _headers(self, guest: dict | None = None) -> dict:
        h = {'pubid': self.pubid, 'channelid': self.channelid, 'device_type': 'web', 'country_code': 'US',
             'Origin': self.website, 'Referer': self.website + '/', 'User-Agent': UA}
        if guest:
            h.update({'dev_id': guest['dev_id'], 'uid': str(guest['uid'])})
            if guest.get('token'):
                h['access-token'] = guest['token']
        return h

    def _guest(self, s, fresh: bool = False) -> dict:
        """The tenant's guest identity with a usable token, from the source cache
        when it has one; else re-authenticated, or registered as a last resort."""
        cache_key = f'gizmott_guest.{self.key}'
        guest = dict(s.cache.get(cache_key) or {})
        if not fresh and guest.get('token') and _jwt_exp(guest['token']) - time.time() > _TOKEN_MARGIN:
            return guest
        guest.setdefault('dev_id', uuid.uuid4().hex)
        guest['token'] = None
        if guest.get('uid'):
            guest['token'] = self._authenticate(s, guest)
        if not guest.get('token'):
            r = s.session.post(f'{_API}/account/register/guest', data='', timeout=20,
                               headers={**self._headers(), 'dev_id': guest['dev_id'], 'ua': UA})
            r.raise_for_status()
            uid = (r.json() or {}).get('user_id')
            if not uid:
                raise RuntimeError(f'{self.name}: guest registration returned no user_id')
            logger.info('[free_networks] %s: registered Gizmott guest %s', self.name, uid)
            guest['uid'] = uid
            guest['token'] = self._authenticate(s, guest)
            if not guest['token']:
                raise RuntimeError(f'{self.name}: new guest could not authenticate')
        s._update_cache(cache_key, guest)
        return guest

    def _authenticate(self, s, guest: dict) -> str | None:
        r = s.session.get(f'{_API}/account/authenticate', timeout=20,
                          headers=self._headers({**guest, 'token': None}))
        try:
            body = r.json() or {}
        except ValueError:
            body = {}
        if r.status_code != 200 or not body.get('token'):
            logger.info('[free_networks] %s: guest %s not accepted (%s %s)',
                        self.name, guest.get('uid'), r.status_code, body.get('message'))
            return None
        return body['token']

    def _schedule(self, s) -> dict:
        def fetch():
            r = s.get(f'{_API}/schedule/fastchannelsv2', params={'timezone': 'UTC'}, headers=self._headers())
            return ((r.json() or {}).get('data') or {}) if r is not None else {}
        return s.memo(('gizmott', self.key), fetch)

    def _details(self, s, guest: dict, channel_id) -> tuple[dict | None, bool]:
        """(channel details, whether the token was refused)."""
        r = s.session.get(f'{_API}/fastchannel/details/{channel_id}', headers=self._headers(guest), timeout=20)
        if r.status_code in (401, 403):
            return None, True
        try:
            body = r.json() or {}
        except ValueError:
            return None, False
        if not body.get('data'):
            msg = str(body.get('message') or '').lower()
            return None, 'token' in msg or 'session' in msg or 'invalid user' in msg
        return body['data'][0], False

    # ── Network ──────────────────────────────────────────

    def _category(self, name: str, groups: list[str]) -> str | None:
        for pattern, category in _CATEGORY_BY_NAME:
            if pattern.search(name):
                return category
        for group in groups:
            if group.lower() in _CATEGORY_BY_GROUP:
                return _CATEGORY_BY_GROUP[group.lower()]
        return self.default_category

    def channels(self, s):
        listing = self._schedule(s).get('channels') or []
        if not listing:
            return []
        guest = self._guest(s)
        out = []
        for row in listing:
            cid = row.get('id')
            if not cid or row.get('is_locked') or _SKIP.search(row.get('channel_name') or ''):
                continue
            d, refused = self._details(s, guest, cid)
            if refused:
                guest = self._guest(s, fresh=True)
                d, _ = self._details(s, guest, cid)
            link = (d or {}).get('live_link_hd') or (d or {}).get('live_link')
            if not link:
                logger.debug('[free_networks] %s: no stream for channel %s', self.name, cid)
                continue
            name = (d.get('channel_name') or row.get('channel_name') or '').strip()
            if name.isupper() and len(name) > 6:  # "SANTA TRACKER LIVE!"; not QVC/HSN
                name = name.title()
            description = (d.get('description') or '').strip() or None
            out.append(NetChannel(
                str(cid), name, _strip_macros(link),
                logo=row.get('logo') or d.get('logo'),
                category=self._category(name, [c.get('category_name') or '' for c in d.get('categories') or []]),
                language=infer_language(name, description),
                description=description,
            ))
        return out

    def programs(self, s, channels):
        wanted = {ch.id for ch in channels}
        out = []
        for row in self._schedule(s).get('schedules') or []:
            cid = str(row.get('channel_id'))
            if cid not in wanted:
                continue
            try:
                start = datetime.fromisoformat(row['start']).replace(tzinfo=UTC)
                end = datetime.fromisoformat(row['end']).replace(tzinfo=UTC)
            except (KeyError, TypeError, ValueError):
                continue
            p = program(cid, row.get('title'), start, end)
            if p:
                out.append(p)
        return out


NETWORKS = [
    Gizmott('christmasplus', 'Christmas Plus', '50189', '522', 'https://christmasplus.tv'),
    Gizmott('docplus', 'Documentary+', '50158', '491', 'https://docplus.com', default_category='Documentary'),
]

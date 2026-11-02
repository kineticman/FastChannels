"""TrueTVplus and Flou TV: channels from the other free apps on OG Network's
Sotal Cloud backend that OG's own lineup doesn't carry. One network each, so
users can find and switch them separately.

play.truetvplus.com (TrueTVplus) and play.floutv.com (Flou TV) are the same
web app as play.theognetwork.net with their own SDP tenant (anonymous web
client cloud:<tenant>_ottweb_device), so token, guide and playback work as in
og_network.py. Most of their channels are OG's under other asset ids; only
the ones whose name isn't in OG's catalog are listed here. Checked
2026-11-02: TrueTVplus adds about a dozen (its "Sub - ..." channels play
without an account: CameraSmileTV, Captiva TV, Race Central, SportTVplus,
...; plus Celebrity Scene MAX and Dr. Joe TV), Flou TV adds VueWho, The TRUMP
Channel, New America Channel and El Reino Infantil plus some carried
elsewhere; some of Flou's (Miami TV, FreeTV, RCN) serve empty playlists, which
resolve() reports as such. Vidway, NOMADlife and Watchie run on the same
backend too but added nothing beyond these. Rows titled "DO NOT USE ..." are
skipped. A channel on both sites (VueWho) goes to TrueTVplus, whose copy plays;
Flou lists it only while the TrueTVplus network is switched off.

Channel ids are "<network>.<asset id>" with the tenant's own asset id, which
is also the stream ref.

Logos: the asset list has none. The web app's tiles carry them, so every
screen in the site's menu (config.json; /screens?ext_id[eq]=<n>) is walked
rail by rail (/rails/<id>/content?$expand=thumbnail_url) and each channel
asset's thumbnail_url, a square logo (280-1254 px), is kept. About 15 calls
per tenant per channel fetch; TrueTVplus's "Sub - ..." channels only appear
on its home screen. A logo failure never drops a channel.
"""
from __future__ import annotations

import re
import time

from ..base import infer_language
from ._common import Network, NetChannel, logger
from .og_network import _description, _get, catalog, playback_url, tenant_assets, tenant_programs, with_token

# The logo walk stops after this long per tenant, so a slow Sotal can't use up
# the network's channel-fetch budget; channels missed just go without a logo.
_LOGO_SECS = 20
_SUB_PREFIX = re.compile(r'^\s*sub\s*-\s*', re.I)
# Spanish channels the name/description check reads as English (their genre
# tags can't be used: the English TRUMP Channel is tagged Hispanic too).
_SPANISH = re.compile(r'^(MI RAZA CANAL|RCN |FreeTV |EL REINO INFANTIL|Captiva TV)', re.I)


def _key(name: str) -> str:
    return re.sub(r'[^a-z0-9]', '', _SUB_PREFIX.sub('', name or '').lower().replace('&', 'and'))


def _usable(asset: dict) -> bool:
    raw = (asset.get('name') or '').strip()
    return bool(asset.get('id') and _SUB_PREFIX.sub('', raw).strip() and not raw.upper().startswith('DO NOT USE'))


class SotalPartner(Network):
    _unguided: frozenset[str] = frozenset()

    def __init__(self, tenant: tuple[str, str], name: str, defers_to: SotalPartner | None = None):
        # tenant = (SDP tenant, web host); the network key is the tenant name.
        self.tenant = tenant
        self.key = tenant[0]
        self.name = name
        self.defers_to = defers_to

    def has_guide(self, channel_id: str) -> bool:
        return channel_id not in self._unguided

    def _logos(self, s, tenant) -> dict[int, str]:
        """Channel asset id -> logo URL from the tiles on the site's screens."""
        logos: dict[int, str] = {}
        try:
            r = s.get(f'https://{tenant[1]}/config.json')
            menu = ((r.json() or {}).get('menu') or []) if r else []
            screens = [m['url'][len('screen://'):] for m in menu if str(m.get('url', '')).startswith('screen://')]

            stop = time.monotonic() + _LOGO_SECS

            def walk(tok):
                for ext_id in screens or ['1', '2', '3']:
                    if time.monotonic() > stop:
                        logger.info('[free_networks] %s logo walk stopped after %ds', tenant[0], _LOGO_SECS)
                        return
                    data = _get(s, tenant, tok, 'screens', {'$expand': 'rails', 'ext_id[eq]': ext_id}).get('data')
                    screen = (data[0] if isinstance(data, list) and data else data) or {}
                    for rail in screen.get('rails') or []:
                        offset = 0
                        while True:
                            items = _get(s, tenant, tok, f'rails/{rail["id"]}/content', {
                                '$expand': 'thumbnail_url', '$limit': '100', '$offset': str(offset)}).get('data') or []
                            for item in items:
                                if item.get('asset_type') == 'channel' and item.get('thumbnail_url'):
                                    logos.setdefault(item.get('asset_id'), item['thumbnail_url'])
                            if len(items) < 100:
                                break
                            offset += 100
            with_token(s, tenant, walk)
        except Exception as exc:  # noqa: BLE001
            logger.warning('[free_networks] %s logos unavailable (%d found): %s', tenant[0], len(logos), exc)
        return logos

    def _assets(self, s) -> list[dict]:
        return [a for a in tenant_assets(s, self.tenant) if a.get('type') == 'channel' and _usable(a)]

    def channels(self, s):
        seen = {_key(ch.get('title')) for ch in catalog(s)}
        other = self.defers_to
        if other and other.key not in s.disabled_networks(s.config):
            seen |= {_key(a.get('name')) for a in other._assets(s)}
        assets = [a for a in self._assets(s) if _key(a.get('name')) not in seen]
        logos = self._logos(s, self.tenant) if assets else {}
        out, added = [], set()
        for asset in assets:
            if _key(asset.get('name')) in added:
                continue
            added.add(_key(asset.get('name')))
            name = _SUB_PREFIX.sub('', asset['name'].strip()).strip()
            genres = [g.get('name') for g in asset.get('genres') or [] if g.get('name')]
            description = _description(asset.get('description'))
            language = 'es' if _SPANISH.match(name) else infer_language(name, description)
            out.append(NetChannel(str(asset['id']), name, str(asset['id']), logo=logos.get(asset['id']),
                                  category=genres[0] if genres else None,
                                  language=language, description=description))
        return out

    def programs(self, s, channels):
        out = tenant_programs(s, self.tenant, channels, str)
        self._unguided = frozenset({ch.id for ch in channels} - {p.source_channel_id for p in out})
        return out

    def resolve(self, s, ref):
        if not str(ref).isdigit():
            raise RuntimeError(f'Unknown {self.name} channel {ref!r}')
        return playback_url(s, self.tenant, ref)


TRUETVPLUS = SotalPartner(('truetvplus', 'play.truetvplus.com'), 'TrueTVplus')
FLOU = SotalPartner(('flou', 'play.floutv.com'), 'Flou TV', defers_to=TRUETVPLUS)
NETWORKS = [TRUETVPLUS, FLOU]

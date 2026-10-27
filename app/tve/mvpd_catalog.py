"""Which TV providers (MVPDs) our TVE networks accept, from the networks themselves.

The provider dropdowns used to come from yt-dlp's bundled `--ap-list-mso` plus
getchannels.com's provider pages. Both lag the real rosters: an audit on
2026-10-23 found 187 of the 624 ids they offered accepted by none of our
networks, ~20 live US providers missing, and stale names on ~57 (e.g.
ATTOTT still "DIRECTV NOW").

The rosters themselves are public:
- every Adobe Pass network's MVPD list at
  sp.auth.adobe.com/adobe-services/config/<requestor> — no client
  registration needed, and the same list the signed-in v2
  /api/v2/<requestor>/configuration returns. FOX One signs in under
  `fbc-fox` (its Adobe authenticate URL says so), so it's covered;
- Discovery's own partner list, whose flows carry the Adobe id as
  `external_partner_id`. 44 ids on it are on no Adobe network of ours.

The catalog keeps each list's ids separately, so a refresh that can't
reach one network reuses that network's last ids rather than dropping
its providers. A snapshot ships in mvpd_catalog.json; the maintenance
worker refreshes a copy under /data daily (refresh_catalog).
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

SNAPSHOT_PATH = Path(__file__).resolve().parent / 'mvpd_catalog.json'
RUNTIME_PATH = Path(os.environ.get('FASTCHANNELS_MVPD_CATALOG_PATH') or '/data/tve_mvpd_catalog.json')
REFRESH_SECONDS = 24 * 3600

CONFIG_URL = 'https://sp.auth.adobe.com/adobe-services/config/{}'
# Every Adobe requestor our sources and sign-ins use (truTV's public config
# only answers to this casing).
ADOBE_REQUESTORS = (
    'AETV', 'HISTORY', 'LIFETIME', 'FYI', 'TBS', 'TNT', 'truTV', 'TCM', 'CNN',
    'AMC', 'BBCA', 'IFC', 'WETV', 'SUNDANCE', 'nbcentertainment', 'sparkmedia',
    'NESNGO', 'CSNMidAtlantic', 'Altitude', 'MyOutdoorTV', 'fbc-fox', 'foxnews', 'foxbusiness', 'univision', 'tceverywhere',
    'Gotham', 'C-SPAN', 'dtci', 'DisneyChannels', 'ESPN',
)
DISCOVERY = 'discovery'
# ESPN's list adds ~170 Latin American, Dutch and Australian providers that
# no other network takes; an id only ESPN lists isn't offered.
_INTERNATIONAL_REQUESTORS = frozenset({'ESPN'})

# Trial passes, employee/guest/staging logins and other non-subscriber entries.
_NOT_A_PROVIDER_RE = re.compile(
    r'temp.?pass|freeview|employee|staging|hospital|^apassidp$|^casso$|_ADFS$|^Turner_MVPD$', re.I)
# Canadian providers (A+E and CNN carry them). FastChannels' networks are US
# services; these were never offered and stay out.
_CANADIAN_PREFIXES = ('ccsa', 'idp_ccn_')
_CANADIAN_IDS = frozenset({
    'Bell', 'BellAliant', 'CableCable', 'Cogeco', 'Eastlink', 'Manitoba', 'Northwestel',
    'Novus', 'Rogers', 'SaskTel', 'ShawGo', 'Source', 'Telus', 'Videotron', 'VirginTV',
    'telus_auth-gateway_net', 'vianetca',
})


def _excluded(mvpd_id: str, name: str) -> bool:
    return bool(_NOT_A_PROVIDER_RE.search(mvpd_id) or _NOT_A_PROVIDER_RE.search(name or '')
                or mvpd_id in _CANADIAN_IDS or mvpd_id.startswith(_CANADIAN_PREFIXES))


# ── fetching ─────────────────────────────────────────────────────────────────

def _fetch_adobe(requestor: str) -> dict[str, str]:
    r = requests.get(CONFIG_URL.format(requestor), headers={'Accept': 'application/json'}, timeout=30)
    r.raise_for_status()
    mvpds = (((r.json() or {}).get('requestor') or {}).get('mvpds') or {}).get('mvpd') or []
    out = {}
    for m in mvpds:
        mvpd_id = ((m.get('id') or {}).get('value') or '').strip()
        if mvpd_id:
            out[mvpd_id] = ((m.get('displayName') or {}).get('value') or mvpd_id).strip()
    if not out:
        raise ValueError('empty MVPD list')
    return out


def _fetch_discovery() -> dict[str, str]:
    from ..scrapers.discovery_tve import API_BASE, BRAND_ID, _auth_widget_headers, _gauth_headers
    session = requests.Session()
    device_id = str(uuid.uuid4())
    session.get(f'{API_BASE}/token', params={'realm': 'go', 'deviceId': device_id, 'shortlived': 'true'},
                headers=_auth_widget_headers(device_id), timeout=30).raise_for_status()
    r = session.get(f'{API_BASE}/v1/gauth/partners', params={'brand_id': BRAND_ID},
                    headers=_gauth_headers(device_id), timeout=30)
    r.raise_for_status()
    out = {}
    for partner in r.json() or []:
        for flow in partner.get('flows') or []:
            mvpd_id = (flow.get('external_partner_id') or '').strip()
            if mvpd_id:
                out[mvpd_id] = (partner.get('name') or mvpd_id).strip()
    if not out:
        raise ValueError('empty partner list')
    return out


def fetch_catalog(previous: dict | None = None) -> dict:
    """Every list, fresh. A list that can't be fetched keeps its ids from
    `previous`; raises if more than a few are missing with nothing to reuse."""
    previous = previous or {}
    fetchers = {rq: (lambda rq=rq: _fetch_adobe(rq)) for rq in ADOBE_REQUESTORS}
    fetchers[DISCOVERY] = _fetch_discovery
    with ThreadPoolExecutor(8) as pool:
        futures = {key: pool.submit(fn) for key, fn in fetchers.items()}
    sources, names, failed = {}, {}, []
    name_votes: dict[str, Counter] = {}
    for key, future in futures.items():
        try:
            listed = future.result()
        except Exception as exc:  # noqa: BLE001
            failed.append(key)
            logger.warning('[mvpd-catalog] %s: list unavailable (%s); keeping its last ids', key, exc)
            if key in (previous.get('sources') or {}):
                sources[key] = previous['sources'][key]
            continue
        sources[key] = sorted(listed)
        for mvpd_id, name in listed.items():
            # Discovery's names only count where Adobe has none.
            name_votes.setdefault(mvpd_id, Counter())[name] += 1 if key != DISCOVERY else 0.1
    unrecovered = [k for k in failed if k not in sources]
    if len(unrecovered) > 3:
        raise RuntimeError(f'too many provider lists unavailable: {", ".join(unrecovered)}')
    for mvpd_id, votes in name_votes.items():
        names[mvpd_id] = votes.most_common(1)[0][0]
    for mvpd_id, name in (previous.get('names') or {}).items():
        names.setdefault(mvpd_id, name)
    return {'fetched_at': int(time.time()), 'sources': sources, 'names': names,
            'unavailable': sorted(failed)}


# ── loading ──────────────────────────────────────────────────────────────────

_cache: dict = {'key': None, 'data': None}


def _read(path: Path) -> dict | None:
    try:
        with open(path) as f:
            data = json.load(f)
        if len(data.get('sources') or {}) >= len(ADOBE_REQUESTORS) // 2:
            return data
    except FileNotFoundError:
        pass
    except Exception as exc:  # noqa: BLE001
        logger.warning('[mvpd-catalog] could not read %s: %s', path, exc)
    return None


def load_catalog() -> dict:
    """The /data copy when there is a valid one, else the shipped snapshot.
    Re-read only when the /data copy changes."""
    try:
        key = RUNTIME_PATH.stat().st_mtime
    except OSError:
        key = None
    if _cache['data'] is None or _cache['key'] != key:
        _cache['data'] = (_read(RUNTIME_PATH) if key else None) or _read(SNAPSHOT_PATH) or {}
        _cache['key'] = key
    return _cache['data']


def accepted_providers() -> dict[str, str]:
    """{mvpd id: display name} for every provider at least one of our
    networks accepts, minus trial passes, staff logins, Canadian providers
    and ids only ESPN's international list carries."""
    catalog = load_catalog()
    names = catalog.get('names') or {}
    ids = set()
    for key, listed in (catalog.get('sources') or {}).items():
        if key not in _INTERNATIONAL_REQUESTORS:
            ids.update(listed)
    return {i: names.get(i, i) for i in ids if not _excluded(i, names.get(i, ''))}


def refresh_catalog(force: bool = False) -> bool:
    """Fetch the lists and write the /data copy. Skipped while the copy is
    under a day old unless `force`. Returns True when it wrote one."""
    current = _read(RUNTIME_PATH)
    if not force and current and time.time() - float(current.get('fetched_at') or 0) < REFRESH_SECONDS:
        return False
    catalog = fetch_catalog(current or _read(SNAPSHOT_PATH))
    tmp = RUNTIME_PATH.with_suffix('.tmp')
    with open(tmp, 'w') as f:
        json.dump(catalog, f, separators=(',', ':'), sort_keys=True)
    os.replace(tmp, RUNTIME_PATH)
    logger.info('[mvpd-catalog] refreshed: %d providers from %d lists%s', len(accepted_providers()),
                len(catalog['sources']),
                f' (reused last ids for {", ".join(catalog["unavailable"])})' if catalog['unavailable'] else '')
    return True

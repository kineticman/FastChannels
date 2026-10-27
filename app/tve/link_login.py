"""Phone-link TVE sign-in: the provider login happens on the user's own device.

The browser-assisted sign-ins (app/tve/browser_login/) drive the provider's
login page in our Camoufox. This is the other way to do the same step: every
Adobe Pass family's sign-in URL works from any browser — no token or cookies
of ours in it (checked 2026-09-29 for all of them: A+E, Warner, NBC, AMC,
FOX TVE, ESPN) — and Adobe binds the finished login to our registration
server-side. So we register exactly as the browser flow does, hand the user
the link, poll until the sign-in lands, and save it exactly where the browser
flow saves it. Playback, token refresh and the audit can't tell the two apart.

Each family is an adapter over helpers the browser flows already use:
  targets()        the networks one start covers (AMC = its 4 channels)
  start(target)    scripted registration → (url, ctx)
  poll(ctx)        None while pending; the result once signed in;
                   TVENotAuthorizedError if the provider said no
  save(ctx, res)   the family's own save → a short outcome message
An adapter whose sign-in uses a typed code rather than a link puts it in
ctx['code']; the status carries it so the UI can show it next to the URL.
YouTube TV's Google step isn't an Adobe sign-in, so it stays browser-only.
"""
from __future__ import annotations

import json
import logging
import time
import uuid

import requests

from .adobe_pass import TVEAuthError, TVENotAuthorizedError, TVEPendingAuthError

logger = logging.getLogger(__name__)

STATUS_KEY = 'tve:link-login:status'
LINK_FAMILIES = ('legacy', 'nbc', 'tcm', 'fox', 'amcn', 'discovery', 'versant', 'univision', 'disney', 'tennis', 'dazn', 'cspan', 'sundance', 'cnn', 'nesn', 'monumental', 'altitude', 'motv', 'snp')
_PER_TARGET_TIMEOUT = 10 * 60
_POLL_SECONDS = 3.0
_POLL_MAX_SECONDS = 15.0

# Adobe has two ids for DIRECTV. Every requestor we use lists `ATTOTT`, and all
# but Versant's sparkmedia also list `DTV`; sparkmedia answers `DTV` with 400
# invalid_integration (adobe-services/config/<requestor>, checked 2026-10-23).
# A start that fails under one is retried once under the other.
_MVPD_ALIASES = {'DTV': 'ATTOTT', 'ATTOTT': 'DTV'}


def job_timeout(family: str) -> int:
    targets = {'amcn': 4, 'disney': 2, 'dazn': 2}.get(family, 1)
    return targets * _PER_TARGET_TIMEOUT + 300


# ── adapters ─────────────────────────────────────────────────────────────────

class _Legacy:
    """A+E / Warner — Adobe's regcode API; the authenticate URL carries the
    reg_code itself. Same client setup as run_mvpd_browser_login."""
    error_key = None  # per requestor

    def __init__(self, account, mso_id: str, requestor_id: str):
        from .mvpd_targets import resolve_requestor_target
        self.account, self.mso_id = account, mso_id
        self.target = resolve_requestor_target(requestor_id)
        self.error_key = self.target['requestor_id']

    def targets(self):
        return [self.target['requestor_id']]

    def start(self, label):
        from .adobe_pass import (AdobePassCoxClient, _ensure_cox_device_fingerprint,
                                 load_cached_adobe_client_creds, save_adobe_client_creds)
        rid = self.target['requestor_id']
        creds = load_cached_adobe_client_creds(self.account, rid)
        client = AdobePassCoxClient(
            requestor_id=rid, resource=self.target['resource'],
            software_statement=self.target['software_statement'], redirect_url=self.target['redirect_url'],
            device_fingerprint=_ensure_cox_device_fingerprint(self.account), client_creds=creds,
        )
        client.setup_client()
        if not creds:
            save_adobe_client_creds(self.account, rid, client.ctx.client_id, client.ctx.client_secret,
                                    client.ctx.access_token)
        client.register_device()
        client.create_regcode()
        return client.authenticate_redirect_url(self.mso_id), client

    def poll(self, client):
        try:
            client.fetch_session_token()
        except TVEPendingAuthError:
            return None
        return client.ctx.authn_token

    def save(self, client, authn_token):
        from .browser_login.mvpd import _save_mvpd_authn_token
        _save_mvpd_authn_token(self.target['requestor_id'], authn_token)
        try:
            client.authorize()
        except TVENotAuthorizedError:
            return 'signed in, but your package doesn\'t include it'
        except TVEAuthError as exc:
            return f'signed in (authorize check failed: {str(exc)[:80]})'
        return 'authorized'


class _Nbc:
    """NBC — Adobe v2 (/sessions + /profiles/<mvpd>), same client, device
    fingerprint and cached client creds as run_nbc_browser_login."""
    error_key = 'nbc'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        self.account, self.mso_id = account, mso_id

    def targets(self):
        return ['NBC']

    def start(self, label):
        from ..config_store import persist_source_config_updates
        from ..models import Source
        from ..scrapers.nbc_tve import ADOBE_BASE, DEFAULT_REDIRECT_URL, REQUESTOR_ID, AdobePassV2Client, NbcTveScraper
        from .adobe_pass import load_cached_adobe_client_creds, save_adobe_client_creds

        source = Source.query.filter_by(name='nbc_tve').first()
        scraper = NbcTveScraper(config=dict((source.config if source else {}) or {}))
        statement = scraper._discover_page_config()['software_statement']
        fingerprint = scraper._ensure_device_fingerprint()
        if source:
            persist_source_config_updates(source.id, scraper._pending_config_updates)
        creds = load_cached_adobe_client_creds(self.account, REQUESTOR_ID)
        client = AdobePassV2Client(REQUESTOR_ID, statement, DEFAULT_REDIRECT_URL, fingerprint, client_creds=creds)
        client._register_client()
        if not creds:
            save_adobe_client_creds(self.account, REQUESTOR_ID, client.client_id, client.client_secret,
                                    client.access_token)
        data = client._post(
            f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/sessions',
            data={'mvpd': self.mso_id, 'redirectUrl': DEFAULT_REDIRECT_URL, 'domainName': 'nbc.com'},
            headers={**client._bearer_headers(), 'Content-Type': 'application/x-www-form-urlencoded'},
        ).json()
        ctx = {'client': client, 'fingerprint': fingerprint,
               'profile_url': f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/profiles/{self.mso_id}'}
        # This client is already signed in with the provider — nothing to open.
        if data.get('reasonType') == 'authenticated':
            return None, ctx
        if not data.get('url'):
            raise TVEAuthError('Adobe Pass v2: sessions call did not return an authenticate url.')
        return ADOBE_BASE + data['url'], ctx

    def poll(self, ctx):
        client = ctx['client']
        r = client.session.get(ctx['profile_url'], headers=client._bearer_headers(), timeout=20)
        if r.status_code == 401:
            client.refresh_access_token()
            return None
        if not r.ok:
            return None
        return ((r.json() or {}).get('profiles') or {}).get(self.mso_id) or None

    def save(self, ctx, profile):
        from .browser_login.nbc import _save_nbc_mvpd_auth
        _save_nbc_mvpd_auth(self.mso_id, ctx['client'], ctx['fingerprint'])
        return 'authorized'


class _Tcm:
    """TCM's Adobe v2 sign-in, using the same client identity as playback."""
    error_key = 'tcm'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        self.account, self.mso_id = account, mso_id

    def targets(self):
        return ['TCM']

    def start(self, label):
        from ..config_store import persist_source_cache_updates, persist_source_config_updates
        from ..models import Source
        from ..scrapers.nbc_tve import AdobePassV2Client
        from ..scrapers.warner_tve import WarnerTVEScraper, TCM_LIVE_URL
        from .adobe_pass import load_cached_adobe_client_creds, save_adobe_client_creds

        source = Source.query.filter_by(name='warner_tve').first()
        if not source or not source.is_enabled:
            raise TVEAuthError('Enable the Warner TVE source before signing in to TCM.')
        scraper = WarnerTVEScraper(config=dict(source.config or {}))
        brand_cfg = scraper._brand_config('tcm')
        fingerprint = scraper.config.get('tcm_device_fingerprint')
        if not fingerprint:
            fingerprint = str(uuid.uuid4())
            scraper._update_config('tcm_device_fingerprint', fingerprint)
        persist_source_config_updates(source.id, scraper._pending_config_updates)
        persist_source_cache_updates(source.id, scraper._pending_cache_updates)

        creds = load_cached_adobe_client_creds(self.account, 'TCM')
        client = AdobePassV2Client('TCM', brand_cfg['software_statement'], TCM_LIVE_URL,
                                   fingerprint, client_creds=creds, origin='https://www.tcm.com')
        client._register_client()
        if not creds:
            save_adobe_client_creds(self.account, 'TCM', client.client_id, client.client_secret,
                                    client.access_token)
        data = client._post(
            'https://sp.auth.adobe.com/api/v2/TCM/sessions',
            data={'mvpd': self.mso_id, 'redirectUrl': TCM_LIVE_URL, 'domainName': 'www.tcm.com'},
            headers={**client._bearer_headers(), 'Content-Type': 'application/x-www-form-urlencoded'},
        ).json()
        ctx = {'client': client, 'fingerprint': fingerprint,
               'profile_url': f'https://sp.auth.adobe.com/api/v2/TCM/profiles/{self.mso_id}'}
        if data.get('reasonType') == 'authenticated':
            return None, ctx
        if not data.get('url'):
            raise TVEAuthError('TCM: Adobe did not return a sign-in URL.')
        return 'https://sp.auth.adobe.com' + data['url'], ctx

    def poll(self, ctx):
        client = ctx['client']
        response = client.session.get(ctx['profile_url'], headers=client._bearer_headers(), timeout=20)
        if response.status_code == 401:
            client.refresh_access_token()
            return None
        if not response.ok:
            return None
        return ((response.json() or {}).get('profiles') or {}).get(self.mso_id) or None

    def save(self, ctx, profile):
        from ..extensions import db
        from .adobe_pass import save_adobe_client_creds

        client = ctx['client']
        response = client._post(
            f'https://sp.auth.adobe.com/api/v2/TCM/decisions/authorize/{self.mso_id}',
            json={'resources': ['TCM']},
            headers={**client._bearer_headers(), 'Content-Type': 'application/json'},
        )
        decision = next((d for d in (response.json().get('decisions') or [])
                         if d.get('resource') == 'TCM'), None)
        if not decision or not decision.get('authorized'):
            raise TVENotAuthorizedError(f'{self.mso_id} account is not entitled to TCM.')
        save_adobe_client_creds(self.account, 'TCM', client.client_id, client.client_secret, client.access_token)
        cfg = dict(self.account.config or {})
        cfg['tcm_mvpd_auth'] = {'mso_id': self.mso_id, 'captured_at': int(time.time())}
        self.account.config = cfg
        db.session.commit()
        return 'authorized'


class _Versant:
    """Versant (USA, E!, Syfy, Oxygen, CNBC, MS NOW, Golf) — one Adobe v2
    sign-in under usanetwork.com's `sparkmedia` requestor covers all of them.
    A re-sign-in reuses the saved client + device fingerprint, so Adobe may
    answer "already signed in" with nothing to open."""
    error_key = 'versant'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        from ..scrapers.versant_tve import VersantTVEScraper
        self.account, self.mso_id = account, mso_id
        self.source = Source.query.filter_by(name='versant_tve').first()
        if not self.source or not self.source.is_enabled:
            raise TVEAuthError('Enable the Versant TVE source before signing in to it.')
        self.scraper = VersantTVEScraper(config=dict(self.source.config or {}))

    def targets(self):
        return ['Versant']

    def start(self, label):
        from ..scrapers.versant_tve import ADOBE_BASE, DOMAIN_NAME, REDIRECT_URL, REQUESTOR_ID
        page = self.scraper._page_config()
        saved = (self.account.config or {}).get('versant_mvpd_auth') or {}
        if saved.get('mso_id') != self.mso_id:
            saved = {}
        auth = {'device_fingerprint': saved.get('device_fingerprint') or str(uuid.uuid4()),
                **{k: saved.get(k) for k in ('client_id', 'client_secret', 'access_token')}}
        client = self.scraper.adobe_client(page['software_statement'], auth)
        if client.client_id and client.client_secret:
            client.refresh_access_token()
        else:
            client._client_creds = None
            client._register_client()
        data = client._post(
            f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/sessions',
            data={'mvpd': self.mso_id, 'redirectUrl': REDIRECT_URL, 'domainName': DOMAIN_NAME},
            headers={**client._bearer_headers(), 'Content-Type': 'application/x-www-form-urlencoded'},
        ).json()
        ctx = {'client': client, 'page': page, 'fingerprint': auth['device_fingerprint'],
               'profile_url': f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/profiles/{self.mso_id}'}
        if data.get('reasonType') == 'authenticated':
            return None, ctx
        if not data.get('url'):
            raise TVEAuthError('Versant: Adobe did not return a sign-in URL.')
        return ADOBE_BASE + data['url'], ctx

    def poll(self, ctx):
        client = ctx['client']
        r = client.session.get(ctx['profile_url'], headers=client._bearer_headers(), timeout=20)
        if r.status_code == 401:
            client.refresh_access_token()
            return None
        if not r.ok:
            return None
        return ((r.json() or {}).get('profiles') or {}).get(self.mso_id) or None

    def save(self, ctx, profile):
        from ..config_store import persist_source_cache_updates
        from ..extensions import db

        client = ctx['client']
        # Raises TVENotAuthorizedError when the package has none of the brands.
        session = self.scraper.sign_in_viewlift(client, self.mso_id, profile, ctx['page'])
        session['adobe_client_id'] = client.client_id
        cfg = dict(self.account.config or {})
        cfg['versant_mvpd_auth'] = {
            'mso_id': self.mso_id, 'client_id': client.client_id, 'client_secret': client.client_secret,
            'access_token': client.access_token, 'device_fingerprint': ctx['fingerprint'],
            'captured_at': int(time.time()),
        }
        self.account.config = cfg
        db.session.commit()
        self.scraper._update_cache('versant_session', session)
        self.scraper._update_cache('versant_playback', {})
        persist_source_cache_updates(self.source.id, self.scraper._pending_cache_updates)
        self.scraper._pending_cache_updates = {}
        denied = sorted(res for res, ok in session['decisions'].items() if not ok)
        if denied:
            return 'authorized (your package doesn\'t include: ' + ', '.join(denied) + ')'
        return 'authorized'


class _ViewLift(_Versant):
    """A ViewLift site (NESN, Monumental, Altitude, MyOutdoorTV) — one Adobe v2
    sign-in under the site's requestor, linked to the user's account on that
    site (see viewlift_tve.py), so the account sign-in on the source has to
    come first. Subclasses name the source."""
    source_name = ''

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        from ..scrapers import registry
        self.account, self.mso_id = account, mso_id
        cls = registry.get(self.source_name)
        self.error_key = cls.key
        self.source = Source.query.filter_by(name=self.source_name).first()
        if not self.source or not self.source.is_enabled:
            raise TVEAuthError(f'Enable the {cls.display_name} source before signing in to it.')
        if not (self.source.config or {}).get(f'{cls.key}_account'):
            raise TVEAuthError(f'Sign in to your {cls.brand} account first '
                               f'(Sources → {cls.display_name} → Sign in to {cls.brand}).')
        self.scraper = cls(config=dict(self.source.config or {}))

    def targets(self):
        return [self.scraper.brand]

    def start(self, label):
        from ..scrapers.viewlift_tve import ADOBE_BASE
        scraper = self.scraper
        page = scraper._page_config()
        saved = (self.account.config or {}).get(f'{scraper.key}_mvpd_auth') or {}
        if saved.get('mso_id') != self.mso_id:
            saved = {}
        auth = {'device_fingerprint': saved.get('device_fingerprint') or str(uuid.uuid4()),
                **{k: saved.get(k) for k in ('client_id', 'client_secret', 'access_token')}}
        client = scraper.adobe_client(page['software_statement'], auth)
        if client.client_id and client.client_secret:
            client.refresh_access_token()
        else:
            client._client_creds = None
            client._register_client()
        data = client._post(
            f'{ADOBE_BASE}/api/v2/{scraper.requestor_id}/sessions',
            data={'mvpd': self.mso_id, 'redirectUrl': scraper.redirect_url, 'domainName': scraper.domain_name},
            headers={**client._bearer_headers(), 'Content-Type': 'application/x-www-form-urlencoded'},
        ).json()
        ctx = {'client': client, 'page': page, 'fingerprint': auth['device_fingerprint'],
               'profile_url': f'{ADOBE_BASE}/api/v2/{scraper.requestor_id}/profiles/{self.mso_id}'}
        if data.get('reasonType') == 'authenticated':
            return None, ctx
        if not data.get('url'):
            raise TVEAuthError(f'{scraper.brand}: Adobe did not return a sign-in URL.')
        return ADOBE_BASE + data['url'], ctx

    def save(self, ctx, profile):
        from ..config_store import persist_source_cache_updates
        from ..extensions import db

        client, key = ctx['client'], self.scraper.key
        # Keep the Adobe sign-in even if linking it to the site's account fails
        # below: playback re-links from it, so the user needn't sign in again.
        cfg = dict(self.account.config or {})
        cfg[f'{key}_mvpd_auth'] = {
            'mso_id': self.mso_id, 'client_id': client.client_id, 'client_secret': client.client_secret,
            'access_token': client.access_token, 'device_fingerprint': ctx['fingerprint'],
            'captured_at': int(time.time()),
        }
        self.account.config = cfg
        db.session.commit()
        try:
            # Raises TVENotAuthorizedError when Adobe says the package has none of it.
            session = self.scraper.sign_in_viewlift(client, self.mso_id, profile, ctx['page'])
            session['adobe_client_id'] = client.client_id
            self.scraper._update_cache(f'{key}_session', session)
            self.scraper._update_cache(f'{key}_playback', {})
        finally:
            persist_source_cache_updates(self.source.id, self.scraper._pending_cache_updates)
            self.scraper._pending_cache_updates = {}
        return 'authorized'


class _Nesn(_ViewLift):
    source_name = 'nesn_tve'


class _Monumental(_ViewLift):
    source_name = 'monumental_tve'


class _Altitude(_ViewLift):
    source_name = 'altitude_tve'


class _MyOutdoorTV(_ViewLift):
    source_name = 'myoutdoortv_tve'


class _Univision:
    """Univision Now TVE (Univision, UniMás, Galavisión, TUDN) — one Adobe v2
    sign-in under tv.univision.com's `univision` requestor; Univision's own
    access token is then minted from the Adobe profile's userID at tune time."""
    error_key = 'univision'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        from ..scrapers.univision_tve import UnivisionTVEScraper
        self.account, self.mso_id = account, mso_id
        self.source = Source.query.filter_by(name='univision_tve').first()
        if not self.source or not self.source.is_enabled:
            raise TVEAuthError('Enable the Univision TVE source before signing in to it.')
        self.scraper = UnivisionTVEScraper(config=dict(self.source.config or {}))

    def targets(self):
        return ['Univision']

    def start(self, label):
        from ..scrapers.univision_tve import ADOBE_BASE, REDIRECT_URL, REQUESTOR_ID
        statement = self.scraper._config()['software_statement']
        saved = (self.account.config or {}).get('univision_mvpd_auth') or {}
        if saved.get('mso_id') != self.mso_id:
            saved = {}
        auth = {'device_fingerprint': saved.get('device_fingerprint') or str(uuid.uuid4()),
                **{k: saved.get(k) for k in ('client_id', 'client_secret', 'access_token')}}
        client = self.scraper.adobe_client(statement, auth)
        if client.client_id and client.client_secret:
            client.refresh_access_token()
        else:
            client._client_creds = None
            client._register_client()
        data = client._post(
            f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/sessions',
            data={'mvpd': self.mso_id, 'redirectUrl': REDIRECT_URL, 'domainName': REDIRECT_URL},
            headers={**client._bearer_headers(), 'Content-Type': 'application/x-www-form-urlencoded'},
        ).json()
        ctx = {'client': client, 'fingerprint': auth['device_fingerprint'],
               'profile_url': f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/profiles/{self.mso_id}'}
        if data.get('reasonType') == 'authenticated':
            return None, ctx
        if not data.get('url'):
            raise TVEAuthError('Univision: Adobe did not return a sign-in URL.')
        return ADOBE_BASE + data['url'], ctx

    def poll(self, ctx):
        client = ctx['client']
        r = client.session.get(ctx['profile_url'], headers=client._bearer_headers(), timeout=20)
        if r.status_code == 401:
            client.refresh_access_token()
            return None
        if not r.ok:
            return None
        return ((r.json() or {}).get('profiles') or {}).get(self.mso_id) or None

    def save(self, ctx, profile):
        from ..config_store import persist_source_cache_updates
        from ..extensions import db

        client = ctx['client']
        # Raises TVENotAuthorizedError when the package has none of the three.
        user = self.scraper.user_from_profile(client, self.mso_id, profile)
        cfg = dict(self.account.config or {})
        cfg['univision_mvpd_auth'] = {
            'mso_id': self.mso_id, 'client_id': client.client_id, 'client_secret': client.client_secret,
            'access_token': client.access_token, 'device_fingerprint': ctx['fingerprint'],
            'captured_at': int(time.time()),
        }
        self.account.config = cfg
        db.session.commit()
        self.scraper._update_cache('univision_user', user)
        self.scraper._update_cache('univision_qpat_user', {})
        self.scraper._update_cache('univision_media_tokens', {})
        persist_source_cache_updates(self.source.id, self.scraper._pending_cache_updates)
        self.scraper._pending_cache_updates = {}
        denied = sorted(res for res, ok in user['decisions'].items() if not ok)
        if denied:
            return 'authorized (your package doesn\'t include: ' + ', '.join(denied) + ')'
        return 'authorized'


class _Disney:
    """Disney TVE — two Adobe v2 sign-ins, one link each: abc.com's `dtci`
    requestor (ABC, Freeform, FX, Nat Geo) and disneynow.com's
    `DisneyChannels` (Disney Channel, Junior, XD). Each requestor's software
    statement is found on its own site."""
    error_key = 'disney'
    _RESOURCES = {'dtci': ('ABC', 'ABCFamily', 'fx', 'fxx', 'fxm', 'ngc', 'ngw'),
                  'DisneyChannels': ('Disney', 'DisneyJunior', 'DisneyXD')}

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        from ..scrapers.disney_tve import REQUESTORS, DisneyTVEScraper
        self.account, self.mso_id = account, mso_id
        self.source = Source.query.filter_by(name='disney_tve').first()
        if not self.source or not self.source.is_enabled:
            raise TVEAuthError('Enable the Disney TVE source before signing in to it.')
        self.scraper = DisneyTVEScraper(config=dict(self.source.config or {}))
        self.requestors = {info['label']: requestor for requestor, info in REQUESTORS.items()}

    def targets(self):
        return list(self.requestors)

    def start(self, label):
        from ..scrapers.disney_tve import ADOBE_BASE, REQUESTORS, software_statements
        from ..scrapers.nbc_tve import AdobePassV2Client
        requestor = self.requestors[label]
        site = REQUESTORS[requestor]
        saved = ((self.account.config or {}).get('disney_mvpd_auth') or {}).get(requestor) or {}
        if saved.get('mso_id') != self.mso_id:
            saved = {}
        fingerprint = saved.get('device_fingerprint') or str(uuid.uuid4())
        client = AdobePassV2Client(requestor, '', site['redirect'], fingerprint,
                                   origin=site['redirect'].rstrip('/'))

        def open_session():
            return client.session.post(
                f'{ADOBE_BASE}/api/v2/{requestor}/sessions',
                data={'mvpd': self.mso_id, 'redirectUrl': site['redirect'], 'domainName': site['domain']},
                headers={**client._bearer_headers(), 'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=30)

        r = None
        if saved.get('client_id') and saved.get('client_secret'):
            client.client_id, client.client_secret = saved['client_id'], saved['client_secret']
            client.refresh_access_token()
            r = open_session()
        if r is None or r.status_code == 401:
            # A site carries one statement per brand; only one is this requestor's.
            for statement in software_statements(requestor):
                client.software_statement, client._client_creds = statement, None
                try:
                    client._register_client()
                except TVEAuthError:
                    continue
                r = open_session()
                if r.status_code != 401:
                    break
            else:
                raise TVEAuthError(f'Disney: none of the settings on {site["domain"]} were accepted — '
                                   'Disney may have changed its site.')
        data = r.json() if r.ok else {}
        ctx = {'client': client, 'requestor': requestor, 'fingerprint': fingerprint,
               'profile_url': f'{ADOBE_BASE}/api/v2/{requestor}/profiles/{self.mso_id}'}
        if data.get('reasonType') == 'authenticated':
            return None, ctx
        if not data.get('url'):
            raise TVEAuthError(f'Disney: Adobe did not return a sign-in URL (HTTP {r.status_code}).')
        return ADOBE_BASE + data['url'], ctx

    def poll(self, ctx):
        client = ctx['client']
        r = client.session.get(ctx['profile_url'], headers=client._bearer_headers(), timeout=20)
        if r.status_code == 401:
            client.refresh_access_token()
            return None
        if not r.ok:
            return None
        return ((r.json() or {}).get('profiles') or {}).get(self.mso_id) or None

    def save(self, ctx, profile):
        from ..config_store import persist_source_cache_updates
        from ..extensions import db
        from ..scrapers.disney_tve import ADOBE_BASE

        client, requestor = ctx['client'], ctx['requestor']
        r = client._post(
            f'{ADOBE_BASE}/api/v2/{requestor}/decisions/preauthorize/{self.mso_id}',
            json={'resources': list(self._RESOURCES[requestor])},
            headers={**client._bearer_headers(), 'Content-Type': 'application/json'},
        )
        decisions = {d.get('resource'): bool(d.get('authorized')) for d in (r.json() or {}).get('decisions') or []}
        cfg = dict(self.account.config or {})
        auth = dict(cfg.get('disney_mvpd_auth') or {})
        auth[requestor] = {
            'mso_id': self.mso_id, 'client_id': client.client_id, 'client_secret': client.client_secret,
            'access_token': client.access_token, 'device_fingerprint': ctx['fingerprint'],
            'captured_at': int(time.time()),
        }
        cfg['disney_mvpd_auth'] = auth
        self.account.config = cfg
        db.session.commit()
        self.scraper._update_cache('disney_adobe_tokens', {})
        self.scraper._update_cache('disney_streams', {})
        persist_source_cache_updates(self.source.id, self.scraper._pending_cache_updates)
        self.scraper._pending_cache_updates = {}
        if decisions and not any(decisions.values()):
            raise TVENotAuthorizedError(f'your package doesn\'t include {ctx["requestor"]} channels')
        denied = sorted(res for res, ok in decisions.items() if not ok)
        if denied:
            return 'authorized (your package doesn\'t include: ' + ', '.join(denied) + ')'
        return 'authorized'


class _Tennis:
    """Tennis Channel — one Adobe v2 sign-in under tennischannel.com's
    `tceverywhere` requestor. The site registers the Adobe client for us and
    hands out the device identifier the sign-in is bound to."""
    error_key = 'tennis'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        from ..scrapers.tennis_tve import TennisTVEScraper
        self.account, self.mso_id = account, mso_id
        self.source = Source.query.filter_by(name='tennis_tve').first()
        if not self.source or not self.source.is_enabled:
            raise TVEAuthError('Enable the Tennis Channel TVE source before signing in to it.')
        self.scraper = TennisTVEScraper(config=dict(self.source.config or {}))

    def targets(self):
        return ['Tennis Channel']

    def start(self, label):
        from ..scrapers.tennis_tve import (ADOBE_BASE, DOMAIN_NAME, REDIRECT_URL, REQUESTOR_ID,
                                           adobe_headers, register_client)
        saved = (self.account.config or {}).get('tennis_mvpd_auth') or {}
        device = saved.get('device_identifier') if saved.get('mso_id') == self.mso_id else None
        client = register_client(device)
        device = client['deviceIdentifier']
        headers = adobe_headers(client['accessToken'], device)
        r = requests.post(f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/sessions', timeout=30, headers=headers,
                          data={'mvpd': self.mso_id, 'domainName': DOMAIN_NAME, 'redirectUrl': REDIRECT_URL})
        data = r.json() if r.ok else {}
        ctx = {'device': device, 'headers': headers,
               'profile_url': f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/profiles/{self.mso_id}'}
        if data.get('reasonType') == 'authenticated':
            return None, ctx
        if not data.get('url'):
            raise TVEAuthError(f'Tennis Channel: Adobe did not return a sign-in URL (HTTP {r.status_code}).')
        return ADOBE_BASE + data['url'], ctx

    def poll(self, ctx):
        r = requests.get(ctx['profile_url'], headers=ctx['headers'], timeout=20)
        if not r.ok:
            return None
        return ((r.json() or {}).get('profiles') or {}).get(self.mso_id) or None

    def save(self, ctx, profile):
        from ..config_store import persist_source_cache_updates
        from ..extensions import db
        from ..scrapers.tennis_tve import ADOBE_BASE, REQUESTOR_ID, RESOURCE

        r = requests.post(f'{ADOBE_BASE}/api/v2/{REQUESTOR_ID}/decisions/preauthorize/{self.mso_id}', timeout=20,
                          json={'resources': [RESOURCE]},
                          headers={**ctx['headers'], 'Content-Type': 'application/json'})
        decisions = (r.json() or {}).get('decisions') or [] if r.ok else []
        cfg = dict(self.account.config or {})
        cfg['tennis_mvpd_auth'] = {'mso_id': self.mso_id, 'device_identifier': ctx['device'],
                                   'captured_at': int(time.time())}
        self.account.config = cfg
        db.session.commit()
        self.scraper._update_cache('tennis_client', {})
        self.scraper._update_cache('tennis_streams', {})
        persist_source_cache_updates(self.source.id, self.scraper._pending_cache_updates)
        self.scraper._pending_cache_updates = {}
        if decisions and not any(d.get('authorized') for d in decisions):
            raise TVENotAuthorizedError('your package doesn\'t include Tennis Channel')
        return 'authorized'


class _Cspan:
    """C-SPAN — Adobe's v1 regcode sign-in under the `C-SPAN` requestor (v2
    isn't enabled for it), one sign-in for all three networks. The sign-in is
    bound to the Adobe client and device it was made with, so both are saved
    with it."""
    error_key = 'cspan'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        from ..scrapers.cspan_tve import CSpanTVEScraper
        self.account, self.mso_id = account, mso_id
        self.source = Source.query.filter_by(name='cspan_tve').first()
        if not self.source or not self.source.is_enabled:
            raise TVEAuthError('Enable the C-SPAN TVE source before signing in to it.')
        self.scraper = CSpanTVEScraper(config=dict(self.source.config or {}))

    def targets(self):
        return ['C-SPAN']

    def start(self, label):
        from ..scrapers.cspan_tve import adobe_client
        client = adobe_client(self.scraper.software_statement())
        client.setup_client()
        client.register_device()
        client.create_regcode()
        return client.authenticate_redirect_url(self.mso_id), client

    def poll(self, client):
        try:
            client.fetch_session_token()
        except TVEPendingAuthError:
            return None
        return client.ctx.authn_token

    def save(self, client, authn_token):
        from ..config_store import persist_source_cache_updates
        from ..extensions import db
        from ..scrapers.cspan_tve import _resource

        cfg = dict(self.account.config or {})
        cfg['cspan_mvpd_auth'] = {
            'mso_id': self.mso_id, 'fingerprint': client._device_fingerprint, 'authn_token': authn_token,
            'client_id': client.ctx.client_id, 'client_secret': client.ctx.client_secret,
            'access_token': client.ctx.access_token, 'captured_at': int(time.time()),
        }
        self.account.config = cfg
        db.session.commit()
        for key in ('cspan_tve_client', 'cspan_tve_authz'):
            self.scraper._update_cache(key, {})
        persist_source_cache_updates(self.source.id, self.scraper._pending_cache_updates)
        self.scraper._pending_cache_updates = {}
        client.resource = _resource('cspan')
        try:
            client.authorize()
        except TVENotAuthorizedError:
            raise TVENotAuthorizedError('your package doesn\'t include C-SPAN')
        return 'authorized'


class _Cnn:
    """CNN — Adobe's v1 regcode sign-in under the `CNN` requestor, one
    sign-in for CNN, CNN International and HLN. Like C-SPAN, the sign-in is
    bound to the Adobe client and device it was made with, so both are saved
    with it."""
    error_key = 'cnn'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        from ..scrapers.cnn_tve import CNNTVEScraper
        self.account, self.mso_id = account, mso_id
        self.source = Source.query.filter_by(name='cnn_tve').first()
        if not self.source or not self.source.is_enabled:
            raise TVEAuthError('Enable the CNN TVE source before signing in to it.')
        self.scraper = CNNTVEScraper(config=dict(self.source.config or {}))

    def targets(self):
        return ['CNN']

    def start(self, label):
        from ..scrapers.cnn_tve import adobe_client
        client = adobe_client(self.scraper.software_statement())
        client.setup_client()
        client.register_device()
        client.create_regcode()
        return client.authenticate_redirect_url(self.mso_id), client

    def poll(self, client):
        try:
            client.fetch_session_token()
        except TVEPendingAuthError:
            return None
        return client.ctx.authn_token

    def save(self, client, authn_token):
        from ..config_store import persist_source_cache_updates
        from ..extensions import db

        cfg = dict(self.account.config or {})
        cfg['cnn_mvpd_auth'] = {
            'mso_id': self.mso_id, 'fingerprint': client._device_fingerprint, 'authn_token': authn_token,
            'client_id': client.ctx.client_id, 'client_secret': client.ctx.client_secret,
            'access_token': client.ctx.access_token, 'captured_at': int(time.time()),
        }
        self.account.config = cfg
        db.session.commit()
        for key in ('cnn_tve_client', 'cnn_tve_authz', 'cnn_streams'):
            self.scraper._update_cache(key, {})
        persist_source_cache_updates(self.source.id, self.scraper._pending_cache_updates)
        self.scraper._pending_cache_updates = {}
        try:
            client.authorize()
        except TVENotAuthorizedError:
            raise TVENotAuthorizedError('your package doesn\'t include CNN')
        return 'authorized'


class _Sundance:
    """SundanceTV — Adobe v1 under the SUNDANCE requestor, driven by DICE:
    log in to the Sundance account (email/password from the source
    settings), have Sundance issue the regcode, then trade the finished
    sign-in for a Sundance session that carries the TV-provider licence."""
    error_key = 'sundance'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        self.account, self.mso_id = account, mso_id
        self.source = Source.query.filter_by(name='sundance_tve').first()
        if not self.source or not self.source.is_enabled:
            raise TVEAuthError('Enable the SundanceTV source before signing in to it.')
        cfg = self.source.config or {}
        self.email, self.password = cfg.get('email') or '', cfg.get('password') or ''
        if not self.email or not self.password:
            raise TVEAuthError('Enter your Sundance email and password in the SundanceTV source settings first.')

    def targets(self):
        return ['SundanceTV']

    def start(self, label):
        from ..scrapers import sundance_tve as sd
        user = sd.login(self.email, self.password)
        client = sd.register_adobe_client()
        reg = sd.request_regcode(user['authorisationToken'], client)
        pro = (sd.claims(user['authorisationToken']).get('pro') or {}).get('id') or ''
        ctx = {'user': user['authorisationToken'], 'client': client, 'access_token': reg['accessToken'],
               'device': pro, 'next_exchange': 0.0}
        return sd.authenticate_url(reg['code'], self.mso_id), ctx

    def poll(self, ctx):
        from ..scrapers import sundance_tve as sd
        signed_in = sd.adobe_signed_in(ctx['access_token'], ctx['device']) if ctx['device'] else None
        if signed_in is False:
            return None
        # Adobe said yes, or gave no usable answer: ask Sundance, but not
        # more than every 15s on the no-answer path (it rate-limits).
        if signed_in is None and time.monotonic() < ctx['next_exchange']:
            return None
        ctx['next_exchange'] = time.monotonic() + 15
        return sd.exchange_adobe(ctx['user'], ctx['client'], ctx['access_token'], self.mso_id)

    def save(self, ctx, session):
        from ..extensions import db
        from ..scrapers import sundance_tve as sd

        if not sd.has_tv_provider(session['authorisationToken']):
            raise TVENotAuthorizedError('your package doesn\'t include SundanceTV')
        captured_at = int(time.time())
        cfg = dict(self.account.config or {})
        cfg['sundance_auth'] = {'mso_id': self.mso_id, 'refresh_token': session['refreshToken'],
                                'client_id': ctx['client']['client_id'],
                                'client_secret': ctx['client']['client_secret'], 'captured_at': captured_at}
        self.account.config = cfg
        db.session.commit()
        scraper = sd.SundanceTVEScraper(config=dict(self.source.config or {}))
        scraper._save_session({'auth': session['authorisationToken'], 'refresh': session['refreshToken'],
                               'signed_in_at': captured_at})
        return 'authorized'


class _Snp:
    """SportsNet Pittsburgh — Adobe v2 under the ATTSPORTSNET requestor, driven
    by DICE like _Sundance: log in to the SNP account (email/password from the
    source settings), sign in at Adobe with the account's profile id as the
    device, then trade the Adobe client's token for an SNP session that
    carries the TV-provider licence."""
    error_key = 'snp'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        self.account, self.mso_id = account, mso_id
        self.source = Source.query.filter_by(name='sportsnet_pittsburgh_tve').first()
        if not self.source or not self.source.is_enabled:
            raise TVEAuthError('Enable the SportsNet Pittsburgh TVE source before signing in to it.')
        cfg = self.source.config or {}
        self.email, self.password = cfg.get('email') or '', cfg.get('password') or ''
        if not self.email or not self.password:
            raise TVEAuthError('Enter your SNP email and password in the SportsNet Pittsburgh TVE source '
                               'settings first.')

    def targets(self):
        return ['SportsNet Pittsburgh']

    def start(self, label):
        from ..scrapers import sportsnet_pittsburgh_tve as snp
        user = snp.login(self.email, self.password)
        device = snp.profile_id(user['authorisationToken'])
        if not device:
            raise TVEAuthError('SportsNet Pittsburgh: the SNP sign-in has no profile id.')
        saved = (self.account.config or {}).get('snp_auth') or {}
        if saved.get('mso_id') != self.mso_id or saved.get('device_id') != device:
            saved = {}
        client = snp.adobe_client(device, saved)
        if client.client_id and client.client_secret:
            client.refresh_access_token()
        else:
            client._client_creds = None
            client._register_client()
        data = client._post(
            f'{snp.ADOBE_API}/api/v2/{snp.REQUESTOR_ID}/sessions',
            data={'mvpd': self.mso_id, 'redirectUrl': snp.REDIRECT_URL, 'domainName': snp.SITE},
            headers={**client._bearer_headers(), 'Content-Type': 'application/x-www-form-urlencoded'},
        ).json()
        ctx = {'user': user['authorisationToken'], 'client': client, 'device': device,
               'profile_url': f'{snp.ADOBE_API}/api/v2/{snp.REQUESTOR_ID}/profiles/{self.mso_id}'}
        if data.get('reasonType') == 'authenticated':
            return None, ctx
        if not data.get('url'):
            raise TVEAuthError('SportsNet Pittsburgh: Adobe did not return a sign-in URL.')
        return snp.ADOBE_API + data['url'], ctx

    def poll(self, ctx):
        from ..scrapers import sportsnet_pittsburgh_tve as snp
        client = ctx['client']
        r = client.session.get(ctx['profile_url'], headers=client._bearer_headers(), timeout=20)
        if r.status_code == 401:
            client.refresh_access_token()
            return None
        if not r.ok or not ((r.json() or {}).get('profiles') or {}).get(self.mso_id):
            return None
        creds = {'client_id': client.client_id, 'client_secret': client.client_secret}
        return snp.exchange_adobe(ctx['user'], creds, client.access_token, self.mso_id)

    def save(self, ctx, session):
        from ..extensions import db
        from ..scrapers import sportsnet_pittsburgh_tve as snp

        if not snp.has_tv_provider(session['authorisationToken']):
            raise TVENotAuthorizedError('your package doesn\'t include SportsNet Pittsburgh')
        client = ctx['client']
        captured_at = int(time.time())
        cfg = dict(self.account.config or {})
        cfg['snp_auth'] = {'mso_id': self.mso_id, 'refresh_token': session['refreshToken'],
                           'client_id': client.client_id, 'client_secret': client.client_secret,
                           'access_token': client.access_token, 'device_id': ctx['device'],
                           'captured_at': captured_at}
        self.account.config = cfg
        db.session.commit()
        scraper = snp.SportsNetPittsburghTVEScraper(config=dict(self.source.config or {}))
        scraper._save_session({'auth': session['authorisationToken'], 'refresh': session['refreshToken'],
                               'signed_in_at': captured_at})
        return 'authorized'


class _Dazn:
    """DAZN — two links, one after the other: DAZN's own "sign in with your
    phone" pairing (dazn.com/signin/device/<id>, opened where the user is
    signed in to DAZN), then the TV provider under the Gotham requestor,
    which DAZN binds to that same DAZN device. A saved DAZN sign-in that still
    renews skips the first link."""
    error_key = 'dazn'
    _ACCOUNT, _PROVIDER = 'DAZN account', 'TV provider (YES, MSG)'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        from ..scrapers.dazn_tve import DaznTVEScraper
        self.account, self.mso_id = account, mso_id
        self.source = Source.query.filter_by(name='dazn_tve').first()
        if not self.source or not self.source.is_enabled:
            raise TVEAuthError('Enable the DAZN source before signing in to it.')
        self.scraper = DaznTVEScraper(config=dict(self.source.config or {}))

    def targets(self):
        return [self._ACCOUNT, self._PROVIDER]

    def _saved(self) -> dict:
        from ..extensions import db
        db.session.refresh(self.account)
        return (self.account.config or {}).get('dazn_auth') or {}

    def start(self, label):
        from ..scrapers import dazn_tve as dz
        if label == self._ACCOUNT:
            saved = self._saved()
            if saved.get('token') and saved.get('device_id'):
                try:
                    token = dz.refresh_token(saved['token'], saved['device_id'])
                    return None, {'step': 'account', 'token': token, 'device_id': saved['device_id'], 'reused': True}
                except TVEAuthError:
                    pass
            device_id = dz.new_device_id()
            link = dz.request_device_link(device_id)
            return link['url'], {'step': 'account', 'device_id': device_id, 'second_screen_id': link['second_screen_id']}

        saved = self._saved()
        if not saved.get('token'):
            raise TVEAuthError('DAZN: sign in to your DAZN account first.')
        client = dz.register_adobe_client(saved['token'])
        headers = dz.adobe_headers(client['access_token'], saved['device_id'])
        r = requests.post(f'{dz.ADOBE_BASE}/api/v2/{dz.REQUESTOR_ID}/sessions', timeout=30, headers=headers,
                          data={'mvpd': self.mso_id, 'domainName': dz.DOMAIN_NAME, 'redirectUrl': dz.REDIRECT_URL})
        data = r.json() if r.ok else {}
        ctx = {'step': 'provider', 'client': client, 'headers': headers, 'device_id': saved['device_id'],
               'profile_url': f'{dz.ADOBE_BASE}/api/v2/{dz.REQUESTOR_ID}/profiles/{self.mso_id}'}
        if data.get('reasonType') == 'authenticated':
            return None, ctx
        if not data.get('url'):
            raise TVEAuthError(f'DAZN: Adobe did not return a sign-in URL (HTTP {r.status_code}).')
        return dz.ADOBE_BASE + data['url'], ctx

    def poll(self, ctx):
        from ..scrapers import dazn_tve as dz
        if ctx['step'] == 'account':
            return ctx.get('token') or dz.poll_device_link(ctx['second_screen_id'])
        r = requests.get(ctx['profile_url'], headers=ctx['headers'], timeout=20)
        if not r.ok:
            return None
        return ((r.json() or {}).get('profiles') or {}).get(self.mso_id) or None

    def save(self, ctx, result):
        from ..config_store import persist_source_cache_updates
        from ..extensions import db
        from ..scrapers import dazn_tve as dz

        cfg = dict(self.account.config or {})
        auth = dict(cfg.get('dazn_auth') or {})
        if ctx['step'] == 'account':
            if ctx.get('reused'):
                auth['token'] = result
                message = 'already signed in'
            else:
                auth = {'token': result, 'device_id': ctx['device_id'], 'captured_at': int(time.time())}
                message = 'signed in'
        else:
            token = dz.link_tv_provider(auth['token'], ctx['device_id'], ctx['client'], self.mso_id, result)
            auth.update({'token': token, 'mso_id': self.mso_id, 'linked_at': int(time.time()),
                         'captured_at': int(time.time())})
            message = 'authorized' if dz.rsn_status(token) == 'ActivePaid' else None
        cfg['dazn_auth'] = auth
        self.account.config = cfg
        db.session.commit()
        self.scraper._update_cache('dazn_session', {})
        self.scraper._update_cache('dazn_streams', {})
        persist_source_cache_updates(self.source.id, self.scraper._pending_cache_updates)
        self.scraper._pending_cache_updates = {}
        if message is None:
            raise TVENotAuthorizedError(f'your TV provider ({self.mso_id}) doesn\'t include YES or MSG on DAZN')
        return message


class _Fox:
    """FOX TVE — FOX's api3 regcode wrapper around an Adobe v2 link; the
    sign-in is tied to a fresh device_id, as in run_fox_browser_login."""
    error_key = 'fox'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        self.account, self.mso_id = account, mso_id

    def targets(self):
        return ['FOX']

    def start(self, label):
        from ..scrapers.fox_tve import _fox_json_headers
        session = requests.Session()
        device_id = str(uuid.uuid4())
        anon = session.post('https://api3.fox.com/v2.0/login', headers=_fox_json_headers(),
                            json={'deviceId': device_id}, timeout=30)
        anon.raise_for_status()
        headers = _fox_json_headers(anon.json()['accessToken'])
        reg = session.post('https://api3.fox.com/v2.0/accountregcode/v2', headers=headers,
                           json={'deviceId': device_id, 'isRegister': False, 'isMvpd': True,
                                 'selectedMvpdId': self.mso_id}, timeout=30)
        reg.raise_for_status()
        code = reg.json()['code']
        mvpd = session.post(f'https://api3.fox.com/v2.0/accountregcode/{code}/mvpdlogin', headers=headers,
                            json={'mvpdId': self.mso_id, 'redirectUrl': 'https://www.foxsports.com/live/fs1'},
                            timeout=30)
        mvpd.raise_for_status()
        return mvpd.json()['authenticateUrl'], {'session': session, 'headers': headers, 'device_id': device_id}

    def poll(self, ctx):
        from ..scrapers.fox_tve import _jwt_payload
        r = ctx['session'].get('https://api3.fox.com/v2.0/checkadobeauthn/v2', headers=ctx['headers'],
                               params={'device_id': ctx['device_id'], 'requestor': 'fbc-fox'}, timeout=30)
        if not r.ok:
            return None
        token = (r.json() or {}).get('accessToken') or ''
        # The same call answers with an anonymous token until the sign-in lands.
        return token if (_jwt_payload(token) or {}).get('mvpdid') == self.mso_id else None

    def save(self, ctx, token):
        from datetime import datetime, timezone

        from ..extensions import db
        from ..scrapers.fox_tve import _jwt_exp
        now = int(time.time())
        cfg = dict(self.account.config or {})
        cfg.update({
            'fox_sports_access_token': token,
            'fox_sports_access_token_exp': _jwt_exp(token) or (now + 3600),
            'fox_sports_access_token_mso': self.mso_id,
            'fox_sports_access_token_captured_at': now,
            'fox_sports_device_id': ctx['device_id'],
        })
        self.account.config = cfg
        self.account.last_auth_status = 'ok'
        self.account.last_auth_message = f'FOX Sports MVPD token obtained through {self.mso_id} (phone sign-in).'
        self.account.last_auth_at = datetime.now(timezone.utc)
        db.session.commit()
        return 'authorized'


class _Amcn:
    """AMC Networks — Adobe v2 per channel (AMC, BBCA, IFC, WETV), each its own
    requestor and link; same session/decision helpers as the browser flow."""
    error_key = 'amcn'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        from ..scrapers.amcn_tve import CHANNELS, AMCNetworksTVEScraper
        self.account, self.mso_id = account, mso_id
        self.source = Source.query.filter_by(name='amcn_tve').first()
        if not self.source:
            raise TVEAuthError('AMC Networks TVE source not found.')
        self.scraper = AMCNetworksTVEScraper(config=dict(self.source.config or {}))
        self.device_id = self.scraper._device_id()
        self.scraper.cache  # noqa: B018 — load before use
        self.channels = {ch.name: ch for ch in CHANNELS.values()}

    def targets(self):
        return list(self.channels)

    def start(self, label):
        channel = self.channels[label]
        statement = self.scraper._amcn_software_statement(channel, self.account)
        client, code, _mso_url, headers, response = self.scraper._adobe_session_redirect(
            channel, statement, self.device_id, self.mso_id, allow_empty_redirect=True)
        return str(response.url), {'channel': channel, 'client': client, 'code': code, 'headers': headers}

    def poll(self, ctx):
        from ..scrapers.amcn_tve import ADOBE_BASE
        # Check for the profile quietly first: _adobe_decision_finish logs a
        # warning with the whole response whenever the profile is empty,
        # which is just "not signed in yet" for as long as the user takes.
        try:
            r = ctx['client'].session.get(
                f"{ADOBE_BASE}/api/v2/{ctx['channel'].requestor_id}/profiles/code/{ctx['code']}",
                headers={**ctx['headers'], 'Content-Type': 'application/json'}, timeout=30)
            if not r.ok or not ((r.json() or {}).get('profiles') or {}).get(self.mso_id):
                return None
        except (requests.RequestException, ValueError):
            return None
        try:
            return self.scraper._adobe_decision_finish(
                ctx['client'].session, ctx['channel'], ctx['code'], self.mso_id, ctx['headers'])
        except TVENotAuthorizedError:
            raise
        except (TVEAuthError, requests.RequestException, ValueError):
            # Adobe answers the same way whether the user hasn't finished yet
            # or something else is off — keep polling (as the browser flow does).
            return None

    def save(self, ctx, result):
        from ..config_store import persist_source_cache_updates, persist_source_config_updates
        adobe_token, adobe_id, notafter_ms = result
        client, channel = ctx['client'], ctx['channel']
        self.scraper._save_adobe_session_cache(channel, self.mso_id, ctx['code'], client.ctx.access_token,
                                               client.ctx.client_id, client.ctx.client_secret)
        self.scraper._save_adobe_auth_cache(channel, self.mso_id, adobe_token, adobe_id, notafter_ms)
        persist_source_config_updates(self.source.id, self.scraper._pending_config_updates)
        persist_source_cache_updates(self.source.id, self.scraper._pending_cache_updates)
        self.scraper._pending_config_updates = {}
        self.scraper._pending_cache_updates = {}
        return 'authorized'


class _FoxOne:
    """FOX One — FOX's id.fox.com regcode wrapper around an Adobe v2 link.
    Signs in with whichever login FOX One is set up for (the shared TV
    provider or its own separate one — see FoxOneScraper._mvpd_login), so
    `mso_id` comes from that, not the caller. Completion has no status to
    poll: like the browser flow, we call the finish step (requests/complete
    + checkauthn) until FOX stops answering 404 — confirmed 2026-09-30 to
    work for a sign-in done on a phone, without our browser ever seeing
    FOX's callback page."""
    error_key = 'foxone'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        from ..scrapers.fox_one import FoxOneScraper
        self.source = Source.query.filter_by(name='fox_one').first()
        if not self.source:
            raise TVEAuthError('FOX One source not found.')
        self.scraper = FoxOneScraper(config=dict(self.source.config or {}))
        self.login = self.scraper._mvpd_login()
        if not self.login:
            raise TVEAuthError('FOX One has no TV provider login set up.')
        self.mso_id = self.login.mso_id

    def targets(self):
        return ['FOX One']

    def _persist(self):
        from ..config_store import persist_source_config_updates
        persist_source_config_updates(self.source.id, self.scraper._pending_config_updates)
        self.scraper._pending_config_updates = {}

    def start(self, label):
        session, request_id, device_id, _mso_url, response = self.scraper._foxone_mvpd_register(self.mso_id)
        self._persist()  # a freshly minted device_id
        return str(response.url), {'session': session, 'request_id': request_id, 'device_id': device_id}

    def poll(self, ctx):
        try:
            return self.scraper._foxone_mvpd_finish(ctx['session'], ctx['request_id'], ctx['device_id'], self.mso_id)
        except Exception:  # noqa: BLE001 — 404 until the sign-in lands
            return None

    def save(self, ctx, result):
        access_token, expires_at = result
        self.scraper._update_config('access_token', access_token)
        self.scraper._update_config('access_expires_at', expires_at)
        self.scraper._update_config('access_token_captured_at', int(time.time()))
        self.scraper.record_signin_result(self.login, None, how=' (phone sign-in)')
        self._persist()
        return 'authorized'


class _Discovery:
    """Discovery (HGTV and siblings) — not an Adobe link but Discovery's own TV
    pairing, the one its Android TV / Roku apps use (found in the HGTV
    Android TV app, 2026-09-30): an anonymous device asks
    /authentication/linkDevice/initiate for a 6-digit code tied to the brand
    and the user's TV provider (gauthPayload), the user enters it at
    watch.hgtv.com/activate and signs in with the provider there, and
    /authentication/linkDevice/login answers 204 until it lands, then returns
    the signed-in token. That token only comes back in the body — the
    session's `st` cookie is still the expired short-lived anonymous one — so
    it has to be set as `st`, after which the scraper's normal path works.
    Worked through Spectrum, which the browser sign-in struggles with."""
    error_key = 'discovery'

    def __init__(self, account, mso_id: str, requestor_id: str | None):
        from ..models import Source
        from ..scrapers.discovery_tve import DiscoveryTVEScraper
        self.account, self.mso_id = account, mso_id
        self.mso_name = ((account.config or {}).get('selected_mso_name') or mso_id).strip()
        self.source = Source.query.filter_by(name='discovery_tve').first()
        if not self.source:
            raise TVEAuthError('Discovery TVE source not found.')
        self.scraper = DiscoveryTVEScraper(config=dict(self.source.config or {}))

    def targets(self):
        return ['Discovery']

    def start(self, label):
        from ..config_store import persist_source_config_updates
        from ..scrapers.discovery_tve import API_BASE, BRAND_ID, PARTNER_ID, _auth_widget_headers
        session = self.scraper._session()
        device_id = self.scraper.config.get('device_id') or str(uuid.uuid4())
        if not self.scraper.config.get('device_id'):
            self.scraper._update_config('device_id', device_id)
            persist_source_config_updates(self.source.id, self.scraper._pending_config_updates)
            self.scraper._pending_config_updates = {}
        headers = _auth_widget_headers(device_id)
        session.get(f'{API_BASE}/token', params={'realm': 'go', 'deviceId': device_id, 'shortlived': 'true'},
                    headers=headers, timeout=30).raise_for_status()
        partner_id = PARTNER_ID if self.mso_id == 'Cox' else self.scraper._discovery_partner_id(
            session, device_id, self.mso_name, self.mso_id)
        if not partner_id:
            raise TVENotAuthorizedError(f'{self.mso_name} is not a participating TV provider for Discovery.')
        r = session.post(f'{API_BASE}/authentication/linkDevice/initiate',
                         json={'gauthPayload': {'brandId': BRAND_ID, 'partnerId': partner_id}},
                         headers={**headers, 'Content-Type': 'application/json'}, timeout=20)
        r.raise_for_status()
        attrs = ((r.json().get('data') or {}).get('attributes') or {})
        code = (attrs.get('linkingCode') or '').strip()
        if not code:
            raise TVEAuthError('Discovery did not return a sign-in code.')
        # The page that takes the code; targetUrl is its /link twin.
        return 'https://watch.hgtv.com/activate', {
            'session': session, 'headers': {**headers, 'Content-Type': 'application/json'}, 'code': code}

    def poll(self, ctx):
        from ..scrapers.discovery_tve import API_BASE
        r = ctx['session'].post(f'{API_BASE}/authentication/linkDevice/login', json={},
                                headers=ctx['headers'], timeout=20)
        if r.status_code != 200:
            return None  # 204 until the code is entered and the sign-in lands
        return ((r.json().get('data') or {}).get('attributes') or {}).get('token') or None

    def save(self, ctx, token):
        from ..config_store import persist_source_cache_updates
        from ..scrapers.discovery_tve import (API_BASE, SESSION_CACHE_KEY, _browser_headers, _cookie_dict,
                                              _jwt_exp, _restore_cookies)
        session = ctx['session']
        for c in list(session.cookies):
            if c.name == 'st':
                session.cookies.clear(c.domain, c.path, c.name)
        _restore_cookies(session, {'st': token})
        me = session.get(f'{API_BASE}/users/me', headers=_browser_headers(), timeout=15)
        attrs = ((me.json().get('data') or {}).get('attributes') or {}) if me.ok else {}
        if not me.ok or attrs.get('anonymous', True):
            raise TVEAuthError(f'Discovery didn\'t accept the new sign-in (HTTP {me.status_code}).')
        self.scraper._update_cache(SESSION_CACHE_KEY, {
            'cookies': _cookie_dict(session),
            'jwt_expires_at': _jwt_exp(token) or 0,
            'cached_at': int(time.time()),
        })
        persist_source_cache_updates(self.source.id, self.scraper._pending_cache_updates)
        self.scraper._pending_cache_updates = {}
        return 'authorized'


_ADAPTERS = {'legacy': _Legacy, 'nbc': _Nbc, 'tcm': _Tcm, 'fox': _Fox, 'amcn': _Amcn, 'foxone': _FoxOne,
             'discovery': _Discovery, 'versant': _Versant, 'univision': _Univision,
             'disney': _Disney, 'tennis': _Tennis, 'dazn': _Dazn, 'cspan': _Cspan,
             'sundance': _Sundance, 'cnn': _Cnn, 'nesn': _Nesn,
             'monumental': _Monumental, 'altitude': _Altitude, 'motv': _MyOutdoorTV,
             'snp': _Snp}


# ── job ──────────────────────────────────────────────────────────────────────

def _reenable_after_authorized_signin(network_key: str | None) -> None:
    """See reenable_not_authorized_channels. Never fails the sign-in."""
    try:
        from .accounts import reenable_not_authorized_channels
        revived = reenable_not_authorized_channels(network_key)
        if revived:
            logger.info('[link-login] %s: re-enabled %d channel(s) that had been switched off as not authorized',
                        network_key, revived)
            from ..routes.api_shared import _invalidate_and_refresh_xml
            _invalidate_and_refresh_xml()
    except Exception as exc:  # noqa: BLE001
        logger.warning('[link-login] %s: could not re-enable not-authorized channels: %s', network_key, exc)


def run_link_login(family: str, requestor_id: str | None, mso_id: str, run_id: str) -> None:
    """RQ job: sign in one network (AMC: its four channels) through links the
    user opens on their own device. Progress goes to STATUS_KEY; quits
    quietly once that key names a different run (a newer start) or is gone
    (stopped)."""
    import redis

    from app.worker import flask_app
    from .accounts import tve_account_for_network
    from .browser_login.common import _record_tve_login_error

    with flask_app.app_context():
        r = redis.from_url(flask_app.config['REDIS_URL'])
        steps: list[dict] = []
        base = {'run_id': run_id, 'family': family, 'requestor_id': requestor_id, 'mso_id': mso_id}

        def superseded() -> bool:
            raw = r.get(STATUS_KEY)
            return not raw or json.loads(raw).get('run_id') != run_id

        def quit_if_superseded(label: str) -> bool:
            """superseded(), logging why the job is quitting."""
            if not superseded():
                return False
            why = 'replaced by a newer sign-in' if r.get(STATUS_KEY) else 'stopped'
            logger.info('[link-login] %s: %s', label, why)
            return True

        def set_status(state: str, message: str = '', **extra) -> None:
            if not superseded():
                r.setex(STATUS_KEY, _PER_TARGET_TIMEOUT + 300, json.dumps(
                    {**base, 'state': state, 'message': message, 'steps': steps, **extra}))

        account = tve_account_for_network(requestor_id if family == 'legacy' else family)
        # FOX One can sign in with its own separate login, without a shared account.
        if not account and family != 'foxone':
            set_status('error', 'Set up your TV provider under Settings → TV Everywhere first.')
            return
        adapter = None
        try:
            adapter = _ADAPTERS[family](account, mso_id, requestor_id)
            targets = adapter.targets()
        except Exception as exc:  # noqa: BLE001
            logger.warning('[link-login] %s %s: setup failed: %s', family, requestor_id, exc)
            set_status('error', f'Could not start: {str(exc)[:200]}')
            return
        steps.extend({'label': label, 'state': 'pending'} for label in targets)

        for i, label in enumerate(targets):
            if quit_if_superseded(label):
                return
            steps[i]['state'] = 'running'
            try:
                url, ctx = adapter.start(label)
            except Exception as exc:  # noqa: BLE001
                tried = getattr(adapter, 'mso_id', mso_id)
                alias = _MVPD_ALIASES.get(tried)
                retried = None
                if alias:
                    logger.info('[link-login] %s: %s failed (%s), retrying as %s', label, tried, exc, alias)
                    try:
                        retry_adapter = _ADAPTERS[family](account, alias, requestor_id)
                        url, ctx = retry_adapter.start(label)
                        # Later targets and the saved sign-in use the id that worked.
                        adapter, mso_id, retried = retry_adapter, alias, True
                        base['mso_id'] = alias
                    except Exception as alias_exc:  # noqa: BLE001
                        logger.info('[link-login] %s: %s failed too: %s', label, alias, alias_exc)
                if not retried:
                    logger.warning('[link-login] %s: registration failed: %s', label, exc)
                    steps[i].update(state='failed', message=str(exc)[:160])
                    continue
            code = ctx.get('code') if isinstance(ctx, dict) else None
            set_status('waiting', f'Sign in for {label}', label=label, url=url or '', code=code or '')
            logger.info('[link-login] %s: waiting for the %s sign-in', label, getattr(adapter, 'mso_id', mso_id))

            result, denied = None, None
            deadline = time.monotonic() + _PER_TARGET_TIMEOUT
            delay = _POLL_SECONDS
            while time.monotonic() < deadline:
                if quit_if_superseded(label):
                    return
                try:
                    result = adapter.poll(ctx)
                except TVENotAuthorizedError as exc:
                    denied = str(exc)
                    break
                except Exception as exc:  # noqa: BLE001
                    logger.info('[link-login] %s: poll failed, retrying: %s', label, exc)
                if result:
                    break
                time.sleep(delay)
                delay = min(delay * 1.3, _POLL_MAX_SECONDS)

            if denied:
                steps[i].update(state='failed', message=denied[:160])
                logger.info('[link-login] %s: not authorized: %s', label, denied)
            elif not result:
                steps[i].update(state='failed', message='the link expired before the sign-in finished')
            else:
                try:
                    message = adapter.save(ctx, result)
                    steps[i].update(state='done', message=message)
                    logger.info('[link-login] %s: signed in via %s (%s)', label, getattr(adapter, 'mso_id', mso_id), message)
                    if message == 'authorized':
                        _reenable_after_authorized_signin(adapter.error_key)
                except TVENotAuthorizedError as exc:
                    steps[i].update(state='failed', message=str(exc)[:160])
                    logger.info('[link-login] %s: not authorized after sign-in: %s', label, exc)
                except Exception as exc:  # noqa: BLE001
                    logger.exception('[link-login] %s: save failed', label)
                    steps[i].update(state='failed', message=f'save failed: {str(exc)[:120]}')

        done = [s['label'] for s in steps if s['state'] == 'done']
        failed = [f"{s['label']}: {s.get('message', 'failed')}" for s in steps if s['state'] != 'done']
        if done:
            message = 'Signed in: ' + ', '.join(done) + '.'
            if failed:
                message += ' Not signed in: ' + '; '.join(failed) + '.'
            set_status('success', message)
        else:
            message = '; '.join(failed) or 'Sign-in failed.'
            if family == 'foxone':
                adapter.scraper.record_signin_result(adapter.login, message)
                adapter._persist()
            else:
                _record_tve_login_error(adapter.error_key, message)
            set_status('error', message)

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
Discovery and YouTube TV's Google step aren't Adobe, so they stay browser-only.
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
LINK_FAMILIES = ('legacy', 'nbc', 'fox', 'amcn')
_PER_TARGET_TIMEOUT = 10 * 60
_POLL_SECONDS = 3.0
_POLL_MAX_SECONDS = 15.0


def job_timeout(family: str) -> int:
    targets = 4 if family == 'amcn' else 1
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


_ADAPTERS = {'legacy': _Legacy, 'nbc': _Nbc, 'fox': _Fox, 'amcn': _Amcn, 'foxone': _FoxOne}


# ── job ──────────────────────────────────────────────────────────────────────

def run_link_login(family: str, requestor_id: str | None, mso_id: str, run_id: str) -> None:
    """RQ job: sign in one network (AMC: its four channels) through links the
    user opens on their own device. Progress goes to STATUS_KEY; quits
    quietly once that key names a different run (a newer start) or is gone
    (stopped)."""
    import redis

    from app.worker import flask_app
    from ..models import TVEAccount
    from .browser_login.common import _record_tve_login_error

    with flask_app.app_context():
        r = redis.from_url(flask_app.config['REDIS_URL'])
        steps: list[dict] = []
        base = {'run_id': run_id, 'family': family, 'requestor_id': requestor_id, 'mso_id': mso_id}

        def superseded() -> bool:
            raw = r.get(STATUS_KEY)
            return not raw or json.loads(raw).get('run_id') != run_id

        def set_status(state: str, message: str = '', **extra) -> None:
            if not superseded():
                r.setex(STATUS_KEY, _PER_TARGET_TIMEOUT + 300, json.dumps(
                    {**base, 'state': state, 'message': message, 'steps': steps, **extra}))

        account = TVEAccount.query.filter_by(provider_id='mvpd').first()
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
            if superseded():
                return
            steps[i]['state'] = 'running'
            try:
                url, ctx = adapter.start(label)
            except Exception as exc:  # noqa: BLE001
                logger.warning('[link-login] %s: registration failed: %s', label, exc)
                steps[i].update(state='failed', message=str(exc)[:160])
                continue
            set_status('waiting', f'Sign in for {label}', label=label, url=url or '')
            logger.info('[link-login] %s: waiting for the %s sign-in', label, getattr(adapter, 'mso_id', mso_id))

            result, denied = None, None
            deadline = time.monotonic() + _PER_TARGET_TIMEOUT
            delay = _POLL_SECONDS
            while time.monotonic() < deadline:
                if superseded():
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
                steps[i].update(state='failed', message='your package doesn\'t include it')
                logger.info('[link-login] %s: not authorized: %s', label, denied)
            elif not result:
                steps[i].update(state='failed', message='the link expired before the sign-in finished')
            else:
                try:
                    message = adapter.save(ctx, result)
                    steps[i].update(state='done', message=message)
                    logger.info('[link-login] %s: signed in via %s (%s)', label, getattr(adapter, 'mso_id', mso_id), message)
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

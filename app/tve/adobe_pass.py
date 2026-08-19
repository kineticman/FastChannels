from __future__ import annotations

import html
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass
from html.parser import HTMLParser
from time import monotonic
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests

logger = logging.getLogger(__name__)

_COX_LOGIN_THROTTLE_KEY = 'tve:cox-login:last-at'
_COX_LOGIN_THROTTLE_SECONDS = 8.0

# Separate key/interval from Cox's — a different MVPD's rate limiting is a
# different budget, so a Cox login shouldn't delay a DIRECTV one or vice
# versa. Interval is longer than Cox's: confirmed live 2026-08-17 that
# Akamai Bot Manager on identity.directv.com starts 403ing DIRECTV login
# attempts fired within ~15s of each other even with proper curl_cffi
# impersonation (see directv.py's directv_login docstring) — an 8s gap
# (Cox's own interval) was not enough headroom when observed back-to-back.
_DIRECTV_LOGIN_THROTTLE_KEY = 'tve:directv-login:last-at'
_DIRECTV_LOGIN_THROTTLE_SECONDS = 20.0

# Atomically reserves the next login slot at least THROTTLE_SECONDS after the
# previous one and returns it, so concurrent callers each get a distinct slot
# instead of racing a separate GET (read) and SET (write).
_RESERVE_SLOT_SCRIPT = """
local last = tonumber(redis.call('GET', KEYS[1]) or '0')
local interval = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local slot = math.max(now, last + interval)
redis.call('SET', KEYS[1], tostring(slot), 'EX', 60)
return tostring(slot)
"""


def _throttle_login(key: str, interval_seconds: float, label: str) -> None:
    """Shared slot-reservation mechanics behind throttle_cox_login() and
    throttle_directv_login() — see throttle_cox_login()'s docstring for the
    full reasoning (Redis-backed so it works across processes/routes,
    sleep-based so callers don't need special handling, fails open if Redis
    is unavailable, and uses a single atomic Lua script so concurrent
    callers get distinct monotonically-spaced slots instead of racing a
    plain GET-then-SET).
    """
    try:
        import redis
        from ..config import Config
        r = redis.from_url(Config.REDIS_URL)
        next_slot = r.eval(_RESERVE_SLOT_SCRIPT, 1, key, interval_seconds, time.time())
    except Exception:  # noqa: BLE001
        return
    remaining = float(next_slot) - time.time()
    if remaining > 0:
        logger.info('[adobe-pass] throttling %s login by %.1fs (another network signed in recently)', label, remaining)
        time.sleep(remaining)


def throttle_cox_login() -> None:
    """Enforces a minimum gap between real Cox credential POSTs across every
    TVE network and entry point — legacy/AMC/Discovery's _cox_saml_login,
    FOX Sports, and FOX One (a different Cox endpoint, identityhydra rather
    than Okta, but the same account and the same "don't hammer it" concern).

    Code review, 2026-08-11: the client-side cooldown built into the admin
    settings modal (settings.js's localStorage-based wait) only throttles
    clicks within one browser tab — a second tab, a different device, or a
    direct API/script call bypassed it completely, leaving the actual
    invariant ("don't trigger Cox/Okta's rate limiting") unenforced where
    the real risk lives. This is a plain wall-clock timestamp in Redis
    (not app.worker's job-id locks, which don't cover FOX One's route at
    all — that one isn't an RQ job) so it works the same regardless of
    which process or route the login came through.
    """
    _throttle_login(_COX_LOGIN_THROTTLE_KEY, _COX_LOGIN_THROTTLE_SECONDS, 'Cox')


def throttle_directv_login() -> None:
    """Enforces a minimum gap between real DIRECTV credential POSTs across
    every TVE network and entry point — see throttle_cox_login()'s docstring
    for the shared reasoning; this exists because DIRECTV needed its own
    (confirmed live 2026-08-17 — see _DIRECTV_LOGIN_THROTTLE_SECONDS)."""
    _throttle_login(_DIRECTV_LOGIN_THROTTLE_KEY, _DIRECTV_LOGIN_THROTTLE_SECONDS, 'DIRECTV')


_DIRECTV_LOGIN_FAILURE_COOLDOWN_KEY = 'tve:directv-login:recent-failure'
# Confirmed live 2026-08-17: unlike the ~20s spacing throttle_directv_login()
# enforces between individual attempts, Akamai's block on identity.directv.com
# outlasted a 90s gap during testing — a single audit pass across a
# multi-brand TVE scraper (e.g. Warner's TNT/TBS/truTV, A+E's four networks)
# calls authorize_mvpd() once per brand/requestor_id, each a fresh live
# DIRECTV login attempt with no memory of the others. Once one fails, every
# other channel in that same audit pass is essentially guaranteed to hit the
# same block — worse, it keeps re-triggering (and likely extending) exactly
# the cooldown being waited out. This short-circuits the rest of that pass
# (and any other TVE scraper's channels checked shortly after) instead of
# burning more live attempts against a currently-blocked identity provider.
_DIRECTV_LOGIN_FAILURE_COOLDOWN_SECONDS = 180.0


def _mark_directv_login_failed(reason: str = '') -> None:
    try:
        import redis
        from ..config import Config
        r = redis.from_url(Config.REDIS_URL)
        r.set(
            _DIRECTV_LOGIN_FAILURE_COOLDOWN_KEY, (reason or 'DIRECTV sign-in failed')[:300],
            ex=int(_DIRECTV_LOGIN_FAILURE_COOLDOWN_SECONDS),
        )
    except Exception:  # noqa: BLE001
        pass


def directv_login_cooldown() -> dict | None:
    """Returns {'remaining': int seconds, 'reason': str} if DIRECTV sign-in
    is currently cooling down after a recent failure, else None.

    Reads Redis's own TTL on the failure key rather than a separately
    tracked "cooldown until" timestamp, so "remaining" is always exact —
    no separate expiry bookkeeping to keep in sync.

    Exposed for MvpdCooldownMixin's duck-typed _cooldown_active/
    _cooldown_remaining/_cooldown_reason trio, the same protocol
    app.worker's audit progress reporter already looks for on any scraper
    (see roku.py's own per-source 403 cooldown for the established
    pattern) so the admin audit modal shows "paused, rate-limited" instead
    of every TVE channel just erroring out with no visible explanation.
    Unlike Roku's cooldown (tracked per-source, in that source's own
    config), this one is account-wide — the shared TVE MVPD login, not any
    one scraper — so it lives in Redis, keyed globally, and every TVE
    scraper's mixin reads the same shared state instead of tracking its
    own.
    """
    try:
        import redis
        from ..config import Config
        r = redis.from_url(Config.REDIS_URL)
        ttl = r.ttl(_DIRECTV_LOGIN_FAILURE_COOLDOWN_KEY)
        if ttl is None or ttl <= 0:
            return None
        reason = r.get(_DIRECTV_LOGIN_FAILURE_COOLDOWN_KEY)
        return {
            'remaining': int(ttl),
            'reason': (reason.decode() if isinstance(reason, bytes) else reason) or 'DIRECTV sign-in failed',
        }
    except Exception:  # noqa: BLE001
        return None


class MvpdCooldownMixin:
    """Duck-typed _cooldown_active/_cooldown_remaining/_cooldown_reason —
    see directv_login_cooldown()'s docstring for why/how. Every TVE scraper
    class mixes this in (ahead of BaseScraper) so app.worker's audit
    progress reporter picks it up automatically, same as roku.py's own
    implementation of the same protocol.

    _cooldown_wait_in_audit = False opts these scrapers out of app.worker's
    audit loop actively sleeping through an active cooldown and retrying —
    that behavior (Roku's default) assumes the cooldown reliably clears
    within the audit's patience, which held for Roku but not for DIRECTV's
    MVPD login as of 2026-08-17 (see worker.py's comment at the call site):
    a retry that fails just re-arms a fresh cooldown, so the wait-and-retry
    loop was burning ~2-3 minutes per channel for zero benefit. Cooldown
    state still surfaces in the audit modal either way — that's driven by
    _audit_progress()'s own independent check, not this loop.
    """

    _cooldown_wait_in_audit = False

    def _cooldown_active(self) -> bool:
        return directv_login_cooldown() is not None

    def _cooldown_remaining(self) -> int:
        cooldown = directv_login_cooldown()
        return cooldown['remaining'] if cooldown else 0

    @property
    def _cooldown_reason(self) -> str | None:
        cooldown = directv_login_cooldown()
        return cooldown['reason'] if cooldown else None


ADOBE_BASE = 'https://sp.auth.adobe.com'
AUTHENTICATE_URL = f'{ADOBE_BASE}/adobe-services/authenticate/saml'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
DEFAULT_HISTORY_REDIRECT_URL = 'https://www.history.com/mvpd-auth?redirect_url=https%3A%2F%2Fplay.history.com%2Flive'
HISTORY_RESOURCE = '<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/"><channel><title>HISTORY</title><item></item></channel></rss>'
HISTORY_DAI_MASTER = 'https://dai.google.com/linear/hls/event/3pCfCAVSTz24VQ7jZDXLzw/master.m3u8'
AENETWORKS_LIVE_PAGES = {
    'history': 'https://play.history.com/live',
    'aetv': 'https://play.aetv.com/live',
    'lifetime': 'https://play.mylifetime.com/live',
    'fyi': 'https://play.fyi.tv/live',
}

_STATEMENT_CACHE: dict[str, tuple[float, str]] = {}
_STATEMENT_TTL_SECONDS = 6 * 60 * 60

# Adobe's own authz_token (from /adobe-services/authorize) is good for
# roughly 24h — confirmed against dev/tve2/README.md's reverse-engineering of
# a comparable Adobe Pass client, which caches it by resource and refreshes
# within 5 minutes of expiry. We previously re-minted it (plus a fresh
# shortAuthorize) on every single resolve() even when authn_token was
# already cached, costing two extra Adobe round trips per play/audit for no
# benefit — see authorize_mvpd()'s cached-authz fast path. shortAuthorize
# itself is NOT cached this way: Adobe gives it only ~7 minutes, so it's
# deliberately re-minted fresh every time regardless of authz freshness.
_AUTHZ_TOKEN_TTL_SECONDS = 24 * 60 * 60
_AUTHZ_REFRESH_MARGIN_SECONDS = 5 * 60


class TVEAuthError(RuntimeError):
    pass


class TVENotAuthorizedError(TVEAuthError):
    pass


class TVEPendingAuthError(TVEAuthError):
    """Adobe session isn't authenticated yet — not a failure, just not done.

    Raised by fetch_session_token() while polling during a browser-assisted
    pairing, before the human has finished logging in at the MSO.
    """
    pass


@dataclass
class AdobeContext:
    software_statement: str
    client_id: str = ''
    client_secret: str = ''
    access_token: str = ''
    device_id: str = ''
    reg_code: str = ''
    pass_sfp: str = ''
    authn_token: str = ''
    authz_token: str = ''
    short_token: str = ''


class _FormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_form = False
        self.action = ''
        self.inputs: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs) -> None:
        attrs = dict(attrs)
        if tag.lower() == 'form' and not self.in_form:
            self.in_form = True
            self.action = attrs.get('action') or ''
            return
        if self.in_form and tag.lower() == 'input':
            name = attrs.get('name')
            if name:
                self.inputs[name] = attrs.get('value') or ''

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == 'form' and self.in_form:
            self.in_form = False


def _hidden_form(document: str, base_url: str) -> tuple[str, dict[str, str]]:
    parser = _FormParser()
    parser.feed(document)
    if not parser.action and not parser.inputs:
        raise TVEAuthError('Expected Cox SAML form but none was found.')
    return urljoin(base_url, parser.action or base_url), parser.inputs


def _text_between(text: str, tag: str) -> str:
    m = re.search(rf'<{tag}\b[^>]*>(.*?)</{tag}>', text, flags=re.S)
    if not m:
        raise TVEAuthError(f'Missing <{tag}> in Adobe response.')
    value = m.group(1)
    value = re.sub(r'^<!\[CDATA\[(.*)\]\]>$', r'\1', value, flags=re.S)
    return value


def _adobe_error_code(text: str) -> str:
    m = re.search(r'<error\b[^>]*>(.*?)</error>', text or '', flags=re.S)
    if not m:
        return ''
    tag = re.search(r'<([A-Za-z0-9_.:-]+)\b[^>]*/?>', m.group(1))
    return tag.group(1) if tag else ''


def _adobe_error_message(text: str) -> str:
    try:
        details = _text_between(text, 'details').strip()
    except TVEAuthError:
        details = ''
    if details:
        return details

    code = _adobe_error_code(text)
    if code:
        return f'Adobe authorization error: {code}'

    return 'Adobe authorization error'


def _safe_url(value: str) -> str:
    split = urlsplit(value or '')
    safe_query = []
    for key, val in parse_qsl(split.query, keep_blank_values=True):
        if re.search(r'auth|token|sig|key|policy|hdntl|hdnts|jwt|saml', key, re.I):
            safe_query.append((key, '<redacted>'))
        else:
            safe_query.append((key, val))
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(safe_query), ''))


def _script_urls(html_text: str, page_url: str) -> list[str]:
    urls: list[str] = []
    for match in re.finditer(r'<script\b[^>]+src=["\']([^"\']+)["\']', html_text, flags=re.I):
        src = match.group(1)
        if '/build/desktop/' in src or '/_next/static/' in src:
            urls.append(urljoin(page_url, src))
    return urls


def _extract_adobe_statement(js_text: str, brand: str) -> Optional[str]:
    brand = re.escape(brand)
    patterns = [
        rf'adobeSoftwareStatement=\{{.*?{brand}:"([^"]+)"',
        rf'adobeSoftwareStatement:\{{.*?{brand}:"([^"]+)"',
    ]
    for pattern in patterns:
        m = re.search(pattern, js_text, flags=re.S)
        if m and m.group(1).startswith('eyJ'):
            return m.group(1)
    return None


def discover_aenetworks_software_statement(brand: str = 'history', session: Optional[requests.Session] = None) -> str:
    key = brand.lower()
    cached = _STATEMENT_CACHE.get(key)
    if cached and monotonic() - cached[0] < _STATEMENT_TTL_SECONDS:
        return cached[1]

    sess = session or requests.Session()
    sess.headers.setdefault('User-Agent', UA)
    page_url = AENETWORKS_LIVE_PAGES.get(key, AENETWORKS_LIVE_PAGES['history'])
    try:
        r = sess.get(page_url, timeout=20)
        r.raise_for_status()
    except requests.RequestException as exc:
        raise TVEAuthError(str(exc)) from exc

    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', r.text, flags=re.S)
    if m:
        try:
            data = json.loads(html.unescape(m.group(1)))
            statement = (((data.get('props') or {}).get('config') or {}).get('adobeSoftwareStatement') or {}).get(key)
            if statement and statement.startswith('eyJ'):
                _STATEMENT_CACHE[key] = (monotonic(), statement)
                return statement
        except (TypeError, ValueError, json.JSONDecodeError):
            pass

    for script_url in _script_urls(r.text, page_url):
        try:
            js = sess.get(script_url, timeout=20).text
        except requests.RequestException:
            continue
        statement = _extract_adobe_statement(js, key)
        if statement:
            _STATEMENT_CACHE[key] = (monotonic(), statement)
            return statement
    raise TVEAuthError('Could not discover A+E Adobe software statement from the live site.')


def invalidate_aenetworks_software_statement(brand: str) -> None:
    """Drop a cached discovered statement so the next call re-discovers it.

    Called when a client bootstrapped from the cached value fails during
    setup, since a stale/rotated statement is rejected before credentials
    are ever checked.
    """
    _STATEMENT_CACHE.pop(brand.lower(), None)


class AdobePassCoxClient:
    def __init__(
        self, *, requestor_id: str, resource: str, software_statement: str,
        redirect_url: str = DEFAULT_HISTORY_REDIRECT_URL, device_fingerprint: str | None = None,
        client_creds: dict | None = None,
    ) -> None:
        self.requestor_id = requestor_id
        self.resource = resource
        self.redirect_url = redirect_url
        self._device_fingerprint = device_fingerprint
        self._client_creds = client_creds
        self.ctx = AdobeContext(software_statement=software_statement)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': UA,
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
        })

    def _post(self, url: str, **kwargs) -> requests.Response:
        try:
            r = self.session.post(url, timeout=30, **kwargs)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            raise TVEAuthError(str(exc)) from exc

    def _post_lenient(self, url: str, **kwargs) -> requests.Response:
        """Like _post(), but doesn't raise on a non-2xx status.

        Adobe returns error XML bodies (including notAuthorized) under a mix
        of HTTP statuses — sometimes 200, sometimes 400 — so callers that need
        to inspect the body for a specific error code (authorize()) use this
        instead of _post(), which would raise_for_status() and discard the
        body before that inspection ever runs.
        """
        try:
            return self.session.post(url, timeout=30, **kwargs)
        except requests.RequestException as exc:
            raise TVEAuthError(str(exc)) from exc

    def setup_client(self) -> None:
        # A fresh client_creds means a caller looked up a still-fresh
        # (client_id, client_secret, access_token) this account already
        # registered with Adobe for this requestor_id — reuse it instead of
        # minting a brand-new OAuth client on every single attempt. See
        # load_cached_adobe_client_creds()'s docstring: registering a new
        # client every attempt is its own form of the identity churn that
        # register_device()'s docstring already documents mattering for
        # device fingerprints.
        cached = self._client_creds
        if cached and cached.get('client_id') and cached.get('access_token'):
            self.ctx.client_id = cached['client_id']
            self.ctx.client_secret = cached.get('client_secret', '')
            self.ctx.access_token = cached['access_token']
            self.session.headers.update({'Authorization': f'Bearer {self.ctx.access_token}'})
            return

        r = self._post(
            f'{ADOBE_BASE}/o/client/register',
            json={'software_statement': self.ctx.software_statement},
            headers={'Content-Type': 'application/json; charset=UTF-8'},
        )
        data = r.json()
        self.ctx.client_id = data['client_id']
        self.ctx.client_secret = data['client_secret']

        r = self._post(
            f'{ADOBE_BASE}/o/client/token',
            data={
                'grant_type': 'client_credentials',
                'client_id': self.ctx.client_id,
                'client_secret': self.ctx.client_secret,
            },
            headers={'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'},
        )
        self.ctx.access_token = r.json()['access_token']
        self.session.headers.update({'Authorization': f'Bearer {self.ctx.access_token}'})

    def register_device(self) -> None:
        # A fresh random fingerprint on every call (the old unconditional
        # uuid4().hex here) registers a NEW device with Adobe each time —
        # confirmed live 2026-08-11 against Warner TVE that this invalidates
        # whatever session a previously cached authn_token belonged to, so
        # authorize_mvpd()'s cache-reuse fast path always got rejected with
        # "session has expired (pendingLogout)" and fell through to a full
        # fresh Cox login on every single play, defeating the cache
        # entirely (same login-storm risk class as the fubo/Cox fixes,
        # just triggered by device churn instead of no caching at all).
        # Callers now pass a stable, persisted fingerprint (see
        # _ensure_cox_device_fingerprint) so repeated calls register the
        # SAME device and a cached authn_token's session stays valid.
        fingerprint = self._device_fingerprint or uuid.uuid4().hex
        r = self._post(
            f'{ADOBE_BASE}/indiv/devices',
            json={'fingerprint': fingerprint},
            headers={'Content-Type': 'application/json; charset=UTF-8'},
        )
        self.ctx.device_id = r.json()['deviceId']
        self.ctx.pass_sfp = r.headers.get('pass_sfp') or ''
        if self.ctx.pass_sfp:
            self.session.headers.update({'pass_sfp': self.ctx.pass_sfp})
        self.session.headers.update({
            'ap_42': 'anonymous',
            'ap_11': 'Windows 10',
            'ap_z': UA,
            'Ap_21': self.ctx.device_id,
        })

    def create_regcode(self) -> None:
        r = self._post(
            f'{ADOBE_BASE}/reggie/v1/{self.requestor_id}/regcode',
            data={'requestor': self.requestor_id, 'deviceId': self.ctx.device_id, 'format': 'json'},
            headers={'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'},
        )
        self.ctx.reg_code = r.json()['code']

    def authenticate_redirect_url(self, mso_id: str) -> str:
        """Build the authenticate/saml URL for a given MSO without fetching it.

        Requires setup_client()/register_device()/create_regcode() to have run
        first. Used by browser-assisted pairing: a real browser (not this
        client) opens this URL and a human completes the MSO's own login page,
        which redirects through Adobe's SAML consumer and binds the resulting
        auth to this client's reg_code server-side — so fetch_session_token()
        can later pick it up from an entirely different HTTP session.
        """
        return AUTHENTICATE_URL + '?' + urlencode({
            'noflash': 'true',
            'mso_id': mso_id,
            'requestor_id': self.requestor_id,
            'no_iframe': 'false',
            'domain_name': 'adobe.com',
            'redirect_url': self.redirect_url,
            'reg_code': self.ctx.reg_code,
        })

    def authenticate_with_cox(self, username: str, password: str) -> None:
        try:
            r = self.session.get(
                AUTHENTICATE_URL,
                params={
                    'noflash': 'true',
                    'mso_id': 'Cox',
                    'requestor_id': self.requestor_id,
                    'no_iframe': 'false',
                    'domain_name': 'adobe.com',
                    'redirect_url': self.redirect_url,
                    'reg_code': self.ctx.reg_code,
                },
                allow_redirects=False,
                timeout=30,
            )
        except requests.RequestException as exc:
            raise TVEAuthError(str(exc)) from exc
        if r.status_code not in {301, 302, 303, 307, 308}:
            raise TVEAuthError(f'Adobe authenticate did not redirect to Cox: HTTP {r.status_code}.')
        cox_saml_url = r.headers.get('location') or ''
        if 'login.cox.com' not in cox_saml_url:
            raise TVEAuthError(f'Unexpected Adobe authenticate redirect host: {urlsplit(cox_saml_url).netloc}.')

        try:
            self.session.get(cox_saml_url, allow_redirects=True, timeout=30)
            throttle_cox_login()
            login_user = username.split('@', 1)[0] if username.lower().endswith('@cox.net') else username
            r = self.session.post(
                'https://login.cox.com/api/v1/authn',
                json={
                    'username': login_user,
                    'password': password,
                    'options': {'warnBeforePasswordExpired': True, 'multiOptionalFactorEnroll': True},
                },
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'Origin': 'https://login.cox.com',
                    'Referer': cox_saml_url,
                    'x-okta-user-agent-extended': 'okta-signin-widget-5.16.1',
                },
                timeout=30,
            )
            r.raise_for_status()
        except requests.RequestException as exc:
            raise TVEAuthError(str(exc)) from exc
        auth = r.json()
        if auth.get('status') != 'SUCCESS' or not auth.get('sessionToken'):
            raise TVEAuthError(f'Cox authn did not succeed: {auth.get("status") or "unknown"}.')

        redirect = 'https://login.cox.com/login/sessionCookieRedirect?' + urlencode({
            'checkAccountSetupComplete': 'true',
            'token': auth['sessionToken'],
            'redirectUrl': cox_saml_url,
        })
        try:
            r = self.session.get(redirect, allow_redirects=True, timeout=30)
            r.raise_for_status()
        except requests.RequestException as exc:
            raise TVEAuthError(str(exc)) from exc
        action, form = _hidden_form(r.text, str(r.url))
        if 'SAMLResponse' not in form:
            raise TVEAuthError('Cox SAML page did not include SAMLResponse.')

        try:
            r = self.session.post(
                action,
                data=form,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                allow_redirects=False,
                timeout=30,
            )
        except requests.RequestException as exc:
            raise TVEAuthError(str(exc)) from exc
        if r.status_code not in {200, 301, 302, 303, 307, 308}:
            raise TVEAuthError(f'Adobe SAML consumer returned HTTP {r.status_code}.')

    def fetch_session_token(self) -> None:
        try:
            r = self.session.post(
                f'{ADOBE_BASE}/adobe-services/session',
                data={'_method': 'GET', 'reg_code': self.ctx.reg_code, 'requestor_id': self.requestor_id},
                headers={'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'},
                timeout=30,
            )
        except requests.RequestException as exc:
            raise TVEAuthError(str(exc)) from exc
        # 401 "Data not found" is Adobe's response when no session record
        # exists yet for this reg_code at all — before the MSO round-trip has
        # completed even once. That's a *different* not-done-yet state than
        # the 200 "<pendingLogout" response (a session exists but isn't
        # authenticated yet), but both mean the same thing to a caller: the
        # human hasn't finished signing in at the MSO. Only surfaced during
        # browser-assisted pairing polling (app.worker.run_mvpd_browser_login)
        # — the scripted Cox flow never observes this, since it only calls
        # fetch_session_token() after authenticate_with_cox() has already
        # completed the round-trip synchronously.
        if r.status_code == 401:
            raise TVEPendingAuthError('Adobe has no session yet for this reg_code.')
        if r.status_code >= 400:
            raise TVEAuthError(f'Adobe session endpoint returned HTTP {r.status_code}: {r.text[:300]}')
        if '<pendingLogout' in r.text:
            raise TVEPendingAuthError('Adobe session is not authenticated yet.')
        self.ctx.authn_token = html.unescape(_text_between(r.text, 'authnToken'))

    def _short_authorize(self, session_guid: str) -> str:
        """POST /adobe-services/shortAuthorize using whatever authz_token is
        already on self.ctx.authz_token — either just minted by authorize()
        or a cached ~24h authz_token reused via authorize_with_cached_authz().
        Always hit fresh regardless: Adobe gives this response only ~7
        minutes, so caching it the way authz_token is cached would just mean
        handing back an already-expired token most of the time.
        """
        r = self._post_lenient(
            f'{ADOBE_BASE}/adobe-services/shortAuthorize',
            data={
                'authz_token': self.ctx.authz_token,
                'requestor_id': self.requestor_id,
                'generic_data': '{}',
                'session_guid': session_guid,
                'hashed_guid': 'false',
            },
            headers={'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'},
        )
        if '<pendingLogout' in r.text:
            raise TVEAuthError('Adobe shortAuthorize returned pendingLogout.')
        if '<error' in r.text:
            message = _adobe_error_message(r.text)
            if _adobe_error_code(r.text) == 'notAuthorized':
                raise TVENotAuthorizedError(message)
            raise TVEAuthError(message)
        if r.status_code >= 400:
            raise TVEAuthError(f'Adobe shortAuthorize returned HTTP {r.status_code}: {r.text[:300]}')
        self.ctx.short_token = r.text
        return self.ctx.short_token

    def authorize(self) -> str:
        mso = _text_between(self.ctx.authn_token, 'simpleTokenMsoID')
        guid = _text_between(self.ctx.authn_token, 'simpleSamlNameID')
        session_index = _text_between(self.ctx.authn_token, 'simpleSamlSessionIndex')
        session_guid = _text_between(self.ctx.authn_token, 'simpleTokenAuthenticationGuid')
        self.session.headers.update({'ap_19': guid, 'ap_23': session_index})

        r = self._post_lenient(
            f'{ADOBE_BASE}/adobe-services/authorize',
            data={
                'resource_id': self.resource,
                'requestor_id': self.requestor_id,
                'authentication_token': self.ctx.authn_token,
                'mso_id': mso,
                'generic_data': '',
                'userMeta': '1',
            },
            headers={'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'},
        )
        if '<pendingLogout' in r.text:
            # A cached authn_token that's since expired/logged-out looks
            # exactly like this — a normal, expected staleness case (see
            # authorize_mvpd()'s cached-token fast path), not a real failure.
            raise TVEAuthError('Adobe authorize: session has expired (pendingLogout) — authn_token needs re-pairing.')
        if '<error' in r.text:
            message = _adobe_error_message(r.text)
            if _adobe_error_code(r.text) == 'notAuthorized':
                raise TVENotAuthorizedError(message)
            raise TVEAuthError(message)
        if r.status_code >= 400:
            raise TVEAuthError(f'Adobe authorize returned HTTP {r.status_code}: {r.text[:300]}')
        self.ctx.authz_token = html.unescape(_text_between(r.text, 'authzToken'))

        return self._short_authorize(session_guid)

    def authorize_with_cached_authz(self, authz_token: str) -> str:
        """Skip the /adobe-services/authorize round trip using a still-fresh
        authz_token authorize_mvpd() already had cached for this
        (account, requestor_id) — see _AUTHZ_TOKEN_TTL_SECONDS. Goes straight
        to /shortAuthorize, which still always runs fresh (see
        _short_authorize's docstring). Caller must set self.ctx.authn_token
        first, same as authorize() — session_guid/ap_19/ap_23 still derive
        from it, only the /authorize call itself is skipped.
        """
        guid = _text_between(self.ctx.authn_token, 'simpleSamlNameID')
        session_index = _text_between(self.ctx.authn_token, 'simpleSamlSessionIndex')
        session_guid = _text_between(self.ctx.authn_token, 'simpleTokenAuthenticationGuid')
        self.session.headers.update({'ap_19': guid, 'ap_23': session_index})
        self.ctx.authz_token = authz_token
        return self._short_authorize(session_guid)

    def authorize_with_cox(self, username: str, password: str) -> str:
        self.setup_client()
        self.register_device()
        self.create_regcode()
        self.authenticate_with_cox(username, password)
        self.fetch_session_token()
        return self.authorize()

    def authenticate_with_xfinity_cookies(self, username: str, password: str, cookie_jar: dict) -> None:
        """Login via Comcast_SSO using a transplanted cookie jar harvested
        from a real authenticated browser session, instead of a browser.
        Thin wrapper around app.tve.mvpd's MSO-protocol-agnostic
        login_to_mvpd() — see xfinity.py's xfinity_cookie_jar_login() for the
        actual mechanics/why. This client's own authenticate_redirect_url()
        builds the legacy XML protocol's specific authenticate/saml URL;
        NBC's/FOX's own v2 REST clients generate their own equivalent
        MSO-login URL and call login_to_mvpd() directly instead of going
        through this class at all.

        Deferred import: app.tve.mvpd imports TVEAuthError from this module
        at its own module level, so importing it back here has to happen
        inside a function, not at this module's top level, to avoid a
        circular import.
        """
        from .mvpd import login_to_mvpd
        login_to_mvpd(
            'Comcast_SSO', '', self.authenticate_redirect_url('Comcast_SSO'), username, password,
            cookie_jar=cookie_jar,
        )

    def authorize_with_xfinity_cookies(self, username: str, password: str, cookie_jar: dict) -> str:
        self.setup_client()
        self.register_device()
        self.create_regcode()
        self.authenticate_with_xfinity_cookies(username, password, cookie_jar)
        self.fetch_session_token()
        return self.authorize()

    def authenticate_with_directv(self, username: str, password: str) -> None:
        """Login via DIRECTV (mso_id='DTV'). Unlike Cox/Xfinity, DIRECTV's
        Adobe Pass authenticate call never redirects (see
        app/tve/mvpd/directv.py's directv_login() docstring for the full
        mechanics) — it returns the actual login page's content directly, so
        this fetches it once here before handing off, rather than just
        building a URL like authenticate_with_xfinity_cookies() does.
        """
        try:
            r = self.session.get(self.authenticate_redirect_url('DTV'), allow_redirects=True, timeout=30)
        except requests.RequestException as exc:
            raise TVEAuthError(str(exc)) from exc
        from .mvpd import login_to_mvpd
        login_to_mvpd('DTV', r.text, str(r.url), username, password)

    def authorize_with_directv(self, username: str, password: str) -> str:
        self.setup_client()
        self.register_device()
        self.create_regcode()
        self.authenticate_with_directv(username, password)
        self.fetch_session_token()
        return self.authorize()


def _ensure_cox_device_fingerprint(account) -> str:
    """Get-or-create a stable device fingerprint for this TVEAccount, reused
    across every AdobePassCoxClient.register_device() call instead of a
    fresh random one each time — see register_device()'s docstring for why
    that mattered (it was invalidating cached authn_token sessions)."""
    from ..extensions import db

    cfg = dict(account.config or {})
    fingerprint = cfg.get('device_fingerprint')
    if fingerprint:
        return fingerprint
    fingerprint = uuid.uuid4().hex
    cfg['device_fingerprint'] = fingerprint
    account.config = cfg
    db.session.commit()
    return fingerprint


_ADOBE_CLIENT_CREDS_TTL_SECONDS = 6 * 60 * 60


def load_cached_adobe_client_creds(account, requestor_id: str) -> dict | None:
    """Get-or-None a still-fresh (client_id, client_secret, access_token)
    this account already registered with Adobe for `requestor_id`.

    setup_client()/_register_client() POST /o/client/register, which mints a
    brand-new OAuth client with Adobe every time it's called — not a token
    refresh, a new client identity. Every caller used to do this on every
    single login/resolve attempt, on top of the (already-fixed, see
    register_device()'s docstring) device-fingerprint churn — the same class
    of attempt-volume/identity churn a rate limiter would plausibly key off
    of. A comparable reverse-engineered implementation caches this for ~6h;
    matched here.
    """
    creds = ((account.config or {}).get('adobe_client_creds') or {}).get(requestor_id)
    if not creds or not creds.get('client_id') or not creds.get('access_token'):
        return None
    if time.time() - creds.get('captured_at', 0) >= _ADOBE_CLIENT_CREDS_TTL_SECONDS:
        return None
    return creds


def save_adobe_client_creds(account, requestor_id: str, client_id: str, client_secret: str, access_token: str) -> None:
    if not client_id or not access_token:
        return
    from ..extensions import db
    cfg = dict(account.config or {})
    all_creds = dict(cfg.get('adobe_client_creds') or {})
    all_creds[requestor_id] = {
        'client_id': client_id,
        'client_secret': client_secret,
        'access_token': access_token,
        'captured_at': int(time.time()),
    }
    cfg['adobe_client_creds'] = all_creds
    account.config = cfg
    db.session.commit()


def _save_mvpd_authn_token(account, requestor_id: str, authn_token: str) -> None:
    if not authn_token:
        return
    from time import time as _wall_time
    from ..extensions import db
    cfg = dict(account.config or {})
    mvpd_authn = dict(cfg.get('mvpd_authn') or {})
    mvpd_authn[requestor_id] = {'authn_token': authn_token, 'captured_at': int(_wall_time())}
    cfg['mvpd_authn'] = mvpd_authn
    account.config = cfg
    db.session.commit()


def _save_mvpd_authz_token(account, requestor_id: str, authz_token: str) -> None:
    """Persists the ~24h resource authz_token a full authorize() call just
    minted, so the next resolve() for this (account, requestor_id) can reuse
    it via authorize_with_cached_authz() instead of hitting
    /adobe-services/authorize again — see _AUTHZ_TOKEN_TTL_SECONDS.
    """
    if not authz_token:
        return
    from time import time as _wall_time
    from ..extensions import db
    cfg = dict(account.config or {})
    mvpd_authz = dict(cfg.get('mvpd_authz') or {})
    mvpd_authz[requestor_id] = {'authz_token': authz_token, 'captured_at': int(_wall_time())}
    cfg['mvpd_authz'] = mvpd_authz
    account.config = cfg
    db.session.commit()


def save_xfinity_cookie_jar(account, cookie_jar: dict) -> None:
    """Persists a cookie jar harvested from a real, already-authenticated
    Xfinity browser session (see app/worker.py's run_mvpd_browser_login,
    which harvests this via Playwright's context.cookies() right after a
    successful Comcast_SSO pairing). Account-wide, not per-requestor_id —
    the Akamai Bot Manager cookies and the Xfinity SESSION cookie are both
    scoped to the account/browser session, not to any one network's
    client_id.
    """
    if not cookie_jar:
        return
    from time import time as _wall_time
    from ..extensions import db
    cfg = dict(account.config or {})
    cfg['xfinity_cookie_jar'] = cookie_jar
    cfg['xfinity_cookie_jar_captured_at'] = int(_wall_time())
    account.config = cfg
    db.session.commit()


def save_google_master_token(account, data: dict) -> None:
    """Persists a Google master_token captured via app.tve.google_master_token
    (see its module docstring for the full technique). Account-wide, not
    per-requestor_id — it's tied to the Google account itself, and every
    YouTubeTV-MSO'd requestor_id's browser-login can reuse it to mint a fresh
    signed-in session with zero interactive login, the same way
    save_xfinity_cookie_jar's cookie jar is shared across every Comcast_SSO
    requestor_id.
    """
    if not data or not data.get('master_token'):
        return
    from ..extensions import db
    cfg = dict(account.config or {})
    cfg['google_master_token'] = data
    account.config = cfg
    db.session.commit()


def load_google_master_token(account) -> dict | None:
    return (account.config or {}).get('google_master_token') or None


# How long a detected soft-block on Adobe/YouTubeTV's SAML bounce chain
# (youtube.auth-gateway.net/saml/module.php/authbypass/firstbookend.php)
# keeps every browser-login flow from even attempting another live request.
# Confirmed live 2026-08-19 via extensive multi-signal testing (network/
# console event logging, session-affinity cookie transfer, filled-in
# window.history values — all ruled out as fixes) that this looks like
# accumulated rate-limiting on Adobe's side: the exact same Camoufox setup
# completed this chain successfully earlier the same day, then began
# returning an inert empty <body></body> only after a very high volume of
# attempts. 3h is a guess, not a documented SLA — Adobe doesn't publish one
# — chosen to comfortably outlast a single bad testing session without
# requiring a human to remember to stop clicking "Sign in".
ADOBE_YOUTUBETV_SOFT_BLOCK_SECONDS = 3 * 60 * 60


def save_adobe_youtubetv_soft_block(account, reason: str = '', details: dict | None = None) -> None:
    """Records that Adobe's YouTubeTV SAML bounce chain just showed the
    known soft-block signature (see ADOBE_YOUTUBETV_SOFT_BLOCK_SECONDS'
    docstring) — an empty <body></body> response on the authbypass/
    firstbookend.php hop instead of the real interstitial. Every browser-
    login flow checks load_adobe_youtubetv_soft_block() before spending any
    more time waiting on that hop, so one detection protects every network
    from repeating the same wasted attempt.

    `details` is an optional non-secret response fingerprint (cookie names
    present, ppp/bbp values, last URL, etc.) captured at the moment of
    detection — not used by load_adobe_youtubetv_soft_block()'s gating logic
    at all, just kept alongside `reason` so a later controlled retest can
    compare a fresh detection against this one (e.g. did ppp start at 1 again
    after cooldown, or pick up where it left off) instead of that evidence
    being discarded once the block record itself expires or gets overwritten.
    """
    from time import time as _wall_time
    from ..extensions import db
    cfg = dict(account.config or {})
    entry = {
        'detected_at': int(_wall_time()),
        'retry_after': int(_wall_time()) + ADOBE_YOUTUBETV_SOFT_BLOCK_SECONDS,
        'reason': reason[:200],
    }
    if details:
        entry['details'] = details
    cfg['adobe_youtubetv_soft_block'] = entry
    account.config = cfg
    db.session.commit()


def load_adobe_youtubetv_soft_block(account) -> dict | None:
    """Returns the active block record ({'detected_at', 'retry_after',
    'reason'}) if still within its window, else None (including once
    retry_after has passed — expired blocks are just ignored, not cleared,
    so there's nothing to clean up and a later real success naturally makes
    the stale record irrelevant)."""
    import time
    block = (account.config or {}).get('adobe_youtubetv_soft_block')
    if not block or block.get('retry_after', 0) <= time.time():
        return None
    return block


def _same_redirect_target(actual: str, expected: str) -> bool:
    a, e = urlsplit(actual), urlsplit(expected)
    return (a.scheme, a.netloc, a.path.rstrip('/')) == (e.scheme, e.netloc, e.path.rstrip('/'))


def _bounced_back_without_mso(requestor_id: str, resource: str, software_statement: str, redirect_url: str, mso_id: str) -> bool:
    """Scripted pre-check for "MSO isn't registered for this content owner at
    all" — mirrors app.worker's browser-flow bounce-back detection, confirmed
    live (2026-08-05, Turner/TNT+Sling) to happen entirely server-side on
    Adobe's end, via a single redirect straight back to redirect_url, BEFORE
    the MSO's own domain is ever touched. That makes this check safe to run
    even for MSOs whose real login page blocks scripted clients outright
    (Sling) — we never reach that domain here. Gives resolve() a clean,
    mechanism-based "definitely not authorized" signal instead of whatever
    cryptic error yt-dlp's generic extractor happens to raise when handed a
    redirect chain it doesn't recognize (e.g. "Unable to extract post url").
    Returns False (inconclusive) rather than raising on any failure, so a
    network hiccup here just falls through to the normal yt-dlp attempt.
    """
    try:
        client = AdobePassCoxClient(
            requestor_id=requestor_id, resource=resource,
            software_statement=software_statement, redirect_url=redirect_url,
        )
        client.setup_client()
        client.register_device()
        client.create_regcode()
        auth_url = client.authenticate_redirect_url(mso_id)
        r = client.session.get(auth_url, allow_redirects=True, timeout=20)
        return _same_redirect_target(r.url, redirect_url)
    except Exception as exc:  # noqa: BLE001
        logger.info('[adobe-pass] bounce-back pre-check for %s/%s inconclusive: %s', requestor_id, mso_id, exc)
        return False


def authorize_mvpd(
    account,
    *,
    requestor_id: str,
    resource: str,
    software_statement: str,
    redirect_url: str,
) -> tuple[str, requests.Session]:
    """Authenticate a TVEAccount against whichever MSO it's configured for.

    Uses the fast native Cox client when the account is Cox + native backend
    (the validated, proven-working path). Any other MSO goes through yt-dlp's
    generic per-provider Adobe Pass login flows (app/tve/ytdlp_mvpd.py) — the
    same legacy sp.auth.adobe.com protocol, just with that MSO's login handled
    by yt-dlp instead of a hand-rolled client. Returns (token, session); the
    session is a plain requests.Session for the yt-dlp path since downstream
    token-exchange calls (NGTV, etc.) authenticate via the token itself, not
    session cookies.
    """
    cfg = account.config or {}
    selected_mso_id = (cfg.get('selected_mso_id') or cfg.get('adobe_mso_id') or 'Cox').strip()
    auth_backend = (cfg.get('auth_backend') or 'native').strip()
    username = account.username or ''
    password = account.password or ''

    # A prior login — this scripted path's own previous run, OR a human's
    # one-time browser-assisted sign-in (app/worker.py's run_mvpd_browser_login,
    # for MSOs whose login page blocks scripted clients outright, e.g. Sling)
    # — may have already produced a long-lived authn_token for this
    # requestor_id. Try resuming it before ever doing a full fresh login.
    # This applies to Cox too: confirmed live (2026-08-05) that without this,
    # the Cox native path did a REAL POST to login.cox.com with live account
    # credentials on every single resolve() call — the same login-retry-storm
    # risk already fixed once for fubo, just not previously caught here since
    # Cox itself never errors, it just silently re-authenticates every time.
    device_fingerprint = _ensure_cox_device_fingerprint(account)
    client_creds = load_cached_adobe_client_creds(account, requestor_id)
    cached_authn = ((cfg.get('mvpd_authn') or {}).get(requestor_id) or {}).get('authn_token')
    if cached_authn:
        try:
            client = AdobePassCoxClient(
                requestor_id=requestor_id,
                resource=resource,
                software_statement=software_statement,
                redirect_url=redirect_url,
                device_fingerprint=device_fingerprint,
                client_creds=client_creds,
            )
            client.setup_client()
            if not client_creds:
                save_adobe_client_creds(account, requestor_id, client.ctx.client_id, client.ctx.client_secret, client.ctx.access_token)
            # authorize() sends the ap_42/ap_11/ap_z/Ap_21/pass_sfp headers
            # register_device() sets on self.session — Adobe 400s without them.
            # Must reuse the SAME persisted device_fingerprint as whatever
            # call originally produced cached_authn, or this re-registration
            # invalidates that session server-side (see register_device()'s
            # docstring) and cached_authn always comes back rejected.
            client.register_device()
            client.ctx.authn_token = cached_authn

            # A prior call may also have left a still-fresh authz_token for
            # this requestor_id (~24h lifetime) — reuse it to skip
            # /adobe-services/authorize entirely and go straight to
            # shortAuthorize (see _AUTHZ_TOKEN_TTL_SECONDS). Falls through to
            # a full authorize() below on any staleness/rejection.
            cached_authz_entry = (cfg.get('mvpd_authz') or {}).get(requestor_id) or {}
            cached_authz = cached_authz_entry.get('authz_token')
            authz_captured_at = cached_authz_entry.get('captured_at') or 0
            authz_is_fresh = bool(cached_authz) and (
                time.time() - authz_captured_at < _AUTHZ_TOKEN_TTL_SECONDS - _AUTHZ_REFRESH_MARGIN_SECONDS
            )
            if authz_is_fresh:
                try:
                    token = client.authorize_with_cached_authz(cached_authz)
                    return token, client.session
                except TVENotAuthorizedError:
                    raise  # definitive answer from Adobe — retrying won't change it
                except TVEAuthError as exc:
                    logger.info(
                        '[adobe-pass] cached authz_token for %s rejected, falling back to full authorize: %s',
                        requestor_id, exc,
                    )

            token = client.authorize()
            _save_mvpd_authz_token(account, requestor_id, client.ctx.authz_token)
            return token, client.session
        except TVENotAuthorizedError:
            raise  # definitive answer from Adobe — retrying won't change it
        except TVEAuthError as exc:
            logger.info('[adobe-pass] cached authn_token for %s rejected, re-authenticating: %s', requestor_id, exc)

    if selected_mso_id == 'Cox' and auth_backend == 'native':
        client = AdobePassCoxClient(
            requestor_id=requestor_id,
            resource=resource,
            software_statement=software_statement,
            redirect_url=redirect_url,
            device_fingerprint=device_fingerprint,
            client_creds=client_creds,
        )
        token = client.authorize_with_cox(username, password)
        if not client_creds:
            save_adobe_client_creds(account, requestor_id, client.ctx.client_id, client.ctx.client_secret, client.ctx.access_token)
        _save_mvpd_authn_token(account, requestor_id, client.ctx.authn_token)
        _save_mvpd_authz_token(account, requestor_id, client.ctx.authz_token)
        return token, client.session

    if selected_mso_id == 'Comcast_SSO':
        # login.xfinity.com's credential POST is blocked by Akamai Bot
        # Manager for any bare HTTP client, confirmed live 2026-08-14 (see
        # dev/comcast/XFINITY_ADOBE_PASS_DIRECT_HTTP_RESEARCH.md) — yt-dlp's
        # generic path below would just 403 the same way, so don't bother
        # trying it for this MSO. A cookie jar harvested from a real browser
        # session (run_mvpd_browser_login) gets straight through instead.
        cookie_jar = cfg.get('xfinity_cookie_jar')
        if cookie_jar:
            client = AdobePassCoxClient(
                requestor_id=requestor_id,
                resource=resource,
                software_statement=software_statement,
                redirect_url=redirect_url,
                device_fingerprint=device_fingerprint,
                client_creds=client_creds,
            )
            try:
                token = client.authorize_with_xfinity_cookies(username, password, cookie_jar)
                if not client_creds:
                    save_adobe_client_creds(account, requestor_id, client.ctx.client_id, client.ctx.client_secret, client.ctx.access_token)
                _save_mvpd_authn_token(account, requestor_id, client.ctx.authn_token)
                _save_mvpd_authz_token(account, requestor_id, client.ctx.authz_token)
                return token, client.session
            except TVENotAuthorizedError:
                raise  # definitive answer from Adobe — retrying won't change it
            except TVEAuthError as exc:
                logger.info(
                    '[adobe-pass] Xfinity cookie-jar sign-in for %s failed, jar likely stale: %s',
                    requestor_id, exc,
                )
        raise TVEAuthError(
            f'{requestor_id}: no usable Xfinity cookie jar (missing or stale) — '
            'needs a fresh browser-assisted sign-in to re-harvest one.'
        )

    if selected_mso_id == 'DTV':
        # DIRECTV's Adobe Pass login page is a JavaScript SPA — yt-dlp's
        # generic form-scraping fallback below can't drive it at all
        # (confirmed live 2026-08-17: zero <form> tags in the response).
        # app.tve.mvpd.directv drives its real ForgeRock/OpenAM backend
        # directly instead. (Its login-failure circuit breaker lives in
        # login_to_mvpd() itself, not here, so every caller — this legacy
        # client and the 5 v2 REST scrapers that call login_to_mvpd()
        # directly — shares the same cooldown state.)
        client = AdobePassCoxClient(
            requestor_id=requestor_id,
            resource=resource,
            software_statement=software_statement,
            redirect_url=redirect_url,
            device_fingerprint=device_fingerprint,
            client_creds=client_creds,
        )
        token = client.authorize_with_directv(username, password)
        if not client_creds:
            save_adobe_client_creds(account, requestor_id, client.ctx.client_id, client.ctx.client_secret, client.ctx.access_token)
        _save_mvpd_authn_token(account, requestor_id, client.ctx.authn_token)
        _save_mvpd_authz_token(account, requestor_id, client.ctx.authz_token)
        return token, client.session

    yt_dlp_mso_id = (cfg.get('yt_dlp_mso_id') or selected_mso_id).strip()

    if _bounced_back_without_mso(requestor_id, resource, software_statement, redirect_url, yt_dlp_mso_id):
        raise TVENotAuthorizedError(f'{yt_dlp_mso_id} is not a participating provider for {requestor_id}.')

    from .ytdlp_mvpd import authorize_via_ytdlp
    token = authorize_via_ytdlp(
        mso_id=yt_dlp_mso_id,
        username=username,
        password=password,
        requestor_id=requestor_id,
        resource=resource,
        software_statement=software_statement,
        redirect_url=redirect_url,
    )
    session = requests.Session()
    session.headers.update({
        'User-Agent': UA,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
    })
    return token, session


def verify_cox_history(username: str, password: str, software_statement: Optional[str] = None) -> dict:
    statement = software_statement or discover_aenetworks_software_statement('history')
    client = AdobePassCoxClient(
        requestor_id='HISTORY',
        resource=HISTORY_RESOURCE,
        software_statement=statement,
        redirect_url=DEFAULT_HISTORY_REDIRECT_URL,
    )
    token = client.authorize_with_cox(username, password)
    return {
        'requestor_id': 'HISTORY',
        'mso_id': 'Cox',
        'short_authorize_obtained': bool(token),
        'short_authorize_len': len(token or ''),
    }


def verify_mvpd_history(account, software_statement: Optional[str] = None) -> dict:
    """Same validation as verify_cox_history, but MSO-aware via authorize_mvpd.

    Works for Cox (native) and any other MSO yt-dlp supports (Sling, Spectrum,
    Fubo, etc.) since it exercises whichever backend the account is configured
    for. Used by the Settings TVE test button.
    """
    cfg = account.config or {}
    selected_mso_id = (cfg.get('selected_mso_id') or cfg.get('adobe_mso_id') or 'Cox').strip()
    statement = software_statement or discover_aenetworks_software_statement('history')
    token, _session = authorize_mvpd(
        account,
        requestor_id='HISTORY',
        resource=HISTORY_RESOURCE,
        software_statement=statement,
        redirect_url=DEFAULT_HISTORY_REDIRECT_URL,
    )
    return {
        'requestor_id': 'HISTORY',
        'mso_id': selected_mso_id,
        'short_authorize_obtained': bool(token),
        'short_authorize_len': len(token or ''),
    }

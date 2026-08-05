from __future__ import annotations

import html
import json
import logging
import re
import uuid
from dataclasses import dataclass
from html.parser import HTMLParser
from time import monotonic
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests

logger = logging.getLogger(__name__)

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
    def __init__(self, *, requestor_id: str, resource: str, software_statement: str, redirect_url: str = DEFAULT_HISTORY_REDIRECT_URL) -> None:
        self.requestor_id = requestor_id
        self.resource = resource
        self.redirect_url = redirect_url
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
        r = self._post(
            f'{ADOBE_BASE}/indiv/devices',
            json={'fingerprint': uuid.uuid4().hex},
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

    def authorize_with_cox(self, username: str, password: str) -> str:
        self.setup_client()
        self.register_device()
        self.create_regcode()
        self.authenticate_with_cox(username, password)
        self.fetch_session_token()
        return self.authorize()


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

    if selected_mso_id == 'Cox' and auth_backend == 'native':
        client = AdobePassCoxClient(
            requestor_id=requestor_id,
            resource=resource,
            software_statement=software_statement,
            redirect_url=redirect_url,
        )
        token = client.authorize_with_cox(username, password)
        return token, client.session

    # A human may have already completed a one-time browser-assisted sign-in
    # for this requestor_id (app/worker.py's run_mvpd_browser_login) for MSOs
    # whose login page blocks scripted clients outright (e.g. Sling). That
    # produces a long-lived authn_token we can keep resuming from here without
    # ever going back through yt-dlp or bothering the human again, until Adobe
    # actually rejects it (expired/revoked).
    cached_authn = ((cfg.get('mvpd_authn') or {}).get(requestor_id) or {}).get('authn_token')
    if cached_authn:
        try:
            client = AdobePassCoxClient(
                requestor_id=requestor_id,
                resource=resource,
                software_statement=software_statement,
                redirect_url=redirect_url,
            )
            client.setup_client()
            # authorize() sends the ap_42/ap_11/ap_z/Ap_21/pass_sfp headers
            # register_device() sets on self.session — Adobe 400s without them
            # even though the fresh device_id here doesn't need to match
            # whichever device the cached authn_token was originally issued to.
            client.register_device()
            client.ctx.authn_token = cached_authn
            token = client.authorize()
            return token, client.session
        except TVENotAuthorizedError:
            raise  # definitive answer from Adobe — retrying via yt-dlp won't change it
        except TVEAuthError as exc:
            logger.info('[adobe-pass] cached authn_token for %s rejected, falling back to yt-dlp: %s', requestor_id, exc)

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

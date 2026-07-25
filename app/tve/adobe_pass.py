from __future__ import annotations

import html
import json
import re
import uuid
from dataclasses import dataclass
from html.parser import HTMLParser
from time import monotonic
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests

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
        r = self._post(
            f'{ADOBE_BASE}/adobe-services/session',
            data={'_method': 'GET', 'reg_code': self.ctx.reg_code, 'requestor_id': self.requestor_id},
            headers={'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'},
        )
        if '<pendingLogout' in r.text:
            raise TVEAuthError('Adobe session returned pendingLogout.')
        self.ctx.authn_token = html.unescape(_text_between(r.text, 'authnToken'))

    def authorize(self) -> str:
        mso = _text_between(self.ctx.authn_token, 'simpleTokenMsoID')
        guid = _text_between(self.ctx.authn_token, 'simpleSamlNameID')
        session_index = _text_between(self.ctx.authn_token, 'simpleSamlSessionIndex')
        session_guid = _text_between(self.ctx.authn_token, 'simpleTokenAuthenticationGuid')
        self.session.headers.update({'ap_19': guid, 'ap_23': session_index})

        r = self._post(
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
        if '<error' in r.text:
            raise TVEAuthError(_text_between(r.text, 'details'))
        self.ctx.authz_token = html.unescape(_text_between(r.text, 'authzToken'))

        r = self._post(
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
        self.ctx.short_token = r.text
        return self.ctx.short_token

    def authorize_with_cox(self, username: str, password: str) -> str:
        self.setup_client()
        self.register_device()
        self.create_regcode()
        self.authenticate_with_cox(username, password)
        self.fetch_session_token()
        return self.authorize()


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

"""Bounded recovery through Wizz's normal HTTPS login and encrypted local vaults."""
import secrets
import time
from urllib.parse import urljoin, urlsplit

import requests
from credential_vault import CredentialVault
from scanner import PRIVATE_PAGE, WizzAYCFClient
from termux.wizz_runtime import normalize_runtime
from wizz_endpoint import extract_availability_url
from wizz_login_forms import CHALLENGE_RE, AUTH_ERROR_RE, _visible_text, _login_form, _field_name, _cookie_state
import wizz_rate_limit as limits


class SignInNeeded(Exception):
    pass


def trusted_url(url):
    parsed = urlsplit(url)
    return (parsed.scheme == 'https' and not parsed.username and not parsed.password
            and parsed.port in (None, 443)
            and (parsed.hostname == 'wizzair.com' or (parsed.hostname or '').endswith('.wizzair.com')))


def navigate(session, url, deadline, payload=None):
    """Follow a bounded normal login flow, keeping passwords off redirect requests."""
    method = 'POST' if payload is not None else 'GET'
    for _ in range(8):
        if not trusted_url(url):
            raise SignInNeeded('Open Wizz to complete sign-in.')
        limits.check_cooldown()
        limits.wait_for_request(1.0, deadline=deadline)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError()
        response = session.request(method, url, data=payload, allow_redirects=False,
                                   timeout=(min(5, remaining), min(10, remaining)))
        if response.status_code == 429:
            detail = limits.retry_after_details(response.headers.get('Retry-After'))
            limits.record_rate_limit(detail['seconds'] or 0, operation='authentication',
                                     retry_after_kind=detail['kind'], response=response)
            limits.check_cooldown()
        if response.status_code in (401, 403, 418):
            raise SignInNeeded('Wizz requires sign-in or verification. Open the Wizz page.')
        response.raise_for_status()
        if not 300 <= response.status_code < 400:
            return response
        if method == 'POST' and response.status_code in (307, 308):
            raise SignInNeeded('Complete this Wizz sign-in in the app.')
        location = response.headers.get('Location')
        if not location:
            raise SignInNeeded('Wizz sign-in needs attention.')
        url = urljoin(url, location)
        method, payload = 'GET', None
    raise SignInNeeded('Wizz sign-in did not finish. Open the Wizz page.')


def validated_state(response, client):
    if urlsplit(response.url).hostname != 'multipass.wizzair.com':
        return None
    if '/private-page/' not in urlsplit(response.url).path or response.status_code != 200:
        return None
    if _login_form(response):
        return None
    endpoint = extract_availability_url(response.text)
    if not endpoint:
        return None
    from morning_scan import _runtime_path
    import json
    try:
        runtime = json.loads(_runtime_path().read_text())
        if not isinstance(runtime, dict):
            runtime = {}
    except (OSError, ValueError):
        runtime = {}
    runtime['availability_url'] = endpoint
    runtime, _ = normalize_runtime(runtime)
    return {'state': dict(_cookie_state(client.http), generation=secrets.token_hex(16)), 'runtime': runtime}


def recover(previous, deadline):
    """Try the existing session first, then submit saved credentials once."""
    client = WizzAYCFClient(previous or {})
    try:
        response = navigate(client.http, PRIVATE_PAGE, deadline)
        candidate = validated_state(response, client)
        if candidate:
            return candidate
        visible = _visible_text(response.text)
        if CHALLENGE_RE.search(visible):
            raise SignInNeeded('Wizz requires verification. Complete it on the Wizz page.')
        credentials = CredentialVault().load()
        if not credentials:
            raise SignInNeeded('Save your Wizz login in the app to enable automatic recovery.')
        form = _login_form(response)
        if not form or form['method'] != 'post':
            raise SignInNeeded('Open Wizz to complete its sign-in form.')
        inputs = form['inputs']
        username = _field_name(inputs, 'username')
        password = _field_name(inputs, 'password')
        if not username or not password:
            raise SignInNeeded('Open Wizz to complete its sign-in form.')
        action = urljoin(response.url, form['action'] or response.url)
        if not trusted_url(action):
            raise SignInNeeded('Open Wizz to complete sign-in.')
        payload = {item['name']: item.get('value', '') for item in inputs
                   if item.get('name') and item.get('type', '').lower() in {'hidden', 'submit'}}
        payload[username], payload[password] = credentials['username'], credentials['password']
        response = navigate(client.http, action, deadline, payload)
        visible = _visible_text(response.text)
        if CHALLENGE_RE.search(visible):
            raise SignInNeeded('Wizz requires verification. Complete it on the Wizz page.')
        if AUTH_ERROR_RE.search(visible) or _login_form(response):
            raise SignInNeeded('Wizz could not accept the saved login. Check it in Wizz login settings.')
        candidate = validated_state(response, client)
        if not candidate:
            response = navigate(client.http, PRIVATE_PAGE, deadline)
            candidate = validated_state(response, client)
        if not candidate:
            raise SignInNeeded('Wizz sign-in needs attention. Open the Wizz page.')
        return candidate
    finally:
        client.http.close()

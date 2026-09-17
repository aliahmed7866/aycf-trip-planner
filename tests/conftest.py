"""Keep Wizz cooldown state isolated from the developer's running services."""
import pytest


@pytest.fixture(autouse=True)
def no_external_network(monkeypatch, tmp_path):
    """Tests may use loopback servers, never provider accounts or live APIs."""
    import socket
    import requests
    from urllib.parse import urlsplit
    original_request = requests.sessions.Session.request
    original_connect = socket.socket.connect
    original_getaddrinfo = socket.getaddrinfo
    local = {'localhost', '127.0.0.1', '::1'}
    def request(self, method, url, *args, **kwargs):
        if urlsplit(url).hostname not in local:
            raise AssertionError('Live HTTP disabled in tests; mock the provider transport')
        return original_request(self, method, url, *args, **kwargs)
    def connect(self, address):
        if isinstance(address, tuple) and address[0] not in local:
            raise AssertionError('External sockets disabled in tests')
        return original_connect(self, address)
    def getaddrinfo(host, *args, **kwargs):
        if host not in local and host is not None:
            raise AssertionError('External DNS disabled in tests')
        return original_getaddrinfo(host, *args, **kwargs)
    monkeypatch.setattr(requests.sessions.Session, 'request', request)
    monkeypatch.setattr(socket.socket, 'connect', connect)
    monkeypatch.setattr(socket, 'getaddrinfo', getaddrinfo)
    # Tests without explicit state fixtures must not read personal config/vaults.
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(tmp_path / 'config'))
    monkeypatch.setenv('AYCF_STATE_DIR', str(tmp_path / 'state'))
    monkeypatch.setenv('AYCF_DB_PATH', str(tmp_path / 'aycf.sqlite3'))
    from termux import run_state
    monkeypatch.setattr(run_state, 'STATE_DIR', tmp_path / 'state')
    monkeypatch.setattr(run_state, 'STATUS_FILE', tmp_path / 'state/scan-status.json')
    monkeypatch.setattr(run_state, 'LOCK_FILE', tmp_path / 'state/scan.lock')


@pytest.fixture(autouse=True)
def isolated_wizz_rate_limits(tmp_path, monkeypatch, request):
    import wizz_rate_limit
    monkeypatch.setenv('AYCF_WIZZ_RATE_LIMIT_PATH', str(tmp_path / 'wizz-rate-limit.sqlite3'))
    # Most HTTP tests use instant fixture responses. Dedicated pacing tests
    # exercise the real reservation loop with controlled clocks/subprocesses;
    # other tests keep the real cooldown guard without sleeping between mocks.
    if request.node.path.name not in {'test_persistent_rate_limits.py', 'test_shared_rate_limits.py', 'test_wizz_request_budget.py'}:
        monkeypatch.setattr(wizz_rate_limit, 'wait_for_request', lambda *a, **kw: wizz_rate_limit.check_cooldown())

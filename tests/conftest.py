"""Keep Wizz cooldown state isolated from the developer's running services."""
import pytest


@pytest.fixture(autouse=True)
def isolated_wizz_rate_limits(tmp_path, monkeypatch, request):
    import wizz_rate_limit
    monkeypatch.setenv('AYCF_WIZZ_RATE_LIMIT_PATH', str(tmp_path / 'wizz-rate-limit.sqlite3'))
    # Most HTTP tests use instant fixture responses. Dedicated pacing tests
    # exercise the real reservation loop with controlled clocks/subprocesses;
    # other tests keep the real cooldown guard without sleeping between mocks.
    if request.node.path.name not in {'test_persistent_rate_limits.py', 'test_shared_rate_limits.py', 'test_wizz_request_budget.py'}:
        monkeypatch.setattr(wizz_rate_limit, 'wait_for_request', lambda *a, **kw: wizz_rate_limit.check_cooldown())

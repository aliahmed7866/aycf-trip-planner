import termux.refresh_wizz_from_chrome as refresh
from termux.refresh_wizz_from_chrome import _extract_availability_url
from termux.wizz_runtime import DEFAULT_TEMPLATE, build_probe_template


CANONICAL = "https://multipass.wizzair.com/w6/subscriptions/json/availability/803e9c9c-5331-4b98-aa74-3104bb3b858e"


def test_extract_searchflight_url():
    html = r'{"searchFlight":"https:\/\/multipass.wizzair.com\/w6\/subscriptions\/json\/availability\/803e9c9c-5331-4b98-aa74-3104bb3b858e"}'
    assert _extract_availability_url(html) == CANONICAL


def test_extract_pass_id():
    html = "window.foo = { pass_id: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' };"
    assert _extract_availability_url(html) == "https://multipass.wizzair.com/w6/subscriptions/json/availability/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def test_extract_none():
    assert _extract_availability_url("<html>no endpoint</html>") is None


class FakeClient:
    def __init__(self, *_args, **_kwargs):
        self.dynamic_url = ""
        self.captured_request_method = ""
        self.captured_template_type = ""
        self.captured_request_template = None
        self.station_ids = {}
        self.http = object()
        self.preflight_calls = 0

    def preflight(self):
        self.preflight_calls += 1
        if not isinstance(self.captured_request_template, dict):
            return {"ok": False, "reason": "no captured request template"}
        return {"ok": True, "response": "no-availability"}


def test_validate_rebuilds_template_after_stale_endpoint_rediscovery(monkeypatch):
    monkeypatch.setattr(refresh, "CapturedRequestWizzClient", FakeClient)
    monkeypatch.setattr(refresh, "_rediscover_endpoint", lambda _client: CANONICAL)
    runtime = {
        "availability_url": "https://multipass.wizzair.com/legacy/flight-search",
        "request_method": "GET",
        "request_template_type": None,
        "request_template": None,
    }

    client, preflight = refresh._validate_candidate({"cookies": []}, runtime)

    assert preflight == {"ok": True, "response": "no-availability"}
    assert client.dynamic_url == CANONICAL
    assert client.captured_request_method == "POST"
    assert client.captured_template_type == "json"
    assert client.captured_request_template == build_probe_template(runtime)
    assert DEFAULT_TEMPLATE["origin"] == ""
    assert runtime["availability_url"] == CANONICAL
    assert runtime["request_template"] == build_probe_template(runtime)
    assert runtime["request_template"] is not DEFAULT_TEMPLATE


def test_validate_repairs_canonical_get_capture_without_rediscovery(monkeypatch):
    monkeypatch.setattr(refresh, "CapturedRequestWizzClient", FakeClient)

    def unexpected(_client):
        raise AssertionError("rediscovery should not be needed for a canonical endpoint")

    monkeypatch.setattr(refresh, "_rediscover_endpoint", unexpected)
    runtime = {
        "availability_url": CANONICAL,
        "request_method": "GET",
        "request_template_type": None,
        "request_template": None,
    }

    client, preflight = refresh._validate_candidate({"cookies": []}, runtime)

    assert preflight["ok"] is True
    assert client.preflight_calls == 1
    assert runtime["request_method"] == "POST"
    assert runtime["request_template_type"] == "json"
    assert runtime["request_template"] == build_probe_template(runtime)
    assert runtime["request_template"] is not DEFAULT_TEMPLATE

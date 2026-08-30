from pathlib import Path

from termux.wizz_runtime import DEFAULT_TEMPLATE, apply_runtime, normalize_runtime, write_runtime


CANONICAL = "https://multipass.wizzair.com/w6/subscriptions/json/availability/803e9c9c-5331-4b98-aa74-3104bb3b858e"


class DummyClient:
    def __init__(self):
        self.dynamic_url = ""
        self.captured_request_method = ""
        self.captured_template_type = ""
        self.captured_request_template = None
        self.station_ids = {}


def test_normalize_get_only_availability_capture():
    runtime = {
        "availability_url": CANONICAL,
        "request_method": "GET",
        "request_template_type": None,
        "request_template": None,
        "station_ids": {"london luton": "LTN"},
    }
    normalized, repaired = normalize_runtime(runtime)

    assert repaired is True
    assert normalized["request_method"] == "POST"
    assert normalized["request_template_type"] == "json"
    assert normalized["request_template"] == DEFAULT_TEMPLATE
    assert normalized["station_ids"] == runtime["station_ids"]
    assert normalized["template_repair_reason"]


def test_normalize_does_not_guess_for_unknown_endpoint():
    runtime = {
        "availability_url": "https://multipass.wizzair.com/legacy/search",
        "request_method": "GET",
        "request_template": None,
    }
    normalized, repaired = normalize_runtime(runtime)

    assert repaired is False
    assert normalized == runtime


def test_apply_runtime_uses_supplied_metadata_only():
    runtime = {
        "availability_url": CANONICAL,
        "request_method": "GET",
        "request_template_type": None,
        "request_template": None,
        "station_ids": {"London Luton": "ltn"},
    }
    client = DummyClient()

    assert apply_runtime(client, runtime) is True
    assert client.dynamic_url == CANONICAL
    assert client.captured_request_method == "POST"
    assert client.captured_template_type == "json"
    assert client.captured_request_template == DEFAULT_TEMPLATE
    assert client.station_ids["london luton"] == "LTN"


def test_write_runtime_is_atomic_and_private(tmp_path: Path):
    path = tmp_path / "wizz_runtime.json"
    write_runtime(path, {"availability_url": CANONICAL})

    assert path.exists()
    assert path.stat().st_mode & 0o777 == 0o600
    assert '"availability_url"' in path.read_text(encoding="utf-8")

"""Shared Wizz AYCF runtime normalization and safe persistence.

The live Multipass availability endpoint is replayed as a POST JSON request.
Older browser captures can contain only an endpoint-discovery GET, and endpoint
rotation can leave the saved URL stale. Keep the request shape in one place so
repair, browser capture, and validation all agree on the same runtime metadata.
"""

from __future__ import annotations

from copy import deepcopy

import json
import os
import tempfile
import time
from datetime import date
from pathlib import Path
from typing import Any

DEFAULT_TEMPLATE = {
    "flightType": "OW",
    "origin": "",
    "destination": "",
    "departure": "",
    "arrival": "",
    "intervalSubtype": None,
}


def is_availability_endpoint(endpoint: str) -> bool:
    value = str(endpoint or "").strip()
    return (
        value.startswith("https://multipass.wizzair.com/")
        and "/subscriptions/json/availability/" in value
    )


def _probe_station_ids(runtime: dict[str, Any]) -> tuple[str, str]:
    """Pick two real-looking station ids for a harmless availability probe."""
    values: list[str] = []
    station_ids = runtime.get("station_ids")
    if isinstance(station_ids, dict):
        for raw in station_ids.values():
            value = str(raw or "").strip().upper()
            if value and value not in values:
                values.append(value)
            if len(values) >= 2:
                break

    # Runtime metadata from very old captures may predate station aliases. Use
    # two stable Wizz stations only as a validation probe; actual scan requests
    # always replace these route fields with the selected concrete airports.
    for fallback in ("BUD", "LTN"):
        if fallback not in values:
            values.append(fallback)
        if len(values) >= 2:
            break
    return values[0], values[1]


def build_probe_template(runtime: dict[str, Any]) -> dict[str, Any]:
    """Build a syntactically valid request used only for session preflight."""
    origin, destination = _probe_station_ids(runtime)
    template = dict(DEFAULT_TEMPLATE)
    template["origin"] = origin
    template["destination"] = destination
    template["departure"] = date.today().isoformat()
    return template


def _template_needs_probe(template: Any) -> bool:
    if not isinstance(template, dict):
        return True
    return not all(str(template.get(key) or "").strip() for key in ("origin", "destination", "departure"))


def normalize_runtime(runtime: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Return a copy with a usable request template for known AYCF endpoints."""
    if not isinstance(runtime, dict):
        return {}, False

    normalized = deepcopy(runtime)
    endpoint = str(normalized.get("availability_url") or "").strip()
    if not is_availability_endpoint(endpoint):
        return normalized, False

    template = normalized.get("request_template")
    method = str(normalized.get("request_method") or "").upper()
    template_type = str(normalized.get("request_template_type") or "").lower()
    if method == "POST" and template_type == "json" and isinstance(template, dict) and not _template_needs_probe(template):
        return normalized, False

    normalized["request_method"] = "POST"
    normalized["request_template_type"] = "json"
    normalized["request_template"] = build_probe_template(normalized)
    normalized["template_repaired_at"] = int(time.time())
    normalized["template_repair_reason"] = "normalized availability endpoint to valid POST JSON probe"
    return normalized, True


def apply_runtime(client: Any, runtime: dict[str, Any]) -> bool:
    """Apply exactly the supplied runtime to a scanner client; never re-read disk."""
    normalized, _ = normalize_runtime(runtime)
    endpoint = str(normalized.get("availability_url") or "").strip()
    if not endpoint.startswith("https://multipass.wizzair.com/"):
        return False

    client.dynamic_url = endpoint
    client.captured_request_method = str(normalized.get("request_method") or "POST").upper()
    client.captured_template_type = str(normalized.get("request_template_type") or "").lower()
    template = normalized.get("request_template")
    client.captured_request_template = deepcopy(template) if isinstance(template, dict) else None

    station_ids = normalized.get("station_ids")
    if isinstance(station_ids, dict):
        for key, value in station_ids.items():
            if key and value:
                client.station_ids[str(key).casefold()] = str(value).upper()
    return True


def write_runtime(path: Path, runtime: dict[str, Any]) -> None:
    """Atomically persist runtime metadata with owner-only permissions."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(runtime, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), 0o600)
        temp_path.replace(path)
        os.chmod(path, 0o600)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass

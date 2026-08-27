"""Persisted scan scope shared by the Termux UI and morning worker."""

from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Iterable

DEFAULT_ORIGINS = [
    "Liverpool",
    "Leeds/Bradford",
    "Birmingham",
    "London Gatwick",
    "London Luton",
    "London Stansted",
]
VALID_DESTINATION_MODES = {"all", "only", "exclude"}


def normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def config_dir() -> Path:
    return Path(os.environ.get("AYCF_CONFIG_DIR", str(Path.home() / ".config/aycf")))


def scope_path() -> Path:
    return config_dir() / "scan_scope.json"


def _clean_names(values: Iterable[str]) -> list[str]:
    out = []
    seen = set()
    for value in values:
        item = str(value or "").strip()
        key = normalize_name(item)
        if not item or not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def default_scope() -> dict:
    return {
        "origins": list(DEFAULT_ORIGINS),
        "destination_mode": "all",
        "destinations": [],
    }


def load_scope() -> dict:
    data = {}
    try:
        data = json.loads(scope_path().read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        pass
    if not isinstance(data, dict):
        data = {}

    origins = _clean_names(data.get("origins") or DEFAULT_ORIGINS)
    if not origins:
        origins = list(DEFAULT_ORIGINS)
    mode = str(data.get("destination_mode") or "all").strip().lower()
    if mode not in VALID_DESTINATION_MODES:
        mode = "all"
    destinations = _clean_names(data.get("destinations") or [])
    return {
        "origins": origins,
        "destination_mode": mode,
        "destinations": destinations,
    }


def save_scope(origins: Iterable[str], destination_mode: str, destinations: Iterable[str]) -> dict:
    mode = str(destination_mode or "all").strip().lower()
    if mode not in VALID_DESTINATION_MODES:
        raise ValueError("Invalid destination mode")
    scope = {
        "origins": _clean_names(origins),
        "destination_mode": mode,
        "destinations": _clean_names(destinations),
    }
    if not scope["origins"]:
        raise ValueError("Select at least one origin airport")
    if mode == "only" and not scope["destinations"]:
        raise ValueError("Choose at least one destination when using Only selected destinations")
    config_dir().mkdir(parents=True, exist_ok=True)
    target = scope_path()
    temp = target.with_suffix(".tmp")
    temp.write_text(json.dumps(scope, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.chmod(temp, 0o600)
    temp.replace(target)
    return scope


def scope_fingerprint(scope: dict) -> str:
    canonical = {
        "origins": sorted(normalize_name(x) for x in scope.get("origins") or []),
        "destination_mode": scope.get("destination_mode") or "all",
        "destinations": sorted(normalize_name(x) for x in scope.get("destinations") or []),
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def filter_routes(route_pairs: Iterable[tuple[str, str]], scope: dict) -> list[tuple[str, str]]:
    origins = {normalize_name(x) for x in scope.get("origins") or []}
    destinations = {normalize_name(x) for x in scope.get("destinations") or []}
    mode = scope.get("destination_mode") or "all"
    selected = []
    for origin, destination in route_pairs:
        if normalize_name(origin) not in origins:
            continue
        dest_key = normalize_name(destination)
        if mode == "only" and dest_key not in destinations:
            continue
        if mode == "exclude" and dest_key in destinations:
            continue
        selected.append((origin, destination))
    return sorted(set(selected))


def scope_summary(scope: dict) -> str:
    origins = ", ".join(scope.get("origins") or [])
    mode = scope.get("destination_mode") or "all"
    destinations = ", ".join(scope.get("destinations") or [])
    if mode == "only":
        return f"{origins} → only {destinations}"
    if mode == "exclude":
        return f"{origins} → all except {destinations}"
    return f"{origins} → all PDF destinations"

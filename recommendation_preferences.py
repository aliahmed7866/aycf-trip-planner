"""Persistent display preferences shared by Recommendations and Stability."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Iterable

from scan_scope import config_dir, normalize_name


def preferences_path() -> Path:
    return config_dir() / "recommendation_preferences.json"


def _clean_destinations(values: Iterable[str]) -> list[str]:
    cleaned, seen = [], set()
    for value in values:
        item = str(value or "").strip()
        key = normalize_name(item)
        if not item or not key or key in seen:
            continue
        seen.add(key)
        cleaned.append(item)
    return cleaned


def load_preferred_destinations() -> list[str]:
    try:
        payload = json.loads(preferences_path().read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return []
    if not isinstance(payload, dict):
        return []
    return _clean_destinations(payload.get("destinations") or [])


def save_preferred_destinations(values: Iterable[str]) -> list[str]:
    destinations = _clean_destinations(values)
    directory = config_dir()
    directory.mkdir(parents=True, exist_ok=True)
    target = preferences_path()
    fd, temporary_name = tempfile.mkstemp(
        prefix=".recommendation-preferences-", suffix=".tmp", dir=str(directory)
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump({"destinations": destinations}, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), 0o600)
        temporary.replace(target)
        try:
            os.chmod(target, 0o600)
        except OSError:
            pass
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return destinations

import os
import shutil
import tempfile
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

DEFAULT_UPSTREAM_ZIP = "https://github.com/markvincevarga/wizzair-aycf-availability/archive/refs/heads/main.zip"


@dataclass
class UpdateResult:
    updated: bool
    message: str
    data_dir: str
    last_updated_epoch: int


def _now_epoch() -> int:
    return int(time.time())


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _read_stamp(stamp_path: Path) -> Optional[int]:
    try:
        return int(stamp_path.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _write_stamp(stamp_path: Path, epoch: int) -> None:
    fd, temp_name = tempfile.mkstemp(prefix=f".{stamp_path.name}.", suffix=".tmp", dir=str(stamp_path.parent))
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(str(epoch))
            handle.flush()
            os.fsync(handle.fileno())
        temp_path.replace(stamp_path)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _extract_data_dir_from_zip(extract_root: Path) -> Path:
    candidates = list(extract_root.glob("**/data"))
    best = None
    best_count = -1
    for candidate in candidates:
        count = len(list(candidate.glob("*.csv")))
        if count > best_count:
            best = candidate
            best_count = count
    if best is None or best_count <= 0:
        raise FileNotFoundError("Could not locate a data/ folder with CSV files in the upstream zip.")
    return best


def _safe_extract(archive: zipfile.ZipFile, extract_root: Path) -> None:
    root = extract_root.resolve()
    for member in archive.infolist():
        destination = (root / member.filename).resolve()
        if destination != root and root not in destination.parents:
            raise ValueError(f"Unsafe path in upstream archive: {member.filename}")
    archive.extractall(extract_root)


def _replace_directory(staged: Path, target: Path, work_root: Path) -> None:
    backup = work_root / "previous-data"
    had_target = target.exists()
    if had_target:
        os.replace(target, backup)
    try:
        os.replace(staged, target)
    except Exception:
        if had_target and backup.exists() and not target.exists():
            os.replace(backup, target)
        raise


def update_data_if_needed(
    cache_root: str,
    upstream_zip_url: str = DEFAULT_UPSTREAM_ZIP,
    refresh_interval_seconds: int = 24 * 3600,
    force: bool = False,
    timeout_seconds: int = 60,
    max_download_bytes: int = 250 * 1024 * 1024,
) -> UpdateResult:
    """Refresh the legacy archive cache without exposing a partial data directory."""
    cache_root_p = Path(cache_root)
    _ensure_dir(cache_root_p)
    data_dst = cache_root_p / "data"
    stamp_path = cache_root_p / "last_update.txt"
    last = _read_stamp(stamp_path) or 0
    now = _now_epoch()

    if not force and (now - last) < refresh_interval_seconds and data_dst.is_dir():
        return UpdateResult(False, "Cache fresh; no update needed.", str(data_dst.resolve()), last)

    try:
        with tempfile.TemporaryDirectory(prefix=".upstream-refresh-", dir=str(cache_root_p)) as temp_dir:
            work_root = Path(temp_dir)
            archive_path = work_root / "upstream.zip"
            extract_root = work_root / "extracted"
            extract_root.mkdir()

            with requests.get(upstream_zip_url, stream=True, timeout=timeout_seconds) as response:
                response.raise_for_status()
                declared = int(response.headers.get("Content-Length") or 0)
                if declared and declared > max_download_bytes:
                    raise RuntimeError("Upstream archive exceeds the configured download limit.")
                written = 0
                with archive_path.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=256 * 1024):
                        if not chunk:
                            continue
                        written += len(chunk)
                        if written > max_download_bytes:
                            raise RuntimeError("Upstream archive exceeds the configured download limit.")
                        handle.write(chunk)

            with zipfile.ZipFile(archive_path, "r") as archive:
                _safe_extract(archive, extract_root)
            data_src = _extract_data_dir_from_zip(extract_root)
            staged = work_root / "staged-data"
            shutil.copytree(data_src, staged)
            _replace_directory(staged, data_dst, work_root)

        _write_stamp(stamp_path, now)
        return UpdateResult(True, "Downloaded and refreshed data cache from upstream.", str(data_dst.resolve()), now)
    except Exception as exc:
        if data_dst.is_dir() and any(data_dst.glob("*.csv")):
            return UpdateResult(False, f"Update failed, but existing cache is available: {exc}", str(data_dst.resolve()), last)
        raise

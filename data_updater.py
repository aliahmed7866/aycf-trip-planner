import os
import threading
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

DEFAULT_UPSTREAM_ZIP = "https://github.com/markvincevarga/wizzair-aycf-availability/archive/refs/heads/main.zip"
_BACKGROUND_REFRESH_LOCK = threading.Lock()

@dataclass
class UpdateResult:
    updated: bool
    message: str
    data_dir: str
    last_updated_epoch: int

def _now_epoch() -> int:
    return int(time.time())

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def _read_stamp(stamp_path: Path) -> Optional[int]:
    try:
        return int(stamp_path.read_text(encoding="utf-8").strip())
    except Exception:
        return None

def _write_stamp(stamp_path: Path, epoch: int):
    stamp_path.write_text(str(epoch), encoding="utf-8")

def _has_data(data_dir: Path) -> bool:
    return data_dir.exists() and any(data_dir.rglob("*.csv"))

def _extract_data_dir_from_zip(extract_root: Path) -> Path:
    candidates = list(extract_root.glob("**/data"))
    best = None
    best_count = -1
    for candidate in candidates:
        count = sum(1 for _ in candidate.rglob("*.csv"))
        if count > best_count:
            best = candidate
            best_count = count
    if best is None or best_count <= 0:
        raise FileNotFoundError("Could not locate a data/ folder with CSV files in the upstream zip.")
    return best

def _schedule_background_refresh(cache_root: str, upstream_zip_url: str, refresh_interval_seconds: int, timeout_seconds: int) -> bool:
    if not _BACKGROUND_REFRESH_LOCK.acquire(blocking=False):
        return False
    def worker():
        try:
            update_data_if_needed(cache_root, upstream_zip_url, refresh_interval_seconds, force=True, timeout_seconds=timeout_seconds)
        except Exception:
            pass
        finally:
            _BACKGROUND_REFRESH_LOCK.release()
    threading.Thread(target=worker, name="aycf-data-refresh", daemon=True).start()
    return True

def update_data_if_needed(cache_root: str, upstream_zip_url: str = DEFAULT_UPSTREAM_ZIP, refresh_interval_seconds: int = 24 * 3600, force: bool = False, timeout_seconds: int = 60) -> UpdateResult:
    """Keep a local upstream data cache without blocking app startup when stale.

    A forced refresh is synchronous. Normal startup returns existing data immediately
    and refreshes stale data in a daemon thread. If no usable local data exists, the
    first download remains synchronous because the planner cannot run without it.
    """
    cache_root_p = Path(cache_root)
    _ensure_dir(cache_root_p)
    stamp_path = cache_root_p / "last_update.txt"
    data_dst = cache_root_p / "data"
    last = _read_stamp(stamp_path) or 0
    now = _now_epoch()

    if (not force) and (now - last) < refresh_interval_seconds and _has_data(data_dst):
        return UpdateResult(False, "Cache fresh; no update needed.", str(data_dst.resolve()), last)

    if (not force) and _has_data(data_dst):
        scheduled = _schedule_background_refresh(cache_root, upstream_zip_url, refresh_interval_seconds, timeout_seconds)
        msg = "Using existing cache; background refresh scheduled." if scheduled else "Using existing cache; background refresh already running."
        return UpdateResult(False, msg, str(data_dst.resolve()), last)

    tmp_zip = cache_root_p / "upstream.zip"
    tmp_extract = cache_root_p / "tmp_extract"
    if tmp_extract.exists():
        import shutil
        shutil.rmtree(tmp_extract)
    _ensure_dir(tmp_extract)

    try:
        response = requests.get(upstream_zip_url, stream=True, timeout=timeout_seconds)
        response.raise_for_status()
        with open(tmp_zip, "wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if chunk:
                    handle.write(chunk)
        with zipfile.ZipFile(tmp_zip, "r") as archive:
            archive.extractall(tmp_extract)
        data_src = _extract_data_dir_from_zip(tmp_extract)

        import shutil
        new_dir = cache_root_p / "data.next"
        old_dir = cache_root_p / "data.previous"
        shutil.rmtree(new_dir, ignore_errors=True)
        shutil.rmtree(old_dir, ignore_errors=True)
        shutil.copytree(data_src, new_dir)
        if data_dst.exists():
            os.replace(data_dst, old_dir)
        os.replace(new_dir, data_dst)
        shutil.rmtree(old_dir, ignore_errors=True)

        # Planner indexes live outside data/ so the dataset can be replaced safely.
        # Invalidate them only after a successful swap; the next page/search rebuilds
        # them from the new dataset without serving stale route counts.
        shutil.rmtree(cache_root_p / ".aycf-index", ignore_errors=True)
        try:
            (cache_root_p / ".aycf-city-options.json").unlink(missing_ok=True)
        except Exception:
            pass

        _write_stamp(stamp_path, now)
        try:
            tmp_zip.unlink(missing_ok=True)
            shutil.rmtree(tmp_extract)
        except Exception:
            pass
        return UpdateResult(True, "Downloaded and refreshed data cache from upstream.", str(data_dst.resolve()), now)
    except Exception as exc:
        if _has_data(data_dst):
            return UpdateResult(False, f"Update failed, but existing cache is available: {exc}", str(data_dst.resolve()), last)
        raise

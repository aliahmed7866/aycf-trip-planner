import os
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

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

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def _read_stamp(stamp_path: Path) -> Optional[int]:
    try:
        return int(stamp_path.read_text(encoding="utf-8").strip())
    except Exception:
        return None

def _write_stamp(stamp_path: Path, epoch: int):
    stamp_path.write_text(str(epoch), encoding="utf-8")

def _extract_data_dir_from_zip(extract_root: Path) -> Path:
    """
    Upstream zip layout: wizzair-aycf-availability-main/<...>
    We want the `data/` folder inside it.
    """
    candidates = list(extract_root.glob("**/data"))
    # Prefer the one that has many csv files
    best = None
    best_count = -1
    for c in candidates:
        csvs = list(c.glob("*.csv"))
        if len(csvs) > best_count:
            best = c
            best_count = len(csvs)
    if best is None or best_count <= 0:
        raise FileNotFoundError("Could not locate a data/ folder with CSV files in the upstream zip.")
    return best

def update_data_if_needed(
    cache_root: str,
    upstream_zip_url: str = DEFAULT_UPSTREAM_ZIP,
    refresh_interval_seconds: int = 24 * 3600,
    force: bool = False,
    timeout_seconds: int = 60,
) -> UpdateResult:
    """
    Downloads upstream repo zip (daily) and extracts the `data/` folder into cache_root/data.
    Uses a stamp file to avoid repeated downloads.
    """
    cache_root_p = Path(cache_root)
    _ensure_dir(cache_root_p)

    stamp_path = cache_root_p / "last_update.txt"
    last = _read_stamp(stamp_path) or 0
    now = _now_epoch()

    if (not force) and (now - last) < refresh_interval_seconds:
        data_dir = str((cache_root_p / "data").resolve())
        return UpdateResult(updated=False, message="Cache fresh; no update needed.", data_dir=data_dir, last_updated_epoch=last)

    # Download
    tmp_zip = cache_root_p / "upstream.zip"
    tmp_extract = cache_root_p / "tmp_extract"

    if tmp_extract.exists():
        import shutil
        shutil.rmtree(tmp_extract)
    _ensure_dir(tmp_extract)

    try:
        r = requests.get(upstream_zip_url, stream=True, timeout=timeout_seconds)
        r.raise_for_status()
        with open(tmp_zip, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

        with zipfile.ZipFile(tmp_zip, "r") as z:
            z.extractall(tmp_extract)

        data_src = _extract_data_dir_from_zip(tmp_extract)

        # Replace cache_root/data atomically
        data_dst = cache_root_p / "data"
        if data_dst.exists():
            import shutil
            shutil.rmtree(data_dst)
        import shutil
        shutil.copytree(data_src, data_dst)

        _write_stamp(stamp_path, now)

        # Cleanup
        try:
            tmp_zip.unlink(missing_ok=True)
            shutil.rmtree(tmp_extract)
        except Exception:
            pass

        return UpdateResult(updated=True, message="Downloaded and refreshed data cache from upstream.", data_dir=str(data_dst.resolve()), last_updated_epoch=now)

    except Exception as e:
        # If we have existing data, keep it
        data_dst = cache_root_p / "data"
        if data_dst.exists() and any(data_dst.glob("*.csv")):
            return UpdateResult(updated=False, message=f"Update failed, but existing cache is available: {e}", data_dir=str(data_dst.resolve()), last_updated_epoch=last)
        raise

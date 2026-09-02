#!/usr/bin/env python3
"""Fetch/import public AYCF history and materialize Stability analytics."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from historical_stability import import_archive
from stability_cache import refresh_stability_cache

REPO_URL = "https://github.com/markvincevarga/wizzair-aycf-availability.git"


def sync_repo(target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if (target / ".git").is_dir():
        subprocess.run(["git", "-C", str(target), "fetch", "--depth", "1", "origin", "main"], check=True)
        subprocess.run(["git", "-C", str(target), "reset", "--hard", "origin/main"], check=True)
    else:
        subprocess.run(["git", "clone", "--depth", "1", REPO_URL, str(target)], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=365, help="Trailing archive days to import")
    parser.add_argument("--cache-dir", default=str(Path.home() / ".cache/aycf/wizzair-aycf-availability"))
    parser.add_argument("--no-fetch", action="store_true", help="Use an already cloned archive without network access")
    args = parser.parse_args()
    target = Path(args.cache_dir).expanduser()
    if not args.no_fetch:
        sync_repo(target)
    result = import_archive(str(target / "data"), days=max(1, args.days))
    result["stability_cache"] = refresh_stability_cache()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

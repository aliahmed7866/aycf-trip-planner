import json
import os
import sys

from data_updater import update_data_if_needed
from planner import AYCFPlanner
from watch_service import check_watches, watch_db_path


def main() -> int:
    cache_root = os.environ.get("AYCF_CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))
    upstream_zip = os.environ.get(
        "AYCF_UPSTREAM_ZIP",
        "https://github.com/markvincevarga/wizzair-aycf-availability/archive/refs/heads/main.zip",
    )
    refresh_seconds = int(os.environ.get("AYCF_REFRESH_SECONDS", str(24 * 3600)))

    upd = update_data_if_needed(
        cache_root=cache_root,
        upstream_zip_url=upstream_zip,
        refresh_interval_seconds=refresh_seconds,
        force=False,
    )
    planner = AYCFPlanner(data_dir=upd.data_dir)
    summary = check_watches(watch_db_path(cache_root), planner, notify=True)
    print(json.dumps(summary, indent=2))
    return 0 if summary["errors"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())

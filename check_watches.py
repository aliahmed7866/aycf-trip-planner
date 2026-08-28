#!/usr/bin/env python3
import json

from cache_db import ScanCacheDB
from watch_service import check_watches


if __name__ == "__main__":
    print(json.dumps(check_watches(ScanCacheDB(), notify=True), indent=2))

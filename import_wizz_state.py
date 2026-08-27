"""Import a temporary plaintext Playwright storage-state JSON locally.

Usage in Termux:
  source ~/.config/aycf/env
  python import_wizz_state.py /sdcard/Download/wizz-storage-state.json

The state is validated against Wizz, encrypted into WIZZ_SESSION_FILE and the
source file is deleted by default after a successful import.
"""

import json
import os
import sys
from pathlib import Path

from scanner import WizzAYCFClient
from session_vault import SessionVault


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python import_wizz_state.py /path/to/wizz-storage-state.json")
    source = Path(sys.argv[1]).expanduser().resolve()
    if not source.is_file():
        raise SystemExit(f"File not found: {source}")
    try:
        state = json.loads(source.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"Could not read storage-state JSON: {exc}")
    if not isinstance(state, dict) or not isinstance(state.get("cookies"), list):
        raise SystemExit("That file is not a Playwright storage_state object.")

    probe = WizzAYCFClient(state).bootstrap()
    SessionVault().save(state)
    print(f"Wizz session validated and encrypted. Station mappings: {probe.get('stations', 0)}")

    if os.environ.get("AYCF_KEEP_PLAINTEXT_STATE", "false").lower() != "true":
        try:
            source.unlink()
            print(f"Deleted temporary plaintext file: {source}")
        except OSError as exc:
            print(f"WARNING: could not delete {source}: {exc}")
            print("Delete it manually now.")


if __name__ == "__main__":
    main()

"""Interactive one-time setup for optional unattended Wizz login."""
import getpass
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from termux.import_wizz_from_chrome import _load_termux_env  # noqa: E402
_load_termux_env()
from credential_vault import CredentialVault  # noqa: E402


def main():
    print("Store Wizz credentials encrypted on this phone.")
    print("They are never written to the repo, SQLite, logs, or plaintext env files.")
    username = input("Wizz email/username: ").strip()
    password = getpass.getpass("Wizz password (input hidden): ")
    CredentialVault().save(username, password)
    print("[AYCF] Wizz credentials encrypted locally. Password cannot be displayed by the UI.")


if __name__ == "__main__":
    main()

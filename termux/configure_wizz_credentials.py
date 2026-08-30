"""Securely configure the encrypted credentials used for unattended Wizz renewal."""

import getpass
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from termux.env_loader import load_termux_env  # noqa: E402

load_termux_env()

from credential_vault import CredentialVault  # noqa: E402


def save_credentials(username: str, password: str) -> None:
    CredentialVault().save(username, password)


def main() -> int:
    print("Configure Wizz credentials for unattended AYCF session renewal.")
    print("Credentials are encrypted locally; the password is not echoed.")
    username = input("Wizz email/username: ").strip()
    password = getpass.getpass("Wizz password: ")
    if not username or not password:
        print("[AYCF] Both username/email and password are required.")
        return 2
    save_credentials(username, password)
    path = CredentialVault().path
    print(f"[AYCF] Encrypted Wizz credentials saved locally to {path}.")
    print("[AYCF] Run `bash termux/auto-refresh-wizz.sh` to validate unattended renewal.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import json
import os
import tempfile
from pathlib import Path
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken


class CredentialVault:
    """Small encrypted local vault for optional unattended Wizz login credentials."""

    def __init__(self, path: Optional[str] = None, key: Optional[str] = None):
        default = Path.home() / ".config" / "aycf" / "wizz_credentials.enc"
        self.path = Path(path or os.environ.get("AYCF_WIZZ_CREDENTIAL_FILE", str(default)))
        raw_key = key or os.environ.get("AYCF_SESSION_ENCRYPTION_KEY", "")
        if not raw_key:
            raise RuntimeError("AYCF_SESSION_ENCRYPTION_KEY is required for the credential vault.")
        try:
            self.fernet = Fernet(raw_key.encode("utf-8"))
        except (ValueError, TypeError) as exc:
            raise RuntimeError("AYCF_SESSION_ENCRYPTION_KEY is not a valid Fernet key.") from exc

    def save(self, username: str, password: str) -> None:
        username, password = str(username or "").strip(), str(password or "")
        if not username or not password:
            raise ValueError("Both Wizz username/email and password are required.")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({"username": username, "password": password}, separators=(",", ":")).encode()
        fd, name = tempfile.mkstemp(dir=self.path.parent, prefix=".credentials-")
        tmp = Path(name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(self.fernet.encrypt(payload))
                handle.flush()
                os.fsync(handle.fileno())
            tmp.replace(self.path)
        finally:
            tmp.unlink(missing_ok=True)

    def load(self) -> Optional[dict]:
        if not self.path.exists():
            return None
        try:
            value = json.loads(self.fernet.decrypt(self.path.read_bytes()).decode())
            if not value.get("username") or not value.get("password"):
                raise ValueError("missing fields")
            return {"username": str(value["username"]), "password": str(value["password"])}
        except (InvalidToken, ValueError, json.JSONDecodeError, OSError) as exc:
            raise RuntimeError("Stored Wizz credentials could not be decrypted or are invalid.") from exc

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()

    def exists(self) -> bool:
        return self.path.is_file()

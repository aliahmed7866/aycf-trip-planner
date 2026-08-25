import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from cryptography.fernet import Fernet, InvalidToken


class SessionVault:
    def __init__(self, path: Optional[str] = None, key: Optional[str] = None):
        self.path = Path(path or os.environ.get("WIZZ_SESSION_FILE", "./cache/wizz_session.enc"))
        raw_key = key or os.environ.get("AYCF_SESSION_ENCRYPTION_KEY", "")
        if not raw_key:
            raise RuntimeError(
                "AYCF_SESSION_ENCRYPTION_KEY is required. Generate one with "
                "`python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"`."
            )
        self.fernet = Fernet(raw_key.encode("utf-8"))

    def save(self, storage_state: Dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(storage_state, separators=(",", ":")).encode("utf-8")
        encrypted = self.fernet.encrypt(payload)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_bytes(encrypted)
        os.chmod(tmp, 0o600)
        tmp.replace(self.path)

    def load(self) -> Optional[Dict[str, Any]]:
        if not self.path.exists():
            return None
        try:
            raw = self.fernet.decrypt(self.path.read_bytes())
            return json.loads(raw.decode("utf-8"))
        except (InvalidToken, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("Stored Wizz session could not be decrypted.") from exc

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()

    def exists(self) -> bool:
        return self.path.exists()

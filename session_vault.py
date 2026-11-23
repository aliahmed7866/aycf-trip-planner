import json
import os
import fcntl
import tempfile
from contextlib import contextmanager
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
        try:
            self.fernet = Fernet(raw_key.encode("utf-8"))
        except (ValueError, TypeError) as exc:
            raise RuntimeError("AYCF_SESSION_ENCRYPTION_KEY is not a valid Fernet key.") from exc

    def save(self, storage_state: Dict[str, Any]) -> None:
        with self.lock():
            self._save(storage_state)

    @contextmanager
    def lock(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.with_suffix(".lock").open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def _save(self, storage_state):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(storage_state, separators=(",", ":")).encode("utf-8")
        encrypted = self.fernet.encrypt(payload)
        fd, name = tempfile.mkstemp(dir=self.path.parent, prefix=".session-")
        tmp = Path(name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(encrypted)
                handle.flush()
                os.fsync(handle.fileno())
            tmp.replace(self.path)
        finally:
            tmp.unlink(missing_ok=True)
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    def load(self) -> Optional[Dict[str, Any]]:
        if not self.path.exists():
            return None
        try:
            raw = self.fernet.decrypt(self.path.read_bytes())
            value = json.loads(raw.decode("utf-8"))
            if not isinstance(value, dict) or not isinstance(value.get("cookies", []), list):
                raise ValueError("invalid storage state")
            return value
        except (InvalidToken, ValueError, json.JSONDecodeError, OSError) as exc:
            raise RuntimeError("Stored Wizz session could not be decrypted or is invalid.") from exc

    def clear(self) -> None:
        with self.lock():
            self.path.unlink(missing_ok=True)

    def merge_cookies(self, previous, updated):
        """Merge server rotations without overwriting another login or worker."""
        def identity(cookie):
            return (cookie.get("domain", ""), cookie.get("path", "/"), cookie["name"])
        def same(left, right):
            if left is None or right is None:
                return left is right
            fields = {"name": "", "value": "", "domain": "", "path": "/",
                      "secure": False, "httpOnly": False, "expires": -1}
            return all(left.get(key, default) == right.get(key, default) for key, default in fields.items())
        old = {identity(c): c for c in previous.get("cookies", [])}
        new = {identity(c): c for c in updated.get("cookies", [])}
        with self.lock():
            current = self.load()
            if current is None or current.get("generation") != previous.get("generation"):
                return
            saved = {identity(c): c for c in current.get("cookies", [])}
            for key in old.keys() | new.keys():
                if not same(old.get(key), new.get(key)) and same(saved.get(key), old.get(key)):
                    if key in new:
                        saved[key] = new[key]
                    else:
                        saved.pop(key, None)
            current["cookies"] = list(saved.values())
            self._save(current)

    def exists(self) -> bool:
        return self.path.is_file()

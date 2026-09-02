import io
import zipfile
from pathlib import Path
from unittest.mock import patch

import data_updater


class FakeResponse:
    def __init__(self, content: bytes, declared_size: int = 0):
        self.content = content
        self.headers = {"Content-Length": str(declared_size)} if declared_size else {}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        for start in range(0, len(self.content), chunk_size):
            yield self.content[start:start + chunk_size]


def _zip(entries) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        for name, value in entries.items():
            archive.writestr(name, value)
    return out.getvalue()


def test_refresh_atomically_replaces_old_data(tmp_path: Path):
    data = tmp_path / "data"
    data.mkdir()
    (data / "old.csv").write_text("old", encoding="utf-8")
    payload = _zip({"archive-main/data/new.csv": "departure_from,departure_to\nA,B\n"})
    with patch.object(data_updater.requests, "get", return_value=FakeResponse(payload)):
        result = data_updater.update_data_if_needed(str(tmp_path), force=True)
    assert result.updated is True
    assert (data / "new.csv").exists()
    assert not (data / "old.csv").exists()
    assert not list(tmp_path.glob(".upstream-refresh-*"))


def test_unsafe_archive_keeps_last_good_cache(tmp_path: Path):
    data = tmp_path / "data"
    data.mkdir()
    (data / "old.csv").write_text("old", encoding="utf-8")
    payload = _zip({"../escape.csv": "bad", "archive-main/data/new.csv": "new"})
    with patch.object(data_updater.requests, "get", return_value=FakeResponse(payload)):
        result = data_updater.update_data_if_needed(str(tmp_path), force=True)
    assert result.updated is False
    assert "existing cache is available" in result.message
    assert (data / "old.csv").read_text(encoding="utf-8") == "old"
    assert not (tmp_path.parent / "escape.csv").exists()


def test_oversized_archive_keeps_last_good_cache(tmp_path: Path):
    data = tmp_path / "data"
    data.mkdir()
    (data / "old.csv").write_text("old", encoding="utf-8")
    payload = _zip({"archive-main/data/new.csv": "new"})
    with patch.object(data_updater.requests, "get", return_value=FakeResponse(payload, declared_size=999)):
        result = data_updater.update_data_if_needed(str(tmp_path), force=True, max_download_bytes=100)
    assert result.updated is False
    assert (data / "old.csv").exists()


def test_fresh_stamp_without_data_does_not_claim_usable_cache(tmp_path: Path):
    (tmp_path / "last_update.txt").write_text(str(data_updater._now_epoch()), encoding="utf-8")
    payload = _zip({"archive-main/data/new.csv": "new"})
    with patch.object(data_updater.requests, "get", return_value=FakeResponse(payload)) as get:
        result = data_updater.update_data_if_needed(str(tmp_path))
    assert result.updated is True
    get.assert_called_once()

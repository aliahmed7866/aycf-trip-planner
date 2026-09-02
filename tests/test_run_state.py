import json
from pathlib import Path
from unittest.mock import patch

from termux import run_state


def test_status_write_is_atomic_private_and_leaves_no_temp_file(tmp_path: Path):
    status = tmp_path / "scan-status.json"
    with patch.object(run_state, "STATE_DIR", tmp_path), patch.object(run_state, "STATUS_FILE", status):
        payload = run_state.write_status("running", "test")
    assert json.loads(status.read_text(encoding="utf-8")) == payload
    assert status.stat().st_mode & 0o777 == 0o600
    assert list(tmp_path.glob(".scan-status-*.tmp")) == []

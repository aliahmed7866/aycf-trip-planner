from contextlib import contextmanager
from unittest.mock import patch

from termux import supervisor


@contextmanager
def _lock(available):
    yield available


def test_active_scan_lock_keeps_busy_status():
    with patch.object(supervisor, "read_status", return_value={"state": "running", "pid": 123}), \
         patch.object(supervisor, "single_scan_lock", return_value=_lock(False)), \
         patch.object(supervisor, "_save") as save, \
         patch.object(supervisor, "write_status") as write:
        assert supervisor.main() == 0
    assert save.call_args.args[0]["state"] == "scan_busy"
    write.assert_not_called()


def test_stale_running_status_is_recovered_when_lock_is_free():
    statuses = [{"state": "running", "pid": 456}, {"state": "interrupted"}]
    with patch.object(supervisor, "read_status", side_effect=statuses), \
         patch.object(supervisor, "single_scan_lock", return_value=_lock(True)), \
         patch.object(supervisor, "write_status") as write, \
         patch.object(supervisor, "_saved_session_health", return_value=True), \
         patch.object(supervisor, "_hours", return_value=set()), \
         patch.object(supervisor, "_save"):
        assert supervisor.main() == 0
    write.assert_called_once()
    assert write.call_args.args[0] == "interrupted"
    assert write.call_args.kwargs["previous_pid"] == 456

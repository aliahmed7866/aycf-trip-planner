from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_full_deployment_probes_listeners_without_bind_race():
    source = (ROOT / "termux" / "finish-full-deployment.sh").read_text(encoding="utf-8")
    assert "connect_ex" in source
    assert "s.bind" not in source


def test_full_deployment_filters_shared_process_names_by_working_directory():
    source = (ROOT / "termux" / "finish-full-deployment.sh").read_text(encoding="utf-8")
    assert 'readlink -f "/proc/$pid/cwd"' in source
    assert '[ "$cwd" = "$APP_DIR" ]' in source
    assert "pkill -TERM -f" not in source
    assert "pkill -KILL -f" not in source

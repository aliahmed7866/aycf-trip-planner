"""Identify interpreter processes by the actual entry script in this checkout."""
import os
from pathlib import Path
import re
import signal
import sys
import time

WEB = ("watch_app.py", "termux/run-web.py", "termux/run-web.sh")
ADMIN = ("termux/admin_hub.py", "termux/run-admin.sh")
JOBS = tuple("termux/" + name for name in (
    "morning-gate.sh", "supervisor.py", "runtime.py", "automated_morning.py",
    "feeder_refresh.py", "auto-refresh-wizz.sh", "repair_wizz_runtime.py",
    "refresh_wizz_direct.py", "refresh_wizz_from_chrome.py", "auto_login_wizz.py",
))


def launched_script(proc, paths):
    try:
        args = (proc / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
        if len(args) < 2 or not re.fullmatch(r"(python[\d.]*|bash|sh|node)", Path(os.fsdecode(args[0])).name):
            return False
        index = 1
        while index < len(args) and args[index] in {b"-u", b"-B", b"--"}:
            index += 1
        if index == len(args) or args[index].startswith(b"-"):
            return False
        script = Path(os.fsdecode(args[index]))
        if not script.is_absolute():
            script = (proc / "cwd").readlink() / script
        return script.resolve() in paths
    except (OSError, ValueError, RuntimeError):
        return False


def pids(root, scripts):
    paths = {(Path(root) / name).resolve() for name in scripts}
    return [int(p.name) for p in Path("/proc").iterdir()
            if p.name.isdigit() and int(p.name) > 1 and launched_script(p, paths)]


def terminate(find, timeout=5):
    original = set(find())
    for sig, seconds in ((signal.SIGTERM, timeout), (signal.SIGKILL, 1)):
        # Repeat discovery to cover a launcher handing off or a child starting.
        for pid in find():
            if pid == os.getpid() or pid not in find():
                continue
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + seconds
        while find() and time.monotonic() < deadline:
            time.sleep(.1)
        if not find():
            return len(original)
    raise RuntimeError("App processes are still running. Refresh status and retry Stop.")


if __name__ == "__main__":
    groups = {"web": WEB, "admin": ADMIN}
    root, *names = sys.argv[1:]
    scripts = [script for name in names for script in groups[name]]
    terminate(lambda: pids(root, scripts))

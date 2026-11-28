"""Bounded startup checks with endpoint and launcher failure details."""
import argparse
import json
import os
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def check(service):
    port = os.environ.get("PORT", "8080") if service == "web" else os.environ.get("AYCF_ADMIN_PORT", "8079")
    identity = "aycf" if service == "web" else "aycf-admin"
    url = f"http://127.0.0.1:{port}/health"
    try:
        with urlopen(url, timeout=2) as response:
            data = json.load(response)
        if not isinstance(data, dict) or data.get("service") != identity:
            return False, f"{identity} on port {port}: endpoint belongs to another app or an older process"
        if data.get("ok") is not True:
            return False, f"{identity} on port {port}: app reports unhealthy"
        return True, f"{identity} on port {port}: ready"
    except HTTPError as exc:
        detail = f"HTTP {exc.code}"
    except URLError as exc:
        detail = str(exc.reason)
    except (OSError, ValueError) as exc:
        detail = str(exc)
    return False, f"{identity} on port {port}: {detail}"


def wait_ready(services, timeout):
    deadline = time.monotonic() + timeout
    previous = None
    while True:
        results = {service: check(service) for service in services}
        failures = {service: detail for service, (ok, detail) in results.items() if not ok}
        if not failures:
            for _, detail in results.values():
                print(detail, flush=True)
            return {}
        if failures != previous:
            for detail in failures.values():
                print(detail, flush=True)
            previous = failures
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return failures
        time.sleep(min(1, remaining))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("service", nargs="?", choices=["all", "web", "admin"], default="all")
    parser.add_argument("--wait", action="store_true")
    args = parser.parse_args()
    root = Path(os.environ.get("AYCF_APP_DIR", str(Path(__file__).resolve().parent.parent)))
    state = Path(os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf")))
    services = []
    if args.service in {"all", "web"} and not (state / "planner.down").exists():
        services.append("web")
    if args.service in {"all", "admin"} and (root / "termux/admin_hub.py").exists():
        services.append("admin")
    timeout = max(1, min(300, float(os.environ.get("AYCF_STARTUP_TIMEOUT", "120")))) if args.wait else 0
    failures = wait_ready(services, timeout)
    if failures and args.wait:
        for service in failures:
            path = state / "logs" / (service + ".log")
            print(f"Recent startup output from {path}", flush=True)
            try:
                with path.open('rb') as log:
                    log.seek(0, 2)
                    log.seek(max(0, log.tell() - 12000))
                    lines = log.read().decode('utf-8', errors='replace').splitlines()
                print("\n".join(lines[-30:]), flush=True)
            except OSError as exc:
                print(str(exc), flush=True)
        print("Startup failed. " + " | ".join(failures.values()), flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

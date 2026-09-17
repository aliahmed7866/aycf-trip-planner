from __future__ import annotations

import fcntl
import re
import sys
import hmac
import ipaddress
import json
import os
import secrets
import shutil
import signal
import shlex
import subprocess
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from flask import Flask, flash, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash


HOME = Path.home()
APP_ROOT = Path(os.environ.get("AYCF_APP_DIR", str(HOME / "aycf-trip-planner"))).expanduser()
STATE_DIR = Path(os.environ.get("AYCF_STATE_DIR", str(HOME / ".local/share/aycf"))).expanduser()
CONFIG_DIR = Path(os.environ.get("AYCF_CONFIG_DIR", str(HOME / ".config/aycf"))).expanduser()
REGISTRY_PATH = Path(os.environ.get("AYCF_ADMIN_REGISTRY", str(CONFIG_DIR / "apps.json"))).expanduser()
LOG_DIR = STATE_DIR / "logs"
PASSWORD_STORE = STATE_DIR / "app-password.json"


def _aycf_rate_limit() -> dict:
    # run-admin.sh executes this file directly; make the shared package available.
    checkout = str(Path(__file__).resolve().parent.parent)
    if checkout not in sys.path:
        sys.path.insert(0, checkout)
    from termux.health_ui import rate_limit_summary
    return rate_limit_summary(_read_json(STATE_DIR / 'scan-status.json'),
                              _read_json(STATE_DIR / 'supervisor-status.json'))

APP_PREFIXES = {"aycf": "AYCF", "sunscape": "SUNSCAPE", "expenses": "EXPENSE", "mediahub": "MEDIAHUB", "places": "PLACES"}


def app_environment(item: dict[str, Any]) -> dict[str, str]:
    """Use one environment contract for status, control, setup and update commands."""
    env = {**os.environ, "AYCF_APP_DIR": str(APP_ROOT), "AYCF_STATE_DIR": str(STATE_DIR),
           "AYCF_ADMIN_REGISTRY": str(REGISTRY_PATH), "GIT_TERMINAL_PROMPT": "0", "GCM_INTERACTIVE": "never"}
    prefix = APP_PREFIXES.get(item.get("id"))
    if prefix:
        env[prefix + "_APP_DIR"] = str(item.get("working_dir") or APP_ROOT)
        if item.get("port"):
            env[prefix + "_PORT"] = str(item["port"])
            if item.get("id") in {"aycf", "sunscape"}:
                env["PORT"] = str(item["port"])
        if item.get("update_branch"):
            env[prefix + ("_DEPLOY_REF" if prefix == "AYCF" else "_BRANCH")] = str(item["update_branch"])
    return env


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _service_dir(service: str) -> Path:
    prefix = Path(os.environ.get("PREFIX", "/data/data/com.termux/files/usr")).expanduser()
    return prefix / "var" / "service" / service


def _service_available(app: dict[str, Any]) -> bool:
    service = str(app.get("service") or "").strip()
    if not service or not shutil.which("sv"):
        return False
    root = _service_dir(service)
    return root.is_dir() and (root / "run").exists()


def _sunscape_direct_start(root: Path, port: int, payload: dict[str, Any]) -> list[str]:
    configured = payload.get("direct_start") or payload.get("start")
    if isinstance(configured, list) and configured and str(configured[0]) != "sv":
        return [str(x) for x in configured]

    run_web = root / "termux" / "run-web.sh"
    if run_web.exists():
        return ["bash", "termux/run-web.sh"]

    venv_gunicorn = root / ".venv" / "bin" / "gunicorn"
    if venv_gunicorn.exists():
        return [str(venv_gunicorn), "--bind", f"127.0.0.1:{port}", "app:app"]

    venv_python = root / ".venv" / "bin" / "python"
    if venv_python.exists():
        return [str(venv_python), "app.py"]

    return ["python", "app.py"]


def _sunscape_manifest() -> dict[str, Any]:
    candidates = [
        Path(os.environ.get("SUNSCAPE_APP_DIR", str(HOME / "sunscape"))).expanduser(),
        HOME / "Sunscape",
    ]
    for root in candidates:
        if not root.is_dir():
            continue
        payload = _read_json(root / "termux" / "service.json")
        port = int(payload.get("port") or 8081)
        service = str(payload.get("service") or "sunscape")
        process_match = str(payload.get("process_match") or f"{root.name}/.venv/bin/gunicorn")
        if not (root / ".venv" / "bin" / "gunicorn").exists() and not payload.get("process_match"):
            process_match = f"{root.name}.*app.py"
        return {
            "working_dir": str(root),
            "port": port,
            "health_url": str(payload.get("health_url") or f"http://127.0.0.1:{port}/health"),
            "open_url": str(payload.get("url") or f"http://127.0.0.1:{port}"),
            "service": service,
            "start": _sunscape_direct_start(root, port, payload),
            "process_match": process_match,
            "description": str(payload.get("description") or "Flask weather and sunshine finder"),
        }
    return {}


def _expand_parts(raw: Any, *, bash_only: bool = False) -> list[str]:
    if not isinstance(raw, list) or not 1 <= len(raw) <= 12 or not all(isinstance(x, str) and x for x in raw):
        return []
    command = [part.replace("$APP_ROOT", str(APP_ROOT)).replace("$HOME", str(HOME)) for part in raw]
    command = [str(HOME) + part[1:] if part.startswith('~/') else part for part in command]
    executable = Path(command[0]).expanduser()
    if bash_only and executable.name != "bash":
        return []
    if executable.is_absolute():
        try:
            executable.resolve().relative_to(HOME.resolve())
        except ValueError:
            if not (bash_only and executable.name == "bash"):
                return []
    if bash_only and len(command) < 2:
        return []
    if bash_only:
        script = Path(command[1]).expanduser().resolve()
        try:
            script.relative_to(HOME.resolve())
        except ValueError:
            return []
    return command


def _command_status(item: dict[str, Any]) -> tuple[str, str]:
    command = _expand_parts(item.get("status_command"))
    if item.get("working_dir") and not Path(item["working_dir"]).is_dir():
        return "missing", "App folder not installed"
    if not command or not (Path(command[0]).is_file() if '/' in command[0] else shutil.which(command[0])):
        return "missing", "Command not installed"
    try:
        proc = subprocess.run(command, cwd=item.get("working_dir") or str(APP_ROOT), env=app_environment(item),
                              capture_output=True, text=True, timeout=4, check=False)
    except Exception as exc:
        return "unavailable", f"Status check failed: {type(exc).__name__}"
    text = (proc.stdout or proc.stderr).strip()
    if proc.returncode == 0 and text.lower().startswith("running"):
        return "running", text
    if text.lower().startswith("stopped"):
        return "stopped", text
    return "unavailable", text or "Status command returned no state"


def _install_command(item: dict[str, Any]) -> list[str]:
    raw = item.get("install_command")
    if not isinstance(raw, list) or not 2 <= len(raw) <= 12 or not all(isinstance(x, str) and x for x in raw):
        return []
    command = [part.replace("$APP_ROOT", str(APP_ROOT)).replace("$HOME", str(HOME)) for part in raw]
    if Path(command[0]).name != "bash":
        return []
    script = Path(command[1]).expanduser().resolve()
    try:
        script.relative_to(HOME.resolve())
    except ValueError:
        return []
    return command


def _normalize_registry_app(item: dict[str, Any]) -> dict[str, Any]:
    row = dict(item)
    custom_fields = set(row.pop("_custom_fields", row.keys()))
    if row.get("id") == "aycf" and row.get("process_match") == "termux/runtime.py web":
        row["process_match"] = "watch_app.py"
    if row.get("id") == "sunscape":
        legacy = row.get("port") == 3000 or row.get("start", [])[:1] == ["npm"]
        registered = str(row.get("working_dir", ""))
        default_roots = {"", "~/sunscape", "~/Sunscape", str(HOME / "sunscape"), str(HOME / "Sunscape")}
        manifest = _sunscape_manifest() if registered in default_roots else {}
        if manifest:
            # Registered custom endpoints win over a checked-in default manifest.
            overrides = {key: row[key] for key in ("port", "health_url", "open_url")
                         if key in row and key in custom_fields and not legacy}
            row.update(manifest)
            row.update(overrides)
        else:
            root = HOME / "sunscape" if legacy else Path(registered.replace("~", str(HOME), 1) or str(HOME / "sunscape"))
            port = 8081 if legacy else int(row.get("port") or 8081)
            row["working_dir"] = str(root)
            row["port"] = port
            for key, suffix in (("health_url", "/health"), ("open_url", "")):
                if legacy or not row.get(key):
                    row[key] = f"http://127.0.0.1:{port}{suffix}"
            row.setdefault("service", "sunscape")
            if legacy or row.get("start", [])[:1] == ["sv"] or not row.get("start"):
                row["start"] = _sunscape_direct_start(root, port, {})
            if legacy or not row.get("process_match"):
                row["process_match"] = f"{root.name}/.venv/bin/gunicorn"
    row["working_dir"] = str(Path(str(row.get("working_dir", "~")).replace("$HOME", str(HOME)).replace("~", str(HOME), 1)))
    scripts = {"aycf": "auto-deploy.sh", "expenses": "auto-deploy.sh", "sunscape": "update-service.sh", "mediahub": "update.sh"}
    raw_update = row.get("update_command")
    app_id = str(row.get("id", ""))
    if app_id in scripts and raw_update and len(raw_update) >= 2:
        # Relocate only the built-in command, preserving deliberately custom ones.
        default_roots = {"aycf": "aycf-trip-planner", "expenses": "Expense_manager", "sunscape": "sunscape", "mediahub": "Django-youtube-video-and-mp3-downloader"}
        known_default = raw_update[1] in {f"$HOME/{default_roots[app_id]}/termux/{scripts[app_id]}", f"$APP_ROOT/termux/{scripts[app_id]}", str(HOME / default_roots[app_id] / "termux" / scripts[app_id])}
        if known_default:
            row["update_command"] = ["bash", str(Path(row["working_dir"]) / "termux" / scripts[app_id])] + (["--once"] if app_id == "expenses" else [])
    if app_id == "places" and raw_update == ["bash", "$HOME/.local/bin/places", "update"]:
        launcher = row.get("start_command", ["$HOME/.local/bin/places", "start"])
        row["update_command"] = launcher[:-1] + ["update"]
    row["update_ready"] = bool(row.get("update_branch") and _expand_parts(row.get("update_command")))
    row["update_status"] = update_status(app_id)
    row["service_ready"] = _service_available(row)
    row["install_ready"] = bool(_install_command(row))
    row["command_details"] = {key.replace("_command", "").replace("_", " "): shlex.join(_expand_parts(value))
                              for key, value in row.items() if key in {"start", "status_command", "start_command", "stop_command", "restart_command", "update_command", "install_command"} and _expand_parts(value)}
    if row.get("service"):
        row["command_details"]["service"] = str(_service_dir(str(row["service"])))
    for action in row.get("actions", []):
        if isinstance(action, dict) and _expand_parts(action.get("command")):
            row["command_details"][str(action.get("label", action.get("id", "Action")))] = shlex.join(_expand_parts(action["command"]))
    return row


def _load_registry() -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    sources = [APP_ROOT / "termux" / "apps.json.example"]
    if REGISTRY_PATH.exists():
        sources.append(REGISTRY_PATH)
    for source in sources:
        payload = _read_json(source)
        apps = payload.get("apps") if isinstance(payload, dict) else None
        if not isinstance(apps, list):
            continue
        for item in apps:
            if not isinstance(item, dict) or not item.get("id") or not item.get("name"):
                continue
            app_id = str(item["id"])
            if app_id not in merged:
                order.append(app_id)
            previous = merged.get(app_id, {})
            row = {**previous, **item}
            row["_custom_fields"] = list(set(previous.get("_custom_fields", [])) | (set(item) if source == REGISTRY_PATH else set()))
            if source != REGISTRY_PATH:
                prefix = APP_PREFIXES.get(app_id)
                configured_root = os.environ.get(prefix + "_APP_DIR") if prefix else None
                if app_id == "aycf":
                    configured_root = str(APP_ROOT)
                if configured_root:
                    row["working_dir"] = configured_root
            if "port" in item and item["port"] != previous.get("port"):
                for key, suffix in (("open_url", ""), ("health_url", "/health")):
                    if key not in item and previous.get(key) == f"http://127.0.0.1:{previous.get('port')}{suffix}":
                        row[key] = f"http://127.0.0.1:{item['port']}{suffix}"
            merged[app_id] = row
    return [_normalize_registry_app(merged[app_id]) for app_id in order]



def _is_loopback_host(value: str) -> bool:
    host = str(value or "").strip().strip("[]")
    try:
        return host.casefold() == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _trusted_local_request() -> bool:
    """Trust only a direct loopback request to a loopback-bound Admin Hub."""
    if os.environ.get("AYCF_REQUIRE_LOCAL_PASSWORD", "false").lower() == "true":
        return False
    return _is_loopback_host(os.environ.get("AYCF_ADMIN_BIND_HOST", "127.0.0.1")) and _is_loopback_host(request.remote_addr or "")


def _verify_admin_password(supplied: str) -> bool:
    payload = _read_json(PASSWORD_STORE)
    stored = payload.get("password_hash", "") if isinstance(payload, dict) else ""
    if isinstance(stored, str) and stored:
        try:
            return check_password_hash(stored, supplied)
        except (ValueError, TypeError):
            return False
    expected = os.environ.get("AYCF_APP_PASSWORD", "")
    return bool(expected and hmac.compare_digest(expected, supplied))


def _pids_for(match: str) -> list[int]:
    if not match:
        return []
    try:
        proc = subprocess.run(["pgrep", "-f", match], capture_output=True, text=True, timeout=3, check=False)
    except Exception:
        return []
    out = []
    for line in proc.stdout.splitlines():
        try:
            pid = int(line.strip())
        except ValueError:
            continue
        if pid != os.getpid():
            out.append(pid)
    return sorted(set(out))


def _health(url: str) -> tuple[bool, str]:
    if not url:
        return False, "No health URL"
    try:
        with urllib.request.urlopen(url, timeout=1.2) as response:
            ok = 200 <= int(response.status) < 400
            return ok, f"HTTP {response.status}"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, type(exc).__name__


def app_status(app: dict[str, Any]) -> dict[str, Any]:
    if app.get("manager") == "command":
        state, detail = _command_status(app)
        healthy, health_text = _health(str(app.get("health_url", ""))) if state == "running" else (False, "Not running")
        if state == "running" and not healthy:
            state = "starting"
        return {**app, "pids": [], "healthy": healthy, "health_text": health_text,
                "state": state, "available": state != "missing", "service_text": detail}
    if _service_available(app):
        try:
            proc = subprocess.run(["sv", "status", str(_service_dir(str(app["service"])))],
                                  capture_output=True, text=True, timeout=4, check=False)
            detail = (proc.stdout or proc.stderr).strip()
            if proc.returncode or not proc.stdout.startswith(("run:", "down:")):
                raise RuntimeError(detail or "Service supervisor returned no status")
        except (OSError, subprocess.SubprocessError, RuntimeError) as exc:
            return {**app, "pids": [], "healthy": False, "health_text": "Status unavailable",
                    "state": "unavailable", "available": True, "service_text": str(exc)[-500:]}
        running = proc.stdout.startswith("run:")
        healthy, health_text = _health(str(app.get("health_url", ""))) if running else (False, "Not running")
        return {**app, "pids": [], "healthy": healthy, "health_text": health_text,
                "state": ("running" if healthy else "starting") if running else "stopped",
                "available": True, "service_text": detail}
    workdir = Path(str(app.get("working_dir", ""))).expanduser()
    pids = _pids_for(str(app.get("process_match", "")))
    healthy, health_text = _health(str(app.get("health_url", ""))) if pids else (False, "Not running")
    if not workdir.exists():
        state = "missing"
    elif pids and healthy:
        state = "running"
    elif pids:
        state = "starting"
    else:
        state = "stopped"
    return {**app, "pids": pids, "healthy": healthy, "health_text": health_text, "state": state, "available": workdir.exists(), "service_text": "PID " + ", ".join(map(str, pids)) if pids else state}


def _service_action(app: dict[str, Any], action: str) -> bool:
    service = str(app.get("service") or "").strip()
    if not service:
        return False
    verb = {"start": "up", "stop": "down", "restart": "restart"}.get(action)
    if not verb:
        raise RuntimeError("Unsupported service action")
    managed = _service_available(app)
    try:
        proc = subprocess.run(["sv", verb, str(_service_dir(service))], capture_output=True, text=True, timeout=12, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        if managed:
            raise RuntimeError(f"Service supervisor unavailable ({type(exc).__name__}). Retry when it responds.") from exc
        return False
    if proc.returncode and managed:
        raise RuntimeError((proc.stderr or proc.stdout or "Service supervisor rejected the action.").strip()[-500:])
    return proc.returncode == 0


def _start_command(app: dict[str, Any], command: list[str], log_name: str) -> int:
    workdir = Path(str(app.get("working_dir", ""))).expanduser()
    if not workdir.exists():
        raise RuntimeError(f"Working directory does not exist: {workdir}")
    command = _expand_parts(command)
    if not command:
        raise RuntimeError("Invalid command configured")
    if Path(log_name).name != log_name:
        raise RuntimeError("Log name must be a filename")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_DIR / log_name, "ab", buffering=0) as log:
        proc = subprocess.Popen(command, cwd=str(workdir), env=app_environment(app), stdin=subprocess.DEVNULL,
                                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    return proc.pid


def _command_action(app: dict[str, Any], action: str) -> None:
    state, _ = _command_status(app)
    install = _install_command(app) if state == "missing" and action == "start" else []
    command = install or _expand_parts(app.get(f"{action}_command"))
    if not command:
        raise RuntimeError(f"No {action} command configured")
    proc = subprocess.run(command, cwd=str(APP_ROOT if install else Path(str(app.get("working_dir",APP_ROOT)))), capture_output=True, text=True,
                          env=app_environment(app), timeout=300 if install else 20, check=False)
    if proc.returncode:
        raise RuntimeError((proc.stderr or proc.stdout or f"{action} failed").strip()[-900:])


def start_app(app: dict[str, Any]) -> int | None:
    if app.get("manager") == "command":
        _command_action(app, "start")
        return None
    if _service_available(app):
        _service_action(app, "start")
        return None
    if _pids_for(str(app.get("process_match", ""))):
        return None
    install = _install_command(app)
    if install:
        proc = subprocess.run(
            install, cwd=str(APP_ROOT), capture_output=True, text=True,
            env=app_environment(app), timeout=300, check=False,
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "setup failed").strip()
            raise RuntimeError(f"Setup failed: {detail[-900:]}")
    if _service_action(app, "start"):
        return None
    command = app.get("start")
    if not isinstance(command, list) or not command:
        raise RuntimeError("No start command configured")
    return _start_command(app, [str(x) for x in command], f"admin-{app['id']}.log")


def stop_app(app: dict[str, Any], timeout: float = 5.0) -> int:
    if app.get("manager") == "command":
        _command_action(app, "stop")
        return 0
    if _service_action(app, "stop"):
        return 0
    pids = _pids_for(str(app.get("process_match", "")))
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.time() + timeout
    while time.time() < deadline and _pids_for(str(app.get("process_match", ""))):
        time.sleep(0.15)
    leftovers = _pids_for(str(app.get("process_match", "")))
    for pid in leftovers:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    return len(pids)


def restart_app(app: dict[str, Any]) -> int | None:
    if app.get("manager") == "command":
        _command_action(app, "restart")
        return None
    if _service_action(app, "restart"):
        return None
    stop_app(app)
    return start_app(app)


def update_path(app_id: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9_-]+",app_id):
        raise ValueError("Invalid app identifier")
    return STATE_DIR / "hub-updates" / (app_id+".json")


@contextmanager
def runtime_action(app_id: str):
    """Serialize control dispatch with detached updates, including concurrent POSTs."""
    path = update_path(app_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path.with_suffix('.lock'), 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("An update or runtime action is already running for this app. Wait for it to finish.")
        yield


def tail_update_log(path: Path, limit: int = 16000) -> str:
    try:
        with path.open('rb') as handle:
            handle.seek(0, os.SEEK_END)
            handle.seek(max(0, handle.tell() - limit))
            return handle.read(limit).decode('utf-8', errors='replace')
    except FileNotFoundError:
        return "No update log yet."


def update_status(app_id: str) -> dict:
    try: path=update_path(app_id)
    except ValueError: return {}
    result=_read_json(path)
    if result.get("state") in {"queued","running"}:
        try:
            with open(path.with_suffix('.lock'),'a') as lock:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                result={**result,"state":"error","message":"Update interrupted. Retry to check and recover."}
        except BlockingIOError: pass
    return result


def start_update(target: dict) -> None:
    if not target.get("update_ready"): raise RuntimeError("Update is not configured for this app.")
    path=update_path(target['id']);path.parent.mkdir(parents=True,exist_ok=True)
    with open(path.with_suffix('.lock'),'a') as lock:
        try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError: raise RuntimeError("An update is already running for this app.")
        path.write_text(json.dumps({"state":"queued","message":"Update queued…","updated_at":int(time.time())}))
        with open(path.with_suffix('.log'),'w') as log:
            subprocess.Popen([sys.executable,str(APP_ROOT/'termux/hub_update.py'),target['id'],str(lock.fileno())],
                cwd=APP_ROOT,env={**os.environ,'AYCF_APP_DIR':str(APP_ROOT),'AYCF_STATE_DIR':str(STATE_DIR),'AYCF_ADMIN_REGISTRY':str(REGISTRY_PATH)},
                stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,pass_fds=(lock.fileno(),))


def _find_app(app_id: str) -> dict[str, Any] | None:
    for app in _load_registry():
        if app.get("id") == app_id:
            return app
    return None


def _csrf_ok() -> bool:
    supplied = request.form.get("csrf_token", "")
    expected = session.get("csrf_token", "")
    return bool(supplied and expected and hmac.compare_digest(supplied, expected))


def _system_status() -> dict[str, str]:
    usage = shutil.disk_usage(HOME)
    free_gb = usage.free / (1024 ** 3)
    used_pct = int((usage.used / usage.total) * 100) if usage.total else 0
    try:
        load = os.getloadavg()[0]
        load_text = f"{load:.2f}"
    except (AttributeError, OSError):
        load_text = "—"
    try:
        seconds = float(Path("/proc/uptime").read_text().split()[0])
        hours = int(seconds // 3600)
        uptime = f"{hours // 24}d {hours % 24}h" if hours >= 24 else f"{hours}h"
    except Exception:
        uptime = "—"
    return {"storage": f"{free_gb:.1f} GB free", "storage_pct": f"{used_pct}% used", "load": load_text, "uptime": uptime}


def _present_app(item: dict[str, Any]) -> dict[str, Any]:
    managed = app_status(item)
    if managed.get('id') == 'aycf':
        managed['rate_limit'] = _aycf_rate_limit()
    update = dict(managed.get('update_status') or {})
    state = update.get('state')
    update['label'] = {'queued': 'Update queued', 'running': 'Installing update',
                       'success': 'Update installed', 'deferred': 'Update waiting',
                       'error': 'Update needs attention'}.get(state, 'Update status')
    message = str(update.get('message', ''))
    if state == 'deferred':
        if message.startswith('deferred scan-active'):
            message = 'A scan is running. Try the update again after it finishes.'
        elif message.startswith('deferred dirty'):
            message = 'Local file changes prevented the update. Review the update log.'
        elif message.startswith('blocked non-fast-forward'):
            message = 'This checkout has diverged from the deployment branch. Review the update log.'
    update['message'] = message
    managed['update_status'] = update if state else {}
    managed['attention'] = (managed['state'] in {'starting', 'unavailable', 'missing'}
                            or state in {'error', 'deferred'}
                            or bool(managed.get('rate_limit', {}).get('notice')))
    managed['revision'] = ''
    root = Path(str(managed.get('working_dir') or ''))
    if (root / '.git').exists():
        try:
            revision = subprocess.check_output(
                ['git', '--no-pager', 'rev-parse', '--short=10', 'HEAD'], cwd=root,
                text=True, stderr=subprocess.DEVNULL, timeout=2).strip()
            if re.fullmatch(r'[0-9a-f]{7,40}', revision):
                managed['revision'] = revision
        except (OSError, subprocess.SubprocessError):
            pass
    return managed


def _workspace_snapshot():
    registry = _load_registry()
    with ThreadPoolExecutor(max_workers=max(1, min(5, len(registry)))) as pool:
        apps = list(pool.map(_present_app, registry))
    return apps, {"running": sum(x['state'] == 'running' for x in apps),
                  "total": len(apps), "attention": sum(x['attention'] for x in apps)}


def create_app() -> Flask:
    app = Flask(__name__, template_folder=str(Path(__file__).with_name("templates")), static_folder=str(Path(__file__).with_name("static")))
    bind_host = os.environ.get("AYCF_ADMIN_BIND_HOST", "127.0.0.1")
    if not _is_loopback_host(bind_host) and not (_read_json(PASSWORD_STORE).get("password_hash") or os.environ.get("AYCF_APP_PASSWORD", "")):
        raise RuntimeError("An Admin Hub password is required when AYCF_ADMIN_BIND_HOST is not loopback.")
    app.secret_key = os.environ.get("FLASK_SECRET_KEY") or secrets.token_urlsafe(32)

    @app.after_request
    def fresh_controls(response):
        if request.endpoint not in {"static", "service_worker"}:
            response.headers["Cache-Control"] = "no-store"
        return response

    def render_page(section):
        authed = bool(session.get("admin_authenticated")) or _trusted_local_request()
        if not authed:
            return render_template("hub.html", authed=False, title="Personal Hub", section=section)
        csrf = session.setdefault("csrf_token", secrets.token_urlsafe(24))
        apps, totals = _workspace_snapshot()
        return render_template("hub.html", authed=True, title="Personal Hub",
            section=section, apps=apps, csrf=csrf, system=_system_status(),
            totals=totals)

    @app.get('/workspace-status')
    def workspace_status():
        if not (session.get('admin_authenticated') or _trusted_local_request()):
            return {'error': 'Sign in to refresh status.'}, 401
        section = 'manage' if request.args.get('section') == 'manage' else 'home'
        csrf = session.setdefault('csrf_token', secrets.token_urlsafe(24))
        apps, totals = _workspace_snapshot()
        return {'cards': render_template('hub_cards.html', apps=apps, section=section, csrf=csrf),
                'totals': totals, 'csrf': csrf, 'busy': any(x.get('update_status', {}).get('state') in
                                             {'queued', 'running'} for x in apps)}

    @app.get("/")
    def index():
        return render_page("home")

    @app.get("/manage")
    def manage():
        return render_page("manage")

    @app.post("/restart-all")
    def restart_all():
        if not (session.get("admin_authenticated") or _trusted_local_request()) or not _csrf_ok():
            return redirect(url_for("index"))
        count = 0
        for target in _load_registry():
            try:
                with runtime_action(str(target["id"])):
                    if app_status(target)["state"] not in {"running", "starting"}:
                        continue
                    restart_app(target)
                    count += 1
            except Exception as exc:
                flash(f"{target['name']}: {exc}")
        flash(f"Restart requested for {count} running apps.")
        return redirect(url_for("manage"))

    @app.post("/apps/<app_id>/update")
    def update_app(app_id):
        if not (session.get("admin_authenticated") or _trusted_local_request()) or not _csrf_ok():
            return redirect(url_for("index"))
        target=_find_app(app_id)
        try:
            if target is None: raise RuntimeError("Unknown app.")
            start_update(target)
            flash(f"{target['name']} update queued. Follow its progress on the app card below.")
        except Exception as exc: flash(str(exc))
        return redirect(url_for("manage"))

    @app.get("/apps/<app_id>/update-log")
    def update_log(app_id):
        if not (session.get("admin_authenticated") or _trusted_local_request()): return redirect(url_for("index"))
        if not _find_app(app_id): return "Unknown app",404
        path=update_path(app_id).with_suffix('.log')
        content = tail_update_log(path)
        response=app.response_class(content,mimetype="text/plain")
        response.headers['Cache-Control']='no-store'
        return response

    @app.post("/login")
    def login():
        supplied = request.form.get("password", "")
        if _verify_admin_password(supplied):
            session.clear()
            session["admin_authenticated"] = True
            session["csrf_token"] = secrets.token_urlsafe(24)
            return redirect(url_for("index"))
        flash("Incorrect password.")
        return redirect(url_for("index"))

    @app.post("/logout")
    def logout():
        if _csrf_ok():
            session.clear()
        return redirect(url_for("index"))

    @app.post("/apps/<app_id>/<action>")
    def control(app_id: str, action: str):
        if not (session.get("admin_authenticated") or _trusted_local_request()) or not _csrf_ok():
            return redirect(url_for("manage"))
        target = _find_app(app_id)
        if not target:
            flash("Unknown app.")
            return redirect(url_for("manage"))
        try:
            with runtime_action(app_id):
                if action == "start":
                    pid = start_app(target)
                    flash(f"{target['name']} start requested" + (f" (PID {pid})." if pid else "."))
                elif action == "stop":
                    stop_app(target)
                    flash(f"{target['name']} stop requested.")
                elif action == "restart":
                    pid = restart_app(target)
                    flash(f"Restart requested for {target['name']}" + (f" (PID {pid})." if pid else "."))
                else:
                    flash("Unsupported action.")
        except Exception as exc:
            flash(f"{target['name']}: {exc}")
        return redirect(url_for("manage"))

    @app.post("/apps/<app_id>/action/<action_id>")
    def custom_action(app_id: str, action_id: str):
        if not (session.get("admin_authenticated") or _trusted_local_request()) or not _csrf_ok():
            return redirect(url_for("index"))
        target = _find_app(app_id)
        if not target:
            flash("Unknown app.")
            return redirect(url_for("index"))
        action = next((x for x in target.get("actions", []) if isinstance(x, dict) and x.get("id") == action_id), None)
        if not action or not isinstance(action.get("command"), list):
            flash("Unknown app action.")
            return redirect(url_for("index"))
        if app_id == 'aycf' and action_id in {'morning_scan', 'repair_auth', 'health_check'}:
            rate_limit = _aycf_rate_limit()
            if rate_limit['blocked']:
                flash(rate_limit['guidance'])
                return redirect(url_for('manage'))
        try:
            with runtime_action(app_id):
                pid = _start_command(target, [str(x) for x in action["command"]], str(action.get("log") or f"admin-{app_id}-{action_id}.log"))
                flash(f"{action.get('label', action_id)} started (PID {pid}).")
        except Exception as exc:
            flash(f"Action failed: {exc}")
        return redirect(url_for("index"))

    @app.get("/service-worker.js")
    def service_worker():
        response = app.send_static_file("admin-service-worker.js")
        response.headers["Content-Type"] = "application/javascript"
        response.headers["Cache-Control"] = "no-cache"
        response.headers["Service-Worker-Allowed"] = "/"
        return response

    @app.get("/health")
    def health():
        return {"ok": True, "apps": len(_load_registry())}

    return app


if __name__ == "__main__":
    create_app().run(host=os.environ.get("AYCF_ADMIN_BIND_HOST", "127.0.0.1"), port=int(os.environ.get("AYCF_ADMIN_PORT", "8079")), debug=False)

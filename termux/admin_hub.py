from __future__ import annotations

import hmac
import ipaddress
import json
import os
import secrets
import shutil
import signal
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from flask import Flask, flash, redirect, render_template_string, request, session, url_for
from werkzeug.security import check_password_hash


HOME = Path.home()
APP_ROOT = Path(os.environ.get("AYCF_APP_DIR", str(HOME / "aycf-trip-planner"))).expanduser()
STATE_DIR = Path(os.environ.get("AYCF_STATE_DIR", str(HOME / ".local/share/aycf"))).expanduser()
CONFIG_DIR = Path(os.environ.get("AYCF_CONFIG_DIR", str(HOME / ".config/aycf"))).expanduser()
REGISTRY_PATH = Path(os.environ.get("AYCF_ADMIN_REGISTRY", str(CONFIG_DIR / "apps.json"))).expanduser()
LOG_DIR = STATE_DIR / "logs"
PASSWORD_STORE = STATE_DIR / "app-password.json"


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
    if not command or not Path(command[0]).expanduser().exists():
        return "missing", "Command not installed"
    try:
        proc = subprocess.run(command, capture_output=True, text=True, timeout=4, check=False)
    except Exception as exc:
        return "missing", type(exc).__name__
    text = (proc.stdout or proc.stderr).strip()
    if proc.returncode == 0 and text.lower().startswith("running"):
        return "running", text
    return "stopped", text or "stopped"


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
    if row.get("id") == "sunscape":
        manifest = _sunscape_manifest()
        if manifest:
            row.update(manifest)
        else:
            root = HOME / "sunscape"
            port = 8081
            row.update({
                "working_dir": str(root),
                "port": port,
                "health_url": f"http://127.0.0.1:{port}/health",
                "open_url": f"http://127.0.0.1:{port}",
                "service": "sunscape",
                "start": _sunscape_direct_start(root, port, {}),
                "process_match": f"{root.name}/.venv/bin/gunicorn",
                "description": str(row.get("description") or "Flask weather and sunshine finder"),
            })
    row["working_dir"] = str(Path(str(row.get("working_dir", "~"))).expanduser())
    row["service_ready"] = _service_available(row)
    row["install_ready"] = bool(_install_command(row))
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
            merged[app_id] = {**merged.get(app_id, {}), **item}
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
    try:
        proc = subprocess.run(["sv", verb, service], capture_output=True, text=True, timeout=12, check=False)
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0


def _start_command(app: dict[str, Any], command: list[str], log_name: str) -> int:
    workdir = Path(str(app.get("working_dir", ""))).expanduser()
    if not workdir.exists():
        raise RuntimeError(f"Working directory does not exist: {workdir}")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = open(LOG_DIR / log_name, "ab", buffering=0)
    proc = subprocess.Popen(command, cwd=str(workdir), env=os.environ.copy(), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    return proc.pid


def _command_action(app: dict[str, Any], action: str) -> None:
    state, _ = _command_status(app)
    install = _install_command(app) if state == "missing" and action == "start" else []
    command = install or _expand_parts(app.get(f"{action}_command"))
    if not command:
        raise RuntimeError(f"No {action} command configured")
    proc = subprocess.run(command, cwd=str(APP_ROOT), capture_output=True, text=True,
                          timeout=300 if install else 20, check=False)
    if proc.returncode:
        raise RuntimeError((proc.stderr or proc.stdout or f"{action} failed").strip()[-900:])


def start_app(app: dict[str, Any]) -> int | None:
    if app.get("manager") == "command":
        _command_action(app, "start")
        return None
    if _pids_for(str(app.get("process_match", ""))):
        return None
    if not _service_available(app):
        install = _install_command(app)
        if install:
            proc = subprocess.run(
                install, cwd=str(APP_ROOT), capture_output=True, text=True,
                timeout=300, check=False,
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
    pids = _pids_for(str(app.get("process_match", "")))
    if _service_action(app, "stop"):
        return len(pids)
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


STYLE = r"""
:root{font-family:Inter,ui-sans-serif,system-ui,sans-serif;color-scheme:dark;--bg:#0b1018;--panel:#131b27;--panel2:#182333;--line:#283548;--text:#f3f6fb;--muted:#95a4b8;--blue:#6e8fff;--good:#76d9a8;--warn:#ffd271;--bad:#ff96a7}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 15% 0,#16243b 0,transparent 34%),var(--bg);color:var(--text);min-height:100vh}.wrap{max-width:1120px;margin:auto;padding:22px 18px 48px}.nav{display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin-bottom:28px}.brand{font-weight:800;font-size:1.05rem;margin-right:auto}.nav a,.nav button{color:var(--muted);text-decoration:none;border:0;background:transparent;border-radius:10px;padding:9px 12px;font:inherit;cursor:pointer}.nav a.active{background:#1b2a3e;color:white}.hero{display:flex;justify-content:space-between;align-items:end;gap:18px;margin:12px 0 24px}.eyebrow{text-transform:uppercase;letter-spacing:.14em;font-size:.7rem;color:#7f91ab;font-weight:800}.hero h1{font-size:clamp(2rem,5vw,3.6rem);letter-spacing:-.04em;margin:5px 0 8px}.muted{color:var(--muted)}.app-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:14px}.app-tile{display:block;text-decoration:none;color:inherit;background:linear-gradient(145deg,#172131,#111924);border:1px solid var(--line);border-radius:22px;padding:20px;min-height:190px;transition:.18s transform,.18s border-color}.app-tile:hover{transform:translateY(-3px);border-color:#536987}.tile-top{display:flex;justify-content:space-between;gap:12px}.icon{font-size:2rem;line-height:1}.app-tile h2{font-size:1.1rem;margin:28px 0 7px}.app-tile p{font-size:.86rem;line-height:1.45;color:var(--muted);margin:0}.badge{display:inline-flex;align-items:center;padding:5px 9px;border-radius:999px;font-size:.68rem;font-weight:800;background:#263548;color:#c4cfdd}.running{background:#183a30;color:#9ce8c1}.stopped{background:#3a2930;color:#ffc0cb}.starting{background:#40371e;color:#ffe08a}.missing{background:#382d45;color:#dabdff}.summary{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:22px}.metric,.card{background:var(--panel);border:1px solid var(--line);border-radius:18px;padding:16px}.metric strong{display:block;font-size:1.35rem}.metric span{font-size:.78rem;color:var(--muted)}.manage-grid{display:grid;gap:12px}.manage-card{background:var(--panel);border:1px solid var(--line);border-radius:18px;padding:17px}.row{display:flex;align-items:center;justify-content:space-between;gap:12px}.app-name{display:flex;align-items:center;gap:12px}.app-name .icon{font-size:1.5rem}.manage-card h2{font-size:1rem;margin:0}.meta{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:14px;color:var(--muted);font-size:.8rem}.actions{display:flex;flex-wrap:wrap;gap:8px;margin-top:14px}.actions form{margin:0}button,a.btn{border:1px solid #34445c;border-radius:10px;padding:9px 11px;background:#223149;color:white;text-decoration:none;font-weight:700;cursor:pointer;font-size:.82rem}.primary{background:#486df1!important;border-color:#486df1!important}.danger{background:#622c38!important;border-color:#743442!important}.ghost{background:transparent!important}.flash{padding:10px 12px;border-radius:11px;background:#19314a;border:1px solid #2b4e6d;margin-bottom:12px}.section-title{display:flex;justify-content:space-between;align-items:end;margin:28px 0 12px}.section-title h2{margin:0}.login{max-width:420px;margin:13vh auto;background:var(--panel);padding:28px;border:1px solid var(--line);border-radius:22px}.login input{width:100%;padding:12px;border-radius:10px;border:1px solid #35465e;background:#0d1521;color:#fff;margin:10px 0 14px}.empty{text-align:center;padding:40px;color:var(--muted)}@media(max-width:720px){.summary{grid-template-columns:repeat(2,1fr)}.meta{grid-template-columns:1fr}.hero{display:block}.nav{position:sticky;top:0;z-index:5;background:#0b1018e8;backdrop-filter:blur(14px);padding:10px 0}.app-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.app-tile{min-height:165px;padding:16px}.app-tile h2{margin-top:22px}}@media(max-width:430px){.app-grid{grid-template-columns:1fr 1fr}.app-tile p{display:none}.app-tile{min-height:135px}.app-tile h2{margin-bottom:0}.summary{gap:8px}}
"""

SHELL = r"""
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="manifest" href="/static/admin-manifest.webmanifest?pwa=3"><link rel="apple-touch-icon" href="/static/admin-icon-192.png"><title>{{ title }}</title><style>""" + STYLE + r"""</style></head><body><div class="wrap">
{% if authed %}<nav class="nav"><div class="brand">Personal Hub</div><a href="{{ url_for('index') }}" class="{{ 'active' if section == 'home' else '' }}">Apps</a><a href="{{ url_for('manage') }}" class="{{ 'active' if section == 'manage' else '' }}">Manage</a><form method="post" action="{{ url_for('logout') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button>Sign out</button></form></nav>{% endif %}
{% with messages=get_flashed_messages() %}{% for message in messages %}<div class="flash">{{ message }}</div>{% endfor %}{% endwith %}
{{ body|safe }}</div><script src="/static/admin-pwa.js" defer></script></body></html>
"""

LOGIN_BODY = r"""<div class="login"><div class="eyebrow">Local control centre</div><h1>Personal Hub</h1><p class="muted">Use your current AYCF app password.</p><form method="post" action="{{ url_for('login') }}"><input type="password" name="password" autocomplete="current-password" required><button class="primary">Sign in</button></form></div>"""
HOME_BODY = r"""<section class="hero"><div><div class="eyebrow">Your local apps</div><h1>Everything, one tap away.</h1><p class="muted">Open the tools you use. Manage every local Flask service from one place.</p></div><a class="btn ghost" href="{{ url_for('manage') }}">System management →</a></section><div class="app-grid">{% for app in apps %}{% if app.open_url %}<a class="app-tile" href="{{ app.open_url if app.state == 'running' else url_for('manage') }}"><div class="tile-top"><span class="icon">{{ app.icon or '◉' }}</span><span class="badge {{ app.state }}">{{ app.state }}</span></div><h2>{{ app.name }}</h2><p>{{ app.description or '' }}</p></a>{% endif %}{% endfor %}</div>"""
MANAGE_BODY = r"""<section class="hero"><div><div class="eyebrow">System management</div><h1>Health & controls.</h1><p class="muted">Check local services, restart an app or recover one that is not installed.</p></div><form method="post" action="{{ url_for('restart_all') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button>Restart running apps</button></form></section><div class="summary"><div class="metric"><strong>{{ totals.running }}/{{ totals.total }}</strong><span>apps running</span></div><div class="metric"><strong>{{ system.uptime }}</strong><span>device uptime</span></div><div class="metric"><strong>{{ system.storage }}</strong><span>{{ system.storage_pct }}</span></div><div class="metric"><strong>{{ system.load }}</strong><span>1 min load</span></div></div><div class="section-title"><h2>Apps</h2><span class="muted">Refresh the page to re-check health</span></div><div class="manage-grid">{% for app in apps %}<article class="manage-card"><div class="row"><div class="app-name"><span class="icon">{{ app.icon or '◉' }}</span><div><h2>{{ app.name }}</h2><span class="muted">{{ app.description or '' }}</span></div></div><span class="badge {{ app.state }}">{{ app.state|upper }}</span></div><div class="meta"><div>Port <strong>{{ app.port or '—' }}</strong></div><div>Health <strong>{{ app.health_text }}</strong></div><div title="{{ app.service_text }}">Runtime <strong>{{ app.service_text[:48] }}{{ '…' if app.service_text|length > 48 else '' }}</strong></div></div><div class="actions"><form method="post" action="{{ url_for('control', app_id=app.id, action='start') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="primary">Start</button></form><form method="post" action="{{ url_for('control', app_id=app.id, action='restart') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button>Restart</button></form><form method="post" action="{{ url_for('control', app_id=app.id, action='stop') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="danger">Stop</button></form>{% if app.open_url %}<a class="btn ghost" href="{{ app.open_url }}">Open</a>{% endif %}{% for action in app.actions or [] %}{% if app.available %}<form method="post" action="{{ url_for('custom_action', app_id=app.id, action_id=action.id) }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button>{{ action.label }}</button></form>{% endif %}{% endfor %}</div></article>{% endfor %}</div>"""


def create_app() -> Flask:
    app = Flask(__name__)
    bind_host = os.environ.get("AYCF_ADMIN_BIND_HOST", "127.0.0.1")
    if not _is_loopback_host(bind_host) and not (_read_json(PASSWORD_STORE).get("password_hash") or os.environ.get("AYCF_APP_PASSWORD", "")):
        raise RuntimeError("An Admin Hub password is required when AYCF_ADMIN_BIND_HOST is not loopback.")
    app.secret_key = os.environ.get("FLASK_SECRET_KEY") or secrets.token_urlsafe(32)

    def render_page(section):
        authed = bool(session.get("admin_authenticated")) or _trusted_local_request()
        if not authed:
            inner = render_template_string(LOGIN_BODY)
            return render_template_string(SHELL, authed=False, body=inner, title="Personal Hub")
        csrf = session.setdefault("csrf_token", secrets.token_urlsafe(24))
        apps = [app_status(item) for item in _load_registry()]
        inner = render_template_string(HOME_BODY if section == "home" else MANAGE_BODY,
            apps=apps, csrf=csrf, system=_system_status() if section == "manage" else {},
            totals={"running": sum(x["state"] == "running" for x in apps), "total": len(apps)})
        return render_template_string(SHELL, authed=True, body=inner, title="Personal Hub", section=section, csrf=csrf)

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
            if app_status(target)["state"] not in {"running", "starting"}:
                continue
            try:
                restart_app(target)
                count += 1
            except Exception as exc:
                flash(f"{target['name']}: {exc}")
        flash(f"Restart requested for {count} running apps.")
        return redirect(url_for("manage"))

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
            return redirect(url_for("index"))
        target = _find_app(app_id)
        if not target:
            flash("Unknown app.")
            return redirect(url_for("index"))
        try:
            if action == "start":
                pid = start_app(target)
                flash(f"{target['name']} start requested" + (f" (PID {pid})." if pid else "."))
            elif action == "stop":
                count = stop_app(target)
                flash(f"Stopped {target['name']} ({count} process{'es' if count != 1 else ''}).")
            elif action == "restart":
                pid = restart_app(target)
                flash(f"Restarted {target['name']}" + (f" (PID {pid})." if pid else "."))
            else:
                flash("Unsupported action.")
        except Exception as exc:
            flash(f"{target['name']}: {exc}")
        return redirect(url_for("index"))

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
        try:
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

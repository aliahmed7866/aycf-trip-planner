from __future__ import annotations

import hmac
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


def _normalize_registry_app(item: dict[str, Any]) -> dict[str, Any]:
    row = dict(item)
    if row.get("id") == "sunscape":
        manifest = _sunscape_manifest()
        if manifest:
            row.update(manifest)
        else:
            root = Path(str(row.get("working_dir") or HOME / "sunscape")).expanduser()
            if not root.exists():
                fallback = HOME / "sunscape"
                if fallback.exists():
                    root = fallback
            port = int(row.get("port") or 8081)
            row.update({
                "working_dir": str(root),
                "port": port,
                "health_url": str(row.get("health_url") or f"http://127.0.0.1:{port}/health"),
                "open_url": str(row.get("open_url") or f"http://127.0.0.1:{port}"),
                "service": str(row.get("service") or "sunscape"),
                "start": _sunscape_direct_start(root, port, row),
                "process_match": str(row.get("process_match") or f"{root.name}/.venv/bin/gunicorn"),
                "description": str(row.get("description") or "Flask weather and sunshine finder"),
            })
    row["working_dir"] = str(Path(str(row.get("working_dir", "~"))).expanduser())
    row["service_ready"] = _service_available(row)
    return row


def _load_registry() -> list[dict[str, Any]]:
    source = REGISTRY_PATH if REGISTRY_PATH.exists() else APP_ROOT / "termux" / "apps.json.example"
    payload = _read_json(source)
    apps = payload.get("apps") if isinstance(payload, dict) else None
    if not isinstance(apps, list):
        return []
    clean = []
    for item in apps:
        if not isinstance(item, dict) or not item.get("id") or not item.get("name"):
            continue
        clean.append(_normalize_registry_app(item))
    return clean


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
    return {**app, "pids": pids, "healthy": healthy, "health_text": health_text, "state": state, "available": workdir.exists()}


def _service_action(app: dict[str, Any], action: str) -> bool:
    if not _service_available(app):
        return False
    service = str(app.get("service") or "").strip()
    verb = {"start": "up", "stop": "down", "restart": "restart"}.get(action)
    if not verb:
        raise RuntimeError("Unsupported service action")
    proc = subprocess.run(["sv", verb, service], capture_output=True, text=True, timeout=12, check=False)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "service command failed").strip()
        raise RuntimeError(detail)
    return True


def _start_command(app: dict[str, Any], command: list[str], log_name: str) -> int:
    workdir = Path(str(app.get("working_dir", ""))).expanduser()
    if not workdir.exists():
        raise RuntimeError(f"Working directory does not exist: {workdir}")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = open(LOG_DIR / log_name, "ab", buffering=0)
    proc = subprocess.Popen(command, cwd=str(workdir), env=os.environ.copy(), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    return proc.pid


def start_app(app: dict[str, Any]) -> int | None:
    if _pids_for(str(app.get("process_match", ""))):
        return None
    if _service_action(app, "start"):
        return None
    command = app.get("start")
    if not isinstance(command, list) or not command:
        raise RuntimeError("No start command configured")
    return _start_command(app, [str(x) for x in command], f"admin-{app['id']}.log")


def stop_app(app: dict[str, Any], timeout: float = 5.0) -> int:
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


PAGE = r"""
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="theme-color" content="#070b14"><title>Phone Admin Hub</title>
<style>
:root{color-scheme:dark;font-family:Inter,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;--bg:#070b14;--panel:#101827;--panel2:#0c1421;--line:#263247;--text:#f4f7fb;--muted:#94a0b3;--accent:#7658f6;--green:#47dda1;--amber:#ffc869;--red:#ff7087}*{box-sizing:border-box}body{margin:0;min-height:100vh;color:var(--text);background:radial-gradient(55rem 28rem at 0 -10%,#5f42d52b,transparent 60%),linear-gradient(180deg,#09101c,#070b14)}.wrap{width:min(1080px,calc(100% - 28px));margin:auto;padding:28px 0 70px}.top{display:flex;justify-content:space-between;align-items:flex-start;gap:18px;margin-bottom:24px}.eyebrow{text-transform:uppercase;letter-spacing:.14em;font-size:.67rem;font-weight:850;color:#b7a8ff}.title{font-size:clamp(2.2rem,7vw,4rem);line-height:.96;letter-spacing:-.055em;margin:8px 0 12px}.muted{color:var(--muted);line-height:1.55}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(310px,1fr));gap:16px}.card{background:linear-gradient(180deg,#111b2c,#0d1624);border:1px solid var(--line);border-radius:24px;padding:22px;box-shadow:0 20px 60px #0005}.row{display:flex;align-items:flex-start;justify-content:space-between;gap:14px}.badge{padding:7px 10px;border-radius:999px;font-size:.72rem;font-weight:850;letter-spacing:.05em;background:#283548}.running{background:#123d2e;color:#9af0c5}.stopped{background:#3c2930;color:#ffb9c4}.starting{background:#40381d;color:#ffe08a}.missing{background:#392c46;color:#d8b9ff}.buttons{display:flex;flex-wrap:wrap;gap:9px;margin-top:20px}button,a.btn{border:1px solid #324058;border-radius:12px;padding:11px 14px;background:#1d2a3d;color:white;text-decoration:none;font-weight:750;cursor:pointer;min-height:44px}.primary{background:var(--accent)!important;border-color:var(--accent)!important}.danger{background:#7b2b39!important;border-color:#8c3343!important}.ghost{background:#141e2d!important}.flash{padding:12px 14px;border:1px solid #30415a;border-radius:13px;background:#122034;margin-bottom:12px}.login{max-width:430px;margin:12vh auto}.login input{width:100%;padding:14px;border-radius:13px;border:1px solid #35465e;background:#09111d;color:#fff;margin:14px 0}.meta{display:grid;gap:7px;margin-top:18px;font-size:.92rem}.path{font-size:.76rem;color:#718096;overflow-wrap:anywhere;margin-top:5px}h2{font-size:1.7rem;letter-spacing:-.035em;margin:0 0 8px}.summary{display:flex;gap:8px;align-items:center;margin-top:6px}.dot{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 0 5px #47dda117}.footer{margin-top:18px;color:#667387;font-size:.78rem}@media(max-width:640px){.wrap{width:calc(100% - 24px);padding-top:22px}.top{align-items:center}.title{font-size:3rem}.grid{grid-template-columns:1fr}.card{border-radius:20px;padding:18px}button,a.btn{flex:1;text-align:center}.buttons form{display:flex;flex:1}.buttons form button{width:100%}}
</style></head><body><div class="wrap">
{% if not authed %}<div class="card login"><div class="eyebrow">Local operations</div><h1 class="title">Phone Admin Hub</h1><p class="muted">Use your current AYCF app password.</p><form method="post" action="{{ url_for('login') }}"><input type="password" name="password" autocomplete="current-password" placeholder="Password" required><button class="primary" type="submit">Sign in</button></form></div>
{% else %}<div class="top"><div><div class="eyebrow">Local operations</div><h1 class="title">Phone Admin Hub</h1><div class="muted">Manage every local Flask service from one place.</div><div class="summary"><span class="dot"></span><span class="muted">{{ apps|selectattr('state','equalto','running')|list|length }} of {{ apps|length }} apps running</span></div></div><form method="post" action="{{ url_for('logout') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="ghost">Sign out</button></form></div>
{% with messages=get_flashed_messages() %}{% for message in messages %}<div class="flash">{{ message }}</div>{% endfor %}{% endwith %}
<div class="grid">{% for app in apps %}<section class="card"><div class="row"><div><h2>{{ app.name }}</h2><div class="muted">{{ app.description or '' }}</div></div><span class="badge {{ app.state }}">{{ app.state|upper }}</span></div><div class="meta"><div><strong>Port</strong> · {{ app.port or '—' }}</div><div><strong>Health</strong> · {{ app.health_text }}</div><div><strong>PID</strong> · {{ app.pids|join(', ') if app.pids else '—' }}</div><div class="path">{{ app.working_dir }}</div></div><div class="buttons">{% if app.available and app.state != 'running' %}<form method="post" action="{{ url_for('control', app_id=app.id, action='start') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="primary">Start</button></form>{% endif %}{% if app.pids %}<form method="post" action="{{ url_for('control', app_id=app.id, action='restart') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button>Restart</button></form><form method="post" action="{{ url_for('control', app_id=app.id, action='stop') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="danger">Stop</button></form>{% endif %}{% if app.available %}<a class="btn ghost" href="{{ app.open_url }}">Open</a>{% endif %}{% for action in app.actions or [] %}{% if app.available %}<form method="post" action="{{ url_for('custom_action', app_id=app.id, action_id=action.id) }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button>{{ action.label }}</button></form>{% endif %}{% endfor %}</div></section>{% endfor %}</div><div class="footer">Admin Hub · 127.0.0.1:8079 · local device only</div>{% endif %}</div></body></html>
"""


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY") or secrets.token_urlsafe(32)

    @app.get("/")
    def index():
        authed = bool(session.get("admin_authenticated"))
        if not authed:
            return render_template_string(PAGE, authed=False)
        session.setdefault("csrf_token", secrets.token_urlsafe(24))
        apps = [app_status(item) for item in _load_registry()]
        return render_template_string(PAGE, authed=True, apps=apps, csrf=session["csrf_token"])

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
        if not session.get("admin_authenticated") or not _csrf_ok():
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
        if not session.get("admin_authenticated") or not _csrf_ok():
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

    @app.get("/health")
    def health():
        apps = [app_status(item) for item in _load_registry()]
        return {"ok": True, "apps": len(apps), "running": sum(1 for item in apps if item["state"] == "running")}

    return app


if __name__ == "__main__":
    create_app().run(host=os.environ.get("AYCF_ADMIN_BIND_HOST", "127.0.0.1"), port=int(os.environ.get("AYCF_ADMIN_PORT", "8079")), debug=False)
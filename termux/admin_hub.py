from __future__ import annotations

import hmac
import json
import os
import secrets
import signal
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from flask import Flask, flash, redirect, render_template_string, request, session, url_for


HOME = Path.home()
APP_ROOT = Path(os.environ.get("AYCF_APP_DIR", str(HOME / "aycf-trip-planner"))).expanduser()
STATE_DIR = Path(os.environ.get("AYCF_STATE_DIR", str(HOME / ".local/share/aycf"))).expanduser()
CONFIG_DIR = Path(os.environ.get("AYCF_CONFIG_DIR", str(HOME / ".config/aycf"))).expanduser()
REGISTRY_PATH = Path(os.environ.get("AYCF_ADMIN_REGISTRY", str(CONFIG_DIR / "apps.json"))).expanduser()
LOG_DIR = STATE_DIR / "logs"


def _load_registry() -> list[dict[str, Any]]:
    source = REGISTRY_PATH if REGISTRY_PATH.exists() else APP_ROOT / "termux" / "apps.json.example"
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except Exception:
        return []
    apps = payload.get("apps") if isinstance(payload, dict) else None
    if not isinstance(apps, list):
        return []
    clean = []
    for item in apps:
        if not isinstance(item, dict) or not item.get("id") or not item.get("name"):
            continue
        row = dict(item)
        row["working_dir"] = str(Path(str(row.get("working_dir", "~"))).expanduser())
        clean.append(row)
    return clean


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


def _start_command(app: dict[str, Any], command: list[str], log_name: str) -> int:
    workdir = Path(str(app.get("working_dir", ""))).expanduser()
    if not workdir.exists():
        raise RuntimeError(f"Working directory does not exist: {workdir}")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = open(LOG_DIR / log_name, "ab", buffering=0)
    proc = subprocess.Popen(
        command,
        cwd=str(workdir),
        env=os.environ.copy(),
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return proc.pid


def start_app(app: dict[str, Any]) -> int | None:
    if _pids_for(str(app.get("process_match", ""))):
        return None
    command = app.get("start")
    if not isinstance(command, list) or not command:
        raise RuntimeError("No start command configured")
    return _start_command(app, [str(x) for x in command], f"admin-{app['id']}.log")


def stop_app(app: dict[str, Any], timeout: float = 5.0) -> int:
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
<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Phone Admin Hub</title>
<style>
:root{font-family:Inter,system-ui,sans-serif;color-scheme:dark;background:#090d14;color:#eef3fb}body{margin:0;background:linear-gradient(180deg,#0d1420,#090d14);min-height:100vh}.wrap{max-width:980px;margin:auto;padding:24px}.top{display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:20px}.muted{color:#95a4b8}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(285px,1fr));gap:16px}.card{background:#121b29;border:1px solid #273448;border-radius:18px;padding:18px;box-shadow:0 12px 30px #0005}.row{display:flex;align-items:center;justify-content:space-between;gap:10px}.badge{padding:5px 9px;border-radius:999px;font-size:.78rem;font-weight:700;background:#283548}.running{background:#173f31;color:#9af0c5}.stopped{background:#3c2930;color:#ffb9c4}.starting{background:#40381d;color:#ffe08a}.missing{background:#392c46;color:#d8b9ff}.buttons{display:flex;flex-wrap:wrap;gap:8px;margin-top:16px}button,a.btn{border:0;border-radius:10px;padding:10px 12px;background:#26364c;color:white;text-decoration:none;font-weight:650;cursor:pointer}.primary{background:#3f6df6!important}.danger{background:#7b2b39!important}.ghost{background:#1a2432!important}.flash{padding:10px 12px;border-radius:10px;background:#1a2a3b;margin-bottom:12px}.login{max-width:420px;margin:14vh auto}.login input{width:100%;box-sizing:border-box;padding:12px;border-radius:10px;border:1px solid #35465e;background:#0d1521;color:#fff;margin:10px 0}small{color:#95a4b8}.meta{display:grid;gap:5px;margin-top:12px;font-size:.9rem}h1,h2,p{margin-top:0}
</style></head><body><div class="wrap">
{% if not authed %}
<div class="card login"><h1>Phone Admin Hub</h1><p class="muted">Use your existing AYCF app password.</p><form method="post" action="{{ url_for('login') }}"><input type="password" name="password" autocomplete="current-password" required><button class="primary" type="submit">Sign in</button></form></div>
{% else %}
<div class="top"><div><h1>Phone Admin Hub</h1><div class="muted">Manage local subapps from one place.</div></div><form method="post" action="{{ url_for('logout') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="ghost">Sign out</button></form></div>
{% with messages=get_flashed_messages() %}{% for message in messages %}<div class="flash">{{ message }}</div>{% endfor %}{% endwith %}
<div class="grid">{% for app in apps %}<div class="card"><div class="row"><div><h2>{{ app.name }}</h2><div class="muted">{{ app.description or '' }}</div></div><span class="badge {{ app.state }}">{{ app.state|upper }}</span></div><div class="meta"><div>Port: {{ app.port or '—' }}</div><div>Health: {{ app.health_text }}</div><div>PID: {{ app.pids|join(', ') if app.pids else '—' }}</div><small>{{ app.working_dir }}</small></div><div class="buttons">{% if app.available and app.state != 'running' %}<form method="post" action="{{ url_for('control', app_id=app.id, action='start') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="primary">Start</button></form>{% endif %}{% if app.pids %}<form method="post" action="{{ url_for('control', app_id=app.id, action='restart') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button>Restart</button></form><form method="post" action="{{ url_for('control', app_id=app.id, action='stop') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="danger">Stop</button></form>{% endif %}{% if app.available %}<a class="btn ghost" href="{{ app.open_url }}">Open</a>{% endif %}{% for action in app.actions or [] %}{% if app.available %}<form method="post" action="{{ url_for('custom_action', app_id=app.id, action_id=action.id) }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button>{{ action.label }}</button></form>{% endif %}{% endfor %}</div></div>{% endfor %}</div>
{% endif %}</div></body></html>
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
        expected = os.environ.get("AYCF_APP_PASSWORD", "")
        supplied = request.form.get("password", "")
        if expected and hmac.compare_digest(expected, supplied):
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
                stop_app(target)
                pid = start_app(target)
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
        return {"ok": True, "apps": len(_load_registry())}

    return app


if __name__ == "__main__":
    create_app().run(host=os.environ.get("AYCF_ADMIN_BIND_HOST", "127.0.0.1"), port=int(os.environ.get("AYCF_ADMIN_PORT", "8079")), debug=False)

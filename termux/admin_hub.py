from __future__ import annotations

import hmac
import json
import os
import secrets
import shlex
import subprocess
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from flask import Flask, flash, redirect, render_template_string, request, session, url_for

HOME = Path.home()
APP_ROOT = Path(os.environ.get("AYCF_APP_DIR", str(HOME / "aycf-trip-planner"))).expanduser()
CONFIG_DIR = Path(os.environ.get("AYCF_CONFIG_DIR", str(HOME / ".config/aycf"))).expanduser()
ENV_FILE = Path(os.environ.get("AYCF_ENV_FILE", str(CONFIG_DIR / "env"))).expanduser()
REGISTRY_PATH = Path(os.environ.get("AYCF_ADMIN_REGISTRY", str(CONFIG_DIR / "apps.json"))).expanduser()


def _load_env_file() -> None:
    if not ENV_FILE.exists():
        return
    for raw in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        try:
            parts = shlex.split(line, posix=True)
        except ValueError:
            continue
        if len(parts) != 1 or "=" not in parts[0]:
            continue
        key, value = parts[0].split("=", 1)
        if key and key not in os.environ:
            os.environ[key] = value


_load_env_file()


def _load_registry() -> list[dict[str, Any]]:
    source = REGISTRY_PATH if REGISTRY_PATH.exists() else APP_ROOT / "termux" / "apps.json.example"
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except Exception:
        return []
    apps = payload.get("apps") if isinstance(payload, dict) else None
    return [dict(x) for x in apps or [] if isinstance(x, dict) and x.get("id") and x.get("name")]


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


def _service_status(name: str) -> tuple[str, str]:
    try:
        proc = subprocess.run(["sv", "status", name], capture_output=True, text=True, timeout=3, check=False)
        text = (proc.stdout or proc.stderr).strip()
    except Exception as exc:
        return "missing", type(exc).__name__
    if text.startswith("run:"):
        return "running", text
    if text.startswith("down:"):
        return "stopped", text
    return "missing", text or "service unavailable"


def _health(url: str) -> tuple[bool, str]:
    if not url:
        return False, "No health URL"
    try:
        with urllib.request.urlopen(url, timeout=1.5) as response:
            return 200 <= int(response.status) < 400, f"HTTP {response.status}"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, type(exc).__name__


def app_status(item: dict[str, Any]) -> dict[str, Any]:
    service = str(item.get("service", ""))
    state, service_text = _service_status(service) if service else ("missing", "No service")
    healthy, health_text = _health(str(item.get("health_url", ""))) if state == "running" else (False, "Not running")
    if state == "running" and not healthy:
        state = "starting"
    return {**item, "state": state, "service_text": service_text, "healthy": healthy, "health_text": health_text}


def _csrf_ok() -> bool:
    supplied = request.form.get("csrf_token", "")
    expected = session.get("csrf_token", "")
    return bool(supplied and expected and hmac.compare_digest(supplied, expected))


PAGE = r"""
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AYCF Admin</title><style>
:root{font-family:Inter,system-ui,sans-serif;color-scheme:dark;background:#090d14;color:#eef3fb}body{margin:0;background:linear-gradient(180deg,#0d1420,#090d14);min-height:100vh}.wrap{max-width:1050px;margin:auto;padding:24px}.top{display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:20px}.muted{color:#95a4b8}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(285px,1fr));gap:16px}.card{background:#121b29;border:1px solid #273448;border-radius:18px;padding:18px;box-shadow:0 12px 30px #0005}.row{display:flex;align-items:center;justify-content:space-between;gap:10px}.badge{padding:5px 9px;border-radius:999px;font-size:.78rem;font-weight:700;background:#283548}.running{background:#173f31;color:#9af0c5}.stopped{background:#3c2930;color:#ffb9c4}.starting{background:#40381d;color:#ffe08a}.missing{background:#392c46;color:#d8b9ff}.buttons{display:flex;flex-wrap:wrap;gap:8px;margin-top:16px}button,a.btn{border:0;border-radius:10px;padding:10px 12px;background:#26364c;color:white;text-decoration:none;font-weight:650;cursor:pointer}.primary{background:#3f6df6!important}.danger{background:#7b2b39!important}.ghost{background:#1a2432!important}.flash{padding:10px 12px;border-radius:10px;background:#1a2a3b;margin-bottom:12px}.login{max-width:420px;margin:14vh auto}.login input{width:100%;box-sizing:border-box;padding:12px;border-radius:10px;border:1px solid #35465e;background:#0d1521;color:#fff;margin:10px 0}small{color:#95a4b8}.meta{display:grid;gap:5px;margin-top:12px;font-size:.9rem}h1,h2,p{margin-top:0}
</style></head><body><div class="wrap">
{% if not authed %}<div class="card login"><h1>AYCF Admin</h1><p class="muted">Use your AYCF app password.</p><form method="post" action="{{ url_for('login') }}"><input type="password" name="password" autocomplete="current-password" required><button class="primary">Sign in</button></form></div>
{% else %}<div class="top"><div><h1>AYCF Admin</h1><div class="muted">One control surface for the phone services.</div></div><form method="post" action="{{ url_for('logout') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="ghost">Sign out</button></form></div>
{% with messages=get_flashed_messages() %}{% for message in messages %}<div class="flash">{{ message }}</div>{% endfor %}{% endwith %}
<div class="grid">{% for app in apps %}<div class="card"><div class="row"><div><h2>{{ app.name }}</h2><div class="muted">{{ app.description or '' }}</div></div><span class="badge {{ app.state }}">{{ app.state|upper }}</span></div><div class="meta"><div>Port: {{ app.port or '—' }}</div><div>Health: {{ app.health_text }}</div><small>{{ app.service_text }}</small></div><div class="buttons"><form method="post" action="{{ url_for('control', app_id=app.id, action='up') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="primary">Start</button></form><form method="post" action="{{ url_for('control', app_id=app.id, action='restart') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button>Restart</button></form><form method="post" action="{{ url_for('control', app_id=app.id, action='down') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="danger">Stop</button></form>{% if app.open_url %}<a class="btn ghost" href="{{ app.open_url }}">Open</a>{% endif %}</div></div>{% endfor %}</div>{% endif %}</div></body></html>
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
        return render_template_string(PAGE, authed=True, apps=[app_status(x) for x in _load_registry()], csrf=session["csrf_token"])

    @app.post("/login")
    def login():
        expected = os.environ.get("AYCF_APP_PASSWORD", "")
        supplied = request.form.get("password", "")
        if expected and hmac.compare_digest(expected, supplied):
            session.clear(); session["admin_authenticated"] = True; session["csrf_token"] = secrets.token_urlsafe(24)
            return redirect(url_for("index"))
        flash("Incorrect password.")
        return redirect(url_for("index"))

    @app.post("/logout")
    def logout():
        if _csrf_ok(): session.clear()
        return redirect(url_for("index"))

    @app.post("/apps/<app_id>/<action>")
    def control(app_id: str, action: str):
        if not session.get("admin_authenticated") or not _csrf_ok():
            return redirect(url_for("index"))
        target = next((x for x in _load_registry() if x.get("id") == app_id), None)
        if not target or action not in {"up", "down", "restart"}:
            flash("Unsupported action."); return redirect(url_for("index"))
        service = str(target.get("service", ""))
        if not service:
            flash("No runit service configured."); return redirect(url_for("index"))
        state, _ = _service_status(service)
        command = _install_command(target) if action == "up" and state == "missing" else []
        if action == "up" and state == "missing" and not command:
            flash(f"{target.get('name', service)} is not installed and has no setup command configured.")
            return redirect(url_for("index"))
        try:
            proc = subprocess.run(
                command or ["sv", action, service],
                cwd=str(APP_ROOT), capture_output=True, text=True,
                timeout=300 if command else 10, check=False,
            )
            detail = (proc.stdout or proc.stderr or f"{action} requested for {service}").strip()
            if len(detail) > 900:
                detail = detail[-900:]
            prefix = "Setup complete. " if command and proc.returncode == 0 else ""
            flash(prefix + detail)
        except subprocess.TimeoutExpired:
            flash(f"Setup timed out for {target.get('name', service)}. Check its service log.")
        except OSError as exc:
            flash(f"Could not control {target.get('name', service)}: {type(exc).__name__}")
        return redirect(url_for("index"))

    @app.get("/health")
    def health():
        return {"ok": True, "password_configured": bool(os.environ.get("AYCF_APP_PASSWORD")), "apps": len(_load_registry())}

    return app


if __name__ == "__main__":
    create_app().run(host=os.environ.get("AYCF_ADMIN_BIND_HOST", "127.0.0.1"), port=int(os.environ.get("AYCF_ADMIN_PORT", "8079")), debug=False)

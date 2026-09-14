from __future__ import annotations

import hmac
import json
import os
import secrets
import shlex
import shutil
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


def _command_status(item: dict[str, Any]) -> tuple[str, str]:
    command = _expand_parts(item.get("status_command"))
    if not command or not Path(command[0]).expanduser().exists():
        return "missing", "Command not installed"
    try:
        proc = subprocess.run(command, capture_output=True, text=True, timeout=4, check=False)
    except Exception as exc:
        return "missing", type(exc).__name__
    text = (proc.stdout or proc.stderr).strip()
    if proc.returncode == 0 and "running" in text.lower():
        return "running", text
    return "stopped", text or "stopped"


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
    if item.get("manager") == "command":
        state, service_text = _command_status(item)
    else:
        service = str(item.get("service", ""))
        state, service_text = _service_status(service) if service else ("missing", "No service")
    healthy, health_text = _health(str(item.get("health_url", ""))) if state == "running" else (False, "Not running")
    if state == "running" and not healthy:
        state = "starting"
    return {**item, "state": state, "service_text": service_text, "healthy": healthy, "health_text": health_text}


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


def _csrf_ok() -> bool:
    supplied = request.form.get("csrf_token", "")
    expected = session.get("csrf_token", "")
    return bool(supplied and expected and hmac.compare_digest(supplied, expected))


STYLE = r"""
:root{font-family:Inter,ui-sans-serif,system-ui,sans-serif;color-scheme:dark;--bg:#0b1018;--panel:#131b27;--panel2:#182333;--line:#283548;--text:#f3f6fb;--muted:#95a4b8;--blue:#6e8fff;--good:#76d9a8;--warn:#ffd271;--bad:#ff96a7}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 15% 0,#16243b 0,transparent 34%),var(--bg);color:var(--text);min-height:100vh}.wrap{max-width:1120px;margin:auto;padding:22px 18px 48px}.nav{display:flex;align-items:center;gap:8px;margin-bottom:28px}.brand{font-weight:800;font-size:1.05rem;margin-right:auto}.nav a,.nav button{color:var(--muted);text-decoration:none;border:0;background:transparent;border-radius:10px;padding:9px 12px;font:inherit;cursor:pointer}.nav a.active{background:#1b2a3e;color:white}.hero{display:flex;justify-content:space-between;align-items:end;gap:18px;margin:12px 0 24px}.eyebrow{text-transform:uppercase;letter-spacing:.14em;font-size:.7rem;color:#7f91ab;font-weight:800}.hero h1{font-size:clamp(2rem,5vw,3.6rem);letter-spacing:-.04em;margin:5px 0 8px}.muted{color:var(--muted)}.app-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:14px}.app-tile{display:block;text-decoration:none;color:inherit;background:linear-gradient(145deg,#172131,#111924);border:1px solid var(--line);border-radius:22px;padding:20px;min-height:190px;transition:.18s transform,.18s border-color}.app-tile:hover{transform:translateY(-3px);border-color:#536987}.tile-top{display:flex;justify-content:space-between;gap:12px}.icon{font-size:2rem;line-height:1}.app-tile h2{font-size:1.1rem;margin:28px 0 7px}.app-tile p{font-size:.86rem;line-height:1.45;color:var(--muted);margin:0}.badge{display:inline-flex;align-items:center;padding:5px 9px;border-radius:999px;font-size:.68rem;font-weight:800;background:#263548;color:#c4cfdd}.running{background:#183a30;color:#9ce8c1}.stopped{background:#3a2930;color:#ffc0cb}.starting{background:#40371e;color:#ffe08a}.missing{background:#382d45;color:#dabdff}.summary{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:22px}.metric,.card{background:var(--panel);border:1px solid var(--line);border-radius:18px;padding:16px}.metric strong{display:block;font-size:1.35rem}.metric span{font-size:.78rem;color:var(--muted)}.manage-grid{display:grid;gap:12px}.manage-card{background:var(--panel);border:1px solid var(--line);border-radius:18px;padding:17px}.row{display:flex;align-items:center;justify-content:space-between;gap:12px}.app-name{display:flex;align-items:center;gap:12px}.app-name .icon{font-size:1.5rem}.manage-card h2{font-size:1rem;margin:0}.meta{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:14px;color:var(--muted);font-size:.8rem}.actions{display:flex;flex-wrap:wrap;gap:8px;margin-top:14px}.actions form{margin:0}button,a.btn{border:1px solid #34445c;border-radius:10px;padding:9px 11px;background:#223149;color:white;text-decoration:none;font-weight:700;cursor:pointer;font-size:.82rem}.primary{background:#486df1!important;border-color:#486df1!important}.danger{background:#622c38!important;border-color:#743442!important}.ghost{background:transparent!important}.flash{padding:10px 12px;border-radius:11px;background:#19314a;border:1px solid #2b4e6d;margin-bottom:12px}.section-title{display:flex;justify-content:space-between;align-items:end;margin:28px 0 12px}.section-title h2{margin:0}.login{max-width:420px;margin:13vh auto;background:var(--panel);padding:28px;border:1px solid var(--line);border-radius:22px}.login input{width:100%;padding:12px;border-radius:10px;border:1px solid #35465e;background:#0d1521;color:#fff;margin:10px 0 14px}.empty{text-align:center;padding:40px;color:var(--muted)}@media(max-width:720px){.summary{grid-template-columns:repeat(2,1fr)}.meta{grid-template-columns:1fr}.hero{display:block}.nav{position:sticky;top:0;z-index:5;background:#0b1018e8;backdrop-filter:blur(14px);padding:10px 0}.app-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.app-tile{min-height:165px;padding:16px}.app-tile h2{margin-top:22px}}@media(max-width:430px){.app-grid{grid-template-columns:1fr 1fr}.app-tile p{display:none}.app-tile{min-height:135px}.app-tile h2{margin-bottom:0}.summary{gap:8px}}
"""

SHELL = r"""
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{{ title }}</title><style>""" + STYLE + r"""</style></head><body><div class="wrap">
{% if authed %}<nav class="nav"><div class="brand">Personal Hub</div><a href="{{ url_for('index') }}" class="{{ 'active' if section == 'home' else '' }}">Apps</a><a href="{{ url_for('manage') }}" class="{{ 'active' if section == 'manage' else '' }}">Manage</a><form method="post" action="{{ url_for('logout') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button>Sign out</button></form></nav>{% endif %}
{% with messages=get_flashed_messages() %}{% for message in messages %}<div class="flash">{{ message }}</div>{% endfor %}{% endwith %}
{{ body|safe }}</div></body></html>
"""

LOGIN_BODY = r"""<div class="login"><div class="eyebrow">Local control centre</div><h1>Personal Hub</h1><p class="muted">Sign in with your AYCF app password.</p><form method="post" action="{{ url_for('login') }}"><input type="password" name="password" autocomplete="current-password" required><button class="primary">Sign in</button></form></div>"""
HOME_BODY = r"""<section class="hero"><div><div class="eyebrow">Your local apps</div><h1>Everything, one tap away.</h1><p class="muted">Open the tools you use. Service controls stay out of the way in Manage.</p></div><a class="btn ghost" href="{{ url_for('manage') }}">System management →</a></section><div class="app-grid">{% for app in apps %}{% if app.open_url %}<a class="app-tile" href="{{ app.open_url }}"><div class="tile-top"><span class="icon">{{ app.icon or '◉' }}</span><span class="badge {{ app.state }}">{{ app.state }}</span></div><h2>{{ app.name }}</h2><p>{{ app.description or '' }}</p></a>{% endif %}{% endfor %}</div>"""
MANAGE_BODY = r"""<section class="hero"><div><div class="eyebrow">System management</div><h1>Health & controls.</h1><p class="muted">Check local services, restart an app or recover one that is not installed.</p></div><form method="post" action="{{ url_for('restart_all') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button>Restart running apps</button></form></section><div class="summary"><div class="metric"><strong>{{ totals.running }}/{{ totals.total }}</strong><span>apps running</span></div><div class="metric"><strong>{{ system.uptime }}</strong><span>device uptime</span></div><div class="metric"><strong>{{ system.storage }}</strong><span>{{ system.storage_pct }}</span></div><div class="metric"><strong>{{ system.load }}</strong><span>1 min load</span></div></div><div class="section-title"><h2>Apps</h2><span class="muted">Refresh the page to re-check health</span></div><div class="manage-grid">{% for app in apps %}<article class="manage-card"><div class="row"><div class="app-name"><span class="icon">{{ app.icon or '◉' }}</span><div><h2>{{ app.name }}</h2><span class="muted">{{ app.description or '' }}</span></div></div><span class="badge {{ app.state }}">{{ app.state|upper }}</span></div><div class="meta"><div>Port <strong>{{ app.port or '—' }}</strong></div><div>Health <strong>{{ app.health_text }}</strong></div><div title="{{ app.service_text }}">Runtime <strong>{{ app.service_text[:48] }}{{ '…' if app.service_text|length > 48 else '' }}</strong></div></div><div class="actions"><form method="post" action="{{ url_for('control', app_id=app.id, action='up') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="primary">Start</button></form><form method="post" action="{{ url_for('control', app_id=app.id, action='restart') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button>Restart</button></form><form method="post" action="{{ url_for('control', app_id=app.id, action='down') }}"><input type="hidden" name="csrf_token" value="{{ csrf }}"><button class="danger">Stop</button></form>{% if app.open_url %}<a class="btn ghost" href="{{ app.open_url }}">Open</a>{% endif %}</div></article>{% endfor %}</div>"""


def _render(body: str, *, title: str, section: str, **context: Any):
    csrf = session.setdefault("csrf_token", secrets.token_urlsafe(24)) if session.get("admin_authenticated") else ""
    inner = render_template_string(body, csrf=csrf, **context)
    return render_template_string(SHELL, body=inner, title=title, section=section, authed=bool(session.get("admin_authenticated")), csrf=csrf)


def _run_control(target: dict[str, Any], action: str) -> tuple[bool, str]:
    if target.get("manager") == "command":
        state, _ = _command_status(target)
        command = _install_command(target) if action == "up" and state == "missing" else _expand_parts(target.get({"up": "start_command", "down": "stop_command", "restart": "restart_command"}[action]))
    else:
        service = str(target.get("service", ""))
        state, _ = _service_status(service) if service else ("missing", "")
        command = _install_command(target) if action == "up" and state == "missing" else (["sv", action, service] if service else [])
    if not command:
        return False, f"{target.get('name', 'App')} has no usable {action} command configured."
    timeout = 300 if command == _install_command(target) else 15
    try:
        proc = subprocess.run(command, cwd=str(APP_ROOT), capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        return False, f"{target.get('name', 'App')} timed out. Check its logs."
    except OSError as exc:
        return False, f"Could not control {target.get('name', 'app')}: {type(exc).__name__}"
    detail = (proc.stdout or proc.stderr or f"{action} requested").strip()
    return proc.returncode == 0, detail[-900:]


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY") or secrets.token_urlsafe(32)

    def require_auth():
        return bool(session.get("admin_authenticated"))

    @app.get("/")
    def index():
        if not require_auth():
            return _render(LOGIN_BODY, title="Personal Hub", section="home")
        apps = [app_status(x) for x in _load_registry()]
        return _render(HOME_BODY, title="Apps · Personal Hub", section="home", apps=apps)

    @app.get("/manage")
    def manage():
        if not require_auth():
            return redirect(url_for("index"))
        apps = [app_status(x) for x in _load_registry()]
        totals = {"total": len(apps), "running": sum(1 for x in apps if x["state"] == "running")}
        return _render(MANAGE_BODY, title="Manage · Personal Hub", section="manage", apps=apps, totals=totals, system=_system_status())

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
        if _csrf_ok():
            session.clear()
        return redirect(url_for("index"))

    @app.post("/apps/<app_id>/<action>")
    def control(app_id: str, action: str):
        if not require_auth() or not _csrf_ok():
            return redirect(url_for("index"))
        target = next((x for x in _load_registry() if x.get("id") == app_id), None)
        if not target or action not in {"up", "down", "restart"}:
            flash("Unsupported action.")
            return redirect(url_for("manage"))
        ok, detail = _run_control(target, action)
        flash(("✓ " if ok else "⚠ ") + (detail or f"{action} requested"))
        return redirect(url_for("manage"))

    @app.post("/restart-all")
    def restart_all():
        if not require_auth() or not _csrf_ok():
            return redirect(url_for("index"))
        restarted = 0
        for target in _load_registry():
            if app_status(target)["state"] not in {"running", "starting"}:
                continue
            ok, _ = _run_control(target, "restart")
            restarted += int(ok)
        flash(f"Restart requested for {restarted} running app{'s' if restarted != 1 else ''}.")
        return redirect(url_for("manage"))

    @app.get("/health")
    def health():
        apps = [app_status(x) for x in _load_registry()]
        return {"ok": True, "password_configured": bool(os.environ.get("AYCF_APP_PASSWORD")), "apps": len(apps), "healthy": sum(1 for x in apps if x["healthy"])}

    return app


if __name__ == "__main__":
    create_app().run(host=os.environ.get("AYCF_ADMIN_BIND_HOST", "127.0.0.1"), port=int(os.environ.get("AYCF_ADMIN_PORT", "8079")), debug=False)

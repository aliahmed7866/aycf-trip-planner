"""Persistent in-app password management for AYCF Flight OS.

The existing AYCF_APP_PASSWORD environment variable remains the bootstrap
credential. Once a password is changed in the web UI, a salted Werkzeug hash is
stored under AYCF_STATE_DIR and becomes authoritative. Plaintext passwords are
never persisted.
"""

from __future__ import annotations

import hmac
import json
import os
import secrets
from pathlib import Path
from urllib.parse import urlsplit

from flask import flash, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash


_STORE_NAME = "app-password.json"
_MIN_PASSWORD_LENGTH = 10
_INSTALLED = False


def _state_dir() -> Path:
    return Path(os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf")))


def _store_path() -> Path:
    return _state_dir() / _STORE_NAME


def _safe_next(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc or not parsed.path.startswith("/") or parsed.path.startswith("//"):
        return None
    return value


class PasswordStore:
    def __init__(self, path: Path | None = None):
        self.path = path or _store_path()

    def has_override(self) -> bool:
        return bool(self._read_hash())

    def configured(self) -> bool:
        return self.has_override() or bool(os.environ.get("AYCF_APP_PASSWORD", ""))

    def verify(self, supplied: str) -> bool:
        stored = self._read_hash()
        if stored:
            try:
                return check_password_hash(stored, supplied)
            except (ValueError, TypeError):
                return False
        bootstrap = os.environ.get("AYCF_APP_PASSWORD", "")
        return bool(bootstrap and hmac.compare_digest(bootstrap, supplied))

    def save(self, password: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "password_hash": generate_password_hash(password, method="scrypt"),
        }
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        try:
            os.chmod(tmp, 0o600)
        except OSError:
            pass
        os.replace(tmp, self.path)
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    def _read_hash(self) -> str:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, TypeError):
            return ""
        value = data.get("password_hash", "") if isinstance(data, dict) else ""
        return value if isinstance(value, str) else ""


def _csrf_ok() -> bool:
    expected = session.get("csrf_token", "")
    supplied = request.form.get("csrf_token", "") or request.headers.get("X-CSRF-Token", "")
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))


def _accepts_html() -> bool:
    return bool(request.accept_mimetypes.accept_html)


def install_flask_password_manager() -> None:
    """Install the password manager on every subsequently-created Flask app.

    AYCF imports sitecustomize at interpreter startup, so this hooks Flask app
    construction before app.create_app() runs. It is deliberately narrow: when
    no in-app override exists, the existing AYCF_APP_PASSWORD behavior is left
    untouched.
    """
    global _INSTALLED
    if _INSTALLED:
        return

    try:
        from flask import Flask
    except Exception:
        return

    original_init = Flask.__init__
    if getattr(original_init, "_aycf_password_manager", False):
        _INSTALLED = True
        return

    def wrapped_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        _attach(self)

    wrapped_init._aycf_password_manager = True
    Flask.__init__ = wrapped_init
    _INSTALLED = True


def _attach(app) -> None:
    store = PasswordStore()

    @app.before_request
    def aycf_password_override_guard():
        # Until an in-app password has been saved, preserve the original app's
        # AYCF_APP_PASSWORD authentication path exactly as-is.
        if not store.has_override():
            return None

        path = request.path
        if path.startswith("/static/") or path in {"/health", "/admin/wizz/session"}:
            return None

        if path == "/login":
            if request.method == "GET":
                # When the environment bootstrap password has later been removed,
                # the original login route would redirect away. Render it here.
                if not os.environ.get("AYCF_APP_PASSWORD", ""):
                    return render_template("login.html")
                return None
            if not _csrf_ok():
                flash("Your login form expired. Please try again.", "warning")
                return redirect(url_for("login"))
            if store.verify(request.form.get("password", "")):
                next_url = _safe_next(request.args.get("next")) or url_for("index")
                session.clear()
                session["aycf_authenticated"] = True
                session["csrf_token"] = secrets.token_urlsafe(32)
                session.permanent = True
                return redirect(next_url)
            flash("Incorrect password.", "danger")
            return render_template("login.html"), 401

        if not session.get("aycf_authenticated"):
            if _accepts_html():
                return redirect(url_for("login", next=path))
            return jsonify({"ok": False, "error": "login required"}), 401
        return None

    @app.route("/account/password", methods=["GET", "POST"], endpoint="manage_password")
    def manage_password():
        if request.method == "GET":
            return render_template(
                "manage_password.html",
                password_override=store.has_override(),
                minimum_length=_MIN_PASSWORD_LENGTH,
            )

        if not _csrf_ok():
            flash("Your password form expired. Please try again.", "warning")
            return redirect(url_for("manage_password"))

        current = request.form.get("current_password", "")
        new_password = request.form.get("new_password", "")
        confirmation = request.form.get("confirm_password", "")

        if not store.configured():
            flash("Set AYCF_APP_PASSWORD once before enabling in-app password management.", "warning")
            return redirect(url_for("manage_password"))
        if not store.verify(current):
            flash("Current password is incorrect.", "danger")
            return redirect(url_for("manage_password"))
        if len(new_password) < _MIN_PASSWORD_LENGTH:
            flash(f"New password must be at least {_MIN_PASSWORD_LENGTH} characters.", "warning")
            return redirect(url_for("manage_password"))
        if new_password != confirmation:
            flash("New password and confirmation do not match.", "warning")
            return redirect(url_for("manage_password"))
        if store.verify(new_password):
            flash("Choose a new password that differs from the current password.", "warning")
            return redirect(url_for("manage_password"))

        store.save(new_password)
        session.clear()
        session["aycf_authenticated"] = True
        session["csrf_token"] = secrets.token_urlsafe(32)
        session.permanent = True
        flash("App password updated. The new password is now active.", "success")
        return redirect(url_for("manage_password"))

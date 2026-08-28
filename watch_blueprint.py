import hmac
import os
from datetime import date

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from cache_db import ScanCacheDB
from watch_service import add_watch, check_watches, delete_watch, list_watches, recent_matches, set_watch_enabled, watch_city_options


def create_watch_blueprint(scan_db: ScanCacheDB | None = None):
    bp = Blueprint("watches", __name__)
    scan_db = scan_db or ScanCacheDB()

    def csrf_ok() -> bool:
        expected = session.get("csrf_token", "")
        supplied = request.form.get("csrf_token", "")
        return bool(expected and supplied and hmac.compare_digest(expected, supplied))

    def require_csrf():
        if not csrf_ok():
            flash("That form expired. Please try again.", "warning")
            return False
        return True

    @bp.get("/watches")
    def watchlist():
        return render_template(
            "watches.html",
            watches=list_watches(),
            matches=recent_matches(limit=40),
            cities=watch_city_options(scan_db),
            today=date.today().isoformat(),
            notifications_enabled=os.environ.get("AYCF_NOTIFICATIONS", "true").lower() not in {"0","false","off","no"},
        )

    @bp.post("/watches/add")
    def add():
        if not require_csrf():
            return redirect(url_for("watches.watchlist"))
        origin=(request.form.get("origin") or "").strip(); destination=(request.form.get("destination") or "").strip()
        date_from=(request.form.get("date_from") or "").strip(); date_to=(request.form.get("date_to") or "").strip() or date_from
        try:
            add_watch(origin,destination,date_from,date_to)
            flash(f"Watch added: {origin} → {destination}.","success")
        except Exception as exc:
            flash(str(exc),"danger")
        return redirect(url_for("watches.watchlist"))

    @bp.post("/watches/<int:watch_id>/toggle")
    def toggle(watch_id: int):
        if not require_csrf():
            return redirect(url_for("watches.watchlist"))
        enabled=(request.form.get("enabled") or "0")=="1"
        if set_watch_enabled(watch_id,enabled):
            flash("Watch enabled." if enabled else "Watch paused.","success")
        return redirect(url_for("watches.watchlist"))

    @bp.post("/watches/<int:watch_id>/delete")
    def remove(watch_id: int):
        if not require_csrf():
            return redirect(url_for("watches.watchlist"))
        if delete_watch(watch_id): flash("Watch deleted.","success")
        return redirect(url_for("watches.watchlist"))

    @bp.post("/watches/check")
    def check_now():
        if not require_csrf():
            return redirect(url_for("watches.watchlist"))
        summary=check_watches(scan_db,notify=True)
        flash(f"Checked {summary['checked']} watch(es): {summary['new_matches']} new match(es), {summary['notifications']} notification(s), {summary['errors']} error(s).", "success" if not summary["errors"] else "warning")
        return redirect(url_for("watches.watchlist"))

    return bp

import os
from datetime import date

from flask import Blueprint, flash, redirect, render_template, request, url_for

from data_updater import update_data_if_needed
from planner import AYCFPlanner
from watch_service import (
    add_watch,
    check_watches,
    delete_watch,
    list_watches,
    recent_matches,
    set_watch_enabled,
    watch_db_path,
)


def create_watch_blueprint(cache_root: str):
    bp = Blueprint("watches", __name__)
    db_path = watch_db_path(cache_root)

    def _planner():
        upstream_zip = os.environ.get(
            "AYCF_UPSTREAM_ZIP",
            "https://github.com/markvincevarga/wizzair-aycf-availability/archive/refs/heads/main.zip",
        )
        refresh_seconds = int(os.environ.get("AYCF_REFRESH_SECONDS", str(24 * 3600)))
        upd = update_data_if_needed(
            cache_root=cache_root,
            upstream_zip_url=upstream_zip,
            refresh_interval_seconds=refresh_seconds,
            force=False,
        )
        return AYCFPlanner(data_dir=upd.data_dir)

    @bp.route("/watches", methods=["GET"])
    def watchlist():
        planner = _planner()
        try:
            cities = planner.city_options(lookback_days=365)
        except Exception:
            cities = []
        return render_template(
            "watches.html",
            watches=list_watches(db_path),
            matches=recent_matches(db_path, 30),
            cities=cities,
            today=date.today().isoformat(),
            notifications_enabled=os.environ.get("AYCF_NOTIFICATIONS", "true").lower() not in {"0", "false", "off", "no"},
        )

    @bp.route("/watches/add", methods=["POST"])
    def add():
        origin = (request.form.get("origin") or "").strip()
        destination = (request.form.get("destination") or "").strip()
        date_from = (request.form.get("date_from") or "").strip()
        date_to = (request.form.get("date_to") or "").strip() or date_from
        try:
            add_watch(db_path, origin, destination, date_from, date_to)
            flash(f"Watch added: {origin} → {destination}.", "success")
        except Exception as exc:
            flash(str(exc), "danger")
        return redirect(url_for("watches.watchlist"))

    @bp.route("/watches/<int:watch_id>/toggle", methods=["POST"])
    def toggle(watch_id: int):
        enabled = (request.form.get("enabled") or "0") == "1"
        if set_watch_enabled(db_path, watch_id, enabled):
            flash("Watch enabled." if enabled else "Watch paused.", "success")
        return redirect(url_for("watches.watchlist"))

    @bp.route("/watches/<int:watch_id>/delete", methods=["POST"])
    def remove(watch_id: int):
        if delete_watch(db_path, watch_id):
            flash("Watch deleted.", "success")
        return redirect(url_for("watches.watchlist"))

    @bp.route("/watches/check", methods=["POST"])
    def check_now():
        summary = check_watches(db_path, _planner(), notify=True)
        flash(
            f"Checked {summary['checked']} watch(es): {summary['new_matches']} new availability match(es), "
            f"{summary['notifications']} notification(s), {summary['errors']} error(s).",
            "success" if not summary["errors"] else "warning",
        )
        return redirect(url_for("watches.watchlist"))

    return bp

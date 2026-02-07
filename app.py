import os
from flask import Flask, render_template, request, flash
from planner import AYCFPlanner

def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key-change-me")

    data_dir = os.environ.get("AYCF_DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
    planner = AYCFPlanner(data_dir=data_dir)

    @app.route("/", methods=["GET", "POST"])
    def index():
        defaults = planner.ui_defaults()
        if request.method == "POST":
            form = request.form

            start_date = form.get("start_date") or None
            end_date = form.get("end_date") or None

            bases = request.form.getlist("bases")
            hubs = request.form.getlist("hubs")
            targets = request.form.getlist("targets")

            # allow comma-separated custom targets
            custom = (form.get("custom_targets") or "").strip()
            if custom:
                extra = [x.strip() for x in custom.split(",") if x.strip()]
                targets.extend(extra)

            require_return_to_base = (form.get("require_return_to_base") == "on")

            top_n = form.get("top_n") or "25"

            lookback_days = form.get("lookback_days") or "180"
            try:
                lookback_days = max(7, min(730, int(lookback_days)))
            except Exception:
                lookback_days = 180

            min_transfer_minutes = form.get("min_transfer_minutes") or "150"
            try:
                min_transfer_minutes = max(60, min(600, int(min_transfer_minutes)))
            except Exception:
                min_transfer_minutes = 150
            try:
                top_n = max(1, min(200, int(top_n)))
            except Exception:
                top_n = 25

            if not bases or not hubs or not targets:
                flash("Please select at least one Base, one Hub, and one Target destination.", "warning")
                return render_template("index.html", **defaults, form=form)

            try:
                results = planner.suggest_itineraries(
                    lookback_days=lookback_days,
                    min_transfer_minutes=min_transfer_minutes,
                    start_date=start_date,
                    end_date=end_date,
                    bases=bases,
                    hubs=hubs,
                    targets=targets,
                    require_return_to_base=require_return_to_base,
                    top_n=top_n,
                )
            except Exception as e:
                flash(f"Error: {e}", "danger")
                return render_template("index.html", **defaults, form=form)

            return render_template(
                "results.html",
                results=results,
                lookback_days=lookback_days,
                min_transfer_minutes=min_transfer_minutes,
                start_date=start_date,
                end_date=end_date,
                bases=bases,
                hubs=hubs,
                targets=targets,
                require_return_to_base=require_return_to_base,
                top_n=top_n,
                data_dir=planner.data_dir,
                total_runs=planner.last_run_count,
            )

        return render_template("index.html", **defaults, form=None)

    @app.route("/health")
    def health():
        return {"status": "ok", "data_dir": planner.data_dir, "files": planner.file_count}

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)

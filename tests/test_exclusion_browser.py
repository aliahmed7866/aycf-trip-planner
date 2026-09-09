"""Real Chromium checks, run by the dedicated browser-tests CI job."""
import os
from pathlib import Path
import threading

import pandas as pd
import pytest
from flask import Flask, request
from werkzeug.serving import make_server

playwright = pytest.importorskip("playwright.sync_api")

from scan_scope import load_scope, save_scope
import scan_settings


@pytest.fixture
def settings_server(tmp_path, monkeypatch):
    monkeypatch.setenv("AYCF_CONFIG_DIR", str(tmp_path / "config"))
    monkeypatch.setattr(scan_settings, "scan_scope_with_preferences", lambda scope: dict(
        scope, preferred_destinations=["Kutaisi"], watch_routes=[("London Luton", "Budapest")]))
    save_scope(["Liverpool", "London Luton", "London Gatwick"], "all", [], ["Budapest", "Rome"],
               excluded_airports=["KUT"], excluded_countries=[], excluded_routes=[])
    root = Path(__file__).resolve().parents[1]
    app = Flask(__name__, template_folder=str(root / "templates"), static_folder=str(root / "static"))
    app.secret_key = "local-browser-test-only"
    app.add_url_rule("/", "index", lambda: "planner")
    app.add_url_rule("/flights", "all_flights", lambda: "flights")
    app.jinja_env.globals["csrf_token"] = lambda: "browser-test"
    pairs = [("London", "Budapest"), ("Budapest", "Kutaisi"), ("Liverpool", "Rome")]
    frame = pd.DataFrame([{"availability_start": "2026-09-09T00:00:00", "availability_end": "2026-09-10T23:59:59"}])
    app.register_blueprint(scan_settings.create_scan_settings_blueprint(
        lambda: (frame, pairs, [], [], ""), lambda: request.form.get("csrf_token") == "browser-test"))
    server = make_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/settings/scan-exclusions"
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.mark.parametrize("width", [390, 1280])
def test_country_airport_route_save_and_restore(settings_server, width):
    with playwright.sync_playwright() as runtime:
        browser = runtime.chromium.launch()
        page = browser.new_page(viewport={"width": width, "height": 844})
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        try:
            page.goto(settings_server, wait_until="domcontentloaded")
            expect = playwright.expect
            expect(page.locator("#exclusion-preview")).to_contain_text("2 days")
            page.locator("#exclusion-search").fill("KUT")
            expect(page.locator('[data-airport-row]:visible')).to_have_count(1)
            kutaisi = page.locator('[name="excluded_airports"][value="Kutaisi"]')
            expect(kutaisi).to_be_checked()
            page.locator("#show-excluded-only").check()
            expect(page.locator("#exclusion-dirty")).to_have_text("Showing saved settings.")
            page.locator("#show-excluded-only").uncheck()
            kutaisi.uncheck()
            page.locator("#exclusion-search").fill("Italy")
            italy = page.locator('[name="excluded_countries"][value="Italy"]')
            italy.check()
            expect(page.locator('[data-airport-row]').filter(has_text="Rome")).to_contain_text("Excluded by country")
            expect(page.locator("#exclusion-preview")).to_contain_text("2 days")
            page.locator("#save-exclusions").click()
            expect(page.locator("#exclusion-dirty")).to_have_text("Showing saved settings.")
            expect(italy).to_be_checked()
            assert load_scope()["excluded_countries"] == ["Italy"]
            assert load_scope()["excluded_airports"] == []
            page.get_by_text("Individual routes", exact=False).first.click()
            page.locator("#route-exclusion-search").fill("LTN Budapest")
            expect(page.locator('[data-route-row]:visible')).to_have_count(1)
            page.locator('[data-route-row]:visible input').check()
            expect(page.locator("#exclusion-conflicts")).to_contain_text("1 watch(es) paused")
            page.locator("#save-exclusions").click()
            expect(page.locator("#exclusion-dirty")).to_have_text("Showing saved settings.")
            assert len(load_scope()["excluded_routes"]) == 1
            # Capture the real responsive layout, including inherited states.
            artifacts = Path(os.environ.get("BROWSER_ARTIFACT_DIR", "/tmp/aycf-browser"))
            artifacts.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(artifacts / f"exclusions-{width}.png"), full_page=True)
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            page.locator("#clear-exclusions").click()
            page.locator("#save-exclusions").click()
            expect(page.locator("#exclusion-dirty")).to_have_text("Showing saved settings.")
            assert all(not load_scope()[key] for key in ("excluded_airports", "excluded_countries", "excluded_routes"))
            assert not errors
        finally:
            browser.close()


def test_stale_save_preserves_unsaved_choices_in_browser(settings_server):
    with playwright.sync_playwright() as runtime:
        browser = runtime.chromium.launch()
        page = browser.new_page()
        try:
            page.goto(settings_server, wait_until="domcontentloaded")
            page.locator("#exclusion-search").fill("Italy")
            italy = page.locator('[name="excluded_countries"][value="Italy"]')
            italy.check()
            # Another tab changes the saved exclusions after this form loaded.
            save_scope(["Liverpool"], "all", [], [], excluded_countries=["Georgia"])
            page.locator("#save-exclusions").click()
            playwright.expect(page.locator("#exclusion-save-error")).to_contain_text("another tab")
            playwright.expect(italy).to_be_checked()
            playwright.expect(page.locator("#save-exclusions")).to_be_enabled()
            assert load_scope()["excluded_countries"] == ["Georgia"]
        finally:
            browser.close()

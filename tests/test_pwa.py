import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _assert_manifest(path):
    manifest = json.loads(path.read_text(encoding="utf-8"))
    assert manifest["display"] == "standalone"
    assert manifest["start_url"] == "/"
    assert manifest["scope"] == "/"
    assert {"192x192", "512x512"} <= {icon["sizes"] for icon in manifest["icons"]}
    assert {"192x192", "512x512"} <= {icon["sizes"] for icon in manifest["icons"] if icon["type"] == "image/png"}
    return manifest


def test_aycf_pwa_installability_contract():
    manifest = _assert_manifest(ROOT / "static" / "manifest.webmanifest")
    assert any(shortcut["url"] == "/flights" for shortcut in manifest["shortcuts"])
    for icon in manifest["icons"]:
        assert (ROOT / icon["src"].lstrip("/")).is_file()

    template = (ROOT / "templates" / "base.html").read_text(encoding="utf-8")
    assert "manifest.webmanifest" in template
    assert "pwa.js" in template

    app_source = (ROOT / "app.py").read_text(encoding="utf-8")
    assert '@app.get("/flights")' in app_source
    assert '"/service-worker.js"' in app_source
    assert '"Service-Worker-Allowed"' in app_source


def test_admin_hub_pwa_installability_contract():
    manifest = _assert_manifest(ROOT / "termux" / "static" / "admin-manifest.webmanifest")
    assert manifest["id"] == "/admin-hub-app"
    for icon in manifest["icons"]:
        relative = icon["src"].removeprefix("/static/")
        assert (ROOT / "termux" / "static" / relative).is_file()

    source = (ROOT / "termux" / "admin_hub.py").read_text(encoding="utf-8")
    assert "admin-manifest.webmanifest" in source
    assert "admin-pwa.js" in source
    assert '"/service-worker.js"' in source
    assert '"Service-Worker-Allowed"' in source


def test_private_navigation_is_not_cached():
    for worker in [
        ROOT / "static" / "service-worker.js",
        ROOT / "termux" / "static" / "admin-service-worker.js",
    ]:
        source = worker.read_text(encoding="utf-8")
        assert 'event.request.mode==="navigate"' in source
        assert "fetch(event.request).catch" in source


def test_install_helper_waits_for_active_worker():
    scripts = [ROOT / "static" / "pwa.js"]
    admin_script = ROOT / "termux" / "static" / "admin-pwa.js"
    if admin_script.exists():
        scripts.append(admin_script)
    for script in scripts:
        source = script.read_text(encoding="utf-8")
        assert "navigator.serviceWorker.ready" in source
        assert "navigator.serviceWorker.controller" in source
        assert "beforeinstallprompt" in source
        assert "Add to Home screen" not in source

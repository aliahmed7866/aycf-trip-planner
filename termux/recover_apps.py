"""Restart opted-in local apps after Termux starts or Android reaps a process."""
import subprocess

if __package__:
    from . import admin_hub as hub
else:
    import admin_hub as hub


def recover():
    for app in hub._load_registry():
        marker = hub.update_path(app["id"]).with_suffix(".on")
        if app["id"] not in hub.RECOVERABLE or not marker.exists():
            continue
        try:
            with hub.runtime_action(app["id"]):
                if (marker.exists() and not hub._off_file(app).exists()
                        and hub.app_status(app)["state"] == "stopped"):
                    hub.change_power(app, "start")
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            print(f"{app['name']}: {exc}", flush=True)


if __name__ == "__main__":
    recover()

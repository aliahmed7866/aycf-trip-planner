import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "termux" / "auto-refresh-wizz.sh"


class AutoRefreshWizzShellTests(unittest.TestCase):
    def test_login_recovery_runs_after_refresh_returns_login_required(self):
        """Regression: refresh RC=4 must reach auto_login instead of exiting via set -e."""
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            fakebin = td / "bin"
            fakebin.mkdir()
            log = td / "calls.log"
            state = td / "refresh-count"

            python = fakebin / "python"
            python.write_text(
                textwrap.dedent(
                    f"""\
                    #!/usr/bin/env bash
                    echo "python:$1" >> {log!s}
                    case "$1" in
                      */repair_wizz_runtime.py) exit 0 ;;
                      */refresh_wizz_direct.py) exit 14 ;;
                      */refresh_wizz_from_chrome.py)
                        n=0
                        [ -f {state!s} ] && n=$(cat {state!s})
                        n=$((n+1))
                        echo "$n" > {state!s}
                        if [ "$n" -eq 1 ]; then exit 4; fi
                        exit 0
                        ;;
                      */auto_login_wizz.py) exit 0 ;;
                      *) exit 0 ;;
                    esac
                    """
                ),
                encoding="utf-8",
            )
            python.chmod(0o755)

            adb = fakebin / "adb"
            adb.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    if [ "$1" = "devices" ]; then
                      printf 'List of devices attached\\nphone:5555\\tdevice\\n'
                    fi
                    exit 0
                    """
                ),
                encoding="utf-8",
            )
            adb.chmod(0o755)

            curl = fakebin / "curl"
            curl.write_text(
                "#!/usr/bin/env bash\nprintf '%s\\n' '{\"webSocketDebuggerUrl\":\"ws://browser\"}'\n",
                encoding="utf-8",
            )
            curl.chmod(0o755)

            env = os.environ.copy()
            env["PATH"] = f"{fakebin}:{env['PATH']}"
            env["AYCF_APP_DIR"] = str(ROOT)
            env["AYCF_CONFIG_DIR"] = str(td / "config")
            env["HOME"] = str(td / "home")

            proc = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=20,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout)
            calls = log.read_text(encoding="utf-8")
            self.assertIn("auto_login_wizz.py", calls)
            self.assertEqual(calls.count("refresh_wizz_from_chrome.py"), 2)


if __name__ == "__main__":
    unittest.main()

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class TermuxRuntimeEnvTests(unittest.TestCase):
    def test_direct_runtime_import_loads_termux_env(self):
        root = Path(__file__).resolve().parent.parent
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            config = home / ".config" / "aycf"
            config.mkdir(parents=True)
            (config / "env").write_text(
                "export AYCF_SESSION_ENCRYPTION_KEY='test-key-from-termux-env'\n"
                "export AYCF_RUNTIME_ENV_TEST='loaded'\n",
                encoding="utf-8",
            )
            env = os.environ.copy()
            env["HOME"] = str(home)
            env.pop("AYCF_SESSION_ENCRYPTION_KEY", None)
            env.pop("AYCF_RUNTIME_ENV_TEST", None)
            env.pop("AYCF_CONFIG_DIR", None)
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "import os; import termux.runtime; "
                    "print(os.environ.get('AYCF_RUNTIME_ENV_TEST')); "
                    "print(os.environ.get('AYCF_SESSION_ENCRYPTION_KEY'))",
                ],
                cwd=root,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.splitlines(), ["loaded", "test-key-from-termux-env"])


if __name__ == "__main__":
    unittest.main()

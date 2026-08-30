import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from cryptography.fernet import Fernet


class TermuxRuntimeEnvTests(unittest.TestCase):
    def test_direct_runtime_import_loads_private_env_before_vault_use(self):
        root = Path(__file__).resolve().parent.parent
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            config = home / ".config" / "aycf"
            config.mkdir(parents=True)
            encryption_key = Fernet.generate_key().decode()
            (config / "env").write_text(
                f"export AYCF_SESSION_ENCRYPTION_KEY='{encryption_key}'\n"
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
                    "import os,sys; import termux.runtime; "
                    "from session_vault import SessionVault; SessionVault(); "
                    "print(os.environ.get('AYCF_RUNTIME_ENV_TEST')); "
                    "print('vault-ok'); "
                    "print('browser-loader-imported=' + str('termux.import_wizz_from_chrome' in sys.modules))",
                ],
                cwd=root,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                result.stdout.splitlines(),
                ["loaded", "vault-ok", "browser-loader-imported=False"],
            )

    def test_explicit_process_env_is_not_overwritten(self):
        root = Path(__file__).resolve().parent.parent
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            config = home / ".config" / "aycf"
            config.mkdir(parents=True)
            (config / "env").write_text(
                "export AYCF_RUNTIME_ENV_TEST='from-file'\n",
                encoding="utf-8",
            )
            env = os.environ.copy()
            env["HOME"] = str(home)
            env["AYCF_RUNTIME_ENV_TEST"] = "explicit"
            env.pop("AYCF_CONFIG_DIR", None)
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "import os; from termux.env_loader import load_termux_env; "
                    "load_termux_env(); print(os.environ['AYCF_RUNTIME_ENV_TEST'])",
                ],
                cwd=root,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "explicit")


if __name__ == "__main__":
    unittest.main()

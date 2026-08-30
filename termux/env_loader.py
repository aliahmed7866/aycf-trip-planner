"""Load the private Termux AYCF environment for direct Python entrypoints."""

import os
from pathlib import Path


def termux_env_file() -> Path:
    """Return the active AYCF env file without freezing config paths at import time."""
    default_config = Path.home() / ".config" / "aycf"
    config_dir = Path(os.environ.get("AYCF_CONFIG_DIR", str(default_config))).expanduser()
    return config_dir / "env"


def load_termux_env(path: Path | None = None) -> Path | None:
    """Load simple ``export KEY=VALUE`` entries while preserving explicit env vars.

    Shell launchers source the same file. Direct Python commands do not, so this
    lightweight helper keeps web, morning, status, repair and Wizz tools on the
    same private configuration without importing browser/CDP dependencies.
    """
    env_file = Path(path).expanduser() if path is not None else termux_env_file()
    if not env_file.exists():
        return None

    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or not line.startswith("export ") or "=" not in line:
            continue
        key, value = line[7:].split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = os.path.expandvars(os.path.expanduser(value))
    return env_file

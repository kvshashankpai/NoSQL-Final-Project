"""Load config/project_config.json and resolve repository-relative paths."""

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config" / "project_config.json"


def load_dotenv(path=REPO_ROOT / ".env"):
    """Load KEY=VALUE lines from .env into the environment (existing variables win).

    .env is git-ignored; see .env.example for the supported settings.
    """
    import os
    path = Path(path)
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def load_config(path=CONFIG_PATH):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def repo_path(value):
    """Resolve a path from the config or the command line against the repo root."""
    p = Path(value)
    return p if p.is_absolute() else REPO_ROOT / p


def default_input_files(config=None):
    config = config or load_config()
    return [repo_path(p) for p in config["dataset"]["input_files"]]


def default_batch_size(config=None):
    config = config or load_config()
    return int(config["batching"]["default_batch_size"])


def adapter_config(name, config=None):
    config = config or load_config()
    for adapter in config["pipelines"]["adapters"]:
        if adapter["name"].lower() == name.lower():
            return adapter
    names = ", ".join(a["name"] for a in config["pipelines"]["adapters"])
    raise ValueError(f"Unknown pipeline {name!r}. Choose one of: {names}")

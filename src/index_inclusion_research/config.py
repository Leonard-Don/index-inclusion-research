from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def default_config_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "markets.yml"


def load_project_config(path: str | Path | None = None) -> dict[str, Any]:
    config_path = Path(path) if path else default_config_path()
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)

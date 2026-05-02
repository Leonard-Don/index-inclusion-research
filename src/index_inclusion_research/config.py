from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from index_inclusion_research import paths


def default_config_path() -> Path:
    return paths.config_path()


def load_project_config(path: str | Path | None = None) -> dict[str, Any]:
    config_path = Path(path) if path else default_config_path()
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)

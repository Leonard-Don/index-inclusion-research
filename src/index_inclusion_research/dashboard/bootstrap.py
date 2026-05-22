from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DashboardPaths:
    root: Path
    src: Path
    templates: Path
    static: Path


def bootstrap_dashboard_paths(current_file: str | Path) -> DashboardPaths:
    os.environ.setdefault("MPLBACKEND", "Agg")

    root = Path(current_file).resolve().parents[2]
    src = root / "src"
    web = src / "index_inclusion_research" / "web"
    templates = web / "templates"
    static = web / "static"

    if str(src) not in sys.path:
        sys.path.insert(0, str(src))

    return DashboardPaths(
        root=root,
        src=src,
        templates=templates,
        static=static,
    )

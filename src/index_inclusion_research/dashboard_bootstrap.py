from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DashboardPaths:
    root: Path
    scripts: Path
    src: Path
    templates: Path
    static: Path


def bootstrap_dashboard_paths(current_file: str | Path) -> DashboardPaths:
    os.environ.setdefault("MPLBACKEND", "Agg")

    root = Path(current_file).resolve().parents[2]
    scripts = root / "scripts"
    src = root / "src"
    templates = scripts / "templates"
    static = scripts / "static"

    for path in (scripts, src):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))

    return DashboardPaths(
        root=root,
        scripts=scripts,
        src=src,
        templates=templates,
        static=static,
    )

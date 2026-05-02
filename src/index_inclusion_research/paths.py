"""Centralized project path resolution.

All modules should call helpers in this module rather than computing
``Path(__file__).resolve().parents[N]`` inline. The single seam lets
deployments override the project root via the ``INDEX_INCLUSION_ROOT``
environment variable (useful for containers, CI workspaces, or testing
against a copy of the repo).

Usage:

    from index_inclusion_research import paths

    verdicts_csv = paths.real_tables_dir() / "cma_hypothesis_verdicts.csv"
"""

from __future__ import annotations

import os
from pathlib import Path

_PACKAGE_ROOT = Path(__file__).resolve().parents[2]


def project_root() -> Path:
    """Return the project root.

    Honors ``INDEX_INCLUSION_ROOT``; otherwise resolves relative to this
    module's location (``<repo>/src/index_inclusion_research/paths.py``
    → ``<repo>``).
    """
    override = os.environ.get("INDEX_INCLUSION_ROOT")
    if override:
        return Path(override).resolve()
    return _PACKAGE_ROOT


def results_dir() -> Path:
    return project_root() / "results"


def real_tables_dir() -> Path:
    return results_dir() / "real_tables"


def real_figures_dir() -> Path:
    return results_dir() / "real_figures"


def literature_results_dir() -> Path:
    return results_dir() / "literature"


def data_dir() -> Path:
    return project_root() / "data"


def raw_data_dir() -> Path:
    return data_dir() / "raw"


def processed_data_dir() -> Path:
    return data_dir() / "processed"


def config_path() -> Path:
    return project_root() / "config" / "markets.yml"


def docs_dir() -> Path:
    return project_root() / "docs"

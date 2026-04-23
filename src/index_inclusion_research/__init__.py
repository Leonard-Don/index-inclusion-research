"""Utilities for index-inclusion event studies."""

from .config import load_project_config
from .literature import (
    compute_did_summary,
    compute_retention_summary,
    summarise_mechanism_changes,
)
from .literature_catalog import (
    build_grouped_literature_frame,
    build_literature_catalog_frame,
    build_literature_dashboard_frame,
    build_literature_review_markdown,
    build_literature_summary_frame,
    build_literature_summary_markdown,
    build_project_track_frame,
    build_project_track_markdown,
    get_literature_paper,
    list_literature_papers,
)
from .results_snapshot import ResultsSnapshot

__all__ = [
    "ResultsSnapshot",
    "build_grouped_literature_frame",
    "build_literature_catalog_frame",
    "build_literature_dashboard_frame",
    "build_literature_review_markdown",
    "build_literature_summary_frame",
    "build_literature_summary_markdown",
    "build_project_track_frame",
    "build_project_track_markdown",
    "compute_did_summary",
    "compute_retention_summary",
    "get_literature_paper",
    "load_project_config",
    "list_literature_papers",
    "summarise_mechanism_changes",
]

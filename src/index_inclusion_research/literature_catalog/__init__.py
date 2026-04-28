"""Literature library: 16 indexing-effect papers + camp / track grouping.

The package was carved out of a 983-line ``literature_catalog.py`` so the
static registry, the DataFrame builders, and the markdown blocks each
live in their own file. Internal modules (``_data``, ``_frames``,
``_markdown``) are private — go through this facade.

Public API (stable; consumers continue importing from this package
exactly as before the split):

- :data:`PAPER_LIBRARY` — the immutable tuple of :class:`LiteraturePaper`.
- :data:`DEEP_ANALYSIS` — per-paper identification / contribution map.
- :data:`CAMP_LABELS`, :data:`TRACK_LABELS` — display config dicts.
- :class:`LiteraturePaper` — frozen dataclass for one paper.
- :func:`list_literature_papers`, :func:`get_literature_paper` — lookup.
- ``build_*_frame`` — dashboard DataFrame builders.
- ``build_*_markdown`` — markdown blocks for the literature pages.
"""

from __future__ import annotations

from ._data import (
    CAMP_LABELS,
    DEEP_ANALYSIS,
    PAPER_LIBRARY,
    PDF_ROOT,
    TRACK_LABELS,
    LiteraturePaper,
    get_literature_paper,
    list_literature_papers,
)
from ._frames import (
    build_camp_summary_frame,
    build_grouped_literature_frame,
    build_literature_catalog_frame,
    build_literature_dashboard_frame,
    build_literature_evolution_frame,
    build_literature_meeting_frame,
    build_literature_summary_frame,
    build_project_track_frame,
    build_project_track_support_records,
)
from ._markdown import (
    build_literature_framework_markdown,
    build_literature_review_markdown,
    build_literature_summary_markdown,
    build_project_track_markdown,
)

__all__ = [
    "CAMP_LABELS",
    "DEEP_ANALYSIS",
    "LiteraturePaper",
    "PAPER_LIBRARY",
    "PDF_ROOT",
    "TRACK_LABELS",
    "build_camp_summary_frame",
    "build_grouped_literature_frame",
    "build_literature_catalog_frame",
    "build_literature_dashboard_frame",
    "build_literature_evolution_frame",
    "build_literature_framework_markdown",
    "build_literature_meeting_frame",
    "build_literature_review_markdown",
    "build_literature_summary_frame",
    "build_literature_summary_markdown",
    "build_project_track_frame",
    "build_project_track_markdown",
    "build_project_track_support_records",
    "get_literature_paper",
    "list_literature_papers",
]

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from index_inclusion_research.dashboard_page_outline_runtime import DashboardPageOutlineRuntime
from index_inclusion_research.dashboard_page_runtime import DashboardPageRuntime
from index_inclusion_research.dashboard_page_sections_runtime import DashboardPageSectionsRuntime


def test_dashboard_page_runtime_composes_outline_and_sections(monkeypatch) -> None:
    page = DashboardPageRuntime(SimpleNamespace(root=Path("/tmp")))
    expected_highlights = [{"label": "demo", "copy": "ok"}]

    monkeypatch.setattr(page.outline, "build_highlights", lambda: expected_highlights)
    monkeypatch.setattr(
        page.sections,
        "build_sample_design_section",
        lambda demo_mode=False: {"id": "design", "demo_mode": demo_mode},
    )

    assert isinstance(page.outline, DashboardPageOutlineRuntime)
    assert isinstance(page.sections, DashboardPageSectionsRuntime)
    assert page.build_highlights() == expected_highlights
    assert page.build_sample_design_section(True) == {"id": "design", "demo_mode": True}

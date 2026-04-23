from __future__ import annotations

from pathlib import Path

import pytest

from index_inclusion_research import dashboard_config
from index_inclusion_research.dashboard_page_outline_runtime import (
    DashboardPageOutlineRuntime,
)
from index_inclusion_research.dashboard_page_sections_runtime import (
    DashboardPageSectionsRuntime,
)
from index_inclusion_research.dashboard_runtime import DashboardRuntime
from index_inclusion_research.dashboard_track_runtime import DashboardTrackRuntime


def _build_runtime() -> DashboardRuntime:
    analyses = dashboard_config.build_analyses(
        run_price_pressure_track=lambda verbose=False: {},
        run_demand_curve_track=lambda verbose=False: {},
        run_identification_china_track=lambda verbose=False: {},
    )
    return DashboardRuntime(
        root=Path("/tmp"),
        analyses=analyses,
        library_card=dashboard_config.LIBRARY_CARD,
        review_card=dashboard_config.REVIEW_CARD,
        framework_card=dashboard_config.FRAMEWORK_CARD,
        supplement_card=dashboard_config.SUPPLEMENT_CARD,
        project_module_display_map=dashboard_config.build_project_module_display(analyses),
    )


def test_dashboard_runtime_delegates_track_and_page_surfaces() -> None:
    runtime = _build_runtime()

    assert runtime.run_cache is runtime.track.run_cache
    assert runtime.table_labels["event_counts"] == "真实事件样本表"
    assert runtime.nav_sections_for_mode("demo")[0]["anchor"] == "overview"

    tabs = runtime.mode_tabs_for_mode(
        "demo",
        lambda mode, anchor=None: f"/?mode={mode}" + (f"#{anchor}" if anchor else ""),
    )
    assert len(tabs) == 3
    assert any(tab["active"] for tab in tabs)


def test_dashboard_runtime_exposes_explicit_facade_methods(monkeypatch) -> None:
    runtime = _build_runtime()
    expected_highlights = [{"label": "demo", "copy": "ok"}]
    monkeypatch.setattr(runtime.page, "build_highlights", lambda: expected_highlights)

    assert "__getattr__" not in DashboardRuntime.__dict__
    assert isinstance(runtime.track, DashboardTrackRuntime)
    assert isinstance(runtime.page.outline, DashboardPageOutlineRuntime)
    assert isinstance(runtime.page.sections, DashboardPageSectionsRuntime)
    assert runtime.column_labels is runtime.track.column_labels
    assert runtime.value_labels is runtime.track.value_labels
    assert runtime.load_literature_library_result()["id"] == "paper_library"
    assert runtime.load_literature_framework_result()["id"] == "paper_framework"
    assert runtime.load_supplement_result()["id"] == "project_supplement"
    assert runtime.track.build_figure_caption(Path("/tmp/sample_event_timeline.png"), prefix="图").startswith("图：")
    assert runtime.build_highlights() == expected_highlights


def test_dashboard_runtime_missing_attribute_raises_attribute_error() -> None:
    runtime = _build_runtime()

    with pytest.raises(AttributeError):
        getattr(runtime, "prepare_track_display")  # noqa: B009 - explicitly probing for attribute existence

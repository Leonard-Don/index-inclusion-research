from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from index_inclusion_research import dashboard_home
from index_inclusion_research import dashboard_presenters


class DashboardPageOutlineRuntime:
    def __init__(self, *, root: Path) -> None:
        self.root = root

    def nav_sections_for_mode(self, mode: str) -> list[dict[str, str]]:
        return dashboard_presenters.nav_sections_for_mode(mode)

    def available_hashes_for_mode(self, mode: str) -> list[str]:
        return dashboard_presenters.available_hashes_for_mode(mode)

    def mode_tabs_for_mode(
        self,
        mode: str,
        url_builder: Callable[..., str],
    ) -> list[dict[str, object]]:
        return dashboard_presenters.mode_tabs_for_mode(mode, url_builder)

    def build_track_notes(self, analysis_id: str) -> list[dict[str, str]]:
        return dashboard_presenters.track_notes_for_analysis(analysis_id)

    def build_overview_metrics(self) -> list[dict[str, str]]:
        return dashboard_home.build_overview_metrics(self.root)

    def build_overview_notes_for_mode(self, mode: str) -> list[dict[str, str]]:
        return dashboard_presenters.overview_notes_for_mode(mode)

    def build_overview_summary_for_mode(self, mode: str) -> str:
        return dashboard_presenters.overview_summary_for_mode(mode)

    def build_cta_copy_for_mode(self, mode: str) -> str:
        return dashboard_presenters.cta_copy_for_mode(mode)

    def build_abstract_lead(self) -> str:
        return dashboard_presenters.abstract_lead()

    def build_abstract_points(self) -> list[dict[str, str]]:
        return dashboard_presenters.abstract_points()

    def build_highlights(self) -> list[dict[str, str]]:
        return dashboard_home.build_highlights(self.root)

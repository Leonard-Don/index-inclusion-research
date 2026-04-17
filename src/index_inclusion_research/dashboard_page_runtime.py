from __future__ import annotations

from collections.abc import Callable

from index_inclusion_research.dashboard_page_outline_runtime import DashboardPageOutlineRuntime
from index_inclusion_research.dashboard_page_sections_runtime import DashboardPageSectionsRuntime


class DashboardPageRuntime:
    def __init__(self, track_runtime) -> None:
        self.track = track_runtime
        self.outline = DashboardPageOutlineRuntime(root=track_runtime.root)
        self.sections = DashboardPageSectionsRuntime(
            track_runtime=track_runtime,
            outline=self.outline,
        )

    def nav_sections_for_mode(self, mode: str) -> list[dict[str, str]]:
        return self.outline.nav_sections_for_mode(mode)

    def available_hashes_for_mode(self, mode: str) -> list[str]:
        return self.outline.available_hashes_for_mode(mode)

    def mode_tabs_for_mode(
        self,
        mode: str,
        url_builder: Callable[..., str],
    ) -> list[dict[str, object]]:
        return self.outline.mode_tabs_for_mode(mode, url_builder)

    def build_track_notes(self, analysis_id: str) -> list[dict[str, str]]:
        return self.outline.build_track_notes(analysis_id)

    def build_overview_metrics(self) -> list[dict[str, str]]:
        return self.outline.build_overview_metrics()

    def build_overview_notes_for_mode(self, mode: str) -> list[dict[str, str]]:
        return self.outline.build_overview_notes_for_mode(mode)

    def build_overview_summary_for_mode(self, mode: str) -> str:
        return self.outline.build_overview_summary_for_mode(mode)

    def build_cta_copy_for_mode(self, mode: str) -> str:
        return self.outline.build_cta_copy_for_mode(mode)

    def build_abstract_lead(self) -> str:
        return self.outline.build_abstract_lead()

    def build_abstract_points(self) -> list[dict[str, str]]:
        return self.outline.build_abstract_points()

    def build_highlights(self) -> list[dict[str, str]]:
        return self.outline.build_highlights()

    def create_sample_design_figures(self) -> list[dict[str, str]]:
        return self.sections.create_sample_design_figures()

    def build_sample_design_section(self, demo_mode: bool = False) -> dict[str, object]:
        return self.sections.build_sample_design_section(demo_mode)

    def build_robustness_section(self) -> dict[str, object]:
        return self.sections.build_robustness_section()

    def build_limits_section(self) -> dict[str, object]:
        return self.sections.build_limits_section()

    def build_home_context(
        self,
        *,
        display_mode: str,
        current_open_panels: str | None,
        mode_tabs_for_mode,
        refresh_status_payload,
        refresh_status_url: str,
    ) -> dict[str, object]:
        return self.sections.build_home_context(
            display_mode=display_mode,
            current_open_panels=current_open_panels,
            mode_tabs_for_mode=mode_tabs_for_mode,
            refresh_status_payload=refresh_status_payload,
            refresh_status_url=refresh_status_url,
        )

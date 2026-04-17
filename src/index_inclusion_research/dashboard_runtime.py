from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

from index_inclusion_research.dashboard_page_runtime import DashboardPageRuntime
from index_inclusion_research.dashboard_track_runtime import DashboardTrackRuntime
from index_inclusion_research.dashboard_types import (
    AnalysesConfig,
    AnalysisResult,
    DashboardCard,
    ModeTab,
    PaperDetailResult,
)


class DashboardRuntime:
    def __init__(
        self,
        *,
        root: Path,
        analyses: AnalysesConfig | Mapping[str, Mapping[str, object]],
        library_card: DashboardCard | Mapping[str, str],
        review_card: DashboardCard | Mapping[str, str],
        framework_card: DashboardCard | Mapping[str, str],
        supplement_card: DashboardCard | Mapping[str, str],
        project_module_display_map: Mapping[str, str],
    ) -> None:
        self.track = DashboardTrackRuntime(
            root=root,
            analyses=analyses,
            library_card=library_card,
            review_card=review_card,
            framework_card=framework_card,
            supplement_card=supplement_card,
            project_module_display_map=project_module_display_map,
        )
        self.page = DashboardPageRuntime(self.track)

    @property
    def run_cache(self) -> dict[str, dict[str, object]]:
        return self.track.run_cache

    @property
    def table_labels(self) -> dict[str, str]:
        return self.track.table_labels

    @property
    def column_labels(self) -> dict[str, str]:
        return self.track.column_labels

    @property
    def value_labels(self) -> dict[str, str]:
        return self.track.value_labels

    def build_dashboard_snapshot_meta(self, snapshot_files: list[Path] | None = None) -> dict[str, object]:
        return self.track.build_dashboard_snapshot_meta(snapshot_files)

    def load_identification_china_saved_result(self) -> AnalysisResult:
        return self.track.load_identification_china_saved_result()

    def load_rdd_status(self, output_dir: Path | None = None) -> dict[str, object]:
        return self.track.load_rdd_status(output_dir)

    def load_literature_library_result(self) -> AnalysisResult:
        return self.track.load_literature_library_result()

    def load_literature_review_result(self) -> AnalysisResult:
        return self.track.load_literature_review_result()

    def load_literature_framework_result(self) -> AnalysisResult:
        return self.track.load_literature_framework_result()

    def load_paper_detail_result(self, paper_id: str) -> PaperDetailResult | None:
        return self.track.load_paper_detail_result(paper_id)

    def load_supplement_result(self) -> AnalysisResult:
        return self.track.load_supplement_result()

    def run_and_cache_all(self) -> None:
        self.track.run_and_cache_all()

    def run_and_cache_analysis(self, analysis_id: str) -> AnalysisResult:
        return self.track.run_and_cache_analysis(analysis_id)

    def nav_sections_for_mode(self, mode: str) -> list[dict[str, str]]:
        return self.page.nav_sections_for_mode(mode)

    def mode_tabs_for_mode(
        self,
        mode: str,
        url_builder: Callable[..., str],
    ) -> list[ModeTab]:
        return self.page.mode_tabs_for_mode(mode, url_builder)

    def build_highlights(self) -> list[dict[str, str]]:
        return self.page.build_highlights()

    def build_home_context(
        self,
        *,
        display_mode: str,
        current_open_panels: str | None,
        mode_tabs_for_mode,
        refresh_status_payload,
        refresh_status_url: str,
    ) -> dict[str, object]:
        return self.page.build_home_context(
            display_mode=display_mode,
            current_open_panels=current_open_panels,
            mode_tabs_for_mode=mode_tabs_for_mode,
            refresh_status_payload=refresh_status_payload,
            refresh_status_url=refresh_status_url,
        )

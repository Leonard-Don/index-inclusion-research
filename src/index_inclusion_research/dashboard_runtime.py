from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from index_inclusion_research.dashboard_page_runtime import DashboardPageRuntime
from index_inclusion_research.dashboard_track_runtime import DashboardTrackRuntime
from index_inclusion_research.dashboard_types import (
    AnalysisCache,
    AnalysesConfig,
    DashboardCard,
    FrameworkResult,
    HighlightItem,
    HomeContext,
    ModeTab,
    ModeTabUrlBuilder,
    ModeName,
    ModeTabsBuilder,
    NavSection,
    PaperDetailResult,
    RefreshStatusPayloadBuilder,
    RddContractCheck,
    RddStatus,
    SnapshotMeta,
    SupplementResult,
    TrackResult,
)


class DashboardRuntime:
    def __init__(
        self,
        *,
        root: Path,
        analyses: AnalysesConfig,
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
    def run_cache(self) -> AnalysisCache:
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

    def dashboard_snapshot_sources(self) -> list[Path]:
        return self.track.dashboard_snapshot_sources()

    def safe_relative(self, path: Path) -> str:
        return self.track.safe_relative(path)

    def build_dashboard_snapshot_meta(self, snapshot_files: list[Path] | None = None) -> SnapshotMeta:
        return self.track.build_dashboard_snapshot_meta(snapshot_files)

    def load_identification_china_saved_result(self) -> TrackResult:
        return self.track.load_identification_china_saved_result()

    def load_rdd_status(self, output_dir: Path | None = None) -> RddStatus:
        return self.track.load_rdd_status(output_dir)

    def load_rdd_contract_check(
        self,
        output_dir: Path | None = None,
        manifest_path: Path | None = None,
        rdd_status: RddStatus | None = None,
    ) -> RddContractCheck:
        return self.track.load_rdd_contract_check(
            output_dir=output_dir,
            manifest_path=manifest_path,
            rdd_status=rdd_status,
        )

    def load_literature_library_result(self) -> TrackResult:
        return self.track.load_literature_library_result()

    def load_literature_review_result(self) -> TrackResult:
        return self.track.load_literature_review_result()

    def load_literature_framework_result(self) -> FrameworkResult:
        return self.track.load_literature_framework_result()

    def load_paper_detail_result(self, paper_id: str) -> PaperDetailResult | None:
        return self.track.load_paper_detail_result(paper_id)

    def load_supplement_result(self) -> SupplementResult:
        return self.track.load_supplement_result()

    def run_and_cache_all(self) -> None:
        self.track.run_and_cache_all()

    def run_and_cache_analysis(self, analysis_id: str) -> TrackResult:
        return self.track.run_and_cache_analysis(analysis_id)

    def nav_sections_for_mode(self, mode: ModeName) -> list[NavSection]:
        return self.page.nav_sections_for_mode(mode)

    def mode_tabs_for_mode(
        self,
        mode: ModeName,
        url_builder: ModeTabUrlBuilder,
    ) -> list[ModeTab]:
        return self.page.mode_tabs_for_mode(mode, url_builder)

    def build_highlights(self) -> list[HighlightItem]:
        return self.page.build_highlights()

    def build_home_context(
        self,
        *,
        display_mode: ModeName,
        current_open_panels: str | None,
        mode_tabs_for_mode: ModeTabsBuilder,
        refresh_status_payload: RefreshStatusPayloadBuilder,
        refresh_status_url: str,
    ) -> HomeContext:
        return self.page.build_home_context(
            display_mode=display_mode,
            current_open_panels=current_open_panels,
            mode_tabs_for_mode=mode_tabs_for_mode,
            refresh_status_payload=refresh_status_payload,
            refresh_status_url=refresh_status_url,
        )

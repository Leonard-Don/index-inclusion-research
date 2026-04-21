from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from index_inclusion_research import dashboard_figures
from index_inclusion_research import dashboard_metrics
from index_inclusion_research import dashboard_presenters
from index_inclusion_research import dashboard_tracks
from index_inclusion_research.dashboard_cache import AnalysisCacheStore
from index_inclusion_research.dashboard_types import (
    AnalysisCache,
    AnalysesConfig,
    CacheEntry,
    FigureEntry,
    SecondarySection,
    TrackDisplaySection,
    TrackResult,
)

from index_inclusion_research.dashboard_track_content_runtime import DashboardTrackContentRuntime
from index_inclusion_research.dashboard_track_support_runtime import DashboardTrackSupportRuntime


class DashboardTrackDisplayRuntime:
    def __init__(
        self,
        *,
        root: Path,
        analyses: AnalysesConfig,
        run_cache: AnalysisCache,
        support: DashboardTrackSupportRuntime,
        content: DashboardTrackContentRuntime,
    ) -> None:
        self.root = root
        self.analyses = analyses
        self.run_cache = run_cache
        self.support = support
        self.content = content

    def _snapshot_run_cache(self) -> dict[str, CacheEntry]:
        if isinstance(self.run_cache, AnalysisCacheStore):
            return self.run_cache.snapshot()
        return dict(self.run_cache)

    def _replace_run_cache(self, next_cache: Mapping[str, CacheEntry]) -> None:
        if isinstance(self.run_cache, AnalysisCacheStore):
            self.run_cache.replace_all(next_cache)
            return
        self.run_cache.clear()
        self.run_cache.update(dict(next_cache))

    def create_price_pressure_figures(self) -> list[FigureEntry]:
        return dashboard_figures.create_price_pressure_figures(
            self.root,
            to_relative=self.support.safe_relative,
        )

    def create_identification_figures(self) -> list[FigureEntry]:
        return dashboard_figures.create_identification_figures(
            self.root,
            load_rdd_status=self.content.load_rdd_status,
            to_relative=self.support.safe_relative,
        )

    def run_and_cache_all(self) -> None:
        staged_cache = self._snapshot_run_cache()

        def _run_and_cache_analysis(analysis_id: str) -> TrackResult:
            return dashboard_tracks.run_and_cache_analysis(
                analysis_id,
                analyses=self.analyses,
                run_cache=staged_cache,
                normalize_result=self.content.normalize_result,
                attach_project_track_context=self.content.attach_project_track_context,
            )

        dashboard_tracks.run_and_cache_all(
            analyses=self.analyses,
            run_cache=staged_cache,
            run_and_cache_analysis=_run_and_cache_analysis,
            load_literature_library_result=self.content.load_literature_library_result,
            load_literature_review_result=self.content.load_literature_review_result,
            load_literature_framework_result=self.content.load_literature_framework_result,
            load_supplement_result=self.content.load_supplement_result,
        )
        self._replace_run_cache(staged_cache)

    def run_and_cache_analysis(self, analysis_id: str) -> TrackResult:
        staged_cache = self._snapshot_run_cache()
        current = dashboard_tracks.run_and_cache_analysis(
            analysis_id,
            analyses=self.analyses,
            run_cache=staged_cache,
            normalize_result=self.content.normalize_result,
            attach_project_track_context=self.content.attach_project_track_context,
        )
        self._replace_run_cache(staged_cache)
        return current

    def load_or_build_track_section(self, analysis_id: str) -> TrackResult:
        return dashboard_tracks.load_or_build_track_section(
            analysis_id,
            analyses=self.analyses,
            run_cache=self.run_cache,
            load_saved_track_result=self.content.load_saved_track_result,
            normalize_result=self.content.normalize_result,
            attach_project_track_context=self.content.attach_project_track_context,
        )

    def prepare_track_display(
        self,
        section: TrackDisplaySection,
        analysis_id: str,
        demo_mode: bool,
    ) -> TrackDisplaySection:
        return dashboard_tracks.prepare_track_display(
            self.root,
            section,
            analysis_id,
            demo_mode,
            load_rdd_status=self.content.load_rdd_status,
            load_rdd_contract_check=self.content.load_rdd_contract_check,
            clean_display_text=self.support.clean_display_text,
            render_table=self.support.render_table,
            format_pct=self.support.format_pct,
            format_p_value=self.support.format_p_value,
            create_price_pressure_figures=self.create_price_pressure_figures,
            create_identification_figures=self.create_identification_figures,
        )

    def prepare_framework_display(self, section: SecondarySection, demo_mode: bool) -> SecondarySection:
        del demo_mode
        return dashboard_presenters.prepare_framework_display(
            section,
            summary_cards=dashboard_metrics.build_framework_summary_cards(),
        )

    def prepare_supplement_display(self, section: SecondarySection, demo_mode: bool) -> SecondarySection:
        del demo_mode
        return dashboard_presenters.prepare_supplement_display(
            section,
            summary_cards=dashboard_metrics.build_supplement_summary_cards(),
        )

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from index_inclusion_research import (
    dashboard_figures,
    dashboard_home,
    dashboard_sections,
)
from index_inclusion_research.analysis.cross_market_asymmetry import (
    dashboard_section as cma_dashboard,
)
from index_inclusion_research.analysis.cross_market_asymmetry import (
    orchestrator as cma_orchestrator,
)
from index_inclusion_research.dashboard_cache import AnalysisCacheStore
from index_inclusion_research.dashboard_page_outline_runtime import (
    DashboardPageOutlineRuntime,
)
from index_inclusion_research.dashboard_types import (
    AnalysisCache,
    DashboardSection,
    FigureEntry,
    HomeContext,
    ModeName,
    ModeTabsBuilder,
    RefreshStatusPayloadBuilder,
    RobustnessSection,
    TrackResult,
)


class DashboardPageSectionsRuntime:
    def __init__(self, *, track_runtime, outline: DashboardPageOutlineRuntime) -> None:
        self.track = track_runtime
        self.outline = outline

    def create_sample_design_figures(self) -> list[FigureEntry]:
        return dashboard_figures.create_sample_design_figures(
            self.track.root,
            to_relative=self.track.safe_relative,
            format_p_value=self.track.format_p_value,
            format_share=self.track.format_share,
        )

    def build_sample_design_section(self, demo_mode: bool = False) -> DashboardSection:
        return dashboard_sections.build_sample_design_section(
            self.track.root,
            demo_mode=demo_mode,
            render_table=self.track.render_table,
            attach_display_tiers=self.track.attach_display_tiers,
            split_items_by_tier=self.track.split_items_by_tier,
            create_sample_design_figures=self.create_sample_design_figures,
            format_share=self.track.format_share,
            format_p_value=self.track.format_p_value,
            value_labels=self.track.value_labels,
        )

    def build_robustness_section(self) -> RobustnessSection:
        return dashboard_sections.build_robustness_section(
            self.track.root,
            read_csv_if_exists=self.track.read_csv_if_exists,
            render_table=self.track.render_table,
            attach_display_tiers=self.track.attach_display_tiers,
            split_items_by_tier=self.track.split_items_by_tier,
            format_share=self.track.format_share,
            format_pct=self.track.format_pct,
        )

    def build_limits_section(self) -> DashboardSection:
        return dashboard_sections.build_limits_section(
            self.track.root,
            apply_live_rdd_status_to_identification_scope=self.track.apply_live_rdd_status_to_identification_scope,
            render_table=self.track.render_table,
            attach_display_tiers=self.track.attach_display_tiers,
            split_items_by_tier=self.track.split_items_by_tier,
            format_share=self.track.format_share,
        )

    def build_paper_audit_section(self) -> DashboardSection:
        return dashboard_sections.build_paper_audit_section(
            self.track.root,
            render_table=self.track.render_table,
            attach_display_tiers=self.track.attach_display_tiers,
            split_items_by_tier=self.track.split_items_by_tier,
        )

    def build_cross_market_section(self, mode: ModeName = "full") -> dict[str, Any]:
        from pathlib import Path

        tables_dir = self.track.root / cma_orchestrator.REAL_TABLES_DIR.relative_to(
            cma_orchestrator.ROOT
        )
        figures_dir = self.track.root / cma_orchestrator.REAL_FIGURES_DIR.relative_to(
            cma_orchestrator.ROOT
        )
        section = cma_dashboard.build_cross_market_section(
            tables_dir=tables_dir,
            figures_dir=figures_dir,
            mode=mode,
        )
        figures = section.get("figures", {})
        if not isinstance(figures, Mapping):
            figures = {}
        relative_figures = {
            name: self.track.safe_relative(Path(abs_path))
            for name, abs_path in figures.items()
        }
        section["figures"] = relative_figures
        return section

    def build_home_context(
        self,
        *,
        display_mode: ModeName,
        current_open_panels: str | None,
        mode_tabs_for_mode: ModeTabsBuilder,
        refresh_status_payload: RefreshStatusPayloadBuilder,
        refresh_status_url: str,
    ) -> HomeContext:
        run_cache_snapshot: AnalysisCache
        if isinstance(self.track.run_cache, AnalysisCacheStore):
            run_cache_snapshot = self.track.run_cache.snapshot()
        else:
            run_cache_snapshot = dict(self.track.run_cache)

        def _load_or_build_track_section(analysis_id: str) -> TrackResult:
            current = run_cache_snapshot.get(analysis_id)
            if current is not None:
                return current  # type: ignore[return-value]
            current = self.track.load_or_build_track_section(analysis_id)
            run_cache_snapshot[analysis_id] = current
            return current

        return dashboard_home.DashboardHomeContextBuilder(
            root=self.track.root,
            analyses=self.track.analyses,
            run_cache=run_cache_snapshot,
            nav_sections_for_mode=self.outline.nav_sections_for_mode,
            mode_tabs_for_mode=mode_tabs_for_mode,
            build_dashboard_snapshot_meta=self.track.build_dashboard_snapshot_meta,
            refresh_status_payload=refresh_status_payload,
            overview_notes_for_mode=self.outline.build_overview_notes_for_mode,
            overview_summary_for_mode=self.outline.build_overview_summary_for_mode,
            cta_copy_for_mode=self.outline.build_cta_copy_for_mode,
            abstract_lead=self.outline.build_abstract_lead,
            abstract_points=self.outline.build_abstract_points,
            load_or_build_track_section=_load_or_build_track_section,
            build_track_notes=self.outline.build_track_notes,
            prepare_track_display=self.track.prepare_track_display,
            load_literature_framework_result=self.track.load_literature_framework_result,
            prepare_framework_display=self.track.prepare_framework_display,
            load_supplement_result=self.track.load_supplement_result,
            prepare_supplement_display=self.track.prepare_supplement_display,
            build_sample_design_section=self.build_sample_design_section,
            build_robustness_section=self.build_robustness_section,
            build_limits_section=self.build_limits_section,
            build_paper_audit_section=self.build_paper_audit_section,
            build_cross_market_section=self.build_cross_market_section,
            write_cache=self.track.run_cache,
        ).build(
            display_mode=display_mode,
            current_open_panels=current_open_panels,
            refresh_status_url=refresh_status_url,
        )

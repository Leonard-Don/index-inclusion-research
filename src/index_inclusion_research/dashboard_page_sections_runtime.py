from __future__ import annotations

from index_inclusion_research import dashboard_figures
from index_inclusion_research import dashboard_home
from index_inclusion_research import dashboard_sections
from index_inclusion_research.dashboard_page_outline_runtime import DashboardPageOutlineRuntime


class DashboardPageSectionsRuntime:
    def __init__(self, *, track_runtime, outline: DashboardPageOutlineRuntime) -> None:
        self.track = track_runtime
        self.outline = outline

    def create_sample_design_figures(self) -> list[dict[str, str]]:
        return dashboard_figures.create_sample_design_figures(
            self.track.root,
            to_relative=self.track.support.safe_relative,
            format_p_value=self.track.support.format_p_value,
            format_share=self.track.support.format_share,
        )

    def build_sample_design_section(self, demo_mode: bool = False) -> dict[str, object]:
        return dashboard_sections.build_sample_design_section(
            self.track.root,
            demo_mode=demo_mode,
            render_table=self.track.support.render_table,
            attach_display_tiers=self.track.support.attach_display_tiers,
            split_items_by_tier=self.track.support.split_items_by_tier,
            create_sample_design_figures=self.create_sample_design_figures,
            format_share=self.track.support.format_share,
            format_p_value=self.track.support.format_p_value,
            value_labels=self.track.support.value_labels,
        )

    def build_robustness_section(self) -> dict[str, object]:
        return dashboard_sections.build_robustness_section(
            self.track.root,
            read_csv_if_exists=self.track.content.read_csv_if_exists,
            render_table=self.track.support.render_table,
            attach_display_tiers=self.track.support.attach_display_tiers,
            split_items_by_tier=self.track.support.split_items_by_tier,
            format_share=self.track.support.format_share,
            format_pct=self.track.support.format_pct,
        )

    def build_limits_section(self) -> dict[str, object]:
        return dashboard_sections.build_limits_section(
            self.track.root,
            apply_live_rdd_status_to_identification_scope=self.track.content.apply_live_rdd_status_to_identification_scope,
            render_table=self.track.support.render_table,
            attach_display_tiers=self.track.support.attach_display_tiers,
            split_items_by_tier=self.track.support.split_items_by_tier,
            format_share=self.track.support.format_share,
        )

    def build_home_context(
        self,
        *,
        display_mode: str,
        current_open_panels: str | None,
        mode_tabs_for_mode,
        refresh_status_payload,
        refresh_status_url: str,
    ) -> dict[str, object]:
        return dashboard_home.DashboardHomeContextBuilder(
            root=self.track.root,
            analyses=self.track.analyses,
            run_cache=self.track.run_cache,
            nav_sections_for_mode=self.outline.nav_sections_for_mode,
            mode_tabs_for_mode=mode_tabs_for_mode,
            build_dashboard_snapshot_meta=self.track.support.build_dashboard_snapshot_meta,
            refresh_status_payload=refresh_status_payload,
            overview_notes_for_mode=self.outline.build_overview_notes_for_mode,
            overview_summary_for_mode=self.outline.build_overview_summary_for_mode,
            cta_copy_for_mode=self.outline.build_cta_copy_for_mode,
            abstract_lead=self.outline.build_abstract_lead,
            abstract_points=self.outline.build_abstract_points,
            load_or_build_track_section=self.track.display.load_or_build_track_section,
            build_track_notes=self.outline.build_track_notes,
            prepare_track_display=self.track.display.prepare_track_display,
            load_literature_framework_result=self.track.content.load_literature_framework_result,
            prepare_framework_display=self.track.display.prepare_framework_display,
            load_supplement_result=self.track.content.load_supplement_result,
            prepare_supplement_display=self.track.display.prepare_supplement_display,
            build_sample_design_section=self.build_sample_design_section,
            build_robustness_section=self.build_robustness_section,
            build_limits_section=self.build_limits_section,
        ).build(
            display_mode=display_mode,
            current_open_panels=current_open_panels,
            refresh_status_url=refresh_status_url,
        )

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from index_inclusion_research.results_snapshot import ResultsSnapshot, require_first_row
from index_inclusion_research.dashboard_types import (
    AbstractLeadBuilder,
    AbstractPointsBuilder,
    AnalysisCache,
    AnalysesConfig,
    CtaCopyBuilder,
    DashboardSectionBuilder,
    DemoModeSectionBuilder,
    HighlightItem,
    HomeContext,
    ModeName,
    ModeTabsBuilder,
    NavSectionsBuilder,
    OverviewNotesBuilder,
    OverviewMetric,
    OverviewSummaryBuilder,
    RefreshStatusPayloadBuilder,
    RobustnessSection,
    RobustnessSectionBuilder,
    SecondarySection,
    SecondarySectionLoader,
    SecondarySectionPreparer,
    SnapshotMetaBuilder,
    TrackDisplayPreparer,
    TrackDisplaySection,
    TrackNotesBuilder,
    TrackSectionLoader,
)


def build_overview_metrics(root: Path, *, snapshot: ResultsSnapshot | None = None) -> list[OverviewMetric]:
    current_snapshot = snapshot or ResultsSnapshot(root)
    event_counts = current_snapshot.csv("results", "real_tables", "event_counts.csv")
    total_events = int(event_counts["n_events"].sum())
    return [
        {"value": "16", "label": "篇核心文献，构成理论基础"},
        {"value": "3", "label": "条研究主线，对应主要实证模块"},
        {"value": "5", "label": "个研究阵营，构成文献演进框架"},
        {"value": str(total_events), "label": "个真实调入/调出事件，构成默认样本"},
    ]


def build_highlights(root: Path, *, snapshot: ResultsSnapshot | None = None) -> list[HighlightItem]:
    current_snapshot = snapshot or ResultsSnapshot(root)
    summary = current_snapshot.csv("results", "real_tables", "event_study_summary.csv")
    asymmetry = current_snapshot.csv("results", "real_tables", "asymmetry_summary.csv")
    us_announce = require_first_row(
        summary.loc[
            (summary["market"] == "US")
            & (summary["event_phase"] == "announce")
            & (summary["window_slug"] == "m1_p1")
            & (summary["inclusion"] == 1)
        ],
        context="US announce CAR[-1,+1]",
    )
    cn_effective = require_first_row(
        summary.loc[
            (summary["market"] == "CN")
            & (summary["event_phase"] == "effective")
            & (summary["window_slug"] == "m1_p1")
            & (summary["inclusion"] == 1)
        ],
        context="CN effective CAR[-1,+1]",
    )
    cn_effective_asymmetry = require_first_row(
        asymmetry.loc[
            (asymmetry["market"] == "CN") & (asymmetry["event_phase"] == "effective")
        ],
        context="CN effective asymmetry",
    )
    if float(cn_effective["p_value"]) < 0.05:
        cn_discussion = (
            f"中国 A 股在生效日 CAR[-1,+1] 平均值为 {cn_effective['mean_car']:.2%}，"
            f"且统计显著。这说明 A 股市场不能机械套用美股的经典指数纳入叙事。"
        )
    else:
        cn_discussion = (
            f"中国 A 股在生效日 CAR[-1,+1] 平均值为 {cn_effective['mean_car']:.2%}，"
            f"但统计上并不显著；更值得关注的是 [0,+120] 窗口下调入与调出的 CAR 差异达到 "
            f"{cn_effective_asymmetry['asymmetry_car_p0_p120']:.2%}。"
            "这说明中国市场的关键分化更多体现在生效后的长期路径，而不是简单复制美股的短期公告效应。"
        )
    return [
        {
            "label": "最强结论",
            "headline": "美股公告日仍然呈现最稳定的短期正向效应。",
            "copy": f"当前真实样本里，美国市场公告日 CAR[-1,+1] 平均值为 {us_announce['mean_car']:.2%}，p 值为 {us_announce['p_value']:.4f}，是整套结果里最稳的短期正向证据。",
        },
        {
            "label": "最值得讨论",
            "headline": "A 股生效日并不简单重复美股叙事。",
            "copy": cn_discussion,
        },
        {
            "label": "方法含义",
            "headline": "研究价值不仅在于涨跌，更在于识别。",
            "copy": "事件研究说明现象，匹配回归帮助控制样本差异，RDD 提供更严格的识别框架。将三者并置展示，有助于更清楚地讨论结论的可信度。",
        },
    ]


@dataclass
class DashboardHomeContextBuilder:
    root: Path
    analyses: AnalysesConfig
    run_cache: AnalysisCache
    nav_sections_for_mode: NavSectionsBuilder
    mode_tabs_for_mode: ModeTabsBuilder
    build_dashboard_snapshot_meta: SnapshotMetaBuilder
    refresh_status_payload: RefreshStatusPayloadBuilder
    overview_notes_for_mode: OverviewNotesBuilder
    overview_summary_for_mode: OverviewSummaryBuilder
    cta_copy_for_mode: CtaCopyBuilder
    abstract_lead: AbstractLeadBuilder
    abstract_points: AbstractPointsBuilder
    load_or_build_track_section: TrackSectionLoader
    build_track_notes: TrackNotesBuilder
    prepare_track_display: TrackDisplayPreparer
    load_literature_framework_result: SecondarySectionLoader
    prepare_framework_display: SecondarySectionPreparer
    load_supplement_result: SecondarySectionLoader
    prepare_supplement_display: SecondarySectionPreparer
    build_sample_design_section: DemoModeSectionBuilder
    build_robustness_section: RobustnessSectionBuilder
    build_limits_section: DashboardSectionBuilder

    @staticmethod
    def _empty_secondary_section() -> SecondarySection:
        return {"display_summary": "", "display_tables": [], "summary_cards": []}

    @staticmethod
    def _empty_robustness_section() -> RobustnessSection:
        return {"summary": "", "summary_cards": [], "tables": []}

    def _build_track_sections(self, *, demo_mode: bool) -> list[TrackDisplaySection]:
        track_sections: list[TrackDisplaySection] = []
        for analysis_id in self.analyses:
            section: TrackDisplaySection = dict(self.load_or_build_track_section(analysis_id))
            section["anchor"] = analysis_id
            section["notes"] = self.build_track_notes(analysis_id)
            section = self.prepare_track_display(section, analysis_id, demo_mode)
            track_sections.append(section)
        return track_sections

    def _build_cached_section(
        self,
        *,
        cache_key: str,
        loader: SecondarySectionLoader,
        preparer: SecondarySectionPreparer,
        demo_mode: bool,
    ) -> SecondarySection:
        section = self.run_cache.get(cache_key) or loader()
        section = preparer(section, demo_mode)
        self.run_cache[cache_key] = section
        return section

    def _build_secondary_sections(
        self,
        *,
        display_mode: ModeName,
        demo_mode: bool,
    ) -> tuple[SecondarySection, SecondarySection]:
        if display_mode == "brief":
            return (
                self._empty_secondary_section(),
                self._empty_secondary_section(),
            )
        framework_section = self._build_cached_section(
            cache_key="paper_framework",
            loader=self.load_literature_framework_result,
            preparer=self.prepare_framework_display,
            demo_mode=demo_mode,
        )
        supplement_section = self._build_cached_section(
            cache_key="project_supplement",
            loader=self.load_supplement_result,
            preparer=self.prepare_supplement_display,
            demo_mode=demo_mode,
        )
        return framework_section, supplement_section

    def build(
        self,
        *,
        display_mode: ModeName,
        current_open_panels: str | None,
        refresh_status_url: str,
    ) -> HomeContext:
        demo_mode = display_mode != "full"
        snapshot = ResultsSnapshot(self.root)
        framework_section, supplement_section = self._build_secondary_sections(
            display_mode=display_mode,
            demo_mode=demo_mode,
        )
        return {
            "mode": display_mode,
            "nav_sections": self.nav_sections_for_mode(display_mode),
            "mode_tabs": self.mode_tabs_for_mode(display_mode, current_open_panels),
            "snapshot_meta": self.build_dashboard_snapshot_meta(),
            "refresh_meta": self.refresh_status_payload(display_mode, "overview", current_open_panels),
            "refresh_status_url": refresh_status_url,
            "current_open_panels": current_open_panels,
            "overview_metrics": build_overview_metrics(self.root, snapshot=snapshot),
            "overview_notes": self.overview_notes_for_mode(display_mode),
            "overview_summary": self.overview_summary_for_mode(display_mode),
            "cta_copy": self.cta_copy_for_mode(display_mode),
            "abstract_lead": self.abstract_lead(),
            "abstract_points": self.abstract_points(),
            "highlights": build_highlights(self.root, snapshot=snapshot),
            "design_section": self.build_sample_design_section(display_mode == "demo"),
            "track_sections": self._build_track_sections(demo_mode=demo_mode),
            "framework_section": framework_section,
            "supplement_section": supplement_section,
            "robustness_section": (
                self.build_robustness_section()
                if display_mode == "full"
                else self._empty_robustness_section()
            ),
            "limits_section": self.build_limits_section(),
        }


def build_home_context(
    *,
    root: Path,
    display_mode: ModeName,
    current_open_panels: str | None,
    analyses: AnalysesConfig,
    run_cache: AnalysisCache,
    nav_sections_for_mode: NavSectionsBuilder,
    mode_tabs_for_mode: ModeTabsBuilder,
    build_dashboard_snapshot_meta: SnapshotMetaBuilder,
    refresh_status_payload: RefreshStatusPayloadBuilder,
    overview_notes_for_mode: OverviewNotesBuilder,
    overview_summary_for_mode: OverviewSummaryBuilder,
    cta_copy_for_mode: CtaCopyBuilder,
    abstract_lead: AbstractLeadBuilder,
    abstract_points: AbstractPointsBuilder,
    load_or_build_track_section: TrackSectionLoader,
    build_track_notes: TrackNotesBuilder,
    prepare_track_display: TrackDisplayPreparer,
    load_literature_framework_result: SecondarySectionLoader,
    prepare_framework_display: SecondarySectionPreparer,
    load_supplement_result: SecondarySectionLoader,
    prepare_supplement_display: SecondarySectionPreparer,
    build_sample_design_section: DemoModeSectionBuilder,
    build_robustness_section: RobustnessSectionBuilder,
    build_limits_section: DashboardSectionBuilder,
    refresh_status_url: str,
) -> HomeContext:
    return DashboardHomeContextBuilder(
        root=root,
        analyses=analyses,
        run_cache=run_cache,
        nav_sections_for_mode=nav_sections_for_mode,
        mode_tabs_for_mode=mode_tabs_for_mode,
        build_dashboard_snapshot_meta=build_dashboard_snapshot_meta,
        refresh_status_payload=refresh_status_payload,
        overview_notes_for_mode=overview_notes_for_mode,
        overview_summary_for_mode=overview_summary_for_mode,
        cta_copy_for_mode=cta_copy_for_mode,
        abstract_lead=abstract_lead,
        abstract_points=abstract_points,
        load_or_build_track_section=load_or_build_track_section,
        build_track_notes=build_track_notes,
        prepare_track_display=prepare_track_display,
        load_literature_framework_result=load_literature_framework_result,
        prepare_framework_display=prepare_framework_display,
        load_supplement_result=load_supplement_result,
        prepare_supplement_display=prepare_supplement_display,
        build_sample_design_section=build_sample_design_section,
        build_robustness_section=build_robustness_section,
        build_limits_section=build_limits_section,
    ).build(
        display_mode=display_mode,
        current_open_panels=current_open_panels,
        refresh_status_url=refresh_status_url,
    )

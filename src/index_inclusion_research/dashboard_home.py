from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from index_inclusion_research import dashboard_loaders
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
    RddStatus,
)


def _overview_rdd_metric(rdd_status: RddStatus) -> OverviewMetric:
    if rdd_status["mode"] == "real":
        return {"value": "L3", "label": "中国 RDD 已进入正式边界样本", "tone": "official"}
    if rdd_status["mode"] == "reconstructed":
        return {"value": "L2", "label": "中国 RDD 当前为公开重建样本", "tone": "reconstructed"}
    if rdd_status["mode"] == "demo":
        return {"value": "L1", "label": "中国 RDD 当前仅为方法展示", "tone": "demo"}
    return {"value": "L0", "label": "中国 RDD 仍待补正式样本", "tone": "missing"}


def build_overview_metrics(
    root: Path,
    *,
    snapshot: ResultsSnapshot | None = None,
    rdd_status: RddStatus | None = None,
) -> list[OverviewMetric]:
    current_snapshot = snapshot or ResultsSnapshot(root)
    event_counts = current_snapshot.csv("results", "real_tables", "event_counts.csv")
    total_events = int(event_counts["n_events"].sum())
    current_rdd_status = dict(rdd_status) if rdd_status is not None else dashboard_loaders.load_rdd_status(root)
    return [
        {"value": "16", "label": "篇核心文献，构成理论基础"},
        {"value": "3", "label": "条研究主线，对应主要实证模块"},
        {"value": "5", "label": "个研究阵营，构成文献演进框架"},
        {"value": str(total_events), "label": "个真实调入/调出事件，构成默认样本"},
        _overview_rdd_metric(current_rdd_status),
    ]


def build_highlights(
    root: Path,
    *,
    snapshot: ResultsSnapshot | None = None,
    rdd_status: RddStatus | None = None,
) -> list[HighlightItem]:
    current_snapshot = snapshot or ResultsSnapshot(root)
    summary = current_snapshot.csv("results", "real_tables", "event_study_summary.csv")
    asymmetry = current_snapshot.csv("results", "real_tables", "asymmetry_summary.csv")
    current_rdd_status = dict(rdd_status) if rdd_status is not None else dashboard_loaders.load_rdd_status(root)
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
    if current_rdd_status["mode"] == "real":
        method_headline = "中国 RDD 已进入正式边界样本口径。"
        method_copy = (
            "当前首页里的识别层已经不是纯方法框架。正式候选样本通过校验后，"
            "RDD 可以和事件研究、匹配回归并列进入主结论，作为更强识别证据。"
        )
    elif current_rdd_status["mode"] == "reconstructed":
        method_headline = "中国 RDD 已进入公开数据版证据链。"
        method_copy = (
            "当前首页展示的不再只是方法框架。公开重建样本已经能支撑一版可读的边界识别结果，"
            "但必须明确标注为公开重建口径，不能写成中证官方历史候选排名表。"
        )
    elif current_rdd_status["mode"] == "demo":
        method_headline = "中国 RDD 当前仍停留在方法展示层。"
        method_copy = (
            "事件研究说明现象，匹配回归帮助控制样本差异；RDD 当前只用于展示识别结构、"
            "字段契约和运行链路，还没有进入正式证据链。"
        )
    else:
        method_headline = "中国 RDD 当前仍待补正式样本。"
        method_copy = (
            "事件研究说明现象，匹配回归帮助控制样本差异；RDD 识别框架已经搭好，"
            "但还需要正式候选样本或公开重建样本，才能进入可读的边界证据。"
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
            "headline": method_headline,
            "copy": method_copy,
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

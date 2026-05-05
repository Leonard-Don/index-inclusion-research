from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from index_inclusion_research import dashboard_loaders
from index_inclusion_research.dashboard_types import (
    AbstractLeadBuilder,
    AbstractPointsBuilder,
    AnalysesConfig,
    AnalysisCache,
    CtaCopyBuilder,
    DashboardSectionBuilder,
    DemoModeSectionBuilder,
    HighlightItem,
    HomeContext,
    ModeName,
    ModeTabsBuilder,
    NavSectionsBuilder,
    OverviewMetric,
    OverviewNotesBuilder,
    OverviewSummaryBuilder,
    PapMeta,
    RddStatus,
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
from index_inclusion_research.rdd_evidence import (
    rdd_evidence_tier,
    rdd_provenance_summary,
)
from index_inclusion_research.results_snapshot import ResultsSnapshot, require_first_row


def dashboard_asset_version(root: Path) -> str:
    static_dir = root / "src" / "index_inclusion_research" / "web" / "static"
    asset_paths = [
        static_dir / "dashboard.css",
        static_dir / "dashboard.js",
        static_dir / "dashboard" / "interactive_charts.js",
        static_dir / "dashboard" / "echarts_theme.js",
        static_dir / "dashboard" / "echarts_test_stub.js",
    ]
    mtimes: list[float] = []
    for path in asset_paths:
        try:
            mtimes.append(path.stat().st_mtime)
        except OSError:
            continue
    return str(int(max(mtimes))) if mtimes else "dev"


def _overview_rdd_metric(rdd_status: RddStatus) -> OverviewMetric:
    meta = rdd_provenance_summary(rdd_status)
    if rdd_status["mode"] == "real":
        return {
            "value": str(rdd_status.get("evidence_tier", ""))
            or rdd_evidence_tier(rdd_status["mode"]),
            "label": "中国 RDD 已进入正式边界样本",
            "tone": "official",
            "meta": meta,
        }
    if rdd_status["mode"] == "reconstructed":
        return {
            "value": str(rdd_status.get("evidence_tier", ""))
            or rdd_evidence_tier(rdd_status["mode"]),
            "label": "中国 RDD 当前为公开重建样本",
            "tone": "reconstructed",
            "meta": meta,
        }
    if rdd_status["mode"] == "demo":
        return {
            "value": str(rdd_status.get("evidence_tier", ""))
            or rdd_evidence_tier(rdd_status["mode"]),
            "label": "中国 RDD 当前仅为方法展示",
            "tone": "demo",
            "meta": meta,
        }
    return {
        "value": str(rdd_status.get("evidence_tier", ""))
        or rdd_evidence_tier(rdd_status["mode"]),
        "label": "中国 RDD 仍待补正式样本",
        "tone": "missing",
        "meta": meta,
    }


def build_overview_metrics(
    root: Path,
    *,
    snapshot: ResultsSnapshot | None = None,
    rdd_status: RddStatus | None = None,
) -> list[OverviewMetric]:
    current_snapshot = snapshot or ResultsSnapshot(root)
    event_counts = current_snapshot.csv("results", "real_tables", "event_counts.csv")
    total_events = int(event_counts["n_events"].sum())
    current_rdd_status = rdd_status if rdd_status is not None else dashboard_loaders.load_rdd_status(root)
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
    current_rdd_status = rdd_status if rdd_status is not None else dashboard_loaders.load_rdd_status(root)
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
            "且统计显著，说明 A 股不能机械套用美股的经典指数纳入叙事，更像一个制度摩擦更强的独立场景。"
        )
    else:
        cn_discussion = (
            f"中国 A 股在生效日 CAR[-1,+1] 平均值为 {cn_effective['mean_car']:.2%}，"
            f"但统计上并不显著；[0,+120] 窗口下调入与调出的 CAR 差异达到 "
            f"{cn_effective_asymmetry['asymmetry_car_p0_p120']:.2%}，"
            "说明关键分化更多出现在生效后的长期路径，也更符合套利更慢、制度摩擦更强的市场图景。"
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
    provenance_summary = rdd_provenance_summary(current_rdd_status)
    method_copy = f"{method_copy} 这也对应新增文献把争论从“涨不涨”推进到“制度差异、套利约束和价格发现如何改变结论”。"
    if provenance_summary:
        method_copy = f"{method_copy} 当前来源为 {provenance_summary}。"
    return [
        {
            "label": "最强结论",
            "headline": "美股公告日仍有稳定短期正向效应，但更像被压缩后的公开信号。",
            "copy": f"美国公告日 CAR[-1,+1] 均值为 {us_announce['mean_car']:.2%}，p 值为 {us_announce['p_value']:.4f}，仍是整套结果里最稳的短期正向证据；更合理的解释是短期冲击仍在，但可见 alpha 已被更成熟的提前交易显著压缩。",
        },
        {
            "label": "最值得讨论",
            "headline": "A 股生效阶段更像独立制度场景，而不是美股镜像。",
            "copy": cn_discussion,
        },
        {
            "label": "方法含义",
            "headline": method_headline,
            "copy": method_copy.replace("当前首页展示的不再只是方法框架。", "")
            .replace("当前来源为 ", "来源：")
            .strip(),
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
    build_cross_market_section: Callable[[ModeName], dict[str, Any]] | None = None
    write_cache: AnalysisCache | None = None

    def build_pap_meta(self) -> PapMeta:
        from index_inclusion_research.dashboard_loaders import load_pap_summary

        return cast(PapMeta, load_pap_summary(self.root))

    @staticmethod
    def _empty_secondary_section() -> SecondarySection:
        return {"display_summary": "", "display_tables": [], "summary_cards": []}

    @staticmethod
    def _empty_robustness_section() -> RobustnessSection:
        return {"summary": "", "summary_cards": [], "tables": []}

    def _build_track_sections(self, *, demo_mode: bool) -> list[TrackDisplaySection]:
        track_sections: list[TrackDisplaySection] = []
        for analysis_id in self.analyses:
            section = cast(
                TrackDisplaySection,
                self.load_or_build_track_section(analysis_id).copy(),
            )
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
        cached = self.run_cache.get(cache_key)
        section = cast(SecondarySection, cached) if cached is not None else loader()
        section = preparer(section, demo_mode)
        self.run_cache[cache_key] = section
        if self.write_cache is not None and self.write_cache is not self.run_cache:
            self.write_cache[cache_key] = section
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
            "refresh_meta": self.refresh_status_payload(
                display_mode, "overview", current_open_panels
            ),
            "pap_meta": self.build_pap_meta(),
            "asset_version": dashboard_asset_version(self.root),
            "use_echarts_test_stub": os.environ.get("DASHBOARD_ECHARTS_TEST_STUB")
            == "1",
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
            "cma_section": (
                self.build_cross_market_section(display_mode)
                if self.build_cross_market_section is not None
                else None
            ),
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
    write_cache: AnalysisCache | None = None,
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
        write_cache=write_cache,
    ).build(
        display_mode=display_mode,
        current_open_panels=current_open_panels,
        refresh_status_url=refresh_status_url,
    )

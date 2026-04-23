from __future__ import annotations

from index_inclusion_research.dashboard_types import (
    DesignSectionView,
    SectionHeadView,
    TableDetailView,
    TablePrimaryView,
    TableSuiteSectionView,
    TrackMetaView,
    TrackSectionView,
)


def build_section_head_view(
    *,
    section_id: str,
    waypoint_label: str,
    kicker: str,
    title: str,
    intro: str,
    side_label: str,
) -> SectionHeadView:
    return {
        "section_id": section_id,
        "waypoint_label": waypoint_label,
        "kicker": kicker,
        "title": title,
        "intro": intro,
        "side_label": side_label,
    }


def build_table_primary_view(
    *,
    key: str,
    title: str,
    copy: str,
    container: str,
    collapsed_copy: str,
    toggle_label: str = "按需展开",
) -> TablePrimaryView:
    return {
        "key": key,
        "title": title,
        "copy": copy,
        "container": container,
        "collapsed_copy": collapsed_copy,
        "toggle_label": toggle_label,
    }


def build_table_detail_view(
    *,
    full_title: str,
    full_copy: str,
    demo_key: str,
    demo_title: str,
    demo_copy: str,
    container: str = "library-panels",
    kicker: str = "补充细表",
    toggle_label: str = "按需展开",
) -> TableDetailView:
    return {
        "full_title": full_title,
        "full_copy": full_copy,
        "demo_key": demo_key,
        "demo_title": demo_title,
        "demo_copy": demo_copy,
        "container": container,
        "kicker": kicker,
        "toggle_label": toggle_label,
    }


def build_table_suite_section_view(
    *,
    head: SectionHeadView,
    primary: TablePrimaryView,
    detail: TableDetailView,
    show_suite: bool = True,
) -> TableSuiteSectionView:
    return {
        "head": head,
        "show_suite": show_suite,
        "primary": primary,
        "detail": detail,
    }


def build_sample_design_view(
    *,
    detail_figures_count: int,
    detail_tables_count: int,
) -> DesignSectionView:
    return {
        "head": build_section_head_view(
            section_id="design",
            waypoint_label="样本与设计",
            kicker="样本与设计",
            title="先交代样本结构，再进入结果解释。",
            intro="先用样本覆盖、事件窗口和识别结构把地基讲清。",
            side_label="阅读焦点",
        ),
        "detail_figures_key": "demo-design-detail-figures",
        "detail_figures_title": f"样本设计补充图表（{detail_figures_count} 张）",
        "detail_figures_copy": "默认收起其余样本图表，按需展开。",
        "primary": build_table_primary_view(
            key="demo-design-primary-tables",
            title="核心摘要表",
            copy="这组表优先交代样本覆盖、跨市场对照与比较口径，帮助先建立研究背景。",
            container="library-panels",
            collapsed_copy="默认先显示样本范围总表，其余主表按需展开。",
        ),
        "detail": build_table_detail_view(
            full_title="补充细表",
            full_copy="这些表用于补充年份分布、数据来源与样本细节，适合在问答或写作时回查。",
            demo_key="demo-design-detail-tables",
            demo_title=f"样本设计补充表（{detail_tables_count} 张）",
            demo_copy="默认收起年份分布和来源细表，问答时再开。",
            container="library-panels",
        ),
        "brief_mode_hint": "3 分钟汇报只保留样本摘要；完整图表可切到展示版或完整材料。",
    }


def build_track_meta_view() -> TrackMetaView:
    return {
        "takeaway_label": "先看结论",
        "summary_label": "一句话背景",
        "refresh_button_label": "只刷新本主线",
        "refresh_running_label": "主线刷新中…",
        "refresh_help_copy": "单模块结果更新后，只刷新这条主线即可。",
        "surface_title": "阅读提示",
    }


def build_track_section_view(
    *,
    anchor: str,
    title: str,
    detail_tables_count: int,
    support_papers_count: int,
) -> TrackSectionView:
    return {
        "meta": build_track_meta_view(),
        "primary": build_table_primary_view(
            key=f"demo-{anchor}-primary-tables",
            title="核心摘要表",
            copy="优先阅读这些表，可以先抓住这条主线的结论方向与比较维度。",
            container="result-grid",
            collapsed_copy="默认先放一张主表，其余主表按需展开。",
        ),
        "detail": build_table_detail_view(
            full_title="补充细表",
            full_copy="这些表保留更细的变量拆解和辅助比较，用于支撑主结论。",
            demo_key=f"demo-{anchor}-detail-tables",
            demo_title=f"{title}补充细表（{detail_tables_count} 张）",
            demo_copy="展示版默认收起补充细表，避免主线被细节打断。",
            container="result-grid",
        ),
        "support_band_title": "支撑文献",
        "support_band_copy": "这一组文献不是简单并列引用，而是为当前主线分别提供问题意识、机制解释与识别支撑。",
        "support_demo_key": f"demo-{anchor}-support-papers",
        "support_demo_title": f"支撑文献（{support_papers_count} 篇）",
        "support_demo_copy": "默认收起支撑文献，追溯来源时再展开。",
        "support_demo_kicker": "文献支持",
        "support_demo_toggle_label": "展开文献",
    }

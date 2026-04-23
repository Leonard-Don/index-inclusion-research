from __future__ import annotations

from index_inclusion_research import dashboard_view_models


def test_build_table_suite_section_view_keeps_head_primary_and_detail() -> None:
    view = dashboard_view_models.build_table_suite_section_view(
        head=dashboard_view_models.build_section_head_view(
            section_id="limits",
            waypoint_label="研究边界",
            kicker="研究边界",
            title="标题",
            intro="说明",
            side_label="焦点",
        ),
        primary=dashboard_view_models.build_table_primary_view(
            key="demo-limits-primary-tables",
            title="核心摘要表",
            copy="说明",
            container="library-panels",
            collapsed_copy="折叠说明",
        ),
        detail=dashboard_view_models.build_table_detail_view(
            full_title="补充说明表",
            full_copy="完整说明",
            demo_key="demo-limits-detail-tables",
            demo_title="研究边界补充表（1 张）",
            demo_copy="折叠说明",
            kicker="补充说明",
        ),
    )

    assert view["head"]["section_id"] == "limits"
    assert view["show_suite"] is True
    assert view["primary"]["key"] == "demo-limits-primary-tables"
    assert view["detail"]["kicker"] == "补充说明"


def test_build_sample_design_view_uses_counts_in_demo_titles() -> None:
    view = dashboard_view_models.build_sample_design_view(
        detail_figures_count=2,
        detail_tables_count=3,
    )

    assert view["head"]["section_id"] == "design"
    assert view["detail_figures_title"] == "样本设计补充图表（2 张）"
    assert view["detail_figures_copy"] == "默认收起其余样本图表，按需展开。"
    assert view["primary"]["key"] == "demo-design-primary-tables"
    assert view["primary"]["collapsed_copy"] == "默认先显示样本范围总表，其余主表按需展开。"
    assert view["detail"]["demo_title"] == "样本设计补充表（3 张）"
    assert view["detail"]["demo_copy"] == "默认收起年份分布和来源细表，问答时再开。"
    assert "3 分钟汇报只保留样本摘要" in view["brief_mode_hint"]


def test_build_track_section_view_uses_anchor_and_counts() -> None:
    view = dashboard_view_models.build_track_section_view(
        anchor="price_pressure_track",
        title="价格压力主线",
        detail_tables_count=4,
        support_papers_count=5,
    )

    assert view["meta"]["refresh_running_label"] == "主线刷新中…"
    assert view["primary"]["key"] == "demo-price_pressure_track-primary-tables"
    assert view["detail"]["demo_title"] == "价格压力主线补充细表（4 张）"
    assert view["support_demo_title"] == "支撑文献（5 篇）"

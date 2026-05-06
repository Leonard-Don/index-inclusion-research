from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import dashboard_presenters, dashboard_sections

ROOT = Path(__file__).resolve().parents[1]


def _render_table_stub(frame, compact: bool = False) -> str:
    return f"<table rows={len(frame)} compact={compact}></table>"


def test_build_sample_design_section_uses_real_inputs_and_demo_split() -> None:
    section = dashboard_sections.build_sample_design_section(
        ROOT,
        demo_mode=True,
        render_table=_render_table_stub,
        attach_display_tiers=dashboard_presenters.attach_display_tiers,
        split_items_by_tier=dashboard_presenters.split_items_by_tier,
        create_sample_design_figures=lambda: [
            {"label": "首图", "path": "a.png", "caption": "A", "layout_class": "wide"},
            {"label": "次图", "path": "b.png", "caption": "B", "layout_class": ""},
        ],
        format_share=lambda value: f"{value:.1%}",
        format_p_value=lambda value: f"p={value:.3f}",
        value_labels={"announce": "公告日", "effective": "生效日"},
    )

    assert section["summary_cards"]
    assert len(section["figures"]) == 1
    assert len(section["detail_figures"]) == 1
    assert [table["label"] for table in section["primary_tables"]] == [
        "样本范围总表",
        "A 股与美股并列总结",
    ]
    assert [table["label"] for table in section["detail_tables"]] == [
        "按年份事件分布",
        "数据来源与口径",
    ]
    assert section["section_view"]["head"]["section_id"] == "design"
    assert section["section_view"]["detail_figures_title"] == "样本设计补充图表（1 张）"
    assert section["section_view"]["primary"]["key"] == "demo-design-primary-tables"
    assert section["section_view"]["detail"]["demo_title"] == "样本设计补充表（2 张）"


def test_build_robustness_section_returns_expected_table_blocks() -> None:
    section = dashboard_sections.build_robustness_section(
        ROOT,
        read_csv_if_exists=lambda path: pd.read_csv(path),
        render_table=_render_table_stub,
        attach_display_tiers=dashboard_presenters.attach_display_tiers,
        split_items_by_tier=dashboard_presenters.split_items_by_tier,
        format_share=lambda value: f"{value:.1%}",
        format_pct=lambda value: f"{value:.2%}",
    )

    assert len(section["summary_cards"]) == 4
    assert [table["label"] for table in section["primary_tables"]] == [
        "样本过滤摘要",
        "事件研究稳健性",
        "回归稳健性",
    ]
    assert [table["label"] for table in section["detail_tables"]] == ["长期保留稳健性"]
    assert section["section_view"]["head"]["section_id"] == "robustness"
    assert section["section_view"]["show_suite"] is True
    assert section["section_view"]["primary"]["key"] == "demo-robustness-primary-tables"
    assert section["section_view"]["detail"]["demo_title"] == "稳健性补充表（1 张）"


def test_build_limits_section_returns_scope_and_identification_tables() -> None:
    section = dashboard_sections.build_limits_section(
        ROOT,
        apply_live_rdd_status_to_identification_scope=lambda frame: frame,
        render_table=_render_table_stub,
        attach_display_tiers=dashboard_presenters.attach_display_tiers,
        split_items_by_tier=dashboard_presenters.split_items_by_tier,
        format_share=lambda value: f"{value:.1%}",
    )

    assert len(section["summary_cards"]) == 3
    assert "中国 RDD L" in section["summary_cards"][1]["meta"]
    assert "证据等级为" in section["summary_cards"][1]["foot"]
    assert [table["label"] for table in section["primary_tables"]] == ["样本与数据范围"]
    assert [table["label"] for table in section["detail_tables"]] == ["识别范围说明", "数据来源 · 引用清单"]
    assert section["artifact_tables"][0]["label"].startswith("原始输出全集")
    assert section["section_view"]["head"]["section_id"] == "limits"
    assert section["section_view"]["show_suite"] is True
    assert section["section_view"]["primary"]["key"] == "demo-limits-primary-tables"
    assert section["section_view"]["detail"]["demo_title"] == "研究边界补充表（2 张）"


def test_build_limits_section_adds_project_relative_artifact_index() -> None:
    captured_tables: list[pd.DataFrame] = []

    def _capture_table(frame: pd.DataFrame, compact: bool = False) -> str:
        captured_tables.append(frame.copy())
        return f"<table rows={len(frame)} compact={compact}></table>"

    section = dashboard_sections.build_limits_section(
        ROOT,
        apply_live_rdd_status_to_identification_scope=lambda frame: frame,
        render_table=_capture_table,
        attach_display_tiers=dashboard_presenters.attach_display_tiers,
        split_items_by_tier=dashboard_presenters.split_items_by_tier,
        format_share=lambda value: f"{value:.1%}",
    )

    assert len(section["artifact_tables"]) == 1
    artifact_index = captured_tables[-1]
    assert list(artifact_index.columns) == ["路径", "分组", "类型", "前端状态", "大小", "更新时间"]
    assert "/Users/" not in "\n".join(artifact_index["路径"].astype(str).tolist())
    assert "results/real_event_study/event_level_metrics.csv" in set(artifact_index["路径"])
    assert "data/raw/hs300_rdd_candidates.csv" in set(artifact_index["路径"])
    assert "索引保留" in " ".join(artifact_index["前端状态"].astype(str).unique())


def test_build_limits_section_derives_live_rdd_tier_for_summary_cards_and_scope_table() -> None:
    captured_tables: list[pd.DataFrame] = []

    def _capture_table(frame: pd.DataFrame, compact: bool = False) -> str:
        captured_tables.append(frame.copy())
        return f"<table rows={len(frame)} compact={compact}></table>"

    def _apply_live_status(frame: pd.DataFrame) -> pd.DataFrame:
        updated = frame.copy()
        mask = updated["分析层"] == "中国 RDD 扩展"
        updated.loc[mask, "证据等级"] = ""
        updated.loc[mask, "证据状态"] = "公开重建样本"
        updated.loc[mask, "当前口径"] = "当前使用公开数据重建的边界样本。"
        updated.loc[mask, "来源摘要"] = "公开重建候选样本文件 · 批次 2024-05-31 · 311 条候选"
        return updated

    section = dashboard_sections.build_limits_section(
        ROOT,
        apply_live_rdd_status_to_identification_scope=_apply_live_status,
        render_table=_capture_table,
        attach_display_tiers=dashboard_presenters.attach_display_tiers,
        split_items_by_tier=dashboard_presenters.split_items_by_tier,
        format_share=lambda value: f"{value:.1%}",
    )

    assert "中国 RDD L2" in section["summary_cards"][1]["meta"]
    assert "L2 · 公开重建样本" in section["summary_cards"][1]["foot"]
    assert "公开重建候选样本文件" in section["summary_cards"][1]["foot"]
    identification_scope = next(frame for frame in captured_tables if "分析层" in frame.columns)
    rdd_row = identification_scope.loc[identification_scope["分析层"] == "中国 RDD 扩展"].iloc[0]
    assert rdd_row["证据等级"] == "L2"


def test_build_paper_audit_section_maps_claims_to_tables() -> None:
    section = dashboard_sections.build_paper_audit_section(
        ROOT,
        render_table=_render_table_stub,
        attach_display_tiers=dashboard_presenters.attach_display_tiers,
        split_items_by_tier=dashboard_presenters.split_items_by_tier,
    )

    assert section["summary_cards"]
    assert section["section_view"]["head"]["section_id"] == "paper_audit"
    assert [table["label"] for table in section["primary_tables"]] == ["交付审计摘要"]
    assert [table["label"] for table in section["detail_tables"]] == ["主张证据路径"]

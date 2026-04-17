from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import dashboard_presenters
from index_inclusion_research import dashboard_sections


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
    assert [table["label"] for table in section["primary_tables"]] == ["样本与数据范围"]
    assert [table["label"] for table in section["detail_tables"]] == ["识别范围说明"]

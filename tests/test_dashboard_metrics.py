from __future__ import annotations

from pathlib import Path

from index_inclusion_research import dashboard_metrics


ROOT = Path(__file__).resolve().parents[1]


def _render_table_stub(frame, compact: bool = False) -> str:
    return f"<table rows={len(frame)} compact={compact}></table>"


def test_library_review_framework_and_supplement_cards_have_expected_shape() -> None:
    library = dashboard_metrics.build_library_summary_cards()
    review = dashboard_metrics.build_review_summary_cards()
    framework = dashboard_metrics.build_framework_summary_cards()
    supplement = dashboard_metrics.build_supplement_summary_cards()

    assert len(library) == 2
    assert library[0]["title"] == "先按阵营读，再按年份读"
    assert len(review) == 3
    assert review[0]["kicker"] == "反方文献"
    assert framework
    assert all(card["kicker"] == "文献阵营" for card in framework)
    assert len(supplement) == 4
    assert [card["kicker"] for card in supplement] == ["事件时钟", "机制链", "冲击估算", "表达场景"]


def test_track_metric_cards_return_expected_labels() -> None:
    price_cards = dashboard_metrics.build_price_pressure_cards(
        ROOT,
        format_pct=lambda value: f"{value:.2%}",
        format_p_value=lambda value: f"p={value:.3f}",
    )
    demand_cards = dashboard_metrics.build_demand_curve_cards(
        ROOT,
        format_pct=lambda value: f"{value:.2%}",
        format_p_value=lambda value: f"p={value:.3f}",
    )
    identification_cards = dashboard_metrics.build_identification_cards(
        ROOT,
        format_pct=lambda value: f"{value:.2%}",
        format_p_value=lambda value: f"p={value:.3f}",
        rdd_status={"mode": "missing"},
    )

    assert [card["label"] for card in price_cards] == [
        "美股公告日 CAR[-1,+1]",
        "A 股生效日 CAR[-1,+1]",
        "美股公告日成交量变化",
        "A 股生效日成交量变化",
    ]
    assert [card["label"] for card in demand_cards] == [
        "美股公告日保留率",
        "A 股生效日保留率",
        "美股公告日 CAR[0,+120]",
        "A 股公告日 CAR[0,+120]",
    ]
    assert "RDD 断点效应" not in [card["label"] for card in identification_cards]
    assert [card["label"] for card in identification_cards] == [
        "中国样本公告日 CAR[-1,+1]",
        "中国公告日非对称差值",
        "DID 异常收益估计",
        "匹配回归处理组系数",
    ]


def test_identification_status_panel_handles_missing_and_real_modes() -> None:
    panel = dashboard_metrics.build_identification_status_panel(
        {
            "mode": "missing",
            "evidence_status": "待补正式样本",
            "message": "等待真实候选样本文件。",
            "candidate_batches": None,
            "treated_rows": None,
            "control_rows": None,
            "crossing_batches": None,
            "validation_error": "",
            "audit_file": "",
        }
    )

    assert panel is not None
    assert panel["title"] == "待补正式样本"
    assert panel["kicker"] == "方法状态"
    assert "index-inclusion-prepare-hs300-rdd" in panel["meta"][1]["value"]
    assert "data/raw/hs300_rdd_candidates.template.csv" in panel["meta"][3]["value"]
    assert dashboard_metrics.build_identification_status_panel({"mode": "real"}) is None


def test_identification_status_panel_includes_candidate_audit_copy_when_available() -> None:
    panel = dashboard_metrics.build_identification_status_panel(
        {
            "mode": "demo",
            "evidence_status": "方法展示",
            "message": "当前处于显式 --demo 模式。",
            "candidate_batches": 2,
            "treated_rows": 3,
            "control_rows": 4,
            "crossing_batches": 2,
            "validation_error": "",
            "audit_file": "results/literature/hs300_rdd/candidate_batch_audit.csv",
        }
    )

    assert panel is not None
    assert panel["title"] == "方法展示"
    assert "2 个候选批次" in panel["meta"][0]["value"]
    assert "candidate_batch_audit.csv" in panel["meta"][3]["value"]


def test_track_metric_tables_return_expected_labels() -> None:
    price_tables = dashboard_metrics.build_price_pressure_tables(ROOT, render_table=_render_table_stub)
    demand_tables = dashboard_metrics.build_demand_curve_tables(ROOT, render_table=_render_table_stub)
    identification_tables = dashboard_metrics.build_identification_tables(
        ROOT,
        render_table=_render_table_stub,
        rdd_status={"mode": "missing"},
    )

    assert [label for label, _ in price_tables] == ["短窗口 CAR 摘要", "时间变化摘要", "机制变量变化"]
    assert [label for label, _ in demand_tables] == ["长短窗口 CAR 对比", "保留率与回吐"]
    assert [label for label, _ in identification_tables] == [
        "中国样本事件研究",
        "调入调出非对称性",
        "DID 摘要",
        "匹配回归核心系数",
    ]

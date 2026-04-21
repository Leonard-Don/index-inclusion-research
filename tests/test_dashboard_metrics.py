from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import dashboard_metrics
from index_inclusion_research.results_snapshot import ResultsSnapshot


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


def test_build_demand_curve_cards_uses_retention_note_when_ratio_invalid(tmp_path: Path) -> None:
    tables_dir = tmp_path / "results" / "real_tables"
    tables_dir.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "market": "US",
                "event_phase": "announce",
                "inclusion": 1,
                "retention_ratio": pd.NA,
                "retention_ratio_valid": False,
                "retention_note": "短窗口基数过小，不适合解释保留率。",
            },
            {
                "market": "CN",
                "event_phase": "effective",
                "inclusion": 1,
                "retention_ratio": 0.132718,
                "retention_ratio_valid": True,
                "retention_note": "短窗口基数充足，可用于解释长期保留率。",
            },
        ]
    ).to_csv(tables_dir / "retention_summary.csv", index=False)

    pd.DataFrame(
        [
            {"market": "US", "event_phase": "announce", "window_slug": "p0_p120", "inclusion": 1, "mean_car": 0.0159, "p_value": 0.117},
            {"market": "CN", "event_phase": "announce", "window_slug": "p0_p120", "inclusion": 1, "mean_car": 0.0155, "p_value": 0.511},
        ]
    ).to_csv(tables_dir / "long_window_event_study_summary.csv", index=False)

    cards = dashboard_metrics.build_demand_curve_cards(
        tmp_path,
        format_pct=lambda value: f"{value:.2%}",
        format_p_value=lambda value: f"p={value:.3f}",
        snapshot=ResultsSnapshot(tmp_path),
    )

    assert cards[0] == {
        "label": "美股公告日保留率",
        "value": "暂不解释",
        "copy": "短窗口基数过小，不适合解释保留率。",
    }
    assert cards[1]["label"] == "A 股生效日保留率"
    assert cards[1]["value"] == "0.13"


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
    assert panel["kicker"] == "证据等级"
    assert panel["tone"] == "missing"
    assert panel["signal_value"] == "L0 · 待补正式样本"
    assert panel["meta"][0]["label"] == "样本来源"
    assert panel["meta"][1]["label"] == "契约一致性"
    assert "live RDD 状态渲染" in panel["meta"][1]["value"]
    assert panel["meta"][2]["label"] == "覆盖说明"
    assert "index-inclusion-prepare-hs300-rdd" in panel["meta"][4]["value"]
    assert "index-inclusion-reconstruct-hs300-rdd" in panel["meta"][4]["value"]
    assert "data/raw/hs300_rdd_candidates.template.csv" in panel["meta"][6]["value"]

    real_panel = dashboard_metrics.build_identification_status_panel(
        {
            "mode": "real",
            "evidence_status": "正式边界样本",
            "message": "当前正在使用你提供的真实候选排名文件。",
            "candidate_batches": 1,
            "treated_rows": 299,
            "control_rows": 12,
            "crossing_batches": 1,
            "validation_error": "",
            "audit_file": "results/literature/hs300_rdd/candidate_batch_audit.csv",
        }
    )

    assert real_panel is not None
    assert real_panel["title"] == "正式边界样本"
    assert real_panel["tone"] == "official"
    assert real_panel["signal_value"] == "L3 · 正式边界样本"
    assert "正式证据链" in real_panel["copy"]
    assert "正式候选样本文件" in real_panel["meta"][0]["value"]
    assert "已满足" in real_panel["meta"][5]["value"]


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
    assert panel["tone"] == "demo"
    assert "demo 伪排名样本" in panel["meta"][0]["value"]
    assert "2 个候选批次" in panel["meta"][2]["value"]
    assert "index-inclusion-reconstruct-hs300-rdd" in panel["meta"][4]["value"]
    assert "candidate_batch_audit.csv" in panel["meta"][6]["value"]


def test_identification_status_panel_marks_reconstructed_mode_as_public_proxy() -> None:
    panel = dashboard_metrics.build_identification_status_panel(
        {
            "mode": "reconstructed",
            "evidence_status": "公开重建样本",
            "message": "当前正在使用公开数据重建的候选样本文件。",
            "candidate_batches": 1,
            "treated_rows": 299,
            "control_rows": 12,
            "crossing_batches": 1,
            "validation_error": "",
            "audit_file": "results/literature/hs300_rdd/candidate_batch_audit.csv",
        }
    )

    assert panel is not None
    assert panel["title"] == "公开重建样本"
    assert panel["tone"] == "reconstructed"
    assert "公开重建样本" in panel["copy"]
    assert "L2 · 公开重建样本" == panel["signal_value"]
    assert "公开重建候选样本文件" in panel["meta"][0]["value"]
    assert "升级到官方口径" in panel["meta"][4]["value"]
    assert "公开数据版证据链" in panel["meta"][5]["value"]


def test_identification_status_panel_surfaces_manifest_mismatch_copy() -> None:
    panel = dashboard_metrics.build_identification_status_panel(
        {
            "mode": "reconstructed",
            "evidence_status": "公开重建样本",
            "message": "当前正在使用公开数据重建的候选样本文件。",
            "candidate_batches": 1,
            "treated_rows": 299,
            "control_rows": 12,
            "crossing_batches": 1,
            "validation_error": "",
            "audit_file": "",
        },
        contract_check={
            "manifest_exists": True,
            "manifest_path": "results/real_tables/results_manifest.csv",
            "manifest_profile": "real",
            "matches": False,
            "mismatched_fields": ["source_label", "coverage_note"],
            "live_status": {"mode": "reconstructed"},
            "manifest": {"rdd_source_label": "待补候选样本"},
        },
    )

    assert panel is not None
    assert "results/real_tables/results_manifest.csv" in panel["meta"][1]["value"]
    assert "来源摘要" in panel["meta"][1]["value"]
    assert "覆盖说明" in panel["meta"][1]["value"]


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

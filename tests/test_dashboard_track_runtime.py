from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import dashboard_config
from index_inclusion_research.dashboard_track_content_runtime import DashboardTrackContentRuntime
from index_inclusion_research.dashboard_track_display_runtime import DashboardTrackDisplayRuntime
from index_inclusion_research.dashboard_track_runtime import DashboardTrackRuntime
from index_inclusion_research.dashboard_track_support_runtime import DashboardTrackSupportRuntime


def _build_track_runtime() -> DashboardTrackRuntime:
    analyses = dashboard_config.build_analyses(
        run_price_pressure_track=lambda verbose=False: {},
        run_demand_curve_track=lambda verbose=False: {},
        run_identification_china_track=lambda verbose=False: {},
    )
    return DashboardTrackRuntime(
        root=Path("/tmp"),
        analyses=analyses,
        library_card=dashboard_config.LIBRARY_CARD,
        review_card=dashboard_config.REVIEW_CARD,
        framework_card=dashboard_config.FRAMEWORK_CARD,
        supplement_card=dashboard_config.SUPPLEMENT_CARD,
        project_module_display_map=dashboard_config.build_project_module_display(analyses),
    )


def test_dashboard_track_runtime_composes_support_content_and_display(monkeypatch) -> None:
    track = _build_track_runtime()
    expected_library = {"id": "paper_library"}

    monkeypatch.setattr(track.content, "load_literature_library_result", lambda: expected_library)
    monkeypatch.setattr(track.display, "run_and_cache_analysis", lambda analysis_id: {"id": analysis_id})

    assert isinstance(track.support, DashboardTrackSupportRuntime)
    assert isinstance(track.content, DashboardTrackContentRuntime)
    assert isinstance(track.display, DashboardTrackDisplayRuntime)
    assert track.table_labels is track.support.table_labels
    assert track.load_literature_library_result() is expected_library
    assert track.run_and_cache_analysis("price_pressure_track") == {"id": "price_pressure_track"}


def test_apply_live_rdd_status_updates_evidence_tier_column(monkeypatch) -> None:
    track = _build_track_runtime()
    frame = pd.DataFrame(
        [
            {"分析层": "短窗口事件研究", "证据状态": "正式样本", "当前口径": "event"},
            {"分析层": "中国 RDD 扩展", "证据状态": "待补正式样本", "当前口径": "old"},
        ]
    )
    monkeypatch.setattr(
        track.content,
        "load_rdd_status",
        lambda output_dir=None: {
            "mode": "reconstructed",
            "evidence_tier": "L2",
            "evidence_status": "公开重建样本",
            "source_kind": "reconstructed",
            "source_label": "公开重建候选样本文件",
            "source_file": "data/raw/hs300_rdd_candidates.reconstructed.csv",
            "generated_at": "2026-04-18T10:00:00+08:00",
            "as_of_date": "2024-05-31",
            "batch_label": "2024-05-31",
            "coverage_note": "311 条候选；1 个批次；1 个批次覆盖 cutoff 两侧。",
            "message": "",
            "note": "public proxy",
            "input_file": "",
            "audit_file": "",
            "candidate_rows": 311,
            "candidate_batches": 1,
            "treated_rows": 299,
            "control_rows": 12,
            "crossing_batches": 1,
            "validation_error": "",
        },
    )

    updated = track.content.apply_live_rdd_status_to_identification_scope(frame)
    rdd_row = updated.loc[updated["分析层"] == "中国 RDD 扩展"].iloc[0]

    assert rdd_row["证据等级"] == "L2"
    assert rdd_row["证据状态"] == "公开重建样本"
    assert rdd_row["当前口径"] == "public proxy"
    assert "公开重建候选样本文件" in rdd_row["来源摘要"]


def test_load_rdd_contract_check_delegates_to_content(monkeypatch) -> None:
    track = _build_track_runtime()
    expected = {
        "manifest_exists": True,
        "manifest_path": "results/real_tables/results_manifest.csv",
        "manifest_profile": "real",
        "matches": True,
        "mismatched_fields": [],
        "live_status": {"mode": "reconstructed"},
        "manifest": {"rdd_mode": "reconstructed"},
    }
    monkeypatch.setattr(track.content, "load_rdd_contract_check", lambda **kwargs: expected)

    assert track.load_rdd_contract_check() is expected

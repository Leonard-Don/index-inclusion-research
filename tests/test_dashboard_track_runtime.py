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

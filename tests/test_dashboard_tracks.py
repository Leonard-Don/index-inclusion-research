from __future__ import annotations

from pathlib import Path

from index_inclusion_research import dashboard_tracks
from index_inclusion_research.dashboard_types import AnalysisCache, RddStatus


ROOT = Path(__file__).resolve().parents[1]


def test_run_and_cache_analysis_normalizes_and_caches_result() -> None:
    analyses = {
        "price_pressure_track": {
            "title": "短期价格压力与效应减弱",
            "subtitle": "Price Pressure",
            "description_zh": "desc",
            "project_module": "短期价格压力",
            "runner": lambda verbose=False: {"description": "runner-desc", "summary_path": "", "tables": {}, "figures": []},
        }
    }
    run_cache: AnalysisCache = {}

    result = dashboard_tracks.run_and_cache_analysis(
        "price_pressure_track",
        analyses=analyses,
        run_cache=run_cache,
        normalize_result=lambda raw: {"normalized": True, **raw},
        attach_project_track_context=lambda current, config: {**current, "context": config["project_module"]},
    )

    assert result["normalized"] is True
    assert result["title"] == "短期价格压力与效应减弱"
    assert result["subtitle"] == "Price Pressure"
    assert result["description"] == "runner-desc"
    assert result["context"] == "短期价格压力"
    assert run_cache["price_pressure_track"] == result


def test_load_or_build_track_section_prefers_saved_result_before_runner() -> None:
    analyses = {
        "price_pressure_track": {
            "title": "短期价格压力与效应减弱",
            "subtitle": "Price Pressure",
            "description_zh": "desc",
            "project_module": "短期价格压力",
            "runner": lambda verbose=False: (_ for _ in ()).throw(AssertionError("runner should not be used")),
        }
    }
    run_cache: AnalysisCache = {}

    result = dashboard_tracks.load_or_build_track_section(
        "price_pressure_track",
        analyses=analyses,
        run_cache=run_cache,
        load_saved_track_result=lambda analysis_id, config: {"id": "saved", "summary_text": "saved"},
        normalize_result=lambda raw: {"normalized": True, **raw},
        attach_project_track_context=lambda current, config: current,
    )

    assert result == {"id": "saved", "summary_text": "saved"}
    assert run_cache["price_pressure_track"] == result


def test_run_and_cache_all_populates_static_sections() -> None:
    run_cache: AnalysisCache = {}
    called: list[str] = []

    dashboard_tracks.run_and_cache_all(
        analyses={
            "price_pressure_track": {"title": "a"},
            "demand_curve_track": {"title": "b"},
        },
        run_cache=run_cache,
        run_and_cache_analysis=lambda analysis_id: called.append(analysis_id) or {"id": analysis_id},
        load_literature_library_result=lambda: {"id": "paper_library"},
        load_literature_review_result=lambda: {"id": "paper_review"},
        load_literature_framework_result=lambda: {"id": "paper_framework"},
        load_supplement_result=lambda: {"id": "project_supplement"},
    )

    assert called == ["price_pressure_track", "demand_curve_track"]
    assert run_cache["paper_library"]["id"] == "paper_library"
    assert run_cache["paper_review"]["id"] == "paper_review"
    assert run_cache["paper_framework"]["id"] == "paper_framework"
    assert run_cache["project_supplement"]["id"] == "project_supplement"


def test_prepare_track_display_hides_formal_rdd_tables_when_status_missing() -> None:
    missing_status: RddStatus = {
        "mode": "missing",
        "evidence_status": "待补正式样本",
        "message": "等待真实候选样本文件。",
        "note": "等待正式样本",
        "input_file": "",
        "audit_file": "",
        "candidate_rows": None,
        "candidate_batches": None,
        "treated_rows": None,
        "control_rows": None,
        "crossing_batches": None,
        "validation_error": "",
    }
    display = dashboard_tracks.prepare_track_display(
        ROOT,
        {"summary_text": "# 标题\n原始摘要", "figure_paths": []},
        "identification_china_track",
        True,
        load_rdd_status=lambda: missing_status,
        clean_display_text=lambda text: text.replace("# 标题", "").strip(),
        render_table=lambda frame, compact=False: f"<table rows={len(frame)} compact={compact}></table>",
        format_pct=lambda value: f"{value:.2%}",
        format_p_value=lambda value: f"p={value:.3f}",
        create_price_pressure_figures=lambda: [{"path": "price.png", "caption": "price"}],
        create_identification_figures=lambda: [{"path": "id.png", "caption": "identification"}],
    )

    labels = [item["label"] for item in display["display_tables"]]
    assert "RDD 摘要" not in labels
    assert display["status_panel"] is not None
    assert display["status_panel"]["title"] == "待补正式样本"
    assert display["status_panel"]["meta"][0]["label"] == "当前状态"
    assert display["display_figures"] == [{"path": "id.png", "caption": "identification"}]


def test_prepare_track_display_keeps_real_rdd_outputs_and_hides_status_panel(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_tracks.dashboard_metrics, "build_price_pressure_cards", lambda *args, **kwargs: [])
    monkeypatch.setattr(dashboard_tracks.dashboard_metrics, "build_demand_curve_cards", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        dashboard_tracks.dashboard_metrics,
        "build_identification_cards",
        lambda *args, **kwargs: [{"label": "RDD 断点效应", "value": "0.1234", "copy": "p=0.010"}],
    )
    monkeypatch.setattr(dashboard_tracks.dashboard_metrics, "build_price_pressure_tables", lambda *args, **kwargs: [])
    monkeypatch.setattr(dashboard_tracks.dashboard_metrics, "build_demand_curve_tables", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        dashboard_tracks.dashboard_metrics,
        "build_identification_tables",
        lambda *args, **kwargs: [("RDD 摘要", "<table rows=1 compact=True></table>")],
    )

    display = dashboard_tracks.prepare_track_display(
        ROOT,
        {"summary_text": "# 标题\n原始摘要", "figure_paths": [{"path": "saved.png", "caption": "saved"}]},
        "identification_china_track",
        False,
        load_rdd_status=lambda: {
            "mode": "real",
            "evidence_status": "正式边界样本",
            "message": "ok",
            "note": "",
            "input_file": "",
            "audit_file": "",
            "candidate_rows": None,
            "candidate_batches": None,
            "treated_rows": None,
            "control_rows": None,
            "crossing_batches": None,
            "validation_error": "",
        },
        clean_display_text=lambda text: text.replace("# 标题", "").strip(),
        render_table=lambda frame, compact=False: f"<table rows={len(frame)} compact={compact}></table>",
        format_pct=lambda value: f"{value:.2%}",
        format_p_value=lambda value: f"p={value:.3f}",
        create_price_pressure_figures=lambda: [],
        create_identification_figures=lambda: [{"path": "id.png", "caption": "identification"}],
    )

    assert display["status_panel"] is None
    assert [item["label"] for item in display["display_tables"]] == ["RDD 摘要"]
    assert display["result_cards"] == [{"label": "RDD 断点效应", "value": "0.1234", "copy": "p=0.010"}]
    assert display["display_figures"] == [
        {"path": "id.png", "caption": "identification"},
        {"path": "saved.png", "caption": "saved"},
    ]
    assert isinstance(display, dict)


def test_prepare_track_display_keeps_reconstructed_rdd_outputs_and_retains_status_panel(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_tracks.dashboard_metrics, "build_price_pressure_cards", lambda *args, **kwargs: [])
    monkeypatch.setattr(dashboard_tracks.dashboard_metrics, "build_demand_curve_cards", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        dashboard_tracks.dashboard_metrics,
        "build_identification_cards",
        lambda *args, **kwargs: [{"label": "RDD 断点效应", "value": "0.1234", "copy": "p=0.010"}],
    )
    monkeypatch.setattr(dashboard_tracks.dashboard_metrics, "build_price_pressure_tables", lambda *args, **kwargs: [])
    monkeypatch.setattr(dashboard_tracks.dashboard_metrics, "build_demand_curve_tables", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        dashboard_tracks.dashboard_metrics,
        "build_identification_tables",
        lambda *args, **kwargs: [("RDD 摘要", "<table rows=1 compact=True></table>")],
    )

    display = dashboard_tracks.prepare_track_display(
        ROOT,
        {"summary_text": "# 标题\n原始摘要", "figure_paths": [{"path": "saved.png", "caption": "saved"}]},
        "identification_china_track",
        False,
        load_rdd_status=lambda: {
            "mode": "reconstructed",
            "evidence_status": "公开重建样本",
            "message": "ok",
            "note": "",
            "input_file": "",
            "audit_file": "",
            "candidate_rows": 311,
            "candidate_batches": 1,
            "treated_rows": 299,
            "control_rows": 12,
            "crossing_batches": 1,
            "validation_error": "",
        },
        clean_display_text=lambda text: text.replace("# 标题", "").strip(),
        render_table=lambda frame, compact=False: f"<table rows={len(frame)} compact={compact}></table>",
        format_pct=lambda value: f"{value:.2%}",
        format_p_value=lambda value: f"p={value:.3f}",
        create_price_pressure_figures=lambda: [],
        create_identification_figures=lambda: [{"path": "id.png", "caption": "identification"}],
    )

    assert display["status_panel"] is not None
    assert display["status_panel"]["title"] == "公开重建样本"
    assert [item["label"] for item in display["display_tables"]] == ["RDD 摘要"]
    assert display["result_cards"] == [{"label": "RDD 断点效应", "value": "0.1234", "copy": "p=0.010"}]

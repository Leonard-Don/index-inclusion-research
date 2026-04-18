from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import dashboard_home
from index_inclusion_research.dashboard_home import DashboardHomeContextBuilder
from index_inclusion_research.dashboard_types import (
    AnalysisCache,
    RefreshStatusPayload,
    SnapshotMeta,
    TrackDisplaySection,
    TrackResult,
)


ROOT = Path(__file__).resolve().parents[1]


def _snapshot_meta() -> SnapshotMeta:
    return {
        "label": "snapshot",
        "copy": "copy",
        "source_path": "results/real_tables/event_counts.csv",
        "source_count": 1,
    }


def _refresh_payload(mode: str, anchor: str, open_panels: str | None) -> RefreshStatusPayload:
    return {
        "accepted": True,
        "status": "idle",
        "message": f"{mode}:{anchor}",
        "error": "",
        "scope_label": "全部刷新",
        "scope_key": "all",
        "started_at": "",
        "finished_at": "",
        "started_ts": 0.0,
        "finished_ts": 0.0,
        "duration_seconds": None,
        "poll_after_ms": 1200,
        "redirect_url": f"/?mode={mode}#{anchor}",
        "snapshot_label": "snapshot",
        "snapshot_copy": open_panels or "",
        "snapshot_source_path": "results/real_tables/event_counts.csv",
        "snapshot_source_count": 1,
        "updated_artifacts": [],
    }


def test_build_overview_metrics_uses_real_event_count() -> None:
    metrics = dashboard_home.build_overview_metrics(
        ROOT,
        rdd_status={
            "mode": "reconstructed",
            "evidence_status": "公开重建样本",
            "message": "当前正在使用公开数据重建的候选样本文件。",
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
    )
    event_counts = pd.read_csv(ROOT / "results" / "real_tables" / "event_counts.csv")

    assert [item["value"] for item in metrics[:3]] == ["16", "3", "5"]
    assert metrics[3]["value"] == str(int(event_counts["n_events"].sum()))
    assert metrics[3]["label"] == "个真实调入/调出事件，构成默认样本"
    assert metrics[4]["value"] == "L2"
    assert metrics[4]["label"] == "中国 RDD 当前为公开重建样本"
    assert metrics[4]["tone"] == "reconstructed"
    assert "公开重建候选样本文件" in metrics[4]["meta"]


def test_build_highlights_keeps_current_cn_effective_discussion() -> None:
    highlights = dashboard_home.build_highlights(
        ROOT,
        rdd_status={
            "mode": "reconstructed",
            "evidence_status": "公开重建样本",
            "message": "当前正在使用公开数据重建的候选样本文件。",
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
    )
    discussion = next(item for item in highlights if item["label"] == "最值得讨论")
    method = next(item for item in highlights if item["label"] == "方法含义")

    assert "但统计上并不显著" in discussion["copy"]
    assert "[0,+120] 窗口下调入与调出的 CAR 差异达到" in discussion["copy"]
    assert "且统计显著。这说明 A 股市场不能机械套用美股的经典指数纳入叙事。" not in discussion["copy"]
    assert method["headline"] == "中国 RDD 已进入公开数据版证据链。"
    assert "公开重建口径" in method["copy"]
    assert "当前来源为 公开重建候选样本文件" in method["copy"]


def test_build_home_context_full_mode_assembles_and_caches_sections() -> None:
    run_cache: AnalysisCache = {}
    analyses = {
        "price_pressure_track": {"title": "短期价格压力与效应减弱"},
        "demand_curve_track": {"title": "需求曲线与长期保留"},
    }

    def _load_or_build_track_section(analysis_id: str) -> TrackResult:
        return {"id": analysis_id, "summary_text": analysis_id}

    def _prepare_track_display(section: TrackDisplaySection, analysis_id: str, demo_mode: bool) -> TrackDisplaySection:
        display = dict(section)
        display["prepared"] = True
        display["demo_mode"] = demo_mode
        return display

    context = dashboard_home.build_home_context(
        root=ROOT,
        display_mode="full",
        current_open_panels="demo-design-detail-tables",
        analyses=analyses,
        run_cache=run_cache,
        nav_sections_for_mode=lambda mode: [{"anchor": "overview", "label": "总览"}],
        mode_tabs_for_mode=lambda mode, open_panels: [{"mode": mode, "open_panels": open_panels}],
        build_dashboard_snapshot_meta=_snapshot_meta,
        refresh_status_payload=_refresh_payload,
        overview_notes_for_mode=lambda mode: [{"title": mode, "copy": "note"}],
        overview_summary_for_mode=lambda mode: f"{mode}-summary",
        cta_copy_for_mode=lambda mode: f"{mode}-cta",
        abstract_lead=lambda: "lead",
        abstract_points=lambda: [{"title": "point", "copy": "copy"}],
        load_or_build_track_section=_load_or_build_track_section,
        build_track_notes=lambda analysis_id: [{"name": analysis_id, "copy": "note"}],
        prepare_track_display=_prepare_track_display,
        load_literature_framework_result=lambda: {"id": "paper_framework"},
        prepare_framework_display=lambda section, demo_mode: {"prepared_framework": True, "demo_mode": demo_mode, **section},
        load_supplement_result=lambda: {"id": "project_supplement"},
        prepare_supplement_display=lambda section, demo_mode: {"prepared_supplement": True, "demo_mode": demo_mode, **section},
        build_sample_design_section=lambda demo_mode: {"id": "design", "demo_mode": demo_mode},
        build_robustness_section=lambda: {"id": "robustness"},
        build_limits_section=lambda: {"id": "limits"},
        refresh_status_url="/refresh/status",
    )

    assert context["mode"] == "full"
    assert context["snapshot_meta"]["source_count"] == 1
    assert context["refresh_meta"]["redirect_url"] == "/?mode=full#overview"
    assert len(context["track_sections"]) == 2
    assert all(section["prepared"] is True for section in context["track_sections"])
    assert all(section["demo_mode"] is False for section in context["track_sections"])
    assert context["framework_section"]["prepared_framework"] is True
    assert context["supplement_section"]["prepared_supplement"] is True
    assert context["design_section"]["demo_mode"] is False
    assert context["robustness_section"] == {"id": "robustness"}
    assert context["limits_section"] == {"id": "limits"}
    assert run_cache["paper_framework"]["prepared_framework"] is True
    assert run_cache["project_supplement"]["prepared_supplement"] is True


def test_build_home_context_brief_mode_collapses_secondary_sections() -> None:
    context = dashboard_home.build_home_context(
        root=ROOT,
        display_mode="brief",
        current_open_panels=None,
        analyses={"price_pressure_track": {"title": "短期价格压力与效应减弱"}},
        run_cache={},
        nav_sections_for_mode=lambda mode: [{"anchor": "overview", "label": "总览"}],
        mode_tabs_for_mode=lambda mode, open_panels: [{"mode": mode}],
        build_dashboard_snapshot_meta=_snapshot_meta,
        refresh_status_payload=_refresh_payload,
        overview_notes_for_mode=lambda mode: [{"title": mode, "copy": "note"}],
        overview_summary_for_mode=lambda mode: f"{mode}-summary",
        cta_copy_for_mode=lambda mode: f"{mode}-cta",
        abstract_lead=lambda: "lead",
        abstract_points=lambda: [{"title": "point", "copy": "copy"}],
        load_or_build_track_section=lambda analysis_id: {"id": analysis_id},
        build_track_notes=lambda analysis_id: [{"name": analysis_id, "copy": "note"}],
        prepare_track_display=lambda section, analysis_id, demo_mode: {**section, "demo_mode": demo_mode},
        load_literature_framework_result=lambda: {"id": "paper_framework"},
        prepare_framework_display=lambda section, demo_mode: {"prepared_framework": True, **section},
        load_supplement_result=lambda: {"id": "project_supplement"},
        prepare_supplement_display=lambda section, demo_mode: {"prepared_supplement": True, **section},
        build_sample_design_section=lambda demo_mode: {"id": "design", "demo_mode": demo_mode},
        build_robustness_section=lambda: {"id": "robustness"},
        build_limits_section=lambda: {"id": "limits"},
        refresh_status_url="/refresh/status",
    )

    assert context["framework_section"] == {"display_summary": "", "display_tables": [], "summary_cards": []}
    assert context["supplement_section"] == {"display_summary": "", "display_tables": [], "summary_cards": []}
    assert set(context["framework_section"]) == {"display_summary", "display_tables", "summary_cards"}
    assert context["design_section"]["demo_mode"] is False
    assert context["robustness_section"] == {"summary": "", "summary_cards": [], "tables": []}


def test_dashboard_home_context_builder_builds_and_caches_secondary_sections() -> None:
    run_cache: AnalysisCache = {}
    builder = DashboardHomeContextBuilder(
        root=ROOT,
        analyses={"price_pressure_track": {"title": "短期价格压力与效应减弱"}},
        run_cache=run_cache,
        nav_sections_for_mode=lambda mode: [{"anchor": "overview", "label": "总览"}],
        mode_tabs_for_mode=lambda mode, open_panels: [{"mode": mode, "open_panels": open_panels}],
        build_dashboard_snapshot_meta=_snapshot_meta,
        refresh_status_payload=_refresh_payload,
        overview_notes_for_mode=lambda mode: [{"title": mode, "copy": "note"}],
        overview_summary_for_mode=lambda mode: f"{mode}-summary",
        cta_copy_for_mode=lambda mode: f"{mode}-cta",
        abstract_lead=lambda: "lead",
        abstract_points=lambda: [{"title": "point", "copy": "copy"}],
        load_or_build_track_section=lambda analysis_id: {"id": analysis_id},
        build_track_notes=lambda analysis_id: [{"name": analysis_id, "copy": "note"}],
        prepare_track_display=lambda section, analysis_id, demo_mode: {**section, "prepared": True},
        load_literature_framework_result=lambda: {"id": "paper_framework"},
        prepare_framework_display=lambda section, demo_mode: {"prepared_framework": True, **section},
        load_supplement_result=lambda: {"id": "project_supplement"},
        prepare_supplement_display=lambda section, demo_mode: {"prepared_supplement": True, **section},
        build_sample_design_section=lambda demo_mode: {"id": "design", "demo_mode": demo_mode},
        build_robustness_section=lambda: {"id": "robustness"},
        build_limits_section=lambda: {"id": "limits"},
    )

    context = builder.build(
        display_mode="demo",
        current_open_panels="demo-design-detail-tables",
        refresh_status_url="/refresh/status",
    )

    assert context["track_sections"][0]["prepared"] is True
    assert context["framework_section"]["prepared_framework"] is True
    assert context["supplement_section"]["prepared_supplement"] is True
    assert context["refresh_meta"]["snapshot_label"] == "snapshot"
    assert run_cache["paper_framework"]["prepared_framework"] is True
    assert run_cache["project_supplement"]["prepared_supplement"] is True

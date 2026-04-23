from __future__ import annotations

from index_inclusion_research import dashboard_refresh
from index_inclusion_research.dashboard_refresh_coordinator import (
    DashboardRefreshCoordinator,
)


def _build_coordinator() -> DashboardRefreshCoordinator:
    return DashboardRefreshCoordinator(
        details_query_param="open",
        allowed_keys=frozenset({"demo-design-detail-tables"}),
        track_anchors=frozenset({"price_pressure_track"}),
        dashboard_snapshot_sources=lambda: [],
        build_rdd_contract_check=lambda: {
            "manifest_exists": True,
            "manifest_path": "results/real_tables/results_manifest.csv",
            "manifest_profile": "real",
            "matches": True,
            "mismatched_fields": [],
            "live_status": {"mode": "reconstructed"},
            "manifest": {"rdd_mode": "reconstructed"},
        },
        to_relative=str,
        build_dashboard_snapshot_meta=lambda snapshot_files=None: {
            "label": "snapshot",
            "copy": "copy",
            "source_path": "results/real_tables/event_study_summary.csv",
            "source_count": 1 if snapshot_files is None else len(snapshot_files),
        },
        nav_sections_for_mode=lambda mode: [{"anchor": "overview", "label": "总览"}],
    )


def test_refresh_coordinator_normalizes_open_panels_and_anchor() -> None:
    coordinator = _build_coordinator()

    assert coordinator.normalize_open_panels("demo-design-detail-tables,unknown") == "demo-design-detail-tables"
    assert coordinator.normalize_anchor_for_mode("demo", "price_pressure_track") == "price_pressure_track"
    assert coordinator.normalize_anchor_for_mode("demo", "unknown") == "overview"


def test_refresh_coordinator_builds_redirect_and_status_payload() -> None:
    coordinator = _build_coordinator()
    redirect = coordinator.refresh_redirect_url(
        "demo",
        "price_pressure_track",
        open_panels="demo-design-detail-tables",
        url_builder=lambda **kwargs: (
            f"/?mode={kwargs['mode']}"
            + (f"&open={kwargs['open']}" if "open" in kwargs else "")
            + (f"#{kwargs['_anchor']}" if "_anchor" in kwargs else "")
        ),
    )

    assert redirect == "/?mode=demo&open=demo-design-detail-tables#price_pressure_track"

    payload = coordinator.refresh_status_payload(
        "demo",
        "overview",
        open_panels=None,
        redirect_url_builder=lambda mode, anchor, open_panels: f"/?mode={mode}#{anchor}",
        now_ts=123.0,
    )

    assert payload["snapshot_label"] == "snapshot"
    assert payload["accepted"] is True
    assert payload["status"] == "idle"
    assert payload["redirect_url"] == ""
    assert payload["contract_status_label"] == "结果状态已同步"
    assert payload["artifact_summary_label"] == "最近结果概览"
    assert "结果状态：结果状态已同步" in payload["artifact_summary_copy"]


def test_refresh_coordinator_queue_and_mark_success() -> None:
    coordinator = _build_coordinator()
    spawned: list[tuple[str, str]] = []

    queued = coordinator.queue_refresh_job(
        runner=lambda: None,
        scope_label="全部材料",
        scope_key="all",
        started_at="2026-04-17 12:00:00",
        started_ts=100.0,
        spawn_refresh_worker=lambda runner, scope_label, scope_key: spawned.append((scope_label, scope_key)),
    )

    assert queued is True
    assert spawned == [("全部材料", "all")]
    assert coordinator.state["snapshot_source_path"] == "results/real_tables/event_study_summary.csv"

    coordinator.mark_refresh_succeeded(
        "全部材料",
        "all",
        finished_at="2026-04-17 12:00:05",
        finished_ts=105.0,
    )

    assert coordinator.state["status"] == "succeeded"
    assert coordinator.state["scope_key"] == "all"
    assert coordinator.state["contract_status_label"] == "结果状态已同步"
    assert coordinator.state["artifact_summary_label"] == "本次未发现新的核心产物"
    assert coordinator.state["updated_artifacts"] == []


def test_refresh_coordinator_delegates_refresh_job_and_worker_helpers(monkeypatch) -> None:
    coordinator = _build_coordinator()
    events: list[tuple[str, str]] = []

    monkeypatch.setattr(
        dashboard_refresh,
        "run_refresh_job",
        lambda runner, scope_label, scope_key, **kwargs: kwargs["mark_refresh_succeeded"](scope_label, scope_key),
    )
    coordinator.run_refresh_job(
        lambda: None,
        "全部材料",
        "all",
        mark_refresh_succeeded=lambda scope_label, scope_key: events.append(("success", scope_key)),
        mark_refresh_failed=lambda scope_label, scope_key, exc: events.append(("failed", scope_key)),
    )

    monkeypatch.setattr(
        dashboard_refresh,
        "spawn_refresh_worker",
        lambda runner, scope_label, scope_key, **kwargs: kwargs["run_refresh_job"](runner, scope_label, scope_key),
    )
    coordinator.spawn_refresh_worker(
        lambda: None,
        "全部材料",
        "all",
        run_refresh_job=lambda runner, scope_label, scope_key: events.append(("spawn", scope_key)),
    )

    assert events == [("success", "all"), ("spawn", "all")]

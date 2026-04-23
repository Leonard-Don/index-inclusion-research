from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from threading import Lock

from index_inclusion_research import dashboard_refresh


def test_normalize_open_panels_filters_unknown_keys_and_duplicates() -> None:
    normalized = dashboard_refresh.normalize_open_panels(
        "demo-design-detail-tables, unknown ,demo-design-detail-tables,demo-framework-detail-tables",
        allowed_keys=frozenset({"demo-design-detail-tables", "demo-framework-detail-tables"}),
    )

    assert normalized == "demo-design-detail-tables,demo-framework-detail-tables"


def test_normalize_anchor_for_mode_falls_back_to_tracks_for_hidden_sections() -> None:
    anchor = dashboard_refresh.normalize_anchor_for_mode(
        "brief",
        "framework",
        nav_sections=[
            {"anchor": "overview", "label": "总览"},
            {"anchor": "tracks", "label": "主线结果"},
        ],
        track_anchors={"price_pressure_track", "demand_curve_track", "identification_china_track"},
    )

    assert anchor == "tracks"


def test_refresh_redirect_url_keeps_open_panels_and_anchor() -> None:
    url = dashboard_refresh.refresh_redirect_url(
        "demo",
        "framework",
        open_panels="demo-design-detail-tables,demo-design-detail-tables",
        details_query_param="open",
        url_builder=lambda **kwargs: f"/?mode={kwargs['mode']}&open={kwargs.get('open', '')}#{kwargs.get('_anchor', '')}",
        normalize_anchor=lambda mode, anchor: dashboard_refresh.normalize_anchor_for_mode(
            mode,
            anchor,
            nav_sections=[
                {"anchor": "overview", "label": "总览"},
                {"anchor": "framework", "label": "文献框架"},
            ],
            track_anchors={"price_pressure_track"},
        ),
        normalize_open_panels=lambda raw: dashboard_refresh.normalize_open_panels(
            raw,
            allowed_keys=frozenset({"demo-design-detail-tables"}),
        ),
    )

    assert url == "/?mode=demo&open=demo-design-detail-tables#framework"


def test_build_dashboard_snapshot_meta_uses_latest_timestamp(tmp_path: Path) -> None:
    older = tmp_path / "older.csv"
    newer = tmp_path / "newer.md"
    older.write_text("older\n", encoding="utf-8")
    newer.write_text("newer\n", encoding="utf-8")
    older_ts = 1_700_000_000
    newer_ts = 1_800_000_000
    older.touch()
    newer.touch()
    older.chmod(0o644)
    newer.chmod(0o644)

    import os

    os.utime(older, (older_ts, older_ts))
    os.utime(newer, (newer_ts, newer_ts))

    meta = dashboard_refresh.build_dashboard_snapshot_meta(
        tmp_path,
        to_relative=str,
        snapshot_files=[older, newer],
    )

    assert meta["source_count"] == 2
    assert meta["source_path"] == str(newer)
    assert meta["label"] == datetime.fromtimestamp(newer_ts, tz=UTC).astimezone().strftime("%Y-%m-%d %H:%M")


def test_refresh_status_payload_reports_duration_error_and_redirect() -> None:
    failed_payload = dashboard_refresh.refresh_status_payload(
        {
            "status": "failed",
            "message": "刷新失败。",
            "scope_label": "全部材料",
            "scope_key": "all",
            "started_at": "2026-04-17 10:00",
            "finished_at": "2026-04-17 10:01",
            "started_ts": 100.0,
            "finished_ts": 160.0,
            "error": "boom",
            "snapshot_label": "",
            "snapshot_copy": "",
            "snapshot_source_path": "",
            "snapshot_source_count": 0,
            "contract_status_label": "",
            "contract_status_copy": "",
            "artifact_summary_label": "",
            "artifact_summary_copy": "",
            "updated_artifacts": [],
            "baseline_artifact_mtimes": {},
        },
        mode="demo",
        anchor="overview",
        open_panels=None,
        snapshot_meta={
            "label": "2026-04-17 10:02",
            "copy": "snapshot",
            "source_path": "results/real_tables/event_study_summary.csv",
            "source_count": 1,
        },
        contract_check={
            "manifest_exists": True,
            "manifest_path": "results/real_tables/results_manifest.csv",
            "manifest_profile": "real",
            "matches": True,
            "mismatched_fields": [],
            "live_status": {"mode": "reconstructed"},
            "manifest": {"rdd_mode": "reconstructed"},
        },
        redirect_url_builder=lambda mode, anchor, open_panels: f"/?mode={mode}#{anchor}",
        now_ts=170.0,
    )

    assert failed_payload["duration_seconds"] == 60
    assert failed_payload["accepted"] is True
    assert failed_payload["message"].endswith("boom")
    assert failed_payload["redirect_url"] == ""
    assert failed_payload["snapshot_source_path"] == "results/real_tables/event_study_summary.csv"
    assert failed_payload["snapshot_source_count"] == 1
    assert failed_payload["contract_status_label"] == "结果状态已同步"
    assert failed_payload["artifact_summary_label"] == "本次刷新未完成"
    assert "结果状态：结果状态已同步" in failed_payload["artifact_summary_copy"]
    assert failed_payload["updated_artifacts"] == []

    success_payload = dashboard_refresh.refresh_status_payload(
        {
            "status": "succeeded",
            "message": "刷新完成。",
            "scope_label": "全部材料",
            "scope_key": "all",
            "started_at": "2026-04-17 10:00",
            "finished_at": "2026-04-17 10:01",
            "started_ts": 100.0,
            "finished_ts": 160.0,
            "error": "",
            "snapshot_label": "",
            "snapshot_copy": "",
            "snapshot_source_path": "",
            "snapshot_source_count": 0,
            "contract_status_label": "",
            "contract_status_copy": "",
            "artifact_summary_label": "",
            "artifact_summary_copy": "",
            "updated_artifacts": [],
            "baseline_artifact_mtimes": {},
        },
        mode="demo",
        anchor="framework",
        open_panels="demo-design-detail-tables",
        snapshot_meta={
            "label": "2026-04-17 10:02",
            "copy": "snapshot",
            "source_path": "results/real_tables/event_study_summary.csv",
            "source_count": 1,
        },
        contract_check={
            "manifest_exists": True,
            "manifest_path": "results/real_tables/results_manifest.csv",
            "manifest_profile": "real",
            "matches": True,
            "mismatched_fields": [],
            "live_status": {"mode": "reconstructed"},
            "manifest": {"rdd_mode": "reconstructed"},
        },
        redirect_url_builder=lambda mode, anchor, open_panels: f"/?mode={mode}&open={open_panels}#{anchor}",
        now_ts=170.0,
    )

    assert success_payload["redirect_url"] == "/?mode=demo&open=demo-design-detail-tables#framework"
    assert success_payload["artifact_summary_label"] == "本次未发现新的核心产物"
    assert "结果状态：结果状态已同步" in success_payload["artifact_summary_copy"]


def test_set_refresh_succeeded_records_contract_sync_status() -> None:
    refresh_state = dashboard_refresh.default_refresh_state()

    dashboard_refresh.set_refresh_succeeded(
        Lock(),
        refresh_state,
        scope_label="全部材料",
        scope_key="all",
        snapshot_meta={
            "label": "2026-04-17 10:02",
            "copy": "snapshot",
            "source_path": "results/real_tables/event_study_summary.csv",
            "source_count": 2,
        },
        contract_check={
            "manifest_exists": False,
            "manifest_path": "results/real_tables/results_manifest.csv",
            "manifest_profile": "",
            "matches": False,
            "mismatched_fields": [],
            "live_status": {"mode": "missing"},
            "manifest": {},
        },
        updated_artifacts=[],
        finished_at="2026-04-17 10:03",
        finished_ts=180.0,
    )

    assert refresh_state["contract_status_label"] == "缺少结果状态文件"
    assert "results/real_tables/results_manifest.csv" in refresh_state["contract_status_copy"]
    assert refresh_state["message"] == "“全部材料”刷新完成，本次更新已同步。"
    assert refresh_state["artifact_summary_label"] == "本次未发现新的核心产物"
    assert "结果状态：缺少结果状态文件" in refresh_state["artifact_summary_copy"]


def test_queue_refresh_job_sets_running_state_and_blocks_parallel_runs() -> None:
    refresh_state = dashboard_refresh.default_refresh_state()
    spawned: list[tuple[str, str]] = []

    def _spawn(runner, scope_label: str, scope_key: str) -> None:
        spawned.append((scope_label, scope_key))

    queued = dashboard_refresh.queue_refresh_job(
        Lock(),
        refresh_state,
        runner=lambda: None,
        scope_label="全部材料",
        scope_key="all",
        started_at="2026-04-17 10:00",
        started_ts=100.0,
        snapshot_meta={
            "label": "2026-04-17 10:00",
            "copy": "snapshot copy",
            "source_path": "results/real_tables/event_study_summary.csv",
            "source_count": 1,
        },
        baseline_artifact_mtimes={},
        spawn_refresh_worker=_spawn,
    )

    assert queued is True
    assert refresh_state["status"] == "running"
    assert refresh_state["scope_key"] == "all"
    assert refresh_state["snapshot_source_path"] == "results/real_tables/event_study_summary.csv"
    assert spawned == [("全部材料", "all")]

    blocked = dashboard_refresh.queue_refresh_job(
        Lock(),
        refresh_state,
        runner=lambda: None,
        scope_label="短期价格压力与效应减弱",
        scope_key="price_pressure_track",
        started_at="2026-04-17 10:01",
        started_ts=101.0,
        snapshot_meta={
            "label": "2026-04-17 10:01",
            "copy": "snapshot copy",
            "source_path": "results/real_tables/event_study_summary.csv",
            "source_count": 1,
        },
        baseline_artifact_mtimes={},
        spawn_refresh_worker=_spawn,
    )

    assert blocked is False


def test_run_refresh_job_routes_success_and_failure_callbacks() -> None:
    events: list[tuple[str, str]] = []

    dashboard_refresh.run_refresh_job(
        lambda: None,
        "全部材料",
        "all",
        mark_refresh_succeeded=lambda scope_label, scope_key: events.append(("success", scope_key)),
        mark_refresh_failed=lambda scope_label, scope_key, exc: events.append(("failed", scope_key)),
    )
    dashboard_refresh.run_refresh_job(
        lambda: (_ for _ in ()).throw(ValueError("boom")),
        "全部材料",
        "all",
        mark_refresh_succeeded=lambda scope_label, scope_key: events.append(("success", scope_key)),
        mark_refresh_failed=lambda scope_label, scope_key, exc: events.append(("failed", scope_key)),
    )

    assert events == [("success", "all"), ("failed", "all")]

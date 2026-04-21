from __future__ import annotations

from pathlib import Path

from flask import Flask, request
import pytest

from index_inclusion_research import dashboard_routes
from index_inclusion_research.dashboard_types import RefreshStatusPayload


def _refresh_payload(mode: str, anchor: str, open_panels: str | None) -> RefreshStatusPayload:
    return {
        "accepted": True,
        "status": "running",
        "message": "刷新中",
        "error": "",
        "scope_label": "全部材料",
        "scope_key": "all",
        "started_at": "",
        "finished_at": "",
        "started_ts": 0.0,
        "finished_ts": 0.0,
        "duration_seconds": None,
        "poll_after_ms": 1200,
        "redirect_url": "",
        "snapshot_label": mode,
        "snapshot_copy": f"{anchor}:{open_panels or ''}",
        "snapshot_source_path": "results/real_tables/event_study_summary.csv",
        "snapshot_source_count": 1,
        "contract_status_label": "",
        "contract_status_copy": "",
        "artifact_summary_label": "",
        "artifact_summary_copy": "",
        "updated_artifacts": [],
    }


def test_dashboard_request_adapter_parses_query_and_form_state() -> None:
    app = Flask(__name__)
    with app.test_request_context(
        "/refresh/status?mode=full&anchor=framework&open=demo-design-detail-tables",
        method="POST",
        data={"anchor": "tracks", "open": "demo-framework-detail-tables"},
    ):
        adapter = dashboard_routes.DashboardRequestAdapter(
            details_query_param="open",
            request_proxy=request,
            dashboard_mode=lambda: "full",
            normalize_open_panels=lambda raw: raw or "",
            normalize_anchor_for_mode=lambda mode, anchor: anchor or "overview",
        )

        assert adapter.home_state() == dashboard_routes.DashboardHomeRequestState(
            display_mode="full",
            current_open_panels="demo-design-detail-tables",
        )
        assert adapter.args_state(default_anchor="overview") == dashboard_routes.DashboardRouteState(
            mode="full",
            anchor="framework",
            open_panels="demo-design-detail-tables",
        )
        assert adapter.form_state(default_anchor="overview") == dashboard_routes.DashboardRouteState(
            mode="full",
            anchor="tracks",
            open_panels="demo-framework-detail-tables",
        )


def test_handle_refresh_async_returns_json_payload() -> None:
    app = Flask(__name__)
    queued_runner = None

    def _queue_refresh_job(runner, scope_label: str, scope_key: str) -> bool:
        nonlocal queued_runner
        queued_runner = runner
        return True

    with app.test_request_context("/refresh"):
        response, status = dashboard_routes.handle_refresh(
            mode="demo",
            anchor="framework",
            open_panels="demo-design-detail-tables",
            wants_async_refresh=True,
            queue_refresh_job=_queue_refresh_job,
            run_and_cache_all=lambda: None,
            refresh_status_payload=_refresh_payload,
            refresh_redirect_url=lambda mode, anchor, open_panels: "/ignored",
        )

    assert status == 202
    assert response.get_json()["status"] == "running"
    assert response.get_json()["accepted"] is True
    assert response.get_json()["snapshot_label"] == "demo"
    assert queued_runner is not None
    assert queued_runner() is None


def test_handle_refresh_async_returns_busy_payload_when_job_already_running() -> None:
    app = Flask(__name__)

    with app.test_request_context("/refresh"):
        response, status = dashboard_routes.handle_refresh(
            mode="demo",
            anchor="framework",
            open_panels="demo-design-detail-tables",
            wants_async_refresh=True,
            queue_refresh_job=lambda runner, scope_label, scope_key: False,
            run_and_cache_all=lambda: None,
            refresh_status_payload=_refresh_payload,
            refresh_redirect_url=lambda mode, anchor, open_panels: "/ignored",
        )

    assert status == 409
    assert response.get_json()["accepted"] is False
    assert "未重复排队" in response.get_json()["message"]


def test_handle_refresh_sync_redirects_after_refresh() -> None:
    app = Flask(__name__)
    called: list[str] = []
    with app.test_request_context("/refresh"):
        response = dashboard_routes.handle_refresh(
            mode="full",
            anchor="tracks",
            open_panels=None,
            wants_async_refresh=False,
            queue_refresh_job=lambda runner, scope_label, scope_key: None,
            run_and_cache_all=lambda: called.append("ran"),
            refresh_status_payload=lambda mode, anchor, open_panels: {},
            refresh_redirect_url=lambda mode, anchor, open_panels: f"/?mode={mode}#{anchor}",
        )

    assert called == ["ran"]
    assert response.status_code == 302
    assert response.headers["Location"] == "/?mode=full#tracks"


def test_handle_run_analysis_404s_for_unknown_analysis() -> None:
    app = Flask(__name__)
    with app.test_request_context("/run/unknown"):
        with pytest.raises(Exception) as exc_info:
            dashboard_routes.handle_run_analysis(
                "unknown",
                analyses={},
                mode="demo",
                anchor="overview",
                open_panels=None,
                wants_async_refresh=False,
                queue_refresh_job=lambda runner, scope_label, scope_key: None,
                refresh_status_payload=lambda mode, anchor, open_panels: {},
                run_and_cache_analysis=lambda analysis_id: None,
                refresh_redirect_url=lambda mode, anchor, open_panels: "/",
            )

    assert getattr(exc_info.value, "code", None) == 404


def test_dashboard_run_analysis_handler_async_returns_json_payload() -> None:
    app = Flask(__name__)
    queued: list[tuple[str, str]] = []
    calls: list[str] = []

    def _queue_refresh_job(runner, scope_label: str, scope_key: str) -> bool:
        queued.append((scope_label, scope_key))
        calls.append(str(runner()))
        return True

    with app.test_request_context("/run/price_pressure_track"):
        response, status = dashboard_routes.DashboardRunAnalysisHandler(
            analyses={
                "price_pressure_track": {
                    "title": "短期价格压力与效应减弱",
                    "subtitle": "Price Pressure",
                    "description_zh": "desc",
                    "project_module": "短期价格压力",
                    "runner": lambda verbose=False: {},
                }
            },
            wants_async_refresh=lambda: True,
            queue_refresh_job=_queue_refresh_job,
            refresh_status_payload=_refresh_payload,
            run_and_cache_analysis=lambda analysis_id: calls.append(analysis_id) or {"id": analysis_id},
            refresh_redirect_url=lambda mode, anchor, open_panels: "/ignored",
        ).handle(
            "price_pressure_track",
            dashboard_routes.DashboardRouteState(
                mode="demo",
                anchor="price_pressure_track",
                open_panels=None,
            ),
        )

    assert status == 202
    assert queued == [("短期价格压力与效应减弱", "price_pressure_track")]
    assert response.get_json()["accepted"] is True
    assert response.get_json()["snapshot_copy"] == "price_pressure_track:"
    assert calls == ["price_pressure_track", "None"]


def test_dashboard_run_analysis_handler_returns_busy_payload_when_job_already_running() -> None:
    app = Flask(__name__)

    with app.test_request_context("/run/price_pressure_track"):
        response, status = dashboard_routes.DashboardRunAnalysisHandler(
            analyses={
                "price_pressure_track": {
                    "title": "短期价格压力与效应减弱",
                    "subtitle": "Price Pressure",
                    "description_zh": "desc",
                    "project_module": "短期价格压力",
                    "runner": lambda verbose=False: {},
                }
            },
            wants_async_refresh=lambda: True,
            queue_refresh_job=lambda runner, scope_label, scope_key: False,
            refresh_status_payload=_refresh_payload,
            run_and_cache_analysis=lambda analysis_id: {"id": analysis_id},
            refresh_redirect_url=lambda mode, anchor, open_panels: "/ignored",
        ).handle(
            "price_pressure_track",
            dashboard_routes.DashboardRouteState(
                mode="demo",
                anchor="price_pressure_track",
                open_panels=None,
            ),
        )

    assert status == 409
    assert response.get_json()["accepted"] is False
    assert "未重复排队" in response.get_json()["message"]


def test_serve_result_file_returns_file_content(tmp_path: Path) -> None:
    app = Flask(__name__)
    target = tmp_path / "demo.txt"
    target.write_text("hello", encoding="utf-8")

    with app.test_request_context("/files/demo.txt"):
        response = dashboard_routes.serve_result_file(tmp_path, "demo.txt")

    assert response.status_code == 200
    response.direct_passthrough = False
    assert response.get_data(as_text=True) == "hello"

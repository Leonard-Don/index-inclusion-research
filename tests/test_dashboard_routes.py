from __future__ import annotations

from pathlib import Path

from flask import Flask, request
import pytest

from index_inclusion_research import dashboard_routes


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
    with app.test_request_context("/refresh"):
        response, status = dashboard_routes.handle_refresh(
            mode="demo",
            anchor="framework",
            open_panels="demo-design-detail-tables",
            wants_async_refresh=True,
            queue_refresh_job=lambda runner, scope_label, scope_key: None,
            run_and_cache_all=lambda: None,
            refresh_status_payload=lambda mode, anchor, open_panels: {
                "status": "running",
                "mode": mode,
                "anchor": anchor,
                "open_panels": open_panels,
            },
            refresh_redirect_url=lambda mode, anchor, open_panels: "/ignored",
        )

    assert status == 202
    assert response.get_json() == {
        "status": "running",
        "mode": "demo",
        "anchor": "framework",
        "open_panels": "demo-design-detail-tables",
    }


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
            queue_refresh_job=lambda runner, scope_label, scope_key: queued.append((scope_label, scope_key)),
            refresh_status_payload=lambda mode, anchor, open_panels: {
                "status": "running",
                "mode": mode,
                "anchor": anchor,
                "open_panels": open_panels,
            },
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

    assert status == 202
    assert queued == [("短期价格压力与效应减弱", "price_pressure_track")]
    assert response.get_json()["anchor"] == "price_pressure_track"


def test_serve_result_file_returns_file_content(tmp_path: Path) -> None:
    app = Flask(__name__)
    target = tmp_path / "demo.txt"
    target.write_text("hello", encoding="utf-8")

    with app.test_request_context("/files/demo.txt"):
        response = dashboard_routes.serve_result_file(tmp_path, "demo.txt")

    assert response.status_code == 200
    response.direct_passthrough = False
    assert response.get_data(as_text=True) == "hello"

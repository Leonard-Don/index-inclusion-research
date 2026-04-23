from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from flask import Flask, request, url_for

from index_inclusion_research.dashboard_route_bindings import (
    DashboardRouteDependencies,
    DashboardRouteFactory,
    build_dashboard_route_dependencies,
    build_dashboard_route_views,
    build_home_url_builder,
)


def _runtime_stub() -> SimpleNamespace:
    return SimpleNamespace(
        build_home_context=lambda **kwargs: {"mode": kwargs["display_mode"]},
    )


def test_build_home_url_builder_targets_home_endpoint() -> None:
    app = Flask(__name__)
    app.add_url_rule("/", endpoint="home", view_func=lambda: "ok")
    home_url_builder = build_home_url_builder(url_for)

    with app.test_request_context("/"):
        assert home_url_builder(mode="demo") == "/?mode=demo"


def test_build_dashboard_route_views_uses_live_namespace_callables() -> None:
    app = Flask(__name__)
    app.add_url_rule("/", endpoint="home", view_func=lambda: "ok")
    app.add_url_rule("/refresh/status", endpoint="refresh_status", view_func=lambda: "ok")

    services = SimpleNamespace(
        request_proxy=request,
        runtime=_runtime_stub(),
        dashboard_mode=lambda: "demo",
        normalize_open_panels=lambda raw: raw or "",
        mode_tabs_for_mode=lambda mode, open_panels=None: [],
        refresh_status_payload=lambda mode, anchor, open_panels=None: {
            "accepted": True,
            "status": "idle",
            "message": "",
            "error": "",
            "mode": mode,
            "anchor": anchor,
            "open_panels": open_panels,
            "scope_label": "全部材料",
            "scope_key": "all",
            "started_at": "",
            "finished_at": "",
            "started_ts": 0.0,
            "finished_ts": 0.0,
            "duration_seconds": None,
            "poll_after_ms": 1200,
            "redirect_url": "",
            "snapshot_label": "snapshot",
            "snapshot_copy": "",
            "snapshot_source_path": "results/real_tables/event_study_summary.csv",
            "snapshot_source_count": 1,
            "contract_status_label": "",
            "contract_status_copy": "",
            "artifact_summary_label": "",
            "artifact_summary_copy": "",
            "updated_artifacts": [],
        },
        normalize_anchor_for_mode=lambda mode, anchor: anchor or "overview",
        wants_async_refresh=lambda: False,
        queue_refresh_job=lambda runner, scope_label, scope_key: None,
        run_and_cache_all=lambda: None,
        run_and_cache_analysis=lambda analysis_id: None,
        refresh_redirect_url=lambda mode, anchor, open_panels=None: f"/?mode={mode}#{anchor}",
        load_paper_detail_result=lambda paper_id: {"id": paper_id},
    )
    calls: list[str] = []
    route_views = build_dashboard_route_views(
        services=services,
        url_builder=url_for,
        details_query_param="open",
        analyses={"price_pressure_track": {"title": "短期价格压力与效应减弱"}},
        root=Path("/tmp"),
        get_literature_paper=lambda paper_id: None,
    )
    services.run_and_cache_analysis = lambda analysis_id: calls.append(analysis_id)

    with app.test_request_context(
        "/run/price_pressure_track?mode=demo",
        method="POST",
        data={"anchor": "price_pressure_track"},
    ):
        response = route_views.run_analysis("price_pressure_track")

    assert response.status_code == 302
    assert response.headers["Location"] == "/?mode=demo#price_pressure_track"
    assert calls == ["price_pressure_track"]


def test_build_dashboard_route_dependencies_and_factory_remain_explicit() -> None:
    app = Flask(__name__)
    app.add_url_rule("/", endpoint="home", view_func=lambda: "ok")
    app.add_url_rule("/refresh/status", endpoint="refresh_status", view_func=lambda: "ok")

    services = SimpleNamespace(
        request_proxy=request,
        runtime=_runtime_stub(),
        dashboard_mode=lambda: "demo",
        normalize_open_panels=lambda raw: raw or "",
        mode_tabs_for_mode=lambda mode, open_panels=None: [],
        refresh_status_payload=lambda mode, anchor, open_panels=None: {
            "accepted": True,
            "status": "idle",
            "message": "",
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
            "snapshot_label": "snapshot",
            "snapshot_copy": "",
            "snapshot_source_path": "results/real_tables/event_study_summary.csv",
            "snapshot_source_count": 1,
            "contract_status_label": "",
            "contract_status_copy": "",
            "artifact_summary_label": "",
            "artifact_summary_copy": "",
            "updated_artifacts": [],
        },
        normalize_anchor_for_mode=lambda mode, anchor: anchor or "overview",
        wants_async_refresh=lambda: False,
        queue_refresh_job=lambda runner, scope_label, scope_key: None,
        run_and_cache_all=lambda: None,
        run_and_cache_analysis=lambda analysis_id: {"id": analysis_id},
        refresh_redirect_url=lambda mode, anchor, open_panels=None: f"/?mode={mode}#{anchor}",
        load_paper_detail_result=lambda paper_id: {"id": paper_id},
    )

    dependencies = build_dashboard_route_dependencies(services)
    route_factory = DashboardRouteFactory(
        dependencies=dependencies,
        url_builder=url_for,
        details_query_param="open",
        analyses={"price_pressure_track": {"title": "短期价格压力与效应减弱"}},
        root=Path("/tmp"),
        get_literature_paper=lambda paper_id: None,
    )

    assert isinstance(dependencies, DashboardRouteDependencies)
    assert isinstance(route_factory, DashboardRouteFactory)
    assert dependencies.request_proxy is services.request_proxy
    assert dependencies.runtime is services.runtime

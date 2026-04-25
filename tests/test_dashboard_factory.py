from __future__ import annotations

import time
from pathlib import Path

from flask import Response, request, url_for

from index_inclusion_research import dashboard_config
from index_inclusion_research.dashboard_factory import (
    DashboardShell,
    build_dashboard_application,
    build_dashboard_shell,
    create_dashboard_app,
    register_dashboard_routes,
)
from index_inclusion_research.dashboard_route_bindings import DashboardRouteViews


def test_build_analyses_and_panel_keys() -> None:
    analyses = dashboard_config.build_analyses(
        run_price_pressure_track=lambda verbose=False: {},
        run_demand_curve_track=lambda verbose=False: {},
        run_identification_china_track=lambda verbose=False: {},
    )

    assert set(analyses) == {
        "price_pressure_track",
        "demand_curve_track",
        "identification_china_track",
    }
    keys = dashboard_config.build_details_panel_keys(analyses)
    assert "demo-price_pressure_track-detail-tables" in keys
    assert "demo-identification_china_track-support-papers" in keys


def test_create_dashboard_app_and_register_routes() -> None:
    app = create_dashboard_app(
        import_name=__name__,
        template_folder="/tmp/templates",
        static_folder="/tmp/static",
        static_url_path="/assets",
    )

    def _ok(*args, **kwargs):
        return Response("ok", mimetype="text/plain")

    register_dashboard_routes(
        app,
        root=Path("/tmp/example"),
        favicon_view=lambda: ("", 204),
        home_view=_ok,
        refresh_dashboard_view=_ok,
        refresh_status_view=_ok,
        run_analysis_view=_ok,
        show_library_view=_ok,
        show_review_view=_ok,
        show_framework_view=_ok,
        show_supplement_view=_ok,
        show_analysis_view=_ok,
        serve_result_file_view=_ok,
        show_paper_brief_view=_ok,
        serve_library_pdf_view=_ok,
    )

    assert app.template_folder == "/tmp/templates"
    assert app.static_folder.endswith("/tmp/static")
    rules = {rule.rule for rule in app.url_map.iter_rules()}
    assert "/" in rules
    assert "/refresh" in rules
    assert "/run/<analysis_id>" in rules
    assert "/paper/<paper_id>/pdf" in rules
    assert "/api/chart/<chart_id>" in rules


def test_register_dashboard_routes_uses_stable_endpoint_names() -> None:
    app = create_dashboard_app(
        import_name=__name__,
        template_folder="/tmp/templates",
        static_folder="/tmp/static",
    )

    def _named(name: str):
        def _ok(*args, **kwargs):
            return Response(name, mimetype="text/plain")

        _ok.__name__ = f"custom_{name}"
        return _ok

    register_dashboard_routes(
        app,
        root=Path("/tmp/example"),
        favicon_view=lambda: ("", 204),
        home_view=_named("home"),
        refresh_dashboard_view=_named("refresh"),
        refresh_status_view=_named("status"),
        run_analysis_view=_named("run"),
        show_library_view=_named("library"),
        show_review_view=_named("review"),
        show_framework_view=_named("framework"),
        show_supplement_view=_named("supplement"),
        show_analysis_view=_named("analysis"),
        serve_result_file_view=_named("files"),
        show_paper_brief_view=_named("paper"),
        serve_library_pdf_view=_named("pdf"),
    )

    with app.test_request_context("/"):
        assert url_for("home") == "/"
        assert url_for("refresh_status") == "/refresh/status"
        assert url_for("show_analysis", analysis_id="demo") == "/analysis/demo"


def test_build_dashboard_shell_wires_runtime_refresh_and_app() -> None:
    shell = build_dashboard_shell(
        import_name=__name__,
        root=Path("/tmp/example"),
        template_folder="/tmp/example/src/index_inclusion_research/web/templates",
        static_folder="/tmp/example/src/index_inclusion_research/web/static",
        run_price_pressure_track=lambda verbose=False: {},
        run_demand_curve_track=lambda verbose=False: {},
        run_identification_china_track=lambda verbose=False: {},
    )

    assert set(shell.analyses) == {
        "price_pressure_track",
        "demand_curve_track",
        "identification_china_track",
    }
    assert shell.runtime.run_cache == {}
    assert shell.refresh_coordinator.state["status"] == "idle"
    assert shell.app.static_folder.endswith("/tmp/example/src/index_inclusion_research/web/static")


def test_build_dashboard_application_wires_services_routes_and_app() -> None:
    application = build_dashboard_application(
        import_name=__name__,
        root=Path("/tmp/example"),
        template_folder="/tmp/example/src/index_inclusion_research/web/templates",
        static_folder="/tmp/example/src/index_inclusion_research/web/static",
        run_price_pressure_track=lambda verbose=False: {},
        run_demand_curve_track=lambda verbose=False: {},
        run_identification_china_track=lambda verbose=False: {},
        request_proxy=request,
        url_builder=url_for,
        time_module=time,
        get_literature_paper=lambda paper_id: None,
    )

    assert isinstance(application.shell, DashboardShell)
    assert isinstance(application.route_views, DashboardRouteViews)
    assert application.services.runtime is application.shell.runtime
    assert application.services.refresh_coordinator is application.shell.refresh_coordinator
    assert application.route_views.home.__name__ == "home"
    assert application.route_views.registration_kwargs()["home_view"] is application.route_views.home
    assert application.app is application.shell.app

from __future__ import annotations

import time
from pathlib import Path

from flask import request, url_for

from index_inclusion_research.dashboard_application import build_dashboard_application
from index_inclusion_research.dashboard_factory import DashboardShell
from index_inclusion_research.dashboard_route_bindings import DashboardRouteViews


def test_build_dashboard_application_wires_services_routes_and_app() -> None:
    application = build_dashboard_application(
        import_name=__name__,
        root=Path("/tmp/example"),
        template_folder="/tmp/example/scripts/templates",
        static_folder="/tmp/example/scripts/static",
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

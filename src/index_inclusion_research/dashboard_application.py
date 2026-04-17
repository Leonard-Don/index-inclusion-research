from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from flask import Flask

from index_inclusion_research.dashboard_factory import (
    DashboardShell,
    build_dashboard_shell,
    register_dashboard_route_views,
)
from index_inclusion_research.dashboard_route_bindings import (
    DashboardRouteViews,
    build_dashboard_route_views,
    build_home_url_builder,
)
from index_inclusion_research.dashboard_services import DashboardServices, build_dashboard_services
from index_inclusion_research.dashboard_types import (
    AnalysisRunner,
    EndpointUrlBuilder,
    LiteraturePaperLookup,
    RequestProxyLike,
    TimeModuleLike,
)


@dataclass(frozen=True)
class DashboardApplication:
    shell: DashboardShell
    services: DashboardServices
    route_views: DashboardRouteViews
    app: Flask


def build_dashboard_application(
    *,
    import_name: str,
    root: Path,
    template_folder: str,
    static_folder: str,
    run_price_pressure_track: AnalysisRunner,
    run_demand_curve_track: AnalysisRunner,
    run_identification_china_track: AnalysisRunner,
    request_proxy: RequestProxyLike,
    url_builder: EndpointUrlBuilder,
    time_module: TimeModuleLike,
    get_literature_paper: LiteraturePaperLookup,
) -> DashboardApplication:
    shell = build_dashboard_shell(
        import_name=import_name,
        root=root,
        template_folder=template_folder,
        static_folder=static_folder,
        run_price_pressure_track=run_price_pressure_track,
        run_demand_curve_track=run_demand_curve_track,
        run_identification_china_track=run_identification_china_track,
    )
    services = build_dashboard_services(
        runtime=shell.runtime,
        refresh_coordinator=shell.refresh_coordinator,
        request_proxy=request_proxy,
        time_module=time_module,
        details_query_param=shell.details_query_param,
        home_url_builder=build_home_url_builder(url_builder),
    )
    route_views = build_dashboard_route_views(
        services=services,
        url_builder=url_builder,
        details_query_param=shell.details_query_param,
        analyses=shell.analyses,
        root=root,
        get_literature_paper=get_literature_paper,
    )
    register_dashboard_route_views(
        shell.app,
        route_registrations=route_views.registration_kwargs(),
    )
    return DashboardApplication(
        shell=shell,
        services=services,
        route_views=route_views,
        app=shell.app,
    )

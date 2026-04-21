from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from flask import Flask

from index_inclusion_research import dashboard_config
from index_inclusion_research.dashboard_refresh_coordinator import DashboardRefreshCoordinator
from index_inclusion_research.dashboard_runtime import DashboardRuntime
from index_inclusion_research.dashboard_types import (
    AnalysesConfig,
    AnalysisRunner,
    DashboardCard,
    DashboardRuntimeLike,
    DashboardRouteRegistrationMap,
    RouteView,
)


@dataclass(frozen=True)
class DashboardShell:
    analyses: AnalysesConfig
    library_card: DashboardCard
    review_card: DashboardCard
    framework_card: DashboardCard
    supplement_card: DashboardCard
    project_module_display: dict[str, str]
    details_query_param: str
    details_panel_keys: frozenset[str]
    runtime: DashboardRuntimeLike
    refresh_coordinator: DashboardRefreshCoordinator
    app: Flask


def build_dashboard_runtime(
    *,
    root: Path,
    analyses: AnalysesConfig,
    library_card: DashboardCard,
    review_card: DashboardCard,
    framework_card: DashboardCard,
    supplement_card: DashboardCard,
    project_module_display_map: dict[str, str],
) -> DashboardRuntime:
    return DashboardRuntime(
        root=root,
        analyses=analyses,
        library_card=library_card,
        review_card=review_card,
        framework_card=framework_card,
        supplement_card=supplement_card,
        project_module_display_map=project_module_display_map,
    )


def build_refresh_coordinator(
    *,
    details_query_param: str,
    allowed_keys: frozenset[str],
    track_anchors: frozenset[str],
    runtime: DashboardRuntimeLike,
) -> DashboardRefreshCoordinator:
    return DashboardRefreshCoordinator(
        details_query_param=details_query_param,
        allowed_keys=allowed_keys,
        track_anchors=track_anchors,
        dashboard_snapshot_sources=runtime.dashboard_snapshot_sources,
        build_rdd_contract_check=runtime.load_rdd_contract_check,
        to_relative=runtime.safe_relative,
        build_dashboard_snapshot_meta=runtime.build_dashboard_snapshot_meta,
        nav_sections_for_mode=runtime.nav_sections_for_mode,
    )


def create_dashboard_app(
    *,
    import_name: str,
    template_folder: str,
    static_folder: str,
    static_url_path: str = "/static",
) -> Flask:
    return Flask(
        import_name,
        template_folder=template_folder,
        static_folder=static_folder,
        static_url_path=static_url_path,
    )


def build_dashboard_shell(
    *,
    import_name: str,
    root: Path,
    template_folder: str,
    static_folder: str,
    run_price_pressure_track: AnalysisRunner,
    run_demand_curve_track: AnalysisRunner,
    run_identification_china_track: AnalysisRunner,
) -> DashboardShell:
    analyses = dashboard_config.build_analyses(
        run_price_pressure_track=run_price_pressure_track,
        run_demand_curve_track=run_demand_curve_track,
        run_identification_china_track=run_identification_china_track,
    )
    project_module_display = dashboard_config.build_project_module_display(analyses)
    details_query_param = dashboard_config.DETAILS_QUERY_PARAM
    details_panel_keys = dashboard_config.build_details_panel_keys(analyses)
    runtime = build_dashboard_runtime(
        root=root,
        analyses=analyses,
        library_card=dashboard_config.LIBRARY_CARD,
        review_card=dashboard_config.REVIEW_CARD,
        framework_card=dashboard_config.FRAMEWORK_CARD,
        supplement_card=dashboard_config.SUPPLEMENT_CARD,
        project_module_display_map=project_module_display,
    )
    refresh_coordinator = build_refresh_coordinator(
        details_query_param=details_query_param,
        allowed_keys=details_panel_keys,
        track_anchors=frozenset(analyses),
        runtime=runtime,
    )
    app = create_dashboard_app(
        import_name=import_name,
        template_folder=template_folder,
        static_folder=static_folder,
        static_url_path="/static",
    )
    return DashboardShell(
        analyses=analyses,
        library_card=dashboard_config.LIBRARY_CARD,
        review_card=dashboard_config.REVIEW_CARD,
        framework_card=dashboard_config.FRAMEWORK_CARD,
        supplement_card=dashboard_config.SUPPLEMENT_CARD,
        project_module_display=project_module_display,
        details_query_param=details_query_param,
        details_panel_keys=details_panel_keys,
        runtime=runtime,
        refresh_coordinator=refresh_coordinator,
        app=app,
    )


def register_dashboard_routes(
    app: Flask,
    *,
    favicon_view: RouteView,
    home_view: RouteView,
    refresh_dashboard_view: RouteView,
    refresh_status_view: RouteView,
    run_analysis_view: RouteView,
    show_library_view: RouteView,
    show_review_view: RouteView,
    show_framework_view: RouteView,
    show_supplement_view: RouteView,
    show_analysis_view: RouteView,
    serve_result_file_view: RouteView,
    show_paper_brief_view: RouteView,
    serve_library_pdf_view: RouteView,
) -> Flask:
    app.add_url_rule("/favicon.ico", endpoint="favicon", view_func=favicon_view, methods=["GET"])
    app.add_url_rule("/", endpoint="home", view_func=home_view, methods=["GET"])
    app.add_url_rule("/refresh", endpoint="refresh_dashboard", view_func=refresh_dashboard_view, methods=["POST"])
    app.add_url_rule("/refresh/status", endpoint="refresh_status", view_func=refresh_status_view, methods=["GET"])
    app.add_url_rule("/run/<analysis_id>", endpoint="run_analysis", view_func=run_analysis_view, methods=["POST"])
    app.add_url_rule("/library", endpoint="show_library", view_func=show_library_view, methods=["GET"])
    app.add_url_rule("/review", endpoint="show_review", view_func=show_review_view, methods=["GET"])
    app.add_url_rule("/framework", endpoint="show_framework", view_func=show_framework_view, methods=["GET"])
    app.add_url_rule("/supplement", endpoint="show_supplement", view_func=show_supplement_view, methods=["GET"])
    app.add_url_rule("/analysis/<analysis_id>", endpoint="show_analysis", view_func=show_analysis_view, methods=["GET"])
    app.add_url_rule("/files/<path:subpath>", endpoint="serve_result_file", view_func=serve_result_file_view, methods=["GET"])
    app.add_url_rule("/paper/<paper_id>", endpoint="show_paper_brief", view_func=show_paper_brief_view, methods=["GET"])
    app.add_url_rule("/paper/<paper_id>/pdf", endpoint="serve_library_pdf", view_func=serve_library_pdf_view, methods=["GET"])
    return app


def register_dashboard_route_views(
    app: Flask,
    route_registrations: DashboardRouteRegistrationMap,
) -> Flask:
    return register_dashboard_routes(app, **route_registrations)

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from flask import Flask

from index_inclusion_research import dashboard_config
from index_inclusion_research.chart_data import build_chart_data
from index_inclusion_research.dashboard_refresh_coordinator import (
    DashboardRefreshCoordinator,
)
from index_inclusion_research.dashboard_route_bindings import (
    DashboardRouteViews,
    build_dashboard_route_views,
    build_home_url_builder,
)
from index_inclusion_research.dashboard_runtime import DashboardRuntime
from index_inclusion_research.dashboard_services import (
    DashboardServices,
    build_dashboard_services,
)
from index_inclusion_research.dashboard_types import (
    AnalysesConfig,
    AnalysisRunner,
    DashboardCard,
    DashboardRuntimeLike,
    EndpointUrlBuilder,
    LiteraturePaperLookup,
    RequestProxyLike,
    RouteView,
    TimeModuleLike,
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
        build_result_health=runtime.build_result_health,
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
    root: Path,
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
    app.add_url_rule(
        "/verdict/<hid>",
        endpoint="show_verdict_redirect",
        view_func=_make_verdict_redirect_view(),
        methods=["GET"],
    )
    _register_chart_api(app, root)
    _register_evidence_routes(app, root)
    _register_rdd_l3_workbench_routes(app, root)
    return app


def _make_verdict_redirect_view():
    """Build a small redirect view that maps /verdict/<hid> to the
    dashboard verdict-card anchor on the home page.

    Validates ``hid`` against the canonical CMA hypothesis registry so
    typo'd URLs return 404 instead of silently dumping the user on a
    page with no matching anchor.
    """
    from flask import abort, redirect

    from index_inclusion_research.analysis.cross_market_asymmetry.hypotheses import (
        HYPOTHESES,
    )

    valid = {h.hid for h in HYPOTHESES}

    def show_verdict_redirect(hid: str):
        if hid not in valid:
            abort(404)
        return redirect(f"/?mode=full#hypothesis-{hid}", code=302)

    show_verdict_redirect.__name__ = "show_verdict_redirect"
    return show_verdict_redirect


def _register_chart_api(app: Flask, root: Path) -> None:
    """Register the ``/api/chart/<chart_id>`` JSON endpoint."""
    from flask import abort, jsonify

    def chart_api(chart_id: str):
        data = build_chart_data(chart_id, root)
        if data is None:
            abort(404)
        return jsonify(data)

    chart_api.__name__ = "chart_api"
    app.add_url_rule("/api/chart/<chart_id>", endpoint="chart_api", view_func=chart_api, methods=["GET"])


def _register_evidence_routes(app: Flask, root: Path) -> None:
    """Register evidence drilldown page and JSON endpoints."""
    from flask import abort, jsonify, render_template

    from index_inclusion_research.evidence_drilldown import build_evidence_detail

    def show_evidence_detail(item: str):
        detail = build_evidence_detail(item, root=root)
        if detail is None:
            abort(404)
        return render_template("evidence_detail.html", detail=detail)

    def evidence_api(item: str):
        detail = build_evidence_detail(item, root=root)
        if detail is None:
            abort(404)
        return jsonify(detail)

    show_evidence_detail.__name__ = "show_evidence_detail"
    evidence_api.__name__ = "evidence_api"
    app.add_url_rule(
        "/evidence/<item>",
        endpoint="show_evidence_detail",
        view_func=show_evidence_detail,
        methods=["GET"],
    )
    app.add_url_rule(
        "/api/evidence/<item>",
        endpoint="evidence_api",
        view_func=evidence_api,
        methods=["GET"],
    )


def _rdd_defaults_from_form(form) -> dict[str, object]:
    from index_inclusion_research.rdd_l3_workbench import _defaults

    return _defaults(
        batch_id=form.get("batch_id") or None,
        announce_date=form.get("announce_date") or None,
        effective_date=form.get("effective_date") or None,
        source=form.get("source") or None,
        source_url=form.get("source_url") or None,
        note=form.get("note") or None,
        sector=form.get("sector") or None,
    )


def _split_online_search_terms(raw: str) -> tuple[str, ...]:
    terms = [term.strip() for term in raw.replace(";", "\n").replace("；", "\n").splitlines()]
    return tuple(term for term in terms if term)


def _register_rdd_l3_workbench_routes(app: Flask, root: Path) -> None:
    """Register the official HS300 RDD L3 candidate import workbench."""
    from flask import render_template, request

    from index_inclusion_research import rdd_l3_workbench

    def show_rdd_l3_workbench():
        context = rdd_l3_workbench.build_rdd_l3_workbench_context(root=root)
        return render_template("rdd_l3_workbench.html", **context)

    def check_rdd_l3_candidates():
        preflight = None
        error = ""
        try:
            storage = request.files.get("candidate_file")
            if storage is None or not storage.filename:
                raise ValueError("请选择要预检的候选名单文件。")
            input_path = rdd_l3_workbench.save_uploaded_candidate_file(storage)
            preflight = rdd_l3_workbench.build_candidate_preflight_result(
                input_path,
                sheet=request.form.get("sheet") or None,
                defaults=_rdd_defaults_from_form(request.form),
            )
        except Exception as exc:  # noqa: BLE001
            error = f"{type(exc).__name__}: {exc}"
        context = rdd_l3_workbench.build_rdd_l3_workbench_context(
            root=root,
            preflight_result=preflight,
            error=error,
        )
        return render_template("rdd_l3_workbench.html", **context)

    def import_rdd_l3_candidates():
        import_result = None
        error = ""
        try:
            storage = request.files.get("candidate_file")
            if storage is None or not storage.filename:
                raise ValueError("请选择要写入的官方候选名单文件。")
            if request.form.get("confirm_import") != "1":
                raise ValueError("正式写入前需要勾选确认项。")
            input_path = rdd_l3_workbench.save_uploaded_candidate_file(storage)
            import_result = rdd_l3_workbench.import_official_candidates(
                input_path,
                sheet=request.form.get("sheet") or None,
                defaults=_rdd_defaults_from_form(request.form),
            )
            import_result["refresh"] = rdd_l3_workbench.refresh_rdd_and_manifest()
        except Exception as exc:  # noqa: BLE001
            error = f"{type(exc).__name__}: {exc}"
        context = rdd_l3_workbench.build_rdd_l3_workbench_context(
            root=root,
            import_result=import_result,
            error=error,
        )
        return render_template("rdd_l3_workbench.html", **context)

    def refresh_rdd_l3_collection():
        collection_result = None
        error = ""
        try:
            raw_window = request.form.get("boundary_window") or ""
            boundary_window = int(raw_window) if raw_window.strip() else 15
            collection_result = rdd_l3_workbench.refresh_collection_package(
                root=root,
                boundary_window=boundary_window,
                force=True,
            )
        except Exception as exc:  # noqa: BLE001
            error = f"{type(exc).__name__}: {exc}"
        context = rdd_l3_workbench.build_rdd_l3_workbench_context(
            root=root,
            collection_result=collection_result,
            error=error,
        )
        return render_template("rdd_l3_workbench.html", **context)

    def refresh_rdd_l3_online_collection():
        online_collection_result = None
        error = ""
        try:
            raw_notice_rows = request.form.get("notice_rows") or ""
            notice_rows = (
                int(raw_notice_rows)
                if raw_notice_rows.strip()
                else rdd_l3_workbench.hs300_rdd_online_sources.DEFAULT_NOTICE_ROWS
            )
            raw_max_notices = request.form.get("max_notices") or ""
            max_notices = int(raw_max_notices) if raw_max_notices.strip() else None
            online_collection_result = rdd_l3_workbench.refresh_online_collection(
                root=root,
                since=request.form.get("since") or None,
                until=request.form.get("until") or None,
                notice_rows=notice_rows,
                max_notices=max_notices,
                extra_search_terms=_split_online_search_terms(request.form.get("search_term") or ""),
                force=True,
            )
        except Exception as exc:  # noqa: BLE001
            error = f"{type(exc).__name__}: {exc}"
        context = rdd_l3_workbench.build_rdd_l3_workbench_context(
            root=root,
            online_collection_result=online_collection_result,
            error=error,
        )
        return render_template("rdd_l3_workbench.html", **context)

    show_rdd_l3_workbench.__name__ = "show_rdd_l3_workbench"
    check_rdd_l3_candidates.__name__ = "check_rdd_l3_candidates"
    import_rdd_l3_candidates.__name__ = "import_rdd_l3_candidates"
    refresh_rdd_l3_collection.__name__ = "refresh_rdd_l3_collection"
    refresh_rdd_l3_online_collection.__name__ = "refresh_rdd_l3_online_collection"
    app.add_url_rule(
        "/rdd-l3",
        endpoint="show_rdd_l3_workbench",
        view_func=show_rdd_l3_workbench,
        methods=["GET"],
    )
    app.add_url_rule(
        "/rdd-l3/check",
        endpoint="check_rdd_l3_candidates",
        view_func=check_rdd_l3_candidates,
        methods=["POST"],
    )
    app.add_url_rule(
        "/rdd-l3/import",
        endpoint="import_rdd_l3_candidates",
        view_func=import_rdd_l3_candidates,
        methods=["POST"],
    )
    app.add_url_rule(
        "/rdd-l3/collection",
        endpoint="refresh_rdd_l3_collection",
        view_func=refresh_rdd_l3_collection,
        methods=["POST"],
    )
    app.add_url_rule(
        "/rdd-l3/online-collection",
        endpoint="refresh_rdd_l3_online_collection",
        view_func=refresh_rdd_l3_online_collection,
        methods=["POST"],
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
    register_dashboard_routes(shell.app, root=root, **route_views.registration_kwargs())
    return DashboardApplication(
        shell=shell,
        services=services,
        route_views=route_views,
        app=shell.app,
    )
